"""Gold specialist.neonatal_care_day: 2 dataset(s).

- datasets: _qc_clinical_neonatal_care_day, clinical_neonatal_care_day
- moved verbatim from: 3_gold_specialist_care:1876, 3_gold_specialist_care:1910, 3_gold_specialist_care:1918, 3_gold_specialist_care:1993, 3_gold_specialist_care:1997, 3_gold_specialist_care:2014

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.neonatal_care_day ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_NEONATAL_CARE_DAY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`entity_id` AS `entity_id`',
    'CASE WHEN CAST(`activity_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `activity_date` END AS `activity_date`',
    '`ward_location` AS `ward_location`',
    '`unit_function` AS `unit_function`',
    '`critical_care_start` AS `critical_care_start`',
    '`critical_care_discharge` AS `critical_care_discharge`',
    '`episode_link_status` AS `episode_link_status`',
    '`neonatal_episode_key` AS `neonatal_episode_key`',
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


# contract v2: QC/batch inputs come from internal _clinical_neonatal_care_day_metadata; source history stays on the main research table.
CLINICAL_NEONATAL_CARE_DAY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 507,781 rows of 507,781 that
    # are current and attributable; identity_status = 'resolved' keeps the 501,933 rows of
    # 507,781 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_NEONATAL_CARE_DAY_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.neonatal_care_day.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 5,848 of 507,781 at the profile.
    "gold.clinical.neonatal_care_day.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 507,781 at the profile.
    "gold.clinical.neonatal_care_day.record_status.default_view_active":
        "record_status = 'active'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1 of 507,781 rows
    # (0.000197%) when profiled on 2026-08-24.
    "gold.clinical.neonatal_care_day.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 48 of 507,781 rows (0.00945%) when profiled on 2026-08-24.
    "gold.clinical.neonatal_care_day.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 17,443 of 507,781 rows (3.44%) when profiled on 2026-08-24.
    "gold.clinical.neonatal_care_day.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_NEONATAL_CARE_DAY_PERSON_SCOPE_RULES = {
    "gold.clinical.neonatal_care_day.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_neonatal_care_day"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_NEONATAL_CARE_DAY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_NEONATAL_CARE_DAY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_NEONATAL_CARE_DAY_PERSON_SCOPE_RULES)
def _gold_qc_clinical_neonatal_care_day():
    """Quality-controlled twin of journey_clinical.neonatal_care_day."""
    df = _qc(
        "clinical_neonatal_care_day",
        CLINICAL_NEONATAL_CARE_DAY_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.neonatal_care_day"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_neonatal_care_day():
    """Contract-v2 public twin of clinical_neonatal_care_day; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_neonatal_care_day")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
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
        '`entity_id` AS `entity_id`',
        '`activity_date` AS `activity_date`',
        '`ward_location` AS `ward_location`',
        '`unit_function` AS `unit_function`',
        '`critical_care_start` AS `critical_care_start`',
        '`critical_care_discharge` AS `critical_care_discharge`',
        '`episode_link_status` AS `episode_link_status`',
        '`neonatal_episode_key` AS `neonatal_episode_key`',
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
    'clinical_neonatal_care_day': GoldRules(
        table='clinical_neonatal_care_day',
        select=CLINICAL_NEONATAL_CARE_DAY_SELECT,
        mandatory=CLINICAL_NEONATAL_CARE_DAY_MANDATORY_RULES,
        advisory=CLINICAL_NEONATAL_CARE_DAY_ADVISORY_RULES,
        person_scope=CLINICAL_NEONATAL_CARE_DAY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth')),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_neonatal_care_day': {'columns': {'activity_date': 'BadgerNet ActivityDate; second component '
                                                                 'of the primary key.',
                                                'confidentiality_code': 'Source confidentiality '
                                                                        'classification attached to the '
                                                                        'record for each neonatal care day '
                                                                        'record. It is produced by the '
                                                                        'silver transformation and has no '
                                                                        'direct bronze-column lineage entry. '
                                                                        'Source code meanings and sentinel '
                                                                        'values are retained unless the '
                                                                        'pipeline explicitly maps them; null '
                                                                        'means no code was supplied.',
                                                'critical_care_discharge': 'Date or time value for critical '
                                                                           'care discharge on the neonatal '
                                                                           'care day record. It is derived '
                                                                           'from bronze field '
                                                                           '`CriticalCareDischargeDate_CLEAN` '
                                                                           'in '
                                                                           '`4_prod.bronze.map_neonatal_critical_care`. '
                                                                           'Source precision and timezone '
                                                                           'handling follow the pipeline '
                                                                           'expression; null means the time '
                                                                           'was unavailable.',
                                                'critical_care_start': 'Date or time value for critical care '
                                                                       'start on the neonatal care day '
                                                                       'record. It is derived from bronze '
                                                                       'field `CriticalCareStartDate_CLEAN` '
                                                                       'in '
                                                                       '`4_prod.bronze.map_neonatal_critical_care`. '
                                                                       'Source precision and timezone '
                                                                       'handling follow the pipeline '
                                                                       'expression; null means the time was '
                                                                       'unavailable.',
                                                'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                                'available.',
                                                'entity_id': 'BadgerNet EntityID; first component of the '
                                                             'primary key.',
                                                'episode_link_status': 'Processing or clinical status of '
                                                                       'episode link for the neonatal care '
                                                                       'day record. It is carried from '
                                                                       'bronze field `EPISODE_LINK_STATUS` '
                                                                       'in '
                                                                       '`4_prod.bronze.map_neonatal_critical_care`. '
                                                                       'Whitespace and source sentinel text '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly normalizes them; null '
                                                                       'means no value was supplied.',
                                                'event_datetime': 'Date of the critical care activity '
                                                                  'record.',
                                                'event_datetime_local': 'Local-time representation of the '
                                                                        'neonatal critical-care activity day '
                                                                        'event start datetime. Gold: NULL '
                                                                        'wherever Gold nulls event_datetime.',
                                                'event_end_datetime': 'Date and time when the represented '
                                                                      'clinical or administrative event '
                                                                      'ended for each neonatal care day '
                                                                      'record. It is produced by the silver '
                                                                      'transformation and has no direct '
                                                                      'bronze-column lineage entry. Source '
                                                                      'precision and timezone handling '
                                                                      'follow the pipeline expression; null '
                                                                      'means the time was unavailable.',
                                                'event_end_datetime_local': 'Local-time representation of '
                                                                            'the neonatal critical-care '
                                                                            'activity day event end '
                                                                            'datetime.',
                                                'fact_category': 'Contract category used to group the record '
                                                                 'with comparable clinical facts for each '
                                                                 'neonatal care day record. It is produced '
                                                                 'by the silver transformation and has no '
                                                                 'direct bronze-column lineage entry. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                                'loaded_at': 'map_neonatal_critical_care.ADC_UPDT carried '
                                                             'unchanged through the care-day stage and '
                                                             'public projection. This is bronze ingestion '
                                                             'provenance, not ActivityDate, critical-care '
                                                             'start/discharge or Silver refresh time; a '
                                                             'missing source timestamp remains null.',
                                                'neonatal_episode_key': 'Deterministic SHA-256 key for the '
                                                                        'parent neonatal episode.',
                                                'patient_event_key': 'Deterministic event key derived from '
                                                                     'source identifiers; retained as a '
                                                                     'stable join key.',
                                                'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                             'resolved.',
                                                'record_status': 'Derived BadgerNet care-day row status: '
                                                                 'retracted when '
                                                                 'map_neonatal_critical_care.SOURCE_PRESENT_IND '
                                                                 'is false, otherwise active; a missing flag '
                                                                 'defaults true. The projection does not '
                                                                 'emit superseded and this status does not '
                                                                 "describe the baby's clinical condition or "
                                                                 'care level.',
                                                'record_status_effective_from': 'map_neonatal_critical_care.ActivityDate '
                                                                                'carried unchanged as the '
                                                                                'history start. This is the '
                                                                                'care-day date/time used as '
                                                                                'a proxy, not an '
                                                                                'independently observed '
                                                                                'status-change timestamp; no '
                                                                                'clinical-care-start or '
                                                                                'ingestion-time fallback is '
                                                                                'applied.',
                                                'record_status_effective_to': 'map_neonatal_critical_care.ADC_UPDT '
                                                                              'only when the care-day row is '
                                                                              'retracted; otherwise null. '
                                                                              'This is an ingestion-time end '
                                                                              'proxy, not ActivityDate or '
                                                                              'the critical-care discharge '
                                                                              'timestamp; a missing ADC_UPDT '
                                                                              'remains null.',
                                                'source_code': 'Neonatal Critical Care Activity Code 1 - '
                                                               'coded activity performed on this day.',
                                                'source_coding_system': 'Coding system or source namespace '
                                                                        'in which the source code is defined '
                                                                        'for each neonatal care day record. '
                                                                        'It is produced by the silver '
                                                                        'transformation and has no direct '
                                                                        'bronze-column lineage entry. Source '
                                                                        'code meanings and sentinel values '
                                                                        'are retained unless the pipeline '
                                                                        'explicitly maps them; null means no '
                                                                        'code was supplied.',
                                                'source_display': 'Neonatal Critical Care Activity Code 1 - '
                                                                  'coded activity performed on this day.',
                                                'source_feed': 'Originating data feed responsible for the '
                                                               'record for each neonatal care day record. It '
                                                               'is produced by the silver transformation and '
                                                               'has no direct bronze-column lineage entry. '
                                                               'Whitespace and source sentinel text are '
                                                               'retained unless the pipeline explicitly '
                                                               'normalizes them; null means no value was '
                                                               'supplied.',
                                                'source_update_timestamp': 'map_neonatal_critical_care.ADC_UPDT '
                                                                           'carried unchanged. It is the '
                                                                           'same contributing bronze load '
                                                                           'clock published as loaded_at, '
                                                                           'not an independent native '
                                                                           'clinical-system modification '
                                                                           'timestamp or Silver refresh '
                                                                           'time.',
                                                'subject_id_system': 'Identifier namespace used to interpret '
                                                                     'the subject identifier on this record '
                                                                     'for each neonatal care day record. It '
                                                                     'is derived from bronze field '
                                                                     '`BABY_PERSON_ID` in '
                                                                     '`4_prod.bronze.map_neonatal_critical_care`. '
                                                                     'Its uniqueness is limited to the '
                                                                     'originating source namespace; null '
                                                                     'means no identifier was supplied.',
                                                'subject_key': 'Deterministic SHA-256 over the strongest '
                                                               'source identifier. Not salted, not secret; a '
                                                               'stable join key across feeds where person_id '
                                                               'is unresolved.',
                                                'unit_function': 'Functional type of the critical care unit '
                                                                 '(e.g. 13 = NICU, 14 = LNU).',
                                                'vip_ind': 'Indicator that the source record carries VIP '
                                                           'handling status for each neonatal care day '
                                                           'record. It is produced by the silver '
                                                           'transformation and has no direct bronze-column '
                                                           'lineage entry. Null means the source did not '
                                                           'state the indicator and must not be interpreted '
                                                           'as false.',
                                                'ward_location': 'Ward location code (e.g. nnu = neonatal '
                                                                 'unit, tc = transitional care).',
                                                'withheld_identity_ind': 'Indicator that the source identity '
                                                                         'was deliberately withheld for each '
                                                                         'neonatal care day record. It is '
                                                                         'produced by the silver '
                                                                         'transformation and has no direct '
                                                                         'bronze-column lineage entry. Null '
                                                                         'means the source did not state the '
                                                                         'indicator and must not be '
                                                                         'interpreted as false.'},
                                    'quantity_override': True,
                                    'table': 'Internal quality-controlled twin of '
                                             'clinical_neonatal_care_day: the Gold repairs, nulls and '
                                             'expectations are applied here and the published Gold MV reads '
                                             'this object. Materialized rather than temporary so the '
                                             'published MV can refresh incrementally.'},
 'clinical_neonatal_care_day': {'columns': {'activity_date': 'BadgerNet ActivityDate; second component of '
                                                             'the primary key.',
                                            'confidentiality_code': 'Source confidentiality classification '
                                                                    'attached to the record for each '
                                                                    'neonatal care day record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Source code meanings and '
                                                                    'sentinel values are retained unless the '
                                                                    'pipeline explicitly maps them; null '
                                                                    'means no code was supplied.',
                                            'critical_care_discharge': 'Date or time value for critical care '
                                                                       'discharge on the neonatal care day '
                                                                       'record. It is derived from bronze '
                                                                       'field '
                                                                       '`CriticalCareDischargeDate_CLEAN` in '
                                                                       '`4_prod.bronze.map_neonatal_critical_care`. '
                                                                       'Source precision and timezone '
                                                                       'handling follow the pipeline '
                                                                       'expression; null means the time was '
                                                                       'unavailable.',
                                            'critical_care_start': 'Date or time value for critical care '
                                                                   'start on the neonatal care day record. '
                                                                   'It is derived from bronze field '
                                                                   '`CriticalCareStartDate_CLEAN` in '
                                                                   '`4_prod.bronze.map_neonatal_critical_care`. '
                                                                   'Source precision and timezone handling '
                                                                   'follow the pipeline expression; null '
                                                                   'means the time was unavailable.',
                                            'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                            'available.',
                                            'entity_id': 'BadgerNet EntityID; first component of the primary '
                                                         'key.',
                                            'episode_link_status': 'Processing or clinical status of episode '
                                                                   'link for the neonatal care day record. '
                                                                   'It is carried from bronze field '
                                                                   '`EPISODE_LINK_STATUS` in '
                                                                   '`4_prod.bronze.map_neonatal_critical_care`. '
                                                                   'Whitespace and source sentinel text are '
                                                                   'retained unless the pipeline explicitly '
                                                                   'normalizes them; null means no value was '
                                                                   'supplied.',
                                            'event_datetime': 'Date of the critical care activity record.',
                                            'event_datetime_local': 'Local-time representation of the '
                                                                    'neonatal critical-care activity day '
                                                                    'event start datetime. Gold: NULL '
                                                                    'wherever Gold nulls event_datetime.',
                                            'event_end_datetime': 'Date and time when the represented '
                                                                  'clinical or administrative event ended '
                                                                  'for each neonatal care day record. It is '
                                                                  'produced by the silver transformation and '
                                                                  'has no direct bronze-column lineage '
                                                                  'entry. Source precision and timezone '
                                                                  'handling follow the pipeline expression; '
                                                                  'null means the time was unavailable.',
                                            'event_end_datetime_local': 'Local-time representation of the '
                                                                        'neonatal critical-care activity day '
                                                                        'event end datetime.',
                                            'fact_category': 'Contract category used to group the record '
                                                             'with comparable clinical facts for each '
                                                             'neonatal care day record. It is produced by '
                                                             'the silver transformation and has no direct '
                                                             'bronze-column lineage entry. Whitespace and '
                                                             'source sentinel text are retained unless the '
                                                             'pipeline explicitly normalizes them; null '
                                                             'means no value was supplied.',
                                            'loaded_at': 'map_neonatal_critical_care.ADC_UPDT carried '
                                                         'unchanged through the care-day stage and public '
                                                         'projection. This is bronze ingestion provenance, '
                                                         'not ActivityDate, critical-care start/discharge or '
                                                         'Silver refresh time; a missing source timestamp '
                                                         'remains null.',
                                            'neonatal_episode_key': 'Deterministic SHA-256 key for the '
                                                                    'parent neonatal episode.',
                                            'patient_event_key': 'Deterministic event key derived from '
                                                                 'source identifiers; retained as a stable '
                                                                 'join key.',
                                            'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                         'resolved.',
                                            'record_status': 'Derived BadgerNet care-day row status: '
                                                             'retracted when '
                                                             'map_neonatal_critical_care.SOURCE_PRESENT_IND '
                                                             'is false, otherwise active; a missing flag '
                                                             'defaults true. The projection does not emit '
                                                             'superseded and this status does not describe '
                                                             "the baby's clinical condition or care level.",
                                            'record_status_effective_from': 'map_neonatal_critical_care.ActivityDate '
                                                                            'carried unchanged as the '
                                                                            'history start. This is the '
                                                                            'care-day date/time used as a '
                                                                            'proxy, not an independently '
                                                                            'observed status-change '
                                                                            'timestamp; no '
                                                                            'clinical-care-start or '
                                                                            'ingestion-time fallback is '
                                                                            'applied.',
                                            'record_status_effective_to': 'map_neonatal_critical_care.ADC_UPDT '
                                                                          'only when the care-day row is '
                                                                          'retracted; otherwise null. This '
                                                                          'is an ingestion-time end proxy, '
                                                                          'not ActivityDate or the '
                                                                          'critical-care discharge '
                                                                          'timestamp; a missing ADC_UPDT '
                                                                          'remains null.',
                                            'source_code': 'Neonatal Critical Care Activity Code 1 - coded '
                                                           'activity performed on this day.',
                                            'source_coding_system': 'Coding system or source namespace in '
                                                                    'which the source code is defined for '
                                                                    'each neonatal care day record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Source code meanings and '
                                                                    'sentinel values are retained unless the '
                                                                    'pipeline explicitly maps them; null '
                                                                    'means no code was supplied.',
                                            'source_display': 'Neonatal Critical Care Activity Code 1 - '
                                                              'coded activity performed on this day.',
                                            'source_feed': 'Originating data feed responsible for the record '
                                                           'for each neonatal care day record. It is '
                                                           'produced by the silver transformation and has no '
                                                           'direct bronze-column lineage entry. Whitespace '
                                                           'and source sentinel text are retained unless the '
                                                           'pipeline explicitly normalizes them; null means '
                                                           'no value was supplied.',
                                            'source_update_timestamp': 'map_neonatal_critical_care.ADC_UPDT '
                                                                       'carried unchanged. It is the same '
                                                                       'contributing bronze load clock '
                                                                       'published as loaded_at, not an '
                                                                       'independent native clinical-system '
                                                                       'modification timestamp or Silver '
                                                                       'refresh time.',
                                            'subject_id_system': 'Identifier namespace used to interpret the '
                                                                 'subject identifier on this record for each '
                                                                 'neonatal care day record. It is derived '
                                                                 'from bronze field `BABY_PERSON_ID` in '
                                                                 '`4_prod.bronze.map_neonatal_critical_care`. '
                                                                 'Its uniqueness is limited to the '
                                                                 'originating source namespace; null means '
                                                                 'no identifier was supplied.',
                                            'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                           'identifier. Not salted, not secret; a stable '
                                                           'join key across feeds where person_id is '
                                                           'unresolved.',
                                            'unit_function': 'Functional type of the critical care unit '
                                                             '(e.g. 13 = NICU, 14 = LNU).',
                                            'vip_ind': 'Indicator that the source record carries VIP '
                                                       'handling status for each neonatal care day record. '
                                                       'It is produced by the silver transformation and has '
                                                       'no direct bronze-column lineage entry. Null means '
                                                       'the source did not state the indicator and must not '
                                                       'be interpreted as false.',
                                            'ward_location': 'Ward location code (e.g. nnu = neonatal unit, '
                                                             'tc = transitional care).',
                                            'withheld_identity_ind': 'Indicator that the source identity was '
                                                                     'deliberately withheld for each '
                                                                     'neonatal care day record. It is '
                                                                     'produced by the silver transformation '
                                                                     'and has no direct bronze-column '
                                                                     'lineage entry. Null means the source '
                                                                     'did not state the indicator and must '
                                                                     'not be interpreted as false.'},
                                'quantity_override': False,
                                'table': 'One admitted BadgerNet neonatal critical-care day. Gold QC twin of '
                                         'the silver product: 2 columns are repaired or nulled, 1 rule(s) '
                                         'drop rows, 5 check(s) are advisory. Each rule states its reason in '
                                         'the pipeline notebook, and Lakeflow expectation metrics report '
                                         'what every rule matched on each update.'}}
