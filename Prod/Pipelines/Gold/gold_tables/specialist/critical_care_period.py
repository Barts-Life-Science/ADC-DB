"""Gold specialist.critical_care_period: 2 dataset(s).

- datasets: _qc_clinical_critical_care_period, clinical_critical_care_period
- moved verbatim from: 3_gold_specialist_care:1196, 3_gold_specialist_care:1242, 3_gold_specialist_care:1250, 3_gold_specialist_care:1330, 3_gold_specialist_care:1334, 3_gold_specialist_care:1351

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.critical_care_period ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CRITICAL_CARE_PERIOD_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`crit_care_period_id` AS `crit_care_period_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`period_end_datetime` AS `period_end_datetime`',
    '`period_business_key` AS `period_business_key`',
    '`no_current_version_ind` AS `no_current_version_ind`',
    '`business_key_status` AS `business_key_status`',
    '`source_valid_ind` AS `source_valid_ind`',
    '`care_type` AS `care_type`',
    '`unit_function` AS `unit_function`',
    '`unit_id` AS `unit_id`',
    '`cds_source_system` AS `cds_source_system`',
    '`level2_days` AS `level2_days`',
    '`level3_days` AS `level3_days`',
    '`organ_systems_supported` AS `organ_systems_supported`',
    '`gestation_length` AS `gestation_length`',
    '`discharge_status` AS `discharge_status`',
    '`discharge_destination` AS `discharge_destination`',
    '`source_encounter_id` AS `source_encounter_id`',
    '`cc_encounter_key` AS `cc_encounter_key`',
    '`cc_encounter_id` AS `cc_encounter_id`',
    '`cds_apc_id` AS `cds_apc_id`',
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


# contract v2: QC/batch inputs come from internal _clinical_critical_care_period_metadata; source history stays on the main research table.
CLINICAL_CRITICAL_CARE_PERIOD_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 176,886 rows of 176,886 that
    # are current and attributable; identity_status = 'resolved' keeps the 169,705 rows of
    # 176,886 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_CRITICAL_CARE_PERIOD_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.critical_care_period.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 7,181 of 176,886 at the profile.
    "gold.clinical.critical_care_period.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 176,886 at the profile.
    "gold.clinical.critical_care_period.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 308 of 176,886 rows (0.174%) when profiled on 2026-08-24.
    "gold.clinical.critical_care_period.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 64 of 176,886 rows (0.0362%) when profiled on 2026-08-24.
    "gold.clinical.critical_care_period.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_CRITICAL_CARE_PERIOD_PERSON_SCOPE_RULES = {
    "gold.clinical.critical_care_period.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_critical_care_period"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_CRITICAL_CARE_PERIOD_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CRITICAL_CARE_PERIOD_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_CRITICAL_CARE_PERIOD_PERSON_SCOPE_RULES)
def _gold_qc_clinical_critical_care_period():
    """Quality-controlled twin of journey_clinical.critical_care_period."""
    df = _qc(
        "clinical_critical_care_period",
        CLINICAL_CRITICAL_CARE_PERIOD_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.critical_care_period"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_critical_care_period():
    """Contract-v2 public twin of clinical_critical_care_period; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_critical_care_period")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`crit_care_period_id` AS `crit_care_period_id`',
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
        '`period_end_datetime` AS `period_end_datetime`',
        '`period_business_key` AS `period_business_key`',
        '`no_current_version_ind` AS `no_current_version_ind`',
        '`business_key_status` AS `business_key_status`',
        '`source_valid_ind` AS `source_valid_ind`',
        '`care_type` AS `care_type`',
        '`unit_function` AS `unit_function`',
        '`unit_id` AS `unit_id`',
        '`cds_source_system` AS `cds_source_system`',
        '`level2_days` AS `level2_days`',
        '`level3_days` AS `level3_days`',
        '`organ_systems_supported` AS `organ_systems_supported`',
        '`gestation_length` AS `gestation_length`',
        '`discharge_status` AS `discharge_status`',
        '`discharge_destination` AS `discharge_destination`',
        '`source_encounter_id` AS `source_encounter_id`',
        '`cc_encounter_key` AS `cc_encounter_key`',
        '`cc_encounter_id` AS `cc_encounter_id`',
        '`cds_apc_id` AS `cds_apc_id`',
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
    'clinical_critical_care_period': GoldRules(
        table='clinical_critical_care_period',
        select=CLINICAL_CRITICAL_CARE_PERIOD_SELECT,
        mandatory=CLINICAL_CRITICAL_CARE_PERIOD_MANDATORY_RULES,
        advisory=CLINICAL_CRITICAL_CARE_PERIOD_ADVISORY_RULES,
        person_scope=CLINICAL_CRITICAL_CARE_PERIOD_PERSON_SCOPE_RULES,
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


COMMENTS = {'_qc_clinical_critical_care_period': {'columns': {'business_key_status': 'Processing or clinical status of '
                                                                          'business key for the critical '
                                                                          'care period record. It is carried '
                                                                          'from bronze field '
                                                                          '`BUSINESS_KEY_STATUS` in '
                                                                          '`4_prod.bronze.map_critical_care_period`. '
                                                                          'Whitespace and source sentinel '
                                                                          'text are retained unless the '
                                                                          'pipeline explicitly normalizes '
                                                                          'them; null means no value was '
                                                                          'supplied.',
                                                   'care_type': 'Textual description of the meaning of the '
                                                                'NHS Data Dictionary code value for the '
                                                                'element.',
                                                   'cc_encounter_id': 'Native Millennium CC_ENCNTR_ID as '
                                                                      'BIGINT; joins to '
                                                                      'spine_encounter.encounter_id.',
                                                   'cc_encounter_key': 'Deterministic SHA-256 key for the '
                                                                       'linked critical-care encounter.',
                                                   'cds_apc_id': 'Identifier linking the critical care '
                                                                 'period to the associated Commissioning '
                                                                 'Data Set admitted patient care record.',
                                                   'cds_source_system': 'Code identifying the source system '
                                                                        'or organisation that supplied the '
                                                                        'critical care period record.',
                                                   'confidentiality_code': 'Source confidentiality '
                                                                           'classification attached to the '
                                                                           'record for each critical care '
                                                                           'period record. It is produced by '
                                                                           'the silver transformation and '
                                                                           'has no direct bronze-column '
                                                                           'lineage entry. Source code '
                                                                           'meanings and sentinel values are '
                                                                           'retained unless the pipeline '
                                                                           'explicitly maps them; null means '
                                                                           'no code was supplied.',
                                                   'crit_care_period_id': 'Source Crit_Care_Period_Id of the '
                                                                          'selected CCMDS revision, exposed '
                                                                          'as BIGINT. Identifies the '
                                                                          'representative source row chosen '
                                                                          'for this business-period record; '
                                                                          'period_business_key and '
                                                                          'patient_event_key retain the '
                                                                          'existing group and event linkage.',
                                                   'discharge_destination': 'Textual description of the '
                                                                            'meaning of the NHS Data '
                                                                            'Dictionary code value for the '
                                                                            'element.',
                                                   'discharge_status': 'Textual description of the meaning '
                                                                       'of the NHS Data Dictionary code '
                                                                       'value for the element.',
                                                   'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT '
                                                                   'when available.',
                                                   'event_datetime': 'Date and time when the represented '
                                                                     'clinical or administrative event '
                                                                     'occurred for each critical care period '
                                                                     'record. It is derived from bronze '
                                                                     'field `CC_Period_Start_Dt_Tm_CLEAN` in '
                                                                     '`4_prod.bronze.map_critical_care_period`. '
                                                                     'Source precision and timezone handling '
                                                                     'follow the pipeline expression; null '
                                                                     'means the time was unavailable.',
                                                   'event_datetime_local': 'Local-time form of the '
                                                                           'critical-care period start '
                                                                           'date-time, corresponding to '
                                                                           'event_datetime.',
                                                   'event_end_datetime': 'Date and time when the represented '
                                                                         'clinical or administrative event '
                                                                         'ended for each critical care '
                                                                         'period record. It is derived from '
                                                                         'bronze field '
                                                                         '`CC_Period_Disch_Dt_Tm_CLEAN` in '
                                                                         '`4_prod.bronze.map_critical_care_period`. '
                                                                         'Source precision and timezone '
                                                                         'handling follow the pipeline '
                                                                         'expression; null means the time '
                                                                         'was unavailable.',
                                                   'event_end_datetime_local': 'Local-time form of the '
                                                                               'critical-care period end '
                                                                               'date-time, corresponding to '
                                                                               'event_end_datetime. Gold: '
                                                                               'NULL wherever Gold nulls '
                                                                               'event_end_datetime.',
                                                   'fact_category': 'Contract category used to group the '
                                                                    'record with comparable clinical facts '
                                                                    'for each critical care period record. '
                                                                    'It is produced by the silver '
                                                                    'transformation and has no direct '
                                                                    'bronze-column lineage entry. Whitespace '
                                                                    'and source sentinel text are retained '
                                                                    'unless the pipeline explicitly '
                                                                    'normalizes them; null means no value '
                                                                    'was supplied.',
                                                   'gestation_length': 'Recorded gestation length in '
                                                                       'completed weeks for the patient '
                                                                       'where relevant to the critical care '
                                                                       'period.',
                                                   'level2_days': 'Number of calendar days of level 2 '
                                                                  'critical care delivered during the '
                                                                  'critical care period.',
                                                   'level3_days': 'Number of days within the critical care '
                                                                  'period during which the patient received '
                                                                  'level 3 (intensive care) support.',
                                                   'loaded_at': 'ADC_UPDT of the selected '
                                                                'map_critical_care_period revision, not the '
                                                                'maximum load time across the '
                                                                'business-period group. Selection maximizes '
                                                                'the struct of CURRENT_IND cast to INT (null '
                                                                '0), Record_Updated_Dt cast to TIMESTAMP '
                                                                '(null 1900-01-01), then Crit_Care_Period_Id '
                                                                '(null -1), within PERIOD_BUSINESS_KEY. The '
                                                                "chosen row's clock is carried unchanged; it "
                                                                'is distinct from PIPELINE_UPDT_DT_TM and '
                                                                'Silver refresh time.',
                                                   'no_current_version_ind': 'Indicator of whether no '
                                                                             'current version applies to the '
                                                                             'critical care period record. '
                                                                             'It is derived from bronze '
                                                                             'field `CURRENT_IND` in '
                                                                             '`4_prod.bronze.map_critical_care_period`. '
                                                                             'Null means the source did not '
                                                                             'state the indicator and must '
                                                                             'not be interpreted as false.',
                                                   'organ_systems_supported': 'Maximum number of organ '
                                                                              'systems supported at any one '
                                                                              'time during the critical care '
                                                                              'period.',
                                                   'patient_event_key': 'Deterministic event key derived '
                                                                        'from source identifiers; retained '
                                                                        'as a stable join key.',
                                                   'period_business_key': 'Bronze-generated 64-bit xxhash64 '
                                                                          'of CC_Period_Local_Id and '
                                                                          'normalized MRN. Identifies the '
                                                                          'business-key group, not a native '
                                                                          'source revision; consult '
                                                                          'business_key_status for ambiguous '
                                                                          'or missing-MRN groups.',
                                                   'period_end_datetime': 'Date and time associated with '
                                                                          'period end for the critical care '
                                                                          'period record. It is derived from '
                                                                          'bronze field '
                                                                          '`CC_Period_Disch_Dt_Tm_CLEAN` in '
                                                                          '`4_prod.bronze.map_critical_care_period`. '
                                                                          'Source precision and timezone '
                                                                          'handling follow the pipeline '
                                                                          'expression; null means the time '
                                                                          'was unavailable.',
                                                   'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                                'resolved.',
                                                   'record_status': 'For the selected CCMDS period revision, '
                                                                    'retracted when SOURCE_PRESENT_IND is '
                                                                    'false, otherwise active; a missing '
                                                                    'presence flag defaults true. No '
                                                                    'superseded status is emitted. '
                                                                    'CURRENT_IND influences '
                                                                    'representative-row selection but does '
                                                                    'not itself determine this status; '
                                                                    'no_current_version_ind and '
                                                                    'source_valid_ind are separate.',
                                                   'record_status_effective_from': 'CC_Period_Start_Dt_Tm_CLEAN '
                                                                                   'of the selected '
                                                                                   'map_critical_care_period '
                                                                                   'revision, carried '
                                                                                   'unchanged. This '
                                                                                   'care-period start is a '
                                                                                   'history proxy, not an '
                                                                                   'independently observed '
                                                                                   'status change; no '
                                                                                   'Record_Updated_Dt or '
                                                                                   'ingestion-time fallback '
                                                                                   'is applied.',
                                                   'record_status_effective_to': 'ADC_UPDT of the selected '
                                                                                 'map_critical_care_period '
                                                                                 'revision only when it is '
                                                                                 'retracted; otherwise null. '
                                                                                 'This is an ingestion-time '
                                                                                 'end proxy, not '
                                                                                 'CC_Period_Disch_Dt_Tm_CLEAN '
                                                                                 'or an independent '
                                                                                 'status-transition '
                                                                                 'timestamp.',
                                                   'source_code': 'Code supplied by the originating source '
                                                                  'system for the represented concept for '
                                                                  'each critical care period record. It is '
                                                                  'produced by the silver transformation and '
                                                                  'has no direct bronze-column lineage '
                                                                  'entry. Source code meanings and sentinel '
                                                                  'values are retained unless the pipeline '
                                                                  'explicitly maps them; null means no code '
                                                                  'was supplied.',
                                                   'source_coding_system': 'Coding system or source '
                                                                           'namespace in which the source '
                                                                           'code is defined for each '
                                                                           'critical care period record. It '
                                                                           'is produced by the silver '
                                                                           'transformation and has no direct '
                                                                           'bronze-column lineage entry. '
                                                                           'Source code meanings and '
                                                                           'sentinel values are retained '
                                                                           'unless the pipeline explicitly '
                                                                           'maps them; null means no code '
                                                                           'was supplied.',
                                                   'source_display': 'Human-readable label supplied by the '
                                                                     'source system for the source code for '
                                                                     'each critical care period record. It '
                                                                     'is produced by the silver '
                                                                     'transformation and has no direct '
                                                                     'bronze-column lineage entry. '
                                                                     'Whitespace and source sentinel text '
                                                                     'are retained unless the pipeline '
                                                                     'explicitly normalizes them; null means '
                                                                     'no value was supplied.',
                                                   'source_encounter_id': 'Source-system identifier for the '
                                                                          'source encounter associated with '
                                                                          'each critical care period record. '
                                                                          'It is derived from bronze field '
                                                                          '`ENCNTR_ID` in '
                                                                          '`4_prod.bronze.map_critical_care_period`. '
                                                                          'Its uniqueness is limited to the '
                                                                          'originating source namespace; '
                                                                          'null means no identifier was '
                                                                          'supplied.',
                                                   'source_feed': 'Originating data feed responsible for the '
                                                                  'record for each critical care period '
                                                                  'record. It is produced by the silver '
                                                                  'transformation and has no direct '
                                                                  'bronze-column lineage entry. Whitespace '
                                                                  'and source sentinel text are retained '
                                                                  'unless the pipeline explicitly normalizes '
                                                                  'them; null means no value was supplied.',
                                                   'source_update_timestamp': 'Bronze pipeline processing '
                                                                              'timestamp carried from '
                                                                              '`PIPELINE_UPDT_DT_TM` in '
                                                                              '`4_prod.bronze.map_critical_care_period`. '
                                                                              'It records when bronze '
                                                                              'processed the source row, not '
                                                                              'a native clinical-system '
                                                                              'update timestamp, clinical '
                                                                              'event time, or the current '
                                                                              'Silver refresh time.',
                                                   'source_valid_ind': 'Flag indicating whether the critical '
                                                                       'care period record passed source '
                                                                       'validation checks.',
                                                   'subject_id_system': 'Identifier namespace used to '
                                                                        'interpret the subject identifier on '
                                                                        'this record for each critical care '
                                                                        'period record. It is derived from '
                                                                        'bronze field `PERSON_ID` in '
                                                                        '`4_prod.bronze.map_critical_care_period`. '
                                                                        'Its uniqueness is limited to the '
                                                                        'originating source namespace; null '
                                                                        'means no identifier was supplied.',
                                                   'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                  'source identifier. Not salted, not '
                                                                  'secret; a stable join key across feeds '
                                                                  'where person_id is unresolved.',
                                                   'unit_function': 'Textual description of the meaning of '
                                                                    'the NHS Data Dictionary code value for '
                                                                    'the element.',
                                                   'unit_id': 'Identifier of the critical care unit to which '
                                                              'the period of care relates.',
                                                   'vip_ind': 'Indicator that the source record carries VIP '
                                                              'handling status for each critical care period '
                                                              'record. It is produced by the silver '
                                                              'transformation and has no direct '
                                                              'bronze-column lineage entry. Null means the '
                                                              'source did not state the indicator and must '
                                                              'not be interpreted as false.',
                                                   'withheld_identity_ind': 'Indicator that the source '
                                                                            'identity was deliberately '
                                                                            'withheld for each critical care '
                                                                            'period record. It is produced '
                                                                            'by the silver transformation '
                                                                            'and has no direct bronze-column '
                                                                            'lineage entry. Null means the '
                                                                            'source did not state the '
                                                                            'indicator and must not be '
                                                                            'interpreted as false.'},
                                       'quantity_override': True,
                                       'table': 'Internal quality-controlled twin of '
                                                'clinical_critical_care_period: the Gold repairs, nulls and '
                                                'expectations are applied here and the published Gold MV '
                                                'reads this object. Materialized rather than temporary so '
                                                'the published MV can refresh incrementally.'},
 'clinical_critical_care_period': {'columns': {'business_key_status': 'Processing or clinical status of '
                                                                      'business key for the critical care '
                                                                      'period record. It is carried from '
                                                                      'bronze field `BUSINESS_KEY_STATUS` in '
                                                                      '`4_prod.bronze.map_critical_care_period`. '
                                                                      'Whitespace and source sentinel text '
                                                                      'are retained unless the pipeline '
                                                                      'explicitly normalizes them; null '
                                                                      'means no value was supplied.',
                                               'care_type': 'Textual description of the meaning of the NHS '
                                                            'Data Dictionary code value for the element.',
                                               'cc_encounter_id': 'Native Millennium CC_ENCNTR_ID as BIGINT; '
                                                                  'joins to spine_encounter.encounter_id. '
                                                                  'Critical-care encounter link; NULL on '
                                                                  'almost every period before 2018 (the link '
                                                                  'was not recorded) and on about 1 in 5 '
                                                                  'periods in 2023-2024.',
                                               'cc_encounter_key': 'Deterministic SHA-256 key for the linked '
                                                                   'critical-care encounter.',
                                               'cds_apc_id': 'Identifier linking the critical care period to '
                                                             'the associated Commissioning Data Set admitted '
                                                             'patient care record.',
                                               'cds_source_system': 'Code identifying the source system or '
                                                                    'organisation that supplied the critical '
                                                                    'care period record.',
                                               'confidentiality_code': 'Source confidentiality '
                                                                       'classification attached to the '
                                                                       'record for each critical care period '
                                                                       'record. It is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'crit_care_period_id': 'Source Crit_Care_Period_Id of the '
                                                                      'selected CCMDS revision, exposed as '
                                                                      'BIGINT. Identifies the representative '
                                                                      'source row chosen for this '
                                                                      'business-period record; '
                                                                      'period_business_key and '
                                                                      'patient_event_key retain the existing '
                                                                      'group and event linkage.',
                                               'discharge_destination': 'Textual description of the meaning '
                                                                        'of the NHS Data Dictionary code '
                                                                        'value for the element.',
                                               'discharge_status': 'Textual description of the meaning of '
                                                                   'the NHS Data Dictionary code value for '
                                                                   'the element.',
                                               'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                               'available.',
                                               'event_datetime': 'Date and time when the represented '
                                                                 'clinical or administrative event occurred '
                                                                 'for each critical care period record. It '
                                                                 'is derived from bronze field '
                                                                 '`CC_Period_Start_Dt_Tm_CLEAN` in '
                                                                 '`4_prod.bronze.map_critical_care_period`. '
                                                                 'Source precision and timezone handling '
                                                                 'follow the pipeline expression; null means '
                                                                 'the time was unavailable.',
                                               'event_datetime_local': 'Local-time form of the critical-care '
                                                                       'period start date-time, '
                                                                       'corresponding to event_datetime.',
                                               'event_end_datetime': 'Date and time when the represented '
                                                                     'clinical or administrative event ended '
                                                                     'for each critical care period record. '
                                                                     'It is derived from bronze field '
                                                                     '`CC_Period_Disch_Dt_Tm_CLEAN` in '
                                                                     '`4_prod.bronze.map_critical_care_period`. '
                                                                     'Source precision and timezone handling '
                                                                     'follow the pipeline expression; null '
                                                                     'means the time was unavailable.',
                                               'event_end_datetime_local': 'Local-time form of the '
                                                                           'critical-care period end '
                                                                           'date-time, corresponding to '
                                                                           'event_end_datetime. Gold: NULL '
                                                                           'wherever Gold nulls '
                                                                           'event_end_datetime.',
                                               'fact_category': 'Contract category used to group the record '
                                                                'with comparable clinical facts for each '
                                                                'critical care period record. It is produced '
                                                                'by the silver transformation and has no '
                                                                'direct bronze-column lineage entry. '
                                                                'Whitespace and source sentinel text are '
                                                                'retained unless the pipeline explicitly '
                                                                'normalizes them; null means no value was '
                                                                'supplied.',
                                               'gestation_length': 'Recorded gestation length in completed '
                                                                   'weeks for the patient where relevant to '
                                                                   'the critical care period.',
                                               'level2_days': 'Number of calendar days of level 2 critical '
                                                              'care delivered during the critical care '
                                                              'period.',
                                               'level3_days': 'Number of days within the critical care '
                                                              'period during which the patient received '
                                                              'level 3 (intensive care) support.',
                                               'loaded_at': 'ADC_UPDT of the selected '
                                                            'map_critical_care_period revision, not the '
                                                            'maximum load time across the business-period '
                                                            'group. Selection maximizes the struct of '
                                                            'CURRENT_IND cast to INT (null 0), '
                                                            'Record_Updated_Dt cast to TIMESTAMP (null '
                                                            '1900-01-01), then Crit_Care_Period_Id (null '
                                                            '-1), within PERIOD_BUSINESS_KEY. The chosen '
                                                            "row's clock is carried unchanged; it is "
                                                            'distinct from PIPELINE_UPDT_DT_TM and Silver '
                                                            'refresh time.',
                                               'no_current_version_ind': 'Indicator of whether no current '
                                                                         'version applies to the critical '
                                                                         'care period record. It is derived '
                                                                         'from bronze field `CURRENT_IND` in '
                                                                         '`4_prod.bronze.map_critical_care_period`. '
                                                                         'Null means the source did not '
                                                                         'state the indicator and must not '
                                                                         'be interpreted as false.',
                                               'organ_systems_supported': 'Maximum number of organ systems '
                                                                          'supported at any one time during '
                                                                          'the critical care period.',
                                               'patient_event_key': 'Deterministic event key derived from '
                                                                    'source identifiers; retained as a '
                                                                    'stable join key.',
                                               'period_business_key': 'Bronze-generated 64-bit xxhash64 of '
                                                                      'CC_Period_Local_Id and normalized '
                                                                      'MRN. Identifies the business-key '
                                                                      'group, not a native source revision; '
                                                                      'consult business_key_status for '
                                                                      'ambiguous or missing-MRN groups.',
                                               'period_end_datetime': 'Date and time associated with period '
                                                                      'end for the critical care period '
                                                                      'record. It is derived from bronze '
                                                                      'field `CC_Period_Disch_Dt_Tm_CLEAN` '
                                                                      'in '
                                                                      '`4_prod.bronze.map_critical_care_period`. '
                                                                      'Source precision and timezone '
                                                                      'handling follow the pipeline '
                                                                      'expression; null means the time was '
                                                                      'unavailable.',
                                               'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                            'resolved.',
                                               'record_status': 'For the selected CCMDS period revision, '
                                                                'retracted when SOURCE_PRESENT_IND is false, '
                                                                'otherwise active; a missing presence flag '
                                                                'defaults true. No superseded status is '
                                                                'emitted. CURRENT_IND influences '
                                                                'representative-row selection but does not '
                                                                'itself determine this status; '
                                                                'no_current_version_ind and source_valid_ind '
                                                                'are separate.',
                                               'record_status_effective_from': 'CC_Period_Start_Dt_Tm_CLEAN '
                                                                               'of the selected '
                                                                               'map_critical_care_period '
                                                                               'revision, carried unchanged. '
                                                                               'This care-period start is a '
                                                                               'history proxy, not an '
                                                                               'independently observed '
                                                                               'status change; no '
                                                                               'Record_Updated_Dt or '
                                                                               'ingestion-time fallback is '
                                                                               'applied.',
                                               'record_status_effective_to': 'ADC_UPDT of the selected '
                                                                             'map_critical_care_period '
                                                                             'revision only when it is '
                                                                             'retracted; otherwise null. '
                                                                             'This is an ingestion-time end '
                                                                             'proxy, not '
                                                                             'CC_Period_Disch_Dt_Tm_CLEAN or '
                                                                             'an independent '
                                                                             'status-transition timestamp.',
                                               'source_code': 'Code supplied by the originating source '
                                                              'system for the represented concept for each '
                                                              'critical care period record. It is produced '
                                                              'by the silver transformation and has no '
                                                              'direct bronze-column lineage entry. Source '
                                                              'code meanings and sentinel values are '
                                                              'retained unless the pipeline explicitly maps '
                                                              'them; null means no code was supplied.',
                                               'source_coding_system': 'Coding system or source namespace in '
                                                                       'which the source code is defined for '
                                                                       'each critical care period record. It '
                                                                       'is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'source_display': 'Human-readable label supplied by the '
                                                                 'source system for the source code for each '
                                                                 'critical care period record. It is '
                                                                 'produced by the silver transformation and '
                                                                 'has no direct bronze-column lineage entry. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                               'source_encounter_id': 'Source-system identifier for the '
                                                                      'source encounter associated with each '
                                                                      'critical care period record. It is '
                                                                      'derived from bronze field `ENCNTR_ID` '
                                                                      'in '
                                                                      '`4_prod.bronze.map_critical_care_period`. '
                                                                      'Its uniqueness is limited to the '
                                                                      'originating source namespace; null '
                                                                      'means no identifier was supplied.',
                                               'source_feed': 'Originating data feed responsible for the '
                                                              'record for each critical care period record. '
                                                              'It is produced by the silver transformation '
                                                              'and has no direct bronze-column lineage '
                                                              'entry. Whitespace and source sentinel text '
                                                              'are retained unless the pipeline explicitly '
                                                              'normalizes them; null means no value was '
                                                              'supplied.',
                                               'source_update_timestamp': 'Bronze pipeline processing '
                                                                          'timestamp carried from '
                                                                          '`PIPELINE_UPDT_DT_TM` in '
                                                                          '`4_prod.bronze.map_critical_care_period`. '
                                                                          'It records when bronze processed '
                                                                          'the source row, not a native '
                                                                          'clinical-system update timestamp, '
                                                                          'clinical event time, or the '
                                                                          'current Silver refresh time.',
                                               'source_valid_ind': 'Flag indicating whether the critical '
                                                                   'care period record passed source '
                                                                   'validation checks.',
                                               'subject_id_system': 'Identifier namespace used to interpret '
                                                                    'the subject identifier on this record '
                                                                    'for each critical care period record. '
                                                                    'It is derived from bronze field '
                                                                    '`PERSON_ID` in '
                                                                    '`4_prod.bronze.map_critical_care_period`. '
                                                                    'Its uniqueness is limited to the '
                                                                    'originating source namespace; null '
                                                                    'means no identifier was supplied.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'unit_function': 'Textual description of the meaning of the '
                                                                'NHS Data Dictionary code value for the '
                                                                'element.',
                                               'unit_id': 'Identifier of the critical care unit to which the '
                                                          'period of care relates.',
                                               'vip_ind': 'Indicator that the source record carries VIP '
                                                          'handling status for each critical care period '
                                                          'record. It is produced by the silver '
                                                          'transformation and has no direct bronze-column '
                                                          'lineage entry. Null means the source did not '
                                                          'state the indicator and must not be interpreted '
                                                          'as false.',
                                               'withheld_identity_ind': 'Indicator that the source identity '
                                                                        'was deliberately withheld for each '
                                                                        'critical care period record. It is '
                                                                        'produced by the silver '
                                                                        'transformation and has no direct '
                                                                        'bronze-column lineage entry. Null '
                                                                        'means the source did not state the '
                                                                        'indicator and must not be '
                                                                        'interpreted as false.'},
                                   'quantity_override': False,
                                   'table': 'One CCMDS critical-care period per business key with '
                                            'deterministic version selection. Gold QC twin of the silver '
                                            'product: 1 columns are repaired or nulled, 1 rule(s) drop rows, '
                                            '4 check(s) are advisory. Each rule states its reason in the '
                                            'pipeline notebook, and Lakeflow expectation metrics report what '
                                            'every rule matched on each update.'}}
