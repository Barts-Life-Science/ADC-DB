"""Gold specialist.neonatal_episode: 2 dataset(s).

- datasets: _qc_clinical_neonatal_episode, clinical_neonatal_episode
- moved verbatim from: 3_gold_specialist_care:2066, 3_gold_specialist_care:2108, 3_gold_specialist_care:2113, 3_gold_specialist_care:2121, 3_gold_specialist_care:2195, 3_gold_specialist_care:2199, 3_gold_specialist_care:2216

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.neonatal_episode ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_NEONATAL_EPISODE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
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
    '`entity_id` AS `entity_id`',
    '`badger_unique_id` AS `badger_unique_id`',
    '`mother_person_id` AS `mother_person_id`',
    '`care_location_id` AS `care_location_id`',
    '`care_location_name` AS `care_location_name`',
    '`birth_datetime` AS `birth_datetime`',
    '`birth_datetime_raw` AS `birth_datetime_raw`',
    '`admit_datetime` AS `admit_datetime`',
    '`discharge_datetime` AS `discharge_datetime`',
    '`gestation_weeks` AS `gestation_weeks`',
    '`gestation_days` AS `gestation_days`',
    '`birthweight` AS `birthweight`',
    '`sex` AS `sex`',
    '`final_nnu_outcome` AS `final_nnu_outcome`',
    '`unit_level` AS `unit_level`',
    '`person_link_status` AS `person_link_status`',
    '`record_timestamp` AS `record_timestamp`',
    '`last_update` AS `last_update`',
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


CLINICAL_NEONATAL_EPISODE_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`unit_level_derived` AS `unit_level_derived`',
]


# contract v2: QC/batch inputs come from internal _clinical_neonatal_episode_metadata; source history stays on the main research table.
CLINICAL_NEONATAL_EPISODE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 36,718 rows of 36,718 that
    # are current and attributable; identity_status = 'resolved' keeps the 36,161 rows of
    # 36,718 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_NEONATAL_EPISODE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.neonatal_episode.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 557 of 36,718 at the profile.
    "gold.clinical.neonatal_episode.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 36,718 at the profile.
    "gold.clinical.neonatal_episode.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 834 of 36,718 rows (2.27%) when profiled on 2026-08-24.
    "gold.clinical.neonatal_episode.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_NEONATAL_EPISODE_PERSON_SCOPE_RULES = {
    "gold.clinical.neonatal_episode.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_neonatal_episode"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_NEONATAL_EPISODE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_NEONATAL_EPISODE_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_NEONATAL_EPISODE_PERSON_SCOPE_RULES)
def _gold_qc_clinical_neonatal_episode():
    """Quality-controlled twin of journey_clinical.neonatal_episode."""
    df = _qc(
        "clinical_neonatal_episode",
        CLINICAL_NEONATAL_EPISODE_SELECT,
        date_flags=["event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.neonatal_episode"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_neonatal_episode():
    """Contract-v2 public twin of clinical_neonatal_episode; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_neonatal_episode")).selectExpr(
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
        '`badger_unique_id` AS `badger_unique_id`',
        '`mother_person_id` AS `mother_person_id`',
        '`care_location_id` AS `care_location_id`',
        '`care_location_name` AS `care_location_name`',
        '`birth_datetime` AS `birth_datetime`',
        '`birth_datetime_raw` AS `birth_datetime_raw`',
        '`admit_datetime` AS `admit_datetime`',
        '`discharge_datetime` AS `discharge_datetime`',
        '`gestation_weeks` AS `gestation_weeks`',
        '`gestation_days` AS `gestation_days`',
        '`birthweight` AS `birthweight`',
        '`sex` AS `sex`',
        '`final_nnu_outcome` AS `final_nnu_outcome`',
        '`unit_level` AS `unit_level`',
        '`person_link_status` AS `person_link_status`',
        '`record_timestamp` AS `record_timestamp`',
        '`last_update` AS `last_update`',
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
        '`unit_level_derived` AS `unit_level_derived`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'clinical_neonatal_episode': GoldRules(
        table='clinical_neonatal_episode',
        select=CLINICAL_NEONATAL_EPISODE_SELECT,
        mandatory=CLINICAL_NEONATAL_EPISODE_MANDATORY_RULES,
        advisory=CLINICAL_NEONATAL_EPISODE_ADVISORY_RULES,
        person_scope=CLINICAL_NEONATAL_EPISODE_PERSON_SCOPE_RULES,
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


COMMENTS = {'_qc_clinical_neonatal_episode': {'columns': {'admit_datetime': 'Date and time associated with admit for '
                                                                 'the neonatal episode record. It is derived '
                                                                 'from bronze field `AdmitTime_CLEAN` in '
                                                                 '`4_prod.bronze.map_neonatal_episode`. '
                                                                 'Source precision and timezone handling '
                                                                 'follow the pipeline expression; null means '
                                                                 'the time was unavailable.',
                                               'badger_unique_id': 'Unique identifier assigned by the Badger '
                                                                   'neonatal system (legacy).',
                                               'birth_datetime': 'Date and time associated with birth for '
                                                                 'the neonatal episode record. It is derived '
                                                                 'from bronze field `BirthTimeBaby_CLEAN` in '
                                                                 '`4_prod.bronze.map_neonatal_episode`. '
                                                                 'Source precision and timezone handling '
                                                                 'follow the pipeline expression; null means '
                                                                 'the time was unavailable.',
                                               'birth_datetime_raw': 'Date and time of birth of the baby.',
                                               'birthweight': 'Birth weight of the baby in grams.',
                                               'care_location_id': 'Identifier for the neonatal care '
                                                                   'location.',
                                               'care_location_name': 'Name of the neonatal care location.',
                                               'confidentiality_code': 'Source confidentiality '
                                                                       'classification attached to the '
                                                                       'record for each neonatal episode '
                                                                       'record. It is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'discharge_datetime': 'Date and time associated with '
                                                                     'discharge for the neonatal episode '
                                                                     'record. It is derived from bronze '
                                                                     'field `DischTime_CLEAN` in '
                                                                     '`4_prod.bronze.map_neonatal_episode`. '
                                                                     'Source precision and timezone handling '
                                                                     'follow the pipeline expression; null '
                                                                     'means the time was unavailable.',
                                               'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                               'available.',
                                               'entity_id': 'BadgerNet EntityID; primary key of this table.',
                                               'event_datetime': 'Date and time when the represented '
                                                                 'clinical or administrative event occurred '
                                                                 'for each neonatal episode record. It is '
                                                                 'derived from bronze field '
                                                                 '`AdmitTime_CLEAN` in '
                                                                 '`4_prod.bronze.map_neonatal_episode`, '
                                                                 'selected as the provenance anchor from 2 '
                                                                 'recorded bronze source fields. Source '
                                                                 'precision and timezone handling follow the '
                                                                 'pipeline expression; null means the time '
                                                                 'was unavailable.',
                                               'event_datetime_local': 'Neonatal episode start date and time '
                                                                       'expressed in local time.',
                                               'event_end_datetime': 'Date and time when the represented '
                                                                     'clinical or administrative event ended '
                                                                     'for each neonatal episode record. It '
                                                                     'is derived from bronze field '
                                                                     '`DischTime_CLEAN` in '
                                                                     '`4_prod.bronze.map_neonatal_episode`. '
                                                                     'Source precision and timezone handling '
                                                                     'follow the pipeline expression; null '
                                                                     'means the time was unavailable.',
                                               'event_end_datetime_local': 'Neonatal episode end date and '
                                                                           'time expressed in local time. '
                                                                           'Gold: NULL wherever Gold nulls '
                                                                           'event_end_datetime.',
                                               'fact_category': 'Contract category used to group the record '
                                                                'with comparable clinical facts for each '
                                                                'neonatal episode record. It is produced by '
                                                                'the silver transformation and has no direct '
                                                                'bronze-column lineage entry. Whitespace and '
                                                                'source sentinel text are retained unless '
                                                                'the pipeline explicitly normalizes them; '
                                                                'null means no value was supplied.',
                                               'final_nnu_outcome': 'Final outcome of the neonatal unit '
                                                                    'admission (e.g. Home, Transfer, Death).',
                                               'gestation_days': 'Remaining days of gestational age at birth '
                                                                 'beyond completed weeks.',
                                               'gestation_weeks': 'Gestational age at birth in completed '
                                                                  'weeks.',
                                               'last_update': 'Timestamp of the last update to this record.',
                                               'loaded_at': 'map_neonatal_episode.ADC_UPDT carried unchanged '
                                                            'for the contributing BadgerNet episode row. It '
                                                            'is bronze ingestion provenance, distinct from '
                                                            'LastUpdate published as source_update_timestamp '
                                                            'and from RecordTimestamp; it is not '
                                                            'admission/birth/discharge time or Silver '
                                                            'refresh time.',
                                               'mother_person_id': 'Source-system identifier for the mother '
                                                                   'person associated with each neonatal '
                                                                   'episode record. It is carried from '
                                                                   'bronze field `MOTHER_PERSON_ID` in '
                                                                   '`4_prod.bronze.map_neonatal_episode`. '
                                                                   'Its uniqueness is limited to the '
                                                                   'originating source namespace; null means '
                                                                   'no identifier was supplied.',
                                               'patient_event_key': 'Deterministic event key derived from '
                                                                    'source identifiers; retained as a '
                                                                    'stable join key.',
                                               'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                            'resolved.',
                                               'person_link_status': 'Processing or clinical status of '
                                                                     'person link for the neonatal episode '
                                                                     'record. It is carried from bronze '
                                                                     'field `PERSON_LINK_STATUS` in '
                                                                     '`4_prod.bronze.map_neonatal_episode`. '
                                                                     'Whitespace and source sentinel text '
                                                                     'are retained unless the pipeline '
                                                                     'explicitly normalizes them; null means '
                                                                     'no value was supplied.',
                                               'record_status': 'Retracted when '
                                                                'map_neonatal_episode.SOURCE_PRESENT_IND is '
                                                                'false, otherwise active; a missing presence '
                                                                'flag defaults true. No superseded status is '
                                                                "emitted, and the baby's outcome, discharge "
                                                                'date or identity-link status do not '
                                                                'determine this label.',
                                               'record_status_effective_from': 'First non-null '
                                                                               'map_neonatal_episode.AdmitTime_CLEAN '
                                                                               'then BirthTimeBaby_CLEAN, '
                                                                               'matching the event-time '
                                                                               'expression. This '
                                                                               'admission/birth-time proxy '
                                                                               'is not an independently '
                                                                               'observed row-status '
                                                                               'transition; no LastUpdate, '
                                                                               'RecordTimestamp or ingestion '
                                                                               'fallback is applied.',
                                               'record_status_effective_to': 'map_neonatal_episode.ADC_UPDT '
                                                                             'only when the episode row is '
                                                                             'retracted; otherwise null. '
                                                                             'This is an ingestion-time end '
                                                                             'proxy, not DischTime_CLEAN, '
                                                                             'LastUpdate or an independent '
                                                                             'status-transition timestamp.',
                                               'record_timestamp': 'Technical timestamp recording when this '
                                                                   'record was created or last written in '
                                                                   'the source extract.',
                                               'sex': 'Sex of the baby.',
                                               'source_code': 'Code supplied by the originating source '
                                                              'system for the represented concept for each '
                                                              'neonatal episode record. It is produced by '
                                                              'the silver transformation and has no direct '
                                                              'bronze-column lineage entry. Source code '
                                                              'meanings and sentinel values are retained '
                                                              'unless the pipeline explicitly maps them; '
                                                              'null means no code was supplied.',
                                               'source_coding_system': 'Coding system or source namespace in '
                                                                       'which the source code is defined for '
                                                                       'each neonatal episode record. It is '
                                                                       'produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'source_display': 'Human-readable label supplied by the '
                                                                 'source system for the source code for each '
                                                                 'neonatal episode record. It is produced by '
                                                                 'the silver transformation and has no '
                                                                 'direct bronze-column lineage entry. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                               'source_feed': 'Originating data feed responsible for the '
                                                              'record for each neonatal episode record. It '
                                                              'is produced by the silver transformation and '
                                                              'has no direct bronze-column lineage entry. '
                                                              'Whitespace and source sentinel text are '
                                                              'retained unless the pipeline explicitly '
                                                              'normalizes them; null means no value was '
                                                              'supplied.',
                                               'source_update_timestamp': 'Timestamp of the last update to '
                                                                          'this record.',
                                               'subject_id_system': 'Identifier namespace used to interpret '
                                                                    'the subject identifier on this record '
                                                                    'for each neonatal episode record. It is '
                                                                    'derived from bronze field '
                                                                    '`BABY_PERSON_ID` in '
                                                                    '`4_prod.bronze.map_neonatal_episode`. '
                                                                    'Its uniqueness is limited to the '
                                                                    'originating source namespace; null '
                                                                    'means no identifier was supplied.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'unit_level': 'Designation level of the neonatal unit.',
                                               'unit_level_derived': 'Neonatal unit level 1, 2 or 3 parsed '
                                                                     'from unit_level (first digit 1-3); '
                                                                     'NULL when absent. INSR_W2 W2.11.',
                                               'vip_ind': 'Indicator that the source record carries VIP '
                                                          'handling status for each neonatal episode record. '
                                                          'It is produced by the silver transformation and '
                                                          'has no direct bronze-column lineage entry. Null '
                                                          'means the source did not state the indicator and '
                                                          'must not be interpreted as false.',
                                               'withheld_identity_ind': 'Indicator that the source identity '
                                                                        'was deliberately withheld for each '
                                                                        'neonatal episode record. It is '
                                                                        'produced by the silver '
                                                                        'transformation and has no direct '
                                                                        'bronze-column lineage entry. Null '
                                                                        'means the source did not state the '
                                                                        'indicator and must not be '
                                                                        'interpreted as false.'},
                                   'quantity_override': True,
                                   'table': 'Internal quality-controlled twin of clinical_neonatal_episode: '
                                            'the Gold repairs, nulls and expectations are applied here and '
                                            'the published Gold MV reads this object. Materialized rather '
                                            'than temporary so the published MV can refresh incrementally.'},
 'clinical_neonatal_episode': {'columns': {'admit_datetime': 'Date and time associated with admit for the '
                                                             'neonatal episode record. It is derived from '
                                                             'bronze field `AdmitTime_CLEAN` in '
                                                             '`4_prod.bronze.map_neonatal_episode`. Source '
                                                             'precision and timezone handling follow the '
                                                             'pipeline expression; null means the time was '
                                                             'unavailable.',
                                           'badger_unique_id': 'Unique identifier assigned by the Badger '
                                                               'neonatal system (legacy).',
                                           'birth_datetime': 'Date and time associated with birth for the '
                                                             'neonatal episode record. It is derived from '
                                                             'bronze field `BirthTimeBaby_CLEAN` in '
                                                             '`4_prod.bronze.map_neonatal_episode`. Source '
                                                             'precision and timezone handling follow the '
                                                             'pipeline expression; null means the time was '
                                                             'unavailable.',
                                           'birth_datetime_raw': 'Date and time of birth of the baby.',
                                           'birthweight': 'Birth weight of the baby in grams.',
                                           'care_location_id': 'Identifier for the neonatal care location.',
                                           'care_location_name': 'Name of the neonatal care location.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each neonatal '
                                                                   'episode record. It is produced by the '
                                                                   'silver transformation and has no direct '
                                                                   'bronze-column lineage entry. Source code '
                                                                   'meanings and sentinel values are '
                                                                   'retained unless the pipeline explicitly '
                                                                   'maps them; null means no code was '
                                                                   'supplied.',
                                           'discharge_datetime': 'Date and time associated with discharge '
                                                                 'for the neonatal episode record. It is '
                                                                 'derived from bronze field '
                                                                 '`DischTime_CLEAN` in '
                                                                 '`4_prod.bronze.map_neonatal_episode`. '
                                                                 'Source precision and timezone handling '
                                                                 'follow the pipeline expression; null means '
                                                                 'the time was unavailable.',
                                           'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                           'available.',
                                           'entity_id': 'BadgerNet EntityID; primary key of this table.',
                                           'event_datetime': 'Date and time when the represented clinical or '
                                                             'administrative event occurred for each '
                                                             'neonatal episode record. It is derived from '
                                                             'bronze field `AdmitTime_CLEAN` in '
                                                             '`4_prod.bronze.map_neonatal_episode`, selected '
                                                             'as the provenance anchor from 2 recorded '
                                                             'bronze source fields. Source precision and '
                                                             'timezone handling follow the pipeline '
                                                             'expression; null means the time was '
                                                             'unavailable.',
                                           'event_datetime_local': 'Neonatal episode start date and time '
                                                                   'expressed in local time.',
                                           'event_end_datetime': 'Date and time when the represented '
                                                                 'clinical or administrative event ended for '
                                                                 'each neonatal episode record. It is '
                                                                 'derived from bronze field '
                                                                 '`DischTime_CLEAN` in '
                                                                 '`4_prod.bronze.map_neonatal_episode`. '
                                                                 'Source precision and timezone handling '
                                                                 'follow the pipeline expression; null means '
                                                                 'the time was unavailable.',
                                           'event_end_datetime_local': 'Neonatal episode end date and time '
                                                                       'expressed in local time. Gold: NULL '
                                                                       'wherever Gold nulls '
                                                                       'event_end_datetime.',
                                           'fact_category': 'Contract category used to group the record with '
                                                            'comparable clinical facts for each neonatal '
                                                            'episode record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Whitespace and source sentinel '
                                                            'text are retained unless the pipeline '
                                                            'explicitly normalizes them; null means no value '
                                                            'was supplied.',
                                           'final_nnu_outcome': 'Final outcome of the neonatal unit '
                                                                'admission (e.g. Home, Transfer, Death).',
                                           'gestation_days': 'Remaining days of gestational age at birth '
                                                             'beyond completed weeks.',
                                           'gestation_weeks': 'Gestational age at birth in completed weeks.',
                                           'last_update': 'Timestamp of the last update to this record.',
                                           'loaded_at': 'map_neonatal_episode.ADC_UPDT carried unchanged for '
                                                        'the contributing BadgerNet episode row. It is '
                                                        'bronze ingestion provenance, distinct from '
                                                        'LastUpdate published as source_update_timestamp and '
                                                        'from RecordTimestamp; it is not '
                                                        'admission/birth/discharge time or Silver refresh '
                                                        'time.',
                                           'mother_person_id': 'Source-system identifier for the mother '
                                                               'person associated with each neonatal episode '
                                                               'record. It is carried from bronze field '
                                                               '`MOTHER_PERSON_ID` in '
                                                               '`4_prod.bronze.map_neonatal_episode`. Its '
                                                               'uniqueness is limited to the originating '
                                                               'source namespace; null means no identifier '
                                                               'was supplied.',
                                           'patient_event_key': 'Deterministic event key derived from source '
                                                                'identifiers; retained as a stable join key.',
                                           'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                        'resolved.',
                                           'person_link_status': 'Processing or clinical status of person '
                                                                 'link for the neonatal episode record. It '
                                                                 'is carried from bronze field '
                                                                 '`PERSON_LINK_STATUS` in '
                                                                 '`4_prod.bronze.map_neonatal_episode`. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                           'record_status': 'Retracted when '
                                                            'map_neonatal_episode.SOURCE_PRESENT_IND is '
                                                            'false, otherwise active; a missing presence '
                                                            'flag defaults true. No superseded status is '
                                                            "emitted, and the baby's outcome, discharge date "
                                                            'or identity-link status do not determine this '
                                                            'label.',
                                           'record_status_effective_from': 'First non-null '
                                                                           'map_neonatal_episode.AdmitTime_CLEAN '
                                                                           'then BirthTimeBaby_CLEAN, '
                                                                           'matching the event-time '
                                                                           'expression. This '
                                                                           'admission/birth-time proxy is '
                                                                           'not an independently observed '
                                                                           'row-status transition; no '
                                                                           'LastUpdate, RecordTimestamp or '
                                                                           'ingestion fallback is applied.',
                                           'record_status_effective_to': 'map_neonatal_episode.ADC_UPDT only '
                                                                         'when the episode row is retracted; '
                                                                         'otherwise null. This is an '
                                                                         'ingestion-time end proxy, not '
                                                                         'DischTime_CLEAN, LastUpdate or an '
                                                                         'independent status-transition '
                                                                         'timestamp.',
                                           'record_timestamp': 'Technical timestamp recording when this '
                                                               'record was created or last written in the '
                                                               'source extract.',
                                           'sex': 'Sex of the baby.',
                                           'source_code': 'Code supplied by the originating source system '
                                                          'for the represented concept for each neonatal '
                                                          'episode record. It is produced by the silver '
                                                          'transformation and has no direct bronze-column '
                                                          'lineage entry. Source code meanings and sentinel '
                                                          'values are retained unless the pipeline '
                                                          'explicitly maps them; null means no code was '
                                                          'supplied.',
                                           'source_coding_system': 'Coding system or source namespace in '
                                                                   'which the source code is defined for '
                                                                   'each neonatal episode record. It is '
                                                                   'produced by the silver transformation '
                                                                   'and has no direct bronze-column lineage '
                                                                   'entry. Source code meanings and sentinel '
                                                                   'values are retained unless the pipeline '
                                                                   'explicitly maps them; null means no code '
                                                                   'was supplied.',
                                           'source_display': 'Human-readable label supplied by the source '
                                                             'system for the source code for each neonatal '
                                                             'episode record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Whitespace and source sentinel '
                                                             'text are retained unless the pipeline '
                                                             'explicitly normalizes them; null means no '
                                                             'value was supplied.',
                                           'source_feed': 'Originating data feed responsible for the record '
                                                          'for each neonatal episode record. It is produced '
                                                          'by the silver transformation and has no direct '
                                                          'bronze-column lineage entry. Whitespace and '
                                                          'source sentinel text are retained unless the '
                                                          'pipeline explicitly normalizes them; null means '
                                                          'no value was supplied.',
                                           'source_update_timestamp': 'Timestamp of the last update to this '
                                                                      'record.',
                                           'subject_id_system': 'Identifier namespace used to interpret the '
                                                                'subject identifier on this record for each '
                                                                'neonatal episode record. It is derived from '
                                                                'bronze field `BABY_PERSON_ID` in '
                                                                '`4_prod.bronze.map_neonatal_episode`. Its '
                                                                'uniqueness is limited to the originating '
                                                                'source namespace; null means no identifier '
                                                                'was supplied.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'unit_level': 'Designation level of the neonatal unit.',
                                           'unit_level_derived': 'Neonatal unit level 1, 2 or 3 parsed from '
                                                                 'unit_level (first digit 1-3); NULL when '
                                                                 'absent. INSR_W2 W2.11.',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each neonatal episode record. It is '
                                                      'produced by the silver transformation and has no '
                                                      'direct bronze-column lineage entry. Null means the '
                                                      'source did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Indicator that the source identity was '
                                                                    'deliberately withheld for each neonatal '
                                                                    'episode record. It is produced by the '
                                                                    'silver transformation and has no direct '
                                                                    'bronze-column lineage entry. Null means '
                                                                    'the source did not state the indicator '
                                                                    'and must not be interpreted as false.'},
                               'quantity_override': False,
                               'table': 'One curated BadgerNet neonatal episode for the baby subject. Gold '
                                        'QC twin of the silver product: 1 columns are repaired or nulled, 1 '
                                        'rule(s) drop rows, 3 check(s) are advisory. Each rule states its '
                                        'reason in the pipeline notebook, and Lakeflow expectation metrics '
                                        'report what every rule matched on each update.'}}
