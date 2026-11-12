"""Gold specialist.baby_delivery: 2 dataset(s).

- datasets: _qc_clinical_baby_delivery, clinical_baby_delivery
- moved verbatim from: 3_gold_specialist_care:165, 3_gold_specialist_care:169, 3_gold_specialist_care:182, 3_gold_specialist_care:31, 3_gold_specialist_care:86, 3_gold_specialist_care:94

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.baby_delivery ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_BABY_DELIVERY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_row_hash` AS `source_row_hash`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    # INSR_W3 W3.05: passthrough: literal_0 retired; MSDS code 0 is a valid value (owner exception).
    "`source_code` AS `source_code`",
    '`source_display` AS `source_display`',
    '`baby_person_id` AS `baby_person_id`',
    '`birth_order` AS `birth_order`',
    '`pregnancy_outcome` AS `pregnancy_outcome`',
    # INSR_W3 W3.05: passthrough: literal_0 retired; MSDS code 0 is a valid value (owner exception).
    "`delivery_method_code` AS `delivery_method_code`",
    '`phenotypic_sex` AS `phenotypic_sex`',
    '`gestation_length_birth` AS `gestation_length_birth`',
    '`birthweight` AS `birthweight`',
    '`apgar_5` AS `apgar_5`',
    '`baby_death_datetime` AS `baby_death_datetime`',
    '`first_feed_datetime` AS `first_feed_datetime`',
    '`first_feed_code` AS `first_feed_code`',
    '`first_feed_breast_milk_status` AS `first_feed_breast_milk_status`',
    '`breast_milk_status_discharge` AS `breast_milk_status_discharge`',
    '`skin_to_skin_ind` AS `skin_to_skin_ind`',
    '`baby_discharge_datetime` AS `baby_discharge_datetime`',
    '`delivery_org_site` AS `delivery_org_site`',
    '`birth_setting` AS `birth_setting`',
    '`birth_place_type` AS `birth_place_type`',
    '`midwifery_place_type` AS `midwifery_place_type`',
    '`labour_delivery_id` AS `labour_delivery_id`',
    '`labour_delivery_key` AS `labour_delivery_key`',
    '`pregnancy_id` AS `pregnancy_id`',
    '`journey_pregnancy_key` AS `journey_pregnancy_key`',
    '`source_link_status` AS `source_link_status`',
    '`pregnancy_orphan_ind` AS `pregnancy_orphan_ind`',
    '`spine_person_mismatch_ind` AS `spine_person_mismatch_ind`',
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


# contract v2: QC/batch inputs come from internal _clinical_baby_delivery_metadata; source history stays on the main research table.
CLINICAL_BABY_DELIVERY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 99,299 rows of 99,299 that
    # are current and attributable; identity_status = 'resolved' keeps the 98,912 rows of
    # 99,299 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_BABY_DELIVERY_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.baby_delivery.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 387 of 99,299 at the profile.
    "gold.clinical.baby_delivery.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 99,299 at the profile.
    "gold.clinical.baby_delivery.record_status.default_view_active":
        "record_status = 'active'",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_BABY_DELIVERY_PERSON_SCOPE_RULES = {
    "gold.clinical.baby_delivery.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_baby_delivery"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_BABY_DELIVERY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_BABY_DELIVERY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_BABY_DELIVERY_PERSON_SCOPE_RULES)
def _gold_qc_clinical_baby_delivery():
    """Quality-controlled twin of journey_clinical.baby_delivery."""
    df = _qc("clinical_baby_delivery", CLINICAL_BABY_DELIVERY_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.baby_delivery"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_baby_delivery():
    """Contract-v2 public twin of clinical_baby_delivery; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_baby_delivery")).selectExpr(
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
        '`baby_person_id` AS `baby_person_id`',
        '`birth_order` AS `birth_order`',
        '`pregnancy_outcome` AS `pregnancy_outcome`',
        '`delivery_method_code` AS `delivery_method_code`',
        '`phenotypic_sex` AS `phenotypic_sex`',
        '`gestation_length_birth` AS `gestation_length_birth`',
        '`birthweight` AS `birthweight`',
        '`apgar_5` AS `apgar_5`',
        '`baby_death_datetime` AS `baby_death_datetime`',
        '`first_feed_datetime` AS `first_feed_datetime`',
        '`first_feed_code` AS `first_feed_code`',
        '`first_feed_breast_milk_status` AS `first_feed_breast_milk_status`',
        '`breast_milk_status_discharge` AS `breast_milk_status_discharge`',
        '`skin_to_skin_ind` AS `skin_to_skin_ind`',
        '`baby_discharge_datetime` AS `baby_discharge_datetime`',
        '`delivery_org_site` AS `delivery_org_site`',
        '`birth_setting` AS `birth_setting`',
        '`birth_place_type` AS `birth_place_type`',
        '`midwifery_place_type` AS `midwifery_place_type`',
        '`labour_delivery_id` AS `labour_delivery_id`',
        '`labour_delivery_key` AS `labour_delivery_key`',
        '`pregnancy_id` AS `pregnancy_id`',
        '`journey_pregnancy_key` AS `journey_pregnancy_key`',
        '`source_link_status` AS `source_link_status`',
        '`pregnancy_orphan_ind` AS `pregnancy_orphan_ind`',
        '`spine_person_mismatch_ind` AS `spine_person_mismatch_ind`',
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
    'clinical_baby_delivery': GoldRules(
        table='clinical_baby_delivery',
        select=CLINICAL_BABY_DELIVERY_SELECT,
        mandatory=CLINICAL_BABY_DELIVERY_MANDATORY_RULES,
        advisory=CLINICAL_BABY_DELIVERY_ADVISORY_RULES,
        person_scope=CLINICAL_BABY_DELIVERY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_baby_delivery': {'columns': {'apgar_5': 'MSDS401 Apgar at 5 minutes (empty at source); use '
                                                       'clinical_birth.apgar_5_minute for Millennium Apgar '
                                                       'scores.',
                                            'baby_death_datetime': 'Date and time associated with baby death '
                                                                   'for the baby delivery record. It is '
                                                                   'derived from bronze field '
                                                                   '`PERSONDEATHDATETIMEBABY_CLEAN` in '
                                                                   '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                   'Source precision and timezone handling '
                                                                   'follow the pipeline expression; null '
                                                                   'means the time was unavailable.',
                                            'baby_discharge_datetime': 'Date and time associated with baby '
                                                                       'discharge for the baby delivery '
                                                                       'record. It is derived from bronze '
                                                                       'field '
                                                                       '`DISCHARGEDATETIMEBABYHSP_CLEAN` in '
                                                                       '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                       'Source precision and timezone '
                                                                       'handling follow the pipeline '
                                                                       'expression; null means the time was '
                                                                       'unavailable.',
                                            'baby_person_id': 'Millennium identifier of the baby, carried as '
                                                              'STRING from bronze BABY_PERSON_ID. Bronze '
                                                              'requires one normalized baby NHS value '
                                                              'matching exactly one active alias-type-18 '
                                                              'person. Null when that unique linkage is '
                                                              'unavailable; distinct from maternal '
                                                              'person_id.',
                                            'birth_order': 'Value describing birth order for the baby '
                                                           'delivery record. It is derived from bronze field '
                                                           '`BIRTHORDERMATERNITYSUS` in '
                                                           '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                           'Whitespace and source sentinel text are retained '
                                                           'unless the pipeline explicitly normalizes them; '
                                                           'null means no value was supplied.',
                                            'birth_place_type': 'Value describing birth place type for the '
                                                                'baby delivery record. It is derived from '
                                                                'bronze field `PLACETYPEACTUALDELIVERY` in '
                                                                '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                'Whitespace and source sentinel text are '
                                                                'retained unless the pipeline explicitly '
                                                                'normalizes them; null means no value was '
                                                                'supplied.',
                                            'birth_setting': 'Value describing birth setting for the baby '
                                                             'delivery record. It is derived from bronze '
                                                             'field `SETTINGPLACEBIRTH` in '
                                                             '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                             'Whitespace and source sentinel text are '
                                                             'retained unless the pipeline explicitly '
                                                             'normalizes them; null means no value was '
                                                             'supplied.',
                                            'birthweight': 'MSDS401 birth weight (empty at source); use '
                                                           'clinical_birth.birth_weight_grams for Millennium '
                                                           'birth weights.',
                                            'breast_milk_status_discharge': 'Value describing breast milk '
                                                                            'status discharge for the baby '
                                                                            'delivery record. It is derived '
                                                                            'from bronze field '
                                                                            '`BABYBREASTMILKSTATUSDISCHARGE` '
                                                                            'in '
                                                                            '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                            'Whitespace and source sentinel '
                                                                            'text are retained unless the '
                                                                            'pipeline explicitly normalizes '
                                                                            'them; null means no value was '
                                                                            'supplied.',
                                            'confidentiality_code': 'Source confidentiality classification '
                                                                    'attached to the record for each baby '
                                                                    'delivery record. It is produced by the '
                                                                    'silver transformation and has no direct '
                                                                    'bronze-column lineage entry. Source '
                                                                    'code meanings and sentinel values are '
                                                                    'retained unless the pipeline explicitly '
                                                                    'maps them; null means no code was '
                                                                    'supplied.',
                                            'delivery_method_code': 'Source-system code representing '
                                                                    'delivery method for the baby delivery '
                                                                    'record. It is derived from bronze field '
                                                                    '`DELIVERYMETHODCODE` in '
                                                                    '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                    'Source code meanings and sentinel '
                                                                    'values are retained unless the pipeline '
                                                                    'explicitly maps them; null means no '
                                                                    'code was supplied.',
                                            'delivery_org_site': 'Value describing delivery org site for the '
                                                                 'baby delivery record. It is derived from '
                                                                 'bronze field `ORGSITEIDACTUALDELIVERY` in '
                                                                 '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                            'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                            'available.',
                                            'event_datetime': 'Date and time when the represented clinical '
                                                              'or administrative event occurred for each '
                                                              'baby delivery record. It is derived from '
                                                              'bronze field `PERSONBIRTHDATETIMEBABY_CLEAN` '
                                                              'in '
                                                              '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                              'Source precision and timezone handling follow '
                                                              'the pipeline expression; null means the time '
                                                              'was unavailable.',
                                            'event_datetime_local': 'Local-time baby birth date and time for '
                                                                    'the MSDS delivery event, derived from '
                                                                    'the cleaned MSDS401 baby birth '
                                                                    'datetime.',
                                            'event_end_datetime': 'Date and time when the represented '
                                                                  'clinical or administrative event ended '
                                                                  'for each baby delivery record. It is '
                                                                  'derived from bronze field '
                                                                  '`DISCHARGEDATETIMEBABYHSP_CLEAN` in '
                                                                  '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                  'Source precision and timezone handling '
                                                                  'follow the pipeline expression; null '
                                                                  'means the time was unavailable.',
                                            'event_end_datetime_local': 'Local-time baby hospital discharge '
                                                                        'date and time closing the MSDS '
                                                                        'delivery event, derived from the '
                                                                        'cleaned MSDS401 baby discharge '
                                                                        'datetime. Gold: NULL wherever Gold '
                                                                        'nulls event_end_datetime.',
                                            'fact_category': 'Contract category used to group the record '
                                                             'with comparable clinical facts for each baby '
                                                             'delivery record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Whitespace and source sentinel '
                                                             'text are retained unless the pipeline '
                                                             'explicitly normalizes them; null means no '
                                                             'value was supplied.',
                                            'first_feed_breast_milk_status': 'Processing or clinical status '
                                                                             'of first feed breast milk for '
                                                                             'the baby delivery record. It '
                                                                             'is derived from bronze field '
                                                                             '`BABYFIRSTFEEDBREASTMILKSTATUS` '
                                                                             'in '
                                                                             '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                             'Whitespace and source sentinel '
                                                                             'text are retained unless the '
                                                                             'pipeline explicitly normalizes '
                                                                             'them; null means no value was '
                                                                             'supplied.',
                                            'first_feed_code': 'Source-system code representing first feed '
                                                               'for the baby delivery record. It is derived '
                                                               'from bronze field `BABYFIRSTFEEDINDCODE` in '
                                                               '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                               'Source code meanings and sentinel values are '
                                                               'retained unless the pipeline explicitly maps '
                                                               'them; null means no code was supplied.',
                                            'first_feed_datetime': 'Date and time associated with first feed '
                                                                   'for the baby delivery record. It is '
                                                                   'derived from bronze field '
                                                                   '`BABYFIRSTFEEDDATETIME_CLEAN` in '
                                                                   '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                   'Source precision and timezone handling '
                                                                   'follow the pipeline expression; null '
                                                                   'means the time was unavailable.',
                                            'gestation_length_birth': 'Value describing gestation length '
                                                                      'birth for the baby delivery record. '
                                                                      'It is derived from bronze field '
                                                                      '`GESTATIONLENGTHBIRTH` in '
                                                                      '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                      'Whitespace and source sentinel text '
                                                                      'are retained unless the pipeline '
                                                                      'explicitly normalizes them; null '
                                                                      'means no value was supplied.',
                                            'journey_pregnancy_key': 'Deterministic SHA-256 key for the '
                                                                     'pregnancy journey.',
                                            'labour_delivery_id': 'MSDS LABOURDELIVERYID; native linkage to '
                                                                  'the labour-delivery record.',
                                            'labour_delivery_key': 'Deterministic SHA-256 key for the linked '
                                                                   'labour delivery.',
                                            'loaded_at': 'map_maternity_baby_delivery.ADC_UPDT carried '
                                                         'unchanged from the deduplicated delivery source '
                                                         'row. Identity fan-out is collapsed without '
                                                         'aggregating this clock; the joined pregnancy spine '
                                                         'does not contribute a load timestamp. This is not '
                                                         'the Silver refresh time.',
                                            'midwifery_place_type': 'Value describing midwifery place type '
                                                                    'for the baby delivery record. It is '
                                                                    'derived from bronze field '
                                                                    '`PLACETYPEACTUALMIDWIFERY` in '
                                                                    '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                    'Whitespace and source sentinel text are '
                                                                    'retained unless the pipeline explicitly '
                                                                    'normalizes them; null means no value '
                                                                    'was supplied.',
                                            'patient_event_key': 'Deterministic event key derived from '
                                                                 'source identifiers; retained as a stable '
                                                                 'join key.',
                                            'person_id': 'Maternal Millennium person identifier, exposed as '
                                                         'BIGINT. Silver prefers Person_ID from the linked '
                                                         'pregnancy spine, falling back to the MSDS adapter '
                                                         'PERSON_ID only when its source-row group has one '
                                                         'distinct person identifier. Null if neither yields '
                                                         "an identifier. This is not the baby's identifier; "
                                                         'baby_person_id is separate.',
                                            'phenotypic_sex': 'Value describing phenotypic sex for the baby '
                                                              'delivery record. It is derived from bronze '
                                                              'field `PERSONPHENSEX` in '
                                                              '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                              'Whitespace and source sentinel text are '
                                                              'retained unless the pipeline explicitly '
                                                              'normalizes them; null means no value was '
                                                              'supplied.',
                                            'pregnancy_id': 'Source-system identifier for the pregnancy '
                                                            'associated with each baby delivery record. It '
                                                            'is derived from bronze field '
                                                            '`PREGNANCY_ID_PARSED` in '
                                                            '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                            'Its uniqueness is limited to the originating '
                                                            'source namespace; null means no identifier was '
                                                            'supplied.',
                                            'pregnancy_orphan_ind': 'Unique pregnancy episode identifier.',
                                            'pregnancy_outcome': 'Value describing pregnancy outcome for the '
                                                                 'baby delivery record. It is derived from '
                                                                 'bronze field `PREGOUTCOME` in '
                                                                 '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                            'record_status': 'Literal active for every retained MSDS '
                                                             'baby-delivery row. This is not derived from '
                                                             'source presence, pregnancy outcome or infant '
                                                             'survival; it does not establish that a source '
                                                             'record has never been superseded or deleted.',
                                            'record_status_effective_from': 'map_maternity_baby_delivery.PERSONBIRTHDATETIMEBABY_CLEAN '
                                                                            'carried unchanged as the '
                                                                            'history start. This is the '
                                                                            "baby's recorded birth time, not "
                                                                            'an independently observed '
                                                                            'row-status change; a missing '
                                                                            'source value remains null.',
                                            'record_status_effective_to': 'Always NULL as a TIMESTAMP in the '
                                                                          'baby-delivery projection. The '
                                                                          'pipeline supplies no row-status '
                                                                          'end boundary and does not '
                                                                          'substitute discharge, death or '
                                                                          'ingestion timestamps.',
                                            'skin_to_skin_ind': 'Indicator of whether skin to skin applies '
                                                                'to the baby delivery record. It is derived '
                                                                'from bronze field '
                                                                '`SKINTOSKINCONTACT1HOURIND` in '
                                                                '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                'Null means the source did not state the '
                                                                'indicator and must not be interpreted as '
                                                                'false.',
                                            'source_code': 'Code supplied by the originating source system '
                                                           'for the represented concept for each baby '
                                                           'delivery record. It is derived from bronze field '
                                                           '`DELIVERYMETHODCODE` in '
                                                           '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                           'Source code meanings and sentinel values are '
                                                           'retained unless the pipeline explicitly maps '
                                                           'them; null means no code was supplied.',
                                            'source_coding_system': 'Coding system or source namespace in '
                                                                    'which the source code is defined for '
                                                                    'each baby delivery record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Source code meanings and '
                                                                    'sentinel values are retained unless the '
                                                                    'pipeline explicitly maps them; null '
                                                                    'means no code was supplied.',
                                            'source_display': 'Human-readable label supplied by the source '
                                                              'system for the source code for each baby '
                                                              'delivery record. It is derived from bronze '
                                                              'field `DELIVERYMETHODCODE` in '
                                                              '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                              'Whitespace and source sentinel text are '
                                                              'retained unless the pipeline explicitly '
                                                              'normalizes them; null means no value was '
                                                              'supplied.',
                                            'source_feed': 'Originating data feed responsible for the record '
                                                           'for each baby delivery record. It is produced by '
                                                           'the silver transformation and has no direct '
                                                           'bronze-column lineage entry. Whitespace and '
                                                           'source sentinel text are retained unless the '
                                                           'pipeline explicitly normalizes them; null means '
                                                           'no value was supplied.',
                                            'source_link_status': 'Processing or clinical status of source '
                                                                  'link for the baby delivery record. It is '
                                                                  'derived from bronze field `PERSON_ID` in '
                                                                  '`4_prod.bronze.map_maternity_baby_delivery`, '
                                                                  'selected as the provenance anchor from 2 '
                                                                  'recorded bronze source fields. Whitespace '
                                                                  'and source sentinel text are retained '
                                                                  'unless the pipeline explicitly normalizes '
                                                                  'them; null means no value was supplied.',
                                            'source_row_hash': 'MSDS ROW_HASH supplied by the source; '
                                                               'evidence for the exception where no durable '
                                                               'baby-delivery row id exists.',
                                            'source_update_timestamp': 'map_maternity_baby_delivery.RECORD_UPDATED_DT '
                                                                       'carried unchanged from the '
                                                                       'deduplicated delivery source row. '
                                                                       'This is not the pregnancy-spine '
                                                                       'update clock or Silver refresh time; '
                                                                       'a missing source timestamp remains '
                                                                       'null.',
                                            'spine_person_mismatch_ind': 'Unique MAT person identifier for '
                                                                         'the mother.',
                                            'subject_id_system': 'Unique MAT person identifier for the '
                                                                 'mother.',
                                            'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                           'identifier. Not salted, not secret; a stable '
                                                           'join key across feeds where person_id is '
                                                           'unresolved.',
                                            'vip_ind': 'Indicator that the source record carries VIP '
                                                       'handling status for each baby delivery record. It is '
                                                       'produced by the silver transformation and has no '
                                                       'direct bronze-column lineage entry. Null means the '
                                                       'source did not state the indicator and must not be '
                                                       'interpreted as false.',
                                            'withheld_identity_ind': 'Indicator that the source identity was '
                                                                     'deliberately withheld for each baby '
                                                                     'delivery record. It is produced by the '
                                                                     'silver transformation and has no '
                                                                     'direct bronze-column lineage entry. '
                                                                     'Null means the source did not state '
                                                                     'the indicator and must not be '
                                                                     'interpreted as false.'},
                                'quantity_override': True,
                                'table': 'Internal quality-controlled twin of clinical_baby_delivery: the '
                                         'Gold repairs, nulls and expectations are applied here and the '
                                         'published Gold MV reads this object. Materialized rather than '
                                         'temporary so the published MV can refresh incrementally.'},
 'clinical_baby_delivery': {'columns': {'apgar_5': 'MSDS401 Apgar at 5 minutes (empty at source); use '
                                                   'clinical_birth.apgar_5_minute for Millennium Apgar '
                                                   'scores.',
                                        'baby_death_datetime': 'Date and time associated with baby death for '
                                                               'the baby delivery record. It is derived from '
                                                               'bronze field `PERSONDEATHDATETIMEBABY_CLEAN` '
                                                               'in '
                                                               '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                               'Source precision and timezone handling '
                                                               'follow the pipeline expression; null means '
                                                               'the time was unavailable.',
                                        'baby_discharge_datetime': 'Date and time associated with baby '
                                                                   'discharge for the baby delivery record. '
                                                                   'It is derived from bronze field '
                                                                   '`DISCHARGEDATETIMEBABYHSP_CLEAN` in '
                                                                   '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                   'Source precision and timezone handling '
                                                                   'follow the pipeline expression; null '
                                                                   'means the time was unavailable.',
                                        'baby_person_id': 'Millennium identifier of the baby, carried as '
                                                          'STRING from bronze BABY_PERSON_ID. Bronze '
                                                          'requires one normalized baby NHS value matching '
                                                          'exactly one active alias-type-18 person. Null '
                                                          'when that unique linkage is unavailable; distinct '
                                                          'from maternal person_id.',
                                        'birth_order': 'Value describing birth order for the baby delivery '
                                                       'record. It is derived from bronze field '
                                                       '`BIRTHORDERMATERNITYSUS` in '
                                                       '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                       'Whitespace and source sentinel text are retained '
                                                       'unless the pipeline explicitly normalizes them; null '
                                                       'means no value was supplied.',
                                        'birth_place_type': 'Value describing birth place type for the baby '
                                                            'delivery record. It is derived from bronze '
                                                            'field `PLACETYPEACTUALDELIVERY` in '
                                                            '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                            'Whitespace and source sentinel text are '
                                                            'retained unless the pipeline explicitly '
                                                            'normalizes them; null means no value was '
                                                            'supplied.',
                                        'birth_setting': 'Value describing birth setting for the baby '
                                                         'delivery record. It is derived from bronze field '
                                                         '`SETTINGPLACEBIRTH` in '
                                                         '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                         'Whitespace and source sentinel text are retained '
                                                         'unless the pipeline explicitly normalizes them; '
                                                         'null means no value was supplied.',
                                        'birthweight': 'MSDS401 birth weight (empty at source); use '
                                                       'clinical_birth.birth_weight_grams for Millennium '
                                                       'birth weights.',
                                        'breast_milk_status_discharge': 'Value describing breast milk status '
                                                                        'discharge for the baby delivery '
                                                                        'record. It is derived from bronze '
                                                                        'field '
                                                                        '`BABYBREASTMILKSTATUSDISCHARGE` in '
                                                                        '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                        'Whitespace and source sentinel text '
                                                                        'are retained unless the pipeline '
                                                                        'explicitly normalizes them; null '
                                                                        'means no value was supplied.',
                                        'confidentiality_code': 'Source confidentiality classification '
                                                                'attached to the record for each baby '
                                                                'delivery record. It is produced by the '
                                                                'silver transformation and has no direct '
                                                                'bronze-column lineage entry. Source code '
                                                                'meanings and sentinel values are retained '
                                                                'unless the pipeline explicitly maps them; '
                                                                'null means no code was supplied.',
                                        'delivery_method_code': 'Source-system code representing delivery '
                                                                'method for the baby delivery record. It is '
                                                                'derived from bronze field '
                                                                '`DELIVERYMETHODCODE` in '
                                                                '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                'Source code meanings and sentinel values '
                                                                'are retained unless the pipeline explicitly '
                                                                'maps them; null means no code was supplied.',
                                        'delivery_org_site': 'Value describing delivery org site for the '
                                                             'baby delivery record. It is derived from '
                                                             'bronze field `ORGSITEIDACTUALDELIVERY` in '
                                                             '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                             'Whitespace and source sentinel text are '
                                                             'retained unless the pipeline explicitly '
                                                             'normalizes them; null means no value was '
                                                             'supplied.',
                                        'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                        'available.',
                                        'event_datetime': 'Date and time when the represented clinical or '
                                                          'administrative event occurred for each baby '
                                                          'delivery record. It is derived from bronze field '
                                                          '`PERSONBIRTHDATETIMEBABY_CLEAN` in '
                                                          '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                          'Source precision and timezone handling follow the '
                                                          'pipeline expression; null means the time was '
                                                          'unavailable.',
                                        'event_datetime_local': 'Local-time baby birth date and time for the '
                                                                'MSDS delivery event, derived from the '
                                                                'cleaned MSDS401 baby birth datetime.',
                                        'event_end_datetime': 'Date and time when the represented clinical '
                                                              'or administrative event ended for each baby '
                                                              'delivery record. It is derived from bronze '
                                                              'field `DISCHARGEDATETIMEBABYHSP_CLEAN` in '
                                                              '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                              'Source precision and timezone handling follow '
                                                              'the pipeline expression; null means the time '
                                                              'was unavailable.',
                                        'event_end_datetime_local': 'Local-time baby hospital discharge date '
                                                                    'and time closing the MSDS delivery '
                                                                    'event, derived from the cleaned MSDS401 '
                                                                    'baby discharge datetime. Gold: NULL '
                                                                    'wherever Gold nulls event_end_datetime.',
                                        'fact_category': 'Contract category used to group the record with '
                                                         'comparable clinical facts for each baby delivery '
                                                         'record. It is produced by the silver '
                                                         'transformation and has no direct bronze-column '
                                                         'lineage entry. Whitespace and source sentinel text '
                                                         'are retained unless the pipeline explicitly '
                                                         'normalizes them; null means no value was supplied.',
                                        'first_feed_breast_milk_status': 'Processing or clinical status of '
                                                                         'first feed breast milk for the '
                                                                         'baby delivery record. It is '
                                                                         'derived from bronze field '
                                                                         '`BABYFIRSTFEEDBREASTMILKSTATUS` in '
                                                                         '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                         'Whitespace and source sentinel '
                                                                         'text are retained unless the '
                                                                         'pipeline explicitly normalizes '
                                                                         'them; null means no value was '
                                                                         'supplied.',
                                        'first_feed_code': 'Source-system code representing first feed for '
                                                           'the baby delivery record. It is derived from '
                                                           'bronze field `BABYFIRSTFEEDINDCODE` in '
                                                           '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                           'Source code meanings and sentinel values are '
                                                           'retained unless the pipeline explicitly maps '
                                                           'them; null means no code was supplied.',
                                        'first_feed_datetime': 'Date and time associated with first feed for '
                                                               'the baby delivery record. It is derived from '
                                                               'bronze field `BABYFIRSTFEEDDATETIME_CLEAN` '
                                                               'in '
                                                               '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                               'Source precision and timezone handling '
                                                               'follow the pipeline expression; null means '
                                                               'the time was unavailable.',
                                        'gestation_length_birth': 'Value describing gestation length birth '
                                                                  'for the baby delivery record. It is '
                                                                  'derived from bronze field '
                                                                  '`GESTATIONLENGTHBIRTH` in '
                                                                  '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                  'Whitespace and source sentinel text are '
                                                                  'retained unless the pipeline explicitly '
                                                                  'normalizes them; null means no value was '
                                                                  'supplied.',
                                        'journey_pregnancy_key': 'Deterministic SHA-256 key for the '
                                                                 'pregnancy journey.',
                                        'labour_delivery_id': 'MSDS LABOURDELIVERYID; native linkage to the '
                                                              'labour-delivery record.',
                                        'labour_delivery_key': 'Deterministic SHA-256 key for the linked '
                                                               'labour delivery.',
                                        'loaded_at': 'map_maternity_baby_delivery.ADC_UPDT carried unchanged '
                                                     'from the deduplicated delivery source row. Identity '
                                                     'fan-out is collapsed without aggregating this clock; '
                                                     'the joined pregnancy spine does not contribute a load '
                                                     'timestamp. This is not the Silver refresh time.',
                                        'midwifery_place_type': 'Value describing midwifery place type for '
                                                                'the baby delivery record. It is derived '
                                                                'from bronze field '
                                                                '`PLACETYPEACTUALMIDWIFERY` in '
                                                                '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                                'Whitespace and source sentinel text are '
                                                                'retained unless the pipeline explicitly '
                                                                'normalizes them; null means no value was '
                                                                'supplied.',
                                        'patient_event_key': 'Deterministic event key derived from source '
                                                             'identifiers; retained as a stable join key.',
                                        'person_id': 'Maternal Millennium person identifier, exposed as '
                                                     'BIGINT. Silver prefers Person_ID from the linked '
                                                     'pregnancy spine, falling back to the MSDS adapter '
                                                     'PERSON_ID only when its source-row group has one '
                                                     'distinct person identifier. Null if neither yields an '
                                                     "identifier. This is not the baby's identifier; "
                                                     'baby_person_id is separate.',
                                        'phenotypic_sex': 'Value describing phenotypic sex for the baby '
                                                          'delivery record. It is derived from bronze field '
                                                          '`PERSONPHENSEX` in '
                                                          '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                          'Whitespace and source sentinel text are retained '
                                                          'unless the pipeline explicitly normalizes them; '
                                                          'null means no value was supplied.',
                                        'pregnancy_id': 'Source-system identifier for the pregnancy '
                                                        'associated with each baby delivery record. It is '
                                                        'derived from bronze field `PREGNANCY_ID_PARSED` in '
                                                        '`4_prod.bronze.map_maternity_baby_delivery`. Its '
                                                        'uniqueness is limited to the originating source '
                                                        'namespace; null means no identifier was supplied.',
                                        'pregnancy_orphan_ind': 'Unique pregnancy episode identifier.',
                                        'pregnancy_outcome': 'Value describing pregnancy outcome for the '
                                                             'baby delivery record. It is derived from '
                                                             'bronze field `PREGOUTCOME` in '
                                                             '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                             'Whitespace and source sentinel text are '
                                                             'retained unless the pipeline explicitly '
                                                             'normalizes them; null means no value was '
                                                             'supplied.',
                                        'record_status': 'Literal active for every retained MSDS '
                                                         'baby-delivery row. This is not derived from source '
                                                         'presence, pregnancy outcome or infant survival; it '
                                                         'does not establish that a source record has never '
                                                         'been superseded or deleted.',
                                        'record_status_effective_from': 'map_maternity_baby_delivery.PERSONBIRTHDATETIMEBABY_CLEAN '
                                                                        'carried unchanged as the history '
                                                                        "start. This is the baby's recorded "
                                                                        'birth time, not an independently '
                                                                        'observed row-status change; a '
                                                                        'missing source value remains null.',
                                        'record_status_effective_to': 'Always NULL as a TIMESTAMP in the '
                                                                      'baby-delivery projection. The '
                                                                      'pipeline supplies no row-status end '
                                                                      'boundary and does not substitute '
                                                                      'discharge, death or ingestion '
                                                                      'timestamps.',
                                        'skin_to_skin_ind': 'Indicator of whether skin to skin applies to '
                                                            'the baby delivery record. It is derived from '
                                                            'bronze field `SKINTOSKINCONTACT1HOURIND` in '
                                                            '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                            'Null means the source did not state the '
                                                            'indicator and must not be interpreted as false.',
                                        'source_code': 'Code supplied by the originating source system for '
                                                       'the represented concept for each baby delivery '
                                                       'record. It is derived from bronze field '
                                                       '`DELIVERYMETHODCODE` in '
                                                       '`4_prod.bronze.map_maternity_baby_delivery`. Source '
                                                       'code meanings and sentinel values are retained '
                                                       'unless the pipeline explicitly maps them; null means '
                                                       'no code was supplied.',
                                        'source_coding_system': 'Coding system or source namespace in which '
                                                                'the source code is defined for each baby '
                                                                'delivery record. It is produced by the '
                                                                'silver transformation and has no direct '
                                                                'bronze-column lineage entry. Source code '
                                                                'meanings and sentinel values are retained '
                                                                'unless the pipeline explicitly maps them; '
                                                                'null means no code was supplied.',
                                        'source_display': 'Human-readable label supplied by the source '
                                                          'system for the source code for each baby delivery '
                                                          'record. It is derived from bronze field '
                                                          '`DELIVERYMETHODCODE` in '
                                                          '`4_prod.bronze.map_maternity_baby_delivery`. '
                                                          'Whitespace and source sentinel text are retained '
                                                          'unless the pipeline explicitly normalizes them; '
                                                          'null means no value was supplied.',
                                        'source_feed': 'Originating data feed responsible for the record for '
                                                       'each baby delivery record. It is produced by the '
                                                       'silver transformation and has no direct '
                                                       'bronze-column lineage entry. Whitespace and source '
                                                       'sentinel text are retained unless the pipeline '
                                                       'explicitly normalizes them; null means no value was '
                                                       'supplied.',
                                        'source_link_status': 'Processing or clinical status of source link '
                                                              'for the baby delivery record. It is derived '
                                                              'from bronze field `PERSON_ID` in '
                                                              '`4_prod.bronze.map_maternity_baby_delivery`, '
                                                              'selected as the provenance anchor from 2 '
                                                              'recorded bronze source fields. Whitespace and '
                                                              'source sentinel text are retained unless the '
                                                              'pipeline explicitly normalizes them; null '
                                                              'means no value was supplied.',
                                        'source_row_hash': 'MSDS ROW_HASH supplied by the source; evidence '
                                                           'for the exception where no durable baby-delivery '
                                                           'row id exists.',
                                        'source_update_timestamp': 'map_maternity_baby_delivery.RECORD_UPDATED_DT '
                                                                   'carried unchanged from the deduplicated '
                                                                   'delivery source row. This is not the '
                                                                   'pregnancy-spine update clock or Silver '
                                                                   'refresh time; a missing source timestamp '
                                                                   'remains null.',
                                        'spine_person_mismatch_ind': 'Unique MAT person identifier for the '
                                                                     'mother.',
                                        'subject_id_system': 'Unique MAT person identifier for the mother.',
                                        'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                       'identifier. Not salted, not secret; a stable join '
                                                       'key across feeds where person_id is unresolved.',
                                        'vip_ind': 'Indicator that the source record carries VIP handling '
                                                   'status for each baby delivery record. It is produced by '
                                                   'the silver transformation and has no direct '
                                                   'bronze-column lineage entry. Null means the source did '
                                                   'not state the indicator and must not be interpreted as '
                                                   'false.',
                                        'withheld_identity_ind': 'Indicator that the source identity was '
                                                                 'deliberately withheld for each baby '
                                                                 'delivery record. It is produced by the '
                                                                 'silver transformation and has no direct '
                                                                 'bronze-column lineage entry. Null means '
                                                                 'the source did not state the indicator and '
                                                                 'must not be interpreted as false.'},
                            'quantity_override': False,
                            'table': 'One MSDS baby-delivery row with mother identity recovered through the '
                                     'pregnancy spine. Gold QC twin of the silver product: 3 columns are '
                                     'repaired or nulled, 1 rule(s) drop rows, 2 check(s) are advisory. Each '
                                     'rule states its reason in the pipeline notebook, and Lakeflow '
                                     'expectation metrics report what every rule matched on each update. '
                                     "Coverage: The baby's person id is empty on 23.5% of baby-delivery "
                                     "rows, where the baby's NHS number does not resolve to exactly one "
                                     'Millennium person; every filled id is a published person; '
                                     'complete-through month per release: '
                                     '6_mgmt.data_quality.release_finding (finding_code '
                                     'F2_complete_through).'}}
