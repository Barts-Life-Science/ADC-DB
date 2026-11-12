"""Gold specialist.labour_delivery: 2 dataset(s).

- datasets: _qc_clinical_labour_delivery, clinical_labour_delivery
- moved verbatim from: 3_gold_specialist_care:1416, 3_gold_specialist_care:1466, 3_gold_specialist_care:1474, 3_gold_specialist_care:1550, 3_gold_specialist_care:1555, 3_gold_specialist_care:1559, 3_gold_specialist_care:1581

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.labour_delivery ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_LABOUR_DELIVERY_SELECT = [
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
    '`labour_delivery_id` AS `labour_delivery_id`',
    '`labour_onset_method` AS `labour_onset_method`',
    '`labour_onset_presentation` AS `labour_onset_presentation`',
    '`caesarean_datetime` AS `caesarean_datetime`',
    '`decision_to_deliver_datetime` AS `decision_to_deliver_datetime`',
    '`rom_datetime` AS `rom_datetime`',
    '`rom_method` AS `rom_method`',
    '`rom_reason` AS `rom_reason`',
    '`second_stage_datetime` AS `second_stage_datetime`',
    '`third_stage_end_datetime` AS `third_stage_end_datetime`',
    '`episiotomy_reason` AS `episiotomy_reason`',
    '`placenta_delivery_method` AS `placenta_delivery_method`',
    '`mother_admission_method` AS `mother_admission_method`',
    '`mother_discharge_datetime` AS `mother_discharge_datetime`',
    '`mother_discharge_method` AS `mother_discharge_method`',
    '`mother_discharge_destination` AS `mother_discharge_destination`',
    '`intrapartum_org_site` AS `intrapartum_org_site`',
    '`intrapartum_setting` AS `intrapartum_setting`',
    '`postnatal_lead_provider` AS `postnatal_lead_provider`',
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


# contract v2: QC/batch inputs come from internal _clinical_labour_delivery_metadata; source history stays on the main research table.
CLINICAL_LABOUR_DELIVERY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 101,238 rows of 101,238 that
    # are current and attributable; identity_status = 'resolved' keeps the 100,859 rows of
    # 101,238 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_LABOUR_DELIVERY_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.labour_delivery.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 379 of 101,238 at the profile.
    "gold.clinical.labour_delivery.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 101,238 at the profile.
    "gold.clinical.labour_delivery.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1 of 101,238 rows (0.000988%) when profiled on 2026-08-24.
    "gold.clinical.labour_delivery.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_LABOUR_DELIVERY_PERSON_SCOPE_RULES = {
    "gold.clinical.labour_delivery.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_LABOUR_DELIVERY_ORPHAN_RULES = {
    "gold.clinical.labour_delivery.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_labour_delivery"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_LABOUR_DELIVERY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_LABOUR_DELIVERY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_LABOUR_DELIVERY_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_LABOUR_DELIVERY_ORPHAN_RULES)
def _gold_qc_clinical_labour_delivery():
    """Quality-controlled twin of journey_clinical.labour_delivery."""
    # 1 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_labour_delivery",
        CLINICAL_LABOUR_DELIVERY_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.labour_delivery"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_labour_delivery():
    """Contract-v2 public twin of clinical_labour_delivery; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_labour_delivery")).selectExpr(
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
        '`labour_delivery_id` AS `labour_delivery_id`',
        '`labour_onset_method` AS `labour_onset_method`',
        '`labour_onset_presentation` AS `labour_onset_presentation`',
        '`caesarean_datetime` AS `caesarean_datetime`',
        '`decision_to_deliver_datetime` AS `decision_to_deliver_datetime`',
        '`rom_datetime` AS `rom_datetime`',
        '`rom_method` AS `rom_method`',
        '`rom_reason` AS `rom_reason`',
        '`second_stage_datetime` AS `second_stage_datetime`',
        '`third_stage_end_datetime` AS `third_stage_end_datetime`',
        '`episiotomy_reason` AS `episiotomy_reason`',
        '`placenta_delivery_method` AS `placenta_delivery_method`',
        '`mother_admission_method` AS `mother_admission_method`',
        '`mother_discharge_datetime` AS `mother_discharge_datetime`',
        '`mother_discharge_method` AS `mother_discharge_method`',
        '`mother_discharge_destination` AS `mother_discharge_destination`',
        '`intrapartum_org_site` AS `intrapartum_org_site`',
        '`intrapartum_setting` AS `intrapartum_setting`',
        '`postnatal_lead_provider` AS `postnatal_lead_provider`',
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
    'clinical_labour_delivery': GoldRules(
        table='clinical_labour_delivery',
        select=CLINICAL_LABOUR_DELIVERY_SELECT,
        mandatory=CLINICAL_LABOUR_DELIVERY_MANDATORY_RULES,
        advisory=CLINICAL_LABOUR_DELIVERY_ADVISORY_RULES,
        person_scope=CLINICAL_LABOUR_DELIVERY_PERSON_SCOPE_RULES,
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


COMMENTS = {'_qc_clinical_labour_delivery': {'columns': {'caesarean_datetime': 'Date and time associated with caesarean '
                                                                    'for the labour delivery record. It is '
                                                                    'derived from bronze field '
                                                                    '`CAESAREANDATETIME_CLEAN` in '
                                                                    '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                                    'Source precision and timezone handling '
                                                                    'follow the pipeline expression; null '
                                                                    'means the time was unavailable.',
                                              'confidentiality_code': 'Source confidentiality classification '
                                                                      'attached to the record for each '
                                                                      'labour delivery record. It is '
                                                                      'produced by the silver transformation '
                                                                      'and has no direct bronze-column '
                                                                      'lineage entry. Source code meanings '
                                                                      'and sentinel values are retained '
                                                                      'unless the pipeline explicitly maps '
                                                                      'them; null means no code was '
                                                                      'supplied.',
                                              'decision_to_deliver_datetime': 'Date and time associated with '
                                                                              'decision to deliver for the '
                                                                              'labour delivery record. It is '
                                                                              'derived from bronze field '
                                                                              '`DECISIONTODELIVERDATETIME_CLEAN` '
                                                                              'in '
                                                                              '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                                              'Source precision and timezone '
                                                                              'handling follow the pipeline '
                                                                              'expression; null means the '
                                                                              'time was unavailable.',
                                              'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                              'available.',
                                              'episiotomy_reason': 'The coded reason an episiotomy was '
                                                                   'performed.',
                                              'event_datetime': 'Date and time when the represented clinical '
                                                                'or administrative event occurred for each '
                                                                'labour delivery record. It is derived from '
                                                                'bronze field `CAESAREANDATETIME_CLEAN` in '
                                                                '`4_prod.bronze.map_maternity_labour_delivery`, '
                                                                'selected as the provenance anchor from 3 '
                                                                'recorded bronze source fields. Source '
                                                                'precision and timezone handling follow the '
                                                                'pipeline expression; null means the time '
                                                                'was unavailable.',
                                              'event_datetime_local': 'Local-time start of the '
                                                                      'labour-delivery event, derived from '
                                                                      'the cleaned MSDS mother delivery '
                                                                      'start, labour onset or caesarean '
                                                                      'datetime.',
                                              'event_end_datetime': 'Date and time when the represented '
                                                                    'clinical or administrative event ended '
                                                                    'for each labour delivery record. It is '
                                                                    'derived from bronze field '
                                                                    '`DISCHARGEDATETIMEMOTHERHSP_CLEAN` in '
                                                                    '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                                    'Source precision and timezone handling '
                                                                    'follow the pipeline expression; null '
                                                                    'means the time was unavailable.',
                                              'event_end_datetime_local': 'Local-time end of the '
                                                                          'labour-delivery event, derived '
                                                                          'from the cleaned MSDS mother '
                                                                          'hospital discharge datetime. '
                                                                          'Gold: NULL wherever Gold nulls '
                                                                          'event_end_datetime.',
                                              'fact_category': 'Contract category used to group the record '
                                                               'with comparable clinical facts for each '
                                                               'labour delivery record. It is produced by '
                                                               'the silver transformation and has no direct '
                                                               'bronze-column lineage entry. Whitespace and '
                                                               'source sentinel text are retained unless the '
                                                               'pipeline explicitly normalizes them; null '
                                                               'means no value was supplied.',
                                              'intrapartum_org_site': 'The NHS organisation site identifier '
                                                                      'where the mother started intrapartum '
                                                                      'care.',
                                              'intrapartum_setting': "The mother's actual location (type) at "
                                                                     'the start of intrapartum care.',
                                              'journey_pregnancy_key': 'Deterministic SHA-256 key for the '
                                                                       'pregnancy journey.',
                                              'labour_delivery_id': 'MSDS LABOURDELIVERYID; primary key of '
                                                                    'this table.',
                                              'labour_onset_method': 'The method by which the process of '
                                                                     'labour began, or delivery by caesarean '
                                                                     'section occurred.',
                                              'labour_onset_presentation': 'The coded fetal presentation '
                                                                           'recorded at the onset of labour.',
                                              'loaded_at': 'map_maternity_labour_delivery.ADC_UPDT carried '
                                                           'unchanged from the deduplicated labour source '
                                                           'row. Identity fan-out is collapsed without '
                                                           'aggregating this clock; the joined pregnancy '
                                                           'spine does not contribute a load timestamp. This '
                                                           'is not the Silver refresh time.',
                                              'mother_admission_method': 'The coded method of admission for '
                                                                         "the mother's hospital provider "
                                                                         'spell in which delivery occurred.',
                                              'mother_discharge_datetime': 'Date and time associated with '
                                                                           'mother discharge for the labour '
                                                                           'delivery record. It is derived '
                                                                           'from bronze field '
                                                                           '`DISCHARGEDATETIMEMOTHERHSP_CLEAN` '
                                                                           'in '
                                                                           '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                                           'Source precision and timezone '
                                                                           'handling follow the pipeline '
                                                                           'expression; null means the time '
                                                                           'was unavailable.',
                                              'mother_discharge_destination': 'The coded destination of the '
                                                                              'mother on discharge from the '
                                                                              'post-delivery hospital '
                                                                              'provider spell.',
                                              'mother_discharge_method': 'The coded method of discharge for '
                                                                         'the mother from the post-delivery '
                                                                         'hospital provider spell.',
                                              'patient_event_key': 'Deterministic event key derived from '
                                                                   'source identifiers; retained as a stable '
                                                                   'join key.',
                                              'person_id': 'Maternal Millennium person identifier, exposed '
                                                           'as BIGINT. Silver prefers Person_ID from the '
                                                           'linked pregnancy spine, falling back to the MSDS '
                                                           'adapter PERSON_ID only when its source-row group '
                                                           'has one distinct person identifier. Null if '
                                                           'neither yields an identifier.',
                                              'placenta_delivery_method': 'The coded method by which the '
                                                                          'placenta was delivered.',
                                              'postnatal_lead_provider': 'NHS organisation identifier of '
                                                                         'post natal lead provider '
                                                                         'organisation.',
                                              'pregnancy_id': 'Source-system identifier for the pregnancy '
                                                              'associated with each labour delivery record. '
                                                              'It is derived from bronze field '
                                                              '`PREGNANCY_ID_PARSED` in '
                                                              '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                              'Its uniqueness is limited to the originating '
                                                              'source namespace; null means no identifier '
                                                              'was supplied.',
                                              'pregnancy_orphan_ind': 'Unique pregnancy episode identifier.',
                                              'record_status': 'Literal active for every retained MSDS '
                                                               'labour-delivery row. This is not derived '
                                                               'from source presence or clinical delivery '
                                                               'outcome and does not establish that a source '
                                                               'record has never been superseded or deleted.',
                                              'record_status_effective_from': 'First non-null '
                                                                              'map_maternity_labour_delivery '
                                                                              'timestamp in the order '
                                                                              'LABOURONSETDATETIME_CLEAN, '
                                                                              'CAESAREANDATETIME_CLEAN, '
                                                                              'STARTDATETIMEMOTHERDELIVERYHPS_CLEAN, '
                                                                              'retained only within '
                                                                              '[1950-01-01, 2100-01-01). '
                                                                              'Selection precedes range '
                                                                              'validation: an out-of-range '
                                                                              'first value becomes null '
                                                                              'rather than falling through '
                                                                              'to a later value. This '
                                                                              'event-time proxy is not an '
                                                                              'independently observed status '
                                                                              'change.',
                                              'record_status_effective_to': 'Always NULL as a TIMESTAMP in '
                                                                            'the labour-delivery projection. '
                                                                            'The pipeline supplies no '
                                                                            'row-status end boundary and '
                                                                            'does not substitute discharge, '
                                                                            'delivery-end or ingestion '
                                                                            'timestamps.',
                                              'rom_datetime': 'Date and time associated with rom for the '
                                                              'labour delivery record. It is derived from '
                                                              'bronze field `ROMDATETIME_CLEAN` in '
                                                              '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                              'Source precision and timezone handling follow '
                                                              'the pipeline expression; null means the time '
                                                              'was unavailable.',
                                              'rom_method': 'The coded method by which rupture of the '
                                                            'membranes occurred.',
                                              'rom_reason': 'The coded reason for the rupture of the '
                                                            'membranes being performed.',
                                              'second_stage_datetime': 'Date and time associated with second '
                                                                       'stage for the labour delivery '
                                                                       'record. It is derived from bronze '
                                                                       'field '
                                                                       '`LABOURONSETSECONDSTAGEDATETIME_CLEAN` '
                                                                       'in '
                                                                       '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                                       'Source precision and timezone '
                                                                       'handling follow the pipeline '
                                                                       'expression; null means the time was '
                                                                       'unavailable.',
                                              'source_code': 'The method by which the process of labour '
                                                             'began, or delivery by caesarean section '
                                                             'occurred.',
                                              'source_coding_system': 'Coding system or source namespace in '
                                                                      'which the source code is defined for '
                                                                      'each labour delivery record. It is '
                                                                      'produced by the silver transformation '
                                                                      'and has no direct bronze-column '
                                                                      'lineage entry. Source code meanings '
                                                                      'and sentinel values are retained '
                                                                      'unless the pipeline explicitly maps '
                                                                      'them; null means no code was '
                                                                      'supplied.',
                                              'source_display': 'The method by which the process of labour '
                                                                'began, or delivery by caesarean section '
                                                                'occurred.',
                                              'source_feed': 'Originating data feed responsible for the '
                                                             'record for each labour delivery record. It is '
                                                             'produced by the silver transformation and has '
                                                             'no direct bronze-column lineage entry. '
                                                             'Whitespace and source sentinel text are '
                                                             'retained unless the pipeline explicitly '
                                                             'normalizes them; null means no value was '
                                                             'supplied.',
                                              'source_link_status': 'Processing or clinical status of source '
                                                                    'link for the labour delivery record. It '
                                                                    'is derived from bronze field '
                                                                    '`PERSON_ID` in '
                                                                    '`4_prod.bronze.map_maternity_labour_delivery`, '
                                                                    'selected as the provenance anchor from '
                                                                    '2 recorded bronze source fields. '
                                                                    'Whitespace and source sentinel text are '
                                                                    'retained unless the pipeline explicitly '
                                                                    'normalizes them; null means no value '
                                                                    'was supplied.',
                                              'source_update_timestamp': 'map_maternity_labour_delivery.RECORD_UPDATED_DT '
                                                                         'carried unchanged from the '
                                                                         'deduplicated labour source row. '
                                                                         'This is not the pregnancy-spine '
                                                                         'update clock or Silver refresh '
                                                                         'time; a missing source timestamp '
                                                                         'remains null.',
                                              'spine_person_mismatch_ind': 'Unique MAT person identifier for '
                                                                           'the mother.',
                                              'subject_id_system': 'Unique MAT person identifier for the '
                                                                   'mother.',
                                              'subject_key': 'Deterministic SHA-256 over the strongest '
                                                             'source identifier. Not salted, not secret; a '
                                                             'stable join key across feeds where person_id '
                                                             'is unresolved.',
                                              'third_stage_end_datetime': 'Date and time associated with '
                                                                          'third stage end for the labour '
                                                                          'delivery record. It is derived '
                                                                          'from bronze field '
                                                                          '`LABOURTHIRDSTAGEENDDATETIME_CLEAN` '
                                                                          'in '
                                                                          '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                                          'Source precision and timezone '
                                                                          'handling follow the pipeline '
                                                                          'expression; null means the time '
                                                                          'was unavailable.',
                                              'vip_ind': 'Indicator that the source record carries VIP '
                                                         'handling status for each labour delivery record. '
                                                         'It is produced by the silver transformation and '
                                                         'has no direct bronze-column lineage entry. Null '
                                                         'means the source did not state the indicator and '
                                                         'must not be interpreted as false.',
                                              'withheld_identity_ind': 'Indicator that the source identity '
                                                                       'was deliberately withheld for each '
                                                                       'labour delivery record. It is '
                                                                       'produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Null '
                                                                       'means the source did not state the '
                                                                       'indicator and must not be '
                                                                       'interpreted as false.'},
                                  'quantity_override': True,
                                  'table': 'Internal quality-controlled twin of clinical_labour_delivery: '
                                           'the Gold repairs, nulls and expectations are applied here and '
                                           'the published Gold MV reads this object. Materialized rather '
                                           'than temporary so the published MV can refresh incrementally.'},
 'clinical_labour_delivery': {'columns': {'caesarean_datetime': 'Date and time associated with caesarean for '
                                                                'the labour delivery record. It is derived '
                                                                'from bronze field `CAESAREANDATETIME_CLEAN` '
                                                                'in '
                                                                '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                                'Source precision and timezone handling '
                                                                'follow the pipeline expression; null means '
                                                                'the time was unavailable.',
                                          'confidentiality_code': 'Source confidentiality classification '
                                                                  'attached to the record for each labour '
                                                                  'delivery record. It is produced by the '
                                                                  'silver transformation and has no direct '
                                                                  'bronze-column lineage entry. Source code '
                                                                  'meanings and sentinel values are retained '
                                                                  'unless the pipeline explicitly maps them; '
                                                                  'null means no code was supplied.',
                                          'decision_to_deliver_datetime': 'Date and time associated with '
                                                                          'decision to deliver for the '
                                                                          'labour delivery record. It is '
                                                                          'derived from bronze field '
                                                                          '`DECISIONTODELIVERDATETIME_CLEAN` '
                                                                          'in '
                                                                          '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                                          'Source precision and timezone '
                                                                          'handling follow the pipeline '
                                                                          'expression; null means the time '
                                                                          'was unavailable.',
                                          'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                          'available.',
                                          'episiotomy_reason': 'The coded reason an episiotomy was '
                                                               'performed.',
                                          'event_datetime': 'Date and time when the represented clinical or '
                                                            'administrative event occurred for each labour '
                                                            'delivery record. It is derived from bronze '
                                                            'field `CAESAREANDATETIME_CLEAN` in '
                                                            '`4_prod.bronze.map_maternity_labour_delivery`, '
                                                            'selected as the provenance anchor from 3 '
                                                            'recorded bronze source fields. Source precision '
                                                            'and timezone handling follow the pipeline '
                                                            'expression; null means the time was '
                                                            'unavailable.',
                                          'event_datetime_local': 'Local-time start of the labour-delivery '
                                                                  'event, derived from the cleaned MSDS '
                                                                  'mother delivery start, labour onset or '
                                                                  'caesarean datetime.',
                                          'event_end_datetime': 'Date and time when the represented clinical '
                                                                'or administrative event ended for each '
                                                                'labour delivery record. It is derived from '
                                                                'bronze field '
                                                                '`DISCHARGEDATETIMEMOTHERHSP_CLEAN` in '
                                                                '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                                'Source precision and timezone handling '
                                                                'follow the pipeline expression; null means '
                                                                'the time was unavailable.',
                                          'event_end_datetime_local': 'Local-time end of the labour-delivery '
                                                                      'event, derived from the cleaned MSDS '
                                                                      'mother hospital discharge datetime. '
                                                                      'Gold: NULL wherever Gold nulls '
                                                                      'event_end_datetime.',
                                          'fact_category': 'Contract category used to group the record with '
                                                           'comparable clinical facts for each labour '
                                                           'delivery record. It is produced by the silver '
                                                           'transformation and has no direct bronze-column '
                                                           'lineage entry. Whitespace and source sentinel '
                                                           'text are retained unless the pipeline explicitly '
                                                           'normalizes them; null means no value was '
                                                           'supplied.',
                                          'intrapartum_org_site': 'The NHS organisation site identifier '
                                                                  'where the mother started intrapartum '
                                                                  'care.',
                                          'intrapartum_setting': "The mother's actual location (type) at the "
                                                                 'start of intrapartum care.',
                                          'journey_pregnancy_key': 'Deterministic SHA-256 key for the '
                                                                   'pregnancy journey.',
                                          'labour_delivery_id': 'MSDS LABOURDELIVERYID; primary key of this '
                                                                'table.',
                                          'labour_onset_method': 'The method by which the process of labour '
                                                                 'began, or delivery by caesarean section '
                                                                 'occurred.',
                                          'labour_onset_presentation': 'The coded fetal presentation '
                                                                       'recorded at the onset of labour.',
                                          'loaded_at': 'map_maternity_labour_delivery.ADC_UPDT carried '
                                                       'unchanged from the deduplicated labour source row. '
                                                       'Identity fan-out is collapsed without aggregating '
                                                       'this clock; the joined pregnancy spine does not '
                                                       'contribute a load timestamp. This is not the Silver '
                                                       'refresh time.',
                                          'mother_admission_method': 'The coded method of admission for the '
                                                                     "mother's hospital provider spell in "
                                                                     'which delivery occurred.',
                                          'mother_discharge_datetime': 'Date and time associated with mother '
                                                                       'discharge for the labour delivery '
                                                                       'record. It is derived from bronze '
                                                                       'field '
                                                                       '`DISCHARGEDATETIMEMOTHERHSP_CLEAN` '
                                                                       'in '
                                                                       '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                                       'Source precision and timezone '
                                                                       'handling follow the pipeline '
                                                                       'expression; null means the time was '
                                                                       'unavailable.',
                                          'mother_discharge_destination': 'The coded destination of the '
                                                                          'mother on discharge from the '
                                                                          'post-delivery hospital provider '
                                                                          'spell.',
                                          'mother_discharge_method': 'The coded method of discharge for the '
                                                                     'mother from the post-delivery hospital '
                                                                     'provider spell.',
                                          'patient_event_key': 'Deterministic event key derived from source '
                                                               'identifiers; retained as a stable join key.',
                                          'person_id': 'Maternal Millennium person identifier, exposed as '
                                                       'BIGINT. Silver prefers Person_ID from the linked '
                                                       'pregnancy spine, falling back to the MSDS adapter '
                                                       'PERSON_ID only when its source-row group has one '
                                                       'distinct person identifier. Null if neither yields '
                                                       'an identifier.',
                                          'placenta_delivery_method': 'The coded method by which the '
                                                                      'placenta was delivered.',
                                          'postnatal_lead_provider': 'NHS organisation identifier of post '
                                                                     'natal lead provider organisation.',
                                          'pregnancy_id': 'Source-system identifier for the pregnancy '
                                                          'associated with each labour delivery record. It '
                                                          'is derived from bronze field '
                                                          '`PREGNANCY_ID_PARSED` in '
                                                          '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                          'Its uniqueness is limited to the originating '
                                                          'source namespace; null means no identifier was '
                                                          'supplied.',
                                          'pregnancy_orphan_ind': 'Unique pregnancy episode identifier.',
                                          'record_status': 'Literal active for every retained MSDS '
                                                           'labour-delivery row. This is not derived from '
                                                           'source presence or clinical delivery outcome and '
                                                           'does not establish that a source record has '
                                                           'never been superseded or deleted.',
                                          'record_status_effective_from': 'First non-null '
                                                                          'map_maternity_labour_delivery '
                                                                          'timestamp in the order '
                                                                          'LABOURONSETDATETIME_CLEAN, '
                                                                          'CAESAREANDATETIME_CLEAN, '
                                                                          'STARTDATETIMEMOTHERDELIVERYHPS_CLEAN, '
                                                                          'retained only within [1950-01-01, '
                                                                          '2100-01-01). Selection precedes '
                                                                          'range validation: an out-of-range '
                                                                          'first value becomes null rather '
                                                                          'than falling through to a later '
                                                                          'value. This event-time proxy is '
                                                                          'not an independently observed '
                                                                          'status change.',
                                          'record_status_effective_to': 'Always NULL as a TIMESTAMP in the '
                                                                        'labour-delivery projection. The '
                                                                        'pipeline supplies no row-status end '
                                                                        'boundary and does not substitute '
                                                                        'discharge, delivery-end or '
                                                                        'ingestion timestamps.',
                                          'rom_datetime': 'Date and time associated with rom for the labour '
                                                          'delivery record. It is derived from bronze field '
                                                          '`ROMDATETIME_CLEAN` in '
                                                          '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                          'Source precision and timezone handling follow the '
                                                          'pipeline expression; null means the time was '
                                                          'unavailable.',
                                          'rom_method': 'The coded method by which rupture of the membranes '
                                                        'occurred.',
                                          'rom_reason': 'The coded reason for the rupture of the membranes '
                                                        'being performed.',
                                          'second_stage_datetime': 'Date and time associated with second '
                                                                   'stage for the labour delivery record. It '
                                                                   'is derived from bronze field '
                                                                   '`LABOURONSETSECONDSTAGEDATETIME_CLEAN` '
                                                                   'in '
                                                                   '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                                   'Source precision and timezone handling '
                                                                   'follow the pipeline expression; null '
                                                                   'means the time was unavailable.',
                                          'source_code': 'The method by which the process of labour began, '
                                                         'or delivery by caesarean section occurred.',
                                          'source_coding_system': 'Coding system or source namespace in '
                                                                  'which the source code is defined for each '
                                                                  'labour delivery record. It is produced by '
                                                                  'the silver transformation and has no '
                                                                  'direct bronze-column lineage entry. '
                                                                  'Source code meanings and sentinel values '
                                                                  'are retained unless the pipeline '
                                                                  'explicitly maps them; null means no code '
                                                                  'was supplied.',
                                          'source_display': 'The method by which the process of labour '
                                                            'began, or delivery by caesarean section '
                                                            'occurred.',
                                          'source_feed': 'Originating data feed responsible for the record '
                                                         'for each labour delivery record. It is produced by '
                                                         'the silver transformation and has no direct '
                                                         'bronze-column lineage entry. Whitespace and source '
                                                         'sentinel text are retained unless the pipeline '
                                                         'explicitly normalizes them; null means no value '
                                                         'was supplied.',
                                          'source_link_status': 'Processing or clinical status of source '
                                                                'link for the labour delivery record. It is '
                                                                'derived from bronze field `PERSON_ID` in '
                                                                '`4_prod.bronze.map_maternity_labour_delivery`, '
                                                                'selected as the provenance anchor from 2 '
                                                                'recorded bronze source fields. Whitespace '
                                                                'and source sentinel text are retained '
                                                                'unless the pipeline explicitly normalizes '
                                                                'them; null means no value was supplied.',
                                          'source_update_timestamp': 'map_maternity_labour_delivery.RECORD_UPDATED_DT '
                                                                     'carried unchanged from the '
                                                                     'deduplicated labour source row. This '
                                                                     'is not the pregnancy-spine update '
                                                                     'clock or Silver refresh time; a '
                                                                     'missing source timestamp remains null.',
                                          'spine_person_mismatch_ind': 'Unique MAT person identifier for the '
                                                                       'mother.',
                                          'subject_id_system': 'Unique MAT person identifier for the mother.',
                                          'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                         'identifier. Not salted, not secret; a stable join '
                                                         'key across feeds where person_id is unresolved.',
                                          'third_stage_end_datetime': 'Date and time associated with third '
                                                                      'stage end for the labour delivery '
                                                                      'record. It is derived from bronze '
                                                                      'field '
                                                                      '`LABOURTHIRDSTAGEENDDATETIME_CLEAN` '
                                                                      'in '
                                                                      '`4_prod.bronze.map_maternity_labour_delivery`. '
                                                                      'Source precision and timezone '
                                                                      'handling follow the pipeline '
                                                                      'expression; null means the time was '
                                                                      'unavailable.',
                                          'vip_ind': 'Indicator that the source record carries VIP handling '
                                                     'status for each labour delivery record. It is produced '
                                                     'by the silver transformation and has no direct '
                                                     'bronze-column lineage entry. Null means the source did '
                                                     'not state the indicator and must not be interpreted as '
                                                     'false.',
                                          'withheld_identity_ind': 'Indicator that the source identity was '
                                                                   'deliberately withheld for each labour '
                                                                   'delivery record. It is produced by the '
                                                                   'silver transformation and has no direct '
                                                                   'bronze-column lineage entry. Null means '
                                                                   'the source did not state the indicator '
                                                                   'and must not be interpreted as false.'},
                              'quantity_override': False,
                              'table': 'One MSDS labour-delivery row with pregnancy-spine identity evidence. '
                                       'Gold QC twin of the silver product: 2 columns are repaired or '
                                       'nulled, 1 rule(s) drop rows, 3 check(s) are advisory. Each rule '
                                       'states its reason in the pipeline notebook, and Lakeflow expectation '
                                       'metrics report what every rule matched on each update. Coverage: '
                                       'Still built from the maternity services dataset (MSDS): Millennium '
                                       'matches 99% of MSDS deliveries from April 2019 but records the '
                                       'delivery method on 73% as many and labour onset on 31% as many, so '
                                       'the table was not moved onto Millennium in this release; '
                                       'complete-through month per release: '
                                       '6_mgmt.data_quality.release_finding (finding_code '
                                       'F2_complete_through).'}}
