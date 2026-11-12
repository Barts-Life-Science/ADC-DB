"""Gold specialist.critical_care_admission: 2 dataset(s).

- datasets: _qc_clinical_critical_care_admission, clinical_critical_care_admission
- moved verbatim from: 3_gold_specialist_care:798, 3_gold_specialist_care:839, 3_gold_specialist_care:847, 3_gold_specialist_care:922, 3_gold_specialist_care:926, 3_gold_specialist_care:943

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.critical_care_admission ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CRITICAL_CARE_ADMISSION_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_pat_id` AS `source_pat_id`',
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
    '`admission_key` AS `admission_key`',
    '`source_unit` AS `source_unit`',
    '`unit_raw` AS `unit_raw`',
    '`source_site` AS `source_site`',
    '`hospital_admission_datetime` AS `hospital_admission_datetime`',
    '`unit_admission_datetime` AS `unit_admission_datetime`',
    '`unit_discharge_datetime` AS `unit_discharge_datetime`',
    '`hospital_discharge_datetime` AS `hospital_discharge_datetime`',
    '`admission_diagnosis_code` AS `admission_diagnosis_code`',
    '`max_organ_support` AS `max_organ_support`',
    '`cause_of_death` AS `cause_of_death`',
    '`date_of_death` AS `date_of_death`',
    '`source_updated_at` AS `source_updated_at`',
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


# contract v2: QC/batch inputs come from internal _clinical_critical_care_admission_metadata; source history stays on the main research table.
CLINICAL_CRITICAL_CARE_ADMISSION_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 141,391 rows of 141,391 that
    # are current and attributable; identity_status = 'resolved' keeps the 135,937 rows of
    # 141,391 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_CRITICAL_CARE_ADMISSION_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.critical_care_admission.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 5,454 of 141,391 at the profile.
    "gold.clinical.critical_care_admission.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 141,391 at the profile.
    "gold.clinical.critical_care_admission.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 12 of 141,391 rows (0.00849%) when profiled on 2026-08-24.
    "gold.clinical.critical_care_admission.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1 of 141,391 rows (0.000707%) when profiled on 2026-08-24.
    "gold.clinical.critical_care_admission.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_CRITICAL_CARE_ADMISSION_PERSON_SCOPE_RULES = {
    "gold.clinical.critical_care_admission.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_critical_care_admission"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_CRITICAL_CARE_ADMISSION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CRITICAL_CARE_ADMISSION_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_CRITICAL_CARE_ADMISSION_PERSON_SCOPE_RULES)
def _gold_qc_clinical_critical_care_admission():
    """Quality-controlled twin of journey_clinical.critical_care_admission."""
    df = _qc(
        "clinical_critical_care_admission",
        CLINICAL_CRITICAL_CARE_ADMISSION_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.critical_care_admission"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_critical_care_admission():
    """Contract-v2 public twin of clinical_critical_care_admission; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_critical_care_admission")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_pat_id` AS `source_pat_id`',
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
        '`admission_key` AS `admission_key`',
        '`source_unit` AS `source_unit`',
        '`unit_raw` AS `unit_raw`',
        '`source_site` AS `source_site`',
        '`hospital_admission_datetime` AS `hospital_admission_datetime`',
        '`unit_admission_datetime` AS `unit_admission_datetime`',
        '`unit_discharge_datetime` AS `unit_discharge_datetime`',
        '`hospital_discharge_datetime` AS `hospital_discharge_datetime`',
        '`admission_diagnosis_code` AS `admission_diagnosis_code`',
        '`max_organ_support` AS `max_organ_support`',
        '`cause_of_death` AS `cause_of_death`',
        '`date_of_death` AS `date_of_death`',
        '`source_updated_at` AS `source_updated_at`',
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
    'clinical_critical_care_admission': GoldRules(
        table='clinical_critical_care_admission',
        select=CLINICAL_CRITICAL_CARE_ADMISSION_SELECT,
        mandatory=CLINICAL_CRITICAL_CARE_ADMISSION_MANDATORY_RULES,
        advisory=CLINICAL_CRITICAL_CARE_ADMISSION_ADVISORY_RULES,
        person_scope=CLINICAL_CRITICAL_CARE_ADMISSION_PERSON_SCOPE_RULES,
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


COMMENTS = {'_qc_clinical_critical_care_admission': {'columns': {'admission_diagnosis_code': 'Source-system code '
                                                                                  'representing admission '
                                                                                  'diagnosis for the '
                                                                                  'critical care admission '
                                                                                  'record. It is derived '
                                                                                  'from bronze field '
                                                                                  '`dgn_adm1_code` in '
                                                                                  '`4_prod.bronze.map_critical_care_admission`. '
                                                                                  'Source code meanings and '
                                                                                  'sentinel values are '
                                                                                  'retained unless the '
                                                                                  'pipeline explicitly maps '
                                                                                  'them; null means no code '
                                                                                  'was supplied.',
                                                      'admission_key': 'Composite admission linkage string '
                                                                       'built in bronze by joining '
                                                                       'SOURCE_UNIT and SOURCE_PAT_ID with a '
                                                                       'colon. The native components are '
                                                                       'published separately; the existing '
                                                                       'linkage value is retained.',
                                                      'cause_of_death': 'Value describing cause of death for '
                                                                        'the critical care admission record. '
                                                                        'It is carried from bronze field '
                                                                        '`cause_of_death` in '
                                                                        '`4_prod.bronze.map_critical_care_admission`. '
                                                                        'Whitespace and source sentinel text '
                                                                        'are retained unless the pipeline '
                                                                        'explicitly normalizes them; null '
                                                                        'means no value was supplied.',
                                                      'confidentiality_code': 'Source confidentiality '
                                                                              'classification attached to '
                                                                              'the record for each critical '
                                                                              'care admission record. It is '
                                                                              'produced by the silver '
                                                                              'transformation and has no '
                                                                              'direct bronze-column lineage '
                                                                              'entry. Source code meanings '
                                                                              'and sentinel values are '
                                                                              'retained unless the pipeline '
                                                                              'explicitly maps them; null '
                                                                              'means no code was supplied.',
                                                      'date_of_death': 'Date or time value for date of death '
                                                                       'on the critical care admission '
                                                                       'record. It is derived from bronze '
                                                                       'field `date_of_death_CLEAN` in '
                                                                       '`4_prod.bronze.map_critical_care_admission`. '
                                                                       'Source precision and timezone '
                                                                       'handling follow the pipeline '
                                                                       'expression; null means the time was '
                                                                       'unavailable.',
                                                      'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT '
                                                                      'when available.',
                                                      'event_datetime': 'Date and time when the represented '
                                                                        'clinical or administrative event '
                                                                        'occurred for each critical care '
                                                                        'admission record. It is derived '
                                                                        'from bronze field '
                                                                        '`date_unit_adm_CLEAN` in '
                                                                        '`4_prod.bronze.map_critical_care_admission`. '
                                                                        'Source precision and timezone '
                                                                        'handling follow the pipeline '
                                                                        'expression; null means the time was '
                                                                        'unavailable.',
                                                      'event_datetime_local': 'Local-time representation of '
                                                                              'the event start datetime for '
                                                                              'the critical-care admission.',
                                                      'event_end_datetime': 'Date and time when the '
                                                                            'represented clinical or '
                                                                            'administrative event ended for '
                                                                            'each critical care admission '
                                                                            'record. It is derived from '
                                                                            'bronze field '
                                                                            '`date_unit_discharge_CLEAN` in '
                                                                            '`4_prod.bronze.map_critical_care_admission`. '
                                                                            'Source precision and timezone '
                                                                            'handling follow the pipeline '
                                                                            'expression; null means the time '
                                                                            'was unavailable.',
                                                      'event_end_datetime_local': 'Local-time representation '
                                                                                  'of the event end datetime '
                                                                                  'for the critical-care '
                                                                                  'admission. Gold: NULL '
                                                                                  'wherever Gold nulls '
                                                                                  'event_end_datetime.',
                                                      'fact_category': 'Contract category used to group the '
                                                                       'record with comparable clinical '
                                                                       'facts for each critical care '
                                                                       'admission record. It is produced by '
                                                                       'the silver transformation and has no '
                                                                       'direct bronze-column lineage entry. '
                                                                       'Whitespace and source sentinel text '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly normalizes them; null '
                                                                       'means no value was supplied.',
                                                      'hospital_admission_datetime': 'Date and time '
                                                                                     'associated with '
                                                                                     'hospital admission for '
                                                                                     'the critical care '
                                                                                     'admission record. It '
                                                                                     'is derived from bronze '
                                                                                     'field '
                                                                                     '`date_hospital_adm_CLEAN` '
                                                                                     'in '
                                                                                     '`4_prod.bronze.map_critical_care_admission`. '
                                                                                     'Source precision and '
                                                                                     'timezone handling '
                                                                                     'follow the pipeline '
                                                                                     'expression; null means '
                                                                                     'the time was '
                                                                                     'unavailable.',
                                                      'hospital_discharge_datetime': 'Date and time '
                                                                                     'associated with '
                                                                                     'hospital discharge for '
                                                                                     'the critical care '
                                                                                     'admission record. It '
                                                                                     'is derived from bronze '
                                                                                     'field '
                                                                                     '`date_hospital_discharge_CLEAN` '
                                                                                     'in '
                                                                                     '`4_prod.bronze.map_critical_care_admission`. '
                                                                                     'Source precision and '
                                                                                     'timezone handling '
                                                                                     'follow the pipeline '
                                                                                     'expression; null means '
                                                                                     'the time was '
                                                                                     'unavailable.',
                                                      'loaded_at': 'map_critical_care_admission.ADC_UPDT '
                                                                   'carried unchanged for the contributing '
                                                                   'Medicus admission row. This is bronze '
                                                                   'ingestion provenance, distinct from '
                                                                   'updated_at/source_update_timestamp and '
                                                                   'the admission/discharge dates; it is not '
                                                                   'Silver refresh time.',
                                                      'max_organ_support': 'Numeric measure of max organ '
                                                                           'support for the critical care '
                                                                           'admission record. It is derived '
                                                                           'from bronze field `maxorgansupp` '
                                                                           'in '
                                                                           '`4_prod.bronze.map_critical_care_admission`. '
                                                                           'Units and source sentinel values '
                                                                           'follow the named source field '
                                                                           'unless the pipeline explicitly '
                                                                           'converts them; null is not '
                                                                           'equivalent to zero.',
                                                      'patient_event_key': 'Deterministic event key derived '
                                                                           'from source identifiers; '
                                                                           'retained as a stable join key.',
                                                      'person_id': 'Native Millennium PERSON_ID as BIGINT '
                                                                   'when resolved.',
                                                      'person_link_status': 'Processing or clinical status '
                                                                            'of person link for the critical '
                                                                            'care admission record. It is '
                                                                            'carried from bronze field '
                                                                            '`PERSON_LINK_STATUS` in '
                                                                            '`4_prod.bronze.map_critical_care_admission`. '
                                                                            'Whitespace and source sentinel '
                                                                            'text are retained unless the '
                                                                            'pipeline explicitly normalizes '
                                                                            'them; null means no value was '
                                                                            'supplied.',
                                                      'record_status': 'Retracted when '
                                                                       'map_critical_care_admission.SOURCE_PRESENT_IND '
                                                                       'is false, otherwise active; a '
                                                                       'missing presence flag defaults true. '
                                                                       'No superseded status is emitted, and '
                                                                       'hospital/unit discharge or death '
                                                                       'dates do not determine this label.',
                                                      'record_status_effective_from': 'map_critical_care_admission.date_unit_adm_CLEAN '
                                                                                      'carried unchanged as '
                                                                                      'the history start. '
                                                                                      'This unit-admission '
                                                                                      'time is a proxy, not '
                                                                                      'an independently '
                                                                                      'observed status '
                                                                                      'transition; no '
                                                                                      'hospital-admission or '
                                                                                      'ingestion fallback is '
                                                                                      'applied.',
                                                      'record_status_effective_to': 'map_critical_care_admission.ADC_UPDT '
                                                                                    'only when the admission '
                                                                                    'row is retracted; '
                                                                                    'otherwise null. This is '
                                                                                    'an ingestion-time end '
                                                                                    'proxy, not '
                                                                                    'unit/hospital discharge '
                                                                                    'or death time; a '
                                                                                    'missing ADC_UPDT '
                                                                                    'remains null.',
                                                      'source_code': 'Code supplied by the originating '
                                                                     'source system for the represented '
                                                                     'concept for each critical care '
                                                                     'admission record. It is produced by '
                                                                     'the silver transformation and has no '
                                                                     'direct bronze-column lineage entry. '
                                                                     'Source code meanings and sentinel '
                                                                     'values are retained unless the '
                                                                     'pipeline explicitly maps them; null '
                                                                     'means no code was supplied.',
                                                      'source_coding_system': 'Coding system or source '
                                                                              'namespace in which the source '
                                                                              'code is defined for each '
                                                                              'critical care admission '
                                                                              'record. It is produced by the '
                                                                              'silver transformation and has '
                                                                              'no direct bronze-column '
                                                                              'lineage entry. Source code '
                                                                              'meanings and sentinel values '
                                                                              'are retained unless the '
                                                                              'pipeline explicitly maps '
                                                                              'them; null means no code was '
                                                                              'supplied.',
                                                      'source_display': 'Human-readable label supplied by '
                                                                        'the source system for the source '
                                                                        'code for each critical care '
                                                                        'admission record. It is produced by '
                                                                        'the silver transformation and has '
                                                                        'no direct bronze-column lineage '
                                                                        'entry. Whitespace and source '
                                                                        'sentinel text are retained unless '
                                                                        'the pipeline explicitly normalizes '
                                                                        'them; null means no value was '
                                                                        'supplied.',
                                                      'source_feed': 'Originating data feed responsible for '
                                                                     'the record for each critical care '
                                                                     'admission record. It is produced by '
                                                                     'the silver transformation and has no '
                                                                     'direct bronze-column lineage entry. '
                                                                     'Whitespace and source sentinel text '
                                                                     'are retained unless the pipeline '
                                                                     'explicitly normalizes them; null means '
                                                                     'no value was supplied.',
                                                      'source_pat_id': 'Native Medicus patient/admission-row '
                                                                       'id from SOURCE_PAT_ID, exposed as '
                                                                       'BIGINT. Together with source_unit it '
                                                                       'identifies the source admission row; '
                                                                       'not a Millennium person identifier.',
                                                      'source_site': 'Value describing source site for the '
                                                                     'critical care admission record. It is '
                                                                     'carried from bronze field '
                                                                     '`SOURCE_SITE` in '
                                                                     '`4_prod.bronze.map_critical_care_admission`. '
                                                                     'Whitespace and source sentinel text '
                                                                     'are retained unless the pipeline '
                                                                     'explicitly normalizes them; null means '
                                                                     'no value was supplied.',
                                                      'source_unit': 'Value describing source unit for the '
                                                                     'critical care admission record. It is '
                                                                     'carried from bronze field '
                                                                     '`SOURCE_UNIT` in '
                                                                     '`4_prod.bronze.map_critical_care_admission`. '
                                                                     'Whitespace and source sentinel text '
                                                                     'are retained unless the pipeline '
                                                                     'explicitly normalizes them; null means '
                                                                     'no value was supplied.',
                                                      'source_update_timestamp': 'Native source-system '
                                                                                 'update timestamp carried '
                                                                                 'through the research '
                                                                                 'transformation for each '
                                                                                 'critical care admission '
                                                                                 'record. It is derived from '
                                                                                 'bronze field `updated_at` '
                                                                                 'in '
                                                                                 '`4_prod.bronze.map_critical_care_admission`. '
                                                                                 'Source precision and '
                                                                                 'timezone handling follow '
                                                                                 'the pipeline expression; '
                                                                                 'null means the time was '
                                                                                 'unavailable.',
                                                      'source_updated_at': 'Date or time value for source '
                                                                           'updated at on the critical care '
                                                                           'admission record. It is derived '
                                                                           'from bronze field `updated_at` '
                                                                           'in '
                                                                           '`4_prod.bronze.map_critical_care_admission`. '
                                                                           'Source precision and timezone '
                                                                           'handling follow the pipeline '
                                                                           'expression; null means the time '
                                                                           'was unavailable.',
                                                      'subject_id_system': 'Identifier namespace used to '
                                                                           'interpret the subject identifier '
                                                                           'on this record for each critical '
                                                                           'care admission record. It is '
                                                                           'derived from bronze field '
                                                                           '`PERSON_ID` in '
                                                                           '`4_prod.bronze.map_critical_care_admission`. '
                                                                           'Its uniqueness is limited to the '
                                                                           'originating source namespace; '
                                                                           'null means no identifier was '
                                                                           'supplied.',
                                                      'subject_key': 'Deterministic SHA-256 over the '
                                                                     'strongest source identifier. Not '
                                                                     'salted, not secret; a stable join key '
                                                                     'across feeds where person_id is '
                                                                     'unresolved.',
                                                      'unit_admission_datetime': 'Date and time associated '
                                                                                 'with unit admission for '
                                                                                 'the critical care '
                                                                                 'admission record. It is '
                                                                                 'derived from bronze field '
                                                                                 '`date_unit_adm_CLEAN` in '
                                                                                 '`4_prod.bronze.map_critical_care_admission`. '
                                                                                 'Source precision and '
                                                                                 'timezone handling follow '
                                                                                 'the pipeline expression; '
                                                                                 'null means the time was '
                                                                                 'unavailable.',
                                                      'unit_discharge_datetime': 'Date and time associated '
                                                                                 'with unit discharge for '
                                                                                 'the critical care '
                                                                                 'admission record. It is '
                                                                                 'derived from bronze field '
                                                                                 '`date_unit_discharge_CLEAN` '
                                                                                 'in '
                                                                                 '`4_prod.bronze.map_critical_care_admission`. '
                                                                                 'Source precision and '
                                                                                 'timezone handling follow '
                                                                                 'the pipeline expression; '
                                                                                 'null means the time was '
                                                                                 'unavailable.',
                                                      'unit_raw': 'Value describing unit raw for the '
                                                                  'critical care admission record. It is '
                                                                  'carried from bronze field `UNIT_RAW` in '
                                                                  '`4_prod.bronze.map_critical_care_admission`. '
                                                                  'Whitespace and source sentinel text are '
                                                                  'retained unless the pipeline explicitly '
                                                                  'normalizes them; null means no value was '
                                                                  'supplied.',
                                                      'vip_ind': 'Indicator that the source record carries '
                                                                 'VIP handling status for each critical care '
                                                                 'admission record. It is produced by the '
                                                                 'silver transformation and has no direct '
                                                                 'bronze-column lineage entry. Null means '
                                                                 'the source did not state the indicator and '
                                                                 'must not be interpreted as false.',
                                                      'withheld_identity_ind': 'Indicator that the source '
                                                                               'identity was deliberately '
                                                                               'withheld for each critical '
                                                                               'care admission record. It is '
                                                                               'produced by the silver '
                                                                               'transformation and has no '
                                                                               'direct bronze-column lineage '
                                                                               'entry. Null means the source '
                                                                               'did not state the indicator '
                                                                               'and must not be interpreted '
                                                                               'as false.'},
                                          'quantity_override': True,
                                          'table': 'Internal quality-controlled twin of '
                                                   'clinical_critical_care_admission: the Gold repairs, '
                                                   'nulls and expectations are applied here and the '
                                                   'published Gold MV reads this object. Materialized rather '
                                                   'than temporary so the published MV can refresh '
                                                   'incrementally.'},
 'clinical_critical_care_admission': {'columns': {'admission_diagnosis_code': 'Source-system code '
                                                                              'representing admission '
                                                                              'diagnosis for the critical '
                                                                              'care admission record. It is '
                                                                              'derived from bronze field '
                                                                              '`dgn_adm1_code` in '
                                                                              '`4_prod.bronze.map_critical_care_admission`. '
                                                                              'Source code meanings and '
                                                                              'sentinel values are retained '
                                                                              'unless the pipeline '
                                                                              'explicitly maps them; null '
                                                                              'means no code was supplied.',
                                                  'admission_key': 'Composite admission linkage string built '
                                                                   'in bronze by joining SOURCE_UNIT and '
                                                                   'SOURCE_PAT_ID with a colon. The native '
                                                                   'components are published separately; the '
                                                                   'existing linkage value is retained.',
                                                  'cause_of_death': 'Value describing cause of death for the '
                                                                    'critical care admission record. It is '
                                                                    'carried from bronze field '
                                                                    '`cause_of_death` in '
                                                                    '`4_prod.bronze.map_critical_care_admission`. '
                                                                    'Whitespace and source sentinel text are '
                                                                    'retained unless the pipeline explicitly '
                                                                    'normalizes them; null means no value '
                                                                    'was supplied.',
                                                  'confidentiality_code': 'Source confidentiality '
                                                                          'classification attached to the '
                                                                          'record for each critical care '
                                                                          'admission record. It is produced '
                                                                          'by the silver transformation and '
                                                                          'has no direct bronze-column '
                                                                          'lineage entry. Source code '
                                                                          'meanings and sentinel values are '
                                                                          'retained unless the pipeline '
                                                                          'explicitly maps them; null means '
                                                                          'no code was supplied.',
                                                  'date_of_death': 'Date or time value for date of death on '
                                                                   'the critical care admission record. It '
                                                                   'is derived from bronze field '
                                                                   '`date_of_death_CLEAN` in '
                                                                   '`4_prod.bronze.map_critical_care_admission`. '
                                                                   'Source precision and timezone handling '
                                                                   'follow the pipeline expression; null '
                                                                   'means the time was unavailable. Date of '
                                                                   'death as recorded on the critical-care '
                                                                   'admission (about 6% of admissions); for '
                                                                   'ICU mortality use spine_person death, '
                                                                   'which finds about 11% within the stay '
                                                                   'plus 30 days.',
                                                  'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT '
                                                                  'when available.',
                                                  'event_datetime': 'Date and time when the represented '
                                                                    'clinical or administrative event '
                                                                    'occurred for each critical care '
                                                                    'admission record. It is derived from '
                                                                    'bronze field `date_unit_adm_CLEAN` in '
                                                                    '`4_prod.bronze.map_critical_care_admission`. '
                                                                    'Source precision and timezone handling '
                                                                    'follow the pipeline expression; null '
                                                                    'means the time was unavailable.',
                                                  'event_datetime_local': 'Local-time representation of the '
                                                                          'event start datetime for the '
                                                                          'critical-care admission.',
                                                  'event_end_datetime': 'Date and time when the represented '
                                                                        'clinical or administrative event '
                                                                        'ended for each critical care '
                                                                        'admission record. It is derived '
                                                                        'from bronze field '
                                                                        '`date_unit_discharge_CLEAN` in '
                                                                        '`4_prod.bronze.map_critical_care_admission`. '
                                                                        'Source precision and timezone '
                                                                        'handling follow the pipeline '
                                                                        'expression; null means the time was '
                                                                        'unavailable.',
                                                  'event_end_datetime_local': 'Local-time representation of '
                                                                              'the event end datetime for '
                                                                              'the critical-care admission. '
                                                                              'Gold: NULL wherever Gold '
                                                                              'nulls event_end_datetime.',
                                                  'fact_category': 'Contract category used to group the '
                                                                   'record with comparable clinical facts '
                                                                   'for each critical care admission record. '
                                                                   'It is produced by the silver '
                                                                   'transformation and has no direct '
                                                                   'bronze-column lineage entry. Whitespace '
                                                                   'and source sentinel text are retained '
                                                                   'unless the pipeline explicitly '
                                                                   'normalizes them; null means no value was '
                                                                   'supplied.',
                                                  'hospital_admission_datetime': 'Date and time associated '
                                                                                 'with hospital admission '
                                                                                 'for the critical care '
                                                                                 'admission record. It is '
                                                                                 'derived from bronze field '
                                                                                 '`date_hospital_adm_CLEAN` '
                                                                                 'in '
                                                                                 '`4_prod.bronze.map_critical_care_admission`. '
                                                                                 'Source precision and '
                                                                                 'timezone handling follow '
                                                                                 'the pipeline expression; '
                                                                                 'null means the time was '
                                                                                 'unavailable.',
                                                  'hospital_discharge_datetime': 'Date and time associated '
                                                                                 'with hospital discharge '
                                                                                 'for the critical care '
                                                                                 'admission record. It is '
                                                                                 'derived from bronze field '
                                                                                 '`date_hospital_discharge_CLEAN` '
                                                                                 'in '
                                                                                 '`4_prod.bronze.map_critical_care_admission`. '
                                                                                 'Source precision and '
                                                                                 'timezone handling follow '
                                                                                 'the pipeline expression; '
                                                                                 'null means the time was '
                                                                                 'unavailable.',
                                                  'loaded_at': 'map_critical_care_admission.ADC_UPDT carried '
                                                               'unchanged for the contributing Medicus '
                                                               'admission row. This is bronze ingestion '
                                                               'provenance, distinct from '
                                                               'updated_at/source_update_timestamp and the '
                                                               'admission/discharge dates; it is not Silver '
                                                               'refresh time.',
                                                  'max_organ_support': 'Numeric measure of max organ support '
                                                                       'for the critical care admission '
                                                                       'record. It is derived from bronze '
                                                                       'field `maxorgansupp` in '
                                                                       '`4_prod.bronze.map_critical_care_admission`. '
                                                                       'Units and source sentinel values '
                                                                       'follow the named source field unless '
                                                                       'the pipeline explicitly converts '
                                                                       'them; null is not equivalent to '
                                                                       'zero.',
                                                  'patient_event_key': 'Deterministic event key derived from '
                                                                       'source identifiers; retained as a '
                                                                       'stable join key.',
                                                  'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                               'resolved.',
                                                  'person_link_status': 'Processing or clinical status of '
                                                                        'person link for the critical care '
                                                                        'admission record. It is carried '
                                                                        'from bronze field '
                                                                        '`PERSON_LINK_STATUS` in '
                                                                        '`4_prod.bronze.map_critical_care_admission`. '
                                                                        'Whitespace and source sentinel text '
                                                                        'are retained unless the pipeline '
                                                                        'explicitly normalizes them; null '
                                                                        'means no value was supplied.',
                                                  'record_status': 'Retracted when '
                                                                   'map_critical_care_admission.SOURCE_PRESENT_IND '
                                                                   'is false, otherwise active; a missing '
                                                                   'presence flag defaults true. No '
                                                                   'superseded status is emitted, and '
                                                                   'hospital/unit discharge or death dates '
                                                                   'do not determine this label.',
                                                  'record_status_effective_from': 'map_critical_care_admission.date_unit_adm_CLEAN '
                                                                                  'carried unchanged as the '
                                                                                  'history start. This '
                                                                                  'unit-admission time is a '
                                                                                  'proxy, not an '
                                                                                  'independently observed '
                                                                                  'status transition; no '
                                                                                  'hospital-admission or '
                                                                                  'ingestion fallback is '
                                                                                  'applied.',
                                                  'record_status_effective_to': 'map_critical_care_admission.ADC_UPDT '
                                                                                'only when the admission row '
                                                                                'is retracted; otherwise '
                                                                                'null. This is an '
                                                                                'ingestion-time end proxy, '
                                                                                'not unit/hospital discharge '
                                                                                'or death time; a missing '
                                                                                'ADC_UPDT remains null.',
                                                  'source_code': 'Code supplied by the originating source '
                                                                 'system for the represented concept for '
                                                                 'each critical care admission record. It is '
                                                                 'produced by the silver transformation and '
                                                                 'has no direct bronze-column lineage entry. '
                                                                 'Source code meanings and sentinel values '
                                                                 'are retained unless the pipeline '
                                                                 'explicitly maps them; null means no code '
                                                                 'was supplied.',
                                                  'source_coding_system': 'Coding system or source namespace '
                                                                          'in which the source code is '
                                                                          'defined for each critical care '
                                                                          'admission record. It is produced '
                                                                          'by the silver transformation and '
                                                                          'has no direct bronze-column '
                                                                          'lineage entry. Source code '
                                                                          'meanings and sentinel values are '
                                                                          'retained unless the pipeline '
                                                                          'explicitly maps them; null means '
                                                                          'no code was supplied.',
                                                  'source_display': 'Human-readable label supplied by the '
                                                                    'source system for the source code for '
                                                                    'each critical care admission record. It '
                                                                    'is produced by the silver '
                                                                    'transformation and has no direct '
                                                                    'bronze-column lineage entry. Whitespace '
                                                                    'and source sentinel text are retained '
                                                                    'unless the pipeline explicitly '
                                                                    'normalizes them; null means no value '
                                                                    'was supplied.',
                                                  'source_feed': 'Originating data feed responsible for the '
                                                                 'record for each critical care admission '
                                                                 'record. It is produced by the silver '
                                                                 'transformation and has no direct '
                                                                 'bronze-column lineage entry. Whitespace '
                                                                 'and source sentinel text are retained '
                                                                 'unless the pipeline explicitly normalizes '
                                                                 'them; null means no value was supplied.',
                                                  'source_pat_id': 'Native Medicus patient/admission-row id '
                                                                   'from SOURCE_PAT_ID, exposed as BIGINT. '
                                                                   'Together with source_unit it identifies '
                                                                   'the source admission row; not a '
                                                                   'Millennium person identifier.',
                                                  'source_site': 'Value describing source site for the '
                                                                 'critical care admission record. It is '
                                                                 'carried from bronze field `SOURCE_SITE` in '
                                                                 '`4_prod.bronze.map_critical_care_admission`. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                                  'source_unit': 'Value describing source unit for the '
                                                                 'critical care admission record. It is '
                                                                 'carried from bronze field `SOURCE_UNIT` in '
                                                                 '`4_prod.bronze.map_critical_care_admission`. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                                  'source_update_timestamp': 'Native source-system update '
                                                                             'timestamp carried through the '
                                                                             'research transformation for '
                                                                             'each critical care admission '
                                                                             'record. It is derived from '
                                                                             'bronze field `updated_at` in '
                                                                             '`4_prod.bronze.map_critical_care_admission`. '
                                                                             'Source precision and timezone '
                                                                             'handling follow the pipeline '
                                                                             'expression; null means the '
                                                                             'time was unavailable.',
                                                  'source_updated_at': 'Date or time value for source '
                                                                       'updated at on the critical care '
                                                                       'admission record. It is derived from '
                                                                       'bronze field `updated_at` in '
                                                                       '`4_prod.bronze.map_critical_care_admission`. '
                                                                       'Source precision and timezone '
                                                                       'handling follow the pipeline '
                                                                       'expression; null means the time was '
                                                                       'unavailable.',
                                                  'subject_id_system': 'Identifier namespace used to '
                                                                       'interpret the subject identifier on '
                                                                       'this record for each critical care '
                                                                       'admission record. It is derived from '
                                                                       'bronze field `PERSON_ID` in '
                                                                       '`4_prod.bronze.map_critical_care_admission`. '
                                                                       'Its uniqueness is limited to the '
                                                                       'originating source namespace; null '
                                                                       'means no identifier was supplied.',
                                                  'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                 'source identifier. Not salted, not secret; '
                                                                 'a stable join key across feeds where '
                                                                 'person_id is unresolved.',
                                                  'unit_admission_datetime': 'Date and time associated with '
                                                                             'unit admission for the '
                                                                             'critical care admission '
                                                                             'record. It is derived from '
                                                                             'bronze field '
                                                                             '`date_unit_adm_CLEAN` in '
                                                                             '`4_prod.bronze.map_critical_care_admission`. '
                                                                             'Source precision and timezone '
                                                                             'handling follow the pipeline '
                                                                             'expression; null means the '
                                                                             'time was unavailable.',
                                                  'unit_discharge_datetime': 'Date and time associated with '
                                                                             'unit discharge for the '
                                                                             'critical care admission '
                                                                             'record. It is derived from '
                                                                             'bronze field '
                                                                             '`date_unit_discharge_CLEAN` in '
                                                                             '`4_prod.bronze.map_critical_care_admission`. '
                                                                             'Source precision and timezone '
                                                                             'handling follow the pipeline '
                                                                             'expression; null means the '
                                                                             'time was unavailable.',
                                                  'unit_raw': 'Value describing unit raw for the critical '
                                                              'care admission record. It is carried from '
                                                              'bronze field `UNIT_RAW` in '
                                                              '`4_prod.bronze.map_critical_care_admission`. '
                                                              'Whitespace and source sentinel text are '
                                                              'retained unless the pipeline explicitly '
                                                              'normalizes them; null means no value was '
                                                              'supplied.',
                                                  'vip_ind': 'Indicator that the source record carries VIP '
                                                             'handling status for each critical care '
                                                             'admission record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Null means the source did not '
                                                             'state the indicator and must not be '
                                                             'interpreted as false.',
                                                  'withheld_identity_ind': 'Indicator that the source '
                                                                           'identity was deliberately '
                                                                           'withheld for each critical care '
                                                                           'admission record. It is produced '
                                                                           'by the silver transformation and '
                                                                           'has no direct bronze-column '
                                                                           'lineage entry. Null means the '
                                                                           'source did not state the '
                                                                           'indicator and must not be '
                                                                           'interpreted as false.'},
                                      'quantity_override': False,
                                      'table': 'One Medicus critical-care admission per source admission '
                                               'key. Gold QC twin of the silver product: 1 columns are '
                                               'repaired or nulled, 1 rule(s) drop rows, 4 check(s) are '
                                               'advisory. Each rule states its reason in the pipeline '
                                               'notebook, and Lakeflow expectation metrics report what every '
                                               'rule matched on each update.'}}
