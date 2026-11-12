"""Gold spine.person_death_evidence: 2 dataset(s).

- datasets: _qc_reference_person_death_evidence, reference_person_death_evidence
- moved verbatim from: 1_gold_spine_and_reference:3666, 1_gold_spine_and_reference:3690, 1_gold_spine_and_reference:3697, 1_gold_spine_and_reference:3710, 1_gold_spine_and_reference:3751, 1_gold_spine_and_reference:3756, 1_gold_spine_and_reference:3760, 1_gold_spine_and_reference:3778

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.person_death_evidence ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PERSON_DEATH_EVIDENCE_SELECT = [
    '`person_death_evidence_key` AS `person_death_evidence_key`',
    '`person_id` AS `person_id`',
    '`deceased_datetime_raw` AS `deceased_datetime_raw`',
    '`deceased_datetime` AS `deceased_datetime`',
    '`calculated_death_date` AS `calculated_death_date`',
    '`precision_flag` AS `precision_flag`',
    '`precision_desc` AS `precision_desc`',
    '`source_desc` AS `source_desc`',
    '`method_desc` AS `method_desc`',
    '`death_date_estimate_source` AS `death_date_estimate_source`',
    '`cause_of_death` AS `cause_of_death`',
    '`autopsy_desc` AS `autopsy_desc`',
    '`age_at_death` AS `age_at_death`',
    "CASE WHEN CAST(`last_encounter_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`last_encounter_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`last_encounter_datetime` AS DATE)) < 9999 THEN NULL ELSE `last_encounter_datetime` END END AS `last_encounter_datetime`",
    "CASE WHEN CAST(`last_clinical_event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`last_clinical_event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE CASE WHEN CAST(`last_clinical_event_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`last_clinical_event_datetime` AS DATE)) < 9999 THEN NULL ELSE `last_clinical_event_datetime` END END AS `last_clinical_event_datetime`",
    "CASE WHEN CAST(`last_known_activity_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`last_known_activity_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE CASE WHEN CAST(`last_known_activity_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`last_known_activity_datetime` AS DATE)) < 9999 THEN NULL ELSE `last_known_activity_datetime` END END AS `last_known_activity_datetime`",
    '`clinical_event_count` AS `clinical_event_count`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]


REFERENCE_PERSON_DEATH_EVIDENCE_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`age_at_death_raw` AS `age_at_death_raw`',
    '`last_clinical_event_datetime_derived` AS `last_clinical_event_datetime_derived`',
    '`post_death_clinical_activity_ind` AS `post_death_clinical_activity_ind`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_PERSON_DEATH_EVIDENCE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 584,657 rows of 584,657 that
    # are current and attributable. Superseded versions and rows whose identity was never
    # resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",

    # The flow nulls person_id when it names a parent the spine does not have, and this rule
    # then drops the row, because evidence that someone died is unusable without knowing
    # who. That was 226 orphaned rows plus 0 that already had no person_id, out of 584,657.
    "gold.reference.person_death_evidence.person_id.fk_containment":
        "`person_id` IS NOT NULL",
}


REFERENCE_PERSON_DEATH_EVIDENCE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 584,657 at the profile.
    "gold.reference.person_death_evidence.record_status.default_view_active":
        "record_status = 'active'",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
REFERENCE_PERSON_DEATH_EVIDENCE_PERSON_SCOPE_RULES = {
    "gold.reference.person_death_evidence.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
REFERENCE_PERSON_DEATH_EVIDENCE_ORPHAN_RULES = {
    "gold.reference.person_death_evidence.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_person_death_evidence"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_PERSON_DEATH_EVIDENCE_MANDATORY_RULES)
@dp.expect_all(REFERENCE_PERSON_DEATH_EVIDENCE_ADVISORY_RULES)
@dp.expect_all_or_drop(REFERENCE_PERSON_DEATH_EVIDENCE_PERSON_SCOPE_RULES)
@dp.expect_all(REFERENCE_PERSON_DEATH_EVIDENCE_ORPHAN_RULES)
def _gold_qc_reference_person_death_evidence():
    """Quality-controlled twin of journey_reference.person_death_evidence."""
    df = _qc(
        "reference_person_death_evidence",
        REFERENCE_PERSON_DEATH_EVIDENCE_SELECT,
        fk_columns=["person_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_reference.person_death_evidence"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_person_death_evidence():
    """Contract-v2 public twin of reference_person_death_evidence; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_person_death_evidence")).selectExpr(
        '`person_death_evidence_key` AS `person_death_evidence_key`',
        '`person_id` AS `person_id`',
        '`deceased_datetime_raw` AS `deceased_datetime_raw`',
        '`deceased_datetime` AS `deceased_datetime`',
        '`calculated_death_date` AS `calculated_death_date`',
        '`precision_flag` AS `precision_flag`',
        '`precision_desc` AS `precision_desc`',
        '`source_desc` AS `source_desc`',
        '`method_desc` AS `method_desc`',
        '`death_date_estimate_source` AS `death_date_estimate_source`',
        '`cause_of_death` AS `cause_of_death`',
        '`autopsy_desc` AS `autopsy_desc`',
        '`age_at_death` AS `age_at_death`',
        '`last_encounter_datetime` AS `last_encounter_datetime`',
        '`last_clinical_event_datetime` AS `last_clinical_event_datetime`',
        '`last_known_activity_datetime` AS `last_known_activity_datetime`',
        '`clinical_event_count` AS `clinical_event_count`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
        '`age_at_death_raw` AS `age_at_death_raw`',  # INSR_W3 W3.12
        '`last_clinical_event_datetime_derived` AS `last_clinical_event_datetime_derived`',  # INSR_W3 W3.12
        '`post_death_clinical_activity_ind` AS `post_death_clinical_activity_ind`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'reference_person_death_evidence': GoldRules(
        table='reference_person_death_evidence',
        select=REFERENCE_PERSON_DEATH_EVIDENCE_SELECT,
        mandatory=REFERENCE_PERSON_DEATH_EVIDENCE_MANDATORY_RULES,
        advisory=REFERENCE_PERSON_DEATH_EVIDENCE_ADVISORY_RULES,
        person_scope=REFERENCE_PERSON_DEATH_EVIDENCE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['person_death_evidence_key'], 'support_columns': ['record_status', 'loaded_at']},
        value_rules={'gold.reference.person_death_evidence.age_at_death.insr_f8_zero_placeholder': {'action': 'invalidate',
                                                                                        'column': 'age_at_death',
                                                                                        'companions': [],
                                                                                        'match': '`age_at_death` '
                                                                                                 '= 0',
                                                                                        'table': 'reference_person_death_evidence'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_person_death_evidence': {'columns': {'_source_row_id': 'This is the value of the unique '
                                                                       'primary identifier of the person '
                                                                       'table. It is an internal system '
                                                                       'assigned number.',
                                                     '_source_system': 'Value describing source system for '
                                                                       'the person death evidence record. It '
                                                                       'is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. '
                                                                       'Whitespace and source sentinel text '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly normalizes them; null '
                                                                       'means no value was supplied.',
                                                     '_source_table': 'Value describing source table for the '
                                                                      'person death evidence record. It is '
                                                                      'produced by the silver transformation '
                                                                      'and has no direct bronze-column '
                                                                      'lineage entry. Whitespace and source '
                                                                      'sentinel text are retained unless the '
                                                                      'pipeline explicitly normalizes them; '
                                                                      'null means no value was supplied.',
                                                     'age_at_death': 'Raw reported age at death; source zero '
                                                                     'values are retained.',
                                                     'age_at_death_raw': 'Source AGE_AT_DEATH exactly as '
                                                                         'supplied (constant 0 at source, '
                                                                         'P7_L5145).',
                                                     'autopsy_desc': 'Resolved autopsy code '
                                                                     'description/display.',
                                                     'calculated_death_date': 'Calendar date associated with '
                                                                              'calculated death for the '
                                                                              'person death evidence record. '
                                                                              'It is derived from bronze '
                                                                              'field `CALC_DEATH_DATE` in '
                                                                              '`4_prod.bronze.map_death`. '
                                                                              'The value has no time-of-day '
                                                                              'component; null means the '
                                                                              'source date was unavailable.',
                                                     'cause_of_death': 'Raw free-text cause of death.',
                                                     'clinical_event_count': 'Count of selected latest '
                                                                             'logical clinical-event source '
                                                                             'states for the person.',
                                                     'death_date_estimate_source': 'RECORDED_DEATH, '
                                                                                   'LAST_ENCOUNTER, '
                                                                                   'LAST_CLINICAL_EVENT or '
                                                                                   'NONE.',
                                                     'deceased_datetime': 'Date and time of death.',
                                                     'deceased_datetime_raw': 'Date and time of death.',
                                                     'last_clinical_event_datetime': 'Latest EVENT_END_DT_TM '
                                                                                     'across selected latest '
                                                                                     'source versions, '
                                                                                     'without result-status '
                                                                                     'filtering.',
                                                     'last_clinical_event_datetime_derived': 'Latest '
                                                                                             'clinical-category '
                                                                                             'event across '
                                                                                             'the silver '
                                                                                             'event feeds '
                                                                                             '(person_scope); '
                                                                                             'excludes the '
                                                                                             'administrative '
                                                                                             'and bulk '
                                                                                             'events that '
                                                                                             'bronze '
                                                                                             'LAST_CLINICAL_EVENT_DT_TM '
                                                                                             'counts '
                                                                                             '(L5145). '
                                                                                             'INSR_W2.',
                                                     'last_encounter_datetime': 'It represents the timestamp '
                                                                                'of the last encounter '
                                                                                'associated with a deceased '
                                                                                'individual.',
                                                     'last_known_activity_datetime': 'Greatest of '
                                                                                     'LAST_ENCNTR_DT_TM and '
                                                                                     'LAST_NON_ERROR_CLINICAL_EVENT_DT_TM. '
                                                                                     'This is activity, not '
                                                                                     'a confirmed death '
                                                                                     'timestamp.',
                                                     'loaded_at': 'map_death.ADC_UPDT carried unchanged for '
                                                                  'the contributing death-evidence row. This '
                                                                  'is bronze load provenance, not '
                                                                  'recorded/calculated death time, '
                                                                  'last-known activity or Silver refresh '
                                                                  'time.',
                                                     'method_desc': 'Description of the code.',
                                                     'person_death_evidence_key': 'Deterministic SHA-256 key '
                                                                                  'for the person '
                                                                                  'death-evidence row.',
                                                     'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                                     'post_death_clinical_activity_ind': 'True when a '
                                                                                         'clinical-category '
                                                                                         'event falls more '
                                                                                         'than one day after '
                                                                                         'the death date. '
                                                                                         'INSR_W2.',
                                                     'precision_desc': 'Decoded death timestamp precision: '
                                                                       'unknown, full date/time, month, year '
                                                                       'or day.',
                                                     'precision_flag': 'Source precision flag for '
                                                                       'DECEASED_DT_TM.',
                                                     'record_status': 'Always active in this projection. The '
                                                                      'label is a literal, not derived from '
                                                                      'death evidence, source presence, '
                                                                      'clinical activity or effective dates; '
                                                                      'it does not assert that the person is '
                                                                      'alive.',
                                                     'source_desc': 'Description of the code.'},
                                         'quantity_override': True,
                                         'table': 'Internal quality-controlled twin of '
                                                  'reference_person_death_evidence: the Gold repairs, nulls '
                                                  'and expectations are applied here and the published Gold '
                                                  'MV reads this object. Materialized rather than temporary '
                                                  'so the published MV can refresh incrementally.'},
 'reference_person_death_evidence': {'columns': {'_source_row_id': 'This is the value of the unique primary '
                                                                   'identifier of the person table. It is an '
                                                                   'internal system assigned number.',
                                                 '_source_system': 'Value describing source system for the '
                                                                   'person death evidence record. It is '
                                                                   'produced by the silver transformation '
                                                                   'and has no direct bronze-column lineage '
                                                                   'entry. Whitespace and source sentinel '
                                                                   'text are retained unless the pipeline '
                                                                   'explicitly normalizes them; null means '
                                                                   'no value was supplied.',
                                                 '_source_table': 'Value describing source table for the '
                                                                  'person death evidence record. It is '
                                                                  'produced by the silver transformation and '
                                                                  'has no direct bronze-column lineage '
                                                                  'entry. Whitespace and source sentinel '
                                                                  'text are retained unless the pipeline '
                                                                  'explicitly normalizes them; null means no '
                                                                  'value was supplied.',
                                                 'age_at_death': 'Raw reported age at death; source zero '
                                                                 'values are retained.',
                                                 'age_at_death_raw': 'Source AGE_AT_DEATH exactly as '
                                                                     'supplied (constant 0 at source, '
                                                                     'P7_L5145).',
                                                 'autopsy_desc': 'Resolved autopsy code description/display.',
                                                 'calculated_death_date': 'Calendar date associated with '
                                                                          'calculated death for the person '
                                                                          'death evidence record. It is '
                                                                          'derived from bronze field '
                                                                          '`CALC_DEATH_DATE` in '
                                                                          '`4_prod.bronze.map_death`. The '
                                                                          'value has no time-of-day '
                                                                          'component; null means the source '
                                                                          'date was unavailable.',
                                                 'cause_of_death': 'Raw free-text cause of death.',
                                                 'clinical_event_count': 'Count of selected latest logical '
                                                                         'clinical-event source states for '
                                                                         'the person.',
                                                 'death_date_estimate_source': 'RECORDED_DEATH, '
                                                                               'LAST_ENCOUNTER, '
                                                                               'LAST_CLINICAL_EVENT or NONE.',
                                                 'deceased_datetime': 'Date and time of death.',
                                                 'deceased_datetime_raw': 'Date and time of death.',
                                                 'last_clinical_event_datetime': 'Latest EVENT_END_DT_TM '
                                                                                 'across selected latest '
                                                                                 'source versions, without '
                                                                                 'result-status filtering.',
                                                 'last_clinical_event_datetime_derived': 'Latest '
                                                                                         'clinical-category '
                                                                                         'event across the '
                                                                                         'silver event feeds '
                                                                                         '(person_scope); '
                                                                                         'excludes the '
                                                                                         'administrative and '
                                                                                         'bulk events that '
                                                                                         'bronze '
                                                                                         'LAST_CLINICAL_EVENT_DT_TM '
                                                                                         'counts (L5145). '
                                                                                         'INSR_W2.',
                                                 'last_encounter_datetime': 'It represents the timestamp of '
                                                                            'the last encounter associated '
                                                                            'with a deceased individual.',
                                                 'last_known_activity_datetime': 'Last known activity of the '
                                                                                 'person: Millennium '
                                                                                 'LAST_KNOWN_ACTIVITY_DT_TM '
                                                                                 'when present, else the '
                                                                                 'latest event date across '
                                                                                 'the event feeds '
                                                                                 '(reference_person_scope.last_activity_date); '
                                                                                 'last_known_activity_basis '
                                                                                 'says which.',
                                                 'loaded_at': 'map_death.ADC_UPDT carried unchanged for the '
                                                              'contributing death-evidence row. This is '
                                                              'bronze load provenance, not '
                                                              'recorded/calculated death time, last-known '
                                                              'activity or Silver refresh time.',
                                                 'method_desc': 'Description of the code.',
                                                 'person_death_evidence_key': 'Deterministic SHA-256 key for '
                                                                              'the person death-evidence '
                                                                              'row.',
                                                 'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                                 'post_death_clinical_activity_ind': 'True when a '
                                                                                     'clinical-category '
                                                                                     'event falls more than '
                                                                                     'one day after the '
                                                                                     'death date. INSR_W2.',
                                                 'precision_desc': 'Decoded death timestamp precision: '
                                                                   'unknown, full date/time, month, year or '
                                                                   'day.',
                                                 'precision_flag': 'Source precision flag for '
                                                                   'DECEASED_DT_TM.',
                                                 'record_status': 'Always active in this projection. The '
                                                                  'label is a literal, not derived from '
                                                                  'death evidence, source presence, clinical '
                                                                  'activity or effective dates; it does not '
                                                                  'assert that the person is alive.',
                                                 'source_desc': 'Description of the code.'},
                                     'quantity_override': False,
                                     'table': 'One corroborating death-evidence row; cause of death is '
                                              'IG-sensitive. Gold QC twin of the silver product: 4 columns '
                                              'are repaired or nulled, 2 rule(s) drop rows, 1 check(s) are '
                                              'advisory. Each rule states its reason in the pipeline '
                                              'notebook, and Lakeflow expectation metrics report what every '
                                              'rule matched on each update.'}}
