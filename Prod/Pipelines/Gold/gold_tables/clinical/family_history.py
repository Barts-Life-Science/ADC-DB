"""Gold clinical.family_history: 2 dataset(s).

- datasets: _qc_clinical_family_history, clinical_family_history
- moved verbatim from: 2_gold_clinical_events:1613, 2_gold_clinical_events:1650, 2_gold_clinical_events:1657, 2_gold_clinical_events:1714, 2_gold_clinical_events:1718, 2_gold_clinical_events:1731

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.family_history ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_FAMILY_HISTORY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`fhx_activity_id` AS `fhx_activity_id`',
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
    '`condition_code` AS `condition_code`',
    '`relationship_code` AS `relationship_code`',
    '`relationship_display` AS `relationship_display`',
    '`relationship_type_code` AS `relationship_type_code`',
    '`relationship_type_display` AS `relationship_type_display`',
    '`onset_age` AS `onset_age`',
    '`onset_age_unit` AS `onset_age_unit`',
    "CASE WHEN UPPER(TRIM(CAST(`severity_code` AS STRING))) = '0' THEN NULL ELSE `severity_code` END AS `severity_code`",
    '`severity_display` AS `severity_display`',
    '`source_lifecycle_status` AS `source_lifecycle_status`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`asserter_practitioner_id` AS `asserter_practitioner_id`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_family_history_metadata; source history stays on the main research table.
CLINICAL_FAMILY_HISTORY_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 10,640 rows of 10,640
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_FAMILY_HISTORY_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.family_history.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 10,640 at the profile.
    "gold.clinical.family_history.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 21 of 10,640 rows (0.197%) when profiled on 2026-08-24.
    "gold.clinical.family_history.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_FAMILY_HISTORY_PERSON_SCOPE_RULES = {
    "gold.clinical.family_history.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_family_history"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_FAMILY_HISTORY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_FAMILY_HISTORY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_FAMILY_HISTORY_PERSON_SCOPE_RULES)
def _gold_qc_clinical_family_history():
    """Quality-controlled twin of journey_clinical.family_history."""
    df = _qc("clinical_family_history", CLINICAL_FAMILY_HISTORY_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.family_history"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_family_history():
    """Contract-v2 public twin of clinical_family_history; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_family_history")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`fhx_activity_id` AS `fhx_activity_id`',
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
        '`__gold_json_condition_code` AS `condition_code`',
        '`relationship_code` AS `relationship_code`',
        '`relationship_display` AS `relationship_display`',
        '`relationship_type_code` AS `relationship_type_code`',
        '`relationship_type_display` AS `relationship_type_display`',
        '`onset_age` AS `onset_age`',
        '`onset_age_unit` AS `onset_age_unit`',
        '`severity_code` AS `severity_code`',
        '`severity_display` AS `severity_display`',
        '`source_lifecycle_status` AS `source_lifecycle_status`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`asserter_practitioner_id` AS `asserter_practitioner_id`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _s3_flatten_gold_public(df, "clinical_family_history")


def _public_steps_clinical_family_history(df):
    """clinical_family_history: the public body after its projection (GoldRules.public_select), moved from gold_clinical_family_history; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_family_history')


RULES = {
    'clinical_family_history': GoldRules(
        table='clinical_family_history',
        select=CLINICAL_FAMILY_HISTORY_SELECT,
        mandatory=CLINICAL_FAMILY_HISTORY_MANDATORY_RULES,
        advisory=CLINICAL_FAMILY_HISTORY_ADVISORY_RULES,
        person_scope=CLINICAL_FAMILY_HISTORY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d',),
        variant_columns=('condition_code',),
        s3_axis_specs={'condition_code': [('condition', False)]},
        value_rules={'gold.clinical.family_history.condition_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                      'column': 'condition_snomed_code',
                                                                                      'companions': ['condition_snomed_display',
                                                                                                     'condition_target_code',
                                                                                                     'condition_target_display'],
                                                                                      'match': '`condition_snomed_code` '
                                                                                               'IS NOT '
                                                                                               'NULL AND '
                                                                                               '(NOT '
                                                                                               '`condition_snomed_code` '
                                                                                               'RLIKE '
                                                                                               "'^[1-9][0-9]{5,17}$' "
                                                                                               'OR '
                                                                                               'substring(`condition_snomed_code`, '
                                                                                               'length(`condition_snomed_code`) '
                                                                                               '- 2, 2) '
                                                                                               'NOT IN '
                                                                                               "('00', "
                                                                                               "'10') OR "
                                                                                               '(substring(`condition_snomed_code`, '
                                                                                               'length(`condition_snomed_code`) '
                                                                                               '- 2, 2) = '
                                                                                               "'10' AND "
                                                                                               'length(`condition_snomed_code`) '
                                                                                               '< 11))',
                                                                                      'table': 'clinical_family_history'},
         'gold.clinical.family_history.condition_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                      'column': 'condition_target_code',
                                                                                      'companions': ['condition_target_display',
                                                                                                     'condition_snomed_code',
                                                                                                     'condition_snomed_display'],
                                                                                      'match': '`condition_target_system` '
                                                                                               '= '
                                                                                               "'http://snomed.info/sct' "
                                                                                               'AND '
                                                                                               '`condition_target_code` '
                                                                                               'IS NOT '
                                                                                               'NULL AND '
                                                                                               '(NOT '
                                                                                               '`condition_target_code` '
                                                                                               'RLIKE '
                                                                                               "'^[1-9][0-9]{5,17}$' "
                                                                                               'OR '
                                                                                               'substring(`condition_target_code`, '
                                                                                               'length(`condition_target_code`) '
                                                                                               '- 2, 2) '
                                                                                               'NOT IN '
                                                                                               "('00', "
                                                                                               "'10') OR "
                                                                                               '(substring(`condition_target_code`, '
                                                                                               'length(`condition_target_code`) '
                                                                                               '- 2, 2) = '
                                                                                               "'10' AND "
                                                                                               'length(`condition_target_code`) '
                                                                                               '< 11))',
                                                                                      'table': 'clinical_family_history'},
         'gold.clinical.family_history.encounter_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                       'column': 'encounter_id',
                                                                       'companions': [],
                                                                       'match': '`encounter_id` = 0',
                                                                       'table': 'clinical_family_history'},
         'gold.clinical.family_history.onset_age.insr_f8_zero_without_unit': {'action': 'invalidate',
                                                                              'column': 'onset_age',
                                                                              'companions': [],
                                                                              'match': '`onset_age` = 0 '
                                                                                       'AND '
                                                                                       '`onset_age_unit` '
                                                                                       'IS NULL',
                                                                              'table': 'clinical_family_history'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`source_patient_event_key` AS `source_patient_event_key`',
            '`fhx_activity_id` AS `fhx_activity_id`',
            '`subject_key` AS `subject_key`',
            '`subject_id_system` AS `subject_id_system`',
            '`person_id` AS `person_id`',
            '`encounter_id` AS `encounter_id`',
            '`event_datetime` AS `event_datetime`',
            '`event_end_datetime` AS `event_end_datetime`',
            '`event_datetime_local` AS `event_datetime_local`',
            '`event_end_datetime_local` AS `event_end_datetime_local`',
            '`source_coding_system` AS `source_coding_system`',
            '`source_code` AS `source_code`',
            '`source_display` AS `source_display`',
            '`__gold_json_condition_code` AS `condition_code`',
            '`relationship_code` AS `relationship_code`',
            '`relationship_display` AS `relationship_display`',
            '`relationship_type_code` AS `relationship_type_code`',
            '`relationship_type_display` AS `relationship_type_display`',
            '`onset_age` AS `onset_age`',
            '`onset_age_unit` AS `onset_age_unit`',
            '`severity_code` AS `severity_code`',
            '`severity_display` AS `severity_display`',
            '`source_lifecycle_status` AS `source_lifecycle_status`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`asserter_practitioner_id` AS `asserter_practitioner_id`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
        ],
        public_steps=(_public_steps_clinical_family_history,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_family_history': {'columns': {'asserter_practitioner_id': 'Millennium personnel PERSON_ID '
                                                                         'when supplied.',
                                             'condition_code': 'Source and mapped condition codings as a '
                                                               'one-level CodeableConcept VARIANT.',
                                             'confidentiality_code': 'Security classification when supplied.',
                                             'encounter_id': 'Millennium ORIGINATING_ENCNTR_ID; joins to '
                                                             'spine_encounter.encounter_id.',
                                             'event_datetime': 'Source family-history effective start.',
                                             'event_datetime_local': 'Local-time rendering of the source '
                                                                     'family-history effective start, '
                                                                     'corresponding to event_datetime.',
                                             'event_end_datetime': 'Source effective end when the assertion '
                                                                   'is no longer current.',
                                             'event_end_datetime_local': 'Local-time rendering of the source '
                                                                         'effective end when the '
                                                                         'family-history assertion is no '
                                                                         'longer current, corresponding to '
                                                                         'event_end_datetime.',
                                             'fhx_activity_id': 'Millennium FHX_ACTIVITY_ID; primary key of '
                                                                'this table.',
                                             'loaded_at': 'map_family_history.ADC_UPDT carried unchanged '
                                                          'through usable-code filtering and QC/public '
                                                          'projections, identical to '
                                                          'source_update_timestamp. QC identity/encounter '
                                                          'joins add no load clocks; neither clinical event '
                                                          'time nor the current Silver refresh time replaces '
                                                          'it.',
                                             'onset_age': 'Source recorded age at onset.',
                                             'onset_age_unit': 'Unit for recorded onset age.',
                                             'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                  'stable cross-feed join key.',
                                             'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                             'record_status': 'Superseded when '
                                                              'map_family_history.END_EFFECTIVE_DT_TM is '
                                                              'non-null and earlier than 2100-01-01; '
                                                              'otherwise active. No retracted status is '
                                                              'emitted. The separately retained '
                                                              'LIFE_CYCLE_STATUS does not determine this '
                                                              'derived label.',
                                             'record_status_effective_from': 'Source-supplied timestamp from '
                                                                             'which the normalized record '
                                                                             'status applies for each family '
                                                                             'history record. It is derived '
                                                                             'from bronze field '
                                                                             '`FHX_ACTIVE_STATUS_DT_TM` in '
                                                                             '`4_prod.bronze.map_family_history`. '
                                                                             'Source precision and timezone '
                                                                             'handling follow the pipeline '
                                                                             'expression; null means the '
                                                                             'time was unavailable.',
                                             'record_status_effective_to': 'map_family_history.END_EFFECTIVE_DT_TM '
                                                                           'only when non-null and earlier '
                                                                           'than 2100-01-01; otherwise null. '
                                                                           'This is the same predicate that '
                                                                           'sets superseded status. No 1950 '
                                                                           'lower clamp, event-end '
                                                                           'substitution or ingestion-time '
                                                                           'fallback is applied.',
                                             'relationship_code': 'Source family relationship code.',
                                             'relationship_display': 'Source family relationship display.',
                                             'relationship_type_code': 'Source relationship record type '
                                                                       'code.',
                                             'relationship_type_display': 'Source relationship record type '
                                                                          'display.',
                                             'severity_code': 'Source severity code.',
                                             'severity_display': 'Source severity display.',
                                             'source_code': 'Source condition code',
                                             'source_coding_system': 'Verbatim source code system.',
                                             'source_display': 'Verbatim source condition display.',
                                             'source_lifecycle_status': 'Verbatim source life-cycle status.',
                                             'source_patient_event_key': 'Stable patient-event identity '
                                                                         'retained from the originating '
                                                                         'Silver row before source '
                                                                         'splitting.',
                                             'source_update_timestamp': 'map_family_history.ADC_UPDT carried '
                                                                        'unchanged, identical to loaded_at. '
                                                                        'No independent native '
                                                                        'application-update clock is '
                                                                        'projected for this field, and the '
                                                                        'current Silver refresh time is not '
                                                                        'substituted.',
                                             'subject_id_system': 'Identifier system used for subject_key.',
                                             'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                            'identifier. Not salted, not secret; a stable '
                                                            'join key across feeds where person_id is '
                                                            'unresolved.',
                                             'vip_ind': 'VIP indicator when supplied.',
                                             'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                      'supplied.'},
                                 'quantity_override': True,
                                 'table': 'Internal quality-controlled twin of clinical_family_history: the '
                                          'Gold repairs, nulls and expectations are applied here and the '
                                          'published Gold MV reads this object. Materialized rather than '
                                          'temporary so the published MV can refresh incrementally.'},
 'clinical_family_history': {'columns': {'asserter_practitioner_id': 'Millennium personnel PERSON_ID when '
                                                                     'supplied.',
                                         'condition_code': 'Source and mapped condition codings as a '
                                                           'one-level CodeableConcept VARIANT.',
                                         'confidentiality_code': 'Security classification when supplied.',
                                         'encounter_id': 'Millennium ORIGINATING_ENCNTR_ID; joins to '
                                                         'spine_encounter.encounter_id.',
                                         'event_datetime': 'Source family-history effective start.',
                                         'event_datetime_local': 'Local-time rendering of the source '
                                                                 'family-history effective start, '
                                                                 'corresponding to event_datetime.',
                                         'event_end_datetime': 'Source effective end when the assertion is '
                                                               'no longer current.',
                                         'event_end_datetime_local': 'Local-time rendering of the source '
                                                                     'effective end when the family-history '
                                                                     'assertion is no longer current, '
                                                                     'corresponding to event_end_datetime.',
                                         'fhx_activity_id': 'Millennium FHX_ACTIVITY_ID; primary key of this '
                                                            'table.',
                                         'loaded_at': 'map_family_history.ADC_UPDT carried unchanged through '
                                                      'usable-code filtering and QC/public projections, '
                                                      'identical to source_update_timestamp. QC '
                                                      'identity/encounter joins add no load clocks; neither '
                                                      'clinical event time nor the current Silver refresh '
                                                      'time replaces it.',
                                         'onset_age': 'Source recorded age at onset.',
                                         'onset_age_unit': 'Unit for recorded onset age.',
                                         'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                              'cross-feed join key.',
                                         'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                         'record_status': 'Superseded when '
                                                          'map_family_history.END_EFFECTIVE_DT_TM is '
                                                          'non-null and earlier than 2100-01-01; otherwise '
                                                          'active. No retracted status is emitted. The '
                                                          'separately retained LIFE_CYCLE_STATUS does not '
                                                          'determine this derived label.',
                                         'record_status_effective_from': 'Source-supplied timestamp from '
                                                                         'which the normalized record status '
                                                                         'applies for each family history '
                                                                         'record. It is derived from bronze '
                                                                         'field `FHX_ACTIVE_STATUS_DT_TM` in '
                                                                         '`4_prod.bronze.map_family_history`. '
                                                                         'Source precision and timezone '
                                                                         'handling follow the pipeline '
                                                                         'expression; null means the time '
                                                                         'was unavailable.',
                                         'record_status_effective_to': 'map_family_history.END_EFFECTIVE_DT_TM '
                                                                       'only when non-null and earlier than '
                                                                       '2100-01-01; otherwise null. This is '
                                                                       'the same predicate that sets '
                                                                       'superseded status. No 1950 lower '
                                                                       'clamp, event-end substitution or '
                                                                       'ingestion-time fallback is applied.',
                                         'relationship_code': 'Source family relationship code.',
                                         'relationship_display': 'Source family relationship display.',
                                         'relationship_type_code': 'Source relationship record type code.',
                                         'relationship_type_display': 'Source relationship record type '
                                                                      'display.',
                                         'severity_code': 'Source severity code.',
                                         'severity_display': 'Source severity display.',
                                         'source_code': 'Source condition code',
                                         'source_coding_system': 'Verbatim source code system.',
                                         'source_display': 'Verbatim source condition display.',
                                         'source_lifecycle_status': 'Verbatim source life-cycle status.',
                                         'source_patient_event_key': 'Stable patient-event identity retained '
                                                                     'from the originating Silver row before '
                                                                     'source splitting.',
                                         'source_update_timestamp': 'map_family_history.ADC_UPDT carried '
                                                                    'unchanged, identical to loaded_at. No '
                                                                    'independent native application-update '
                                                                    'clock is projected for this field, and '
                                                                    'the current Silver refresh time is not '
                                                                    'substituted.',
                                         'subject_id_system': 'Identifier system used for subject_key.',
                                         'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                        'identifier. Not salted, not secret; a stable join '
                                                        'key across feeds where person_id is unresolved.',
                                         'vip_ind': 'VIP indicator when supplied.',
                                         'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                  'supplied.'},
                             'quantity_override': False,
                             'table': 'One family-history assertion with the standard event block and '
                                      'retained source lifecycle. Gold QC twin of the silver product: 2 '
                                      'columns are repaired or nulled, 1 rule(s) drop rows, 2 check(s) are '
                                      'advisory. Each rule states its reason in the pipeline notebook, and '
                                      'Lakeflow expectation metrics report what every rule matched on each '
                                      'update. Coverage: Family history stopped landing in the raw layer on '
                                      '2024-09-23 (Millennium family-history activity, last source update '
                                      '2024-09-10); re-landing is outside this release and the F2 freshness '
                                      'guard watches for its return; complete-through month per release: '
                                      '6_mgmt.data_quality.release_finding (finding_code '
                                      'F2_complete_through).'}}
