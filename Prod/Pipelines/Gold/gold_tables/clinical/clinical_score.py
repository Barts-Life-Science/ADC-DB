"""Gold clinical.clinical_score: 2 dataset(s).

- datasets: _qc_clinical_clinical_score, clinical_clinical_score
- moved verbatim from: 2_gold_clinical_events:485, 2_gold_clinical_events:527, 2_gold_clinical_events:535, 2_gold_clinical_events:543, 2_gold_clinical_events:630, 2_gold_clinical_events:634, 2_gold_clinical_events:647

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.clinical_score ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CLINICAL_SCORE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`event_id` AS `event_id`',
    '`doc_response_key` AS `doc_response_key`',
    '`source_dcp_forms_activity_id` AS `source_dcp_forms_activity_id`',
    '`source_object` AS `source_object`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    "CASE WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 AND timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) BETWEEN TIMESTAMP'1990-01-01 00:00:00' AND `loaded_at` THEN timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_datetime` END AS `event_datetime`",
    'CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    "CASE WHEN `event_datetime` IS NULL THEN `event_datetime_local` WHEN (CASE WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 AND timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) BETWEEN TIMESTAMP'1990-01-01 00:00:00' AND `loaded_at` THEN timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL WHEN CAST((CASE WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 AND timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) BETWEEN TIMESTAMP'1990-01-01 00:00:00' AND `loaded_at` THEN timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_datetime` END) AS TIMESTAMP) = CAST(`event_datetime` AS TIMESTAMP) THEN `event_datetime_local` WHEN `event_datetime_local` = from_utc_timestamp(`event_datetime`, 'Europe/London') THEN from_utc_timestamp((CASE WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 AND timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) BETWEEN TIMESTAMP'1990-01-01 00:00:00' AND `loaded_at` THEN timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_datetime` END), 'Europe/London') ELSE `event_datetime_local` + (CAST((CASE WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 AND timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) BETWEEN TIMESTAMP'1990-01-01 00:00:00' AND `loaded_at` THEN timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_datetime` END) AS TIMESTAMP) - CAST(`event_datetime` AS TIMESTAMP)) END AS `event_datetime_local`",  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`score_code` AS `score_code`',
    '`score_name` AS `score_name`',
    'CASE WHEN ABS(CAST(`value_number` AS DOUBLE)) > 1e12 OR `value_number` < 0 THEN NULL ELSE `value_number` END AS `value_number`',
    '`value_text` AS `value_text`',
    '`unit_source_value` AS `unit_source_value`',
    '`unit_concept_id` AS `unit_concept_id`',
    '`component_count` AS `component_count`',
    "CASE WHEN UPPER(TRIM(CAST(`interpretation_code` AS STRING))) = '0' THEN NULL ELSE `interpretation_code` END AS `interpretation_code`",
    '`interpretation_display` AS `interpretation_display`',
    '`result_status_code` AS `result_status_code`',
    '`result_status_display` AS `result_status_display`',
    # INSR_W3 W3.06: Gold: NULL where the Cerner system user id 1 (J5; silver publishes the flag).
    "CASE WHEN CAST(`performer_practitioner_id` AS BIGINT) = 1 THEN NULL ELSE `performer_practitioner_id` END AS `performer_practitioner_id`",
    '`source_form_key` AS `source_form_key`',
    '`promotion_rule_id` AS `promotion_rule_id`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_CLINICAL_SCORE_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`score_arm` AS `score_arm`',
    '`score_band` AS `score_band`',
    '`band_rule_id` AS `band_rule_id`',
    '`performer_system_ind` AS `performer_system_ind`',
]


# contract v2: QC/batch inputs come from internal _clinical_clinical_score_metadata; source history stays on the main research table.
CLINICAL_CLINICAL_SCORE_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 303,073,419 rows of
    # 303,073,548 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_CLINICAL_SCORE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.clinical_score.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 129 of 303,073,548 at the profile.
    "gold.clinical.clinical_score.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 35,015 of 303,073,548 rows (0.0116%) when profiled on
    # 2026-08-24.
    "gold.clinical.clinical_score.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 6,147,121 of 303,073,548 rows (2.03%) when profiled on
    # 2026-08-24.
    "gold.clinical.clinical_score.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # Left as a warning because it fires on 247,483,238 of 303,073,548 rows (81.7%) when
    # profiled on 2026-08-24 -- at that rate the rule's assumption about what event_datetime
    # and event_end_datetime mean is the thing in doubt, not the data. The inverted gaps are
    # mostly minutes, which reads as two clocks rather than two events in the wrong order.
    "gold.clinical.clinical_score.table.ordering_violation_event_datetime_event_end_datetime":
        "NOT COALESCE((`event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime`), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_CLINICAL_SCORE_PERSON_SCOPE_RULES = {
    "gold.clinical.clinical_score.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_clinical_score"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_CLINICAL_SCORE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CLINICAL_SCORE_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_CLINICAL_SCORE_PERSON_SCOPE_RULES)
def _gold_qc_clinical_clinical_score():
    """Quality-controlled twin of journey_clinical.clinical_score."""
    df = _qc("clinical_clinical_score", CLINICAL_CLINICAL_SCORE_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.clinical_score"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_clinical_score():
    """Contract-v2 public twin of clinical_clinical_score; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_clinical_score")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`event_id` AS `event_id`',
        '`doc_response_key` AS `doc_response_key`',
        '`source_dcp_forms_activity_id` AS `source_dcp_forms_activity_id`',
        '`source_object` AS `source_object`',
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
        '`__gold_json_score_code` AS `score_code`',
        '`score_name` AS `score_name`',
        '`value_number` AS `value_number`',
        '`value_text` AS `value_text`',
        '`unit_source_value` AS `unit_source_value`',
        '`unit_concept_id` AS `unit_concept_id`',
        '`component_count` AS `component_count`',
        '`interpretation_code` AS `interpretation_code`',
        '`interpretation_display` AS `interpretation_display`',
        '`result_status_code` AS `result_status_code`',
        '`result_status_display` AS `result_status_display`',
        '`performer_practitioner_id` AS `performer_practitioner_id`',
        '`source_form_key` AS `source_form_key`',
        '`promotion_rule_id` AS `promotion_rule_id`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        '`score_arm` AS `score_arm`',  # INSR_W3 W3.12
        '`score_band` AS `score_band`',  # INSR_W3 W3.12
        '`band_rule_id` AS `band_rule_id`',  # INSR_W3 W3.12
        '`performer_system_ind` AS `performer_system_ind`',  # INSR_W3 W3.12
    )
    return _s3_flatten_gold_public(df, "clinical_clinical_score")


def _public_steps_clinical_clinical_score(df):
    """clinical_clinical_score: the public body after its projection (GoldRules.public_select), moved from gold_clinical_clinical_score; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_clinical_score')


RULES = {
    'clinical_clinical_score': GoldRules(
        table='clinical_clinical_score',
        select=CLINICAL_CLINICAL_SCORE_SELECT,
        mandatory=CLINICAL_CLINICAL_SCORE_MANDATORY_RULES,
        advisory=CLINICAL_CLINICAL_SCORE_ADVISORY_RULES,
        person_scope=CLINICAL_CLINICAL_SCORE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'person_id_resolved',
                             'encounter_id_resolved',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth'),
        variant_columns=('score_code',),
        s3_axis_specs={'score_code': [('score', False)]},
        value_rules={'gold.clinical.clinical_score.performer_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                    'column': 'performer_practitioner_id',
                                                                                    'companions': [],
                                                                                    'match': '`performer_practitioner_id` '
                                                                                             '= 0',
                                                                                    'table': 'clinical_clinical_score'},
         'gold.clinical.clinical_score.score_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                  'column': 'score_snomed_code',
                                                                                  'companions': ['score_snomed_display',
                                                                                                 'score_target_code',
                                                                                                 'score_target_display'],
                                                                                  'match': '`score_snomed_code` '
                                                                                           'IS NOT NULL '
                                                                                           'AND (NOT '
                                                                                           '`score_snomed_code` '
                                                                                           'RLIKE '
                                                                                           "'^[1-9][0-9]{5,17}$' "
                                                                                           'OR '
                                                                                           'substring(`score_snomed_code`, '
                                                                                           'length(`score_snomed_code`) '
                                                                                           '- 2, 2) NOT IN '
                                                                                           "('00', '10') "
                                                                                           'OR '
                                                                                           '(substring(`score_snomed_code`, '
                                                                                           'length(`score_snomed_code`) '
                                                                                           "- 2, 2) = '10' "
                                                                                           'AND '
                                                                                           'length(`score_snomed_code`) '
                                                                                           '< 11))',
                                                                                  'table': 'clinical_clinical_score'},
         'gold.clinical.clinical_score.score_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                  'column': 'score_target_code',
                                                                                  'companions': ['score_target_display',
                                                                                                 'score_snomed_code',
                                                                                                 'score_snomed_display'],
                                                                                  'match': '`score_target_system` '
                                                                                           '= '
                                                                                           "'http://snomed.info/sct' "
                                                                                           'AND '
                                                                                           '`score_target_code` '
                                                                                           'IS NOT NULL '
                                                                                           'AND (NOT '
                                                                                           '`score_target_code` '
                                                                                           'RLIKE '
                                                                                           "'^[1-9][0-9]{5,17}$' "
                                                                                           'OR '
                                                                                           'substring(`score_target_code`, '
                                                                                           'length(`score_target_code`) '
                                                                                           '- 2, 2) NOT IN '
                                                                                           "('00', '10') "
                                                                                           'OR '
                                                                                           '(substring(`score_target_code`, '
                                                                                           'length(`score_target_code`) '
                                                                                           "- 2, 2) = '10' "
                                                                                           'AND '
                                                                                           'length(`score_target_code`) '
                                                                                           '< 11))',
                                                                                  'table': 'clinical_clinical_score'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`event_id` AS `event_id`',
            '`doc_response_key` AS `doc_response_key`',
            '`source_dcp_forms_activity_id` AS `source_dcp_forms_activity_id`',
            '`source_object` AS `source_object`',
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
            '`__gold_json_score_code` AS `score_code`',
            '`score_name` AS `score_name`',
            '`value_number` AS `value_number`',
            '`value_text` AS `value_text`',
            '`unit_source_value` AS `unit_source_value`',
            '`unit_concept_id` AS `unit_concept_id`',
            '`component_count` AS `component_count`',
            '`interpretation_code` AS `interpretation_code`',
            '`interpretation_display` AS `interpretation_display`',
            '`result_status_code` AS `result_status_code`',
            '`result_status_display` AS `result_status_display`',
            '`performer_practitioner_id` AS `performer_practitioner_id`',
            '`source_form_key` AS `source_form_key`',
            '`promotion_rule_id` AS `promotion_rule_id`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`source_feed` AS `source_feed`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
            '`score_arm` AS `score_arm`',
            '`score_band` AS `score_band`',
            '`band_rule_id` AS `band_rule_id`',
            '`performer_system_ind` AS `performer_system_ind`',
        ],
        public_steps=(_public_steps_clinical_clinical_score,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_clinical_score': {'columns': {'band_rule_id': 'Rule that set score_band. INSR_W2 W2.11.',
                                             'component_count': 'Number of ordered component objects.',
                                             'confidentiality_code': 'Security classification when supplied.',
                                             'doc_response_key': 'PowerForm DOC_RESPONSE_KEY; native key for '
                                                                 'the form_promotion arm.',
                                             'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                             'spine_encounter.encounter_id.',
                                             'event_datetime': 'Clinical start date and time of the event as '
                                                               'recorded on the Millennium CLINICAL_EVENT '
                                                               'row.',
                                             'event_datetime_local': 'Local-time representation of the '
                                                                     'clinical score event timestamp. Gold: '
                                                                     'NULL wherever Gold nulls '
                                                                     'event_datetime; where Gold repairs a '
                                                                     'millisecond-epoch event_datetime, the '
                                                                     'repaired time in Europe/London.',
                                             'event_end_datetime': 'Score end timestamp.',
                                             'event_end_datetime_local': 'Local-time representation of the '
                                                                         'clinical score end timestamp. '
                                                                         'Gold: NULL wherever Gold nulls '
                                                                         'event_end_datetime.',
                                             'event_id': 'Millennium numeric EVENT_ID; native key for the '
                                                         'numeric_event arm.',
                                             'interpretation_code': 'Source interpretation code.',
                                             'interpretation_display': 'Source interpretation display.',
                                             'loaded_at': 'map_numeric_events.ADC_UPDT for numeric-event '
                                                          'rows, or map_powerform_assessment_item.ADC_UPDT '
                                                          "for form-promoted rows. Each arm's bronze load "
                                                          'clock is carried unchanged through its staging, '
                                                          'union and quality wrappers. No form-parent or '
                                                          'promotion-configuration clock is added; missing '
                                                          'values remain null and this is not Silver refresh '
                                                          'time.',
                                             'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                  'stable cross-feed join key.',
                                             'performer_practitioner_id': 'Millennium personnel PERSON_ID '
                                                                          'for the performer.',
                                             'performer_system_ind': 'True when the source '
                                                                     'performer_practitioner_id was 1, the '
                                                                     'Cerner system user (J5); the id is '
                                                                     'then published as NULL. NULL when the '
                                                                     'source id was NULL. INSR_W2.',
                                             'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                             'promotion_rule_id': 'Governed promotion-rule identifier.',
                                             'record_status': 'Arm-specific row status. Numeric events are '
                                                              'retracted when '
                                                              'map_numeric_events.SOURCE_DELETED_IND is true '
                                                              '(null defaults false), otherwise superseded '
                                                              'when CLINICAL_EVENT_VALID_UNTIL_DT_TM is '
                                                              'before 2100-01-01, otherwise active. '
                                                              'Form-promoted rows are retracted when '
                                                              'CANONICAL_SOURCE_DELETED_IND is true or '
                                                              'SOURCE_PRESENT_IND is false or missing; '
                                                              'otherwise superseded when ACTIVE_IND is zero '
                                                              'or missing, otherwise active. Retraction has '
                                                              'priority over supersession; this is not the '
                                                              'clinical result-status code.',
                                             'record_status_effective_from': 'map_numeric_events.CLINICAL_EVENT_VALID_FROM_DT_TM '
                                                                             'on the numeric-event arm; '
                                                                             'map_powerform_assessment_item.FIRST_DOCUMENTED_DT_TM '
                                                                             'on the form-promotion arm. '
                                                                             'Each is carried unchanged, '
                                                                             'with no '
                                                                             'performed/response/ingestion-time '
                                                                             'fallback; a missing value '
                                                                             'remains null.',
                                             'record_status_effective_to': 'Numeric events return '
                                                                           'CLINICAL_EVENT_VALID_UNTIL_DT_TM '
                                                                           'when deleted or ended, otherwise '
                                                                           'null; deletion can therefore '
                                                                           'retain a far-future end value. '
                                                                           'Form-promoted rows return '
                                                                           'LAST_DOCUMENTED_DT_TM when '
                                                                           'deleted, absent or inactive, '
                                                                           'otherwise null. No missing-end '
                                                                           'fallback is applied, and the '
                                                                           'form documentation end is not an '
                                                                           'independently observed '
                                                                           'status-transition time.',
                                             'result_status_code': 'Source result status code.',
                                             'result_status_display': 'Source result status display.',
                                             'score_arm': 'news2 or must when the score is one of those '
                                                          "instruments' totals; NULL otherwise. INSR_W2 "
                                                          'W2.11.',
                                             'score_band': 'Risk band: NEWS2 total 0-4 low, 5-6 medium, 7-20 '
                                                           'high (RCP 2017); MUST 0 low, 1 medium, 2-6 high '
                                                           '(BAPEN); NULL for a value outside the '
                                                           "instrument's range. The NEWS2 single-parameter-3 "
                                                           'rule needs components and is not applied. '
                                                           'INSR_W2 W2.11.',
                                             'score_code': 'Source and mapped score CodeableConcept.',
                                             'score_name': 'Human-readable score or scale name.',
                                             'source_code': 'Source score code.',
                                             'source_coding_system': 'Coding system or source namespace in '
                                                                     'which the source code is defined for '
                                                                     'each clinical score record. It is '
                                                                     'produced by the silver transformation '
                                                                     'and has no direct bronze-column '
                                                                     'lineage entry. Source code meanings '
                                                                     'and sentinel values are retained '
                                                                     'unless the pipeline explicitly maps '
                                                                     'them; null means no code was supplied.',
                                             'source_dcp_forms_activity_id': 'Millennium '
                                                                             'DCP_FORMS_ACTIVITY_ID of the '
                                                                             'source form for promoted rows.',
                                             'source_display': 'Source score display.',
                                             'source_feed': 'Registered source route.',
                                             'source_form_key': 'Deterministic SHA-256 key of the source '
                                                                'PowerForm instance for promoted rows.',
                                             'source_object': 'Native source arm: numeric_event or '
                                                              'form_promotion.',
                                             'source_update_timestamp': 'For numeric events, greatest of '
                                                                        'map_numeric_events.STRING_RESULT_UPDT_DT_TM, '
                                                                        'CLINICAL_EVENT_UPDT_DT_TM and '
                                                                        'ADC_UPDT. For form-promoted rows, '
                                                                        'map_powerform_assessment_item.ADC_UPDT '
                                                                        'directly. This mixes source and '
                                                                        'ingestion provenance rather than '
                                                                        'uniformly representing a native '
                                                                        'modification clock; the promotion '
                                                                        'lookup adds no clock and Silver '
                                                                        'refresh time is not substituted.',
                                             'subject_id_system': 'Identifier system used for subject_key.',
                                             'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                            'identifier. Not salted, not secret; a stable '
                                                            'join key across feeds where person_id is '
                                                            'unresolved.',
                                             'unit_concept_id': 'Mapped unit concept identifier.',
                                             'unit_source_value': 'Source unit when supplied.',
                                             'value_number': 'Numeric score value.',
                                             'value_text': 'Verbatim score text.',
                                             'vip_ind': 'VIP indicator when supplied.',
                                             'withheld_identity_ind': 'Withheld identity indicator.'},
                                 'quantity_override': True,
                                 'table': 'Internal quality-controlled twin of clinical_clinical_score: the '
                                          'Gold repairs, nulls and expectations are applied here and the '
                                          'published Gold MV reads this object. Materialized rather than '
                                          'temporary so the published MV can refresh incrementally.'},
 'clinical_clinical_score': {'columns': {'band_rule_id': 'Rule that set score_band. INSR_W2 W2.11.',
                                         'component_count': 'Number of ordered component objects.',
                                         'confidentiality_code': 'Security classification when supplied.',
                                         'doc_response_key': 'PowerForm DOC_RESPONSE_KEY; native key for the '
                                                             'form_promotion arm.',
                                         'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                         'spine_encounter.encounter_id.',
                                         'event_datetime': 'Clinical start date and time of the event as '
                                                           'recorded on the Millennium CLINICAL_EVENT row.',
                                         'event_datetime_local': 'Local-time representation of the clinical '
                                                                 'score event timestamp. Gold: NULL wherever '
                                                                 'Gold nulls event_datetime; where Gold '
                                                                 'repairs a millisecond-epoch '
                                                                 'event_datetime, the repaired time in '
                                                                 'Europe/London.',
                                         'event_end_datetime': 'Score end timestamp.',
                                         'event_end_datetime_local': 'Local-time representation of the '
                                                                     'clinical score end timestamp. Gold: '
                                                                     'NULL wherever Gold nulls '
                                                                     'event_end_datetime.',
                                         'event_id': 'Millennium numeric EVENT_ID; native key for the '
                                                     'numeric_event arm.',
                                         'interpretation_code': 'Source interpretation code.',
                                         'interpretation_display': 'Source interpretation display.',
                                         'loaded_at': 'map_numeric_events.ADC_UPDT for numeric-event rows, '
                                                      'or map_powerform_assessment_item.ADC_UPDT for '
                                                      "form-promoted rows. Each arm's bronze load clock is "
                                                      'carried unchanged through its staging, union and '
                                                      'quality wrappers. No form-parent or '
                                                      'promotion-configuration clock is added; missing '
                                                      'values remain null and this is not Silver refresh '
                                                      'time.',
                                         'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                              'cross-feed join key.',
                                         'performer_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                      'the performer.',
                                         'performer_system_ind': 'True when the source '
                                                                 'performer_practitioner_id was 1, the '
                                                                 'Cerner system user (J5); the id is then '
                                                                 'published as NULL. NULL when the source id '
                                                                 'was NULL. INSR_W2.',
                                         'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                         'promotion_rule_id': 'Governed promotion-rule identifier.',
                                         'record_status': 'Arm-specific row status. Numeric events are '
                                                          'retracted when '
                                                          'map_numeric_events.SOURCE_DELETED_IND is true '
                                                          '(null defaults false), otherwise superseded when '
                                                          'CLINICAL_EVENT_VALID_UNTIL_DT_TM is before '
                                                          '2100-01-01, otherwise active. Form-promoted rows '
                                                          'are retracted when CANONICAL_SOURCE_DELETED_IND '
                                                          'is true or SOURCE_PRESENT_IND is false or '
                                                          'missing; otherwise superseded when ACTIVE_IND is '
                                                          'zero or missing, otherwise active. Retraction has '
                                                          'priority over supersession; this is not the '
                                                          'clinical result-status code.',
                                         'record_status_effective_from': 'map_numeric_events.CLINICAL_EVENT_VALID_FROM_DT_TM '
                                                                         'on the numeric-event arm; '
                                                                         'map_powerform_assessment_item.FIRST_DOCUMENTED_DT_TM '
                                                                         'on the form-promotion arm. Each is '
                                                                         'carried unchanged, with no '
                                                                         'performed/response/ingestion-time '
                                                                         'fallback; a missing value remains '
                                                                         'null.',
                                         'record_status_effective_to': 'Numeric events return '
                                                                       'CLINICAL_EVENT_VALID_UNTIL_DT_TM '
                                                                       'when deleted or ended, otherwise '
                                                                       'null; deletion can therefore retain '
                                                                       'a far-future end value. '
                                                                       'Form-promoted rows return '
                                                                       'LAST_DOCUMENTED_DT_TM when deleted, '
                                                                       'absent or inactive, otherwise null. '
                                                                       'No missing-end fallback is applied, '
                                                                       'and the form documentation end is '
                                                                       'not an independently observed '
                                                                       'status-transition time.',
                                         'result_status_code': 'Source result status code.',
                                         'result_status_display': 'Source result status display.',
                                         'score_arm': 'news2 or must when the score is one of those '
                                                      "instruments' totals; NULL otherwise. INSR_W2 W2.11.",
                                         'score_band': 'Risk band: NEWS2 total 0-4 low, 5-6 medium, 7-20 '
                                                       'high (RCP 2017); MUST 0 low, 1 medium, 2-6 high '
                                                       "(BAPEN); NULL for a value outside the instrument's "
                                                       'range. The NEWS2 single-parameter-3 rule needs '
                                                       'components and is not applied. INSR_W2 W2.11.',
                                         'score_code': 'Source and mapped score CodeableConcept.',
                                         'score_name': 'Human-readable score or scale name.',
                                         'source_code': 'Source score code.',
                                         'source_coding_system': 'Coding system or source namespace in which '
                                                                 'the source code is defined for each '
                                                                 'clinical score record. It is produced by '
                                                                 'the silver transformation and has no '
                                                                 'direct bronze-column lineage entry. Source '
                                                                 'code meanings and sentinel values are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'maps them; null means no code was '
                                                                 'supplied.',
                                         'source_dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID '
                                                                         'of the source form for promoted '
                                                                         'rows.',
                                         'source_display': 'Source score display.',
                                         'source_feed': 'Registered source route.',
                                         'source_form_key': 'Deterministic SHA-256 key of the source '
                                                            'PowerForm instance for promoted rows.',
                                         'source_object': 'Native source arm: numeric_event or '
                                                          'form_promotion.',
                                         'source_update_timestamp': 'For numeric events, greatest of '
                                                                    'map_numeric_events.STRING_RESULT_UPDT_DT_TM, '
                                                                    'CLINICAL_EVENT_UPDT_DT_TM and ADC_UPDT. '
                                                                    'For form-promoted rows, '
                                                                    'map_powerform_assessment_item.ADC_UPDT '
                                                                    'directly. This mixes source and '
                                                                    'ingestion provenance rather than '
                                                                    'uniformly representing a native '
                                                                    'modification clock; the promotion '
                                                                    'lookup adds no clock and Silver refresh '
                                                                    'time is not substituted.',
                                         'subject_id_system': 'Identifier system used for subject_key.',
                                         'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                        'identifier. Not salted, not secret; a stable join '
                                                        'key across feeds where person_id is unresolved.',
                                         'unit_concept_id': 'Mapped unit concept identifier.',
                                         'unit_source_value': 'Source unit when supplied.',
                                         'value_number': 'Numeric score value.',
                                         'value_text': 'Verbatim score text.',
                                         'vip_ind': 'VIP indicator when supplied.',
                                         'withheld_identity_ind': 'Withheld identity indicator.'},
                             'quantity_override': False,
                             'table': 'One native or form-promoted clinical score with ordered component '
                                      'evidence. Gold QC twin of the silver product: 8 columns are repaired '
                                      'or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each rule '
                                      'states its reason in the pipeline notebook, and Lakeflow expectation '
                                      'metrics report what every rule matched on each update.'}}
