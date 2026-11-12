"""Gold clinical.vital_sign: 2 dataset(s).

- datasets: _qc_clinical_vital_sign, clinical_vital_sign
- moved verbatim from: 2_gold_clinical_events:6730, 2_gold_clinical_events:6787, 2_gold_clinical_events:6791, 2_gold_clinical_events:6797, 2_gold_clinical_events:6805, 2_gold_clinical_events:6909, 2_gold_clinical_events:6911, 2_gold_clinical_events:6926

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from pyspark.sql.window import Window
import pyspark.sql.functions as F
from gold_core.gold_core import _n, _s3_axis_columns, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.vital_sign ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_VITAL_SIGN_SELECT = [
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
    '`vital_code` AS `vital_code`',
    # DQ4 G5 tympanic/axillary/oral temperature outside 25-45 C is not a living reading
    # (23,145 rows; every unit is Celsius or blank, so there is no Fahrenheit to rescue; skin
    # temperature is left alone); G8 a weight, height or BMI of exactly 0 is a blank (46,661 rows).
    'CASE WHEN ABS(CAST(`value_number` AS DOUBLE)) > 1e12 OR `value_number` < 0 THEN NULL'
    ' WHEN `vital_omop_concept_id` IN (4215364, 4188706, 4218834) AND (`value_number` < 25 OR `value_number` > 45) THEN NULL'
    # BBR_FOLDIN_V1 (BBR-5): a zero weight, height or BMI is a blank for every anthropometric concept: the
    # SNOMED, LOINC and LOINC-estimated standard of each (bbr_rules.ANTHROPOMETRIC_OMOP_IDS).
    ' WHEN `vital_omop_concept_id` IN (3025315, 3026600, 3035463, 3036277, 3038553, 4099154, 4177340, 4245997, 36304833) AND `value_number` = 0 THEN NULL'
    ' ELSE `value_number` END AS `value_number`',
    '`value_text` AS `value_text`',
    '`unit_source_value` AS `unit_source_value`',
    '`unit_concept_id` AS `unit_concept_id`',
    '`reference_range_low` AS `reference_range_low`',
    '`reference_range_high` AS `reference_range_high`',
    '`method_code` AS `method_code`',
    '`method_display` AS `method_display`',
    '`body_site_code` AS `body_site_code`',
    '`body_site_display` AS `body_site_display`',
    "CASE WHEN UPPER(TRIM(CAST(`interpretation_code` AS STRING))) = '0' THEN NULL ELSE `interpretation_code` END AS `interpretation_code`",
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
    '`registry_field_id` AS `registry_field_id`',
    '`event_datetime_status` AS `event_datetime_status`',
    '`vital_omop_concept_id` AS `_qc_vital_concept_id`',
    '`vital_snomed_code` AS `_qc_vital_snomed_code`',  # BBR_FOLDIN_V1 (BBR-5): read by insr_bbr_anthropometric_range; the vital axis is restored from JSON only in the public step
]


CLINICAL_VITAL_SIGN_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`vital_omop_map_rule_id` AS `vital_omop_map_rule_id`',
    '`vital_omop_map_version` AS `vital_omop_map_version`',
]


for _s3b_axis in ("unit", "method", "body_site", "interpretation"):
    CLINICAL_VITAL_SIGN_SELECT.extend(
        [f"`{name}` AS `{name}`" for name in _s3_axis_columns(_s3b_axis)]
    )


# contract v2: QC/batch inputs come from internal _clinical_vital_sign_metadata; source history stays on the main research table.
CLINICAL_VITAL_SIGN_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 283,503,962 rows of
    # 283,505,704 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_VITAL_SIGN_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.vital_sign.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 1,742 of 283,505,704 at the profile.
    "gold.clinical.vital_sign.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 31,497 of 283,505,704 rows (0.0111%) when profiled on
    # 2026-08-24.
    "gold.clinical.vital_sign.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 6,504,311 of 283,505,704 rows (2.29%) when profiled on
    # 2026-08-24.
    "gold.clinical.vital_sign.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # Left as a warning because it fires on 213,158,920 of 283,505,704 rows (75.2%) when
    # profiled on 2026-08-24 -- at that rate the rule's assumption about what event_datetime
    # and event_end_datetime mean is the thing in doubt, not the data. The inverted gaps are
    # mostly minutes, which reads as two clocks rather than two events in the wrong order.
    "gold.clinical.vital_sign.table.ordering_violation_event_datetime_event_end_datetime":
        "NOT COALESCE((`event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime`), FALSE)",

    # DQ4 G1. Left as a warning because a pair cannot say which of its two readings is wrong,
    # and nulling both would lose the sound one. Seen on 5,173 of 26,158,007 unambiguous pairs
    # (both rows flagged) when profiled on 2026-09-26.
    "gold.clinical.vital_sign.value_number.bp_pair_implausible":
        "NOT COALESCE(bp_pair_implausible, FALSE)",
}


# DQ4 G1: rule gold.clinical.vital_sign.value_number.bp_pair_implausible. A systolic and a
# diastolic reading pair when each is the only one of its kind for the person at that instant
# (26,158,007 of 33,001,175 instants; the rest are ambiguous and never judged). A pair is
# implausible when systolic <= diastolic, systolic < 20 or diastolic > 200.
_G1_SYSTOLIC, _G1_DIASTOLIC = 4152194, 4154790


def _g1_bp_pair_implausible(df):
    from pyspark.sql import Window
    systolic = F.col("_qc_vital_concept_id") == _G1_SYSTOLIC
    diastolic = F.col("_qc_vital_concept_id") == _G1_DIASTOLIC
    is_bp = F.coalesce(systolic | diastolic, F.lit(False))
    pair = Window.partitionBy("person_id", "event_datetime", is_bp)
    n_sys = F.count(F.when(systolic, 1)).over(pair)
    n_dia = F.count(F.when(diastolic, 1)).over(pair)
    sbp = F.max(F.when(systolic, F.col("value_number"))).over(pair)
    dbp = F.max(F.when(diastolic, F.col("value_number"))).over(pair)
    judged = is_bp & F.col("person_id").isNotNull() & F.col("event_datetime").isNotNull() & (n_sys == 1) & (n_dia == 1)
    return df.withColumn("bp_pair_implausible", judged & ((sbp <= dbp) | (sbp < 20) | (dbp > 200)))


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_VITAL_SIGN_PERSON_SCOPE_RULES = {
    "gold.clinical.vital_sign.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_vital_sign"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_VITAL_SIGN_MANDATORY_RULES)
@dp.expect_all(CLINICAL_VITAL_SIGN_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_VITAL_SIGN_PERSON_SCOPE_RULES)
def _gold_qc_clinical_vital_sign():
    """Quality-controlled twin of journey_clinical.vital_sign."""
    df = _g1_bp_pair_implausible(_qc("clinical_vital_sign", CLINICAL_VITAL_SIGN_SELECT))
    return df


@dp.materialized_view(
    name=_n("gold_clinical.vital_sign"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_vital_sign():
    """Contract-v2 public twin of clinical_vital_sign; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_vital_sign")).selectExpr(
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
        '`__gold_json_vital_code` AS `vital_code`',
        '`value_number` AS `value_number`',
        '`value_text` AS `value_text`',
        '`unit_source_value` AS `unit_source_value`',
        '`unit_concept_id` AS `unit_concept_id`',
        '`reference_range_low` AS `reference_range_low`',
        '`reference_range_high` AS `reference_range_high`',
        '`method_code` AS `method_code`',
        '`method_display` AS `method_display`',
        '`body_site_code` AS `body_site_code`',
        '`body_site_display` AS `body_site_display`',
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
        '`registry_field_id` AS `registry_field_id`',
        '`event_datetime_status` AS `event_datetime_status`',
        '`vital_omop_map_rule_id` AS `vital_omop_map_rule_id`',  # INSR_W3 W3.12
        '`vital_omop_map_version` AS `vital_omop_map_version`',  # INSR_W3 W3.12
        *[
            f"`{name}` AS `{name}`"
            for axis in ("unit", "method", "body_site", "interpretation")
            for name in _s3_axis_columns(axis)
        ],
        *[
            f"`_{axis}_gate_pass` AS `_{axis}_gate_pass`"
            for axis in ("unit", "method", "body_site", "interpretation")
        ],
        *[
            f"`_{axis}_policy_id` AS `_{axis}_policy_id`"
            for axis in ("unit", "method", "body_site", "interpretation")
        ],
    )
    df = _s3_flatten_gold_public(df, "clinical_vital_sign")
    return (_s3_gate_direct_public(df, "clinical_vital_sign")
            .drop(*[f"_{axis}_{suffix}" for axis in ("unit", "method", "body_site", "interpretation")
                    for suffix in ("gate_pass", "policy_id")]))


def _public_steps_clinical_vital_sign(df):
    """clinical_vital_sign: the public body after its projection (GoldRules.public_select), moved from gold_clinical_vital_sign; df is the projected twin frame."""
    df = _s3_flatten_gold_public(df, 'clinical_vital_sign')
    return _s3_gate_direct_public(df, 'clinical_vital_sign').drop(*[f'_{axis}_{suffix}' for axis in ('unit', 'method', 'body_site', 'interpretation') for suffix in ('gate_pass', 'policy_id')])


RULES = {
    'clinical_vital_sign': GoldRules(
        table='clinical_vital_sign',
        select=CLINICAL_VITAL_SIGN_SELECT,
        mandatory=CLINICAL_VITAL_SIGN_MANDATORY_RULES,
        advisory=CLINICAL_VITAL_SIGN_ADVISORY_RULES,
        person_scope=CLINICAL_VITAL_SIGN_PERSON_SCOPE_RULES,
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
        variant_columns=('vital_code',),
        s3_axis_specs={'vital_code': [('vital', False)]},
        s3b_direct_axes=(('unit', False), ('method', False), ('body_site', False), ('interpretation', False)),
        value_rules={'gold.clinical.vital_sign.body_site_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                  'column': 'body_site_snomed_code',
                                                                                  'companions': ['body_site_snomed_display',
                                                                                                 'body_site_target_code',
                                                                                                 'body_site_target_display'],
                                                                                  'match': '`body_site_snomed_code` '
                                                                                           'IS NOT NULL '
                                                                                           'AND (NOT '
                                                                                           '`body_site_snomed_code` '
                                                                                           'RLIKE '
                                                                                           "'^[1-9][0-9]{5,17}$' "
                                                                                           'OR '
                                                                                           'substring(`body_site_snomed_code`, '
                                                                                           'length(`body_site_snomed_code`) '
                                                                                           '- 2, 2) NOT IN '
                                                                                           "('00', '10') "
                                                                                           'OR '
                                                                                           '(substring(`body_site_snomed_code`, '
                                                                                           'length(`body_site_snomed_code`) '
                                                                                           "- 2, 2) = '10' "
                                                                                           'AND '
                                                                                           'length(`body_site_snomed_code`) '
                                                                                           '< 11))',
                                                                                  'table': 'clinical_vital_sign'},
         'gold.clinical.vital_sign.body_site_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                  'column': 'body_site_target_code',
                                                                                  'companions': ['body_site_target_display',
                                                                                                 'body_site_snomed_code',
                                                                                                 'body_site_snomed_display'],
                                                                                  'match': '`body_site_target_system` '
                                                                                           '= '
                                                                                           "'http://snomed.info/sct' "
                                                                                           'AND '
                                                                                           '`body_site_target_code` '
                                                                                           'IS NOT NULL '
                                                                                           'AND (NOT '
                                                                                           '`body_site_target_code` '
                                                                                           'RLIKE '
                                                                                           "'^[1-9][0-9]{5,17}$' "
                                                                                           'OR '
                                                                                           'substring(`body_site_target_code`, '
                                                                                           'length(`body_site_target_code`) '
                                                                                           '- 2, 2) NOT IN '
                                                                                           "('00', '10') "
                                                                                           'OR '
                                                                                           '(substring(`body_site_target_code`, '
                                                                                           'length(`body_site_target_code`) '
                                                                                           "- 2, 2) = '10' "
                                                                                           'AND '
                                                                                           'length(`body_site_target_code`) '
                                                                                           '< 11))',
                                                                                  'table': 'clinical_vital_sign'},
         'gold.clinical.vital_sign.interpretation_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                       'column': 'interpretation_snomed_code',
                                                                                       'companions': ['interpretation_snomed_display',
                                                                                                      'interpretation_target_code',
                                                                                                      'interpretation_target_display'],
                                                                                       'match': '`interpretation_snomed_code` '
                                                                                                'IS NOT '
                                                                                                'NULL AND '
                                                                                                '(NOT '
                                                                                                '`interpretation_snomed_code` '
                                                                                                'RLIKE '
                                                                                                "'^[1-9][0-9]{5,17}$' "
                                                                                                'OR '
                                                                                                'substring(`interpretation_snomed_code`, '
                                                                                                'length(`interpretation_snomed_code`) '
                                                                                                '- 2, 2) '
                                                                                                'NOT IN '
                                                                                                "('00', "
                                                                                                "'10') OR "
                                                                                                '(substring(`interpretation_snomed_code`, '
                                                                                                'length(`interpretation_snomed_code`) '
                                                                                                '- 2, 2) = '
                                                                                                "'10' AND "
                                                                                                'length(`interpretation_snomed_code`) '
                                                                                                '< 11))',
                                                                                       'table': 'clinical_vital_sign'},
         'gold.clinical.vital_sign.interpretation_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                       'column': 'interpretation_target_code',
                                                                                       'companions': ['interpretation_target_display',
                                                                                                      'interpretation_snomed_code',
                                                                                                      'interpretation_snomed_display'],
                                                                                       'match': '`interpretation_target_system` '
                                                                                                '= '
                                                                                                "'http://snomed.info/sct' "
                                                                                                'AND '
                                                                                                '`interpretation_target_code` '
                                                                                                'IS NOT '
                                                                                                'NULL AND '
                                                                                                '(NOT '
                                                                                                '`interpretation_target_code` '
                                                                                                'RLIKE '
                                                                                                "'^[1-9][0-9]{5,17}$' "
                                                                                                'OR '
                                                                                                'substring(`interpretation_target_code`, '
                                                                                                'length(`interpretation_target_code`) '
                                                                                                '- 2, 2) '
                                                                                                'NOT IN '
                                                                                                "('00', "
                                                                                                "'10') OR "
                                                                                                '(substring(`interpretation_target_code`, '
                                                                                                'length(`interpretation_target_code`) '
                                                                                                '- 2, 2) = '
                                                                                                "'10' AND "
                                                                                                'length(`interpretation_target_code`) '
                                                                                                '< 11))',
                                                                                       'table': 'clinical_vital_sign'},
         'gold.clinical.vital_sign.method_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                               'column': 'method_snomed_code',
                                                                               'companions': ['method_snomed_display',
                                                                                              'method_target_code',
                                                                                              'method_target_display'],
                                                                               'match': '`method_snomed_code` '
                                                                                        'IS NOT NULL AND '
                                                                                        '(NOT '
                                                                                        '`method_snomed_code` '
                                                                                        'RLIKE '
                                                                                        "'^[1-9][0-9]{5,17}$' "
                                                                                        'OR '
                                                                                        'substring(`method_snomed_code`, '
                                                                                        'length(`method_snomed_code`) '
                                                                                        '- 2, 2) NOT IN '
                                                                                        "('00', '10') OR "
                                                                                        '(substring(`method_snomed_code`, '
                                                                                        'length(`method_snomed_code`) '
                                                                                        "- 2, 2) = '10' "
                                                                                        'AND '
                                                                                        'length(`method_snomed_code`) '
                                                                                        '< 11))',
                                                                               'table': 'clinical_vital_sign'},
         'gold.clinical.vital_sign.method_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                               'column': 'method_target_code',
                                                                               'companions': ['method_target_display',
                                                                                              'method_snomed_code',
                                                                                              'method_snomed_display'],
                                                                               'match': '`method_target_system` '
                                                                                        '= '
                                                                                        "'http://snomed.info/sct' "
                                                                                        'AND '
                                                                                        '`method_target_code` '
                                                                                        'IS NOT NULL AND '
                                                                                        '(NOT '
                                                                                        '`method_target_code` '
                                                                                        'RLIKE '
                                                                                        "'^[1-9][0-9]{5,17}$' "
                                                                                        'OR '
                                                                                        'substring(`method_target_code`, '
                                                                                        'length(`method_target_code`) '
                                                                                        '- 2, 2) NOT IN '
                                                                                        "('00', '10') OR "
                                                                                        '(substring(`method_target_code`, '
                                                                                        'length(`method_target_code`) '
                                                                                        "- 2, 2) = '10' "
                                                                                        'AND '
                                                                                        'length(`method_target_code`) '
                                                                                        '< 11))',
                                                                               'table': 'clinical_vital_sign'},
         'gold.clinical.vital_sign.unit_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                             'column': 'unit_snomed_code',
                                                                             'companions': ['unit_snomed_display',
                                                                                            'unit_target_code',
                                                                                            'unit_target_display'],
                                                                             'match': '`unit_snomed_code` '
                                                                                      'IS NOT NULL AND '
                                                                                      '(NOT '
                                                                                      '`unit_snomed_code` '
                                                                                      'RLIKE '
                                                                                      "'^[1-9][0-9]{5,17}$' "
                                                                                      'OR '
                                                                                      'substring(`unit_snomed_code`, '
                                                                                      'length(`unit_snomed_code`) '
                                                                                      '- 2, 2) NOT IN '
                                                                                      "('00', '10') OR "
                                                                                      '(substring(`unit_snomed_code`, '
                                                                                      'length(`unit_snomed_code`) '
                                                                                      "- 2, 2) = '10' AND "
                                                                                      'length(`unit_snomed_code`) '
                                                                                      '< 11))',
                                                                             'table': 'clinical_vital_sign'},
         'gold.clinical.vital_sign.unit_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                             'column': 'unit_target_code',
                                                                             'companions': ['unit_target_display',
                                                                                            'unit_snomed_code',
                                                                                            'unit_snomed_display'],
                                                                             'match': '`unit_target_system` '
                                                                                      '= '
                                                                                      "'http://snomed.info/sct' "
                                                                                      'AND '
                                                                                      '`unit_target_code` '
                                                                                      'IS NOT NULL AND '
                                                                                      '(NOT '
                                                                                      '`unit_target_code` '
                                                                                      'RLIKE '
                                                                                      "'^[1-9][0-9]{5,17}$' "
                                                                                      'OR '
                                                                                      'substring(`unit_target_code`, '
                                                                                      'length(`unit_target_code`) '
                                                                                      '- 2, 2) NOT IN '
                                                                                      "('00', '10') OR "
                                                                                      '(substring(`unit_target_code`, '
                                                                                      'length(`unit_target_code`) '
                                                                                      "- 2, 2) = '10' AND "
                                                                                      'length(`unit_target_code`) '
                                                                                      '< 11))',
                                                                             'table': 'clinical_vital_sign'},
         'gold.clinical.vital_sign.value_number.insr_bbr_anthropometric_range': {'action': 'invalidate',
                                                                                 'column': 'value_number',
                                                                                 'companions': ['value_canonical'],
                                                                                 'match': '`value_number` '
                                                                                          'IS NOT NULL AND '
                                                                                          '((`_qc_vital_snomed_code` '
                                                                                          "= '27113001' "
                                                                                          'AND '
                                                                                          '(`value_canonical` '
                                                                                          '<= 0 OR '
                                                                                          '`value_canonical` '
                                                                                          '> 400)) OR '
                                                                                          '(`_qc_vital_snomed_code` '
                                                                                          "= '50373000' "
                                                                                          'AND '
                                                                                          '(`value_canonical` '
                                                                                          '< 20 OR '
                                                                                          '`value_canonical` '
                                                                                          '> 250)) OR '
                                                                                          '(`_qc_vital_snomed_code` '
                                                                                          "= '60621009' "
                                                                                          'AND '
                                                                                          '(`value_canonical` '
                                                                                          '< 5 OR '
                                                                                          '`value_canonical` '
                                                                                          '> 150)))',
                                                                                 'table': 'clinical_vital_sign'},
         'gold.clinical.vital_sign.vital_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                              'column': 'vital_snomed_code',
                                                                              'companions': ['vital_snomed_display',
                                                                                             'vital_target_code',
                                                                                             'vital_target_display'],
                                                                              'match': '`vital_snomed_code` '
                                                                                       'IS NOT NULL AND '
                                                                                       '(NOT '
                                                                                       '`vital_snomed_code` '
                                                                                       'RLIKE '
                                                                                       "'^[1-9][0-9]{5,17}$' "
                                                                                       'OR '
                                                                                       'substring(`vital_snomed_code`, '
                                                                                       'length(`vital_snomed_code`) '
                                                                                       '- 2, 2) NOT IN '
                                                                                       "('00', '10') OR "
                                                                                       '(substring(`vital_snomed_code`, '
                                                                                       'length(`vital_snomed_code`) '
                                                                                       "- 2, 2) = '10' AND "
                                                                                       'length(`vital_snomed_code`) '
                                                                                       '< 11))',
                                                                              'table': 'clinical_vital_sign'},
         'gold.clinical.vital_sign.vital_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                              'column': 'vital_target_code',
                                                                              'companions': ['vital_target_display',
                                                                                             'vital_snomed_code',
                                                                                             'vital_snomed_display'],
                                                                              'match': '`vital_target_system` '
                                                                                       '= '
                                                                                       "'http://snomed.info/sct' "
                                                                                       'AND '
                                                                                       '`vital_target_code` '
                                                                                       'IS NOT NULL AND '
                                                                                       '(NOT '
                                                                                       '`vital_target_code` '
                                                                                       'RLIKE '
                                                                                       "'^[1-9][0-9]{5,17}$' "
                                                                                       'OR '
                                                                                       'substring(`vital_target_code`, '
                                                                                       'length(`vital_target_code`) '
                                                                                       '- 2, 2) NOT IN '
                                                                                       "('00', '10') OR "
                                                                                       '(substring(`vital_target_code`, '
                                                                                       'length(`vital_target_code`) '
                                                                                       "- 2, 2) = '10' AND "
                                                                                       'length(`vital_target_code`) '
                                                                                       '< 11))',
                                                                              'table': 'clinical_vital_sign'}},
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
            '`__gold_json_vital_code` AS `vital_code`',
            '`value_number` AS `value_number`',
            '`value_text` AS `value_text`',
            '`unit_source_value` AS `unit_source_value`',
            '`unit_concept_id` AS `unit_concept_id`',
            '`reference_range_low` AS `reference_range_low`',
            '`reference_range_high` AS `reference_range_high`',
            '`method_code` AS `method_code`',
            '`method_display` AS `method_display`',
            '`body_site_code` AS `body_site_code`',
            '`body_site_display` AS `body_site_display`',
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
            '`registry_field_id` AS `registry_field_id`',
            '`event_datetime_status` AS `event_datetime_status`',
            '`vital_omop_map_rule_id` AS `vital_omop_map_rule_id`',
            '`vital_omop_map_version` AS `vital_omop_map_version`',
            *[f'`{name}` AS `{name}`' for axis in ('unit', 'method', 'body_site', 'interpretation') for name in _s3_axis_columns(axis)],
            *[f'`_{axis}_gate_pass` AS `_{axis}_gate_pass`' for axis in ('unit', 'method', 'body_site', 'interpretation')],
            *[f'`_{axis}_policy_id` AS `_{axis}_policy_id`' for axis in ('unit', 'method', 'body_site', 'interpretation')],
        ],
        public_steps=(_public_steps_clinical_vital_sign,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public
_s3_gate_direct_public = _rules._s3_gate_direct_public


finalise()


COMMENTS = {'_qc_clinical_vital_sign': {'columns': {'body_site_code': 'Body-site code when supplied.',
                                         'body_site_display': 'Body-site display when supplied.',
                                         'body_site_map_candidate_count': 'Governed S3b body site mapping '
                                                                          'provenance.',
                                         'body_site_map_candidate_set_id': 'Governed S3b body site mapping '
                                                                           'provenance.',
                                         'body_site_map_competing_count': 'Governed S3b body site mapping '
                                                                          'provenance.',
                                         'body_site_map_component_count': 'Governed S3b body site mapping '
                                                                          'provenance.',
                                         'body_site_map_component_index': 'Governed S3b body site mapping '
                                                                          'provenance.',
                                         'body_site_map_confidence': 'Governed S3b body site mapping '
                                                                     'provenance.',
                                         'body_site_map_cosine': 'Governed S3b body site mapping provenance.',
                                         'body_site_map_method': 'Governed S3b body site mapping provenance.',
                                         'body_site_map_rollup_levels': 'Governed S3b body site mapping '
                                                                        'provenance.',
                                         'body_site_map_rule_id': 'Governed S3b body site mapping '
                                                                  'provenance.',
                                         'body_site_map_scoring_model': 'Governed S3b body site mapping '
                                                                        'provenance.',
                                         'body_site_map_status': 'Governed S3b body site mapping provenance.',
                                         'body_site_map_status_reason': 'Governed S3b body site mapping '
                                                                        'provenance.',
                                         'body_site_map_version': 'Governed S3b body site mapping '
                                                                  'provenance.',
                                         'body_site_omop_concept_id': 'Governed S3b body site mapping '
                                                                      'provenance.',
                                         'body_site_omop_source_concept_id': 'Governed S3b body site mapping '
                                                                             'provenance.',
                                         'body_site_snomed_code': 'Governed S3b body site mapping '
                                                                  'provenance.',
                                         'body_site_snomed_display': 'Governed S3b body site mapping '
                                                                     'provenance.',
                                         'body_site_source_code': 'Governed S3b body site mapping '
                                                                  'provenance.',
                                         'body_site_source_display': 'Governed S3b body site mapping '
                                                                     'provenance.',
                                         'body_site_source_system': 'Governed S3b body site mapping '
                                                                    'provenance.',
                                         'body_site_target_code': 'Governed S3b body site mapping '
                                                                  'provenance.',
                                         'body_site_target_display': 'Governed S3b body site mapping '
                                                                     'provenance.',
                                         'body_site_target_system': 'Governed S3b body site mapping '
                                                                    'provenance.',
                                         'confidentiality_code': 'Security classification when supplied.',
                                         'doc_response_key': 'PowerForm DOC_RESPONSE_KEY; native key for the '
                                                             'form_promotion arm.',
                                         'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                         'spine_encounter.encounter_id.',
                                         'event_datetime': 'Clinical start date and time of the event as '
                                                           'recorded on the Millennium CLINICAL_EVENT row.',
                                         'event_datetime_local': 'Local-time representation of the '
                                                                 'vital-sign measurement timestamp. Gold: '
                                                                 'NULL wherever Gold nulls event_datetime; '
                                                                 'where Gold repairs a millisecond-epoch '
                                                                 'event_datetime, the repaired time in '
                                                                 'Europe/London.',
                                         'event_datetime_status': 'registry_dated or registry_undated for '
                                                                  'registry observations; null on native '
                                                                  'rows.',
                                         'event_end_datetime': 'Measurement end timestamp.',
                                         'event_end_datetime_local': 'Local-time representation of the '
                                                                     'vital-sign measurement end timestamp. '
                                                                     'Gold: NULL wherever Gold nulls '
                                                                     'event_end_datetime.',
                                         'event_id': 'Millennium numeric EVENT_ID; native key for the '
                                                     'numeric_event arm.',
                                         'interpretation_code': 'Source interpretation code.',
                                         'interpretation_display': 'Source interpretation display.',
                                         'interpretation_map_candidate_count': 'Governed S3b interpretation '
                                                                               'mapping provenance.',
                                         'interpretation_map_candidate_set_id': 'Governed S3b interpretation '
                                                                                'mapping provenance.',
                                         'interpretation_map_competing_count': 'Governed S3b interpretation '
                                                                               'mapping provenance.',
                                         'interpretation_map_component_count': 'Governed S3b interpretation '
                                                                               'mapping provenance.',
                                         'interpretation_map_component_index': 'Governed S3b interpretation '
                                                                               'mapping provenance.',
                                         'interpretation_map_confidence': 'Governed S3b interpretation '
                                                                          'mapping provenance.',
                                         'interpretation_map_cosine': 'Governed S3b interpretation mapping '
                                                                      'provenance.',
                                         'interpretation_map_method': 'Governed S3b interpretation mapping '
                                                                      'provenance.',
                                         'interpretation_map_rollup_levels': 'Governed S3b interpretation '
                                                                             'mapping provenance.',
                                         'interpretation_map_rule_id': 'Governed S3b interpretation mapping '
                                                                       'provenance.',
                                         'interpretation_map_scoring_model': 'Governed S3b interpretation '
                                                                             'mapping provenance.',
                                         'interpretation_map_status': 'Governed S3b interpretation mapping '
                                                                      'provenance.',
                                         'interpretation_map_status_reason': 'Governed S3b interpretation '
                                                                             'mapping provenance.',
                                         'interpretation_map_version': 'Governed S3b interpretation mapping '
                                                                       'provenance.',
                                         'interpretation_omop_concept_id': 'Governed S3b interpretation '
                                                                           'mapping provenance.',
                                         'interpretation_omop_source_concept_id': 'Governed S3b '
                                                                                  'interpretation mapping '
                                                                                  'provenance.',
                                         'interpretation_snomed_code': 'Governed S3b interpretation mapping '
                                                                       'provenance.',
                                         'interpretation_snomed_display': 'Governed S3b interpretation '
                                                                          'mapping provenance.',
                                         'interpretation_source_code': 'Governed S3b interpretation mapping '
                                                                       'provenance.',
                                         'interpretation_source_display': 'Governed S3b interpretation '
                                                                          'mapping provenance.',
                                         'interpretation_source_system': 'Governed S3b interpretation '
                                                                         'mapping provenance.',
                                         'interpretation_target_code': 'Governed S3b interpretation mapping '
                                                                       'provenance.',
                                         'interpretation_target_display': 'Governed S3b interpretation '
                                                                          'mapping provenance.',
                                         'interpretation_target_system': 'Governed S3b interpretation '
                                                                         'mapping provenance.',
                                         'loaded_at': 'map_numeric_events.ADC_UPDT for numeric-event rows, '
                                                      'or map_powerform_assessment_item.ADC_UPDT for '
                                                      "form-promoted rows. Each arm's bronze load clock is "
                                                      'carried unchanged through its staging, union and '
                                                      'quality wrappers. No form-parent or '
                                                      'promotion-configuration clock is added; missing '
                                                      'values remain null and this is not Silver refresh '
                                                      'time.',
                                         'method_code': 'Source measurement method code.',
                                         'method_display': 'Source measurement method display.',
                                         'method_map_candidate_count': 'Governed S3b method mapping '
                                                                       'provenance.',
                                         'method_map_candidate_set_id': 'Governed S3b method mapping '
                                                                        'provenance.',
                                         'method_map_competing_count': 'Governed S3b method mapping '
                                                                       'provenance.',
                                         'method_map_component_count': 'Governed S3b method mapping '
                                                                       'provenance.',
                                         'method_map_component_index': 'Governed S3b method mapping '
                                                                       'provenance.',
                                         'method_map_confidence': 'Governed S3b method mapping provenance.',
                                         'method_map_cosine': 'Governed S3b method mapping provenance.',
                                         'method_map_method': 'Governed S3b method mapping provenance.',
                                         'method_map_rollup_levels': 'Governed S3b method mapping '
                                                                     'provenance.',
                                         'method_map_rule_id': 'Governed S3b method mapping provenance.',
                                         'method_map_scoring_model': 'Governed S3b method mapping '
                                                                     'provenance.',
                                         'method_map_status': 'Governed S3b method mapping provenance.',
                                         'method_map_status_reason': 'Governed S3b method mapping '
                                                                     'provenance.',
                                         'method_map_version': 'Governed S3b method mapping provenance.',
                                         'method_omop_concept_id': 'Governed S3b method mapping provenance.',
                                         'method_omop_source_concept_id': 'Governed S3b method mapping '
                                                                          'provenance.',
                                         'method_snomed_code': 'Governed S3b method mapping provenance.',
                                         'method_snomed_display': 'Governed S3b method mapping provenance.',
                                         'method_source_code': 'Governed S3b method mapping provenance.',
                                         'method_source_display': 'Governed S3b method mapping provenance.',
                                         'method_source_system': 'Governed S3b method mapping provenance.',
                                         'method_target_code': 'Governed S3b method mapping provenance.',
                                         'method_target_display': 'Governed S3b method mapping provenance.',
                                         'method_target_system': 'Governed S3b method mapping provenance.',
                                         'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                              'cross-feed join key.',
                                         'performer_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                      'the performer.',
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
                                         'reference_range_high': 'Reference-range upper bound.',
                                         'reference_range_low': 'Reference-range lower bound.',
                                         'registry_field_id': 'Governed SHA-256 registry product and field '
                                                              'identifier for registry observations; null on '
                                                              'native rows.',
                                         'result_status_code': 'Source result status code.',
                                         'result_status_display': 'Source result status display.',
                                         'source_code': 'Source vital code.',
                                         'source_coding_system': 'Coding system or source namespace in which '
                                                                 'the source code is defined for each vital '
                                                                 'sign record. It is produced by the silver '
                                                                 'transformation and has no direct '
                                                                 'bronze-column lineage entry. Source code '
                                                                 'meanings and sentinel values are retained '
                                                                 'unless the pipeline explicitly maps them; '
                                                                 'null means no code was supplied.',
                                         'source_dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID '
                                                                         'of the source form for promoted '
                                                                         'rows.',
                                         'source_display': 'Source vital display.',
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
                                         'unit_map_candidate_count': 'Governed S3b unit mapping provenance.',
                                         'unit_map_candidate_set_id': 'Governed S3b unit mapping provenance.',
                                         'unit_map_competing_count': 'Governed S3b unit mapping provenance.',
                                         'unit_map_component_count': 'Governed S3b unit mapping provenance.',
                                         'unit_map_component_index': 'Governed S3b unit mapping provenance.',
                                         'unit_map_confidence': 'Governed S3b unit mapping provenance.',
                                         'unit_map_cosine': 'Governed S3b unit mapping provenance.',
                                         'unit_map_method': 'Governed S3b unit mapping provenance.',
                                         'unit_map_rollup_levels': 'Governed S3b unit mapping provenance.',
                                         'unit_map_rule_id': 'Governed S3b unit mapping provenance.',
                                         'unit_map_scoring_model': 'Governed S3b unit mapping provenance.',
                                         'unit_map_status': 'Governed S3b unit mapping provenance.',
                                         'unit_map_status_reason': 'Governed S3b unit mapping provenance.',
                                         'unit_map_version': 'Governed S3b unit mapping provenance.',
                                         'unit_omop_concept_id': 'Governed S3b unit mapping provenance.',
                                         'unit_omop_source_concept_id': 'Governed S3b unit mapping '
                                                                        'provenance.',
                                         'unit_snomed_code': 'Governed S3b unit mapping provenance.',
                                         'unit_snomed_display': 'Governed S3b unit mapping provenance.',
                                         'unit_source_code': 'Governed S3b unit mapping provenance.',
                                         'unit_source_display': 'Governed S3b unit mapping provenance.',
                                         'unit_source_system': 'Governed S3b unit mapping provenance.',
                                         'unit_source_value': 'Source result unit.',
                                         'unit_target_code': 'Governed S3b unit mapping provenance.',
                                         'unit_target_display': 'Governed S3b unit mapping provenance.',
                                         'unit_target_system': 'Governed S3b unit mapping provenance.',
                                         'value_number': 'Numeric vital value.',
                                         'value_text': 'Verbatim result text.',
                                         'vip_ind': 'VIP indicator when supplied.',
                                         'vital_code': 'Source and mapped vital CodeableConcept.',
                                         'vital_omop_map_rule_id': 'Rule that filled vital_omop_concept_id / '
                                                                   'vital_omop_source_concept_id from the '
                                                                   "OMOP 'Maps to' lookup "
                                                                   '(omop:maps_to:<vocabulary>); NULL when '
                                                                   "those ids came with the row's own "
                                                                   'mapping. INSR_W2 W2.03.',
                                         'vital_omop_map_version': 'Vocabulary release '
                                                                   '(<vocabulary>|<version>) of the lookup '
                                                                   'that filled the vital OMOP ids; NULL '
                                                                   'when the lane filled nothing. INSR_W2 '
                                                                   'W2.03.',
                                         'withheld_identity_ind': 'Withheld identity indicator.'},
                             'quantity_override': True,
                             'table': 'Internal quality-controlled twin of clinical_vital_sign: the Gold '
                                      'repairs, nulls and expectations are applied here and the published '
                                      'Gold MV reads this object. Materialized rather than temporary so the '
                                      'published MV can refresh incrementally.'},
 'clinical_vital_sign': {'columns': {'body_site_code': 'Body-site code when supplied.',
                                     'body_site_display': 'Body-site display when supplied.',
                                     'body_site_map_candidate_count': 'Governed S3b body site mapping '
                                                                      'provenance.',
                                     'body_site_map_candidate_set_id': 'Governed S3b body site mapping '
                                                                       'provenance.',
                                     'body_site_map_competing_count': 'Governed S3b body site mapping '
                                                                      'provenance.',
                                     'body_site_map_component_count': 'Governed S3b body site mapping '
                                                                      'provenance.',
                                     'body_site_map_component_index': 'Governed S3b body site mapping '
                                                                      'provenance.',
                                     'body_site_map_confidence': 'Governed S3b body site mapping provenance.',
                                     'body_site_map_cosine': 'Governed S3b body site mapping provenance.',
                                     'body_site_map_method': 'Governed S3b body site mapping provenance.',
                                     'body_site_map_rollup_levels': 'Governed S3b body site mapping '
                                                                    'provenance.',
                                     'body_site_map_rule_id': 'Governed S3b body site mapping provenance.',
                                     'body_site_map_scoring_model': 'Governed S3b body site mapping '
                                                                    'provenance.',
                                     'body_site_map_status': 'Governed S3b body site mapping provenance.',
                                     'body_site_map_status_reason': 'Governed S3b body site mapping '
                                                                    'provenance.',
                                     'body_site_map_version': 'Governed S3b body site mapping provenance.',
                                     'body_site_omop_concept_id': 'Governed S3b body site mapping '
                                                                  'provenance.',
                                     'body_site_omop_source_concept_id': 'Governed S3b body site mapping '
                                                                         'provenance.',
                                     'body_site_snomed_code': 'Governed S3b body site mapping provenance.',
                                     'body_site_snomed_display': 'Governed S3b body site mapping provenance.',
                                     'body_site_source_code': 'Governed S3b body site mapping provenance.',
                                     'body_site_source_display': 'Governed S3b body site mapping provenance.',
                                     'body_site_source_system': 'Governed S3b body site mapping provenance.',
                                     'body_site_target_code': 'Governed S3b body site mapping provenance.',
                                     'body_site_target_display': 'Governed S3b body site mapping provenance.',
                                     'body_site_target_system': 'Governed S3b body site mapping provenance.',
                                     'confidentiality_code': 'Security classification when supplied.',
                                     'doc_response_key': 'PowerForm DOC_RESPONSE_KEY; native key for the '
                                                         'form_promotion arm.',
                                     'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                     'spine_encounter.encounter_id.',
                                     'event_datetime': 'Clinical start date and time of the event as '
                                                       'recorded on the Millennium CLINICAL_EVENT row.',
                                     'event_datetime_local': 'Local-time representation of the vital-sign '
                                                             'measurement timestamp. Gold: NULL wherever '
                                                             'Gold nulls event_datetime; where Gold repairs '
                                                             'a millisecond-epoch event_datetime, the '
                                                             'repaired time in Europe/London.',
                                     'event_datetime_status': 'registry_dated or registry_undated for '
                                                              'registry observations; null on native rows.',
                                     'event_end_datetime': 'Measurement end timestamp.',
                                     'event_end_datetime_local': 'Local-time representation of the '
                                                                 'vital-sign measurement end timestamp. '
                                                                 'Gold: NULL wherever Gold nulls '
                                                                 'event_end_datetime.',
                                     'event_id': 'Millennium numeric EVENT_ID; native key for the '
                                                 'numeric_event arm.',
                                     'interpretation_code': 'Source interpretation code.',
                                     'interpretation_display': 'Source interpretation display.',
                                     'interpretation_map_candidate_count': 'Governed S3b interpretation '
                                                                           'mapping provenance.',
                                     'interpretation_map_candidate_set_id': 'Governed S3b interpretation '
                                                                            'mapping provenance.',
                                     'interpretation_map_competing_count': 'Governed S3b interpretation '
                                                                           'mapping provenance.',
                                     'interpretation_map_component_count': 'Governed S3b interpretation '
                                                                           'mapping provenance.',
                                     'interpretation_map_component_index': 'Governed S3b interpretation '
                                                                           'mapping provenance.',
                                     'interpretation_map_confidence': 'Governed S3b interpretation mapping '
                                                                      'provenance.',
                                     'interpretation_map_cosine': 'Governed S3b interpretation mapping '
                                                                  'provenance.',
                                     'interpretation_map_method': 'Governed S3b interpretation mapping '
                                                                  'provenance.',
                                     'interpretation_map_rollup_levels': 'Governed S3b interpretation '
                                                                         'mapping provenance.',
                                     'interpretation_map_rule_id': 'Governed S3b interpretation mapping '
                                                                   'provenance.',
                                     'interpretation_map_scoring_model': 'Governed S3b interpretation '
                                                                         'mapping provenance.',
                                     'interpretation_map_status': 'Governed S3b interpretation mapping '
                                                                  'provenance.',
                                     'interpretation_map_status_reason': 'Governed S3b interpretation '
                                                                         'mapping provenance.',
                                     'interpretation_map_version': 'Governed S3b interpretation mapping '
                                                                   'provenance.',
                                     'interpretation_omop_concept_id': 'Governed S3b interpretation mapping '
                                                                       'provenance.',
                                     'interpretation_omop_source_concept_id': 'Governed S3b interpretation '
                                                                              'mapping provenance.',
                                     'interpretation_snomed_code': 'Governed S3b interpretation mapping '
                                                                   'provenance.',
                                     'interpretation_snomed_display': 'Governed S3b interpretation mapping '
                                                                      'provenance.',
                                     'interpretation_source_code': 'Governed S3b interpretation mapping '
                                                                   'provenance.',
                                     'interpretation_source_display': 'Governed S3b interpretation mapping '
                                                                      'provenance.',
                                     'interpretation_source_system': 'Governed S3b interpretation mapping '
                                                                     'provenance.',
                                     'interpretation_target_code': 'Governed S3b interpretation mapping '
                                                                   'provenance.',
                                     'interpretation_target_display': 'Governed S3b interpretation mapping '
                                                                      'provenance.',
                                     'interpretation_target_system': 'Governed S3b interpretation mapping '
                                                                     'provenance.',
                                     'loaded_at': 'map_numeric_events.ADC_UPDT for numeric-event rows, or '
                                                  'map_powerform_assessment_item.ADC_UPDT for form-promoted '
                                                  "rows. Each arm's bronze load clock is carried unchanged "
                                                  'through its staging, union and quality wrappers. No '
                                                  'form-parent or promotion-configuration clock is added; '
                                                  'missing values remain null and this is not Silver refresh '
                                                  'time.',
                                     'method_code': 'Source measurement method code.',
                                     'method_display': 'Source measurement method display.',
                                     'method_map_candidate_count': 'Governed S3b method mapping provenance.',
                                     'method_map_candidate_set_id': 'Governed S3b method mapping provenance.',
                                     'method_map_competing_count': 'Governed S3b method mapping provenance.',
                                     'method_map_component_count': 'Governed S3b method mapping provenance.',
                                     'method_map_component_index': 'Governed S3b method mapping provenance.',
                                     'method_map_confidence': 'Governed S3b method mapping provenance.',
                                     'method_map_cosine': 'Governed S3b method mapping provenance.',
                                     'method_map_method': 'Governed S3b method mapping provenance.',
                                     'method_map_rollup_levels': 'Governed S3b method mapping provenance.',
                                     'method_map_rule_id': 'Governed S3b method mapping provenance.',
                                     'method_map_scoring_model': 'Governed S3b method mapping provenance.',
                                     'method_map_status': 'Governed S3b method mapping provenance.',
                                     'method_map_status_reason': 'Governed S3b method mapping provenance.',
                                     'method_map_version': 'Governed S3b method mapping provenance.',
                                     'method_omop_concept_id': 'Governed S3b method mapping provenance.',
                                     'method_omop_source_concept_id': 'Governed S3b method mapping '
                                                                      'provenance.',
                                     'method_snomed_code': 'Governed S3b method mapping provenance.',
                                     'method_snomed_display': 'Governed S3b method mapping provenance.',
                                     'method_source_code': 'Governed S3b method mapping provenance.',
                                     'method_source_display': 'Governed S3b method mapping provenance.',
                                     'method_source_system': 'Governed S3b method mapping provenance.',
                                     'method_target_code': 'Governed S3b method mapping provenance.',
                                     'method_target_display': 'Governed S3b method mapping provenance.',
                                     'method_target_system': 'Governed S3b method mapping provenance.',
                                     'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                          'cross-feed join key.',
                                     'performer_practitioner_id': 'Millennium personnel PERSON_ID for the '
                                                                  'performer.',
                                     'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                     'promotion_rule_id': 'Governed promotion-rule identifier.',
                                     'record_status': 'Arm-specific row status. Numeric events are retracted '
                                                      'when map_numeric_events.SOURCE_DELETED_IND is true '
                                                      '(null defaults false), otherwise superseded when '
                                                      'CLINICAL_EVENT_VALID_UNTIL_DT_TM is before '
                                                      '2100-01-01, otherwise active. Form-promoted rows are '
                                                      'retracted when CANONICAL_SOURCE_DELETED_IND is true '
                                                      'or SOURCE_PRESENT_IND is false or missing; otherwise '
                                                      'superseded when ACTIVE_IND is zero or missing, '
                                                      'otherwise active. Retraction has priority over '
                                                      'supersession; this is not the clinical result-status '
                                                      'code.',
                                     'record_status_effective_from': 'map_numeric_events.CLINICAL_EVENT_VALID_FROM_DT_TM '
                                                                     'on the numeric-event arm; '
                                                                     'map_powerform_assessment_item.FIRST_DOCUMENTED_DT_TM '
                                                                     'on the form-promotion arm. Each is '
                                                                     'carried unchanged, with no '
                                                                     'performed/response/ingestion-time '
                                                                     'fallback; a missing value remains '
                                                                     'null.',
                                     'record_status_effective_to': 'Numeric events return '
                                                                   'CLINICAL_EVENT_VALID_UNTIL_DT_TM when '
                                                                   'deleted or ended, otherwise null; '
                                                                   'deletion can therefore retain a '
                                                                   'far-future end value. Form-promoted rows '
                                                                   'return LAST_DOCUMENTED_DT_TM when '
                                                                   'deleted, absent or inactive, otherwise '
                                                                   'null. No missing-end fallback is '
                                                                   'applied, and the form documentation end '
                                                                   'is not an independently observed '
                                                                   'status-transition time.',
                                     'reference_range_high': 'Reference-range upper bound.',
                                     'reference_range_low': 'Reference-range lower bound.',
                                     'registry_field_id': 'Governed SHA-256 registry product and field '
                                                          'identifier for registry observations; null on '
                                                          'native rows.',
                                     'result_status_code': 'Source result status code.',
                                     'result_status_display': 'Source result status display.',
                                     'source_code': 'Source vital code.',
                                     'source_coding_system': 'Coding system or source namespace in which the '
                                                             'source code is defined for each vital sign '
                                                             'record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Source code meanings and '
                                                             'sentinel values are retained unless the '
                                                             'pipeline explicitly maps them; null means no '
                                                             'code was supplied.',
                                     'source_dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID of '
                                                                     'the source form for promoted rows.',
                                     'source_display': 'Source vital display.',
                                     'source_feed': 'Registered source route.',
                                     'source_form_key': 'Deterministic SHA-256 key of the source PowerForm '
                                                        'instance for promoted rows.',
                                     'source_object': 'Native source arm: numeric_event or form_promotion.',
                                     'source_update_timestamp': 'For numeric events, greatest of '
                                                                'map_numeric_events.STRING_RESULT_UPDT_DT_TM, '
                                                                'CLINICAL_EVENT_UPDT_DT_TM and ADC_UPDT. For '
                                                                'form-promoted rows, '
                                                                'map_powerform_assessment_item.ADC_UPDT '
                                                                'directly. This mixes source and ingestion '
                                                                'provenance rather than uniformly '
                                                                'representing a native modification clock; '
                                                                'the promotion lookup adds no clock and '
                                                                'Silver refresh time is not substituted.',
                                     'subject_id_system': 'Identifier system used for subject_key.',
                                     'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                    'identifier. Not salted, not secret; a stable join key '
                                                    'across feeds where person_id is unresolved.',
                                     'unit_concept_id': 'OMOP unit concept of the value. Body weight, height '
                                                        "and BMI rows with no source unit take the concept's "
                                                        'default unit (kg, cm, kg/m2; unit_map_rule_id '
                                                        'vital:concept_default_unit); see '
                                                        'canonical_conversion.',
                                     'unit_map_candidate_count': 'Governed S3b unit mapping provenance.',
                                     'unit_map_candidate_set_id': 'Governed S3b unit mapping provenance.',
                                     'unit_map_competing_count': 'Governed S3b unit mapping provenance.',
                                     'unit_map_component_count': 'Governed S3b unit mapping provenance.',
                                     'unit_map_component_index': 'Governed S3b unit mapping provenance.',
                                     'unit_map_confidence': 'Governed S3b unit mapping provenance.',
                                     'unit_map_cosine': 'Governed S3b unit mapping provenance.',
                                     'unit_map_method': 'Governed S3b unit mapping provenance.',
                                     'unit_map_rollup_levels': 'Governed S3b unit mapping provenance.',
                                     'unit_map_rule_id': 'Governed S3b unit mapping provenance.',
                                     'unit_map_scoring_model': 'Governed S3b unit mapping provenance.',
                                     'unit_map_status': 'Governed S3b unit mapping provenance.',
                                     'unit_map_status_reason': 'Governed S3b unit mapping provenance.',
                                     'unit_map_version': 'Governed S3b unit mapping provenance.',
                                     'unit_omop_concept_id': 'Governed S3b unit mapping provenance.',
                                     'unit_omop_source_concept_id': 'Governed S3b unit mapping provenance.',
                                     'unit_snomed_code': 'Governed S3b unit mapping provenance.',
                                     'unit_snomed_display': 'Governed S3b unit mapping provenance.',
                                     'unit_source_code': 'Governed S3b unit mapping provenance.',
                                     'unit_source_display': 'Governed S3b unit mapping provenance.',
                                     'unit_source_system': 'Governed S3b unit mapping provenance.',
                                     'unit_source_value': 'Source result unit.',
                                     'unit_target_code': 'Governed S3b unit mapping provenance.',
                                     'unit_target_display': 'Governed S3b unit mapping provenance.',
                                     'unit_target_system': 'Governed S3b unit mapping provenance.',
                                     'value_number': 'Numeric vital value.',
                                     'value_text': 'Verbatim result text.',
                                     'vip_ind': 'VIP indicator when supplied.',
                                     'vital_code': 'Source and mapped vital CodeableConcept.',
                                     'vital_omop_map_rule_id': 'Rule that filled vital_omop_concept_id / '
                                                               'vital_omop_source_concept_id from the OMOP '
                                                               "'Maps to' lookup "
                                                               '(omop:maps_to:<vocabulary>); NULL when those '
                                                               "ids came with the row's own mapping. INSR_W2 "
                                                               'W2.03.',
                                     'vital_omop_map_version': 'Vocabulary release (<vocabulary>|<version>) '
                                                               'of the lookup that filled the vital OMOP '
                                                               'ids; NULL when the lane filled nothing. '
                                                               'INSR_W2 W2.03.',
                                     'withheld_identity_ind': 'Withheld identity indicator.'},
                         'quantity_override': False,
                         'table': 'One native or form-promoted vital measurement. Gold QC twin of the silver '
                                  'product: 8 columns are repaired or nulled, 1 rule(s) drop rows, 4 '
                                  'check(s) are advisory. Each rule states its reason in the pipeline '
                                  'notebook, and Lakeflow expectation metrics report what every rule matched '
                                  'on each update.'}}
