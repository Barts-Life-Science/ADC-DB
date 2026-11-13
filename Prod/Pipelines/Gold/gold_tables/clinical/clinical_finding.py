"""Gold clinical.clinical_finding: 2 dataset(s).

- datasets: _qc_clinical_clinical_finding, clinical_clinical_finding
- moved verbatim from: 2_gold_clinical_events:239, 2_gold_clinical_events:285, 2_gold_clinical_events:292, 2_gold_clinical_events:300, 2_gold_clinical_events:394, 2_gold_clinical_events:399, 2_gold_clinical_events:404, 2_gold_clinical_events:418

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.clinical_finding ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CLINICAL_FINDING_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`event_id` AS `event_id`',
    '`sequence_nbr` AS `sequence_nbr`',
    '`source_object` AS `source_object`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    "CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`event_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_datetime` AS DATE)) < 9999 OR CAST(`event_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`event_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_datetime` END END AS `event_datetime`",
    "CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_end_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`event_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_end_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_end_datetime` END END AS `event_end_datetime`",
    "CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`event_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_datetime` AS DATE)) < 9999 OR CAST(`event_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`event_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_datetime` END END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`",  # GOLD_COLUMN_PARITY_V1
    "CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_end_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`event_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_end_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_end_datetime` END END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`",  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`finding_code` AS `finding_code`',
    '`finding_source_system` AS `finding_source_system`',  # SRC_LINEAGE_V1
    '`finding_source_code` AS `finding_source_code`',  # SRC_LINEAGE_V1
    '`finding_source_display` AS `finding_source_display`',  # SRC_LINEAGE_V1
    '`value_source_system` AS `value_source_system`',  # SRC_LINEAGE_V1
    '`value_source_code` AS `value_source_code`',  # SRC_LINEAGE_V1
    '`value_source_display` AS `value_source_display`',  # SRC_LINEAGE_V1
    '`finding_kind` AS `finding_kind`',
    "CASE WHEN TRIM(CAST(`value_text` AS STRING)) = '' THEN NULL ELSE `value_text` END AS `value_text`",
    "CASE WHEN CAST(`value_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`value_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`value_datetime` AS DATE)) < 9999 OR CAST(`value_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`value_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`value_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `value_datetime` END END AS `value_datetime`",
    '`value_code` AS `value_code`',
    '`value_display` AS `value_display`',
    "CASE WHEN UPPER(TRIM(CAST(`normalcy_code` AS STRING))) = '0' THEN NULL ELSE `normalcy_code` END AS `normalcy_code`",
    '`normalcy_display` AS `normalcy_display`',
    '`result_status_code` AS `result_status_code`',
    '`result_status_display` AS `result_status_display`',
    '`order_id` AS `order_id`',
    '`parent_event_id` AS `parent_event_id`',
    # INSR_W3 W3.06: Gold: NULL where the Cerner system user id 1 (J5; silver publishes the flag).
    "CASE WHEN CAST(`performer_practitioner_id` AS BIGINT) = 1 THEN NULL ELSE `performer_practitioner_id` END AS `performer_practitioner_id`",
    # INSR_W3 W3.06: Gold: NULL where the Cerner system user id 1 (J5; silver publishes the flag).
    "CASE WHEN CAST(`verifier_practitioner_id` AS BIGINT) = 1 THEN NULL ELSE `verifier_practitioner_id` END AS `verifier_practitioner_id`",
    '`organization_id` AS `organization_id`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    # INSR_W3 W3.05: Gold: NULL where an open-end marker in [2099-12-31, 9999).
    "CASE WHEN (CAST(`record_status_effective_to` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`record_status_effective_to` AS DATE)) < 9999) THEN NULL ELSE `record_status_effective_to` END AS `record_status_effective_to`",
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
    '`registry_field_id` AS `registry_field_id`',
    '`event_datetime_status` AS `event_datetime_status`',
]


CLINICAL_CLINICAL_FINDING_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`value_time` AS `value_time`',
    '`performer_system_ind` AS `performer_system_ind`',
    '`verifier_system_ind` AS `verifier_system_ind`',
]


# contract v2: QC/batch inputs come from internal _clinical_clinical_finding_metadata; source history stays on the main research table.
CLINICAL_CLINICAL_FINDING_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 1,591,253,116 rows of
    # 1,591,253,116 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_CLINICAL_FINDING_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.clinical_finding.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # INSR_W4 W4.05 date_future (W405_F5d_obs): Observation classes (nomen, text, coded, registry) cannot be charted after their own load (J12, 0-day horizon).
    'gold.clinical.clinical_finding.table.insr_f5d_observation_future':
        "NOT COALESCE((`source_object` <> 'date_event' AND `event_datetime` > `loaded_at`), FALSE)",
    # INSR_W4 W4.05 date_future (W405_F5d_date): Date events are planned/expiry dates (discharge, review, surgery, expiry): 90-day horizon (J12).
    'gold.clinical.clinical_finding.table.insr_f5d_schedule_future':
        "NOT COALESCE((`source_object` = 'date_event' AND `event_datetime` > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 1,591,253,116 at the profile.
    "gold.clinical.clinical_finding.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 250,156 of 1,591,253,116 rows (0.0157%) when profiled on
    # 2026-08-24.
    "gold.clinical.clinical_finding.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 29,903,501 of 1,591,253,116 rows (1.88%) when profiled on
    # 2026-08-24.
    "gold.clinical.clinical_finding.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # Left as a warning because it fires on 42,016,359 of 1,591,253,116 rows (2.64%) when
    # profiled on 2026-08-24 -- at that rate the rule's assumption about what event_datetime
    # and event_end_datetime mean is the thing in doubt, not the data. The inverted gaps are
    # mostly minutes, which reads as two clocks rather than two events in the wrong order.
    "gold.clinical.clinical_finding.table.ordering_violation_event_datetime_event_end_datetime":
        "NOT COALESCE((`event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime`), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_CLINICAL_FINDING_PERSON_SCOPE_RULES = {
    "gold.clinical.clinical_finding.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.06: warn_row rules (J4); rows are kept.
CLINICAL_CLINICAL_FINDING_W3_06_ADVISORY_RULES = {
    "gold.clinical.clinical_finding.event_datetime.d2000_01_01_time_only": "NOT COALESCE(CAST(`event_datetime` AS DATE) = DATE'2000-01-01', FALSE)",
    "gold.clinical.clinical_finding.event_datetime_local.d2000_01_01_time_only": "NOT COALESCE(CAST(`event_datetime_local` AS DATE) = DATE'2000-01-01', FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_clinical_finding"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_CLINICAL_FINDING_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CLINICAL_FINDING_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_CLINICAL_FINDING_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_CLINICAL_FINDING_W3_06_ADVISORY_RULES)
def _gold_qc_clinical_clinical_finding():
    """Quality-controlled twin of journey_clinical.clinical_finding."""
    df = _qc("clinical_clinical_finding", CLINICAL_CLINICAL_FINDING_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.clinical_finding"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_clinical_finding():
    """Contract-v2 public twin of clinical_clinical_finding; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_clinical_finding")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`event_id` AS `event_id`',
        '`sequence_nbr` AS `sequence_nbr`',
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
        '`__gold_json_finding_code` AS `finding_code`',
        '`finding_source_system` AS `finding_source_system`',  # SRC_LINEAGE_V1
        '`finding_source_code` AS `finding_source_code`',  # SRC_LINEAGE_V1
        '`finding_source_display` AS `finding_source_display`',  # SRC_LINEAGE_V1
        '`value_source_system` AS `value_source_system`',  # SRC_LINEAGE_V1
        '`value_source_code` AS `value_source_code`',  # SRC_LINEAGE_V1
        '`value_source_display` AS `value_source_display`',  # SRC_LINEAGE_V1
        '`finding_kind` AS `finding_kind`',
        '`value_text` AS `value_text`',
        '`value_datetime` AS `value_datetime`',
        '`value_code` AS `value_code`',
        '`value_display` AS `value_display`',
        '`normalcy_code` AS `normalcy_code`',
        '`normalcy_display` AS `normalcy_display`',
        '`result_status_code` AS `result_status_code`',
        '`result_status_display` AS `result_status_display`',
        '`order_id` AS `order_id`',
        '`parent_event_id` AS `parent_event_id`',
        '`performer_practitioner_id` AS `performer_practitioner_id`',
        '`verifier_practitioner_id` AS `verifier_practitioner_id`',
        '`organization_id` AS `organization_id`',
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
        '`value_time` AS `value_time`',  # INSR_W3 W3.12
        '`performer_system_ind` AS `performer_system_ind`',  # INSR_W3 W3.12
        '`verifier_system_ind` AS `verifier_system_ind`',  # INSR_W3 W3.12
    )
    return _s3_flatten_gold_public(df, "clinical_clinical_finding")


def _public_steps_clinical_clinical_finding(df):
    """clinical_clinical_finding: the public body after its projection (GoldRules.public_select), moved from gold_clinical_clinical_finding; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_clinical_finding')


RULES = {
    'clinical_clinical_finding': GoldRules(
        table='clinical_clinical_finding',
        select=CLINICAL_CLINICAL_FINDING_SELECT,
        mandatory=CLINICAL_CLINICAL_FINDING_MANDATORY_RULES,
        advisory=CLINICAL_CLINICAL_FINDING_ADVISORY_RULES,
        person_scope=CLINICAL_CLINICAL_FINDING_PERSON_SCOPE_RULES,
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
        variant_columns=('finding_code',),
        s3_axis_specs={'finding_code': [('finding', False), ('value', False)]},
        value_rules={'gold.clinical.clinical_finding.finding_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                      'column': 'finding_snomed_code',
                                                                                      'companions': ['finding_snomed_display',
                                                                                                     'finding_target_code',
                                                                                                     'finding_target_display'],
                                                                                      'match': '`finding_snomed_code` '
                                                                                               'IS NOT '
                                                                                               'NULL AND '
                                                                                               '(NOT '
                                                                                               '`finding_snomed_code` '
                                                                                               'RLIKE '
                                                                                               "'^[1-9][0-9]{5,17}$' "
                                                                                               'OR '
                                                                                               'substring(`finding_snomed_code`, '
                                                                                               'length(`finding_snomed_code`) '
                                                                                               '- 2, 2) '
                                                                                               'NOT IN '
                                                                                               "('00', "
                                                                                               "'10') OR "
                                                                                               '(substring(`finding_snomed_code`, '
                                                                                               'length(`finding_snomed_code`) '
                                                                                               '- 2, 2) = '
                                                                                               "'10' AND "
                                                                                               'length(`finding_snomed_code`) '
                                                                                               '< 11))',
                                                                                      'table': 'clinical_clinical_finding'},
         'gold.clinical.clinical_finding.finding_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                      'column': 'finding_target_code',
                                                                                      'companions': ['finding_target_display',
                                                                                                     'finding_snomed_code',
                                                                                                     'finding_snomed_display'],
                                                                                      'match': '`finding_target_system` '
                                                                                               '= '
                                                                                               "'http://snomed.info/sct' "
                                                                                               'AND '
                                                                                               '`finding_target_code` '
                                                                                               'IS NOT '
                                                                                               'NULL AND '
                                                                                               '(NOT '
                                                                                               '`finding_target_code` '
                                                                                               'RLIKE '
                                                                                               "'^[1-9][0-9]{5,17}$' "
                                                                                               'OR '
                                                                                               'substring(`finding_target_code`, '
                                                                                               'length(`finding_target_code`) '
                                                                                               '- 2, 2) '
                                                                                               'NOT IN '
                                                                                               "('00', "
                                                                                               "'10') OR "
                                                                                               '(substring(`finding_target_code`, '
                                                                                               'length(`finding_target_code`) '
                                                                                               '- 2, 2) = '
                                                                                               "'10' AND "
                                                                                               'length(`finding_target_code`) '
                                                                                               '< 11))',
                                                                                      'table': 'clinical_clinical_finding'},
         'gold.clinical.clinical_finding.order_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                     'column': 'order_id',
                                                                     'companions': [],
                                                                     'match': "TRIM(`order_id`) = '0'",
                                                                     'table': 'clinical_clinical_finding'},
         'gold.clinical.clinical_finding.organization_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                            'column': 'organization_id',
                                                                            'companions': [],
                                                                            'match': '`organization_id` = '
                                                                                     '0',
                                                                            'table': 'clinical_clinical_finding'},
         'gold.clinical.clinical_finding.performer_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                      'column': 'performer_practitioner_id',
                                                                                      'companions': [],
                                                                                      'match': '`performer_practitioner_id` '
                                                                                               '= 0',
                                                                                      'table': 'clinical_clinical_finding'},
         'gold.clinical.clinical_finding.value_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                    'column': 'value_snomed_code',
                                                                                    'companions': ['value_snomed_display',
                                                                                                   'value_target_code',
                                                                                                   'value_target_display'],
                                                                                    'match': '`value_snomed_code` '
                                                                                             'IS NOT NULL '
                                                                                             'AND (NOT '
                                                                                             '`value_snomed_code` '
                                                                                             'RLIKE '
                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                             'OR '
                                                                                             'substring(`value_snomed_code`, '
                                                                                             'length(`value_snomed_code`) '
                                                                                             '- 2, 2) NOT '
                                                                                             "IN ('00', "
                                                                                             "'10') OR "
                                                                                             '(substring(`value_snomed_code`, '
                                                                                             'length(`value_snomed_code`) '
                                                                                             '- 2, 2) = '
                                                                                             "'10' AND "
                                                                                             'length(`value_snomed_code`) '
                                                                                             '< 11))',
                                                                                    'table': 'clinical_clinical_finding'},
         'gold.clinical.clinical_finding.value_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                    'column': 'value_target_code',
                                                                                    'companions': ['value_target_display',
                                                                                                   'value_snomed_code',
                                                                                                   'value_snomed_display'],
                                                                                    'match': '`value_target_system` '
                                                                                             '= '
                                                                                             "'http://snomed.info/sct' "
                                                                                             'AND '
                                                                                             '`value_target_code` '
                                                                                             'IS NOT NULL '
                                                                                             'AND (NOT '
                                                                                             '`value_target_code` '
                                                                                             'RLIKE '
                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                             'OR '
                                                                                             'substring(`value_target_code`, '
                                                                                             'length(`value_target_code`) '
                                                                                             '- 2, 2) NOT '
                                                                                             "IN ('00', "
                                                                                             "'10') OR "
                                                                                             '(substring(`value_target_code`, '
                                                                                             'length(`value_target_code`) '
                                                                                             '- 2, 2) = '
                                                                                             "'10' AND "
                                                                                             'length(`value_target_code`) '
                                                                                             '< 11))',
                                                                                    'table': 'clinical_clinical_finding'},
         'gold.clinical.clinical_finding.verifier_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                     'column': 'verifier_practitioner_id',
                                                                                     'companions': [],
                                                                                     'match': '`verifier_practitioner_id` '
                                                                                              '= 0',
                                                                                     'table': 'clinical_clinical_finding'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`event_id` AS `event_id`',
            '`sequence_nbr` AS `sequence_nbr`',
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
            '`__gold_json_finding_code` AS `finding_code`',
            '`finding_source_system` AS `finding_source_system`',  # SRC_LINEAGE_V1
            '`finding_source_code` AS `finding_source_code`',  # SRC_LINEAGE_V1
            '`finding_source_display` AS `finding_source_display`',  # SRC_LINEAGE_V1
            '`value_source_system` AS `value_source_system`',  # SRC_LINEAGE_V1
            '`value_source_code` AS `value_source_code`',  # SRC_LINEAGE_V1
            '`value_source_display` AS `value_source_display`',  # SRC_LINEAGE_V1
            '`finding_kind` AS `finding_kind`',
            '`value_text` AS `value_text`',
            '`value_datetime` AS `value_datetime`',
            '`value_code` AS `value_code`',
            '`value_display` AS `value_display`',
            '`normalcy_code` AS `normalcy_code`',
            '`normalcy_display` AS `normalcy_display`',
            '`result_status_code` AS `result_status_code`',
            '`result_status_display` AS `result_status_display`',
            '`order_id` AS `order_id`',
            '`parent_event_id` AS `parent_event_id`',
            '`performer_practitioner_id` AS `performer_practitioner_id`',
            '`verifier_practitioner_id` AS `verifier_practitioner_id`',
            '`organization_id` AS `organization_id`',
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
            '`value_time` AS `value_time`',
            '`performer_system_ind` AS `performer_system_ind`',
            '`verifier_system_ind` AS `verifier_system_ind`',
        ],
        public_steps=(_public_steps_clinical_clinical_finding,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_clinical_finding': {'columns': {'confidentiality_code': 'Source confidentiality code.',
                                               'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                               'spine_encounter.encounter_id.',
                                               'event_datetime': 'Clinically relevant finding timestamp.',
                                               'event_datetime_local': 'Local-time representation of the '
                                                                       'clinically relevant timestamp for '
                                                                       'the clinical finding. Gold: NULL '
                                                                       'wherever Gold nulls event_datetime.',
                                               'event_datetime_status': 'registry_dated or registry_undated '
                                                                        'for registry observations; null on '
                                                                        'native rows.',
                                               'event_end_datetime': 'Source effective end timestamp.',
                                               'event_end_datetime_local': 'Local-time representation of the '
                                                                           'source effective end timestamp '
                                                                           'for the clinical finding. Gold: '
                                                                           'NULL wherever Gold nulls '
                                                                           'event_end_datetime.',
                                               'event_id': 'Millennium EVENT_ID; native key for coded, '
                                                           'nomenclature, date, and text event arms.',
                                               'finding_code': 'Source finding CodeableConcept.',
                                               'finding_kind': 'Generic result representation carried by the '
                                                               'source feed.',
                                               'loaded_at': 'Own ADC_UPDT from map_coded_events, '
                                                            'map_nomen_events, map_date_events or '
                                                            'map_text_events, carried per arm through '
                                                            'routing/usable-code filters, the union and '
                                                            'QC/public projections. The additional result, '
                                                            'clinical-event, lookup and long-text clocks '
                                                            'used by source_update_timestamp are not added '
                                                            'here; no current Silver refresh timestamp is '
                                                            'substituted.',
                                               'normalcy_code': 'Source normalcy or interpretation code.',
                                               'normalcy_display': 'Source normalcy or interpretation '
                                                                   'display.',
                                               'order_id': 'Source order identifier when supplied.',
                                               'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                                  'organization.',
                                               'parent_event_id': 'Source parent-event identifier.',
                                               'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                    'stable cross-feed join key.',
                                               'performer_practitioner_id': 'Millennium personnel PERSON_ID '
                                                                            'for the performer.',
                                               'performer_system_ind': 'True when the source '
                                                                       'performer_practitioner_id was 1, the '
                                                                       'Cerner system user (J5); the id is '
                                                                       'then published as NULL. NULL when '
                                                                       'the source id was NULL. INSR_W2.',
                                               'person_id': 'Millennium PERSON_ID; native person foreign '
                                                            'key.',
                                               'record_status': 'Coded-event rows are superseded only when '
                                                                'the least CR_VALID_UNTIL_DT_TM and '
                                                                'CE_VALID_UNTIL_DT_TM is non-null and before '
                                                                '2100-01-01; otherwise active, never '
                                                                'retracted. Nomenclature, date and text rows '
                                                                'are retracted for SOURCE_DELETED_IND cast '
                                                                'to BOOLEAN true (null defaults false); '
                                                                'otherwise superseded for the least '
                                                                'result/event validity end being non-null '
                                                                'and before 2100-01-01 or AUTHENTIC_FLAG '
                                                                'cast to LONG zero (null defaults one); '
                                                                'otherwise active. Retraction takes '
                                                                'priority, and clinical result-status codes '
                                                                'are separate.',
                                               'record_status_effective_from': 'First non-null '
                                                                               'result-valid-from, '
                                                                               'event-valid-from, then '
                                                                               'clinical event time. '
                                                                               'Coded/nomenclature use '
                                                                               'CR_VALID_FROM_DT_TM and '
                                                                               'CE_VALID_FROM_DT_TM; date '
                                                                               'uses '
                                                                               'DATE_RESULT_VALID_FROM_DT_TM '
                                                                               'and '
                                                                               'CLINICAL_EVENT_VALID_FROM_DT_TM; '
                                                                               'text substitutes '
                                                                               'STRING_RESULT_VALID_FROM_DT_TM. '
                                                                               'Coded event time falls back '
                                                                               'through PERFORMED_DT_TM, '
                                                                               'EVENT_END_DT_TM, '
                                                                               'EVENT_START_DT_TM. '
                                                                               'Nomenclature uses '
                                                                               'CLINICAL_EVENT_DT_TM, '
                                                                               'EVENT_END_DT_TM, '
                                                                               'PERFORMED_DT_TM, '
                                                                               'EVENT_START_DT_TM. Date/text '
                                                                               'use RESULT_DT_TM, '
                                                                               'PERFORMED_DT_TM, '
                                                                               'EVENT_END_DT_TM, '
                                                                               'EVENT_START_DT_TM. No '
                                                                               'status-bound clamp or '
                                                                               'ingestion-time fallback is '
                                                                               'added.',
                                               'record_status_effective_to': 'Coded rows use the least '
                                                                             'CR_VALID_UNTIL_DT_TM and '
                                                                             'CE_VALID_UNTIL_DT_TM only when '
                                                                             'superseded, otherwise null. '
                                                                             'Nomenclature, date and text '
                                                                             'rows use their computed '
                                                                             'source_update_timestamp when '
                                                                             'retracted; otherwise their '
                                                                             'least result/event validity '
                                                                             'end when superseded, otherwise '
                                                                             'null. Nomenclature uses CR/CE '
                                                                             'ends, date '
                                                                             'DATE_RESULT/CLINICAL_EVENT '
                                                                             'ends and text '
                                                                             'STRING_RESULT/CLINICAL_EVENT '
                                                                             'ends. An AUTHENTIC_FLAG-only '
                                                                             'supersession can therefore '
                                                                             'retain a null or '
                                                                             'future/sentinel end; no extra '
                                                                             'end clamp or fallback is '
                                                                             'applied.',
                                               'registry_field_id': 'Governed SHA-256 registry product and '
                                                                    'field identifier for registry '
                                                                    'observations; null on native rows.',
                                               'result_status_code': 'Source result-status code.',
                                               'result_status_display': 'Source result-status display.',
                                               'sequence_nbr': 'Sequence number that makes the coded result '
                                                               'primary key unique when a single result '
                                                               'spans more than one row.',
                                               'source_code': 'Verbatim source event code.',
                                               'source_coding_system': 'Verbatim source event coding system.',
                                               'source_display': 'Verbatim source event display.',
                                               'source_feed': 'Registered generic-event feed.',
                                               'source_object': 'Native source arm: coded_event, '
                                                                'nomen_event, date_event, or text_event.',
                                               'source_update_timestamp': 'Per-arm greatest contributing '
                                                                          'clock, including own ADC_UPDT: '
                                                                          'coded adds SOURCE_ADC_UPDT, '
                                                                          'CR_UPDT_DT_TM and CE_UPDT_DT_TM; '
                                                                          'nomenclature adds '
                                                                          'SOURCE_CHANGE_TS and '
                                                                          'NOMENCLATURE_ADC_UPDT; date adds '
                                                                          'DATE_RESULT_EFFECTIVE_UPDT_DT_TM, '
                                                                          'CLINICAL_EVENT_ADC_UPDT and '
                                                                          'LOOKUP_ADC_UPDT; text adds '
                                                                          'STRING_RESULT_EFFECTIVE_UPDT_DT_TM, '
                                                                          'CLINICAL_EVENT_ADC_UPDT, '
                                                                          'LONG_TEXT_ADC_UPDT and '
                                                                          'LOOKUP_ADC_UPDT. Null inputs are '
                                                                          'ignored and all null gives null. '
                                                                          'These are not uniformly native '
                                                                          'application-update clocks, and '
                                                                          'the union takes no cross-arm '
                                                                          'maximum.',
                                               'subject_id_system': 'Identifier system used for subject_key.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'value_code': 'Source coded result value.',
                                               'value_datetime': 'Date or date-time result value.',
                                               'value_display': 'Source coded result display.',
                                               'value_text': 'Verbatim text result or descriptor.',
                                               'value_time': 'Time of day (HH:mm:ss) when the source '
                                                             'recorded a time with no date (Millennium '
                                                             'placeholder date 2000-01-01); value_datetime '
                                                             'is then NULL. INSR_W2 W2.07 (J4, L2051).',
                                               'verifier_practitioner_id': 'Millennium personnel PERSON_ID '
                                                                           'for the verifier.',
                                               'verifier_system_ind': 'True when the source '
                                                                      'verifier_practitioner_id was 1, the '
                                                                      'Cerner system user (J5); the id is '
                                                                      'then published as NULL. NULL when the '
                                                                      'source id was NULL. INSR_W2.',
                                               'vip_ind': 'Source VIP indicator.',
                                               'withheld_identity_ind': 'Identity-withholding status is not '
                                                                        'populated by the current source '
                                                                        'projections; null does not mean '
                                                                        'identity was not withheld.'},
                                   'quantity_override': True,
                                   'table': 'Internal quality-controlled twin of clinical_clinical_finding: '
                                            'the Gold repairs, nulls and expectations are applied here and '
                                            'the published Gold MV reads this object. Materialized rather '
                                            'than temporary so the published MV can refresh incrementally.'},
 'clinical_clinical_finding': {'columns': {'confidentiality_code': 'Source confidentiality code.',
                                           'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                           'spine_encounter.encounter_id.',
                                           'event_datetime': 'Clinically relevant finding timestamp.',
                                           'event_datetime_local': 'Local-time representation of the '
                                                                   'clinically relevant timestamp for the '
                                                                   'clinical finding. Gold: NULL wherever '
                                                                   'Gold nulls event_datetime.',
                                           'event_datetime_status': 'registry_dated or registry_undated for '
                                                                    'registry observations; null on native '
                                                                    'rows.',
                                           'event_end_datetime': 'Source effective end timestamp.',
                                           'event_end_datetime_local': 'Local-time representation of the '
                                                                       'source effective end timestamp for '
                                                                       'the clinical finding. Gold: NULL '
                                                                       'wherever Gold nulls '
                                                                       'event_end_datetime.',
                                           'event_id': 'Millennium EVENT_ID; native key for coded, '
                                                       'nomenclature, date, and text event arms.',
                                           'finding_code': 'Source finding CodeableConcept.',
                                           'finding_kind': 'Generic result representation carried by the '
                                                           'source feed.',
                                           'loaded_at': 'Own ADC_UPDT from map_coded_events, '
                                                        'map_nomen_events, map_date_events or '
                                                        'map_text_events, carried per arm through '
                                                        'routing/usable-code filters, the union and '
                                                        'QC/public projections. The additional result, '
                                                        'clinical-event, lookup and long-text clocks used by '
                                                        'source_update_timestamp are not added here; no '
                                                        'current Silver refresh timestamp is substituted.',
                                           'normalcy_code': 'Source normalcy or interpretation code. '
                                                            'Normal/abnormal flag as recorded; the source '
                                                            'sets it on well under 1% of findings, and '
                                                            'nothing is lost between bronze and gold.',
                                           'normalcy_display': 'Source normalcy or interpretation display.',
                                           'order_id': 'Source order identifier when supplied.',
                                           'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                              'organization.',
                                           'parent_event_id': 'Source parent-event identifier.',
                                           'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                'stable cross-feed join key.',
                                           'performer_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                        'the performer.',
                                           'performer_system_ind': 'True when the source '
                                                                   'performer_practitioner_id was 1, the '
                                                                   'Cerner system user (J5); the id is then '
                                                                   'published as NULL. NULL when the source '
                                                                   'id was NULL. INSR_W2.',
                                           'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                           'record_status': 'Coded-event rows are superseded only when the '
                                                            'least CR_VALID_UNTIL_DT_TM and '
                                                            'CE_VALID_UNTIL_DT_TM is non-null and before '
                                                            '2100-01-01; otherwise active, never retracted. '
                                                            'Nomenclature, date and text rows are retracted '
                                                            'for SOURCE_DELETED_IND cast to BOOLEAN true '
                                                            '(null defaults false); otherwise superseded for '
                                                            'the least result/event validity end being '
                                                            'non-null and before 2100-01-01 or '
                                                            'AUTHENTIC_FLAG cast to LONG zero (null defaults '
                                                            'one); otherwise active. Retraction takes '
                                                            'priority, and clinical result-status codes are '
                                                            'separate.',
                                           'record_status_effective_from': 'First non-null '
                                                                           'result-valid-from, '
                                                                           'event-valid-from, then clinical '
                                                                           'event time. Coded/nomenclature '
                                                                           'use CR_VALID_FROM_DT_TM and '
                                                                           'CE_VALID_FROM_DT_TM; date uses '
                                                                           'DATE_RESULT_VALID_FROM_DT_TM and '
                                                                           'CLINICAL_EVENT_VALID_FROM_DT_TM; '
                                                                           'text substitutes '
                                                                           'STRING_RESULT_VALID_FROM_DT_TM. '
                                                                           'Coded event time falls back '
                                                                           'through PERFORMED_DT_TM, '
                                                                           'EVENT_END_DT_TM, '
                                                                           'EVENT_START_DT_TM. Nomenclature '
                                                                           'uses CLINICAL_EVENT_DT_TM, '
                                                                           'EVENT_END_DT_TM, '
                                                                           'PERFORMED_DT_TM, '
                                                                           'EVENT_START_DT_TM. Date/text use '
                                                                           'RESULT_DT_TM, PERFORMED_DT_TM, '
                                                                           'EVENT_END_DT_TM, '
                                                                           'EVENT_START_DT_TM. No '
                                                                           'status-bound clamp or '
                                                                           'ingestion-time fallback is '
                                                                           'added.',
                                           'record_status_effective_to': 'Coded rows use the least '
                                                                         'CR_VALID_UNTIL_DT_TM and '
                                                                         'CE_VALID_UNTIL_DT_TM only when '
                                                                         'superseded, otherwise null. '
                                                                         'Nomenclature, date and text rows '
                                                                         'use their computed '
                                                                         'source_update_timestamp when '
                                                                         'retracted; otherwise their least '
                                                                         'result/event validity end when '
                                                                         'superseded, otherwise null. '
                                                                         'Nomenclature uses CR/CE ends, date '
                                                                         'DATE_RESULT/CLINICAL_EVENT ends '
                                                                         'and text '
                                                                         'STRING_RESULT/CLINICAL_EVENT ends. '
                                                                         'An AUTHENTIC_FLAG-only '
                                                                         'supersession can therefore retain '
                                                                         'a null or future/sentinel end; no '
                                                                         'extra end clamp or fallback is '
                                                                         'applied.',
                                           'registry_field_id': 'Governed SHA-256 registry product and field '
                                                                'identifier for registry observations; null '
                                                                'on native rows.',
                                           'result_status_code': 'Source result-status code.',
                                           'result_status_display': 'Source result-status display.',
                                           'sequence_nbr': 'Sequence number that makes the coded result '
                                                           'primary key unique when a single result spans '
                                                           'more than one row.',
                                           'source_code': 'Verbatim source event code.',
                                           'source_coding_system': 'Verbatim source event coding system.',
                                           'source_display': 'Verbatim source event display.',
                                           'source_feed': 'Registered generic-event feed.',
                                           'source_object': 'Native source arm: coded_event, nomen_event, '
                                                            'date_event, or text_event.',
                                           'source_update_timestamp': 'Per-arm greatest contributing clock, '
                                                                      'including own ADC_UPDT: coded adds '
                                                                      'SOURCE_ADC_UPDT, CR_UPDT_DT_TM and '
                                                                      'CE_UPDT_DT_TM; nomenclature adds '
                                                                      'SOURCE_CHANGE_TS and '
                                                                      'NOMENCLATURE_ADC_UPDT; date adds '
                                                                      'DATE_RESULT_EFFECTIVE_UPDT_DT_TM, '
                                                                      'CLINICAL_EVENT_ADC_UPDT and '
                                                                      'LOOKUP_ADC_UPDT; text adds '
                                                                      'STRING_RESULT_EFFECTIVE_UPDT_DT_TM, '
                                                                      'CLINICAL_EVENT_ADC_UPDT, '
                                                                      'LONG_TEXT_ADC_UPDT and '
                                                                      'LOOKUP_ADC_UPDT. Null inputs are '
                                                                      'ignored and all null gives null. '
                                                                      'These are not uniformly native '
                                                                      'application-update clocks, and the '
                                                                      'union takes no cross-arm maximum.',
                                           'subject_id_system': 'Identifier system used for subject_key.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'value_code': 'Source coded result value.',
                                           'value_datetime': 'Date or date-time result value.',
                                           'value_display': 'Source coded result display.',
                                           'value_source_code': 'Source result-value code used as input to '
                                                                'value-concept mapping. Source value code; 0 '
                                                                'is a real Millennium code that carries a '
                                                                'display, so it is not treated as missing.',
                                           'value_text': 'Verbatim text result or descriptor.',
                                           'value_time': 'Time of day (HH:mm:ss) when the source recorded a '
                                                         'time with no date (Millennium placeholder date '
                                                         '2000-01-01); value_datetime is then NULL. INSR_W2 '
                                                         'W2.07 (J4, L2051).',
                                           'verifier_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                       'the verifier.',
                                           'verifier_system_ind': 'True when the source '
                                                                  'verifier_practitioner_id was 1, the '
                                                                  'Cerner system user (J5); the id is then '
                                                                  'published as NULL. NULL when the source '
                                                                  'id was NULL. INSR_W2.',
                                           'vip_ind': 'Source VIP indicator.',
                                           'withheld_identity_ind': 'Identity-withholding status is not '
                                                                    'populated by the current source '
                                                                    'projections; null does not mean '
                                                                    'identity was not withheld.'},
                               'quantity_override': False,
                               'table': 'One governed residual clinical observation from a generic '
                                        'Millennium event feed, without diagnosis inference. Gold QC twin of '
                                        'the silver product: 10 columns are repaired or nulled, 1 rule(s) '
                                        'drop rows, 4 check(s) are advisory. Each rule states its reason in '
                                        'the pipeline notebook, and Lakeflow expectation metrics report what '
                                        'every rule matched on each update.'}}
