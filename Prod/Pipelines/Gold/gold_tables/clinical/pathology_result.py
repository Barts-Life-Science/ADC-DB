"""Gold clinical.pathology_result: 2 dataset(s).

- datasets: _qc_clinical_pathology_result, clinical_pathology_result
- moved verbatim from: 2_gold_clinical_events:5003, 2_gold_clinical_events:5071, 2_gold_clinical_events:5084, 2_gold_clinical_events:5184, 2_gold_clinical_events:5188, 2_gold_clinical_events:5201

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.pathology_result ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_PATHOLOGY_RESULT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    '`encounter_link_method` AS `encounter_link_method`',  # GOLD_COLUMN_PARITY_V1
    # INSR_W3 W3.06: Gold: NULL where a pinned LIMS placeholder day (P3e).
    "CASE WHEN CAST(`event_datetime` AS DATE) IN (DATE'1972-01-19', DATE'1999-12-30') THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END END AS `event_datetime`",
    'CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`result_code` AS `result_code`',
    '`test_source_system` AS `test_source_system`',  # SRC_LINEAGE_V1
    '`test_source_code` AS `test_source_code`',  # SRC_LINEAGE_V1
    '`test_source_display` AS `test_source_display`',  # SRC_LINEAGE_V1
    '`result_source_system` AS `result_source_system`',  # SRC_LINEAGE_V1
    '`result_source_code` AS `result_source_code`',  # SRC_LINEAGE_V1
    '`result_source_display` AS `result_source_display`',  # SRC_LINEAGE_V1
    '`pathology_report_key` AS `pathology_report_key`',
    '`specimen_key` AS `specimen_key`',
    '`equivalence_group_key` AS `equivalence_group_key`',
    '`representation_role` AS `representation_role`',
    '`preferred_result_ind` AS `preferred_result_ind`',
    '`person_projection_status` AS `person_projection_status`',
    # DQ4_G_GOLD_RULES_V1: G6 label-only tests ZZZB/ZZZM/ZZZS carry no measured number (3,945,068 rows);
    # G7 FIT/FIT2/UMNT/24MT 999, 9999 and 99999 are "not measurable" fillers (8,075 rows; 9 and 99
    # are real FIT values and stay); G4 haematocrit is L/L, so 10-100 is a percentage and is
    # rescaled (4,686,743 rows, median 38 every year) and anything else above 1 is nulled (215).
    "CASE WHEN ABS(CAST(`value_number` AS DOUBLE)) > 1e12 THEN NULL"
    " WHEN `source_code` IN ('ZZZB', 'ZZZM', 'ZZZS') THEN NULL"
    " WHEN `source_code` IN ('FIT', 'FIT2', 'UMNT', '24MT') AND `value_number` IN (999, 9999, 99999) THEN NULL"
    " WHEN `source_code` IN ('HCT', 'BHCT') AND `value_number` BETWEEN 10 AND 100 THEN CAST(`value_number` / 100 AS DECIMAL(38,10))"
    " WHEN `source_code` IN ('HCT', 'BHCT') AND `value_number` > 1 THEN NULL"
    " ELSE `value_number` END AS `value_number`",
    "CASE WHEN TRIM(CAST(`value_text` AS STRING)) = '' THEN NULL ELSE `value_text` END AS `value_text`",
    "CASE WHEN CAST(`value_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`value_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`value_datetime` AS DATE)) < 9999 OR CAST(`value_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`value_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `value_datetime` END END AS `value_datetime`",
    '`value_concept_id` AS `value_concept_id`',
    '`value_concept_display` AS `value_concept_display`',
    '`operator_concept_id` AS `operator_concept_id`',
    "CASE WHEN TRIM(CAST(`unit_source_value` AS STRING)) = '' THEN NULL ELSE `unit_source_value` END AS `unit_source_value`",
    '`ucum_code` AS `ucum_code`',
    "`test_mapping_match_type` AS `test_mapping_match_type`",  # SDI_PATH_V1
    "`test_mapping_rule_id` AS `test_mapping_rule_id`",  # SDI_PATH_V1
    "CASE WHEN (CASE WHEN ABS(CAST(`value_number` AS DOUBLE)) > 1e12 THEN NULL WHEN `source_code` IN ('ZZZB', 'ZZZM', 'ZZZS') THEN NULL WHEN `source_code` IN ('FIT', 'FIT2', 'UMNT', '24MT') AND `value_number` IN (999, 9999, 99999) THEN NULL WHEN `source_code` IN ('HCT', 'BHCT') AND `value_number` BETWEEN 10 AND 100 THEN CAST(`value_number` / 100 AS DECIMAL(38,10)) WHEN `source_code` IN ('HCT', 'BHCT') AND `value_number` > 1 THEN NULL ELSE `value_number` END) IS NULL THEN NULL ELSE `canonical_ucum_code` END AS `canonical_ucum_code`",  # SDI_PATH_V1
    "CASE WHEN (CASE WHEN ABS(CAST(`value_number` AS DOUBLE)) > 1e12 THEN NULL WHEN `source_code` IN ('ZZZB', 'ZZZM', 'ZZZS') THEN NULL WHEN `source_code` IN ('FIT', 'FIT2', 'UMNT', '24MT') AND `value_number` IN (999, 9999, 99999) THEN NULL WHEN `source_code` IN ('HCT', 'BHCT') AND `value_number` BETWEEN 10 AND 100 THEN CAST(`value_number` / 100 AS DECIMAL(38,10)) WHEN `source_code` IN ('HCT', 'BHCT') AND `value_number` > 1 THEN NULL ELSE `value_number` END) IS NULL THEN NULL WHEN `canonical_conversion` IN ('identity', 'egfr_unit_implied', 'inr_unit_implied') THEN CAST((CASE WHEN ABS(CAST(`value_number` AS DOUBLE)) > 1e12 THEN NULL WHEN `source_code` IN ('ZZZB', 'ZZZM', 'ZZZS') THEN NULL WHEN `source_code` IN ('FIT', 'FIT2', 'UMNT', '24MT') AND `value_number` IN (999, 9999, 99999) THEN NULL WHEN `source_code` IN ('HCT', 'BHCT') AND `value_number` BETWEEN 10 AND 100 THEN CAST(`value_number` / 100 AS DECIMAL(38,10)) WHEN `source_code` IN ('HCT', 'BHCT') AND `value_number` > 1 THEN NULL ELSE `value_number` END) AS DECIMAL(38,10)) ELSE `value_canonical` END AS `value_canonical`",  # SDI_PATH_V1
    "CASE WHEN (CASE WHEN ABS(CAST(`value_number` AS DOUBLE)) > 1e12 THEN NULL WHEN `source_code` IN ('ZZZB', 'ZZZM', 'ZZZS') THEN NULL WHEN `source_code` IN ('FIT', 'FIT2', 'UMNT', '24MT') AND `value_number` IN (999, 9999, 99999) THEN NULL WHEN `source_code` IN ('HCT', 'BHCT') AND `value_number` BETWEEN 10 AND 100 THEN CAST(`value_number` / 100 AS DECIMAL(38,10)) WHEN `source_code` IN ('HCT', 'BHCT') AND `value_number` > 1 THEN NULL ELSE `value_number` END) IS NULL THEN NULL ELSE `canonical_conversion` END AS `canonical_conversion`",  # SDI_PATH_V1
    '`unit_concept_id` AS `unit_concept_id`',
    '`reference_range_low` AS `reference_range_low`',
    '`reference_range_high` AS `reference_range_high`',
    '`interpretation_code` AS `interpretation_code`',
    '`polarity` AS `polarity`',
    '`finding_axis` AS `finding_axis`',
    '`result_status` AS `result_status`',
    "CASE WHEN UPPER(TRIM(CAST(`body_site_code` AS STRING))) = '0' THEN NULL ELSE `body_site_code` END AS `body_site_code`",
    '`clinician_code` AS `clinician_code`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
    # BBR_FOLDIN_V1 (BBR-10): internal flag read by the warn rule insr_bbr_mapped_without_code (the test axis
    # is a JSON payload in the twin, restored only in the public step).
    "CASE WHEN `test_map_status` = 'mapped' AND `test_snomed_code` IS NULL AND `test_omop_concept_id` IS NULL AND `test_target_code` IS NULL THEN TRUE ELSE FALSE END AS `_qc_test_mapped_without_code`",
]


# contract v2: QC/batch inputs come from internal _clinical_pathology_result_metadata; source history stays on the main research table.
CLINICAL_PATHOLOGY_RESULT_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 1,497,882,424 rows of
    # 1,877,832,347 that are current and attributable; preferred_result_ind = TRUE keeps the
    # 0 rows of 1,877,832,347 that are current and attributable. Superseded versions and
    # rows whose identity was never resolved are not research data, and a consumer who wants
    # them has silver.
    "research_surface": "(identity_status = 'resolved') AND (preferred_result_ind = TRUE)",
    # BBR_FOLDIN_V1 (BBR-9b): LIMS housekeeping rows (placeholder codes, internal/unused-code results, eGFR
    # input and factor rows, ZZZ tests; silver non_clinical_test_ind) are not clinical tests and leave gold
    # (10-29 ruling: rows of no clinical use). Serum indices are kept (Ben decision BBR-D1).
    "gold.clinical.pathology_result.table.bbr_non_clinical_test": "NOT coalesce(non_clinical_test_ind, FALSE)",
}


CLINICAL_PATHOLOGY_RESULT_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.pathology_result.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # INSR_W4 BBR mapping_coverage (design BBR-10): A test axis that says mapped must carry a code (BBR-10). R1's status_guard turns new resolves into no_candidate / rule_applied_no_code; this warns on anything that still slips through.
    'gold.clinical.pathology_result.test_map_status.insr_bbr_mapped_without_code':
        'NOT COALESCE((`_qc_test_mapped_without_code`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 379,949,923 of 1,877,832,347 at the profile.
    "gold.clinical.pathology_result.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing preferred_result_ind = TRUE.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 1,877,832,347 of 1,877,832,347 at the profile.
    "gold.clinical.pathology_result.preferred_result_ind.default_view_preferred":
        "preferred_result_ind = TRUE",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,240,568 of 1,877,832,347 rows (0.0661%) when profiled on
    # 2026-08-24.
    "gold.clinical.pathology_result.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 5,132,232 of 1,877,832,347 rows (0.273%) when profiled on
    # 2026-08-24.
    "gold.clinical.pathology_result.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # Negative values here are signed by design -- credit lines in the costing feed and
    # below-zero readings on a calibrated scale -- so they are counted, not removed. Seen on
    # 4,908,621 of 1,877,832,347 rows (0.261%) when profiled on 2026-08-24.
    "gold.clinical.pathology_result.value_number.negative_mass_scale_owner":
        "NOT COALESCE((`value_number` < 0), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_PATHOLOGY_RESULT_PERSON_SCOPE_RULES = {
    "gold.clinical.pathology_result.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_pathology_result"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_PATHOLOGY_RESULT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PATHOLOGY_RESULT_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_PATHOLOGY_RESULT_PERSON_SCOPE_RULES)
def _gold_qc_clinical_pathology_result():
    """Quality-controlled twin of journey_clinical.pathology_result."""
    df = _qc("clinical_pathology_result", CLINICAL_PATHOLOGY_RESULT_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.pathology_result"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_pathology_result():
    """Contract-v2 public twin of clinical_pathology_result; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_pathology_result")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`encounter_link_method` AS `encounter_link_method`',  # GOLD_COLUMN_PARITY_V1
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_result_code` AS `result_code`',
        '`test_source_system` AS `test_source_system`',  # SRC_LINEAGE_V1
        '`test_source_code` AS `test_source_code`',  # SRC_LINEAGE_V1
        '`test_source_display` AS `test_source_display`',  # SRC_LINEAGE_V1
        '`result_source_system` AS `result_source_system`',  # SRC_LINEAGE_V1
        '`result_source_code` AS `result_source_code`',  # SRC_LINEAGE_V1
        '`result_source_display` AS `result_source_display`',  # SRC_LINEAGE_V1
        '`pathology_report_key` AS `pathology_report_key`',
        '`specimen_key` AS `specimen_key`',
        '`equivalence_group_key` AS `equivalence_group_key`',
        '`representation_role` AS `representation_role`',
        '`preferred_result_ind` AS `preferred_result_ind`',
        '`person_projection_status` AS `person_projection_status`',
        '`value_number` AS `value_number`',
        '`value_text` AS `value_text`',
        '`value_datetime` AS `value_datetime`',
        '`value_concept_id` AS `value_concept_id`',
        '`value_concept_display` AS `value_concept_display`',
        '`operator_concept_id` AS `operator_concept_id`',
        '`unit_source_value` AS `unit_source_value`',
        '`ucum_code` AS `ucum_code`',
        '`test_mapping_match_type` AS `test_mapping_match_type`',  # SDI_PATH_V1
        '`test_mapping_rule_id` AS `test_mapping_rule_id`',  # SDI_PATH_V1
        '`canonical_ucum_code` AS `canonical_ucum_code`',  # SDI_PATH_V1
        '`value_canonical` AS `value_canonical`',  # SDI_PATH_V1
        '`canonical_conversion` AS `canonical_conversion`',  # SDI_PATH_V1
        '`unit_concept_id` AS `unit_concept_id`',
        '`reference_range_low` AS `reference_range_low`',
        '`reference_range_high` AS `reference_range_high`',
        '`interpretation_code` AS `interpretation_code`',
        '`polarity` AS `polarity`',
        '`finding_axis` AS `finding_axis`',
        '`result_status` AS `result_status`',
        '`body_site_code` AS `body_site_code`',
        '`clinician_code` AS `clinician_code`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _s3_flatten_gold_public(df, "clinical_pathology_result")


def _public_steps_clinical_pathology_result(df):
    """clinical_pathology_result: the public body after its projection (GoldRules.public_select), moved from gold_clinical_pathology_result; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_pathology_result')


RULES = {
    'clinical_pathology_result': GoldRules(
        table='clinical_pathology_result',
        select=CLINICAL_PATHOLOGY_RESULT_SELECT,
        mandatory=CLINICAL_PATHOLOGY_RESULT_MANDATORY_RULES,
        advisory=CLINICAL_PATHOLOGY_RESULT_ADVISORY_RULES,
        person_scope=CLINICAL_PATHOLOGY_RESULT_PERSON_SCOPE_RULES,
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
        variant_columns=('result_code',),
        s3_axis_specs={'result_code': [('test', False), ('result', False)]},
        value_rules={'gold.clinical.pathology_result.result_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                     'column': 'result_snomed_code',
                                                                                     'companions': ['result_snomed_display',
                                                                                                    'result_target_code',
                                                                                                    'result_target_display'],
                                                                                     'match': '`result_snomed_code` '
                                                                                              'IS NOT NULL '
                                                                                              'AND (NOT '
                                                                                              '`result_snomed_code` '
                                                                                              'RLIKE '
                                                                                              "'^[1-9][0-9]{5,17}$' "
                                                                                              'OR '
                                                                                              'substring(`result_snomed_code`, '
                                                                                              'length(`result_snomed_code`) '
                                                                                              '- 2, 2) NOT '
                                                                                              "IN ('00', "
                                                                                              "'10') OR "
                                                                                              '(substring(`result_snomed_code`, '
                                                                                              'length(`result_snomed_code`) '
                                                                                              '- 2, 2) = '
                                                                                              "'10' AND "
                                                                                              'length(`result_snomed_code`) '
                                                                                              '< 11))',
                                                                                     'table': 'clinical_pathology_result'},
         'gold.clinical.pathology_result.result_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                     'column': 'result_target_code',
                                                                                     'companions': ['result_target_display',
                                                                                                    'result_snomed_code',
                                                                                                    'result_snomed_display'],
                                                                                     'match': '`result_target_system` '
                                                                                              '= '
                                                                                              "'http://snomed.info/sct' "
                                                                                              'AND '
                                                                                              '`result_target_code` '
                                                                                              'IS NOT NULL '
                                                                                              'AND (NOT '
                                                                                              '`result_target_code` '
                                                                                              'RLIKE '
                                                                                              "'^[1-9][0-9]{5,17}$' "
                                                                                              'OR '
                                                                                              'substring(`result_target_code`, '
                                                                                              'length(`result_target_code`) '
                                                                                              '- 2, 2) NOT '
                                                                                              "IN ('00', "
                                                                                              "'10') OR "
                                                                                              '(substring(`result_target_code`, '
                                                                                              'length(`result_target_code`) '
                                                                                              '- 2, 2) = '
                                                                                              "'10' AND "
                                                                                              'length(`result_target_code`) '
                                                                                              '< 11))',
                                                                                     'table': 'clinical_pathology_result'},
         'gold.clinical.pathology_result.test_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                   'column': 'test_snomed_code',
                                                                                   'companions': ['test_snomed_display',
                                                                                                  'test_target_code',
                                                                                                  'test_target_display'],
                                                                                   'match': '`test_snomed_code` '
                                                                                            'IS NOT NULL '
                                                                                            'AND (NOT '
                                                                                            '`test_snomed_code` '
                                                                                            'RLIKE '
                                                                                            "'^[1-9][0-9]{5,17}$' "
                                                                                            'OR '
                                                                                            'substring(`test_snomed_code`, '
                                                                                            'length(`test_snomed_code`) '
                                                                                            '- 2, 2) NOT '
                                                                                            "IN ('00', "
                                                                                            "'10') OR "
                                                                                            '(substring(`test_snomed_code`, '
                                                                                            'length(`test_snomed_code`) '
                                                                                            '- 2, 2) = '
                                                                                            "'10' AND "
                                                                                            'length(`test_snomed_code`) '
                                                                                            '< 11))',
                                                                                   'table': 'clinical_pathology_result'},
         'gold.clinical.pathology_result.test_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                   'column': 'test_target_code',
                                                                                   'companions': ['test_target_display',
                                                                                                  'test_snomed_code',
                                                                                                  'test_snomed_display'],
                                                                                   'match': '`test_target_system` '
                                                                                            '= '
                                                                                            "'http://snomed.info/sct' "
                                                                                            'AND '
                                                                                            '`test_target_code` '
                                                                                            'IS NOT NULL '
                                                                                            'AND (NOT '
                                                                                            '`test_target_code` '
                                                                                            'RLIKE '
                                                                                            "'^[1-9][0-9]{5,17}$' "
                                                                                            'OR '
                                                                                            'substring(`test_target_code`, '
                                                                                            'length(`test_target_code`) '
                                                                                            '- 2, 2) NOT '
                                                                                            "IN ('00', "
                                                                                            "'10') OR "
                                                                                            '(substring(`test_target_code`, '
                                                                                            'length(`test_target_code`) '
                                                                                            '- 2, 2) = '
                                                                                            "'10' AND "
                                                                                            'length(`test_target_code`) '
                                                                                            '< 11))',
                                                                                   'table': 'clinical_pathology_result'},
         'gold.clinical.pathology_result.ucum_code.insr_bbr_radiation_unit': {'action': 'invalidate',
                                                                              'column': 'ucum_code',
                                                                              'companions': ['unit_concept_id',
                                                                                             'canonical_ucum_code',
                                                                                             'value_canonical'],
                                                                              'match': '`ucum_code` IN '
                                                                                       "('Gy', 'cGy', "
                                                                                       "'mGy', 'Sv', "
                                                                                       "'mSv')",
                                                                              'table': 'clinical_pathology_result'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`subject_key` AS `subject_key`',
            '`subject_id_system` AS `subject_id_system`',
            '`person_id` AS `person_id`',
            '`encounter_id` AS `encounter_id`',
            '`encounter_link_method` AS `encounter_link_method`',
            '`event_datetime` AS `event_datetime`',
            '`event_end_datetime` AS `event_end_datetime`',
            '`event_datetime_local` AS `event_datetime_local`',
            '`event_end_datetime_local` AS `event_end_datetime_local`',
            '`source_coding_system` AS `source_coding_system`',
            '`source_code` AS `source_code`',
            '`source_display` AS `source_display`',
            '`__gold_json_result_code` AS `result_code`',
            '`test_source_system` AS `test_source_system`',  # SRC_LINEAGE_V1
            '`test_source_code` AS `test_source_code`',  # SRC_LINEAGE_V1
            '`test_source_display` AS `test_source_display`',  # SRC_LINEAGE_V1
            '`result_source_system` AS `result_source_system`',  # SRC_LINEAGE_V1
            '`result_source_code` AS `result_source_code`',  # SRC_LINEAGE_V1
            '`result_source_display` AS `result_source_display`',  # SRC_LINEAGE_V1
            '`pathology_report_key` AS `pathology_report_key`',
            '`specimen_key` AS `specimen_key`',
            '`equivalence_group_key` AS `equivalence_group_key`',
            '`representation_role` AS `representation_role`',
            '`preferred_result_ind` AS `preferred_result_ind`',
            '`person_projection_status` AS `person_projection_status`',
            '`value_number` AS `value_number`',
            '`value_text` AS `value_text`',
            '`value_datetime` AS `value_datetime`',
            '`value_concept_id` AS `value_concept_id`',
            '`value_concept_display` AS `value_concept_display`',
            '`operator_concept_id` AS `operator_concept_id`',
            '`unit_source_value` AS `unit_source_value`',
            '`ucum_code` AS `ucum_code`',
            '`test_mapping_match_type` AS `test_mapping_match_type`',
            '`test_mapping_rule_id` AS `test_mapping_rule_id`',
            '`canonical_ucum_code` AS `canonical_ucum_code`',
            '`value_canonical` AS `value_canonical`',
            '`canonical_conversion` AS `canonical_conversion`',
            '`unit_concept_id` AS `unit_concept_id`',
            '`reference_range_low` AS `reference_range_low`',
            '`reference_range_high` AS `reference_range_high`',
            '`interpretation_code` AS `interpretation_code`',
            '`polarity` AS `polarity`',
            '`finding_axis` AS `finding_axis`',
            '`result_status` AS `result_status`',
            '`body_site_code` AS `body_site_code`',
            '`clinician_code` AS `clinician_code`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`source_feed` AS `source_feed`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
        ],
        public_steps=(_public_steps_clinical_pathology_result,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_pathology_result': {'columns': {'body_site_code': 'Source body-site code.',
                                               'canonical_conversion': 'NULL wherever Gold nulls '
                                                                       'value_number. '
                                                                       'map_pathology.canonical_conversion: '
                                                                       'g/dL->g/L (x10) | %->mmol/mol (IFCC '
                                                                       '= (NGSP - 2.15) x 10.929, 1 dp) | '
                                                                       'ug/L->ng/L (x1000, troponin T only) '
                                                                       '| unit_label_conflict (troponin I '
                                                                       'labelled ug/L: TFC TROI has held '
                                                                       'ng/L-scale values since 2014, so '
                                                                       'neither the label nor a conversion '
                                                                       'is trusted; value_canonical and '
                                                                       'canonical_ucum_code are NULL) | '
                                                                       'egfr_unit_implied / inr_unit_implied '
                                                                       '(unit named by the test concept; '
                                                                       'value unchanged) | identity '
                                                                       '(ucum_code kept) | NULL (no numeric '
                                                                       'value, or no unit).',
                                               'canonical_ucum_code': 'NULL wherever Gold nulls '
                                                                      'value_number. '
                                                                      'map_pathology.canonical_ucum_code: '
                                                                      'UCUM unit of value_canonical '
                                                                      '(SDI_PATH_UNIT_V1): g/L for g/dL '
                                                                      'results, mmol/mol (IFCC) for HbA1c in '
                                                                      '%, ng/L for troponin T in ug/L, '
                                                                      'mL/min/{1.73_m2} for eGFR, {INR} for '
                                                                      'INR, else ucum_code. NULL when '
                                                                      'value_as_number is NULL, no unit is '
                                                                      'known, or the unit label contradicts '
                                                                      'the values (unit_label_conflict).',
                                               'clinician_code': 'Source clinician code.',
                                               'confidentiality_code': 'Source confidentiality '
                                                                       'classification attached to the '
                                                                       'record for each pathology result '
                                                                       'record. It is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                               'available.',
                                               'encounter_link_method': 'How encounter_id was set '
                                                                        '(PATH_ENCOUNTER_V1). '
                                                                        "source_accession: the row's own "
                                                                        'Millennium source names the '
                                                                        'encounter. accession_single: every '
                                                                        'current Millennium source of the '
                                                                        'alias-resolved accession names this '
                                                                        'one encounter (how TFC LIMS rows '
                                                                        'inherit one once their accession is '
                                                                        'merged with the Millennium order). '
                                                                        'accession_multiple: those sources '
                                                                        'name different encounters, so '
                                                                        'encounter_id is NULL. none: the '
                                                                        'accession has no Millennium source, '
                                                                        'so encounter_id is NULL. Gold: '
                                                                        'encounter_id can still be NULL here '
                                                                        'when the encounter is not in the '
                                                                        'Gold encounter spine.',
                                               'equivalence_group_key': 'Derived cross-arm '
                                                                        'result-equivalence key.',
                                               'event_datetime': 'Date and time when the represented '
                                                                 'clinical or administrative event occurred '
                                                                 'for each pathology result record. It is '
                                                                 'derived from bronze field '
                                                                 '`measurement_datetime` in '
                                                                 '`4_prod.bronze.map_pathology`. Source '
                                                                 'precision and timezone handling follow the '
                                                                 'pipeline expression; null means the time '
                                                                 'was unavailable.',
                                               'event_datetime_local': 'Measurement timestamp expressed in '
                                                                       'local time. Gold: NULL wherever Gold '
                                                                       'nulls event_datetime.',
                                               'event_end_datetime': 'Measurement end timestamp.',
                                               'event_end_datetime_local': 'Measurement end timestamp '
                                                                           'expressed in local time. Gold: '
                                                                           'NULL wherever Gold nulls '
                                                                           'event_end_datetime.',
                                               'finding_axis': 'Source finding/result axis.',
                                               'interpretation_code': 'Source interpretation or normalcy.',
                                               'loaded_at': 'map_pathology.ADC_UPDT carried unchanged '
                                                            'through parent-link and result-equivalence '
                                                            'joins, usable-code filtering and QC/public '
                                                            'projections. Those joins select '
                                                            'linkage/equivalence fields, not additional '
                                                            'clocks; parent-link loaded_at is not '
                                                            'propagated. This differs from source_adc_updt '
                                                            'used for source_update_timestamp and from '
                                                            'Silver refresh time.',
                                               'operator_concept_id': 'Result comparison operator concept.',
                                               'pathology_report_key': 'Deterministic pathology-report '
                                                                       'series key.',
                                               'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                    'stable cross-feed join key.',
                                               'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                               'person_projection_status': 'Equivalence-layer person '
                                                                           'projection state.',
                                               'polarity': 'Source result polarity or growth grade.',
                                               'preferred_result_ind': 'Whether this is the preferred '
                                                                       'representation.',
                                               'record_status': 'Superseded when '
                                                                'map_pathology.valid_until_dt_tm is non-null '
                                                                'and earlier than 2100-01-01; otherwise '
                                                                'active. No retracted status is emitted. '
                                                                'Result-status text, equivalence preference '
                                                                'and parent linkage do not determine this '
                                                                'label; the usable-code filter controls '
                                                                'eligibility separately.',
                                               'record_status_effective_from': 'Timestamp from which the '
                                                                               'normalised record status '
                                                                               'applies, when the source '
                                                                               'supplies one.',
                                               'record_status_effective_to': 'map_pathology.valid_until_dt_tm '
                                                                             'only when non-null and earlier '
                                                                             'than 2100-01-01; otherwise '
                                                                             'null. The predicate is '
                                                                             'identical to the '
                                                                             'superseded-status test. Unlike '
                                                                             'event timestamps, this history '
                                                                             'end has no 1950 lower clamp; '
                                                                             'neither event_end_dt_tm nor '
                                                                             'ingestion time is substituted.',
                                               'reference_range_high': 'Reference-range upper bound.',
                                               'reference_range_low': 'Reference-range lower bound.',
                                               'representation_role': 'Result representation role within the '
                                                                      'equivalence group.',
                                               'result_code': 'Source and mapped test codings.',
                                               'result_status': 'Source result status.',
                                               'source_code': 'Source test code.',
                                               'source_coding_system': 'Source test coding system.',
                                               'source_display': 'Source test display.',
                                               'source_feed': 'Registered owning feed.',
                                               'source_update_timestamp': 'Greatest contributing source '
                                                                          'update timestamp; never changed '
                                                                          'by mapping-only backfills.',
                                               'specimen_key': 'Deterministic specimen key.',
                                               'subject_id_system': 'Identifier system used for subject_key.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'test_mapping_match_type': 'map_pathology.test_mapping_match_type: '
                                                                          'How measurement_concept_id was '
                                                                          'set: exact_context (the map key '
                                                                          'of this row code and '
                                                                          'description), native_event_cd / '
                                                                          'native_nlmc (native code map), '
                                                                          'safe_code (every observed '
                                                                          'description of the code maps to '
                                                                          'one concept), cross_arm_display '
                                                                          '(SDI_PATH_XARM_V1: a '
                                                                          'Millennium-linked row takes the '
                                                                          'concept the TFC LIMS arm map '
                                                                          'gives the same analyte display, '
                                                                          'and that arm tier), '
                                                                          'placeholder_code '
                                                                          '(SDI_PATH_PLACEHOLDER_V1: an '
                                                                          'unused/internal placeholder test '
                                                                          'code, unmapped by design), or '
                                                                          'unmapped.',
                                               'test_mapping_rule_id': 'map_pathology.test_mapping_rule_id: '
                                                                       'Rule that set a cross_arm_display or '
                                                                       'placeholder_code test mapping '
                                                                       '(SDI_PATH_XARM_V1 | '
                                                                       'SDI_PATH_PLACEHOLDER_V1); NULL when '
                                                                       'the row map key, a native map or the '
                                                                       'safe-code rule set it, or nothing '
                                                                       'did.',
                                               'ucum_code': 'UCUM unit code.',
                                               'unit_concept_id': 'OMOP unit concept identifier.',
                                               'unit_source_value': 'Verbatim source unit.',
                                               'value_canonical': 'Silver value_canonical after the Gold '
                                                                  'value_number repairs: NULL wherever '
                                                                  'value_number is nulled; where the '
                                                                  'conversion leaves the value unchanged '
                                                                  '(identity, egfr_unit_implied, '
                                                                  'inr_unit_implied) it is the repaired '
                                                                  'value_number, so the haematocrit '
                                                                  'percentage rescale carries through. '
                                                                  'value_as_number restated in '
                                                                  'canonical_ucum_code (see '
                                                                  'canonical_conversion); equal to '
                                                                  'value_as_number unless converted. NULL '
                                                                  'when canonical_conversion is NULL or '
                                                                  'unit_label_conflict.',
                                               'value_concept_display': 'Coded result concept display.',
                                               'value_concept_id': 'Coded result concept identifier.',
                                               'value_datetime': 'Datetime result value.',
                                               'value_number': 'Numeric result value.',
                                               'value_text': 'Verbatim result value.',
                                               'vip_ind': 'Indicator that the source record carries VIP '
                                                          'handling status for each pathology result record. '
                                                          'It is produced by the silver transformation and '
                                                          'has no direct bronze-column lineage entry. Null '
                                                          'means the source did not state the indicator and '
                                                          'must not be interpreted as false.',
                                               'withheld_identity_ind': 'Withheld identity indicator.'},
                                   'quantity_override': True,
                                   'table': 'Internal quality-controlled twin of clinical_pathology_result: '
                                            'the Gold repairs, nulls and expectations are applied here and '
                                            'the published Gold MV reads this object. Materialized rather '
                                            'than temporary so the published MV can refresh incrementally.'},
 'clinical_pathology_result': {'columns': {'body_site_code': 'Source body-site code.',
                                           'canonical_conversion': 'NULL wherever Gold nulls value_number. '
                                                                   'map_pathology.canonical_conversion: '
                                                                   'g/dL->g/L (x10) | %->mmol/mol (IFCC = '
                                                                   '(NGSP - 2.15) x 10.929, 1 dp) | '
                                                                   'ug/L->ng/L (x1000, troponin T only) | '
                                                                   'unit_label_conflict (troponin I labelled '
                                                                   'ug/L: TFC TROI has held ng/L-scale '
                                                                   'values since 2014, so neither the label '
                                                                   'nor a conversion is trusted; '
                                                                   'value_canonical and canonical_ucum_code '
                                                                   'are NULL) | egfr_unit_implied / '
                                                                   'inr_unit_implied (unit named by the test '
                                                                   'concept; value unchanged) | identity '
                                                                   '(ucum_code kept) | NULL (no numeric '
                                                                   'value, or no unit).',
                                           'canonical_ucum_code': 'NULL wherever Gold nulls value_number. '
                                                                  'map_pathology.canonical_ucum_code: UCUM '
                                                                  'unit of value_canonical '
                                                                  '(SDI_PATH_UNIT_V1): g/L for g/dL results, '
                                                                  'mmol/mol (IFCC) for HbA1c in %, ng/L for '
                                                                  'troponin T in ug/L, mL/min/{1.73_m2} for '
                                                                  'eGFR, {INR} for INR, else ucum_code. NULL '
                                                                  'when value_as_number is NULL, no unit is '
                                                                  'known, or the unit label contradicts the '
                                                                  'values (unit_label_conflict).',
                                           'clinician_code': 'Source clinician code.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each '
                                                                   'pathology result record. It is produced '
                                                                   'by the silver transformation and has no '
                                                                   'direct bronze-column lineage entry. '
                                                                   'Source code meanings and sentinel values '
                                                                   'are retained unless the pipeline '
                                                                   'explicitly maps them; null means no code '
                                                                   'was supplied.',
                                           'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                           'encounter_link_method': 'How encounter_id was set '
                                                                    '(PATH_ENCOUNTER_V1). source_accession: '
                                                                    "the row's own Millennium source names "
                                                                    'the encounter. accession_single: every '
                                                                    'current Millennium source of the '
                                                                    'alias-resolved accession names this one '
                                                                    'encounter (how TFC LIMS rows inherit '
                                                                    'one once their accession is merged with '
                                                                    'the Millennium order). '
                                                                    'accession_multiple: those sources name '
                                                                    'different encounters, so encounter_id '
                                                                    'is NULL. none: the accession has no '
                                                                    'Millennium source, so encounter_id is '
                                                                    'NULL. Gold: encounter_id can still be '
                                                                    'NULL here when the encounter is not in '
                                                                    'the Gold encounter spine.',
                                           'equivalence_group_key': 'Derived cross-arm result-equivalence '
                                                                    'key.',
                                           'event_datetime': 'Date and time when the represented clinical or '
                                                             'administrative event occurred for each '
                                                             'pathology result record. It is derived from '
                                                             'bronze field `measurement_datetime` in '
                                                             '`4_prod.bronze.map_pathology`. Source '
                                                             'precision and timezone handling follow the '
                                                             'pipeline expression; null means the time was '
                                                             'unavailable.',
                                           'event_datetime_local': 'Measurement timestamp expressed in local '
                                                                   'time. Gold: NULL wherever Gold nulls '
                                                                   'event_datetime.',
                                           'event_end_datetime': 'Measurement end timestamp.',
                                           'event_end_datetime_local': 'Measurement end timestamp expressed '
                                                                       'in local time. Gold: NULL wherever '
                                                                       'Gold nulls event_end_datetime.',
                                           'finding_axis': 'Source finding/result axis.',
                                           'interpretation_code': 'Source interpretation or normalcy.',
                                           'loaded_at': 'map_pathology.ADC_UPDT carried unchanged through '
                                                        'parent-link and result-equivalence joins, '
                                                        'usable-code filtering and QC/public projections. '
                                                        'Those joins select linkage/equivalence fields, not '
                                                        'additional clocks; parent-link loaded_at is not '
                                                        'propagated. This differs from source_adc_updt used '
                                                        'for source_update_timestamp and from Silver refresh '
                                                        'time.',
                                           'operator_concept_id': 'Result comparison operator concept.',
                                           'pathology_report_key': 'Deterministic pathology-report series '
                                                                   'key.',
                                           'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                'stable cross-feed join key.',
                                           'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                           'person_projection_status': 'Equivalence-layer person projection '
                                                                       'state.',
                                           'polarity': 'Source result polarity or growth grade.',
                                           'preferred_result_ind': 'Whether this is the preferred '
                                                                   'representation.',
                                           'record_status': 'Superseded when map_pathology.valid_until_dt_tm '
                                                            'is non-null and earlier than 2100-01-01; '
                                                            'otherwise active. No retracted status is '
                                                            'emitted. Result-status text, equivalence '
                                                            'preference and parent linkage do not determine '
                                                            'this label; the usable-code filter controls '
                                                            'eligibility separately.',
                                           'record_status_effective_from': 'Timestamp from which the '
                                                                           'normalised record status '
                                                                           'applies, when the source '
                                                                           'supplies one.',
                                           'record_status_effective_to': 'map_pathology.valid_until_dt_tm '
                                                                         'only when non-null and earlier '
                                                                         'than 2100-01-01; otherwise null. '
                                                                         'The predicate is identical to the '
                                                                         'superseded-status test. Unlike '
                                                                         'event timestamps, this history end '
                                                                         'has no 1950 lower clamp; neither '
                                                                         'event_end_dt_tm nor ingestion time '
                                                                         'is substituted.',
                                           'reference_range_high': 'Reference-range upper bound. Reference '
                                                                   'range upper limit; present only on '
                                                                   'Cerner-linked results (about 86% of '
                                                                   'them). The legacy LIMS feed has no range '
                                                                   'column, so LIMS-only results carry none.',
                                           'reference_range_low': 'Reference-range lower bound.',
                                           'representation_role': 'Result representation role within the '
                                                                  'equivalence group.',
                                           'result_code': 'Source and mapped test codings.',
                                           'result_source_code': 'Source code for the pathology result '
                                                                 'value, derived from the verbatim result as '
                                                                 'supplied by the source feed. Source result '
                                                                 'code; 0 is a real code that carries a '
                                                                 'display, so it is not treated as missing.',
                                           'result_status': 'Source result status.',
                                           'source_code': 'Source test code.',
                                           'source_coding_system': 'Source test coding system.',
                                           'source_display': 'Source test display.',
                                           'source_feed': 'Registered owning feed.',
                                           'source_update_timestamp': 'Greatest contributing source update '
                                                                      'timestamp; never changed by '
                                                                      'mapping-only backfills.',
                                           'specimen_key': 'Deterministic specimen key.',
                                           'subject_id_system': 'Identifier system used for subject_key.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'test_mapping_match_type': 'map_pathology.test_mapping_match_type: '
                                                                      'How measurement_concept_id was set: '
                                                                      'exact_context (the map key of this '
                                                                      'row code and description), '
                                                                      'native_event_cd / native_nlmc (native '
                                                                      'code map), safe_code (every observed '
                                                                      'description of the code maps to one '
                                                                      'concept), cross_arm_display '
                                                                      '(SDI_PATH_XARM_V1: a '
                                                                      'Millennium-linked row takes the '
                                                                      'concept the TFC LIMS arm map gives '
                                                                      'the same analyte display, and that '
                                                                      'arm tier), placeholder_code '
                                                                      '(SDI_PATH_PLACEHOLDER_V1: an '
                                                                      'unused/internal placeholder test '
                                                                      'code, unmapped by design), or '
                                                                      'unmapped.',
                                           'test_mapping_rule_id': 'map_pathology.test_mapping_rule_id: Rule '
                                                                   'that set a cross_arm_display or '
                                                                   'placeholder_code test mapping '
                                                                   '(SDI_PATH_XARM_V1 | '
                                                                   'SDI_PATH_PLACEHOLDER_V1); NULL when the '
                                                                   'row map key, a native map or the '
                                                                   'safe-code rule set it, or nothing did.',
                                           'ucum_code': 'UCUM unit code.',
                                           'unit_concept_id': 'OMOP unit concept identifier.',
                                           'unit_source_value': 'Verbatim source unit.',
                                           'value_canonical': 'Silver value_canonical after the Gold '
                                                              'value_number repairs: NULL wherever '
                                                              'value_number is nulled; where the conversion '
                                                              'leaves the value unchanged (identity, '
                                                              'egfr_unit_implied, inr_unit_implied) it is '
                                                              'the repaired value_number, so the haematocrit '
                                                              'percentage rescale carries through. '
                                                              'value_as_number restated in '
                                                              'canonical_ucum_code (see '
                                                              'canonical_conversion); equal to '
                                                              'value_as_number unless converted. NULL when '
                                                              'canonical_conversion is NULL or '
                                                              'unit_label_conflict.',
                                           'value_concept_display': 'Coded result concept display.',
                                           'value_concept_id': 'Coded result concept identifier.',
                                           'value_datetime': 'Datetime result value.',
                                           'value_number': 'Numeric result value.',
                                           'value_text': 'Verbatim result value.',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each pathology result record. It is '
                                                      'produced by the silver transformation and has no '
                                                      'direct bronze-column lineage entry. Null means the '
                                                      'source did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Withheld identity indicator.'},
                               'quantity_override': False,
                               'table': 'One pathology result with typed value, units, range, interpretation '
                                        'and mapping evidence. Gold QC twin of the silver product: 14 '
                                        'columns are repaired or nulled, 1 rule(s) drop rows, 5 check(s) are '
                                        'advisory. Each rule states its reason in the pipeline notebook, and '
                                        'Lakeflow expectation metrics report what every rule matched on each '
                                        'update. Coverage: HbA1c results for October-December 2021 are held '
                                        'as (Pending) with no value in the source LIMS (9% of October, 60% '
                                        'of November and 79% of December keys) and were never replaced by a '
                                        'numeric version; complete-through month per release: '
                                        '6_mgmt.data_quality.release_finding (finding_code '
                                        'F2_complete_through).'}}
