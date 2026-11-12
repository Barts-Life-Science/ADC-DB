"""Gold spine.person: 2 dataset(s).

- datasets: _qc_spine_person, spine_person
- moved verbatim from: 1_gold_spine_and_reference:1535, 1_gold_spine_and_reference:1579, 1_gold_spine_and_reference:1634, 1_gold_spine_and_reference:1643, 1_gold_spine_and_reference:1647, 1_gold_spine_and_reference:1670

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, _s3_axis_columns, _src, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_spine.person ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_PERSON_SELECT = [
    '`person_id` AS `person_id`',
    '`active` AS `active`',
    "CASE WHEN UPPER(TRIM(CAST(`gender_code` AS STRING))) = '0' THEN NULL ELSE `gender_code` END AS `gender_code`",
    '`gender_display` AS `gender_display`',
    "CASE WHEN UPPER(TRIM(CAST(`ethnicity_code` AS STRING))) = '0' THEN NULL ELSE `ethnicity_code` END AS `ethnicity_code`",
    '`ethnicity_display` AS `ethnicity_display`',
    # INSR_W3 W3.06: Gold: NULL where a 1901-01-01 / 1902-01-01 / 1910-01-01 birth placeholder (P3a: x66 / x24 / x160 the median day).
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN CAST(`birth_date` AS DATE) IN (DATE'1901-01-01', DATE'1902-01-01', DATE'1910-01-01') THEN NULL ELSE CASE WHEN CAST(`birth_date` AS DATE) = DATE'1899-12-30' OR CAST(`birth_date` AS DATE) = DATE'1900-01-01' OR YEAR(CAST(`birth_date` AS DATE)) >= 2100 OR CAST(`birth_date` AS DATE) < DATE'1901-01-01' AND CAST(`birth_date` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `birth_date` END END AS `birth_date`",
    # INSR_W3 W3.06: Gold: NULL where a 1901-01-01 / 1902-01-01 / 1910-01-01 birth placeholder (P3a: x66 / x24 / x160 the median day).
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN CAST(`birth_datetime` AS DATE) IN (DATE'1901-01-01', DATE'1902-01-01', DATE'1910-01-01') THEN NULL ELSE CASE WHEN CAST(`birth_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`birth_datetime` AS DATE) = DATE'1900-01-01' OR YEAR(CAST(`birth_datetime` AS DATE)) >= 2100 OR CAST(`birth_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`birth_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `birth_datetime` END END AS `birth_datetime`",
    "CASE WHEN UPPER(TRIM(CAST(`birth_precision_code` AS STRING))) = '0' THEN NULL ELSE `birth_precision_code` END AS `birth_precision_code`",
    '`birth_precision_display` AS `birth_precision_display`',
    '`birth_precision_flag` AS `birth_precision_flag`',
    "CASE WHEN UPPER(TRIM(CAST(`language_code` AS STRING))) = '0' THEN NULL ELSE `language_code` END AS `language_code`",
    '`language_display` AS `language_display`',
    "CASE WHEN UPPER(TRIM(CAST(`marital_status_code` AS STRING))) = '0' THEN NULL ELSE `marital_status_code` END AS `marital_status_code`",
    '`marital_status_display` AS `marital_status_display`',
    "CASE WHEN UPPER(TRIM(CAST(`religion_code` AS STRING))) = '0' THEN NULL ELSE `religion_code` END AS `religion_code`",
    '`religion_display` AS `religion_display`',
    '`deceased_ind` AS `deceased_ind`',
    '`deceased_ind_source` AS `deceased_ind_source`',  # SDI_DECEASED_V1/gold:
    'CASE WHEN `deceased_datetime` < `birth_datetime` THEN NULL ELSE `deceased_datetime` END AS `deceased_datetime`',
    '`deceased_datetime_precision` AS `deceased_datetime_precision`',
    "CASE WHEN UPPER(TRIM(CAST(`confidentiality_code` AS STRING))) = '0' THEN NULL ELSE `confidentiality_code` END AS `confidentiality_code`",
    '`vip_ind` AS `vip_ind`',
    '`current_address_id` AS `current_address_id`',
    '`latest_known_address_id` AS `latest_known_address_id`',
    '`address_selection_status` AS `address_selection_status`',
    '`current_mrn` AS `current_mrn`',
    '`current_mrn_status` AS `current_mrn_status`',
    '`mrn_selection_status` AS `mrn_selection_status`',
    '`current_mrn_pool` AS `current_mrn_pool`',  # SDI_MRN_POOL_V1/gold:
    '`nhs_number` AS `nhs_number`',
    '`nhs_number_status` AS `nhs_number_status`',
    '`nhs_number_selection_status` AS `nhs_number_selection_status`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


SPINE_PERSON_SELECT += [f"`{name}` AS `{name}`" for name in _s3_axis_columns("ethnicity")]


# INSR_W3 W3.03 (J8, F24): test patients are reported, never dropped.
SPINE_PERSON_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.spine.person.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    "gold.spine.person.test_patient.warn": "NOT COALESCE(`test_patient_ind`, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
SPINE_PERSON_PERSON_SCOPE_RULES = {
    "gold.spine.person.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_person"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(SPINE_PERSON_PERSON_SCOPE_RULES)
@dp.expect_all(SPINE_PERSON_ADVISORY_RULES)  # INSR_W3 W3.03
def _gold_qc_spine_person():
    """Quality-controlled twin of journey_spine.person."""
    df = _qc("spine_person", SPINE_PERSON_SELECT)
    # INSR_W3 W3.03 (J8): person descriptors from silver reference_person_scope, joined here only.
    descriptors = spark.read.table(_src("reference_person_scope")).select(
        F.col("person_id").cast("bigint").alias("_qc_desc_person_id"),
        "bhrut_mrn",
        "has_any_activity",
        "person_source",
        "test_patient_ind",
        "test_patient_basis",
        F.col("last_activity_date").alias("last_known_activity_date"),  # GOLD_MV_SPLIT_V1: as the R4 view published
    )
    df = (df.join(descriptors, F.expr("try_cast(person_id AS BIGINT)") == F.col("_qc_desc_person_id"), "left")
          .drop("_qc_desc_person_id"))
    return df


@dp.materialized_view(
    name=_n("gold_spine.person"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_person():
    """Contract-v2 public twin of spine_person; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_person")).selectExpr(
        '`person_id` AS `person_id`',
        '`active` AS `active`',
        '`gender_code` AS `gender_code`',
        '`gender_display` AS `gender_display`',
        '`ethnicity_code` AS `ethnicity_code`',
        '`ethnicity_display` AS `ethnicity_display`',
        '`birth_date` AS `birth_date`',
        '`birth_datetime` AS `birth_datetime`',
        '`birth_precision_code` AS `birth_precision_code`',
        '`birth_precision_display` AS `birth_precision_display`',
        '`birth_precision_flag` AS `birth_precision_flag`',
        '`language_code` AS `language_code`',
        '`language_display` AS `language_display`',
        '`marital_status_code` AS `marital_status_code`',
        '`marital_status_display` AS `marital_status_display`',
        '`religion_code` AS `religion_code`',
        '`religion_display` AS `religion_display`',
        '`deceased_ind` AS `deceased_ind`',
        '`deceased_ind_source` AS `deceased_ind_source`',  # SDI_DECEASED_V1/gold:
        '`deceased_datetime` AS `deceased_datetime`',
        '`deceased_datetime_precision` AS `deceased_datetime_precision`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`current_address_id` AS `current_address_id`',
        '`latest_known_address_id` AS `latest_known_address_id`',
        '`address_selection_status` AS `address_selection_status`',
        '`current_mrn` AS `current_mrn`',
        '`current_mrn_status` AS `current_mrn_status`',
        '`mrn_selection_status` AS `mrn_selection_status`',
        '`current_mrn_pool` AS `current_mrn_pool`',  # SDI_MRN_POOL_V1/gold:
        '`bhrut_mrn` AS `bhrut_mrn`',  # INSR_W3 W3.03 (J8): has_any_activity..test_patient_basis too
        '`has_any_activity` AS `has_any_activity`',
        '`person_source` AS `person_source`',
        '`test_patient_ind` AS `test_patient_ind`',
        '`test_patient_basis` AS `test_patient_basis`',
        '`nhs_number` AS `nhs_number`',
        '`nhs_number_status` AS `nhs_number_status`',
        '`nhs_number_selection_status` AS `nhs_number_selection_status`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        *[f"`{name}` AS `{name}`" for name in _s3_axis_columns("ethnicity")],
        '`_ethnicity_gate_pass` AS `_ethnicity_gate_pass`',
        '`_ethnicity_policy_id` AS `_ethnicity_policy_id`',
    )
    return (_s3_gate_direct_public(df, "spine_person")
            .drop("_ethnicity_gate_pass", "_ethnicity_policy_id"))


def _public_steps_spine_person(df):
    """spine_person: the public body after its projection (GoldRules.public_select), moved from gold_spine_person; df is the projected twin frame."""
    return _s3_gate_direct_public(df, 'spine_person').drop('_ethnicity_gate_pass', '_ethnicity_policy_id')


RULES = {
    'spine_person': GoldRules(
        table='spine_person',
        select=SPINE_PERSON_SELECT,
        advisory=SPINE_PERSON_ADVISORY_RULES,
        person_scope=SPINE_PERSON_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['person_id'],
         'support_columns': ['record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        s3b_direct_axes=(('ethnicity', False),),
        value_rules={'gold.spine.person.ethnicity_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                           'column': 'ethnicity_snomed_code',
                                                                           'companions': ['ethnicity_snomed_display',
                                                                                          'ethnicity_target_code',
                                                                                          'ethnicity_target_display'],
                                                                           'match': '`ethnicity_snomed_code` '
                                                                                    'IS NOT NULL AND (NOT '
                                                                                    '`ethnicity_snomed_code` '
                                                                                    'RLIKE '
                                                                                    "'^[1-9][0-9]{5,17}$' "
                                                                                    'OR '
                                                                                    'substring(`ethnicity_snomed_code`, '
                                                                                    'length(`ethnicity_snomed_code`) '
                                                                                    "- 2, 2) NOT IN ('00', "
                                                                                    "'10') OR "
                                                                                    '(substring(`ethnicity_snomed_code`, '
                                                                                    'length(`ethnicity_snomed_code`) '
                                                                                    "- 2, 2) = '10' AND "
                                                                                    'length(`ethnicity_snomed_code`) '
                                                                                    '< 11))',
                                                                           'table': 'spine_person'},
         'gold.spine.person.ethnicity_source_code.insr_f7_zero_code': {'action': 'normalize',
                                                                       'column': 'ethnicity_source_code',
                                                                       'companions': [],
                                                                       'match': 'TRIM(`ethnicity_source_code`) '
                                                                                "= '0' AND "
                                                                                '`ethnicity_source_display` '
                                                                                'IS NULL',
                                                                       'table': 'spine_person'},
         'gold.spine.person.ethnicity_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                           'column': 'ethnicity_target_code',
                                                                           'companions': ['ethnicity_target_display',
                                                                                          'ethnicity_snomed_code',
                                                                                          'ethnicity_snomed_display'],
                                                                           'match': '`ethnicity_target_system` '
                                                                                    '= '
                                                                                    "'http://snomed.info/sct' "
                                                                                    'AND '
                                                                                    '`ethnicity_target_code` '
                                                                                    'IS NOT NULL AND (NOT '
                                                                                    '`ethnicity_target_code` '
                                                                                    'RLIKE '
                                                                                    "'^[1-9][0-9]{5,17}$' "
                                                                                    'OR '
                                                                                    'substring(`ethnicity_target_code`, '
                                                                                    'length(`ethnicity_target_code`) '
                                                                                    "- 2, 2) NOT IN ('00', "
                                                                                    "'10') OR "
                                                                                    '(substring(`ethnicity_target_code`, '
                                                                                    'length(`ethnicity_target_code`) '
                                                                                    "- 2, 2) = '10' AND "
                                                                                    'length(`ethnicity_target_code`) '
                                                                                    '< 11))',
                                                                           'table': 'spine_person'}},
        public_select=[
            '`person_id` AS `person_id`',
            '`active` AS `active`',
            '`gender_code` AS `gender_code`',
            '`gender_display` AS `gender_display`',
            '`ethnicity_code` AS `ethnicity_code`',
            '`ethnicity_display` AS `ethnicity_display`',
            '`birth_date` AS `birth_date`',
            '`birth_datetime` AS `birth_datetime`',
            '`birth_precision_code` AS `birth_precision_code`',
            '`birth_precision_display` AS `birth_precision_display`',
            '`birth_precision_flag` AS `birth_precision_flag`',
            '`language_code` AS `language_code`',
            '`language_display` AS `language_display`',
            '`marital_status_code` AS `marital_status_code`',
            '`marital_status_display` AS `marital_status_display`',
            '`religion_code` AS `religion_code`',
            '`religion_display` AS `religion_display`',
            '`deceased_ind` AS `deceased_ind`',
            '`deceased_ind_source` AS `deceased_ind_source`',
            '`deceased_datetime` AS `deceased_datetime`',
            '`deceased_datetime_precision` AS `deceased_datetime_precision`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`current_address_id` AS `current_address_id`',
            '`latest_known_address_id` AS `latest_known_address_id`',
            '`address_selection_status` AS `address_selection_status`',
            '`current_mrn` AS `current_mrn`',
            '`current_mrn_status` AS `current_mrn_status`',
            '`mrn_selection_status` AS `mrn_selection_status`',
            '`current_mrn_pool` AS `current_mrn_pool`',
            '`bhrut_mrn` AS `bhrut_mrn`',
            '`has_any_activity` AS `has_any_activity`',
            '`person_source` AS `person_source`',
            '`test_patient_ind` AS `test_patient_ind`',
            '`test_patient_basis` AS `test_patient_basis`',
            '`last_known_activity_date` AS `last_known_activity_date`',  # GOLD_MV_SPLIT_V1: as the R4 view
            '`nhs_number` AS `nhs_number`',
            '`nhs_number_status` AS `nhs_number_status`',
            '`nhs_number_selection_status` AS `nhs_number_selection_status`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
            *[f'`{name}` AS `{name}`' for name in _s3_axis_columns('ethnicity')],
            '`_ethnicity_gate_pass` AS `_ethnicity_gate_pass`',
            '`_ethnicity_policy_id` AS `_ethnicity_policy_id`',
        ],
        public_steps=(_public_steps_spine_person,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_gate_direct_public = _rules._s3_gate_direct_public


finalise()


COMMENTS = {'_qc_spine_person': {'columns': {'active': 'Source active indicator.',
                                  'address_selection_status': 'Provenance for current versus latest-known '
                                                              'address selection.',
                                  'bhrut_mrn': 'Current BHRUT MRN (RF4 MRN pool) from silver '
                                               'reference_person_scope, selected by the same governed alias '
                                               'rule as current_mrn; NULL when the person has no BHRUT alias '
                                               'or the selection is ambiguous.',
                                  'birth_date': 'Calendar birth date.',
                                  'birth_datetime': 'Source-compatible birth timestamp.',
                                  'birth_precision_code': 'Source code describing birth-date precision.',
                                  'birth_precision_display': 'Display for the source birth precision.',
                                  'birth_precision_flag': 'Raw source birth precision flag.',
                                  'confidentiality_code': 'Source confidentiality level carried for '
                                                          'downstream access control.',
                                  'current_address_id': 'Current source address reference when available.',
                                  'current_mrn': 'Current Barts hospital MRN (RNJ 5C4 MRN pool, 6200990) '
                                                 'selected from map_patient_identifier: active alias first, '
                                                 'then latest valid end-effective, NULLS LAST, deterministic '
                                                 'SOURCE_PK tiebreak. MRNs from other pools, including BHRUT '
                                                 'RF4 MRNs, are never selected. NULL when more than one '
                                                 'distinct active Barts MRN exists (ambiguous) or the person '
                                                 'has no Barts MRN.',
                                  'current_mrn_pool': 'Alias pool of the selected MRN: always barts (RNJ 5C4 '
                                                      'MRN) when the person has a Barts MRN, including '
                                                      'ambiguous selections; NULL when the person has no '
                                                      'Barts MRN.',
                                  'current_mrn_status': 'Lifecycle status of the selected hospital MRN '
                                                        'alias; NULL when selection is ambiguous or no alias '
                                                        'exists.',
                                  'deceased_datetime': 'Source death timestamp.',
                                  'deceased_datetime_precision': 'Raw source death-time precision.',
                                  'deceased_ind': 'True when a death date is recorded or '
                                                  "map_person.deceased_display is 'Deceased' (DECEASED_CD "
                                                  '3768549, CDF YES); false otherwise, including DECEASED_CD '
                                                  "'No' (684730) and 0. Never NULL.",
                                  'deceased_ind_source': 'Evidence behind deceased_ind: death_date (a death '
                                                         'timestamp is recorded) or deceased_code '
                                                         "(DECEASED_CD displays 'Deceased' with no date); "
                                                         'NULL when deceased_ind is false.',
                                  'ethnicity_code': 'Source ethnicity code.',
                                  'ethnicity_display': 'Source ethnicity display.',
                                  'ethnicity_map_candidate_count': 'Governed S3b ethnic-category mapping '
                                                                   'provenance.',
                                  'ethnicity_map_candidate_set_id': 'Governed S3b ethnic-category mapping '
                                                                    'provenance.',
                                  'ethnicity_map_competing_count': 'Governed S3b ethnic-category mapping '
                                                                   'provenance.',
                                  'ethnicity_map_component_count': 'Governed S3b ethnic-category mapping '
                                                                   'provenance.',
                                  'ethnicity_map_component_index': 'Governed S3b ethnic-category mapping '
                                                                   'provenance.',
                                  'ethnicity_map_confidence': 'Governed S3b ethnic-category mapping '
                                                              'provenance.',
                                  'ethnicity_map_cosine': 'Governed S3b ethnic-category mapping provenance.',
                                  'ethnicity_map_method': 'Governed S3b ethnic-category mapping provenance.',
                                  'ethnicity_map_rollup_levels': 'Governed S3b ethnic-category mapping '
                                                                 'provenance.',
                                  'ethnicity_map_rule_id': 'Governed S3b ethnic-category mapping provenance.',
                                  'ethnicity_map_scoring_model': 'Governed S3b ethnic-category mapping '
                                                                 'provenance.',
                                  'ethnicity_map_status': 'Governed S3b ethnic-category mapping provenance.',
                                  'ethnicity_map_status_reason': 'Governed S3b ethnic-category mapping '
                                                                 'provenance.',
                                  'ethnicity_map_version': 'Governed S3b ethnic-category mapping provenance.',
                                  'ethnicity_omop_concept_id': 'Governed S3b ethnic-category mapping '
                                                               'provenance.',
                                  'ethnicity_omop_source_concept_id': 'Governed S3b ethnic-category mapping '
                                                                      'provenance.',
                                  'ethnicity_snomed_code': 'Governed S3b ethnic-category mapping provenance.',
                                  'ethnicity_snomed_display': 'Governed S3b ethnic-category mapping '
                                                              'provenance.',
                                  'ethnicity_source_code': 'Governed S3b ethnic-category mapping provenance.',
                                  'ethnicity_source_display': 'Governed S3b ethnic-category mapping '
                                                              'provenance.',
                                  'ethnicity_source_system': 'Governed S3b ethnic-category mapping '
                                                             'provenance.',
                                  'ethnicity_target_code': 'Governed S3b ethnic-category mapping provenance.',
                                  'ethnicity_target_display': 'Governed S3b ethnic-category mapping '
                                                              'provenance.',
                                  'ethnicity_target_system': 'Governed S3b ethnic-category mapping '
                                                             'provenance.',
                                  'gender_code': 'Source administrative gender code.',
                                  'gender_display': 'Source administrative gender display.',
                                  'has_any_activity': 'TRUE when the person has a row of any category '
                                                      '(clinical or administrative) in a silver event feed, '
                                                      'or any Millennium encounter row. TRUE for almost '
                                                      'every gold person; FALSE only when the person is in '
                                                      'scope solely through a non-Millennium contact '
                                                      '(SLAM/CSDS/ED/theatre). The full spine, including '
                                                      'persons without activity, is in silver.',
                                  'language_code': 'Preferred language source code.',
                                  'language_display': 'Preferred language display.',
                                  'latest_known_address_id': 'Latest-known source address reference.',
                                  'loaded_at': 'Greatest of map_person.ADC_UPDT and the maximum '
                                               'map_patient_identifier.PIPELINE_UPDT_DT_TM in each matching '
                                               'MRN and NHS alias pool. These are pool-wide clocks, not just '
                                               'the displayed winning alias; missing alias clocks fall back '
                                               'to the person clock. This is contributing ingestion '
                                               'provenance, not the Silver refresh time; null only when all '
                                               'contributing clocks are null.',
                                  'marital_status_code': 'Source marital-status code.',
                                  'marital_status_display': 'Source marital-status display.',
                                  'mrn_selection_status': 'Selection outcome for the Barts MRN: '
                                                          'active_alias, latest_valid_alias, ambiguous (more '
                                                          'than one distinct active Barts MRN) or no_alias '
                                                          '(no Barts MRN, including persons known only by a '
                                                          'BHRUT MRN).',
                                  'nhs_number': 'Current NHS number selected from map_patient_identifier: '
                                                'active alias first, then latest valid end-effective, NULLS '
                                                'LAST, deterministic SOURCE_PK tiebreak; NULL when selection '
                                                'is ambiguous or no alias exists.',
                                  'nhs_number_selection_status': 'Selection outcome for the NHS-number '
                                                                 'alias: active_alias, latest_valid_alias, '
                                                                 'ambiguous (more than one distinct active '
                                                                 'NHS number) or no_alias.',
                                  'nhs_number_status': 'Lifecycle status of the selected NHS-number alias; '
                                                       'NULL when selection is ambiguous or no alias exists.',
                                  'person_id': 'Millennium person identifier and table primary key.',
                                  'person_source': 'barts, bhrut, both or none: which Trust MRN pools hold '
                                                   'an alias for the person (silver reference_person_scope).',
                                  'record_status': 'Derived person-row status: active only when '
                                                   'map_person.active_ind is 1 and end_effective_dt_tm is '
                                                   'null or not earlier than 2100-01-01; otherwise '
                                                   'superseded, including a missing active_ind. This is a '
                                                   'normalized row-status label, not the source numeric '
                                                   'ACTIVE_IND.',
                                  'record_status_effective_from': 'Source person active-status timestamp '
                                                                  'carried from `active_status_dt_tm` '
                                                                  'without a fallback timestamp.',
                                  'record_status_effective_to': 'Source `end_effective_dt_tm` when earlier '
                                                                'than 2100-01-01; otherwise null, including '
                                                                'missing or open-ended sentinel timestamps.',
                                  'religion_code': 'Source religion code.',
                                  'religion_display': 'Source religion display.',
                                  'source_update_timestamp': 'Greatest of map_person.source_updt_dt_tm and '
                                                             'the maximum '
                                                             'map_patient_identifier.SOURCE_ADC_UPDT in each '
                                                             'matching MRN and NHS alias pool. The pool '
                                                             'maxima include all aliases, even when the '
                                                             'displayed identifier is ambiguous; missing '
                                                             'alias clocks fall back to the person clock. '
                                                             'This is not the Silver refresh time; null only '
                                                             'when all contributing clocks are null.',
                                  'test_patient_basis': 'The rule that set test_patient_ind, or '
                                                        'unavailable_no_name_keys_in_bronze.',
                                  'test_patient_ind': 'TRUE when the Millennium person record matches a '
                                                      'test-patient name pattern and is a Person-type '
                                                      'record; NULL while bronze carries no name keys (see '
                                                      'test_patient_basis). Warning only: test patients are '
                                                      'not dropped (J8).',
                                  'vip_ind': 'Source VIP indicator carried as data.'},
                      'quantity_override': True,
                      'table': 'Internal quality-controlled twin of spine_person: the Gold repairs, nulls '
                               'and expectations are applied here and the published Gold MV reads this '
                               'object. Materialized rather than temporary so the published MV can refresh '
                               'incrementally.'},
 'spine_person': {'columns': {'active': 'Source active indicator.',
                              'address_selection_status': 'Provenance for current versus latest-known '
                                                          'address selection.',
                              'bhrut_mrn': 'Current BHRUT MRN (RF4 MRN pool) from silver '
                                           'reference_person_scope, selected by the same governed alias rule '
                                           'as current_mrn; NULL when the person has no BHRUT alias or the '
                                           'selection is ambiguous.',
                              'birth_date': 'Calendar birth date.',
                              'birth_datetime': 'Source-compatible birth timestamp.',
                              'birth_precision_code': 'Source code describing birth-date precision.',
                              'birth_precision_display': 'Display for the source birth precision.',
                              'birth_precision_flag': 'Raw source birth precision flag.',
                              'confidentiality_code': 'Source confidentiality level carried for downstream '
                                                      'access control.',
                              'current_address_id': 'Current source address reference when available.',
                              'current_mrn': 'Current Barts hospital MRN (RNJ 5C4 MRN pool, 6200990) '
                                             'selected from map_patient_identifier: active alias first, then '
                                             'latest valid end-effective, NULLS LAST, deterministic '
                                             'SOURCE_PK tiebreak. MRNs from other pools, including BHRUT RF4 '
                                             'MRNs, are never selected. NULL when more than one distinct '
                                             'active Barts MRN exists (ambiguous) or the person has no Barts '
                                             'MRN.',
                              'current_mrn_pool': 'Alias pool of the selected MRN: always barts (RNJ 5C4 '
                                                  'MRN) when the person has a Barts MRN, including ambiguous '
                                                  'selections; NULL when the person has no Barts MRN.',
                              'current_mrn_status': 'Lifecycle status of the selected hospital MRN alias; '
                                                    'NULL when selection is ambiguous or no alias exists.',
                              'deceased_datetime': 'Source death timestamp.',
                              'deceased_datetime_precision': 'Raw source death-time precision.',
                              'deceased_ind': 'True when a death date is recorded or '
                                              "map_person.deceased_display is 'Deceased' (DECEASED_CD "
                                              '3768549, CDF YES); false otherwise, including DECEASED_CD '
                                              "'No' (684730) and 0. Never NULL.",
                              'deceased_ind_source': 'Evidence behind deceased_ind: death_date (a death '
                                                     'timestamp is recorded) or deceased_code (DECEASED_CD '
                                                     "displays 'Deceased' with no date); NULL when "
                                                     'deceased_ind is false.',
                              'ethnicity_code': 'Source ethnicity code. Ethnicity as recorded in Millennium '
                                                '(best-effort SNOMED/OMOP map with provenance in '
                                                'ethnicity_map_*); SUS-derived ethnicity is never used to '
                                                'fill gaps.',
                              'ethnicity_display': 'Source ethnicity display.',
                              'ethnicity_map_candidate_count': 'Governed S3b ethnic-category mapping '
                                                               'provenance.',
                              'ethnicity_map_candidate_set_id': 'Governed S3b ethnic-category mapping '
                                                                'provenance.',
                              'ethnicity_map_competing_count': 'Governed S3b ethnic-category mapping '
                                                               'provenance.',
                              'ethnicity_map_component_count': 'Governed S3b ethnic-category mapping '
                                                               'provenance.',
                              'ethnicity_map_component_index': 'Governed S3b ethnic-category mapping '
                                                               'provenance.',
                              'ethnicity_map_confidence': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_map_cosine': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_map_method': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_map_rollup_levels': 'Governed S3b ethnic-category mapping '
                                                             'provenance.',
                              'ethnicity_map_rule_id': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_map_scoring_model': 'Governed S3b ethnic-category mapping '
                                                             'provenance.',
                              'ethnicity_map_status': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_map_status_reason': 'Governed S3b ethnic-category mapping '
                                                             'provenance.',
                              'ethnicity_map_version': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_omop_concept_id': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_omop_source_concept_id': 'Governed S3b ethnic-category mapping '
                                                                  'provenance.',
                              'ethnicity_snomed_code': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_snomed_display': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_source_code': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_source_display': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_source_system': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_target_code': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_target_display': 'Governed S3b ethnic-category mapping provenance.',
                              'ethnicity_target_system': 'Governed S3b ethnic-category mapping provenance.',
                              'gender_code': 'Source administrative gender code.',
                              'gender_display': 'Source administrative gender display.',
                              'has_any_activity': 'TRUE when the person has a row of any category (clinical '
                                                  'or administrative) in a silver event feed, or any '
                                                  'Millennium encounter row. TRUE for almost every gold '
                                                  'person; FALSE only when the person is in scope solely '
                                                  'through a non-Millennium contact (SLAM/CSDS/ED/theatre). '
                                                  'The full spine, including persons without activity, is in '
                                                  'silver.',
                              'language_code': 'Preferred language source code.',
                              'language_display': 'Preferred language display.',
                              'last_known_activity_date': 'Latest event date across the event feeds (silver '
                                                          'reference_person_scope.last_activity_date). Use '
                                                          'as the censor (end of follow-up) date for living '
                                                          "persons; the data cut is the feed's "
                                                          'complete-through date '
                                                          '(6_mgmt.data_quality.release_finding '
                                                          'F2_complete_through).',
                              'latest_known_address_id': 'Latest-known source address reference.',
                              'loaded_at': 'Greatest of map_person.ADC_UPDT and the maximum '
                                           'map_patient_identifier.PIPELINE_UPDT_DT_TM in each matching MRN '
                                           'and NHS alias pool. These are pool-wide clocks, not just the '
                                           'displayed winning alias; missing alias clocks fall back to the '
                                           'person clock. This is contributing ingestion provenance, not the '
                                           'Silver refresh time; null only when all contributing clocks are '
                                           'null.',
                              'marital_status_code': 'Source marital-status code.',
                              'marital_status_display': 'Source marital-status display.',
                              'mrn_selection_status': 'Selection outcome for the Barts MRN: active_alias, '
                                                      'latest_valid_alias, ambiguous (more than one distinct '
                                                      'active Barts MRN) or no_alias (no Barts MRN, '
                                                      'including persons known only by a BHRUT MRN).',
                              'nhs_number': 'Current NHS number selected from map_patient_identifier: active '
                                            'alias first, then latest valid end-effective, NULLS LAST, '
                                            'deterministic SOURCE_PK tiebreak; NULL when selection is '
                                            'ambiguous or no alias exists.',
                              'nhs_number_selection_status': 'Selection outcome for the NHS-number alias: '
                                                             'active_alias, latest_valid_alias, ambiguous '
                                                             '(more than one distinct active NHS number) or '
                                                             'no_alias.',
                              'nhs_number_status': 'Lifecycle status of the selected NHS-number alias; NULL '
                                                   'when selection is ambiguous or no alias exists.',
                              'person_id': 'Millennium person identifier and table primary key.',
                              'person_source': 'barts, bhrut, both or none: which Trust MRN pools hold an '
                                               'alias for the person (silver reference_person_scope).',
                              'record_status': 'Derived person-row status: active only when '
                                               'map_person.active_ind is 1 and end_effective_dt_tm is null '
                                               'or not earlier than 2100-01-01; otherwise superseded, '
                                               'including a missing active_ind. This is a normalized '
                                               'row-status label, not the source numeric ACTIVE_IND.',
                              'record_status_effective_from': 'Source person active-status timestamp carried '
                                                              'from `active_status_dt_tm` without a fallback '
                                                              'timestamp.',
                              'record_status_effective_to': 'Source `end_effective_dt_tm` when earlier than '
                                                            '2100-01-01; otherwise null, including missing '
                                                            'or open-ended sentinel timestamps.',
                              'religion_code': 'Source religion code.',
                              'religion_display': 'Source religion display.',
                              'source_update_timestamp': 'Greatest of map_person.source_updt_dt_tm and the '
                                                         'maximum map_patient_identifier.SOURCE_ADC_UPDT in '
                                                         'each matching MRN and NHS alias pool. The pool '
                                                         'maxima include all aliases, even when the '
                                                         'displayed identifier is ambiguous; missing alias '
                                                         'clocks fall back to the person clock. This is not '
                                                         'the Silver refresh time; null only when all '
                                                         'contributing clocks are null.',
                              'test_patient_basis': 'The rule that set test_patient_ind, or '
                                                    'unavailable_no_name_keys_in_bronze.',
                              'test_patient_ind': 'TRUE when the Millennium person record matches a '
                                                  'test-patient name pattern and is a Person-type record; '
                                                  'NULL while bronze carries no name keys (see '
                                                  'test_patient_basis). Warning only: test patients are not '
                                                  'dropped (J8).',
                              'vip_ind': 'Source VIP indicator carried as data.'},
                  'quantity_override': False,
                  'table': 'One resolved Millennium person with FHIR-aligned demographics and retained '
                           'source lifecycle. Gold QC twin of the silver product: 11 columns are repaired or '
                           'nulled, 0 check(s) are advisory. Each rule states its reason in the pipeline '
                           'notebook, and Lakeflow expectation metrics report what every rule matched on '
                           'each update. Gold person scope: a person is published when they have a '
                           'qualifying attended encounter or any clinical row (person_scope in_scope = '
                           'has_encounter OR has_clinical_data); test patients are flagged, not dropped. '
                           'Silver keeps every person. Coverage: Thirteen 1 January dates (1930-1980 decade '
                           'and mid-decade years, and 2000-01-01) hold 5 to 7 times the usual daily births: '
                           'dates estimated to the year at registration; kept as recorded; complete-through '
                           'month per release: 6_mgmt.data_quality.release_finding (finding_code '
                           'F2_complete_through).'}}
