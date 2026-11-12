"""Gold specialist.cancer_treatment_cycle: 2 dataset(s).

- datasets: _qc_clinical_cancer_treatment_cycle, clinical_cancer_treatment_cycle
- moved verbatim from: 3_gold_specialist_care:501, 3_gold_specialist_care:526, 3_gold_specialist_care:533, 3_gold_specialist_care:565, 3_gold_specialist_care:577

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.cancer_treatment_cycle ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CANCER_TREATMENT_CYCLE_SELECT = [
    '`cancer_treatment_cycle_key` AS `cancer_treatment_cycle_key`',
    '`iqemo_course_id` AS `iqemo_course_id`',
    '`cycle_sequence_id` AS `cycle_sequence_id`',
    '`regimen_cycle_id` AS `regimen_cycle_id`',
    '`cycle_code` AS `cycle_code`',
    '`prescribed_datetime` AS `prescribed_datetime`',
    '`pharmacy_confirmed_datetime` AS `pharmacy_confirmed_datetime`',
    '`cycle_start_datetime` AS `cycle_start_datetime`',
    '`cancellation_datetime` AS `cancellation_datetime`',
    "CASE WHEN UPPER(TRIM(CAST(`cycle_status_code` AS STRING))) = '0' THEN NULL ELSE `cycle_status_code` END AS `cycle_status_code`",
    '`treatment_response_id` AS `treatment_response_id`',
    '`line_of_treatment` AS `line_of_treatment`',
    '`regimen_number` AS `regimen_number`',
    '`course_link_status` AS `course_link_status`',
    '`outcome_comments` AS `outcome_comments`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`chemotherapy_course_id` AS `chemotherapy_course_id`',
    '`treatment_cycle_id` AS `treatment_cycle_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_cancer_treatment_cycle_metadata; source history stays on the main research table.
CLINICAL_CANCER_TREATMENT_CYCLE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 294,917 rows of 294,917 that
    # are current and attributable. Superseded versions and rows whose identity was never
    # resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}


CLINICAL_CANCER_TREATMENT_CYCLE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 294,917 at the profile.
    "gold.clinical.cancer_treatment_cycle.record_status.default_view_active":
        "record_status = 'active'",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_cancer_treatment_cycle"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_CANCER_TREATMENT_CYCLE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CANCER_TREATMENT_CYCLE_ADVISORY_RULES)
def _gold_qc_clinical_cancer_treatment_cycle():
    """Quality-controlled twin of journey_clinical.cancer_treatment_cycle."""
    df = _qc("clinical_cancer_treatment_cycle", CLINICAL_CANCER_TREATMENT_CYCLE_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.cancer_treatment_cycle"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_cancer_treatment_cycle():
    """Contract-v2 public twin of clinical_cancer_treatment_cycle; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_cancer_treatment_cycle")).selectExpr(
        '`cancer_treatment_cycle_key` AS `cancer_treatment_cycle_key`',
        '`iqemo_course_id` AS `iqemo_course_id`',
        '`cycle_sequence_id` AS `cycle_sequence_id`',
        '`regimen_cycle_id` AS `regimen_cycle_id`',
        '`cycle_code` AS `cycle_code`',
        '`prescribed_datetime` AS `prescribed_datetime`',
        '`pharmacy_confirmed_datetime` AS `pharmacy_confirmed_datetime`',
        '`cycle_start_datetime` AS `cycle_start_datetime`',
        '`cancellation_datetime` AS `cancellation_datetime`',
        '`cycle_status_code` AS `cycle_status_code`',
        '`treatment_response_id` AS `treatment_response_id`',
        '`line_of_treatment` AS `line_of_treatment`',
        '`regimen_number` AS `regimen_number`',
        '`course_link_status` AS `course_link_status`',
        '`outcome_comments` AS `outcome_comments`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`chemotherapy_course_id` AS `chemotherapy_course_id`',
        '`treatment_cycle_id` AS `treatment_cycle_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'clinical_cancer_treatment_cycle': GoldRules(
        table='clinical_cancer_treatment_cycle',
        select=CLINICAL_CANCER_TREATMENT_CYCLE_SELECT,
        mandatory=CLINICAL_CANCER_TREATMENT_CYCLE_MANDATORY_RULES,
        advisory=CLINICAL_CANCER_TREATMENT_CYCLE_ADVISORY_RULES,
        lifecycle={'join_keys': ['cancer_treatment_cycle_key'],
         'support_columns': ['record_status', 'load_batch_id', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_cancer_treatment_cycle': {'columns': {'cancellation_datetime': 'Sentinel-cleaned cancellation '
                                                                              'timestamp.',
                                                     'cancer_treatment_cycle_key': 'Deterministic SHA-256 '
                                                                                   'key for the iQemo '
                                                                                   'course-cycle pair.',
                                                     'chemotherapy_course_id': 'iQemo CHEMOTHERAPY_COURSE_ID '
                                                                               'as BIGINT; first component '
                                                                               'of the primary key.',
                                                     'course_link_status': 'Parent course-link status.',
                                                     'cycle_code': 'Source cycle code.',
                                                     'cycle_sequence_id': 'Verbatim treatment-cycle sequence '
                                                                          'token.',
                                                     'cycle_start_datetime': 'Sentinel-cleaned cycle start '
                                                                             'timestamp.',
                                                     'cycle_status_code': 'Source cycle status code.',
                                                     'iqemo_course_id': 'iQemo chemotherapy course '
                                                                        'identifier.',
                                                     'line_of_treatment': 'Coded lookup identifier for the '
                                                                          'line of treatment represented by '
                                                                          'the parent chemotherapy course.',
                                                     'loaded_at': 'map_cancer_treatment_cycle.PIPELINE_UPDT_DT_TM '
                                                                  'carried unchanged. No treatment-parent '
                                                                  'clock is joined, and ADC_UPDT, '
                                                                  'prescription dates, cancellation time and '
                                                                  'the current Silver refresh time are not '
                                                                  'substituted.',
                                                     'outcome_comments': 'Source outcome comments.',
                                                     'pharmacy_confirmed_datetime': 'Sentinel-cleaned '
                                                                                    'pharmacy-confirmed '
                                                                                    'timestamp.',
                                                     'prescribed_datetime': 'Sentinel-cleaned prescribed '
                                                                            'timestamp.',
                                                     'record_status': 'Always active in the current '
                                                                      'map_cancer_treatment_cycle '
                                                                      'projection. CycleStatus and '
                                                                      'CANCELLATION_DATE_CLEAN remain '
                                                                      'separate clinical fields and do not '
                                                                      'change this literal label; no '
                                                                      'superseded or retracted status is '
                                                                      'emitted.',
                                                     'regimen_cycle_id': 'Regimen cycle identifier.',
                                                     'regimen_number': 'Sequential number identifying the '
                                                                       'regimen within the parent '
                                                                       'chemotherapy course.',
                                                     'source_row_id': 'Composite source row identifier.',
                                                     'source_table': 'Registered source table.',
                                                     'treatment_cycle_id': 'iQemo TREATMENT_CYCLE_ID as '
                                                                           'BIGINT; second component of the '
                                                                           'primary key.',
                                                     'treatment_response_id': 'Source treatment-response '
                                                                              'identifier from '
                                                                              '`TreatmentResponseID`, '
                                                                              'retained as a string on the '
                                                                              'treatment cycle.'},
                                         'quantity_override': True,
                                         'table': 'Internal quality-controlled twin of '
                                                  'clinical_cancer_treatment_cycle: the Gold repairs, nulls '
                                                  'and expectations are applied here and the published Gold '
                                                  'MV reads this object. Materialized rather than temporary '
                                                  'so the published MV can refresh incrementally.'},
 'clinical_cancer_treatment_cycle': {'columns': {'cancellation_datetime': 'Sentinel-cleaned cancellation '
                                                                          'timestamp.',
                                                 'cancer_treatment_cycle_key': 'Deterministic SHA-256 key '
                                                                               'for the iQemo course-cycle '
                                                                               'pair.',
                                                 'chemotherapy_course_id': 'iQemo CHEMOTHERAPY_COURSE_ID as '
                                                                           'BIGINT; first component of the '
                                                                           'primary key.',
                                                 'course_link_status': 'Parent course-link status.',
                                                 'cycle_code': 'Source cycle code.',
                                                 'cycle_sequence_id': 'Verbatim treatment-cycle sequence '
                                                                      'token.',
                                                 'cycle_start_datetime': 'Sentinel-cleaned cycle start '
                                                                         'timestamp.',
                                                 'cycle_status_code': 'Source cycle status code.',
                                                 'iqemo_course_id': 'iQemo chemotherapy course identifier.',
                                                 'line_of_treatment': 'Coded lookup identifier for the line '
                                                                      'of treatment represented by the '
                                                                      'parent chemotherapy course.',
                                                 'loaded_at': 'map_cancer_treatment_cycle.PIPELINE_UPDT_DT_TM '
                                                              'carried unchanged. No treatment-parent clock '
                                                              'is joined, and ADC_UPDT, prescription dates, '
                                                              'cancellation time and the current Silver '
                                                              'refresh time are not substituted.',
                                                 'outcome_comments': 'Source outcome comments.',
                                                 'pharmacy_confirmed_datetime': 'Sentinel-cleaned '
                                                                                'pharmacy-confirmed '
                                                                                'timestamp.',
                                                 'prescribed_datetime': 'Sentinel-cleaned prescribed '
                                                                        'timestamp.',
                                                 'record_status': 'Always active in the current '
                                                                  'map_cancer_treatment_cycle projection. '
                                                                  'CycleStatus and CANCELLATION_DATE_CLEAN '
                                                                  'remain separate clinical fields and do '
                                                                  'not change this literal label; no '
                                                                  'superseded or retracted status is '
                                                                  'emitted.',
                                                 'regimen_cycle_id': 'Regimen cycle identifier.',
                                                 'regimen_number': 'Sequential number identifying the '
                                                                   'regimen within the parent chemotherapy '
                                                                   'course.',
                                                 'source_row_id': 'Composite source row identifier.',
                                                 'source_table': 'Registered source table.',
                                                 'treatment_cycle_id': 'iQemo TREATMENT_CYCLE_ID as BIGINT; '
                                                                       'second component of the primary key.',
                                                 'treatment_response_id': 'Source treatment-response '
                                                                          'identifier from '
                                                                          '`TreatmentResponseID`, retained '
                                                                          'as a string on the treatment '
                                                                          'cycle.'},
                                     'quantity_override': False,
                                     'table': 'One iQemo chemotherapy cycle child reference at composite '
                                              'course/cycle grain. Gold QC twin of the silver product: 1 '
                                              'columns are repaired or nulled, 1 rule(s) drop rows, 1 '
                                              'check(s) are advisory. Each rule states its reason in the '
                                              'pipeline notebook, and Lakeflow expectation metrics report '
                                              'what every rule matched on each update. Coverage: iQemo holds '
                                              'regimens, courses and cycles but no per-drug rows at source '
                                              '(its only drug column is a G-CSF regimen drug id), so '
                                              'chemotherapy is published at cycle grain without individual '
                                              'agents; complete-through month per release: '
                                              '6_mgmt.data_quality.release_finding (finding_code '
                                              'F2_complete_through).'}}
