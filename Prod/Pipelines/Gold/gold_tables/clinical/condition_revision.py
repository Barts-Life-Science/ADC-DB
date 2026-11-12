"""Gold clinical.condition_revision: 2 dataset(s).

- datasets: _qc_clinical_condition_revision, clinical_condition_revision
- moved verbatim from: 2_gold_clinical_events:7115, 2_gold_clinical_events:7162, 2_gold_clinical_events:7168, 2_gold_clinical_events:7184

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.condition_revision ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_condition applies the parent Gold drop rule.
CLINICAL_CONDITION_REVISION_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`problem_id` AS `problem_id`',
    '`sequence` AS `sequence`',
    '`revision_rank` AS `revision_rank`',
    '`is_current_revision` AS `is_current_revision`',
    '`clinical_status_code` AS `clinical_status_code`',
    '`clinical_status_display` AS `clinical_status_display`',
    '`clinical_status_datetime` AS `clinical_status_datetime`',
    '`verification_status_code` AS `verification_status_code`',
    '`verification_status_display` AS `verification_status_display`',
    '`display` AS `display`',
    '`severity_code` AS `severity_code`',
    '`severity_display` AS `severity_display`',
    '`onset_datetime` AS `onset_datetime`',
    '`asserted_datetime` AS `asserted_datetime`',
    '`effective_start` AS `effective_start`',
    # INSR_W3 W3.07: Gold: NULL where an open-end marker (J17).
    "CASE WHEN (CAST(`effective_end` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`effective_end` AS DATE)) < 9999) THEN NULL ELSE `effective_end` END AS `effective_end`",
    '`source_tombstone_ind` AS `source_tombstone_ind`',
    '`loaded_at` AS `loaded_at`',
    '`source_update_timestamp` AS `source_update_timestamp`',
]


CLINICAL_CONDITION_REVISION_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_condition_revision.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_condition_revision"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_CONDITION_REVISION_MANDATORY_RULES)
def _gold_qc_clinical_condition_revision():
    """Task 3 Gold child of clinical_condition; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.condition")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_condition_revision"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_CONDITION_REVISION_SELECT, "__gold_parent_present AS __gold_parent_present")
    df = insr_w4_apply_value_rules('clinical_condition_revision', df)  # INSR_W4
    return df


@dp.materialized_view(
    name=_n("gold_clinical.condition_revision"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_condition_revision():
    """Publish clinical_condition_revision without exposing the internal parent-admission marker."""
    return _with_row_key(spark.read.table(_n("gold_qc._clinical_condition_revision")).drop("__gold_parent_present"), "clinical_condition_revision")  # INSR_W3 W3.09


def _public_steps_clinical_condition_revision(df):
    """clinical_condition_revision: the public body after its projection (GoldRules.public_select), moved from gold_clinical_condition_revision; df is the projected twin frame."""
    return _with_row_key(df, 'clinical_condition_revision')


RULES = {
    'clinical_condition_revision': GoldRules(
        table='clinical_condition_revision',
        select=CLINICAL_CONDITION_REVISION_SELECT,
        mandatory=CLINICAL_CONDITION_REVISION_MANDATORY_RULES,
        row_key_sql=("sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), "
         "map('ignoreNullFields', 'false')), 256)"),
        row_key_columns='patient_event_key, sequence',
        value_rules={'gold.clinical.condition_revision.severity_code.insr_f7_zero_code': {'action': 'normalize',
                                                                              'column': 'severity_code',
                                                                              'companions': [],
                                                                              'match': 'TRIM(`severity_code`) '
                                                                                       "= '0' AND "
                                                                                       '`severity_display` '
                                                                                       'IS NULL',
                                                                              'table': 'clinical_condition_revision'},
         'gold.clinical.condition_revision.verification_status_code.insr_f7_zero_code': {'action': 'normalize',
                                                                                         'column': 'verification_status_code',
                                                                                         'companions': [],
                                                                                         'match': 'TRIM(`verification_status_code`) '
                                                                                                  "= '0' "
                                                                                                  'AND '
                                                                                                  '`verification_status_display` '
                                                                                                  'IS NULL',
                                                                                         'table': 'clinical_condition_revision'}},
        public_select={'all_but': ['__gold_parent_present']},
        public_steps=(_public_steps_clinical_condition_revision,)),
}
_rules = QcBinding(RULES)
_with_row_key = _rules._with_row_key
insr_w4_apply_value_rules = _rules.insr_w4_apply_value_rules


finalise()


COMMENTS = {'_qc_clinical_condition_revision': {'columns': {'asserted_datetime': 'Source assertion timestamp carried by '
                                                                      'this revision.',
                                                 'clinical_status_code': 'Source lifecycle-status code for '
                                                                         'this revision.',
                                                 'clinical_status_datetime': 'Source lifecycle-status '
                                                                             'timestamp for this revision.',
                                                 'clinical_status_display': 'Source lifecycle-status display '
                                                                            'for this revision.',
                                                 'display': 'Best available source problem display for this '
                                                            'revision.',
                                                 'effective_end': 'Source revision effective-end timestamp.',
                                                 'effective_start': 'Source revision effective-start '
                                                                    'timestamp.',
                                                 'is_current_revision': 'Whether the source marks this as '
                                                                        'the current problem revision.',
                                                 'loaded_at': 'Bronze load timestamp of the problem-history '
                                                              'row.',
                                                 'onset_datetime': 'Source onset timestamp carried by this '
                                                                   'revision.',
                                                 'patient_event_key': 'Deterministic key of the parent '
                                                                      'clinical_condition row.',
                                                 'problem_id': 'Millennium PROBLEM_ID of the parent problem '
                                                               'condition.',
                                                 'revision_rank': 'Source problem revision rank.',
                                                 'sequence': 'Problem revision sequence, defaulted to zero '
                                                             'when the source rank is null.',
                                                 'severity_code': 'Source severity code for this revision.',
                                                 'severity_display': 'Source severity display for this '
                                                                     'revision.',
                                                 'source_tombstone_ind': 'Whether the source revision is '
                                                                         'marked deleted.',
                                                 'source_update_timestamp': 'Source application update '
                                                                            'datetime.',
                                                 'verification_status_code': 'Source confirmation-status '
                                                                             'code for this revision.',
                                                 'verification_status_display': 'Source confirmation-status '
                                                                                'display for this revision.'},
                                     'quantity_override': True,
                                     'table': 'Internal QC of clinical_condition_revision; counts rows whose '
                                              'Gold parent is absent.'},
 'clinical_condition_revision': {'columns': {'row_key': 'Row key: SHA-256 of the JSON of (patient_event_key, '
                                                        'sequence), NULLs written explicitly. Unique per '
                                                        'row; stable while those values are unchanged. '
                                                        'INSR_W3 W3.09.'},
                                 'quantity_override': False,
                                 'table': 'One Millennium problem-history revision per parent '
                                          'clinical_condition problem row.'}}
