"""Gold pathways.hrg_grouping_diagnosis: 2 dataset(s).

- datasets: _qc_clinical_hrg_grouping_diagnosis, clinical_hrg_grouping_diagnosis
- moved verbatim from: 4_gold_care_pathways:4369, 4_gold_care_pathways:4389, 4_gold_care_pathways:4395, 4_gold_care_pathways:4410

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.hrg_grouping_diagnosis ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_hrg_grouping applies the parent Gold drop rule.
CLINICAL_HRG_GROUPING_DIAGNOSIS_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_object` AS `source_object`',
    '`cds_id` AS `cds_id`',
    '`sequence` AS `sequence`',
    '`icd10_code` AS `icd10_code`',
    '`display` AS `display`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_HRG_GROUPING_DIAGNOSIS_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_hrg_grouping_diagnosis.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_hrg_grouping_diagnosis"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_HRG_GROUPING_DIAGNOSIS_MANDATORY_RULES)
def _gold_qc_clinical_hrg_grouping_diagnosis():
    """Task 3 Gold child of clinical_hrg_grouping; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.hrg_grouping")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_hrg_grouping_diagnosis"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_HRG_GROUPING_DIAGNOSIS_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.hrg_grouping_diagnosis"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_hrg_grouping_diagnosis():
    """Publish clinical_hrg_grouping_diagnosis without exposing the internal parent-admission marker."""
    return _with_row_key(spark.read.table(_n("gold_qc._clinical_hrg_grouping_diagnosis")).drop("__gold_parent_present"), "clinical_hrg_grouping_diagnosis")  # INSR_W3 W3.09


def _public_steps_clinical_hrg_grouping_diagnosis(df):
    """clinical_hrg_grouping_diagnosis: the public body after its projection (GoldRules.public_select), moved from gold_clinical_hrg_grouping_diagnosis; df is the projected twin frame."""
    return _with_row_key(df, 'clinical_hrg_grouping_diagnosis')


RULES = {
    'clinical_hrg_grouping_diagnosis': GoldRules(
        table='clinical_hrg_grouping_diagnosis',
        select=CLINICAL_HRG_GROUPING_DIAGNOSIS_SELECT,
        mandatory=CLINICAL_HRG_GROUPING_DIAGNOSIS_MANDATORY_RULES,
        row_key_sql=("sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), "
         "map('ignoreNullFields', 'false')), 256)"),
        row_key_columns='patient_event_key, sequence',
        public_select={'all_but': ['__gold_parent_present']},
        public_steps=(_public_steps_clinical_hrg_grouping_diagnosis,)),
}
_rules = QcBinding(RULES)
_with_row_key = _rules._with_row_key


finalise()


COMMENTS = {'_qc_clinical_hrg_grouping_diagnosis': {'columns': {'cds_id': 'Native CDS_APC_ID or CDS_OPA_ID within '
                                                               'source_object.',
                                                     'display': 'Reserved display text; populated in Session '
                                                                '3.',
                                                     'icd10_code': 'Verbatim ICD-10 diagnosis code from the '
                                                                   'HRG source array.',
                                                     'loaded_at': 'Bronze load timestamp inherited from the '
                                                                  'HRG grouping row.',
                                                     'patient_event_key': 'Deterministic key of the parent '
                                                                          'clinical_hrg_grouping row.',
                                                     'sequence': 'One-based source order of the ICD-10 '
                                                                 'diagnosis code.',
                                                     'source_object': 'SLAM source arm; part of the native '
                                                                      'parent key.'},
                                         'quantity_override': True,
                                         'table': 'Internal QC of clinical_hrg_grouping_diagnosis; counts '
                                                  'rows whose Gold parent is absent.'},
 'clinical_hrg_grouping_diagnosis': {'columns': {'row_key': 'Row key: SHA-256 of the JSON of '
                                                            '(patient_event_key, sequence), NULLs written '
                                                            'explicitly. Unique per row; stable while those '
                                                            'values are unchanged. INSR_W3 W3.09.'},
                                     'quantity_override': False,
                                     'table': 'One ICD-10 diagnosis code per HRG grouping row in source '
                                              'order.'}}
