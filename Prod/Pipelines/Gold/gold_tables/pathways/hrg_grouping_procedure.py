"""Gold pathways.hrg_grouping_procedure: 2 dataset(s).

- datasets: _qc_clinical_hrg_grouping_procedure, clinical_hrg_grouping_procedure
- moved verbatim from: 4_gold_care_pathways:4425, 4_gold_care_pathways:4445, 4_gold_care_pathways:4451, 4_gold_care_pathways:4466

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.hrg_grouping_procedure ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_hrg_grouping applies the parent Gold drop rule.
CLINICAL_HRG_GROUPING_PROCEDURE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_object` AS `source_object`',
    '`cds_id` AS `cds_id`',
    '`sequence` AS `sequence`',
    '`opcs4_code` AS `opcs4_code`',
    '`display` AS `display`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_HRG_GROUPING_PROCEDURE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_hrg_grouping_procedure.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_hrg_grouping_procedure"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_HRG_GROUPING_PROCEDURE_MANDATORY_RULES)
def _gold_qc_clinical_hrg_grouping_procedure():
    """Task 3 Gold child of clinical_hrg_grouping; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.hrg_grouping")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_hrg_grouping_procedure"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_HRG_GROUPING_PROCEDURE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.hrg_grouping_procedure"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_hrg_grouping_procedure():
    """Publish clinical_hrg_grouping_procedure without exposing the internal parent-admission marker."""
    return _with_row_key(spark.read.table(_n("gold_qc._clinical_hrg_grouping_procedure")).drop("__gold_parent_present"), "clinical_hrg_grouping_procedure")  # INSR_W3 W3.09


def _public_steps_clinical_hrg_grouping_procedure(df):
    """clinical_hrg_grouping_procedure: the public body after its projection (GoldRules.public_select), moved from gold_clinical_hrg_grouping_procedure; df is the projected twin frame."""
    return _with_row_key(df, 'clinical_hrg_grouping_procedure')


RULES = {
    'clinical_hrg_grouping_procedure': GoldRules(
        table='clinical_hrg_grouping_procedure',
        select=CLINICAL_HRG_GROUPING_PROCEDURE_SELECT,
        mandatory=CLINICAL_HRG_GROUPING_PROCEDURE_MANDATORY_RULES,
        row_key_sql=("sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), "
         "map('ignoreNullFields', 'false')), 256)"),
        row_key_columns='patient_event_key, sequence',
        public_select={'all_but': ['__gold_parent_present']},
        public_steps=(_public_steps_clinical_hrg_grouping_procedure,)),
}
_rules = QcBinding(RULES)
_with_row_key = _rules._with_row_key


finalise()


COMMENTS = {'_qc_clinical_hrg_grouping_procedure': {'columns': {'cds_id': 'Native CDS_APC_ID or CDS_OPA_ID within '
                                                               'source_object.',
                                                     'display': 'Reserved display text; populated in Session '
                                                                '3.',
                                                     'loaded_at': 'Bronze load timestamp inherited from the '
                                                                  'HRG grouping row.',
                                                     'opcs4_code': 'Verbatim OPCS-4 procedure code from the '
                                                                   'HRG source array.',
                                                     'patient_event_key': 'Deterministic key of the parent '
                                                                          'clinical_hrg_grouping row.',
                                                     'sequence': 'One-based source order of the OPCS-4 '
                                                                 'procedure code.',
                                                     'source_object': 'SLAM source arm; part of the native '
                                                                      'parent key.'},
                                         'quantity_override': True,
                                         'table': 'Internal QC of clinical_hrg_grouping_procedure; counts '
                                                  'rows whose Gold parent is absent.'},
 'clinical_hrg_grouping_procedure': {'columns': {'row_key': 'Row key: SHA-256 of the JSON of '
                                                            '(patient_event_key, sequence), NULLs written '
                                                            'explicitly. Unique per row; stable while those '
                                                            'values are unchanged. INSR_W3 W3.09.'},
                                     'quantity_override': False,
                                     'table': 'One OPCS-4 procedure code per HRG grouping row in source '
                                              'order.'}}
