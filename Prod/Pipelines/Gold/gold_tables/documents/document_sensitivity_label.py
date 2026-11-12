"""Gold documents.document_sensitivity_label: 2 dataset(s).

- datasets: _qc_text_document_sensitivity_label, text_document_sensitivity_label
- moved verbatim from: 5_gold_documents:418, 5_gold_documents:432, 5_gold_documents:438, 5_gold_documents:453

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_text.document_sensitivity_label ====

# contract v2: Task 3 child; mandatory parent-admission check against text_document applies the parent Gold drop rule.
TEXT_DOCUMENT_SENSITIVITY_LABEL_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`sequence` AS `sequence`',
    '`sensitivity_label` AS `sensitivity_label`',
    '`loaded_at` AS `loaded_at`',
]


TEXT_DOCUMENT_SENSITIVITY_LABEL_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.text_document_sensitivity_label.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._text_document_sensitivity_label"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(TEXT_DOCUMENT_SENSITIVITY_LABEL_MANDATORY_RULES)
def _gold_qc_text_document_sensitivity_label():
    """Task 3 Gold child of text_document; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_text.document")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("text_document_sensitivity_label"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*TEXT_DOCUMENT_SENSITIVITY_LABEL_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_text.document_sensitivity_label"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_text_document_sensitivity_label():
    """Publish text_document_sensitivity_label without exposing the internal parent-admission marker."""
    return _with_row_key(spark.read.table(_n("gold_qc._text_document_sensitivity_label")).drop("__gold_parent_present"), "text_document_sensitivity_label")  # INSR_W3 W3.09


def _public_steps_text_document_sensitivity_label(df):
    """text_document_sensitivity_label: the public body after its projection (GoldRules.public_select), moved from gold_text_document_sensitivity_label; df is the projected twin frame."""
    return _with_row_key(df, 'text_document_sensitivity_label')


RULES = {
    'text_document_sensitivity_label': GoldRules(
        table='text_document_sensitivity_label',
        select=TEXT_DOCUMENT_SENSITIVITY_LABEL_SELECT,
        mandatory=TEXT_DOCUMENT_SENSITIVITY_LABEL_MANDATORY_RULES,
        row_key_sql=("sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), "
         "map('ignoreNullFields', 'false')), 256)"),
        row_key_columns='patient_event_key, sequence',
        public_select={'all_but': ['__gold_parent_present']},
        public_steps=(_public_steps_text_document_sensitivity_label,)),
}
_rules = QcBinding(RULES)
_with_row_key = _rules._with_row_key


finalise()


COMMENTS = {'_qc_text_document_sensitivity_label': {'columns': {'loaded_at': 'Bronze load timestamp inherited from the '
                                                                  'parent document row.',
                                                     'patient_event_key': 'Deterministic key of the parent '
                                                                          'text_document row.',
                                                     'sensitivity_label': 'Verbatim source sensitivity or '
                                                                          'security label.',
                                                     'sequence': 'One-based order of the source sensitivity '
                                                                 'label.'},
                                         'quantity_override': True,
                                         'table': 'Internal QC of text_document_sensitivity_label; counts '
                                                  'rows whose Gold parent is absent.'},
 'text_document_sensitivity_label': {'columns': {'row_key': 'Row key: SHA-256 of the JSON of '
                                                            '(patient_event_key, sequence), NULLs written '
                                                            'explicitly. Unique per row; stable while those '
                                                            'values are unchanged. INSR_W3 W3.09.'},
                                     'quantity_override': False,
                                     'table': 'One ordered sensitivity label per clinical text document.'}}
