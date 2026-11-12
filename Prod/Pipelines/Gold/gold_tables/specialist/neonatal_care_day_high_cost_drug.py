"""Gold specialist.neonatal_care_day_high_cost_drug: 2 dataset(s).

- datasets: _qc_clinical_neonatal_care_day_high_cost_drug, clinical_neonatal_care_day_high_cost_drug
- moved verbatim from: 3_gold_specialist_care:2822, 3_gold_specialist_care:2840, 3_gold_specialist_care:2846, 3_gold_specialist_care:2861

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.neonatal_care_day_high_cost_drug ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_neonatal_care_day applies the parent Gold drop rule.
CLINICAL_NEONATAL_CARE_DAY_HIGH_COST_DRUG_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entity_id` AS `entity_id`',
    '`activity_date` AS `activity_date`',
    '`sequence` AS `sequence`',
    '`high_cost_drug_code` AS `high_cost_drug_code`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_NEONATAL_CARE_DAY_HIGH_COST_DRUG_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_neonatal_care_day_high_cost_drug.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_neonatal_care_day_high_cost_drug"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_NEONATAL_CARE_DAY_HIGH_COST_DRUG_MANDATORY_RULES)
def _gold_qc_clinical_neonatal_care_day_high_cost_drug():
    """Task 3 Gold child of clinical_neonatal_care_day; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.neonatal_care_day")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_neonatal_care_day_high_cost_drug"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_NEONATAL_CARE_DAY_HIGH_COST_DRUG_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.neonatal_care_day_high_cost_drug"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_neonatal_care_day_high_cost_drug():
    """Publish clinical_neonatal_care_day_high_cost_drug without exposing the internal parent-admission marker."""
    return _with_row_key(spark.read.table(_n("gold_qc._clinical_neonatal_care_day_high_cost_drug")).drop("__gold_parent_present"), "clinical_neonatal_care_day_high_cost_drug")  # INSR_W3 W3.09


def _public_steps_clinical_neonatal_care_day_high_cost_drug(df):
    """clinical_neonatal_care_day_high_cost_drug: the public body after its projection (GoldRules.public_select), moved from gold_clinical_neonatal_care_day_high_cost_drug; df is the projected twin frame."""
    return _with_row_key(df, 'clinical_neonatal_care_day_high_cost_drug')


RULES = {
    'clinical_neonatal_care_day_high_cost_drug': GoldRules(
        table='clinical_neonatal_care_day_high_cost_drug',
        select=CLINICAL_NEONATAL_CARE_DAY_HIGH_COST_DRUG_SELECT,
        mandatory=CLINICAL_NEONATAL_CARE_DAY_HIGH_COST_DRUG_MANDATORY_RULES,
        row_key_sql=("sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), "
         "map('ignoreNullFields', 'false')), 256)"),
        row_key_columns='patient_event_key, sequence',
        public_select={'all_but': ['__gold_parent_present']},
        public_steps=(_public_steps_clinical_neonatal_care_day_high_cost_drug,)),
}
_rules = QcBinding(RULES)
_with_row_key = _rules._with_row_key


finalise()


COMMENTS = {'_qc_clinical_neonatal_care_day_high_cost_drug': {'columns': {'activity_date': 'BadgerNet ActivityDate; '
                                                                                'second native parent-key '
                                                                                'component.',
                                                               'entity_id': 'BadgerNet EntityID; first '
                                                                            'native parent-key component.',
                                                               'high_cost_drug_code': 'Verbatim neonatal '
                                                                                      'high-cost-drug code.',
                                                               'loaded_at': 'Bronze load timestamp inherited '
                                                                            'from the care-day row.',
                                                               'patient_event_key': 'Deterministic key of '
                                                                                    'the parent '
                                                                                    'clinical_neonatal_care_day '
                                                                                    'row.',
                                                               'sequence': 'One-based source position from '
                                                                           'HCDRUG1 through HCDRUG20.'},
                                                   'quantity_override': True,
                                                   'table': 'Internal QC of '
                                                            'clinical_neonatal_care_day_high_cost_drug; '
                                                            'counts rows whose Gold parent is absent.'},
 'clinical_neonatal_care_day_high_cost_drug': {'columns': {'row_key': 'Row key: SHA-256 of the JSON of '
                                                                      '(patient_event_key, sequence), NULLs '
                                                                      'written explicitly. Unique per row; '
                                                                      'stable while those values are '
                                                                      'unchanged. INSR_W3 W3.09.'},
                                               'quantity_override': False,
                                               'table': 'One BadgerNet high-cost-drug code per neonatal care '
                                                        'day.'}}
