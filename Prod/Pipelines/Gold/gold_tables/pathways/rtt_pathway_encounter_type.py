"""Gold pathways.rtt_pathway_encounter_type: 2 dataset(s).

- datasets: _qc_clinical_rtt_pathway_encounter_type, clinical_rtt_pathway_encounter_type
- moved verbatim from: 4_gold_care_pathways:4481, 4_gold_care_pathways:4501, 4_gold_care_pathways:4507, 4_gold_care_pathways:4522

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.rtt_pathway_encounter_type ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_rtt_pathway applies the parent Gold drop rule.
CLINICAL_RTT_PATHWAY_ENCOUNTER_TYPE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_system_oid` AS `source_system_oid`',
    '`pathway_oid` AS `pathway_oid`',
    '`period_oid` AS `period_oid`',
    '`sequence` AS `sequence`',
    '`encounter_type` AS `encounter_type`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_RTT_PATHWAY_ENCOUNTER_TYPE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_rtt_pathway_encounter_type.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_rtt_pathway_encounter_type"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_RTT_PATHWAY_ENCOUNTER_TYPE_MANDATORY_RULES)
def _gold_qc_clinical_rtt_pathway_encounter_type():
    """Task 3 Gold child of clinical_rtt_pathway; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.rtt_pathway")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_rtt_pathway_encounter_type"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_RTT_PATHWAY_ENCOUNTER_TYPE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.rtt_pathway_encounter_type"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_rtt_pathway_encounter_type():
    """Publish clinical_rtt_pathway_encounter_type without exposing the internal parent-admission marker."""
    return _with_row_key(spark.read.table(_n("gold_qc._clinical_rtt_pathway_encounter_type")).drop("__gold_parent_present"), "clinical_rtt_pathway_encounter_type")  # INSR_W3 W3.09


def _public_steps_clinical_rtt_pathway_encounter_type(df):
    """clinical_rtt_pathway_encounter_type: the public body after its projection (GoldRules.public_select), moved from gold_clinical_rtt_pathway_encounter_type; df is the projected twin frame."""
    return _with_row_key(df, 'clinical_rtt_pathway_encounter_type')


RULES = {
    'clinical_rtt_pathway_encounter_type': GoldRules(
        table='clinical_rtt_pathway_encounter_type',
        select=CLINICAL_RTT_PATHWAY_ENCOUNTER_TYPE_SELECT,
        mandatory=CLINICAL_RTT_PATHWAY_ENCOUNTER_TYPE_MANDATORY_RULES,
        row_key_sql=("sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), "
         "map('ignoreNullFields', 'false')), 256)"),
        row_key_columns='patient_event_key, sequence',
        public_select={'all_but': ['__gold_parent_present']},
        public_steps=(_public_steps_clinical_rtt_pathway_encounter_type,)),
}
_rules = QcBinding(RULES)
_with_row_key = _rules._with_row_key


finalise()


COMMENTS = {'_qc_clinical_rtt_pathway_encounter_type': {'columns': {'encounter_type': 'Verbatim LUNA pathway '
                                                                           'encounter-type tag.',
                                                         'loaded_at': 'Bronze load timestamp inherited from '
                                                                      'the parent pathway row.',
                                                         'pathway_oid': 'LUNA PATHWAY_OID; second native '
                                                                        'pathway-period key component.',
                                                         'patient_event_key': 'Deterministic key of the '
                                                                              'parent clinical_rtt_pathway '
                                                                              'row.',
                                                         'period_oid': 'LUNA PERIOD_OID; third native '
                                                                       'pathway-period key component.',
                                                         'sequence': 'One-based order of the encounter-type '
                                                                     'tag in the source array.',
                                                         'source_system_oid': 'LUNA SOURCE_SYSTEM_OID; first '
                                                                              'native pathway-period key '
                                                                              'component.'},
                                             'quantity_override': True,
                                             'table': 'Internal QC of clinical_rtt_pathway_encounter_type; '
                                                      'counts rows whose Gold parent is absent.'},
 'clinical_rtt_pathway_encounter_type': {'columns': {'row_key': 'Row key: SHA-256 of the JSON of '
                                                                '(patient_event_key, sequence), NULLs '
                                                                'written explicitly. Unique per row; stable '
                                                                'while those values are unchanged. INSR_W3 '
                                                                'W3.09.'},
                                         'quantity_override': False,
                                         'table': 'One ordered encounter-type tag per LUNA RTT pathway '
                                                  'period.'}}
