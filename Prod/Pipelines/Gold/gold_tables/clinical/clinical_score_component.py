"""Gold clinical.clinical_score_component: 2 dataset(s).

- datasets: _qc_clinical_clinical_score_component, clinical_clinical_score_component
- moved verbatim from: 2_gold_clinical_events:7032, 2_gold_clinical_events:7069, 2_gold_clinical_events:7075, 2_gold_clinical_events:7096

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, _s3_axis_columns, _s3_axis_gate_and_policy, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# EIR_20261021_V1: gold events_patient_event (and its QC twin) retired with the silver event index.

# ==== journey_clinical.clinical_score_component ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_clinical_score applies the parent Gold drop rule.
CLINICAL_CLINICAL_SCORE_COMPONENT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`event_id` AS `event_id`',
    '`doc_response_key` AS `doc_response_key`',
    '`source_object` AS `source_object`',
    '`sequence` AS `sequence`',
    '`component_id` AS `component_id`',
    '`coding_system` AS `coding_system`',
    '`coding_code` AS `coding_code`',
    '`coding_display` AS `coding_display`',
    *[f'`{name}` AS `{name}`' for name in _s3_axis_columns("component")],
    '`value_number` AS `value_number`',
    '`value_text` AS `value_text`',
    '`unit` AS `unit`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_CLINICAL_SCORE_COMPONENT_SELECT += [  # GOLD_MV_SPLIT_V1: parent drop-rule inputs silver carries; the R4 view published them
    '`parent_identity_status` AS `parent_identity_status`',
    '`parent_person_id` AS `parent_person_id`',
    '`parent_row_count` AS `parent_row_count`',
]


CLINICAL_CLINICAL_SCORE_COMPONENT_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_clinical_score_component.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_clinical_score_component"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_CLINICAL_SCORE_COMPONENT_MANDATORY_RULES)
def _gold_qc_clinical_clinical_score_component():
    """Prepare internal QC data for clinical_clinical_score_component; native rules run on the returned rows."""
    parent = spark.read.table(_n("gold_clinical.clinical_score")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_clinical_score_component"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_CLINICAL_SCORE_COMPONENT_SELECT, "__gold_parent_present AS __gold_parent_present")
    axis = "component"
    required = _s3_axis_columns(axis)
    item = {name[len(axis)+1:]: F.col(name) for name in required}
    gate, policy_id = _s3_axis_gate_and_policy(item)
    has_target = item["target_code"].isNotNull() | item["snomed_code"].isNotNull() | item["omop_concept_id"].isNotNull()
    return (df.withColumn("_component_gate_pass", F.when(has_target, gate))
            .withColumn("_component_policy_id", F.when(has_target, policy_id)))


@dp.materialized_view(
    name=_n("gold_clinical.clinical_score_component"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_clinical_score_component():
    """Task 3 Gold child with governed component targets and retained provenance."""
    df = spark.read.table(_n("gold_qc._clinical_clinical_score_component"))
    public = (_s3_gate_direct_public(df, "clinical_clinical_score_component")
              .drop("_component_gate_pass", "_component_policy_id", "__gold_parent_present"))
    public = insr_w4_apply_value_rules('clinical_clinical_score_component', public)  # INSR_W4
    return public


def _public_steps_clinical_clinical_score_component(df):
    """clinical_clinical_score_component: the public body after its projection (GoldRules.public_select), moved from gold_clinical_clinical_score_component; df is the projected twin frame."""
    public = _s3_gate_direct_public(df, 'clinical_clinical_score_component').drop('_component_gate_pass', '_component_policy_id', '__gold_parent_present')
    public = insr_w4_apply_value_rules('clinical_clinical_score_component', public)
    return public


RULES = {
    'clinical_clinical_score_component': GoldRules(
        table='clinical_clinical_score_component',
        select=CLINICAL_CLINICAL_SCORE_COMPONENT_SELECT,
        mandatory=CLINICAL_CLINICAL_SCORE_COMPONENT_MANDATORY_RULES,
        s3b_direct_axes=(('component', False),),
        value_rules={'gold.clinical.clinical_score_component.component_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                'column': 'component_snomed_code',
                                                                                                'companions': ['component_snomed_display',
                                                                                                               'component_target_code',
                                                                                                               'component_target_display'],
                                                                                                'match': '`component_snomed_code` '
                                                                                                         'IS '
                                                                                                         'NOT '
                                                                                                         'NULL '
                                                                                                         'AND '
                                                                                                         '(NOT '
                                                                                                         '`component_snomed_code` '
                                                                                                         'RLIKE '
                                                                                                         "'^[1-9][0-9]{5,17}$' "
                                                                                                         'OR '
                                                                                                         'substring(`component_snomed_code`, '
                                                                                                         'length(`component_snomed_code`) '
                                                                                                         '- '
                                                                                                         '2, '
                                                                                                         '2) '
                                                                                                         'NOT '
                                                                                                         'IN '
                                                                                                         "('00', "
                                                                                                         "'10') "
                                                                                                         'OR '
                                                                                                         '(substring(`component_snomed_code`, '
                                                                                                         'length(`component_snomed_code`) '
                                                                                                         '- '
                                                                                                         '2, '
                                                                                                         '2) '
                                                                                                         '= '
                                                                                                         "'10' "
                                                                                                         'AND '
                                                                                                         'length(`component_snomed_code`) '
                                                                                                         '< '
                                                                                                         '11))',
                                                                                                'table': 'clinical_clinical_score_component'},
         'gold.clinical.clinical_score_component.component_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                'column': 'component_target_code',
                                                                                                'companions': ['component_target_display',
                                                                                                               'component_snomed_code',
                                                                                                               'component_snomed_display'],
                                                                                                'match': '`component_target_system` '
                                                                                                         '= '
                                                                                                         "'http://snomed.info/sct' "
                                                                                                         'AND '
                                                                                                         '`component_target_code` '
                                                                                                         'IS '
                                                                                                         'NOT '
                                                                                                         'NULL '
                                                                                                         'AND '
                                                                                                         '(NOT '
                                                                                                         '`component_target_code` '
                                                                                                         'RLIKE '
                                                                                                         "'^[1-9][0-9]{5,17}$' "
                                                                                                         'OR '
                                                                                                         'substring(`component_target_code`, '
                                                                                                         'length(`component_target_code`) '
                                                                                                         '- '
                                                                                                         '2, '
                                                                                                         '2) '
                                                                                                         'NOT '
                                                                                                         'IN '
                                                                                                         "('00', "
                                                                                                         "'10') "
                                                                                                         'OR '
                                                                                                         '(substring(`component_target_code`, '
                                                                                                         'length(`component_target_code`) '
                                                                                                         '- '
                                                                                                         '2, '
                                                                                                         '2) '
                                                                                                         '= '
                                                                                                         "'10' "
                                                                                                         'AND '
                                                                                                         'length(`component_target_code`) '
                                                                                                         '< '
                                                                                                         '11))',
                                                                                                'table': 'clinical_clinical_score_component'}},
        public_select={'all_but': []},
        public_steps=(_public_steps_clinical_clinical_score_component,)),
}
_rules = QcBinding(RULES)
_s3_gate_direct_public = _rules._s3_gate_direct_public
insr_w4_apply_value_rules = _rules.insr_w4_apply_value_rules


finalise()


COMMENTS = {'_qc_clinical_clinical_score_component': {'columns': {},
                                           'quantity_override': False,
                                           'table': 'Internal quality-controlled twin of '
                                                    'clinical_clinical_score_component with the S3b '
                                                    'admission decision.'},
 'clinical_clinical_score_component': {'columns': {'coding_code': 'Source component code.',
                                                   'coding_display': 'Source component display.',
                                                   'coding_system': 'Source coding-system namespace for the '
                                                                    'component.',
                                                   'component_id': 'Native source component identifier.',
                                                   'component_map_candidate_count': 'Governed S3b '
                                                                                    'clinical-score '
                                                                                    'component mapping field '
                                                                                    'carried from Silver.',
                                                   'component_map_candidate_set_id': 'Governed S3b '
                                                                                     'clinical-score '
                                                                                     'component mapping '
                                                                                     'field carried from '
                                                                                     'Silver.',
                                                   'component_map_competing_count': 'Governed S3b '
                                                                                    'clinical-score '
                                                                                    'component mapping field '
                                                                                    'carried from Silver.',
                                                   'component_map_component_count': 'Governed S3b '
                                                                                    'clinical-score '
                                                                                    'component mapping field '
                                                                                    'carried from Silver.',
                                                   'component_map_component_index': 'Governed S3b '
                                                                                    'clinical-score '
                                                                                    'component mapping field '
                                                                                    'carried from Silver.',
                                                   'component_map_confidence': 'Governed S3b clinical-score '
                                                                               'component mapping field '
                                                                               'carried from Silver.',
                                                   'component_map_cosine': 'Governed S3b clinical-score '
                                                                           'component mapping field carried '
                                                                           'from Silver.',
                                                   'component_map_method': 'Governed S3b clinical-score '
                                                                           'component mapping field carried '
                                                                           'from Silver.',
                                                   'component_map_rollup_levels': 'Governed S3b '
                                                                                  'clinical-score component '
                                                                                  'mapping field carried '
                                                                                  'from Silver.',
                                                   'component_map_rule_id': 'Governed S3b clinical-score '
                                                                            'component mapping field carried '
                                                                            'from Silver.',
                                                   'component_map_scoring_model': 'Governed S3b '
                                                                                  'clinical-score component '
                                                                                  'mapping field carried '
                                                                                  'from Silver.',
                                                   'component_map_status': 'Governed S3b clinical-score '
                                                                           'component mapping field carried '
                                                                           'from Silver.',
                                                   'component_map_status_reason': 'Governed S3b '
                                                                                  'clinical-score component '
                                                                                  'mapping field carried '
                                                                                  'from Silver.',
                                                   'component_map_version': 'Governed S3b clinical-score '
                                                                            'component mapping field carried '
                                                                            'from Silver.',
                                                   'component_omop_concept_id': 'Governed S3b clinical-score '
                                                                                'component mapping field '
                                                                                'carried from Silver.',
                                                   'component_omop_source_concept_id': 'Governed S3b '
                                                                                       'clinical-score '
                                                                                       'component mapping '
                                                                                       'field carried from '
                                                                                       'Silver.',
                                                   'component_snomed_code': 'Governed S3b clinical-score '
                                                                            'component mapping field carried '
                                                                            'from Silver.',
                                                   'component_snomed_display': 'Governed S3b clinical-score '
                                                                               'component mapping field '
                                                                               'carried from Silver.',
                                                   'component_source_code': 'Governed S3b clinical-score '
                                                                            'component mapping field carried '
                                                                            'from Silver.',
                                                   'component_source_display': 'Governed S3b clinical-score '
                                                                               'component mapping field '
                                                                               'carried from Silver.',
                                                   'component_source_system': 'Governed S3b clinical-score '
                                                                              'component mapping field '
                                                                              'carried from Silver.',
                                                   'component_target_code': 'Governed S3b clinical-score '
                                                                            'component mapping field carried '
                                                                            'from Silver.',
                                                   'component_target_display': 'Governed S3b clinical-score '
                                                                               'component mapping field '
                                                                               'carried from Silver.',
                                                   'component_target_system': 'Governed S3b clinical-score '
                                                                              'component mapping field '
                                                                              'carried from Silver.',
                                                   'doc_response_key': 'PowerForm DOC_RESPONSE_KEY for the '
                                                                       'form-promotion parent arm.',
                                                   'event_id': 'Millennium numeric EVENT_ID for the '
                                                               'numeric-event parent arm.',
                                                   'loaded_at': 'Bronze load timestamp inherited from the '
                                                                'parent source row.',
                                                   'patient_event_key': 'Deterministic key of the parent '
                                                                        'clinical_clinical_score row.',
                                                   'sequence': 'Component sequence within the parent score.',
                                                   'source_object': 'Parent source arm: numeric_event or '
                                                                    'form_promotion.',
                                                   'unit': 'Source component unit when supplied.',
                                                   'value_number': 'Numeric component value when supplied.',
                                                   'value_text': 'Verbatim component text when supplied.'},
                                       'quantity_override': False,
                                       'table': 'One ordered component per native or governed promoted '
                                                'clinical score.'}}
