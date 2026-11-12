"""Gold forms.form_response: 2 dataset(s).

- datasets: _qc_clinical_form_response, clinical_form_response
- moved verbatim from: 6_gold_forms:233, 6_gold_forms:256, 6_gold_forms:268

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_tables.forms._shared import _s3b_form_qc
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


CLINICAL_FORM_RESPONSE_MANDATORY_RULES = {
    # Preserve the existing parent requirement, with native rejection counts.
    "gold.clinical_form_response.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_form_response"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_FORM_RESPONSE_MANDATORY_RULES)
def _gold_qc_clinical_form_response():
    """Prepare internal QC data for clinical_form_response; native rules run on the returned rows."""
    df = _s3b_form_qc("clinical_form_response", ("question", "answer", "unit"))
    df = insr_w4_apply_value_rules('clinical_form_response', df)  # INSR_W4
    return df


@dp.materialized_view(
    name=_n("gold_clinical.form_response"), table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_form_response():
    """Publish clinical_form_response with its existing research columns and quality policy."""
    df = spark.read.table(_n("gold_qc._clinical_form_response")).drop("__gold_parent_present")
    return (_s3_gate_direct_public(df, "clinical_form_response")
            .drop(*[f"_{axis}_{suffix}" for axis in ("question", "answer", "unit")
                    for suffix in ("gate_pass", "policy_id")]))


def _public_steps_clinical_form_response(df):
    """clinical_form_response: the public body after its projection (GoldRules.public_select), moved from gold_clinical_form_response; df is the projected twin frame."""
    return _s3_gate_direct_public(df, 'clinical_form_response').drop(*[f'_{axis}_{suffix}' for axis in ('question', 'answer', 'unit') for suffix in ('gate_pass', 'policy_id')])


RULES = {
    'clinical_form_response': GoldRules(
        table='clinical_form_response',
        mandatory=CLINICAL_FORM_RESPONSE_MANDATORY_RULES,
        s3b_direct_axes=(('question', False), ('answer', False), ('unit', False)),
        value_rules={'gold.clinical.form_response.answer_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                  'column': 'answer_snomed_code',
                                                                                  'companions': ['answer_snomed_display',
                                                                                                 'answer_target_code',
                                                                                                 'answer_target_display'],
                                                                                  'match': '`answer_snomed_code` '
                                                                                           'IS NOT NULL '
                                                                                           'AND (NOT '
                                                                                           '`answer_snomed_code` '
                                                                                           'RLIKE '
                                                                                           "'^[1-9][0-9]{5,17}$' "
                                                                                           'OR '
                                                                                           'substring(`answer_snomed_code`, '
                                                                                           'length(`answer_snomed_code`) '
                                                                                           '- 2, 2) NOT IN '
                                                                                           "('00', '10') "
                                                                                           'OR '
                                                                                           '(substring(`answer_snomed_code`, '
                                                                                           'length(`answer_snomed_code`) '
                                                                                           "- 2, 2) = '10' "
                                                                                           'AND '
                                                                                           'length(`answer_snomed_code`) '
                                                                                           '< 11))',
                                                                                  'table': 'clinical_form_response'},
         'gold.clinical.form_response.answer_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                  'column': 'answer_target_code',
                                                                                  'companions': ['answer_target_display',
                                                                                                 'answer_snomed_code',
                                                                                                 'answer_snomed_display'],
                                                                                  'match': '`answer_target_system` '
                                                                                           '= '
                                                                                           "'http://snomed.info/sct' "
                                                                                           'AND '
                                                                                           '`answer_target_code` '
                                                                                           'IS NOT NULL '
                                                                                           'AND (NOT '
                                                                                           '`answer_target_code` '
                                                                                           'RLIKE '
                                                                                           "'^[1-9][0-9]{5,17}$' "
                                                                                           'OR '
                                                                                           'substring(`answer_target_code`, '
                                                                                           'length(`answer_target_code`) '
                                                                                           '- 2, 2) NOT IN '
                                                                                           "('00', '10') "
                                                                                           'OR '
                                                                                           '(substring(`answer_target_code`, '
                                                                                           'length(`answer_target_code`) '
                                                                                           "- 2, 2) = '10' "
                                                                                           'AND '
                                                                                           'length(`answer_target_code`) '
                                                                                           '< 11))',
                                                                                  'table': 'clinical_form_response'},
         'gold.clinical.form_response.question_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                    'column': 'question_snomed_code',
                                                                                    'companions': ['question_snomed_display',
                                                                                                   'question_target_code',
                                                                                                   'question_target_display'],
                                                                                    'match': '`question_snomed_code` '
                                                                                             'IS NOT NULL '
                                                                                             'AND (NOT '
                                                                                             '`question_snomed_code` '
                                                                                             'RLIKE '
                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                             'OR '
                                                                                             'substring(`question_snomed_code`, '
                                                                                             'length(`question_snomed_code`) '
                                                                                             '- 2, 2) NOT '
                                                                                             "IN ('00', "
                                                                                             "'10') OR "
                                                                                             '(substring(`question_snomed_code`, '
                                                                                             'length(`question_snomed_code`) '
                                                                                             '- 2, 2) = '
                                                                                             "'10' AND "
                                                                                             'length(`question_snomed_code`) '
                                                                                             '< 11))',
                                                                                    'table': 'clinical_form_response'},
         'gold.clinical.form_response.question_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                    'column': 'question_target_code',
                                                                                    'companions': ['question_target_display',
                                                                                                   'question_snomed_code',
                                                                                                   'question_snomed_display'],
                                                                                    'match': '`question_target_system` '
                                                                                             '= '
                                                                                             "'http://snomed.info/sct' "
                                                                                             'AND '
                                                                                             '`question_target_code` '
                                                                                             'IS NOT NULL '
                                                                                             'AND (NOT '
                                                                                             '`question_target_code` '
                                                                                             'RLIKE '
                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                             'OR '
                                                                                             'substring(`question_target_code`, '
                                                                                             'length(`question_target_code`) '
                                                                                             '- 2, 2) NOT '
                                                                                             "IN ('00', "
                                                                                             "'10') OR "
                                                                                             '(substring(`question_target_code`, '
                                                                                             'length(`question_target_code`) '
                                                                                             '- 2, 2) = '
                                                                                             "'10' AND "
                                                                                             'length(`question_target_code`) '
                                                                                             '< 11))',
                                                                                    'table': 'clinical_form_response'},
         'gold.clinical.form_response.unit_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                'column': 'unit_snomed_code',
                                                                                'companions': ['unit_snomed_display',
                                                                                               'unit_target_code',
                                                                                               'unit_target_display'],
                                                                                'match': '`unit_snomed_code` '
                                                                                         'IS NOT NULL AND '
                                                                                         '(NOT '
                                                                                         '`unit_snomed_code` '
                                                                                         'RLIKE '
                                                                                         "'^[1-9][0-9]{5,17}$' "
                                                                                         'OR '
                                                                                         'substring(`unit_snomed_code`, '
                                                                                         'length(`unit_snomed_code`) '
                                                                                         '- 2, 2) NOT IN '
                                                                                         "('00', '10') OR "
                                                                                         '(substring(`unit_snomed_code`, '
                                                                                         'length(`unit_snomed_code`) '
                                                                                         "- 2, 2) = '10' "
                                                                                         'AND '
                                                                                         'length(`unit_snomed_code`) '
                                                                                         '< 11))',
                                                                                'table': 'clinical_form_response'},
         'gold.clinical.form_response.unit_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                'column': 'unit_target_code',
                                                                                'companions': ['unit_target_display',
                                                                                               'unit_snomed_code',
                                                                                               'unit_snomed_display'],
                                                                                'match': '`unit_target_system` '
                                                                                         '= '
                                                                                         "'http://snomed.info/sct' "
                                                                                         'AND '
                                                                                         '`unit_target_code` '
                                                                                         'IS NOT NULL AND '
                                                                                         '(NOT '
                                                                                         '`unit_target_code` '
                                                                                         'RLIKE '
                                                                                         "'^[1-9][0-9]{5,17}$' "
                                                                                         'OR '
                                                                                         'substring(`unit_target_code`, '
                                                                                         'length(`unit_target_code`) '
                                                                                         '- 2, 2) NOT IN '
                                                                                         "('00', '10') OR "
                                                                                         '(substring(`unit_target_code`, '
                                                                                         'length(`unit_target_code`) '
                                                                                         "- 2, 2) = '10' "
                                                                                         'AND '
                                                                                         'length(`unit_target_code`) '
                                                                                         '< 11))',
                                                                                'table': 'clinical_form_response'}},
        public_select={'all_but': ['__gold_parent_present']},
        public_steps=(_public_steps_clinical_form_response,)),
}
_rules = QcBinding(RULES)
_s3_gate_direct_public = _rules._s3_gate_direct_public
insr_w4_apply_value_rules = _rules.insr_w4_apply_value_rules


finalise()


COMMENTS = {'_qc_clinical_form_response': {'columns': {},
                                'quantity_override': False,
                                'table': 'Internal S3b Gold twin of response-grain PowerForm observations.'},
 'clinical_form_response': {'columns': {},
                            'quantity_override': False,
                            'table': 'Response-grain PowerForm observations with governed question, answer '
                                     'and unit targets.'}}
