"""Gold forms.form_smoking_status: 2 dataset(s).

- datasets: _qc_clinical_form_smoking_status, clinical_form_smoking_status
- moved verbatim from: 6_gold_forms:228, 6_gold_forms:280, 6_gold_forms:295

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_tables.forms._shared import _s3b_form_qc
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


CLINICAL_FORM_SMOKING_STATUS_MANDATORY_RULES = {
    # Preserve the existing parent requirement, with native rejection counts.
    "gold.clinical_form_smoking_status.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_form_smoking_status"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_FORM_SMOKING_STATUS_MANDATORY_RULES)
def _gold_qc_clinical_form_smoking_status():
    """Prepare internal QC data for clinical_form_smoking_status; native rules run on the returned rows."""
    df = _s3b_form_qc(
        "clinical_form_smoking_status",
        ("smoking_status", "cessation_advice", "cessation_referral", "nicotine_treatment", "dependence_level"),
        rules=RULES["clinical_form_smoking_status"],  # INSR_R4_GOLD_V1: admission verdicts (D4)
    )
    df = insr_w4_apply_value_rules('clinical_form_smoking_status', df)  # INSR_W4
    return df


@dp.materialized_view(
    name=_n("gold_clinical.form_smoking_status"), table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_form_smoking_status():
    """Publish clinical_form_smoking_status with its existing research columns and quality policy."""
    axes = ("smoking_status", "cessation_advice", "cessation_referral", "nicotine_treatment", "dependence_level")
    df = spark.read.table(_n("gold_qc._clinical_form_smoking_status")).drop("__gold_parent_present")
    return (_s3_gate_direct_public(df, "clinical_form_smoking_status")
            .drop(*[f"_{axis}_{suffix}" for axis in axes for suffix in ("gate_pass", "policy_id")]))


def _public_steps_clinical_form_smoking_status(df):
    """clinical_form_smoking_status: the public body after its projection (GoldRules.public_select), moved from gold_clinical_form_smoking_status; df is the projected twin frame."""
    axes = ('smoking_status', 'cessation_advice', 'cessation_referral', 'nicotine_treatment', 'dependence_level')
    return _s3_gate_direct_public(df, 'clinical_form_smoking_status').drop(*[f'_{axis}_{suffix}' for axis in axes for suffix in ('gate_pass', 'policy_id')])


RULES = {
    'clinical_form_smoking_status': GoldRules(
        table='clinical_form_smoking_status',
        mandatory=CLINICAL_FORM_SMOKING_STATUS_MANDATORY_RULES,
        s3b_direct_axes=(('smoking_status', False),
         ('cessation_advice', False),
         ('cessation_referral', False),
         ('nicotine_treatment', False),
         ('dependence_level', False)),
        value_rules={'gold.clinical.form_smoking_status.cessation_advice_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                  'column': 'cessation_advice_snomed_code',
                                                                                                  'companions': ['cessation_advice_snomed_display',
                                                                                                                 'cessation_advice_target_code',
                                                                                                                 'cessation_advice_target_display'],
                                                                                                  'match': '`cessation_advice_snomed_code` '
                                                                                                           'IS '
                                                                                                           'NOT '
                                                                                                           'NULL '
                                                                                                           'AND '
                                                                                                           '(NOT '
                                                                                                           '`cessation_advice_snomed_code` '
                                                                                                           'RLIKE '
                                                                                                           "'^[1-9][0-9]{5,17}$' "
                                                                                                           'OR '
                                                                                                           'substring(`cessation_advice_snomed_code`, '
                                                                                                           'length(`cessation_advice_snomed_code`) '
                                                                                                           '- '
                                                                                                           '2, '
                                                                                                           '2) '
                                                                                                           'NOT '
                                                                                                           'IN '
                                                                                                           "('00', "
                                                                                                           "'10') "
                                                                                                           'OR '
                                                                                                           '(substring(`cessation_advice_snomed_code`, '
                                                                                                           'length(`cessation_advice_snomed_code`) '
                                                                                                           '- '
                                                                                                           '2, '
                                                                                                           '2) '
                                                                                                           '= '
                                                                                                           "'10' "
                                                                                                           'AND '
                                                                                                           'length(`cessation_advice_snomed_code`) '
                                                                                                           '< '
                                                                                                           '11))',
                                                                                                  'table': 'clinical_form_smoking_status'},
         'gold.clinical.form_smoking_status.cessation_advice_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                  'column': 'cessation_advice_target_code',
                                                                                                  'companions': ['cessation_advice_target_display',
                                                                                                                 'cessation_advice_snomed_code',
                                                                                                                 'cessation_advice_snomed_display'],
                                                                                                  'match': '`cessation_advice_target_system` '
                                                                                                           '= '
                                                                                                           "'http://snomed.info/sct' "
                                                                                                           'AND '
                                                                                                           '`cessation_advice_target_code` '
                                                                                                           'IS '
                                                                                                           'NOT '
                                                                                                           'NULL '
                                                                                                           'AND '
                                                                                                           '(NOT '
                                                                                                           '`cessation_advice_target_code` '
                                                                                                           'RLIKE '
                                                                                                           "'^[1-9][0-9]{5,17}$' "
                                                                                                           'OR '
                                                                                                           'substring(`cessation_advice_target_code`, '
                                                                                                           'length(`cessation_advice_target_code`) '
                                                                                                           '- '
                                                                                                           '2, '
                                                                                                           '2) '
                                                                                                           'NOT '
                                                                                                           'IN '
                                                                                                           "('00', "
                                                                                                           "'10') "
                                                                                                           'OR '
                                                                                                           '(substring(`cessation_advice_target_code`, '
                                                                                                           'length(`cessation_advice_target_code`) '
                                                                                                           '- '
                                                                                                           '2, '
                                                                                                           '2) '
                                                                                                           '= '
                                                                                                           "'10' "
                                                                                                           'AND '
                                                                                                           'length(`cessation_advice_target_code`) '
                                                                                                           '< '
                                                                                                           '11))',
                                                                                                  'table': 'clinical_form_smoking_status'},
         'gold.clinical.form_smoking_status.cessation_referral_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                    'column': 'cessation_referral_snomed_code',
                                                                                                    'companions': ['cessation_referral_snomed_display',
                                                                                                                   'cessation_referral_target_code',
                                                                                                                   'cessation_referral_target_display'],
                                                                                                    'match': '`cessation_referral_snomed_code` '
                                                                                                             'IS '
                                                                                                             'NOT '
                                                                                                             'NULL '
                                                                                                             'AND '
                                                                                                             '(NOT '
                                                                                                             '`cessation_referral_snomed_code` '
                                                                                                             'RLIKE '
                                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                                             'OR '
                                                                                                             'substring(`cessation_referral_snomed_code`, '
                                                                                                             'length(`cessation_referral_snomed_code`) '
                                                                                                             '- '
                                                                                                             '2, '
                                                                                                             '2) '
                                                                                                             'NOT '
                                                                                                             'IN '
                                                                                                             "('00', "
                                                                                                             "'10') "
                                                                                                             'OR '
                                                                                                             '(substring(`cessation_referral_snomed_code`, '
                                                                                                             'length(`cessation_referral_snomed_code`) '
                                                                                                             '- '
                                                                                                             '2, '
                                                                                                             '2) '
                                                                                                             '= '
                                                                                                             "'10' "
                                                                                                             'AND '
                                                                                                             'length(`cessation_referral_snomed_code`) '
                                                                                                             '< '
                                                                                                             '11))',
                                                                                                    'table': 'clinical_form_smoking_status'},
         'gold.clinical.form_smoking_status.cessation_referral_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                    'column': 'cessation_referral_target_code',
                                                                                                    'companions': ['cessation_referral_target_display',
                                                                                                                   'cessation_referral_snomed_code',
                                                                                                                   'cessation_referral_snomed_display'],
                                                                                                    'match': '`cessation_referral_target_system` '
                                                                                                             '= '
                                                                                                             "'http://snomed.info/sct' "
                                                                                                             'AND '
                                                                                                             '`cessation_referral_target_code` '
                                                                                                             'IS '
                                                                                                             'NOT '
                                                                                                             'NULL '
                                                                                                             'AND '
                                                                                                             '(NOT '
                                                                                                             '`cessation_referral_target_code` '
                                                                                                             'RLIKE '
                                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                                             'OR '
                                                                                                             'substring(`cessation_referral_target_code`, '
                                                                                                             'length(`cessation_referral_target_code`) '
                                                                                                             '- '
                                                                                                             '2, '
                                                                                                             '2) '
                                                                                                             'NOT '
                                                                                                             'IN '
                                                                                                             "('00', "
                                                                                                             "'10') "
                                                                                                             'OR '
                                                                                                             '(substring(`cessation_referral_target_code`, '
                                                                                                             'length(`cessation_referral_target_code`) '
                                                                                                             '- '
                                                                                                             '2, '
                                                                                                             '2) '
                                                                                                             '= '
                                                                                                             "'10' "
                                                                                                             'AND '
                                                                                                             'length(`cessation_referral_target_code`) '
                                                                                                             '< '
                                                                                                             '11))',
                                                                                                    'table': 'clinical_form_smoking_status'},
         'gold.clinical.form_smoking_status.dependence_level_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                  'column': 'dependence_level_snomed_code',
                                                                                                  'companions': ['dependence_level_snomed_display',
                                                                                                                 'dependence_level_target_code',
                                                                                                                 'dependence_level_target_display'],
                                                                                                  'match': '`dependence_level_snomed_code` '
                                                                                                           'IS '
                                                                                                           'NOT '
                                                                                                           'NULL '
                                                                                                           'AND '
                                                                                                           '(NOT '
                                                                                                           '`dependence_level_snomed_code` '
                                                                                                           'RLIKE '
                                                                                                           "'^[1-9][0-9]{5,17}$' "
                                                                                                           'OR '
                                                                                                           'substring(`dependence_level_snomed_code`, '
                                                                                                           'length(`dependence_level_snomed_code`) '
                                                                                                           '- '
                                                                                                           '2, '
                                                                                                           '2) '
                                                                                                           'NOT '
                                                                                                           'IN '
                                                                                                           "('00', "
                                                                                                           "'10') "
                                                                                                           'OR '
                                                                                                           '(substring(`dependence_level_snomed_code`, '
                                                                                                           'length(`dependence_level_snomed_code`) '
                                                                                                           '- '
                                                                                                           '2, '
                                                                                                           '2) '
                                                                                                           '= '
                                                                                                           "'10' "
                                                                                                           'AND '
                                                                                                           'length(`dependence_level_snomed_code`) '
                                                                                                           '< '
                                                                                                           '11))',
                                                                                                  'table': 'clinical_form_smoking_status'},
         'gold.clinical.form_smoking_status.dependence_level_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                  'column': 'dependence_level_target_code',
                                                                                                  'companions': ['dependence_level_target_display',
                                                                                                                 'dependence_level_snomed_code',
                                                                                                                 'dependence_level_snomed_display'],
                                                                                                  'match': '`dependence_level_target_system` '
                                                                                                           '= '
                                                                                                           "'http://snomed.info/sct' "
                                                                                                           'AND '
                                                                                                           '`dependence_level_target_code` '
                                                                                                           'IS '
                                                                                                           'NOT '
                                                                                                           'NULL '
                                                                                                           'AND '
                                                                                                           '(NOT '
                                                                                                           '`dependence_level_target_code` '
                                                                                                           'RLIKE '
                                                                                                           "'^[1-9][0-9]{5,17}$' "
                                                                                                           'OR '
                                                                                                           'substring(`dependence_level_target_code`, '
                                                                                                           'length(`dependence_level_target_code`) '
                                                                                                           '- '
                                                                                                           '2, '
                                                                                                           '2) '
                                                                                                           'NOT '
                                                                                                           'IN '
                                                                                                           "('00', "
                                                                                                           "'10') "
                                                                                                           'OR '
                                                                                                           '(substring(`dependence_level_target_code`, '
                                                                                                           'length(`dependence_level_target_code`) '
                                                                                                           '- '
                                                                                                           '2, '
                                                                                                           '2) '
                                                                                                           '= '
                                                                                                           "'10' "
                                                                                                           'AND '
                                                                                                           'length(`dependence_level_target_code`) '
                                                                                                           '< '
                                                                                                           '11))',
                                                                                                  'table': 'clinical_form_smoking_status'},
         'gold.clinical.form_smoking_status.nicotine_treatment_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                    'column': 'nicotine_treatment_snomed_code',
                                                                                                    'companions': ['nicotine_treatment_snomed_display',
                                                                                                                   'nicotine_treatment_target_code',
                                                                                                                   'nicotine_treatment_target_display'],
                                                                                                    'match': '`nicotine_treatment_snomed_code` '
                                                                                                             'IS '
                                                                                                             'NOT '
                                                                                                             'NULL '
                                                                                                             'AND '
                                                                                                             '(NOT '
                                                                                                             '`nicotine_treatment_snomed_code` '
                                                                                                             'RLIKE '
                                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                                             'OR '
                                                                                                             'substring(`nicotine_treatment_snomed_code`, '
                                                                                                             'length(`nicotine_treatment_snomed_code`) '
                                                                                                             '- '
                                                                                                             '2, '
                                                                                                             '2) '
                                                                                                             'NOT '
                                                                                                             'IN '
                                                                                                             "('00', "
                                                                                                             "'10') "
                                                                                                             'OR '
                                                                                                             '(substring(`nicotine_treatment_snomed_code`, '
                                                                                                             'length(`nicotine_treatment_snomed_code`) '
                                                                                                             '- '
                                                                                                             '2, '
                                                                                                             '2) '
                                                                                                             '= '
                                                                                                             "'10' "
                                                                                                             'AND '
                                                                                                             'length(`nicotine_treatment_snomed_code`) '
                                                                                                             '< '
                                                                                                             '11))',
                                                                                                    'table': 'clinical_form_smoking_status'},
         'gold.clinical.form_smoking_status.nicotine_treatment_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                    'column': 'nicotine_treatment_target_code',
                                                                                                    'companions': ['nicotine_treatment_target_display',
                                                                                                                   'nicotine_treatment_snomed_code',
                                                                                                                   'nicotine_treatment_snomed_display'],
                                                                                                    'match': '`nicotine_treatment_target_system` '
                                                                                                             '= '
                                                                                                             "'http://snomed.info/sct' "
                                                                                                             'AND '
                                                                                                             '`nicotine_treatment_target_code` '
                                                                                                             'IS '
                                                                                                             'NOT '
                                                                                                             'NULL '
                                                                                                             'AND '
                                                                                                             '(NOT '
                                                                                                             '`nicotine_treatment_target_code` '
                                                                                                             'RLIKE '
                                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                                             'OR '
                                                                                                             'substring(`nicotine_treatment_target_code`, '
                                                                                                             'length(`nicotine_treatment_target_code`) '
                                                                                                             '- '
                                                                                                             '2, '
                                                                                                             '2) '
                                                                                                             'NOT '
                                                                                                             'IN '
                                                                                                             "('00', "
                                                                                                             "'10') "
                                                                                                             'OR '
                                                                                                             '(substring(`nicotine_treatment_target_code`, '
                                                                                                             'length(`nicotine_treatment_target_code`) '
                                                                                                             '- '
                                                                                                             '2, '
                                                                                                             '2) '
                                                                                                             '= '
                                                                                                             "'10' "
                                                                                                             'AND '
                                                                                                             'length(`nicotine_treatment_target_code`) '
                                                                                                             '< '
                                                                                                             '11))',
                                                                                                    'table': 'clinical_form_smoking_status'},
         'gold.clinical.form_smoking_status.smoking_status_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                'column': 'smoking_status_snomed_code',
                                                                                                'companions': ['smoking_status_snomed_display',
                                                                                                               'smoking_status_target_code',
                                                                                                               'smoking_status_target_display'],
                                                                                                'match': '`smoking_status_snomed_code` '
                                                                                                         'IS '
                                                                                                         'NOT '
                                                                                                         'NULL '
                                                                                                         'AND '
                                                                                                         '(NOT '
                                                                                                         '`smoking_status_snomed_code` '
                                                                                                         'RLIKE '
                                                                                                         "'^[1-9][0-9]{5,17}$' "
                                                                                                         'OR '
                                                                                                         'substring(`smoking_status_snomed_code`, '
                                                                                                         'length(`smoking_status_snomed_code`) '
                                                                                                         '- '
                                                                                                         '2, '
                                                                                                         '2) '
                                                                                                         'NOT '
                                                                                                         'IN '
                                                                                                         "('00', "
                                                                                                         "'10') "
                                                                                                         'OR '
                                                                                                         '(substring(`smoking_status_snomed_code`, '
                                                                                                         'length(`smoking_status_snomed_code`) '
                                                                                                         '- '
                                                                                                         '2, '
                                                                                                         '2) '
                                                                                                         '= '
                                                                                                         "'10' "
                                                                                                         'AND '
                                                                                                         'length(`smoking_status_snomed_code`) '
                                                                                                         '< '
                                                                                                         '11))',
                                                                                                'table': 'clinical_form_smoking_status'},
         'gold.clinical.form_smoking_status.smoking_status_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                'column': 'smoking_status_target_code',
                                                                                                'companions': ['smoking_status_target_display',
                                                                                                               'smoking_status_snomed_code',
                                                                                                               'smoking_status_snomed_display'],
                                                                                                'match': '`smoking_status_target_system` '
                                                                                                         '= '
                                                                                                         "'http://snomed.info/sct' "
                                                                                                         'AND '
                                                                                                         '`smoking_status_target_code` '
                                                                                                         'IS '
                                                                                                         'NOT '
                                                                                                         'NULL '
                                                                                                         'AND '
                                                                                                         '(NOT '
                                                                                                         '`smoking_status_target_code` '
                                                                                                         'RLIKE '
                                                                                                         "'^[1-9][0-9]{5,17}$' "
                                                                                                         'OR '
                                                                                                         'substring(`smoking_status_target_code`, '
                                                                                                         'length(`smoking_status_target_code`) '
                                                                                                         '- '
                                                                                                         '2, '
                                                                                                         '2) '
                                                                                                         'NOT '
                                                                                                         'IN '
                                                                                                         "('00', "
                                                                                                         "'10') "
                                                                                                         'OR '
                                                                                                         '(substring(`smoking_status_target_code`, '
                                                                                                         'length(`smoking_status_target_code`) '
                                                                                                         '- '
                                                                                                         '2, '
                                                                                                         '2) '
                                                                                                         '= '
                                                                                                         "'10' "
                                                                                                         'AND '
                                                                                                         'length(`smoking_status_target_code`) '
                                                                                                         '< '
                                                                                                         '11))',
                                                                                                'table': 'clinical_form_smoking_status'}},
        public_select={'all_but': ['__gold_parent_present']},
        public_steps=(_public_steps_clinical_form_smoking_status,)),
}
_rules = QcBinding(RULES)
_s3_gate_direct_public = _rules._s3_gate_direct_public
insr_w4_apply_value_rules = _rules.insr_w4_apply_value_rules


finalise()


COMMENTS = {'_qc_clinical_form_smoking_status': {'columns': {},
                                      'quantity_override': False,
                                      'table': 'Internal S3b Gold twin of the smoking-status instrument.'},
 'clinical_form_smoking_status': {'columns': {},
                                  'quantity_override': False,
                                  'table': 'Smoking status and cessation support with separate governed '
                                           'semantic axes.'}}
