"""Silver forms.form_smoking_status: 1 dataset(s).

- datasets: clinical_form_smoking_status
- moved verbatim from: 6_silver_forms:100, 6_silver_forms:168, 6_silver_forms:8231

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import spark
from silver_tables.forms._shared import (
    FORM_CONFIG_BY_SLUG,
    _instrument_parent,
    _instrument_responses,
    _multi_aggregations,
    _n,
    _single_aggregations,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


S3B_SMOKING_LOOKUP_SOURCE = spark.conf.get(
    "journey.s3b.smoking_lookup", "3_lookup.omop.smoking_answer_snomed_map"
)


def _s3b_smoking_axis(df, axis, question, answer):
    # Match a normalized smoking question and answer to the smoking lookup, retaining its
    # mapping provenance.
    key = F.sha2(F.concat_ws("|", F.lower(F.trim(F.lit(question))), F.lower(F.trim(answer))), 256)
    prefix = f"_s3b_{axis}_"
    lookup = spark.read.table(S3B_SMOKING_LOOKUP_SOURCE).select(
        F.col("source_code").alias(prefix + "key"),
        *[F.col(name).alias(prefix + name) for name in (
            "target_coding_system", "target_code", "target_display", "omop_concept_id",
            "mapping_method", "mapping_version", "legacy_confidence", "mapping_rule_id",
            "candidate_count", "mapping_cosine", "scoring_model", "mapping_status",
            "status_reason", "competing_count", "candidate_set_id", "component_index",
            "component_count", "rollup_levels",
        )],
    )
    df = df.join(F.broadcast(lookup), key == F.col(prefix + "key"), "left")
    mapped = F.col(prefix + "target_code").isNotNull()
    fields = {
        "source_system": F.lit("urn:barts:form-answer:smoking"),
        "source_code": key, "source_display": answer,
        "snomed_code": F.when(F.col(prefix + "target_coding_system") == "http://snomed.info/sct",
                              F.col(prefix + "target_code")),
        "snomed_display": F.when(F.col(prefix + "target_coding_system") == "http://snomed.info/sct",
                                 F.col(prefix + "target_display")),
        "omop_concept_id": F.col(prefix + "omop_concept_id"), "map_method": F.col(prefix + "mapping_method"),
        "map_version": F.col(prefix + "mapping_version"), "map_confidence": F.col(prefix + "legacy_confidence"),
        "map_rule_id": F.col(prefix + "mapping_rule_id"), "map_candidate_count": F.col(prefix + "candidate_count"),
        "target_system": F.col(prefix + "target_coding_system"), "target_code": F.col(prefix + "target_code"),
        "target_display": F.col(prefix + "target_display"), "omop_source_concept_id": F.lit(None).cast("bigint"),
        "map_cosine": F.col(prefix + "mapping_cosine"), "map_scoring_model": F.col(prefix + "scoring_model"),
        "map_status": F.coalesce(F.col(prefix + "mapping_status"),
                                 F.when(answer.isNotNull(), F.lit("no_candidate"))),
        "map_status_reason": F.col(prefix + "status_reason"),
        "map_competing_count": F.col(prefix + "competing_count"),
        "map_candidate_set_id": F.col(prefix + "candidate_set_id"),
        "map_component_index": F.coalesce(F.col(prefix + "component_index"), F.when(answer.isNotNull(), F.lit(1))),
        "map_component_count": F.coalesce(F.col(prefix + "component_count"), F.when(answer.isNotNull(), F.lit(1))),
        "map_rollup_levels": F.col(prefix + "rollup_levels"),
    }
    for suffix, expression in fields.items():
        df = df.withColumn(f"{axis}_{suffix}", expression)
    return df.drop(*[name for name in df.columns if name.startswith(prefix)])


@materialized_view(
    name=_n("journey_clinical.form_smoking_status"),
    refresh_policy="auto",
)
def form_smoking_status():
    # Build the declared dataset: Smoking status and cessation support: one row per form
    # instance with configured questions as columns. Residual responses are in
    # clinical_form_response.
    cfg = FORM_CONFIG_BY_SLUG["smoking_status"]
    parent = _instrument_parent(cfg)
    responses = _instrument_responses("smoking_status")
    picked = responses.groupBy("dcp_forms_activity_id").agg(
        *_single_aggregations("smoking_status", "CODED"),
        *_single_aggregations("stop_smoking_referral", "CODED"),
        *_single_aggregations("nicotine_replacement_therapy", "CODED"),
        *_single_aggregations("very_brief_advice_given", "CODED"),
        *_single_aggregations("nicotine_dependence_level", "CODED"),
        *_multi_aggregations(cfg, "nrt_supplied", "CODED"),
        *_single_aggregations("referral_comments", "TEXT"),
    )
    df = parent.join(picked, "dcp_forms_activity_id", "left")
    for axis, field in (
        ("smoking_status", "smoking_status"),
        ("cessation_advice", "very_brief_advice_given"),
        ("cessation_referral", "stop_smoking_referral"),
        ("nicotine_treatment", "nicotine_replacement_therapy"),
        ("dependence_level", "nicotine_dependence_level"),
    ):
        df = _s3b_smoking_axis(df, axis, field, F.col(field))
    return df


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_form_smoking_status': {'columns': {'active_response_row_count': 'Number of active response rows.',
                                              'authored_datetime': 'Earliest form documentation timestamp.',
                                              'cessation_advice_map_candidate_count': 'Governed S3b '
                                                                                      'cessation advice '
                                                                                      'mapping provenance.',
                                              'cessation_advice_map_candidate_set_id': 'Governed S3b '
                                                                                       'cessation advice '
                                                                                       'mapping provenance.',
                                              'cessation_advice_map_competing_count': 'Governed S3b '
                                                                                      'cessation advice '
                                                                                      'mapping provenance.',
                                              'cessation_advice_map_component_count': 'Governed S3b '
                                                                                      'cessation advice '
                                                                                      'mapping provenance.',
                                              'cessation_advice_map_component_index': 'Governed S3b '
                                                                                      'cessation advice '
                                                                                      'mapping provenance.',
                                              'cessation_advice_map_confidence': 'Governed S3b cessation '
                                                                                 'advice mapping provenance.',
                                              'cessation_advice_map_cosine': 'Governed S3b cessation advice '
                                                                             'mapping provenance.',
                                              'cessation_advice_map_method': 'Governed S3b cessation advice '
                                                                             'mapping provenance.',
                                              'cessation_advice_map_rollup_levels': 'Governed S3b cessation '
                                                                                    'advice mapping '
                                                                                    'provenance.',
                                              'cessation_advice_map_rule_id': 'Governed S3b cessation advice '
                                                                              'mapping provenance.',
                                              'cessation_advice_map_scoring_model': 'Governed S3b cessation '
                                                                                    'advice mapping '
                                                                                    'provenance.',
                                              'cessation_advice_map_status': 'Governed S3b cessation advice '
                                                                             'mapping provenance.',
                                              'cessation_advice_map_status_reason': 'Governed S3b cessation '
                                                                                    'advice mapping '
                                                                                    'provenance.',
                                              'cessation_advice_map_version': 'Governed S3b cessation advice '
                                                                              'mapping provenance.',
                                              'cessation_advice_omop_concept_id': 'Governed S3b cessation '
                                                                                  'advice mapping '
                                                                                  'provenance.',
                                              'cessation_advice_omop_source_concept_id': 'Governed S3b '
                                                                                         'cessation advice '
                                                                                         'mapping '
                                                                                         'provenance.',
                                              'cessation_advice_snomed_code': 'Governed S3b cessation advice '
                                                                              'mapping provenance.',
                                              'cessation_advice_snomed_display': 'Governed S3b cessation '
                                                                                 'advice mapping provenance.',
                                              'cessation_advice_source_code': 'Governed S3b cessation advice '
                                                                              'mapping provenance.',
                                              'cessation_advice_source_display': 'Governed S3b cessation '
                                                                                 'advice mapping provenance.',
                                              'cessation_advice_source_system': 'Governed S3b cessation '
                                                                                'advice mapping provenance.',
                                              'cessation_advice_target_code': 'Governed S3b cessation advice '
                                                                              'mapping provenance.',
                                              'cessation_advice_target_display': 'Governed S3b cessation '
                                                                                 'advice mapping provenance.',
                                              'cessation_advice_target_system': 'Governed S3b cessation '
                                                                                'advice mapping provenance.',
                                              'cessation_referral_map_candidate_count': 'Governed S3b '
                                                                                        'cessation referral '
                                                                                        'mapping provenance.',
                                              'cessation_referral_map_candidate_set_id': 'Governed S3b '
                                                                                         'cessation referral '
                                                                                         'mapping '
                                                                                         'provenance.',
                                              'cessation_referral_map_competing_count': 'Governed S3b '
                                                                                        'cessation referral '
                                                                                        'mapping provenance.',
                                              'cessation_referral_map_component_count': 'Governed S3b '
                                                                                        'cessation referral '
                                                                                        'mapping provenance.',
                                              'cessation_referral_map_component_index': 'Governed S3b '
                                                                                        'cessation referral '
                                                                                        'mapping provenance.',
                                              'cessation_referral_map_confidence': 'Governed S3b cessation '
                                                                                   'referral mapping '
                                                                                   'provenance.',
                                              'cessation_referral_map_cosine': 'Governed S3b cessation '
                                                                               'referral mapping provenance.',
                                              'cessation_referral_map_method': 'Governed S3b cessation '
                                                                               'referral mapping provenance.',
                                              'cessation_referral_map_rollup_levels': 'Governed S3b '
                                                                                      'cessation referral '
                                                                                      'mapping provenance.',
                                              'cessation_referral_map_rule_id': 'Governed S3b cessation '
                                                                                'referral mapping '
                                                                                'provenance.',
                                              'cessation_referral_map_scoring_model': 'Governed S3b '
                                                                                      'cessation referral '
                                                                                      'mapping provenance.',
                                              'cessation_referral_map_status': 'Governed S3b cessation '
                                                                               'referral mapping provenance.',
                                              'cessation_referral_map_status_reason': 'Governed S3b '
                                                                                      'cessation referral '
                                                                                      'mapping provenance.',
                                              'cessation_referral_map_version': 'Governed S3b cessation '
                                                                                'referral mapping '
                                                                                'provenance.',
                                              'cessation_referral_omop_concept_id': 'Governed S3b cessation '
                                                                                    'referral mapping '
                                                                                    'provenance.',
                                              'cessation_referral_omop_source_concept_id': 'Governed S3b '
                                                                                           'cessation '
                                                                                           'referral mapping '
                                                                                           'provenance.',
                                              'cessation_referral_snomed_code': 'Governed S3b cessation '
                                                                                'referral mapping '
                                                                                'provenance.',
                                              'cessation_referral_snomed_display': 'Governed S3b cessation '
                                                                                   'referral mapping '
                                                                                   'provenance.',
                                              'cessation_referral_source_code': 'Governed S3b cessation '
                                                                                'referral mapping '
                                                                                'provenance.',
                                              'cessation_referral_source_display': 'Governed S3b cessation '
                                                                                   'referral mapping '
                                                                                   'provenance.',
                                              'cessation_referral_source_system': 'Governed S3b cessation '
                                                                                  'referral mapping '
                                                                                  'provenance.',
                                              'cessation_referral_target_code': 'Governed S3b cessation '
                                                                                'referral mapping '
                                                                                'provenance.',
                                              'cessation_referral_target_display': 'Governed S3b cessation '
                                                                                   'referral mapping '
                                                                                   'provenance.',
                                              'cessation_referral_target_system': 'Governed S3b cessation '
                                                                                  'referral mapping '
                                                                                  'provenance.',
                                              'completed_datetime': 'Latest documentation or performed '
                                                                    'timestamp.',
                                              'confidentiality_code': 'Security classification when '
                                                                      'supplied.',
                                              'dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID; '
                                                                       'primary key of this table.',
                                              'dependence_level_map_candidate_count': 'Governed S3b '
                                                                                      'dependence level '
                                                                                      'mapping provenance.',
                                              'dependence_level_map_candidate_set_id': 'Governed S3b '
                                                                                       'dependence level '
                                                                                       'mapping provenance.',
                                              'dependence_level_map_competing_count': 'Governed S3b '
                                                                                      'dependence level '
                                                                                      'mapping provenance.',
                                              'dependence_level_map_component_count': 'Governed S3b '
                                                                                      'dependence level '
                                                                                      'mapping provenance.',
                                              'dependence_level_map_component_index': 'Governed S3b '
                                                                                      'dependence level '
                                                                                      'mapping provenance.',
                                              'dependence_level_map_confidence': 'Governed S3b dependence '
                                                                                 'level mapping provenance.',
                                              'dependence_level_map_cosine': 'Governed S3b dependence level '
                                                                             'mapping provenance.',
                                              'dependence_level_map_method': 'Governed S3b dependence level '
                                                                             'mapping provenance.',
                                              'dependence_level_map_rollup_levels': 'Governed S3b dependence '
                                                                                    'level mapping '
                                                                                    'provenance.',
                                              'dependence_level_map_rule_id': 'Governed S3b dependence level '
                                                                              'mapping provenance.',
                                              'dependence_level_map_scoring_model': 'Governed S3b dependence '
                                                                                    'level mapping '
                                                                                    'provenance.',
                                              'dependence_level_map_status': 'Governed S3b dependence level '
                                                                             'mapping provenance.',
                                              'dependence_level_map_status_reason': 'Governed S3b dependence '
                                                                                    'level mapping '
                                                                                    'provenance.',
                                              'dependence_level_map_version': 'Governed S3b dependence level '
                                                                              'mapping provenance.',
                                              'dependence_level_omop_concept_id': 'Governed S3b dependence '
                                                                                  'level mapping provenance.',
                                              'dependence_level_omop_source_concept_id': 'Governed S3b '
                                                                                         'dependence level '
                                                                                         'mapping '
                                                                                         'provenance.',
                                              'dependence_level_snomed_code': 'Governed S3b dependence level '
                                                                              'mapping provenance.',
                                              'dependence_level_snomed_display': 'Governed S3b dependence '
                                                                                 'level mapping provenance.',
                                              'dependence_level_source_code': 'Governed S3b dependence level '
                                                                              'mapping provenance.',
                                              'dependence_level_source_display': 'Governed S3b dependence '
                                                                                 'level mapping provenance.',
                                              'dependence_level_source_system': 'Governed S3b dependence '
                                                                                'level mapping provenance.',
                                              'dependence_level_target_code': 'Governed S3b dependence level '
                                                                              'mapping provenance.',
                                              'dependence_level_target_display': 'Governed S3b dependence '
                                                                                 'level mapping provenance.',
                                              'dependence_level_target_system': 'Governed S3b dependence '
                                                                                'level mapping provenance.',
                                              'empty_response_row_count': 'Number of retained empty response '
                                                                          'rows.',
                                              'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                              'spine_encounter.encounter_id.',
                                              'event_datetime': 'First documented timestamp.',
                                              'event_datetime_local': 'event_datetime in Europe/London '
                                                                      'wall-clock time (GMT/BST). Millennium '
                                                                      'rows take the bronze *_LOCAL '
                                                                      'companions of the same source columns '
                                                                      'and fallback order; rows from '
                                                                      'local-clock sources (LUNA, '
                                                                      'PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                      'BadgerNet, EndoBase, SLAM, iWeb, '
                                                                      'Datix and similar) carry '
                                                                      'event_datetime unchanged. '
                                                                      'event_datetime is a UTC instant for '
                                                                      'Millennium rows, so take local '
                                                                      'calendar dates and clock times from '
                                                                      'this column.',
                                              'event_end_datetime': 'Last documented or performed timestamp.',
                                              'event_end_datetime_local': 'event_end_datetime in '
                                                                          'Europe/London wall-clock time '
                                                                          '(GMT/BST), by the same rule as '
                                                                          'event_datetime_local; null where '
                                                                          'event_end_datetime is null.',
                                              'form_status_code': 'Source form status code.',
                                              'form_status_display': 'Source form status display.',
                                              'form_type_code': 'Source form type code.',
                                              'form_type_display': 'Source form type display.',
                                              'invalid_response_row_count': 'Number of source-classified '
                                                                            'invalid response rows.',
                                              'loaded_at': 'Inherited unchanged from the matching '
                                                           'clinical_form parent. Greatest of the form-level '
                                                           'maximum ADC_UPDT values from mill_form_activity, '
                                                           'map_powerform_assessment_item and '
                                                           'map_powerform_assessment, and maximum '
                                                           'Source_ADC_UPDT from map_mat_vte_assessment. '
                                                           'This is the contributing-source '
                                                           'ingestion/provenance clock, not the Silver '
                                                           'refresh time or form event time; null only when '
                                                           'every contributing clock is null.',
                                              'matched_response_row_count': 'Number of canonical matched '
                                                                            'response rows.',
                                              'nicotine_dependence_level': 'SC Level dependence to Nicotine '
                                                                           '— CODED/single selected value; '
                                                                           'matches: SC Level dependence to '
                                                                           'Nicotine [Smoking Status Form]',
                                              'nicotine_dependence_level_concept_id': 'Mapped value concept '
                                                                                      'identifier for SC '
                                                                                      'Level dependence to '
                                                                                      'Nicotine.',
                                              'nicotine_dependence_level_provenance': 'JSON mapping '
                                                                                      'provenance for SC '
                                                                                      'Level dependence to '
                                                                                      'Nicotine: mapping '
                                                                                      'source, rule ID, '
                                                                                      'ruleset version, '
                                                                                      'canonical rule ID and '
                                                                                      'match status, '
                                                                                      'question concept ID '
                                                                                      'and unit concept ID.',
                                              'nicotine_dependence_level_text': 'Source response text for SC '
                                                                                'Level dependence to '
                                                                                'Nicotine; matches: SC Level '
                                                                                'dependence to Nicotine '
                                                                                '[Smoking Status Form]',
                                              'nicotine_replacement_therapy': 'SC Nicotine Replacement '
                                                                              'Therapy — CODED/single '
                                                                              'selected value; matches: SC '
                                                                              'Nicotine Replacement Therapy '
                                                                              '[Smoking Status Form]',
                                              'nicotine_replacement_therapy_concept_id': 'Mapped value '
                                                                                         'concept identifier '
                                                                                         'for SC Nicotine '
                                                                                         'Replacement '
                                                                                         'Therapy.',
                                              'nicotine_replacement_therapy_provenance': 'JSON mapping '
                                                                                         'provenance for SC '
                                                                                         'Nicotine '
                                                                                         'Replacement '
                                                                                         'Therapy: mapping '
                                                                                         'source, rule ID, '
                                                                                         'ruleset version, '
                                                                                         'canonical rule ID '
                                                                                         'and match status, '
                                                                                         'question concept '
                                                                                         'ID and unit '
                                                                                         'concept ID.',
                                              'nicotine_replacement_therapy_text': 'Source response text for '
                                                                                   'SC Nicotine Replacement '
                                                                                   'Therapy; matches: SC '
                                                                                   'Nicotine Replacement '
                                                                                   'Therapy [Smoking Status '
                                                                                   'Form]',
                                              'nicotine_treatment_map_candidate_count': 'Governed S3b '
                                                                                        'nicotine treatment '
                                                                                        'mapping provenance.',
                                              'nicotine_treatment_map_candidate_set_id': 'Governed S3b '
                                                                                         'nicotine treatment '
                                                                                         'mapping '
                                                                                         'provenance.',
                                              'nicotine_treatment_map_competing_count': 'Governed S3b '
                                                                                        'nicotine treatment '
                                                                                        'mapping provenance.',
                                              'nicotine_treatment_map_component_count': 'Governed S3b '
                                                                                        'nicotine treatment '
                                                                                        'mapping provenance.',
                                              'nicotine_treatment_map_component_index': 'Governed S3b '
                                                                                        'nicotine treatment '
                                                                                        'mapping provenance.',
                                              'nicotine_treatment_map_confidence': 'Governed S3b nicotine '
                                                                                   'treatment mapping '
                                                                                   'provenance.',
                                              'nicotine_treatment_map_cosine': 'Governed S3b nicotine '
                                                                               'treatment mapping '
                                                                               'provenance.',
                                              'nicotine_treatment_map_method': 'Governed S3b nicotine '
                                                                               'treatment mapping '
                                                                               'provenance.',
                                              'nicotine_treatment_map_rollup_levels': 'Governed S3b nicotine '
                                                                                      'treatment mapping '
                                                                                      'provenance.',
                                              'nicotine_treatment_map_rule_id': 'Governed S3b nicotine '
                                                                                'treatment mapping '
                                                                                'provenance.',
                                              'nicotine_treatment_map_scoring_model': 'Governed S3b nicotine '
                                                                                      'treatment mapping '
                                                                                      'provenance.',
                                              'nicotine_treatment_map_status': 'Governed S3b nicotine '
                                                                               'treatment mapping '
                                                                               'provenance.',
                                              'nicotine_treatment_map_status_reason': 'Governed S3b nicotine '
                                                                                      'treatment mapping '
                                                                                      'provenance.',
                                              'nicotine_treatment_map_version': 'Governed S3b nicotine '
                                                                                'treatment mapping '
                                                                                'provenance.',
                                              'nicotine_treatment_omop_concept_id': 'Governed S3b nicotine '
                                                                                    'treatment mapping '
                                                                                    'provenance.',
                                              'nicotine_treatment_omop_source_concept_id': 'Governed S3b '
                                                                                           'nicotine '
                                                                                           'treatment '
                                                                                           'mapping '
                                                                                           'provenance.',
                                              'nicotine_treatment_snomed_code': 'Governed S3b nicotine '
                                                                                'treatment mapping '
                                                                                'provenance.',
                                              'nicotine_treatment_snomed_display': 'Governed S3b nicotine '
                                                                                   'treatment mapping '
                                                                                   'provenance.',
                                              'nicotine_treatment_source_code': 'Governed S3b nicotine '
                                                                                'treatment mapping '
                                                                                'provenance.',
                                              'nicotine_treatment_source_display': 'Governed S3b nicotine '
                                                                                   'treatment mapping '
                                                                                   'provenance.',
                                              'nicotine_treatment_source_system': 'Governed S3b nicotine '
                                                                                  'treatment mapping '
                                                                                  'provenance.',
                                              'nicotine_treatment_target_code': 'Governed S3b nicotine '
                                                                                'treatment mapping '
                                                                                'provenance.',
                                              'nicotine_treatment_target_display': 'Governed S3b nicotine '
                                                                                   'treatment mapping '
                                                                                   'provenance.',
                                              'nicotine_treatment_target_system': 'Governed S3b nicotine '
                                                                                  'treatment mapping '
                                                                                  'provenance.',
                                              'nrt_supplied__cystine': "SC NRT Supplied option 'Cystine': "
                                                                       'true when selected, false when the '
                                                                       'question was answered with other '
                                                                       'options only, null when the question '
                                                                       'was not answered; matches: SC NRT '
                                                                       'Supplied [Smoking Status Form]',
                                              'nrt_supplied__gum_2mg': "SC NRT Supplied option 'Gum 2mg': "
                                                                       'true when selected, false when the '
                                                                       'question was answered with other '
                                                                       'options only, null when the question '
                                                                       'was not answered; matches: SC NRT '
                                                                       'Supplied [Smoking Status Form]',
                                              'nrt_supplied__gum_4mg': "SC NRT Supplied option 'Gum 4mg': "
                                                                       'true when selected, false when the '
                                                                       'question was answered with other '
                                                                       'options only, null when the question '
                                                                       'was not answered; matches: SC NRT '
                                                                       'Supplied [Smoking Status Form]',
                                              'nrt_supplied__inhalator': 'SC NRT Supplied option '
                                                                         "'Inhalator': true when selected, "
                                                                         'false when the question was '
                                                                         'answered with other options only, '
                                                                         'null when the question was not '
                                                                         'answered; matches: SC NRT Supplied '
                                                                         '[Smoking Status Form]',
                                              'nrt_supplied__mouth_spray': "SC NRT Supplied option 'Mouth "
                                                                           "spray': true when selected, "
                                                                           'false when the question was '
                                                                           'answered with other options '
                                                                           'only, null when the question was '
                                                                           'not answered; matches: SC NRT '
                                                                           'Supplied [Smoking Status Form]',
                                              'nrt_supplied__nicotine_gum_2mg': 'SC NRT Supplied option '
                                                                                "'Nicotine gum 2mg': true "
                                                                                'when selected, false when '
                                                                                'the question was answered '
                                                                                'with other options only, '
                                                                                'null when the question was '
                                                                                'not answered; matches: SC '
                                                                                'NRT Supplied [Smoking '
                                                                                'Status Form]',
                                              'nrt_supplied__nicotine_gum_4mg': 'SC NRT Supplied option '
                                                                                "'Nicotine gum 4mg': true "
                                                                                'when selected, false when '
                                                                                'the question was answered '
                                                                                'with other options only, '
                                                                                'null when the question was '
                                                                                'not answered; matches: SC '
                                                                                'NRT Supplied [Smoking '
                                                                                'Status Form]',
                                              'nrt_supplied__nicotine_inhalator_15mg': 'SC NRT Supplied '
                                                                                       "option 'Nicotine "
                                                                                       "inhalator 15mg': "
                                                                                       'true when selected, '
                                                                                       'false when the '
                                                                                       'question was '
                                                                                       'answered with other '
                                                                                       'options only, null '
                                                                                       'when the question '
                                                                                       'was not answered; '
                                                                                       'matches: SC NRT '
                                                                                       'Supplied [Smoking '
                                                                                       'Status Form]',
                                              'nrt_supplied__nicotine_lozenges_1mg': 'SC NRT Supplied option '
                                                                                     "'Nicotine lozenges "
                                                                                     "1mg': true when "
                                                                                     'selected, false when '
                                                                                     'the question was '
                                                                                     'answered with other '
                                                                                     'options only, null '
                                                                                     'when the question was '
                                                                                     'not answered; matches: '
                                                                                     'SC NRT Supplied '
                                                                                     '[Smoking Status Form]',
                                              'nrt_supplied__nicotine_lozenges_2mg': 'SC NRT Supplied option '
                                                                                     "'Nicotine lozenges "
                                                                                     "2mg': true when "
                                                                                     'selected, false when '
                                                                                     'the question was '
                                                                                     'answered with other '
                                                                                     'options only, null '
                                                                                     'when the question was '
                                                                                     'not answered; matches: '
                                                                                     'SC NRT Supplied '
                                                                                     '[Smoking Status Form]',
                                              'nrt_supplied__nicotine_nasal_spray_10mg_ml': 'SC NRT Supplied '
                                                                                            'option '
                                                                                            "'Nicotine Nasal "
                                                                                            "Spray 10mg/ml': "
                                                                                            'true when '
                                                                                            'selected, false '
                                                                                            'when the '
                                                                                            'question was '
                                                                                            'answered with '
                                                                                            'other options '
                                                                                            'only, null when '
                                                                                            'the question '
                                                                                            'was not '
                                                                                            'answered; '
                                                                                            'matches: SC NRT '
                                                                                            'Supplied '
                                                                                            '[Smoking Status '
                                                                                            'Form]',
                                              'nrt_supplied__nicotine_oral_mucosal_spray_1mg_spray': 'SC NRT '
                                                                                                     'Supplied '
                                                                                                     'option '
                                                                                                     "'Nicotine "
                                                                                                     'oral '
                                                                                                     'mucosal '
                                                                                                     'spray '
                                                                                                     "1mg/spray': "
                                                                                                     'true '
                                                                                                     'when '
                                                                                                     'selected, '
                                                                                                     'false '
                                                                                                     'when '
                                                                                                     'the '
                                                                                                     'question '
                                                                                                     'was '
                                                                                                     'answered '
                                                                                                     'with '
                                                                                                     'other '
                                                                                                     'options '
                                                                                                     'only, '
                                                                                                     'null '
                                                                                                     'when '
                                                                                                     'the '
                                                                                                     'question '
                                                                                                     'was '
                                                                                                     'not '
                                                                                                     'answered; '
                                                                                                     'matches: '
                                                                                                     'SC NRT '
                                                                                                     'Supplied '
                                                                                                     '[Smoking '
                                                                                                     'Status '
                                                                                                     'Form]',
                                              'nrt_supplied__nicotine_patch_10mg_16hrs': 'SC NRT Supplied '
                                                                                         "option 'Nicotine "
                                                                                         "patch 10mg/16hrs': "
                                                                                         'true when '
                                                                                         'selected, false '
                                                                                         'when the question '
                                                                                         'was answered with '
                                                                                         'other options '
                                                                                         'only, null when '
                                                                                         'the question was '
                                                                                         'not answered; '
                                                                                         'matches: SC NRT '
                                                                                         'Supplied [Smoking '
                                                                                         'Status Form]',
                                              'nrt_supplied__nicotine_patch_14mg_24hrs': 'SC NRT Supplied '
                                                                                         "option 'Nicotine "
                                                                                         "patch 14mg/24hrs': "
                                                                                         'true when '
                                                                                         'selected, false '
                                                                                         'when the question '
                                                                                         'was answered with '
                                                                                         'other options '
                                                                                         'only, null when '
                                                                                         'the question was '
                                                                                         'not answered; '
                                                                                         'matches: SC NRT '
                                                                                         'Supplied [Smoking '
                                                                                         'Status Form]',
                                              'nrt_supplied__nicotine_patch_15mg_16hrs': 'SC NRT Supplied '
                                                                                         "option 'Nicotine "
                                                                                         "patch 15mg/16hrs': "
                                                                                         'true when '
                                                                                         'selected, false '
                                                                                         'when the question '
                                                                                         'was answered with '
                                                                                         'other options '
                                                                                         'only, null when '
                                                                                         'the question was '
                                                                                         'not answered; '
                                                                                         'matches: SC NRT '
                                                                                         'Supplied [Smoking '
                                                                                         'Status Form]',
                                              'nrt_supplied__nicotine_patch_21mg_24hrs': 'SC NRT Supplied '
                                                                                         "option 'Nicotine "
                                                                                         "patch 21mg/24hrs': "
                                                                                         'true when '
                                                                                         'selected, false '
                                                                                         'when the question '
                                                                                         'was answered with '
                                                                                         'other options '
                                                                                         'only, null when '
                                                                                         'the question was '
                                                                                         'not answered; '
                                                                                         'matches: SC NRT '
                                                                                         'Supplied [Smoking '
                                                                                         'Status Form]',
                                              'nrt_supplied__nicotine_patch_25mg_16hrs': 'SC NRT Supplied '
                                                                                         "option 'Nicotine "
                                                                                         "patch 25mg/16hrs': "
                                                                                         'true when '
                                                                                         'selected, false '
                                                                                         'when the question '
                                                                                         'was answered with '
                                                                                         'other options '
                                                                                         'only, null when '
                                                                                         'the question was '
                                                                                         'not answered; '
                                                                                         'matches: SC NRT '
                                                                                         'Supplied [Smoking '
                                                                                         'Status Form]',
                                              'nrt_supplied__nicotine_patch_7mg_24hrs': 'SC NRT Supplied '
                                                                                        "option 'Nicotine "
                                                                                        "patch 7mg/24hrs': "
                                                                                        'true when selected, '
                                                                                        'false when the '
                                                                                        'question was '
                                                                                        'answered with other '
                                                                                        'options only, null '
                                                                                        'when the question '
                                                                                        'was not answered; '
                                                                                        'matches: SC NRT '
                                                                                        'Supplied [Smoking '
                                                                                        'Status Form]',
                                              'nrt_supplied__nicotine_sublingual_tablets_2mg': 'SC NRT '
                                                                                               'Supplied '
                                                                                               'option '
                                                                                               "'Nicotine "
                                                                                               'sublingual '
                                                                                               'tablets '
                                                                                               "2mg': true "
                                                                                               'when '
                                                                                               'selected, '
                                                                                               'false when '
                                                                                               'the question '
                                                                                               'was answered '
                                                                                               'with other '
                                                                                               'options '
                                                                                               'only, null '
                                                                                               'when the '
                                                                                               'question was '
                                                                                               'not '
                                                                                               'answered; '
                                                                                               'matches: SC '
                                                                                               'NRT Supplied '
                                                                                               '[Smoking '
                                                                                               'Status Form]',
                                              'nrt_supplied__varenicline': 'SC NRT Supplied option '
                                                                           "'Varenicline': true when "
                                                                           'selected, false when the '
                                                                           'question was answered with other '
                                                                           'options only, null when the '
                                                                           'question was not answered; '
                                                                           'matches: SC NRT Supplied '
                                                                           '[Smoking Status Form]',
                                              'nrt_supplied_text': 'Pipe-joined selected CODED values for SC '
                                                                   'NRT Supplied, ordered by sequence and '
                                                                   'response ID; matches: SC NRT Supplied '
                                                                   '[Smoking Status Form]',
                                              'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                                 'organization.',
                                              'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                   'stable cross-feed join key.',
                                              'performed_practitioner_id': 'Millennium personnel PERSON_ID '
                                                                           'for the performer.',
                                              'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                              'record_status': 'Inherited unchanged from the matching '
                                                               'clinical_form parent. Derived PowerForm row '
                                                               'status: retracted when the grouped maximum '
                                                               'of '
                                                               'map_powerform_assessment.SOURCE_PRESENT_IND '
                                                               'is 0; otherwise superseded when the grouped '
                                                               'maximum of ASSESSMENT_ACTIVE_IND is 0 or no '
                                                               'mill_form_activity response has ACTIVE_IND = '
                                                               '1; otherwise active. Missing assessment '
                                                               'flags alone do not trigger retraction or '
                                                               'supersession. This is a status label, not '
                                                               'the raw Boolean assessment flag.',
                                              'record_status_effective_from': 'Inherited unchanged from the '
                                                                              'matching clinical_form '
                                                                              'parent. Earliest '
                                                                              'mill_form_activity.DOCUMENTATION_DT_TM '
                                                                              'for the form instance, '
                                                                              'carried from '
                                                                              'authored_datetime. This is '
                                                                              'the form documentation start '
                                                                              'used by the research status '
                                                                              'projection, not an '
                                                                              'independently recorded '
                                                                              'status-change timestamp; null '
                                                                              'when all contributing '
                                                                              'documentation timestamps are '
                                                                              'null.',
                                              'record_status_effective_to': 'Inherited unchanged from the '
                                                                            'matching clinical_form parent. '
                                                                            'For a retracted or superseded '
                                                                            'form, completed_datetime: the '
                                                                            'maximum of '
                                                                            'coalesce(mill_form_activity.LAST_DOCUMENTED_DT_TM, '
                                                                            'mill_form_activity.PERFORMED_DT_TM) '
                                                                            'across its responses. Null for '
                                                                            'active forms or when no '
                                                                            'contributing completion '
                                                                            'timestamp is available. This is '
                                                                            'a documentation/completion '
                                                                            'boundary, not a Boolean '
                                                                            'assessment flag or an '
                                                                            'independently recorded '
                                                                            'status-change time.',
                                              'referral_comments': 'SC Referral Comments — TEXT/single '
                                                                   'selected value; matches: SC Referral '
                                                                   'Comments [Smoking Status Form]',
                                              'referral_comments_concept_id': 'Mapped value concept '
                                                                              'identifier for SC Referral '
                                                                              'Comments.',
                                              'referral_comments_provenance': 'JSON mapping provenance for '
                                                                              'SC Referral Comments: mapping '
                                                                              'source, rule ID, ruleset '
                                                                              'version, canonical rule ID '
                                                                              'and match status, question '
                                                                              'concept ID and unit concept '
                                                                              'ID.',
                                              'referral_comments_text': 'Source response text for SC '
                                                                        'Referral Comments; matches: SC '
                                                                        'Referral Comments [Smoking Status '
                                                                        'Form]',
                                              'response_row_count': 'Number of response rows grouped into '
                                                                    'the form.',
                                              'smoking_status': 'SC Smoking Status — CODED/single selected '
                                                                'value; matches: SC Smoking Status [Smoking '
                                                                'Status Form]',
                                              'smoking_status_concept_id': 'Mapped value concept identifier '
                                                                           'for SC Smoking Status.',
                                              'smoking_status_map_candidate_count': 'Governed S3b smoking '
                                                                                    'status mapping '
                                                                                    'provenance.',
                                              'smoking_status_map_candidate_set_id': 'Governed S3b smoking '
                                                                                     'status mapping '
                                                                                     'provenance.',
                                              'smoking_status_map_competing_count': 'Governed S3b smoking '
                                                                                    'status mapping '
                                                                                    'provenance.',
                                              'smoking_status_map_component_count': 'Governed S3b smoking '
                                                                                    'status mapping '
                                                                                    'provenance.',
                                              'smoking_status_map_component_index': 'Governed S3b smoking '
                                                                                    'status mapping '
                                                                                    'provenance.',
                                              'smoking_status_map_confidence': 'Governed S3b smoking status '
                                                                               'mapping provenance.',
                                              'smoking_status_map_cosine': 'Governed S3b smoking status '
                                                                           'mapping provenance.',
                                              'smoking_status_map_method': 'Governed S3b smoking status '
                                                                           'mapping provenance.',
                                              'smoking_status_map_rollup_levels': 'Governed S3b smoking '
                                                                                  'status mapping '
                                                                                  'provenance.',
                                              'smoking_status_map_rule_id': 'Governed S3b smoking status '
                                                                            'mapping provenance.',
                                              'smoking_status_map_scoring_model': 'Governed S3b smoking '
                                                                                  'status mapping '
                                                                                  'provenance.',
                                              'smoking_status_map_status': 'Governed S3b smoking status '
                                                                           'mapping provenance.',
                                              'smoking_status_map_status_reason': 'Governed S3b smoking '
                                                                                  'status mapping '
                                                                                  'provenance.',
                                              'smoking_status_map_version': 'Governed S3b smoking status '
                                                                            'mapping provenance.',
                                              'smoking_status_omop_concept_id': 'Governed S3b smoking status '
                                                                                'mapping provenance.',
                                              'smoking_status_omop_source_concept_id': 'Governed S3b smoking '
                                                                                       'status mapping '
                                                                                       'provenance.',
                                              'smoking_status_provenance': 'JSON mapping provenance for SC '
                                                                           'Smoking Status: mapping source, '
                                                                           'rule ID, ruleset version, '
                                                                           'canonical rule ID and match '
                                                                           'status, question concept ID and '
                                                                           'unit concept ID.',
                                              'smoking_status_snomed_code': 'Governed S3b smoking status '
                                                                            'mapping provenance.',
                                              'smoking_status_snomed_display': 'Governed S3b smoking status '
                                                                               'mapping provenance.',
                                              'smoking_status_source_code': 'Governed S3b smoking status '
                                                                            'mapping provenance.',
                                              'smoking_status_source_display': 'Governed S3b smoking status '
                                                                               'mapping provenance.',
                                              'smoking_status_source_system': 'Governed S3b smoking status '
                                                                              'mapping provenance.',
                                              'smoking_status_target_code': 'Governed S3b smoking status '
                                                                            'mapping provenance.',
                                              'smoking_status_target_display': 'Governed S3b smoking status '
                                                                               'mapping provenance.',
                                              'smoking_status_target_system': 'Governed S3b smoking status '
                                                                              'mapping provenance.',
                                              'smoking_status_text': 'Source response text for SC Smoking '
                                                                     'Status; matches: SC Smoking Status '
                                                                     '[Smoking Status Form]',
                                              'source_code': 'Source form reference identifier.',
                                              'source_coding_system': 'Source form coding system.',
                                              'source_display': 'Source form description.',
                                              'source_feed': 'Registered owning feed.',
                                              'source_update_timestamp': 'Inherited unchanged from the '
                                                                         'matching clinical_form parent. '
                                                                         "Maximum, across the form's "
                                                                         'mill_form_activity rows, of the '
                                                                         'greatest ADC_UPDT, '
                                                                         'SOURCE_ACTIVITY_ADC_UPDT, '
                                                                         'SOURCE_EVENT_ADC_UPDT and '
                                                                         'SOURCE_LABEL_ADC_UPDT. This '
                                                                         'combines the contributing '
                                                                         'activity, event, label and '
                                                                         'bronze-ingestion clocks; it is not '
                                                                         'uniformly a native clinical-system '
                                                                         'update time. Null only when every '
                                                                         'contributing clock is null.',
                                              'stop_smoking_referral': 'SC Referral to Stop Smoking Service '
                                                                       '— CODED/single selected value; '
                                                                       'matches: SC Referral to Stop Smoking '
                                                                       'Service [Smoking Status Form]',
                                              'stop_smoking_referral_concept_id': 'Mapped value concept '
                                                                                  'identifier for SC '
                                                                                  'Referral to Stop Smoking '
                                                                                  'Service.',
                                              'stop_smoking_referral_provenance': 'JSON mapping provenance '
                                                                                  'for SC Referral to Stop '
                                                                                  'Smoking Service: mapping '
                                                                                  'source, rule ID, ruleset '
                                                                                  'version, canonical rule '
                                                                                  'ID and match status, '
                                                                                  'question concept ID and '
                                                                                  'unit concept ID.',
                                              'stop_smoking_referral_text': 'Source response text for SC '
                                                                            'Referral to Stop Smoking '
                                                                            'Service; matches: SC Referral '
                                                                            'to Stop Smoking Service '
                                                                            '[Smoking Status Form]',
                                              'subject_id_system': 'Identifier system used for subject_key.',
                                              'subject_key': 'Deterministic SHA-256 over the strongest '
                                                             'source identifier. Not salted, not secret; a '
                                                             'stable join key across feeds where person_id '
                                                             'is unresolved.',
                                              'unmatched_response_row_count': 'Number of canonical unmatched '
                                                                              'response rows.',
                                              'very_brief_advice_given': 'SC Very brief advice given — '
                                                                         'CODED/single selected value; '
                                                                         'matches: SC Very brief advice '
                                                                         'given [Smoking Status Form]',
                                              'very_brief_advice_given_concept_id': 'Mapped value concept '
                                                                                    'identifier for SC Very '
                                                                                    'brief advice given.',
                                              'very_brief_advice_given_provenance': 'JSON mapping provenance '
                                                                                    'for SC Very brief '
                                                                                    'advice given: mapping '
                                                                                    'source, rule ID, '
                                                                                    'ruleset version, '
                                                                                    'canonical rule ID and '
                                                                                    'match status, question '
                                                                                    'concept ID and unit '
                                                                                    'concept ID.',
                                              'very_brief_advice_given_text': 'Source response text for SC '
                                                                              'Very brief advice given; '
                                                                              'matches: SC Very brief advice '
                                                                              'given [Smoking Status Form]',
                                              'vip_ind': 'VIP indicator when supplied.',
                                              'withheld_identity_ind': 'Withheld identity indicator.'},
                                  'table': 'Smoking status and cessation support: one row per form instance '
                                           'with configured questions as columns. Residual responses are in '
                                           'clinical_form_response.'}}
