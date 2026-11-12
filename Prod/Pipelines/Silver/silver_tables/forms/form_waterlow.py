"""Silver forms.form_waterlow: 1 dataset(s).

- datasets: clinical_form_waterlow
- moved verbatim from: 6_silver_forms:9441

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


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


@materialized_view(
    name=_n("journey_clinical.form_waterlow"),
    refresh_policy="auto",
)
def form_waterlow():
    # Build the declared dataset: Waterlow pressure-ulcer risk assessment: one row per form
    # instance with configured questions as columns. Residual responses are in
    # clinical_form_response.
    cfg = FORM_CONFIG_BY_SLUG["waterlow"]
    parent = _instrument_parent(cfg)
    responses = _instrument_responses("waterlow")
    picked = responses.groupBy("dcp_forms_activity_id").agg(
        *_single_aggregations("major_surgery_trauma_waterlow_score_nuh", "CODED"),
        *_multi_aggregations(cfg, "skin_type_and_visual_risk_areas", "CODED"),
        *_single_aggregations("appetite_waterlow_score", "CODED"),
        *_single_aggregations("continence_waterlow_score_nuh", "CODED"),
        *_single_aggregations("neurological_deficits_waterlow_score_nuh", "CODED"),
        *_single_aggregations("recent_weight_loss_waterlow_score", "CODED"),
        *_single_aggregations("mobility_waterlow_score", "CODED"),
        *_single_aggregations("tissue_malnutrition_waterlow_score_nuh", "CODED"),
        *_single_aggregations("medication_waterlow_score_nuh", "CODED"),
        *_multi_aggregations(cfg, "sex_age_waterlow_score", "CODED"),
        *_single_aggregations("waterlow_score_result", "NUMERIC"),
        *_single_aggregations("build_for_height_waterlow_score_nuh", "CODED"),
        *_single_aggregations("weight_loss_waterlow_score", "CODED"),
        *_single_aggregations("age_waterlow_score", "CODED"),
        *_single_aggregations("build_height_for_weight", "CODED"),
        *_single_aggregations("continence", "CODED"),
        *_multi_aggregations(cfg, "risk_areas_visual_skin_type", "CODED"),
        *_single_aggregations("sex_waterlow_score", "CODED"),
        *_single_aggregations("waterlow_appetite", "CODED"),
        *_single_aggregations("waterlow_major_surgery_or_trauma", "CODED"),
        *_single_aggregations("waterlow_neurological_deficit", "CODED"),
        *_single_aggregations("waterlow_tissue_malnutrition", "CODED"),
        *_single_aggregations("waterlow_mobility", "CODED"),
        *_single_aggregations("waterlow_total_score", "NUMERIC"),
        *_single_aggregations("waterlow_total_score_risk", "CODED"),
        *_single_aggregations("waterlow_medications", "CODED"),
        *_single_aggregations("height_length_measured", "NUMERIC"),
        *_single_aggregations("comments_waterlow", "TEXT"),
        *_single_aggregations("weight_kg", "NUMERIC"),
        *_single_aggregations("zzzheight_metres", "NUMERIC"),
        *_single_aggregations("weight_measured", "NUMERIC"),
        *_single_aggregations("body_mass_index_measured", "NUMERIC"),
    )
    return parent.join(picked, "dcp_forms_activity_id", "left")


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_form_waterlow': {'columns': {'active_response_row_count': 'Number of active response rows.',
                                        'age_waterlow_score': 'Age (Waterlow Score) — CODED/single selected '
                                                              'value; matches: Age (Waterlow Score) '
                                                              '[WATERLOW SCORE]',
                                        'age_waterlow_score_concept_id': 'Mapped value concept identifier '
                                                                         'for Age (Waterlow Score).',
                                        'age_waterlow_score_provenance': 'JSON mapping provenance for Age '
                                                                         '(Waterlow Score): mapping source, '
                                                                         'rule ID, ruleset version, '
                                                                         'canonical rule ID and match '
                                                                         'status, question concept ID and '
                                                                         'unit concept ID.',
                                        'age_waterlow_score_text': 'Source response text for Age (Waterlow '
                                                                   'Score); matches: Age (Waterlow Score) '
                                                                   '[WATERLOW SCORE]',
                                        'appetite_waterlow_score': 'Appetite (Waterlow Score) — CODED/single '
                                                                   'selected value; matches: Appetite '
                                                                   '(Waterlow Score) [WATERLOW SCORE]',
                                        'appetite_waterlow_score_concept_id': 'Mapped value concept '
                                                                              'identifier for Appetite '
                                                                              '(Waterlow Score).',
                                        'appetite_waterlow_score_provenance': 'JSON mapping provenance for '
                                                                              'Appetite (Waterlow Score): '
                                                                              'mapping source, rule ID, '
                                                                              'ruleset version, canonical '
                                                                              'rule ID and match status, '
                                                                              'question concept ID and unit '
                                                                              'concept ID.',
                                        'appetite_waterlow_score_text': 'Source response text for Appetite '
                                                                        '(Waterlow Score); matches: Appetite '
                                                                        '(Waterlow Score) [WATERLOW SCORE]',
                                        'authored_datetime': 'Earliest form documentation timestamp.',
                                        'body_mass_index_measured': 'Body Mass Index Measured — '
                                                                    'NUMERIC/single selected value; matches: '
                                                                    'Body Mass Index Measured [WATERLOW '
                                                                    'SCORE]',
                                        'body_mass_index_measured_concept_id': 'Mapped value concept '
                                                                               'identifier for Body Mass '
                                                                               'Index Measured.',
                                        'body_mass_index_measured_provenance': 'JSON mapping provenance for '
                                                                               'Body Mass Index Measured: '
                                                                               'mapping source, rule ID, '
                                                                               'ruleset version, canonical '
                                                                               'rule ID and match status, '
                                                                               'question concept ID and unit '
                                                                               'concept ID.',
                                        'body_mass_index_measured_text': 'Source response text for Body Mass '
                                                                         'Index Measured; matches: Body Mass '
                                                                         'Index Measured [WATERLOW SCORE]',
                                        'build_for_height_waterlow_score_nuh': 'Build for Height (Waterlow '
                                                                               'Score NUH) — CODED/single '
                                                                               'selected value; matches: '
                                                                               'Build for Height (Waterlow '
                                                                               'Score NUH) [WATERLOW SCORE]',
                                        'build_for_height_waterlow_score_nuh_concept_id': 'Mapped value '
                                                                                          'concept '
                                                                                          'identifier for '
                                                                                          'Build for Height '
                                                                                          '(Waterlow Score '
                                                                                          'NUH).',
                                        'build_for_height_waterlow_score_nuh_provenance': 'JSON mapping '
                                                                                          'provenance for '
                                                                                          'Build for Height '
                                                                                          '(Waterlow Score '
                                                                                          'NUH): mapping '
                                                                                          'source, rule ID, '
                                                                                          'ruleset version, '
                                                                                          'canonical rule ID '
                                                                                          'and match status, '
                                                                                          'question concept '
                                                                                          'ID and unit '
                                                                                          'concept ID.',
                                        'build_for_height_waterlow_score_nuh_text': 'Source response text '
                                                                                    'for Build for Height '
                                                                                    '(Waterlow Score NUH); '
                                                                                    'matches: Build for '
                                                                                    'Height (Waterlow Score '
                                                                                    'NUH) [WATERLOW SCORE]',
                                        'build_height_for_weight': 'Build/height for weight — CODED/single '
                                                                   'selected value; matches: Build/height '
                                                                   'for weight [WATERLOW SCORE]',
                                        'build_height_for_weight_concept_id': 'Mapped value concept '
                                                                              'identifier for Build/height '
                                                                              'for weight.',
                                        'build_height_for_weight_provenance': 'JSON mapping provenance for '
                                                                              'Build/height for weight: '
                                                                              'mapping source, rule ID, '
                                                                              'ruleset version, canonical '
                                                                              'rule ID and match status, '
                                                                              'question concept ID and unit '
                                                                              'concept ID.',
                                        'build_height_for_weight_text': 'Source response text for '
                                                                        'Build/height for weight; matches: '
                                                                        'Build/height for weight [WATERLOW '
                                                                        'SCORE]',
                                        'comments_waterlow': 'Comments (Waterlow) — TEXT/single selected '
                                                             'value; matches: Comments (Waterlow) [WATERLOW '
                                                             'SCORE]',
                                        'comments_waterlow_concept_id': 'Mapped value concept identifier for '
                                                                        'Comments (Waterlow).',
                                        'comments_waterlow_provenance': 'JSON mapping provenance for '
                                                                        'Comments (Waterlow): mapping '
                                                                        'source, rule ID, ruleset version, '
                                                                        'canonical rule ID and match status, '
                                                                        'question concept ID and unit '
                                                                        'concept ID.',
                                        'comments_waterlow_text': 'Source response text for Comments '
                                                                  '(Waterlow); matches: Comments (Waterlow) '
                                                                  '[WATERLOW SCORE]',
                                        'completed_datetime': 'Latest documentation or performed timestamp.',
                                        'confidentiality_code': 'Security classification when supplied.',
                                        'continence': 'Continence — CODED/single selected value; matches: '
                                                      'Continence [WATERLOW SCORE]',
                                        'continence_concept_id': 'Mapped value concept identifier for '
                                                                 'Continence.',
                                        'continence_provenance': 'JSON mapping provenance for Continence: '
                                                                 'mapping source, rule ID, ruleset version, '
                                                                 'canonical rule ID and match status, '
                                                                 'question concept ID and unit concept ID.',
                                        'continence_text': 'Source response text for Continence; matches: '
                                                           'Continence [WATERLOW SCORE]',
                                        'continence_waterlow_score_nuh': 'Continence (Waterlow Score NUH) — '
                                                                         'CODED/single selected value; '
                                                                         'matches: Continence (Waterlow '
                                                                         'Score NUH) [WATERLOW SCORE]',
                                        'continence_waterlow_score_nuh_concept_id': 'Mapped value concept '
                                                                                    'identifier for '
                                                                                    'Continence (Waterlow '
                                                                                    'Score NUH).',
                                        'continence_waterlow_score_nuh_provenance': 'JSON mapping provenance '
                                                                                    'for Continence '
                                                                                    '(Waterlow Score NUH): '
                                                                                    'mapping source, rule '
                                                                                    'ID, ruleset version, '
                                                                                    'canonical rule ID and '
                                                                                    'match status, question '
                                                                                    'concept ID and unit '
                                                                                    'concept ID.',
                                        'continence_waterlow_score_nuh_text': 'Source response text for '
                                                                              'Continence (Waterlow Score '
                                                                              'NUH); matches: Continence '
                                                                              '(Waterlow Score NUH) '
                                                                              '[WATERLOW SCORE]',
                                        'dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID; primary '
                                                                 'key of this table.',
                                        'empty_response_row_count': 'Number of retained empty response rows.',
                                        'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                        'spine_encounter.encounter_id.',
                                        'event_datetime': 'First documented timestamp.',
                                        'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                                'time (GMT/BST). Millennium rows take the '
                                                                'bronze *_LOCAL companions of the same '
                                                                'source columns and fallback order; rows '
                                                                'from local-clock sources (LUNA, PACS/DICOM, '
                                                                'TFC LIMS, CCMDS, MSDS, BadgerNet, EndoBase, '
                                                                'SLAM, iWeb, Datix and similar) carry '
                                                                'event_datetime unchanged. event_datetime is '
                                                                'a UTC instant for Millennium rows, so take '
                                                                'local calendar dates and clock times from '
                                                                'this column.',
                                        'event_end_datetime': 'Last documented or performed timestamp.',
                                        'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                    'wall-clock time (GMT/BST), by the same '
                                                                    'rule as event_datetime_local; null '
                                                                    'where event_end_datetime is null.',
                                        'form_status_code': 'Source form status code.',
                                        'form_status_display': 'Source form status display.',
                                        'form_type_code': 'Source form type code.',
                                        'form_type_display': 'Source form type display.',
                                        'height_length_measured': 'Height/Length Measured — NUMERIC/single '
                                                                  'selected value; matches: Height/Length '
                                                                  'Measured [WATERLOW SCORE]',
                                        'height_length_measured_concept_id': 'Mapped value concept '
                                                                             'identifier for Height/Length '
                                                                             'Measured.',
                                        'height_length_measured_provenance': 'JSON mapping provenance for '
                                                                             'Height/Length Measured: '
                                                                             'mapping source, rule ID, '
                                                                             'ruleset version, canonical '
                                                                             'rule ID and match status, '
                                                                             'question concept ID and unit '
                                                                             'concept ID.',
                                        'height_length_measured_text': 'Source response text for '
                                                                       'Height/Length Measured; matches: '
                                                                       'Height/Length Measured [WATERLOW '
                                                                       'SCORE]',
                                        'invalid_response_row_count': 'Number of source-classified invalid '
                                                                      'response rows.',
                                        'loaded_at': 'Inherited unchanged from the matching clinical_form '
                                                     'parent. Greatest of the form-level maximum ADC_UPDT '
                                                     'values from mill_form_activity, '
                                                     'map_powerform_assessment_item and '
                                                     'map_powerform_assessment, and maximum Source_ADC_UPDT '
                                                     'from map_mat_vte_assessment. This is the '
                                                     'contributing-source ingestion/provenance clock, not '
                                                     'the Silver refresh time or form event time; null only '
                                                     'when every contributing clock is null.',
                                        'major_surgery_trauma_waterlow_score_nuh': 'Major Surgery/Trauma '
                                                                                   '(Waterlow Score NUH) — '
                                                                                   'CODED/single selected '
                                                                                   'value; matches: Major '
                                                                                   'Surgery/Trauma (Waterlow '
                                                                                   'Score NUH) [WATERLOW '
                                                                                   'SCORE]',
                                        'major_surgery_trauma_waterlow_score_nuh_concept_id': 'Mapped value '
                                                                                              'concept '
                                                                                              'identifier '
                                                                                              'for Major '
                                                                                              'Surgery/Trauma '
                                                                                              '(Waterlow '
                                                                                              'Score NUH).',
                                        'major_surgery_trauma_waterlow_score_nuh_provenance': 'JSON mapping '
                                                                                              'provenance '
                                                                                              'for Major '
                                                                                              'Surgery/Trauma '
                                                                                              '(Waterlow '
                                                                                              'Score NUH): '
                                                                                              'mapping '
                                                                                              'source, rule '
                                                                                              'ID, ruleset '
                                                                                              'version, '
                                                                                              'canonical '
                                                                                              'rule ID and '
                                                                                              'match status, '
                                                                                              'question '
                                                                                              'concept ID '
                                                                                              'and unit '
                                                                                              'concept ID.',
                                        'major_surgery_trauma_waterlow_score_nuh_text': 'Source response '
                                                                                        'text for Major '
                                                                                        'Surgery/Trauma '
                                                                                        '(Waterlow Score '
                                                                                        'NUH); matches: '
                                                                                        'Major '
                                                                                        'Surgery/Trauma '
                                                                                        '(Waterlow Score '
                                                                                        'NUH) [WATERLOW '
                                                                                        'SCORE]',
                                        'matched_response_row_count': 'Number of canonical matched response '
                                                                      'rows.',
                                        'medication_waterlow_score_nuh': 'Medication (Waterlow Score NUH) — '
                                                                         'CODED/single selected value; '
                                                                         'matches: Medication (Waterlow '
                                                                         'Score NUH) [WATERLOW SCORE]',
                                        'medication_waterlow_score_nuh_concept_id': 'Mapped value concept '
                                                                                    'identifier for '
                                                                                    'Medication (Waterlow '
                                                                                    'Score NUH).',
                                        'medication_waterlow_score_nuh_provenance': 'JSON mapping provenance '
                                                                                    'for Medication '
                                                                                    '(Waterlow Score NUH): '
                                                                                    'mapping source, rule '
                                                                                    'ID, ruleset version, '
                                                                                    'canonical rule ID and '
                                                                                    'match status, question '
                                                                                    'concept ID and unit '
                                                                                    'concept ID.',
                                        'medication_waterlow_score_nuh_text': 'Source response text for '
                                                                              'Medication (Waterlow Score '
                                                                              'NUH); matches: Medication '
                                                                              '(Waterlow Score NUH) '
                                                                              '[WATERLOW SCORE]',
                                        'mobility_waterlow_score': 'Mobility (Waterlow Score) — CODED/single '
                                                                   'selected value; matches: Mobility '
                                                                   '(Waterlow Score) [WATERLOW SCORE]',
                                        'mobility_waterlow_score_concept_id': 'Mapped value concept '
                                                                              'identifier for Mobility '
                                                                              '(Waterlow Score).',
                                        'mobility_waterlow_score_provenance': 'JSON mapping provenance for '
                                                                              'Mobility (Waterlow Score): '
                                                                              'mapping source, rule ID, '
                                                                              'ruleset version, canonical '
                                                                              'rule ID and match status, '
                                                                              'question concept ID and unit '
                                                                              'concept ID.',
                                        'mobility_waterlow_score_text': 'Source response text for Mobility '
                                                                        '(Waterlow Score); matches: Mobility '
                                                                        '(Waterlow Score) [WATERLOW SCORE]',
                                        'neurological_deficits_waterlow_score_nuh': 'Neurological Deficits '
                                                                                    '(Waterlow Score NUH) — '
                                                                                    'CODED/single selected '
                                                                                    'value; matches: '
                                                                                    'Neurological Deficits '
                                                                                    '(Waterlow Score NUH) '
                                                                                    '[WATERLOW SCORE]',
                                        'neurological_deficits_waterlow_score_nuh_concept_id': 'Mapped value '
                                                                                               'concept '
                                                                                               'identifier '
                                                                                               'for '
                                                                                               'Neurological '
                                                                                               'Deficits '
                                                                                               '(Waterlow '
                                                                                               'Score NUH).',
                                        'neurological_deficits_waterlow_score_nuh_provenance': 'JSON mapping '
                                                                                               'provenance '
                                                                                               'for '
                                                                                               'Neurological '
                                                                                               'Deficits '
                                                                                               '(Waterlow '
                                                                                               'Score NUH): '
                                                                                               'mapping '
                                                                                               'source, rule '
                                                                                               'ID, ruleset '
                                                                                               'version, '
                                                                                               'canonical '
                                                                                               'rule ID and '
                                                                                               'match '
                                                                                               'status, '
                                                                                               'question '
                                                                                               'concept ID '
                                                                                               'and unit '
                                                                                               'concept ID.',
                                        'neurological_deficits_waterlow_score_nuh_text': 'Source response '
                                                                                         'text for '
                                                                                         'Neurological '
                                                                                         'Deficits (Waterlow '
                                                                                         'Score NUH); '
                                                                                         'matches: '
                                                                                         'Neurological '
                                                                                         'Deficits (Waterlow '
                                                                                         'Score NUH) '
                                                                                         '[WATERLOW SCORE]',
                                        'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                           'organization.',
                                        'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                             'cross-feed join key.',
                                        'performed_practitioner_id': 'Millennium personnel PERSON_ID for the '
                                                                     'performer.',
                                        'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                        'recent_weight_loss_waterlow_score': 'Recent Weight Loss (Waterlow '
                                                                             'Score) — CODED/single selected '
                                                                             'value; matches: Recent Weight '
                                                                             'Loss (Waterlow Score) '
                                                                             '[WATERLOW SCORE]',
                                        'recent_weight_loss_waterlow_score_concept_id': 'Mapped value '
                                                                                        'concept identifier '
                                                                                        'for Recent Weight '
                                                                                        'Loss (Waterlow '
                                                                                        'Score).',
                                        'recent_weight_loss_waterlow_score_provenance': 'JSON mapping '
                                                                                        'provenance for '
                                                                                        'Recent Weight Loss '
                                                                                        '(Waterlow Score): '
                                                                                        'mapping source, '
                                                                                        'rule ID, ruleset '
                                                                                        'version, canonical '
                                                                                        'rule ID and match '
                                                                                        'status, question '
                                                                                        'concept ID and unit '
                                                                                        'concept ID.',
                                        'recent_weight_loss_waterlow_score_text': 'Source response text for '
                                                                                  'Recent Weight Loss '
                                                                                  '(Waterlow Score); '
                                                                                  'matches: Recent Weight '
                                                                                  'Loss (Waterlow Score) '
                                                                                  '[WATERLOW SCORE]',
                                        'record_status': 'Inherited unchanged from the matching '
                                                         'clinical_form parent. Derived PowerForm row '
                                                         'status: retracted when the grouped maximum of '
                                                         'map_powerform_assessment.SOURCE_PRESENT_IND is 0; '
                                                         'otherwise superseded when the grouped maximum of '
                                                         'ASSESSMENT_ACTIVE_IND is 0 or no '
                                                         'mill_form_activity response has ACTIVE_IND = 1; '
                                                         'otherwise active. Missing assessment flags alone '
                                                         'do not trigger retraction or supersession. This is '
                                                         'a status label, not the raw Boolean assessment '
                                                         'flag.',
                                        'record_status_effective_from': 'Inherited unchanged from the '
                                                                        'matching clinical_form parent. '
                                                                        'Earliest '
                                                                        'mill_form_activity.DOCUMENTATION_DT_TM '
                                                                        'for the form instance, carried from '
                                                                        'authored_datetime. This is the form '
                                                                        'documentation start used by the '
                                                                        'research status projection, not an '
                                                                        'independently recorded '
                                                                        'status-change timestamp; null when '
                                                                        'all contributing documentation '
                                                                        'timestamps are null.',
                                        'record_status_effective_to': 'Inherited unchanged from the matching '
                                                                      'clinical_form parent. For a retracted '
                                                                      'or superseded form, '
                                                                      'completed_datetime: the maximum of '
                                                                      'coalesce(mill_form_activity.LAST_DOCUMENTED_DT_TM, '
                                                                      'mill_form_activity.PERFORMED_DT_TM) '
                                                                      'across its responses. Null for active '
                                                                      'forms or when no contributing '
                                                                      'completion timestamp is available. '
                                                                      'This is a documentation/completion '
                                                                      'boundary, not a Boolean assessment '
                                                                      'flag or an independently recorded '
                                                                      'status-change time.',
                                        'response_row_count': 'Number of response rows grouped into the '
                                                              'form.',
                                        'risk_areas_visual_skin_type__broken_spot_3': 'Risk areas visual '
                                                                                      'skin type option '
                                                                                      "'Broken Spot (3)': "
                                                                                      'true when selected, '
                                                                                      'false when the '
                                                                                      'question was answered '
                                                                                      'with other options '
                                                                                      'only, null when the '
                                                                                      'question was not '
                                                                                      'answered; matches: '
                                                                                      'Risk areas visual '
                                                                                      'skin type [WATERLOW '
                                                                                      'SCORE]',
                                        'risk_areas_visual_skin_type__clammy_1': 'Risk areas visual skin '
                                                                                 "type option 'Clammy (1)': "
                                                                                 'true when selected, false '
                                                                                 'when the question was '
                                                                                 'answered with other '
                                                                                 'options only, null when '
                                                                                 'the question was not '
                                                                                 'answered; matches: Risk '
                                                                                 'areas visual skin type '
                                                                                 '[WATERLOW SCORE]',
                                        'risk_areas_visual_skin_type__discoloured_2': 'Risk areas visual '
                                                                                      'skin type option '
                                                                                      "'Discoloured (2)': "
                                                                                      'true when selected, '
                                                                                      'false when the '
                                                                                      'question was answered '
                                                                                      'with other options '
                                                                                      'only, null when the '
                                                                                      'question was not '
                                                                                      'answered; matches: '
                                                                                      'Risk areas visual '
                                                                                      'skin type [WATERLOW '
                                                                                      'SCORE]',
                                        'risk_areas_visual_skin_type__dry_1': 'Risk areas visual skin type '
                                                                              "option 'Dry (1)': true when "
                                                                              'selected, false when the '
                                                                              'question was answered with '
                                                                              'other options only, null when '
                                                                              'the question was not '
                                                                              'answered; matches: Risk areas '
                                                                              'visual skin type [WATERLOW '
                                                                              'SCORE]',
                                        'risk_areas_visual_skin_type__healthy_0': 'Risk areas visual skin '
                                                                                  "type option 'Healthy "
                                                                                  "(0)': true when selected, "
                                                                                  'false when the question '
                                                                                  'was answered with other '
                                                                                  'options only, null when '
                                                                                  'the question was not '
                                                                                  'answered; matches: Risk '
                                                                                  'areas visual skin type '
                                                                                  '[WATERLOW SCORE]',
                                        'risk_areas_visual_skin_type__oedematous_1': 'Risk areas visual skin '
                                                                                     'type option '
                                                                                     "'Oedematous (1)': true "
                                                                                     'when selected, false '
                                                                                     'when the question was '
                                                                                     'answered with other '
                                                                                     'options only, null '
                                                                                     'when the question was '
                                                                                     'not answered; matches: '
                                                                                     'Risk areas visual skin '
                                                                                     'type [WATERLOW SCORE]',
                                        'risk_areas_visual_skin_type__tissue_paper_1': 'Risk areas visual '
                                                                                       'skin type option '
                                                                                       "'Tissue Paper (1)': "
                                                                                       'true when selected, '
                                                                                       'false when the '
                                                                                       'question was '
                                                                                       'answered with other '
                                                                                       'options only, null '
                                                                                       'when the question '
                                                                                       'was not answered; '
                                                                                       'matches: Risk areas '
                                                                                       'visual skin type '
                                                                                       '[WATERLOW SCORE]',
                                        'risk_areas_visual_skin_type_text': 'Pipe-joined selected CODED '
                                                                            'values for Risk areas visual '
                                                                            'skin type, ordered by sequence '
                                                                            'and response ID; matches: Risk '
                                                                            'areas visual skin type '
                                                                            '[WATERLOW SCORE]',
                                        'sex_age_waterlow_score__14_49': 'Sex/Age (Waterlow Score) option '
                                                                         "'14-49': true when selected, false "
                                                                         'when the question was answered '
                                                                         'with other options only, null when '
                                                                         'the question was not answered; '
                                                                         'matches: Sex/Age (Waterlow Score) '
                                                                         '[WATERLOW SCORE]',
                                        'sex_age_waterlow_score__50_64': 'Sex/Age (Waterlow Score) option '
                                                                         "'50-64': true when selected, false "
                                                                         'when the question was answered '
                                                                         'with other options only, null when '
                                                                         'the question was not answered; '
                                                                         'matches: Sex/Age (Waterlow Score) '
                                                                         '[WATERLOW SCORE]',
                                        'sex_age_waterlow_score__65_74': 'Sex/Age (Waterlow Score) option '
                                                                         "'65-74': true when selected, false "
                                                                         'when the question was answered '
                                                                         'with other options only, null when '
                                                                         'the question was not answered; '
                                                                         'matches: Sex/Age (Waterlow Score) '
                                                                         '[WATERLOW SCORE]',
                                        'sex_age_waterlow_score__75_80': 'Sex/Age (Waterlow Score) option '
                                                                         "'75-80': true when selected, false "
                                                                         'when the question was answered '
                                                                         'with other options only, null when '
                                                                         'the question was not answered; '
                                                                         'matches: Sex/Age (Waterlow Score) '
                                                                         '[WATERLOW SCORE]',
                                        'sex_age_waterlow_score__80': 'Sex/Age (Waterlow Score) option '
                                                                      "'80+': true when selected, false when "
                                                                      'the question was answered with other '
                                                                      'options only, null when the question '
                                                                      'was not answered; matches: Sex/Age '
                                                                      '(Waterlow Score) [WATERLOW SCORE]',
                                        'sex_age_waterlow_score__female': 'Sex/Age (Waterlow Score) option '
                                                                          "'Female': true when selected, "
                                                                          'false when the question was '
                                                                          'answered with other options only, '
                                                                          'null when the question was not '
                                                                          'answered; matches: Sex/Age '
                                                                          '(Waterlow Score) [WATERLOW SCORE]',
                                        'sex_age_waterlow_score__male': 'Sex/Age (Waterlow Score) option '
                                                                        "'Male': true when selected, false "
                                                                        'when the question was answered with '
                                                                        'other options only, null when the '
                                                                        'question was not answered; matches: '
                                                                        'Sex/Age (Waterlow Score) [WATERLOW '
                                                                        'SCORE]',
                                        'sex_age_waterlow_score_text': 'Pipe-joined selected CODED values '
                                                                       'for Sex/Age (Waterlow Score), '
                                                                       'ordered by sequence and response ID; '
                                                                       'matches: Sex/Age (Waterlow Score) '
                                                                       '[WATERLOW SCORE]',
                                        'sex_waterlow_score': 'Sex (Waterlow Score) — CODED/single selected '
                                                              'value; matches: Sex (Waterlow Score) '
                                                              '[WATERLOW SCORE]',
                                        'sex_waterlow_score_concept_id': 'Mapped value concept identifier '
                                                                         'for Sex (Waterlow Score).',
                                        'sex_waterlow_score_provenance': 'JSON mapping provenance for Sex '
                                                                         '(Waterlow Score): mapping source, '
                                                                         'rule ID, ruleset version, '
                                                                         'canonical rule ID and match '
                                                                         'status, question concept ID and '
                                                                         'unit concept ID.',
                                        'sex_waterlow_score_text': 'Source response text for Sex (Waterlow '
                                                                   'Score); matches: Sex (Waterlow Score) '
                                                                   '[WATERLOW SCORE]',
                                        'skin_type_and_visual_risk_areas__broken_spots': 'Skin Type and '
                                                                                         'Visual Risk Areas '
                                                                                         'option '
                                                                                         "'Broken/Spots': "
                                                                                         'true when '
                                                                                         'selected, false '
                                                                                         'when the question '
                                                                                         'was answered with '
                                                                                         'other options '
                                                                                         'only, null when '
                                                                                         'the question was '
                                                                                         'not answered; '
                                                                                         'matches: Skin Type '
                                                                                         'and Visual Risk '
                                                                                         'Areas [WATERLOW '
                                                                                         'SCORE]',
                                        'skin_type_and_visual_risk_areas__clammy_tempreature': 'Skin Type '
                                                                                               'and Visual '
                                                                                               'Risk Areas '
                                                                                               'option '
                                                                                               "'Clammy/Tempreature': "
                                                                                               'true when '
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
                                                                                               'matches: '
                                                                                               'Skin Type '
                                                                                               'and Visual '
                                                                                               'Risk Areas '
                                                                                               '[WATERLOW '
                                                                                               'SCORE]',
                                        'skin_type_and_visual_risk_areas__discoloured': 'Skin Type and '
                                                                                        'Visual Risk Areas '
                                                                                        'option '
                                                                                        "'Discoloured': true "
                                                                                        'when selected, '
                                                                                        'false when the '
                                                                                        'question was '
                                                                                        'answered with other '
                                                                                        'options only, null '
                                                                                        'when the question '
                                                                                        'was not answered; '
                                                                                        'matches: Skin Type '
                                                                                        'and Visual Risk '
                                                                                        'Areas [WATERLOW '
                                                                                        'SCORE]',
                                        'skin_type_and_visual_risk_areas__dry': 'Skin Type and Visual Risk '
                                                                                "Areas option 'Dry': true "
                                                                                'when selected, false when '
                                                                                'the question was answered '
                                                                                'with other options only, '
                                                                                'null when the question was '
                                                                                'not answered; matches: Skin '
                                                                                'Type and Visual Risk Areas '
                                                                                '[WATERLOW SCORE]',
                                        'skin_type_and_visual_risk_areas__healthy': 'Skin Type and Visual '
                                                                                    'Risk Areas option '
                                                                                    "'Healthy': true when "
                                                                                    'selected, false when '
                                                                                    'the question was '
                                                                                    'answered with other '
                                                                                    'options only, null when '
                                                                                    'the question was not '
                                                                                    'answered; matches: Skin '
                                                                                    'Type and Visual Risk '
                                                                                    'Areas [WATERLOW SCORE]',
                                        'skin_type_and_visual_risk_areas__oedematous': 'Skin Type and Visual '
                                                                                       'Risk Areas option '
                                                                                       "'Oedematous': true "
                                                                                       'when selected, false '
                                                                                       'when the question '
                                                                                       'was answered with '
                                                                                       'other options only, '
                                                                                       'null when the '
                                                                                       'question was not '
                                                                                       'answered; matches: '
                                                                                       'Skin Type and Visual '
                                                                                       'Risk Areas [WATERLOW '
                                                                                       'SCORE]',
                                        'skin_type_and_visual_risk_areas__tissue_paper': 'Skin Type and '
                                                                                         'Visual Risk Areas '
                                                                                         "option 'Tissue "
                                                                                         "Paper': true when "
                                                                                         'selected, false '
                                                                                         'when the question '
                                                                                         'was answered with '
                                                                                         'other options '
                                                                                         'only, null when '
                                                                                         'the question was '
                                                                                         'not answered; '
                                                                                         'matches: Skin Type '
                                                                                         'and Visual Risk '
                                                                                         'Areas [WATERLOW '
                                                                                         'SCORE]',
                                        'skin_type_and_visual_risk_areas_text': 'Pipe-joined selected CODED '
                                                                                'values for Skin Type and '
                                                                                'Visual Risk Areas, ordered '
                                                                                'by sequence and response '
                                                                                'ID; matches: Skin Type and '
                                                                                'Visual Risk Areas [WATERLOW '
                                                                                'SCORE]',
                                        'source_code': 'Source form reference identifier.',
                                        'source_coding_system': 'Source form coding system.',
                                        'source_display': 'Source form description.',
                                        'source_feed': 'Registered owning feed.',
                                        'source_update_timestamp': 'Inherited unchanged from the matching '
                                                                   'clinical_form parent. Maximum, across '
                                                                   "the form's mill_form_activity rows, of "
                                                                   'the greatest ADC_UPDT, '
                                                                   'SOURCE_ACTIVITY_ADC_UPDT, '
                                                                   'SOURCE_EVENT_ADC_UPDT and '
                                                                   'SOURCE_LABEL_ADC_UPDT. This combines the '
                                                                   'contributing activity, event, label and '
                                                                   'bronze-ingestion clocks; it is not '
                                                                   'uniformly a native clinical-system '
                                                                   'update time. Null only when every '
                                                                   'contributing clock is null.',
                                        'subject_id_system': 'Identifier system used for subject_key.',
                                        'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                       'identifier. Not salted, not secret; a stable join '
                                                       'key across feeds where person_id is unresolved.',
                                        'tissue_malnutrition_waterlow_score_nuh': 'Tissue Malnutrition '
                                                                                  '(Waterlow Score NUH) — '
                                                                                  'CODED/single selected '
                                                                                  'value; matches: Tissue '
                                                                                  'Malnutrition (Waterlow '
                                                                                  'Score NUH) [WATERLOW '
                                                                                  'SCORE]',
                                        'tissue_malnutrition_waterlow_score_nuh_concept_id': 'Mapped value '
                                                                                             'concept '
                                                                                             'identifier for '
                                                                                             'Tissue '
                                                                                             'Malnutrition '
                                                                                             '(Waterlow '
                                                                                             'Score NUH).',
                                        'tissue_malnutrition_waterlow_score_nuh_provenance': 'JSON mapping '
                                                                                             'provenance for '
                                                                                             'Tissue '
                                                                                             'Malnutrition '
                                                                                             '(Waterlow '
                                                                                             'Score NUH): '
                                                                                             'mapping '
                                                                                             'source, rule '
                                                                                             'ID, ruleset '
                                                                                             'version, '
                                                                                             'canonical rule '
                                                                                             'ID and match '
                                                                                             'status, '
                                                                                             'question '
                                                                                             'concept ID and '
                                                                                             'unit concept '
                                                                                             'ID.',
                                        'tissue_malnutrition_waterlow_score_nuh_text': 'Source response text '
                                                                                       'for Tissue '
                                                                                       'Malnutrition '
                                                                                       '(Waterlow Score '
                                                                                       'NUH); matches: '
                                                                                       'Tissue Malnutrition '
                                                                                       '(Waterlow Score NUH) '
                                                                                       '[WATERLOW SCORE]',
                                        'unmatched_response_row_count': 'Number of canonical unmatched '
                                                                        'response rows.',
                                        'vip_ind': 'VIP indicator when supplied.',
                                        'waterlow_appetite': 'Waterlow Appetite — CODED/single selected '
                                                             'value; matches: Waterlow Appetite [WATERLOW '
                                                             'SCORE]',
                                        'waterlow_appetite_concept_id': 'Mapped value concept identifier for '
                                                                        'Waterlow Appetite.',
                                        'waterlow_appetite_provenance': 'JSON mapping provenance for '
                                                                        'Waterlow Appetite: mapping source, '
                                                                        'rule ID, ruleset version, canonical '
                                                                        'rule ID and match status, question '
                                                                        'concept ID and unit concept ID.',
                                        'waterlow_appetite_text': 'Source response text for Waterlow '
                                                                  'Appetite; matches: Waterlow Appetite '
                                                                  '[WATERLOW SCORE]',
                                        'waterlow_major_surgery_or_trauma': 'Waterlow Major Surgery or '
                                                                            'trauma — CODED/single selected '
                                                                            'value; matches: Waterlow Major '
                                                                            'Surgery or trauma [WATERLOW '
                                                                            'SCORE]',
                                        'waterlow_major_surgery_or_trauma_concept_id': 'Mapped value concept '
                                                                                       'identifier for '
                                                                                       'Waterlow Major '
                                                                                       'Surgery or trauma.',
                                        'waterlow_major_surgery_or_trauma_provenance': 'JSON mapping '
                                                                                       'provenance for '
                                                                                       'Waterlow Major '
                                                                                       'Surgery or trauma: '
                                                                                       'mapping source, rule '
                                                                                       'ID, ruleset version, '
                                                                                       'canonical rule ID '
                                                                                       'and match status, '
                                                                                       'question concept ID '
                                                                                       'and unit concept ID.',
                                        'waterlow_major_surgery_or_trauma_text': 'Source response text for '
                                                                                 'Waterlow Major Surgery or '
                                                                                 'trauma; matches: Waterlow '
                                                                                 'Major Surgery or trauma '
                                                                                 '[WATERLOW SCORE]',
                                        'waterlow_medications': 'Waterlow Medications — CODED/single '
                                                                'selected value; matches: Waterlow '
                                                                'Medications [WATERLOW SCORE]',
                                        'waterlow_medications_concept_id': 'Mapped value concept identifier '
                                                                           'for Waterlow Medications.',
                                        'waterlow_medications_provenance': 'JSON mapping provenance for '
                                                                           'Waterlow Medications: mapping '
                                                                           'source, rule ID, ruleset '
                                                                           'version, canonical rule ID and '
                                                                           'match status, question concept '
                                                                           'ID and unit concept ID.',
                                        'waterlow_medications_text': 'Source response text for Waterlow '
                                                                     'Medications; matches: Waterlow '
                                                                     'Medications [WATERLOW SCORE]',
                                        'waterlow_mobility': 'Waterlow mobility — CODED/single selected '
                                                             'value; matches: Waterlow mobility [WATERLOW '
                                                             'SCORE]',
                                        'waterlow_mobility_concept_id': 'Mapped value concept identifier for '
                                                                        'Waterlow mobility.',
                                        'waterlow_mobility_provenance': 'JSON mapping provenance for '
                                                                        'Waterlow mobility: mapping source, '
                                                                        'rule ID, ruleset version, canonical '
                                                                        'rule ID and match status, question '
                                                                        'concept ID and unit concept ID.',
                                        'waterlow_mobility_text': 'Source response text for Waterlow '
                                                                  'mobility; matches: Waterlow mobility '
                                                                  '[WATERLOW SCORE]',
                                        'waterlow_neurological_deficit': 'Waterlow Neurological deficit — '
                                                                         'CODED/single selected value; '
                                                                         'matches: Waterlow Neurological '
                                                                         'deficit [WATERLOW SCORE]',
                                        'waterlow_neurological_deficit_concept_id': 'Mapped value concept '
                                                                                    'identifier for Waterlow '
                                                                                    'Neurological deficit.',
                                        'waterlow_neurological_deficit_provenance': 'JSON mapping provenance '
                                                                                    'for Waterlow '
                                                                                    'Neurological deficit: '
                                                                                    'mapping source, rule '
                                                                                    'ID, ruleset version, '
                                                                                    'canonical rule ID and '
                                                                                    'match status, question '
                                                                                    'concept ID and unit '
                                                                                    'concept ID.',
                                        'waterlow_neurological_deficit_text': 'Source response text for '
                                                                              'Waterlow Neurological '
                                                                              'deficit; matches: Waterlow '
                                                                              'Neurological deficit '
                                                                              '[WATERLOW SCORE]',
                                        'waterlow_score_result': 'Waterlow Score Result — NUMERIC/single '
                                                                 'selected value; matches: Waterlow Score '
                                                                 'Result [WATERLOW SCORE]',
                                        'waterlow_score_result_concept_id': 'Mapped value concept identifier '
                                                                            'for Waterlow Score Result.',
                                        'waterlow_score_result_provenance': 'JSON mapping provenance for '
                                                                            'Waterlow Score Result: mapping '
                                                                            'source, rule ID, ruleset '
                                                                            'version, canonical rule ID and '
                                                                            'match status, question concept '
                                                                            'ID and unit concept ID.',
                                        'waterlow_score_result_text': 'Source response text for Waterlow '
                                                                      'Score Result; matches: Waterlow Score '
                                                                      'Result [WATERLOW SCORE]',
                                        'waterlow_tissue_malnutrition': 'Waterlow Tissue Malnutrition — '
                                                                        'CODED/single selected value; '
                                                                        'matches: Waterlow Tissue '
                                                                        'Malnutrition [WATERLOW SCORE]',
                                        'waterlow_tissue_malnutrition_concept_id': 'Mapped value concept '
                                                                                   'identifier for Waterlow '
                                                                                   'Tissue Malnutrition.',
                                        'waterlow_tissue_malnutrition_provenance': 'JSON mapping provenance '
                                                                                   'for Waterlow Tissue '
                                                                                   'Malnutrition: mapping '
                                                                                   'source, rule ID, ruleset '
                                                                                   'version, canonical rule '
                                                                                   'ID and match status, '
                                                                                   'question concept ID and '
                                                                                   'unit concept ID.',
                                        'waterlow_tissue_malnutrition_text': 'Source response text for '
                                                                             'Waterlow Tissue Malnutrition; '
                                                                             'matches: Waterlow Tissue '
                                                                             'Malnutrition [WATERLOW SCORE]',
                                        'waterlow_total_score': 'Waterlow Total Score — NUMERIC/single '
                                                                'selected value; matches: Waterlow Total '
                                                                'Score [WATERLOW SCORE]',
                                        'waterlow_total_score_concept_id': 'Mapped value concept identifier '
                                                                           'for Waterlow Total Score.',
                                        'waterlow_total_score_provenance': 'JSON mapping provenance for '
                                                                           'Waterlow Total Score: mapping '
                                                                           'source, rule ID, ruleset '
                                                                           'version, canonical rule ID and '
                                                                           'match status, question concept '
                                                                           'ID and unit concept ID.',
                                        'waterlow_total_score_risk': 'Waterlow Total Score - Risk — '
                                                                     'CODED/single selected value; matches: '
                                                                     'Waterlow Total Score - Risk [WATERLOW '
                                                                     'SCORE]',
                                        'waterlow_total_score_risk_concept_id': 'Mapped value concept '
                                                                                'identifier for Waterlow '
                                                                                'Total Score - Risk.',
                                        'waterlow_total_score_risk_provenance': 'JSON mapping provenance for '
                                                                                'Waterlow Total Score - '
                                                                                'Risk: mapping source, rule '
                                                                                'ID, ruleset version, '
                                                                                'canonical rule ID and match '
                                                                                'status, question concept ID '
                                                                                'and unit concept ID.',
                                        'waterlow_total_score_risk_text': 'Source response text for Waterlow '
                                                                          'Total Score - Risk; matches: '
                                                                          'Waterlow Total Score - Risk '
                                                                          '[WATERLOW SCORE]',
                                        'waterlow_total_score_text': 'Source response text for Waterlow '
                                                                     'Total Score; matches: Waterlow Total '
                                                                     'Score [WATERLOW SCORE]',
                                        'weight_kg': 'Weight (kg) — NUMERIC/single selected value; matches: '
                                                     'Weight (kg) [WATERLOW SCORE]',
                                        'weight_kg_concept_id': 'Mapped value concept identifier for Weight '
                                                                '(kg).',
                                        'weight_kg_provenance': 'JSON mapping provenance for Weight (kg): '
                                                                'mapping source, rule ID, ruleset version, '
                                                                'canonical rule ID and match status, '
                                                                'question concept ID and unit concept ID.',
                                        'weight_kg_text': 'Source response text for Weight (kg); matches: '
                                                          'Weight (kg) [WATERLOW SCORE]',
                                        'weight_loss_waterlow_score': 'Weight Loss (Waterlow Score) — '
                                                                      'CODED/single selected value; matches: '
                                                                      'Weight Loss (Waterlow Score) '
                                                                      '[WATERLOW SCORE]',
                                        'weight_loss_waterlow_score_concept_id': 'Mapped value concept '
                                                                                 'identifier for Weight Loss '
                                                                                 '(Waterlow Score).',
                                        'weight_loss_waterlow_score_provenance': 'JSON mapping provenance '
                                                                                 'for Weight Loss (Waterlow '
                                                                                 'Score): mapping source, '
                                                                                 'rule ID, ruleset version, '
                                                                                 'canonical rule ID and '
                                                                                 'match status, question '
                                                                                 'concept ID and unit '
                                                                                 'concept ID.',
                                        'weight_loss_waterlow_score_text': 'Source response text for Weight '
                                                                           'Loss (Waterlow Score); matches: '
                                                                           'Weight Loss (Waterlow Score) '
                                                                           '[WATERLOW SCORE]',
                                        'weight_measured': 'Weight Measured — NUMERIC/single selected value; '
                                                           'matches: Weight Measured [WATERLOW SCORE]',
                                        'weight_measured_concept_id': 'Mapped value concept identifier for '
                                                                      'Weight Measured.',
                                        'weight_measured_provenance': 'JSON mapping provenance for Weight '
                                                                      'Measured: mapping source, rule ID, '
                                                                      'ruleset version, canonical rule ID '
                                                                      'and match status, question concept ID '
                                                                      'and unit concept ID.',
                                        'weight_measured_text': 'Source response text for Weight Measured; '
                                                                'matches: Weight Measured [WATERLOW SCORE]',
                                        'withheld_identity_ind': 'Withheld identity indicator.',
                                        'zzzheight_metres': 'zzzHeight (metres) — NUMERIC/single selected '
                                                            'value; matches: zzzHeight (metres) [WATERLOW '
                                                            'SCORE]',
                                        'zzzheight_metres_concept_id': 'Mapped value concept identifier for '
                                                                       'zzzHeight (metres).',
                                        'zzzheight_metres_provenance': 'JSON mapping provenance for '
                                                                       'zzzHeight (metres): mapping source, '
                                                                       'rule ID, ruleset version, canonical '
                                                                       'rule ID and match status, question '
                                                                       'concept ID and unit concept ID.',
                                        'zzzheight_metres_text': 'Source response text for zzzHeight '
                                                                 '(metres); matches: zzzHeight (metres) '
                                                                 '[WATERLOW SCORE]'},
                            'table': 'Waterlow pressure-ulcer risk assessment: one row per form instance '
                                     'with configured questions as columns. Residual responses are in '
                                     'clinical_form_response.'}}
