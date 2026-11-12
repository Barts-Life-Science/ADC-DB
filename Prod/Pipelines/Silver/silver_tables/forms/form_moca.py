"""Silver forms.form_moca: 1 dataset(s).

- datasets: clinical_form_moca
- moved verbatim from: 6_silver_forms:9248

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
    name=_n("journey_clinical.form_moca"),
    refresh_policy="auto",
)
def form_moca():
    # Build the declared dataset: Montreal Cognitive Assessment: one row per form instance with
    # configured questions as columns. Residual responses are in clinical_form_response.
    cfg = FORM_CONFIG_BY_SLUG["moca"]
    parent = _instrument_parent(cfg)
    responses = _instrument_responses("moca")
    picked = responses.groupBy("dcp_forms_activity_id").agg(
        *_single_aggregations("moca_abstraction_points", "NUMERIC"),
        *_single_aggregations("moca_attention_task_one_points", "NUMERIC"),
        *_single_aggregations("moca_attention_task_three_points", "NUMERIC"),
        *_single_aggregations("moca_attention_task_two_points", "NUMERIC"),
        *_single_aggregations("moca_fluency_points", "NUMERIC"),
        *_single_aggregations("moca_naming_points", "NUMERIC"),
        *_single_aggregations("moca_orientation_points", "NUMERIC"),
        *_single_aggregations("moca_repeat_points", "NUMERIC"),
        *_single_aggregations("moca_visuospatial_points", "NUMERIC"),
        *_single_aggregations("moca_abstraction_first", "CODED"),
        *_single_aggregations("moca_abstraction_second", "CODED"),
        *_single_aggregations("moca_attention_task_one_a", "CODED"),
        *_single_aggregations("moca_attention_task_one_b", "CODED"),
        *_single_aggregations("moca_attention_task_three", "CODED"),
        *_single_aggregations("moca_attention_task_two", "CODED"),
        *_single_aggregations("moca_camel", "CODED"),
        *_single_aggregations("moca_city", "CODED"),
        *_single_aggregations("moca_contour", "CODED"),
        *_single_aggregations("moca_copy_cube", "CODED"),
        *_single_aggregations("moca_date", "CODED"),
        *_single_aggregations("moca_day", "CODED"),
        *_single_aggregations("moca_fluency", "CODED"),
        *_single_aggregations("moca_hands", "CODED"),
        *_single_aggregations("moca_lion", "CODED"),
        *_single_aggregations("moca_month", "CODED"),
        *_single_aggregations("moca_numbers", "CODED"),
        *_single_aggregations("moca_place", "CODED"),
        *_single_aggregations("moca_repeat_first", "CODED"),
        *_single_aggregations("moca_repeat_second", "CODED"),
        *_single_aggregations("moca_rhinoceros", "CODED"),
        *_single_aggregations("moca_unable_to_assess", "CODED"),
        *_single_aggregations("moca_visuospatial", "CODED"),
        *_single_aggregations("moca_year", "CODED"),
        *_single_aggregations("moca_delayed_recall_points", "NUMERIC"),
        *_single_aggregations("moca_total_score", "NUMERIC"),
        *_multi_aggregations(cfg, "moca_delayed_recall", "CODED"),
        *_single_aggregations("moca_administered", "TEXT"),
        *_multi_aggregations(cfg, "moca_category_cue", "CODED"),
        *_multi_aggregations(cfg, "moca_memory_first", "CODED"),
        *_multi_aggregations(cfg, "moca_memory_second", "CODED"),
        *_single_aggregations("moca_comments", "TEXT"),
        *_multi_aggregations(cfg, "moca_multi_choice_cue", "CODED"),
    )
    return parent.join(picked, "dcp_forms_activity_id", "left")


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_form_moca': {'columns': {'active_response_row_count': 'Number of active response rows.',
                                    'authored_datetime': 'Earliest form documentation timestamp.',
                                    'completed_datetime': 'Latest documentation or performed timestamp.',
                                    'confidentiality_code': 'Security classification when supplied.',
                                    'dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID; primary key '
                                                             'of this table.',
                                    'empty_response_row_count': 'Number of retained empty response rows.',
                                    'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                    'spine_encounter.encounter_id.',
                                    'event_datetime': 'First documented timestamp.',
                                    'event_datetime_local': 'event_datetime in Europe/London wall-clock time '
                                                            '(GMT/BST). Millennium rows take the bronze '
                                                            '*_LOCAL companions of the same source columns '
                                                            'and fallback order; rows from local-clock '
                                                            'sources (LUNA, PACS/DICOM, TFC LIMS, CCMDS, '
                                                            'MSDS, BadgerNet, EndoBase, SLAM, iWeb, Datix '
                                                            'and similar) carry event_datetime unchanged. '
                                                            'event_datetime is a UTC instant for Millennium '
                                                            'rows, so take local calendar dates and clock '
                                                            'times from this column.',
                                    'event_end_datetime': 'Last documented or performed timestamp.',
                                    'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                'wall-clock time (GMT/BST), by the same rule '
                                                                'as event_datetime_local; null where '
                                                                'event_end_datetime is null.',
                                    'form_status_code': 'Source form status code.',
                                    'form_status_display': 'Source form status display.',
                                    'form_type_code': 'Source form type code.',
                                    'form_type_display': 'Source form type display.',
                                    'invalid_response_row_count': 'Number of source-classified invalid '
                                                                  'response rows.',
                                    'loaded_at': 'Inherited unchanged from the matching clinical_form '
                                                 'parent. Greatest of the form-level maximum ADC_UPDT values '
                                                 'from mill_form_activity, map_powerform_assessment_item and '
                                                 'map_powerform_assessment, and maximum Source_ADC_UPDT from '
                                                 'map_mat_vte_assessment. This is the contributing-source '
                                                 'ingestion/provenance clock, not the Silver refresh time or '
                                                 'form event time; null only when every contributing clock '
                                                 'is null.',
                                    'matched_response_row_count': 'Number of canonical matched response '
                                                                  'rows.',
                                    'moca_abstraction_first': 'MOCA - Abstraction First — CODED/single '
                                                              'selected value; matches: MOCA - Abstraction '
                                                              'First [MOCA II]',
                                    'moca_abstraction_first_concept_id': 'Mapped value concept identifier '
                                                                         'for MOCA - Abstraction First.',
                                    'moca_abstraction_first_provenance': 'JSON mapping provenance for MOCA - '
                                                                         'Abstraction First: mapping source, '
                                                                         'rule ID, ruleset version, '
                                                                         'canonical rule ID and match '
                                                                         'status, question concept ID and '
                                                                         'unit concept ID.',
                                    'moca_abstraction_first_text': 'Source response text for MOCA - '
                                                                   'Abstraction First; matches: MOCA - '
                                                                   'Abstraction First [MOCA II]',
                                    'moca_abstraction_points': 'MOCA - Abstraction Points — NUMERIC/single '
                                                               'selected value; matches: MOCA - Abstraction '
                                                               'Points [MOCA II]',
                                    'moca_abstraction_points_concept_id': 'Mapped value concept identifier '
                                                                          'for MOCA - Abstraction Points.',
                                    'moca_abstraction_points_provenance': 'JSON mapping provenance for MOCA '
                                                                          '- Abstraction Points: mapping '
                                                                          'source, rule ID, ruleset version, '
                                                                          'canonical rule ID and match '
                                                                          'status, question concept ID and '
                                                                          'unit concept ID.',
                                    'moca_abstraction_points_text': 'Source response text for MOCA - '
                                                                    'Abstraction Points; matches: MOCA - '
                                                                    'Abstraction Points [MOCA II]',
                                    'moca_abstraction_second': 'MOCA - Abstraction Second — CODED/single '
                                                               'selected value; matches: MOCA - Abstraction '
                                                               'Second [MOCA II]',
                                    'moca_abstraction_second_concept_id': 'Mapped value concept identifier '
                                                                          'for MOCA - Abstraction Second.',
                                    'moca_abstraction_second_provenance': 'JSON mapping provenance for MOCA '
                                                                          '- Abstraction Second: mapping '
                                                                          'source, rule ID, ruleset version, '
                                                                          'canonical rule ID and match '
                                                                          'status, question concept ID and '
                                                                          'unit concept ID.',
                                    'moca_abstraction_second_text': 'Source response text for MOCA - '
                                                                    'Abstraction Second; matches: MOCA - '
                                                                    'Abstraction Second [MOCA II]',
                                    'moca_administered': 'MOCA - Administered — TEXT/single selected value; '
                                                         'matches: MOCA - Administered [MOCA II]',
                                    'moca_administered_concept_id': 'Mapped value concept identifier for '
                                                                    'MOCA - Administered.',
                                    'moca_administered_provenance': 'JSON mapping provenance for MOCA - '
                                                                    'Administered: mapping source, rule ID, '
                                                                    'ruleset version, canonical rule ID and '
                                                                    'match status, question concept ID and '
                                                                    'unit concept ID.',
                                    'moca_administered_text': 'Source response text for MOCA - Administered; '
                                                              'matches: MOCA - Administered [MOCA II]',
                                    'moca_attention_task_one_a': 'MOCA - Attention Task One A — CODED/single '
                                                                 'selected value; matches: MOCA - Attention '
                                                                 'Task One A [MOCA I]',
                                    'moca_attention_task_one_a_concept_id': 'Mapped value concept identifier '
                                                                            'for MOCA - Attention Task One '
                                                                            'A.',
                                    'moca_attention_task_one_a_provenance': 'JSON mapping provenance for '
                                                                            'MOCA - Attention Task One A: '
                                                                            'mapping source, rule ID, '
                                                                            'ruleset version, canonical rule '
                                                                            'ID and match status, question '
                                                                            'concept ID and unit concept ID.',
                                    'moca_attention_task_one_a_text': 'Source response text for MOCA - '
                                                                      'Attention Task One A; matches: MOCA - '
                                                                      'Attention Task One A [MOCA I]',
                                    'moca_attention_task_one_b': 'MOCA - Attention Task One B — CODED/single '
                                                                 'selected value; matches: MOCA - Attention '
                                                                 'Task One B [MOCA I]',
                                    'moca_attention_task_one_b_concept_id': 'Mapped value concept identifier '
                                                                            'for MOCA - Attention Task One '
                                                                            'B.',
                                    'moca_attention_task_one_b_provenance': 'JSON mapping provenance for '
                                                                            'MOCA - Attention Task One B: '
                                                                            'mapping source, rule ID, '
                                                                            'ruleset version, canonical rule '
                                                                            'ID and match status, question '
                                                                            'concept ID and unit concept ID.',
                                    'moca_attention_task_one_b_text': 'Source response text for MOCA - '
                                                                      'Attention Task One B; matches: MOCA - '
                                                                      'Attention Task One B [MOCA I]',
                                    'moca_attention_task_one_points': 'MOCA - Attention Task One Points — '
                                                                      'NUMERIC/single selected value; '
                                                                      'matches: MOCA - Attention Task One '
                                                                      'Points [MOCA I]',
                                    'moca_attention_task_one_points_concept_id': 'Mapped value concept '
                                                                                 'identifier for MOCA - '
                                                                                 'Attention Task One Points.',
                                    'moca_attention_task_one_points_provenance': 'JSON mapping provenance '
                                                                                 'for MOCA - Attention Task '
                                                                                 'One Points: mapping '
                                                                                 'source, rule ID, ruleset '
                                                                                 'version, canonical rule ID '
                                                                                 'and match status, question '
                                                                                 'concept ID and unit '
                                                                                 'concept ID.',
                                    'moca_attention_task_one_points_text': 'Source response text for MOCA - '
                                                                           'Attention Task One Points; '
                                                                           'matches: MOCA - Attention Task '
                                                                           'One Points [MOCA I]',
                                    'moca_attention_task_three': 'MOCA - Attention Task Three — CODED/single '
                                                                 'selected value; matches: MOCA - Attention '
                                                                 'Task Three [MOCA I]',
                                    'moca_attention_task_three_concept_id': 'Mapped value concept identifier '
                                                                            'for MOCA - Attention Task '
                                                                            'Three.',
                                    'moca_attention_task_three_points': 'MOCA - Attention Task Three Points '
                                                                        '— NUMERIC/single selected value; '
                                                                        'matches: MOCA - Attention Task '
                                                                        'Three Points [MOCA I]',
                                    'moca_attention_task_three_points_concept_id': 'Mapped value concept '
                                                                                   'identifier for MOCA - '
                                                                                   'Attention Task Three '
                                                                                   'Points.',
                                    'moca_attention_task_three_points_provenance': 'JSON mapping provenance '
                                                                                   'for MOCA - Attention '
                                                                                   'Task Three Points: '
                                                                                   'mapping source, rule ID, '
                                                                                   'ruleset version, '
                                                                                   'canonical rule ID and '
                                                                                   'match status, question '
                                                                                   'concept ID and unit '
                                                                                   'concept ID.',
                                    'moca_attention_task_three_points_text': 'Source response text for MOCA '
                                                                             '- Attention Task Three Points; '
                                                                             'matches: MOCA - Attention Task '
                                                                             'Three Points [MOCA I]',
                                    'moca_attention_task_three_provenance': 'JSON mapping provenance for '
                                                                            'MOCA - Attention Task Three: '
                                                                            'mapping source, rule ID, '
                                                                            'ruleset version, canonical rule '
                                                                            'ID and match status, question '
                                                                            'concept ID and unit concept ID.',
                                    'moca_attention_task_three_text': 'Source response text for MOCA - '
                                                                      'Attention Task Three; matches: MOCA - '
                                                                      'Attention Task Three [MOCA I]',
                                    'moca_attention_task_two': 'MOCA - Attention Task Two — CODED/single '
                                                               'selected value; matches: MOCA - Attention '
                                                               'Task Two [MOCA I]',
                                    'moca_attention_task_two_concept_id': 'Mapped value concept identifier '
                                                                          'for MOCA - Attention Task Two.',
                                    'moca_attention_task_two_points': 'MOCA - Attention Task Two Points — '
                                                                      'NUMERIC/single selected value; '
                                                                      'matches: MOCA - Attention Task Two '
                                                                      'Points [MOCA I]',
                                    'moca_attention_task_two_points_concept_id': 'Mapped value concept '
                                                                                 'identifier for MOCA - '
                                                                                 'Attention Task Two Points.',
                                    'moca_attention_task_two_points_provenance': 'JSON mapping provenance '
                                                                                 'for MOCA - Attention Task '
                                                                                 'Two Points: mapping '
                                                                                 'source, rule ID, ruleset '
                                                                                 'version, canonical rule ID '
                                                                                 'and match status, question '
                                                                                 'concept ID and unit '
                                                                                 'concept ID.',
                                    'moca_attention_task_two_points_text': 'Source response text for MOCA - '
                                                                           'Attention Task Two Points; '
                                                                           'matches: MOCA - Attention Task '
                                                                           'Two Points [MOCA I]',
                                    'moca_attention_task_two_provenance': 'JSON mapping provenance for MOCA '
                                                                          '- Attention Task Two: mapping '
                                                                          'source, rule ID, ruleset version, '
                                                                          'canonical rule ID and match '
                                                                          'status, question concept ID and '
                                                                          'unit concept ID.',
                                    'moca_attention_task_two_text': 'Source response text for MOCA - '
                                                                    'Attention Task Two; matches: MOCA - '
                                                                    'Attention Task Two [MOCA I]',
                                    'moca_camel': 'MOCA - Camel — CODED/single selected value; matches: MOCA '
                                                  '- Camel [MOCA I]',
                                    'moca_camel_concept_id': 'Mapped value concept identifier for MOCA - '
                                                             'Camel.',
                                    'moca_camel_provenance': 'JSON mapping provenance for MOCA - Camel: '
                                                             'mapping source, rule ID, ruleset version, '
                                                             'canonical rule ID and match status, question '
                                                             'concept ID and unit concept ID.',
                                    'moca_camel_text': 'Source response text for MOCA - Camel; matches: MOCA '
                                                       '- Camel [MOCA I]',
                                    'moca_category_cue__church': "MOCA - Category Cue option 'Church': true "
                                                                 'when selected, false when the question was '
                                                                 'answered with other options only, null '
                                                                 'when the question was not answered; '
                                                                 'matches: MOCA - Category Cue [MOCA II]',
                                    'moca_category_cue__daisy': "MOCA - Category Cue option 'Daisy': true "
                                                                'when selected, false when the question was '
                                                                'answered with other options only, null when '
                                                                'the question was not answered; matches: '
                                                                'MOCA - Category Cue [MOCA II]',
                                    'moca_category_cue__face': "MOCA - Category Cue option 'Face': true when "
                                                               'selected, false when the question was '
                                                               'answered with other options only, null when '
                                                               'the question was not answered; matches: MOCA '
                                                               '- Category Cue [MOCA II]',
                                    'moca_category_cue__red': "MOCA - Category Cue option 'Red': true when "
                                                              'selected, false when the question was '
                                                              'answered with other options only, null when '
                                                              'the question was not answered; matches: MOCA '
                                                              '- Category Cue [MOCA II]',
                                    'moca_category_cue__velvet': "MOCA - Category Cue option 'Velvet': true "
                                                                 'when selected, false when the question was '
                                                                 'answered with other options only, null '
                                                                 'when the question was not answered; '
                                                                 'matches: MOCA - Category Cue [MOCA II]',
                                    'moca_category_cue_text': 'Pipe-joined selected CODED values for MOCA - '
                                                              'Category Cue, ordered by sequence and '
                                                              'response ID; matches: MOCA - Category Cue '
                                                              '[MOCA II]',
                                    'moca_city': 'MOCA - City — CODED/single selected value; matches: MOCA - '
                                                 'City [MOCA II]',
                                    'moca_city_concept_id': 'Mapped value concept identifier for MOCA - '
                                                            'City.',
                                    'moca_city_provenance': 'JSON mapping provenance for MOCA - City: '
                                                            'mapping source, rule ID, ruleset version, '
                                                            'canonical rule ID and match status, question '
                                                            'concept ID and unit concept ID.',
                                    'moca_city_text': 'Source response text for MOCA - City; matches: MOCA - '
                                                      'City [MOCA II]',
                                    'moca_comments': 'MOCA - Comments — TEXT/single selected value; matches: '
                                                     'MOCA - Comments [MOCA II]',
                                    'moca_comments_concept_id': 'Mapped value concept identifier for MOCA - '
                                                                'Comments.',
                                    'moca_comments_provenance': 'JSON mapping provenance for MOCA - '
                                                                'Comments: mapping source, rule ID, ruleset '
                                                                'version, canonical rule ID and match '
                                                                'status, question concept ID and unit '
                                                                'concept ID.',
                                    'moca_comments_text': 'Source response text for MOCA - Comments; '
                                                          'matches: MOCA - Comments [MOCA II]',
                                    'moca_contour': 'MOCA - Contour — CODED/single selected value; matches: '
                                                    'MOCA - Contour [MOCA I]',
                                    'moca_contour_concept_id': 'Mapped value concept identifier for MOCA - '
                                                               'Contour.',
                                    'moca_contour_provenance': 'JSON mapping provenance for MOCA - Contour: '
                                                               'mapping source, rule ID, ruleset version, '
                                                               'canonical rule ID and match status, question '
                                                               'concept ID and unit concept ID.',
                                    'moca_contour_text': 'Source response text for MOCA - Contour; matches: '
                                                         'MOCA - Contour [MOCA I]',
                                    'moca_copy_cube': 'MOCA - Copy Cube — CODED/single selected value; '
                                                      'matches: MOCA - Copy Cube [MOCA I]',
                                    'moca_copy_cube_concept_id': 'Mapped value concept identifier for MOCA - '
                                                                 'Copy Cube.',
                                    'moca_copy_cube_provenance': 'JSON mapping provenance for MOCA - Copy '
                                                                 'Cube: mapping source, rule ID, ruleset '
                                                                 'version, canonical rule ID and match '
                                                                 'status, question concept ID and unit '
                                                                 'concept ID.',
                                    'moca_copy_cube_text': 'Source response text for MOCA - Copy Cube; '
                                                           'matches: MOCA - Copy Cube [MOCA I]',
                                    'moca_date': 'MOCA - Date — CODED/single selected value; matches: MOCA - '
                                                 'Date [MOCA II]',
                                    'moca_date_concept_id': 'Mapped value concept identifier for MOCA - '
                                                            'Date.',
                                    'moca_date_provenance': 'JSON mapping provenance for MOCA - Date: '
                                                            'mapping source, rule ID, ruleset version, '
                                                            'canonical rule ID and match status, question '
                                                            'concept ID and unit concept ID.',
                                    'moca_date_text': 'Source response text for MOCA - Date; matches: MOCA - '
                                                      'Date [MOCA II]',
                                    'moca_day': 'MOCA - Day — CODED/single selected value; matches: MOCA - '
                                                'Day [MOCA II]',
                                    'moca_day_concept_id': 'Mapped value concept identifier for MOCA - Day.',
                                    'moca_day_provenance': 'JSON mapping provenance for MOCA - Day: mapping '
                                                           'source, rule ID, ruleset version, canonical rule '
                                                           'ID and match status, question concept ID and '
                                                           'unit concept ID.',
                                    'moca_day_text': 'Source response text for MOCA - Day; matches: MOCA - '
                                                     'Day [MOCA II]',
                                    'moca_delayed_recall__church': "MOCA - Delayed Recall option 'Church': "
                                                                   'true when selected, false when the '
                                                                   'question was answered with other options '
                                                                   'only, null when the question was not '
                                                                   'answered; matches: MOCA - Delayed Recall '
                                                                   '[MOCA II]',
                                    'moca_delayed_recall__daisy': "MOCA - Delayed Recall option 'Daisy': "
                                                                  'true when selected, false when the '
                                                                  'question was answered with other options '
                                                                  'only, null when the question was not '
                                                                  'answered; matches: MOCA - Delayed Recall '
                                                                  '[MOCA II]',
                                    'moca_delayed_recall__face': "MOCA - Delayed Recall option 'Face': true "
                                                                 'when selected, false when the question was '
                                                                 'answered with other options only, null '
                                                                 'when the question was not answered; '
                                                                 'matches: MOCA - Delayed Recall [MOCA II]',
                                    'moca_delayed_recall__red': "MOCA - Delayed Recall option 'Red': true "
                                                                'when selected, false when the question was '
                                                                'answered with other options only, null when '
                                                                'the question was not answered; matches: '
                                                                'MOCA - Delayed Recall [MOCA II]',
                                    'moca_delayed_recall__velvet': "MOCA - Delayed Recall option 'Velvet': "
                                                                   'true when selected, false when the '
                                                                   'question was answered with other options '
                                                                   'only, null when the question was not '
                                                                   'answered; matches: MOCA - Delayed Recall '
                                                                   '[MOCA II]',
                                    'moca_delayed_recall_points': 'MOCA - Delayed Recall Points — '
                                                                  'NUMERIC/single selected value; matches: '
                                                                  'MOCA - Delayed Recall Points [MOCA II]',
                                    'moca_delayed_recall_points_concept_id': 'Mapped value concept '
                                                                             'identifier for MOCA - Delayed '
                                                                             'Recall Points.',
                                    'moca_delayed_recall_points_provenance': 'JSON mapping provenance for '
                                                                             'MOCA - Delayed Recall Points: '
                                                                             'mapping source, rule ID, '
                                                                             'ruleset version, canonical '
                                                                             'rule ID and match status, '
                                                                             'question concept ID and unit '
                                                                             'concept ID.',
                                    'moca_delayed_recall_points_text': 'Source response text for MOCA - '
                                                                       'Delayed Recall Points; matches: MOCA '
                                                                       '- Delayed Recall Points [MOCA II]',
                                    'moca_delayed_recall_text': 'Pipe-joined selected CODED values for MOCA '
                                                                '- Delayed Recall, ordered by sequence and '
                                                                'response ID; matches: MOCA - Delayed Recall '
                                                                '[MOCA II]',
                                    'moca_fluency': 'MOCA - Fluency — CODED/single selected value; matches: '
                                                    'MOCA - Fluency [MOCA II]',
                                    'moca_fluency_concept_id': 'Mapped value concept identifier for MOCA - '
                                                               'Fluency.',
                                    'moca_fluency_points': 'MOCA - Fluency Points — NUMERIC/single selected '
                                                           'value; matches: MOCA - Fluency Points [MOCA II]',
                                    'moca_fluency_points_concept_id': 'Mapped value concept identifier for '
                                                                      'MOCA - Fluency Points.',
                                    'moca_fluency_points_provenance': 'JSON mapping provenance for MOCA - '
                                                                      'Fluency Points: mapping source, rule '
                                                                      'ID, ruleset version, canonical rule '
                                                                      'ID and match status, question concept '
                                                                      'ID and unit concept ID.',
                                    'moca_fluency_points_text': 'Source response text for MOCA - Fluency '
                                                                'Points; matches: MOCA - Fluency Points '
                                                                '[MOCA II]',
                                    'moca_fluency_provenance': 'JSON mapping provenance for MOCA - Fluency: '
                                                               'mapping source, rule ID, ruleset version, '
                                                               'canonical rule ID and match status, question '
                                                               'concept ID and unit concept ID.',
                                    'moca_fluency_text': 'Source response text for MOCA - Fluency; matches: '
                                                         'MOCA - Fluency [MOCA II]',
                                    'moca_hands': 'MOCA - Hands — CODED/single selected value; matches: MOCA '
                                                  '- Hands [MOCA I]',
                                    'moca_hands_concept_id': 'Mapped value concept identifier for MOCA - '
                                                             'Hands.',
                                    'moca_hands_provenance': 'JSON mapping provenance for MOCA - Hands: '
                                                             'mapping source, rule ID, ruleset version, '
                                                             'canonical rule ID and match status, question '
                                                             'concept ID and unit concept ID.',
                                    'moca_hands_text': 'Source response text for MOCA - Hands; matches: MOCA '
                                                       '- Hands [MOCA I]',
                                    'moca_lion': 'MOCA - Lion — CODED/single selected value; matches: MOCA - '
                                                 'Lion [MOCA I]',
                                    'moca_lion_concept_id': 'Mapped value concept identifier for MOCA - '
                                                            'Lion.',
                                    'moca_lion_provenance': 'JSON mapping provenance for MOCA - Lion: '
                                                            'mapping source, rule ID, ruleset version, '
                                                            'canonical rule ID and match status, question '
                                                            'concept ID and unit concept ID.',
                                    'moca_lion_text': 'Source response text for MOCA - Lion; matches: MOCA - '
                                                      'Lion [MOCA I]',
                                    'moca_memory_first__church': "MOCA - Memory First option 'Church': true "
                                                                 'when selected, false when the question was '
                                                                 'answered with other options only, null '
                                                                 'when the question was not answered; '
                                                                 'matches: MOCA - Memory First [MOCA I]',
                                    'moca_memory_first__daisy': "MOCA - Memory First option 'Daisy': true "
                                                                'when selected, false when the question was '
                                                                'answered with other options only, null when '
                                                                'the question was not answered; matches: '
                                                                'MOCA - Memory First [MOCA I]',
                                    'moca_memory_first__face': "MOCA - Memory First option 'Face': true when "
                                                               'selected, false when the question was '
                                                               'answered with other options only, null when '
                                                               'the question was not answered; matches: MOCA '
                                                               '- Memory First [MOCA I]',
                                    'moca_memory_first__red': "MOCA - Memory First option 'Red': true when "
                                                              'selected, false when the question was '
                                                              'answered with other options only, null when '
                                                              'the question was not answered; matches: MOCA '
                                                              '- Memory First [MOCA I]',
                                    'moca_memory_first__velvet': "MOCA - Memory First option 'Velvet': true "
                                                                 'when selected, false when the question was '
                                                                 'answered with other options only, null '
                                                                 'when the question was not answered; '
                                                                 'matches: MOCA - Memory First [MOCA I]',
                                    'moca_memory_first_text': 'Pipe-joined selected CODED values for MOCA - '
                                                              'Memory First, ordered by sequence and '
                                                              'response ID; matches: MOCA - Memory First '
                                                              '[MOCA I]',
                                    'moca_memory_second__church': "MOCA - Memory Second option 'Church': "
                                                                  'true when selected, false when the '
                                                                  'question was answered with other options '
                                                                  'only, null when the question was not '
                                                                  'answered; matches: MOCA - Memory Second '
                                                                  '[MOCA I]',
                                    'moca_memory_second__daisy': "MOCA - Memory Second option 'Daisy': true "
                                                                 'when selected, false when the question was '
                                                                 'answered with other options only, null '
                                                                 'when the question was not answered; '
                                                                 'matches: MOCA - Memory Second [MOCA I]',
                                    'moca_memory_second__face': "MOCA - Memory Second option 'Face': true "
                                                                'when selected, false when the question was '
                                                                'answered with other options only, null when '
                                                                'the question was not answered; matches: '
                                                                'MOCA - Memory Second [MOCA I]',
                                    'moca_memory_second__red': "MOCA - Memory Second option 'Red': true when "
                                                               'selected, false when the question was '
                                                               'answered with other options only, null when '
                                                               'the question was not answered; matches: MOCA '
                                                               '- Memory Second [MOCA I]',
                                    'moca_memory_second__velvet': "MOCA - Memory Second option 'Velvet': "
                                                                  'true when selected, false when the '
                                                                  'question was answered with other options '
                                                                  'only, null when the question was not '
                                                                  'answered; matches: MOCA - Memory Second '
                                                                  '[MOCA I]',
                                    'moca_memory_second_text': 'Pipe-joined selected CODED values for MOCA - '
                                                               'Memory Second, ordered by sequence and '
                                                               'response ID; matches: MOCA - Memory Second '
                                                               '[MOCA I]',
                                    'moca_month': 'MOCA - Month — CODED/single selected value; matches: MOCA '
                                                  '- Month [MOCA II]',
                                    'moca_month_concept_id': 'Mapped value concept identifier for MOCA - '
                                                             'Month.',
                                    'moca_month_provenance': 'JSON mapping provenance for MOCA - Month: '
                                                             'mapping source, rule ID, ruleset version, '
                                                             'canonical rule ID and match status, question '
                                                             'concept ID and unit concept ID.',
                                    'moca_month_text': 'Source response text for MOCA - Month; matches: MOCA '
                                                       '- Month [MOCA II]',
                                    'moca_multi_choice_cue__church': 'MOCA - Multi Choice Cue option '
                                                                     "'Church': true when selected, false "
                                                                     'when the question was answered with '
                                                                     'other options only, null when the '
                                                                     'question was not answered; matches: '
                                                                     'MOCA - Multi Choice Cue [MOCA II]',
                                    'moca_multi_choice_cue__daisy': "MOCA - Multi Choice Cue option 'Daisy': "
                                                                    'true when selected, false when the '
                                                                    'question was answered with other '
                                                                    'options only, null when the question '
                                                                    'was not answered; matches: MOCA - Multi '
                                                                    'Choice Cue [MOCA II]',
                                    'moca_multi_choice_cue__face': "MOCA - Multi Choice Cue option 'Face': "
                                                                   'true when selected, false when the '
                                                                   'question was answered with other options '
                                                                   'only, null when the question was not '
                                                                   'answered; matches: MOCA - Multi Choice '
                                                                   'Cue [MOCA II]',
                                    'moca_multi_choice_cue__red': "MOCA - Multi Choice Cue option 'Red': "
                                                                  'true when selected, false when the '
                                                                  'question was answered with other options '
                                                                  'only, null when the question was not '
                                                                  'answered; matches: MOCA - Multi Choice '
                                                                  'Cue [MOCA II]',
                                    'moca_multi_choice_cue__velvet': 'MOCA - Multi Choice Cue option '
                                                                     "'Velvet': true when selected, false "
                                                                     'when the question was answered with '
                                                                     'other options only, null when the '
                                                                     'question was not answered; matches: '
                                                                     'MOCA - Multi Choice Cue [MOCA II]',
                                    'moca_multi_choice_cue_text': 'Pipe-joined selected CODED values for '
                                                                  'MOCA - Multi Choice Cue, ordered by '
                                                                  'sequence and response ID; matches: MOCA - '
                                                                  'Multi Choice Cue [MOCA II]',
                                    'moca_naming_points': 'MOCA - Naming Points — NUMERIC/single selected '
                                                          'value; matches: MOCA - Naming Points [MOCA I]',
                                    'moca_naming_points_concept_id': 'Mapped value concept identifier for '
                                                                     'MOCA - Naming Points.',
                                    'moca_naming_points_provenance': 'JSON mapping provenance for MOCA - '
                                                                     'Naming Points: mapping source, rule '
                                                                     'ID, ruleset version, canonical rule ID '
                                                                     'and match status, question concept ID '
                                                                     'and unit concept ID.',
                                    'moca_naming_points_text': 'Source response text for MOCA - Naming '
                                                               'Points; matches: MOCA - Naming Points [MOCA '
                                                               'I]',
                                    'moca_numbers': 'MOCA - Numbers — CODED/single selected value; matches: '
                                                    'MOCA - Numbers [MOCA I]',
                                    'moca_numbers_concept_id': 'Mapped value concept identifier for MOCA - '
                                                               'Numbers.',
                                    'moca_numbers_provenance': 'JSON mapping provenance for MOCA - Numbers: '
                                                               'mapping source, rule ID, ruleset version, '
                                                               'canonical rule ID and match status, question '
                                                               'concept ID and unit concept ID.',
                                    'moca_numbers_text': 'Source response text for MOCA - Numbers; matches: '
                                                         'MOCA - Numbers [MOCA I]',
                                    'moca_orientation_points': 'MOCA - Orientation Points — NUMERIC/single '
                                                               'selected value; matches: MOCA - Orientation '
                                                               'Points [MOCA II]',
                                    'moca_orientation_points_concept_id': 'Mapped value concept identifier '
                                                                          'for MOCA - Orientation Points.',
                                    'moca_orientation_points_provenance': 'JSON mapping provenance for MOCA '
                                                                          '- Orientation Points: mapping '
                                                                          'source, rule ID, ruleset version, '
                                                                          'canonical rule ID and match '
                                                                          'status, question concept ID and '
                                                                          'unit concept ID.',
                                    'moca_orientation_points_text': 'Source response text for MOCA - '
                                                                    'Orientation Points; matches: MOCA - '
                                                                    'Orientation Points [MOCA II]',
                                    'moca_place': 'MOCA - Place — CODED/single selected value; matches: MOCA '
                                                  '- Place [MOCA II]',
                                    'moca_place_concept_id': 'Mapped value concept identifier for MOCA - '
                                                             'Place.',
                                    'moca_place_provenance': 'JSON mapping provenance for MOCA - Place: '
                                                             'mapping source, rule ID, ruleset version, '
                                                             'canonical rule ID and match status, question '
                                                             'concept ID and unit concept ID.',
                                    'moca_place_text': 'Source response text for MOCA - Place; matches: MOCA '
                                                       '- Place [MOCA II]',
                                    'moca_repeat_first': 'MOCA - Repeat First — CODED/single selected value; '
                                                         'matches: MOCA - Repeat First [MOCA II]',
                                    'moca_repeat_first_concept_id': 'Mapped value concept identifier for '
                                                                    'MOCA - Repeat First.',
                                    'moca_repeat_first_provenance': 'JSON mapping provenance for MOCA - '
                                                                    'Repeat First: mapping source, rule ID, '
                                                                    'ruleset version, canonical rule ID and '
                                                                    'match status, question concept ID and '
                                                                    'unit concept ID.',
                                    'moca_repeat_first_text': 'Source response text for MOCA - Repeat First; '
                                                              'matches: MOCA - Repeat First [MOCA II]',
                                    'moca_repeat_points': 'MOCA - Repeat Points — NUMERIC/single selected '
                                                          'value; matches: MOCA - Repeat Points [MOCA II]',
                                    'moca_repeat_points_concept_id': 'Mapped value concept identifier for '
                                                                     'MOCA - Repeat Points.',
                                    'moca_repeat_points_provenance': 'JSON mapping provenance for MOCA - '
                                                                     'Repeat Points: mapping source, rule '
                                                                     'ID, ruleset version, canonical rule ID '
                                                                     'and match status, question concept ID '
                                                                     'and unit concept ID.',
                                    'moca_repeat_points_text': 'Source response text for MOCA - Repeat '
                                                               'Points; matches: MOCA - Repeat Points [MOCA '
                                                               'II]',
                                    'moca_repeat_second': 'MOCA - Repeat Second — CODED/single selected '
                                                          'value; matches: MOCA - Repeat Second [MOCA II]',
                                    'moca_repeat_second_concept_id': 'Mapped value concept identifier for '
                                                                     'MOCA - Repeat Second.',
                                    'moca_repeat_second_provenance': 'JSON mapping provenance for MOCA - '
                                                                     'Repeat Second: mapping source, rule '
                                                                     'ID, ruleset version, canonical rule ID '
                                                                     'and match status, question concept ID '
                                                                     'and unit concept ID.',
                                    'moca_repeat_second_text': 'Source response text for MOCA - Repeat '
                                                               'Second; matches: MOCA - Repeat Second [MOCA '
                                                               'II]',
                                    'moca_rhinoceros': 'MOCA - Rhinoceros — CODED/single selected value; '
                                                       'matches: MOCA - Rhinoceros [MOCA I]',
                                    'moca_rhinoceros_concept_id': 'Mapped value concept identifier for MOCA '
                                                                  '- Rhinoceros.',
                                    'moca_rhinoceros_provenance': 'JSON mapping provenance for MOCA - '
                                                                  'Rhinoceros: mapping source, rule ID, '
                                                                  'ruleset version, canonical rule ID and '
                                                                  'match status, question concept ID and '
                                                                  'unit concept ID.',
                                    'moca_rhinoceros_text': 'Source response text for MOCA - Rhinoceros; '
                                                            'matches: MOCA - Rhinoceros [MOCA I]',
                                    'moca_total_score': 'MOCA - Total Score — NUMERIC/single selected value; '
                                                        'matches: MOCA - Total Score [MOCA II]',
                                    'moca_total_score_concept_id': 'Mapped value concept identifier for MOCA '
                                                                   '- Total Score.',
                                    'moca_total_score_provenance': 'JSON mapping provenance for MOCA - Total '
                                                                   'Score: mapping source, rule ID, ruleset '
                                                                   'version, canonical rule ID and match '
                                                                   'status, question concept ID and unit '
                                                                   'concept ID.',
                                    'moca_total_score_text': 'Source response text for MOCA - Total Score; '
                                                             'matches: MOCA - Total Score [MOCA II]',
                                    'moca_unable_to_assess': 'MOCA - Unable to Assess — CODED/single '
                                                             'selected value; matches: MOCA - Unable to '
                                                             'Assess [MOCA I]',
                                    'moca_unable_to_assess_concept_id': 'Mapped value concept identifier for '
                                                                        'MOCA - Unable to Assess.',
                                    'moca_unable_to_assess_provenance': 'JSON mapping provenance for MOCA - '
                                                                        'Unable to Assess: mapping source, '
                                                                        'rule ID, ruleset version, canonical '
                                                                        'rule ID and match status, question '
                                                                        'concept ID and unit concept ID.',
                                    'moca_unable_to_assess_text': 'Source response text for MOCA - Unable to '
                                                                  'Assess; matches: MOCA - Unable to Assess '
                                                                  '[MOCA I]',
                                    'moca_visuospatial': 'MOCA - Visuospatial — CODED/single selected value; '
                                                         'matches: MOCA - Visuospatial [MOCA I]',
                                    'moca_visuospatial_concept_id': 'Mapped value concept identifier for '
                                                                    'MOCA - Visuospatial.',
                                    'moca_visuospatial_points': 'MOCA - Visuospatial Points — NUMERIC/single '
                                                                'selected value; matches: MOCA - '
                                                                'Visuospatial Points [MOCA I]',
                                    'moca_visuospatial_points_concept_id': 'Mapped value concept identifier '
                                                                           'for MOCA - Visuospatial Points.',
                                    'moca_visuospatial_points_provenance': 'JSON mapping provenance for MOCA '
                                                                           '- Visuospatial Points: mapping '
                                                                           'source, rule ID, ruleset '
                                                                           'version, canonical rule ID and '
                                                                           'match status, question concept '
                                                                           'ID and unit concept ID.',
                                    'moca_visuospatial_points_text': 'Source response text for MOCA - '
                                                                     'Visuospatial Points; matches: MOCA - '
                                                                     'Visuospatial Points [MOCA I]',
                                    'moca_visuospatial_provenance': 'JSON mapping provenance for MOCA - '
                                                                    'Visuospatial: mapping source, rule ID, '
                                                                    'ruleset version, canonical rule ID and '
                                                                    'match status, question concept ID and '
                                                                    'unit concept ID.',
                                    'moca_visuospatial_text': 'Source response text for MOCA - Visuospatial; '
                                                              'matches: MOCA - Visuospatial [MOCA I]',
                                    'moca_year': 'MOCA - Year — CODED/single selected value; matches: MOCA - '
                                                 'Year [MOCA II]',
                                    'moca_year_concept_id': 'Mapped value concept identifier for MOCA - '
                                                            'Year.',
                                    'moca_year_provenance': 'JSON mapping provenance for MOCA - Year: '
                                                            'mapping source, rule ID, ruleset version, '
                                                            'canonical rule ID and match status, question '
                                                            'concept ID and unit concept ID.',
                                    'moca_year_text': 'Source response text for MOCA - Year; matches: MOCA - '
                                                      'Year [MOCA II]',
                                    'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                       'organization.',
                                    'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                         'cross-feed join key.',
                                    'performed_practitioner_id': 'Millennium personnel PERSON_ID for the '
                                                                 'performer.',
                                    'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                    'record_status': 'Inherited unchanged from the matching clinical_form '
                                                     'parent. Derived PowerForm row status: retracted when '
                                                     'the grouped maximum of '
                                                     'map_powerform_assessment.SOURCE_PRESENT_IND is 0; '
                                                     'otherwise superseded when the grouped maximum of '
                                                     'ASSESSMENT_ACTIVE_IND is 0 or no mill_form_activity '
                                                     'response has ACTIVE_IND = 1; otherwise active. Missing '
                                                     'assessment flags alone do not trigger retraction or '
                                                     'supersession. This is a status label, not the raw '
                                                     'Boolean assessment flag.',
                                    'record_status_effective_from': 'Inherited unchanged from the matching '
                                                                    'clinical_form parent. Earliest '
                                                                    'mill_form_activity.DOCUMENTATION_DT_TM '
                                                                    'for the form instance, carried from '
                                                                    'authored_datetime. This is the form '
                                                                    'documentation start used by the '
                                                                    'research status projection, not an '
                                                                    'independently recorded status-change '
                                                                    'timestamp; null when all contributing '
                                                                    'documentation timestamps are null.',
                                    'record_status_effective_to': 'Inherited unchanged from the matching '
                                                                  'clinical_form parent. For a retracted or '
                                                                  'superseded form, completed_datetime: the '
                                                                  'maximum of '
                                                                  'coalesce(mill_form_activity.LAST_DOCUMENTED_DT_TM, '
                                                                  'mill_form_activity.PERFORMED_DT_TM) '
                                                                  'across its responses. Null for active '
                                                                  'forms or when no contributing completion '
                                                                  'timestamp is available. This is a '
                                                                  'documentation/completion boundary, not a '
                                                                  'Boolean assessment flag or an '
                                                                  'independently recorded status-change '
                                                                  'time.',
                                    'response_row_count': 'Number of response rows grouped into the form.',
                                    'source_code': 'Source form reference identifier.',
                                    'source_coding_system': 'Source form coding system.',
                                    'source_display': 'Source form description.',
                                    'source_feed': 'Registered owning feed.',
                                    'source_update_timestamp': 'Inherited unchanged from the matching '
                                                               'clinical_form parent. Maximum, across the '
                                                               "form's mill_form_activity rows, of the "
                                                               'greatest ADC_UPDT, SOURCE_ACTIVITY_ADC_UPDT, '
                                                               'SOURCE_EVENT_ADC_UPDT and '
                                                               'SOURCE_LABEL_ADC_UPDT. This combines the '
                                                               'contributing activity, event, label and '
                                                               'bronze-ingestion clocks; it is not uniformly '
                                                               'a native clinical-system update time. Null '
                                                               'only when every contributing clock is null.',
                                    'subject_id_system': 'Identifier system used for subject_key.',
                                    'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                   'identifier. Not salted, not secret; a stable join key '
                                                   'across feeds where person_id is unresolved.',
                                    'unmatched_response_row_count': 'Number of canonical unmatched response '
                                                                    'rows.',
                                    'vip_ind': 'VIP indicator when supplied.',
                                    'withheld_identity_ind': 'Withheld identity indicator.'},
                        'table': 'Montreal Cognitive Assessment: one row per form instance with configured '
                                 'questions as columns. Residual responses are in clinical_form_response.'}}
