"""Silver forms.form_audit_c: 1 dataset(s).

- datasets: clinical_form_audit_c
- moved verbatim from: 6_silver_forms:7951

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
    name=_n("journey_clinical.form_audit_c"),
    refresh_policy="auto",
)
def form_audit_c():
    # Build the declared dataset: AUDIT-C alcohol screening (both form eras): one row per form
    # instance with configured questions as columns. Residual responses are in
    # clinical_form_response.
    cfg = FORM_CONFIG_BY_SLUG["audit_c"]
    parent = _instrument_parent(cfg)
    responses = _instrument_responses("audit_c")
    picked = responses.groupBy("dcp_forms_activity_id").agg(
        *_single_aggregations("drink_frequency", "CODED"),
        *_single_aggregations("units_per_drink_day", "CODED"),
        *_single_aggregations("six_plus_units_frequency", "CODED"),
        *_single_aggregations("basic_score", "NUMERIC"),
        *_single_aggregations("scoring_guidance_calc", "NUMERIC"),
        *_multi_aggregations(cfg, "actions_score_5_10", "CODED"),
        *_multi_aggregations(cfg, "actions_score_11_12", "CODED"),
        *_multi_aggregations(cfg, "specialist_referral", "CODED"),
        *_single_aggregations("referral_site_calc", "NUMERIC"),
        *_single_aggregations("contact_number", "TEXT"),
        *_single_aggregations("consent_to_aln", "CODED"),
        *_single_aggregations("unable_to_stop_drinking", "CODED"),
        *_single_aggregations("failed_duties_due_to_drinking", "CODED"),
        *_single_aggregations("morning_drink_frequency", "CODED"),
        *_single_aggregations("guilt_after_drinking_frequency", "CODED"),
        *_single_aggregations("memory_loss_frequency", "CODED"),
        *_single_aggregations("injury_due_to_drinking", "CODED"),
        *_single_aggregations("others_concerned", "CODED"),
        *_single_aggregations("full_audit_score", "NUMERIC"),
    )
    return parent.join(picked, "dcp_forms_activity_id", "left")


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_form_audit_c': {'columns': {'actions_score_11_12__brief_advice_and_or_patient_information': 'AuditC '
                                                                                                       'Basic '
                                                                                                       '11-12 '
                                                                                                       'option '
                                                                                                       "'Brief "
                                                                                                       'advice '
                                                                                                       'and/or '
                                                                                                       'patient '
                                                                                                       'information '
                                                                                                       'leaflet '
                                                                                                       "given': "
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
                                                                                                       'AuditC '
                                                                                                       'Basic '
                                                                                                       '11-12 '
                                                                                                       '[Alcohol '
                                                                                                       'Screening '
                                                                                                       'and '
                                                                                                       'Referral '
                                                                                                       'Form '
                                                                                                       '- '
                                                                                                       'AUDIT-C]; '
                                                                                                       'AuditC '
                                                                                                       'Basic '
                                                                                                       '11-12 '
                                                                                                       '[Audit '
                                                                                                       'C - '
                                                                                                       'Basic]',
                                       'actions_score_11_12__patient_offered_a_specialist_referral': 'AuditC '
                                                                                                     'Basic '
                                                                                                     '11-12 '
                                                                                                     'option '
                                                                                                     "'Patient "
                                                                                                     'offered '
                                                                                                     'a '
                                                                                                     'specialist '
                                                                                                     "referral': "
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
                                                                                                     'AuditC '
                                                                                                     'Basic '
                                                                                                     '11-12 '
                                                                                                     '[Alcohol '
                                                                                                     'Screening '
                                                                                                     'and '
                                                                                                     'Referral '
                                                                                                     'Form - '
                                                                                                     'AUDIT-C]; '
                                                                                                     'AuditC '
                                                                                                     'Basic '
                                                                                                     '11-12 '
                                                                                                     '[Audit '
                                                                                                     'C - '
                                                                                                     'Basic]',
                                       'actions_score_11_12_text': 'Pipe-joined selected CODED values for '
                                                                   'AuditC Basic 11-12, ordered by sequence '
                                                                   'and response ID; matches: AuditC Basic '
                                                                   '11-12 [Alcohol Screening and Referral '
                                                                   'Form - AUDIT-C]; AuditC Basic 11-12 '
                                                                   '[Audit C - Basic]',
                                       'actions_score_5_10__brief_advice_and_or_patient_information': 'AuditC '
                                                                                                      'Basic '
                                                                                                      '5-10 '
                                                                                                      'option '
                                                                                                      "'Brief "
                                                                                                      'advice '
                                                                                                      'and/or '
                                                                                                      'patient '
                                                                                                      'information '
                                                                                                      'leaflet '
                                                                                                      "given': "
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
                                                                                                      'AuditC '
                                                                                                      'Basic '
                                                                                                      '5-10 '
                                                                                                      '[Alcohol '
                                                                                                      'Screening '
                                                                                                      'and '
                                                                                                      'Referral '
                                                                                                      'Form '
                                                                                                      '- '
                                                                                                      'AUDIT-C]; '
                                                                                                      'AuditC '
                                                                                                      'Basic '
                                                                                                      '5-10 '
                                                                                                      '[Audit '
                                                                                                      'C - '
                                                                                                      'Basic]',
                                       'actions_score_5_10__make_referral_based_on_clinical_judgemen': 'AuditC '
                                                                                                       'Basic '
                                                                                                       '5-10 '
                                                                                                       'option '
                                                                                                       "'Make "
                                                                                                       'referral '
                                                                                                       'based '
                                                                                                       'on '
                                                                                                       'clinical '
                                                                                                       "judgement': "
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
                                                                                                       'AuditC '
                                                                                                       'Basic '
                                                                                                       '5-10 '
                                                                                                       '[Alcohol '
                                                                                                       'Screening '
                                                                                                       'and '
                                                                                                       'Referral '
                                                                                                       'Form '
                                                                                                       '- '
                                                                                                       'AUDIT-C]; '
                                                                                                       'AuditC '
                                                                                                       'Basic '
                                                                                                       '5-10 '
                                                                                                       '[Audit '
                                                                                                       'C - '
                                                                                                       'Basic]',
                                       'actions_score_5_10__patient_offered_a_specialist_referral': 'AuditC '
                                                                                                    'Basic '
                                                                                                    '5-10 '
                                                                                                    'option '
                                                                                                    "'Patient "
                                                                                                    'offered '
                                                                                                    'a '
                                                                                                    'specialist '
                                                                                                    "referral': "
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
                                                                                                    'was not '
                                                                                                    'answered; '
                                                                                                    'matches: '
                                                                                                    'AuditC '
                                                                                                    'Basic '
                                                                                                    '5-10 '
                                                                                                    '[Alcohol '
                                                                                                    'Screening '
                                                                                                    'and '
                                                                                                    'Referral '
                                                                                                    'Form - '
                                                                                                    'AUDIT-C]; '
                                                                                                    'AuditC '
                                                                                                    'Basic '
                                                                                                    '5-10 '
                                                                                                    '[Audit '
                                                                                                    'C - '
                                                                                                    'Basic]',
                                       'actions_score_5_10_text': 'Pipe-joined selected CODED values for '
                                                                  'AuditC Basic 5-10, ordered by sequence '
                                                                  'and response ID; matches: AuditC Basic '
                                                                  '5-10 [Alcohol Screening and Referral Form '
                                                                  '- AUDIT-C]; AuditC Basic 5-10 [Audit C - '
                                                                  'Basic]',
                                       'active_response_row_count': 'Number of active response rows.',
                                       'authored_datetime': 'Earliest form documentation timestamp.',
                                       'basic_score': 'AuditC Basic Score — NUMERIC/single selected value; '
                                                      'matches: AuditC Basic Score [Alcohol Screening and '
                                                      'Referral Form - AUDIT-C]; AuditC Basic Score [Audit C '
                                                      '- Basic]',
                                       'basic_score_concept_id': 'Mapped value concept identifier for AuditC '
                                                                 'Basic Score.',
                                       'basic_score_provenance': 'JSON mapping provenance for AuditC Basic '
                                                                 'Score: mapping source, rule ID, ruleset '
                                                                 'version, canonical rule ID and match '
                                                                 'status, question concept ID and unit '
                                                                 'concept ID.',
                                       'basic_score_text': 'Source response text for AuditC Basic Score; '
                                                           'matches: AuditC Basic Score [Alcohol Screening '
                                                           'and Referral Form - AUDIT-C]; AuditC Basic Score '
                                                           '[Audit C - Basic]',
                                       'completed_datetime': 'Latest documentation or performed timestamp.',
                                       'confidentiality_code': 'Security classification when supplied.',
                                       'consent_to_aln': 'AuditC Consent to ALN — CODED/single selected '
                                                         'value; matches: AuditC Consent to ALN [Audit C - '
                                                         'Basic]',
                                       'consent_to_aln_concept_id': 'Mapped value concept identifier for '
                                                                    'AuditC Consent to ALN.',
                                       'consent_to_aln_provenance': 'JSON mapping provenance for AuditC '
                                                                    'Consent to ALN: mapping source, rule '
                                                                    'ID, ruleset version, canonical rule ID '
                                                                    'and match status, question concept ID '
                                                                    'and unit concept ID.',
                                       'consent_to_aln_text': 'Source response text for AuditC Consent to '
                                                              'ALN; matches: AuditC Consent to ALN [Audit C '
                                                              '- Basic]',
                                       'contact_number': 'AuditC Contact Number — TEXT/single selected '
                                                         'value; matches: AuditC Contact Number [Alcohol '
                                                         'Screening and Referral Form - AUDIT-C]; AuditC '
                                                         'Contact Number [Audit C - Basic]; AuditC Contact '
                                                         'Number [Audit C - Positive]',
                                       'contact_number_concept_id': 'Mapped value concept identifier for '
                                                                    'AuditC Contact Number.',
                                       'contact_number_provenance': 'JSON mapping provenance for AuditC '
                                                                    'Contact Number: mapping source, rule '
                                                                    'ID, ruleset version, canonical rule ID '
                                                                    'and match status, question concept ID '
                                                                    'and unit concept ID.',
                                       'contact_number_text': 'Source response text for AuditC Contact '
                                                              'Number; matches: AuditC Contact Number '
                                                              '[Alcohol Screening and Referral Form - '
                                                              'AUDIT-C]; AuditC Contact Number [Audit C - '
                                                              'Basic]; AuditC Contact Number [Audit C - '
                                                              'Positive]',
                                       'dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID; primary '
                                                                'key of this table.',
                                       'drink_frequency': 'AuditC Alcoholic Drink Frequency — CODED/single '
                                                          'selected value; matches: AuditC Alcoholic Drink '
                                                          'Frequency [Alcohol Screening and Referral Form - '
                                                          'AUDIT-C]; AuditC Alcoholic Drink Frequency [Audit '
                                                          'C - Basic]',
                                       'drink_frequency_concept_id': 'Mapped value concept identifier for '
                                                                     'AuditC Alcoholic Drink Frequency.',
                                       'drink_frequency_provenance': 'JSON mapping provenance for AuditC '
                                                                     'Alcoholic Drink Frequency: mapping '
                                                                     'source, rule ID, ruleset version, '
                                                                     'canonical rule ID and match status, '
                                                                     'question concept ID and unit concept '
                                                                     'ID.',
                                       'drink_frequency_text': 'Source response text for AuditC Alcoholic '
                                                               'Drink Frequency; matches: AuditC Alcoholic '
                                                               'Drink Frequency [Alcohol Screening and '
                                                               'Referral Form - AUDIT-C]; AuditC Alcoholic '
                                                               'Drink Frequency [Audit C - Basic]',
                                       'empty_response_row_count': 'Number of retained empty response rows.',
                                       'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                       'spine_encounter.encounter_id.',
                                       'event_datetime': 'First documented timestamp.',
                                       'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                               'time (GMT/BST). Millennium rows take the '
                                                               'bronze *_LOCAL companions of the same source '
                                                               'columns and fallback order; rows from '
                                                               'local-clock sources (LUNA, PACS/DICOM, TFC '
                                                               'LIMS, CCMDS, MSDS, BadgerNet, EndoBase, '
                                                               'SLAM, iWeb, Datix and similar) carry '
                                                               'event_datetime unchanged. event_datetime is '
                                                               'a UTC instant for Millennium rows, so take '
                                                               'local calendar dates and clock times from '
                                                               'this column.',
                                       'event_end_datetime': 'Last documented or performed timestamp.',
                                       'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                   'wall-clock time (GMT/BST), by the same '
                                                                   'rule as event_datetime_local; null where '
                                                                   'event_end_datetime is null.',
                                       'failed_duties_due_to_drinking': 'AuditC Failed to do Because '
                                                                        'Drinking — CODED/single selected '
                                                                        'value; matches: AuditC Failed to do '
                                                                        'Because Drinking [Audit C - '
                                                                        'Positive]',
                                       'failed_duties_due_to_drinking_concept_id': 'Mapped value concept '
                                                                                   'identifier for AuditC '
                                                                                   'Failed to do Because '
                                                                                   'Drinking.',
                                       'failed_duties_due_to_drinking_provenance': 'JSON mapping provenance '
                                                                                   'for AuditC Failed to do '
                                                                                   'Because Drinking: '
                                                                                   'mapping source, rule ID, '
                                                                                   'ruleset version, '
                                                                                   'canonical rule ID and '
                                                                                   'match status, question '
                                                                                   'concept ID and unit '
                                                                                   'concept ID.',
                                       'failed_duties_due_to_drinking_text': 'Source response text for '
                                                                             'AuditC Failed to do Because '
                                                                             'Drinking; matches: AuditC '
                                                                             'Failed to do Because Drinking '
                                                                             '[Audit C - Positive]',
                                       'form_status_code': 'Source form status code.',
                                       'form_status_display': 'Source form status display.',
                                       'form_type_code': 'Source form type code.',
                                       'form_type_display': 'Source form type display.',
                                       'full_audit_score': 'AuditC Total Score — NUMERIC/single selected '
                                                           'value; matches: AuditC Total Score [Audit C - '
                                                           'Positive]',
                                       'full_audit_score_concept_id': 'Mapped value concept identifier for '
                                                                      'AuditC Total Score.',
                                       'full_audit_score_provenance': 'JSON mapping provenance for AuditC '
                                                                      'Total Score: mapping source, rule ID, '
                                                                      'ruleset version, canonical rule ID '
                                                                      'and match status, question concept ID '
                                                                      'and unit concept ID.',
                                       'full_audit_score_text': 'Source response text for AuditC Total '
                                                                'Score; matches: AuditC Total Score [Audit C '
                                                                '- Positive]',
                                       'guilt_after_drinking_frequency': 'AuditC Frequency Guilt Afer '
                                                                         'Drinking — CODED/single selected '
                                                                         'value; matches: AuditC Frequency '
                                                                         'Guilt Afer Drinking [Audit C - '
                                                                         'Positive]',
                                       'guilt_after_drinking_frequency_concept_id': 'Mapped value concept '
                                                                                    'identifier for AuditC '
                                                                                    'Frequency Guilt Afer '
                                                                                    'Drinking.',
                                       'guilt_after_drinking_frequency_provenance': 'JSON mapping provenance '
                                                                                    'for AuditC Frequency '
                                                                                    'Guilt Afer Drinking: '
                                                                                    'mapping source, rule '
                                                                                    'ID, ruleset version, '
                                                                                    'canonical rule ID and '
                                                                                    'match status, question '
                                                                                    'concept ID and unit '
                                                                                    'concept ID.',
                                       'guilt_after_drinking_frequency_text': 'Source response text for '
                                                                              'AuditC Frequency Guilt Afer '
                                                                              'Drinking; matches: AuditC '
                                                                              'Frequency Guilt Afer Drinking '
                                                                              '[Audit C - Positive]',
                                       'injury_due_to_drinking': 'AuditC Injured as Result of Drinking — '
                                                                 'CODED/single selected value; matches: '
                                                                 'AuditC Injured as Result of Drinking '
                                                                 '[Audit C - Positive]',
                                       'injury_due_to_drinking_concept_id': 'Mapped value concept identifier '
                                                                            'for AuditC Injured as Result of '
                                                                            'Drinking.',
                                       'injury_due_to_drinking_provenance': 'JSON mapping provenance for '
                                                                            'AuditC Injured as Result of '
                                                                            'Drinking: mapping source, rule '
                                                                            'ID, ruleset version, canonical '
                                                                            'rule ID and match status, '
                                                                            'question concept ID and unit '
                                                                            'concept ID.',
                                       'injury_due_to_drinking_text': 'Source response text for AuditC '
                                                                      'Injured as Result of Drinking; '
                                                                      'matches: AuditC Injured as Result of '
                                                                      'Drinking [Audit C - Positive]',
                                       'invalid_response_row_count': 'Number of source-classified invalid '
                                                                     'response rows.',
                                       'loaded_at': 'Inherited unchanged from the matching clinical_form '
                                                    'parent. Greatest of the form-level maximum ADC_UPDT '
                                                    'values from mill_form_activity, '
                                                    'map_powerform_assessment_item and '
                                                    'map_powerform_assessment, and maximum Source_ADC_UPDT '
                                                    'from map_mat_vte_assessment. This is the '
                                                    'contributing-source ingestion/provenance clock, not the '
                                                    'Silver refresh time or form event time; null only when '
                                                    'every contributing clock is null.',
                                       'matched_response_row_count': 'Number of canonical matched response '
                                                                     'rows.',
                                       'memory_loss_frequency': 'AuditC Frequency Unable to Remember — '
                                                                'CODED/single selected value; matches: '
                                                                'AuditC Frequency Unable to Remember [Audit '
                                                                'C - Positive]',
                                       'memory_loss_frequency_concept_id': 'Mapped value concept identifier '
                                                                           'for AuditC Frequency Unable to '
                                                                           'Remember.',
                                       'memory_loss_frequency_provenance': 'JSON mapping provenance for '
                                                                           'AuditC Frequency Unable to '
                                                                           'Remember: mapping source, rule '
                                                                           'ID, ruleset version, canonical '
                                                                           'rule ID and match status, '
                                                                           'question concept ID and unit '
                                                                           'concept ID.',
                                       'memory_loss_frequency_text': 'Source response text for AuditC '
                                                                     'Frequency Unable to Remember; matches: '
                                                                     'AuditC Frequency Unable to Remember '
                                                                     '[Audit C - Positive]',
                                       'morning_drink_frequency': 'AuditC Frequency Needed Drink in Morning '
                                                                  '— CODED/single selected value; matches: '
                                                                  'AuditC Frequency Needed Drink in Morning '
                                                                  '[Audit C - Positive]',
                                       'morning_drink_frequency_concept_id': 'Mapped value concept '
                                                                             'identifier for AuditC '
                                                                             'Frequency Needed Drink in '
                                                                             'Morning.',
                                       'morning_drink_frequency_provenance': 'JSON mapping provenance for '
                                                                             'AuditC Frequency Needed Drink '
                                                                             'in Morning: mapping source, '
                                                                             'rule ID, ruleset version, '
                                                                             'canonical rule ID and match '
                                                                             'status, question concept ID '
                                                                             'and unit concept ID.',
                                       'morning_drink_frequency_text': 'Source response text for AuditC '
                                                                       'Frequency Needed Drink in Morning; '
                                                                       'matches: AuditC Frequency Needed '
                                                                       'Drink in Morning [Audit C - '
                                                                       'Positive]',
                                       'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                          'organization.',
                                       'others_concerned': 'AuditC Someone Been Concerned — CODED/single '
                                                           'selected value; matches: AuditC Someone Been '
                                                           'Concerned [Audit C - Positive]',
                                       'others_concerned_concept_id': 'Mapped value concept identifier for '
                                                                      'AuditC Someone Been Concerned.',
                                       'others_concerned_provenance': 'JSON mapping provenance for AuditC '
                                                                      'Someone Been Concerned: mapping '
                                                                      'source, rule ID, ruleset version, '
                                                                      'canonical rule ID and match status, '
                                                                      'question concept ID and unit concept '
                                                                      'ID.',
                                       'others_concerned_text': 'Source response text for AuditC Someone '
                                                                'Been Concerned; matches: AuditC Someone '
                                                                'Been Concerned [Audit C - Positive]',
                                       'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                            'cross-feed join key.',
                                       'performed_practitioner_id': 'Millennium personnel PERSON_ID for the '
                                                                    'performer.',
                                       'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                       'record_status': 'Inherited unchanged from the matching clinical_form '
                                                        'parent. Derived PowerForm row status: retracted '
                                                        'when the grouped maximum of '
                                                        'map_powerform_assessment.SOURCE_PRESENT_IND is 0; '
                                                        'otherwise superseded when the grouped maximum of '
                                                        'ASSESSMENT_ACTIVE_IND is 0 or no mill_form_activity '
                                                        'response has ACTIVE_IND = 1; otherwise active. '
                                                        'Missing assessment flags alone do not trigger '
                                                        'retraction or supersession. This is a status label, '
                                                        'not the raw Boolean assessment flag.',
                                       'record_status_effective_from': 'Inherited unchanged from the '
                                                                       'matching clinical_form parent. '
                                                                       'Earliest '
                                                                       'mill_form_activity.DOCUMENTATION_DT_TM '
                                                                       'for the form instance, carried from '
                                                                       'authored_datetime. This is the form '
                                                                       'documentation start used by the '
                                                                       'research status projection, not an '
                                                                       'independently recorded status-change '
                                                                       'timestamp; null when all '
                                                                       'contributing documentation '
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
                                       'referral_site_calc': 'AuditC Referral Site Cal — NUMERIC/single '
                                                             'selected value; matches: AuditC Referral Site '
                                                             'Cal [Alcohol Screening and Referral Form - '
                                                             'AUDIT-C]; AuditC Referral Site Cal [Audit C - '
                                                             'Basic]',
                                       'referral_site_calc_concept_id': 'Mapped value concept identifier for '
                                                                        'AuditC Referral Site Cal.',
                                       'referral_site_calc_provenance': 'JSON mapping provenance for AuditC '
                                                                        'Referral Site Cal: mapping source, '
                                                                        'rule ID, ruleset version, canonical '
                                                                        'rule ID and match status, question '
                                                                        'concept ID and unit concept ID.',
                                       'referral_site_calc_text': 'Source response text for AuditC Referral '
                                                                  'Site Cal; matches: AuditC Referral Site '
                                                                  'Cal [Alcohol Screening and Referral Form '
                                                                  '- AUDIT-C]; AuditC Referral Site Cal '
                                                                  '[Audit C - Basic]',
                                       'response_row_count': 'Number of response rows grouped into the form.',
                                       'scoring_guidance_calc': 'AuditC Scoring Guidance Calc — '
                                                                'NUMERIC/single selected value; matches: '
                                                                'AuditC Scoring Guidance Calc [Alcohol '
                                                                'Screening and Referral Form - AUDIT-C]; '
                                                                'AuditC Scoring Guidance Calc [Audit C - '
                                                                'Basic]',
                                       'scoring_guidance_calc_concept_id': 'Mapped value concept identifier '
                                                                           'for AuditC Scoring Guidance '
                                                                           'Calc.',
                                       'scoring_guidance_calc_provenance': 'JSON mapping provenance for '
                                                                           'AuditC Scoring Guidance Calc: '
                                                                           'mapping source, rule ID, ruleset '
                                                                           'version, canonical rule ID and '
                                                                           'match status, question concept '
                                                                           'ID and unit concept ID.',
                                       'scoring_guidance_calc_text': 'Source response text for AuditC '
                                                                     'Scoring Guidance Calc; matches: AuditC '
                                                                     'Scoring Guidance Calc [Alcohol '
                                                                     'Screening and Referral Form - '
                                                                     'AUDIT-C]; AuditC Scoring Guidance Calc '
                                                                     '[Audit C - Basic]',
                                       'six_plus_units_frequency': 'AuditC 6 Or 8 More Units On An Occaision '
                                                                   '— CODED/single selected value; matches: '
                                                                   'AuditC 6 Or 8 More Units On An Occaision '
                                                                   '[Alcohol Screening and Referral Form - '
                                                                   'AUDIT-C]; AuditC 6 Or 8 More Units On An '
                                                                   'Occaision [Audit C - Basic]',
                                       'six_plus_units_frequency_concept_id': 'Mapped value concept '
                                                                              'identifier for AuditC 6 Or 8 '
                                                                              'More Units On An Occaision.',
                                       'six_plus_units_frequency_provenance': 'JSON mapping provenance for '
                                                                              'AuditC 6 Or 8 More Units On '
                                                                              'An Occaision: mapping source, '
                                                                              'rule ID, ruleset version, '
                                                                              'canonical rule ID and match '
                                                                              'status, question concept ID '
                                                                              'and unit concept ID.',
                                       'six_plus_units_frequency_text': 'Source response text for AuditC 6 '
                                                                        'Or 8 More Units On An Occaision; '
                                                                        'matches: AuditC 6 Or 8 More Units '
                                                                        'On An Occaision [Alcohol Screening '
                                                                        'and Referral Form - AUDIT-C]; '
                                                                        'AuditC 6 Or 8 More Units On An '
                                                                        'Occaision [Audit C - Basic]',
                                       'source_code': 'Source form reference identifier.',
                                       'source_coding_system': 'Source form coding system.',
                                       'source_display': 'Source form description.',
                                       'source_feed': 'Registered owning feed.',
                                       'source_update_timestamp': 'Inherited unchanged from the matching '
                                                                  'clinical_form parent. Maximum, across the '
                                                                  "form's mill_form_activity rows, of the "
                                                                  'greatest ADC_UPDT, '
                                                                  'SOURCE_ACTIVITY_ADC_UPDT, '
                                                                  'SOURCE_EVENT_ADC_UPDT and '
                                                                  'SOURCE_LABEL_ADC_UPDT. This combines the '
                                                                  'contributing activity, event, label and '
                                                                  'bronze-ingestion clocks; it is not '
                                                                  'uniformly a native clinical-system update '
                                                                  'time. Null only when every contributing '
                                                                  'clock is null.',
                                       'specialist_referral__newham_community_service_cgl': 'AuditC '
                                                                                            'specialist '
                                                                                            'referral option '
                                                                                            "'Newham - "
                                                                                            'Community '
                                                                                            "Service CGL': "
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
                                                                                            'matches: AuditC '
                                                                                            'specialist '
                                                                                            'referral '
                                                                                            '[Alcohol '
                                                                                            'Screening and '
                                                                                            'Referral Form - '
                                                                                            'AUDIT-C]; '
                                                                                            'AuditC '
                                                                                            'specialist '
                                                                                            'referral [Audit '
                                                                                            'C - Basic]',
                                       'specialist_referral__newham_refer_to_onsite_aln': 'AuditC specialist '
                                                                                          'referral option '
                                                                                          "'Newham - refer "
                                                                                          'to onsite ALN / '
                                                                                          'Newham – refer to '
                                                                                          "onsite ALN': true "
                                                                                          'when selected, '
                                                                                          'false when the '
                                                                                          'question was '
                                                                                          'answered with '
                                                                                          'other options '
                                                                                          'only, null when '
                                                                                          'the question was '
                                                                                          'not answered; '
                                                                                          'matches: AuditC '
                                                                                          'specialist '
                                                                                          'referral [Alcohol '
                                                                                          'Screening and '
                                                                                          'Referral Form - '
                                                                                          'AUDIT-C]; AuditC '
                                                                                          'specialist '
                                                                                          'referral [Audit C '
                                                                                          '- Basic]',
                                       'specialist_referral__patient_declined_specialist_referral': 'AuditC '
                                                                                                    'specialist '
                                                                                                    'referral '
                                                                                                    'option '
                                                                                                    "'Patient "
                                                                                                    'declined '
                                                                                                    'specialist '
                                                                                                    "referral': "
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
                                                                                                    'was not '
                                                                                                    'answered; '
                                                                                                    'matches: '
                                                                                                    'AuditC '
                                                                                                    'specialist '
                                                                                                    'referral '
                                                                                                    '[Alcohol '
                                                                                                    'Screening '
                                                                                                    'and '
                                                                                                    'Referral '
                                                                                                    'Form - '
                                                                                                    'AUDIT-C]; '
                                                                                                    'AuditC '
                                                                                                    'specialist '
                                                                                                    'referral '
                                                                                                    '[Audit '
                                                                                                    'C - '
                                                                                                    'Basic]',
                                       'specialist_referral__rlh_refer_to_onsite_aln': 'AuditC specialist '
                                                                                       "referral option 'RLH "
                                                                                       '- refer to onsite '
                                                                                       "ALN': true when "
                                                                                       'selected, false when '
                                                                                       'the question was '
                                                                                       'answered with other '
                                                                                       'options only, null '
                                                                                       'when the question '
                                                                                       'was not answered; '
                                                                                       'matches: AuditC '
                                                                                       'specialist referral '
                                                                                       '[Alcohol Screening '
                                                                                       'and Referral Form - '
                                                                                       'AUDIT-C]; AuditC '
                                                                                       'specialist referral '
                                                                                       '[Audit C - Basic]',
                                       'specialist_referral__tower_hamlets_community_service_reset': 'AuditC '
                                                                                                     'specialist '
                                                                                                     'referral '
                                                                                                     'option '
                                                                                                     "'Tower "
                                                                                                     'Hamlets '
                                                                                                     '- '
                                                                                                     'Community '
                                                                                                     'Service '
                                                                                                     "RESET': "
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
                                                                                                     'AuditC '
                                                                                                     'specialist '
                                                                                                     'referral '
                                                                                                     '[Alcohol '
                                                                                                     'Screening '
                                                                                                     'and '
                                                                                                     'Referral '
                                                                                                     'Form - '
                                                                                                     'AUDIT-C]; '
                                                                                                     'AuditC '
                                                                                                     'specialist '
                                                                                                     'referral '
                                                                                                     '[Audit '
                                                                                                     'C - '
                                                                                                     'Basic]',
                                       'specialist_referral__wx_refer_to_onsite_aln': 'AuditC specialist '
                                                                                      "referral option 'WX - "
                                                                                      "refer to onsite ALN': "
                                                                                      'true when selected, '
                                                                                      'false when the '
                                                                                      'question was answered '
                                                                                      'with other options '
                                                                                      'only, null when the '
                                                                                      'question was not '
                                                                                      'answered; matches: '
                                                                                      'AuditC specialist '
                                                                                      'referral [Alcohol '
                                                                                      'Screening and '
                                                                                      'Referral Form - '
                                                                                      'AUDIT-C]; AuditC '
                                                                                      'specialist referral '
                                                                                      '[Audit C - Basic]',
                                       'specialist_referral__wx_refer_to_onsite_aln_psych_liaison_tea': 'AuditC '
                                                                                                        'specialist '
                                                                                                        'referral '
                                                                                                        'option '
                                                                                                        "'WX "
                                                                                                        '- '
                                                                                                        'refer '
                                                                                                        'to '
                                                                                                        'onsite '
                                                                                                        'ALN '
                                                                                                        '& '
                                                                                                        'psych '
                                                                                                        'liaison '
                                                                                                        "team': "
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
                                                                                                        'AuditC '
                                                                                                        'specialist '
                                                                                                        'referral '
                                                                                                        '[Alcohol '
                                                                                                        'Screening '
                                                                                                        'and '
                                                                                                        'Referral '
                                                                                                        'Form '
                                                                                                        '- '
                                                                                                        'AUDIT-C]; '
                                                                                                        'AuditC '
                                                                                                        'specialist '
                                                                                                        'referral '
                                                                                                        '[Audit '
                                                                                                        'C - '
                                                                                                        'Basic]',
                                       'specialist_referral_text': 'Pipe-joined selected CODED values for '
                                                                   'AuditC specialist referral, ordered by '
                                                                   'sequence and response ID; matches: '
                                                                   'AuditC specialist referral [Alcohol '
                                                                   'Screening and Referral Form - AUDIT-C]; '
                                                                   'AuditC specialist referral [Audit C - '
                                                                   'Basic]',
                                       'subject_id_system': 'Identifier system used for subject_key.',
                                       'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                      'identifier. Not salted, not secret; a stable join key '
                                                      'across feeds where person_id is unresolved.',
                                       'unable_to_stop_drinking': 'AuditC Not Able to Stop Drinking — '
                                                                  'CODED/single selected value; matches: '
                                                                  'AuditC Not Able to Stop Drinking [Audit C '
                                                                  '- Positive]',
                                       'unable_to_stop_drinking_concept_id': 'Mapped value concept '
                                                                             'identifier for AuditC Not Able '
                                                                             'to Stop Drinking.',
                                       'unable_to_stop_drinking_provenance': 'JSON mapping provenance for '
                                                                             'AuditC Not Able to Stop '
                                                                             'Drinking: mapping source, rule '
                                                                             'ID, ruleset version, canonical '
                                                                             'rule ID and match status, '
                                                                             'question concept ID and unit '
                                                                             'concept ID.',
                                       'unable_to_stop_drinking_text': 'Source response text for AuditC Not '
                                                                       'Able to Stop Drinking; matches: '
                                                                       'AuditC Not Able to Stop Drinking '
                                                                       '[Audit C - Positive]',
                                       'units_per_drink_day': 'AuditC Units Per Drink Day — CODED/single '
                                                              'selected value; matches: AuditC Units Per '
                                                              'Drink Day [Alcohol Screening and Referral '
                                                              'Form - AUDIT-C]; AuditC Units Per Drink Day '
                                                              '[Audit C - Basic]',
                                       'units_per_drink_day_concept_id': 'Mapped value concept identifier '
                                                                         'for AuditC Units Per Drink Day.',
                                       'units_per_drink_day_provenance': 'JSON mapping provenance for AuditC '
                                                                         'Units Per Drink Day: mapping '
                                                                         'source, rule ID, ruleset version, '
                                                                         'canonical rule ID and match '
                                                                         'status, question concept ID and '
                                                                         'unit concept ID.',
                                       'units_per_drink_day_text': 'Source response text for AuditC Units '
                                                                   'Per Drink Day; matches: AuditC Units Per '
                                                                   'Drink Day [Alcohol Screening and '
                                                                   'Referral Form - AUDIT-C]; AuditC Units '
                                                                   'Per Drink Day [Audit C - Basic]',
                                       'unmatched_response_row_count': 'Number of canonical unmatched '
                                                                       'response rows.',
                                       'vip_ind': 'VIP indicator when supplied.',
                                       'withheld_identity_ind': 'Withheld identity indicator.'},
                           'table': 'AUDIT-C alcohol screening (both form eras): one row per form instance '
                                    'with configured questions as columns. Residual responses are in '
                                    'clinical_form_response.'}}
