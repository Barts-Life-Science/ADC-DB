"""Silver forms.form_maternity_co_smoking: 1 dataset(s).

- datasets: clinical_form_maternity_co_smoking
- moved verbatim from: 6_silver_forms:8951

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_tables.forms._shared import (
    FORM_CONFIG_BY_SLUG,
    _instrument_parent,
    _instrument_responses,
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
    name=_n("journey_clinical.form_maternity_co_smoking"),
    refresh_policy="auto",
)
def form_maternity_co_smoking():
    # Build the declared dataset: Maternity carbon monoxide and smoking assessment: one row per
    # form instance with configured questions as columns. Residual responses are in
    # clinical_form_response.
    cfg = FORM_CONFIG_BY_SLUG["maternity_co_smoking"]
    parent = _instrument_parent(cfg)
    responses = _instrument_responses("maternity_co_smoking")
    picked = responses.groupBy("dcp_forms_activity_id").agg(
        *_single_aggregations("gestation_days", "NUMERIC"),
        *_single_aggregations("gestation_weeks", "NUMERIC"),
        *_single_aggregations("date", "DATE"),
        *_single_aggregations("carbon_monoxide_reading_performed", "CODED"),
        *_single_aggregations("mco_smoking_status_mother", "CODED"),
        *_single_aggregations("carbon_monoxide_reading", "NUMERIC"),
        *_single_aggregations("sc_referral_to_stop_smoking_service", "CODED"),
        *_single_aggregations("sc_very_brief_advice_given", "CODED"),
        *_single_aggregations("stop_smoking_service_referral_required", "CODED"),
        *_single_aggregations("stop_smoking_support_received_this_preg", "CODED"),
        *_single_aggregations("comments", "TEXT"),
        *_single_aggregations("reason_carbon_monoxide_reading_not_performed", "CODED"),
        F.max(F.col("pregnancy_id")).alias("pregnancy_id"),
        F.max(F.col("pregnancy_match_method")).alias("pregnancy_match_method"),
    )
    return parent.join(picked, "dcp_forms_activity_id", "left")


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_form_maternity_co_smoking': {'columns': {'active_response_row_count': 'Number of active response '
                                                                                 'rows.',
                                                    'authored_datetime': 'Earliest form documentation '
                                                                         'timestamp.',
                                                    'carbon_monoxide_reading': 'Carbon Monoxide Reading — '
                                                                               'NUMERIC/single selected '
                                                                               'value; matches: Carbon '
                                                                               'Monoxide Reading [MATERNITY '
                                                                               'CARBON MONOXIDE / SMOKING]',
                                                    'carbon_monoxide_reading_concept_id': 'Mapped value '
                                                                                          'concept '
                                                                                          'identifier for '
                                                                                          'Carbon Monoxide '
                                                                                          'Reading.',
                                                    'carbon_monoxide_reading_performed': 'Carbon Monoxide '
                                                                                         'Reading Performed '
                                                                                         '— CODED/single '
                                                                                         'selected value; '
                                                                                         'matches: Carbon '
                                                                                         'Monoxide Reading '
                                                                                         'Performed '
                                                                                         '[MATERNITY CARBON '
                                                                                         'MONOXIDE / '
                                                                                         'SMOKING]',
                                                    'carbon_monoxide_reading_performed_concept_id': 'Mapped '
                                                                                                    'value '
                                                                                                    'concept '
                                                                                                    'identifier '
                                                                                                    'for '
                                                                                                    'Carbon '
                                                                                                    'Monoxide '
                                                                                                    'Reading '
                                                                                                    'Performed.',
                                                    'carbon_monoxide_reading_performed_provenance': 'JSON '
                                                                                                    'mapping '
                                                                                                    'provenance '
                                                                                                    'for '
                                                                                                    'Carbon '
                                                                                                    'Monoxide '
                                                                                                    'Reading '
                                                                                                    'Performed: '
                                                                                                    'mapping '
                                                                                                    'source, '
                                                                                                    'rule '
                                                                                                    'ID, '
                                                                                                    'ruleset '
                                                                                                    'version, '
                                                                                                    'canonical '
                                                                                                    'rule ID '
                                                                                                    'and '
                                                                                                    'match '
                                                                                                    'status, '
                                                                                                    'question '
                                                                                                    'concept '
                                                                                                    'ID and '
                                                                                                    'unit '
                                                                                                    'concept '
                                                                                                    'ID.',
                                                    'carbon_monoxide_reading_performed_text': 'Source '
                                                                                              'response text '
                                                                                              'for Carbon '
                                                                                              'Monoxide '
                                                                                              'Reading '
                                                                                              'Performed; '
                                                                                              'matches: '
                                                                                              'Carbon '
                                                                                              'Monoxide '
                                                                                              'Reading '
                                                                                              'Performed '
                                                                                              '[MATERNITY '
                                                                                              'CARBON '
                                                                                              'MONOXIDE / '
                                                                                              'SMOKING]',
                                                    'carbon_monoxide_reading_provenance': 'JSON mapping '
                                                                                          'provenance for '
                                                                                          'Carbon Monoxide '
                                                                                          'Reading: mapping '
                                                                                          'source, rule ID, '
                                                                                          'ruleset version, '
                                                                                          'canonical rule ID '
                                                                                          'and match status, '
                                                                                          'question concept '
                                                                                          'ID and unit '
                                                                                          'concept ID.',
                                                    'carbon_monoxide_reading_text': 'Source response text '
                                                                                    'for Carbon Monoxide '
                                                                                    'Reading; matches: '
                                                                                    'Carbon Monoxide Reading '
                                                                                    '[MATERNITY CARBON '
                                                                                    'MONOXIDE / SMOKING]',
                                                    'comments': 'Comments — TEXT/single selected value; '
                                                                'matches: Comments [MATERNITY CARBON '
                                                                'MONOXIDE / SMOKING]',
                                                    'comments_concept_id': 'Mapped value concept identifier '
                                                                           'for Comments.',
                                                    'comments_provenance': 'JSON mapping provenance for '
                                                                           'Comments: mapping source, rule '
                                                                           'ID, ruleset version, canonical '
                                                                           'rule ID and match status, '
                                                                           'question concept ID and unit '
                                                                           'concept ID.',
                                                    'comments_text': 'Source response text for Comments; '
                                                                     'matches: Comments [MATERNITY CARBON '
                                                                     'MONOXIDE / SMOKING]',
                                                    'completed_datetime': 'Latest documentation or performed '
                                                                          'timestamp.',
                                                    'confidentiality_code': 'Security classification when '
                                                                            'supplied.',
                                                    'date': 'Date — DATE/single selected value; matches: '
                                                            'Date [MATERNITY CARBON MONOXIDE / SMOKING]',
                                                    'date_concept_id': 'Mapped value concept identifier for '
                                                                       'Date.',
                                                    'date_provenance': 'JSON mapping provenance for Date: '
                                                                       'mapping source, rule ID, ruleset '
                                                                       'version, canonical rule ID and match '
                                                                       'status, question concept ID and unit '
                                                                       'concept ID.',
                                                    'date_text': 'Source response text for Date; matches: '
                                                                 'Date [MATERNITY CARBON MONOXIDE / SMOKING]',
                                                    'dcp_forms_activity_id': 'Millennium '
                                                                             'DCP_FORMS_ACTIVITY_ID; primary '
                                                                             'key of this table.',
                                                    'empty_response_row_count': 'Number of retained empty '
                                                                                'response rows.',
                                                    'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                                    'spine_encounter.encounter_id.',
                                                    'event_datetime': 'First documented timestamp.',
                                                    'event_datetime_local': 'event_datetime in Europe/London '
                                                                            'wall-clock time (GMT/BST). '
                                                                            'Millennium rows take the bronze '
                                                                            '*_LOCAL companions of the same '
                                                                            'source columns and fallback '
                                                                            'order; rows from local-clock '
                                                                            'sources (LUNA, PACS/DICOM, TFC '
                                                                            'LIMS, CCMDS, MSDS, BadgerNet, '
                                                                            'EndoBase, SLAM, iWeb, Datix and '
                                                                            'similar) carry event_datetime '
                                                                            'unchanged. event_datetime is a '
                                                                            'UTC instant for Millennium '
                                                                            'rows, so take local calendar '
                                                                            'dates and clock times from this '
                                                                            'column.',
                                                    'event_end_datetime': 'Last documented or performed '
                                                                          'timestamp.',
                                                    'event_end_datetime_local': 'event_end_datetime in '
                                                                                'Europe/London wall-clock '
                                                                                'time (GMT/BST), by the same '
                                                                                'rule as '
                                                                                'event_datetime_local; null '
                                                                                'where event_end_datetime is '
                                                                                'null.',
                                                    'form_status_code': 'Source form status code.',
                                                    'form_status_display': 'Source form status display.',
                                                    'form_type_code': 'Source form type code.',
                                                    'form_type_display': 'Source form type display.',
                                                    'gestation_days': 'Gestation days — NUMERIC/single '
                                                                      'selected value; matches: Gestation '
                                                                      'days [MATERNITY CARBON MONOXIDE / '
                                                                      'SMOKING]',
                                                    'gestation_days_concept_id': 'Mapped value concept '
                                                                                 'identifier for Gestation '
                                                                                 'days.',
                                                    'gestation_days_provenance': 'JSON mapping provenance '
                                                                                 'for Gestation days: '
                                                                                 'mapping source, rule ID, '
                                                                                 'ruleset version, canonical '
                                                                                 'rule ID and match status, '
                                                                                 'question concept ID and '
                                                                                 'unit concept ID.',
                                                    'gestation_days_text': 'Source response text for '
                                                                           'Gestation days; matches: '
                                                                           'Gestation days [MATERNITY CARBON '
                                                                           'MONOXIDE / SMOKING]',
                                                    'gestation_weeks': 'Gestation weeks — NUMERIC/single '
                                                                       'selected value; matches: Gestation '
                                                                       'weeks [MATERNITY CARBON MONOXIDE / '
                                                                       'SMOKING]',
                                                    'gestation_weeks_concept_id': 'Mapped value concept '
                                                                                  'identifier for Gestation '
                                                                                  'weeks.',
                                                    'gestation_weeks_provenance': 'JSON mapping provenance '
                                                                                  'for Gestation weeks: '
                                                                                  'mapping source, rule ID, '
                                                                                  'ruleset version, '
                                                                                  'canonical rule ID and '
                                                                                  'match status, question '
                                                                                  'concept ID and unit '
                                                                                  'concept ID.',
                                                    'gestation_weeks_text': 'Source response text for '
                                                                            'Gestation weeks; matches: '
                                                                            'Gestation weeks [MATERNITY '
                                                                            'CARBON MONOXIDE / SMOKING]',
                                                    'invalid_response_row_count': 'Number of '
                                                                                  'source-classified invalid '
                                                                                  'response rows.',
                                                    'loaded_at': 'Inherited unchanged from the matching '
                                                                 'clinical_form parent. Greatest of the '
                                                                 'form-level maximum ADC_UPDT values from '
                                                                 'mill_form_activity, '
                                                                 'map_powerform_assessment_item and '
                                                                 'map_powerform_assessment, and maximum '
                                                                 'Source_ADC_UPDT from '
                                                                 'map_mat_vte_assessment. This is the '
                                                                 'contributing-source ingestion/provenance '
                                                                 'clock, not the Silver refresh time or form '
                                                                 'event time; null only when every '
                                                                 'contributing clock is null.',
                                                    'matched_response_row_count': 'Number of canonical '
                                                                                  'matched response rows.',
                                                    'mco_smoking_status_mother': 'MCO Smoking Status Mother '
                                                                                 '— CODED/single selected '
                                                                                 'value; matches: MCO '
                                                                                 'Smoking Status Mother '
                                                                                 '[MATERNITY CARBON MONOXIDE '
                                                                                 '/ SMOKING]',
                                                    'mco_smoking_status_mother_concept_id': 'Mapped value '
                                                                                            'concept '
                                                                                            'identifier for '
                                                                                            'MCO Smoking '
                                                                                            'Status Mother.',
                                                    'mco_smoking_status_mother_provenance': 'JSON mapping '
                                                                                            'provenance for '
                                                                                            'MCO Smoking '
                                                                                            'Status Mother: '
                                                                                            'mapping source, '
                                                                                            'rule ID, '
                                                                                            'ruleset '
                                                                                            'version, '
                                                                                            'canonical rule '
                                                                                            'ID and match '
                                                                                            'status, '
                                                                                            'question '
                                                                                            'concept ID and '
                                                                                            'unit concept '
                                                                                            'ID.',
                                                    'mco_smoking_status_mother_text': 'Source response text '
                                                                                      'for MCO Smoking '
                                                                                      'Status Mother; '
                                                                                      'matches: MCO Smoking '
                                                                                      'Status Mother '
                                                                                      '[MATERNITY CARBON '
                                                                                      'MONOXIDE / SMOKING]',
                                                    'organization_id': 'Millennium ORGANIZATION_ID for the '
                                                                       'source organization.',
                                                    'patient_event_key': 'Deterministic SHA-256 event '
                                                                         'identity; stable cross-feed join '
                                                                         'key.',
                                                    'performed_practitioner_id': 'Millennium personnel '
                                                                                 'PERSON_ID for the '
                                                                                 'performer.',
                                                    'person_id': 'Millennium PERSON_ID; native person '
                                                                 'foreign key.',
                                                    'pregnancy_id': 'Pregnancy episode linked on the source '
                                                                    'form responses.',
                                                    'pregnancy_match_method': 'Method used to link the '
                                                                              'source form response to '
                                                                              'pregnancy.',
                                                    'reason_carbon_monoxide_reading_not_performed': 'Reason '
                                                                                                    'Carbon '
                                                                                                    'Monoxide '
                                                                                                    'reading '
                                                                                                    'not '
                                                                                                    'performed '
                                                                                                    '— '
                                                                                                    'CODED/single '
                                                                                                    'selected '
                                                                                                    'value; '
                                                                                                    'matches: '
                                                                                                    'Reason '
                                                                                                    'Carbon '
                                                                                                    'Monoxide '
                                                                                                    'reading '
                                                                                                    'not '
                                                                                                    'performed '
                                                                                                    '[MATERNITY '
                                                                                                    'CARBON '
                                                                                                    'MONOXIDE '
                                                                                                    '/ '
                                                                                                    'SMOKING]',
                                                    'reason_carbon_monoxide_reading_not_performed_concept_id': 'Mapped '
                                                                                                               'value '
                                                                                                               'concept '
                                                                                                               'identifier '
                                                                                                               'for '
                                                                                                               'Reason '
                                                                                                               'Carbon '
                                                                                                               'Monoxide '
                                                                                                               'reading '
                                                                                                               'not '
                                                                                                               'performed.',
                                                    'reason_carbon_monoxide_reading_not_performed_provenance': 'JSON '
                                                                                                               'mapping '
                                                                                                               'provenance '
                                                                                                               'for '
                                                                                                               'Reason '
                                                                                                               'Carbon '
                                                                                                               'Monoxide '
                                                                                                               'reading '
                                                                                                               'not '
                                                                                                               'performed: '
                                                                                                               'mapping '
                                                                                                               'source, '
                                                                                                               'rule '
                                                                                                               'ID, '
                                                                                                               'ruleset '
                                                                                                               'version, '
                                                                                                               'canonical '
                                                                                                               'rule '
                                                                                                               'ID '
                                                                                                               'and '
                                                                                                               'match '
                                                                                                               'status, '
                                                                                                               'question '
                                                                                                               'concept '
                                                                                                               'ID '
                                                                                                               'and '
                                                                                                               'unit '
                                                                                                               'concept '
                                                                                                               'ID.',
                                                    'reason_carbon_monoxide_reading_not_performed_text': 'Source '
                                                                                                         'response '
                                                                                                         'text '
                                                                                                         'for '
                                                                                                         'Reason '
                                                                                                         'Carbon '
                                                                                                         'Monoxide '
                                                                                                         'reading '
                                                                                                         'not '
                                                                                                         'performed; '
                                                                                                         'matches: '
                                                                                                         'Reason '
                                                                                                         'Carbon '
                                                                                                         'Monoxide '
                                                                                                         'reading '
                                                                                                         'not '
                                                                                                         'performed '
                                                                                                         '[MATERNITY '
                                                                                                         'CARBON '
                                                                                                         'MONOXIDE '
                                                                                                         '/ '
                                                                                                         'SMOKING]',
                                                    'record_status': 'Inherited unchanged from the matching '
                                                                     'clinical_form parent. Derived '
                                                                     'PowerForm row status: retracted when '
                                                                     'the grouped maximum of '
                                                                     'map_powerform_assessment.SOURCE_PRESENT_IND '
                                                                     'is 0; otherwise superseded when the '
                                                                     'grouped maximum of '
                                                                     'ASSESSMENT_ACTIVE_IND is 0 or no '
                                                                     'mill_form_activity response has '
                                                                     'ACTIVE_IND = 1; otherwise active. '
                                                                     'Missing assessment flags alone do not '
                                                                     'trigger retraction or supersession. '
                                                                     'This is a status label, not the raw '
                                                                     'Boolean assessment flag.',
                                                    'record_status_effective_from': 'Inherited unchanged '
                                                                                    'from the matching '
                                                                                    'clinical_form parent. '
                                                                                    'Earliest '
                                                                                    'mill_form_activity.DOCUMENTATION_DT_TM '
                                                                                    'for the form instance, '
                                                                                    'carried from '
                                                                                    'authored_datetime. This '
                                                                                    'is the form '
                                                                                    'documentation start '
                                                                                    'used by the research '
                                                                                    'status projection, not '
                                                                                    'an independently '
                                                                                    'recorded status-change '
                                                                                    'timestamp; null when '
                                                                                    'all contributing '
                                                                                    'documentation '
                                                                                    'timestamps are null.',
                                                    'record_status_effective_to': 'Inherited unchanged from '
                                                                                  'the matching '
                                                                                  'clinical_form parent. For '
                                                                                  'a retracted or superseded '
                                                                                  'form, completed_datetime: '
                                                                                  'the maximum of '
                                                                                  'coalesce(mill_form_activity.LAST_DOCUMENTED_DT_TM, '
                                                                                  'mill_form_activity.PERFORMED_DT_TM) '
                                                                                  'across its responses. '
                                                                                  'Null for active forms or '
                                                                                  'when no contributing '
                                                                                  'completion timestamp is '
                                                                                  'available. This is a '
                                                                                  'documentation/completion '
                                                                                  'boundary, not a Boolean '
                                                                                  'assessment flag or an '
                                                                                  'independently recorded '
                                                                                  'status-change time.',
                                                    'response_row_count': 'Number of response rows grouped '
                                                                          'into the form.',
                                                    'sc_referral_to_stop_smoking_service': 'SC Referral to '
                                                                                           'Stop Smoking '
                                                                                           'Service — '
                                                                                           'CODED/single '
                                                                                           'selected value; '
                                                                                           'matches: SC '
                                                                                           'Referral to Stop '
                                                                                           'Smoking Service '
                                                                                           '[MATERNITY '
                                                                                           'CARBON MONOXIDE '
                                                                                           '/ SMOKING]',
                                                    'sc_referral_to_stop_smoking_service_concept_id': 'Mapped '
                                                                                                      'value '
                                                                                                      'concept '
                                                                                                      'identifier '
                                                                                                      'for '
                                                                                                      'SC '
                                                                                                      'Referral '
                                                                                                      'to '
                                                                                                      'Stop '
                                                                                                      'Smoking '
                                                                                                      'Service.',
                                                    'sc_referral_to_stop_smoking_service_provenance': 'JSON '
                                                                                                      'mapping '
                                                                                                      'provenance '
                                                                                                      'for '
                                                                                                      'SC '
                                                                                                      'Referral '
                                                                                                      'to '
                                                                                                      'Stop '
                                                                                                      'Smoking '
                                                                                                      'Service: '
                                                                                                      'mapping '
                                                                                                      'source, '
                                                                                                      'rule '
                                                                                                      'ID, '
                                                                                                      'ruleset '
                                                                                                      'version, '
                                                                                                      'canonical '
                                                                                                      'rule '
                                                                                                      'ID '
                                                                                                      'and '
                                                                                                      'match '
                                                                                                      'status, '
                                                                                                      'question '
                                                                                                      'concept '
                                                                                                      'ID '
                                                                                                      'and '
                                                                                                      'unit '
                                                                                                      'concept '
                                                                                                      'ID.',
                                                    'sc_referral_to_stop_smoking_service_text': 'Source '
                                                                                                'response '
                                                                                                'text for SC '
                                                                                                'Referral to '
                                                                                                'Stop '
                                                                                                'Smoking '
                                                                                                'Service; '
                                                                                                'matches: SC '
                                                                                                'Referral to '
                                                                                                'Stop '
                                                                                                'Smoking '
                                                                                                'Service '
                                                                                                '[MATERNITY '
                                                                                                'CARBON '
                                                                                                'MONOXIDE / '
                                                                                                'SMOKING]',
                                                    'sc_very_brief_advice_given': 'SC Very brief advice '
                                                                                  'given — CODED/single '
                                                                                  'selected value; matches: '
                                                                                  'SC Very brief advice '
                                                                                  'given [MATERNITY CARBON '
                                                                                  'MONOXIDE / SMOKING]',
                                                    'sc_very_brief_advice_given_concept_id': 'Mapped value '
                                                                                             'concept '
                                                                                             'identifier for '
                                                                                             'SC Very brief '
                                                                                             'advice given.',
                                                    'sc_very_brief_advice_given_provenance': 'JSON mapping '
                                                                                             'provenance for '
                                                                                             'SC Very brief '
                                                                                             'advice given: '
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
                                                    'sc_very_brief_advice_given_text': 'Source response text '
                                                                                       'for SC Very brief '
                                                                                       'advice given; '
                                                                                       'matches: SC Very '
                                                                                       'brief advice given '
                                                                                       '[MATERNITY CARBON '
                                                                                       'MONOXIDE / SMOKING]',
                                                    'source_code': 'Source form reference identifier.',
                                                    'source_coding_system': 'Source form coding system.',
                                                    'source_display': 'Source form description.',
                                                    'source_feed': 'Registered owning feed.',
                                                    'source_update_timestamp': 'Inherited unchanged from the '
                                                                               'matching clinical_form '
                                                                               'parent. Maximum, across the '
                                                                               "form's mill_form_activity "
                                                                               'rows, of the greatest '
                                                                               'ADC_UPDT, '
                                                                               'SOURCE_ACTIVITY_ADC_UPDT, '
                                                                               'SOURCE_EVENT_ADC_UPDT and '
                                                                               'SOURCE_LABEL_ADC_UPDT. This '
                                                                               'combines the contributing '
                                                                               'activity, event, label and '
                                                                               'bronze-ingestion clocks; it '
                                                                               'is not uniformly a native '
                                                                               'clinical-system update time. '
                                                                               'Null only when every '
                                                                               'contributing clock is null.',
                                                    'stop_smoking_service_referral_required': 'Stop Smoking '
                                                                                              'Service  '
                                                                                              'Referral '
                                                                                              'Required — '
                                                                                              'CODED/single '
                                                                                              'selected '
                                                                                              'value; '
                                                                                              'matches: Stop '
                                                                                              'Smoking '
                                                                                              'Service  '
                                                                                              'Referral '
                                                                                              'Required '
                                                                                              '[MATERNITY '
                                                                                              'CARBON '
                                                                                              'MONOXIDE / '
                                                                                              'SMOKING]',
                                                    'stop_smoking_service_referral_required_concept_id': 'Mapped '
                                                                                                         'value '
                                                                                                         'concept '
                                                                                                         'identifier '
                                                                                                         'for '
                                                                                                         'Stop '
                                                                                                         'Smoking '
                                                                                                         'Service  '
                                                                                                         'Referral '
                                                                                                         'Required.',
                                                    'stop_smoking_service_referral_required_provenance': 'JSON '
                                                                                                         'mapping '
                                                                                                         'provenance '
                                                                                                         'for '
                                                                                                         'Stop '
                                                                                                         'Smoking '
                                                                                                         'Service  '
                                                                                                         'Referral '
                                                                                                         'Required: '
                                                                                                         'mapping '
                                                                                                         'source, '
                                                                                                         'rule '
                                                                                                         'ID, '
                                                                                                         'ruleset '
                                                                                                         'version, '
                                                                                                         'canonical '
                                                                                                         'rule '
                                                                                                         'ID '
                                                                                                         'and '
                                                                                                         'match '
                                                                                                         'status, '
                                                                                                         'question '
                                                                                                         'concept '
                                                                                                         'ID '
                                                                                                         'and '
                                                                                                         'unit '
                                                                                                         'concept '
                                                                                                         'ID.',
                                                    'stop_smoking_service_referral_required_text': 'Source '
                                                                                                   'response '
                                                                                                   'text for '
                                                                                                   'Stop '
                                                                                                   'Smoking '
                                                                                                   'Service  '
                                                                                                   'Referral '
                                                                                                   'Required; '
                                                                                                   'matches: '
                                                                                                   'Stop '
                                                                                                   'Smoking '
                                                                                                   'Service  '
                                                                                                   'Referral '
                                                                                                   'Required '
                                                                                                   '[MATERNITY '
                                                                                                   'CARBON '
                                                                                                   'MONOXIDE '
                                                                                                   '/ '
                                                                                                   'SMOKING]',
                                                    'stop_smoking_support_received_this_preg': 'Stop Smoking '
                                                                                               'Support '
                                                                                               'Received '
                                                                                               'this Preg — '
                                                                                               'CODED/single '
                                                                                               'selected '
                                                                                               'value; '
                                                                                               'matches: '
                                                                                               'Stop Smoking '
                                                                                               'Support '
                                                                                               'Received '
                                                                                               'this Preg '
                                                                                               '[MATERNITY '
                                                                                               'CARBON '
                                                                                               'MONOXIDE / '
                                                                                               'SMOKING]',
                                                    'stop_smoking_support_received_this_preg_concept_id': 'Mapped '
                                                                                                          'value '
                                                                                                          'concept '
                                                                                                          'identifier '
                                                                                                          'for '
                                                                                                          'Stop '
                                                                                                          'Smoking '
                                                                                                          'Support '
                                                                                                          'Received '
                                                                                                          'this '
                                                                                                          'Preg.',
                                                    'stop_smoking_support_received_this_preg_provenance': 'JSON '
                                                                                                          'mapping '
                                                                                                          'provenance '
                                                                                                          'for '
                                                                                                          'Stop '
                                                                                                          'Smoking '
                                                                                                          'Support '
                                                                                                          'Received '
                                                                                                          'this '
                                                                                                          'Preg: '
                                                                                                          'mapping '
                                                                                                          'source, '
                                                                                                          'rule '
                                                                                                          'ID, '
                                                                                                          'ruleset '
                                                                                                          'version, '
                                                                                                          'canonical '
                                                                                                          'rule '
                                                                                                          'ID '
                                                                                                          'and '
                                                                                                          'match '
                                                                                                          'status, '
                                                                                                          'question '
                                                                                                          'concept '
                                                                                                          'ID '
                                                                                                          'and '
                                                                                                          'unit '
                                                                                                          'concept '
                                                                                                          'ID.',
                                                    'stop_smoking_support_received_this_preg_text': 'Source '
                                                                                                    'response '
                                                                                                    'text '
                                                                                                    'for '
                                                                                                    'Stop '
                                                                                                    'Smoking '
                                                                                                    'Support '
                                                                                                    'Received '
                                                                                                    'this '
                                                                                                    'Preg; '
                                                                                                    'matches: '
                                                                                                    'Stop '
                                                                                                    'Smoking '
                                                                                                    'Support '
                                                                                                    'Received '
                                                                                                    'this '
                                                                                                    'Preg '
                                                                                                    '[MATERNITY '
                                                                                                    'CARBON '
                                                                                                    'MONOXIDE '
                                                                                                    '/ '
                                                                                                    'SMOKING]',
                                                    'subject_id_system': 'Identifier system used for '
                                                                         'subject_key.',
                                                    'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                   'source identifier. Not salted, not '
                                                                   'secret; a stable join key across feeds '
                                                                   'where person_id is unresolved.',
                                                    'unmatched_response_row_count': 'Number of canonical '
                                                                                    'unmatched response '
                                                                                    'rows.',
                                                    'vip_ind': 'VIP indicator when supplied.',
                                                    'withheld_identity_ind': 'Withheld identity indicator.'},
                                        'table': 'Maternity carbon monoxide and smoking assessment: one row '
                                                 'per form instance with configured questions as columns. '
                                                 'Residual responses are in clinical_form_response.'}}
