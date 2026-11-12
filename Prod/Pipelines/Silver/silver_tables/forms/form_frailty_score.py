"""Silver forms.form_frailty_score: 1 dataset(s).

- datasets: clinical_form_frailty_score
- moved verbatim from: 6_silver_forms:8284

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


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
    name=_n("journey_clinical.form_frailty_score"),
    refresh_policy="auto",
)
def form_frailty_score():
    # Build the declared dataset: Rockwood clinical frailty score (both form eras): one row per
    # form instance with configured questions as columns. Residual responses are in
    # clinical_form_response.
    cfg = FORM_CONFIG_BY_SLUG["frailty_score"]
    parent = _instrument_parent(cfg)
    responses = _instrument_responses("frailty_score")
    picked = responses.groupBy("dcp_forms_activity_id").agg(
        *_single_aggregations("rockwood_frailty_score", "CODED"),
    )
    return parent.join(picked, "dcp_forms_activity_id", "left")


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_form_frailty_score': {'columns': {'active_response_row_count': 'Number of active response rows.',
                                             'authored_datetime': 'Earliest form documentation timestamp.',
                                             'completed_datetime': 'Latest documentation or performed '
                                                                   'timestamp.',
                                             'confidentiality_code': 'Security classification when supplied.',
                                             'dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID; '
                                                                      'primary key of this table.',
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
                                                                     'local-clock sources (LUNA, PACS/DICOM, '
                                                                     'TFC LIMS, CCMDS, MSDS, BadgerNet, '
                                                                     'EndoBase, SLAM, iWeb, Datix and '
                                                                     'similar) carry event_datetime '
                                                                     'unchanged. event_datetime is a UTC '
                                                                     'instant for Millennium rows, so take '
                                                                     'local calendar dates and clock times '
                                                                     'from this column.',
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
                                                          'Source_ADC_UPDT from map_mat_vte_assessment. This '
                                                          'is the contributing-source ingestion/provenance '
                                                          'clock, not the Silver refresh time or form event '
                                                          'time; null only when every contributing clock is '
                                                          'null.',
                                             'matched_response_row_count': 'Number of canonical matched '
                                                                           'response rows.',
                                             'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                                'organization.',
                                             'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                  'stable cross-feed join key.',
                                             'performed_practitioner_id': 'Millennium personnel PERSON_ID '
                                                                          'for the performer.',
                                             'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                             'record_status': 'Inherited unchanged from the matching '
                                                              'clinical_form parent. Derived PowerForm row '
                                                              'status: retracted when the grouped maximum of '
                                                              'map_powerform_assessment.SOURCE_PRESENT_IND '
                                                              'is 0; otherwise superseded when the grouped '
                                                              'maximum of ASSESSMENT_ACTIVE_IND is 0 or no '
                                                              'mill_form_activity response has ACTIVE_IND = '
                                                              '1; otherwise active. Missing assessment flags '
                                                              'alone do not trigger retraction or '
                                                              'supersession. This is a status label, not the '
                                                              'raw Boolean assessment flag.',
                                             'record_status_effective_from': 'Inherited unchanged from the '
                                                                             'matching clinical_form parent. '
                                                                             'Earliest '
                                                                             'mill_form_activity.DOCUMENTATION_DT_TM '
                                                                             'for the form instance, carried '
                                                                             'from authored_datetime. This '
                                                                             'is the form documentation '
                                                                             'start used by the research '
                                                                             'status projection, not an '
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
                                             'response_row_count': 'Number of response rows grouped into the '
                                                                   'form.',
                                             'rockwood_frailty_score': 'Rockwood Frailty Score — '
                                                                       'CODED/single selected value; '
                                                                       'matches: Rockwood Frailty Score '
                                                                       '[Frailty Score]; Rockwood Frailty '
                                                                       'Score [Clinical Frailty Scale]',
                                             'rockwood_frailty_score_concept_id': 'Mapped value concept '
                                                                                  'identifier for Rockwood '
                                                                                  'Frailty Score.',
                                             'rockwood_frailty_score_provenance': 'JSON mapping provenance '
                                                                                  'for Rockwood Frailty '
                                                                                  'Score: mapping source, '
                                                                                  'rule ID, ruleset version, '
                                                                                  'canonical rule ID and '
                                                                                  'match status, question '
                                                                                  'concept ID and unit '
                                                                                  'concept ID.',
                                             'rockwood_frailty_score_text': 'Source response text for '
                                                                            'Rockwood Frailty Score; '
                                                                            'matches: Rockwood Frailty Score '
                                                                            '[Frailty Score]; Rockwood '
                                                                            'Frailty Score [Clinical Frailty '
                                                                            'Scale]',
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
                                                                        'combines the contributing activity, '
                                                                        'event, label and bronze-ingestion '
                                                                        'clocks; it is not uniformly a '
                                                                        'native clinical-system update time. '
                                                                        'Null only when every contributing '
                                                                        'clock is null.',
                                             'subject_id_system': 'Identifier system used for subject_key.',
                                             'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                            'identifier. Not salted, not secret; a stable '
                                                            'join key across feeds where person_id is '
                                                            'unresolved.',
                                             'unmatched_response_row_count': 'Number of canonical unmatched '
                                                                             'response rows.',
                                             'vip_ind': 'VIP indicator when supplied.',
                                             'withheld_identity_ind': 'Withheld identity indicator.'},
                                 'table': 'Rockwood clinical frailty score (both form eras): one row per '
                                          'form instance with configured questions as columns. Residual '
                                          'responses are in clinical_form_response.'}}
