"""Silver forms.form_must: 1 dataset(s).

- datasets: clinical_form_must
- moved verbatim from: 6_silver_forms:8588

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
    name=_n("journey_clinical.form_must"),
    refresh_policy="auto",
)
def form_must():
    # Build the declared dataset: Malnutrition Universal Screening Tool: one row per form
    # instance with configured questions as columns. Residual responses are in
    # clinical_form_response.
    cfg = FORM_CONFIG_BY_SLUG["must"]
    parent = _instrument_parent(cfg)
    responses = _instrument_responses("must")
    picked = responses.groupBy("dcp_forms_activity_id").agg(
        *_single_aggregations("source_of_bmi", "CODED"),
        *_single_aggregations("acute_disease_effect_score", "CODED"),
        *_single_aggregations("height_length_measured", "NUMERIC"),
        *_single_aggregations("weight_measured", "NUMERIC"),
        *_single_aggregations("weight_type", "CODED"),
        *_single_aggregations("overall_risk_of_malnutrition", "NUMERIC"),
        *_single_aggregations("bmi", "NUMERIC"),
        *_single_aggregations("usual_weight_prior_to_admission", "NUMERIC"),
        *_single_aggregations("weight_loss_score_must", "CODED"),
        *_single_aggregations("weight_loss", "NUMERIC"),
        *_single_aggregations("patient_type_must", "CODED"),
        *_single_aggregations("admission_clinic_date_must", "DATE"),
        *_single_aggregations("body_mass_index_measured", "NUMERIC"),
        *_single_aggregations("ip_actions_plan_must", "CODED"),
        *_single_aggregations("mid_upper_arm_circumference_cm", "NUMERIC"),
    )
    return parent.join(picked, "dcp_forms_activity_id", "left")


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_form_must': {'columns': {'active_response_row_count': 'Number of active response rows.',
                                    'acute_disease_effect_score': 'Acute Disease Effect Score — CODED/single '
                                                                  'selected value; matches: Acute Disease '
                                                                  'Effect Score [MUST]',
                                    'acute_disease_effect_score_concept_id': 'Mapped value concept '
                                                                             'identifier for Acute Disease '
                                                                             'Effect Score.',
                                    'acute_disease_effect_score_provenance': 'JSON mapping provenance for '
                                                                             'Acute Disease Effect Score: '
                                                                             'mapping source, rule ID, '
                                                                             'ruleset version, canonical '
                                                                             'rule ID and match status, '
                                                                             'question concept ID and unit '
                                                                             'concept ID.',
                                    'acute_disease_effect_score_text': 'Source response text for Acute '
                                                                       'Disease Effect Score; matches: Acute '
                                                                       'Disease Effect Score [MUST]',
                                    'admission_clinic_date_must': 'Admission /Clinic Date (MUST) — '
                                                                  'DATE/single selected value; matches: '
                                                                  'Admission /Clinic Date (MUST) [MUST]',
                                    'admission_clinic_date_must_concept_id': 'Mapped value concept '
                                                                             'identifier for Admission '
                                                                             '/Clinic Date (MUST).',
                                    'admission_clinic_date_must_provenance': 'JSON mapping provenance for '
                                                                             'Admission /Clinic Date (MUST): '
                                                                             'mapping source, rule ID, '
                                                                             'ruleset version, canonical '
                                                                             'rule ID and match status, '
                                                                             'question concept ID and unit '
                                                                             'concept ID.',
                                    'admission_clinic_date_must_text': 'Source response text for Admission '
                                                                       '/Clinic Date (MUST); matches: '
                                                                       'Admission /Clinic Date (MUST) [MUST]',
                                    'authored_datetime': 'Earliest form documentation timestamp.',
                                    'bmi': 'BMI — NUMERIC/single selected value; matches: BMI [MUST]',
                                    'bmi_concept_id': 'Mapped value concept identifier for BMI.',
                                    'bmi_provenance': 'JSON mapping provenance for BMI: mapping source, rule '
                                                      'ID, ruleset version, canonical rule ID and match '
                                                      'status, question concept ID and unit concept ID.',
                                    'bmi_text': 'Source response text for BMI; matches: BMI [MUST]',
                                    'body_mass_index_measured': 'Body Mass Index Measured — NUMERIC/single '
                                                                'selected value; matches: Body Mass Index '
                                                                'Measured [MUST]',
                                    'body_mass_index_measured_concept_id': 'Mapped value concept identifier '
                                                                           'for Body Mass Index Measured.',
                                    'body_mass_index_measured_provenance': 'JSON mapping provenance for Body '
                                                                           'Mass Index Measured: mapping '
                                                                           'source, rule ID, ruleset '
                                                                           'version, canonical rule ID and '
                                                                           'match status, question concept '
                                                                           'ID and unit concept ID.',
                                    'body_mass_index_measured_text': 'Source response text for Body Mass '
                                                                     'Index Measured; matches: Body Mass '
                                                                     'Index Measured [MUST]',
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
                                    'height_length_measured': 'Height/Length Measured — NUMERIC/single '
                                                              'selected value; matches: Height/Length '
                                                              'Measured [MUST]',
                                    'height_length_measured_concept_id': 'Mapped value concept identifier '
                                                                         'for Height/Length Measured.',
                                    'height_length_measured_provenance': 'JSON mapping provenance for '
                                                                         'Height/Length Measured: mapping '
                                                                         'source, rule ID, ruleset version, '
                                                                         'canonical rule ID and match '
                                                                         'status, question concept ID and '
                                                                         'unit concept ID.',
                                    'height_length_measured_text': 'Source response text for Height/Length '
                                                                   'Measured; matches: Height/Length '
                                                                   'Measured [MUST]',
                                    'invalid_response_row_count': 'Number of source-classified invalid '
                                                                  'response rows.',
                                    'ip_actions_plan_must': 'IP Actions Plan (MUST) — CODED/single selected '
                                                            'value; matches: IP Actions Plan (MUST) [MUST]',
                                    'ip_actions_plan_must_concept_id': 'Mapped value concept identifier for '
                                                                       'IP Actions Plan (MUST).',
                                    'ip_actions_plan_must_provenance': 'JSON mapping provenance for IP '
                                                                       'Actions Plan (MUST): mapping source, '
                                                                       'rule ID, ruleset version, canonical '
                                                                       'rule ID and match status, question '
                                                                       'concept ID and unit concept ID.',
                                    'ip_actions_plan_must_text': 'Source response text for IP Actions Plan '
                                                                 '(MUST); matches: IP Actions Plan (MUST) '
                                                                 '[MUST]',
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
                                    'mid_upper_arm_circumference_cm': 'Mid Upper Arm Circumference (cm) — '
                                                                      'NUMERIC/single selected value; '
                                                                      'matches: Mid Upper Arm Circumference '
                                                                      '(cm) [MUST]',
                                    'mid_upper_arm_circumference_cm_concept_id': 'Mapped value concept '
                                                                                 'identifier for Mid Upper '
                                                                                 'Arm Circumference (cm).',
                                    'mid_upper_arm_circumference_cm_provenance': 'JSON mapping provenance '
                                                                                 'for Mid Upper Arm '
                                                                                 'Circumference (cm): '
                                                                                 'mapping source, rule ID, '
                                                                                 'ruleset version, canonical '
                                                                                 'rule ID and match status, '
                                                                                 'question concept ID and '
                                                                                 'unit concept ID.',
                                    'mid_upper_arm_circumference_cm_text': 'Source response text for Mid '
                                                                           'Upper Arm Circumference (cm); '
                                                                           'matches: Mid Upper Arm '
                                                                           'Circumference (cm) [MUST]',
                                    'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                       'organization.',
                                    'overall_risk_of_malnutrition': 'Overall Risk of Malnutrition — '
                                                                    'NUMERIC/single selected value; matches: '
                                                                    'Overall Risk of Malnutrition [MUST]',
                                    'overall_risk_of_malnutrition_concept_id': 'Mapped value concept '
                                                                               'identifier for Overall Risk '
                                                                               'of Malnutrition.',
                                    'overall_risk_of_malnutrition_provenance': 'JSON mapping provenance for '
                                                                               'Overall Risk of '
                                                                               'Malnutrition: mapping '
                                                                               'source, rule ID, ruleset '
                                                                               'version, canonical rule ID '
                                                                               'and match status, question '
                                                                               'concept ID and unit concept '
                                                                               'ID.',
                                    'overall_risk_of_malnutrition_text': 'Source response text for Overall '
                                                                         'Risk of Malnutrition; matches: '
                                                                         'Overall Risk of Malnutrition '
                                                                         '[MUST]',
                                    'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                         'cross-feed join key.',
                                    'patient_type_must': 'Patient Type (MUST) — CODED/single selected value; '
                                                         'matches: Patient Type (MUST) [MUST]',
                                    'patient_type_must_concept_id': 'Mapped value concept identifier for '
                                                                    'Patient Type (MUST).',
                                    'patient_type_must_provenance': 'JSON mapping provenance for Patient '
                                                                    'Type (MUST): mapping source, rule ID, '
                                                                    'ruleset version, canonical rule ID and '
                                                                    'match status, question concept ID and '
                                                                    'unit concept ID.',
                                    'patient_type_must_text': 'Source response text for Patient Type (MUST); '
                                                              'matches: Patient Type (MUST) [MUST]',
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
                                    'source_of_bmi': 'Source of BMI — CODED/single selected value; matches: '
                                                     'Source of BMI [MUST]',
                                    'source_of_bmi_concept_id': 'Mapped value concept identifier for Source '
                                                                'of BMI.',
                                    'source_of_bmi_provenance': 'JSON mapping provenance for Source of BMI: '
                                                                'mapping source, rule ID, ruleset version, '
                                                                'canonical rule ID and match status, '
                                                                'question concept ID and unit concept ID.',
                                    'source_of_bmi_text': 'Source response text for Source of BMI; matches: '
                                                          'Source of BMI [MUST]',
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
                                    'usual_weight_prior_to_admission': 'Usual Weight Prior to Admission — '
                                                                       'NUMERIC/single selected value; '
                                                                       'matches: Usual Weight Prior to '
                                                                       'Admission [MUST]',
                                    'usual_weight_prior_to_admission_concept_id': 'Mapped value concept '
                                                                                  'identifier for Usual '
                                                                                  'Weight Prior to '
                                                                                  'Admission.',
                                    'usual_weight_prior_to_admission_provenance': 'JSON mapping provenance '
                                                                                  'for Usual Weight Prior to '
                                                                                  'Admission: mapping '
                                                                                  'source, rule ID, ruleset '
                                                                                  'version, canonical rule '
                                                                                  'ID and match status, '
                                                                                  'question concept ID and '
                                                                                  'unit concept ID.',
                                    'usual_weight_prior_to_admission_text': 'Source response text for Usual '
                                                                            'Weight Prior to Admission; '
                                                                            'matches: Usual Weight Prior to '
                                                                            'Admission [MUST]',
                                    'vip_ind': 'VIP indicator when supplied.',
                                    'weight_loss': '% Weight Loss — NUMERIC/single selected value; matches: '
                                                   '% Weight Loss [MUST]',
                                    'weight_loss_concept_id': 'Mapped value concept identifier for % Weight '
                                                              'Loss.',
                                    'weight_loss_provenance': 'JSON mapping provenance for % Weight Loss: '
                                                              'mapping source, rule ID, ruleset version, '
                                                              'canonical rule ID and match status, question '
                                                              'concept ID and unit concept ID.',
                                    'weight_loss_score_must': 'Weight Loss Score (MUST) — CODED/single '
                                                              'selected value; matches: Weight Loss Score '
                                                              '(MUST) [MUST]',
                                    'weight_loss_score_must_concept_id': 'Mapped value concept identifier '
                                                                         'for Weight Loss Score (MUST).',
                                    'weight_loss_score_must_provenance': 'JSON mapping provenance for Weight '
                                                                         'Loss Score (MUST): mapping source, '
                                                                         'rule ID, ruleset version, '
                                                                         'canonical rule ID and match '
                                                                         'status, question concept ID and '
                                                                         'unit concept ID.',
                                    'weight_loss_score_must_text': 'Source response text for Weight Loss '
                                                                   'Score (MUST); matches: Weight Loss Score '
                                                                   '(MUST) [MUST]',
                                    'weight_loss_text': 'Source response text for % Weight Loss; matches: % '
                                                        'Weight Loss [MUST]',
                                    'weight_measured': 'Weight Measured — NUMERIC/single selected value; '
                                                       'matches: Weight Measured [MUST]',
                                    'weight_measured_concept_id': 'Mapped value concept identifier for '
                                                                  'Weight Measured.',
                                    'weight_measured_provenance': 'JSON mapping provenance for Weight '
                                                                  'Measured: mapping source, rule ID, '
                                                                  'ruleset version, canonical rule ID and '
                                                                  'match status, question concept ID and '
                                                                  'unit concept ID.',
                                    'weight_measured_text': 'Source response text for Weight Measured; '
                                                            'matches: Weight Measured [MUST]',
                                    'weight_type': 'Weight Type — CODED/single selected value; matches: '
                                                   'Weight Type [MUST]',
                                    'weight_type_concept_id': 'Mapped value concept identifier for Weight '
                                                              'Type.',
                                    'weight_type_provenance': 'JSON mapping provenance for Weight Type: '
                                                              'mapping source, rule ID, ruleset version, '
                                                              'canonical rule ID and match status, question '
                                                              'concept ID and unit concept ID.',
                                    'weight_type_text': 'Source response text for Weight Type; matches: '
                                                        'Weight Type [MUST]',
                                    'withheld_identity_ind': 'Withheld identity indicator.'},
                        'table': 'Malnutrition Universal Screening Tool: one row per form instance with '
                                 'configured questions as columns. Residual responses are in '
                                 'clinical_form_response.'}}
