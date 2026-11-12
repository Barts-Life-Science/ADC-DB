"""Silver forms.form_diabetes_in_pregnancy: 1 dataset(s).

- datasets: clinical_form_diabetes_in_pregnancy
- moved verbatim from: 6_silver_forms:13121

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
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
    name=_n("journey_clinical.form_diabetes_in_pregnancy"),
    refresh_policy="auto",
)
def form_diabetes_in_pregnancy():
    # Build the declared dataset: Diabetes in pregnancy specialist assessment: one row per form
    # instance with configured questions as columns. Residual responses are in
    # clinical_form_response.
    cfg = FORM_CONFIG_BY_SLUG["diabetes_in_pregnancy"]
    parent = _instrument_parent(cfg)
    responses = _instrument_responses("diabetes_in_pregnancy")
    picked = responses.groupBy("dcp_forms_activity_id").agg(
        *_multi_aggregations(cfg, "name_of_key_professional_diabetes", "TEXT"),
        *_multi_aggregations(cfg, "date_attended_diabetes", "DATE"),
        *_multi_aggregations(cfg, "advice_and_future_insulin_doses", "TEXT"),
        *_multi_aggregations(cfg, "current_insulin_dose", "TEXT"),
        *_multi_aggregations(cfg, "gestation_at_attendance_diabetes", "TEXT"),
        *_multi_aggregations(cfg, "home_blood_test_results", "TEXT"),
        *_single_aggregations("type_of_diabetes", "CODED"),
        *_single_aggregations("method_of_diagnosis_diabetes", "CODED"),
        *_single_aggregations("fasting_result_from_ogtt", "NUMERIC"),
        *_single_aggregations("ogtt_result_after_2_hours", "NUMERIC"),
        *_single_aggregations("date_of_1st_ogtt", "DATE"),
        *_multi_aggregations(cfg, "current_medication", "TEXT"),
        *_multi_aggregations(cfg, "prescription_drugs_route", "CODED"),
        *_single_aggregations("item_1st_antenatal_diabetic_appointment", "DATE"),
        *_multi_aggregations(cfg, "prescription_drugs_dose", "TEXT"),
        *_multi_aggregations(cfg, "prescription_drugs_frequency", "CODED"),
        *_multi_aggregations(cfg, "prescription_drugs_comment", "TEXT"),
        *_single_aggregations("random_blood_sugar_date", "DATE"),
        F.max(F.col("pregnancy_id")).alias("pregnancy_id"),
        F.max(F.col("pregnancy_match_method")).alias("pregnancy_match_method"),
    )
    return parent.join(picked, "dcp_forms_activity_id", "left")


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_form_diabetes_in_pregnancy': {'columns': {'active_response_row_count': 'Number of active response '
                                                                                  'rows.',
                                                     'advice_and_future_insulin_doses': 'JSON array of every '
                                                                                        'selected TEXT '
                                                                                        'response for Advice '
                                                                                        'and Future Insulin '
                                                                                        'Doses in sequence '
                                                                                        'order (sequence, '
                                                                                        'response ID, value, '
                                                                                        'source text, value '
                                                                                        'concept ID, mapping '
                                                                                        'source, mapping '
                                                                                        'rule ID); [] when '
                                                                                        'the question was '
                                                                                        'not answered; '
                                                                                        'matches: Advice and '
                                                                                        'Future Insulin '
                                                                                        'Doses [US AND '
                                                                                        'ASSESSMENT]',
                                                     'advice_and_future_insulin_doses_text': 'Pipe-joined '
                                                                                             'selected TEXT '
                                                                                             'values for '
                                                                                             'Advice and '
                                                                                             'Future Insulin '
                                                                                             'Doses, ordered '
                                                                                             'by sequence '
                                                                                             'and response '
                                                                                             'ID; matches: '
                                                                                             'Advice and '
                                                                                             'Future Insulin '
                                                                                             'Doses [US AND '
                                                                                             'ASSESSMENT]',
                                                     'authored_datetime': 'Earliest form documentation '
                                                                          'timestamp.',
                                                     'completed_datetime': 'Latest documentation or '
                                                                           'performed timestamp.',
                                                     'confidentiality_code': 'Security classification when '
                                                                             'supplied.',
                                                     'current_insulin_dose': 'JSON array of every selected '
                                                                             'TEXT response for Current '
                                                                             'Insulin Dose in sequence order '
                                                                             '(sequence, response ID, value, '
                                                                             'source text, value concept ID, '
                                                                             'mapping source, mapping rule '
                                                                             'ID); [] when the question was '
                                                                             'not answered; matches: Current '
                                                                             'Insulin Dose [US AND '
                                                                             'ASSESSMENT]',
                                                     'current_insulin_dose_text': 'Pipe-joined selected TEXT '
                                                                                  'values for Current '
                                                                                  'Insulin Dose, ordered by '
                                                                                  'sequence and response ID; '
                                                                                  'matches: Current Insulin '
                                                                                  'Dose [US AND ASSESSMENT]',
                                                     'current_medication': 'JSON array of every selected '
                                                                           'TEXT response for Current '
                                                                           'Medication in sequence order '
                                                                           '(sequence, response ID, value, '
                                                                           'source text, value concept ID, '
                                                                           'mapping source, mapping rule '
                                                                           'ID); [] when the question was '
                                                                           'not answered; matches: Current '
                                                                           'Medication [DIABETES IN '
                                                                           'PREGNANCY]',
                                                     'current_medication_text': 'Pipe-joined selected TEXT '
                                                                                'values for Current '
                                                                                'Medication, ordered by '
                                                                                'sequence and response ID; '
                                                                                'matches: Current Medication '
                                                                                '[DIABETES IN PREGNANCY]',
                                                     'date_attended_diabetes': 'JSON array of every selected '
                                                                               'DATE response for Date '
                                                                               'Attended (Diabetes) in '
                                                                               'sequence order (sequence, '
                                                                               'response ID, value, source '
                                                                               'text, value concept ID, '
                                                                               'mapping source, mapping rule '
                                                                               'ID); [] when the question '
                                                                               'was not answered; matches: '
                                                                               'Date Attended (Diabetes) [US '
                                                                               'AND ASSESSMENT]',
                                                     'date_attended_diabetes_text': 'Pipe-joined selected '
                                                                                    'DATE values for Date '
                                                                                    'Attended (Diabetes), '
                                                                                    'ordered by sequence and '
                                                                                    'response ID; matches: '
                                                                                    'Date Attended '
                                                                                    '(Diabetes) [US AND '
                                                                                    'ASSESSMENT]',
                                                     'date_of_1st_ogtt': 'Date of 1st OGTT — DATE/single '
                                                                         'selected value; matches: Date of '
                                                                         '1st OGTT [DIABETES IN PREGNANCY]',
                                                     'date_of_1st_ogtt_concept_id': 'Mapped value concept '
                                                                                    'identifier for Date of '
                                                                                    '1st OGTT.',
                                                     'date_of_1st_ogtt_provenance': 'JSON mapping provenance '
                                                                                    'for Date of 1st OGTT: '
                                                                                    'mapping source, rule '
                                                                                    'ID, ruleset version, '
                                                                                    'canonical rule ID and '
                                                                                    'match status, question '
                                                                                    'concept ID and unit '
                                                                                    'concept ID.',
                                                     'date_of_1st_ogtt_text': 'Source response text for Date '
                                                                              'of 1st OGTT; matches: Date of '
                                                                              '1st OGTT [DIABETES IN '
                                                                              'PREGNANCY]',
                                                     'dcp_forms_activity_id': 'Millennium '
                                                                              'DCP_FORMS_ACTIVITY_ID; '
                                                                              'primary key of this table.',
                                                     'empty_response_row_count': 'Number of retained empty '
                                                                                 'response rows.',
                                                     'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                                     'spine_encounter.encounter_id.',
                                                     'event_datetime': 'First documented timestamp.',
                                                     'event_datetime_local': 'event_datetime in '
                                                                             'Europe/London wall-clock time '
                                                                             '(GMT/BST). Millennium rows '
                                                                             'take the bronze *_LOCAL '
                                                                             'companions of the same source '
                                                                             'columns and fallback order; '
                                                                             'rows from local-clock sources '
                                                                             '(LUNA, PACS/DICOM, TFC LIMS, '
                                                                             'CCMDS, MSDS, BadgerNet, '
                                                                             'EndoBase, SLAM, iWeb, Datix '
                                                                             'and similar) carry '
                                                                             'event_datetime unchanged. '
                                                                             'event_datetime is a UTC '
                                                                             'instant for Millennium rows, '
                                                                             'so take local calendar dates '
                                                                             'and clock times from this '
                                                                             'column.',
                                                     'event_end_datetime': 'Last documented or performed '
                                                                           'timestamp.',
                                                     'event_end_datetime_local': 'event_end_datetime in '
                                                                                 'Europe/London wall-clock '
                                                                                 'time (GMT/BST), by the '
                                                                                 'same rule as '
                                                                                 'event_datetime_local; null '
                                                                                 'where event_end_datetime '
                                                                                 'is null.',
                                                     'fasting_result_from_ogtt': 'Fasting Result From OGTT — '
                                                                                 'NUMERIC/single selected '
                                                                                 'value; matches: Fasting '
                                                                                 'Result From OGTT [DIABETES '
                                                                                 'IN PREGNANCY]',
                                                     'fasting_result_from_ogtt_concept_id': 'Mapped value '
                                                                                            'concept '
                                                                                            'identifier for '
                                                                                            'Fasting Result '
                                                                                            'From OGTT.',
                                                     'fasting_result_from_ogtt_provenance': 'JSON mapping '
                                                                                            'provenance for '
                                                                                            'Fasting Result '
                                                                                            'From OGTT: '
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
                                                     'fasting_result_from_ogtt_text': 'Source response text '
                                                                                      'for Fasting Result '
                                                                                      'From OGTT; matches: '
                                                                                      'Fasting Result From '
                                                                                      'OGTT [DIABETES IN '
                                                                                      'PREGNANCY]',
                                                     'form_status_code': 'Source form status code.',
                                                     'form_status_display': 'Source form status display.',
                                                     'form_type_code': 'Source form type code.',
                                                     'form_type_display': 'Source form type display.',
                                                     'gestation_at_attendance_diabetes': 'JSON array of '
                                                                                         'every selected '
                                                                                         'TEXT response for '
                                                                                         'Gestation at '
                                                                                         'Attendance '
                                                                                         '(Diabetes) in '
                                                                                         'sequence order '
                                                                                         '(sequence, '
                                                                                         'response ID, '
                                                                                         'value, source '
                                                                                         'text, value '
                                                                                         'concept ID, '
                                                                                         'mapping source, '
                                                                                         'mapping rule ID); '
                                                                                         '[] when the '
                                                                                         'question was not '
                                                                                         'answered; matches: '
                                                                                         'Gestation at '
                                                                                         'Attendance '
                                                                                         '(Diabetes) [US AND '
                                                                                         'ASSESSMENT]',
                                                     'gestation_at_attendance_diabetes_text': 'Pipe-joined '
                                                                                              'selected TEXT '
                                                                                              'values for '
                                                                                              'Gestation at '
                                                                                              'Attendance '
                                                                                              '(Diabetes), '
                                                                                              'ordered by '
                                                                                              'sequence and '
                                                                                              'response ID; '
                                                                                              'matches: '
                                                                                              'Gestation at '
                                                                                              'Attendance '
                                                                                              '(Diabetes) '
                                                                                              '[US AND '
                                                                                              'ASSESSMENT]',
                                                     'home_blood_test_results': 'JSON array of every '
                                                                                'selected TEXT response for '
                                                                                'Home Blood Test Results in '
                                                                                'sequence order (sequence, '
                                                                                'response ID, value, source '
                                                                                'text, value concept ID, '
                                                                                'mapping source, mapping '
                                                                                'rule ID); [] when the '
                                                                                'question was not answered; '
                                                                                'matches: Home Blood Test '
                                                                                'Results [US AND ASSESSMENT]',
                                                     'home_blood_test_results_text': 'Pipe-joined selected '
                                                                                     'TEXT values for Home '
                                                                                     'Blood Test Results, '
                                                                                     'ordered by sequence '
                                                                                     'and response ID; '
                                                                                     'matches: Home Blood '
                                                                                     'Test Results [US AND '
                                                                                     'ASSESSMENT]',
                                                     'invalid_response_row_count': 'Number of '
                                                                                   'source-classified '
                                                                                   'invalid response rows.',
                                                     'item_1st_antenatal_diabetic_appointment': '1st '
                                                                                                'Antenatal '
                                                                                                'Diabetic '
                                                                                                'Appointment '
                                                                                                '— '
                                                                                                'DATE/single '
                                                                                                'selected '
                                                                                                'value; '
                                                                                                'matches: '
                                                                                                '1st '
                                                                                                'Antenatal '
                                                                                                'Diabetic '
                                                                                                'Appointment '
                                                                                                '[DIABETES '
                                                                                                'IN '
                                                                                                'PREGNANCY]',
                                                     'item_1st_antenatal_diabetic_appointment_concept_id': 'Mapped '
                                                                                                           'value '
                                                                                                           'concept '
                                                                                                           'identifier '
                                                                                                           'for '
                                                                                                           '1st '
                                                                                                           'Antenatal '
                                                                                                           'Diabetic '
                                                                                                           'Appointment.',
                                                     'item_1st_antenatal_diabetic_appointment_provenance': 'JSON '
                                                                                                           'mapping '
                                                                                                           'provenance '
                                                                                                           'for '
                                                                                                           '1st '
                                                                                                           'Antenatal '
                                                                                                           'Diabetic '
                                                                                                           'Appointment: '
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
                                                     'item_1st_antenatal_diabetic_appointment_text': 'Source '
                                                                                                     'response '
                                                                                                     'text '
                                                                                                     'for '
                                                                                                     '1st '
                                                                                                     'Antenatal '
                                                                                                     'Diabetic '
                                                                                                     'Appointment; '
                                                                                                     'matches: '
                                                                                                     '1st '
                                                                                                     'Antenatal '
                                                                                                     'Diabetic '
                                                                                                     'Appointment '
                                                                                                     '[DIABETES '
                                                                                                     'IN '
                                                                                                     'PREGNANCY]',
                                                     'loaded_at': 'Inherited unchanged from the matching '
                                                                  'clinical_form parent. Greatest of the '
                                                                  'form-level maximum ADC_UPDT values from '
                                                                  'mill_form_activity, '
                                                                  'map_powerform_assessment_item and '
                                                                  'map_powerform_assessment, and maximum '
                                                                  'Source_ADC_UPDT from '
                                                                  'map_mat_vte_assessment. This is the '
                                                                  'contributing-source ingestion/provenance '
                                                                  'clock, not the Silver refresh time or '
                                                                  'form event time; null only when every '
                                                                  'contributing clock is null.',
                                                     'matched_response_row_count': 'Number of canonical '
                                                                                   'matched response rows.',
                                                     'method_of_diagnosis_diabetes': 'Method Of Diagnosis '
                                                                                     '(Diabetes) — '
                                                                                     'CODED/single selected '
                                                                                     'value; matches: Method '
                                                                                     'Of Diagnosis '
                                                                                     '(Diabetes) [DIABETES '
                                                                                     'IN PREGNANCY]',
                                                     'method_of_diagnosis_diabetes_concept_id': 'Mapped '
                                                                                                'value '
                                                                                                'concept '
                                                                                                'identifier '
                                                                                                'for Method '
                                                                                                'Of '
                                                                                                'Diagnosis '
                                                                                                '(Diabetes).',
                                                     'method_of_diagnosis_diabetes_provenance': 'JSON '
                                                                                                'mapping '
                                                                                                'provenance '
                                                                                                'for Method '
                                                                                                'Of '
                                                                                                'Diagnosis '
                                                                                                '(Diabetes): '
                                                                                                'mapping '
                                                                                                'source, '
                                                                                                'rule ID, '
                                                                                                'ruleset '
                                                                                                'version, '
                                                                                                'canonical '
                                                                                                'rule ID and '
                                                                                                'match '
                                                                                                'status, '
                                                                                                'question '
                                                                                                'concept ID '
                                                                                                'and unit '
                                                                                                'concept ID.',
                                                     'method_of_diagnosis_diabetes_text': 'Source response '
                                                                                          'text for Method '
                                                                                          'Of Diagnosis '
                                                                                          '(Diabetes); '
                                                                                          'matches: Method '
                                                                                          'Of Diagnosis '
                                                                                          '(Diabetes) '
                                                                                          '[DIABETES IN '
                                                                                          'PREGNANCY]',
                                                     'name_of_key_professional_diabetes': 'JSON array of '
                                                                                          'every selected '
                                                                                          'TEXT response for '
                                                                                          'Name of Key '
                                                                                          'Professional '
                                                                                          '(Diabetes) in '
                                                                                          'sequence order '
                                                                                          '(sequence, '
                                                                                          'response ID, '
                                                                                          'value, source '
                                                                                          'text, value '
                                                                                          'concept ID, '
                                                                                          'mapping source, '
                                                                                          'mapping rule ID); '
                                                                                          '[] when the '
                                                                                          'question was not '
                                                                                          'answered; '
                                                                                          'matches: Name of '
                                                                                          'Key Professional '
                                                                                          '(Diabetes) [US '
                                                                                          'AND ASSESSMENT]',
                                                     'name_of_key_professional_diabetes_text': 'Pipe-joined '
                                                                                               'selected '
                                                                                               'TEXT values '
                                                                                               'for Name of '
                                                                                               'Key '
                                                                                               'Professional '
                                                                                               '(Diabetes), '
                                                                                               'ordered by '
                                                                                               'sequence and '
                                                                                               'response ID; '
                                                                                               'matches: '
                                                                                               'Name of Key '
                                                                                               'Professional '
                                                                                               '(Diabetes) '
                                                                                               '[US AND '
                                                                                               'ASSESSMENT]',
                                                     'ogtt_result_after_2_hours': 'OGTT Result After 2 Hours '
                                                                                  '— NUMERIC/single selected '
                                                                                  'value; matches: OGTT '
                                                                                  'Result After 2 Hours '
                                                                                  '[DIABETES IN PREGNANCY]',
                                                     'ogtt_result_after_2_hours_concept_id': 'Mapped value '
                                                                                             'concept '
                                                                                             'identifier for '
                                                                                             'OGTT Result '
                                                                                             'After 2 Hours.',
                                                     'ogtt_result_after_2_hours_provenance': 'JSON mapping '
                                                                                             'provenance for '
                                                                                             'OGTT Result '
                                                                                             'After 2 Hours: '
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
                                                     'ogtt_result_after_2_hours_text': 'Source response text '
                                                                                       'for OGTT Result '
                                                                                       'After 2 Hours; '
                                                                                       'matches: OGTT Result '
                                                                                       'After 2 Hours '
                                                                                       '[DIABETES IN '
                                                                                       'PREGNANCY]',
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
                                                     'prescription_drugs_comment': 'JSON array of every '
                                                                                   'selected TEXT response '
                                                                                   'for Prescription Drugs '
                                                                                   'Comment in sequence '
                                                                                   'order (sequence, '
                                                                                   'response ID, value, '
                                                                                   'source text, value '
                                                                                   'concept ID, mapping '
                                                                                   'source, mapping rule '
                                                                                   'ID); [] when the '
                                                                                   'question was not '
                                                                                   'answered; matches: '
                                                                                   'Prescription Drugs '
                                                                                   'Comment [DIABETES IN '
                                                                                   'PREGNANCY]',
                                                     'prescription_drugs_comment_text': 'Pipe-joined '
                                                                                        'selected TEXT '
                                                                                        'values for '
                                                                                        'Prescription Drugs '
                                                                                        'Comment, ordered by '
                                                                                        'sequence and '
                                                                                        'response ID; '
                                                                                        'matches: '
                                                                                        'Prescription Drugs '
                                                                                        'Comment [DIABETES '
                                                                                        'IN PREGNANCY]',
                                                     'prescription_drugs_dose': 'JSON array of every '
                                                                                'selected TEXT response for '
                                                                                'Prescription Drugs Dose in '
                                                                                'sequence order (sequence, '
                                                                                'response ID, value, source '
                                                                                'text, value concept ID, '
                                                                                'mapping source, mapping '
                                                                                'rule ID); [] when the '
                                                                                'question was not answered; '
                                                                                'matches: Prescription Drugs '
                                                                                'Dose [DIABETES IN '
                                                                                'PREGNANCY]',
                                                     'prescription_drugs_dose_text': 'Pipe-joined selected '
                                                                                     'TEXT values for '
                                                                                     'Prescription Drugs '
                                                                                     'Dose, ordered by '
                                                                                     'sequence and response '
                                                                                     'ID; matches: '
                                                                                     'Prescription Drugs '
                                                                                     'Dose [DIABETES IN '
                                                                                     'PREGNANCY]',
                                                     'prescription_drugs_frequency__as_required': 'Prescription '
                                                                                                  'Drugs '
                                                                                                  'Frequency '
                                                                                                  'option '
                                                                                                  "'As "
                                                                                                  "Required': "
                                                                                                  'true when '
                                                                                                  'selected, '
                                                                                                  'false '
                                                                                                  'when the '
                                                                                                  'question '
                                                                                                  'was '
                                                                                                  'answered '
                                                                                                  'with '
                                                                                                  'other '
                                                                                                  'options '
                                                                                                  'only, '
                                                                                                  'null when '
                                                                                                  'the '
                                                                                                  'question '
                                                                                                  'was not '
                                                                                                  'answered; '
                                                                                                  'matches: '
                                                                                                  'Prescription '
                                                                                                  'Drugs '
                                                                                                  'Frequency '
                                                                                                  '[DIABETES '
                                                                                                  'IN '
                                                                                                  'PREGNANCY]',
                                                     'prescription_drugs_frequency__daily': 'Prescription '
                                                                                            'Drugs Frequency '
                                                                                            "option 'Daily': "
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
                                                                                            'matches: '
                                                                                            'Prescription '
                                                                                            'Drugs Frequency '
                                                                                            '[DIABETES IN '
                                                                                            'PREGNANCY]',
                                                     'prescription_drugs_frequency__occasionally': 'Prescription '
                                                                                                   'Drugs '
                                                                                                   'Frequency '
                                                                                                   'option '
                                                                                                   "'Occasionally': "
                                                                                                   'true '
                                                                                                   'when '
                                                                                                   'selected, '
                                                                                                   'false '
                                                                                                   'when the '
                                                                                                   'question '
                                                                                                   'was '
                                                                                                   'answered '
                                                                                                   'with '
                                                                                                   'other '
                                                                                                   'options '
                                                                                                   'only, '
                                                                                                   'null '
                                                                                                   'when the '
                                                                                                   'question '
                                                                                                   'was not '
                                                                                                   'answered; '
                                                                                                   'matches: '
                                                                                                   'Prescription '
                                                                                                   'Drugs '
                                                                                                   'Frequency '
                                                                                                   '[DIABETES '
                                                                                                   'IN '
                                                                                                   'PREGNANCY]',
                                                     'prescription_drugs_frequency_text': 'Pipe-joined '
                                                                                          'selected CODED '
                                                                                          'values for '
                                                                                          'Prescription '
                                                                                          'Drugs Frequency, '
                                                                                          'ordered by '
                                                                                          'sequence and '
                                                                                          'response ID; '
                                                                                          'matches: '
                                                                                          'Prescription '
                                                                                          'Drugs Frequency '
                                                                                          '[DIABETES IN '
                                                                                          'PREGNANCY]',
                                                     'prescription_drugs_route__inhaled': 'Prescription '
                                                                                          'Drugs Route '
                                                                                          "option 'Inhaled': "
                                                                                          'true when '
                                                                                          'selected, false '
                                                                                          'when the question '
                                                                                          'was answered with '
                                                                                          'other options '
                                                                                          'only, null when '
                                                                                          'the question was '
                                                                                          'not answered; '
                                                                                          'matches: '
                                                                                          'Prescription '
                                                                                          'Drugs Route '
                                                                                          '[DIABETES IN '
                                                                                          'PREGNANCY]',
                                                     'prescription_drugs_route__oral': 'Prescription Drugs '
                                                                                       "Route option 'Oral': "
                                                                                       'true when selected, '
                                                                                       'false when the '
                                                                                       'question was '
                                                                                       'answered with other '
                                                                                       'options only, null '
                                                                                       'when the question '
                                                                                       'was not answered; '
                                                                                       'matches: '
                                                                                       'Prescription Drugs '
                                                                                       'Route [DIABETES IN '
                                                                                       'PREGNANCY]',
                                                     'prescription_drugs_route__rectal': 'Prescription Drugs '
                                                                                         'Route option '
                                                                                         "'Rectal': true "
                                                                                         'when selected, '
                                                                                         'false when the '
                                                                                         'question was '
                                                                                         'answered with '
                                                                                         'other options '
                                                                                         'only, null when '
                                                                                         'the question was '
                                                                                         'not answered; '
                                                                                         'matches: '
                                                                                         'Prescription Drugs '
                                                                                         'Route [DIABETES IN '
                                                                                         'PREGNANCY]',
                                                     'prescription_drugs_route__subcutaneous': 'Prescription '
                                                                                               'Drugs Route '
                                                                                               'option '
                                                                                               "'Subcutaneous': "
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
                                                                                               'Prescription '
                                                                                               'Drugs Route '
                                                                                               '[DIABETES IN '
                                                                                               'PREGNANCY]',
                                                     'prescription_drugs_route__suppository': 'Prescription '
                                                                                              'Drugs Route '
                                                                                              'option '
                                                                                              "'Suppository': "
                                                                                              'true when '
                                                                                              'selected, '
                                                                                              'false when '
                                                                                              'the question '
                                                                                              'was answered '
                                                                                              'with other '
                                                                                              'options only, '
                                                                                              'null when the '
                                                                                              'question was '
                                                                                              'not answered; '
                                                                                              'matches: '
                                                                                              'Prescription '
                                                                                              'Drugs Route '
                                                                                              '[DIABETES IN '
                                                                                              'PREGNANCY]',
                                                     'prescription_drugs_route_text': 'Pipe-joined selected '
                                                                                      'CODED values for '
                                                                                      'Prescription Drugs '
                                                                                      'Route, ordered by '
                                                                                      'sequence and response '
                                                                                      'ID; matches: '
                                                                                      'Prescription Drugs '
                                                                                      'Route [DIABETES IN '
                                                                                      'PREGNANCY]',
                                                     'random_blood_sugar_date': 'Random Blood Sugar Date — '
                                                                                'DATE/single selected value; '
                                                                                'matches: Random Blood Sugar '
                                                                                'Date [DIABETES IN '
                                                                                'PREGNANCY]',
                                                     'random_blood_sugar_date_concept_id': 'Mapped value '
                                                                                           'concept '
                                                                                           'identifier for '
                                                                                           'Random Blood '
                                                                                           'Sugar Date.',
                                                     'random_blood_sugar_date_provenance': 'JSON mapping '
                                                                                           'provenance for '
                                                                                           'Random Blood '
                                                                                           'Sugar Date: '
                                                                                           'mapping source, '
                                                                                           'rule ID, ruleset '
                                                                                           'version, '
                                                                                           'canonical rule '
                                                                                           'ID and match '
                                                                                           'status, question '
                                                                                           'concept ID and '
                                                                                           'unit concept ID.',
                                                     'random_blood_sugar_date_text': 'Source response text '
                                                                                     'for Random Blood Sugar '
                                                                                     'Date; matches: Random '
                                                                                     'Blood Sugar Date '
                                                                                     '[DIABETES IN '
                                                                                     'PREGNANCY]',
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
                                                                                     'authored_datetime. '
                                                                                     'This is the form '
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
                                                                                   'clinical_form parent. '
                                                                                   'For a retracted or '
                                                                                   'superseded form, '
                                                                                   'completed_datetime: the '
                                                                                   'maximum of '
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
                                                     'source_code': 'Source form reference identifier.',
                                                     'source_coding_system': 'Source form coding system.',
                                                     'source_display': 'Source form description.',
                                                     'source_feed': 'Registered owning feed.',
                                                     'source_update_timestamp': 'Inherited unchanged from '
                                                                                'the matching clinical_form '
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
                                                                                'clinical-system update '
                                                                                'time. Null only when every '
                                                                                'contributing clock is null.',
                                                     'subject_id_system': 'Identifier system used for '
                                                                          'subject_key.',
                                                     'subject_key': 'Deterministic SHA-256 over the '
                                                                    'strongest source identifier. Not '
                                                                    'salted, not secret; a stable join key '
                                                                    'across feeds where person_id is '
                                                                    'unresolved.',
                                                     'type_of_diabetes': 'Type of Diabetes — CODED/single '
                                                                         'selected value; matches: Type of '
                                                                         'Diabetes [DIABETES IN PREGNANCY]',
                                                     'type_of_diabetes_concept_id': 'Mapped value concept '
                                                                                    'identifier for Type of '
                                                                                    'Diabetes.',
                                                     'type_of_diabetes_provenance': 'JSON mapping provenance '
                                                                                    'for Type of Diabetes: '
                                                                                    'mapping source, rule '
                                                                                    'ID, ruleset version, '
                                                                                    'canonical rule ID and '
                                                                                    'match status, question '
                                                                                    'concept ID and unit '
                                                                                    'concept ID.',
                                                     'type_of_diabetes_text': 'Source response text for Type '
                                                                              'of Diabetes; matches: Type of '
                                                                              'Diabetes [DIABETES IN '
                                                                              'PREGNANCY]',
                                                     'unmatched_response_row_count': 'Number of canonical '
                                                                                     'unmatched response '
                                                                                     'rows.',
                                                     'vip_ind': 'VIP indicator when supplied.',
                                                     'withheld_identity_ind': 'Withheld identity indicator.'},
                                         'table': 'Diabetes in pregnancy specialist assessment: one row per '
                                                  'form instance with configured questions as columns. '
                                                  'Residual responses are in clinical_form_response.'}}
