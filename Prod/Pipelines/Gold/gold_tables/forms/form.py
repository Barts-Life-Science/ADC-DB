"""Gold forms.form: 2 dataset(s).

- datasets: _qc_clinical_form, clinical_form
- moved verbatim from: 6_gold_forms:156, 6_gold_forms:160, 6_gold_forms:173, 6_gold_forms:33, 6_gold_forms:73, 6_gold_forms:81

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.form ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_FORM_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`dcp_forms_activity_id` AS `dcp_forms_activity_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`form_type_code` AS `form_type_code`',
    '`form_type_display` AS `form_type_display`',
    '`form_status_code` AS `form_status_code`',
    '`form_status_display` AS `form_status_display`',
    'CASE WHEN CAST(`authored_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `authored_datetime` END AS `authored_datetime`',
    '`completed_datetime` AS `completed_datetime`',
    '`performed_practitioner_id` AS `performed_practitioner_id`',
    '`organization_id` AS `organization_id`',
    '`response_row_count` AS `response_row_count`',
    '`active_response_row_count` AS `active_response_row_count`',
    '`empty_response_row_count` AS `empty_response_row_count`',
    '`invalid_response_row_count` AS `invalid_response_row_count`',
    '`matched_response_row_count` AS `matched_response_row_count`',
    '`unmatched_response_row_count` AS `unmatched_response_row_count`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_form_metadata; source history stays on the main research table.
CLINICAL_FORM_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 55,387,704 rows of
    # 55,393,916 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_FORM_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.form.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 6,212 of 55,393,916 at the profile.
    "gold.clinical.form.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 45 of 55,393,916
    # rows (8.12e-05%) when profiled on 2026-08-24.
    "gold.clinical.form.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 8,310 of 55,393,916 rows (0.015%) when profiled on 2026-08-24.
    "gold.clinical.form.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 12,285 of 55,393,916 rows (0.0222%) when profiled on 2026-08-24.
    "gold.clinical.form.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_FORM_PERSON_SCOPE_RULES = {
    "gold.clinical.form.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_form"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_FORM_MANDATORY_RULES)
@dp.expect_all(CLINICAL_FORM_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_FORM_PERSON_SCOPE_RULES)
def _gold_qc_clinical_form():
    """Quality-controlled twin of journey_clinical.form."""
    df = _qc("clinical_form", CLINICAL_FORM_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.form"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_form():
    """Contract-v2 public twin of clinical_form; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_form")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`dcp_forms_activity_id` AS `dcp_forms_activity_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`form_type_code` AS `form_type_code`',
        '`form_type_display` AS `form_type_display`',
        '`form_status_code` AS `form_status_code`',
        '`form_status_display` AS `form_status_display`',
        '`authored_datetime` AS `authored_datetime`',
        '`completed_datetime` AS `completed_datetime`',
        '`performed_practitioner_id` AS `performed_practitioner_id`',
        '`organization_id` AS `organization_id`',
        '`response_row_count` AS `response_row_count`',
        '`active_response_row_count` AS `active_response_row_count`',
        '`empty_response_row_count` AS `empty_response_row_count`',
        '`invalid_response_row_count` AS `invalid_response_row_count`',
        '`matched_response_row_count` AS `matched_response_row_count`',
        '`unmatched_response_row_count` AS `unmatched_response_row_count`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'clinical_form': GoldRules(
        table='clinical_form',
        select=CLINICAL_FORM_SELECT,
        mandatory=CLINICAL_FORM_MANDATORY_RULES,
        advisory=CLINICAL_FORM_ADVISORY_RULES,
        person_scope=CLINICAL_FORM_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'person_id_resolved',
                             'encounter_id_resolved',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth')),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_form': {'columns': {'active_response_row_count': 'Number of active response rows.',
                                   'authored_datetime': 'Earliest form documentation timestamp.',
                                   'completed_datetime': 'Latest documentation or performed timestamp.',
                                   'confidentiality_code': 'Security classification when supplied.',
                                   'dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID; primary key '
                                                            'of this table.',
                                   'empty_response_row_count': 'Number of retained empty response rows.',
                                   'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                   'spine_encounter.encounter_id.',
                                   'event_datetime': 'First documented timestamp.',
                                   'event_datetime_local': 'Local-time representation of the first '
                                                           'documented timestamp for the form instance. '
                                                           'Gold: NULL wherever Gold nulls event_datetime.',
                                   'event_end_datetime': 'Last documented or performed timestamp.',
                                   'event_end_datetime_local': 'Local-time representation of the last '
                                                               'documented or performed timestamp for the '
                                                               'form instance. Gold: NULL wherever Gold '
                                                               'nulls event_end_datetime.',
                                   'form_status_code': 'Source form status code.',
                                   'form_status_display': 'Source form status display.',
                                   'form_type_code': 'Source form type code.',
                                   'form_type_display': 'Source form type display.',
                                   'invalid_response_row_count': 'Number of source-classified invalid '
                                                                 'response rows.',
                                   'loaded_at': 'Greatest of the form-level maximum ADC_UPDT values from '
                                                'mill_form_activity, map_powerform_assessment_item and '
                                                'map_powerform_assessment, and maximum Source_ADC_UPDT from '
                                                'map_mat_vte_assessment. This is the contributing-source '
                                                'ingestion/provenance clock, not the Silver refresh time or '
                                                'form event time; null only when every contributing clock is '
                                                'null.',
                                   'matched_response_row_count': 'Number of canonical matched response rows.',
                                   'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                      'organization.',
                                   'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                        'cross-feed join key.',
                                   'performed_practitioner_id': 'Millennium personnel PERSON_ID for the '
                                                                'performer.',
                                   'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                   'record_status': 'Derived PowerForm row status: retracted when the '
                                                    'grouped maximum of '
                                                    'map_powerform_assessment.SOURCE_PRESENT_IND is 0; '
                                                    'otherwise superseded when the grouped maximum of '
                                                    'ASSESSMENT_ACTIVE_IND is 0 or no mill_form_activity '
                                                    'response has ACTIVE_IND = 1; otherwise active. Missing '
                                                    'assessment flags alone do not trigger retraction or '
                                                    'supersession. This is a status label, not the raw '
                                                    'Boolean assessment flag.',
                                   'record_status_effective_from': 'Earliest '
                                                                   'mill_form_activity.DOCUMENTATION_DT_TM '
                                                                   'for the form instance, carried from '
                                                                   'authored_datetime. This is the form '
                                                                   'documentation start used by the research '
                                                                   'status projection, not an independently '
                                                                   'recorded status-change timestamp; null '
                                                                   'when all contributing documentation '
                                                                   'timestamps are null.',
                                   'record_status_effective_to': 'For a retracted or superseded form, '
                                                                 'completed_datetime: the maximum of '
                                                                 'coalesce(mill_form_activity.LAST_DOCUMENTED_DT_TM, '
                                                                 'mill_form_activity.PERFORMED_DT_TM) across '
                                                                 'its responses. Null for active forms or '
                                                                 'when no contributing completion timestamp '
                                                                 'is available. This is a '
                                                                 'documentation/completion boundary, not a '
                                                                 'Boolean assessment flag or an '
                                                                 'independently recorded status-change time.',
                                   'response_row_count': 'Number of response rows grouped into the form.',
                                   'source_code': 'Source form reference identifier.',
                                   'source_coding_system': 'Source form coding system.',
                                   'source_display': 'Source form description.',
                                   'source_feed': 'Registered owning feed.',
                                   'source_update_timestamp': "Maximum, across the form's mill_form_activity "
                                                              'rows, of the greatest ADC_UPDT, '
                                                              'SOURCE_ACTIVITY_ADC_UPDT, '
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
                       'quantity_override': True,
                       'table': 'Internal quality-controlled twin of clinical_form: the Gold repairs, nulls '
                                'and expectations are applied here and the published Gold MV reads this '
                                'object. Materialized rather than temporary so the published MV can refresh '
                                'incrementally.'},
 'clinical_form': {'columns': {'active_response_row_count': 'Number of active response rows.',
                               'authored_datetime': 'Earliest form documentation timestamp.',
                               'completed_datetime': 'Latest documentation or performed timestamp.',
                               'confidentiality_code': 'Security classification when supplied.',
                               'dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID; primary key of '
                                                        'this table.',
                               'empty_response_row_count': 'Number of retained empty response rows.',
                               'encounter_id': 'Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.',
                               'event_datetime': 'First documented timestamp.',
                               'event_datetime_local': 'Local-time representation of the first documented '
                                                       'timestamp for the form instance. Gold: NULL wherever '
                                                       'Gold nulls event_datetime.',
                               'event_end_datetime': 'Last documented or performed timestamp.',
                               'event_end_datetime_local': 'Local-time representation of the last documented '
                                                           'or performed timestamp for the form instance. '
                                                           'Gold: NULL wherever Gold nulls '
                                                           'event_end_datetime.',
                               'form_status_code': 'Source form status code.',
                               'form_status_display': 'Source form status display.',
                               'form_type_code': 'Source form type code.',
                               'form_type_display': 'Source form type display.',
                               'invalid_response_row_count': 'Number of source-classified invalid response '
                                                             'rows.',
                               'loaded_at': 'Greatest of the form-level maximum ADC_UPDT values from '
                                            'mill_form_activity, map_powerform_assessment_item and '
                                            'map_powerform_assessment, and maximum Source_ADC_UPDT from '
                                            'map_mat_vte_assessment. This is the contributing-source '
                                            'ingestion/provenance clock, not the Silver refresh time or form '
                                            'event time; null only when every contributing clock is null.',
                               'matched_response_row_count': 'Number of canonical matched response rows.',
                               'organization_id': 'Millennium ORGANIZATION_ID for the source organization.',
                               'patient_event_key': 'Deterministic SHA-256 event identity; stable cross-feed '
                                                    'join key.',
                               'performed_practitioner_id': 'Millennium personnel PERSON_ID for the '
                                                            'performer.',
                               'person_id': 'Millennium PERSON_ID; native person foreign key.',
                               'record_status': 'Derived PowerForm row status: retracted when the grouped '
                                                'maximum of map_powerform_assessment.SOURCE_PRESENT_IND is '
                                                '0; otherwise superseded when the grouped maximum of '
                                                'ASSESSMENT_ACTIVE_IND is 0 or no mill_form_activity '
                                                'response has ACTIVE_IND = 1; otherwise active. Missing '
                                                'assessment flags alone do not trigger retraction or '
                                                'supersession. This is a status label, not the raw Boolean '
                                                'assessment flag.',
                               'record_status_effective_from': 'Earliest '
                                                               'mill_form_activity.DOCUMENTATION_DT_TM for '
                                                               'the form instance, carried from '
                                                               'authored_datetime. This is the form '
                                                               'documentation start used by the research '
                                                               'status projection, not an independently '
                                                               'recorded status-change timestamp; null when '
                                                               'all contributing documentation timestamps '
                                                               'are null.',
                               'record_status_effective_to': 'For a retracted or superseded form, '
                                                             'completed_datetime: the maximum of '
                                                             'coalesce(mill_form_activity.LAST_DOCUMENTED_DT_TM, '
                                                             'mill_form_activity.PERFORMED_DT_TM) across its '
                                                             'responses. Null for active forms or when no '
                                                             'contributing completion timestamp is '
                                                             'available. This is a documentation/completion '
                                                             'boundary, not a Boolean assessment flag or an '
                                                             'independently recorded status-change time.',
                               'response_row_count': 'Number of response rows grouped into the form.',
                               'source_code': 'Source form reference identifier.',
                               'source_coding_system': 'Source form coding system.',
                               'source_display': 'Source form description.',
                               'source_feed': 'Registered owning feed.',
                               'source_update_timestamp': "Maximum, across the form's mill_form_activity "
                                                          'rows, of the greatest ADC_UPDT, '
                                                          'SOURCE_ACTIVITY_ADC_UPDT, SOURCE_EVENT_ADC_UPDT '
                                                          'and SOURCE_LABEL_ADC_UPDT. This combines the '
                                                          'contributing activity, event, label and '
                                                          'bronze-ingestion clocks; it is not uniformly a '
                                                          'native clinical-system update time. Null only '
                                                          'when every contributing clock is null.',
                               'subject_id_system': 'Identifier system used for subject_key.',
                               'subject_key': 'Deterministic SHA-256 over the strongest source identifier. '
                                              'Not salted, not secret; a stable join key across feeds where '
                                              'person_id is unresolved.',
                               'unmatched_response_row_count': 'Number of canonical unmatched response rows.',
                               'vip_ind': 'VIP indicator when supplied.',
                               'withheld_identity_ind': 'Withheld identity indicator.'},
                   'quantity_override': False,
                   'table': 'One completed or retained PowerForm instance with ordered response content. '
                            'Gold QC twin of the silver product: 8 columns are repaired or nulled, 1 rule(s) '
                            'drop rows, 4 check(s) are advisory. Each rule states its reason in the pipeline '
                            'notebook, and Lakeflow expectation metrics report what every rule matched on '
                            'each update.'}}
