"""Gold clinical.imaging_exam: 2 dataset(s).

- datasets: _qc_clinical_imaging_exam, clinical_imaging_exam
- moved verbatim from: 2_gold_clinical_events:2366, 2_gold_clinical_events:2443, 2_gold_clinical_events:2451, 2_gold_clinical_events:2634, 2_gold_clinical_events:2638, 2_gold_clinical_events:2651

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.imaging_exam ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_IMAGING_EXAM_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`event_id` AS `event_id`',
    '`pacs_examination_id` AS `pacs_examination_id`',
    '`organization_key` AS `organization_key`',
    '`source_object` AS `source_object`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    "CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`event_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_datetime` END END AS `event_datetime`",
    'CASE WHEN YEAR(CAST(`event_end_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    "CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`event_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_datetime` END END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`",  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN YEAR(CAST(`event_end_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`exam_code` AS `exam_code`',
    '`status_code` AS `status_code`',
    '`accession_identifier` AS `accession_identifier`',
    '`study_instance_uid` AS `study_instance_uid`',
    '`modality_code` AS `modality_code`',
    '`modality_dicom_code` AS `modality_dicom_code`',  # SDI_DICOM_MODALITY_V1
    '`modality_dicom_method` AS `modality_dicom_method`',  # SDI_DICOM_MODALITY_V1
    "CASE WHEN UPPER(TRIM(CAST(`body_site_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `body_site_code` END AS `body_site_code`",
    '`body_site_snomed_code` AS `body_site_snomed_code`',  # SDI_IMAGING_BODY_SITE_V1
    '`body_site_snomed_display` AS `body_site_snomed_display`',  # SDI_IMAGING_BODY_SITE_V1
    '`body_site_snomed_method` AS `body_site_snomed_method`',  # SDI_IMAGING_BODY_SITE_V1
    '`report_patient_event_key` AS `report_patient_event_key`',
    '`requester_practitioner_id` AS `requester_practitioner_id`',
    '`performer_practitioner_id` AS `performer_practitioner_id`',
    '`sectra_accession_number` AS `sectra_accession_number`',
    '`accession_format` AS `accession_format`',
    '`accession_parse_method` AS `accession_parse_method`',
    '`accession_rule_version` AS `accession_rule_version`',
    '`accession_evidence_level` AS `accession_evidence_level`',
    '`accession_identity_status` AS `accession_identity_status`',
    '`accession_candidate_count` AS `accession_candidate_count`',
    '`linked_pacs_examination_id` AS `linked_pacs_examination_id`',
    '`pacs_link_method` AS `pacs_link_method`',
    '`source_event_class_code` AS `source_event_class_code`',
    '`source_status_code` AS `source_status_code`',
    '`performed_datetime` AS `performed_datetime`',
    '`performed_datetime_source` AS `performed_datetime_source`',
    '`performed_evidence` AS `performed_evidence`',
    '`performed_evidence_verified_ind` AS `performed_evidence_verified_ind`',
    '`report_available_ind` AS `report_available_ind`',
    '`report_link_status` AS `report_link_status`',
    '`report_count` AS `report_count`',
    '`report_reference_count` AS `report_reference_count`',
    '`linked_report_event_id` AS `linked_report_event_id`',
    '`native_image_count` AS `native_image_count`',
    '`native_series_count` AS `native_series_count`',
    '`measured_series_count` AS `measured_series_count`',
    '`request_clinical_question` AS `request_clinical_question`',
    '`request_clinical_history` AS `request_clinical_history`',
    '`request_clinical_question_exam_segment` AS `request_clinical_question_exam_segment`',
    '`request_clinical_question_segment_status` AS `request_clinical_question_segment_status`',
    '`request_clinical_history_exam_segment` AS `request_clinical_history_exam_segment`',
    '`request_clinical_history_segment_status` AS `request_clinical_history_segment_status`',
    '`request_clinical_question_anonymised` AS `request_clinical_question_anonymised`',
    '`request_clinical_history_anonymised` AS `request_clinical_history_anonymised`',
    '`request_clinical_question_exam_segment_anonymised` AS `request_clinical_question_exam_segment_anonymised`',
    '`request_clinical_history_exam_segment_anonymised` AS `request_clinical_history_exam_segment_anonymised`',
    '`request_text_anonymisation_status` AS `request_text_anonymisation_status`',
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


# contract v2: QC/batch inputs come from internal _clinical_imaging_exam_metadata; source history stays on the main research table.
CLINICAL_IMAGING_EXAM_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 48,943,391 rows of
    # 50,008,147 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_IMAGING_EXAM_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.imaging_exam.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # INSR_W4 BBR date_ordering (design BBR-18): Radiology event_end_datetime is the clinical-event end clock (EVENT_END_DT_TM_CLEAN), not a measured exam end; it precedes the performed time on about 8% of radiology_event rows (BBR-18).
    'gold.clinical.imaging_exam.table.insr_bbr_imaging_end_before_start':
        "NOT COALESCE((`source_object` = 'radiology_event' AND `event_end_datetime` < `event_datetime`), FALSE)",
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 1,064,756 of 50,008,147 at the profile.
    "gold.clinical.imaging_exam.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 36,156 of 50,008,147 rows (0.0723%) when profiled on 2026-08-24.
    "gold.clinical.imaging_exam.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,247,681 of 50,008,147 rows (2.49%) when profiled on
    # 2026-08-24.
    "gold.clinical.imaging_exam.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # Left as a warning because it fires on 2,567,329 of 50,008,147 rows (5.13%) when
    # profiled on 2026-08-24 -- at that rate the rule's assumption about what event_datetime
    # and event_end_datetime mean is the thing in doubt, not the data. The inverted gaps are
    # mostly minutes, which reads as two clocks rather than two events in the wrong order.
    "gold.clinical.imaging_exam.table.ordering_violation_event_datetime_event_end_datetime":
        "NOT COALESCE((`event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime`), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_IMAGING_EXAM_PERSON_SCOPE_RULES = {
    "gold.clinical.imaging_exam.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_imaging_exam"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_IMAGING_EXAM_MANDATORY_RULES)
@dp.expect_all(CLINICAL_IMAGING_EXAM_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_IMAGING_EXAM_PERSON_SCOPE_RULES)
def _gold_qc_clinical_imaging_exam():
    """Quality-controlled twin of journey_clinical.imaging_exam."""
    df = _qc("clinical_imaging_exam", CLINICAL_IMAGING_EXAM_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.imaging_exam"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_imaging_exam():
    """Contract-v2 public twin of clinical_imaging_exam; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_imaging_exam")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`event_id` AS `event_id`',
        '`pacs_examination_id` AS `pacs_examination_id`',
        '`organization_key` AS `organization_key`',
        '`source_object` AS `source_object`',
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
        '`__gold_json_exam_code` AS `exam_code`',
        '`status_code` AS `status_code`',
        '`accession_identifier` AS `accession_identifier`',
        '`study_instance_uid` AS `study_instance_uid`',
        '`modality_code` AS `modality_code`',
        '`modality_dicom_code` AS `modality_dicom_code`',  # SDI_DICOM_MODALITY_V1
        '`modality_dicom_method` AS `modality_dicom_method`',  # SDI_DICOM_MODALITY_V1
        '`body_site_code` AS `body_site_code`',
        '`body_site_snomed_code` AS `body_site_snomed_code`',  # SDI_IMAGING_BODY_SITE_V1
        '`body_site_snomed_display` AS `body_site_snomed_display`',  # SDI_IMAGING_BODY_SITE_V1
        '`body_site_snomed_method` AS `body_site_snomed_method`',  # SDI_IMAGING_BODY_SITE_V1
        '`report_patient_event_key` AS `report_patient_event_key`',
        '`requester_practitioner_id` AS `requester_practitioner_id`',
        '`performer_practitioner_id` AS `performer_practitioner_id`',
        '`sectra_accession_number` AS `sectra_accession_number`',
        '`accession_format` AS `accession_format`',
        '`accession_parse_method` AS `accession_parse_method`',
        '`accession_rule_version` AS `accession_rule_version`',
        '`accession_evidence_level` AS `accession_evidence_level`',
        '`accession_identity_status` AS `accession_identity_status`',
        '`accession_candidate_count` AS `accession_candidate_count`',
        '`linked_pacs_examination_id` AS `linked_pacs_examination_id`',
        '`pacs_link_method` AS `pacs_link_method`',
        '`source_event_class_code` AS `source_event_class_code`',
        '`source_status_code` AS `source_status_code`',
        '`performed_datetime` AS `performed_datetime`',
        '`performed_datetime_source` AS `performed_datetime_source`',
        '`performed_evidence` AS `performed_evidence`',
        '`performed_evidence_verified_ind` AS `performed_evidence_verified_ind`',
        '`report_available_ind` AS `report_available_ind`',
        '`report_link_status` AS `report_link_status`',
        '`report_count` AS `report_count`',
        '`report_reference_count` AS `report_reference_count`',
        '`linked_report_event_id` AS `linked_report_event_id`',
        '`native_image_count` AS `native_image_count`',
        '`native_series_count` AS `native_series_count`',
        '`measured_series_count` AS `measured_series_count`',
        '`request_clinical_question` AS `request_clinical_question`',
        '`request_clinical_history` AS `request_clinical_history`',
        '`request_clinical_question_exam_segment` AS `request_clinical_question_exam_segment`',
        '`request_clinical_question_segment_status` AS `request_clinical_question_segment_status`',
        '`request_clinical_history_exam_segment` AS `request_clinical_history_exam_segment`',
        '`request_clinical_history_segment_status` AS `request_clinical_history_segment_status`',
        '`request_clinical_question_anonymised` AS `request_clinical_question_anonymised`',
        '`request_clinical_history_anonymised` AS `request_clinical_history_anonymised`',
        '`request_clinical_question_exam_segment_anonymised` AS `request_clinical_question_exam_segment_anonymised`',
        '`request_clinical_history_exam_segment_anonymised` AS `request_clinical_history_exam_segment_anonymised`',
        '`request_text_anonymisation_status` AS `request_text_anonymisation_status`',
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
    return _s3_flatten_gold_public(df, "clinical_imaging_exam")


def _public_steps_clinical_imaging_exam(df):
    """clinical_imaging_exam: the public body after its projection (GoldRules.public_select), moved from gold_clinical_imaging_exam; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_imaging_exam')


RULES = {
    'clinical_imaging_exam': GoldRules(
        table='clinical_imaging_exam',
        select=CLINICAL_IMAGING_EXAM_SELECT,
        mandatory=CLINICAL_IMAGING_EXAM_MANDATORY_RULES,
        advisory=CLINICAL_IMAGING_EXAM_ADVISORY_RULES,
        person_scope=CLINICAL_IMAGING_EXAM_PERSON_SCOPE_RULES,
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
        date_flags=('event_after_death_30d', 'event_before_birth'),
        variant_columns=('exam_code',),
        s3_axis_specs={'exam_code': [('exam', False)]},
        value_rules={'gold.clinical.imaging_exam.body_site_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                    'column': 'body_site_snomed_code',
                                                                                    'companions': ['body_site_snomed_display'],
                                                                                    'match': '`body_site_snomed_code` '
                                                                                             'IS NOT NULL '
                                                                                             'AND (NOT '
                                                                                             '`body_site_snomed_code` '
                                                                                             'RLIKE '
                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                             'OR '
                                                                                             'substring(`body_site_snomed_code`, '
                                                                                             'length(`body_site_snomed_code`) '
                                                                                             '- 2, 2) NOT '
                                                                                             "IN ('00', "
                                                                                             "'10') OR "
                                                                                             '(substring(`body_site_snomed_code`, '
                                                                                             'length(`body_site_snomed_code`) '
                                                                                             '- 2, 2) = '
                                                                                             "'10' AND "
                                                                                             'length(`body_site_snomed_code`) '
                                                                                             '< 11))',
                                                                                    'table': 'clinical_imaging_exam'},
         'gold.clinical.imaging_exam.exam_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                               'column': 'exam_snomed_code',
                                                                               'companions': ['exam_snomed_display',
                                                                                              'exam_target_code',
                                                                                              'exam_target_display'],
                                                                               'match': '`exam_snomed_code` '
                                                                                        'IS NOT NULL AND '
                                                                                        '(NOT '
                                                                                        '`exam_snomed_code` '
                                                                                        'RLIKE '
                                                                                        "'^[1-9][0-9]{5,17}$' "
                                                                                        'OR '
                                                                                        'substring(`exam_snomed_code`, '
                                                                                        'length(`exam_snomed_code`) '
                                                                                        '- 2, 2) NOT IN '
                                                                                        "('00', '10') OR "
                                                                                        '(substring(`exam_snomed_code`, '
                                                                                        'length(`exam_snomed_code`) '
                                                                                        "- 2, 2) = '10' "
                                                                                        'AND '
                                                                                        'length(`exam_snomed_code`) '
                                                                                        '< 11))',
                                                                               'table': 'clinical_imaging_exam'},
         'gold.clinical.imaging_exam.exam_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                               'column': 'exam_target_code',
                                                                               'companions': ['exam_target_display',
                                                                                              'exam_snomed_code',
                                                                                              'exam_snomed_display'],
                                                                               'match': '`exam_target_system` '
                                                                                        '= '
                                                                                        "'http://snomed.info/sct' "
                                                                                        'AND '
                                                                                        '`exam_target_code` '
                                                                                        'IS NOT NULL AND '
                                                                                        '(NOT '
                                                                                        '`exam_target_code` '
                                                                                        'RLIKE '
                                                                                        "'^[1-9][0-9]{5,17}$' "
                                                                                        'OR '
                                                                                        'substring(`exam_target_code`, '
                                                                                        'length(`exam_target_code`) '
                                                                                        '- 2, 2) NOT IN '
                                                                                        "('00', '10') OR "
                                                                                        '(substring(`exam_target_code`, '
                                                                                        'length(`exam_target_code`) '
                                                                                        "- 2, 2) = '10' "
                                                                                        'AND '
                                                                                        'length(`exam_target_code`) '
                                                                                        '< 11))',
                                                                               'table': 'clinical_imaging_exam'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`event_id` AS `event_id`',
            '`pacs_examination_id` AS `pacs_examination_id`',
            '`organization_key` AS `organization_key`',
            '`source_object` AS `source_object`',
            '`subject_key` AS `subject_key`',
            '`subject_id_system` AS `subject_id_system`',
            '`person_id` AS `person_id`',
            '`encounter_id` AS `encounter_id`',
            '`event_datetime` AS `event_datetime`',
            '`event_end_datetime` AS `event_end_datetime`',
            '`event_datetime_local` AS `event_datetime_local`',
            '`event_end_datetime_local` AS `event_end_datetime_local`',
            '`source_coding_system` AS `source_coding_system`',
            '`source_code` AS `source_code`',
            '`source_display` AS `source_display`',
            '`__gold_json_exam_code` AS `exam_code`',
            '`status_code` AS `status_code`',
            '`accession_identifier` AS `accession_identifier`',
            '`study_instance_uid` AS `study_instance_uid`',
            '`modality_code` AS `modality_code`',
            '`modality_dicom_code` AS `modality_dicom_code`',
            '`modality_dicom_method` AS `modality_dicom_method`',
            '`body_site_code` AS `body_site_code`',
            '`body_site_snomed_code` AS `body_site_snomed_code`',
            '`body_site_snomed_display` AS `body_site_snomed_display`',
            '`body_site_snomed_method` AS `body_site_snomed_method`',
            '`report_patient_event_key` AS `report_patient_event_key`',
            '`requester_practitioner_id` AS `requester_practitioner_id`',
            '`performer_practitioner_id` AS `performer_practitioner_id`',
            '`sectra_accession_number` AS `sectra_accession_number`',
            '`accession_format` AS `accession_format`',
            '`accession_parse_method` AS `accession_parse_method`',
            '`accession_rule_version` AS `accession_rule_version`',
            '`accession_evidence_level` AS `accession_evidence_level`',
            '`accession_identity_status` AS `accession_identity_status`',
            '`accession_candidate_count` AS `accession_candidate_count`',
            '`linked_pacs_examination_id` AS `linked_pacs_examination_id`',
            '`pacs_link_method` AS `pacs_link_method`',
            '`source_event_class_code` AS `source_event_class_code`',
            '`source_status_code` AS `source_status_code`',
            '`performed_datetime` AS `performed_datetime`',
            '`performed_datetime_source` AS `performed_datetime_source`',
            '`performed_evidence` AS `performed_evidence`',
            '`performed_evidence_verified_ind` AS `performed_evidence_verified_ind`',
            '`report_available_ind` AS `report_available_ind`',
            '`report_link_status` AS `report_link_status`',
            '`report_count` AS `report_count`',
            '`report_reference_count` AS `report_reference_count`',
            '`linked_report_event_id` AS `linked_report_event_id`',
            '`native_image_count` AS `native_image_count`',
            '`native_series_count` AS `native_series_count`',
            '`measured_series_count` AS `measured_series_count`',
            '`request_clinical_question` AS `request_clinical_question`',
            '`request_clinical_history` AS `request_clinical_history`',
            '`request_clinical_question_exam_segment` AS `request_clinical_question_exam_segment`',
            '`request_clinical_question_segment_status` AS `request_clinical_question_segment_status`',
            '`request_clinical_history_exam_segment` AS `request_clinical_history_exam_segment`',
            '`request_clinical_history_segment_status` AS `request_clinical_history_segment_status`',
            '`request_clinical_question_anonymised` AS `request_clinical_question_anonymised`',
            '`request_clinical_history_anonymised` AS `request_clinical_history_anonymised`',
            '`request_clinical_question_exam_segment_anonymised` AS `request_clinical_question_exam_segment_anonymised`',
            '`request_clinical_history_exam_segment_anonymised` AS `request_clinical_history_exam_segment_anonymised`',
            '`request_text_anonymisation_status` AS `request_text_anonymisation_status`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`source_feed` AS `source_feed`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
        ],
        public_steps=(_public_steps_clinical_imaging_exam,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_imaging_exam': {'columns': {'accession_candidate_count': 'PACS: published examinations '
                                                                        'sharing the accession. Millennium: '
                                                                        'present PACS examinations sharing '
                                                                        'the parsed accession before code '
                                                                        'and identity filtering.',
                                           'accession_evidence_level': 'pacs_request (PACS request '
                                                                       'accession), '
                                                                       'cerner_reference_pacs_linked (Cerner '
                                                                       'reference resolved to one PACS '
                                                                       'examination), '
                                                                       'cerner_reference_parsed (Cerner-only '
                                                                       'parsed reference), or NULL when no '
                                                                       'accession.',
                                           'accession_format': 'Bronze accession-rule format class '
                                                               '(SECTRA_16, SITE_16, RNH_14, OTHER_SITE_14, '
                                                               'NUMERIC_16, LEGACY_7_8, LEGACY_6, '
                                                               'NUMERIC_OTHER, OTHER or UNRECOGNISED).',
                                           'accession_identifier': 'Imaging accession identifier, unchanged: '
                                                                   'PACS MILL_LINK_REF, Millennium '
                                                                   'REFERENCE_NBR. For the Barts/Sectra '
                                                                   'extraction accession use '
                                                                   'sectra_accession_number.',
                                           'accession_identity_status': 'Row-level accession identity '
                                                                        'evidence. PACS: '
                                                                        'ACCESSION_IDENTITY_STATUS over the '
                                                                        "accession's PACS examinations. "
                                                                        'Millennium: CONFLICTING_PERSONS '
                                                                        'when the PACS link was refused for '
                                                                        'identity conflict, otherwise NULL. '
                                                                        'The cross-arm accession status is '
                                                                        'on '
                                                                        'clinical_imaging_accession_member.',
                                           'accession_parse_method': 'PACS: ACCESSION_PARSE_METHOD. '
                                                                     'Millennium: REF_PARSE_METHOD. '
                                                                     'Provenance of sectra_accession_number.',
                                           'accession_rule_version': 'Version of the shared bronze accession '
                                                                     'rules that produced '
                                                                     'sectra_accession_number.',
                                           'body_site_code': 'Source body-site value as recorded (PACS '
                                                             'BODY_PART); not a SNOMED code.',
                                           'body_site_snomed_code': 'SNOMED CT body structure derived from '
                                                                    'the mapped examination concept '
                                                                    '(exam_omop_concept_id) through the OMOP '
                                                                    'procedure-site relationships: Has dir '
                                                                    'proc site, else Has proc site, else Has '
                                                                    'indir proc site; the lowest concept id '
                                                                    'when several share that rank.',
                                           'body_site_snomed_display': 'SNOMED CT preferred name for '
                                                                       'body_site_snomed_code.',
                                           'body_site_snomed_method': 'omop_has_dir_proc_site | '
                                                                      'omop_has_proc_site | '
                                                                      'omop_has_indir_proc_site, with a '
                                                                      '_multiple suffix when the examination '
                                                                      'names several sites at that rank '
                                                                      '(only one is published). NULL when '
                                                                      'the examination has no mapped concept '
                                                                      'or no site relationship.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each imaging '
                                                                   'exam record. It is produced by the '
                                                                   'silver transformation and has no direct '
                                                                   'bronze-column lineage entry. Source code '
                                                                   'meanings and sentinel values are '
                                                                   'retained unless the pipeline explicitly '
                                                                   'maps them; null means no code was '
                                                                   'supplied.',
                                           'encounter_id': 'Millennium ENCNTR_ID when supplied; native '
                                                           'encounter foreign key.',
                                           'event_datetime': 'Imaging study start timestamp.',
                                           'event_datetime_local': 'Imaging study start timestamp expressed '
                                                                   'in local time. Gold: NULL wherever Gold '
                                                                   'nulls event_datetime.',
                                           'event_end_datetime': 'Imaging study end timestamp.',
                                           'event_end_datetime_local': 'Imaging study end timestamp '
                                                                       'expressed in local time. Gold: NULL '
                                                                       'wherever Gold nulls '
                                                                       'event_end_datetime.',
                                           'event_id': 'Millennium radiology EVENT_ID; native key for the '
                                                       'millennium arm.',
                                           'exam_code': 'Source and mapped imaging-exam CodeableConcept.',
                                           'linked_pacs_examination_id': 'PACS examination this row resolves '
                                                                         'to. PACS rows: their own '
                                                                         'PACS_EXAMINATION_ID. Millennium '
                                                                         'rows: '
                                                                         'map_radiology_event.PACS_EXAMINATION_ID, '
                                                                         'set only for a unique '
                                                                         'accession+code or unique '
                                                                         'accession-only match.',
                                           'linked_report_event_id': 'Millennium: RADIOLOGY_REPORT_EVENT_ID, '
                                                                     'the latest identity-compatible report '
                                                                     'document event. A convenience pointer; '
                                                                     'other reports stay relational. NULL '
                                                                     'for PACS rows.',
                                           'loaded_at': 'PACS: map_pacs_examination.ADC_UPDT. Millennium: '
                                                        'map_radiology_event.PIPELINE_UPDT_DT_TM, not its '
                                                        'ADC_UPDT. These are arm-specific load/processing '
                                                        'clocks, carried unchanged through the union and '
                                                        'QC/public projections; no report or cross-arm clock '
                                                        'maximum is taken and the current Silver refresh '
                                                        'time is not substituted.',
                                           'measured_series_count': 'SERIES_COUNT_MEASURED from the raw '
                                                                    'series table. The series feed has a '
                                                                    'separate clock and ends 2024-11-02; '
                                                                    'NULL after that.',
                                           'modality_code': 'Local modality label as recorded: PACS '
                                                            'MODALITY, or the Millennium NHS Imaging '
                                                            'category (NHSI_MODALITY_CATEGORY). Not a DICOM '
                                                            'code; see modality_dicom_code.',
                                           'modality_dicom_code': 'DICOM Modality (0008,0060) defined term. '
                                                                  'PACS: MODALITY when it is a DICOM term. '
                                                                  "Millennium: the linked PACS examination's "
                                                                  'acquisition modality, else the bronze '
                                                                  'NHSI-category inference '
                                                                  '(map_radiology_event.MODALITY_DICOM). '
                                                                  'NULL when no evidence names a single '
                                                                  'modality.',
                                           'modality_dicom_method': 'How modality_dicom_code was set: pacs '
                                                                    '(PACS MODALITY is a DICOM term), '
                                                                    'pacs_non_dicom (PACS MODALITY is a site '
                                                                    'label, e.g. Pano/XDEXA; no code), '
                                                                    'pacs_link (Millennium exam, linked PACS '
                                                                    'examination), dicom_passthrough / '
                                                                    'nhsi_category_map / nhsi_category_mixed '
                                                                    '/ unmapped (bronze NHSI-category '
                                                                    'inference). NULL when no modality was '
                                                                    'recorded.',
                                           'native_image_count': 'Native Sectra IMAGE_COUNT (sparsely '
                                                                 'populated; PACS feed clock). Not evidence '
                                                                 'of present retrievability.',
                                           'native_series_count': 'Native Sectra SERIES_COUNT (sparsely '
                                                                  'populated).',
                                           'organization_key': 'Nullable deterministic organization key; no '
                                                               'organization expression is present in the '
                                                               'imaging source cells.',
                                           'pacs_examination_id': 'PACS_EXAMINATION_ID; native key for the '
                                                                  'pacs arm.',
                                           'pacs_link_method': 'NATIVE_PACS_EXAMINATION for PACS rows; the '
                                                               'bronze PACS_LINK_METHOD for Millennium rows. '
                                                               'Ambiguous and conflicting candidates are '
                                                               'never resolved by choice.',
                                           'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                'stable cross-feed join key.',
                                           'performed_datetime': 'Valid performed time. PACS: '
                                                                 'PERFORMED_DT_TM_CLEAN (examination time, '
                                                                 'else the documented arrival fallback; '
                                                                 'future and 2099 sentinel values are NULL). '
                                                                 'Millennium: PERFORMED_DT_TM_CLEAN. Never a '
                                                                 'scheduled, request, report or load time. '
                                                                 'event_datetime keeps its existing meaning.',
                                           'performed_datetime_source': 'Which clean clock '
                                                                        'performed_datetime came from '
                                                                        '(EXAMINATION_DT_TM, ARRIVAL_DT_TM '
                                                                        'or PERFORMED_DT_TM).',
                                           'performed_evidence': 'PACS: bronze PERFORMED_EVIDENCE set '
                                                                 '(STATUS, REPORT, IMAGES, SERIES). '
                                                                 'Millennium: RESULT_STATUS for an '
                                                                 'examination event whose status is not '
                                                                 'cancelled, in error or not done.',
                                           'performed_evidence_verified_ind': 'True only for verified '
                                                                              'performed evidence. PACS '
                                                                              'status 40 (booking) and '
                                                                              'dangling-only report links '
                                                                              'are not evidence. Millennium: '
                                                                              'examination event with a '
                                                                              'performed result status.',
                                           'performer_practitioner_id': 'Nullable Millennium personnel '
                                                                        'PERSON_ID for the performer.',
                                           'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                        'foreign key.',
                                           'record_status': 'PACS rows are retracted when '
                                                            'map_pacs_examination.SOURCE_PRESENT_IND is '
                                                            'false, otherwise active; missing presence does '
                                                            'not retract. Millennium rows are superseded '
                                                            'when map_radiology_event.IN_ERROR_IND is true, '
                                                            'otherwise active; missing error defaults false. '
                                                            'The arms are unioned without reconciling their '
                                                            'labels; a Millennium validity end does not '
                                                            'itself supersede a row.',
                                           'record_status_effective_from': 'PACS: '
                                                                           'map_pacs_examination.SRC_ADC_UPDT. '
                                                                           'Millennium: '
                                                                           'map_radiology_event.VALID_FROM_DT_TM_CLEAN. '
                                                                           'Each arm carries its value '
                                                                           'through the union and QC/public '
                                                                           'projections; no cross-arm '
                                                                           'maximum or clinical-event '
                                                                           'timestamp is substituted.',
                                           'record_status_effective_to': 'PACS: '
                                                                         'map_pacs_examination.ADC_UPDT only '
                                                                         'when source presence is false, '
                                                                         'otherwise null. Millennium: '
                                                                         'UPDT_DT_TM when IN_ERROR_IND is '
                                                                         'true, otherwise '
                                                                         'VALID_UNTIL_DT_TM_CLEAN directly, '
                                                                         'including a finite or sentinel end '
                                                                         'on an active row. No sentinel '
                                                                         'filtering or fallback between '
                                                                         'Millennium branches is applied.',
                                           'report_available_ind': 'PACS: REPORT_AVAILABLE_IND (at least one '
                                                                   'resolved report; a dangling report id '
                                                                   'never sets it). Millennium: a report '
                                                                   'document event is linked through '
                                                                   'REF_EXAM_KEY.',
                                           'report_count': 'PACS: REPORT_COUNT_RESOLVED. Millennium: '
                                                           'identity-compatible report document events '
                                                           'sharing REF_EXAM_KEY.',
                                           'report_link_status': 'PACS: RESOLVED, DANGLING_ONLY or NONE. '
                                                                 'Millennium: REPORT_LINK_STATUS (SELF, '
                                                                 'SIBLING, SIBLING_LATEST_OF_MULTIPLE, '
                                                                 'AMBIGUOUS or NONE). All links are in '
                                                                 'clinical_imaging_report_link.',
                                           'report_patient_event_key': 'Deterministic SHA-256 key of the '
                                                                       'linked report event.',
                                           'report_reference_count': 'PACS: REPORT_REF_COUNT, including '
                                                                     'dangling report ids. NULL for '
                                                                     'Millennium rows.',
                                           'request_clinical_history': 'PACS request clinical information '
                                                                       '(CLINICAL_ANAMNESIS), verbatim and '
                                                                       'request-wide. Identifiable free text '
                                                                       '(ig_risk 4).',
                                           'request_clinical_history_anonymised': 'Approved-lane anonymised '
                                                                                  'clinical history; same '
                                                                                  'rule as '
                                                                                  'request_clinical_question_anonymised.',
                                           'request_clinical_history_exam_segment': 'The clinical-history '
                                                                                    'section headed by this '
                                                                                    "examination's code; set "
                                                                                    'only when its segment '
                                                                                    'status is EXAM_SEGMENT. '
                                                                                    'Identifiable free text '
                                                                                    '(ig_risk 4).',
                                           'request_clinical_history_exam_segment_anonymised': 'Approved-lane '
                                                                                               'anonymised '
                                                                                               'examination-specific '
                                                                                               'history '
                                                                                               'segment; '
                                                                                               'same rule.',
                                           'request_clinical_history_segment_status': 'Same vocabulary as '
                                                                                      'request_clinical_question_segment_status.',
                                           'request_clinical_question': 'PACS request clinical question, '
                                                                        'verbatim and request-wide. '
                                                                        'Identifiable free text (ig_risk 4); '
                                                                        'never copied into accession '
                                                                        'summaries or anonymised serving '
                                                                        'projections.',
                                           'request_clinical_question_anonymised': 'Approved-lane anonymised '
                                                                                   'clinical question. NULL '
                                                                                   'unless the registered '
                                                                                   'request-text feed '
                                                                                   'reports anonymized for '
                                                                                   'the current source text '
                                                                                   'hash. Never falls back '
                                                                                   'to raw text or a '
                                                                                   "report's anonymised "
                                                                                   'text.',
                                           'request_clinical_question_exam_segment': 'The clinical-question '
                                                                                     'section headed by this '
                                                                                     "examination's code; "
                                                                                     'set only when its '
                                                                                     'segment status is '
                                                                                     'EXAM_SEGMENT. '
                                                                                     'Identifiable free text '
                                                                                     '(ig_risk 4).',
                                           'request_clinical_question_exam_segment_anonymised': 'Approved-lane '
                                                                                                'anonymised '
                                                                                                'examination-specific '
                                                                                                'question '
                                                                                                'segment; '
                                                                                                'same rule.',
                                           'request_clinical_question_segment_status': 'EXAM_SEGMENT, '
                                                                                       'NOT_SEGMENTED (text '
                                                                                       'is request-wide), '
                                                                                       'CODE_NOT_IN_SEGMENTS, '
                                                                                       'DUPLICATE_SEGMENT_CODE, '
                                                                                       'DUPLICATE_EXAM_CODE_ON_REQUEST, '
                                                                                       'NO_EXAM_CODE or '
                                                                                       'NO_TEXT.',
                                           'request_text_anonymisation_status': 'not_registered (feed not '
                                                                                'yet deployed), current '
                                                                                '(anonymized for the current '
                                                                                'text), stale (text changed '
                                                                                'since redaction), pending, '
                                                                                'failed, unresolved_person, '
                                                                                'or no_text. Millennium rows '
                                                                                'are NULL.',
                                           'requester_practitioner_id': 'Nullable Millennium personnel '
                                                                        'PERSON_ID for the requester.',
                                           'sectra_accession_number': 'Barts/Sectra extraction accession. '
                                                                      'PACS: trimmed REQUEST_ID_STRING '
                                                                      '(map_pacs_examination.SECTRA_ACCESSION_NBR). '
                                                                      'Millennium: REFERENCE_NBR parsed by '
                                                                      'the shared bronze accession rules. '
                                                                      'Scopes an extraction request: one '
                                                                      'accession can hold several '
                                                                      'examinations and study UIDs. Leading '
                                                                      'zeros preserved. Distinct from '
                                                                      'accession_identifier, which keeps its '
                                                                      'existing meaning. Identifier.',
                                           'source_code': 'Source exam code',
                                           'source_coding_system': 'Source exam coding system.',
                                           'source_display': 'Source exam display.',
                                           'source_event_class_code': 'Millennium EVENT_CLASS_CD (234 '
                                                                      'examination, 224 report document, 231 '
                                                                      'section); NULL for PACS rows.',
                                           'source_feed': 'Registered PACS or Millennium radiology source '
                                                          'feed.',
                                           'source_object': 'Native source arm: millennium or pacs.',
                                           'source_status_code': 'Native status. PACS: '
                                                                 'EXAMINATION_STATUS_CD. Millennium: '
                                                                 'RESULT_STATUS_CD.',
                                           'source_update_timestamp': 'PACS: '
                                                                      'map_pacs_examination.SRC_ADC_UPDT. '
                                                                      'Millennium: '
                                                                      'map_radiology_event.UPDT_DT_TM. These '
                                                                      'distinct clocks are carried per arm '
                                                                      'through the union, not combined as a '
                                                                      'maximum. Neither joined report clocks '
                                                                      'nor the current Silver refresh time '
                                                                      'replaces them.',
                                           'status_code': 'Imaging study status.',
                                           'study_instance_uid': 'DICOM study instance UID.',
                                           'subject_id_system': 'Identifier system used for subject_key.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each imaging exam record. It is produced '
                                                      'by the silver transformation and has no direct '
                                                      'bronze-column lineage entry. Null means the source '
                                                      'did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Identity-withholding status is not '
                                                                    'populated by the current source '
                                                                    'projections; null does not mean '
                                                                    'identity was not withheld.'},
                               'quantity_override': True,
                               'table': 'Internal quality-controlled twin of clinical_imaging_exam: the Gold '
                                        'repairs, nulls and expectations are applied here and the published '
                                        'Gold MV reads this object. Materialized rather than temporary so '
                                        'the published MV can refresh incrementally.'},
 'clinical_imaging_exam': {'columns': {'accession_candidate_count': 'PACS: published examinations sharing '
                                                                    'the accession. Millennium: present PACS '
                                                                    'examinations sharing the parsed '
                                                                    'accession before code and identity '
                                                                    'filtering.',
                                       'accession_evidence_level': 'pacs_request (PACS request accession), '
                                                                   'cerner_reference_pacs_linked (Cerner '
                                                                   'reference resolved to one PACS '
                                                                   'examination), cerner_reference_parsed '
                                                                   '(Cerner-only parsed reference), or NULL '
                                                                   'when no accession.',
                                       'accession_format': 'Bronze accession-rule format class (SECTRA_16, '
                                                           'SITE_16, RNH_14, OTHER_SITE_14, NUMERIC_16, '
                                                           'LEGACY_7_8, LEGACY_6, NUMERIC_OTHER, OTHER or '
                                                           'UNRECOGNISED).',
                                       'accession_identifier': 'Imaging accession identifier, unchanged: '
                                                               'PACS MILL_LINK_REF, Millennium '
                                                               'REFERENCE_NBR. For the Barts/Sectra '
                                                               'extraction accession use '
                                                               'sectra_accession_number.',
                                       'accession_identity_status': 'Row-level accession identity evidence. '
                                                                    'PACS: ACCESSION_IDENTITY_STATUS over '
                                                                    "the accession's PACS examinations. "
                                                                    'Millennium: CONFLICTING_PERSONS when '
                                                                    'the PACS link was refused for identity '
                                                                    'conflict, otherwise NULL. The cross-arm '
                                                                    'accession status is on '
                                                                    'clinical_imaging_accession_member.',
                                       'accession_parse_method': 'PACS: ACCESSION_PARSE_METHOD. Millennium: '
                                                                 'REF_PARSE_METHOD. Provenance of '
                                                                 'sectra_accession_number.',
                                       'accession_rule_version': 'Version of the shared bronze accession '
                                                                 'rules that produced '
                                                                 'sectra_accession_number.',
                                       'body_site_code': 'Source body-site value as recorded (PACS '
                                                         'BODY_PART); not a SNOMED code.',
                                       'body_site_snomed_code': 'SNOMED CT body structure derived from the '
                                                                'mapped examination concept '
                                                                '(exam_omop_concept_id) through the OMOP '
                                                                'procedure-site relationships: Has dir proc '
                                                                'site, else Has proc site, else Has indir '
                                                                'proc site; the lowest concept id when '
                                                                'several share that rank.',
                                       'body_site_snomed_display': 'SNOMED CT preferred name for '
                                                                   'body_site_snomed_code.',
                                       'body_site_snomed_method': 'omop_has_dir_proc_site | '
                                                                  'omop_has_proc_site | '
                                                                  'omop_has_indir_proc_site, with a '
                                                                  '_multiple suffix when the examination '
                                                                  'names several sites at that rank (only '
                                                                  'one is published). NULL when the '
                                                                  'examination has no mapped concept or no '
                                                                  'site relationship.',
                                       'confidentiality_code': 'Source confidentiality classification '
                                                               'attached to the record for each imaging exam '
                                                               'record. It is produced by the silver '
                                                               'transformation and has no direct '
                                                               'bronze-column lineage entry. Source code '
                                                               'meanings and sentinel values are retained '
                                                               'unless the pipeline explicitly maps them; '
                                                               'null means no code was supplied.',
                                       'encounter_id': 'Millennium ENCNTR_ID when the source names the '
                                                       'encounter; NULL otherwise.',
                                       'event_datetime': 'Imaging study start timestamp.',
                                       'event_datetime_local': 'Imaging study start timestamp expressed in '
                                                               'local time. Gold: NULL wherever Gold nulls '
                                                               'event_datetime.',
                                       'event_end_datetime': 'Millennium EVENT_END_DT_TM_CLEAN, the '
                                                             'clinical-event end clock, not a measured exam '
                                                             'end: equal to the performed time on about 29% '
                                                             'and earlier than it on about 8% of '
                                                             'radiology_event rows (warn rule '
                                                             'insr_bbr_imaging_end_before_start); PACS has '
                                                             'no end (NULL).',
                                       'event_end_datetime_local': 'Imaging study end timestamp expressed in '
                                                                   'local time. Gold: NULL wherever Gold '
                                                                   'nulls event_end_datetime.',
                                       'event_id': 'Millennium radiology EVENT_ID; native key for the '
                                                   'millennium arm.',
                                       'exam_code': 'Source and mapped imaging-exam CodeableConcept.',
                                       'linked_pacs_examination_id': 'PACS examination this row resolves to. '
                                                                     'PACS rows: their own '
                                                                     'PACS_EXAMINATION_ID. Millennium rows: '
                                                                     'map_radiology_event.PACS_EXAMINATION_ID, '
                                                                     'set only for a unique accession+code '
                                                                     'or unique accession-only match.',
                                       'linked_report_event_id': 'Millennium: RADIOLOGY_REPORT_EVENT_ID, the '
                                                                 'latest identity-compatible report document '
                                                                 'event. A convenience pointer; other '
                                                                 'reports stay relational. NULL for PACS '
                                                                 'rows.',
                                       'loaded_at': 'PACS: map_pacs_examination.ADC_UPDT. Millennium: '
                                                    'map_radiology_event.PIPELINE_UPDT_DT_TM, not its '
                                                    'ADC_UPDT. These are arm-specific load/processing '
                                                    'clocks, carried unchanged through the union and '
                                                    'QC/public projections; no report or cross-arm clock '
                                                    'maximum is taken and the current Silver refresh time is '
                                                    'not substituted.',
                                       'measured_series_count': 'SERIES_COUNT_MEASURED from the raw series '
                                                                'table. The series feed has a separate clock '
                                                                'and ends 2024-11-02; NULL after that.',
                                       'modality_code': 'Local modality label as recorded: PACS MODALITY, or '
                                                        'the Millennium NHS Imaging category '
                                                        '(NHSI_MODALITY_CATEGORY). Not a DICOM code; see '
                                                        'modality_dicom_code.',
                                       'modality_dicom_code': 'DICOM Modality (0008,0060) defined term. '
                                                              'PACS: MODALITY when it is a DICOM term. '
                                                              "Millennium: the linked PACS examination's "
                                                              'acquisition modality, else the bronze '
                                                              'NHSI-category inference '
                                                              '(map_radiology_event.MODALITY_DICOM). NULL '
                                                              'when no evidence names a single modality.',
                                       'modality_dicom_method': 'How modality_dicom_code was set: pacs (PACS '
                                                                'MODALITY is a DICOM term), pacs_non_dicom '
                                                                '(PACS MODALITY is a site label, e.g. '
                                                                'Pano/XDEXA; no code), pacs_link (Millennium '
                                                                'exam, linked PACS examination), '
                                                                'dicom_passthrough / nhsi_category_map / '
                                                                'nhsi_category_mixed / unmapped (bronze '
                                                                'NHSI-category inference). NULL when no '
                                                                'modality was recorded.',
                                       'native_image_count': 'Native Sectra IMAGE_COUNT (sparsely populated; '
                                                             'PACS feed clock). Not evidence of present '
                                                             'retrievability.',
                                       'native_series_count': 'Native Sectra SERIES_COUNT (sparsely '
                                                              'populated).',
                                       'organization_key': 'Nullable deterministic organization key; no '
                                                           'organization expression is present in the '
                                                           'imaging source cells.',
                                       'pacs_examination_id': 'PACS_EXAMINATION_ID; native key for the pacs '
                                                              'arm.',
                                       'pacs_link_method': 'NATIVE_PACS_EXAMINATION for PACS rows; the '
                                                           'bronze PACS_LINK_METHOD for Millennium rows. '
                                                           'Ambiguous and conflicting candidates are never '
                                                           'resolved by choice.',
                                       'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                            'cross-feed join key.',
                                       'performed_datetime': 'Valid performed time. PACS: '
                                                             'PERFORMED_DT_TM_CLEAN (examination time, else '
                                                             'the documented arrival fallback; future and '
                                                             '2099 sentinel values are NULL). Millennium: '
                                                             'PERFORMED_DT_TM_CLEAN. Never a scheduled, '
                                                             'request, report or load time. event_datetime '
                                                             'keeps its existing meaning.',
                                       'performed_datetime_source': 'Which clean clock performed_datetime '
                                                                    'came from (EXAMINATION_DT_TM, '
                                                                    'ARRIVAL_DT_TM or PERFORMED_DT_TM).',
                                       'performed_evidence': 'PACS: bronze PERFORMED_EVIDENCE set (STATUS, '
                                                             'REPORT, IMAGES, SERIES). Millennium: '
                                                             'RESULT_STATUS for an examination event whose '
                                                             'status is not cancelled, in error or not done.',
                                       'performed_evidence_verified_ind': 'True only for verified performed '
                                                                          'evidence. PACS status 40 '
                                                                          '(booking) and dangling-only '
                                                                          'report links are not evidence. '
                                                                          'Millennium: examination event '
                                                                          'with a performed result status.',
                                       'performer_practitioner_id': 'Nullable Millennium personnel PERSON_ID '
                                                                    'for the performer.',
                                       'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                    'foreign key.',
                                       'record_status': 'PACS rows are retracted when '
                                                        'map_pacs_examination.SOURCE_PRESENT_IND is false, '
                                                        'otherwise active; missing presence does not '
                                                        'retract. Millennium rows are superseded when '
                                                        'map_radiology_event.IN_ERROR_IND is true, otherwise '
                                                        'active; missing error defaults false. The arms are '
                                                        'unioned without reconciling their labels; a '
                                                        'Millennium validity end does not itself supersede a '
                                                        'row.',
                                       'record_status_effective_from': 'PACS: '
                                                                       'map_pacs_examination.SRC_ADC_UPDT. '
                                                                       'Millennium: '
                                                                       'map_radiology_event.VALID_FROM_DT_TM_CLEAN. '
                                                                       'Each arm carries its value through '
                                                                       'the union and QC/public projections; '
                                                                       'no cross-arm maximum or '
                                                                       'clinical-event timestamp is '
                                                                       'substituted.',
                                       'record_status_effective_to': 'PACS: map_pacs_examination.ADC_UPDT '
                                                                     'only when source presence is false, '
                                                                     'otherwise null. Millennium: UPDT_DT_TM '
                                                                     'when IN_ERROR_IND is true, otherwise '
                                                                     'VALID_UNTIL_DT_TM_CLEAN directly, '
                                                                     'including a finite or sentinel end on '
                                                                     'an active row. No sentinel filtering '
                                                                     'or fallback between Millennium '
                                                                     'branches is applied.',
                                       'report_available_ind': 'PACS: REPORT_AVAILABLE_IND (at least one '
                                                               'resolved report; a dangling report id never '
                                                               'sets it). Millennium: a report document '
                                                               'event is linked through REF_EXAM_KEY.',
                                       'report_count': 'PACS: REPORT_COUNT_RESOLVED. Millennium: '
                                                       'identity-compatible report document events sharing '
                                                       'REF_EXAM_KEY.',
                                       'report_link_status': 'PACS: RESOLVED, DANGLING_ONLY or NONE. '
                                                             'Millennium: REPORT_LINK_STATUS (SELF, SIBLING, '
                                                             'SIBLING_LATEST_OF_MULTIPLE, AMBIGUOUS or '
                                                             'NONE). All links are in '
                                                             'clinical_imaging_report_link.',
                                       'report_patient_event_key': 'Deterministic SHA-256 key of the linked '
                                                                   'report event.',
                                       'report_reference_count': 'PACS: REPORT_REF_COUNT, including dangling '
                                                                 'report ids. NULL for Millennium rows.',
                                       'request_clinical_history': 'PACS request clinical information '
                                                                   '(CLINICAL_ANAMNESIS), verbatim and '
                                                                   'request-wide. Identifiable free text '
                                                                   '(ig_risk 4).',
                                       'request_clinical_history_anonymised': 'Approved-lane anonymised '
                                                                              'clinical history; same rule '
                                                                              'as '
                                                                              'request_clinical_question_anonymised.',
                                       'request_clinical_history_exam_segment': 'The clinical-history '
                                                                                'section headed by this '
                                                                                "examination's code; set "
                                                                                'only when its segment '
                                                                                'status is EXAM_SEGMENT. '
                                                                                'Identifiable free text '
                                                                                '(ig_risk 4).',
                                       'request_clinical_history_exam_segment_anonymised': 'Approved-lane '
                                                                                           'anonymised '
                                                                                           'examination-specific '
                                                                                           'history segment; '
                                                                                           'same rule.',
                                       'request_clinical_history_segment_status': 'Same vocabulary as '
                                                                                  'request_clinical_question_segment_status.',
                                       'request_clinical_question': 'PACS request clinical question, '
                                                                    'verbatim and request-wide. Identifiable '
                                                                    'free text (ig_risk 4); never copied '
                                                                    'into accession summaries or anonymised '
                                                                    'serving projections.',
                                       'request_clinical_question_anonymised': 'Approved-lane anonymised '
                                                                               'clinical question. NULL '
                                                                               'unless the registered '
                                                                               'request-text feed reports '
                                                                               'anonymized for the current '
                                                                               'source text hash. Never '
                                                                               'falls back to raw text or a '
                                                                               "report's anonymised text.",
                                       'request_clinical_question_exam_segment': 'The clinical-question '
                                                                                 'section headed by this '
                                                                                 "examination's code; set "
                                                                                 'only when its segment '
                                                                                 'status is EXAM_SEGMENT. '
                                                                                 'Identifiable free text '
                                                                                 '(ig_risk 4).',
                                       'request_clinical_question_exam_segment_anonymised': 'Approved-lane '
                                                                                            'anonymised '
                                                                                            'examination-specific '
                                                                                            'question '
                                                                                            'segment; same '
                                                                                            'rule.',
                                       'request_clinical_question_segment_status': 'EXAM_SEGMENT, '
                                                                                   'NOT_SEGMENTED (text is '
                                                                                   'request-wide), '
                                                                                   'CODE_NOT_IN_SEGMENTS, '
                                                                                   'DUPLICATE_SEGMENT_CODE, '
                                                                                   'DUPLICATE_EXAM_CODE_ON_REQUEST, '
                                                                                   'NO_EXAM_CODE or NO_TEXT.',
                                       'request_text_anonymisation_status': 'not_registered (feed not yet '
                                                                            'deployed), current (anonymized '
                                                                            'for the current text), stale '
                                                                            '(text changed since redaction), '
                                                                            'pending, failed, '
                                                                            'unresolved_person, or no_text. '
                                                                            'Millennium rows are NULL.',
                                       'requester_practitioner_id': 'Nullable Millennium personnel PERSON_ID '
                                                                    'for the requester.',
                                       'sectra_accession_number': 'Barts/Sectra extraction accession. PACS: '
                                                                  'trimmed REQUEST_ID_STRING '
                                                                  '(map_pacs_examination.SECTRA_ACCESSION_NBR). '
                                                                  'Millennium: REFERENCE_NBR parsed by the '
                                                                  'shared bronze accession rules. Scopes an '
                                                                  'extraction request: one accession can '
                                                                  'hold several examinations and study UIDs. '
                                                                  'Leading zeros preserved. Distinct from '
                                                                  'accession_identifier, which keeps its '
                                                                  'existing meaning. Identifier.',
                                       'source_code': 'Source exam code',
                                       'source_coding_system': 'Source exam coding system.',
                                       'source_display': 'Source exam display.',
                                       'source_event_class_code': 'Millennium EVENT_CLASS_CD (234 '
                                                                  'examination, 224 report document, 231 '
                                                                  'section); NULL for PACS rows.',
                                       'source_feed': 'Registered PACS or Millennium radiology source feed.',
                                       'source_object': 'Native source arm: millennium or pacs.',
                                       'source_status_code': 'Native status. PACS: EXAMINATION_STATUS_CD. '
                                                             'Millennium: RESULT_STATUS_CD.',
                                       'source_update_timestamp': 'PACS: map_pacs_examination.SRC_ADC_UPDT. '
                                                                  'Millennium: '
                                                                  'map_radiology_event.UPDT_DT_TM. These '
                                                                  'distinct clocks are carried per arm '
                                                                  'through the union, not combined as a '
                                                                  'maximum. Neither joined report clocks nor '
                                                                  'the current Silver refresh time replaces '
                                                                  'them.',
                                       'status_code': 'Imaging study status.',
                                       'study_instance_uid': 'DICOM study instance UID.',
                                       'subject_id_system': 'Identifier system used for subject_key.',
                                       'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                      'identifier. Not salted, not secret; a stable join key '
                                                      'across feeds where person_id is unresolved.',
                                       'vip_ind': 'Indicator that the source record carries VIP handling '
                                                  'status for each imaging exam record. It is produced by '
                                                  'the silver transformation and has no direct bronze-column '
                                                  'lineage entry. Null means the source did not state the '
                                                  'indicator and must not be interpreted as false.',
                                       'withheld_identity_ind': 'Identity-withholding status is not '
                                                                'populated by the current source '
                                                                'projections; null does not mean identity '
                                                                'was not withheld.'},
                           'quantity_override': False,
                           'table': 'One PACS or Millennium radiology examination/study; the Millennium '
                                    'report-link field is schema-only until it first populates. Gold QC twin '
                                    'of the silver product: 8 columns are repaired or nulled, 1 rule(s) drop '
                                    'rows, 4 check(s) are advisory. Each rule states its reason in the '
                                    'pipeline notebook, and Lakeflow expectation metrics report what every '
                                    'rule matched on each update.'}}
