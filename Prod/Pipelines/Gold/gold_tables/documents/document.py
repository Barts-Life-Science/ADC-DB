"""Gold documents.document: 2 dataset(s).

- datasets: _qc_text_document, text_document
- moved verbatim from: 5_gold_documents:126, 5_gold_documents:132, 5_gold_documents:141, 5_gold_documents:290, 5_gold_documents:294, 5_gold_documents:307, 5_gold_documents:34

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_text.document ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
TEXT_DOCUMENT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`event_id` AS `event_id`',
    '`update_count` AS `update_count`',
    '`valid_from_datetime` AS `valid_from_datetime`',
    '`pacs_report_id` AS `pacs_report_id`',
    '`endobase_exam_id` AS `endobase_exam_id`',
    '`report_version_id` AS `report_version_id`',
    '`organization_key` AS `organization_key`',
    '`source_object` AS `source_object`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    "CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_datetime` END END AS `event_datetime`",
    "CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` OR CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_end_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_end_datetime` END END AS `event_end_datetime`",
    "CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_datetime` END END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`",  # GOLD_COLUMN_PARITY_V1
    "CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` OR CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_end_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_end_datetime` END END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`",  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`document_type` AS `document_type`',
    '`title` AS `title`',
    # INSR_W3 W3.06: Gold: NULL where the Cerner system user id 1 (J5; silver publishes the flag).
    "CASE WHEN CAST(`author_practitioner_id` AS BIGINT) = 1 THEN NULL ELSE `author_practitioner_id` END AS `author_practitioner_id`",
    '`author_role` AS `author_role`',
    '`service_id` AS `service_id`',
    "CASE WHEN UPPER(TRIM(CAST(`status_code` AS STRING))) = '0' OR UPPER(TRIM(CAST(`status_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `status_code` END AS `status_code`",
    '`version_id` AS `version_id`',
    '`document_thread_key` AS `document_thread_key`',
    '`supersedes_document_key` AS `supersedes_document_key`',
    '`version_ordinal` AS `version_ordinal`',
    '`is_latest_version` AS `is_latest_version`',  # GOLD_COLUMN_PARITY_V1
    "CASE WHEN TRIM(CAST(`document_text` AS STRING)) = '' THEN NULL ELSE `document_text` END AS `document_text`",
    '`parser_version` AS `parser_version`',
    '`decompressor_version` AS `decompressor_version`',
    '`post_processor_version` AS `post_processor_version`',
    '`content_type` AS `content_type`',
    '`encoding` AS `encoding`',
    '`language` AS `language`',
    '`text_sha256` AS `text_sha256`',
    '`raw_content_sha256` AS `raw_content_sha256`',
    '`text_length` AS `text_length`',
    '`content_class` AS `content_class`',
    '`date_quality` AS `date_quality`',
    '`text_is_truncated` AS `text_is_truncated`',
    '`linkage_route` AS `linkage_route`',
    '`source_class` AS `source_class`',
    '`assembly_status` AS `assembly_status`',
    '`chunk_count` AS `chunk_count`',
    '`corpus_frequency` AS `corpus_frequency`',
    '`is_boilerplate` AS `is_boilerplate`',
    '`source_link_event_id` AS `source_link_event_id`',
    '`source_link_system` AS `source_link_system`',
    '`author_id_system` AS `author_id_system`',
    '`author_source_id` AS `author_source_id`',
    '`verified_practitioner_id` AS `verified_practitioner_id`',
    "CASE WHEN CAST(`verified_datetime` AS DATE) = DATE'1899-12-30' THEN NULL ELSE `verified_datetime` END AS `verified_datetime`",
    '`source_organization_key` AS `source_organization_key`',
    '`source_organization_display` AS `source_organization_display`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`document_class` AS `document_class`',
    '`contributor_system` AS `contributor_system`',
    '`succession_status` AS `succession_status`',
    '`source_parent_event_id` AS `source_parent_event_id`',
    '`parent_relation` AS `parent_relation`',
    '`source_parent_display` AS `source_parent_display`',
    '`source_parent_title` AS `source_parent_title`',
    '`source_parent_tag` AS `source_parent_tag`',
    '`source_tag` AS `source_tag`',
    '`source_record_status` AS `source_record_status`',
    '`document_text_anonymised` AS `document_text_anonymised`',
    '`text_is_anonymised` AS `text_is_anonymised`',
    '`prsb_document_type` AS `prsb_document_type`',
    '`prsb_subtype` AS `prsb_subtype`',
    '`prsb_standard` AS `prsb_standard`',
    '`prsb_setting` AS `prsb_setting`',
    '`prsb_map_method` AS `prsb_map_method`',
    '`prsb_map_score` AS `prsb_map_score`',
    '`prsb_map_version` AS `prsb_map_version`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    # INSR_W3 W3.05: Gold: NULL where the OLE zero date or a post-2100 year below 9999.
    "CASE WHEN CAST(`record_status_effective_from` AS DATE) = DATE'1899-12-30' OR (CAST(`record_status_effective_from` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`record_status_effective_from` AS DATE)) < 9999) THEN NULL ELSE `record_status_effective_from` END AS `record_status_effective_from`",
    # INSR_W3 W3.05: Gold: NULL where the OLE zero date or an open-end marker in [2099-12-31, 9999).
    "CASE WHEN CAST(`record_status_effective_to` AS DATE) = DATE'1899-12-30' OR (CAST(`record_status_effective_to` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`record_status_effective_to` AS DATE)) < 9999) THEN NULL ELSE `record_status_effective_to` END AS `record_status_effective_to`",
    # INSR_W3 W3.05: Gold: NULL where the OLE zero date or a post-2100 year below 9999.
    "CASE WHEN CAST(`source_update_timestamp` AS DATE) = DATE'1899-12-30' OR (CAST(`source_update_timestamp` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`source_update_timestamp` AS DATE)) < 9999) THEN NULL ELSE `source_update_timestamp` END AS `source_update_timestamp`",
    '`loaded_at` AS `loaded_at`',
]


TEXT_DOCUMENT_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`author_system_ind` AS `author_system_ind`',
]


# contract v2: QC/batch inputs come from internal _text_document_metadata; source history stays on the main research table.
# S3D_GOLD_ADMISSION_PATCH_APPLIED v1
TEXT_DOCUMENT_MANDATORY_RULES = {
    # The research surface: the latest version of every attributable document, whatever its
    # succession status. Interim, preliminary, unknown and blank-status documents are
    # published with succession_status so a consumer can keep completed documents only;
    # superseded versions and rows whose identity was never resolved stay in silver.
    "research_surface":
        "(identity_status = 'resolved') AND (is_latest_version = TRUE)",
}


TEXT_DOCUMENT_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.text.document.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # INSR_W4 BBR cross_field (design BBR-17): A Millennium result status code that did not decode through mill_code_value (BBR-17): consumers filter on result_status_display (Auth/Modified/In Error), so an undecoded code is invisible to them.
    'gold.text.document.table.insr_bbr_status_undecoded':
        "NOT COALESCE((`result_status_code` RLIKE '^[0-9]+$' AND `result_status_display` IS NULL), FALSE)",
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 337,496,145 of 656,894,348 at the profile.
    "gold.text.document.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing is_latest_version = TRUE. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 32,871,918 of 656,894,348 at the profile.
    "gold.text.document.is_latest_version.default_view_latest": "is_latest_version = TRUE",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 8 of 656,894,348
    # rows (1.22e-06%) when profiled on 2026-08-24.
    "gold.text.document.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1 of 656,894,348
    # rows (1.52e-07%) when profiled on 2026-08-24.
    "gold.text.document.record_status_effective_to.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # INSR_W3 W3.05: succession_status.default_view_completed retired (J9: is_latest_version only).

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 937,755 of 656,894,348 rows (0.143%) when profiled on
    # 2026-08-24.
    "gold.text.document.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 176,524 of 656,894,348 rows (0.0269%) when profiled on
    # 2026-08-24.
    "gold.text.document.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
TEXT_DOCUMENT_PERSON_SCOPE_RULES = {
    "gold.text.document.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._text_document"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(TEXT_DOCUMENT_MANDATORY_RULES)
@dp.expect_all(TEXT_DOCUMENT_ADVISORY_RULES)
@dp.expect_all_or_drop(TEXT_DOCUMENT_PERSON_SCOPE_RULES)
def _gold_qc_text_document():
    """Quality-controlled twin of journey_text.document."""
    df = _qc("text_document", TEXT_DOCUMENT_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_text.document"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_text_document():
    """Contract-v2 public twin of text_document; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._text_document")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`event_id` AS `event_id`',
        '`update_count` AS `update_count`',
        '`valid_from_datetime` AS `valid_from_datetime`',
        '`pacs_report_id` AS `pacs_report_id`',
        '`endobase_exam_id` AS `endobase_exam_id`',
        '`report_version_id` AS `report_version_id`',
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
        '`__gold_json_document_type` AS `document_type`',
        '`title` AS `title`',
        '`author_practitioner_id` AS `author_practitioner_id`',
        '`author_role` AS `author_role`',
        '`service_id` AS `service_id`',
        '`status_code` AS `status_code`',
        '`version_id` AS `version_id`',
        '`document_thread_key` AS `document_thread_key`',
        '`supersedes_document_key` AS `supersedes_document_key`',
        '`version_ordinal` AS `version_ordinal`',
        '`is_latest_version` AS `is_latest_version`',  # GOLD_COLUMN_PARITY_V1
        '`document_text` AS `document_text`',
        '`parser_version` AS `parser_version`',
        '`decompressor_version` AS `decompressor_version`',
        '`post_processor_version` AS `post_processor_version`',
        '`content_type` AS `content_type`',
        '`encoding` AS `encoding`',
        '`language` AS `language`',
        '`text_sha256` AS `text_sha256`',
        '`raw_content_sha256` AS `raw_content_sha256`',
        '`text_length` AS `text_length`',
        '`content_class` AS `content_class`',
        '`date_quality` AS `date_quality`',
        '`text_is_truncated` AS `text_is_truncated`',
        '`linkage_route` AS `linkage_route`',
        '`source_class` AS `source_class`',
        '`assembly_status` AS `assembly_status`',
        '`chunk_count` AS `chunk_count`',
        '`corpus_frequency` AS `corpus_frequency`',
        '`is_boilerplate` AS `is_boilerplate`',
        '`source_link_event_id` AS `source_link_event_id`',
        '`source_link_system` AS `source_link_system`',
        '`author_id_system` AS `author_id_system`',
        '`author_source_id` AS `author_source_id`',
        '`verified_practitioner_id` AS `verified_practitioner_id`',
        '`verified_datetime` AS `verified_datetime`',
        '`source_organization_key` AS `source_organization_key`',
        '`source_organization_display` AS `source_organization_display`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`document_class` AS `document_class`',
        '`contributor_system` AS `contributor_system`',
        '`succession_status` AS `succession_status`',
        '`source_parent_event_id` AS `source_parent_event_id`',
        '`parent_relation` AS `parent_relation`',
        '`source_parent_display` AS `source_parent_display`',
        '`source_parent_title` AS `source_parent_title`',
        '`source_parent_tag` AS `source_parent_tag`',
        '`source_tag` AS `source_tag`',
        '`source_record_status` AS `source_record_status`',
        '`document_text_anonymised` AS `document_text_anonymised`',
        '`text_is_anonymised` AS `text_is_anonymised`',
        '`prsb_document_type` AS `prsb_document_type`',
        '`prsb_subtype` AS `prsb_subtype`',
        '`prsb_standard` AS `prsb_standard`',
        '`prsb_setting` AS `prsb_setting`',
        '`prsb_map_method` AS `prsb_map_method`',
        '`prsb_map_score` AS `prsb_map_score`',
        '`prsb_map_version` AS `prsb_map_version`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        '`author_system_ind` AS `author_system_ind`',  # INSR_W3 W3.12
    )
    return _s3_flatten_gold_public(df, "text_document")


def _public_steps_text_document(df):
    """text_document: the public body after its projection (GoldRules.public_select), moved from gold_text_document; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'text_document')


RULES = {
    'text_document': GoldRules(
        table='text_document',
        select=TEXT_DOCUMENT_SELECT,
        mandatory=TEXT_DOCUMENT_MANDATORY_RULES,
        advisory=TEXT_DOCUMENT_ADVISORY_RULES,
        person_scope=TEXT_DOCUMENT_PERSON_SCOPE_RULES,
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
        source_support=('is_latest_version',),
        variant_columns=('document_type',),
        s3_axis_specs={'document_type': [('document_type', True)]},
        value_rules={'gold.text.document.author_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                       'column': 'author_practitioner_id',
                                                                       'companions': [],
                                                                       'match': '`author_practitioner_id` '
                                                                                '= 0',
                                                                       'table': 'text_document'},
         'gold.text.document.author_source_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                 'column': 'author_source_id',
                                                                 'companions': [],
                                                                 'match': "TRIM(`author_source_id`) = '0'",
                                                                 'table': 'text_document'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`event_id` AS `event_id`',
            '`update_count` AS `update_count`',
            '`valid_from_datetime` AS `valid_from_datetime`',
            '`pacs_report_id` AS `pacs_report_id`',
            '`endobase_exam_id` AS `endobase_exam_id`',
            '`report_version_id` AS `report_version_id`',
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
            '`__gold_json_document_type` AS `document_type`',
            '`title` AS `title`',
            '`author_practitioner_id` AS `author_practitioner_id`',
            '`author_role` AS `author_role`',
            '`service_id` AS `service_id`',
            '`status_code` AS `status_code`',
            '`version_id` AS `version_id`',
            '`document_thread_key` AS `document_thread_key`',
            '`supersedes_document_key` AS `supersedes_document_key`',
            '`version_ordinal` AS `version_ordinal`',
            '`is_latest_version` AS `is_latest_version`',
            '`document_text` AS `document_text`',
            '`parser_version` AS `parser_version`',
            '`decompressor_version` AS `decompressor_version`',
            '`post_processor_version` AS `post_processor_version`',
            '`content_type` AS `content_type`',
            '`encoding` AS `encoding`',
            '`language` AS `language`',
            '`text_sha256` AS `text_sha256`',
            '`raw_content_sha256` AS `raw_content_sha256`',
            '`text_length` AS `text_length`',
            '`content_class` AS `content_class`',
            '`date_quality` AS `date_quality`',
            '`text_is_truncated` AS `text_is_truncated`',
            '`linkage_route` AS `linkage_route`',
            '`source_class` AS `source_class`',
            '`assembly_status` AS `assembly_status`',
            '`chunk_count` AS `chunk_count`',
            '`corpus_frequency` AS `corpus_frequency`',
            '`is_boilerplate` AS `is_boilerplate`',
            '`source_link_event_id` AS `source_link_event_id`',
            '`source_link_system` AS `source_link_system`',
            '`author_id_system` AS `author_id_system`',
            '`author_source_id` AS `author_source_id`',
            '`verified_practitioner_id` AS `verified_practitioner_id`',
            '`verified_datetime` AS `verified_datetime`',
            '`source_organization_key` AS `source_organization_key`',
            '`source_organization_display` AS `source_organization_display`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`document_class` AS `document_class`',
            '`contributor_system` AS `contributor_system`',
            '`succession_status` AS `succession_status`',
            '`source_parent_event_id` AS `source_parent_event_id`',
            '`parent_relation` AS `parent_relation`',
            '`source_parent_display` AS `source_parent_display`',
            '`source_parent_title` AS `source_parent_title`',
            '`source_parent_tag` AS `source_parent_tag`',
            '`source_tag` AS `source_tag`',
            '`source_record_status` AS `source_record_status`',
            '`document_text_anonymised` AS `document_text_anonymised`',
            '`text_is_anonymised` AS `text_is_anonymised`',
            '`prsb_document_type` AS `prsb_document_type`',
            '`prsb_subtype` AS `prsb_subtype`',
            '`prsb_standard` AS `prsb_standard`',
            '`prsb_setting` AS `prsb_setting`',
            '`prsb_map_method` AS `prsb_map_method`',
            '`prsb_map_score` AS `prsb_map_score`',
            '`prsb_map_version` AS `prsb_map_version`',
            '`source_feed` AS `source_feed`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
            '`author_system_ind` AS `author_system_ind`',
        ],
        public_steps=(_public_steps_text_document,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_text_document': {'columns': {'assembly_status': 'Source-row assembly outcome; NULL for parked SCD '
                                                      'working-copy rows.',
                                   'author_id_system': 'Identifier system for the verbatim source author '
                                                       'identifier.',
                                   'author_practitioner_id': 'Millennium personnel PERSON_ID when the author '
                                                             'resolves to Millennium personnel.',
                                   'author_role': 'Source author role when supplied.',
                                   'author_source_id': 'Verbatim source author identifier retained '
                                                       'separately from any resolved practitioner reference.',
                                   'author_system_ind': 'True when the source author_practitioner_id was 1, '
                                                        'the Cerner system user (J5); the id is then '
                                                        'published as NULL. NULL when the source id was '
                                                        'NULL. INSR_W2.',
                                   'chunk_count': 'Number of source chunks represented by this document row; '
                                                  'NULL for parked SCD working-copy rows.',
                                   'confidentiality_code': 'Source confidentiality classification.',
                                   'content_class': 'Deterministic retained-text quality class; rows are '
                                                    'flagged rather than dropped.',
                                   'content_type': 'Source MIME or content type.',
                                   'contributor_system': 'Source contributor-system display such as '
                                                         'PowerChart or BLT_TIE_RAD.',
                                   'corpus_frequency': 'Number of current Journey document rows sharing '
                                                       'text_sha256 when the governed release-built '
                                                       'frequency is at least 100; NULL means below that '
                                                       'storage floor.',
                                   'date_quality': 'Event-date quality relative to source provenance: null, '
                                                   'pre-1975 epoch sentinel, future beyond source/load time '
                                                   'plus one day, or ok.',
                                   'decompressor_version': 'Decompressor version used by bronze.',
                                   'document_class': 'Source event-class display such as Document or mdoc.',
                                   'document_text': 'Identifiable source document text under default-deny '
                                                    'access.',
                                   'document_text_anonymised': 'Reserved NULL field; de-identification is '
                                                               'applied at serve time.',
                                   'document_thread_key': 'Deterministic SHA-256 key for all versions of one '
                                                          'logical document.',
                                   'document_type': 'Source document-type CodeableConcept.',
                                   'encoding': 'Source text encoding',
                                   'encounter_id': 'Millennium ENCNTR_ID when supplied; native encounter '
                                                   'foreign key.',
                                   'endobase_exam_id': 'ENDOBASE_EXAM_ID; native key for the endobase_exam '
                                                       'arm.',
                                   'event_datetime': 'Document clinical or version timestamp.',
                                   'event_datetime_local': 'Local-time representation of the document '
                                                           'clinical or version timestamp held in '
                                                           'event_datetime. Gold: NULL wherever Gold nulls '
                                                           'event_datetime.',
                                   'event_end_datetime': 'Source version-validity end.',
                                   'event_end_datetime_local': 'Local-time representation of the source '
                                                               'version-validity end held in '
                                                               'event_end_datetime. Gold: NULL wherever Gold '
                                                               'nulls event_end_datetime.',
                                   'event_id': 'Millennium document EVENT_ID; native key for the mill_blob '
                                               'arm.',
                                   'is_boilerplate': 'True exactly when governed corpus_frequency is at '
                                                     'least 1000; absent frequency is false.',
                                   'is_latest_version': 'Exactly one row per document_thread_id selected by '
                                                        'non-empty text then effective-from and source '
                                                        'version ordinal descending with version_id '
                                                        'descending as deterministic tiebreak.',
                                   'language': 'Document language defaulted to English (`en`) because no '
                                               'source language field is available; method=default.',
                                   'linkage_route': 'Deterministic provenance route used to resolve document '
                                                    'subject or encounter linkage.',
                                   'loaded_at': 'Per-arm contributing load clock: text events use own '
                                                'map_text_events.ADC_UPDT; deduplicated blob versions use '
                                                'the selected mill_blob_text.ADC_UPDT; PACS reports use own '
                                                'report ADC_UPDT, excluding bridge, examination and linkage '
                                                'clocks; neonatal narratives and pathology versions use own '
                                                'ADC_UPDT, excluding joined episode, accession, identity and '
                                                'alias clocks. EndoBase takes greatest of maximum ADC_UPDT '
                                                'across retained nonblank source-present terms, joined exam '
                                                'ADC_UPDT and joined encounter ADC_UPDT. Order comments take '
                                                'greatest of SOURCE_COMMENT_ADC_UPDT, SOURCE_TEXT_ADC_UPDT, '
                                                'PIPELINE_UPDT_DT_TM and joined order SOURCE_ADC_UPDT. '
                                                'Elective-access comments use own ADC_UPDT. Text hygiene, '
                                                'PRSB/organization enrichment, corpus frequency, thread-head '
                                                'selection and QC/public projections add no clock; current '
                                                'and historical pathology versions retain their own clocks. '
                                                'Null inputs follow the arm expression, and no current '
                                                'Silver refresh time is substituted.',
                                   'organization_key': 'Deterministic organization key retained for mixed '
                                                       'Millennium and PACS organization evidence.',
                                   'pacs_report_id': 'PACS_REPORT_ID; native key for the pacs_report arm.',
                                   'parent_relation': 'Event relation display such as Root or Child.',
                                   'parser_version': 'Parser version that produced document_text or '
                                                     'sections. PACS reports whose text comes from the '
                                                     'report-text bridge (v4, re-sourced from '
                                                     'mill_blob_text) carry the bridge '
                                                     'BRIDGED_TEXT_PARSER_VERSION, formatted '
                                                     'mill_blob_text:d<decompressor>/p<parser>/pp<post-processor>; '
                                                     'pacs-bridge-v1 marks v3 bridge text.',
                                   'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                        'cross-feed join key.',
                                   'person_id': 'Millennium PERSON_ID when resolved; native person foreign '
                                                'key.',
                                   'post_processor_version': 'Post-processor version used by bronze.',
                                   'prsb_document_type': 'PRSB-aligned canonical document type from the '
                                                         'governed doc_type_prsb_map lookup.',
                                   'prsb_map_method': 'Mapping method provenance.',
                                   'prsb_map_score': 'Embedder cosine score for embedder-method rows.',
                                   'prsb_map_version': 'doc_type_prsb_map version label.',
                                   'prsb_setting': 'Care-setting bucket of the canonical type.',
                                   'prsb_standard': 'Source PRSB or Royal-College standard label for the '
                                                    'canonical type.',
                                   'prsb_subtype': 'Canonical subtype qualifier when the mapping supplies '
                                                   'one.',
                                   'raw_content_sha256': 'SHA-256 of the original binary payload when '
                                                         'supplied by the blob source; distinct from '
                                                         'text_sha256.',
                                   'record_status': 'Arm-specific status, with a final text override: after '
                                                    'document text hygiene, lower(trim(document_text)) equal '
                                                    'to deleted forces retracted. Otherwise text-event rows '
                                                    'use SOURCE_DELETED_IND (null false) for retraction, '
                                                    'then a validity end before 2100-01-01 or AUTHENTIC_FLAG '
                                                    'zero (null one) for supersession. Blob rows retract for '
                                                    'result-status codes 29, 30 or 31, otherwise supersede '
                                                    'for a validity end before 2100. PACS and neonatal '
                                                    'narratives retract for false source presence (null '
                                                    'true). EndoBase supersedes only when a joined parent '
                                                    'exam exists and is source-absent. Order comments '
                                                    'supersede for TEXT_ACTIVE_IND zero (null one). '
                                                    'Elective-access comments retract for source absence, '
                                                    'otherwise supersede for inactive status (both null '
                                                    'true). Pathology retracts cancelled/entered_in_error '
                                                    'versions, otherwise uses active for is_current true and '
                                                    'superseded for false/null. Other rows are active. This '
                                                    'is not a verbatim closure flag or the independently '
                                                    'selected latest-version marker.',
                                   'record_status_effective_from': 'Source-arm history start, unchanged by '
                                                                   'text hygiene and thread-head selection. '
                                                                   'Text events: first '
                                                                   'STRING_RESULT_VALID_FROM_DT_TM, '
                                                                   'CLINICAL_EVENT_VALID_FROM_DT_TM, then '
                                                                   'clinical time falling back through '
                                                                   'RESULT_DT_TM, PERFORMED_DT_TM, '
                                                                   'EVENT_END_DT_TM and EVENT_START_DT_TM. '
                                                                   'Blob versions: first VALID_FROM_DT_TM, '
                                                                   'UPDT_DT_TM, ADC_UPDT. PACS: '
                                                                   'SRC_ADC_UPDT. EndoBase: parent exam '
                                                                   'SOURCE_CREATE_TS then earliest retained '
                                                                   'term CREATED_TS. Order comments: first '
                                                                   'COMMENT_DT_TM_CLEAN, '
                                                                   'COMMENT_UPDT_DT_TM_CLEAN, parent '
                                                                   'ORIG_ORDER_DT_TM_CLEAN, parent '
                                                                   'CURRENT_START_DT_TM_CLEAN, '
                                                                   'COMMENT_DT_TM, COMMENT_UPDT_DT_TM. '
                                                                   'Elective-access comments: CREATED_DT_TM. '
                                                                   'Neonatal narratives: parent episode '
                                                                   'DischTime_CLEAN then AdmitTime_CLEAN. '
                                                                   'Pathology versions: valid_from. No '
                                                                   'common clinical-time clamp or current '
                                                                   'refresh timestamp is added.',
                                   'record_status_effective_to': 'Arm-specific end before text hygiene: '
                                                                 'text-event retractions use their computed '
                                                                 'source_update_timestamp; supersessions use '
                                                                 'the least string-result/event validity '
                                                                 'end, which can be null or a sentinel for '
                                                                 'AUTHENTIC_FLAG-only supersession. Blob '
                                                                 'rows use VALID_UNTIL_DT_TM only when it is '
                                                                 'non-null and before 2100-01-01; a '
                                                                 'result-status-only retraction can have no '
                                                                 'end. Source-absent PACS and neonatal rows '
                                                                 'use own ADC_UPDT. Source-absent joined '
                                                                 'EndoBase exams use the maximum '
                                                                 'retained-term, exam and encounter '
                                                                 'ADC_UPDT. Inactive order comments use '
                                                                 'TEXT_UPDT_DT_TM_CLEAN. Absent/inactive '
                                                                 'elective-access comments use first '
                                                                 'MODIFIED_DT_TM, CREATED_DT_TM, '
                                                                 'WAITING_LIST_STATUS_CHANGE_DT_TM_CLEAN, '
                                                                 'DECIDED_TO_ADMIT_DT_TM_CLEAN, '
                                                                 'TCI_DT_TM_CLEAN. Pathology carries '
                                                                 'valid_to directly. Other arm cases have '
                                                                 'null ends. Finally, text equal to deleted '
                                                                 'after hygiene forces first existing end, '
                                                                 'source_update_timestamp, loaded_at; it '
                                                                 'does not guarantee a non-null end. This is '
                                                                 'not a clinical document completion time.',
                                   'report_version_id': 'Pathology report_version_id; native key for the '
                                                        'pathology_report arm.',
                                   'service_id': 'Service reference when supplied.',
                                   'source_class': 'Fail-closed source-content class; the public document '
                                                   'product contains clinical text only.',
                                   'source_code': 'Verbatim source document-type code.',
                                   'source_coding_system': 'Verbatim source document-type system.',
                                   'source_display': 'Verbatim source document-type display.',
                                   'source_feed': 'Registered owning source route; all document text is '
                                                  'IG-sensitive clinical content.',
                                   'source_link_event_id': 'Verbatim linked source event identifier when a '
                                                           'governed cross-feed bridge supplies one. PACS '
                                                           'reports: the Cerner report document EVENT_ID of '
                                                           'the version-safe bridge row; source-absent '
                                                           'bridge tombstones are not read. Imaging report '
                                                           'relationships are in '
                                                           'clinical_imaging_report_link.',
                                   'source_link_system': 'Identifier system for source_link_event_id.',
                                   'source_object': 'Native document source arm; contract arms are '
                                                    'endobase_exam, mill_blob, pacs_report, and '
                                                    'pathology_report.',
                                   'source_organization_display': 'Source-organization display from the '
                                                                  'governed organization dimension or '
                                                                  'verbatim PACS institution.',
                                   'source_organization_key': 'Deterministic source-organization key from '
                                                              'governed source evidence.',
                                   'source_parent_display': 'Parent event-code description; the legacy '
                                                            'MainEventDesc document-type label.',
                                   'source_parent_event_id': 'Parent clinical-event identifier for document '
                                                             'threading when supplied.',
                                   'source_parent_tag': 'Parent event tag text; the legacy MainTagText.',
                                   'source_parent_title': 'Parent event title text; the legacy '
                                                          'MainTitleText.',
                                   'source_record_status': 'Clinical-event record-status display such as '
                                                           'Active or Deleted; the legacy Status.',
                                   'source_tag': 'Own event tag text; the legacy ChildTagText.',
                                   'source_update_timestamp': 'Source-arm-specific timestamp; interpret with '
                                                              'source_feed. text_event takes the greatest '
                                                              'STRING_RESULT_EFFECTIVE_UPDT_DT_TM, '
                                                              'CLINICAL_EVENT_ADC_UPDT, LONG_TEXT_ADC_UPDT, '
                                                              'LOOKUP_ADC_UPDT and ADC_UPDT from '
                                                              'map_text_events. mill_blob_text uses '
                                                              'UPDT_DT_TM; pacs_report uses '
                                                              'REPORT_MODIFIED_UTC. endobase_exam takes the '
                                                              'greatest contributing term ADC_UPDT and '
                                                              'parent-exam PIPELINE_UPDT_DT_TM. '
                                                              'order_comment uses TEXT_UPDT_DT_TM_CLEAN, '
                                                              'falling back to COMMENT_UPDT_DT_TM_CLEAN. '
                                                              'elective_access_comment uses MODIFIED_DT_TM; '
                                                              'neonatal_episode_narrative uses ADC_UPDT; '
                                                              'pathology_report uses valid_from. This mixes '
                                                              'source, version and processing clocks; it is '
                                                              'not a uniform clinical event time or '
                                                              'native-system update time.',
                                   'status_code': 'Verbatim document extraction or source status.',
                                   'subject_id_system': 'Identifier system used for subject_key.',
                                   'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                  'identifier. Not salted, not secret; a stable join key '
                                                  'across feeds where person_id is unresolved.',
                                   'succession_status': 'Blob succession status display such as Interim or '
                                                        'Final.',
                                   'supersedes_document_key': 'Deterministic SHA-256 key of the directly '
                                                              'superseded document version.',
                                   'text_is_anonymised': 'Always false in silver; de-identification is '
                                                         'applied at serve time.',
                                   'text_is_truncated': 'True when retained text length equals a known '
                                                        'source or parser cap (1000000, 65535, 32767, or '
                                                        '32000 characters).',
                                   'text_length': 'Retained document-text character count recomputed after '
                                                  'parsing and post-processing.',
                                   'text_sha256': 'SHA-256 of retained document_text only; the anonymous '
                                                  'alternative never changes this hash or any stable '
                                                  'identifier. NULL is represented by the empty-string '
                                                  'digest.',
                                   'title': 'Source document title or label.',
                                   'update_count': 'De-identified version of the decoded blob text in which '
                                                   'detected patient identifiers have been replaced with '
                                                   'placeholder tokens.',
                                   'valid_from_datetime': 'De-identified version of the decoded blob text in '
                                                          'which detected patient identifiers have been '
                                                          'replaced with placeholder tokens.',
                                   'verified_datetime': 'Source verification timestamp associated with '
                                                        'verified_practitioner_id.',
                                   'verified_practitioner_id': 'Millennium personnel PERSON_ID when the '
                                                               'verifier resolves to Millennium personnel.',
                                   'version_id': 'Source document-version token.',
                                   'version_ordinal': 'Source-supplied version ordinal when available; one '
                                                      'for single-version lanes.',
                                   'vip_ind': 'VIP indicator when supplied.',
                                   'withheld_identity_ind': 'Withheld-identity indicator when supplied.'},
                       'quantity_override': True,
                       'table': 'Internal quality-controlled twin of text_document: the Gold repairs, nulls '
                                'and expectations are applied here and the published Gold MV reads this '
                                'object. Materialized rather than temporary so the published MV can refresh '
                                'incrementally.'},
 'text_document': {'columns': {'assembly_status': 'Source-row assembly outcome; NULL for parked SCD '
                                                  'working-copy rows.',
                               'author_id_system': 'Identifier system for the verbatim source author '
                                                   'identifier.',
                               'author_practitioner_id': 'Millennium personnel PERSON_ID when the author '
                                                         'resolves to Millennium personnel.',
                               'author_role': 'Source author role when supplied.',
                               'author_source_id': 'Verbatim source author identifier retained separately '
                                                   'from any resolved practitioner reference.',
                               'author_system_ind': 'True when the source author_practitioner_id was 1, the '
                                                    'Cerner system user (J5); the id is then published as '
                                                    'NULL. NULL when the source id was NULL. INSR_W2.',
                               'chunk_count': 'Number of source chunks represented by this document row; '
                                              'NULL for parked SCD working-copy rows.',
                               'confidentiality_code': 'Source confidentiality classification.',
                               'content_class': 'Deterministic retained-text quality class; rows are flagged '
                                                'rather than dropped.',
                               'content_type': 'Source MIME or content type.',
                               'contributor_system': 'Source contributor-system display such as PowerChart '
                                                     'or BLT_TIE_RAD.',
                               'corpus_frequency': 'Number of current Journey document rows sharing '
                                                   'text_sha256 when the governed release-built frequency is '
                                                   'at least 100; NULL means below that storage floor.',
                               'date_quality': 'Event-date quality relative to source provenance: null, '
                                               'pre-1975 epoch sentinel, future beyond source/load time plus '
                                               'one day, or ok.',
                               'decompressor_version': 'Decompressor version used by bronze.',
                               'document_class': 'Source event-class display such as Document or mdoc.',
                               'document_text': 'Identifiable source document text under default-deny '
                                                'access.',
                               'document_text_anonymised': 'Reserved NULL field; de-identification is '
                                                           'applied at serve time.',
                               'document_thread_key': 'Deterministic SHA-256 key for all versions of one '
                                                      'logical document.',
                               'document_type': 'Source document-type CodeableConcept.',
                               'encoding': 'Source text encoding',
                               'encounter_id': 'Millennium ENCNTR_ID when supplied; native encounter foreign '
                                               'key.',
                               'endobase_exam_id': 'ENDOBASE_EXAM_ID; native key for the endobase_exam arm.',
                               'event_datetime': 'Document clinical or version timestamp.',
                               'event_datetime_local': 'Local-time representation of the document clinical '
                                                       'or version timestamp held in event_datetime. Gold: '
                                                       'NULL wherever Gold nulls event_datetime.',
                               'event_end_datetime': 'Source version-validity end.',
                               'event_end_datetime_local': 'Local-time representation of the source '
                                                           'version-validity end held in event_end_datetime. '
                                                           'Gold: NULL wherever Gold nulls '
                                                           'event_end_datetime.',
                               'event_id': 'Millennium document EVENT_ID; native key for the mill_blob arm.',
                               'is_boilerplate': 'True exactly when governed corpus_frequency is at least '
                                                 '1000; absent frequency is false.',
                               'is_latest_version': 'Exactly one row per document_thread_id selected by '
                                                    'non-empty text then effective-from and source version '
                                                    'ordinal descending with version_id descending as '
                                                    'deterministic tiebreak.',
                               'language': 'Document language defaulted to English (`en`) because no source '
                                           'language field is available; method=default.',
                               'linkage_route': 'Deterministic provenance route used to resolve document '
                                                'subject or encounter linkage.',
                               'loaded_at': 'Per-arm contributing load clock: text events use own '
                                            'map_text_events.ADC_UPDT; deduplicated blob versions use the '
                                            'selected mill_blob_text.ADC_UPDT; PACS reports use own report '
                                            'ADC_UPDT, excluding bridge, examination and linkage clocks; '
                                            'neonatal narratives and pathology versions use own ADC_UPDT, '
                                            'excluding joined episode, accession, identity and alias clocks. '
                                            'EndoBase takes greatest of maximum ADC_UPDT across retained '
                                            'nonblank source-present terms, joined exam ADC_UPDT and joined '
                                            'encounter ADC_UPDT. Order comments take greatest of '
                                            'SOURCE_COMMENT_ADC_UPDT, SOURCE_TEXT_ADC_UPDT, '
                                            'PIPELINE_UPDT_DT_TM and joined order SOURCE_ADC_UPDT. '
                                            'Elective-access comments use own ADC_UPDT. Text hygiene, '
                                            'PRSB/organization enrichment, corpus frequency, thread-head '
                                            'selection and QC/public projections add no clock; current and '
                                            'historical pathology versions retain their own clocks. Null '
                                            'inputs follow the arm expression, and no current Silver refresh '
                                            'time is substituted.',
                               'organization_key': 'Deterministic organization key retained for mixed '
                                                   'Millennium and PACS organization evidence.',
                               'pacs_report_id': 'PACS_REPORT_ID; native key for the pacs_report arm.',
                               'parent_relation': 'Event relation display such as Root or Child.',
                               'parser_version': 'Parser version that produced document_text or sections. '
                                                 'PACS reports whose text comes from the report-text bridge '
                                                 '(v4, re-sourced from mill_blob_text) carry the bridge '
                                                 'BRIDGED_TEXT_PARSER_VERSION, formatted '
                                                 'mill_blob_text:d<decompressor>/p<parser>/pp<post-processor>; '
                                                 'pacs-bridge-v1 marks v3 bridge text.',
                               'patient_event_key': 'Deterministic SHA-256 event identity; stable cross-feed '
                                                    'join key.',
                               'person_id': 'Millennium PERSON_ID when resolved; native person foreign key.',
                               'post_processor_version': 'Post-processor version used by bronze.',
                               'prsb_document_type': 'PRSB-aligned canonical document type from the governed '
                                                     'doc_type_prsb_map lookup.',
                               'prsb_map_method': 'Mapping method provenance.',
                               'prsb_map_score': 'Embedder cosine score for embedder-method rows.',
                               'prsb_map_version': 'doc_type_prsb_map version label.',
                               'prsb_setting': 'Care-setting bucket of the canonical type.',
                               'prsb_standard': 'Source PRSB or Royal-College standard label for the '
                                                'canonical type.',
                               'prsb_subtype': 'Canonical subtype qualifier when the mapping supplies one.',
                               'raw_content_sha256': 'SHA-256 of the original binary payload when supplied '
                                                     'by the blob source; distinct from text_sha256.',
                               'record_status': 'Arm-specific status, with a final text override: after '
                                                'document text hygiene, lower(trim(document_text)) equal to '
                                                'deleted forces retracted. Otherwise text-event rows use '
                                                'SOURCE_DELETED_IND (null false) for retraction, then a '
                                                'validity end before 2100-01-01 or AUTHENTIC_FLAG zero (null '
                                                'one) for supersession. Blob rows retract for result-status '
                                                'codes 29, 30 or 31, otherwise supersede for a validity end '
                                                'before 2100. PACS and neonatal narratives retract for false '
                                                'source presence (null true). EndoBase supersedes only when '
                                                'a joined parent exam exists and is source-absent. Order '
                                                'comments supersede for TEXT_ACTIVE_IND zero (null one). '
                                                'Elective-access comments retract for source absence, '
                                                'otherwise supersede for inactive status (both null true). '
                                                'Pathology retracts cancelled/entered_in_error versions, '
                                                'otherwise uses active for is_current true and superseded '
                                                'for false/null. Other rows are active. This is not a '
                                                'verbatim closure flag or the independently selected '
                                                'latest-version marker.',
                               'record_status_effective_from': 'Source-arm history start, unchanged by text '
                                                               'hygiene and thread-head selection. Text '
                                                               'events: first '
                                                               'STRING_RESULT_VALID_FROM_DT_TM, '
                                                               'CLINICAL_EVENT_VALID_FROM_DT_TM, then '
                                                               'clinical time falling back through '
                                                               'RESULT_DT_TM, PERFORMED_DT_TM, '
                                                               'EVENT_END_DT_TM and EVENT_START_DT_TM. Blob '
                                                               'versions: first VALID_FROM_DT_TM, '
                                                               'UPDT_DT_TM, ADC_UPDT. PACS: SRC_ADC_UPDT. '
                                                               'EndoBase: parent exam SOURCE_CREATE_TS then '
                                                               'earliest retained term CREATED_TS. Order '
                                                               'comments: first COMMENT_DT_TM_CLEAN, '
                                                               'COMMENT_UPDT_DT_TM_CLEAN, parent '
                                                               'ORIG_ORDER_DT_TM_CLEAN, parent '
                                                               'CURRENT_START_DT_TM_CLEAN, COMMENT_DT_TM, '
                                                               'COMMENT_UPDT_DT_TM. Elective-access '
                                                               'comments: CREATED_DT_TM. Neonatal '
                                                               'narratives: parent episode DischTime_CLEAN '
                                                               'then AdmitTime_CLEAN. Pathology versions: '
                                                               'valid_from. No common clinical-time clamp or '
                                                               'current refresh timestamp is added.',
                               'record_status_effective_to': 'Arm-specific end before text hygiene: '
                                                             'text-event retractions use their computed '
                                                             'source_update_timestamp; supersessions use the '
                                                             'least string-result/event validity end, which '
                                                             'can be null or a sentinel for '
                                                             'AUTHENTIC_FLAG-only supersession. Blob rows '
                                                             'use VALID_UNTIL_DT_TM only when it is non-null '
                                                             'and before 2100-01-01; a result-status-only '
                                                             'retraction can have no end. Source-absent PACS '
                                                             'and neonatal rows use own ADC_UPDT. '
                                                             'Source-absent joined EndoBase exams use the '
                                                             'maximum retained-term, exam and encounter '
                                                             'ADC_UPDT. Inactive order comments use '
                                                             'TEXT_UPDT_DT_TM_CLEAN. Absent/inactive '
                                                             'elective-access comments use first '
                                                             'MODIFIED_DT_TM, CREATED_DT_TM, '
                                                             'WAITING_LIST_STATUS_CHANGE_DT_TM_CLEAN, '
                                                             'DECIDED_TO_ADMIT_DT_TM_CLEAN, TCI_DT_TM_CLEAN. '
                                                             'Pathology carries valid_to directly. Other arm '
                                                             'cases have null ends. Finally, text equal to '
                                                             'deleted after hygiene forces first existing '
                                                             'end, source_update_timestamp, loaded_at; it '
                                                             'does not guarantee a non-null end. This is not '
                                                             'a clinical document completion time.',
                               'report_version_id': 'Pathology report_version_id; native key for the '
                                                    'pathology_report arm.',
                               'service_id': 'Service reference when supplied.',
                               'source_class': 'Fail-closed source-content class; the public document '
                                               'product contains clinical text only.',
                               'source_code': 'Verbatim source document-type code.',
                               'source_coding_system': 'Verbatim source document-type system.',
                               'source_display': 'Verbatim source document-type display.',
                               'source_feed': 'Registered owning source route; all document text is '
                                              'IG-sensitive clinical content.',
                               'source_link_event_id': 'Verbatim linked source event identifier when a '
                                                       'governed cross-feed bridge supplies one. PACS '
                                                       'reports: the Cerner report document EVENT_ID of the '
                                                       'version-safe bridge row; source-absent bridge '
                                                       'tombstones are not read. Imaging report '
                                                       'relationships are in clinical_imaging_report_link.',
                               'source_link_system': 'Identifier system for source_link_event_id.',
                               'source_object': 'Native document source arm; contract arms are '
                                                'endobase_exam, mill_blob, pacs_report, and '
                                                'pathology_report.',
                               'source_organization_display': 'Source-organization display from the governed '
                                                              'organization dimension or verbatim PACS '
                                                              'institution.',
                               'source_organization_key': 'Deterministic source-organization key from '
                                                          'governed source evidence.',
                               'source_parent_display': 'Parent event-code description; the legacy '
                                                        'MainEventDesc document-type label.',
                               'source_parent_event_id': 'Parent clinical-event identifier for document '
                                                         'threading when supplied.',
                               'source_parent_tag': 'Parent event tag text; the legacy MainTagText.',
                               'source_parent_title': 'Parent event title text; the legacy MainTitleText.',
                               'source_record_status': 'Clinical-event record-status display such as Active '
                                                       'or Deleted; the legacy Status.',
                               'source_tag': 'Own event tag text; the legacy ChildTagText.',
                               'source_update_timestamp': 'Source-arm-specific timestamp; interpret with '
                                                          'source_feed. text_event takes the greatest '
                                                          'STRING_RESULT_EFFECTIVE_UPDT_DT_TM, '
                                                          'CLINICAL_EVENT_ADC_UPDT, LONG_TEXT_ADC_UPDT, '
                                                          'LOOKUP_ADC_UPDT and ADC_UPDT from '
                                                          'map_text_events. mill_blob_text uses UPDT_DT_TM; '
                                                          'pacs_report uses REPORT_MODIFIED_UTC. '
                                                          'endobase_exam takes the greatest contributing '
                                                          'term ADC_UPDT and parent-exam '
                                                          'PIPELINE_UPDT_DT_TM. order_comment uses '
                                                          'TEXT_UPDT_DT_TM_CLEAN, falling back to '
                                                          'COMMENT_UPDT_DT_TM_CLEAN. elective_access_comment '
                                                          'uses MODIFIED_DT_TM; neonatal_episode_narrative '
                                                          'uses ADC_UPDT; pathology_report uses valid_from. '
                                                          'This mixes source, version and processing clocks; '
                                                          'it is not a uniform clinical event time or '
                                                          'native-system update time.',
                               'status_code': "mill_blob arm: decoder status, always 'Decoded'; text_event "
                                              'arm: raw Millennium result status code (see '
                                              'result_status_display for document finality).',
                               'subject_id_system': 'Identifier system used for subject_key.',
                               'subject_key': 'Deterministic SHA-256 over the strongest source identifier. '
                                              'Not salted, not secret; a stable join key across feeds where '
                                              'person_id is unresolved.',
                               'succession_status': "Cerner succession type display; '? Unknown' is a source "
                                                    'value. Not document finality: use result_status_display '
                                                    '(Auth (Verified), Modified, In Error ...).',
                               'supersedes_document_key': 'Deterministic SHA-256 key of the directly '
                                                          'superseded document version.',
                               'text_is_anonymised': 'Always false in silver; de-identification is applied '
                                                     'at serve time.',
                               'text_is_truncated': 'True when retained text length equals a known source or '
                                                    'parser cap (1000000, 65535, 32767, or 32000 '
                                                    'characters).',
                               'text_length': 'Retained document-text character count recomputed after '
                                              'parsing and post-processing.',
                               'text_sha256': 'SHA-256 of retained document_text only; the anonymous '
                                              'alternative never changes this hash or any stable identifier. '
                                              'NULL is represented by the empty-string digest.',
                               'title': 'Source document title or label.',
                               'update_count': 'Millennium clinical-event UPDT_CNT of the blob document '
                                               'version; with event_id and valid_from_datetime it identifies '
                                               'the document version. NULL for non-Millennium document '
                                               'feeds.',
                               'valid_from_datetime': 'Millennium clinical-event VALID_FROM_DT_TM (UTC) of '
                                                      'the blob document version; with event_id and '
                                                      'update_count it identifies the version. NULL for '
                                                      'non-Millennium document feeds.',
                               'verified_datetime': 'Source verification timestamp associated with '
                                                    'verified_practitioner_id.',
                               'verified_practitioner_id': 'Millennium personnel PERSON_ID when the verifier '
                                                           'resolves to Millennium personnel.',
                               'version_id': 'Source document-version token.',
                               'version_ordinal': 'Source-supplied version ordinal when available; one for '
                                                  'single-version lanes.',
                               'vip_ind': 'VIP indicator when supplied.',
                               'withheld_identity_ind': 'Withheld-identity indicator when supplied.'},
                   'quantity_override': False,
                   'table': 'One governed clinical document version with identifiable text and parser '
                            'provenance. Includes the RDE-parity block and governed PRSB typing; blob-lane '
                            'document_type became the clinical event coding (recorded change). Gold QC twin '
                            'of the silver product: 12 columns are repaired or nulled, 1 rule(s) drop rows, '
                            '7 check(s) are advisory. Each rule states its reason in the pipeline notebook, '
                            'and Lakeflow expectation metrics report what every rule matched on each '
                            'update.'}}
