# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Documents
# MAGIC Research documents, sensitivity labels and fail-closed safety-document text.
# MAGIC
# MAGIC Numbers indicate reading order; Lakeflow uses dataset dependencies to schedule work.
# MAGIC Mandatory rules use `dp.expect_all_or_drop`; advisory rules use `dp.expect_all`.
# MAGIC Repairs and nulling stay in the projection. Public names and identifiers are preserved.
# MAGIC Helpers are in `gold_journey_shared.py`, an ordinary Python file.

# COMMAND ----------

from gold_journey_shared import (
    INTERNAL_SCHEMA,
    PUBLIC_SCHEMA,
    _n,
    _qc,
    _s3_flatten_gold_public,
    _src,
    _tdx_product,
    _tdx_qc,
    _with_comments,
    _with_parent_status,
    dp,
)

# COMMAND ----------

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
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`document_type` AS `document_type`',
    '`title` AS `title`',
    '`author_practitioner_id` AS `author_practitioner_id`',
    '`author_role` AS `author_role`',
    '`service_id` AS `service_id`',
    "CASE WHEN UPPER(TRIM(CAST(`status_code` AS STRING))) = '0' OR UPPER(TRIM(CAST(`status_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `status_code` END AS `status_code`",
    '`version_id` AS `version_id`',
    '`document_thread_key` AS `document_thread_key`',
    '`supersedes_document_key` AS `supersedes_document_key`',
    '`version_ordinal` AS `version_ordinal`',
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
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _text_document_metadata; source history stays on the main research table.
TEXT_DOCUMENT_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 319,398,203 rows of
    # 656,894,348 that are current and attributable; is_latest_version = TRUE keeps the
    # 624,022,430 rows of 656,894,348 that are current and attributable; source_feed IN
    # ('ancil_long_blob', 'elective_access_comment', 'endobase_exam',
    # 'neonatal_episode_narrative', 'order_comment', 'pathology_report', 'text_event') OR
    # LOWER(TRIM(succession_status)) IN ('final', 'addendum') keeps the 448,698,905 rows of
    # 656,894,348 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface":
        "(identity_status = 'resolved') AND (is_latest_version = TRUE) AND (source_feed IN ('ancil_long_blob', 'elective_access_comment', 'endobase_exam', 'neonatal_episode_narrative', 'order_comment', 'pathology_report', 'text_event') OR LOWER(TRIM(succession_status)) IN ('final', 'addendum'))",
}

TEXT_DOCUMENT_ADVISORY_RULES = {
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

    # Counts what the research surface removed: rows failing source_feed IN
    # ('ancil_long_blob', 'elective_access_comment', 'endobase_exam',
    # 'neonatal_episode_narrative', 'order_comment', 'pathology_report', 'text_event') OR
    # LOWER(TRIM(succession_status)) IN ('final', 'addendum'). The mandatory rule above has
    # already dropped them, so this is how many went, not how many survive. 208,195,443 of
    # 656,894,348 at the profile.
    "gold.text.document.succession_status.default_view_completed":
        "source_feed IN ('ancil_long_blob', 'elective_access_comment', 'endobase_exam', 'neonatal_episode_narrative', 'order_comment', 'pathology_report', 'text_event') OR LOWER(TRIM(succession_status)) IN ('final', 'addendum')",

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

TEXT_DOCUMENT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "event_id": "Millennium document EVENT_ID; native key for the mill_blob arm.",
    "update_count": "De-identified version of the decoded blob text in which detected patient identifiers have been replaced with placeholder tokens.",
    "valid_from_datetime": "De-identified version of the decoded blob text in which detected patient identifiers have been replaced with placeholder tokens.",
    "pacs_report_id": "PACS_REPORT_ID; native key for the pacs_report arm.",
    "endobase_exam_id": "ENDOBASE_EXAM_ID; native key for the endobase_exam arm.",
    "report_version_id": "Pathology report_version_id; native key for the pathology_report arm.",
    "organization_key": "Deterministic organization key retained for mixed Millennium and PACS organization evidence.",
    "source_object": "Native document source arm; contract arms are endobase_exam, mill_blob, pacs_report, and pathology_report.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID when supplied; native encounter foreign key.",
    "event_datetime": "Document clinical or version timestamp.",
    "event_end_datetime": "Source version-validity end.",
    "source_coding_system": "Verbatim source document-type system.",
    "source_code": "Verbatim source document-type code.",
    "source_display": "Verbatim source document-type display.",
    "document_type": "Source document-type CodeableConcept.",
    "title": "Source document title or label.",
    "author_practitioner_id": "Millennium personnel PERSON_ID when the author resolves to Millennium personnel.",
    "author_role": "Source author role when supplied.",
    "service_id": "Service reference when supplied.",
    "status_code": "Verbatim document extraction or source status.",
    "version_id": "Source document-version token.",
    "document_thread_key": "Deterministic SHA-256 key for all versions of one logical document.",
    "supersedes_document_key": "Deterministic SHA-256 key of the directly superseded document version.",
    "version_ordinal": "Source-supplied version ordinal when available; one for single-version lanes.",
    "document_text": "Identifiable source document text under default-deny access.",
    "parser_version": "Parser version that produced document_text or sections. PACS reports whose text comes from the report-text bridge (v4, re-sourced from mill_blob_text) carry the bridge BRIDGED_TEXT_PARSER_VERSION, formatted mill_blob_text:d<decompressor>/p<parser>/pp<post-processor>; pacs-bridge-v1 marks v3 bridge text.",
    "decompressor_version": "Decompressor version used by bronze.",
    "post_processor_version": "Post-processor version used by bronze.",
    "content_type": "Source MIME or content type.",
    "encoding": "Source text encoding",
    "language": "Document language defaulted to English (`en`) because no source language field is available; method=default.",
    "text_sha256": "SHA-256 of retained document_text only; the anonymous alternative never changes this hash or any stable identifier. NULL is represented by the empty-string digest.",
    "raw_content_sha256": "SHA-256 of the original binary payload when supplied by the blob source; distinct from text_sha256.",
    "text_length": "Retained document-text character count recomputed after parsing and post-processing.",
    "content_class": "Deterministic retained-text quality class; rows are flagged rather than dropped.",
    "date_quality": "Event-date quality relative to source provenance: null, pre-1975 epoch sentinel, future beyond source/load time plus one day, or ok.",
    "text_is_truncated": "True when retained text length equals a known source or parser cap (1000000, 65535, 32767, or 32000 characters).",
    "linkage_route": "Deterministic provenance route used to resolve document subject or encounter linkage.",
    "source_class": "Fail-closed source-content class; the public document product contains clinical text only.",
    "assembly_status": "Source-row assembly outcome; NULL for parked SCD working-copy rows.",
    "chunk_count": "Number of source chunks represented by this document row; NULL for parked SCD working-copy rows.",
    "corpus_frequency": "Number of current Journey document rows sharing text_sha256 when the governed release-built frequency is at least 100; NULL means below that storage floor.",
    "is_boilerplate": "True exactly when governed corpus_frequency is at least 1000; absent frequency is false.",
    "source_link_event_id": "Verbatim linked source event identifier when a governed cross-feed bridge supplies one. PACS reports: the Cerner report document EVENT_ID of the version-safe bridge row; source-absent bridge tombstones are not read. Imaging report relationships are in clinical_imaging_report_link.",
    "source_link_system": "Identifier system for source_link_event_id.",
    "author_id_system": "Identifier system for the verbatim source author identifier.",
    "author_source_id": "Verbatim source author identifier retained separately from any resolved practitioner reference.",
    "verified_practitioner_id": "Millennium personnel PERSON_ID when the verifier resolves to Millennium personnel.",
    "verified_datetime": "Source verification timestamp associated with verified_practitioner_id.",
    "source_organization_key": "Deterministic source-organization key from governed source evidence.",
    "source_organization_display": "Source-organization display from the governed organization dimension or verbatim PACS institution.",
    "confidentiality_code": "Source confidentiality classification.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "document_class": "Source event-class display such as Document or mdoc.",
    "contributor_system": "Source contributor-system display such as PowerChart or BLT_TIE_RAD.",
    "succession_status": "Blob succession status display such as Interim or Final.",
    "source_parent_event_id": "Parent clinical-event identifier for document threading when supplied.",
    "parent_relation": "Event relation display such as Root or Child.",
    "source_parent_display": "Parent event-code description; the legacy MainEventDesc document-type label.",
    "source_parent_title": "Parent event title text; the legacy MainTitleText.",
    "source_parent_tag": "Parent event tag text; the legacy MainTagText.",
    "source_tag": "Own event tag text; the legacy ChildTagText.",
    "source_record_status": "Clinical-event record-status display such as Active or Deleted; the legacy Status.",
    "document_text_anonymised": "Reserved NULL field; de-identification is applied at serve time.",
    "text_is_anonymised": "Always false in silver; de-identification is applied at serve time.",
    "prsb_document_type": "PRSB-aligned canonical document type from the governed doc_type_prsb_map lookup.",
    "prsb_subtype": "Canonical subtype qualifier when the mapping supplies one.",
    "prsb_standard": "Source PRSB or Royal-College standard label for the canonical type.",
    "prsb_setting": "Care-setting bucket of the canonical type.",
    "prsb_map_method": "Mapping method provenance.",
    "prsb_map_score": "Embedder cosine score for embedder-method rows.",
    "prsb_map_version": "doc_type_prsb_map version label.",
    "source_feed": "Registered owning source route; all document text is IG-sensitive clinical content.",
    "is_latest_version": "Exactly one row per document_thread_id selected by non-empty text then effective-from and source version ordinal descending with version_id descending as deterministic tiebreak.",
    "record_status": "Arm-specific status, with a final text override: after document text hygiene, lower(trim(document_text)) equal to deleted forces retracted. Otherwise text-event rows use SOURCE_DELETED_IND (null false) for retraction, then a validity end before 2100-01-01 or AUTHENTIC_FLAG zero (null one) for supersession. Blob rows retract for result-status codes 29, 30 or 31, otherwise supersede for a validity end before 2100. PACS and neonatal narratives retract for false source presence (null true). EndoBase supersedes only when a joined parent exam exists and is source-absent. Order comments supersede for TEXT_ACTIVE_IND zero (null one). Elective-access comments retract for source absence, otherwise supersede for inactive status (both null true). Pathology retracts cancelled/entered_in_error versions, otherwise uses active for is_current true and superseded for false/null. Other rows are active. This is not a verbatim closure flag or the independently selected latest-version marker.",
    "record_status_effective_from": "Source-arm history start, unchanged by text hygiene and thread-head selection. Text events: first STRING_RESULT_VALID_FROM_DT_TM, CLINICAL_EVENT_VALID_FROM_DT_TM, then clinical time falling back through RESULT_DT_TM, PERFORMED_DT_TM, EVENT_END_DT_TM and EVENT_START_DT_TM. Blob versions: first VALID_FROM_DT_TM, UPDT_DT_TM, ADC_UPDT. PACS: SRC_ADC_UPDT. EndoBase: parent exam SOURCE_CREATE_TS then earliest retained term CREATED_TS. Order comments: first COMMENT_DT_TM_CLEAN, COMMENT_UPDT_DT_TM_CLEAN, parent ORIG_ORDER_DT_TM_CLEAN, parent CURRENT_START_DT_TM_CLEAN, COMMENT_DT_TM, COMMENT_UPDT_DT_TM. Elective-access comments: CREATED_DT_TM. Neonatal narratives: parent episode DischTime_CLEAN then AdmitTime_CLEAN. Pathology versions: valid_from. No common clinical-time clamp or current refresh timestamp is added.",
    "record_status_effective_to": "Arm-specific end before text hygiene: text-event retractions use their computed source_update_timestamp; supersessions use the least string-result/event validity end, which can be null or a sentinel for AUTHENTIC_FLAG-only supersession. Blob rows use VALID_UNTIL_DT_TM only when it is non-null and before 2100-01-01; a result-status-only retraction can have no end. Source-absent PACS and neonatal rows use own ADC_UPDT. Source-absent joined EndoBase exams use the maximum retained-term, exam and encounter ADC_UPDT. Inactive order comments use TEXT_UPDT_DT_TM_CLEAN. Absent/inactive elective-access comments use first MODIFIED_DT_TM, CREATED_DT_TM, WAITING_LIST_STATUS_CHANGE_DT_TM_CLEAN, DECIDED_TO_ADMIT_DT_TM_CLEAN, TCI_DT_TM_CLEAN. Pathology carries valid_to directly. Other arm cases have null ends. Finally, text equal to deleted after hygiene forces first existing end, source_update_timestamp, loaded_at; it does not guarantee a non-null end. This is not a clinical document completion time.",
    "source_update_timestamp": "Source-arm-specific timestamp; interpret with source_feed. text_event takes the greatest STRING_RESULT_EFFECTIVE_UPDT_DT_TM, CLINICAL_EVENT_ADC_UPDT, LONG_TEXT_ADC_UPDT, LOOKUP_ADC_UPDT and ADC_UPDT from map_text_events. mill_blob_text uses UPDT_DT_TM; pacs_report uses REPORT_MODIFIED_UTC. endobase_exam takes the greatest contributing term ADC_UPDT and parent-exam PIPELINE_UPDT_DT_TM. order_comment uses TEXT_UPDT_DT_TM_CLEAN, falling back to COMMENT_UPDT_DT_TM_CLEAN. elective_access_comment uses MODIFIED_DT_TM; neonatal_episode_narrative uses ADC_UPDT; pathology_report uses valid_from. This mixes source, version and processing clocks; it is not a uniform clinical event time or native-system update time.",
    "loaded_at": "Per-arm contributing load clock: text events use own map_text_events.ADC_UPDT; deduplicated blob versions use the selected mill_blob_text.ADC_UPDT; PACS reports use own report ADC_UPDT, excluding bridge, examination and linkage clocks; neonatal narratives and pathology versions use own ADC_UPDT, excluding joined episode, accession, identity and alias clocks. EndoBase takes greatest of maximum ADC_UPDT across retained nonblank source-present terms, joined exam ADC_UPDT and joined encounter ADC_UPDT. Order comments take greatest of SOURCE_COMMENT_ADC_UPDT, SOURCE_TEXT_ADC_UPDT, PIPELINE_UPDT_DT_TM and joined order SOURCE_ADC_UPDT. Elective-access comments use own ADC_UPDT. Text hygiene, PRSB/organization enrichment, corpus frequency, thread-head selection and QC/public projections add no clock; current and historical pathology versions retain their own clocks. Null inputs follow the arm expression, and no current Silver refresh time is substituted.",
}

@dp.materialized_view(
    name=_n("gold_qc._text_document"),
    comment="Internal quality-controlled twin of text_document: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(TEXT_DOCUMENT_MANDATORY_RULES)
@dp.expect_all(TEXT_DOCUMENT_ADVISORY_RULES)
def _gold_qc_text_document():
    """Quality-controlled twin of journey_text.document."""
    df = _qc("text_document", TEXT_DOCUMENT_SELECT)
    return _with_comments(df, TEXT_DOCUMENT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_text.document"),
    comment=(
        "One governed clinical document version with identifiable text and parser provenance. "
        "Includes the RDE-parity block and governed PRSB typing; blob-lane document_type "
        "became the clinical event coding (recorded change). Gold QC twin of the silver "
        "product: 12 columns are repaired or nulled, 1 rule(s) drop rows, 7 check(s) are "
        "advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
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
    )
    return _s3_flatten_gold_public(_with_comments(df, TEXT_DOCUMENT_COLUMN_COMMENTS), "text_document")

# COMMAND ----------

# ==== journey_text.document_sensitivity_label ====

# contract v2: Task 3 child; mandatory parent-admission check against text_document applies the parent Gold drop rule.
TEXT_DOCUMENT_SENSITIVITY_LABEL_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`sequence` AS `sequence`',
    '`sensitivity_label` AS `sensitivity_label`',
    '`loaded_at` AS `loaded_at`',
]

TEXT_DOCUMENT_SENSITIVITY_LABEL_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent text_document row.",
    "sequence": "One-based order of the source sensitivity label.",
    "sensitivity_label": "Verbatim source sensitivity or security label.",
    "loaded_at": "Bronze load timestamp inherited from the parent document row.",
}

TEXT_DOCUMENT_SENSITIVITY_LABEL_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.text_document_sensitivity_label.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._text_document_sensitivity_label"),
    comment="Internal QC of text_document_sensitivity_label; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(TEXT_DOCUMENT_SENSITIVITY_LABEL_MANDATORY_RULES)
def _gold_qc_text_document_sensitivity_label():
    """Task 3 Gold child of text_document; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_text.document")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("text_document_sensitivity_label"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*TEXT_DOCUMENT_SENSITIVITY_LABEL_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, TEXT_DOCUMENT_SENSITIVITY_LABEL_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_text.document_sensitivity_label"),
    comment='One ordered sensitivity label per clinical text document.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_text_document_sensitivity_label():
    """Publish text_document_sensitivity_label without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._text_document_sensitivity_label")).drop("__gold_parent_present")

# COMMAND ----------

# ==== text_safety_document ====

TEXT_SAFETY_DOCUMENT_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.text_safety_document.active_record": "COALESCE(record_status = 'active', FALSE)",
}
TEXT_SAFETY_DOCUMENT_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.text_safety_document.key_present": "safety_document_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.text_safety_document.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.text_safety_document",
    comment="TDX v2.1 Gold QC twin for text_safety_document.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(TEXT_SAFETY_DOCUMENT_MANDATORY_RULES)
@dp.expect_all(TEXT_SAFETY_DOCUMENT_ADVISORY_RULES)
def gold_text_safety_document():
    """Publish text_safety_document; native rules count exclusions and advisory failures."""
    return _tdx_product("text_safety_document")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_text_safety_document",
    comment="Compatibility evidence of admitted text_safety_document rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_text_safety_document():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("text_safety_document", "safety_document_key")

