# Databricks notebook source
# MAGIC %md
# MAGIC # clinical_v_imaging_accession
# MAGIC One row per imaging accession with at least one default-selection-eligible member
# MAGIC (PACS integration v1). Built over the Gold products `clinical_imaging_accession_member`
# MAGIC and `clinical_imaging_report_link`; every eligibility decision is made upstream in Silver.
# MAGIC
# MAGIC Arrays are display summaries. To select accessions, filter member rows (and link rows for
# MAGIC report conditions) on the same member, then take distinct `accession_key`: see
# MAGIC `deploy/example_accession_selection.sql`.
# MAGIC
# MAGIC `CREATE OR REPLACE VIEW` drops column tags, so this notebook reapplies every column's
# MAGIC `ig_risk` / `ig_severity` after each create. Writes outside `8_dev` need
# MAGIC `allow_production_write=true` (human-gated).

# COMMAND ----------

def _widget_text(name, default):
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    try:
        v = dbutils.widgets.get(name)
    except Exception:
        v = default
    return (v or default).strip()

SOURCE_SCHEMA = _widget_text("source_schema", "8_dev.gold_v2")
TARGET_SCHEMA = _widget_text("target_schema", "8_dev.gold_v2")
ALLOW_PROD_WRITE = _widget_text("allow_production_write", "false").lower() == "true"
assert TARGET_SCHEMA.startswith("8_dev.") or ALLOW_PROD_WRITE, (
    f"Refusing to write {TARGET_SCHEMA} without allow_production_write=true")

VIEW = f"{TARGET_SCHEMA}.clinical_v_imaging_accession"
MEMBER = f"{SOURCE_SCHEMA}.clinical_imaging_accession_member"
LINK = f"{SOURCE_SCHEMA}.clinical_imaging_report_link"

# COMMAND ----------

# (column, comment, ig_risk, ig_severity), in view order.
COLUMNS = [
    ("accession_key", "upper(trim(sectra_accession_number)); the accession grouping key.", "4", "2"),
    ("sectra_accession_number", "Barts/Sectra extraction accession (smallest spelling among eligible members).", "4", "2"),
    ("person_id", "The accession's single resolved Millennium PERSON_ID. Accessions with conflicting persons are not in this view.", "2", "1"),
    ("accession_person_status", "single_person or single_person_partial (some members unresolved); no_person when no member resolves.", "0", "0"),
    ("first_performed_datetime", "Earliest valid performed time over eligible members.", "1", "1"),
    ("last_performed_datetime", "Latest valid performed time over eligible members.", "1", "1"),
    ("examination_count", "Distinct physical examinations among eligible members (a Millennium event linked to a PACS examination counts once).", "0", "0"),
    ("study_instance_uid_count", "Distinct DICOM StudyInstanceUIDs among eligible members.", "0", "0"),
    ("cerner_event_count", "Distinct Millennium examination EVENT_IDs among eligible members.", "0", "0"),
    ("eligible_member_count", "Eligible member rows.", "0", "0"),
    ("member_count", "All member rows of the accession, eligible or not.", "0", "0"),
    ("source_objects", "Display summary: member source arms (pacs, millennium).", "0", "0"),
    ("accession_evidence_levels", "Display summary: accession evidence levels of eligible members.", "0", "0"),
    ("modality_codes", "Display summary: observed modality codes.", "0", "0"),
    ("body_site_codes", "Display summary: observed body parts.", "0", "0"),
    ("exam_source_codes", "Display summary: source examination codes.", "0", "0"),
    ("exam_snomed_codes", "Display summary: Gold-gated SNOMED examination concepts (mapped once in Silver).", "0", "0"),
    ("exam_omop_concept_ids", "Display summary: Gold-gated OMOP examination concept ids.", "0", "0"),
    ("report_available_ind", "At least one resolved, active report is linked to an eligible member or to the accession's request.", "0", "0"),
    ("report_text_available_ind", "At least one such report has certified, non-empty text.", "0", "0"),
    ("approved_anonymised_text_available_ind", "At least one such report has approved-lane anonymised output for its current text. Kept separate from report_text_available_ind.", "0", "0"),
    ("report_document_count", "Distinct resolved report documents linked to eligible members or the request.", "0", "0"),
    ("member_keys", "Reference list: eligible member_key values.", "0", "0"),
    ("patient_event_keys", "Reference list: eligible members' clinical_imaging_exam patient_event_key values.", "0", "0"),
    ("encounter_ids", "Reference list: Millennium encounters of eligible members.", "2", "1"),
    ("study_instance_uids", "Reference list: DICOM StudyInstanceUIDs of eligible members.", "3", "2"),
    ("document_patient_event_keys", "Reference list: linked report document keys (text_document patient_event_key).", "0", "0"),
]

# COMMAND ----------

column_list = ",\n  ".join(
    f"{c} COMMENT '{comment.replace(chr(39), chr(39) * 2)}'" for c, comment, _, _ in COLUMNS)
spark.sql(f"""
CREATE OR REPLACE VIEW {VIEW} (
  {column_list}
)
COMMENT 'One row per imaging accession with at least one default-selection-eligible member (PACS integration v1). Arrays are display summaries: select via member/link rows, then distinct accession_key.'
AS
WITH eligible AS (
  SELECT * FROM {MEMBER} WHERE default_selection_eligible_ind
),
acc AS (
  SELECT
    accession_key,
    MIN(sectra_accession_number) AS sectra_accession_number,
    MAX(person_id) AS person_id,
    MIN(accession_person_status) AS accession_person_status,
    MIN(performed_datetime) AS first_performed_datetime,
    MAX(performed_datetime) AS last_performed_datetime,
    COUNT(DISTINCT examination_key) AS examination_count,
    COUNT(DISTINCT study_instance_uid) AS study_instance_uid_count,
    COUNT(DISTINCT event_id) AS cerner_event_count,
    COUNT(*) AS eligible_member_count,
    ARRAY_SORT(COLLECT_SET(source_object)) AS source_objects,
    ARRAY_SORT(COLLECT_SET(accession_evidence_level)) AS accession_evidence_levels,
    ARRAY_SORT(COLLECT_SET(modality_code)) AS modality_codes,
    ARRAY_SORT(COLLECT_SET(body_site_code)) AS body_site_codes,
    ARRAY_SORT(COLLECT_SET(source_code)) AS exam_source_codes,
    ARRAY_SORT(COLLECT_SET(exam_snomed_code)) AS exam_snomed_codes,
    ARRAY_SORT(COLLECT_SET(exam_omop_concept_id)) AS exam_omop_concept_ids,
    ARRAY_SORT(COLLECT_SET(member_key)) AS member_keys,
    ARRAY_SORT(COLLECT_SET(patient_event_key)) AS patient_event_keys,
    ARRAY_SORT(COLLECT_SET(encounter_id)) AS encounter_ids,
    ARRAY_SORT(COLLECT_SET(study_instance_uid)) AS study_instance_uids
  FROM eligible
  GROUP BY accession_key
),
all_members AS (
  SELECT accession_key, COUNT(*) AS member_count FROM {MEMBER} GROUP BY accession_key
),
reachable_links AS (
  -- Examination and Cerner links reach an accession only through an eligible member;
  -- request-scoped links reach it by accession.
  SELECT e.accession_key, l.document_patient_event_key, l.report_resolved_ind,
         l.report_text_available_ind, l.approved_anonymised_text_available_ind, l.record_status
  FROM {LINK} l JOIN eligible e ON l.member_patient_event_key = e.patient_event_key
  UNION ALL
  SELECT l.accession_key, l.document_patient_event_key, l.report_resolved_ind,
         l.report_text_available_ind, l.approved_anonymised_text_available_ind, l.record_status
  FROM {LINK} l
  WHERE l.link_scope = 'accession' AND l.accession_key IN (SELECT accession_key FROM acc)
),
reports AS (
  SELECT
    accession_key,
    BOOL_OR(report_resolved_ind AND record_status = 'active') AS report_available_ind,
    BOOL_OR(report_text_available_ind) AS report_text_available_ind,
    BOOL_OR(approved_anonymised_text_available_ind) AS approved_anonymised_text_available_ind,
    COUNT(DISTINCT CASE WHEN report_resolved_ind THEN document_patient_event_key END)
      AS report_document_count,
    ARRAY_SORT(COLLECT_SET(CASE WHEN report_resolved_ind THEN document_patient_event_key END))
      AS document_patient_event_keys
  FROM reachable_links
  GROUP BY accession_key
)
SELECT
  a.accession_key, a.sectra_accession_number, a.person_id, a.accession_person_status,
  a.first_performed_datetime, a.last_performed_datetime, a.examination_count,
  a.study_instance_uid_count, a.cerner_event_count, a.eligible_member_count, m.member_count,
  a.source_objects, a.accession_evidence_levels, a.modality_codes, a.body_site_codes,
  a.exam_source_codes, a.exam_snomed_codes, a.exam_omop_concept_ids,
  COALESCE(r.report_available_ind, FALSE),
  COALESCE(r.report_text_available_ind, FALSE),
  COALESCE(r.approved_anonymised_text_available_ind, FALSE),
  COALESCE(r.report_document_count, 0),
  a.member_keys, a.patient_event_keys, a.encounter_ids, a.study_instance_uids,
  COALESCE(r.document_patient_event_keys, ARRAY())
FROM acc a
JOIN all_members m ON m.accession_key = a.accession_key
LEFT JOIN reports r ON r.accession_key = a.accession_key
""")

# COMMAND ----------

# Column tags on views are set with ALTER TABLE (ALTER VIEW ... ALTER COLUMN does not parse);
# verified on 8_dev 2026-09-24.
for c, _, risk, severity in COLUMNS:
    spark.sql(f"ALTER TABLE {VIEW} ALTER COLUMN {c} "
              f"SET TAGS ('ig_risk' = '{risk}', 'ig_severity' = '{severity}')")

cat, sch, tbl = VIEW.split(".")
tagged = spark.sql(f"""
    SELECT COUNT(DISTINCT column_name) AS n FROM `{cat}`.information_schema.column_tags
    WHERE schema_name = '{sch}' AND table_name = '{tbl}' AND tag_name IN ('ig_risk', 'ig_severity')
""").first().n
assert tagged == len(COLUMNS), f"{tagged} of {len(COLUMNS)} columns tagged"
print(f"{VIEW}: {len(COLUMNS)} columns, all tagged")

