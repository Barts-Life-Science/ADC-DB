"""Silver _common.clinical_events: 0 dataset(s).

- shared helpers (no datasets)
- moved verbatim from: 2_silver_clinical_events:3041, 2_silver_clinical_events:3091, 2_silver_clinical_events:3193, 2_silver_clinical_events:3927, 2_silver_clinical_events:6705, 2_silver_clinical_events:6747, 2_silver_clinical_events:6792, 2_silver_clinical_events:6793

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_tables._common.journey import (
    IMAGING_ACCESSION_MEMBER_COLUMNS,
    IMAGING_INTEGRATION_COLUMNS,
    S3_TABLE_AXIS_SPECS,
    _indication_canonical,
    _s3_replace_public_variant,
)


# Clinical / indication

# contract v2: retain the full canonical shape for internal reuse.
INDICATION_SOURCE_COLUMNS = [
    "patient_event_key",
    "indication_id",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "encounter_link_method",  # PATH_ENCOUNTER_V1
    "event_datetime",
    "event_end_datetime",
    "event_datetime_local",
    "event_end_datetime_local",
    "source_coding_system",
    "source_code",
    "source_display",
    "relation_type",
    "source_field",
    "source_text",
    "evidence_text",
    "evidence_start",
    "evidence_end",
    "snomed_code",
    "snomed_term",
    "omop_concept_id",
    "assertion",
    "temporality",
    "experiencer",
    "rule_id",
    "rule_version",
    "confidence",
    "mapping_status",
    "ig_release_status",
    "is_current",
    "research_qi_only",
    "specimen_key",
    "accession_identifier",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
INDICATION_PUBLIC_COLUMNS = [
    'patient_event_key',
    'indication_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'encounter_link_method',  # PATH_ENCOUNTER_V1
    'event_datetime',
    'event_end_datetime',
    'event_datetime_local',
    'event_end_datetime_local',
    'source_coding_system',
    'source_code',
    'source_display',
    'relation_type',
    'source_field',
    'source_text',
    'evidence_text',
    'evidence_start',
    'evidence_end',
    'snomed_code',
    'snomed_term',
    'omop_concept_id',
    'assertion',
    'temporality',
    'experiencer',
    'rule_id',
    'rule_version',
    'confidence',
    'mapping_status',
    'ig_release_status',
    'is_current',
    'research_qi_only',
    'specimen_key',
    'accession_identifier',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_indication():
    # Assemble indication rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    return _indication_canonical().select(*INDICATION_SOURCE_COLUMNS)


# Clinical /  form grouped
SRC_FORM_ITEM = "4_prod.bronze.map_powerform_assessment_item"


# contract v2: retain the full canonical shape for internal reuse.
IMAGING_EXAM_SOURCE_COLUMNS = [
    "patient_event_key",
    "event_id",
    "pacs_examination_id",
    "organization_key",
    "source_object",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "event_datetime_local",
    "event_end_datetime_local",
    "source_coding_system",
    "source_code",
    "source_display",
    "exam_code", '_r1_exam_mapping', '_r1_exam_map_key', '_r1_exam_components_json',
    "status_code",
    "accession_identifier",
    "study_instance_uid",
    "modality_code",
    "modality_dicom_code",  # SDI_DICOM_MODALITY_V1
    "modality_dicom_method",  # SDI_DICOM_MODALITY_V1
    "body_site_code",
    "report_patient_event_key",
    "requester_practitioner_id",
    "performer_practitioner_id",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
IMAGING_EXAM_PUBLIC_COLUMNS = [
    'patient_event_key',
    'event_id',
    'pacs_examination_id',
    'organization_key',
    'source_object',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'event_datetime_local',
    'event_end_datetime_local',
    'source_coding_system',
    'source_code',
    'source_display',
    'exam_code',
    'status_code',
    'accession_identifier',
    'study_instance_uid',
    'modality_code',
    'modality_dicom_code',  # SDI_DICOM_MODALITY_V1
    'modality_dicom_method',  # SDI_DICOM_MODALITY_V1
    'body_site_code',
    'body_site_snomed_code',  # SDI_IMAGING_BODY_SITE_V1
    'body_site_snomed_display',  # SDI_IMAGING_BODY_SITE_V1
    'body_site_snomed_method',  # SDI_IMAGING_BODY_SITE_V1
    'report_patient_event_key',
    'requester_practitioner_id',
    'performer_practitioner_id',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


# PACS_INTEGRATION_SILVER_CLINICAL_V1
# PACS integration v1: accession, performed-evidence, report-link and request-text fields.
# accession_identifier keeps its meaning; sectra_accession_number is added beside it.
_at = IMAGING_EXAM_SOURCE_COLUMNS.index("record_status")


IMAGING_EXAM_SOURCE_COLUMNS[_at:_at] = IMAGING_INTEGRATION_COLUMNS


_at = IMAGING_EXAM_PUBLIC_COLUMNS.index("performer_practitioner_id") + 1


IMAGING_EXAM_PUBLIC_COLUMNS[_at:_at] = IMAGING_INTEGRATION_COLUMNS


IMAGING_EXAM_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'event_id',
    'pacs_examination_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


_at = IMAGING_ACCESSION_MEMBER_COLUMNS.index("source_display") + 1


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_imaging_exam"].items():
    _s3_replace_public_variant(IMAGING_EXAM_PUBLIC_COLUMNS, old_name, axes)
