"""Silver pathology._shared: 0 dataset(s).

- shared helpers (no datasets)
- moved verbatim from: 2_silver_clinical_events:1745, 2_silver_clinical_events:1792, 2_silver_clinical_events:3232, 2_silver_clinical_events:3281, 2_silver_clinical_events:3381, 2_silver_clinical_events:3419, 2_silver_clinical_events:3469, 2_silver_clinical_events:3571

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_tables._common.journey import (
    S3_TABLE_AXIS_SPECS,
    _micro_isolate_canonical,
    _s3_replace_public_variant,
    _susceptibility_canonical,
)


# Clinical / specimen

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
SPECIMEN_PUBLIC_COLUMNS = [
    'patient_event_key',
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
    'specimen_type',
    'accession_identifier',
    'resolved_accession_key',
    'normalized_lab_no',
    'canonical_accession_status',
    'person_resolution_status',
    'lab_series',
    'discipline',
    'urgent_flag',
    'research_qi_only',
    'clinical_details',
    'tlcs_requested',
    'conditions',
    'reason',
    'body_site_code',
    'body_site_snomed_code',
    'specimen_type_code',
    'specimen_type_snomed_code',
    'sample_datetime',
    'request_datetime',
    'report_datetime',
    'source_history_row_count',
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


SPECIMEN_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'identity_status',
    'person_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


# Clinical / microbiology isolate

# contract v2: retain the full canonical shape for internal reuse.
MICRO_ISOLATE_SOURCE_COLUMNS = [
    "patient_event_key",
    "microbiology_isolate_id",
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
    "pathology_result_id",
    "report_version_key",
    "specimen_type_code",
    "organism_text",
    "organism_snomed_code",
    "organism_omop_concept_id",
    "suspected_ind",
    "growth_grade",
    "organism_code",
    "panel_code",
    "isolate_ordinal",
    "isolate_comment",
    "lims_no",
    "parse_status",
    "parser_version",
    "lifecycle_status",
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
MICRO_ISOLATE_PUBLIC_COLUMNS = [
    'patient_event_key',
    'microbiology_isolate_id',
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
    'pathology_result_id',
    'report_version_key',
    'specimen_type_code',
    'organism_text',
    'organism_snomed_code',
    'organism_omop_concept_id',
    'suspected_ind',
    'growth_grade',
    'organism_code',
    'panel_code',
    'isolate_ordinal',
    'isolate_comment',
    'lims_no',
    'parse_status',
    'parser_version',
    'lifecycle_status',
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
def _lifecycle_source_microbiology_isolate():
    # Assemble microbiology isolate rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _micro_isolate_canonical().select(*MICRO_ISOLATE_SOURCE_COLUMNS)


# Clinical / susceptibility result

# contract v2: retain the full canonical shape for internal reuse.
SUSCEPTIBILITY_SOURCE_COLUMNS = [
    "patient_event_key",
    "susceptibility_result_id",
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
    "microbiology_isolate_id",
    "isolate_event_key",
    "pathology_result_id",
    "link_status",
    "antimicrobial_text",
    "antimicrobial_code",
    "antimicrobial_omop_concept_id",
    "interpretation_raw",
    "interpretation",
    "mic_raw",
    "mic",
    "unit_source_value",
    "method",
    "token_class",
    "token_ordinal",
    "parser_version",
    "lifecycle_status",
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
SUSCEPTIBILITY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'susceptibility_result_id',
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
    'microbiology_isolate_id',
    'isolate_event_key',
    'pathology_result_id',
    'link_status',
    'antimicrobial_text',
    'antimicrobial_code',
    'antimicrobial_omop_concept_id',
    'interpretation_raw',
    'interpretation',
    'mic_raw',
    'mic',
    'unit_source_value',
    'method',
    'token_class',
    'token_ordinal',
    'parser_version',
    'lifecycle_status',
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
def _lifecycle_source_susceptibility_result():
    # Assemble susceptibility result rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _susceptibility_canonical().select(*SUSCEPTIBILITY_SOURCE_COLUMNS)


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_specimen"].items():
    _s3_replace_public_variant(SPECIMEN_PUBLIC_COLUMNS, old_name, axes)
