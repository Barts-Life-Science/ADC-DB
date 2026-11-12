"""Silver specialist._shared: 0 dataset(s).

- shared helpers (no datasets)
- moved verbatim from: 3_silver_specialist_care:6255, 3_silver_specialist_care:6295, 3_silver_specialist_care:8067, 8_silver_patient_safety:34, 8_silver_patient_safety:35

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import spark
from silver_tables._common.journey import S3_TABLE_AXIS_SPECS, _s3_replace_public_variant


# Clinical / device

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
DEVICE_PUBLIC_COLUMNS = [
    'patient_event_key',
    'mc_device_id',
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
    'device_code',
    'device_role',
    'model_name',
    'model_code',
    'manufacturer',
    'manufacturer_parent',
    'serial_number',
    'implanted_datetime',
    'implant_date_quality',
    'explanted_ind',
    'lead_chamber',
    'lead_location',
    'pocket_site',
    'status_display',
    'device_mapping_status',
    'comment_text',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


DEVICE_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'mc_device_id',
    'identity_status',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


# Finalise the public column lists used by this notebook.
for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_device"].items():
    _s3_replace_public_variant(DEVICE_PUBLIC_COLUMNS, old_name, axes)


PUBLIC_SCHEMA = spark.conf.get("journey.public_schema")


INTERNAL_SCHEMA = spark.conf.get("journey.internal_schema")
