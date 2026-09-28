# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Patient Flow
# MAGIC Tracking, pre-arrival, movements, bed observations and location history.
# MAGIC
# MAGIC Numbers indicate reading order; Lakeflow uses dataset dependencies to schedule work.
# MAGIC Mandatory rules use `dp.expect_all_or_drop`; advisory rules use `dp.expect_all`.
# MAGIC Repairs and nulling stay in the projection. Public names and identifiers are preserved.
# MAGIC Helpers are in `gold_journey_shared.py`, an ordinary Python file.

# COMMAND ----------

from gold_journey_shared import (
    INTERNAL_SCHEMA,
    PUBLIC_SCHEMA,
    _tdx_product,
    _tdx_qc,
    dp,
)

# COMMAND ----------

# ==== clinical_tracking_episode ====

CLINICAL_TRACKING_EPISODE_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_tracking_episode.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_tracking_episode.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
}
CLINICAL_TRACKING_EPISODE_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_tracking_episode.key_present": "tracking_episode_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_tracking_episode.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_tracking_episode",
    comment="TDX v2.1 Gold QC twin for clinical_tracking_episode.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_TRACKING_EPISODE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_TRACKING_EPISODE_ADVISORY_RULES)
def gold_clinical_tracking_episode():
    """Publish clinical_tracking_episode; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_tracking_episode")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_tracking_episode",
    comment="Compatibility evidence of admitted clinical_tracking_episode rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_tracking_episode():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_tracking_episode", "tracking_episode_key")


# COMMAND ----------

# ==== clinical_tracking_attendance ====

CLINICAL_TRACKING_ATTENDANCE_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_tracking_attendance.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_tracking_attendance.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
}
CLINICAL_TRACKING_ATTENDANCE_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_tracking_attendance.key_present": "tracking_attendance_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_tracking_attendance.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_tracking_attendance",
    comment="TDX v2.1 Gold QC twin for clinical_tracking_attendance.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_TRACKING_ATTENDANCE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_TRACKING_ATTENDANCE_ADVISORY_RULES)
def gold_clinical_tracking_attendance():
    """Publish clinical_tracking_attendance; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_tracking_attendance")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_tracking_attendance",
    comment="Compatibility evidence of admitted clinical_tracking_attendance rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_tracking_attendance():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_tracking_attendance", "tracking_attendance_key")


# COMMAND ----------

# ==== clinical_tracking_location_stay ====

CLINICAL_TRACKING_LOCATION_STAY_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_tracking_location_stay.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_tracking_location_stay.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
}
CLINICAL_TRACKING_LOCATION_STAY_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_tracking_location_stay.key_present": "tracking_location_stay_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_tracking_location_stay.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_tracking_location_stay",
    comment="TDX v2.1 Gold QC twin for clinical_tracking_location_stay.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_TRACKING_LOCATION_STAY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_TRACKING_LOCATION_STAY_ADVISORY_RULES)
def gold_clinical_tracking_location_stay():
    """Publish clinical_tracking_location_stay; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_tracking_location_stay")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_tracking_location_stay",
    comment="Compatibility evidence of admitted clinical_tracking_location_stay rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_tracking_location_stay():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_tracking_location_stay", "tracking_location_stay_key")


# COMMAND ----------

# ==== clinical_tracking_milestone ====

CLINICAL_TRACKING_MILESTONE_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_tracking_milestone.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_tracking_milestone.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
    # An undated milestone was previously excluded from this product.
    "gold.clinical_tracking_milestone.dated_milestone": "milestone_datetime IS NOT NULL",
}
CLINICAL_TRACKING_MILESTONE_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_tracking_milestone.key_present": "tracking_milestone_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_tracking_milestone.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_tracking_milestone",
    comment="TDX v2.1 Gold QC twin for clinical_tracking_milestone.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_TRACKING_MILESTONE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_TRACKING_MILESTONE_ADVISORY_RULES)
def gold_clinical_tracking_milestone():
    """Publish clinical_tracking_milestone; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_tracking_milestone")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_tracking_milestone",
    comment="Compatibility evidence of admitted clinical_tracking_milestone rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_tracking_milestone():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_tracking_milestone", "tracking_milestone_key")


# COMMAND ----------

# ==== clinical_prearrival ====

CLINICAL_PREARRIVAL_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_prearrival.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_prearrival.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
}
CLINICAL_PREARRIVAL_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_prearrival.key_present": "prearrival_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_prearrival.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_prearrival",
    comment="TDX v2.1 Gold QC twin for clinical_prearrival.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_PREARRIVAL_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PREARRIVAL_ADVISORY_RULES)
def gold_clinical_prearrival():
    """Publish clinical_prearrival; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_prearrival")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_prearrival",
    comment="Compatibility evidence of admitted clinical_prearrival rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_prearrival():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_prearrival", "prearrival_key")


# COMMAND ----------

# ==== clinical_pending_movement ====

CLINICAL_PENDING_MOVEMENT_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_pending_movement.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_pending_movement.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
}
CLINICAL_PENDING_MOVEMENT_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_pending_movement.key_present": "pending_movement_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_pending_movement.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_pending_movement",
    comment="TDX v2.1 Gold QC twin for clinical_pending_movement.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_PENDING_MOVEMENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PENDING_MOVEMENT_ADVISORY_RULES)
def gold_clinical_pending_movement():
    """Publish clinical_pending_movement; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_pending_movement")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_pending_movement",
    comment="Compatibility evidence of admitted clinical_pending_movement rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_pending_movement():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_pending_movement", "pending_movement_key")


# COMMAND ----------

# ==== clinical_appointment_action ====

CLINICAL_APPOINTMENT_ACTION_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_appointment_action.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_appointment_action.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
}
CLINICAL_APPOINTMENT_ACTION_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_appointment_action.key_present": "appointment_action_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_appointment_action.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_appointment_action",
    comment="TDX v2.1 Gold QC twin for clinical_appointment_action.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_APPOINTMENT_ACTION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_APPOINTMENT_ACTION_ADVISORY_RULES)
def gold_clinical_appointment_action():
    """Publish clinical_appointment_action; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_appointment_action")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_appointment_action",
    comment="Compatibility evidence of admitted clinical_appointment_action rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_appointment_action():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_appointment_action", "appointment_action_key")


# COMMAND ----------

# ==== clinical_bed_status_observation ====

CLINICAL_BED_STATUS_OBSERVATION_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_bed_status_observation.active_record": "COALESCE(record_status = 'active', FALSE)",
}
CLINICAL_BED_STATUS_OBSERVATION_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_bed_status_observation.key_present": "bed_status_observation_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_bed_status_observation.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_bed_status_observation",
    comment="TDX v2.1 Gold QC twin for clinical_bed_status_observation.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_BED_STATUS_OBSERVATION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_BED_STATUS_OBSERVATION_ADVISORY_RULES)
def gold_clinical_bed_status_observation():
    """Publish clinical_bed_status_observation; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_bed_status_observation")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_bed_status_observation",
    comment="Compatibility evidence of admitted clinical_bed_status_observation rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_bed_status_observation():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_bed_status_observation", "bed_status_observation_key")


# COMMAND ----------

# ==== clinical_bed_status_observation_coverage ====

CLINICAL_BED_STATUS_OBSERVATION_COVERAGE_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_bed_status_observation_coverage.active_record": "COALESCE(record_status = 'active', FALSE)",
}
CLINICAL_BED_STATUS_OBSERVATION_COVERAGE_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_bed_status_observation_coverage.key_present": "bed_status_coverage_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_bed_status_observation_coverage.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_bed_status_observation_coverage",
    comment="TDX v2.1 Gold QC twin for clinical_bed_status_observation_coverage.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_BED_STATUS_OBSERVATION_COVERAGE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_BED_STATUS_OBSERVATION_COVERAGE_ADVISORY_RULES)
def gold_clinical_bed_status_observation_coverage():
    """Publish clinical_bed_status_observation_coverage; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_bed_status_observation_coverage")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_bed_status_observation_coverage",
    comment="Compatibility evidence of admitted clinical_bed_status_observation_coverage rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_bed_status_observation_coverage():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_bed_status_observation_coverage", "bed_status_coverage_key")


# COMMAND ----------

# ==== reference_location_unit ====

REFERENCE_LOCATION_UNIT_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.reference_location_unit.active_record": "COALESCE(record_status = 'active', FALSE)",
}
REFERENCE_LOCATION_UNIT_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.reference_location_unit.key_present": "location_unit_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.reference_location_unit.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.reference_location_unit",
    comment="TDX v2.1 Gold QC twin for reference_location_unit.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_LOCATION_UNIT_MANDATORY_RULES)
@dp.expect_all(REFERENCE_LOCATION_UNIT_ADVISORY_RULES)
def gold_reference_location_unit():
    """Publish reference_location_unit; native rules count exclusions and advisory failures."""
    return _tdx_product("reference_location_unit")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_reference_location_unit",
    comment="Compatibility evidence of admitted reference_location_unit rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_reference_location_unit():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("reference_location_unit", "location_unit_key")


# COMMAND ----------

# ==== reference_location_attribute_history ====

REFERENCE_LOCATION_ATTRIBUTE_HISTORY_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.reference_location_attribute_history.active_record": "COALESCE(record_status = 'active', FALSE)",
}
REFERENCE_LOCATION_ATTRIBUTE_HISTORY_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.reference_location_attribute_history.key_present": "location_attribute_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.reference_location_attribute_history.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.reference_location_attribute_history",
    comment="TDX v2.1 Gold QC twin for reference_location_attribute_history.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_LOCATION_ATTRIBUTE_HISTORY_MANDATORY_RULES)
@dp.expect_all(REFERENCE_LOCATION_ATTRIBUTE_HISTORY_ADVISORY_RULES)
def gold_reference_location_attribute_history():
    """Publish reference_location_attribute_history; native rules count exclusions and advisory failures."""
    return _tdx_product("reference_location_attribute_history")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_reference_location_attribute_history",
    comment="Compatibility evidence of admitted reference_location_attribute_history rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_reference_location_attribute_history():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("reference_location_attribute_history", "location_attribute_key")

