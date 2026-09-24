# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Patient Safety
# MAGIC Safety incidents, participants, injuries, factors, actions and status history.
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

# ==== clinical_safety_incident ====

CLINICAL_SAFETY_INCIDENT_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident.active_record": "COALESCE(record_status = 'active', FALSE)",
}
CLINICAL_SAFETY_INCIDENT_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident.key_present": "safety_incident_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident",
    comment="TDX v2.1 Gold QC twin for clinical_safety_incident.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_ADVISORY_RULES)
def gold_clinical_safety_incident():
    """Publish clinical_safety_incident; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident",
    comment="Compatibility evidence of admitted clinical_safety_incident rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_safety_incident():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident", "safety_incident_key")


# COMMAND ----------

# ==== clinical_safety_incident_participant ====

CLINICAL_SAFETY_INCIDENT_PARTICIPANT_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident_participant.active_record": "COALESCE(record_status = 'active', FALSE)",
    # Patient participants require resolution; non-patients retain their original treatment.
    # A null class passes only if the resolved-identity side makes the expression true.
    "gold.clinical_safety_incident_participant.patient_identity": "COALESCE(lower(participant_class) <> 'patient' OR identity_status = 'resolved', FALSE)",
}
CLINICAL_SAFETY_INCIDENT_PARTICIPANT_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident_participant.key_present": "safety_incident_participant_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident_participant.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident_participant",
    comment="TDX v2.1 Gold QC twin for clinical_safety_incident_participant.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_PARTICIPANT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_PARTICIPANT_ADVISORY_RULES)
def gold_clinical_safety_incident_participant():
    """Publish clinical_safety_incident_participant; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident_participant")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident_participant",
    comment="Compatibility evidence of admitted clinical_safety_incident_participant rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_safety_incident_participant():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident_participant", "safety_incident_participant_key")


# COMMAND ----------

# ==== clinical_safety_incident_injury ====

CLINICAL_SAFETY_INCIDENT_INJURY_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident_injury.active_record": "COALESCE(record_status = 'active', FALSE)",
}
CLINICAL_SAFETY_INCIDENT_INJURY_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident_injury.key_present": "safety_incident_injury_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident_injury.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident_injury",
    comment="TDX v2.1 Gold QC twin for clinical_safety_incident_injury.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_INJURY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_INJURY_ADVISORY_RULES)
def gold_clinical_safety_incident_injury():
    """Publish clinical_safety_incident_injury; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident_injury")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident_injury",
    comment="Compatibility evidence of admitted clinical_safety_incident_injury rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_safety_incident_injury():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident_injury", "safety_incident_injury_key")


# COMMAND ----------

# ==== clinical_safety_incident_factor ====

CLINICAL_SAFETY_INCIDENT_FACTOR_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident_factor.active_record": "COALESCE(record_status = 'active', FALSE)",
}
CLINICAL_SAFETY_INCIDENT_FACTOR_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident_factor.key_present": "safety_incident_factor_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident_factor.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident_factor",
    comment="TDX v2.1 Gold QC twin for clinical_safety_incident_factor.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_FACTOR_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_FACTOR_ADVISORY_RULES)
def gold_clinical_safety_incident_factor():
    """Publish clinical_safety_incident_factor; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident_factor")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident_factor",
    comment="Compatibility evidence of admitted clinical_safety_incident_factor rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_safety_incident_factor():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident_factor", "safety_incident_factor_key")


# COMMAND ----------

# ==== clinical_safety_incident_lfpse ====

CLINICAL_SAFETY_INCIDENT_LFPSE_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident_lfpse.active_record": "COALESCE(record_status = 'active', FALSE)",
}
CLINICAL_SAFETY_INCIDENT_LFPSE_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident_lfpse.key_present": "safety_incident_lfpse_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident_lfpse.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident_lfpse",
    comment="TDX v2.1 Gold QC twin for clinical_safety_incident_lfpse.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_LFPSE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_LFPSE_ADVISORY_RULES)
def gold_clinical_safety_incident_lfpse():
    """Publish clinical_safety_incident_lfpse; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident_lfpse")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident_lfpse",
    comment="Compatibility evidence of admitted clinical_safety_incident_lfpse rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_safety_incident_lfpse():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident_lfpse", "safety_incident_lfpse_key")


# COMMAND ----------

# ==== clinical_safety_incident_action ====

CLINICAL_SAFETY_INCIDENT_ACTION_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident_action.active_record": "COALESCE(record_status = 'active', FALSE)",
}
CLINICAL_SAFETY_INCIDENT_ACTION_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident_action.key_present": "safety_incident_action_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident_action.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident_action",
    comment="TDX v2.1 Gold QC twin for clinical_safety_incident_action.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_ACTION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_ACTION_ADVISORY_RULES)
def gold_clinical_safety_incident_action():
    """Publish clinical_safety_incident_action; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident_action")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident_action",
    comment="Compatibility evidence of admitted clinical_safety_incident_action rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_safety_incident_action():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident_action", "safety_incident_action_key")


# COMMAND ----------

# ==== clinical_safety_incident_status ====

CLINICAL_SAFETY_INCIDENT_STATUS_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident_status.active_record": "COALESCE(record_status = 'active', FALSE)",
}
CLINICAL_SAFETY_INCIDENT_STATUS_ADVISORY_RULES = {
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident_status.key_present": "safety_incident_status_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident_status.load_time_present": "loaded_at IS NOT NULL",
}

@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident_status",
    comment="TDX v2.1 Gold QC twin for clinical_safety_incident_status.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_STATUS_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_STATUS_ADVISORY_RULES)
def gold_clinical_safety_incident_status():
    """Publish clinical_safety_incident_status; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident_status")

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident_status",
    comment="Compatibility evidence of admitted clinical_safety_incident_status rows; advisory outcomes are in Lakeflow expectation metrics.",
    refresh_policy="incremental",
)
def _gold_qc_clinical_safety_incident_status():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident_status", "safety_incident_status_key")

