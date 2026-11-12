"""Which bronze rows are allowed into silver (ACTIVE_ROWS_V1).

Ruling (Ben, 2026-10-18): a bronze row that is not active is not brought into silver. A row is not active when
any of these holds:

  1. ACTIVE_IND = 0 (Millennium's logical delete), or a table-specific row flag of the same meaning is 0;
  2. ACTIVE_STATUS_CD is set to anything other than 188 'Active' (Inactive, Deleted, Combined, Suspended, Review,
     Draft, Recall, ...). Tables that carry only the description use ACTIVE_STATUS_DESC <> 'Active';
  3. SOURCE_PRESENT_IND is false: the row has disappeared from the source system;
  4. for a VERSIONED table only, a valid-until / end-effective date before Millennium's 2100 open end: the row is a
     superseded version of a fact, or an event removed from the chart.

An unset flag, status or date (NULL, or status 0) counts as active: absence of evidence is not a delete.

Validity-period history keeps its ended rows (addresses, identifiers, GP registrations, encounter practitioners,
person attributes, waiting-list history, research enrolment): there the end date is part of the fact and silver
publishes it as valid_to. Reference tables whose ACTIVE_IND means "retired" (staff who have left, closed wards and
beds) keep their inactive rows too, because historical facts still point at them; only a source delete removes them.

The 2100 sentinel is a fixed literal rather than now(), so every materialized view that reads through this filter
stays incrementally refreshable. Only bronze tables are filtered; lookups and silver tables are returned unchanged.
"""

from pyspark.sql import DataFrame, functions as F

ACTIVE_STATUS_ACTIVE_CD = 188  # Millennium code set 48 'Active'
OPEN_END_SENTINEL = "2100-01-01"

# Row-level active flags kept under a table-specific name, with the same meaning as ACTIVE_IND.
TABLE_ACTIVE_FLAGS = {
    "map_powerform_assessment": ("ASSESSMENT_ACTIVE_IND",),
    "map_powerform_assessment_current": ("ASSESSMENT_ACTIVE_IND",),
    "map_order_comment": ("TEXT_ACTIVE_IND",),
    "map_tracking_episode": ("ITEM_ACTIVE_IND",),
    "map_rtt_pathway": ("CORE_ACTIVE_IND",),
}

# Reference tables where ACTIVE_IND / ACTIVE_STATUS_CD mark a retired thing that historical facts still reference.
RETIREMENT_FLAG_TABLES = {
    "map_medical_personnel",
    "map_medical_personnel_alias",
    "map_medical_personnel_group",
    "map_location_unit",
    "map_bed_status_observation",
    "map_care_site",
}

# Versioned tables: an end date before the open end means the row is a superseded version (or a removed event).
# Clinical-event tables list both the event row's and its result row's validity.
VERSIONED_END_COLUMNS = {
    "map_diagnosis": ("END_EFFECTIVE_DT_TM",),
    "map_problem": ("END_EFFECTIVE_DT_TM",),
    "map_problem_history": ("END_EFFECTIVE_DT_TM",),
    "map_allergy": ("END_EFFECTIVE_DT_TM",),
    "map_allergy_current": ("END_EFFECTIVE_DT_TM",),
    "map_encounter": ("END_EFFECTIVE_DT_TM",),
    "map_death": ("END_EFFECTIVE_DT_TM",),
    "map_family_history": ("END_EFFECTIVE_DT_TM",),
    "map_episode": ("END_EFFECTIVE_DT_TM",),
    "map_nomen_events": ("CE_VALID_UNTIL_DT_TM", "CR_VALID_UNTIL_DT_TM"),
    "map_coded_events": ("CE_VALID_UNTIL_DT_TM", "CR_VALID_UNTIL_DT_TM"),
    "map_numeric_events": ("CLINICAL_EVENT_VALID_UNTIL_DT_TM", "STRING_RESULT_VALID_UNTIL_DT_TM"),
    "map_text_events": ("CLINICAL_EVENT_VALID_UNTIL_DT_TM", "STRING_RESULT_VALID_UNTIL_DT_TM"),
    "map_date_events": ("CLINICAL_EVENT_VALID_UNTIL_DT_TM", "DATE_RESULT_VALID_UNTIL_DT_TM"),
    "map_med_admin": ("CE_VALID_UNTIL_DT_TM", "MR_VALID_UNTIL_DT_TM"),
    "map_implant_detail_events": ("VALID_UNTIL_DT_TM",),
    "map_radiology_event": ("VALID_UNTIL_DT_TM",),
    "map_pathology": ("valid_until_dt_tm",),
    "map_pathology_report": ("valid_to",),
    "mill_blob_text": ("VALID_UNTIL_DT_TM",),
}


def _is_bronze(table_name):
    """True for a three-part name in a bronze schema (4_prod.bronze or a development bronze twin)."""
    parts = str(table_name).replace("`", "").split(".")
    return len(parts) == 3 and parts[1].lower().endswith("bronze")


def _find(df, name):
    """The DataFrame's own spelling of a column, matched case-insensitively, or None when it has no such column."""
    wanted = name.lower()
    return next((column for column in df.columns if column.lower() == wanted), None)


def _flag_not_zero(name):
    """A 0/1 or boolean flag column is not 0 (NULL counts as active)."""
    return F.coalesce(F.col(f"`{name}`").cast("int") != 0, F.lit(True))


def active_condition(df, table_name):
    """The Column predicate that keeps a table's active rows, or None when no rule applies to it."""
    if not _is_bronze(table_name):
        return None
    base = str(table_name).replace("`", "").split(".")[-1].lower()
    checks = []

    if base not in RETIREMENT_FLAG_TABLES:
        for flag in ("ACTIVE_IND",) + TABLE_ACTIVE_FLAGS.get(base, ()):
            column = _find(df, flag)
            if column:
                checks.append(_flag_not_zero(column))
        status_cd = _find(df, "ACTIVE_STATUS_CD")
        status_desc = _find(df, "ACTIVE_STATUS_DESC")
        if status_cd:
            code = F.col(f"`{status_cd}`").cast("bigint")
            checks.append(F.coalesce(code.isin(0, ACTIVE_STATUS_ACTIVE_CD), F.lit(True)))
        elif status_desc:
            label = F.lower(F.trim(F.col(f"`{status_desc}`")))
            checks.append(F.coalesce((label == "active") | (label == ""), F.lit(True)))

    present = _find(df, "SOURCE_PRESENT_IND")
    if present:
        checks.append(F.coalesce(F.col(f"`{present}`").cast("boolean"), F.lit(True)))

    for end_name in VERSIONED_END_COLUMNS.get(base, ()):
        column = _find(df, end_name)
        if column:
            checks.append(F.coalesce(F.col(f"`{column}`") >= F.lit(OPEN_END_SENTINEL).cast("timestamp"), F.lit(True)))

    if not checks:
        return None
    keep = checks[0]
    for check in checks[1:]:
        keep = keep & check
    return keep


def active_rows(df: DataFrame, table_name: str) -> DataFrame:
    """Keep only the active rows of a bronze table (see the module docstring); other tables pass through unchanged."""
    keep = active_condition(df, table_name)
    return df if keep is None else df.where(keep)
