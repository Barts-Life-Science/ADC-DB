"""Rule tables for the RBI resource-use dataset — pure Python, no Spark.

Every rule that decides eligibility, consumption, classification or cost family lives here
with an id, so the pipeline, the tests and the published provenance read one source.
Changing a rule means bumping RULE_VERSION; every stream row records the version it was
built under. Spec: docs/superpowers/specs/2026-10-16-rbi-resource-use-dataset-design.md (§3, §4, §10).
"""

RULE_VERSION = "rbi-rules-2"  # RBI_REL_20261025: observation bound on stream rows (not the HCD ledger) and unknown bed stays

# ---- Observation bound (RBI_REL_20261025; same rule as silver/gold) ----------------------
# An instant later than the source row's own loaded_at cannot have been observed. A booking
# dated after its load is a plan, not an event: its row is kept as 'scheduled' (never priced,
# never a first/last-event instant). A row the source already calls consumed but dates after
# its load is contradictory, so it becomes 'unknown' with this evidence.
SCHEDULED_STATUS = "scheduled"
AFTER_OBSERVATION_EVIDENCE = "event_after_observation"
# Statuses whose event instants count as observed activity (first/last-event rollups).
OBSERVED_STATUSES = ("consumed", "not_consumed")

# ---- Beds (spec §3, §10.8) -------------------------------------------------------------
# Encounter types whose location stays hold a bed. Outpatient*, Pre-registration, Referral,
# Waiting List, Results Only and ED encounters have location stays too, but those are
# administrative or booking intervals, not bed occupancy.
BED_ENCOUNTER_TYPES = (
    "Inpatient", "Day Case", "Regular Day Admission", "Regular Night Admission", "Maternity", "Newborn",
)
# Occupancy bases that are observed evidence of physical presence (silver basis vocabulary).
OCCUPANCY_START_OBSERVED = ("LOC_ARRIVE", "ENCOUNTER_ARRIVAL_BEST", "STOP_START")
OCCUPANCY_END_OBSERVED = ("LOC_DEPART", "NEXT_STOP_START", "FINITE_EFFECTIVE_END")

# ---- Attendance-interval fallback (spec §5, §10.1) -------------------------------------
# Physical-attendance encounter types and their precedence tier (1 = strongest claim).
ATTENDANCE_TIER = {
    "Inpatient": 1, "Maternity": 1, "Newborn": 1,
    "Emergency Department": 2,
    "Day Case": 3, "Regular Day Admission": 3, "Regular Night Admission": 3,
    "Outpatient": 4, "Outpatient Services": 4,
}
OBSERVED_CONFIDENCE = ("HIGH", "MEDIUM")
OBSERVED_CLOSE_STATE = "departed"

# ---- Consumption maps (spec §10.9). Anything unlisted is "unknown". --------------------
APPOINTMENT_CONSUMPTION = {
    "CHECKED OUT": "consumed", "CHECKED IN": "consumed",
    "NOSHOW": "not_consumed", "CANCELED": "not_consumed", "RESCHEDULED": "not_consumed",
}
THEATRE_CONSUMPTION = {"PERFORMED": "consumed", "CANCELLED": "not_consumed"}
MEDICATION_ADMIN_CONSUMPTION = {
    "Auth (Verified)": "consumed", "Modified": "consumed",
    "Not Done": "not_consumed", "In Error": "not_consumed",
}
TRANSFUSION_CONSUMPTION = {
    "COMPLETED": "consumed", "MULTIPLE_SUCCESS_EVENTS": "consumed",
    "STARTED_NO_END": "consumed", "END_WITHOUT_START": "consumed",
}


def consumption(mapping, value):
    """Return the consumption status for a source value; unlisted or NULL means unknown."""
    return mapping.get(value, "unknown")


# ---- Tracking groups (spec §3). Ordered: the first matching rule wins. -----------------
# Theatre is tested before labour so "R1H WX Obs Th" (obstetric theatre) is a theatre;
# SDEC before ED so "KGH eSDEC Tracking Group" is SDEC.
TRACKING_GROUP_RULES = (
    ("TG01", "inpatient_board", r"^Inpatient$"),
    ("TG02", "cath_lab", r"Cath Lab"),
    ("TG03", "endoscopy", r"Endoscopy|Bronchoscopy"),
    ("TG04", "theatre", r"Theatre|\bTh$|\bDSU\b|GA Suite"),
    ("TG05", "sdec", r"SDEC"),
    ("TG06", "ed", r"A&E|\bED\b|\bMIU\b"),
    ("TG07", "labour", r"Labour|Delivery|\bBBC\b|\bBC$|\bObs$"),
)
PRICED_TRACKING_CLASSES = ("ed", "sdec")          # priced from EMC* via calibration
PRICED_BY_OTHER_TRACKING_CLASSES = ("theatre",)   # theatre_case carries the £

# ---- PLICS activity cost-code families (spec §4.1, §4.2, §10.6) ------------------------
PLICS_FAMILY = {
    "WRD": ("ward_bed_day", "ward_care"),
    "THR": ("theatre_case", "theatre"),
    "EMC": ("tracking_attendance", "ed"),
    "DIM": ("imaging_exam", "imaging"),
    "PAT": ("pathology_tests", "pathology"),
    "PHA": ("medication_admin", "pharmacy_other"),
    "BLD": ("transfusion", "blood"),
    "OUT": ("outpatient_appointment", "outpatient"),
    "SUP": (None, "overhead"),
}
# PHA codes that are high-cost drugs (confirmed by Task 3 profiling; see notes/profiling.md).
HCD_PHA_CODES = frozenset({"PHA003"})


def plics_family(code):
    """Map an activity cost code to (stream, component); unknown prefixes are unallocated."""
    if code is None:
        return (None, "unallocated")
    if code in HCD_PHA_CODES:
        return ("high_cost_drug", "pharmacy_hcd")
    return PLICS_FAMILY.get(code[:3], (None, "unallocated"))


# ---- Pricing scope ---------------------------------------------------------------------
# Streams priced by calibrated unit costs. critical_care_day and neonatal_care_day are
# quantity-only in v1 (priced within ward bed-days, spec §10.6); high_cost_drug carries
# its own ledger £.
PRICED_STREAMS = (
    "ward_bed_day", "theatre_case", "outpatient_appointment", "tracking_attendance",
    "pathology_tests", "imaging_exam", "medication_admin", "transfusion",
)
# Overhead setting per stream (spec §10.4) and per PLICS feed_type.
STREAM_SETTING = {
    "ward_bed_day": "admitted", "theatre_case": "admitted", "pathology_tests": "admitted",
    "imaging_exam": "admitted", "medication_admin": "admitted", "transfusion": "admitted",
    "outpatient_appointment": "outpatient", "tracking_attendance": "emergency",
}
FEED_TYPE_SETTING = {"APC": "admitted", "OP": "outpatient", "OUT": "outpatient", "OUTS": "outpatient", "EC": "emergency"}
MIN_CALIBRATION_ACTIVITIES = 100


def financial_year_label(d):
    """NHS financial-year label for a date: 2023-04-01 → '2023/24'."""
    start = d.year if d.month >= 4 else d.year - 1
    return f"{start}/{(start + 1) % 100:02d}"
