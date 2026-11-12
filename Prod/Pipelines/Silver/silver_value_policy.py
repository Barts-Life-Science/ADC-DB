"""Silver column value policy (INSR_W2_VALUE_POLICY_V1, release INSR_20261029 workstream W2).

One importable place for column-level rules that apply after a public table's builder has produced its rows:

  §1  Millennium "0 = no reference" ids and codes become NULL; personnel id 1 (the Cerner system user) is flagged
      in <role>_system_ind and then NULLed (J5, F7). Hub tables lose their phantom id-0 row.
  §2  Derived columns: additive companions computed from columns the table already publishes (J22 and others).
      Source columns are never changed by §2.
  §3+ Helpers used inside builders (time-only values, W2.07; ...), appended by later W2 tasks.

Keys are minted inside the builders, before this policy runs, so every key still hashes the original source text.
The shared `materialized_view` decorator calls `finish_public` for the tables listed here; notebook 7 calls it from
`canonical()`. pyspark is imported lazily so the registries can be unit-tested without Spark.
"""
from collections import namedtuple

POLICY_VERSION = "INSR_W2_VALUE_POLICY_V1"


def _F():
    """pyspark.sql.functions, imported on first use (keeps the registries importable without Spark)."""
    from pyspark.sql import functions as F
    return F


# ---------------------------------------------------------------------------------------------------------------
# §1 Millennium zero references and the system user (W2.01; ledger P4a/P4d/P4e/P4f)
# ---------------------------------------------------------------------------------------------------------------

# Zero carries a meaning in these columns (P4d, DOCUMENT): never NULLed.
MILL_ZERO_KEEP = frozenset({"orig_action_id", "sch_reason_cd", "recurrence_parent_sch_event_id", "period_oid"})

# P4a ruling list: 58 FK ids + 40 codes (gold names = silver names; allergy severity_code added 2026-10-31). Zero means "no reference" in each.
# clinical_appointment_action.req_action_id: the requesting action; 0 = no request (W0.11 doubt resolved by the
# W2.01 Step 6 prod aggregate: no action has sch_action_id 0).
ZERO_AS_NULL = {
    # Ben 2026-10-31 (S04.1 ruling): allergy severity 0 is "no severity recorded", not a code; it had no display.
    "clinical_allergy_intolerance": ("severity_code",),
    "clinical_appointment": ("allocated_practitioner_id", "organization_id", "requested_practitioner_id"),
    "clinical_appointment_action": ("organization_id", "req_action_id", "schedule_id", "resource_cd"),
    "clinical_bed_status_observation": ("bed_status_cd",),
    "clinical_clinical_finding": ("order_id", "organization_id", "performer_practitioner_id", "verifier_practitioner_id"),
    "clinical_clinical_score": ("performer_practitioner_id",),
    "clinical_condition": ("asserter_practitioner_id", "encounter_slice_id", "recorder_practitioner_id"),
    "clinical_condition_revision": ("severity_code", "verification_status_code"),
    "clinical_endoscopy_finding": ("encounter_id",),
    "clinical_family_history": ("encounter_id",),
    "clinical_medication_admin": ("verifier_practitioner_id",),
    "clinical_medication_admin_status": ("status_code",),
    "clinical_medication_order": ("frequency_id", "requester_practitioner_id"),
    "clinical_medication_order_status": ("action_qualifier_code",),
    "clinical_pending_movement": ("attenddoc_id", "accommodation_cd", "disch_disposition_cd", "disch_to_loctn_cd",
                                  "isolation_cd", "med_service_cd", "pend_bed_cd", "pend_building_cd",
                                  "pend_facility_cd", "pend_nurse_unit_cd", "pend_room_cd", "pending_priority_cd",
                                  "pending_status_cd", "transaction_reason_cd"),
    "clinical_prearrival": ("pcp_provider_id", "referring_source_id", "reg_prsnl_id", "prearrival_type_cd", "sex_cd"),
    "clinical_procedure": ("performer_practitioner_id",),
    "clinical_research_enrollment": ("encounter_id", "protocol_arm_id", "source_episode_id"),
    "clinical_rtt_activity": ("appointment_oid",),
    "clinical_tracking_attendance": ("checkin_id", "checkout_id", "primary_doc_id", "primary_nurse_id",
                                     "reactivate_user_id", "registration_status_id", "secondary_doc_id",
                                     "secondary_nurse_id", "specialty_id", "team_id", "checkout_disposition_cd",
                                     "family_present_cd"),
    "clinical_tracking_episode": ("cur_tracking_locator_id", "parent_entity_id", "base_loc_cd"),
    "clinical_tracking_location_stay": ("tracking_acuity_level_id", "unavail_tracking_event_id", "loc_bed_cd",
                                        "loc_room_cd", "tracking_reason_cd"),
    "clinical_tracking_milestone": ("complete_id", "onset_id", "requested_id", "event_status_cd",
                                    "tracking_event_type_cd"),
    "clinical_waiting_list_entry": ("sch_event_id", "location_code"),
    "clinical_waiting_list_snapshot": ("sch_event_id", "location_code"),
    "reference_location_attribute_history": ("value_id", "value_cd"),
    "reference_location_unit": ("current_bed_status_cd",),
    "reference_theatre_attendance": ("attendee_personnel_id", "attendee_practitioner_id"),
    "reference_theatre_case": ("anaesthetist_practitioner_id", "surgeon_practitioner_id", "surgical_specialty_id",
                               "asa_class_code", "cancel_reason_code", "case_level_code", "patient_type_code",
                               "wound_class_code"),
    "reference_theatre_implant": ("implanted_by_practitioner_id", "item_id"),
    "spine_care_participation": ("practitioner_id",),
    "spine_person": ("ethnicity_source_code",),
    "text_document": ("author_practitioner_id", "author_source_id"),
}
# Personnel columns whose id 1 is the Cerner system user (P4e): column -> role used in <role>_system_ind.
# The system-user rule NULLs both 1 (system user) and 0 (no person) in these columns itself, so a column listed
# here needs no ZERO_AS_NULL entry (action_prsnl_id has none: P4a found no zeros there).
SYSTEM_USER_COLUMNS = {
    "clinical_appointment_action": {"action_prsnl_id": "action"},
    "clinical_clinical_finding": {"performer_practitioner_id": "performer", "verifier_practitioner_id": "verifier"},
    "clinical_clinical_score": {"performer_practitioner_id": "performer"},
    "clinical_condition": {"recorder_practitioner_id": "recorder"},
    "clinical_medication_order": {"requester_practitioner_id": "requester"},
    "clinical_tracking_attendance": {"reactivate_user_id": "reactivate"},
    "text_document": {"author_practitioner_id": "author"},
}

# Hub tables whose id-0 row is a phantom "no reference" member (P4f): the row is removed.
HUB_ZERO_KEYS = {
    "reference_practitioner": "practitioner_id",
    "reference_practitioner_group": "practitioner_id",
    "reference_practitioner_identifier": "practitioner_id",
    "reference_research_study": "source_protocol_id",
    "reference_pregnancy_reconciliation": "pregnancy_id",
}

# Measured values where 0 means "not recorded" (P7_L2686): NULLed like ids.
ZERO_VALUE_AS_NULL = {"clinical_family_history": ("onset_age",)}

# Tables whose policy waits for the next full rebuild (W2.15(d) rebuild_patchset), so this release does not pay
# for them: clinical_medication_order_status is over ~500M rows and no other W2 task recomputes it; spine_person is
# small, but every fact _qc_* primitive joins it (shared:1062), so a definition change there rewrites spine_person
# and cascades a full recompute into every fact table (the 10-21 release-day pattern; correction 22).
REBUILD_ONLY = {"clinical_medication_order_status", "spine_person"}


def _is_zero(column):
    """True when a column holds a numeric zero in any type (0, 0.0, '0', '0.0'); NULL and text give False.

    The text test decides, not a cast: a DOUBLE id 0.0 renders as '0.0' (P_W112 lesson: an equality test with '0'
    misses it), and a non-numeric code is never a zero.
    """
    F = _F()
    text = F.trim(column.cast("string"))
    return F.coalesce(text.rlike(r"^[+-]?0+(\.0+)?$"), F.lit(False))


def zero_to_null(column):
    """Millennium uses 0 as 'no reference': return NULL for a numeric zero, the value (same type) otherwise."""
    F = _F()
    return F.when(~_is_zero(column), column)


def try_bigint(name):
    """Column `name` as BIGINT, NULL when its text is not a number.

    try_cast through F.expr (portable below Spark 3.5): under ANSI a plain .cast("bigint") raises on text such
    as 'UNKNOWN' and would fail the whole MV refresh.
    """
    return _F().expr(f"try_cast(`{name}` AS BIGINT)")


def mill_fk(name):
    """Millennium id column `name` as BIGINT with 0 -> NULL (patient-flow ids, text_document encounter_id)."""
    F = _F()
    as_long = try_bigint(name)
    return F.when(as_long != 0, as_long)


def applies_to(table):
    """True when finish_public has anything to do for this public table name (leaf, e.g. clinical_condition)."""
    if table in REBUILD_ONLY:
        return False
    return (table in ZERO_AS_NULL or table in HUB_ZERO_KEYS or table in ZERO_VALUE_AS_NULL
            or table in DERIVED or table in VALUE_RULES or table in EXTRA_COLUMN_COMMENTS)


def apply_mill_zero_policy(df, table):
    """Flag the system user, NULL zero references/values, drop the hub id-0 row. Missing columns fail loudly."""
    F = _F()
    listed = list(ZERO_AS_NULL.get(table, ())) + list(ZERO_VALUE_AS_NULL.get(table, ()))
    missing = [c for c in listed if c not in df.columns]
    if missing:
        raise AssertionError(f"INSR_W2 value policy: {table} lacks listed columns {missing}")
    for column, role in SYSTEM_USER_COLUMNS.get(table, {}).items():
        source = F.col(column)
        df = df.withColumn(f"{role}_system_ind",
                           F.when(source.isNotNull(), F.coalesce(try_bigint(column) == 1, F.lit(False))))
        df = df.withColumn(column, F.when(~F.coalesce(try_bigint(column).isin(0, 1), F.lit(False)), source))
    for column in listed:
        df = df.withColumn(column, zero_to_null(F.col(column)))
    key = HUB_ZERO_KEYS.get(table)
    if key:
        df = df.where(F.col(key).isNull() | ~_is_zero(F.col(key)))
    return df


# ---------------------------------------------------------------------------------------------------------------
# §2 Derived columns (additive; inputs must already be published by the table)
# ---------------------------------------------------------------------------------------------------------------

# name: output column; inputs: columns it reads; build: callable(F) -> Column; comment: UC column comment.
Derived = namedtuple("Derived", "name inputs build comment")
DERIVED = {}


def register(table, name, inputs, build, comment):
    """Add one derived column to a table's registry (called at import by the §2 blocks of later tasks)."""
    DERIVED.setdefault(table, []).append(Derived(name, tuple(inputs), build, comment))


def apply_derived(df, table):
    """Add the table's derived columns in registry order. A missing input fails the graph build."""
    F = _F()
    for item in DERIVED.get(table, ()):
        missing = [c for c in item.inputs if c not in df.columns]
        if missing:
            raise AssertionError(f"INSR_W2 derived {table}.{item.name} lacks inputs {missing}")
        df = df.withColumn(item.name, item.build(F))
    return df


# ---------------------------------------------------------------------------------------------------------------
# Hook entry points
# ---------------------------------------------------------------------------------------------------------------

def column_comments(table):
    """UC comments for the columns this policy adds to a table (system flags and derived columns)."""
    comments = {f"{role}_system_ind": (f"True when the source {column} was 1, the Cerner system user (J5); the id is "
                                       f"then published as NULL. NULL when the source id was NULL. INSR_W2.")
                for column, role in SYSTEM_USER_COLUMNS.get(table, {}).items()}
    comments.update({d.name: d.comment for d in DERIVED.get(table, ())})
    comments.update(EXTRA_COLUMN_COMMENTS.get(table, {}))
    return comments


def finish_public(df, table):
    """§2 derived columns (read source values), then §4 value rules, then the §1 zero/system-user policy."""
    if not applies_to(table):
        return df
    return apply_mill_zero_policy(apply_value_rules(apply_derived(df, table), table), table)


# ---------------------------------------------------------------------------------------------------------------
# §2 registrations
# ---------------------------------------------------------------------------------------------------------------

# W2.02 (J26): the tracking milestone configuration changed on 2024-04-01; the era lets readers split series.
register("clinical_tracking_milestone", "milestone_config_era", ["milestone_datetime"],
         lambda F: F.when(F.col("milestone_datetime") < F.lit("2024-04-01").cast("timestamp"), F.lit("pre_2024_04"))
                    .when(F.col("milestone_datetime").isNotNull(), F.lit("post_2024_04")),
         "Tracking milestone configuration era: pre_2024_04 or post_2024_04 by milestone_datetime (the "
         "configuration changed on 2024-04-01, so step changes at that date are configuration, not activity). "
         "NULL when milestone_datetime is NULL. INSR_W2.")

# W2.06 J23 (L2306): PLICS FY-end defaults. The supplied end is kept as episode_end_raw and the default is flagged;
# the value rule in §4 NULLs the defaulted end on admitted PODs only.
_J23_DEFAULT = "2021-03-31 23:59:00"
_J23_ADMITTED_PODS = ("NEL", "EL", "DC")


def _j23_defaulted(F):
    """True when episode_end_datetime is the PLICS FY-end default (2021-03-31 23:59, no departure date)."""
    return F.coalesce((F.col("episode_end_datetime") == F.lit(_J23_DEFAULT).cast("timestamp"))
                      & F.col("departure_date").isNull(), F.lit(False))


register("clinical_costed_activity", "episode_end_raw", ["episode_end_datetime"],
         lambda F: F.col("episode_end_datetime"),
         "episode_end_datetime exactly as supplied by the PLICS extract, before the J23 FY-end default rule. INSR_W2.")
register("clinical_costed_activity", "episode_end_defaulted_ind", ["episode_end_datetime", "departure_date"],
         _j23_defaulted,
         "True when the episode end is the PLICS financial-year-end default (2021-03-31 23:59 with no departure "
         "date): an extract boundary, not a discharge (J23). INSR_W2.")

# W2.06 (L2306 sibling): CSDS community activity dated 1 January before July 2007 is a year-only default.
register("clinical_community_care_activity", "january_default_suspected", ["event_datetime"],
         lambda F: F.coalesce((F.month("event_datetime") == 1) & (F.dayofmonth("event_datetime") == 1)
                              & (F.col("event_datetime") < F.lit("2007-07-01").cast("timestamp")), F.lit(False)),
         "True for activity dated 1 January before 2007-07, where the date heaps at more than 5x the other January "
         "days: the source recorded the year only. INSR_W2.")

# W2.06 (L3185): mortality review completion, from the dates the form already publishes.
register("clinical_registry_mortality_review", "review_status",
         ["date_of_completion_of_form", "date_of_mortality_review"],
         lambda F: F.when(F.col("date_of_completion_of_form").isNotNull(), F.lit("completed"))
                    .when(F.col("date_of_mortality_review").isNotNull(), F.lit("review_dated_not_completed"))
                    .otherwise(F.lit("not_completed")),
         "Mortality review state: completed (form completion date set), review_dated_not_completed (review date "
         "only) or not_completed. INSR_W2 (L3185).")

# W2.06 (L5172): ODS validity of the registered practice code.
register("reference_person_gp_registration", "practice_code_ods_valid", ["practice_ods_code"],
         lambda F: F.when(F.col("practice_ods_code").isNull(), F.lit(None).cast("string"))
                    .when(F.upper(F.col("practice_ods_code")).rlike(r"^V81"), F.lit("pseudo_v81"))
                    .when(F.upper(F.col("practice_ods_code")).rlike(r"^5[A-Z0-9]{2}$"), F.lit("legacy_pct"))
                    .when(F.upper(F.col("practice_ods_code")).rlike(r"^[A-Z][0-9]{5}$"), F.lit("valid"))
                    .otherwise(F.lit("malformed")),
         "Practice code class: valid (England/Wales/NI letter + 5 digits), pseudo_v81 (V81 'no registered GP' "
         "pseudo codes), legacy_pct (5xx primary care trust codes), malformed, or NULL. INSR_W2 (L5172).")

# W2.10 (J38 L3465): WL entry waiting start: source, else referral > decision to admit > requested (fill order P7_L3465b).
# Each pair is (published column, basis label); the tuple order is the precedence.
_WAIT = (("event_datetime", "source_waiting_start"), ("referral_datetime", "referral"),
         ("admit_decision_datetime", "decision_to_admit"), ("requested_datetime", "requested"))
register("clinical_waiting_list_entry", "waiting_start_derived", [c for c, _ in _WAIT],
         lambda F: F.coalesce(*[F.col(c) for c, _ in _WAIT]),
         "Waiting start: the source WAITING_START_DT_TM, else referral, else decision to admit, else requested "
         "(precedence by measured fill, P7_L3465b). The source column is unchanged. INSR_W2 W2.10 (J22, J38).")
register("clinical_waiting_list_entry", "waiting_start_basis", [c for c, _ in _WAIT],
         lambda F: F.coalesce(*[F.when(F.col(c).isNotNull(), F.lit(b)) for c, b in _WAIT]),
         "Which column supplied waiting_start_derived; NULL iff it is NULL. INSR_W2 W2.10.")

# W2.10 (L2637): elective access event time: the decision to admit, else the entry's creation time.
register("clinical_elective_access_entry", "event_datetime_derived", ["decided_to_admit_datetime", "created_datetime"],
         lambda F: F.coalesce(F.col("decided_to_admit_datetime"), F.col("created_datetime")),
         "Clinical event time for the entry: decided_to_admit_datetime, else created_datetime (event_datetime is the "
         "LUNA creation time, L2637). INSR_W2 W2.10 (J22).")
register("clinical_elective_access_entry", "event_datetime_basis", ["decided_to_admit_datetime", "created_datetime"],
         lambda F: F.when(F.col("decided_to_admit_datetime").isNotNull(), F.lit("decided_to_admit"))
                    .when(F.col("created_datetime").isNotNull(), F.lit("created")),
         "decided_to_admit or created; NULL iff event_datetime_derived is NULL. INSR_W2 W2.10.")

# W2.10 (L1936, L1326): theatre procedure start and surgery duration.
register("reference_theatre_case", "procedure_start_derived", ["surgery_start", "checkin_datetime", "scheduled_start", "case_status"],
         lambda F: F.coalesce(F.col("surgery_start"), F.col("checkin_datetime"),
                              F.when(F.col("case_status") == "PERFORMED", F.col("scheduled_start"))),
         "Surgery start, else theatre check-in, else (performed cases only) the scheduled start. INSR_W2 W2.10 (L1936).")
register("reference_theatre_case", "time_source", ["surgery_start", "checkin_datetime", "scheduled_start", "case_status"],
         lambda F: F.when(F.col("surgery_start").isNotNull(), F.lit("surgery_start"))
                    .when(F.col("checkin_datetime").isNotNull(), F.lit("checkin"))
                    .when((F.col("case_status") == "PERFORMED") & F.col("scheduled_start").isNotNull(), F.lit("scheduled_start")),
         "Which time supplied procedure_start_derived. INSR_W2 W2.10.")


def _surgery_stop_minus_start_minutes(F):
    """Minutes from surgery_start to surgery_stop (fractional); NULL when either end is NULL."""
    return (F.unix_timestamp("surgery_stop") - F.unix_timestamp("surgery_start")) / 60


register("reference_theatre_case", "duration_minutes_derived", ["surgery_duration_minutes", "surgery_start", "surgery_stop"],
         lambda F: F.when(F.col("surgery_duration_minutes").between(1, 1440), F.col("surgery_duration_minutes"))
                    .when(_surgery_stop_minus_start_minutes(F).between(1, 1440),
                          _surgery_stop_minus_start_minutes(F).cast("int")),
         "Surgery duration in minutes within 1-1440: the bronze value, else stop minus start; NULL otherwise "
         "(PMS Device L1326 outliers). INSR_W2 W2.10.")
register("reference_theatre_case", "duration_source", ["surgery_duration_minutes", "surgery_start", "surgery_stop"],
         lambda F: F.when(F.col("surgery_duration_minutes").between(1, 1440), F.lit("bronze_duration"))
                    .when(_surgery_stop_minus_start_minutes(F).between(1, 1440), F.lit("stop_minus_start")),
         "bronze_duration or stop_minus_start; NULL iff duration_minutes_derived is NULL. INSR_W2 W2.10.")

# W2.10 (L5193) spine_episode period_start/end_derived: NOT built. Gate W2_10_episode_drift (ledger) found the
# earliest member-encounter start more than 1 day from the source period_start on 83.3% of 4.33M episodes
# (median 27 days later), above the 0.20 ship line, so neither the episode edit nor its comments ship.

# W2.11 (L3301, J18): period_oid 0 is LUNA's clockless-pathway sentinel (MILL_ZERO_KEEP); flag it.
register("clinical_rtt_pathway", "is_clockless", ["period_oid"],
         lambda F: F.coalesce(F.col("period_oid").cast("bigint") == 0, F.lit(False)),
         "True for a clockless pathway (period_oid 0, LUNA's no-clock sentinel; kept, not NULLed). INSR_W2 W2.11.")

# W2.11 (L3029): ACS troponin in ng/L, with the unit decided per assay (measured in prod 2026-10-30):
#   High sensitivity Troponin T: 8,232 rows, median 0.06, 7,603 below 5 -> recorded in ug/L;
#   Troponin I / Troponin T:     medians 0.16 / 0.07                     -> recorded in ug/L;
#   High sensitivity Troponin I: 1,918 rows, median 101                  -> recorded in ng/L (values < 5 are genuine);
#   Unknown / NULL assay (146 rows): unit cannot be decided              -> NULL, unit_method 'undetermined'.
# ug/L assays: values below 5 are x1000; values >= 5 were already ng/L and are kept. The source value is unchanged.
_TROPONIN_UG_L_ASSAYS = ("high sensitivity troponin t", "troponin t", "troponin i")
_TROPONIN_NG_L_ASSAYS = ("high sensitivity troponin i",)


def _troponin_assay(F):
    """Lower-cased, single-spaced assay name (NULL stays NULL)."""
    return F.lower(F.trim(F.regexp_replace(F.col("troponin_assay"), r"\s+", " ")))


register("clinical_registry_acs_transfer", "peak_troponin_ng_l", ["peak_troponin", "troponin_assay"],
         lambda F: F.when(_troponin_assay(F).isin(*_TROPONIN_NG_L_ASSAYS), F.col("peak_troponin"))
                    .when(_troponin_assay(F).isin(*_TROPONIN_UG_L_ASSAYS),
                          F.when(F.col("peak_troponin") < 5, F.col("peak_troponin") * 1000)
                           .otherwise(F.col("peak_troponin"))),
         "Peak troponin in ng/L, unit decided per assay: High sensitivity Troponin I is recorded in ng/L and kept; "
         "High sensitivity Troponin T, Troponin T and Troponin I are recorded in ug/L, so values below 5 are x1000 "
         "(values >= 5 were already ng/L). NULL when the assay is unknown or missing. INSR_W2 W2.11 (L3029).")
register("clinical_registry_acs_transfer", "unit_method", ["peak_troponin", "troponin_assay"],
         lambda F: F.when(F.col("peak_troponin").isNull(), F.lit(None).cast("string"))
                    .when(_troponin_assay(F).isin(*_TROPONIN_NG_L_ASSAYS), F.lit("assay_ng_l"))
                    .when(_troponin_assay(F).isin(*_TROPONIN_UG_L_ASSAYS) & (F.col("peak_troponin") < 5),
                          F.lit("assay_ug_l_x1000"))
                    .when(_troponin_assay(F).isin(*_TROPONIN_UG_L_ASSAYS), F.lit("assay_ug_l_value_already_ng_l"))
                    .otherwise(F.lit("undetermined")),
         "How peak_troponin_ng_l was derived: assay_ng_l, assay_ug_l_x1000, assay_ug_l_value_already_ng_l, or "
         "undetermined (unknown/missing assay; peak_troponin_ng_l NULL). INSR_W2 W2.11.")

# W2.11 (L5028): PLICS activity counts as decimals (the source mixes fractional counts into a text column).
# activity_count is published by the SLAM cost-line table (4_silver_care_pathways:3354-3400, public leaf
# reference_cost_line_item, checked in 4_prod.information_schema 2026-10-30); clinical_costed_activity has only
# cost_line_count.
register("reference_cost_line_item", "activity_count_decimal", ["activity_count"],
         lambda F: F.expr("try_cast(activity_count AS DECIMAL(18,4))"),
         "activity_count as DECIMAL(18,4); NULL when the source text is not a number. INSR_W2 W2.11 (L5028).")

# W2.11 (J24 neonatal): the NNU level as a number 1-3 from the free-text unit level.
register("clinical_neonatal_episode", "unit_level_derived", ["unit_level"],
         lambda F: F.expr("try_cast(regexp_extract(unit_level, '([123])', 1) AS INT)"),
         "Neonatal unit level 1, 2 or 3 parsed from unit_level (first digit 1-3); NULL when absent. INSR_W2 W2.11.")

# W2.11 (J25): NEWS2 and MUST bands from the total score.
_NEWS2 = r"(?i)\bnews[\s\S]?2\b|national early warning"
_MUST = r"(?i)\bmust\b|malnutrition universal"
register("clinical_clinical_score", "score_arm", ["source_display"],
         lambda F: F.when(F.coalesce(F.col("source_display"), F.lit("")).rlike(_NEWS2), F.lit("news2"))
                    .when(F.coalesce(F.col("source_display"), F.lit("")).rlike(_MUST), F.lit("must")),
         "news2 or must when the score is one of those instruments' totals; NULL otherwise. INSR_W2 W2.11.")
register("clinical_clinical_score", "score_band", ["source_display", "value_number"],
         lambda F: F.when(F.coalesce(F.col("source_display"), F.lit("")).rlike(_NEWS2),
                          # A total outside the instrument's range (NEWS2 0-20) is not a total: no band.
                          F.when(F.col("value_number") > 20, F.lit(None).cast("string"))
                           .when(F.col("value_number") >= 7, "high").when(F.col("value_number") >= 5, "medium")
                           .when(F.col("value_number") >= 0, "low"))
                    .when(F.coalesce(F.col("source_display"), F.lit("")).rlike(_MUST),
                          # MUST totals run 0-6; prod carries 'MUST Score' rows of 23-235235 (W2 review 2026-10-30).
                          F.when(F.col("value_number") > 6, F.lit(None).cast("string"))
                           .when(F.col("value_number") >= 2, "high").when(F.col("value_number") == 1, "medium")
                           .when(F.col("value_number") == 0, "low")),
         "Risk band: NEWS2 total 0-4 low, 5-6 medium, 7-20 high (RCP 2017); MUST 0 low, 1 medium, 2-6 high (BAPEN); "
         "NULL for a value outside the instrument's range. The NEWS2 single-parameter-3 rule needs components and is "
         "not applied. INSR_W2 W2.11.")
register("clinical_clinical_score", "band_rule_id", ["source_display"],
         lambda F: F.when(F.coalesce(F.col("source_display"), F.lit("")).rlike(_NEWS2), F.lit("rcp_news2_2017_aggregate"))
                    .when(F.coalesce(F.col("source_display"), F.lit("")).rlike(_MUST), F.lit("bapen_must_2003")),
         "Rule that set score_band. INSR_W2 W2.11.")

# W2.11 (L2513, P9_L2513): A3A = 99.9% of unresolved commissioner codes (77.9% of all community contacts, ledger
# W2_11_a3a); the ODS check (lanes/ods_a3a_check.py -> lanes/ods_a3a.json, 2026-10-30) confirmed it is the
# 3-character prefix of exactly one active sub-ICB location (A3A8R, RO98) and named its parent ICB (QMF, via RE5).
_A3A = {"alias": "A3A", "ods_code": "A3A8R", "parent_icb_ods_code": "QMF"}   # from lanes/ods_a3a.json
register("clinical_community_care_contact", "commissioner_ods_code_resolved", ["commissioner_ods_code"],
         lambda F: F.when(F.upper(F.trim(F.col("commissioner_ods_code"))) == _A3A["alias"], F.lit(_A3A["ods_code"]))
                    .otherwise(F.col("commissioner_ods_code")),
         "Commissioner ODS code with the 3-character form A3A resolved to its sub-ICB location (ODS ORD API, "
         "RO98/RO319); other codes unchanged. INSR_W2 W2.11 (L2513).")
register("clinical_community_care_contact", "commissioner_parent_icb_ods_code", ["commissioner_ods_code"],
         lambda F: F.when(F.upper(F.trim(F.col("commissioner_ods_code"))) == _A3A["alias"],
                          F.lit(_A3A["parent_icb_ods_code"])),
         "Parent ICB of the sub-ICB location resolved from A3A (ODS RE5 relationship); NULL for other codes. "
         "INSR_W2 W2.11.")
register("clinical_community_care_contact", "commissioner_resolution_method", ["commissioner_ods_code"],
         lambda F: F.when(F.upper(F.trim(F.col("commissioner_ods_code"))) == _A3A["alias"],
                          F.lit("ods:sub_icb_location_prefix:A3A"))
                    .when(F.col("commissioner_ods_code").isNotNull(), F.lit("submitted")),
         "submitted, or ods:sub_icb_location_prefix:A3A when resolved from the 3-character form. INSR_W2 W2.11.")

# W2.11 not built here (gate results in the ledger, 8_dev.insr_probe.ledger): the neonatal exam gate (P9_L2905),
# request-thread person basis (P9_L3932b), pause_days sign (P9_L5193), score-component fold, and the pathology
# request-thread lab-number fallback edge (W2_11_arm: accessions per lab number rose only 1.14x from 2017-08 while
# lab numbers held, so the edge loss is not the accession split; DOCUMENT in W8.05).


# ---------------------------------------------------------------------------------------------------------------
# §5 Extra public columns produced by canonical lanes of _cross_qc_public models (W2.04, W2.05, W2.20)
# ---------------------------------------------------------------------------------------------------------------
# model_name (as passed to _cross_qc_public) -> columns appended to the public column list when present.
EXTRA_PUBLIC_COLUMNS = {}
# public table leaf -> {column: comment}; merged into column_comments().
EXTRA_COLUMN_COMMENTS = {}


def add_public_column(model_name, table, column, comment):
    """Publish a canonical column the model's public list does not name yet, with its UC comment."""
    EXTRA_PUBLIC_COLUMNS.setdefault(model_name, []).append(column)
    EXTRA_COLUMN_COMMENTS.setdefault(table, {})[column] = comment


def public_columns_for(model_name, public_columns, available):
    """The model's public column list plus its extra columns that the rows actually carry."""
    extra = [c for c in EXTRA_PUBLIC_COLUMNS.get(model_name, ()) if c in available and c not in public_columns]
    return list(public_columns) + extra


add_public_column("presenting_complaint", "clinical_presenting_complaint", "source_text",
                  "Free text the patient or registration clerk recorded (registration reason for visit, ED free-text "
                  "complaint). These arms carry no code, so source_code is NULL on them; the exact ECDS pick-list "
                  "mapping applies only to coded rows. INSR_W2 W2.04 (L2982).")

# W2.19(b) (L931) and L4261: the problem arm carries its life-cycle status date and the onset precision Millennium
# recorded. The meaning of each ONSET_DT_FLAG value is not asserted here: the W2_19 probe found flag values 1 and 2
# only partly aligned with first-of-month / 1 January local dates, so the comment states what the column is.
for _c, _t in (
        ("clinical_status_effective_datetime",
         "When the problem took its current life-cycle status (map_problem LIFE_CYCLE_DT_TM, UTC instant); NULL on "
         "diagnosis and maternity rows. INSR_W2 W2.19 (L931)."),
        ("onset_precision_flag",
         "Millennium ONSET_DT_FLAG as recorded: the precision indicator for onset_datetime (0 = full date and time; "
         "a non-zero value means reduced precision, so the day or month of onset_datetime may be a placeholder). "
         "Set on every dated problem onset; NULL on diagnosis and maternity rows. INSR_W2 (L4261)."),
        ("onset_precision_code",
         "Millennium ONSET_DT_CD (code value qualifying the onset date; 0, the no-code value, is NULL; set on about 0.5% "
         "of active problems). NULL on diagnosis and maternity rows. INSR_W2 (L4261)."),
):
    add_public_column("condition", "clinical_condition", _c, _t)

# W2.03: provenance of the OMOP ids filled by the Maps-to 'omop' / 'source_concept' lanes (master provenance rule).
# This is patches/w2_03.POLICY_APPENDIX, which W2.03 left for W2.04 to append after this section and which was
# missing until the W2 review (2026-10-30): without it the lanes computed the pair but _cross_qc_finish dropped it.
for _model, _table, _axis in (("condition", "clinical_condition", "condition"),
                              ("vital_sign", "clinical_vital_sign", "vital"),
                              ("allergy_intolerance", "clinical_allergy_intolerance", "substance")):
    add_public_column(_model, _table, f"{_axis}_omop_map_rule_id",
                      f"Rule that filled {_axis}_omop_concept_id / {_axis}_omop_source_concept_id from the OMOP "
                      "'Maps to' lookup (omop:maps_to:<vocabulary>); NULL when those ids came with the row's own "
                      "mapping. INSR_W2 W2.03.")
    add_public_column(_model, _table, f"{_axis}_omop_map_version",
                      f"Vocabulary release (<vocabulary>|<version>) of the lookup that filled the {_axis} OMOP ids; "
                      "NULL when the lane filled nothing. INSR_W2 W2.03.")


# ---------------------------------------------------------------------------------------------------------------
# §4 Value rules: listed changes to existing columns, applied after §2 and before §1 (W2.05, W2.06, W2.07)
# ---------------------------------------------------------------------------------------------------------------
# Placed below §5 because it uses add_public_column and EXTRA_COLUMN_COMMENTS. applies_to() and finish_public()
# read VALUE_RULES / apply_value_rules at call time, after the whole module has been imported.
# table -> [(column, inputs, build(F) -> Column)]; inputs must be published by the table.
VALUE_RULES = {}


def value_rule(table, column, inputs, build):
    """Register one listed change to an existing column."""
    VALUE_RULES.setdefault(table, []).append((column, tuple(inputs), build))


def _zero_measure_rule(value, unit):
    """J24/F8: a measure of 0 with no unit (NULL, blank or the zero code) records nothing: NULL it."""
    def build(F):
        unit_blank = F.col(unit).isNull() | F.trim(F.col(unit).cast("string")).isin("", "0", "0.0")
        return F.when(~((F.col(value) == 0) & unit_blank), F.col(value))
    return build


ZERO_MEASURE_PAIRS = {
    "clinical_medication_admin": [("dose_value", "dose_unit"), ("initial_dose_value", "initial_dose_unit"),
                                  ("infused_volume", "infused_volume_unit"), ("infusion_rate", "infusion_rate_unit")],
    "clinical_medication_admin_ingredient": [("strength_value", "strength_unit"), ("volume_value", "volume_unit"),
                                             ("dose_value", "dose_unit"), ("rate_value", "rate_unit"),
                                             ("concentration_value", "concentration_unit")],
    "clinical_medication_order_ingredient": [("strength_value", "strength_unit"), ("volume_value", "volume_unit"),
                                             ("dose_value", "dose_unit"), ("rate_value", "rate_unit"),
                                             ("concentration_value", "concentration_unit")],
}
for _table, _pairs in ZERO_MEASURE_PAIRS.items():
    for _value, _unit in _pairs:
        value_rule(_table, _value, [_value, _unit], _zero_measure_rule(_value, _unit))

# Master W2.05 "initial_dose_value -> NULL" (F8: correct the initial_dose_value pass rule). Measured on a 1% prod
# sample (2026-10-30): every non-NULL initial_dose_value is 0 (547,324 of 547,324), always with a unit, so the
# zero-with-no-unit rule above never fires on it. A 0 initial dose records nothing: NULL it whatever the unit.
value_rule("clinical_medication_admin", "initial_dose_value", ["initial_dose_value"],
           lambda F: F.when(F.col("initial_dose_value") != 0, F.col("initial_dose_value")))

# Master W2.05 "infusion zeros on non-infusion rows -> NULL": a 0 infused volume or rate on an administration that
# is not IV (iv_ind false or NULL) records nothing (1% sample: 527,384 such rows, 39,566 of them with a unit).
# IV rows keep a recorded 0.
for _value in ("infused_volume", "infusion_rate"):
    value_rule("clinical_medication_admin", _value, [_value, "iv_ind"],
               lambda F, _v=_value: F.when(~((F.col(_v) == 0) & ~F.coalesce(F.col("iv_ind"), F.lit(False))), F.col(_v)))

# J24: when every ingredient dose of an administration is zero, the doses record nothing (the flag says so).
value_rule("clinical_medication_admin_ingredient", "dose_value", ["dose_value", "all_ingredients_zero_ind"],
           lambda F: F.when(~F.coalesce(F.col("all_ingredients_zero_ind"), F.lit(False)), F.col("dose_value")))
add_public_column("medication_admin", "clinical_medication_admin", "all_ingredients_zero_ind",
                  "True when every ingredient of this administration recorded a dose of 0 (J24): the ingredient "
                  "doses are then published as NULL in ingredients and clinical_medication_admin_ingredient. "
                  "INSR_W2 W2.05.")
EXTRA_COLUMN_COMMENTS.setdefault("clinical_medication_admin_ingredient", {})["all_ingredients_zero_ind"] = (
    "True when every ingredient of the parent administration recorded a dose of 0 (J24); dose_value is then NULL. "
    "INSR_W2 W2.05.")


# W2.06 J23 (L2306): admitted PODs (NEL/EL/DC; 216 rows) lose the defaulted end, which is not a discharge.
# Non-admitted PODs (1,342,681 rows) keep it, flagged by episode_end_defaulted_ind. A NULL pod_cd is not an admitted
# POD (isin on NULL is NULL, so it is coalesced; the lean fixture found the NULL path NULLing the end).
value_rule("clinical_costed_activity", "episode_end_datetime", ["episode_end_datetime", "departure_date", "pod_cd"],
           lambda F: F.when(~(_j23_defaulted(F)
                              & F.coalesce(F.upper(F.trim(F.col("pod_cd"))).isin(*_J23_ADMITTED_PODS), F.lit(False))),
                            F.col("episode_end_datetime")))

# W2.06 (L5267): the theatre implant date columns are built in notebook 4; their comments come from here.
EXTRA_COLUMN_COMMENTS.setdefault("reference_theatre_implant", {}).update({
    "implant_datetime": "Time of the theatre case the implant was logged against (surgery start, else check-in, "
                        "else scheduled start); the implant log has no own date. INSR_W2 W2.06 (L5267).",
    "implant_datetime_basis": "case_surgery_start, case_checkin or case_scheduled_start. INSR_W2 W2.06."})

# W2.09 (L5145): the death-evidence columns derived from person_scope's clinical-category last event (notebook 1).
EXTRA_COLUMN_COMMENTS.setdefault("reference_person_death_evidence", {}).update({
    "last_clinical_event_datetime_derived": "Latest clinical-category event across the silver event feeds "
                                            "(person_scope); excludes the administrative and bulk events that bronze "
                                            "LAST_CLINICAL_EVENT_DT_TM counts (L5145). INSR_W2.",
    "post_death_clinical_activity_ind": "True when a clinical-category event falls more than one day after the death "
                                        "date. INSR_W2."})

# W2.11 (L4164): thin-dimension member columns built in notebook 1; their comments come from here.
EXTRA_COLUMN_COMMENTS.setdefault("reference_organization", {})["member_source"] = (
    "map_care_site for organisations with a care-site row; in_use_fk for organisation ids that encounters, "
    "appointments, medication orders or ED tracking reference but map_care_site lacks (name and address unknown, "
    "status referenced_only). INSR_W2 W2.11 (L4164).")
EXTRA_COLUMN_COMMENTS.setdefault("reference_service", {})["member_source"] = (
    "personnel_group (MEDSERVICE_GROUP_ID from personnel assignments) or med_service_code (encounter "
    "MED_SERVICE_CD, named by its bronze description). INSR_W2 W2.11 (L4164).")
EXTRA_COLUMN_COMMENTS.setdefault("reference_location", {})["organization_id_basis"] = (
    "care_site_source when map_care_site carries the organisation; parent_care_site / grandparent_care_site / "
    "great_grandparent_care_site when a map_location_unit row inherits it through PARENT_LOCATION_CD; NULL when "
    "no care-site ancestor within three hops has one. INSR_W2 W2.11 (L4164).")

# W2.13 (L3665, F14c): every mapped specimen-type row carries a method and a rule id. Rows the bronze lookup mapped
# without saying how get rule / lookup.specimen_type_snomed_map; values already set (a lane's) are kept; unmapped
# rows stay NULL.
value_rule("clinical_specimen", "specimen_type_map_method", ["specimen_type_map_method", "specimen_type_target_code"],
           lambda F: F.when(F.col("specimen_type_target_code").isNotNull(),
                            F.coalesce(F.col("specimen_type_map_method"), F.lit("rule"))).otherwise(F.col("specimen_type_map_method")))
value_rule("clinical_specimen", "specimen_type_map_rule_id", ["specimen_type_map_rule_id", "specimen_type_target_code"],
           lambda F: F.when(F.col("specimen_type_target_code").isNotNull(),
                            F.coalesce(F.col("specimen_type_map_rule_id"), F.lit("lookup.specimen_type_snomed_map")))
                      .otherwise(F.col("specimen_type_map_rule_id")))

# W2.15(b) (J31, critic 2.5): text_document.encounter_id STRING -> BIGINT, 0 -> NULL. The text chain recomputes
# in the BI refresh anyway, so this costs nothing extra. Prod gate 2026-10-30 (W2.15 Step 1): 331,677,677 rows,
# cast_fail 0, zeros 578,261 (those become NULL).
value_rule("text_document", "encounter_id", ["encounter_id"], lambda F: mill_fk("encounter_id"))



# ---------------------------------------------------------------------------------------------------------------
# §3 Time-only values (W2.07, J4): Millennium stores a time of day with the placeholder date 2000-01-01.
# ---------------------------------------------------------------------------------------------------------------
# Placed after §4's value_rule because its rules use both register (§2) and value_rule (§4). The helpers are also
# imported by builders (silver_journey_shared date_event arm, 6_silver_forms response split).
TIME_ONLY_DATE = "2000-01-01"


def time_only(column):
    """True when a timestamp sits on the 2000-01-01 placeholder date (P3d: 99.9% non-midnight, x38,000 neighbours)."""
    F = _F()
    return F.coalesce(F.to_date(column) == F.lit(TIME_ONLY_DATE).cast("date"), F.lit(False))


def drop_time_only(column):
    """The timestamp, or NULL when it is a time-only placeholder."""
    F = _F()
    return F.when(~time_only(column), column)


def time_part(column):
    """'HH:mm:ss' for a time-only placeholder, else NULL."""
    F = _F()
    return F.when(time_only(column), F.date_format(column, "HH:mm:ss"))


register("clinical_clinical_finding", "value_time", ["value_datetime"],
         lambda F: time_part(F.col("value_datetime")),
         "Time of day (HH:mm:ss) when the source recorded a time with no date (Millennium placeholder date "
         "2000-01-01); value_datetime is then NULL. INSR_W2 W2.07 (J4, L2051).")
value_rule("clinical_clinical_finding", "value_datetime", ["value_datetime"],
           lambda F: drop_time_only(F.col("value_datetime")))

# P9_L2786: the endobase end is coalesce(TRUE_END_TS, END_TS); a modify clock months after the exam is not an end.
_ENDOBASE_LOAD_CLOCK = ("(source_object = 'endobase_exam' AND to_date(performed_end) = to_date(source_update_timestamp) "
                        "AND trunc(performed_end, 'MM') = trunc(source_update_timestamp, 'MM') "
                        "AND datediff(performed_end, performed_start) > 30)")
register("clinical_procedure", "performed_end_basis",
         ["source_object", "performed_start", "performed_end", "source_update_timestamp"],
         lambda F: F.when(F.expr(_ENDOBASE_LOAD_CLOCK), F.lit("load_clock_suppressed"))
                    .when(F.col("performed_end").isNotNull(), F.lit("source_end")),
         "source_end, or load_clock_suppressed where the Endobase end equals the record's update day and month and "
         "follows the start by more than 30 days (a modify clock, P9_L2786); performed_end is then NULL. INSR_W2.")
for _col in ("performed_end", "event_end_datetime", "event_end_datetime_local"):
    value_rule("clinical_procedure", _col, ["performed_end_basis", _col],
               # NULL basis (no performed_end) keeps the column: != on NULL is NULL (found by the lean fixture).
               lambda F, c=_col: F.when(F.coalesce(F.col("performed_end_basis"), F.lit("")) != "load_clock_suppressed",
                                        F.col(c)))


def apply_value_rules(df, table):
    """Apply the table's listed value rules in order. A missing input fails the graph build."""
    F = _F()
    for column, inputs, build in VALUE_RULES.get(table, ()):
        missing = [c for c in inputs if c not in df.columns]
        if missing:
            raise AssertionError(f"INSR_W2 value rule {table}.{column} lacks inputs {missing}")
        df = df.withColumn(column, build(F))
    return df


# INSR_W2 W2.20: comments and public columns of the W1 bronze carries (generated from w1_new_columns.json by patches/w2_20.py).
EXTRA_COLUMN_COMMENTS.setdefault('clinical_device', {}).update({'device_mapping_rule_id': 'Rule id of the device-type mapping row (INSR_W1_MEDICONNECT_V1). (bronze DEVICE_MAPPING_RULE_ID, INSR W1). INSR_W2 W2.20.', 'device_mapping_llm_decision': 'Verifier verdict of the device-type mapping row. (bronze DEVICE_MAPPING_LLM_DECISION, INSR W1). INSR_W2 W2.20.'})
EXTRA_COLUMN_COMMENTS.setdefault('clinical_medication_order_detail', {}).update({'detail_action_basis': 'LATEST_ACTION or CARRIED_FORWARD (INSR_W1_MED_DETAIL_EFFECTIVE_V1). INSR_W2 W2.20.'})
EXTRA_COLUMN_COMMENTS.setdefault('clinical_medication_supply', {}).update({'drug_mapping_rule_id': 'Rule id of an EMBEDDING_LLM_VERIFIED match. (bronze DRUG_MAPPING_RULE_ID, INSR W1). INSR_W2 W2.20.', 'drug_mapping_model': 'Verifier model of an EMBEDDING_LLM_VERIFIED match. (bronze DRUG_MAPPING_MODEL, INSR W1). INSR_W2 W2.20.', 'drug_mapping_score': 'Cosine similarity of an EMBEDDING_LLM_VERIFIED match. (bronze DRUG_MAPPING_SCORE, INSR W1). INSR_W2 W2.20.', 'homecare_contact_name': 'Homecare delivery contact name. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze HOMECARE_CONTACT_NAME, INSR W1). INSR_W2 W2.20.', 'homecare_contact_addr1': 'Homecare delivery address line 1. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze HOMECARE_CONTACT_ADDR1, INSR W1). INSR_W2 W2.20.', 'homecare_contact_addr2': 'Homecare delivery address line 2. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze HOMECARE_CONTACT_ADDR2, INSR W1). INSR_W2 W2.20.', 'homecare_contact_addr3': 'Homecare delivery address line 3. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze HOMECARE_CONTACT_ADDR3, INSR W1). INSR_W2 W2.20.', 'homecare_contact_addr4': 'Homecare delivery address line 4. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze HOMECARE_CONTACT_ADDR4, INSR W1). INSR_W2 W2.20.', 'homecare_contact_postcode': 'Homecare delivery postcode. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze HOMECARE_CONTACT_POSTCODE, INSR W1). INSR_W2 W2.20.', 'homecare_contact_telephone': 'Homecare delivery contact telephone. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze HOMECARE_CONTACT_TELEPHONE, INSR W1). INSR_W2 W2.20.'})
EXTRA_COLUMN_COMMENTS.setdefault('clinical_procedure', {}).update({'quantity_parse_rule': 'Rule set behind QUANTITY_NUMERIC/QUANTITY_PARSE_STATUS (IMPLANT_QTY_V2). (bronze QUANTITY_PARSE_RULE, INSR W1). INSR_W2 W2.20.', 'quantity_parse_status': 'Status indicator showing whether the recorded implant quantity text was successfully converted to a numeric value. (bronze QUANTITY_PARSE_STATUS, INSR W1). INSR_W2 W2.20.', 'procedure_date_basis': 'Basis of PROCEDURE_DT_TM_EFFECTIVE: proc_dt_tm, proc_start_dt_tm, proc_end_dt_tm or encounter_arrival. (bronze PROCEDURE_DATE_BASIS, INSR W1). INSR_W2 W2.20.'})
EXTRA_COLUMN_COMMENTS.setdefault('clinical_referral', {}).update({'referred_by_prid': 'LUNA ReferredBy_PRID: referring professional id (identifier). (bronze REFERRED_BY_PRID, INSR W1). INSR_W2 W2.20.', 'local_patient_id': "LUNA LocalPatientID (Barts MRN) of the referral's patient. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze LOCAL_PATIENT_ID, INSR W1). INSR_W2 W2.20.", 'nhs_number': "LUNA NHSNumber of the referral's patient, as recorded. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze NHS_NUMBER, INSR W1). INSR_W2 W2.20.", 'referral_reason': 'LUNA free-text referral reason. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze REFERRAL_REASON, INSR W1). INSR_W2 W2.20.', 'referral_comments': 'LUNA free-text referral comments. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze REFERRAL_COMMENTS, INSR W1). INSR_W2 W2.20.', 'referred_by_organisation': 'LUNA referring organisation text. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze REFERRED_BY_ORGANISATION, INSR W1). INSR_W2 W2.20.', 'ers_local_patient_id': 'e-RS LocalPatientID (MRN as sent by e-RS). Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_LOCAL_PATIENT_ID, INSR W1). INSR_W2 W2.20.', 'ers_nhs_number': 'e-RS NHSNumber. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_NHS_NUMBER, INSR W1). INSR_W2 W2.20.', 'ers_patient_name': 'e-RS patient name. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_PATIENT_NAME, INSR W1). INSR_W2 W2.20.', 'ers_referral_reason': 'e-RS free-text referral reason. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_REFERRAL_REASON, INSR W1). INSR_W2 W2.20.', 'ers_referral_comments': 'e-RS free-text referral comments. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_REFERRAL_COMMENTS, INSR W1). INSR_W2 W2.20.', 'ers_referred_by_organisation': 'e-RS referring organisation. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_REFERRED_BY_ORGANISATION, INSR W1). INSR_W2 W2.20.', 'ers_referred_by_team_name': 'e-RS referring team name. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_REFERRED_BY_TEAM_NAME, INSR W1). INSR_W2 W2.20.', 'ers_referred_to_team_name': 'e-RS receiving team name. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_REFERRED_TO_TEAM_NAME, INSR W1). INSR_W2 W2.20.', 'ers_lead_clinician_forename': 'e-RS lead clinician forename. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_LEAD_CLINICIAN_FORENAME, INSR W1). INSR_W2 W2.20.', 'ers_lead_clinician_surname': 'e-RS lead clinician surname. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_LEAD_CLINICIAN_SURNAME, INSR W1). INSR_W2 W2.20.', 'ers_referred_by_care_provider_forename': 'e-RS referring care provider forename. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_REFERRED_BY_CARE_PROVIDER_FORENAME, INSR W1). INSR_W2 W2.20.', 'ers_referred_by_care_provider_surname': 'e-RS referring care provider surname. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_REFERRED_BY_CARE_PROVIDER_SURNAME, INSR W1). INSR_W2 W2.20.', 'ers_referred_to_care_provider_forename': 'e-RS receiving care provider forename. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_REFERRED_TO_CARE_PROVIDER_FORENAME, INSR W1). INSR_W2 W2.20.', 'ers_referred_to_care_provider_surname': 'e-RS receiving care provider surname. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_REFERRED_TO_CARE_PROVIDER_SURNAME, INSR W1). INSR_W2 W2.20.'})
EXTRA_COLUMN_COMMENTS.setdefault('clinical_registry_eracs_episode', {}).update({'timing_of1st_time': "'HH:MM' parsed from TIMING_OF1ST (ERACS_TYPED_V1).", 'timing_of1st_parse_status': 'PARSED, BLANK or UNPARSED for TIMING_OF1ST.', 'timing_of2nd_time': "'HH:MM' parsed from TIMING_OF2ND (ERACS_TYPED_V1).", 'timing_of2nd_parse_status': 'PARSED, BLANK or UNPARSED for TIMING_OF2ND.', 'timing_of_ondansetron_time': "'HH:MM' parsed from TIMING_OF_ONDANSETRON (ERACS_TYPED_V1).", 'timing_of_ondansetron_parse_status': 'PARSED, BLANK or UNPARSED for TIMING_OF_ONDANSETRON.', 'timing_of_paracetamol_administered_time': "'HH:MM' parsed from TIMING_OF_PARACETAMOL_ADMINISTERED (ERACS_TYPED_V1).", 'timing_of_paracetamol_administered_parse_status': 'PARSED, BLANK or UNPARSED for TIMING_OF_PARACETAMOL_ADMINISTERED.', 'baseline_act_num': 'Leading number parsed from BASELINE_ACT (ERACS_TYPED_V1).', 'baseline_act_parse_status': 'PARSED, BLANK or UNPARSED for BASELINE_ACT.', 'post_protamine_act_num': 'Leading number parsed from POST_PROTAMINE_ACT (ERACS_TYPED_V1).', 'post_protamine_act_parse_status': 'PARSED, BLANK or UNPARSED for POST_PROTAMINE_ACT.'})
EXTRA_COLUMN_COMMENTS.setdefault('clinical_transfusion', {}).update({'end_quantity_unit_inferred': 'BT_QTY_UNIT_V1 inferred unit of END_QUANTITY_VALUE. (bronze END_QUANTITY_UNIT_INFERRED, INSR W1). INSR_W2 W2.20.', 'end_quantity_ml': 'END_QUANTITY_VALUE when the inferred unit is ML. (bronze END_QUANTITY_ML, INSR W1). INSR_W2 W2.20.', 'quantity_unit_rule': 'Rule id of the unit inference (BT_QTY_UNIT_V1). (bronze QUANTITY_UNIT_RULE, INSR W1). INSR_W2 W2.20.', 'isbt_product_concept_id': 'SNOMED blood-product concept for the ISBT 128 PRODUCT_CODE. (bronze ISBT_PRODUCT_CONCEPT_ID, INSR W1). INSR_W2 W2.20.', 'isbt_product_concept_name': 'Name of ISBT_PRODUCT_CONCEPT_ID. (bronze ISBT_PRODUCT_CONCEPT_NAME, INSR W1). INSR_W2 W2.20.', 'isbt_mapping_method': 'EXACT_NAME, EMBEDDING_LLM_VERIFIED or GROUP_RULE. (bronze ISBT_MAPPING_METHOD, INSR W1). INSR_W2 W2.20.', 'isbt_mapping_rule_id': 'Rule id of the ISBT mapping. (bronze ISBT_MAPPING_RULE_ID, INSR W1). INSR_W2 W2.20.', 'isbt_mapping_model': 'Verifier model of the ISBT mapping. (bronze ISBT_MAPPING_MODEL, INSR W1). INSR_W2 W2.20.', 'isbt_mapping_score': 'Similarity score of the ISBT mapping. (bronze ISBT_MAPPING_SCORE, INSR W1). INSR_W2 W2.20.', 'product_mapping_method': 'PRODUCT map provenance (method). (bronze PRODUCT_MAPPING_METHOD, INSR W1). INSR_W2 W2.20.', 'product_mapping_rule_id': 'PRODUCT map provenance (rule_id). (bronze PRODUCT_MAPPING_RULE_ID, INSR W1). INSR_W2 W2.20.', 'unit_group_mapping_method': 'UNIT_GROUP map provenance (method). (bronze UNIT_GROUP_MAPPING_METHOD, INSR W1). INSR_W2 W2.20.', 'unit_group_mapping_rule_id': 'UNIT_GROUP map provenance (rule_id). (bronze UNIT_GROUP_MAPPING_RULE_ID, INSR W1). INSR_W2 W2.20.', 'patient_group_mapping_method': 'PATIENT_GROUP map provenance (method). (bronze PATIENT_GROUP_MAPPING_METHOD, INSR W1). INSR_W2 W2.20.', 'patient_group_mapping_rule_id': 'PATIENT_GROUP map provenance (rule_id). (bronze PATIENT_GROUP_MAPPING_RULE_ID, INSR W1). INSR_W2 W2.20.', 'product_mapping_source': 'Map table the product concept came from. (bronze PRODUCT_MAPPING_SOURCE, INSR W1). INSR_W2 W2.20.', 'source_mrn_norm': 'Digits-only normalisation of SOURCE_PATIENT_NUMBER (the BloodTrack MRN) used for MRN linkage and TRANSFUSION_KEY. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze SOURCE_MRN_NORM, INSR W1). INSR_W2 W2.20.'})
EXTRA_COLUMN_COMMENTS.setdefault('clinical_transfusion_event', {}).update({'product_mapping_method': 'PRODUCT map provenance (method). (bronze PRODUCT_MAPPING_METHOD, INSR W1). INSR_W2 W2.20.', 'product_mapping_rule_id': 'PRODUCT map provenance (rule_id). (bronze PRODUCT_MAPPING_RULE_ID, INSR W1). INSR_W2 W2.20.', 'unit_group_mapping_method': 'UNIT_GROUP map provenance (method). (bronze UNIT_GROUP_MAPPING_METHOD, INSR W1). INSR_W2 W2.20.', 'unit_group_mapping_rule_id': 'UNIT_GROUP map provenance (rule_id). (bronze UNIT_GROUP_MAPPING_RULE_ID, INSR W1). INSR_W2 W2.20.', 'patient_group_mapping_method': 'PATIENT_GROUP map provenance (method). (bronze PATIENT_GROUP_MAPPING_METHOD, INSR W1). INSR_W2 W2.20.', 'patient_group_mapping_rule_id': 'PATIENT_GROUP map provenance (rule_id). (bronze PATIENT_GROUP_MAPPING_RULE_ID, INSR W1). INSR_W2 W2.20.', 'product_mapping_source': 'Map table the product concept came from. (bronze PRODUCT_MAPPING_SOURCE, INSR W1). INSR_W2 W2.20.', 'first_name': 'BloodTrack FIRST_NAME as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze FIRST_NAME, INSR W1). INSR_W2 W2.20.', 'health_number': 'BloodTrack HEALTH_NUMBER as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze HEALTH_NUMBER, INSR W1). INSR_W2 W2.20.', 'last_name': 'BloodTrack LAST_NAME as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze LAST_NAME, INSR W1). INSR_W2 W2.20.', 'patient_birth_date': 'BloodTrack PATIENT_BIRTH_DATE as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze PATIENT_BIRTH_DATE, INSR W1). INSR_W2 W2.20.', 'patient_gender': 'BloodTrack PATIENT_GENDER as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze PATIENT_GENDER, INSR W1). INSR_W2 W2.20.', 'patient_id': 'BloodTrack PATIENT_ID as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze PATIENT_ID, INSR W1). INSR_W2 W2.20.', 'patient_name': 'BloodTrack PATIENT_NAME as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze PATIENT_NAME, INSR W1). INSR_W2 W2.20.', 'patient_number': 'BloodTrack PATIENT_NUMBER as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze PATIENT_NUMBER, INSR W1). INSR_W2 W2.20.', 'porter_user_name': 'BloodTrack PORTER_USER_NAME as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze PORTER_USER_NAME, INSR W1). INSR_W2 W2.20.', 'secondary_first_name': 'BloodTrack SECONDARY_FIRST_NAME as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze SECONDARY_FIRST_NAME, INSR W1). INSR_W2 W2.20.', 'secondary_last_name': 'BloodTrack SECONDARY_LAST_NAME as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze SECONDARY_LAST_NAME, INSR W1). INSR_W2 W2.20.', 'secondary_user_name': 'BloodTrack SECONDARY_USER_NAME as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze SECONDARY_USER_NAME, INSR W1). INSR_W2 W2.20.', 'user_name': 'BloodTrack USER_NAME as sent by the source; identifier, published and tagged (INSR_W1_BLOODTRACK_V1). (bronze USER_NAME, INSR W1). INSR_W2 W2.20.', 'source_mrn_norm': 'Digits-only normalisation of SOURCE_PATIENT_NUMBER (the BloodTrack MRN) used for MRN linkage and TRANSFUSION_KEY. Identifier, published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze SOURCE_MRN_NORM, INSR W1). INSR_W2 W2.20.'})
EXTRA_COLUMN_COMMENTS.setdefault('clinical_waiting_list_snapshot', {}).update({'plausibly_open_ind': 'Census row plausibly still open at the cut-off (W1.09). (bronze PLAUSIBLY_OPEN_IND, INSR W1). INSR_W2 W2.20.'})
EXTRA_COLUMN_COMMENTS.setdefault('reference_encounter_bounds', {}).update({'bounds_birth_floor_applied_ind': 'True when a bound before birth or at the epoch was nulled (INSR_W1_BOUNDS_COHERENT_V1). (bronze BOUNDS_BIRTH_FLOOR_APPLIED_IND, INSR W1). INSR_W2 W2.20.'})
add_public_column('procedure', 'clinical_procedure', 'quantity_parse_rule', 'Rule set behind QUANTITY_NUMERIC/QUANTITY_PARSE_STATUS (IMPLANT_QTY_V2). (bronze QUANTITY_PARSE_RULE, INSR W1). INSR_W2 W2.20.')
add_public_column('procedure', 'clinical_procedure', 'quantity_parse_status', 'Status indicator showing whether the recorded implant quantity text was successfully converted to a numeric value. (bronze QUANTITY_PARSE_STATUS, INSR W1). INSR_W2 W2.20.')
add_public_column('procedure', 'clinical_procedure', 'procedure_date_basis', 'Basis of PROCEDURE_DT_TM_EFFECTIVE: proc_dt_tm, proc_start_dt_tm, proc_end_dt_tm or encounter_arrival. (bronze PROCEDURE_DATE_BASIS, INSR W1). INSR_W2 W2.20.')
add_public_column('device', 'clinical_device', 'device_mapping_rule_id', 'Rule id of the device-type mapping row (INSR_W1_MEDICONNECT_V1). (bronze DEVICE_MAPPING_RULE_ID, INSR W1). INSR_W2 W2.20.')
add_public_column('device', 'clinical_device', 'device_mapping_llm_decision', 'Verifier verdict of the device-type mapping row. (bronze DEVICE_MAPPING_LLM_DECISION, INSR W1). INSR_W2 W2.20.')
add_public_column('condition_stage', 'clinical_condition_stage', 'stage_source_kind', 'URO_TNM_EVENT or PATH_REPORT_PTNM. NULL on ARIA rows (bronze map_cancer_stage_mill.SOURCE_KIND, INSR W1.14). INSR_W2 W2.20.')
add_public_column('condition_stage', 'clinical_condition_stage', 'stage_source_id', 'Clinical EVENT_ID, or pathology report_version_id. NULL on ARIA rows (bronze map_cancer_stage_mill.SOURCE_ID, INSR W1.14). INSR_W2 W2.20.')
add_public_column('condition_stage', 'clinical_condition_stage', 'stage_prefix', 'p, c, yp or yc when stated; NULL otherwise. NULL on ARIA rows (bronze map_cancer_stage_mill.STAGE_PREFIX, INSR W1.14). INSR_W2 W2.20.')
add_public_column('condition_stage', 'clinical_condition_stage', 't_stage', 'T category as recorded (e.g. T2a, Tis, Tx). NULL on ARIA rows (bronze map_cancer_stage_mill.T_STAGE, INSR W1.14). INSR_W2 W2.20.')
add_public_column('condition_stage', 'clinical_condition_stage', 'n_stage', 'N category as recorded. NULL on ARIA rows (bronze map_cancer_stage_mill.N_STAGE, INSR W1.14). INSR_W2 W2.20.')
add_public_column('condition_stage', 'clinical_condition_stage', 'm_stage', 'M category as recorded. NULL on ARIA rows (bronze map_cancer_stage_mill.M_STAGE, INSR W1.14). INSR_W2 W2.20.')
add_public_column('condition_stage', 'clinical_condition_stage', 'stage_text', 'The matched statement or the event result text. NULL on ARIA rows (bronze map_cancer_stage_mill.STAGE_TEXT, INSR W1.14). INSR_W2 W2.20.')
add_public_column('condition_stage', 'clinical_condition_stage', 'stage_rule_id', 'URO_TNM_EVENTCD_V1 or PTNM_REGEX_V1. NULL on ARIA rows (bronze map_cancer_stage_mill.RULE_ID, INSR W1.14). INSR_W2 W2.20.')
add_public_column('condition_stage', 'clinical_condition_stage', 'source_row_hash', 'Payload hash. NULL on ARIA rows (bronze map_cancer_stage_mill.ROW_HASH, INSR W1.14). INSR_W2 W2.20.')

# BBR_FOLDIN_V1 (BBR-4): the vital canonical columns _bbr_vital_canonical adds.
add_public_column('vital_sign', 'clinical_vital_sign', 'value_canonical', 'value_number restated in canonical_ucum_code (see canonical_conversion): heights in cm, weights in kg; equal to value_number otherwise. BBR_FOLDIN_V1 BBR-4.')
add_public_column('vital_sign', 'clinical_vital_sign', 'canonical_ucum_code', "UCUM unit of value_canonical: the anthropometric concept's unit (kg, cm, kg/m2), else the unit axis's UCUM target. BBR_FOLDIN_V1 BBR-4.")
add_public_column('vital_sign', 'clinical_vital_sign', 'canonical_conversion', "identity | m->cm (height <= 3 with no unit or metres, x100) | g->kg (weight in g, /1000) | concept_default_unit (no source unit; the concept's unit assumed, unit_map_rule_id vital:concept_default_unit) | NULL (no numeric value, or no unit). BBR_FOLDIN_V1 BBR-4.")
