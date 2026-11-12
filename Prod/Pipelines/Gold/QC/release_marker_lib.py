"""INSR R4 release marker (design R4 "Release boundary", decision 16): pure SQL builders and the gate decision.

Gold is views over silver, so a consumer (browser publish, TRE delivery, RBI, PMS, OMOP) reads gold only when no
release is open. Silver_Domains (R2) opens a release before S1 and closes it after S8, the gold MVs, the view deploy,
the rule counts and OMOP have all succeeded; INSR_Apply opens INSR_20261029 at P0 and closes it at P9b. A failed
release stays open until it is fixed or marked rolled back. The gate is a cost alert, not a human gate: jobs skip,
notebooks stop with RELEASE_OPEN, and both log the decision.
"""
import datetime
import re

STATUSES = ("open", "closed", "rolled_back")
ALLOWED_CONTEXTS = ("silver_domains",)   # Silver_Domains' own OMOP task reads mid-release by design
_SAFE = re.compile(r"[A-Za-z0-9_\-]+")
# Event times are passed in, never read by the SQL itself (review fix 2026-11-02, Ben's ruling: a parameter, not a
# CURDATE waiver). The release_marker notebook passes its driver's UTC clock; a step in a generated plan passes
# NOW_TOKEN, which the step runner (INSR_Apply's engine, R0's dev runner) replaces with the time the statement runs.
NOW_TOKEN = "{NOW_UTC}"


def timestamp_sql(at):
    """SQL for one event time: a timezone-aware datetime -> TIMESTAMP 'YYYY-MM-DD HH:MM:SS.ffffff' (UTC);
    NOW_TOKEN passes through unchanged for the step runner to fill."""
    if at == NOW_TOKEN:
        return at
    assert getattr(at, "tzinfo", None) is not None, f"event time must be a timezone-aware datetime, got {at!r}"
    return "TIMESTAMP '" + at.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f") + "'"


def _id(value):
    """A release or job id, checked before it enters SQL."""
    assert _SAFE.fullmatch(str(value)), f"unsafe id {value!r}"
    return str(value)


def ddl(schema):
    """CREATE TABLE IF NOT EXISTS for release_state and release_gate_log (Delta, commented)."""
    return [
        f"""CREATE TABLE IF NOT EXISTS {schema}.release_state (
  release_id STRING NOT NULL COMMENT 'Release id (INSR_20261029, or SD_<job run id> for a Silver_Domains run).',
  status STRING NOT NULL COMMENT 'open, closed or rolled_back. Consumers read gold only when no release is open.',
  opened_at TIMESTAMP COMMENT 'When the release opened (before any silver write).',
  closed_at TIMESTAMP COMMENT 'When it closed (after gold, the view deploy, the rule counts and OMOP succeeded).',
  opened_by STRING COMMENT 'Job run or INSR step that opened it.',
  note STRING COMMENT 'Free text, e.g. why a release was rolled back.')
USING DELTA COMMENT 'INSR R4 release marker: gold views are live over silver, so consumers wait for a closed release.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.enableRowTracking = true, delta.enableDeletionVectors = true)""",
        f"""CREATE TABLE IF NOT EXISTS {schema}.release_gate_log (
  checked_at TIMESTAMP COMMENT 'When a consumer checked the marker.',
  consumer STRING COMMENT 'Job id or notebook that checked.',
  run_id STRING COMMENT 'Run that checked.',
  release_open BOOLEAN COMMENT 'True when the consumer skipped or stopped because a release was open.',
  open_releases STRING COMMENT 'Comma list of the open release ids at the time.',
  context STRING COMMENT 'release_context the consumer passed (silver_domains lets OMOP run inside its own release).')
USING DELTA COMMENT 'INSR R4: every release-marker check by a gold consumer (the skip alert trail).'""",
    ]


def open_sql(schema, release_id, opened_by, at):
    """Open a release at event time `at` (see timestamp_sql); idempotent: an existing row is left as it is."""
    rid, by = _id(release_id), _id(opened_by)
    return (f"MERGE INTO {schema}.release_state t USING (SELECT '{rid}' AS release_id) s ON t.release_id = s.release_id "
            f"WHEN NOT MATCHED THEN INSERT (release_id, status, opened_at, opened_by) "
            f"VALUES (s.release_id, 'open', {timestamp_sql(at)}, '{by}')")


def close_sql(schema, release_id, at, outcome="closed", note=""):
    """Close (or mark rolled back) an open release at event time `at`; a closed one is left alone."""
    assert outcome in ("closed", "rolled_back"), outcome
    note = note.replace("\\", "\\\\").replace("'", "\\'")
    return (f"UPDATE {schema}.release_state SET status = '{outcome}', closed_at = {timestamp_sql(at)}, note = '{note}' "
            f"WHERE release_id = '{_id(release_id)}' AND status = 'open'")


def open_releases_sql(schema):
    """The ids of every open release."""
    return f"SELECT release_id FROM {schema}.release_state WHERE status = 'open' ORDER BY opened_at"


def decide(open_ids, context):
    """(release_open, reason) for one consumer: open when any release is open, unless its context is allowed."""
    if not open_ids:
        return False, "no release open"
    if context in ALLOWED_CONTEXTS:
        return False, f"context {context} reads inside its own release"
    return True, "open: " + ",".join(open_ids)


def log_sql(schema, consumer, run_id, release_open, open_ids, context, at):
    """One gate-log row, checked at event time `at` (see timestamp_sql)."""
    ids = ",".join(_id(i) for i in open_ids)
    return (f"INSERT INTO {schema}.release_gate_log VALUES ({timestamp_sql(at)}, '{_id(consumer)}', '{_id(run_id or 'none')}', "
            f"{'true' if release_open else 'false'}, '{ids}', '{_id(context or 'none')}')")
