"""INSR R4 rules stage of the gold release report (design R4 "QC evidence", decision 15; Deviation D3): pure helpers.

Every gold rule id gets a count per release in <out_schema>.release_rule_count, keyed by (release_id, table_fqn,
rule_id). A table is counted again only when its count SQL changed (GoldSpec or renderer change) or its silver source
was altered since the last count, or on a GLOBAL release; otherwise the previous counts are carried with the release
id that produced them (counted_release_id), as the design rules.
"""
PROPS = ("TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.enableRowTracking = true, "
         "delta.enableDeletionVectors = true)")


def ddl(schema):
    """CREATE TABLE IF NOT EXISTS for release_rule_count (Delta, commented)."""
    return [f"""CREATE TABLE IF NOT EXISTS {schema}.release_rule_count (
  release_id STRING NOT NULL COMMENT 'Release the counts belong to.',
  table_fqn STRING NOT NULL COMMENT 'Gold table (view or single-copy MV), three-part backticked name.',
  tier STRING COMMENT 'view or mv (INSR R4 tiers).',
  rule_id STRING NOT NULL COMMENT 'Rule id as declared in the gold table module (6_mgmt.data_quality.dq_rule where catalogued).',
  action STRING COMMENT 'drop (row leaves gold), warn (row kept) or fail.',
  rows_in BIGINT COMMENT 'Rows before any drop rule (the silver rows the gold table starts from).',
  rows_kept BIGINT COMMENT 'Rows passing every drop rule: the row count of the gold table.',
  failed BIGINT COMMENT 'Rows for which the rule is not TRUE (NULL counts as failed, as Lakeflow counted).',
  counted_release_id STRING COMMENT 'Release whose run produced these numbers (differs from release_id when carried).',
  signature STRING COMMENT 'sha256 of the count SQL that produced them.',
  source_altered TIMESTAMP COMMENT 'information_schema last_altered of the silver source when counted.',
  counted_at TIMESTAMP COMMENT 'Start of the report run that counted them.')
USING DELTA CLUSTER BY (release_id, table_fqn)
COMMENT 'INSR R4 gold release report: per-rule drop/warn counts per release; replaces Lakeflow expectation metrics.'
{PROPS}"""]


def latest_counts(history):
    """The rows of the most recent count of one table, one row per rule id.

    history holds every earlier release's rows for the table. A count carried through several releases appears once
    per carrying release, all with the same counted_at, so the latest counted_at alone would carry it N times; keep
    the first row per rule id (the copies are identical apart from release_id).
    """
    latest = max((r["counted_at"] for r in history), default=None)
    by_rule = {}
    for r in history:
        if r["counted_at"] == latest:
            by_rule.setdefault(r["rule_id"], r)
    return [by_rule[rule_id] for rule_id in sorted(by_rule)]


def needs_count(entry, altered, previous, is_global):
    """True when this table must be counted in this run (see module docstring)."""
    if is_global or not previous:
        return True
    last = previous[0]
    return last["signature"] != entry["signature"] or (altered is not None and last["source_altered"] is not None
                                                       and altered > last["source_altered"])


def count_rows(release_id, table, entry, result, altered, run_ts):
    """Rows of one counted table from the one-row result of its count SQL."""
    return [(release_id, entry["fqn"], entry["tier"], rule_id, action, int(result["rows_in"]), int(result["rows_kept"]),
             int(result[column]), release_id, entry["signature"], altered, run_ts)
            for rule_id, action, column in entry["rules"]]


def carried_rows(release_id, previous):
    """The previous rows of one table under this release id (counted_release_id keeps the producer)."""
    return [(release_id, r["table_fqn"], r["tier"], r["rule_id"], r["action"], r["rows_in"], r["rows_kept"],
             r["failed"], r["counted_release_id"], r["signature"], r["source_altered"], r["counted_at"])
            for r in previous]


def coverage_problems(manifest, rows):
    """Rule ids of the manifest with no row in this release (P9b asserts none)."""
    have = {(r[1], r[3]) for r in rows}
    return [f"{e['fqn']}.{rule_id}" for e in manifest.values() for rule_id, _, _ in e["rules"]
            if (e["fqn"], rule_id) not in have]
