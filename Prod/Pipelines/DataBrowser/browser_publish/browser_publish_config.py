# Databricks notebook source
# DataBrowserGold — shared configuration and ALL pure logic.
#
# Every other notebook in this folder `%run`s this one. Nothing here touches
# spark at import time, which is what makes it both a %run-safe library and a
# plain importable module for agent/tests/test_browser_publish_config.py.
#
# The policy dict is NOT read from here: callers pass it in, so the tests can
# supply a fixture. In the notebooks it is derived per run by derive_policy()
# from the source's Unity Catalog IG tags, column types and a sample of the
# data (see "Column treatment" below); there is no hand-maintained file.
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

MASK_TRANSFORMS = ("hash", "month", "year")
# publish: in the view (queryable, profiled). describe: listed in the
# catalogue with its type, comment and IG scores, but no contents (not in the
# view, not profiled, not in Cube). mask is still honoured by the view DDL,
# though no derivation rule produces it.
ACTIONS = ("publish", "mask", "describe")
REQUIRED_SCHEMA_PRIVILEGES = ("SELECT", "USE_SCHEMA")

# Pipeline internals — never published, never policy-classified.
INTERNAL_COLUMNS = ("load_batch_id", "fact_row_id", "source_row_id",
                    "_source_row_id")


def missing_schema_privileges(schemas, granted):
    """Return required UC information-schema privilege tokens not granted."""
    return {(schema, privilege) for schema in schemas
            for privilege in REQUIRED_SCHEMA_PRIVILEGES
            if (schema, privilege) not in granted}


def expected_ig_tags(objects, columns, policy):
    """Return the exact information-schema IG tag rows for published columns."""
    expected = set()
    for object_name in objects:
        for column in columns[object_name]:
            if column in INTERNAL_COLUMNS:
                continue
            entry = policy[policy_key(object_name, column)]
            if entry["action"] == "describe":
                continue
            expected.add((object_name, column, "ig_risk", entry["ig_risk"]))
            expected.add((object_name, column, "ig_severity",
                          entry["ig_severity"]))
    return expected


def receipt_has_phase(receipt_payloads, phase_name, **expected):
    """Whether any prior receipt records a completed matching phase."""
    for payload in receipt_payloads:
        for phase in payload.get("phases", []):
            if phase.get("phase") != phase_name:
                continue
            if all(phase.get(key) == value
                   for key, value in expected.items()):
                return True
    return False


def completely_tagged_objects(expected, actual):
    """Objects whose expected IG tag rows are all present exactly."""
    names = {row[0] for row in expected}
    return {name for name in names
            if {row for row in expected if row[0] == name}
            <= {row for row in actual if row[0] == name}}


def policy_key(object_name: str, column: str) -> str:
    return f"{object_name}.{column}"


def validate_policy(policy: dict, objects: dict) -> list[str]:
    """Problems with `policy` against {object_name: [column, ...]}.

    Empty list = the run may proceed. Any entry means stop: an unclassified
    column reaching the browser is the failure this whole file exists to
    prevent, so this is fail-closed by construction.
    """
    problems: list[str] = []
    for object_name in sorted(objects):
        for column in objects[object_name]:
            if column in INTERNAL_COLUMNS:
                continue
            key = policy_key(object_name, column)
            entry = policy.get(key)
            if entry is None:
                problems.append(f"{key}: no policy entry")
                continue
            action = entry.get("action")
            if action not in ACTIONS:
                problems.append(f"{key}: action must be one of "
                                f"{'|'.join(ACTIONS)}")
                continue
            if action == "mask":
                mask = entry.get("mask")
                if not mask:
                    problems.append(f"{key}: mask needs a transform "
                                    f"({'|'.join(MASK_TRANSFORMS)})")
                    continue
                if mask not in MASK_TRANSFORMS:
                    problems.append(f"{key}: unknown mask {mask!r} "
                                    f"({'|'.join(MASK_TRANSFORMS)})")
                    continue
            if entry.get("rollup_dimension") and action != "publish":
                problems.append(f"{key}: rollup_dimension requires action "
                                "publish")
    return problems


def qi(identifier: str) -> str:
    """Backtick-quote a UC identifier, refusing unsafe values."""
    assert identifier and "`" not in identifier and "\n" not in identifier, \
        f"unsafe identifier {identifier!r}"
    return f"`{identifier}`"


def _masked_sql(column: str, mask: str, salt: str) -> str:
    col = qi(column)
    if mask == "hash":
        assert "'" not in salt, f"unsafe salt {salt!r}"
        return f"sha2(concat('{salt}', cast({col} AS STRING)), 256)"
    return f"date_trunc('{mask}', {col})"


def build_view_ddl(*, target: str, source: str, object_name: str,
                   columns: list, policy: dict, salt: str,
                   where_sql: str | None = None,
                   column_types: dict | None = None,
                   inherit: dict | None = None,
                   bigint: list | None = None) -> str:
    """Build a view DDL with an explicit, policy-filtered column list.

    `bigint` (a source plan's) names STRING key columns published as BIGINT:
    every value was proved to cast exactly (STRING_PERSON_KEYS).
    `inherit` (a source plan's) adds the parent's columns the child lacks
    (its event time, its person) through a LEFT JOIN to one row per parent
    key, so the join never adds or drops rows. A query that reads none of
    them doesn't pay for the join: the optimiser drops a LEFT JOIN to a
    subquery grouped on the join key (EXPLAIN, 2026-10-24).
    """
    problems = validate_policy(policy, {object_name: columns})
    assert not problems, "; ".join(problems)
    select = []
    for column in columns:
        if column in INTERNAL_COLUMNS:
            continue
        entry = policy[policy_key(object_name, column)]
        action = entry["action"]
        if action == "describe":
            continue
        if action == "publish":
            source_type = (column_types or {}).get(column, "").upper()
            if source_type == "VARIANT":
                expr = f"cast({qi(column)} AS STRING)"
            elif column in (bigint or ()):
                expr = f"try_cast({qi(column)} AS BIGINT)"
            else:
                expr = qi(column)
        else:
            expr = _masked_sql(column, entry["mask"], salt)
        select.append(f"  {expr} AS {qi(column)}")
    assert select, f"{object_name}: no published columns"
    if where_sql is not None:
        assert where_sql in ALLOWED_RESEARCH_FILTERS, \
            f"unapproved research filter {where_sql!r}"
    suffix = f"\nWHERE {where_sql}" if where_sql else ""
    from_sql = source
    if inherit is not None:
        parent_where = inherit.get("parent_where")
        assert parent_where is None or parent_where in ALLOWED_RESEARCH_FILTERS, \
            f"unapproved research filter {parent_where!r}"
        parent = source.rsplit(".", 1)[0] + "." + qi(inherit["parent"])
        carried = ", ".join(
            f"{_INHERITED_AGGREGATE[column].format(c=qi(column))} AS {qi(column)}"
            for column in inherited_columns(inherit))
        from_sql = (
            f"{source} c\nLEFT JOIN (\n"
            f"  SELECT {qi(inherit['parent_key'])} AS `_parent_key`, "
            f"{carried}\n"
            f"  FROM {parent}"
            + (f" WHERE {parent_where}" if parent_where else "")
            + f"\n  GROUP BY {qi(inherit['parent_key'])}\n) p "
            f"ON c.{qi(inherit['key'])} = p.`_parent_key`")
    return (f"CREATE OR REPLACE VIEW {target} AS\nSELECT\n"
            + ",\n".join(select) + f"\nFROM {from_sql}" + suffix)


# How the parent join reduces each inherited column to one value per parent
# key: the earliest event time; the person only when every parent row agrees
# (1% samples, 2026-10-24: they always do), else NULL rather than a guess.
_INHERITED_AGGREGATE = {
    "event_datetime": "min({c})",
    "person_id": "CASE WHEN min({c}) = max({c}) THEN min({c}) END",
}


def inherited_columns(inherit: dict) -> list:
    """The parent columns an inherit plan carries (plans written before the
    person was inherited carry only the event time)."""
    return list(inherit.get("columns", [INHERITED_TIME_COLUMN]))


def inherited_comment_sql(target: str, inherit: dict,
                          column: str | None = None) -> str:
    """Column comment for one inherited column (CREATE OR REPLACE drops it)."""
    column = column or INHERITED_TIME_COLUMN
    if column == INHERITED_TIME_COLUMN:
        comment = (f"Date/time of the parent {inherit['parent']} row "
                   f"({INHERITED_TIME_COLUMN}), joined on {inherit['key']} at "
                   "publish; this table has no time column of its own.")
    else:
        assert column == INHERITED_PERSON_COLUMN, f"not inherited: {column!r}"
        comment = (f"Person of the parent {inherit['parent']} row "
                   f"({INHERITED_PERSON_COLUMN}), joined on {inherit['key']} "
                   "at publish; this table has no person_id of its own. "
                   "NULL where the child row has no parent row.")
    assert "'" not in comment, f"unsafe comment {comment!r}"
    return (f"COMMENT ON COLUMN {target}.{qi(column)} IS '{comment}'")


def build_column_tag_sql(*, fqn: str, object_name: str, columns: list,
                         policy: dict, tag_comment: str) -> str:
    """Build one SQL compound statement containing all column tag writes."""
    assert tag_comment.startswith("/* ") and tag_comment.endswith(" */"), \
        "tag_comment must be a SQL block comment"
    statements = []
    for column in columns:
        if column in INTERNAL_COLUMNS:
            continue
        entry = policy[policy_key(object_name, column)]
        if entry["action"] == "describe":
            continue
        for tag_name in ("ig_risk", "ig_severity"):
            tag_value = entry[tag_name]
            assert tag_value in ("low", "medium", "high"), \
                f"unsafe tag value {tag_value!r}"
            statements.append(
                f"  {tag_comment}\n"
                f"  SET TAG ON COLUMN {fqn}.{qi(column)} "
                f"{qi(tag_name)} = {qi(tag_value)};")
    assert statements, f"{object_name}: no columns to tag"
    return "BEGIN\n" + "\n".join(statements) + "\nEND"


def combine_column_tag_sql(statements: list[str]) -> str:
    """Combine per-object compound tag statements into one SQL round trip."""
    assert statements, "column tag batch must not be empty"
    bodies = []
    for statement in statements:
        assert statement.startswith("BEGIN\n") and statement.endswith("\nEND"), \
            "column tag statement must be a BEGIN/END compound statement"
        bodies.append(statement[len("BEGIN\n"):-len("\nEND")])
    return "BEGIN\n" + "\n".join(bodies) + "\nEND"


# ---- IG scores from Unity Catalog ------------------------------------------
# 4_prod.gold carries numeric column tags: ig_risk 0-4 and ig_severity 0-3
# (estate convention: direct identifiers 4/2, free text 3/2, staff/role 2/1,
# codes 1/0). They are the ONLY source of IG values, and (with the column
# type and a data sample) decide each column's treatment; see "Column
# treatment" below. The low/medium/high words are derived only for consumers
# that still key on them (view tags, Cube descriptions).
IG_SCORE_RANGES = {"ig_risk": range(0, 5), "ig_severity": range(0, 4)}
IG_RISK_LABELS = {0: "none", 1: "low", 2: "moderate",
                  3: "high (may contain identifiers)", 4: "direct identifier"}


def ig_word(score: int) -> str:
    """Legacy word for a score: 0-1 low, 2 medium, 3+ high."""
    return "low" if score <= 1 else "medium" if score == 2 else "high"


def source_ig_sql(catalog: str, schema: str, objects) -> str:
    """Read the IG tags of the named source objects."""
    names = ", ".join(f"'{name}'" for name in sorted(objects))
    return ("SELECT table_name, column_name, tag_name, tag_value "
            f"FROM {catalog}.information_schema.column_tags "
            f"WHERE schema_name = '{schema}' AND table_name IN ({names}) "
            "AND tag_name IN ('ig_risk', 'ig_severity')")


def _source_tags(source_rows) -> dict:
    """{(table, column): {tag_name: tag_value}} from tag rows."""
    tags: dict = {}
    for row in source_rows:
        tags.setdefault((row["table_name"], row["column_name"]),
                        {})[row["tag_name"]] = row["tag_value"]
    return tags


def _tag_source(found: dict, object_name: str, column: str,
                profile: "DatasetProfile | None" = None) -> str:
    """Whose IG tags a published column carries: its own, or, for a column an
    inheriting child takes from its parent (event_datetime, person_id; it has
    no such column, so no tags, in the source), the parent's. A child that
    gains its own column has its own tags and stops inheriting, as
    _inherit_plan does."""
    p = profile or GOLD
    for inherited, links in ((INHERITED_TIME_COLUMN, p.inherited_time),
                             (INHERITED_PERSON_COLUMN, p.inherited_person)):
        if (column == inherited and object_name in links
                and (object_name, column) not in found):
            return links[object_name][1]
    return object_name


def _score(value, allowed):
    """An integer score in range, or None."""
    text = "" if value is None else str(value).strip()
    return int(text) if text.isdigit() and int(text) in allowed else None


# ---- Column treatment ---------------------------------------------------
# Every gold column is published unless one of these holds, in which case it
# is described only (listed, no contents). The browser needs to know such
# columns exist; the data crawler works from coded values and never needs
# their contents.
#   1. not a single value: ARRAY / MAP / STRUCT
#   2. a direct identifier: ig_risk 4, unless it is a date (age and mortality
#      need birth and death dates; access is enforced per dataset)
#   3. a string that may hold identifiers: ig_risk 3 or more
#   4. free text: a string whose sampled values read as prose. Calibrated
#      2026-10-15 on every gold string column against the reviewed drop list;
#      the tags alone missed 93 free-text columns, and the hand list had
#      published several (cancer treatment indications, mortality-review
#      actions, LFPSE incident descriptions). Prose is either short phrases
#      that rarely repeat, or longer text that is not a repeated label: on
#      live gold the 4-word band adds exactly five columns, all free text.
#   0. no values: any sampled column with no non-null value in the table
#      (2026-10-16 audit: 877 such gold columns, most silver placeholders).
#   5. too little evidence: a string with fewer than MIN_TEXT_EVIDENCE values
#      in the whole table cannot be shown to be coded, and a group-by would
#      reveal each value even with its count suppressed. Person-level tables
#      only: any table with a person-linking column (_PERSON_LINK) or an
#      inherited person; a table with none holds no patient's row, so its
#      rare label reveals no one (RBI backtest labels). Never the
#      profile's min_text_evidence_exempt tables (OMOP cdm_source /
#      metadata, L692). INSR_W7 W7.03.
#   6. a raw area code (DatasetProfile.area_code_columns): described; the
#      browser shows derived area data instead (J15, 2026-10-29).
# Names ending _code / _display / _desc, or name / *_name, are code labels:
# rules 4 and 5 never apply to them. The sample is up to TEXT_SAMPLE_VALUES
# non-null values per column, so sparse columns in huge tables are judged on
# their own values (a first-rows sample read 0 values of populated columns).
NON_SCALAR_TYPES = ("ARRAY", "MAP", "STRUCT")
DATE_TYPES = ("DATE", "TIMESTAMP")
TEXT_SAMPLE_VALUES = 500
MIN_TEXT_EVIDENCE = 20
SHORT_PROSE = (2.5, 0.3)   # (min average words, max share of repeats)
LONG_PROSE = (4.0, 0.6)    # longer text may repeat more (templated phrasing)
CODE_LABEL = re.compile(r"(^code$|_code$|_display$|_desc$|^name$|_name$)")

# Cube pre-aggregation dimensions (Tier-C queries may group only by these):
# the 13 reviewed flags, unchanged, from the hand-written column policy file
# this derivation replaced (2026-10-15). At most
# MAX_ROLLUP_DIMENSIONS per object, each distinct_count <= MAX_ROLLUP_CARDINALITY.
ROLLUP_DIMENSIONS = {
    "clinical_clinical_finding": ("finding_kind", "normalcy_display",
                                  "organization_id", "result_status_display"),
    "clinical_indication": ("assertion",),
    "clinical_pathology_result": ("finding_axis", "interpretation_code",
                                  "result_status", "value_concept_display"),
}


def is_prose(stats) -> bool:
    """Whether a sampled string column reads as free text: short phrases that
    rarely repeat, or long text that is not a repeated label."""
    if not stats or not stats.get("n") or stats.get("words") is None:
        return False
    repeat = 1 - stats["distinct"] / stats["n"]
    return any(stats["words"] >= words and repeat < max_repeat
               for words, max_repeat in (SHORT_PROSE, LONG_PROSE))


def text_stats_sql(source_fqn: str, columns, numeric=()) -> str:
    """One statement per source object: for each column, up to
    TEXT_SAMPLE_VALUES non-null values, with their count and distinct count,
    plus the average word count for string `columns` (NULL for `numeric`,
    i.e. every other scalar column, sampled only to see whether it holds any
    value). LIMIT stops early on populated columns; Delta's null counts let
    it skip all-null files."""
    parts = []
    for column, words in ([(c, f"avg(size(split(trim(v), '\\\\s+')))") for c in columns]
                          + [(c, "CAST(NULL AS DOUBLE)") for c in numeric]):
        assert "'" not in column, f"unsafe column {column!r}"
        parts.append(
            f"SELECT '{column}' AS column_name, count(v) AS n, "
            f"{words} AS words, "
            f"count(DISTINCT v) AS distinct_values "
            f"FROM (SELECT {qi(column)} AS v FROM {source_fqn} "
            f"WHERE {qi(column)} IS NOT NULL LIMIT {TEXT_SAMPLE_VALUES})")
    return "\nUNION ALL\n".join(parts)


def parse_text_stats(object_name: str, rows) -> dict:
    """{(object, column): {n, words, distinct}} from text_stats_sql rows."""
    out = {}
    for row in rows:
        words = row["words"]
        out[(object_name, row["column_name"])] = {
            "n": float(row["n"]),
            "words": None if words is None else float(words),
            "distinct": float(row["distinct_values"])}
    return out


def _treatment(column, type_name, risk, stats, *, area_code=False,
               person_level=True, evidence_exempt=False):
    """(action, reason) for one column under the rules above (INSR_W7 W7.03:
    rule 6 first; rule 5 only on person-level, non-exempt tables)."""
    type_name = (type_name or "").upper()
    if area_code:
        return "describe", AREA_CODE_REASON
    if type_name.startswith(NON_SCALAR_TYPES):
        return "describe", "not a single value"
    if stats is not None and not stats.get("n"):
        # sampled and found empty: nothing to publish, whatever its type
        return "describe", "no values"
    if risk == 4 and not type_name.startswith(DATE_TYPES):
        return "describe", "direct identifier"
    if type_name.startswith("STRING"):
        if risk >= 3:
            return "describe", "may hold identifiers"
        if not CODE_LABEL.search(column.lower()):
            if is_prose(stats):
                return "describe", "free text"
            if (person_level and not evidence_exempt
                    and (not stats or (stats.get("n") or 0) < MIN_TEXT_EVIDENCE)):
                return "describe", "too few values to classify"
    return "publish", None


# INSR_W7 W7.03 (rule 5 scope, review 2026-10-30): a column name that links a
# row to a patient, directly or through an encounter, event, exposure, visit,
# infant or incident. Patient-row tables without a person_id of their own
# carry one of these (PMS Device pms_outcome_event / pms_baseline /
# pms_followup / pms_procedure_context on exposure_id, OMOP infant on
# infant_id and episode_event on event_id, RBI cost_allocation_ledger on
# resource_use_event_key, gold clinical_form_response on patient_event_key,
# the safety incident children on inc_id, reference_encounter_attribute and
# spine_episode_encounter on encounter_id, spine_person_relationship on
# source_person_id), so they keep the rare-string control. INSR_W7 W7.04
# replay (2026-10-30, source column lists of the 10-25 releases) added three
# more patient-row links: gold reference_pregnancy_reconciliation on
# pregnancy_id (one row per pregnancy, unmatched ones included), OMOP
# genomic_gene_tested on genomic_test_id (one row per gene per test) and OMOP
# omop_concept_provenance on cdm_row_id (one row per mapped fact field).
_PERSON_LINK = re.compile(
    r"(^|_)(person_id|patient_event_key|encounter_id|exposure_id"
    r"|visit_occurrence_id|infant_id|resource_use_event_key|event_id"
    r"|inc_id|incident_id|pregnancy_id|genomic_test_id|cdm_row_id)$")


def _person_level(object_name: str, objects: dict,
                  profile: "DatasetProfile") -> bool:
    """INSR_W7 W7.03 (rule 5 scope): a table is person-level unless it has
    no person-linking column at all (_PERSON_LINK) and takes no person from
    a parent (INHERITED_PERSON). Rule 5 is skipped only for tables that
    cannot hold a patient's row (RBI backtests and summaries, reference
    tables)."""
    if object_name in profile.inherited_person:
        return True
    return any(_PERSON_LINK.search(column)
               for column in objects.get(object_name, ()))


def derive_policy(objects: dict, column_types: dict, source_rows,
                  text_stats: dict, profile: "DatasetProfile | None" = None) -> tuple:
    """The run's column policy, derived (never hand-written).

    objects: {object_name: [column, ...]}; column_types: {object: {column:
    type}}; source_rows: the source's ig_risk / ig_severity tag rows;
    text_stats: {(object, column): {n, words, distinct}} for string columns.
    Returns (policy, problems). Every non-internal column needs both IG
    scores in range, published or not: a missing score is a problem and the
    caller must stop (fail closed).
    """
    p = profile or GOLD
    found = _source_tags(source_rows)
    policy, problems = {}, []
    for object_name in sorted(objects):
        for column in objects[object_name]:
            if column in INTERNAL_COLUMNS:
                continue
            key = policy_key(object_name, column)
            owner = _tag_source(found, object_name, column, p)
            tags = found.get((owner, column), {})
            scores = {}
            for name, allowed in IG_SCORE_RANGES.items():
                score = _score(tags.get(name), allowed)
                if score is None:
                    problems.append(
                        f"{key}: source {name} tag {tags.get(name)!r} missing "
                        f"or outside {allowed.start}-{allowed.stop - 1}")
                else:
                    scores[name] = score
            if len(scores) < len(IG_SCORE_RANGES):
                continue
            action, reason = _treatment(
                column, column_types.get(owner, {}).get(column),
                scores["ig_risk"], text_stats.get((object_name, column)),
                area_code=column in p.area_code_columns.get(object_name, ()),
                person_level=_person_level(object_name, objects, p),
                evidence_exempt=object_name in p.min_text_evidence_exempt)
            entry = {"action": action,
                     "ig_risk_score": scores["ig_risk"],
                     "ig_severity_score": scores["ig_severity"],
                     "ig_risk": ig_word(scores["ig_risk"]),
                     "ig_severity": ig_word(scores["ig_severity"])}
            if reason:
                entry["reason"] = reason
            if column in p.rollup_dimensions.get(object_name, ()):
                if action == "publish":
                    entry["rollup_dimension"] = True
                elif reason == "no values":
                    # an empty column has nothing to roll up: it is described
                    # and not flagged, and the flag returns with its values
                    # (gold clinical_indication emptied 2026-10-27)
                    pass
                else:
                    problems.append(f"{key}: rollup dimension is not "
                                    f"published ({reason})")
            policy[key] = entry
    return policy, problems



_UNTAGGED = re.compile(
    r"^(?P<table>[^.]+)\.(?P<column>[^:]+): source ig_(?:risk|severity) tag ")


def untagged_tables(problems: list) -> dict:
    """{table: [column, ...]} for the derive_policy problems that are a
    missing or out-of-range IG tag. Such a table waits for the estate's
    tagging: it is left out of the release and listed, and the rest of the
    dataset publishes. Any other problem still stops the publish."""
    out: dict = {}
    for problem in problems:
        match = _UNTAGGED.match(problem)
        if match:
            out.setdefault(match["table"], set()).add(match["column"])
    return {table: sorted(columns) for table, columns in sorted(out.items())}


def empty_tables(policy: dict, objects) -> list:
    """The objects whose every column was described for having no values: an
    empty table (gold clinical_indication once its context rows moved out,
    2026-10-27). It has no view to build, so it is left out of the release
    and listed, and returns at the first publish after it has rows again."""
    reasons: dict = {}
    for key, entry in policy.items():
        reasons.setdefault(key.split(".", 1)[0], []).append(entry.get("reason"))
    return sorted(o for o in objects
                  if reasons.get(o) and all(r == "no values" for r in reasons[o]))


def ig_score_mismatches(doc: dict, source_rows,
                        profile: "DatasetProfile | None" = None) -> list:
    """Published columns whose scores differ from (or lack) source tags."""
    found = _source_tags(source_rows)
    out = []
    for table_name, table in sorted(doc["tables"].items()):
        for column in table["columns"]:
            owner = _tag_source(found, table_name, column["name"], profile)
            tags = found.get((owner, column["name"]), {})
            source = tuple(_score(tags.get(name), allowed)
                           for name, allowed in IG_SCORE_RANGES.items())
            published = (column.get("ig_risk_score"),
                         column.get("ig_severity_score"))
            if None in source or source != published:
                out.append(f"{table_name}.{column['name']}: published "
                           f"{published} source {source}")
    return out


# Tier thresholds (spec §7.1). Tier A is raw-queryable; B needs a bounded time
# filter; C is rollup-only. Row counts come from the stats pass.
TIER_B_MIN = 10_000_000
TIER_C_MIN = 1_000_000_000


def tier_for(rows: int) -> str:
    if rows >= TIER_C_MIN:
        return "C"
    if rows >= TIER_B_MIN:
        return "B"
    return "A"


def _base_twin(object_name: str) -> str:
    """Return the materialized-view name a `_v_` view would shadow."""
    return object_name.replace("_v_", "_", 1)


def select_objects(inventory: list, profile: "DatasetProfile | None" = None
                   ) -> list:
    """Select every public relation of the dataset, skipping superseded `_v_`
    views where the profile says so.

    Since the 2026-09-23 restructure the materialized views ARE the research
    products (identity resolution happens upstream). A `_v_` view whose base
    twin still exists is a pre-restructure leftover and is skipped; a `_v_`
    view without a twin (e.g. clinical_v_imaging_accession) is a product.
    """
    p = profile or GOLD
    names = {n for n, table_type in inventory
             if table_type != "MANAGED"
             and not n.startswith(p.skip_prefixes)
             and n not in p.skip_tables}
    if not p.skip_v_twins:
        return sorted(names)
    return sorted(name for name in names
                  if "_v_" not in name or _base_twin(name) not in names)


# The only row filter the browser adds: superseded/retracted history stays in
# gold but is not what a research count means. A closed allow-list, so no
# other text can become view DDL.
ACTIVE_FILTER = "(record_status = 'active')"

# INSR_W7 W7.03 (J9, 2026-10-29): the closed list of extra default filters a
# profile may name per table (DatasetProfile.default_view_filters). Gold keeps
# every text_document version; the browser shows the latest one.
LATEST_VERSION_FILTER = "(is_latest_version = TRUE)"
DEFAULT_VIEW_FILTERS = (LATEST_VERSION_FILTER,)
# INSR_W7 W7.03 (L5097): appended to the description of every table the
# browser filters, so a reader knows the history exists.
FILTERED_TABLE_NOTE = ("Browser shows the current version only; history "
                       "stays in gold.")
# INSR_W7 W7.03 (J15, 2026-10-29): why a raw area code is listed without
# contents.
AREA_CODE_REASON = ("raw area code: browser shows derived area data "
                    "(J15 ruling 2026-10-29)")


# Child tables with no time column of their own take their parent event's
# event_datetime, so a Tier B/C table can be bounded by date (and a Tier C one
# gets its monthly rollup). child -> (child key, parent, parent key). Measured
# 2026-09-26 on 4_prod.gold: no child row is added or lost, >= 98% of rows get
# a datetime, and a one-year monthly count over medication_order_detail
# (2.86B rows) through the join runs in ~10 s. reference_cost_line_item's own
# unbundled_currency_datetime is 98% NULL; its costed activity's date is the
# date of the care the line costs.
INHERITED_TIME_COLUMN = "event_datetime"
INHERITED_TIME = {
    "clinical_clinical_score_component": (
        "patient_event_key", "clinical_clinical_score", "patient_event_key"),
    "clinical_form_response": (
        "patient_event_key", "clinical_form", "patient_event_key"),
    "clinical_hrg_grouping_diagnosis": (
        "patient_event_key", "clinical_hrg_grouping", "patient_event_key"),
    "clinical_hrg_grouping_procedure": (
        "patient_event_key", "clinical_hrg_grouping", "patient_event_key"),
    "clinical_medication_admin_ingredient": (
        "event_id", "clinical_medication_admin", "event_id"),
    "clinical_medication_order_detail": (
        "order_id", "clinical_medication_order", "order_id"),
    "clinical_medication_order_ingredient": (
        "order_id", "clinical_medication_order", "order_id"),
    "reference_cost_line_item": (
        "costed_activity_key", "clinical_costed_activity",
        "patient_event_key"),
}


# Child -> (child key, parent table, parent key): the table each child
# details, declared rather than guessed (the hub list in HUB_FOR_KEY only
# knows dataset-wide keys, and names alone are ambiguous: event_id is in
# findings, scores and imaging too). Every inherited-time child is one.
# Each link is published only once the publish has checked it: the child's
# key finds its parent (orphan rate) and, where the parent is person-keyed,
# each parent key belongs to one person, because cohort scoping keeps a
# child row when its parent row belongs to a member.
PARENT_LINKS = {
    **INHERITED_TIME,
    # checked on the live release 2026-10-24: no orphans in a 100k sample
    # (relationships 0.06%), and no parent key held by two people
    "clinical_medication_order_attribute": (
        "order_id", "clinical_medication_order", "order_id"),
    "clinical_medication_order_status": (
        "order_id", "clinical_medication_order", "order_id"),
    "clinical_medication_admin_status": (
        "event_id", "clinical_medication_admin", "event_id"),
    "clinical_appointment_booking": (
        "sch_event_id", "clinical_appointment", "sch_event_id"),
    "clinical_appointment_resource": (
        "sch_event_id", "clinical_appointment", "sch_event_id"),
    "clinical_condition_revision": (
        "problem_id", "clinical_condition", "problem_id"),
    "clinical_neonatal_care_day_activity": (
        "entity_id", "clinical_neonatal_care_day", "entity_id"),
    "clinical_neonatal_care_day_high_cost_drug": (
        "entity_id", "clinical_neonatal_care_day", "entity_id"),
    "clinical_rtt_pathway_encounter_type": (
        "patient_event_key", "clinical_rtt_pathway", "patient_event_key"),
    "text_document_sensitivity_label": (
        "patient_event_key", "text_document", "patient_event_key"),
    # a relationship row belongs with its source person
    "spine_person_relationship": (
        "source_person_id", "spine_person", "person_id"),
    # 1% key-hash samples 2026-10-24: every matched child row maps to one
    # person, but many rows have no parent row (cancer treatment cycles 25%,
    # genes tested 23% -- 342,176 genomic_test parents exist in silver only,
    # INSR W0 P9_L2204g 2026-10-29 --, elective access procedures 2.6%,
    # implant attributes 0%), so the first three fail the 2% orphan limit as
    # links. Each publish re-measures these rates (the run receipt's
    # relationships phase lists every rejected link with its rate); the
    # children are still scoped through the person they inherit.
    "clinical_cancer_treatment_cycle": (
        "iqemo_course_id", "clinical_cancer_treatment", "iqemo_course_id"),
    "clinical_procedure_implant_attribute": (
        "patient_event_key", "clinical_procedure", "patient_event_key"),
    "reference_elective_access_procedure": (
        "elective_access_entry_key", "clinical_elective_access_entry",
        "patient_event_key"),
    "reference_gene_tested": (
        "genomic_test_key", "clinical_genomic_test", "patient_event_key"),
}


# Every parent-linked child without a person_id of its own also takes its
# parent's, through the same one-row-per-parent-key join INHERITED_TIME
# uses, so it is person-keyed like any other table: a cohort scopes it
# directly, and its people can be counted. The person is NULL where the child
# row has no parent row, or (never seen, 2026-10-24) where the parent rows
# disagree. A child in INHERITED_TIME takes both columns in one join.
# spine_person_relationship is left out: its parent is the person itself
# (source_person_id, a STRING, against spine_person's BIGINT key), so it is
# scoped through its parent link alone.
INHERITED_PERSON_COLUMN = "person_id"
INHERITED_PERSON = {child: link for child, link in PARENT_LINKS.items()
                    if child != "spine_person_relationship"}


# INSR_W7 W7.04 (L3132, L4817, F21): relationships the naming conventions
# cannot propose, each checked by INSR W0.05 (ledger P5g/P5h, 2026-10-29) and
# re-measured exactly at every publish (accept_declared_edges). `join`: offer
# it to Cube when the parent has one row per key and that key is its row key;
# every other kept edge is documented with its measured cardinality. The LUNA
# pathway edges are many-to-many (up to 82 rtt_pathway rows per pathway_oid),
# so they are documented, never joined. Edges 25-27 (waiting_list_oid and
# referral_oid) were rejected at C0 (coverage 0 / parent key 100% NULL).
_REGISTRY_CHILDREN = ("acs_transfer", "cardiac_mdt", "coronary_lesion",
                      "coronary_procedure", "eracs_episode", "mortality_review",
                      "noncoronary_procedure", "surgery_episode",
                      "surgery_followup", "surgery_procedure")
GOLD_DECLARED_EDGES = (
    {"name": "registry_coronary_lesion_procedure", "ref": "P5g_edge01",
     "from": ("clinical_registry_coronary_lesion", ("parent_entry_id",)),
     "to": ("clinical_registry_coronary_procedure", ("entry_id",)),
     "join": False},
    {"name": "registry_surgery_procedure_episode", "ref": "P5g_edge02",
     "from": ("clinical_registry_surgery_procedure", ("parent_entry_id",)),
     "to": ("clinical_registry_surgery_episode", ("entry_id",)),
     "join": False},
    *({"name": f"registry_{child}_entry", "ref": f"P5g_edge{3 + i:02d}",
       "from": (f"clinical_registry_{child}", ("patient_event_key",)),
       "to": ("clinical_registry_entry", ("patient_event_key",)), "join": True}
      for i, child in enumerate(_REGISTRY_CHILDREN)),
    # INSR_W7 W7.10: the child's "family" is its own registry name, a constant
    # (the child tables have no family column); W0 P5g matched it to
    # registry_entry.source_object. A quoted key is a constant (edge_coverage_sql).
    *({"name": f"registry_{child}_entry_family", "ref": f"P5g_edge{13 + i:02d}",
       "from": (f"clinical_registry_{child}", (f"'{child}'", "entry_id")),
       "to": ("clinical_registry_entry", ("source_object", "entry_id")),
       "join": False}
      for i, child in enumerate(_REGISTRY_CHILDREN)),
    {"name": "luna_referral_rtt_pathway", "ref": "P5h_edge23",
     "from": ("clinical_referral", ("pathway_oid",)),
     "to": ("clinical_rtt_pathway", ("pathway_oid",)), "join": False},
    {"name": "luna_referral_pathway_tracking", "ref": "P5h_edge24",
     "from": ("clinical_referral", ("pathway_oid",)),
     "to": ("clinical_pathway_tracking", ("pathway_oid",)), "join": False},
    {"name": "luna_elective_access_rtt_pathway", "ref": "P5h_edge28",
     "from": ("clinical_elective_access_entry", ("pathway_oid",)),
     "to": ("clinical_rtt_pathway", ("pathway_oid",)), "join": False},
    {"name": "luna_pathway_tracking_rtt_pathway", "ref": "P5h_edge29",
     "from": ("clinical_pathway_tracking", ("pathway_oid",)),
     "to": ("clinical_rtt_pathway", ("pathway_oid",)), "join": False},
    {"name": "luna_rtt_activity_rtt_pathway", "ref": "P5h_edge30",
     "from": ("clinical_rtt_activity", ("pathway_oid",)),
     "to": ("clinical_rtt_pathway", ("pathway_oid",)), "join": False},
)

# INSR_W7 W7.04 (L2167): W3.09's view-level row_key on the small keyless
# gold children (INSR W3 release/row_key_declarations.json); a declared key
# is used only once the column exists.
W3_ROW_KEY_TABLES = (
    "clinical_appointment_booking", "clinical_hrg_grouping_diagnosis",
    "clinical_medication_order_ingredient", "clinical_hrg_grouping_procedure",
    "clinical_procedure_implant_attribute", "clinical_rtt_pathway_encounter_type",
    "clinical_condition_revision", "clinical_neonatal_care_day_activity",
    "text_document_sensitivity_label", "clinical_neonatal_care_day_high_cost_drug")


# Tables whose person_id is a STRING in the source although every value is a
# person id: published as BIGINT like every other table's, so the model types
# it as a number and a cohort can be limited to its members. Census
# 2026-10-24 over every row of 4_prod.gold: each non-null value satisfies
# cast(try_cast(x AS BIGINT) AS STRING) = x. The safety tables are only
# partly filled (incidents 67%, participants 44%, documents 72%): staff and
# incidents with no patient have none.
STRING_PERSON_KEYS = (
    "clinical_appointment_action", "clinical_pending_movement",
    "clinical_prearrival", "clinical_safety_incident",
    "clinical_safety_incident_participant", "clinical_tracking_attendance",
    "clinical_tracking_episode", "clinical_tracking_location_stay",
    "clinical_tracking_milestone", "text_safety_document",
)


def _active_filter(object_name: str, columns: dict,
                   profile: "DatasetProfile | None" = None) -> str | None:
    """The dataset's active-row filter, for a table that has record_status."""
    p = profile or GOLD
    return (p.active_filter
            if p.active_filter and "record_status" in columns.get(object_name, ())
            else None)


def _default_filter(object_name: str, columns: dict,
                    profile: "DatasetProfile") -> str | None:
    """INSR_W7 W7.03: the profile's extra default filter for one table, once
    the table has the column the filter reads (a release built before the
    column exists keeps its active filter only)."""
    text = profile.default_view_filters.get(object_name)
    if text is None:
        return None
    assert text in DEFAULT_VIEW_FILTERS, f"unlisted default filter {text!r}"
    column = re.match(r"\((\w+) ", text).group(1)
    return text if column in columns.get(object_name, ()) else None


def _row_filter(object_name: str, columns: dict,
                profile: "DatasetProfile | None" = None) -> str | None:
    """INSR_W7 W7.03: a view's whole row filter: the active filter (tables
    with record_status) AND the default filter, or whichever applies, or
    None. Every result is in ALLOWED_RESEARCH_FILTERS by construction."""
    p = profile or GOLD
    parts = [f for f in (_active_filter(object_name, columns, p),
                         _default_filter(object_name, columns, p)) if f]
    return " AND ".join(parts) or None


def _inherit_plan(object_name: str, columns: dict,
                  profile: "DatasetProfile | None" = None) -> dict | None:
    """The parent join for a child without its own time column or person,
    if any, naming the columns it carries.

    A child that gains its own event_datetime or person_id in the source
    stops inheriting that column.
    """
    p = profile or GOLD
    own = columns.get(object_name, ())
    wanted = [(column, links[object_name]) for column, links in (
        (INHERITED_TIME_COLUMN, p.inherited_time),
        (INHERITED_PERSON_COLUMN, p.inherited_person))
        if object_name in links and column not in own]
    # a parent with no person has none to give: the child stays unscoped
    # (cohort mode denies it), whereas a missing parent time is an error
    wanted = [(column, link) for column, link in wanted
              if column != INHERITED_PERSON_COLUMN
              or INHERITED_PERSON_COLUMN in columns.get(link[1], ())]
    if not wanted:
        return None
    links = {link for _, link in wanted}
    assert len(links) == 1, (
        f"{object_name}: inherits from more than one parent {sorted(links)}")
    key, parent, parent_key = wanted[0][1]
    carried = [column for column, _ in wanted]
    parent_columns = columns.get(parent, ())
    assert key in own, f"{object_name}: inherit key {key} missing"
    missing = [c for c in (parent_key, *carried) if c not in parent_columns]
    assert not missing, f"{object_name}: parent {parent} lacks {missing}"
    return {"key": key, "parent": parent, "parent_key": parent_key,
            "parent_where": _active_filter(parent, columns, profile),
            "columns": carried}


def build_source_plans(objects: list, inventory: list,
                       columns: dict, profile: "DatasetProfile | None" = None) -> dict:
    """Plan each object as a direct read, active rows only when versioned."""
    table_types = dict(inventory)
    missing = sorted(set(objects) - set(table_types))
    assert not missing, f"published source objects missing: {missing}"
    plans = {}
    for object_name in objects:
        plan = {"source": object_name,
                "where": _row_filter(object_name, columns, profile)}
        inherit = _inherit_plan(object_name, columns, profile)
        if inherit is not None:
            plan["inherit"] = inherit
        if (object_name in (profile or GOLD).string_person_keys
                and "person_id" in columns.get(object_name, ())):
            plan["bigint"] = ["person_id"]
        plans[object_name] = plan
    return plans


def with_inherited_columns(columns: dict, plans: dict) -> dict:
    """Published column lists, with each inherited column appended."""
    return {object_name: (list(names) + inherited_columns(
                              plans[object_name]["inherit"])
                          if "inherit" in plans.get(object_name, {})
                          else list(names))
            for object_name, names in columns.items()}


def with_inherited_types(column_types: dict, plans: dict) -> dict:
    """Each object's {column: source type}, with each inherited column added
    under its parent's type: the shape profiling and the view DDL walk, so a
    joined-in event_datetime or person_id is profiled like any other column.
    Needs the parent's types, so it runs before tag-source parents are
    dropped."""
    out = {name: dict(types) for name, types in column_types.items()}
    for object_name, plan in plans.items():
        inherit = plan.get("inherit")
        if inherit is None or object_name not in out:
            continue
        parent_types = column_types.get(inherit["parent"], {})
        for column in inherited_columns(inherit):
            assert column in parent_types, (
                f"{object_name}: parent {inherit['parent']} has no "
                f"{column} type")
            out[object_name][column] = parent_types[column]
    return out


def with_bigint_types(column_types: dict, plans: dict) -> dict:
    """Each object's {column: type}, with the STRING keys its plan publishes
    as BIGINT typed BIGINT, as the view will be."""
    out = {name: dict(types) for name, types in column_types.items()}
    for object_name, plan in plans.items():
        for column in plan.get("bigint", ()):
            if object_name in out and column in out[object_name]:
                out[object_name][column] = "BIGINT"
    return out


def rounded_row_count(rows: int, *, person_level: bool) -> int:
    """Row count as shown in the catalogue and used for tiering.

    Reference tables (no person_id) are not disclosive and stay exact, so a
    31-row organisation table no longer reads as 0. Person-level tables round
    to the nearest 1000, or to the nearest 10 below 1000 (never to zero).
    """
    if not person_level or rows <= 0:
        return int(rows)
    if rows < 1000:
        return max(10, int(10 * round(rows / 10)))
    return int(1000 * round(rows / 1000))


MAX_ROLLUP_DIMENSIONS = 8
MAX_ROLLUP_CARDINALITY = 1000

# Spine hubs are `dimension` kind; everything else is a `fact`.
DIMENSION_PREFIXES = ("spine_", "reference_")


def object_kind(object_name: str,
                profile: "DatasetProfile | None" = None) -> str:
    """`dimension` for the dataset's reference tables, else `fact`."""
    p = profile or GOLD
    return ("dimension" if object_name.startswith(p.dimension_prefixes)
            or object_name in p.dimension_tables else "fact")


def _pk_candidates(object_name: str,
                   profile: "DatasetProfile | None" = None) -> list:
    """Key columns to try, in order: gold's naming conventions, then any key
    the dataset's profile declares for this table as a join hub (RBI's
    spell_resource_summary is keyed on spell_encounter_id, which no
    convention guesses)."""
    stem = _base_twin(object_name)
    suffix = stem.split("_", 1)[-1]
    last = stem.rsplit("_", 1)[-1]
    declared = (profile or GOLD).declared_keys.get(object_name)
    out = [f"{stem}_key", f"{suffix}_key", f"{suffix}_id", f"{stem}_id",
           f"{suffix}_row_key", "patient_event_key", "source_record_key",
           f"{last}_key"]
    # a declared key is tried first (OMOP's death is keyed on person_id)
    if declared is not None:
        out = [declared] + [name for name in out if name != declared]
    for hub, hub_key in sorted((profile or GOLD).join_hubs.values()):
        if hub == object_name and hub_key not in out:
            out.append(hub_key)
    return out


# approx_count_distinct (HLL++, ~5% relative error) cannot prove a key
# unique: it read spine_person.person_id as 94.5% distinct when it is exactly
# unique. So every conventional candidate the estimate cannot rule out is
# checked exactly at publish (key_check_sql). Tier C tables are too large for
# an exact distinct and keep the estimate; none of them is a join target.
APPROX_UNIQUE_FLOOR = 0.85  # 3 sigma below the estimator's 5% error
APPROX_UNIQUE_BAND = 0.95
EXACT_KEY_CHECK_MAX_ROWS = 1_000_000_000


def _may_be_unique(meta: dict) -> bool:
    """Whether the approximate stats leave a column possibly unique."""
    profiled = meta.get("profiled_rows")
    if not profiled:
        return True
    return (meta.get("null_count") == 0
            and (meta.get("distinct_count") or 0)
            >= APPROX_UNIQUE_FLOOR * profiled)


def _is_unique(meta: dict) -> bool:
    """Whether a column can serve as the row key.

    An exact publish-time check (`unique_exact`) is authoritative. Without
    one, fall back to the approximate stats with a small error band; any NULL
    disqualifies. Columns profiled without stats (unit fixtures) are accepted
    on naming alone.
    """
    if "unique_exact" in meta:
        return bool(meta["unique_exact"])
    profiled = meta.get("profiled_rows")
    if not profiled:
        return True
    return (meta.get("null_count") == 0
            and (meta.get("distinct_count") or 0)
            >= APPROX_UNIQUE_BAND * profiled)


def key_check_sql(relation: str, object_name: str, columns: list,
                  profile: "DatasetProfile | None" = None) -> str | None:
    """One exact uniqueness query over every plausible key candidate.

    `count(DISTINCT c) = count(*)` is true only when c is non-NULL and unique
    on every published row. None when no candidate needs checking. A
    declared key is always checked: the estimate can't rule it out either.
    """
    by_name = {c["name"]: c for c in columns}
    declared = (profile or GOLD).declared_keys.get(object_name)
    candidates = [name for name in _pk_candidates(object_name, profile)
                  if name in by_name and (name == declared
                                          or _may_be_unique(by_name[name]))]
    if not candidates:
        return None
    for name in candidates:
        assert re.fullmatch(r"[a-z][a-z0-9_]*", name), f"unsafe column {name!r}"
    checks = ", ".join(f"count(DISTINCT `{name}`) = count(*) AS `{name}`"
                       for name in candidates)
    return f"SELECT {checks} FROM {relation}"


def _pk_column(object_name: str, columns: list,
               profile: "DatasetProfile | None" = None) -> str | None:
    """Infer the conventional row key; None rather than a wrong guess.

    A wrong primary key silently corrupts Cube's join de-duplication, so the
    chosen conventional candidate must also be unique in the profiled stats
    (events_patient_event.patient_event_key repeats per mapping row, so its
    key is patient_event_row_key).
    """
    by_name = {c["name"]: c for c in columns}
    for candidate in _pk_candidates(object_name, profile):
        if candidate in by_name and _is_unique(by_name[candidate]):
            return candidate
    return None


def owner_handover_statements(rows: list, catalog: str, owner: str) -> list:
    """ALTER VIEW ... OWNER TO statements for live views not yet `owner`'s.

    The browser views run with their owner's rights, so they must belong to
    the publishing service principal, not to whichever person ran the
    publish. `rows` are information_schema.tables rows (table_schema,
    table_name, table_type, table_owner).
    """
    assert re.fullmatch(r"[0-9a-f-]{36}", owner), f"unsafe owner {owner!r}"
    out = []
    for row in rows:
        if row["table_type"] != "VIEW" or row["table_owner"] == owner:
            continue
        for identifier in (catalog, row["table_schema"], row["table_name"]):
            assert re.fullmatch(r"[A-Za-z0-9_]+", identifier), \
                f"unsafe identifier {identifier!r}"
        out.append(f"ALTER VIEW {catalog}.{row['table_schema']}."
                   f"`{row['table_name']}` OWNER TO `{owner}`")
    return out


# Columns that never serve as the Tier B/C time bound (audit/lineage times).
AUDIT_TIME_COLUMNS = {
    "loaded_at", "source_update_timestamp", "computed_at", "published_at",
    "run_at", "record_status_effective_from", "record_status_effective_to",
    "anonymised_at",
}
PREFERRED_TIME_COLUMNS = ("event_datetime", "event_date", "start_datetime",
                          "period_start", "admission_datetime")
MAX_TIME_NULL_FRACTION = 0.5


def _time_dimension(columns: list,
                    profile: "DatasetProfile | None" = None) -> str | None:
    """Pick the time column the tier guard bounds: a populated clinical time.

    A mostly-NULL column would make every bounded query silently drop most
    rows (reference_cost_line_item.unbundled_currency_datetime is 99% NULL),
    so candidates above MAX_TIME_NULL_FRACTION NULLs are skipped.
    """
    def usable(meta):
        name = meta["name"]
        if meta.get("type") not in ("timestamp", "date"):
            return False
        if name in AUDIT_TIME_COLUMNS or name.startswith(
                ("load_", "ingested_", "updated_", "_")):
            return False
        profiled = meta.get("profiled_rows")
        if profiled:
            return (meta.get("null_count") or 0) <= (
                MAX_TIME_NULL_FRACTION * profiled)
        return True

    candidates = [meta for meta in columns if usable(meta)]
    by_name = {meta["name"]: meta for meta in candidates}
    for name in (profile or GOLD).preferred_time_columns:
        if name in by_name:
            return name
    return candidates[0]["name"] if candidates else None


# Money columns get a `sum_<col>` Cube measure. Per-unit prices are averaged,
# never summed. All gold money is GBP (SLR/PLICS and pharmacy ledgers).
_MONEY = re.compile(r"(^|_)(cost|o_cost|income|margin|fee|expenditure|"
                    r"tariff|price)(_|$)")
# INSR_W7 W7.03: deciles, quintiles, ranks, scores and bands are positions on
# a scale, never summed (imd2025_income_decile read as GBP income, 10-23).
_NOT_MEASURE = re.compile(
    r"(_cd|_code|_id|_key|_ind|_flag|_desc|_display|_min|_max"
    r"|_decile|_quintile|_rank|_score|_band)$")
# INSR_W7 W7.03: area statistics are never money or counts, whatever word
# their name contains (IoD "income", census percentages).
_AREA_STATISTIC = re.compile(r"^(imd|iod|census_)")
# Quantities are deliberately absent: their units differ row to row.
_COUNT_MEASURE = re.compile(r"(^|_)line_count$")


def column_semantics(column: str, type_name: str,
                     profile: "DatasetProfile | None" = None) -> dict:
    """Semantic hints carried into the Cube model and the agent grounding."""
    out = {}
    numeric = type_name in ("int64", "float64")
    if (numeric and not _NOT_MEASURE.search(column)
            and not _AREA_STATISTIC.search(column)):
        if _COUNT_MEASURE.search(column):
            out.update(semantic_role="measure", aggregation="sum")
        elif _MONEY.search(column):
            out.update(semantic_role="measure", unit="GBP",
                       aggregation="avg" if "price" in column else "sum")
    if column == "snomed_code" or column.endswith("_snomed_code"):
        out["code_system"] = "SNOMED CT"
    elif column.endswith("omop_concept_id") or column.endswith(
            "standard_concept_id"):
        out["code_system"] = "OMOP concept_id"
    elif re.search(r"(^|_)icd(10)?(_|$)|_diag_cd$", column):
        out["code_system"] = "ICD-10"
    elif re.search(r"(^|_)opcs(4)?(_|$)|_proc_cd$", column):
        out["code_system"] = "OPCS-4"
    elif column.endswith("loinc_code"):
        out["code_system"] = "LOINC"
    elif column.startswith("dmd_") and column.endswith(("_code", "_id")):
        out["code_system"] = "dm+d"
    # dataset conventions the rules above miss (OMOP's *_concept_id)
    p = profile or GOLD
    if "code_system" not in out:
        for suffix, system in p.code_system_rules:
            if column.endswith(suffix):
                out["code_system"] = system
                break
    return out


# INSR_W7 W7.04 (L1753): one gold row may stand for several identical source
# rows (source_duplicate_count); its money must be summed that many times.
DUPLICATE_WEIGHT = "source_duplicate_count"


def dup_weight_problems(doc: dict) -> list:
    """INSR_W7 W7.04 gate cube_sums_dup_weighted: summed columns of a table
    that publishes source_duplicate_count but carry no sum_weight."""
    out = []
    for name, table in sorted(doc["tables"].items()):
        cols = {c["name"]: c for c in table["columns"]
                if c.get("type") != "excluded"}
        if DUPLICATE_WEIGHT not in cols:
            continue
        out += [f"{name}.{column}" for column, meta in sorted(cols.items())
                if meta.get("aggregation") == "sum"
                and meta.get("sum_weight") != DUPLICATE_WEIGHT]
    return out


def hub_publish_problems(doc: dict, profile: "DatasetProfile") -> list:
    """INSR_W7 W7.04 gate hub_published (F21): each profile hub whose key
    another table in the release carries must have its hub key published and
    the hub table keyed, or nothing can join to it (PMS 10-27: person_id
    described as a direct identifier, 0 relationships)."""
    problems = []
    tables = doc["tables"]
    for key, (hub, hub_key) in sorted(profile.join_hubs.items()):
        if hub not in tables:
            continue
        carriers = [name for name, table in tables.items() if name != hub
                    and any(c["name"] == key for c in table["columns"])]
        if not carriers:
            continue
        col = next((c for c in tables[hub]["columns"] if c["name"] == hub_key),
                   None)
        if col is None or col.get("type") == "excluded":
            problems.append(f"{hub}.{hub_key}: hub key not published "
                            f"({(col or {}).get('withheld')})")
        elif not any(c.get("pk") for c in tables[hub]["columns"]):
            problems.append(f"{hub}: hub table has no row key")
    return problems


def estate_tag_mode(tag_rows) -> dict:
    """INSR_W7 W7.04 (J1): {column_name: (ig_risk, ig_severity)}, the commonest
    pair per column name across the given gold tag rows; ties go to the higher
    risk, then the higher severity. The same rule as the ADC-DB helper
    Prod/Pipelines/_shared/estate_tags.modal_tags (shared fixture
    tests/fixtures/estate_tags_parity.json)."""
    counts = {}
    for (_table, column), tags in _source_tags(tag_rows).items():
        risk = _score(tags.get("ig_risk"), IG_SCORE_RANGES["ig_risk"])
        severity = _score(tags.get("ig_severity"), IG_SCORE_RANGES["ig_severity"])
        if risk is None or severity is None:
            continue
        pairs = counts.setdefault(column, {})
        pairs[(risk, severity)] = pairs.get((risk, severity), 0) + 1
    return {column: max(pairs, key=lambda pair: (pairs[pair], pair))
            for column, pairs in counts.items()}


def estate_key_tags_sql(columns, catalog: str = "4_prod",
                        schema: str = "gold") -> str:
    """INSR_W7 W7.04: the estate's tag rows for the given key column names
    (input to estate_tag_mode); one information_schema read."""
    for name in (catalog, schema, *columns):
        assert re.fullmatch(r"[A-Za-z0-9_]+", name), f"unsafe identifier {name!r}"
    names = ", ".join(f"'{c}'" for c in sorted(columns))
    return ("SELECT table_name, column_name, tag_name, tag_value "
            f"FROM {catalog}.information_schema.column_tags "
            f"WHERE schema_name = '{schema}' AND column_name IN ({names}) "
            "AND tag_name IN ('ig_risk', 'ig_severity') "
            "AND left(table_name, 1) <> '_'")


def tag_consistency_findings(doc: dict, mode: dict,
                             profile: "DatasetProfile") -> list:
    """INSR_W7 W7.04 gate tag_consistency (WARN): key columns (person_id,
    encounter_id, the profile's hub keys) whose published IG scores differ
    from the gold estate mode."""
    keys = {"person_id", "encounter_id", *profile.join_hubs}
    out = []
    for name, table in sorted(doc["tables"].items()):
        for col in table["columns"]:
            if col["name"] not in keys or col["name"] not in mode:
                continue
            got = (col.get("ig_risk_score"), col.get("ig_severity_score"))
            if got != tuple(mode[col["name"]]):
                out.append(f"{name}.{col['name']}: {got} vs estate mode "
                           f"{tuple(mode[col['name']])}")
    return out


def unresolved_expectation_findings(doc: dict, qc_rows) -> list:
    """INSR_W7 W7.04 gate unresolved_expectations (WARN): exported QC results
    whose table the release does not publish (an internal twin, or a renamed
    or retired table): shown, never failing."""
    tables = set(doc["tables"])
    found = set()
    for row in qc_rows:
        table = str(row["dataset"]).split(".")[-1].strip("`")
        if table not in tables:
            found.add(f"{table}:{row['expectation']}")
    return sorted(found)


def _described(object_name: str, descriptions: dict,
               profile: "DatasetProfile", filtered: bool) -> str:
    """INSR_W7 W7.03: the table description plus its notes: the profile's
    note for the table (OMOP person denominator) and, for a table the browser
    filters, the current-version note (L5097)."""
    parts = [descriptions.get(object_name, "")]
    if profile.description_notes.get(object_name):
        parts.append(profile.description_notes[object_name])
    if filtered:
        parts.append(FILTERED_TABLE_NOTE)
    return " ".join(part for part in parts if part)


def build_schema_doc(*, dataset: str, run_id: str, objects: list,
                     cols_meta: dict, row_counts: dict, descriptions: dict,
                     policy: dict, relationships: list,
                     profile: "DatasetProfile | None" = None,
                     filtered_tables=()) -> dict:
    """Build the exact schema document consumed by scripts/uc_model_gen.py.
    filtered_tables: the objects whose view carries a row filter (their
    description gets the history note, INSR_W7 W7.03)."""
    p = profile or GOLD
    tables = {}
    for object_name in objects:
        columns_meta = cols_meta[object_name]
        rows = row_counts[object_name]
        pk = _pk_column(object_name, columns_meta, p)
        entry_columns, rollup_dims, published_meta = [], [], []
        published_names = {m["name"] for m in columns_meta
                           if m["name"] not in INTERNAL_COLUMNS
                           and policy[policy_key(object_name, m["name"])]["action"]
                           == "publish"}
        for meta in columns_meta:
            column = meta["name"]
            if column in INTERNAL_COLUMNS:
                continue
            entry = policy[policy_key(object_name, column)]
            if entry["action"] == "describe":
                # listed with its details, no contents (package_gen marks a
                # type-'excluded' column not queryable and unprofiled)
                entry_columns.append({
                    "name": column, "type": "excluded",
                    "source_type": meta["type"],
                    "comment": meta.get("comment") or column,
                    "ig_risk": entry["ig_risk"],
                    "ig_severity": entry["ig_severity"],
                    "ig_risk_score": entry.get("ig_risk_score"),
                    "ig_severity_score": entry.get("ig_severity_score"),
                    "withheld": entry.get("reason")})
                continue
            out = {"name": column, "type": meta["type"],
                   "comment": meta.get("comment") or column,
                   "ig_risk": entry["ig_risk"],
                   "ig_severity": entry["ig_severity"],
                   "ig_risk_score": entry.get("ig_risk_score"),
                   "ig_severity_score": entry.get("ig_severity_score"),
                   "nullable": bool(meta.get("nullable", True)),
                   "synthesis": "source"}
            if entry["action"] == "mask":
                out["masked"] = entry["mask"]
                out["type"] = ("string" if entry["mask"] == "hash"
                               else "timestamp")
            else:
                out.update(column_semantics(column, out["type"], p))
                if (out.get("aggregation") == "sum"
                        and DUPLICATE_WEIGHT in published_names):
                    # INSR_W7 W7.04 (L1753): weighted by the duplicate count
                    out["sum_weight"] = DUPLICATE_WEIGHT
                published_meta.append(meta)
            if column == pk:
                out["pk"] = True
            entry_columns.append(out)
            if entry.get("rollup_dimension"):
                distinct = meta.get("distinct_count")
                assert distinct is not None and distinct <= MAX_ROLLUP_CARDINALITY, (
                    f"{object_name}.{column}: rollup_dimension needs "
                    f"distinct_count <= {MAX_ROLLUP_CARDINALITY}, got {distinct}")
                rollup_dims.append(column)
        assert len(rollup_dims) <= MAX_ROLLUP_DIMENSIONS, (
            f"{object_name}: at most {MAX_ROLLUP_DIMENSIONS} rollup "
            f"dimensions, got {len(rollup_dims)}")
        assert any(c["type"] != "excluded" for c in entry_columns), \
            f"{object_name}: no published columns"
        tables[object_name] = {
            "kind": object_kind(object_name, p),
            "view_name": object_name,
            "description": _described(object_name, descriptions, p,
                                      object_name in filtered_tables),
            "grain": None,
            "rows": rows,
            "source_rows_rounded": rows,
            "columns": entry_columns,
            "tier": tier_for(rows),
            "time_dimension": _time_dimension(published_meta, p),
            "rollup_dimensions": rollup_dims,
        }
    # Cube cannot compile a join from or to a cube without a primary key. A
    # keyless join target would cost every join into it, so fail the publish
    # here rather than at model deploy. A keyless source loses only its own
    # joins: drop them, and record why, so the table still publishes.
    # A parent link is a Cube join only when it lands on the parent's
    # primary key; on any other column (medication orders are keyed by
    # patient_event_key, their children carry order_id) it ties a child to
    # the cohort but is no join, or Cube would fan the parent out.
    relationships, link_only = _split_parent_links(relationships, tables)
    keyed = {name for name, table in tables.items()
             if any(column.get("pk") for column in table["columns"])}
    keyless_targets = sorted({rel["to"].split(".", 1)[0]
                              for rel in relationships} - keyed)
    assert not keyless_targets, (
        f"join targets without a primary key: {keyless_targets}")
    dropped = [rel for rel in relationships
               if rel["from"].split(".", 1)[0] not in keyed]
    relationships = [rel for rel in relationships if rel not in dropped]
    if dropped:
        print("joins dropped (source has no unique key): "
              f"{sorted(rel['name'] for rel in dropped)}")
    # a parent link ties a child to the cohort whether or not Cube can join
    # from it (cohort scoping needs only the key), so a keyless child keeps
    # its link here even when its join was dropped above
    parent_links = [_parent_link(rel) for rel in relationships + dropped
                    + link_only if rel.get("parent")]
    return {
        "dataset": {"name": dataset, "run_id": run_id,
                    "description": p.description},
        "tables": tables,
        "relationships": [{k: v for k, v in rel.items() if k != "parent"}
                          for rel in relationships],
        "dropped_relationships": [
            dict({k: v for k, v in rel.items() if k != "parent"},
                 reason="source has no unique key") for rel in dropped],
        "parent_links": sorted(parent_links, key=lambda l: l["child"]),
    }


# Naming-convention relationship candidates. Every candidate is probed before
# publication because matching names alone are not evidence of a valid join.
# Every hub key below was checked unique on 2026-09-25. patient_event_key is
# deliberately absent: events_patient_event holds N mapping rows per event, so
# a fact -> event join would fan out.
HUB_FOR_KEY = {
    "person_id": ("spine_person", "person_id"),
    "encounter_id": ("spine_encounter", "encounter_id"),
    "organization_id": ("reference_organization", "organization_id"),
    "organization_key": ("reference_organization", "organization_key"),
    "location_key": ("reference_location", "location_key"),
    "specimen_key": ("clinical_specimen", "patient_event_key"),
    "costed_activity_key": ("clinical_costed_activity", "patient_event_key"),
    "research_study_key": ("reference_research_study", "research_study_key"),
    "journey_key": ("spine_journey", "journey_key"),
    "safety_incident_key": ("clinical_safety_incident", "safety_incident_key"),
    # Role-qualified fact keys point to the practitioner's canonical key.
    "performer_practitioner_id": ("reference_practitioner", "practitioner_id"),
}
MAX_ORPHAN_RATE = 0.02
# INSR_W7 W7.04 (F21): a relationship whose key is set on under 1% of the
# child's rows joins almost nothing; it is reported, not modelled.
MIN_NON_NULL_COVERAGE = 0.01
_HUB_PREFIXES = ("spine_", "events_", "clinical_", "reference_")


def _hub_suffix(hub: str, profile: "DatasetProfile | None" = None) -> str:
    """The hub's name without the dataset's layer prefix, for join names."""
    for prefix in (profile or GOLD).hub_prefixes:
        if hub.startswith(prefix):
            return hub[len(prefix):]
    return hub


def candidate_relationships(cols_meta: dict,
                            profile: "DatasetProfile | None" = None) -> list:
    """Return every object-to-hub edge proposed by naming convention."""
    p = profile or GOLD
    out = []
    for object_name in sorted(cols_meta):
        names = {column["name"] for column in cols_meta[object_name]}
        taken = set()
        for key, (hub, hub_key) in sorted(p.join_hubs.items()):
            hub_names = {column["name"] for column in cols_meta.get(hub, [])}
            if (hub == object_name or hub not in cols_meta or key not in names
                    or hub_key not in hub_names):
                continue
            # One join per hub: Cube cannot join the same cube twice.
            if hub in taken:
                continue
            taken.add(hub)
            out.append({
                "name": f"{object_name}_{_hub_suffix(hub, p)}",
                "from": f"{object_name}.{key}",
                "to": f"{hub}.{hub_key}",
                "cardinality": "many_to_one",
            })
        declared = p.parent_links.get(object_name)
        if declared:
            key, parent, parent_key = declared
            parent_names = {column["name"]
                            for column in cols_meta.get(parent, [])}
            if key in names and parent_key in parent_names:
                link = {"from": f"{object_name}.{key}",
                        "to": f"{parent}.{parent_key}"}
                same = next((r for r in out if r["from"] == link["from"]
                             and r["to"] == link["to"]), None)
                if same is not None:          # a hub join already proposes it
                    same["parent"] = True
                elif parent not in taken:
                    taken.add(parent)
                    out.append({
                        "name": f"{object_name}_{_hub_suffix(parent, p)}",
                        **link, "cardinality": "many_to_one",
                        "parent": True})
    return out


def _split_parent_links(relationships: list, tables: dict) -> tuple:
    """(joins, parent links that are no join): a declared parent link whose
    parent column isn't the parent's primary key is kept out of the joins."""
    pk_of = {name: next((c["name"] for c in table["columns"] if c.get("pk")),
                        None) for name, table in tables.items()}
    link_only = [rel for rel in relationships if rel.get("parent")
                 and pk_of.get(rel["to"].split(".", 1)[0])
                 != rel["to"].split(".", 1)[1]]
    return [rel for rel in relationships if rel not in link_only], link_only


def _parent_link(rel: dict) -> dict:
    """A checked parent-link relationship as the schema doc records it."""
    child, column = rel["from"].split(".", 1)
    parent, parent_column = rel["to"].split(".", 1)
    return {"child": child, "column": column, "parent": parent,
            "parent_column": parent_column}


def parent_person_probe_sql(*, catalog: str, schema: str, rel: dict) -> str:
    """How many of a parent link's parent keys belong to more than one
    person: must be none, or the cohort filter could admit another person's
    child rows through a shared key."""
    to_table, to_col = rel["to"].split(".", 1)
    return f"""
SELECT COUNT(*) AS multi FROM (
    SELECT {qi(to_col)}
    FROM {catalog}.{schema}.{to_table}
    WHERE {qi(to_col)} IS NOT NULL
    GROUP BY {qi(to_col)}
    HAVING COUNT(DISTINCT person_id) > 1
) shared
""".strip()


def orphan_probe_sql(*, catalog: str, schema: str, rel: dict,
                     sample_rows: int = 100_000) -> str:
    """Build one bounded anti-join probe for a candidate relationship."""
    assert sample_rows > 0, "sample_rows must be positive"
    from_table, from_col = rel["from"].split(".", 1)
    to_table, to_col = rel["to"].split(".", 1)
    return f"""
SELECT
    COUNT(*) AS n,
    SUM(CASE WHEN h.{qi(to_col)} IS NULL THEN 1 ELSE 0 END) AS orphans
FROM (
    SELECT {qi(from_col)}
    FROM {catalog}.{schema}.{from_table}
    WHERE {qi(from_col)} IS NOT NULL
    LIMIT {int(sample_rows)}
) f
LEFT JOIN {catalog}.{schema}.{to_table} h
    ON h.{qi(to_col)} = f.{qi(from_col)}
""".strip()


def orphan_probe_batch_sql(*, catalog: str, schema: str,
                           relationships: list,
                           sample_rows: int = 100_000) -> str:
    """Return one UNION query for a bounded batch of relationship probes."""
    assert relationships, "relationship probe batch must not be empty"
    selects = []
    for rel in relationships:
        name = rel["name"]
        assert re.fullmatch(r"[A-Za-z0-9_]+", name), \
            f"unsafe relationship name {name!r}"
        probe = orphan_probe_sql(
            catalog=catalog, schema=schema, rel=rel,
            sample_rows=sample_rows)
        selects.append(
            f"SELECT '{name}' AS relationship_name, n, orphans "
            f"FROM ({probe}) probe")
    return "\nUNION ALL\n".join(selects)


def orphan_probe_group_sql(*, catalog: str, schema: str,
                           relationships: list, schema_of: dict | None = None,
                           sample_rows: int = 100_000) -> str:
    """Probe all relationships to one hub while scanning that hub once.

    schema_of: {table: schema} for tables read from a schema other than
    `schema` (a targeted republish probes its staged tables against live
    hubs)."""
    assert relationships, "relationship probe group must not be empty"
    targets = {rel["to"] for rel in relationships}
    assert len(targets) == 1, \
        f"relationship probe group has multiple targets: {sorted(targets)}"
    to_table, to_col = next(iter(targets)).split(".", 1)

    def where(table):
        """The table's relation, in its own schema."""
        return f"{catalog}.{(schema_of or {}).get(table, schema)}.{table}"

    samples = []
    for rel in relationships:
        name = rel["name"]
        assert re.fullmatch(r"[A-Za-z0-9_]+", name), \
            f"unsafe relationship name {name!r}"
        from_table, from_col = rel["from"].split(".", 1)
        samples.append(f"""
SELECT '{name}' AS relationship_name, {qi(from_col)} AS relationship_key
FROM (
    SELECT {qi(from_col)}
    FROM {where(from_table)}
    WHERE {qi(from_col)} IS NOT NULL
    LIMIT {int(sample_rows)}
) sampled
""".strip())
    union_sql = "\nUNION ALL\n".join(samples)
    return f"""
WITH relationship_samples AS (
{union_sql}
)
SELECT f.relationship_name, COUNT(*) AS n,
       SUM(CASE WHEN h.{qi(to_col)} IS NULL THEN 1 ELSE 0 END) AS orphans
FROM relationship_samples f
LEFT JOIN {where(to_table)} h
    ON h.{qi(to_col)} = f.relationship_key
GROUP BY f.relationship_name
""".strip()


def non_null_coverage(candidates: list, cols_meta: dict) -> dict:
    """INSR_W7 W7.04: {relationship name: share of the child's profiled rows
    whose key is set}, from the publish's own column stats (no extra scan).
    Absent when the key column has no profile counts (a carried table)."""
    out = {}
    for rel in candidates:
        table, column = rel["from"].split(".", 1)
        meta = next((m for m in cols_meta.get(table, []) if m["name"] == column),
                    None)
        profiled = (meta or {}).get("profiled_rows")
        if profiled:
            out[rel["name"]] = 1 - (meta.get("null_count") or 0) / profiled
    return out


def accepted_relationships(candidates: list, orphan_rates: dict,
                           max_orphan_rate: float = MAX_ORPHAN_RATE,
                           shared_parent_keys: dict | None = None,
                           non_null_coverage: dict | None = None,
                           min_non_null_coverage: float = MIN_NON_NULL_COVERAGE):
    """Partition candidates into accepted and rejected relationship edges.
    shared_parent_keys: {parent link name: parent keys held by more than one
    person}; a parent link with any is rejected (and one not checked is kept
    as a join but not as a parent link)."""
    shared = shared_parent_keys or {}
    kept, rejected = [], []
    for rel in candidates:
        rate = orphan_rates.get(rel["name"])
        coverage = (non_null_coverage or {}).get(rel["name"])
        if rate is None or rate > max_orphan_rate:
            rejected.append((rel["name"], rate))
        elif coverage is not None and coverage < min_non_null_coverage:
            rejected.append((rel["name"], f"non-null coverage {coverage:.4f}"))
        elif rel.get("parent") and shared.get(rel["name"]):
            rejected.append((rel["name"], f"{shared[rel['name']]} parent "
                                          "keys shared between people"))
        else:
            if rel.get("parent") and rel["name"] not in shared:
                rel = {k: v for k, v in rel.items() if k != "parent"}
            kept.append(rel)
    return kept, rejected


_EDGE_LITERAL = re.compile(r"'[A-Za-z0-9_]+'")


def _child_key_sql(key: str) -> str:
    """INSR_W7 W7.10: a child key of a declared edge as SQL: a quoted plain
    name ('acs_transfer') is a constant, anything else a child column."""
    if key.startswith("'"):
        assert _EDGE_LITERAL.fullmatch(key), f"unsafe edge constant {key!r}"
        return key
    assert re.fullmatch(r"[A-Za-z0-9_]+", key), f"unsafe identifier {key!r}"
    return f"c.{qi(key)}"


def declared_edges_to_measure(edges, rebuilt, live=()):
    """INSR W7 review: the declared edges a publish measures. Both ends must
    be in the release (built by this run, or `live` tables a targeted
    republish carries from the parent) and at least one end built by this
    run. A full publish passes no live tables, so both ends must be built;
    a republish also re-measures an edge from a rebuilt table to a live one,
    which would otherwise leave the release's Cube joins."""
    rebuilt, present = set(rebuilt), set(rebuilt) | set(live)
    return [e for e in edges
            if e["from"][0] in present and e["to"][0] in present
            and (e["from"][0] in rebuilt or e["to"][0] in rebuilt)]


def merge_declared_entries(parent_doc, changed, declared_doc,
                           declared_rejected, edges):
    """INSR W7 review: (declared_relationships, rejected) for a republished
    release: the parent's entries for edges with no changed end, plus this
    run's measurement of the edges it re-measured. An edge with a changed end
    that was not re-measured (its table left the release) is dropped.
    Entries are matched by edge name (rejected entries carry no from/to)."""
    changed = set(changed)
    touched = {e["name"] for e in edges
               if e["from"][0] in changed or e["to"][0] in changed}
    kept = [r for r in parent_doc.get("declared_relationships", [])
            if r["name"] not in touched] + list(declared_doc)
    rejected = [r for r in parent_doc.get("rejected_declared_relationships", [])
                if r["name"] not in touched] + list(declared_rejected)
    return kept, rejected


def edge_coverage_sql(*, catalog: str, schema: str, edge: dict,
                      schema_of: dict | None = None) -> str:
    """INSR_W7 W7.04: one exact aggregate for a declared edge on the staged
    views: child rows, rows with every key column set, rows whose key finds a
    parent row, and the parent's largest row count per key (above 1 means
    many-to-many). The parent is grouped first, so the join never fans out.
    A child key may be a constant (W7.10, _child_key_sql); only the child's
    columns count towards keyed_rows. `schema_of` (INSR W7 review) names the
    schema of a table read from elsewhere than `schema`: a targeted republish
    reads the tables it rebuilt from staging and the rest from live."""
    child, child_cols = edge["from"]
    parent, parent_cols = edge["to"]
    child_schema = (schema_of or {}).get(child, schema)
    parent_schema = (schema_of or {}).get(parent, schema)
    for name in (catalog, child_schema, parent_schema, child, parent, *parent_cols):
        assert re.fullmatch(r"[A-Za-z0-9_]+", name), f"unsafe identifier {name!r}"
    child_keys = [_child_key_sql(c) for c in child_cols]
    keyed = " AND ".join(f"{k} IS NOT NULL" for k in child_keys
                         if k.startswith("c.")) or "TRUE"
    on = " AND ".join(f"p.{qi(pc)} = {ck}"
                      for ck, pc in zip(child_keys, parent_cols))
    pkeys = ", ".join(qi(c) for c in parent_cols)
    return f"""
WITH p AS (
  SELECT {pkeys}, COUNT(*) AS parent_rows
  FROM {catalog}.{parent_schema}.{qi(parent)}
  GROUP BY {pkeys}
)
SELECT COUNT(*) AS child_rows,
       COUNT_IF({keyed}) AS keyed_rows,
       COUNT_IF(p.parent_rows IS NOT NULL) AS matched_rows,
       (SELECT MAX(parent_rows) FROM p) AS parent_max_rows_per_key
FROM {catalog}.{child_schema}.{qi(child)} c
LEFT JOIN p ON {on}
""".strip()


def _edge_end(end: tuple) -> str:
    """'table.column' for a one-column edge end, 'table.(a,b)' otherwise."""
    table, cols = end
    return f"{table}.{cols[0]}" if len(cols) == 1 else f"{table}.({','.join(cols)})"


def accept_declared_edges(edges, coverage: dict,
                          min_non_null_coverage: float = MIN_NON_NULL_COVERAGE):
    """INSR_W7 W7.04: (joins, documented, rejected) for measured declared
    edges. coverage = matched child rows / all child rows. Below the minimum
    an edge is rejected with its figure. A kept edge becomes a Cube join only
    when declared one, single-column, and the parent has one row per key;
    every other kept edge is documented with its measured cardinality."""
    joins, documented, rejected = [], [], []
    for edge in edges:
        m = coverage.get(edge["name"]) or {}
        if not m.get("child_rows"):
            rejected.append({"name": edge["name"], "ref": edge["ref"],
                             "reason": "not measured or empty child"})
            continue
        rate = m["matched_rows"] / m["child_rows"]
        if rate < min_non_null_coverage:
            rejected.append({"name": edge["name"], "ref": edge["ref"],
                             "reason": f"coverage {rate:.4f} < {min_non_null_coverage}"})
            continue
        one_parent = (m.get("parent_max_rows_per_key") or 0) <= 1
        if edge["join"] and one_parent and len(edge["from"][1]) == 1:
            joins.append({"name": edge["name"], "from": _edge_end(edge["from"]),
                          "to": _edge_end(edge["to"]),
                          "cardinality": "many_to_one"})
            continue
        documented.append({
            "name": edge["name"], "ref": edge["ref"],
            "from": _edge_end(edge["from"]), "to": _edge_end(edge["to"]),
            "cardinality": "many_to_one" if one_parent else "many_to_many",
            "coverage": round(rate, 4),
            "keyed_match": (round(m["matched_rows"] / m["keyed_rows"], 4)
                            if m.get("keyed_rows") else None),
            "parent_max_rows_per_key": m.get("parent_max_rows_per_key")})
    return joins, documented, rejected


def reusable_relationship_result(candidates: list,
                                 receipt_payloads: list[dict]):
    """Recover a complete probe result from an earlier same-run failure."""
    by_name = {rel["name"]: rel for rel in candidates}
    if len(by_name) != len(candidates):
        return None
    for payload in receipt_payloads:
        phases = payload.get("phases") if isinstance(payload, dict) else None
        if not isinstance(phases, list):
            continue
        phase = next((item for item in phases
                      if item.get("phase") == "relationships"), None)
        if not phase or not isinstance(phase.get("rejected"), list):
            continue
        rejected_names = set(phase["rejected"])
        if (not rejected_names <= set(by_name)
                or phase.get("kept") != len(candidates) - len(rejected_names)):
            continue
        kept = [rel for rel in candidates if rel["name"] not in rejected_names]
        rejected = [(name, None) for name in sorted(rejected_names)]
        return kept, rejected
    return None


def row_counts_from_stats(records: list, objects: list) -> dict:
    """Recover one consistent display row count per published object."""
    counts = {}
    for record in records:
        table = record["table_name"]
        value = record["source_row_count_rounded"]
        if table in counts:
            assert counts[table] == value, (
                f"inconsistent source row counts for {table}: "
                f"{counts[table]} and {value}")
        else:
            counts[table] = value
    missing = sorted(set(objects) - set(counts))
    assert not missing, f"source row counts missing for {missing}"
    return {object_name: counts[object_name] for object_name in objects}


# ---- Time coverage, measured at every publish -------------------------------
# Each table with a time dimension gets its coverage from its own monthly row
# counts at publish: where its continuous feed starts, whether it has stopped,
# and the last month that is complete at the release boundary. Nothing is read
# from a file. Curated explanations of source gaps are not computed here: they
# belong in the source table's comment, which the publish already carries into
# the description.
COMPLETE_MONTH_SHARE = 0.85     # of the month's reference level
COVERAGE_LOOKBACK_MONTHS = 12   # the trailing median is taken over these
COVERAGE_MIN_HISTORY_MONTHS = 6  # fewer than this and completeness is unknown
COVERAGE_WALK_BACK_MONTHS = 12  # how far back from the latest month to look
FEED_MONTH_SHARE = 0.01         # of the busiest year's median month: running
BUSIEST_MONTHS = 12             # the months that level is taken over
STALLED_AFTER_MONTHS = 3        # quiet longer than this: a gap or a stop
COVERAGE_KEYS = ("coverage_start", "coverage_end", "complete_through")
_MONTH = re.compile(r"\d{4}-\d{2}")


def _month_index(month: str) -> int:
    assert _MONTH.fullmatch(month), f"not a YYYY-MM month: {month!r}"
    return int(month[:4]) * 12 + int(month[5:]) - 1


def _month_name(index: int) -> str:
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def _median(values: list) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    return (ordered[middle] if len(ordered) % 2
            else (ordered[middle - 1] + ordered[middle]) / 2)


def release_boundary(pipeline_rows: list, profile: "DatasetProfile",
                     published_at: str) -> str:
    """The release's data boundary, YYYY-MM-DD: the end of the pipeline update
    that built it (the same role estate_enrich's fetch bounds lineage with),
    or the publish time for a dataset with no pipeline the publish can see."""
    roles = set(profile.pipelines)
    if not roles:
        return str(published_at)[:10]
    role = profile.qc_role if profile.qc_role in roles else sorted(roles)[0]
    end = next((row["update_end"] for row in pipeline_rows
                if row["role"] == role), None)
    assert end, f"no completed {role} update in pipeline_config"
    return str(end)[:10]


def monthly_counts_sql(relation: str, time_column: str) -> str:
    """Rows per calendar month of one table's time column."""
    column = qi(time_column)
    return (f"SELECT date_format(date_trunc('MONTH', {column}), 'yyyy-MM') "
            f"AS month, COUNT(*) AS n FROM {relation} "
            f"WHERE {column} IS NOT NULL GROUP BY 1")


def table_coverage(monthly: dict, boundary: str) -> dict:
    """{coverage_start, coverage_end, complete_through} (YYYY-MM or None) from
    one table's rows per month.

    Months from the boundary's month on are left out: that month is still
    filling in, and later ones are future-dated (booked appointments).
    A month is running when it holds at least FEED_MONTH_SHARE of the busiest
    year's level (the median of the BUSIEST_MONTHS largest months, so one
    bulk-load spike does not set it).
    - coverage_start: the first month of the continuous run that ends at the
      last running month, where continuous allows up to STALLED_AFTER_MONTHS
      quiet months in a row (a small table's lulls). Stray old dates,
      year-only onsets heaped on 1 January, pilot islands and anything
      before a longer gap are not the start; a ramp-up from go-live is.
    - coverage_end: the last running month, only when it is more than
      STALLED_AFTER_MONTHS before the boundary's month (a stopped feed).
    - complete_through: walking back from the last running month, the latest
      month whose rows reach COMPLETE_MONTH_SHARE of its reference level: the
      lower of the median of the COVERAGE_LOOKBACK_MONTHS before it and the
      same month a year earlier (so a month that is always quiet, such as
      August, is not taken for a lagging one). An earlier dip does not
      matter; a recent shortfall is coding lag.
    """
    bound = _month_index(boundary[:7])
    seen = {_month_index(m): int(n) for m, n in monthly.items()
            if m and _month_index(m) < bound}
    if not any(seen.values()):
        return dict.fromkeys(COVERAGE_KEYS)
    busiest = sorted(seen.values(), reverse=True)[:BUSIEST_MONTHS]
    floor = FEED_MONTH_SHARE * _median(busiest)
    count = lambda month: seen.get(month, 0)  # noqa: E731
    last = max(i for i in seen if count(i) >= floor)
    start, probe = last, last - 1
    while probe >= start - 1 - STALLED_AFTER_MONTHS and probe >= min(seen):
        if count(probe) >= floor:
            start = probe
        probe -= 1
    complete = None
    for month in range(last, max(start, last - COVERAGE_WALK_BACK_MONTHS) - 1, -1):
        history = [count(i) for i in
                   range(max(start, month - COVERAGE_LOOKBACK_MONTHS), month)]
        if len(history) < COVERAGE_MIN_HISTORY_MONTHS:
            break
        reference = _median(history)
        if month - 12 >= start:
            reference = min(reference, count(month - 12))
        if count(month) >= COMPLETE_MONTH_SHARE * reference:
            complete = month
            break
    stopped = (bound - 1) - last > STALLED_AFTER_MONTHS
    return {"coverage_start": _month_name(start),
            "coverage_end": _month_name(last) if stopped else None,
            "complete_through": (_month_name(complete)
                                 if complete is not None else None)}


def coverage_problems(doc: dict) -> list:
    """Verify: the release names its boundary, and every table with a time
    dimension carries a coverage object measured at publish."""
    problems = []
    if not _BOUNDARY_DATE.fullmatch(
            str(doc.get("dataset", {}).get("release_boundary") or "")):
        problems.append("dataset.release_boundary missing")
    for name, table in sorted(doc.get("tables", {}).items()):
        if not table.get("time_dimension"):
            continue
        coverage = table.get("coverage")
        if not isinstance(coverage, dict) or set(coverage) < set(COVERAGE_KEYS):
            problems.append(f"{name}: no coverage")
    return problems


_BOUNDARY_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


# ---- Privileged exports into the browser-readable lookup schema ------------
# The browser SP never reads 4_prod, the Lakeflow event log or pipeline
# settings. The publisher exports exactly what enrichment needs.
GOLD_PIPELINE_ID = "a69f3e68-8e93-408a-946e-17d286c69544"
SILVER_PIPELINE_ID = "061f9450-ec36-4c3d-b8fc-8b6bcbcc397a"  # S2_clinical: role "silver" kept for existing readers
# INSR R0: every silver domain pipeline's configuration and latest update is exported too.
SILVER_DOMAIN_PIPELINE_IDS = {"silver_s1_spine": "53f239d6-198f-4ab1-8e1c-dd2b78022a17", "silver_s2_clinical": "061f9450-ec36-4c3d-b8fc-8b6bcbcc397a", "silver_s3_pathology": "b23a5a34-026f-4db1-a303-da506999b3e9", "silver_s4_forms": "169ff06d-7439-490b-ad7e-38eac9d15f80", "silver_s5_specialist": "896634e6-16de-460c-844a-a6e6105753bc", "silver_s6_activity": "a4c27cd9-6177-428d-9bdd-1a15ec7b99eb", "silver_s7_documents": "354f7d05-615f-48e8-9f4c-e66f158d738c", "silver_s8_index": "55f800f5-debe-4414-af69-02995b11b9b4"}
_SAFE_ID = re.compile(r"[A-Za-z0-9_.\-]+")
_EVENT_LOG_RELATION = re.compile(
    r"event_log\('[0-9a-f\-]{36}'\)|[A-Za-z0-9_`]+\.[A-Za-z0-9_`]+\.[A-Za-z0-9_`]+")

SOURCE_COLUMN_META_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    run_id STRING, table_schema STRING, table_name STRING,
    column_name STRING, comment STRING, ig_risk STRING, ig_severity STRING
) USING DELTA
"""

PIPELINE_CONFIG_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    run_id STRING, role STRING, pipeline_id STRING, catalog STRING,
    configuration STRING, update_id STRING, update_end STRING
) USING DELTA
"""

QC_RESULT_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    run_id STRING, pipeline_id STRING, update_id STRING, dataset STRING,
    expectation STRING, passed BIGINT, passed_band STRING, failed BIGINT,
    failed_band STRING, computed_at STRING
) USING DELTA
"""


def source_column_meta_sql(*, target: str, run_id: str,
                           source_catalog: str = "4_prod",
                           schemas: tuple = ("silver", "bronze", "tmp"),
                           lookback_days: int = 365) -> str:
    """Comments and IG tags for the dataset's upstream schemas (gold's:
    silver, bronze and internal tmp), plus raw tables that column lineage
    shows feeding the estate."""
    assert _SAFE_ID.fullmatch(run_id) and _SAFE_ID.fullmatch(target)
    assert all(_SAFE_ID.fullmatch(s) for s in schemas), schemas
    return f"""INSERT INTO {target}
SELECT '{run_id}' AS run_id, c.table_schema, c.table_name, c.column_name,
       c.comment,
       max(CASE WHEN t.tag_name = 'ig_risk' THEN t.tag_value END) AS ig_risk,
       max(CASE WHEN t.tag_name = 'ig_severity' THEN t.tag_value END)
           AS ig_severity
FROM {source_catalog}.information_schema.columns c
LEFT JOIN {source_catalog}.information_schema.column_tags t
  ON t.schema_name = c.table_schema AND t.table_name = c.table_name
 AND t.column_name = c.column_name
 AND t.tag_name IN ('ig_risk', 'ig_severity')
WHERE c.table_schema IN ({", ".join(f"'{s}'" for s in schemas)})
   OR (c.table_schema = 'raw' AND c.table_name IN (
        SELECT DISTINCT source_table_name FROM system.access.column_lineage
        WHERE source_table_catalog = '{source_catalog}'
          AND source_table_schema = 'raw'
          AND event_date >= date_sub(current_date(), {int(lookback_days)})))
GROUP BY c.table_schema, c.table_name, c.column_name, c.comment"""


def pipeline_config_sql(*, target: str, run_id: str, pipelines: dict,
                        catalog: str = "4_prod") -> str:
    """Per pipeline: its configuration (the spark.conf the code resolves
    names with) and the latest COMPLETED update, i.e. the one whose tables
    this release published."""
    assert _SAFE_ID.fullmatch(run_id) and _SAFE_ID.fullmatch(target)
    for role, pipeline_id in pipelines.items():
        assert _SAFE_ID.fullmatch(role) and _SAFE_ID.fullmatch(pipeline_id), \
            f"unsafe pipeline entry {role!r}={pipeline_id!r}"
    values = ", ".join(f"('{role}', '{pid}')"
                       for role, pid in sorted(pipelines.items()))
    return f"""INSERT INTO {target}
WITH roles AS (SELECT * FROM VALUES {values} AS r(role, pipeline_id)),
cfg AS (
  SELECT pipeline_id, to_json(configuration) AS configuration
  FROM system.lakeflow.pipelines
  WHERE pipeline_id IN (SELECT pipeline_id FROM roles)
  QUALIFY row_number() OVER (PARTITION BY pipeline_id
                             ORDER BY change_time DESC) = 1),
upd AS (
  SELECT pipeline_id, max_by(update_id, period_end_time) AS update_id,
         CAST(max(period_end_time) AS STRING) AS update_end
  FROM system.lakeflow.pipeline_update_timeline
  WHERE pipeline_id IN (SELECT pipeline_id FROM roles)
    AND result_state = 'COMPLETED'
  GROUP BY pipeline_id)
SELECT '{run_id}' AS run_id, roles.role, roles.pipeline_id,
       '{catalog}' AS catalog, cfg.configuration, upd.update_id,
       upd.update_end
FROM roles
LEFT JOIN cfg ON cfg.pipeline_id = roles.pipeline_id
LEFT JOIN upd ON upd.pipeline_id = roles.pipeline_id"""


def qc_events_sql(*, event_log_relation: str, update_id: str) -> str:
    """Per (dataset, expectation): passed/failed records for one update.

    event_log_relation is event_log('<pipeline-id>') (pipeline owner only)
    or a published event-log table <catalog>.<schema>.<table>."""
    assert _EVENT_LOG_RELATION.fullmatch(event_log_relation), \
        f"unsupported event log relation {event_log_relation!r}"
    assert _SAFE_ID.fullmatch(update_id), f"unsafe update id {update_id!r}"
    return f"""SELECT e.dataset AS dataset, e.name AS expectation,
       sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
FROM (
  SELECT explode(from_json(details:flow_progress:data_quality:expectations,
         'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
  FROM {event_log_relation}
  WHERE event_type = 'flow_progress' AND origin.update_id = '{update_id}'
) x
GROUP BY e.dataset, e.name"""


# ---------------------------------------------------------------------------
# Dataset profiles: everything the publish needs to know about one browser
# dataset. Gold's profile is exactly the constants above (pinned by
# tests/test_browser_publish_profiles.py and the golden test); every
# profile-aware function takes `profile=None`, meaning gold, so code and
# tests written for gold behave as before.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DatasetProfile:
    """One browser dataset: its id (the d_databrowser schema and the key the
    app hangs grants, cohorts and threads off), label, source schema and the
    conventions the publish needs to read it."""
    id: str
    label: str
    description: str
    source_catalog: str
    source_schema: str
    # relations to leave out: by name prefix, by name, and `_v_` views whose
    # base twin exists (gold's pre-restructure leftovers)
    skip_prefixes: tuple = ("_", "event_log_")
    skip_tables: tuple = ()
    skip_v_twins: bool = False
    # the one row filter the browser may add (tables with record_status)
    active_filter: str | None = None
    # child -> (child key, parent table, parent key): children without a time
    # column take their parent's event_datetime
    inherited_time: dict = field(default_factory=dict)
    # the same, for patient children without a person_id of their own
    inherited_person: dict = field(default_factory=dict)
    # tables whose STRING person_id is published as BIGINT
    string_person_keys: tuple = ()
    # table -> its row key, where naming can't guess it or the table is too
    # big for the exact check by default: a declared key is always checked
    # exactly at publish, whatever the table's size
    declared_keys: dict = field(default_factory=dict)
    # `dimension` kind: by prefix or by name; everything else is a `fact`
    dimension_prefixes: tuple = ()
    dimension_tables: tuple = ()
    # key column -> (hub table, hub key): naming-convention join candidates
    join_hubs: dict = field(default_factory=dict)
    # child -> (child key, parent table, parent key): declared parent links
    # (PARENT_LINKS), proposed and checked like the hub joins
    parent_links: dict = field(default_factory=dict)
    # prefixes stripped from a hub's name in relationship names
    hub_prefixes: tuple = ()
    # table -> Cube pre-aggregation dimensions (Tier C)
    rollup_dimensions: dict = field(default_factory=dict)
    # the Tier B/C time bound, in order of preference
    preferred_time_columns: tuple = PREFERRED_TIME_COLUMNS
    # (column suffix, code system) for columns the default rules miss
    code_system_rules: tuple = ()
    # pipeline role -> Lakeflow pipeline id (QC export and lineage)
    pipelines: dict = field(default_factory=dict)
    qc_role: str | None = None
    # source schemas whose column comments/tags the exports carry
    upstream_schemas: tuple = ()
    # (catalog, schema) of a search index to expose as lookup.search_index
    search_index: tuple | None = None
    # boilerplate cut from table comments to make descriptions
    qc_boilerplate: str | None = None
    # how clinical concepts are coded, which steers the chat's rules and code
    # tools: snomed (gold's *_snomed_code columns), omop (*_concept_id over
    # the OMOP vocabulary) or none (no clinical coding)
    coding: str = "snomed"
    # the Insights lenses the dataset offers, default first: research, or
    # operational (BI). RBI is the only BI dataset.
    lenses: tuple = ("research",)
    # ADC-DB path prefixes of the dataset's pipeline code (Discover's code
    # tab and quality rules; scripts/estate_enrich.py)
    code_paths: tuple = ()
    # INSR_W7 W7.03 (J15, 2026-10-29): raw area codes listed but never served
    # (table -> columns); the browser shows derived area data instead
    area_code_columns: dict = field(default_factory=dict)
    # INSR_W7 W7.03 (J9): an extra default row filter per table, ANDed with
    # the active filter; each value must be in DEFAULT_VIEW_FILTERS
    default_view_filters: dict = field(default_factory=dict)
    # INSR_W7 W7.03 (L692): one-row metadata tables rule 5 never applies to
    min_text_evidence_exempt: tuple = ()
    # INSR_W7 W7.03 (L315): a sentence appended to a table's description
    description_notes: dict = field(default_factory=dict)
    # INSR_W7 W7.04: declared relationships, measured at every publish
    # ({name, ref, from: (table, (cols)), to: (table, (cols)), join})
    declared_edges: tuple = ()


GOLD = DatasetProfile(
    id="gold", label="Research",
    description=("Barts Health curated gold research products "
                 "(4_prod.gold): SNOMED CT-coded clinical facts with OMOP "
                 "concept ids, patient spine, reference data and SLR/PLICS "
                 "activity costs."),
    source_catalog="4_prod", source_schema="gold",
    skip_prefixes=("_", "event_log_", "reference_dq_"),
    skip_v_twins=True,
    active_filter=ACTIVE_FILTER,
    inherited_time=INHERITED_TIME,
    inherited_person=INHERITED_PERSON,
    string_person_keys=STRING_PERSON_KEYS,
    # the accession view's only natural key is the raw accession number,
    # never published (ig_risk 4); SILVER_RELEASE_20261025 adds a hashed key
    # (a declared key is used only once the column exists)
    declared_keys={"clinical_v_imaging_accession": "accession_row_key",
                   **{table: "row_key" for table in W3_ROW_KEY_TABLES}},
    declared_edges=GOLD_DECLARED_EDGES,
    dimension_prefixes=DIMENSION_PREFIXES,
    join_hubs=HUB_FOR_KEY,
    parent_links=PARENT_LINKS,
    hub_prefixes=_HUB_PREFIXES,
    rollup_dimensions=ROLLUP_DIMENSIONS,
    pipelines={"gold": GOLD_PIPELINE_ID, "silver": SILVER_PIPELINE_ID, **SILVER_DOMAIN_PIPELINE_IDS},
    qc_role="gold",
    upstream_schemas=("silver", "bronze", "tmp"),
    search_index=("3_lookup", "gold"),
    qc_boilerplate="Gold QC twin of the silver product",
    code_paths=("Prod/Pipelines/Gold/",),
    # INSR_W7 W7.03 (J9): text_document keeps every version in gold
    default_view_filters={"text_document": LATEST_VERSION_FILTER},
    # INSR_W7 W7.03 (J15): a person's raw address geography. The area
    # reference tables (reference_area_iod2025_lsoa, reference_area_census2021_oa)
    # hold no person and stay published; reference_location is Trust sites.
    # uprn_match_quality is a match-quality class, not an area code (W7.01).
    area_code_columns={"reference_person_address": (
        "lsoa", "msoa", "output_area", "postcode_outward", "postcode_masked",
        "radon_tile_ref")},
)

OMOP_VOCABULARY = ("concept", "concept_ancestor", "concept_class",
                   "concept_cpt4", "concept_relationship", "concept_synonym",
                   "domain", "drug_strength", "relationship", "vocabulary")

OMOP = DatasetProfile(
    id="omop", label="OMOP",
    description=("Barts Health OMOP CDM 5.4 (4_prod.omop_gold): the QC'd "
                 "OMOP model built over omop_silver with the gold-journey "
                 "data-quality rules, and the OMOP vocabulary."),
    source_catalog="4_prod", source_schema="omop_gold",
    # omop_gold_dq_finding / omop_gold_drop_metrics are the pipeline's QC
    # accounting, skipped like gold's reference_dq_* tables
    skip_prefixes=("_", "event_log_", "omop_gold_"),
    # vocabulary tables are reference data: dimensions, never person-level
    dimension_tables=("person", "provider", "care_site", "location",
                      "cdm_source", *OMOP_VOCABULARY),
    join_hubs={"person_id": ("person", "person_id"),
               "visit_occurrence_id": ("visit_occurrence",
                                       "visit_occurrence_id"),
               "visit_detail_id": ("visit_detail", "visit_detail_id"),
               "provider_id": ("provider", "provider_id"),
               "care_site_id": ("care_site", "care_site_id"),
               "location_id": ("location", "location_id")},
    preferred_time_columns=(
        "condition_start_date", "drug_exposure_start_date", "procedure_date",
        "measurement_date", "observation_date", "visit_start_date",
        "visit_detail_start_date", "device_exposure_start_date",
        "death_date", "observation_period_start_date"),
    code_system_rules=(("_concept_id", "OMOP concept_id"),),
    # death has one row per person (547,610 rows, 0 repeats, 2026-10-24);
    # measurement (1.68bn rows) is over the exact-check limit, and the
    # estimate can't prove measurement_id unique (0 repeats in a 1% sample)
    declared_keys={"death": "person_id", "measurement": "measurement_id"},
    coding="omop",
    # no prod omop_gold pipeline is visible to the browser SP (2026-10-21):
    # lineage cuts off at the release's publish time instead
    code_paths=("Prod/Pipelines/Gold/omop_gold_pipeline.py",
                "Prod/Pipelines/Silver/omop_model_pipeline.py"),
    upstream_schemas=("omop_silver", "silver"),
    # INSR_W7 W7.03 (J15): location's 31,778 LSOA-grain rows carry the LSOA
    # code in location_source_value and the area in county (2026-10-29); their
    # latitude/longitude are NULL, and the 410 Trust-site rows' coordinates
    # are published (J15 detail, open question 7)
    area_code_columns={"location": ("location_source_value", "county")},
    min_text_evidence_exempt=("cdm_source", "metadata"),
    description_notes={"person": (
        "Every person here has an observation period: OMOP person shares use "
        "this table as their denominator (J7, 2026-10-29).")},
)

PMS_PHARMA = DatasetProfile(
    id="pms_pharma", label="PMS Pharma",
    description=("Post-marketing surveillance of medicines (4_prod.pms_pharma):"
                 " exposures, outcomes, labs, vitals, scores and safety "
                 "signals for the drugs of interest."),
    source_catalog="4_prod", source_schema="pms_pharma",
    dimension_tables=("pms_patient", "pms_lab_reference"),
    # one hub only: encounters, exposures and lab references repeat per drug
    # set (drug_set_id), so their own ids are not keys (8_dev, 2026-10-21:
    # encounter_id 1.6M rows / 0.78M distinct) and cannot be join targets
    join_hubs={"person_id": ("pms_patient", "person_id")},
    hub_prefixes=("pms_",),
    upstream_schemas=("gold", "silver"),
    pipelines={"pms_pharma": "dee61ff7-f217-45e6-a3f5-1970c865a5cf"},
    qc_role="pms_pharma",
    code_paths=("Prod/Pipelines/PMS/pms_pharma_",),
    area_code_columns={"pms_patient": ("lsoa",)},  # INSR_W7 W7.03 (J15)
)

PMS_DEVICE = DatasetProfile(
    id="pms_device", label="PMS Device",
    description=("Post-marketing surveillance of implanted medical devices "
                 "(4_prod.pms_device): device exposures, procedures, "
                 "follow-up and outcome events."),
    source_catalog="4_prod", source_schema="pms_device",
    dimension_tables=("pms_patient",),
    join_hubs={"person_id": ("pms_patient", "person_id"),
               "exposure_id": ("pms_device_exposure", "exposure_id")},
    hub_prefixes=("pms_",),
    upstream_schemas=("gold", "silver"),
    pipelines={"pms_device": "1ac14c85-d177-4e31-bf6a-6a1987e54bee"},
    qc_role="pms_device",
    code_paths=("Prod/Pipelines/PMS/pms_device_",),
    area_code_columns={"pms_patient": ("lsoa",), "pms_dataset": ("lsoa",)},  # INSR_W7 W7.03 (J15)
)

RBI = DatasetProfile(
    id="rbi", label="RBI",
    description=("Resource use and cost for business intelligence "
                 "(4_prod.rbi): costed activity, resource-use events, "
                 "spell, journey and person-month summaries and unit costs."),
    source_catalog="4_prod", source_schema="rbi",
    dimension_tables=("unit_cost_reference",),
    join_hubs={"resource_use_event_key": ("resource_use_event",
                                          "resource_use_event_key"),
               "spell_encounter_id": ("spell_resource_summary",
                                      "spell_encounter_id")},
    upstream_schemas=("gold", "silver"),
    pipelines={"rbi": "9accbce3-a180-4008-89e7-c49e127dd96f"},
    qc_role="rbi",
    code_paths=("Prod/Pipelines/RBI/",),
    coding="none",
    lenses=("operational",),
)

PROFILES = {p.id: p for p in (GOLD, OMOP, PMS_PHARMA, PMS_DEVICE, RBI)}

# The closed allow-list of row filters a view may carry, so no other text can
# become view DDL: each profile's active filter, each listed default filter,
# and each active filter ANDed with each default filter (INSR_W7 W7.03) --
# exactly what _row_filter can build.
def _allowed_filters() -> tuple:
    """Every row filter _row_filter can produce, sorted."""
    actives = {p.active_filter for p in PROFILES.values() if p.active_filter}
    combined = {f"{a} AND {d}" for a in actives for d in DEFAULT_VIEW_FILTERS}
    return tuple(sorted(actives | set(DEFAULT_VIEW_FILTERS) | combined))


ALLOWED_RESEARCH_FILTERS = _allowed_filters()

_SAFE_NAME = re.compile(r"[A-Za-z0-9_]+")


def validate_profile(profile: DatasetProfile) -> list[str]:
    """Problems with one profile, in words; [] when it is usable."""
    problems = []
    for name in (profile.id, profile.source_catalog, profile.source_schema,
                 *profile.skip_tables, *profile.dimension_tables,
                 *profile.join_hubs, *profile.rollup_dimensions):
        if not _SAFE_NAME.fullmatch(name):
            problems.append(f"unsafe identifier {name!r}")
    for key, (hub, hub_key) in profile.join_hubs.items():
        if not (_SAFE_NAME.fullmatch(hub) and _SAFE_NAME.fullmatch(hub_key)):
            problems.append(f"unsafe hub {hub}.{hub_key} for {key}")
    for table, key in profile.declared_keys.items():
        if not (_SAFE_NAME.fullmatch(table) and _SAFE_NAME.fullmatch(key)):
            problems.append(f"unsafe declared key {table}.{key}")
    for child, (key, parent, parent_key) in profile.inherited_time.items():
        if parent == child:
            problems.append(f"{child} cannot inherit time from its own parent")
    for child, link in profile.inherited_person.items():
        if link[1] == child:
            problems.append(f"{child} cannot inherit a person from itself")
        # one join per child: both columns must come from the same parent
        if child in profile.inherited_time and \
                profile.inherited_time[child] != link:
            problems.append(f"{child} inherits time and person from "
                            "different parents")
    if not profile.lenses or any(lens not in ("research", "operational")
                                 for lens in profile.lenses):
        problems.append(f"lenses {profile.lenses!r} must be research and/or "
                        "operational")
    if profile.coding not in ("snomed", "omop", "none"):
        problems.append(f"coding {profile.coding!r} is not snomed, omop or none")
    if profile.qc_role is not None and profile.qc_role not in profile.pipelines:
        problems.append(f"qc_role {profile.qc_role!r} names no pipeline")
    for role, pid in profile.pipelines.items():
        if not (_SAFE_ID.fullmatch(role) and _SAFE_ID.fullmatch(pid)):
            problems.append(f"unsafe pipeline {role}={pid}")
    if profile.search_index is not None and not all(
            _SAFE_NAME.fullmatch(part) for part in profile.search_index):
        problems.append(f"unsafe search index {profile.search_index}")
    return problems


def targets_for(profile: DatasetProfile, env: str) -> dict:
    """Where one dataset publishes: dev keeps every dataset in its own
    `<id>_browser*` schemas in 8_dev (gold's are the existing ones); prod puts
    each dataset's views in d_databrowser.<id> and shares the lookup, meta
    and app ledgers, which key every row on `dataset`."""
    if env == "dev":
        base = f"{profile.id}_browser"
        return {"catalog": "8_dev", "schema": base, "lookup": f"{base}_lookup",
                "meta": f"{base}_meta", "app": f"{base}_app"}
    assert env == "prod", f"unknown env {env!r}"
    return {"catalog": "d_databrowser", "schema": profile.id,
            "lookup": "lookup", "meta": "meta", "app": "app"}


# ---------------------------------------------------------------------------
# Targeted republish: rebuild some tables of the current verified release.
# ---------------------------------------------------------------------------

def _table_of(ref: str) -> str:
    """'table.column' -> 'table'."""
    return ref.split(".", 1)[0]


def check_parent_is_current(parent_run_id: str, *, newest: str | None,
                            verified: bool) -> None:
    """A targeted republish builds only on the dataset's current release:
    its newest registry row, with an ok verify receipt. An older verified
    release would carry stale metadata forward over newer live views."""
    assert re.fullmatch(r"[0-9a-fA-F]{32}", parent_run_id or ""), \
        f"invalid parent_run_id {parent_run_id!r}"
    assert parent_run_id == newest, (
        f"{parent_run_id} is not the current release (newest registry row "
        f"is {newest}); republish on the current release")
    assert verified, f"release {parent_run_id} has no ok verify receipt"


def parent_format_problems(parent_doc: dict, *, profile_rows: int) -> list:
    """Why a release cannot be the parent of a targeted republish, in
    words; [] when it can. The carried tables keep the parent's schema doc
    and profiles, so the parent must have been published by the current
    publish: every column carries its IG scores, and its column profiles
    exist. An older release needs one full publish first."""
    problems = []
    unscored = sum(1 for table in parent_doc["tables"].values()
                   for column in table["columns"]
                   if column.get("ig_risk_score") is None
                   or column.get("ig_severity_score") is None)
    if unscored:
        problems.append(f"{unscored} columns in the parent's schema doc carry "
                        "no IG scores (published before IG scores were "
                        "recorded); run a full publish first")
    if not profile_rows:
        problems.append("the parent has no column profiles (published before "
                        "the profile stage); run a full publish first")
    return problems

def merge_release(parent_doc: dict, fresh_doc: dict, changed: list,
                  fresh_relationships: list) -> dict:
    """The release a targeted republish publishes: the parent's schema doc
    with the `changed` tables replaced by the fresh build (or removed, if a
    changed table now waits for IG tags). Joins touching a changed table are
    the freshly probed ones; all others are the parent's. Join keys are
    re-checked across the whole release, as build_schema_doc does: a keyless
    target refuses the release, a keyless source loses its joins."""
    changed = sorted(set(changed))
    doc = json.loads(json.dumps(parent_doc))
    for name in changed:
        if name in fresh_doc["tables"]:
            doc["tables"][name] = fresh_doc["tables"][name]
            doc["sources"][name] = fresh_doc["sources"][name]
        else:
            doc["tables"].pop(name, None)
            doc["sources"].pop(name, None)
    doc["manifest"] = sorted(doc["tables"])
    waiting = {t: c for t, c in doc.get("waiting_for_tags", {}).items()
               if t not in changed}
    waiting.update(fresh_doc.get("waiting_for_tags", {}))
    doc["waiting_for_tags"] = dict(sorted(waiting.items()))

    def touched(rel):
        return _table_of(rel["from"]) in changed or _table_of(rel["to"]) in changed

    def present(rel):
        return (_table_of(rel["from"]) in doc["tables"]
                and _table_of(rel["to"]) in doc["tables"])

    candidates = ([r for r in doc["relationships"] if not touched(r)]
                  + [r for r in fresh_relationships if touched(r)])
    candidates = [r for r in candidates if present(r)]
    candidates, _link_only = _split_parent_links(candidates, doc["tables"])
    keyed = {name for name, table in doc["tables"].items()
             if any(column.get("pk") for column in table["columns"])}
    keyless_targets = sorted({_table_of(r["to"]) for r in candidates} - keyed)
    assert not keyless_targets, (
        f"join targets without a primary key: {keyless_targets}")
    dropped = [r for r in candidates if _table_of(r["from"]) not in keyed]
    bare = lambda r: {k: v for k, v in r.items() if k != "parent"}  # noqa: E731
    doc["relationships"] = [bare(r) for r in candidates if r not in dropped]
    doc["dropped_relationships"] = (
        [r for r in doc.get("dropped_relationships", []) if not touched(r)]
        + [dict(bare(r), reason="source has no unique key") for r in dropped])
    # parent links: the parent's for untouched tables, the fresh probe's for
    # changed ones (a keyless child keeps its link, as build_schema_doc does)
    links = [link for link in doc.get("parent_links", [])
             if link["child"] not in changed and link["parent"] not in changed
             and link["child"] in doc["tables"]
             and link["parent"] in doc["tables"]]
    links += [_parent_link(r) for r in fresh_relationships
              if r.get("parent") and touched(r) and present(r)]
    doc["parent_links"] = sorted(links, key=lambda link: link["child"])
    doc["dataset"] = {**doc["dataset"],
                      "run_id": fresh_doc["dataset"]["run_id"]}
    doc["parent_run_id"] = parent_doc["dataset"]["run_id"]
    doc["changed_tables"] = changed
    return doc


def release_exits(changed: list, doc: dict) -> list:
    """Changed tables the merged release no longer lists (they now wait for
    tags): their live views must be dropped, or they stay queryable and
    verify's no_stale_live_views fails."""
    return sorted(set(changed) - set(doc["manifest"]))


def doc_columns(doc: dict) -> dict:
    """{table: [{"name": column}]} for a release's published columns: the
    shape candidate_relationships reads, for the tables a targeted
    republish does not rebuild."""
    return {name: [{"name": column["name"]} for column in table["columns"]
                   if column.get("type") != "excluded"]
            for name, table in doc["tables"].items()}


def carry_forward_sql(*, table: str, columns: list, dataset: str,
                      parent_run_id: str, run_id: str,
                      exclude_tables: list) -> str:
    """Copy a ledger's rows for the parent release to the new run_id,
    except the rebuilt tables' (which the republish writes fresh), so the
    new release's stats and profiles cover the whole dataset."""
    assert _SAFE_ID.fullmatch(table) and _SAFE_NAME.fullmatch(dataset)
    assert _SAFE_ID.fullmatch(parent_run_id) and _SAFE_ID.fullmatch(run_id)
    assert all(_SAFE_NAME.fullmatch(c) for c in columns)
    assert all(_SAFE_NAME.fullmatch(t) for t in exclude_tables)
    names = ", ".join(qi(c) for c in columns)
    select = ", ".join(f"'{run_id}'" if c == "run_id" else qi(c)
                       for c in columns)
    excluded = ", ".join(f"'{t}'" for t in exclude_tables) or "''"
    return (f"INSERT INTO {table} ({names})\n"
            f"SELECT {select}\n"
            f"FROM {table}\n"
            f"WHERE dataset = '{dataset}' AND run_id = '{parent_run_id}'\n"
            f"  AND table_name NOT IN ({excluded})")


_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def release_step(newest: dict | None, *, since: str) -> str:
    """What the all-datasets driver (browser_publish_all) does next for one
    dataset in prod.

    `newest` is the dataset's newest prod registry row with its latest verify
    status ({"run_id", "published_at", "verify_status": "ok" / "failed" /
    None}), or None when the dataset has never been published. `since` is the
    date (YYYY-MM-DD) from which a release counts as this round's.

      skip     a release from this round already verified ok
      verify   a release from this round is staged but not yet verified
      publish  anything else: no release, an older one, or a failed verify
    """
    assert _DATE.fullmatch(since), f"since must be YYYY-MM-DD, got {since!r}"
    if newest is None or str(newest["published_at"])[:10] < since:
        return "publish"
    if newest["verify_status"] == "ok":
        return "skip"
    if newest["verify_status"] is None:
        return "verify"
    return "publish"


def verified_release_envelope(dataset: str, run_id: str,
                              schema_doc_json: str) -> dict:
    """The offline export of a verified release that uc_model_gen,
    catalogue_gen, estate_enrich and omop_vocab_pull read with
    --verified-release. Only built for a release whose verify receipt is ok,
    so it records that status; the doc must name the same dataset and run."""
    doc = json.loads(schema_doc_json)
    assert doc.get("dataset", {}).get("name") == dataset, (
        f"schema_doc is for {doc.get('dataset', {}).get('name')!r}, "
        f"not {dataset!r}")
    assert doc.get("dataset", {}).get("run_id") == run_id, (
        "schema_doc run_id does not match the registry row")
    return {"dataset": dataset, "run_id": run_id, "verify_status": "ok",
            "schema_doc": doc}

