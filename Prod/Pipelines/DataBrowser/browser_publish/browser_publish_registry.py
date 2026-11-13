# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# BrowserPublish / registry — staged publish orchestrator.
#
# Order is the safety property: derive the column policy and stage views,
# collect stats,
# probe relationships, assemble schema_doc, promote, verify grants, then write
# the registry row and a publish/staged receipt. A separate notebook certifies
# the live release with a verify/ok receipt.

# COMMAND ----------

# MAGIC %run ./browser_publish_config

# COMMAND ----------

# MAGIC %run ./browser_publish_views

# COMMAND ----------

# MAGIC %run ./browser_publish_stats

# COMMAND ----------

# MAGIC %run ./browser_publish_profile

# COMMAND ----------

# MAGIC %run ./browser_publish_exports

# COMMAND ----------

import hashlib
import json
import re
import traceback
from datetime import datetime, timezone

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"])
dbutils.widgets.dropdown("registry_action", "none",
                         ["none", "publish", "resume", "republish"])
dbutils.widgets.text("tables", "", "republish: comma-separated tables")
dbutils.widgets.text("parent_run_id", "", "republish: the current verified release")
dbutils.widgets.text("resume_run_id", "", "failed run with complete stats")

ENV = dbutils.widgets.get("env")
ACTION = dbutils.widgets.get("registry_action")
RESUME_RUN_ID = dbutils.widgets.get("resume_run_id")
TABLES = dbutils.widgets.get("tables")
PARENT_RUN_ID = dbutils.widgets.get("parent_run_id")

dbutils.widgets.dropdown("dataset", "gold", sorted(PROFILES))
DATASET = dbutils.widgets.get("dataset")
PROFILE = PROFILES[DATASET]
_TARGETS = targets_for(PROFILE, ENV)
TARGET_CATALOG, TARGET_SCHEMA = _TARGETS["catalog"], _TARGETS["schema"]
TARGET_LOOKUP_SCHEMA = _TARGETS["lookup"]
META_SCHEMA = _TARGETS["meta"]
APP_SCHEMA = _TARGETS["app"]
BROWSER_SP_ID = "ffd02b90-a312-47d1-8029-8d3b9bc50324"
EXPECTED_OWNER = "8c733111-4a66-4207-833c-f0ec87c2f910"
TAG = (f"/* dbrowse ctx=system u={DATASET}-publish "
       f"feature={DATASET}-registry */ ")

# Keep the existing shared metadata schemas. `status` is appended to the
# existing receipt contract so older explicit-column writers remain valid.
REGISTRY_DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{META_SCHEMA}.dataset_registry (
    dataset        STRING,
    run_id         STRING,
    published_at   STRING,
    basis          STRING,
    profile_sha256 STRING,
    table_count    INT,
    column_count   INT,
    schema_doc     STRING,
    comparisons    STRING
) USING DELTA
"""

RECEIPT_DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{APP_SCHEMA}.run_receipt (
    dataset STRING,
    run_id  STRING,
    run_at  STRING,
    kind    STRING,
    receipt STRING,
    status  STRING
) USING DELTA
"""


def ensure_ledgers():
    spark.sql(TAG + f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{META_SCHEMA}")
    spark.sql(TAG + f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{APP_SCHEMA}")
    spark.sql(TAG + REGISTRY_DDL)
    spark.sql(TAG + RECEIPT_DDL)
    columns = {row["column_name"] for row in spark.sql(TAG + f"""
        SELECT column_name
        FROM {TARGET_CATALOG}.information_schema.columns
        WHERE table_schema = '{APP_SCHEMA}' AND table_name = 'run_receipt'
    """).collect()}
    if "status" not in columns:
        spark.sql(TAG + f"ALTER TABLE {TARGET_CATALOG}.{APP_SCHEMA}.run_receipt "
                  "ADD COLUMNS (status STRING)")


def probe_relationships(objects, cols_meta, schema=None, candidates=None,
                        schema_of=None):
    """Probe naming-convention joins (or the given candidates) for orphan
    rates, one scan per hub. schema_of reads named tables from another
    schema (a targeted republish's staged tables)."""
    if candidates is None:
        candidates = candidate_relationships(cols_meta, PROFILE)
    rates = {}
    groups = {}
    for rel in candidates:
        groups.setdefault(rel["to"], []).append(rel)
    completed = 0
    for target, group in sorted(groups.items()):
        rows = spark.sql(TAG + orphan_probe_group_sql(
            catalog=TARGET_CATALOG, schema=schema or TARGET_SCHEMA,
            relationships=group, schema_of=schema_of)).collect()
        for row in rows:
            n = row["n"] or 0
            rates[row["relationship_name"]] = (
                row["orphans"] / n) if n else None
        completed += len(group)
        print(f"relationship probes: {completed}/{len(candidates)} "
              f"({target})")
    # a declared parent link also needs each parent key to belong to one
    # person (where the parent is person-keyed), or the cohort filter could
    # admit another person's child rows through a shared key
    shared = {}
    for rel in candidates:
        if not rel.get("parent") or (rates.get(rel["name"]) is None):
            continue
        parent = rel["to"].split(".", 1)[0]
        if "person_id" not in {c["name"] for c in cols_meta.get(parent, [])}:
            continue
        row = spark.sql(TAG + parent_person_probe_sql(
            catalog=TARGET_CATALOG,
            schema=(schema_of or {}).get(parent, schema or TARGET_SCHEMA),
            rel=rel)).collect()[0]
        shared[rel["name"]] = int(row["multi"] or 0)
    print(f"parent links checked for shared keys: {len(shared)}")
    kept, rejected = accepted_relationships(
        candidates, rates, shared_parent_keys=shared,
        non_null_coverage=non_null_coverage(candidates, cols_meta))
    print(f"relationships: kept {len(kept)}, rejected {len(rejected)}")
    for name, rate in rejected:
        print(f"  rejected {name}: orphan_rate={rate}")
    return kept, rejected


def probe_declared_edges(objects, cols_meta, schema, live_keys=None):
    """INSR_W7 W7.04: measure each declared edge whose two tables are in this
    release (one exact aggregate per edge) and sort them into Cube joins,
    documented edges and rejections. A join whose parent column is not the
    parent's row key is documented instead (Cube needs the target's primary
    key). Tables built by this run are read from the staging `schema`.
    INSR W7 review: a targeted republish passes `live_keys` ({table it did
    not rebuild: that table's row key in the parent release}); an edge from
    a rebuilt table to a live one is measured against the live schema, so it
    stays in the release instead of silently leaving it."""
    live_keys = live_keys or {}
    edges = declared_edges_to_measure(PROFILE.declared_edges, set(objects),
                                      set(live_keys))
    schema_of = {name: schema for name in objects}
    coverage = {e["name"]: spark.sql(TAG + edge_coverage_sql(
        catalog=TARGET_CATALOG, schema=TARGET_SCHEMA, edge=e,
        schema_of=schema_of)).first().asDict()
                for e in edges}
    joins, documented, rejected = accept_declared_edges(edges, coverage)
    keyed_joins = []
    for rel in joins:
        parent, parent_column = rel["to"].split(".", 1)
        parent_key = (_pk_column(parent, cols_meta[parent], PROFILE)
                      if parent in cols_meta else live_keys.get(parent))
        if parent_key == parent_column:
            keyed_joins.append(rel)
        else:
            edge = next(e for e in edges if e["name"] == rel["name"])
            documented.append({**rel, "ref": edge["ref"],
                               "note": "parent column is not the row key"})
    print(f"declared edges: {len(keyed_joins)} joins, {len(documented)} "
          f"documented, {len(rejected)} rejected")
    return keyed_joins, documented, rejected


def prior_relationships(run_id, candidates):
    """Reuse a completed relationship phase from an earlier same-run failure."""
    rows = spark.sql(TAG + f"""
        SELECT receipt
        FROM {TARGET_CATALOG}.{APP_SCHEMA}.run_receipt
        WHERE dataset = '{DATASET}' AND run_id = '{run_id}'
          AND kind = 'publish' AND status = 'failed'
        ORDER BY run_at DESC
    """).collect()
    payloads = []
    for row in rows:
        try:
            payloads.append(json.loads(row["receipt"]))
        except (TypeError, ValueError):
            continue
    return reusable_relationship_result(candidates, payloads)


def prior_promotion_completed(run_id, object_count):
    """Whether this run already completed promotion before a later failure."""
    rows = spark.sql(TAG + f"""
        SELECT receipt
        FROM {TARGET_CATALOG}.{APP_SCHEMA}.run_receipt
        WHERE dataset = '{DATASET}' AND run_id = '{run_id}'
          AND kind = 'publish' AND status = 'failed'
        ORDER BY run_at DESC
    """).collect()
    payloads = []
    for row in rows:
        try:
            payloads.append(json.loads(row["receipt"]))
        except (TypeError, ValueError):
            continue
    return receipt_has_phase(
        payloads, "promote", objects=object_count)


def apply_and_verify_grants():
    """Apply both schema grants and verify each privilege by read-back."""
    schemas = (TARGET_SCHEMA, TARGET_LOOKUP_SCHEMA)
    for schema in schemas:
        spark.sql(TAG + f"GRANT USE SCHEMA ON SCHEMA "
                  f"{TARGET_CATALOG}.{schema} TO `{BROWSER_SP_ID}`")
        spark.sql(TAG + f"GRANT SELECT ON SCHEMA "
                  f"{TARGET_CATALOG}.{schema} TO `{BROWSER_SP_ID}`")
    names = ", ".join(f"'{schema}'" for schema in schemas)
    granted = spark.sql(TAG + f"""
        SELECT schema_name, privilege_type
        FROM {TARGET_CATALOG}.information_schema.schema_privileges
        WHERE grantee = '{BROWSER_SP_ID}' AND schema_name IN ({names})
    """).collect()
    privileges = {(row["schema_name"], row["privilege_type"])
                  for row in granted}
    # information_schema reports USE_SCHEMA, while GRANT uses USE SCHEMA.
    missing = missing_schema_privileges(schemas, privileges)
    assert not missing, f"grant read-back failed: missing {sorted(missing)}"
    print(f"grants verified for {BROWSER_SP_ID}: {sorted(privileges)}")


def hand_over_views():
    """Give every live view to EXPECTED_OWNER, then verify by read-back.

    Runs after tagging and grants, which need the runner to own the views.
    A no-op when the service principal itself publishes. A later publish by
    a person can still replace these views because replacing needs schema
    ownership (or MANAGE), not view ownership.
    """
    schemas = ", ".join(f"'{schema}'" for schema in
                        (TARGET_SCHEMA, TARGET_LOOKUP_SCHEMA))
    query = TAG + f"""
        SELECT table_schema, table_name, table_type, table_owner
        FROM {TARGET_CATALOG}.information_schema.tables
        WHERE table_schema IN ({schemas}) AND table_type = 'VIEW'
    """
    statements = owner_handover_statements(
        [row.asDict() for row in spark.sql(query).collect()],
        TARGET_CATALOG, EXPECTED_OWNER)
    for statement in statements:
        spark.sql(TAG + statement)
    owners = {row["table_owner"] for row in spark.sql(query).collect()}
    assert owners <= {EXPECTED_OWNER}, (
        f"view hand-over read-back failed: owners={sorted(owners)}")
    print(f"handed {len(statements)} views to {EXPECTED_OWNER}")
    return len(statements)


_TYPE_MAP = {
    "BIGINT": "int64", "INT": "int64", "SMALLINT": "int64",
    "TINYINT": "int64", "LONG": "int64", "INTEGER": "int64",
    "DOUBLE": "float64", "FLOAT": "float64", "REAL": "float64",
    "STRING": "string", "VARCHAR": "string", "CHAR": "string",
    "BINARY": "string", "VARIANT": "string",
    "BOOLEAN": "bool",
    "TIMESTAMP": "timestamp", "TIMESTAMP_NTZ": "timestamp",
    "DATE": "date",
}


def normalise_type(full_data_type: str) -> str:
    """Normalise Databricks types to package_gen's six-value vocabulary."""
    base = full_data_type.upper().split("(")[0].strip()
    if base.startswith("DECIMAL") or base.startswith("NUMERIC"):
        return "float64"
    normalised = _TYPE_MAP.get(base)
    assert normalised, (
        f"unmapped Databricks type {full_data_type!r} — add it to _TYPE_MAP, "
        "or exclude the column in the policy. package_gen only understands "
        f"{sorted(set(_TYPE_MAP.values()))}.")
    return normalised


def collect_column_meta(objects, run_id, schema):
    """Collect view metadata plus cardinality parsed from stats JSON."""
    names = ", ".join(f"'{object_name}'" for object_name in objects)
    rows = spark.sql(TAG + f"""
        SELECT c.table_name, c.column_name, c.full_data_type, c.comment,
               c.is_nullable,
               try_cast(get_json_object(s.stats, '$.distinct_count') AS BIGINT)
                   AS distinct_count,
               try_cast(get_json_object(s.stats, '$.null_count') AS BIGINT)
                   AS null_count,
               try_cast(get_json_object(s.stats, '$.profiled_rows') AS BIGINT)
                   AS profiled_rows,
               c.ordinal_position
        FROM {TARGET_CATALOG}.information_schema.columns c
        LEFT JOIN {TARGET_CATALOG}.{META_SCHEMA}.column_stats s
               ON s.table_name = c.table_name
              AND s.column_name = c.column_name
              AND s.dataset = '{DATASET}'
              AND s.run_id = '{run_id}'
        WHERE c.table_schema = '{schema}'
          AND c.table_name IN ({names})
        ORDER BY c.table_name, c.ordinal_position
    """).collect()
    out = {}
    for row in rows:
        out.setdefault(row.table_name, []).append({
            "name": row.column_name,
            "type": normalise_type(row.full_data_type),
            "comment": row.comment or "",
            "nullable": row.is_nullable == "YES",
            "distinct_count": row.distinct_count,
            "null_count": row.null_count,
            "profiled_rows": row.profiled_rows,
        })
    missing = sorted(set(objects) - set(out))
    assert not missing, f"column metadata missing for {missing}"
    return out


def confirm_unique_keys(objects, cols_meta, row_counts, schema):
    """Exact-check key candidates; the result overrides the estimate. Tables
    over the size limit are checked only when the profile declares their key
    (OMOP measurement's 1.68bn rows: one count-distinct per publish)."""
    checked = 0
    for object_name in objects:
        if (row_counts[object_name] >= EXACT_KEY_CHECK_MAX_ROWS
                and object_name not in PROFILE.declared_keys):
            continue
        statement = key_check_sql(
            f"{TARGET_CATALOG}.{schema}.`{object_name}`", object_name,
            cols_meta[object_name], PROFILE)
        if statement is None:
            continue
        result = spark.sql(TAG + statement).first().asDict()
        for meta in cols_meta[object_name]:
            if meta["name"] in result:
                meta["unique_exact"] = bool(result[meta["name"]])
        checked += 1
    return checked




def collect_descriptions(objects):
    names = ", ".join(f"'{object_name}'" for object_name in objects)
    rows = spark.sql(TAG + f"""
        SELECT table_name, comment
        FROM {SOURCE_CATALOG}.information_schema.tables
        WHERE table_schema = '{SOURCE_SCHEMA}'
          AND table_name IN ({names})
    """).collect()
    out = {}
    for row in rows:
        value = (row.comment or "").strip()
        cut = (value.find(PROFILE.qc_boilerplate)
               if PROFILE.qc_boilerplate else -1)
        out[row.table_name] = value[:cut].strip() if cut >= 0 else value
    return out


def add_coverage(doc, run_id, schema_of):
    """Measure every time-dimensioned table's coverage (table_coverage) and
    record it, with the release boundary, in the schema doc. schema_of(name)
    is the schema the table's view is read from (staging or live)."""
    lookup = f"{TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}"
    pipeline_rows = [row.asDict() for row in spark.sql(
        TAG + f"SELECT role, update_end FROM {lookup}.pipeline_config "
        f"WHERE run_id = '{run_id}'").collect()] if PROFILE.pipelines else []
    now = spark.sql(TAG + "SELECT CAST(current_timestamp() AS STRING) AS t"
                    ).first()["t"]
    boundary = release_boundary(pipeline_rows, PROFILE, now)
    doc["dataset"]["release_boundary"] = boundary
    measured = complete = 0
    for name, table in sorted(doc["tables"].items()):
        column = table.get("time_dimension")
        table.pop("coverage", None)
        if not column:
            continue
        relation = f"{TARGET_CATALOG}.{schema_of(name)}.{qi(name)}"
        monthly = {row["month"]: row["n"] for row in spark.sql(
            TAG + monthly_counts_sql(relation, column)).collect()}
        table["coverage"] = dict(table_coverage(monthly, boundary),
                                 time_column=column)
        measured += 1
        complete += table["coverage"]["complete_through"] is not None
    print(f"coverage: {measured} tables measured against {boundary}, "
          f"{complete} with a complete-through month")
    return {"phase": "coverage", "boundary": boundary, "tables": measured,
            "complete_through": complete}


def write_registry_row(run_id, doc, policy_digest=None):
    """The release's registry row. A targeted republish passes the digest of
    its whole release's policy (POLICY holds only the rebuilt tables')."""
    column_count = sum(len(table["columns"])
                       for table in doc["tables"].values())
    now = datetime.now(timezone.utc).isoformat()
    spark.sql(TAG + f"DELETE FROM {TARGET_CATALOG}.{META_SCHEMA}.dataset_registry "
              f"WHERE dataset = '{DATASET}' AND run_id = '{run_id}'")
    frame = spark.createDataFrame(
        [(DATASET, run_id, now, f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}",
          policy_digest or hashlib.sha256(
              json.dumps(POLICY, sort_keys=True).encode()).hexdigest(),
          len(doc["tables"]), column_count,
          json.dumps(doc, sort_keys=True), None)],
        "dataset string, run_id string, published_at string, basis string, "
        "profile_sha256 string, table_count int, column_count int, "
        "schema_doc string, comparisons string")
    frame.write.mode("append").saveAsTable(
        f"{TARGET_CATALOG}.{META_SCHEMA}.dataset_registry")


def write_receipt(run_id, kind, status, payload):
    now = datetime.now(timezone.utc).isoformat()
    frame = spark.createDataFrame(
        [(DATASET, run_id, now, kind, json.dumps(payload), status)],
        "dataset string, run_id string, run_at string, kind string, "
        "receipt string, status string")
    frame.write.mode("append").saveAsTable(
        f"{TARGET_CATALOG}.{APP_SCHEMA}.run_receipt")


def publish():
    """Stage, validate, promote, and publish an uncertified release."""
    ensure_ledgers()
    run_id = spark.sql(
        TAG + "SELECT replace(uuid(), '-', '') AS run_id"
    ).first()["run_id"]
    schema = staging_schema(run_id)
    print(f"run_id={run_id} env={ENV} staging={schema}")
    receipt = {"run_id": run_id, "env": ENV, "staging_schema": schema,
               "phases": []}
    try:
        objects, cols, column_types, plans = build(schema, tag_columns=False)
        receipt["phases"].append({
            "phase": "views", "objects": len(objects),
            "active_filtered": sum(plan["where"] is not None
                                   for plan in plans.values()),
            "excluded_unreadable": list(EXCLUDED_UNREADABLE),
            "excluded_empty": list(EXCLUDED_EMPTY),
            "waiting_for_tags": dict(WAITING_FOR_TAGS)})

        stats = build_stats(objects, run_id, schema)
        receipt["phases"].append({"phase": "stats", "rows": len(stats)})
        row_counts = row_counts_from_stats(stats, objects)
        profiled = build_profiles(
            objects, run_id, schema=schema, column_types=column_types,
            policy=POLICY, rows=row_counts,
            lookup=f"{TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}", tag=TAG,
            dataset=DATASET)
        receipt["phases"].append({"phase": "profile", "columns": profiled})
        exported = run_exports(
            run_id, lookup=f"{TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}", tag=TAG,
            profile=PROFILE)
        receipt["phases"].append({"phase": "exports", **exported})

        cols_meta = collect_column_meta(objects, run_id, schema)
        receipt["phases"].append({
            "phase": "key_check",
            "tables": confirm_unique_keys(objects, cols_meta, row_counts,
                                          schema)})
        candidates = candidate_relationships(cols_meta, PROFILE)
        prior = prior_relationships(run_id, candidates)
        if prior is None:
            rels, rejected = probe_relationships(objects, cols_meta, schema)
            relationships_reused = False
        else:
            rels, rejected = prior
            relationships_reused = True
            print(f"relationships: reused {len(rels)} kept and "
                  f"{len(rejected)} rejected from an earlier run receipt")
        receipt["phases"].append({
            "phase": "relationships", "kept": len(rels),
            "rejected": [item[0] for item in rejected],
            "reused": relationships_reused})
        # INSR_W7 W7.04: declared edges, measured on this run's staged views
        declared_joins, declared_doc, declared_rejected = probe_declared_edges(
            objects, cols_meta, schema)
        rels = rels + declared_joins
        receipt["phases"].append({
            "phase": "declared_edges", "joins": len(declared_joins),
            "documented": len(declared_doc), "rejected": declared_rejected})

        doc = build_schema_doc(
            dataset=DATASET, run_id=run_id, objects=objects,
            cols_meta=cols_meta,
            row_counts=row_counts,
            descriptions=collect_descriptions(objects),
            policy=POLICY, relationships=rels, profile=PROFILE,
            filtered_tables=sorted(o for o, plan in plans.items()
                                   if plan.get("where")))
        doc["manifest"] = sorted(objects)
        doc["sources"] = plans
        doc["excluded_unreadable"] = list(EXCLUDED_UNREADABLE)
        doc["excluded_empty"] = list(EXCLUDED_EMPTY)
        doc["waiting_for_tags"] = dict(WAITING_FOR_TAGS)
        doc["declared_relationships"] = declared_doc
        doc["rejected_declared_relationships"] = declared_rejected
        receipt["phases"].append(add_coverage(doc, run_id, lambda _: schema))

        promotion_reused = prior_promotion_completed(run_id, len(objects))
        if promotion_reused:
            print("live promotion already completed for this run; reusing it")
        else:
            promote(objects, cols, column_types, plans, tag_columns=False)
        if tags_match_policy(objects, cols, TARGET_SCHEMA):
            print("live IG tags already match policy; reusing them")
        else:
            apply_tags(objects, cols, TARGET_SCHEMA)
        receipt["phases"].append({"phase": "promote",
                                  "objects": len(objects),
                                  "reused": promotion_reused})

        apply_and_verify_grants()
        receipt["phases"].append({"phase": "grants", "ok": True})
        receipt["phases"].append({"phase": "owner",
                                  "transferred": hand_over_views()})

        write_registry_row(run_id, doc)
        receipt["phases"].append({"phase": "registry",
                                  "tables": len(doc["tables"])})
        write_receipt(run_id, "publish", "staged", receipt)
        drop_staging(schema)
        print(f"STAGED {DATASET} run {run_id}: {len(objects)} objects — "
              f"now run browser_publish_verify with dataset={DATASET} "
              f"run_id={run_id}")
        return run_id
    except Exception:
        error = traceback.format_exc()
        receipt["error"] = (error if len(error) <= 8000
                            else error[:4000] + "\n...\n" + error[-4000:])
        write_receipt(run_id, "publish", "failed", receipt)
        raise


def resume_after_stats(run_id):
    """Continue a failed dev run without repeating its expensive profiles."""
    assert ENV == "dev", "resume is intentionally limited to 8_dev"
    assert re.fullmatch(r"[0-9a-fA-F]{32}", run_id), \
        f"invalid resume_run_id {run_id!r}"
    schema = staging_schema(run_id)
    receipt = {"run_id": run_id, "env": ENV, "staging_schema": schema,
               "resumed": True, "phases": []}
    try:
        # Recreate the views (re-deriving the policy) before trusting stats
        # produced by an earlier attempt.
        objects, cols, column_types, plans = build(schema, tag_columns=False)
        receipt["phases"].append({
            "phase": "views", "waiting_for_tags": dict(WAITING_FOR_TAGS)})
        expected = {(object_name, column)
                    for object_name in objects
                    for column in cols[object_name]
                    if column not in INTERNAL_COLUMNS
                    and POLICY[policy_key(object_name, column)]["action"]
                    != "describe"}
        rows = spark.sql(TAG + f"""
            SELECT table_name, column_name, source_row_count_rounded
            FROM {TARGET_CATALOG}.{META_SCHEMA}.column_stats
            WHERE dataset = '{DATASET}' AND run_id = '{run_id}'
        """).collect()
        observed = {(row["table_name"], row["column_name"]) for row in rows}
        assert observed == expected, (
            "cannot resume: column_stats coverage differs from the current "
            f"published policy; missing={sorted(expected - observed)[:20]} "
            f"extra={sorted(observed - expected)[:20]}")
        receipt["phases"].append({"phase": "stats", "rows": len(observed),
                                  "reused": True})
        row_counts = row_counts_from_stats(rows, objects)
        profiled = build_profiles(
            objects, run_id, schema=schema, column_types=column_types,
            policy=POLICY, rows=row_counts,
            lookup=f"{TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}", tag=TAG,
            dataset=DATASET)
        receipt["phases"].append({"phase": "profile", "columns": profiled})
        exported = run_exports(
            run_id, lookup=f"{TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}", tag=TAG,
            profile=PROFILE)
        receipt["phases"].append({"phase": "exports", **exported})

        cols_meta = collect_column_meta(objects, run_id, schema)
        receipt["phases"].append({
            "phase": "key_check",
            "tables": confirm_unique_keys(objects, cols_meta, row_counts,
                                          schema)})
        candidates = candidate_relationships(cols_meta, PROFILE)
        prior = prior_relationships(run_id, candidates)
        if prior is None:
            rels, rejected = probe_relationships(objects, cols_meta, schema)
            relationships_reused = False
        else:
            rels, rejected = prior
            relationships_reused = True
            print(f"relationships: reused {len(rels)} kept and "
                  f"{len(rejected)} rejected from an earlier run receipt")
        receipt["phases"].append({
            "phase": "relationships", "kept": len(rels),
            "rejected": [item[0] for item in rejected],
            "reused": relationships_reused})
        # INSR_W7 W7.04: declared edges, measured on this run's staged views
        declared_joins, declared_doc, declared_rejected = probe_declared_edges(
            objects, cols_meta, schema)
        rels = rels + declared_joins
        receipt["phases"].append({
            "phase": "declared_edges", "joins": len(declared_joins),
            "documented": len(declared_doc), "rejected": declared_rejected})

        doc = build_schema_doc(
            dataset=DATASET, run_id=run_id, objects=objects,
            cols_meta=cols_meta,
            row_counts=row_counts,
            descriptions=collect_descriptions(objects),
            policy=POLICY, relationships=rels, profile=PROFILE,
            filtered_tables=sorted(o for o, plan in plans.items()
                                   if plan.get("where")))
        doc["manifest"] = sorted(objects)
        doc["sources"] = plans
        doc["excluded_unreadable"] = list(EXCLUDED_UNREADABLE)
        doc["excluded_empty"] = list(EXCLUDED_EMPTY)
        doc["waiting_for_tags"] = dict(WAITING_FOR_TAGS)
        doc["declared_relationships"] = declared_doc
        doc["rejected_declared_relationships"] = declared_rejected
        receipt["phases"].append(add_coverage(doc, run_id, lambda _: schema))

        promotion_reused = prior_promotion_completed(run_id, len(objects))
        if promotion_reused:
            print("live promotion already completed for this run; reusing it")
        else:
            promote(objects, cols, column_types, plans, tag_columns=False)
        if tags_match_policy(objects, cols, TARGET_SCHEMA):
            print("live IG tags already match policy; reusing them")
        else:
            apply_tags(objects, cols, TARGET_SCHEMA)
        receipt["phases"].append({"phase": "promote",
                                  "objects": len(objects),
                                  "reused": promotion_reused})
        apply_and_verify_grants()
        receipt["phases"].append({"phase": "grants", "ok": True})
        receipt["phases"].append({"phase": "owner",
                                  "transferred": hand_over_views()})
        write_registry_row(run_id, doc)
        receipt["phases"].append({"phase": "registry",
                                  "tables": len(doc["tables"])})
        write_receipt(run_id, "publish", "staged", receipt)
        drop_staging(schema)
        print(f"RESUMED {DATASET} run {run_id}: {len(objects)} objects — "
              f"now run browser_publish_verify with dataset={DATASET} "
              "and the same run_id")
        return run_id
    except Exception:
        error = traceback.format_exc()
        receipt["error"] = (error if len(error) <= 8000
                            else error[:4000] + "\n...\n" + error[-4000:])
        write_receipt(run_id, "publish", "failed", receipt)
        raise


def current_release():
    """(run_id, profile_sha256, verified) of the dataset's newest registry
    row — the release the live views are — or (None, None, False)."""
    row = spark.sql(TAG + f"""
        SELECT run_id, profile_sha256
        FROM {TARGET_CATALOG}.{META_SCHEMA}.dataset_registry
        WHERE dataset = '{DATASET}'
        ORDER BY published_at DESC LIMIT 1
    """).first()
    if row is None:
        return None, None, False
    verify = spark.sql(TAG + f"""
        SELECT status FROM {TARGET_CATALOG}.{APP_SCHEMA}.run_receipt
        WHERE dataset = '{DATASET}' AND run_id = '{row["run_id"]}'
          AND kind = 'verify'
        ORDER BY run_at DESC LIMIT 1
    """).first()
    return (row["run_id"], row["profile_sha256"],
            verify is not None and verify["status"] == "ok")


def verified_parent_doc(parent_run_id):
    """The schema doc and policy digest of the dataset's current verified
    release, refusing any other parent."""
    newest, digest, verified = current_release()
    check_parent_is_current(parent_run_id, newest=newest, verified=verified)
    row = spark.sql(TAG + f"""
        SELECT schema_doc FROM {TARGET_CATALOG}.{META_SCHEMA}.dataset_registry
        WHERE dataset = '{DATASET}' AND run_id = '{parent_run_id}'
    """).first()
    doc = json.loads(row["schema_doc"])
    profile_rows = spark.sql(TAG + f"""
        SELECT COUNT(*) AS n
        FROM {TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}.column_profile
        WHERE dataset = '{DATASET}' AND run_id = '{parent_run_id}'
    """).first()["n"] if spark.catalog.tableExists(
        f"{TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}.column_profile") else 0
    problems = parent_format_problems(doc, profile_rows=profile_rows)
    assert not problems, (f"{DATASET} release {parent_run_id} cannot be "
                          "carried forward:\n  " + "\n  ".join(problems))
    return doc, digest


def carry_forward(parent_run_id, run_id, changed):
    """Copy the parent's stats and profiles for every table not rebuilt to
    the new run_id (idempotent: a retry deletes its own copies first)."""
    copied = {}
    for schema, name in ((META_SCHEMA, "column_stats"),
                         (TARGET_LOOKUP_SCHEMA, "column_profile")):
        table = f"{TARGET_CATALOG}.{schema}.{name}"
        columns = [row["column_name"] for row in spark.sql(TAG + f"""
            SELECT column_name FROM {TARGET_CATALOG}.information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{name}'
            ORDER BY ordinal_position
        """).collect()]
        excluded = ", ".join(f"'{t}'" for t in changed)
        spark.sql(TAG + f"DELETE FROM {table} WHERE dataset = '{DATASET}' "
                  f"AND run_id = '{run_id}' AND table_name NOT IN ({excluded})")
        spark.sql(TAG + carry_forward_sql(
            table=table, columns=columns, dataset=DATASET,
            parent_run_id=parent_run_id, run_id=run_id,
            exclude_tables=changed))
        copied[name] = spark.sql(TAG + f"SELECT COUNT(*) AS n FROM {table} "
                                 f"WHERE dataset = '{DATASET}' AND run_id = "
                                 f"'{run_id}'").first()["n"]
    return copied


def _touches(rel, tables):
    """True when a relationship starts or ends at one of these tables."""
    return rel["from"].split(".", 1)[0] in tables or \
        rel["to"].split(".", 1)[0] in tables


def republish(tables_csv, parent_run_id):
    """Rebuild the named tables of the current verified release, validate
    the merged release with nothing live changed, then go live with only
    those tables. See Task 7's preamble for why the order matters."""
    changed = sorted({t.strip() for t in tables_csv.split(",") if t.strip()})
    assert changed, "republish needs tables="
    parent, parent_digest = verified_parent_doc(parent_run_id)
    ensure_ledgers()
    run_id = spark.sql(
        TAG + "SELECT replace(uuid(), '-', '') AS run_id").first()["run_id"]
    schema = staging_schema(run_id)
    print(f"run_id={run_id} env={ENV} dataset={DATASET} "
          f"parent={parent_run_id} tables={changed}")
    receipt = {"run_id": run_id, "env": ENV, "staging_schema": schema,
               "parent_run_id": parent_run_id, "changed_tables": changed,
               "phases": []}
    try:
        # 1. build and profile the changed tables in staging only
        objects, cols, column_types, plans = build(
            schema, tag_columns=False, only=changed)
        receipt["phases"].append({
            "phase": "views", "objects": len(objects),
            "waiting_for_tags": dict(WAITING_FOR_TAGS)})
        stats = build_stats(objects, run_id, schema)
        row_counts = row_counts_from_stats(stats, objects)
        profiled = build_profiles(
            objects, run_id, schema=schema, column_types=column_types,
            policy=POLICY, rows=row_counts,
            lookup=f"{TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}", tag=TAG,
            dataset=DATASET)
        receipt["phases"].append({"phase": "profile", "columns": profiled,
                                  "carried": carry_forward(
                                      parent_run_id, run_id, changed)})
        exported = run_exports(
            run_id, lookup=f"{TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}",
            tag=TAG, profile=PROFILE)
        receipt["phases"].append({"phase": "exports", **exported})

        # 2. validate against staging (changed) + live (unchanged): keys,
        # joins, then the merged release's own key checks
        cols_meta = collect_column_meta(objects, run_id, schema)
        receipt["phases"].append({
            "phase": "key_check",
            "tables": confirm_unique_keys(objects, cols_meta, row_counts,
                                          schema)})
        everything = {**doc_columns(parent), **cols_meta}
        for name in changed:
            if name not in cols_meta:
                everything.pop(name, None)        # it now waits for tags
        candidates = [r for r in candidate_relationships(everything, PROFILE)
                      if _touches(r, objects)]
        rels, rejected = probe_relationships(
            objects, cols_meta, TARGET_SCHEMA, candidates,
            schema_of={name: schema for name in objects})
        receipt["phases"].append({
            "phase": "relationships", "kept": len(rels),
            "rejected": [item[0] for item in rejected]})
        # INSR_W7 W7.04 (+ review): declared edges with a rebuilt end, measured
        # on this run's staging views and, for the other end, the live tables
        # of the parent release; edges with no rebuilt end stay the parent's
        live_keys = {name: next((c["name"] for c in table["columns"]
                                 if c.get("pk")), None)
                     for name, table in parent["tables"].items()
                     if name not in changed}
        declared_joins, declared_doc, declared_rejected = probe_declared_edges(
            objects, cols_meta, schema, live_keys=live_keys)
        rels = rels + declared_joins
        receipt["phases"].append({
            "phase": "declared_edges", "joins": len(declared_joins),
            "documented": len(declared_doc), "rejected": declared_rejected})
        fresh = build_schema_doc(
            dataset=DATASET, run_id=run_id, objects=objects,
            cols_meta=cols_meta, row_counts=row_counts,
            descriptions=collect_descriptions(objects), policy=POLICY,
            relationships=[], profile=PROFILE,
            filtered_tables=sorted(o for o, plan in plans.items()
                                   if plan.get("where")))
        fresh["sources"] = plans
        fresh["waiting_for_tags"] = dict(WAITING_FOR_TAGS)
        doc = merge_release(parent, fresh, changed, rels)
        # INSR_W7 W7.04 (+ review): the parent's declared edges with a changed
        # end give way to this run's measurement; every other one stays
        doc["declared_relationships"], doc["rejected_declared_relationships"] = (
            merge_declared_entries(parent, changed, declared_doc,
                                   declared_rejected, PROFILE.declared_edges))
        exits = release_exits(changed, doc)
        # coverage is measured for the whole release against this run's
        # boundary: rebuilt tables from staging, the others from live
        receipt["phases"].append(add_coverage(
            doc, run_id, lambda name: schema if name in objects
            else TARGET_SCHEMA))

        # 3. still the current release? only now touch the live schema
        newest, _, verified = current_release()
        check_parent_is_current(parent_run_id, newest=newest,
                                verified=verified)
        promote(objects, cols, column_types, plans, tag_columns=False,
                drop_stale=False)
        dropped = drop_live_views(exits)
        apply_tags(objects, cols, TARGET_SCHEMA)
        receipt["phases"].append({"phase": "promote",
                                  "objects": len(objects),
                                  "left_release": dropped})
        apply_and_verify_grants()
        receipt["phases"].append({"phase": "grants", "ok": True})
        receipt["phases"].append({"phase": "owner",
                                  "transferred": hand_over_views()})

        # 4. the release record: the parent's policy digest chained with
        # the rebuilt tables' policy, since POLICY covers only those
        digest = hashlib.sha256(json.dumps(
            {"parent": parent_digest, "changed": POLICY},
            sort_keys=True).encode()).hexdigest()
        write_registry_row(run_id, doc, policy_digest=digest)
        receipt["phases"].append({"phase": "registry",
                                  "tables": len(doc["tables"])})
        write_receipt(run_id, "publish", "staged", receipt)
        drop_staging(schema)
        print(f"STAGED {DATASET} republish {run_id} of {changed} — now run "
              f"browser_publish_verify with dataset={DATASET} run_id={run_id}")
        return run_id
    except Exception:
        error = traceback.format_exc()
        receipt["error"] = (error if len(error) <= 8000
                            else error[:4000] + "\n...\n" + error[-4000:])
        write_receipt(run_id, "publish", "failed", receipt)
        raise


if ACTION == "publish":
    published_run_id = publish()
    dbutils.notebook.exit(json.dumps({"run_id": published_run_id}))
elif ACTION == "resume":
    resumed_run_id = resume_after_stats(RESUME_RUN_ID)
    dbutils.notebook.exit(json.dumps({"run_id": resumed_run_id,
                                     "resumed": True}))
elif ACTION == "republish":
    republished_run_id = republish(TABLES, PARENT_RUN_ID)
    dbutils.notebook.exit(json.dumps({"run_id": republished_run_id,
                                     "parent_run_id": PARENT_RUN_ID}))
