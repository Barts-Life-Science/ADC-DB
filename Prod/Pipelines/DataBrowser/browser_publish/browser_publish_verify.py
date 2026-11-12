# Databricks notebook source
# BrowserPublish / verify — independent acceptance gates for one live run.

# COMMAND ----------
# MAGIC %run ./browser_publish_registry

# COMMAND ----------
import json
import re

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"])
dbutils.widgets.text("run_id", "", "run_id")
dbutils.widgets.dropdown("verify_action", "none", ["none", "verify"])

ENV = dbutils.widgets.get("env")
RUN_ID = dbutils.widgets.get("run_id")
ACTION = dbutils.widgets.get("verify_action")

dbutils.widgets.dropdown("dataset", "gold", sorted(PROFILES))
DATASET = dbutils.widgets.get("dataset")
PROFILE = PROFILES[DATASET]
_TARGETS = targets_for(PROFILE, ENV)
TARGET_CATALOG, TARGET_SCHEMA = _TARGETS["catalog"], _TARGETS["schema"]
TARGET_LOOKUP_SCHEMA = _TARGETS["lookup"]
META_SCHEMA = _TARGETS["meta"]
APP_SCHEMA = _TARGETS["app"]
EXPECTED_OWNER = "8c733111-4a66-4207-833c-f0ec87c2f910"
TAG = (f"/* dbrowse ctx=system u={DATASET}-verify "
       f"feature={DATASET}-verify */ ")

results = []


def gate(name, passed, detail=""):
    results.append({"gate": name, "passed": bool(passed), "detail": detail})
    print(f"{'PASS' if passed else 'FAIL'}  {name}  {detail}")


def warn(name, findings):
    """INSR_W7 W7.04: an advisory gate: recorded with its findings, never
    failing the release."""
    results.append({"gate": name, "passed": True, "severity": "warn",
                    "findings": len(findings), "detail": str(findings[:20])})
    print(f"{'WARN' if findings else 'PASS'}  {name}  {len(findings)} "
          f"findings {findings[:20]}")


def _sql_names(names):
    return ", ".join(f"'{name}'" for name in names)


def _source_sql(plan):
    source = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{qi(plan['source'])}"
    where_sql = plan.get("where")
    if where_sql is not None:
        assert where_sql in ALLOWED_RESEARCH_FILTERS, \
            f"unapproved research filter in schema_doc: {where_sql!r}"
        return f"SELECT * FROM {source} WHERE {where_sql}"
    return f"SELECT * FROM {source}"


def verify_mask_transforms(doc, live_cols):
    """Compare each masked value with its transform on the same row."""
    leaks, mismatches, unverifiable = [], [], []
    masked = [(object_name, col["name"], {"mask": col["masked"]})
              for object_name, table in sorted(doc["tables"].items())
              for col in table["columns"] if col.get("masked")]
    for object_name, column, entry in masked:
        key = f"{object_name}.{column}"
        if object_name not in live_cols:
            continue
        pk = next((col["name"] for col in doc["tables"][object_name]["columns"]
                   if col.get("pk")), None)
        if not pk:
            unverifiable.append(key)
            continue
        source_sql = _source_sql(doc["sources"][object_name])
        raw = f"s.{qi(column)}"
        if entry["mask"] == "hash":
            expected = f"sha2(concat('{MASK_SALT}', cast({raw} AS STRING)), 256)"
            raw_leak_test = "observed <=> raw_value"
        else:
            expected = f"date_trunc('{entry['mask']}', {raw})"
            # A legitimate month/year truncation can equal the raw value on a
            # boundary date, so raw equality is only a leak signal for hashes.
            raw_leak_test = "FALSE"
        row = spark.sql(TAG + f"""
            SELECT
                SUM(CASE WHEN {raw_leak_test} THEN 1 ELSE 0 END) AS raw_equal,
                SUM(CASE WHEN NOT (observed <=> expected) THEN 1 ELSE 0 END)
                    AS transform_mismatch
            FROM (
                SELECT v.{qi(column)} AS observed, {raw} AS raw_value,
                       {expected} AS expected
                FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.{qi(object_name)} v
                JOIN ({source_sql}) s
                  ON v.{qi(pk)} = s.{qi(pk)}
                WHERE {raw} IS NOT NULL
                LIMIT 1000
            ) sample
        """).first()
        if (row["raw_equal"] or 0) > 0:
            leaks.append(key)
        if (row["transform_mismatch"] or 0) > 0:
            mismatches.append(key)
    return leaks, mismatches, unverifiable


def verify():
    assert re.fullmatch(r"[0-9a-fA-F]{32}", RUN_ID), \
        f"invalid run_id {RUN_ID!r}"
    results.clear()
    registry = spark.sql(TAG + f"""
        SELECT schema_doc FROM {TARGET_CATALOG}.{META_SCHEMA}.dataset_registry
        WHERE dataset = '{DATASET}' AND run_id = '{RUN_ID}'
    """).first()
    assert registry is not None, f"no registry row for {DATASET} run {RUN_ID}"
    doc = json.loads(registry["schema_doc"])
    objects = doc["manifest"]
    names = _sql_names(objects)

    # The release must be exactly what the planner selects from the source
    # today, less any unreadable `_v_` view the publish recorded as excluded,
    # any table with no rows and any table waiting for IG tags.
    source_inventory = [(row["table_name"], row["table_type"])
                        for row in spark.sql(TAG + f"""
        SELECT table_name, table_type
        FROM {SOURCE_CATALOG}.information_schema.tables
        WHERE table_schema = '{SOURCE_SCHEMA}'
    """).collect()]
    expected = (set(select_objects(source_inventory, PROFILE))
                - set(doc.get("excluded_unreadable", []))
                - set(doc.get("excluded_empty", []))
                - set(doc.get("waiting_for_tags", {})))
    gate("object_set_matches_source", set(objects) == expected,
         f"{len(objects)} objects; missing={sorted(expected - set(objects))} "
         f"extra={sorted(set(objects) - expected)}")

    stale = sorted(row["table_name"] for row in spark.sql(TAG + f"""
        SELECT table_name FROM {TARGET_CATALOG}.information_schema.tables
        WHERE table_schema = '{TARGET_SCHEMA}'
    """).collect() if row["table_name"] not in set(objects))
    gate("no_stale_live_views", not stale, f"stale={stale}")

    live_cols = {}
    for row in spark.sql(TAG + f"""
        SELECT table_name, column_name
        FROM {TARGET_CATALOG}.information_schema.columns
        WHERE table_schema = '{TARGET_SCHEMA}' AND table_name IN ({names})
        ORDER BY table_name, ordinal_position
    """).collect():
        live_cols.setdefault(row["table_name"], []).append(row["column_name"])
    gate("live_manifest", set(live_cols) == set(objects),
         f"missing={sorted(set(objects) - set(live_cols))} "
         f"extra={sorted(set(live_cols) - set(objects))}")

    # The live views expose exactly the release's published columns: every
    # described (type 'excluded') column is absent, nothing unlisted appears.
    drift = []
    for object_name, table in sorted(doc["tables"].items()):
        published = [c["name"] for c in table["columns"]
                     if c["type"] != "excluded"]
        live = [c for c in live_cols.get(object_name, [])
                if c not in INTERNAL_COLUMNS]
        if sorted(live) != sorted(published):
            drift.append(f"{object_name}: extra={sorted(set(live) - set(published))[:5]} "
                         f"missing={sorted(set(published) - set(live))[:5]}")
    gate("published_columns_match_release", not drift,
         f"{len(drift)} views differ: {drift[:5]}")

    ig_source_rows = [row.asDict() for row in spark.sql(
        TAG + source_ig_sql(SOURCE_CATALOG, SOURCE_SCHEMA, objects)).collect()]
    ig_mismatches = ig_score_mismatches(doc, ig_source_rows, PROFILE)
    gate("ig_scores_match_source", not ig_mismatches,
         f"{len(ig_mismatches)} columns differ: {ig_mismatches[:10]}")

    profiled = {(row["table_name"], row["column_name"]) for row in spark.sql(
        TAG + f"SELECT table_name, column_name FROM "
        f"{TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}.column_profile "
        f"WHERE dataset = '{DATASET}' AND run_id = '{RUN_ID}'").collect()}
    published_cols = {(table_name, column["name"])
                      for table_name, table in doc["tables"].items()
                      for column in table["columns"]}
    unprofiled = sorted(published_cols - profiled)
    gate("profile_coverage", not unprofiled,
         f"{len(unprofiled)} unprofiled: {unprofiled[:10]}")

    leaks, mismatches, unverifiable = verify_mask_transforms(doc, live_cols)
    gate("mask_transforms", not leaks and not mismatches and not unverifiable,
         f"raw_equal={leaks} mismatched={mismatches} "
         f"unverifiable={unverifiable}")

    parity_mismatches = []
    for object_name in objects[:10]:
        got = spark.sql(TAG + f"SELECT COUNT(*) AS n FROM "
                        f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{qi(object_name)}"
                        ).first()["n"]
        source = spark.sql(
            TAG + f"SELECT COUNT(*) AS n FROM "
            f"({_source_sql(doc['sources'][object_name])}) source"
        ).first()["n"]
        if got != source:
            parity_mismatches.append((object_name, got, source))
    gate("row_parity_spot_check", not parity_mismatches,
         f"{parity_mismatches}")

    owners = {row["table_owner"] for row in spark.sql(TAG + f"""
        SELECT table_owner
        FROM {TARGET_CATALOG}.information_schema.tables
        WHERE (table_schema = '{TARGET_SCHEMA}' AND table_name IN ({names}))
           OR (table_schema = '{TARGET_LOOKUP_SCHEMA}'
               AND table_type = 'VIEW')
    """).collect()}
    gate("view_owner", owners == {EXPECTED_OWNER},
         f"owners={sorted(owners)} (expected {EXPECTED_OWNER})")

    schemas = (TARGET_SCHEMA, TARGET_LOOKUP_SCHEMA)
    schema_names = _sql_names(schemas)
    grant_rows = spark.sql(TAG + f"""
        SELECT schema_name, privilege_type
        FROM {TARGET_CATALOG}.information_schema.schema_privileges
        WHERE grantee = '{BROWSER_SP_ID}' AND schema_name IN ({schema_names})
    """).collect()
    grants = {(row["schema_name"], row["privilege_type"])
              for row in grant_rows}
    # information_schema reports USE_SCHEMA, while GRANT uses USE SCHEMA.
    missing_grants = missing_schema_privileges(schemas, grants)
    gate("browser_grants", not missing_grants,
         f"missing={sorted(missing_grants)}")

    receipt = spark.sql(TAG + f"""
        SELECT status FROM {TARGET_CATALOG}.{APP_SCHEMA}.run_receipt
        WHERE dataset = '{DATASET}' AND run_id = '{RUN_ID}' AND kind = 'publish'
        ORDER BY run_at DESC LIMIT 1
    """).first()
    gate("publish_staged", receipt is not None
         and receipt["status"] == "staged",
         f"status={receipt['status'] if receipt else 'MISSING'}")

    newest = spark.sql(TAG + f"""
        SELECT run_id FROM {TARGET_CATALOG}.{META_SCHEMA}.dataset_registry
        WHERE dataset = '{DATASET}' ORDER BY published_at DESC LIMIT 1
    """).first()
    gate("registry_row_is_newest", newest is not None
         and newest["run_id"] == RUN_ID,
         f"newest={newest['run_id'] if newest else 'MISSING'} "
         f"expected={RUN_ID}")

    # INSR_W7 W7.04 (F21): structural gates (FAIL) and consistency gates (WARN)
    hub_problems = hub_publish_problems(doc, PROFILE)
    gate("hub_published", not hub_problems, f"{hub_problems}")
    dup_problems = dup_weight_problems(doc)
    gate("cube_sums_dup_weighted", not dup_problems, f"{dup_problems}")
    # time coverage measured at publish (the browser reads nothing else)
    missing_coverage = coverage_problems(doc)
    gate("time_coverage_measured", not missing_coverage,
         f"{missing_coverage[:20]}")
    key_columns = sorted({"person_id", "encounter_id", *PROFILE.join_hubs})
    estate_rows = [row.asDict() for row in spark.sql(
        TAG + estate_key_tags_sql(key_columns)).collect()]
    warn("tag_consistency", tag_consistency_findings(
        doc, estate_tag_mode(estate_rows), PROFILE))
    has_qc = spark.sql(TAG + f"SHOW TABLES IN {TARGET_CATALOG}."
                       f"{TARGET_LOOKUP_SCHEMA} LIKE 'qc_result'").collect()
    qc_rows = [row.asDict() for row in spark.sql(TAG + f"""
        SELECT dataset, expectation
        FROM {TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}.qc_result
        WHERE run_id = '{RUN_ID}'""").collect()] if has_qc else []
    warn("unresolved_expectations",
         unresolved_expectation_findings(doc, qc_rows))

    failed = [result["gate"] for result in results if not result["passed"]]
    if failed:
        write_receipt(RUN_ID, "verify", "failed", {"gates": results})
        raise AssertionError(f"verification gates failed: {failed}")

    write_receipt(RUN_ID, "verify", "ok", {"gates": results})
    return results


if ACTION == "verify":
    dbutils.notebook.exit(json.dumps(verify()))

