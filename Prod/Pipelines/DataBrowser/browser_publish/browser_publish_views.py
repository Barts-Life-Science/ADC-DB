# Databricks notebook source
# BrowserPublish / views — the research-product views over one dataset's
# source schema (its DatasetProfile, chosen by the `dataset` widget) plus,
# for gold, the single read-only view over 3_lookup.gold.search_index.
#
# Owner's-rights is the isolation boundary: the browser service principal can
# read these views while holding no grant on 4_prod or 3_lookup.

# COMMAND ----------
# MAGIC %run ./browser_publish_config

# COMMAND ----------
import json
import re
import time

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"])
dbutils.widgets.dropdown("action", "none", ["none", "build"])
dbutils.widgets.text("run_id", "")

ENV = dbutils.widgets.get("env")
ACTION = dbutils.widgets.get("action")

dbutils.widgets.dropdown("dataset", "gold", sorted(PROFILES))
DATASET = dbutils.widgets.get("dataset")
PROFILE = PROFILES[DATASET]

SOURCE_CATALOG, SOURCE_SCHEMA = PROFILE.source_catalog, PROFILE.source_schema
# dev runs may read a dataset that exists only in 8_dev (e.g. 8_dev.rbi);
# prod always reads the profile's source
dbutils.widgets.text("dev_source", "", "dev only: catalog.schema to read")
DEV_SOURCE = dbutils.widgets.get("dev_source").strip()
if DEV_SOURCE:
    assert ENV == "dev", "dev_source is refused outside env=dev"
    assert DEV_SOURCE.startswith("8_dev."), DEV_SOURCE
    SOURCE_CATALOG, SOURCE_SCHEMA = DEV_SOURCE.split(".", 1)
# gold alone exposes a search index (3_lookup.gold.search_index)
LOOKUP_CATALOG, LOOKUP_SCHEMA = PROFILE.search_index or (None, None)
_TARGETS = targets_for(PROFILE, ENV)
TARGET_CATALOG = _TARGETS["catalog"]
TARGET_SCHEMA = _TARGETS["schema"]
TARGET_LOOKUP_SCHEMA = _TARGETS["lookup"]
TAG = f"/* dbrowse ctx=system u={DATASET}-publish feature={DATASET}-views */ "

# The run's column policy: derived by build() from the source's IG tags,
# column types and a data sample (derive_policy), never hand-written. Later
# notebooks in the same context (stats, profile, registry) read it.
POLICY = {}
TEXT_SAMPLE_WORKERS = 8

# Deterministic within each environment so masked joins survive re-publishes,
# and distinct across environments so a dev hash is not a prod hash.
MASK_SALT = f"{DATASET}_browser::{ENV}"

for _identifier in (SOURCE_CATALOG, SOURCE_SCHEMA, TARGET_CATALOG,
                    TARGET_SCHEMA, TARGET_LOOKUP_SCHEMA):
    assert re.fullmatch(r"[A-Za-z0-9_]+", _identifier), \
        f"unsafe identifier {_identifier!r}"


def inventory():
    rows = spark.sql(TAG + f"""
        SELECT table_name, table_type
        FROM {SOURCE_CATALOG}.information_schema.tables
        WHERE table_schema = '{SOURCE_SCHEMA}'
    """).collect()
    return [(row["table_name"], row["table_type"]) for row in rows]


def unreadable_research_views(objects):
    """Return `_v_` objects whose definitions fail a zero-row resolution.

    A plain view over gold can break when gold is restructured underneath
    it; such a view is excluded from the release (and named in the receipt)
    rather than failing the whole publish.
    """
    broken = set()
    for object_name in objects:
        if "_v_" not in object_name:
            continue
        try:
            spark.sql(TAG + f"SELECT 1 FROM {SOURCE_CATALOG}."
                      f"{SOURCE_SCHEMA}.{qi(object_name)} LIMIT 0").collect()
        except Exception as error:
            broken.add(object_name)
            print(f"excluding unreadable {object_name}: "
                  f"{type(error).__name__}")
    return broken


def columns_for(objects):
    """Return source columns and types per object, in ordinal order."""
    names = ", ".join(f"'{name}'" for name in objects)
    rows = spark.sql(TAG + f"""
        SELECT table_name, column_name, data_type, ordinal_position
        FROM {SOURCE_CATALOG}.information_schema.columns
        WHERE table_schema = '{SOURCE_SCHEMA}' AND table_name IN ({names})
        ORDER BY table_name, ordinal_position
    """).collect()
    columns, types = {}, {}
    for row in rows:
        columns.setdefault(row["table_name"], []).append(row["column_name"])
        types.setdefault(row["table_name"], {})[row["column_name"]] = \
            row["data_type"]
    missing = sorted(set(objects) - set(columns))
    assert not missing, f"no source columns for {missing}"
    return columns, types


def staging_schema(run_id: str) -> str:
    """Return the run-scoped schema used before live promotion."""
    suffix = run_id.replace("-", "")[:8]
    assert re.fullmatch(r"[A-Za-z0-9]{8}", suffix), \
        f"unsafe run_id {run_id!r}"
    return f"{TARGET_SCHEMA}_stg_{suffix}"


# Unreadable `_v_` views skipped by the latest build(), for the receipt.
EXCLUDED_UNREADABLE = []

# Tables with no rows skipped by the latest build() (see empty_tables), for
# the receipt and the release doc.
EXCLUDED_EMPTY = []

# Tables left out because a column lacks an IG tag, for the receipt and the
# release doc: {table: [untagged columns]}.
WAITING_FOR_TAGS = {}


def sample_text_stats(objects, column_types):
    """{(object, column): {n, words, distinct}} for every scalar column (word
    counts for strings only), from up to TEXT_SAMPLE_VALUES non-null values
    each (objects in parallel)."""
    from concurrent.futures import ThreadPoolExecutor

    def one(object_name):
        scalar = {c: (t or "").upper() for c, t in column_types[object_name].items()
                  if c not in INTERNAL_COLUMNS
                  and not (t or "").upper().startswith(NON_SCALAR_TYPES)}
        strings = [c for c, t in scalar.items() if t.startswith("STRING")]
        others = [c for c, t in scalar.items() if not t.startswith("STRING")]
        if not scalar:
            return {}
        source = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{qi(object_name)}"
        rows = spark.sql(TAG + text_stats_sql(source, strings, others)).collect()
        return parse_text_stats(object_name, [row.asDict() for row in rows])

    stats = {}
    with ThreadPoolExecutor(max_workers=TEXT_SAMPLE_WORKERS) as pool:
        for part in pool.map(one, objects):
            stats.update(part)
    return stats


def load_policy(objects, cols, column_types, *, tag_sources=()):
    """(policy, problems) for these objects, from the source's IG tags,
    column types and sampled string contents. tag_sources are extra tables
    whose tags and types are read but which are not sampled or classified:
    the parents an inherited-time child takes event_datetime from."""
    tagged = sorted(set(objects) | set(tag_sources))
    source_rows = [row.asDict() for row in spark.sql(
        TAG + source_ig_sql(SOURCE_CATALOG, SOURCE_SCHEMA, tagged)).collect()]
    sampled_types = {o: column_types[o] for o in objects}
    return derive_policy(cols, column_types, source_rows,
                         sample_text_stats(objects, sampled_types),
                         profile=PROFILE)


def build(schema: str, *, tag_columns: bool = True, only: list | None = None):
    inventory_rows = inventory()
    selected = select_objects(inventory_rows, PROFILE)
    broken = unreadable_research_views(selected)
    EXCLUDED_UNREADABLE[:] = sorted(broken)
    objects = [name for name in selected if name not in broken]
    parents = []
    if only is not None:
        # a targeted republish rebuilds these tables only; an inheriting
        # child still needs its parent's columns, tags and types
        unknown = sorted(set(only) - set(objects))
        assert not unknown, (f"not publishable from {SOURCE_CATALOG}."
                             f"{SOURCE_SCHEMA}: {unknown}")
        objects = sorted(only)
        links = {**PROFILE.inherited_time, **PROFILE.inherited_person}
        parents = sorted({links[o][1] for o in objects if o in links}
                         - set(objects))
    cols, column_types = columns_for(objects + parents)
    plans = build_source_plans(objects, inventory_rows, cols, PROFILE)
    cols = with_inherited_columns({o: cols[o] for o in objects}, plans)
    print(f"selected {len(objects)} objects -> {TARGET_CATALOG}.{schema} "
          f"({sum(plan['where'] is not None for plan in plans.values())} "
          f"filtered to active rows; "
          f"{sum('inherit' in plan for plan in plans.values())} inherit a "
          f"parent time or person; {len(broken)} unreadable excluded)")

    # Derive the column policy before any DDL. Every column needs both IG
    # scores in Unity Catalog, published or not: a table with an untagged
    # column waits (left out and listed); any other problem stops the run.
    policy, problems = load_policy(objects, cols, column_types,
                                   tag_sources=parents)
    # From here on the inherited event_datetime carries its parent's type, so
    # profiling covers it (load_policy above must see source columns only:
    # it samples the source tables, where the child has no such column).
    column_types = with_bigint_types(
        with_inherited_types(column_types, plans), plans)
    waiting = untagged_tables(problems)
    WAITING_FOR_TAGS.clear()
    WAITING_FOR_TAGS.update(waiting)
    if waiting:
        print(f"waiting for IG tags (left out of this release): "
              f"{sorted(waiting)}")
        objects = [o for o in objects if o not in waiting]
        cols = {o: cols[o] for o in objects}
        column_types = {o: column_types[o] for o in objects}
        plans = {o: plans[o] for o in objects}
        policy = {key: entry for key, entry in policy.items()
                  if key.split(".", 1)[0] in set(objects)}
    problems = [p for p in problems if not _UNTAGGED.match(p)]
    assert not problems, (
        f"cannot classify these {SOURCE_CATALOG}.{SOURCE_SCHEMA} columns:\n  "
        + "\n  ".join(problems[:50])
        + (f"\n  ... and {len(problems) - 50} more"
           if len(problems) > 50 else ""))
    # A table with no rows has no column to publish: leave it out and list it.
    empty = empty_tables(policy, objects)
    EXCLUDED_EMPTY[:] = empty
    if empty:
        print(f"no rows (left out of this release): {empty}")
        objects = [o for o in objects if o not in empty]
        cols = {o: cols[o] for o in objects}
        column_types = {o: column_types[o] for o in objects}
        plans = {o: plans[o] for o in objects}
        policy = {key: entry for key, entry in policy.items()
                  if key.split(".", 1)[0] in set(objects)}
    POLICY.clear()
    POLICY.update(policy)
    described = sum(entry["action"] == "describe" for entry in policy.values())
    print(f"policy: {len(policy) - described} columns published, "
          f"{described} described only")

    spark.sql(TAG + f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{schema}")
    spark.sql(TAG + "CREATE SCHEMA IF NOT EXISTS "
              f"{TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}")

    for object_name in objects:
        plan = plans[object_name]
        target = f"{TARGET_CATALOG}.{schema}.{qi(object_name)}"
        ddl = build_view_ddl(
            target=target,
            source=f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{qi(plan['source'])}",
            object_name=object_name, columns=cols[object_name],
            policy=POLICY, salt=MASK_SALT, where_sql=plan["where"],
            column_types=column_types[object_name],
            inherit=plan.get("inherit"), bigint=plan.get("bigint"))
        spark.sql(TAG + ddl)
        for column in inherited_columns(plan.get("inherit") or {"columns": []}):
            spark.sql(TAG + inherited_comment_sql(target, plan["inherit"],
                                                  column))

    # Carry twin-resolution fields and rows without vectors so sync can report
    # release-scoped coverage rather than hiding gaps at the view boundary.
    if PROFILE.search_index:
        spark.sql(TAG + f"""
            CREATE OR REPLACE VIEW
                {TARGET_CATALOG}.{TARGET_LOOKUP_SCHEMA}.search_index AS
            SELECT corpus_id, facet, object_name, object_type, is_view,
                   base_object_name, column_name, ordinal_position,
                   coding_system, code_system_family, code,
                   embed_text, display_text, text_source,
                   embedding_vector, model_version, has_vector, crawled_at
            FROM {LOOKUP_CATALOG}.{LOOKUP_SCHEMA}.search_index
        """)

    if tag_columns:
        apply_tags(objects, cols, schema)
    print(f"built {len(objects)} views"
          f"{' + lookup.search_index' if PROFILE.search_index else ''} in "
          f"{TARGET_CATALOG}.{schema}")
    # stats, profiles and DDL see only the rebuilt tables, not tag sources
    column_types = {o: column_types[o] for o in objects}
    return objects, cols, column_types, plans


def promote(objects, cols, column_types, plans, *, tag_columns=True,
            drop_stale=True):
    """Replay validated DDL against the live schema."""
    spark.sql(TAG + f"CREATE SCHEMA IF NOT EXISTS "
              f"{TARGET_CATALOG}.{TARGET_SCHEMA}")
    for object_name in objects:
        plan = plans[object_name]
        target = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{qi(object_name)}"
        ddl = build_view_ddl(
            target=target,
            source=f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{qi(plan['source'])}",
            object_name=object_name, columns=cols[object_name],
            policy=POLICY, salt=MASK_SALT, where_sql=plan["where"],
            column_types=column_types[object_name],
            inherit=plan.get("inherit"), bigint=plan.get("bigint"))
        spark.sql(TAG + ddl)
        for column in inherited_columns(plan.get("inherit") or {"columns": []}):
            spark.sql(TAG + inherited_comment_sql(target, plan["inherit"],
                                                  column))
    # a targeted republish replaces its own views and leaves the rest live
    if drop_stale:
        drop_stale_live_views(objects)
    if tag_columns:
        apply_tags(objects, cols, TARGET_SCHEMA)
    print(f"promoted {len(objects)} views to "
          f"{TARGET_CATALOG}.{TARGET_SCHEMA}")


def drop_stale_live_views(objects):
    """Drop live views that are no longer part of the release.

    Gold renames (e.g. clinical_v_condition -> clinical_condition) would
    otherwise leave broken views in the browser's catalogue. Only VIEWs in
    the live schema are touched, never tables.
    """
    live = spark.sql(TAG + f"""
        SELECT table_name, table_type
        FROM {TARGET_CATALOG}.information_schema.tables
        WHERE table_schema = '{TARGET_SCHEMA}'
    """).collect()
    stale = sorted(row["table_name"] for row in live
                   if row["table_type"] == "VIEW"
                   and row["table_name"] not in set(objects))
    for name in stale:
        spark.sql(TAG + f"DROP VIEW IF EXISTS "
                  f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{qi(name)}")
    if stale:
        print(f"dropped {len(stale)} stale live views: {stale}")


def drop_live_views(names):
    """Drop these live views by name: the changed tables a targeted
    republish removed from the release (they now wait for IG tags). Only
    VIEWs are dropped; a name that is not a live view is ignored."""
    if not names:
        return []
    listed = ", ".join(f"'{name}'" for name in names)
    live = [row["table_name"] for row in spark.sql(TAG + f"""
        SELECT table_name FROM {TARGET_CATALOG}.information_schema.tables
        WHERE table_schema = '{TARGET_SCHEMA}' AND table_type = 'VIEW'
          AND table_name IN ({listed})
    """).collect()]
    for name in sorted(live):
        spark.sql(TAG + f"DROP VIEW IF EXISTS "
                  f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{qi(name)}")
    print(f"dropped {len(live)} live view(s) that left the release: {live}")
    return sorted(live)


def drop_staging(schema: str):
    assert schema.startswith(f"{TARGET_SCHEMA}_stg_"), \
        f"refusing to drop {schema!r} — not a staging schema"
    spark.sql(TAG + f"DROP SCHEMA IF EXISTS {TARGET_CATALOG}.{schema} CASCADE")


def apply_tags(objects, cols, schema):
    """Apply the policy-authored IG tags to every published view column."""
    actual = current_ig_tags(objects, schema)
    complete = completely_tagged_objects(
        expected_ig_tags(objects, cols, POLICY), actual)
    if complete:
        print(f"IG tags already complete on {len(complete)}/{len(objects)} "
              "objects; applying only missing objects")
    statements = []
    for object_name in objects:
        if object_name in complete:
            continue
        fqn = f"{TARGET_CATALOG}.{schema}.{qi(object_name)}"
        statements.append(build_column_tag_sql(
            fqn=fqn, object_name=object_name, columns=cols[object_name],
            policy=POLICY, tag_comment=TAG.strip()))
    for start in range(0, len(statements), 5):
        sql = combine_column_tag_sql(statements[start:start + 5])
        for attempt in range(1, 5):
            try:
                spark.sql(sql)
                break
            except Exception as exc:
                if ("TABLE_OR_VIEW_NOT_FOUND" not in str(exc)
                        or attempt == 4):
                    raise
                delay = 5 * attempt
                print(f"tag target not visible yet; retrying batch in "
                      f"{delay}s (attempt {attempt}/4)")
                time.sleep(delay)


def current_ig_tags(objects, schema):
    """Read the current IG tag tuples for the named published objects."""
    names = ", ".join(f"'{name}'" for name in objects)
    rows = spark.sql(TAG + f"""
        SELECT table_name, column_name, tag_name, tag_value
        FROM {TARGET_CATALOG}.information_schema.column_tags
        WHERE schema_name = '{schema}'
          AND table_name IN ({names})
          AND tag_name IN ('ig_risk', 'ig_severity')
    """).collect()
    return {(row["table_name"], row["column_name"], row["tag_name"],
             row["tag_value"]) for row in rows}


def tags_match_policy(objects, cols, schema):
    """True only when every live published column has both exact IG tags."""
    return current_ig_tags(objects, schema) == expected_ig_tags(
        objects, cols, POLICY)


if ACTION == "build":
    built, _, _, _ = build(staging_schema(dbutils.widgets.get("run_id")))
    dbutils.notebook.exit(json.dumps({"objects": built}))

