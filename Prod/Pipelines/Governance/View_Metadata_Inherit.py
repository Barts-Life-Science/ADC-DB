# Databricks notebook source
# MAGIC %md
# MAGIC # View_Metadata_Inherit — give view columns the ig tags and comments of the columns they come from
# MAGIC
# MAGIC Views (the bronze `*_current` views, `map_pathology_enriched/_preferred`, `3_lookup.gold.v_search_*`, …) expose
# MAGIC columns of their base tables, identifiers included, but Table Metadata Compliance only seeds view metadata once,
# MAGIC so most view columns carry no `ig_risk`/`ig_severity` tags (11-10: 969 of 999 bronze view columns). This notebook
# MAGIC fills what is **missing** from the source columns Unity Catalog lineage records for each view column. It never
# MAGIC invents a value:
# MAGIC
# MAGIC - **tags**: copied when every current source column carries both tags and they agree; otherwise reported
# MAGIC   (`no_source_tags`, `conflict`), never guessed;
# MAGIC - **comment**: copied only for a pass-through column (one source column with the same name and a comment);
# MAGIC - existing tags and comments are never overwritten.
# MAGIC
# MAGIC "Current sources" = the sources lineage recorded on the latest date that view column appears as a target, so an old
# MAGIC view definition does not leak in. Materialized views are skipped (their pipeline / TMC own them).
# MAGIC
# MAGIC Widgets: `mode` = `dry_run` (report only) | `apply`; `scope` = comma list of `catalog` or `catalog.schema`
# MAGIC (default `4_prod,3_lookup,6_mgmt`); `lineage_days` = how far back lineage is read (default 400).
# MAGIC Cost: information_schema reads plus one filtered read of `system.access.column_lineage`; a few minutes serverless.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "dry_run", ["dry_run", "apply"], "Mode")
dbutils.widgets.text("scope", "4_prod,3_lookup,6_mgmt", "Catalogs or catalog.schema, comma separated")
dbutils.widgets.text("lineage_days", "400", "Days of lineage to read")
MODE = dbutils.widgets.get("mode")
SCOPE = [x.strip() for x in dbutils.widgets.get("scope").split(",") if x.strip()]
LINEAGE_DAYS = int(dbutils.widgets.get("lineage_days"))

import json
import re

SAFE = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$")
assert SCOPE and all(SAFE.match(x) for x in SCOPE), f"unsafe scope {SCOPE}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. One query: view columns missing metadata, their current lineage sources, the sources' metadata
# MAGIC
# MAGIC (One statement with CTEs: serverless temp views do not survive cell boundaries.)

# COMMAND ----------


def scope_predicate(cat_col, sch_col):
    """SQL predicate selecting the scope's catalogs / catalog.schema pairs."""
    parts = []
    for item in SCOPE:
        if "." in item:
            cat, sch = item.split(".")
            parts.append(f"({cat_col} = '{cat}' AND {sch_col} = '{sch}')")
        else:
            parts.append(f"{cat_col} = '{item}'")
    return "(" + " OR ".join(parts) + ")"


DECISION_SQL = f"""
WITH views AS (
    SELECT table_catalog, table_schema, table_name FROM system.information_schema.tables
    WHERE table_type = 'VIEW' AND table_schema <> 'information_schema'
      AND {scope_predicate('table_catalog', 'table_schema')}),
all_tags AS (
    SELECT concat_ws('.', catalog_name, schema_name, table_name) AS tbl, column_name,
           max(CASE WHEN tag_name = 'ig_risk' THEN tag_value END) AS ig_risk,
           max(CASE WHEN tag_name = 'ig_severity' THEN tag_value END) AS ig_severity
    FROM system.information_schema.column_tags WHERE tag_name IN ('ig_risk', 'ig_severity') GROUP BY 1, 2),
needing AS (                                   -- view columns missing a comment or either tag
    SELECT concat_ws('.', c.table_catalog, c.table_schema, c.table_name) AS view_name, c.column_name,
           nullif(trim(c.comment), '') AS comment, t.ig_risk, t.ig_severity
    FROM system.information_schema.columns c
    JOIN views v USING (table_catalog, table_schema, table_name)
    LEFT JOIN all_tags t ON t.tbl = concat_ws('.', c.table_catalog, c.table_schema, c.table_name)
                        AND t.column_name = c.column_name
    WHERE nullif(trim(c.comment), '') IS NULL OR t.ig_risk IS NULL OR t.ig_severity IS NULL),
lin AS (
    SELECT target_table_full_name AS view_name, target_column_name AS column_name,
           source_table_full_name AS src_table, source_column_name AS src_column, event_date
    FROM system.access.column_lineage
    WHERE event_date >= date_sub(current_date(), {LINEAGE_DAYS})
      AND target_table_full_name IN (SELECT DISTINCT view_name FROM needing)
      AND source_table_full_name IS NOT NULL AND source_column_name IS NOT NULL),
latest AS (SELECT view_name, column_name, max(event_date) AS d FROM lin GROUP BY 1, 2),
cur AS (                                       -- sources recorded on the latest date: the current view definition
    SELECT DISTINCT l.view_name, l.column_name, l.src_table, l.src_column
    FROM lin l JOIN latest m ON m.view_name = l.view_name AND m.column_name = l.column_name AND l.event_date = m.d),
src AS (                                       -- each current source column (must still exist) with its metadata
    SELECT cur.view_name, cur.column_name, cur.src_column, nullif(trim(sc.comment), '') AS src_comment,
           st.ig_risk AS src_risk, st.ig_severity AS src_severity
    FROM cur
    JOIN system.information_schema.columns sc
      ON concat_ws('.', sc.table_catalog, sc.table_schema, sc.table_name) = cur.src_table AND sc.column_name = cur.src_column
    LEFT JOIN all_tags st ON st.tbl = cur.src_table AND st.column_name = cur.src_column),
agg AS (
    SELECT view_name, column_name,
           count(*) AS n_sources,
           count_if(src_risk IS NULL OR src_severity IS NULL) AS n_untagged_sources,
           count(DISTINCT concat(src_risk, '/', src_severity)) AS n_tag_values,
           max(src_risk) AS risk, max(src_severity) AS severity,
           max(CASE WHEN src_column = column_name THEN src_comment END) AS same_name_comment
    FROM src GROUP BY 1, 2)
SELECT n.view_name, n.column_name, n.comment, n.ig_risk, n.ig_severity,
       a.n_sources, a.n_untagged_sources, a.n_tag_values, a.risk, a.severity, a.same_name_comment
FROM needing n LEFT JOIN agg a USING (view_name, column_name)"""

def load_decisions():
    """Rows of view columns still missing metadata, with their current sources' metadata (re-read every pass)."""
    return spark.sql(DECISION_SQL).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide per column (copy, or report why not)

# COMMAND ----------


def decide(r):
    """(tag_action, comment_action) for one view column: what to copy, or the reason nothing is copied."""
    tag = "has_tags"
    if r.ig_risk is None or r.ig_severity is None:
        if not r.n_sources:
            tag = "no_lineage"
        elif r.n_untagged_sources:
            tag = "no_source_tags"
        elif r.n_tag_values != 1:
            tag = "conflict"
        else:
            tag = "copy"
    comment = "has_comment"
    if r.comment is None:
        if not r.n_sources:
            comment = "no_lineage"
        elif r.n_sources == 1 and r.same_name_comment:
            comment = "copy"
        else:
            comment = "not_pass_through"
    return tag, comment


def plan(decisions):
    """(statements, per-view outcome counts) for one pass."""
    statements, report = [], {}
    for r in decisions:
        tag, comment = decide(r)
        entry = report.setdefault(r.view_name, {})
        for kind, outcome in (("tags", tag), ("comment", comment)):
            entry[f"{kind}:{outcome}"] = entry.get(f"{kind}:{outcome}", 0) + 1
        if tag == "copy":
            statements.append(f"ALTER TABLE {r.view_name} ALTER COLUMN `{r.column_name}` SET TAGS "
                              f"('ig_risk' = '{r.risk}', 'ig_severity' = '{r.severity}')")
        if comment == "copy":
            text = r.same_name_comment.replace("'", "''")
            statements.append(f"COMMENT ON COLUMN {r.view_name}.`{r.column_name}` IS '{text}'")
    return statements, report

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply (mode=apply) and summarise

# COMMAND ----------

# A view over another view (map_pathology_preferred reads map_pathology_enriched) can only inherit once its source
# view has been filled, so apply mode repeats the pass until nothing more is copied (at most MAX_PASSES).
MAX_PASSES = 3
errors, applied, passes = [], 0, 0
statements, report = plan(load_decisions())
first_statements = list(statements)
while MODE == "apply" and statements and passes < MAX_PASSES:
    passes += 1
    for sql in statements:
        try:
            spark.sql(sql)
            applied += 1
        except Exception as e:                   # report and carry on: one bad view must not block the rest
            errors.append(f"{sql[:160]} -> {type(e).__name__}: {str(e)[:200]}")
    print(f"pass {passes}: {len(statements)} statements")
    statements, report = plan(load_decisions())   # what is still missing after this pass
for e in errors[:20]:
    print("ERROR", e)
for view, counts in sorted(report.items()):
    print(view, json.dumps(counts, sort_keys=True))

totals = {}
for counts in report.values():
    for k, v in counts.items():
        totals[k] = totals.get(k, 0) + v
# Per-view list of what stays unresolved (reported, never guessed), and a sample of identifier columns copied.
UNRESOLVED = ("tags:no_source_tags", "tags:conflict", "tags:no_lineage")
unresolved = {v: {k: n for k, n in c.items() if k in UNRESOLVED} for v, c in report.items()}
unresolved = {v: c for v, c in unresolved.items() if c}
identifier_sample = [s_ for s_ in first_statements if re.search(r"`(NHS_Number|MRN|PERSON_ID)`", s_)][:6]
result = {"mode": MODE, "scope": SCOPE, "views": len(report), "planned_first_pass": len(first_statements),
          "applied": applied, "passes": passes, "left_copyable": len(statements) if MODE == "apply" else None,
          "errors": len(errors), "totals": totals, "unresolved_by_view": unresolved,
          "identifier_sample": identifier_sample}
print(json.dumps(result, sort_keys=True))
assert not errors, f"{len(errors)} statements failed (first: {errors[0]})"
dbutils.notebook.exit(json.dumps(result, sort_keys=True))

