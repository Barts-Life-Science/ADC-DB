# Databricks notebook source
# INSR_W6_PMS_RBI_V1 (INSR_20261029 W6: PMS Pharma, PMS Device, RBI)
# RBI governance: re-apply table/column comments and ig_risk / ig_severity tags after every pipeline update.
# MV column comments only land on the first build, so this runs as the job task after the pipeline.
# Fails (listing them) if any published column has no documentation, so drift is caught, not hidden.
import sys

import rbi_docs as D

dbutils.widgets.text("target", "8_dev.rbi")
TARGET = dbutils.widgets.get("target")
catalog, schema = TARGET.split(".")
assert catalog in ("8_dev", "4_prod"), TARGET

# INSR_W6 W6.02 (J1, 2026-10-29): estate tags from the one shared helper (W7.02).
dbutils.widgets.text("shared_path", "/Workspace/Shared/ADC-DB/Prod/Pipelines/_shared")
sys.path.insert(0, dbutils.widgets.get("shared_path"))
from estate_tags import estate_tags  # noqa: E402

ESTATE = estate_tags(spark)  # column_name -> (ig_risk, ig_severity) as integers; D.ig_tags returns strings


def q(name):
    """Backtick-quote an identifier."""
    return "`" + name.replace("`", "``") + "`"


def lit(text):
    """SQL string literal with single quotes doubled."""
    return "'" + text.replace("'", "''") + "'"

# COMMAND ----------

cols = spark.sql(f"""
    SELECT c.table_name, c.column_name FROM {q(catalog)}.information_schema.columns c
    JOIN {q(catalog)}.information_schema.tables t USING (table_catalog, table_schema, table_name)
    WHERE c.table_schema = {lit(schema)} AND t.table_type = 'MATERIALIZED_VIEW'
      AND c.table_name NOT LIKE '\\_%' AND c.table_name NOT LIKE 'event_log%'
""").collect()
undocumented = sorted({(r.table_name, r.column_name) for r in cols if r.column_name not in D.COLUMN_COMMENTS})
untabled = sorted({r.table_name for r in cols} - set(D.TABLE_COMMENTS))
assert not undocumented and not untabled, f"undocumented columns {undocumented}; tables without comment {untabled}"

# COMMAND ----------

# INSR_W6 W6.02: the tags as they are now, so each run prints its before -> after diff (empty on a re-run).
before_tags = {}
for t in spark.sql(f"""SELECT table_name, column_name, tag_name, tag_value FROM {q(catalog)}.information_schema.column_tags
                       WHERE schema_name = {lit(schema)} AND tag_name IN ('ig_risk', 'ig_severity')""").collect():
    before_tags.setdefault((t.table_name, t.column_name), {})[t.tag_name] = t.tag_value
changed = []
applied = 0
for table in sorted({r.table_name for r in cols}):
    fq = f"{q(catalog)}.{q(schema)}.{q(table)}"
    spark.sql(f"COMMENT ON TABLE {fq} IS {lit(D.TABLE_COMMENTS[table])}")
    for r in [r for r in cols if r.table_name == table]:
        risk, sev = D.ig_tags(r.column_name, ESTATE)
        old = before_tags.get((table, r.column_name), {})
        if (old.get("ig_risk"), old.get("ig_severity")) != (risk, sev):
            changed.append((table, r.column_name, old.get("ig_risk"), old.get("ig_severity"), risk, sev))
        spark.sql(f"ALTER MATERIALIZED VIEW {fq} ALTER COLUMN {q(r.column_name)} COMMENT {lit(D.COLUMN_COMMENTS[r.column_name])}")
        spark.sql(f"ALTER MATERIALIZED VIEW {fq} ALTER COLUMN {q(r.column_name)} SET TAGS ('ig_risk' = '{risk}', 'ig_severity' = '{sev}')")
        applied += 1
print(f"tag changes ({len(changed)}):")
for row in changed:
    print("  ", row)

# COMMAND ----------

missing_tags = spark.sql(f"""
    SELECT c.table_name, c.column_name FROM {q(catalog)}.information_schema.columns c
    JOIN {q(catalog)}.information_schema.tables t USING (table_catalog, table_schema, table_name)
    LEFT JOIN (SELECT table_name, column_name, count(DISTINCT tag_name) AS n FROM {q(catalog)}.information_schema.column_tags
               WHERE schema_name = {lit(schema)} AND tag_name IN ('ig_risk', 'ig_severity') GROUP BY 1, 2) g
      USING (table_name, column_name)
    WHERE c.table_schema = {lit(schema)} AND t.table_type = 'MATERIALIZED_VIEW'
      AND c.table_name NOT LIKE '\\_%' AND c.table_name NOT LIKE 'event_log%' AND coalesce(g.n, 0) < 2
""").collect()
assert not missing_tags, f"columns missing a tag after apply: {[(r.table_name, r.column_name) for r in missing_tags]}"
dbutils.notebook.exit(f"governance applied to {applied} columns in {TARGET}")
