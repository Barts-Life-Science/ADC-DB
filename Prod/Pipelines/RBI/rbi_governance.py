# Databricks notebook source
# RBI governance: re-apply table/column comments and ig_risk / ig_severity tags after every pipeline update.
# MV column comments only land on the first build, so this runs as the job task after the pipeline.
# Fails (listing them) if any published column has no documentation, so drift is caught, not hidden.
import rbi_docs as D

dbutils.widgets.text("target", "8_dev.rbi")
TARGET = dbutils.widgets.get("target")
catalog, schema = TARGET.split(".")
assert catalog in ("8_dev", "4_prod"), TARGET


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

applied = 0
for table in sorted({r.table_name for r in cols}):
    fq = f"{q(catalog)}.{q(schema)}.{q(table)}"
    spark.sql(f"COMMENT ON TABLE {fq} IS {lit(D.TABLE_COMMENTS[table])}")
    for r in [r for r in cols if r.table_name == table]:
        risk, sev = D.ig_tags(r.column_name)
        spark.sql(f"ALTER MATERIALIZED VIEW {fq} ALTER COLUMN {q(r.column_name)} COMMENT {lit(D.COLUMN_COMMENTS[r.column_name])}")
        spark.sql(f"ALTER MATERIALIZED VIEW {fq} ALTER COLUMN {q(r.column_name)} SET TAGS ('ig_risk' = '{risk}', 'ig_severity' = '{sev}')")
        applied += 1

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
