"""INSR_W7_ESTATE_TAGS_V1 -- the single estate tag helper (J1, Ben 2026-10-29; INSR_20261029 W7.02).

Identifier and key columns are always published and tagged, and their tags are copied from the estate, never
invented. "The estate" is 4_prod.gold's ig_risk / ig_severity column tags: for each column name, the most common
(ig_risk, ig_severity) pair across every public gold table. A tie goes to the higher risk, then the higher
severity (the single tie rule, override O3). De-identification happens at serve time; a tag only describes the
column.

Consumers: PMS/RBI governance (W6.02), the silver bhrut_mrn tag (W2.09), and new columns with no estate
counterpart that copy a named sibling (tags_like; W7.08 coordinate_source copies source_table). The browser
publish computes the same mode in browser_publish_config.estate_tag_mode; both are pinned to one fixture
(estate_tags_parity.json).
"""
import re

GOLD_TAGS_SQL = """
SELECT table_name, column_name,
       max(CASE WHEN tag_name = 'ig_risk' THEN tag_value END) AS ig_risk,
       max(CASE WHEN tag_name = 'ig_severity' THEN tag_value END) AS ig_severity
FROM `{catalog}`.information_schema.column_tags
WHERE schema_name = '{schema}' AND tag_name IN ('ig_risk', 'ig_severity') AND left(table_name, 1) <> '_'
GROUP BY table_name, column_name
"""
_SAFE = re.compile(r"[A-Za-z0-9_]+")


def _score(value, top):
    """An integer tag value between 0 and `top`, or None for anything else."""
    text = "" if value is None else str(value).strip()
    return int(text) if text.isdigit() and int(text) <= top else None


def modal_tags(rows):
    """{column_name: (ig_risk, ig_severity)} from rows with table_name, column_name, ig_risk, ig_severity:
    the commonest pair per column name; a tie goes to the higher risk, then the higher severity. Rows missing
    either tag (or holding an unreadable value) are ignored."""
    counts = {}
    for row in rows:
        risk, severity = _score(row["ig_risk"], 4), _score(row["ig_severity"], 3)
        if risk is None or severity is None:
            continue
        pairs = counts.setdefault(row["column_name"], {})
        pairs[(risk, severity)] = pairs.get((risk, severity), 0) + 1
    return {column: max(pairs, key=lambda pair: (pairs[pair], pair)) for column, pairs in counts.items()}


def estate_tags(spark, catalog="4_prod", schema="gold"):
    """The estate's (ig_risk, ig_severity) for every gold column name (see modal_tags). One metadata read."""
    assert _SAFE.fullmatch(catalog) and _SAFE.fullmatch(schema), (catalog, schema)
    rows = spark.sql(GOLD_TAGS_SQL.format(catalog=catalog, schema=schema)).collect()
    return modal_tags(row.asDict() for row in rows)


def tags_like(spark, table, column, catalog="4_prod", schema="gold"):
    """The (ig_risk, ig_severity) one existing gold column carries, for a new column with no estate counterpart
    that plays the same role. Raises when the model column is untagged, so nothing is ever invented."""
    for name in (table, column, catalog, schema):
        assert _SAFE.fullmatch(name or ""), f"unsafe identifier {name!r}"
    sql = (GOLD_TAGS_SQL.format(catalog=catalog, schema=schema)
           + f" HAVING table_name = '{table}' AND column_name = '{column}'")
    rows = [row.asDict() for row in spark.sql(sql).collect()]
    got = modal_tags(rows)
    assert column in got, f"{schema}.{table}.{column} carries no readable ig tags to copy"
    return got[column]


def tag_statements(fqn, object_type, assignments):
    """ALTER statements setting {column: (ig_risk, ig_severity)} on one object. A materialized view takes the
    ALTER MATERIALIZED VIEW form; a table or a plain view takes ALTER TABLE (ALTER VIEW ... SET TAGS does not
    parse; ALTER TABLE <view> ... SET TAGS works, TMC 2026-10-22)."""
    keyword = "MATERIALIZED VIEW" if object_type == "MATERIALIZED_VIEW" else "TABLE"
    return [f"ALTER {keyword} {fqn} ALTER COLUMN `{column}` SET TAGS "
            f"('ig_risk' = '{risk}', 'ig_severity' = '{severity}')"
            for column, (risk, severity) in sorted(assignments.items())]
