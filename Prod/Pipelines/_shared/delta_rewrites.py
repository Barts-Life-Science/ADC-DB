"""R1FIX_20261109: find the commits in a Delta version window that rewrote the whole table.

Why this exists: incremental readers (the R1 resolver and attach, pathology_incremental, device_pipeline) read a
table's change feed from a stored version to the latest one. When a commit inside that window replaced the table
(CREATE OR REPLACE TABLE AS SELECT, a DataFrame overwrite, INSERT OVERWRITE, or a column drop), two things are true:

- every row of the table is "changed" in that window, so a change-feed read buys nothing over reading the table;
- if the rewrite also narrowed the schema, the change-feed read fails with
  DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE (bronze run 178236917345566, 2026-11-08).

Each reader decides for itself what a rewrite means for it; this module only answers "which commits in the window
rewrote the table?" from DESCRIBE HISTORY, a metadata read that touches no data files.
"""

# Operations that replace every row (or drop columns). A MERGE, UPDATE, DELETE or append WRITE never appears here.
_REPLACE_OPERATIONS = {
    "CREATE OR REPLACE TABLE AS SELECT",
    "REPLACE TABLE AS SELECT",
    "CREATE OR REPLACE TABLE",
    "REPLACE TABLE",
    "DROP COLUMNS",
    "REPLACE COLUMNS",
}


def _is_rewrite(operation, parameters):
    """True when one DESCRIBE HISTORY row is a whole-table rewrite.

    A plain WRITE is only a rewrite in Overwrite mode without a predicate (a replaceWhere overwrite replaces a slice,
    which a change-feed read handles normally)."""
    if operation in _REPLACE_OPERATIONS:
        return True
    if operation == "WRITE":
        mode = str((parameters or {}).get("mode", ""))
        predicate = str((parameters or {}).get("predicate", "") or "")
        return mode == "Overwrite" and predicate in ("", "[]")
    return False


def rewrite_versions(spark, table_name, start_version, end_version):
    """Versions in [start_version, end_version] whose commit rewrote `table_name`, ascending; [] when none.

    start_version is the first version the reader would read (its stored version + 1), matching the change-feed
    reads this guards. An empty or inverted window returns []."""
    start_version, end_version = int(start_version), int(end_version)
    if start_version > end_version:
        return []
    history = spark.sql(f"DESCRIBE HISTORY {table_name}").select("version", "operation", "operationParameters")
    rows = history.where(f"version BETWEEN {start_version} AND {end_version}").collect()
    return sorted(int(r["version"]) for r in rows if _is_rewrite(r["operation"], r["operationParameters"]))


def narrowing_versions(spark, table_name, start_version, end_version):
    """The rewrite versions in the window that dropped at least one column: {version: [dropped column names]}.

    Only these make a change-feed read across the window fail (a same-schema replace reads fine and simply shows
    every row as changed). Two schema reads per rewrite (versions v-1 and v), metadata only."""
    narrowed = {}
    for version in rewrite_versions(spark, table_name, start_version, end_version):
        if version == 0:
            continue
        before = set(spark.read.option("versionAsOf", version - 1).table(table_name).columns)
        after = set(spark.read.option("versionAsOf", version).table(table_name).columns)
        if before - after:
            narrowed[version] = sorted(before - after)
    return narrowed
