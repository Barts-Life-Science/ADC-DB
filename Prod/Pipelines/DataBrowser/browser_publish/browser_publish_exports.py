# Databricks notebook source
# BrowserPublish / exports — upstream metadata and pipeline configuration
# for the VM-side enrichment (scripts/estate_enrich.py), written into the
# browser-readable lookup schema. %run by browser_publish_registry, which
# supplies browser_publish_config (SQL builders) and the publisher identity.

# COMMAND ----------
def run_exports(run_id, *, lookup, tag, profile):
    """Write source_column_meta (the dataset's upstream schemas) and, when
    the profile names its pipelines, pipeline_config for one release."""
    counts = {}
    exports = [("source_column_meta", SOURCE_COLUMN_META_DDL,
                source_column_meta_sql(target=f"{lookup}.source_column_meta",
                                       run_id=run_id,
                                       source_catalog=profile.source_catalog,
                                       schemas=profile.upstream_schemas))]
    if profile.pipelines:
        exports.append(("pipeline_config", PIPELINE_CONFIG_DDL,
                        pipeline_config_sql(
                            target=f"{lookup}.pipeline_config", run_id=run_id,
                            pipelines=profile.pipelines,
                            catalog=profile.source_catalog)))
    for name, ddl, sql in exports:
        table = f"{lookup}.{name}"
        spark.sql(tag + ddl.format(table=table))
        spark.sql(tag + f"DELETE FROM {table} WHERE run_id = '{run_id}'")
        spark.sql(tag + sql)
        counts[name] = spark.sql(
            tag + f"SELECT COUNT(*) AS n FROM {table} "
            f"WHERE run_id = '{run_id}'").first()["n"]
    if profile.pipelines:
        missing_update = spark.sql(
            tag + f"SELECT role FROM {lookup}.pipeline_config "
            f"WHERE run_id = '{run_id}' AND update_id IS NULL").collect()
        assert not missing_update, (
            f"no completed update found for "
            f"{[r['role'] for r in missing_update]}")
    print(f"exports for {run_id}: {counts}")
    return counts

