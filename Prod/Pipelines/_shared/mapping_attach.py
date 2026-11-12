"""R1 bronze attach: put each lane's resolution on the bronze rows as <LANE>_MAPPING (STRUCT), <LANE>_MAP_KEY and
<LANE>_MAP_COMPONENTS_JSON.

  attach_axes(df, target)            inline, in the builders that overwrite their table (deviation D1) and in the
                                     initial rewrite; returns df with the lane columns (NULL struct for keys the
                                     resolver has not seen yet; the post-landing attach fills them in the same run)
  attach_changed(spark, target, run) post-landing task: rows in the change feed since the watermark, re-keyed and
                                     MERGEd only where the key or the resolution hash changed
  initial_rewrite(spark, target)     P2: add the columns, rewrite the table once with INSERT OVERWRITE (every existing
                                     column carried unchanged, ADC_UPDT above all; properties, comments, tags kept)
  heal_if_dropped(spark, target)     a builder's full-rebuild fallback dropped the columns of an attached table:
                                     re-add, rewrite, re-tag (HEAL_DEFERRED for tables over 1bn rows)
  retag_if_missing(spark, target)    an inline overwrite dropped the R1 column comments/tags: re-apply them
  attach_silver_arm(df, source, model) D4: a silver-derived arm's rows get the lane columns a bronze pregate
                                     passes through (key computed in silver, component 1 of mapping_resolution)
"""
import json

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

import delta_rewrites as DR      # R1FIX_20261109: whole-table rewrites inside a change-feed window
import mapping_contract as M
import mapping_lanes as L
import mapping_rowhash as RH

# How a reader recognises R1's own key-scoped commits without any state: both R1 MERGEs that write lane columns
# (attach_changed below, mapping_backfill.apply_lane) carry a matched condition on `<LANE>_MAPPING.resolution_hash`,
# and no builder MERGE names that field. DESCRIBE HISTORY records the condition in
# operationParameters.matchedPredicates, so PATEXP's pathology_incremental (optional R1 component, D13) can drop those
# commits from its map_pathology change read. (A commit userMetadata tag was the first choice, but serverless ignores
# spark.databricks.delta.commitInfo.userMetadata: dev proof 2026-11-02.)
OWN_COMMIT_MARKER = ".resolution_hash"

GOLD_TABLE = {"condition": "clinical_condition", "family_history": "clinical_family_history",
              "procedure": "clinical_procedure", "pathology_result": "clinical_pathology_result",
              "vital_sign": "clinical_vital_sign", "clinical_score": "clinical_clinical_score",
              "medication_admin": "clinical_medication_admin", "medication_order": "clinical_medication_order",
              "medication_dispense": "clinical_medication_dispense", "clinical_finding": "clinical_clinical_finding",
              "imaging_exam": "clinical_imaging_exam", "allergy_intolerance": "clinical_allergy_intolerance",
              "cancer_treatment": "clinical_cancer_treatment", "condition_stage": "clinical_condition_stage",
              "device": "clinical_device", "presenting_complaint": "clinical_presenting_complaint",
              "specimen": "clinical_specimen"}
COMMENTS = {
    "MAPPING": "R1 resolved mapping of lane {lane} for this row (component 1): the 24-field S3 axis contract plus "
               "extras_json, component_identity, resolution_hash and resolver_version, from "
               "3_lookup.omop.mapping_resolution by {key}. NULL until the key is resolved.",
    "MAP_KEY": "R1 map key of lane {lane}: sha2 of the lane id and its normalised mapping inputs (rows with equal "
               "inputs share one resolution).",
    "MAP_COMPONENTS_JSON": "R1: every component of the lane {lane} resolution as a JSON array when the source code "
                           "splits into several concepts; NULL otherwise. Silver explodes it.",
}


def _bare(target):
    """Bare table name of a fully qualified target."""
    return target.replace("`", "").split(".")[-1]


def resolution_source(spark, names, lane_id):
    """(map_key, mapping STRUCT of component 1, components_json when there are several) for one lane."""
    res = spark.read.table(names["resolution"]).where(F.col("lane_id") == lane_id)
    comp = F.struct(*[F.col(n) for n, _ in M.AXIS_FIELDS + M.R1_FIELDS])
    keyed = res.groupBy("map_key").agg(F.array_sort(F.collect_list(
        F.struct(F.col("component_index").alias("i"), comp.alias("c")))).alias("_cs"))
    return keyed.select("map_key", F.col("_cs")[0]["c"].alias("mapping"),
                        F.when(F.size("_cs") > 1, F.to_json(F.transform("_cs", lambda x: x["c"]))).alias("components"))


def _drop_projection_helpers(df):
    """Drop the projection columns (_r1_bridge, _r1_in_*) but keep the computed _r1_key_* columns. A table that feeds
    two models (map_numeric_events: vital_sign + clinical_score; nomen/text events: clinical_finding +
    presenting_complaint) is projected once per model with select("*", ...), so the first model's helpers must go
    before the second projection re-creates them under the same names."""
    return df.drop(*[c for c in df.columns if c.startswith("_r1_") and not c.startswith("_r1_key_")])


def attach_axes(df, target, broadcast_batch=True):
    """df plus the lane columns of `target` (no-op for a table without lanes). broadcast_batch: the small incoming
    batch is the broadcast side (landing); False for the one-off full rewrite (shuffle join, priced in P2)."""
    table = _bare(target)
    lanes = L.lanes_of_table(table)
    if not lanes:
        return df
    spark = SparkSession.getActiveSession()
    names = M.table_names(M.schemas_for(target))
    out = df.drop(*[c for c in L.r1_columns(table) if c in df.columns])
    base_cols = out.columns
    for model in dict.fromkeys(L.LANES[l].model for l in lanes):
        projected = L.project(out, table, model)
        for lane_id in [l for l in lanes if L.LANES[l].model == model]:
            projected = L.key_frame(projected, lane_id)
        out = _drop_projection_helpers(projected)
    for lane_id in lanes:
        mapping_col, key_col, comps_col = M.lane_columns(lane_id)
        key = f"_r1_key_{lane_id}"
        src = resolution_source(spark, names, lane_id)
        if broadcast_batch:
            batch_keys = out.select(F.col(key).alias("map_key")).where("map_key IS NOT NULL").distinct()
            src = F.broadcast(src.join(F.broadcast(batch_keys), "map_key", "left_semi"))
        out = (out.join(src.withColumnRenamed("map_key", key), key, "left")
               .withColumnRenamed(key, key_col).withColumnRenamed("mapping", mapping_col)
               .withColumnRenamed("components", comps_col))
    return out.select(*base_cols, *L.r1_columns(table))


def add_columns_sql(target, lanes, existing):
    """ALTER TABLE ADD COLUMNS for the lane columns not yet on the table, or None."""
    wanted = []
    for lane_id in lanes:
        mapping_col, key_col, comps_col = M.lane_columns(lane_id)
        wanted += [(mapping_col, M.axis_struct_ddl()), (key_col, "STRING"), (comps_col, "STRING")]
    missing = [(c, t) for c, t in wanted if c not in existing]
    if not missing:
        return None
    return f"ALTER TABLE {target} ADD COLUMNS (" + ", ".join(f"{c} {t}" for c, t in missing) + ")"


def changed_condition(lanes):
    """MERGE matched-condition: any lane whose key or resolution hash differs (null-safe)."""
    parts = []
    for lane_id in lanes:
        mapping_col, key_col, comps_col = M.lane_columns(lane_id)
        parts += [f"NOT (t.{key_col} <=> s.{key_col})",
                  f"NOT (t.{mapping_col}.resolution_hash <=> s.{mapping_col}.resolution_hash)"]
    return " OR ".join(parts)


def audit_row(spark, names, run_id, step, lane_id, table, status, metrics):
    """One mapping_resolver_audit row, written through the DataFrame API: the metrics JSON is a literal value, never
    pasted into SQL text (Spark SQL reads a backslash in a string literal as an escape and does not treat a doubled
    single quote as an escaped quote, so pasted JSON could be stored altered)."""
    (spark.range(1).select(F.lit(run_id).alias("run_id"), F.lit(step).alias("step"),
                           F.lit(lane_id).cast("string").alias("lane_id"), F.lit(table).cast("string").alias("table_name"),
                           F.lit(status).alias("status"), F.lit(json.dumps(metrics, default=str)).alias("metrics_json"),
                           F.current_timestamp().alias("at"))
     .write.mode("append").saveAsTable(names["audit"]))


def _audit(spark, names, run_id, step, table, status, metrics):
    """One mapping_resolver_audit row for a table-level attach step (no lane)."""
    audit_row(spark, names, run_id, step, None, table, status, metrics)


def _set_watermark(spark, names, target, version, run_id):
    """Record the last processed change-feed version of a target; skip versions at or below it are pruned."""
    spark.sql(f"""MERGE INTO {names['attach_state']} t
                  USING (SELECT '{target}' AS table_name, CAST({version} AS BIGINT) AS last_version) s
                  ON t.table_name = s.table_name
                  WHEN MATCHED THEN UPDATE SET t.last_version = s.last_version, t.updated_at = current_timestamp(),
                       t.run_id = '{run_id}',
                       t.skip_versions = filter(coalesce(t.skip_versions, array()), v -> v > s.last_version)
                  WHEN NOT MATCHED THEN INSERT (table_name, last_version, skip_versions, updated_at, run_id)
                       VALUES (s.table_name, s.last_version, array(), current_timestamp(), '{run_id}')""")


def _state(spark, names, target):
    """(last_version, skip_versions) of a target, or None when it was never attached."""
    rows = spark.read.table(names["attach_state"]).where(F.col("table_name") == target).collect()
    return (rows[0]["last_version"], list(rows[0]["skip_versions"] or [])) if rows else None


def record_own_commit(spark, names, target, version_before, version_after, run_id):
    """A commit R1 made itself (the backfill MERGE): nothing for the attach or the resolver to re-read. When no other
    commit happened in between, move the watermark past it; otherwise remember it in skip_versions."""
    state = _state(spark, names, target)
    if state is None:
        return
    if state[0] == version_before:
        _set_watermark(spark, names, target, version_after, run_id)
    else:
        spark.sql(f"UPDATE {names['attach_state']} SET skip_versions = array_union(coalesce(skip_versions, array()), "
                  f"array(CAST({version_after} AS BIGINT))) WHERE table_name = '{target}'")


def changed_keys(spark, target, keys, v0, v1, skip):
    """Row keys inserted or updated by commits v0+1..v1, minus R1's own commits (skip). Callers check v1 > v0 first:
    a change-feed read that starts past the latest version raises a Delta CDC range error."""
    cdf = (spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", v0 + 1)
           .option("endingVersion", v1).table(target)
           .where(F.col("_change_type").isin("insert", "update_postimage")))
    if skip:
        cdf = cdf.where(~F.col("_commit_version").isin(*skip))
    return cdf.select(*keys).distinct()


def changed_rows(spark, target, keys, v0, v1, skip):
    """Current rows of `target` whose row key changed in commits v0+1..v1 (minus `skip`). The semi-join is null-safe
    (<=>), like the attach MERGE: some row keys have parts that can be NULL (map_maternity_diagnosis LOCALFETALID and
    FETALORDER, map_coded_events SEQUENCE_NBR), and a plain equi-join would never match those rows, so they would
    never be attached or resolved incrementally."""
    # R1FIX_20261109: a commit in v0+1..v1 that rewrote the whole table changed every row, and a change-feed read
    # across it fails when the rewrite narrowed the schema (DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE, bronze
    # run 178236917345566). Every current row is then "changed": read the table itself.
    rewrites = DR.rewrite_versions(spark, target, v0 + 1, v1)
    if rewrites:
        print(f"[R1] {target}: whole-table rewrite at versions {rewrites}; reading the table, not the change feed")
        return spark.read.table(target)
    changed = changed_keys(spark, target, keys, v0, v1, skip)
    renamed = changed.select(*[F.col(k).alias(f"_r1_ck_{i}") for i, k in enumerate(keys)])
    on = [F.col(k).eqNullSafe(F.col(f"_r1_ck_{i}")) for i, k in enumerate(keys)]
    return spark.read.table(target).join(renamed, on, "left_semi")


def _latest_version(spark, target):
    """Current Delta version (metadata read)."""
    return spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()["version"]


def attach_changed(spark, target, run_id):
    """Post-landing attach: rows changed since the watermark get their keys and resolutions (MERGE on the row key,
    only where key or hash differ). Heals first when a full rebuild dropped the columns; re-tags when an inline
    overwrite dropped the column metadata."""
    names = M.table_names(M.schemas_for(target))
    table = _bare(target)
    healed = heal_if_dropped(spark, target, run_id)
    if healed:
        return {"table": table, "healed": healed}
    retagged = retag_if_missing(spark, target)
    state = _state(spark, names, target)
    assert state, f"{target}: no attach watermark (the P2 initial rewrite sets it)"
    (v0, skip), v1 = state, _latest_version(spark, target)
    if v1 <= v0:
        return {"table": table, "changed_rows": 0, "retagged": retagged}
    keys = L.ROW_KEYS[table]
    # R1FIX_20261109: after a whole-table rewrite every row needs attaching. Over 1bn rows that is the job-cluster
    # initial attach (R1_Mapping_Attach initial_tables), never this serverless task: audit it, keep the watermark.
    if table in LARGE and DR.rewrite_versions(spark, target, v0 + 1, v1):
        _audit(spark, names, run_id, "attach", table, "REWRITE_DEFERRED", {"from": v0, "to": v1})
        return {"table": table, "healed": "deferred", "reason": "rewrite of a table over 1bn rows"}
    rows = changed_rows(spark, target, keys, v0, v1, skip)
    attached = attach_axes(rows, target, broadcast_batch=True)
    from delta.tables import DeltaTable
    on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    updates = {c: f"s.{c}" for c in L.r1_columns(table)}
    (DeltaTable.forName(spark, target).alias("t").merge(attached.alias("s"), on)
     .whenMatchedUpdate(condition=changed_condition(L.lanes_of_table(table)), set=updates).execute())
    v2 = _latest_version(spark, target)
    _set_watermark(spark, names, target, v2 if v2 == v1 + 1 else v1, run_id)
    metrics = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()["operationMetrics"]
    return {"table": table, "from": v0, "to": v1, "updated": int(metrics.get("numTargetRowsUpdated", 0)),
            "retagged": retagged}


def _props(spark, target):
    """Table properties that must survive a rewrite (Enzyme, column mapping, type widening, clustering)."""
    keep = ("delta.enableRowTracking", "delta.enableChangeDataFeed", "delta.enableDeletionVectors",
            "delta.columnMapping.mode", "delta.enableTypeWidening", "clusteringColumns")
    return {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {target}").collect() if r["key"] in keep}


def _metadata_counts(spark, target):
    """(commented columns, column tags) of a target from information_schema: the rewrite must keep both."""
    catalog, schema, table = target.replace("`", "").split(".")
    comments = spark.sql(f"SELECT count(comment) AS n FROM `{catalog}`.information_schema.columns "
                         f"WHERE table_schema = '{schema}' AND table_name = '{table}'").first().n
    tags = spark.sql(f"SELECT count(*) AS n FROM `{catalog}`.information_schema.column_tags "
                     f"WHERE schema_name = '{schema}' AND table_name = '{table}'").first().n
    return int(comments), int(tags)


def _carried_columns(columns, table):
    """The columns the initial rewrite must carry unchanged: all but the R1 lane columns and the row-hash column
    post_r1_row_hash recomputes (fold-in R1-b)."""
    recomputed = RH.recomputed_column(table)
    return [c for c in columns if not M.is_r1_column(c) and c != recomputed]


def _rewrite_checksum(spark, target, carried, version=None):
    """(rows, sum of xxhash64(ADC_UPDT), sum of xxhash64(every carried column)) at a version, in ONE scan: proves
    ADC_UPDT and every other non-R1 column were carried unchanged (DECIMAL sums cannot overflow)."""
    reader = spark.read.option("versionAsOf", version) if version is not None else spark.read
    df = reader.table(target)
    col = "ADC_UPDT" if "ADC_UPDT" in df.columns else df.columns[0]
    r = df.agg(F.count(F.lit(1)).alias("n"), F.sum(F.xxhash64(F.col(col)).cast("decimal(38,0)")).alias("h"),
               F.sum(F.xxhash64(*[F.col(f"`{c}`") for c in carried]).cast("decimal(38,0)")).alias("o")).first()
    return int(r.n), str(r.h), str(r.o)


def _fill_counts(spark, target, lanes):
    """{lane: {"keyed": rows with a map key, "attached": rows with a resolved STRUCT}} in ONE scan of the target
    (one .agg over every lane, not one count per lane)."""
    aggs = []
    for lane_id in lanes:
        mapping_col, key_col, _ = M.lane_columns(lane_id)
        aggs += [F.sum(F.col(key_col).isNotNull().cast("long")).alias(f"{lane_id}__keyed"),
                 F.sum(F.col(mapping_col).isNotNull().cast("long")).alias(f"{lane_id}__attached")]
    row = spark.read.table(target).agg(*aggs).first()
    return {lane_id: {"keyed": int(row[f"{lane_id}__keyed"] or 0), "attached": int(row[f"{lane_id}__attached"] or 0)}
            for lane_id in lanes}


def initial_rewrite(spark, target, run_id):
    """P2 (and heal): add the lane columns, then overwrite the table's data once with every row attached.

    The write is `INSERT OVERWRITE TABLE <target> SELECT <every column, in order>` from a temp view registered in the
    same call: one of the design's probed forms, which keeps table properties, clustering, column comments and tags.
    (A DataFrame overwrite with save-as-table is NOT used: Databricks logs it as CREATE OR REPLACE TABLE AS SELECT
    and it dropped the comments of all 1,162 commented bronze columns on 2026-10-11.) The guard compares properties,
    row count, the ADC_UPDT checksum, a checksum of every other carried column and the counts of column comments and
    column tags before and after.

    Fold-in R1-b: the stored row hash is written with the post-INSR builder formula (mapping_rowhash), so the first
    weekly run after INSR does not rewrite the table a second time. Only that column and the R1 columns may differ."""
    names = M.table_names(M.schemas_for(target))
    table = _bare(target)
    lanes = L.lanes_of_table(table)
    before_props = _props(spark, target)
    alter = add_columns_sql(target, lanes, set(spark.read.table(target).columns))
    if alter:
        spark.sql(alter)
    before_meta = _metadata_counts(spark, target)
    version = _latest_version(spark, target)
    current = spark.read.option("versionAsOf", version).table(target)
    order = current.columns
    carried = _carried_columns(order, table)
    before = _rewrite_checksum(spark, target, carried, version)
    attached = RH.post_r1_row_hash(attach_axes(current, target, broadcast_batch=False), table).select(*order)
    view = f"_r1_rewrite_{table}"
    attached.createOrReplaceTempView(view)               # same call as the INSERT: serverless-safe
    spark.sql(f"INSERT OVERWRITE TABLE {target} SELECT " + ", ".join(f"`{c}`" for c in order) + f" FROM {view}")
    spark.catalog.dropTempView(view)
    after = _rewrite_checksum(spark, target, carried)
    assert after[:2] == before[:2], f"{target}: row count or ADC_UPDT changed by the rewrite: {before} -> {after}"
    assert after[2] == before[2], f"{target}: a non-R1 column other than the recomputed row hash changed"
    assert _props(spark, target) == before_props, f"{target}: properties changed: {before_props} -> {_props(spark, target)}"
    after_meta = _metadata_counts(spark, target)
    assert after_meta == before_meta, f"{target}: column comments/tags changed by the rewrite: {before_meta} -> {after_meta}"
    _set_watermark(spark, names, target, _latest_version(spark, target), run_id)
    fill = _fill_counts(spark, target, lanes)
    _audit(spark, names, run_id, "initial_rewrite", table, "OK",
           {"rows": before[0], "fill": fill, "comments_tags": list(after_meta)})
    return {"table": table, "rows": before[0], "fill": fill}


def tag_source(lane_id):
    """(gold table, column) whose estate tags the three lane columns copy: the axis' source display."""
    lane = L.LANES[lane_id]
    return GOLD_TABLE[lane.model], f"{lane.axis}_source_display"


def tag_and_comment(spark, target):
    """Comments, plus ig tags copied from the estate (estate_tags.tags_like), on every lane column of a target."""
    from estate_tags import tag_statements, tags_like
    table = _bare(target)
    stmts = []
    for lane_id in L.lanes_of_table(table):
        tags = tags_like(spark, *tag_source(lane_id))
        cols = M.lane_columns(lane_id)
        for col, suffix in zip(cols, ("MAPPING", "MAP_KEY", "MAP_COMPONENTS_JSON")):
            text = COMMENTS[suffix].format(lane=lane_id, key=cols[1]).replace("'", "''")
            stmts.append(f"ALTER TABLE {target} ALTER COLUMN {col} COMMENT '{text}'")
        stmts += tag_statements(target, "TABLE", {c: tags for c in cols})
    for sql in stmts:
        spark.sql(sql)
    return len(stmts)


def retag_if_missing(spark, target):
    """True after re-applying comments and tags to a target whose R1 columns lost them (an inline overwrite with
    overwriteSchema drops column metadata; the builder's own _r1_retag normally restores it in the same run)."""
    catalog, schema, table = target.replace("`", "").split(".")
    cols = L.r1_columns(table)
    if not cols:
        return False
    names = ", ".join(f"'{c}'" for c in cols)
    tagged = spark.sql(f"SELECT count(*) AS n FROM (SELECT column_name FROM `{catalog}`.information_schema.column_tags "
                       f"WHERE schema_name = '{schema}' AND table_name = '{table}' AND column_name IN ({names}) "
                       "AND tag_name IN ('ig_risk', 'ig_severity') GROUP BY 1 HAVING count(DISTINCT tag_name) = 2)").first().n
    commented = spark.sql(f"SELECT count(comment) AS n FROM `{catalog}`.information_schema.columns "
                          f"WHERE table_schema = '{schema}' AND table_name = '{table}' AND column_name IN ({names})").first().n
    if tagged == len(cols) and commented == len(cols):
        return False
    tag_and_comment(spark, target)
    return True


LARGE = {"map_pathology", "map_numeric_events", "map_nomen_events", "map_coded_events"}   # over 1bn rows


def heal_if_dropped(spark, target, run_id):
    """Re-add, re-attach and re-tag lane columns a full-rebuild fallback dropped from an ATTACHED table.

    Returns False when nothing is missing, and also when the table has no attach_state row: never attached is not
    dropped (the P2 initial attach owns it; this keeps a weekly run that lands mid-P2 from rewriting). A table over
    1bn rows is not rewritten here (serverless task, outside the job-cluster rule and decision 6): the audit records
    HEAL_DEFERRED and the caller skips the table until initial_attach_on_cluster runs."""
    table = _bare(target)
    existing = set(spark.read.table(target).columns)
    missing = sorted(set(L.r1_columns(table)) - existing)
    if not missing:
        return False
    names = M.table_names(M.schemas_for(target))
    if _state(spark, names, target) is None:
        return False
    if table in LARGE:
        _audit(spark, names, run_id, "heal", table, "HEAL_DEFERRED", {"missing": missing})
        return "deferred"
    initial_rewrite(spark, target, run_id)
    tag_and_comment(spark, target)
    _audit(spark, names, run_id, "heal", table, "HEALED", {"missing": missing})
    return True


def attach_silver_arm(df, source, model, read_table=None):
    """D4: rows of a silver-derived arm (no bronze row) get _r1_<lane>_mapping / _r1_<lane>_components_json, the
    values a bronze row's pregate passes through: the shared projection and key, then component 1 of the lane's
    resolution (these lanes never split, so no aggregation: an equi-join Enzyme plans incrementally).

    `read_table` reads the resolution table by its prod name; silver passes its read_source, so a dev pipeline's
    journey.source_overrides routes it to the R1 dev copy (review fix 11-02). Default: spark.read.table."""
    spark = SparkSession.getActiveSession()
    read_table = read_table or spark.read.table
    names = M.table_names(M.schemas_for("4_prod.bronze.x"))
    lanes = [l for l in L.lanes_of_model(model) if source in L.LANES[l].silver_sources]
    projected = L.project(df, source, model)
    for lane_id in lanes:
        assert not L.LANES[lane_id].allow_split, f"{lane_id}: a splitting lane cannot be silver-derived"
        projected = L.key_frame(projected, lane_id)
    out = projected
    for lane_id in lanes:
        mapping_col, key_col, comps_col = M.lane_columns(lane_id)
        comp1 = (read_table(names["resolution"]).where((F.col("lane_id") == lane_id) & (F.col("component_index") == 1))
                 .select(F.col("map_key").alias(f"_r1_key_{lane_id}"),
                         F.struct(*[F.col(n) for n, _ in M.AXIS_FIELDS + M.R1_FIELDS]).alias(mapping_col)))
        out = (out.join(comp1, f"_r1_key_{lane_id}", "left")
               .withColumn(comps_col, F.lit(None).cast("string")))
    # The key travels with the mapping (Ben's 2026-11-01 ruling): silver publishes it as <axis>_map_key.
    return out.select(*df.columns, *[F.col(c).alias(f"_r1_{l}_{suffix}") for l in lanes
                                     for c, suffix in ((M.lane_columns(l)[0], "mapping"),
                                                       (f"_r1_key_{l}", "map_key"),
                                                       (M.lane_columns(l)[2], "components_json"))])
