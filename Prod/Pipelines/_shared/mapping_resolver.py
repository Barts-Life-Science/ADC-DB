"""R1 resolver: resolve new keys (and every key of a lane whose input maps changed), write new keys to
mapping_resolution, stage changed keys as pending in mapping_resolution_change (decision 6 promotes them with the
bronze backfill, in mapping_backfill), and keep the crosswalk copy current.

The environment variables that mapping_chain reads at import (R1_CHAIN_CONF, R1_READ_AS_OF, R1_CROSSWALK_TABLE) are
set by prepare_environment() before mapping_lanes / mapping_chain are imported.
"""
import json
import os


def full_resolve(stored_hashes, current_hashes, mode):
    """True when the lane must re-resolve every key: initial/parity mode, never resolved, or any map's content
    hash changed."""
    return mode in ("initial", "parity") or stored_hashes is None or stored_hashes != current_hashes


def classify(current_hash, new_hash, old_version=None, new_version=None, conf_changed=False):
    """new | changed | same for one key. After a chain-conf (hierarchy) change, a key whose map_version moved is
    'changed' even with an equal clinical hash: the gold gate matches map_version against the policy (D12)."""
    if current_hash is None:
        return "new"
    if current_hash != new_hash or (conf_changed and old_version != new_version):
        return "changed"
    return "same"


def trigger_hashes(map_hashes_, chain_conf_json, lane_id=None):
    """The lane's stored trigger: input-map content hashes plus the chain conf and the resolver version, so a
    hierarchy bump or a resolver release re-resolves the lane in full (D12).

    A lane listed in mapping_lanes.LANE_RULE_VERSIONS also carries its rule version (COMPLAINT_PICKLIST_TEXT_V1):
    a rule change scoped to one lane re-resolves that lane alone in full, once, instead of a RESOLVER_VERSION bump
    re-resolving every lane. A lane not listed gets exactly the trigger it had before, so it does not re-resolve."""
    import mapping_contract as M
    import mapping_lanes as L
    trigger = {**map_hashes_, "__chain_conf__": chain_conf_json, "__resolver_version__": M.RESOLVER_VERSION}
    if lane_id in L.LANE_RULE_VERSIONS:
        trigger["__lane_rule_version__"] = L.LANE_RULE_VERSIONS[lane_id]
    return trigger


def prepare_environment(spark, names, read_as_of=None):
    """Set the chain's import-time environment from mapping_lane (chain conf) and the run's options, and the session's
    SQL semantics to silver's: the verbatim chain and projections were written for Lakeflow, which runs them with ANSI
    off (element_at(filter(...), 1) on an empty coding array is NULL there; with ANSI on, the serverless job default,
    it raises INVALID_ARRAY_INDEX: Task 14 dev run, condition lane)."""
    spark.conf.set("spark.sql.ansi.enabled", "false")
    rows = spark.read.table(names["lane"]).select("chain_conf_json").distinct().collect()
    assert len(rows) == 1, "lanes disagree on chain_conf_json"
    os.environ["R1_CHAIN_CONF"] = rows[0]["chain_conf_json"]
    os.environ["R1_CROSSWALK_TABLE"] = names["crosswalk"]
    os.environ.setdefault("R1_LOOKUP_OVERRIDES", "{}")    # dev runs set it before calling (plan conventions)
    if read_as_of:
        os.environ["R1_READ_AS_OF"] = read_as_of


def map_hashes(spark, maps, read_as_of=None):
    """{map: "rows:sum(xxhash64(row json))"}: an order-independent content hash per input map. Dev overrides
    (R1_LOOKUP_OVERRIDES) are hashed at their current version, as mapping_chain.read_source reads them."""
    import pyspark.sql.functions as F
    import mapping_contract as M
    overrides = json.loads(os.environ.get("R1_LOOKUP_OVERRIDES", "{}"))
    out = {}
    for name in sorted(maps):
        if name in overrides:
            reader, source = spark.read, overrides[name]
        else:
            reader = M.reader_as_of(spark, name, read_as_of)
            source = name
        r = reader.table(source).agg(F.count(F.lit(1)).alias("n"),
                                   F.sum(F.xxhash64(F.to_json(F.struct("*"))).cast("decimal(38,0)")).alias("h")).first()
        out[name] = f"{r.n}:{r.h}"
    return out


def refresh_crosswalk(spark, names, concept_changed, read_as_of=None):
    """The silver private _snomed_concept_crosswalk (1_silver_spine_and_reference.py:226-233), as a table, rebuilt
    only when 3_lookup.omop.concept content changed (or the copy is empty); in parity mode always rebuilt from the
    concept table as of T."""
    if not read_as_of and not concept_changed and spark.read.table(names["crosswalk"]).limit(1).count():
        return False
    import mapping_contract as M
    as_of = M.sql_as_of(spark, "`3_lookup`.omop.concept", read_as_of)
    spark.sql(f"""INSERT OVERWRITE {names['crosswalk']}
                  SELECT CAST(concept_id AS BIGINT), CAST(concept_code AS STRING), concept_name, standard_concept
                  FROM `3_lookup`.omop.concept{as_of} WHERE vocabulary_id = 'SNOMED' AND invalid_reason IS NULL""")
    return True


def concept_changed_since(stored_by_lane, concept_hash):
    """True when any lane that tracks 3_lookup.omop.concept stored a different concept hash than the current one.
    Lanes that never read the concept table (no key in their stored trigger) do not count."""
    key = "3_lookup.omop.concept"
    return any(key in stored and stored[key] != concept_hash for stored in stored_by_lane.values())


def _arm_frames(spark, lane, source_map, silver_map, since):
    """Projected and keyed rows of every arm of a lane. source_map/silver_map repoint tables (dev samples, P2
    physical private copies); since={table: (version, skip_versions)} limits bronze arms to rows changed after that
    version, minus R1's own backfill commits. A table that does not exist (yet) is skipped and listed; a table with
    no commit since its watermark contributes nothing (a change-feed read past the latest version raises)."""
    import mapping_attach as A
    import mapping_lanes as L
    frames, skipped = [], []
    for table in lane.tables:
        name = source_map.get(table, f"`4_prod`.bronze.{table}")
        if not spark.catalog.tableExists(name.replace("`", "")):
            skipped.append(table)
            continue
        if since and table in since:
            v0, skip = since[table]
            v1 = A._latest_version(spark, name)
            if v1 <= v0:
                continue
            rows = A.changed_rows(spark, name, L.ROW_KEYS[table], v0, v1, skip)     # null-safe on the row key
        else:
            rows = spark.read.table(name)
        frames.append(L.key_frame(L.project(rows, table, lane.model), lane.lane_id))
    for source in lane.silver_sources:
        name = silver_map.get(source, f"`4_prod`.tmp.{source}")
        if not spark.catalog.tableExists(name.replace("`", "")):
            skipped.append(source)        # an arm table not published yet: skipped and listed, like a bronze table
            continue
        rows = spark.read.table(name)
        frames.append(L.key_frame(L.project(rows, source, lane.model), lane.lane_id))
    return frames, skipped


def resolve_lane(spark, names, lane_id, run_id, mode, source_map=None, silver_map=None, read_as_of=None):
    """One lane: inputs -> resolution -> diff. Returns counts; writes new keys and stages changed keys."""
    import pyspark.sql.functions as F
    from delta.tables import DeltaTable
    import mapping_contract as M
    import mapping_lanes as L
    lane = L.LANES[lane_id]
    lane_row = spark.read.table(names["lane"]).where(F.col("lane_id") == lane_id).first()
    current_hashes = trigger_hashes(map_hashes(spark, lane.input_maps, read_as_of), lane_row["chain_conf_json"],
                                    lane_id)
    stored = json.loads(lane_row["input_maps_hash_json"]) if lane_row["input_maps_hash_json"] else None
    full = full_resolve(stored, current_hashes, mode)
    conf_changed = stored is not None and stored.get("__chain_conf__") != current_hashes["__chain_conf__"]

    # Incremental: only bronze rows changed since each table's attach watermark feed the lane.
    since = None
    if not full:
        since = {r["table_name"].split(".")[-1].strip("`"): (r["last_version"], list(r["skip_versions"] or []))
                 for r in spark.read.table(names["attach_state"]).collect()}
    frames, skipped = _arm_frames(spark, lane, source_map or {}, silver_map or {}, since)
    if not frames:
        return {"lane": lane_id, "full": full, "skipped_tables": skipped, "new": 0}

    # Distinct inputs over every arm; incremental runs resolve only keys never resolved before.
    inputs = L.distinct_inputs(frames[0], lane_id)
    for frame in frames[1:]:
        inputs = inputs.unionByName(L.distinct_inputs(frame, lane_id))
    inputs = inputs.dropDuplicates(["map_key"])
    current = (spark.read.table(names["resolution"]).where((F.col("lane_id") == lane_id) & (F.col("component_index") == 1))
               .select("map_key", F.col("resolution_hash").alias("_cur_hash"), F.col("map_version").alias("_cur_ver")))
    if not full:
        inputs = inputs.join(current, "map_key", "left_anti")          # only keys never resolved

    # Resolve, group components per key, and check the chain did not multiply rows.
    resolved = L.resolve(inputs, lane_id)
    components = resolved.groupBy("map_key").agg(F.collect_list(F.struct(*[c for c in resolved.columns
                                                                         if c != "map_key"])).alias("_cs"),
                                                 F.count(F.lit(1)).alias("_n"),
                                                 F.countDistinct("component_index").alias("_d"),
                                                 F.max("component_index").alias("_m"))
    bad = components.where("NOT (_n = _d AND _d = _m)").limit(5).collect()
    assert not bad, f"{lane_id}: components not 1..n per key (chain multiplied rows): {[r.map_key for r in bad]}"

    # Hash and classify every key (the classify() rule, as a column); stage once, read three times (no .cache()).
    hashed = (components.withColumn("resolution_hash", M.resolution_hash_of_components(F.col("_cs")))
              .withColumn("_new_ver", F.element_at(F.array_sort("_cs"), 1)["map_version"]))
    version_moved = F.lit(conf_changed) & ~F.col("_cur_ver").eqNullSafe(F.col("_new_ver"))
    classified = hashed.join(current, "map_key", "left").withColumn(
        "_kind", F.when(F.col("_cur_hash").isNull(), "new")
        .when((F.col("_cur_hash") == F.col("resolution_hash")) & ~version_moved, "same").otherwise("changed"))
    stage_table = f"{names['change']}_stage_{lane_id}"
    classified.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(stage_table)
    staged = spark.read.table(stage_table)

    # New keys go straight into mapping_resolution (nothing on bronze carries them yet).
    lookup_json = json.dumps(current_hashes, sort_keys=True)
    new_rows = (staged.where("_kind = 'new'").select("map_key", "resolution_hash", F.explode("_cs").alias("c"))
                .select(F.lit(lane_id).alias("lane_id"), "map_key", F.col("c.component_index").alias("component_index"),
                        *[F.col(f"c.{n}").alias(n) for n, _ in M.AXIS_FIELDS],
                        F.col("c.extras_json").alias("extras_json"), F.col("c.component_identity").alias("component_identity"),
                        "resolution_hash", F.lit(M.RESOLVER_VERSION).alias("resolver_version"),
                        F.lit(lookup_json).alias("lookup_versions_json"), F.current_timestamp().alias("resolved_at")))
    new_rows.write.mode("append").saveAsTable(names["resolution"])

    # Ledger: new keys applied now; changed keys pending until the backfill promotes them (decision 6).
    ledger = (staged.where("_kind IN ('new', 'changed')")
              .select(F.lit(run_id).alias("run_id"), F.lit(lane_id).alias("lane_id"), "map_key",
                      F.col("_kind").alias("change_kind"), F.col("_cur_hash").alias("old_hash"),
                      F.col("resolution_hash").alias("new_hash"),
                      F.when(F.col("_kind") == "changed", F.to_json("_cs")).alias("new_components_json"),
                      F.when(F.col("_kind") == "new", F.lit("applied")).otherwise(F.lit("pending")).alias("status"),
                      F.lit(None).cast("bigint").alias("rows_estimate"), F.lit(None).cast("double").alias("usd_estimate"),
                      F.current_timestamp().alias("staged_at"),
                      F.when(F.col("_kind") == "new", F.current_timestamp()).alias("applied_at")))
    ledger.write.mode("append").saveAsTable(names["change"])
    counts = {r["_kind"]: r["n"] for r in staged.groupBy("_kind").agg(F.count(F.lit(1)).alias("n")).collect()}
    spark.sql(f"DROP TABLE IF EXISTS {stage_table}")

    # Store the lane's trigger hashes (a DataFrame-API update: the JSON carries quotes and backslashes).
    DeltaTable.forName(spark, names["lane"]).update(
        condition=F.col("lane_id") == lane_id,
        set={"input_maps_hash_json": F.lit(lookup_json), "updated_at": F.current_timestamp()})
    return {"lane": lane_id, "full": full, "skipped_tables": skipped, **counts}


def resolve_native(spark, names, arm, run_id, source_map=None):
    """One native arm (design: "the resolver also writes native keys, with method native"): distinct coded values
    of a feed column no lane maps, written as component-1 resolutions with map_method 'native' and the source code
    as the concept. No chain; new keys only (displays sit outside the hash, so nothing else ever changes)."""
    import pyspark.sql.functions as F
    import mapping_contract as M
    name = (source_map or {}).get(arm["table"], f"`4_prod`.bronze.{arm['table']}")
    if not spark.catalog.tableExists(name.replace("`", "")):
        return {"lane": arm["lane_id"], "skipped_tables": [arm["table"]], "new": 0}
    values = (spark.read.table(name)
              .select(F.expr(arm["coding_system"]).cast("string").alias("source_system"),
                      F.expr(arm["code"]).cast("string").alias("source_code"),
                      F.expr(arm["display"]).cast("string").alias("source_display"))
              .where(F.col("source_code").isNotNull())
              .groupBy("source_system", "source_code").agg(F.max("source_display").alias("source_display"))
              .withColumn("map_key", M.map_key_expr(arm["lane_id"], ["source_system", "source_code"])))
    known = spark.read.table(names["resolution"]).where(F.col("lane_id") == arm["lane_id"]).select("map_key")
    fresh = values.join(known, "map_key", "left_anti")
    fields = {n: F.lit(None).cast(t) for n, t in M.AXIS_FIELDS}
    fields.update(source_system=F.col("source_system"), source_code=F.col("source_code"),
                  source_display=F.col("source_display"), map_method=F.lit("native"), map_status=F.lit("native"),
                  map_component_index=F.lit(1), map_component_count=F.lit(1))
    comp = F.struct(*[fields[n].alias(n) for n, _ in M.AXIS_FIELDS], F.lit(None).cast("string").alias("extras_json"),
                    F.lit(None).cast("string").alias("component_identity"))
    rows = fresh.select("map_key", F.array(comp).alias("_cs"))
    (rows.select(F.lit(arm["lane_id"]).alias("lane_id"), "map_key", F.lit(1).alias("component_index"),
                 *[F.col("_cs")[0][n].alias(n) for n, _ in M.AXIS_FIELDS],
                 F.lit(None).cast("string").alias("extras_json"), F.lit(None).cast("string").alias("component_identity"),
                 M.resolution_hash_of_components(F.col("_cs")).alias("resolution_hash"),
                 F.lit(M.RESOLVER_VERSION).alias("resolver_version"), F.lit("{}").alias("lookup_versions_json"),
                 F.current_timestamp().alias("resolved_at"))
     .write.mode("append").saveAsTable(names["resolution"]))
    return {"lane": arm["lane_id"], "keys": spark.read.table(names["resolution"])
            .where(F.col("lane_id") == arm["lane_id"]).count()}
