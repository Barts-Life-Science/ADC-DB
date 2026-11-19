"""R1 resolver: resolve new keys (and every key of a lane whose input maps changed), write new keys to
mapping_resolution, stage changed keys as pending in mapping_resolution_change (decision 6 promotes them with the
bronze backfill, in mapping_backfill), and keep the crosswalk copy current.

The environment variables that mapping_chain reads at import (R1_CHAIN_CONF, R1_READ_AS_OF, R1_CROSSWALK_TABLE) are
set by prepare_environment() before mapping_lanes / mapping_chain are imported.
"""
import functools
import json
import os
import threading
import time

# BFIX_RESOLVER_PARALLEL_V1 (BRONZE_FIX_20261116): the resolver timed out in weekly bronze run 1021647626060126 with
# 8 of 25 lanes done in 60 minutes. Two causes: every lane re-hashed its lookup maps (the 7.5M-row concept table,
# the 3.3M-row SNOMED RF2 table and others, once per lane that lists them), and the lanes ran one at a time.
# Each lookup map is now hashed once per run (the maps do not change while the resolver runs), and resolve_lanes
# runs several lanes at once. The only state lanes share is this cache and the mapping_lane table, whose
# one-row-per-lane UPDATE is serialised below; everything else a lane writes is a blind append (never conflicts in
# Delta) or its own stage table.
_HASH_CACHE = {}                    # (map, read_as_of, source table) -> "rows:hash"
_HASH_CACHE_GUARD = threading.Lock()
_HASH_KEY_LOCKS = {}                # one lock per map, so two lanes never hash the same map at the same time
_LANE_TABLE_LOCK = threading.Lock()

# BFIX_RESOLVER_READONCE_V1 (RESOLVER_READONCE_20261117): per-run scratch tables of distinct arm key inputs, one per
# (source table, model, read window). Filled only while resolve_lanes runs (_ARM_STAGING["run"] is set).
_ARM_STAGING = {"run": None, "names": None, "tables": []}
_ARM_STAGES = {}                    # (source, model, window) -> scratch table name
_ARM_GUARD = threading.Lock()
_ARM_KEY_LOCKS = {}


def reset_hash_cache():
    """Forget every cached map hash (the dev parity proof runs the resolver twice in one Python process)."""
    with _HASH_CACHE_GUARD:
        _HASH_CACHE.clear()
        _HASH_KEY_LOCKS.clear()


def _cached_hash(cache_key, compute):
    """Return the cached value for cache_key, computing it at most once per run. Lanes asking for the same map at the
    same time wait for the one computation (a per-map lock); different maps are hashed in parallel."""
    with _HASH_CACHE_GUARD:
        if cache_key in _HASH_CACHE:
            return _HASH_CACHE[cache_key]
        key_lock = _HASH_KEY_LOCKS.setdefault(cache_key, threading.Lock())
    with key_lock:
        with _HASH_CACHE_GUARD:
            if cache_key in _HASH_CACHE:
                return _HASH_CACHE[cache_key]
        value = compute()
        with _HASH_CACHE_GUARD:
            _HASH_CACHE[cache_key] = value
        return value


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
    overrides = json.loads(os.environ.get("R1_LOOKUP_OVERRIDES", "{}"))
    out = {}
    for name in sorted(maps):
        source = overrides.get(name, name)
        # BFIX_RESOLVER_PARALLEL_V1: computed once per run per (map, as-of, source), then served from the cache.
        compute = functools.partial(_content_hash, spark, name, source, name in overrides, read_as_of)
        out[name] = _cached_hash((name, read_as_of, source), compute)
    return out


def _content_hash(spark, name, source, is_override, read_as_of):
    """"rows:sum(xxhash64(row json))" of one input map: exactly the computation map_hashes always did. A dev override
    is read at its current version; a prod map at `read_as_of` when one is given (parity runs)."""
    import pyspark.sql.functions as F
    import mapping_contract as M
    reader = spark.read if is_override else M.reader_as_of(spark, name, read_as_of)
    r = reader.table(source).agg(F.count(F.lit(1)).alias("n"),
                               F.sum(F.xxhash64(F.to_json(F.struct("*"))).cast("decimal(38,0)")).alias("h")).first()
    return f"{r.n}:{r.h}"


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
            window = ("changes", v0, v1, tuple(sorted(skip)))

            def rows(name=name, table=table, v0=v0, v1=v1, skip=skip):
                """This arm's rows changed since the watermark (null-safe on the row key)."""
                return A.changed_rows(spark, name, L.ROW_KEYS[table], v0, v1, skip)
        else:
            window = ("full",)

            def rows(name=name):
                """This arm's rows, all of them."""
                return spark.read.table(name)
        frames.append(_arm_frame(spark, name, table, lane, window, rows))
    for source in lane.silver_sources:
        name = silver_map.get(source, f"`4_prod`.tmp.{source}")
        if not spark.catalog.tableExists(name.replace("`", "")):
            skipped.append(source)        # an arm table not published yet: skipped and listed, like a bronze table
            continue
        frames.append(_arm_frame(spark, name, source, lane, ("full",), lambda name=name: spark.read.table(name)))
    return frames, skipped


def _arm_frame(spark, name, table, lane, window, rows):
    """One arm's keyed frame for a lane. Inside resolve_lanes (BFIX_RESOLVER_READONCE_V1) the projection of
    (name, model, window) is built once per run and shared by every lane of the model; otherwise as before."""
    import mapping_lanes as L
    if _ARM_STAGING["run"] is None:
        return L.key_frame(L.project(rows(), table, lane.model), lane.lane_id)
    staged = _staged_arm_inputs(spark, name, table, lane.model, window, rows)
    return L.key_frame(spark.read.table(staged), lane.lane_id)


def _model_key_inputs(model):
    """Every key input column of every lane of a model, in registry order (what any of its lanes may key on)."""
    import mapping_lanes as L
    columns = []
    for lane in L.LANES.values():
        if lane.model == model:
            columns += [c for c in lane.key_inputs if c not in columns]
    return columns


def _staged_arm_inputs(spark, name, table, model, window, rows):
    """Name of the scratch table holding the distinct in-scope key inputs of project(rows, table, model), built at
    most once per run. Rows outside scope never get a map key (key_frame), so dropping them first changes nothing;
    distinct_inputs keeps one row per key, so keeping one row per distinct key-input combination changes nothing."""
    import pyspark.sql.functions as F
    import mapping_lanes as L
    key = (name.replace("`", ""), model, window)
    with _ARM_GUARD:
        if key in _ARM_STAGES:
            return _ARM_STAGES[key]
        key_lock = _ARM_KEY_LOCKS.setdefault(key, threading.Lock())
    with key_lock:
        with _ARM_GUARD:
            if key in _ARM_STAGES:
                return _ARM_STAGES[key]
        started = time.monotonic()
        projected = L.project(rows(), table, model)
        columns = [c for c in _model_key_inputs(model) if c in projected.columns]
        distinct = (projected.where(F.col("_r1_in_scope"))
                    .select(F.lit(True).alias("_r1_in_scope"), *columns).dropDuplicates(columns))
        with _ARM_GUARD:
            stage = f"{_ARM_STAGING['names']['change']}_arm_{_ARM_STAGING['run']}_{len(_ARM_STAGING['tables'])}"
            _ARM_STAGING["tables"].append(stage)
        distinct.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(stage)
        print(f"[R1] staged {model} inputs of {key[0]} {window[0]} -> {stage} in "
              f"{round(time.monotonic() - started, 1)} s", flush=True)
        with _ARM_GUARD:
            _ARM_STAGES[key] = stage
        return stage


def _drop_arm_stages(spark):
    """Drop this run's scratch tables and forget them (always called when resolve_lanes ends)."""
    for stage in list(_ARM_STAGING["tables"]):
        spark.sql(f"DROP TABLE IF EXISTS {stage}")
    _ARM_STAGING.update({"run": None, "names": None, "tables": []})
    _ARM_STAGES.clear()
    _ARM_KEY_LOCKS.clear()


def resolve_lane(spark, names, lane_id, run_id, mode, source_map=None, silver_map=None, read_as_of=None):
    """One lane: inputs -> resolution -> diff. Returns counts and the lane's wall time in seconds; writes new keys and
    stages changed keys."""
    started = time.monotonic()      # BFIX_RESOLVER_PARALLEL_V1: per-lane timing for the run's audit row
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
        return {"lane": lane_id, "full": full, "skipped_tables": skipped, "new": 0,
                "seconds": round(time.monotonic() - started, 1)}

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
    # BFIX_RESOLVER_CHAIN_ONCE_V1: the chain's plan is huge (dozens of lookup joins; up to 48 GB of plan description per
    # query on 2026-11-17) and was planned and run twice per lane (the component check below, then the stage write).
    # Inside resolve_lanes it is written once to a scratch table, and both uses read that.
    resolved_table = None
    if _ARM_STAGING["run"] is not None:
        resolved_table = f"{names['change']}_resolved_{lane_id}"
        with _ARM_GUARD:                 # also dropped when the run ends, if this lane fails after writing it
            _ARM_STAGING["tables"].append(resolved_table)
        resolved.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(resolved_table)
        resolved = spark.read.table(resolved_table)
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
    if resolved_table:
        spark.sql(f"DROP TABLE IF EXISTS {resolved_table}")

    # Store the lane's trigger hashes (a DataFrame-API update: the JSON carries quotes and backslashes).
    # BFIX_RESOLVER_PARALLEL_V1: mapping_lane is one small file, so two lanes updating it at once would conflict in
    # Delta; the updates take turns.
    with _LANE_TABLE_LOCK:
        DeltaTable.forName(spark, names["lane"]).update(
            condition=F.col("lane_id") == lane_id,
            set={"input_maps_hash_json": F.lit(lookup_json), "updated_at": F.current_timestamp()})
    return {"lane": lane_id, "full": full, "skipped_tables": skipped, **counts,
            "seconds": round(time.monotonic() - started, 1)}


def resolve_lanes(spark, names, lane_ids, run_id, mode, source_map=None, silver_map=None, read_as_of=None,
                  workers=1):
    """Resolve several lanes, `workers` at a time (BFIX_RESOLVER_PARALLEL_V1), with the unchanged resolve_lane.

    Returns the results in the order of lane_ids. A lane that fails does not stop the others: every lane is tried,
    then one error naming every failed lane is raised. What a finished lane wrote stays committed (the same as a
    sequential run that stops part-way)."""
    from concurrent.futures import ThreadPoolExecutor

    def resolve_one(lane_id):
        """Resolve one lane and print its result as soon as it finishes, so the task log shows progress."""
        result = resolve_lane(spark, names, lane_id, run_id, mode, source_map, silver_map, read_as_of)
        print(result, flush=True)
        return result

    import hashlib
    results, errors = {}, {}
    # BFIX_RESOLVER_READONCE_V1: lanes of one model share one projection of each input per run (scratch tables in
    # the change table's schema, dropped when the run ends, even on failure).
    _drop_arm_stages(spark)
    _ARM_STAGING.update({"run": hashlib.sha256(str(run_id).encode()).hexdigest()[:10], "names": names, "tables": []})
    try:
        with ThreadPoolExecutor(max_workers=max(1, int(workers)), thread_name_prefix="r1-lane") as pool:
            futures = {lane_id: pool.submit(resolve_one, lane_id) for lane_id in lane_ids}
            for lane_id, future in futures.items():
                try:
                    results[lane_id] = future.result()
                except Exception as error:      # collected; raised together below
                    errors[lane_id] = error
                    print(f"[R1] lane {lane_id} FAILED: {error!r}"[:2000], flush=True)
    finally:
        _drop_arm_stages(spark)
    if errors:
        first = next(iter(errors.values()))
        raise RuntimeError(f"R1 resolver: {len(errors)} lane(s) failed: {sorted(errors)}; first error: {first!r}"[:4000]) \
            from first
    return [results[lane_id] for lane_id in lane_ids]


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
