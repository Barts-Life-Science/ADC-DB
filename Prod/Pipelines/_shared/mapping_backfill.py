"""R1 change-driven backfill (decision 6, ruled 2026-11-01: 200M rows or $50 per lane per run, all-or-nothing).

For each lane with pending changed keys: price the lane from mapping_resolution_usage, then either
  (1) MERGE the new resolution onto the bronze rows carrying those keys (key-scoped; deletion vectors, no file
      rewrite: R1 sizing probe), (2) replace the keys' rows in mapping_resolution, (3) mark the ledger applied;
or defer the whole lane (audit row DEFERRED = the cost alert) and leave it pending; the next run applies it
regardless of size (APPLIED_AFTER_DEFER, estimate logged).
Steps (1)-(3) are idempotent, so a crash between them is closed by the next run; bronze and the resolution table
never disagree after a completed run.
"""
ROW_BOUND = 200_000_000
USD_BOUND = 50.0
USD_PER_MILLION_ROWS = 0.04     # R1 sizing probe: 25.4M rows ~ $0.5-1
SCAN_USD_PER_GB = 0.002         # R1 sizing probe: key-column scan of map_pathology (199.6 GB) ~ $0.4
BROADCAST_KEYS = 1_000_000      # above this many pending keys the MERGE source is shuffled, not broadcast (driver memory)


def lane_decision(rows_by_table, size_gb_by_table, row_bound=ROW_BOUND, usd_bound=USD_BOUND, deferred_before=False):
    """{"apply", "rows", "usd", "reason"} for one lane's pending change (pure; unit-tested). A lane deferred on an
    earlier run applies now regardless of the bounds (decision 6: "applied in full on the next run")."""
    rows = sum(rows_by_table.values())
    usd = (sum(SCAN_USD_PER_GB * size_gb_by_table.get(t, 0.0) for t in rows_by_table)
           + rows / 1e6 * USD_PER_MILLION_ROWS)
    if deferred_before:
        return {"apply": True, "rows": rows, "usd": round(usd, 2), "reason": "after_defer"}
    if rows > row_bound:
        return {"apply": False, "rows": rows, "usd": round(usd, 2), "reason": "rows"}
    if usd > usd_bound:
        return {"apply": False, "rows": rows, "usd": round(usd, 2), "reason": "usd"}
    return {"apply": True, "rows": rows, "usd": round(usd, 2), "reason": ""}


def status_of(decision):
    """Audit status of a decision: APPLIED, APPLIED_AFTER_DEFER or DEFERRED."""
    if not decision["apply"]:
        return "DEFERRED"
    return "APPLIED_AFTER_DEFER" if decision["reason"] == "after_defer" else "APPLIED"


def _deferred_before(spark, names, lane_id, run_id):
    """True when an earlier run deferred this lane (an audit DEFERRED row from another run) and its changes are
    still pending (the oldest pending staged_at is not after that deferral)."""
    import pyspark.sql.functions as F
    last = (spark.read.table(names["audit"])
            .where((F.col("step") == "backfill") & (F.col("lane_id") == lane_id) & (F.col("status") == "DEFERRED")
                   & (F.col("run_id") != run_id))
            .agg(F.max("at").alias("at")).first().at)
    if last is None:
        return False
    oldest = (spark.read.table(names["change"]).where((F.col("lane_id") == lane_id) & (F.col("status") == "pending"))
              .agg(F.min("staged_at").alias("t")).first().t)
    return oldest is not None and oldest <= last


def release_open(spark, names):
    """True while R4's release marker has an open row. An absent marker table means no release machinery yet, so
    no release is open (dev, and prod before INSR P0)."""
    table = names["release_state"]
    if not spark.catalog.tableExists(table.replace("`", "")):
        return False
    return spark.sql(f"SELECT 1 FROM {table} WHERE status = 'open' LIMIT 1").count() > 0


def _audit_backfill(spark, names, run_id, lane_id, status, metrics):
    """One mapping_resolver_audit row for a lane's backfill decision (mapping_attach.audit_row: DataFrame write, so
    the metrics JSON is stored exactly)."""
    import mapping_attach as A
    A.audit_row(spark, names, run_id, "backfill", lane_id, None, status, metrics)


def _maybe_broadcast(df, n_keys):
    """Broadcast hint for a small pending-key frame only; an after-defer lane can carry millions of keys."""
    import pyspark.sql.functions as F
    return F.broadcast(df) if n_keys <= BROADCAST_KEYS else df


def _components_schema():
    """Schema of new_components_json (what mapping_resolver.resolve_lane serialised)."""
    import mapping_contract as M
    fields = ["component_index:int"] + [f"{n}:{t}" for n, t in M.AXIS_FIELDS] + ["extras_json:string",
                                                                                 "component_identity:string"]
    return "array<struct<" + ",".join(fields) + ">>"


def _table_sizes_gb(spark, tables):
    """{table: size in GB} from DESCRIBE DETAIL (metadata only)."""
    return {t: spark.sql(f"DESCRIBE DETAIL `4_prod`.bronze.{t}").first()["sizeInBytes"] / 1e9 for t in tables
            if spark.catalog.tableExists(f"4_prod.bronze.{t}")}


def apply_lane(spark, names, lane_id, run_id, target_schema="`4_prod`.bronze"):
    """Price, then apply or defer, one lane's pending changes. Returns the decision plus what was written."""
    import pyspark.sql.functions as F
    from delta.tables import DeltaTable
    from pyspark.sql import Window
    import mapping_contract as M
    import mapping_lanes as L
    lane = L.LANES[lane_id]
    latest = Window.partitionBy("map_key").orderBy(F.col("staged_at").desc_nulls_last(), F.col("run_id").desc())
    pending = (spark.read.table(names["change"]).where((F.col("lane_id") == lane_id) & (F.col("status") == "pending"))
               .withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn"))
    n_keys = pending.count()
    if not n_keys:
        return {"lane": lane_id, "pending_keys": 0}
    if release_open(spark, names):
        # Ben's 2026-11-01 ruling: no lane change lands while a release is open, so gold's per-key verdicts and the
        # silver rows they gate come from one resolution. The first run after the close applies the lane.
        metrics = {"pending_keys": n_keys, "reason": "release_open"}
        _audit_backfill(spark, names, run_id, lane_id, "HELD_RELEASE_OPEN", metrics)
        return {"lane": lane_id, "status": "HELD_RELEASE_OPEN", **metrics}
    usage = spark.read.table(names["usage"]).where(F.col("lane_id") == lane_id)
    rows_by_table = {r["bronze_table"]: int(r["rows"]) for r in usage.join(pending.select("map_key"), "map_key")
                     .groupBy("bronze_table").agg(F.sum("row_count").alias("rows")).collect()
                     if r["bronze_table"] in lane.tables}
    decision = lane_decision(rows_by_table, _table_sizes_gb(spark, rows_by_table),
                             row_bound=ROW_BOUND, usd_bound=USD_BOUND,      # read at call time (Task 14 lowers them)
                             deferred_before=_deferred_before(spark, names, lane_id, run_id))
    status = status_of(decision)
    heal_pending = []                  # lane tables without lane columns (heal deferred): re-attached on the job cluster
    if decision["apply"]:
        comps = (pending.withColumn("_cs", F.from_json("new_components_json", _components_schema()))
                 .withColumn("_cs", F.array_sort("_cs")))
        mapping = F.struct(*[F.col("_cs")[0][n].alias(n) for n, _ in M.AXIS_FIELDS],
                           F.col("_cs")[0]["extras_json"].alias("extras_json"),
                           F.col("_cs")[0]["component_identity"].alias("component_identity"),
                           F.col("new_hash").alias("resolution_hash"), F.lit(M.RESOLVER_VERSION).alias("resolver_version"))
        src = comps.select("map_key", "new_hash", mapping.alias("mapping"),
                           F.when(F.size("_cs") > 1, F.to_json(F.transform("_cs", lambda c: F.struct(
                               *[c[n].alias(n) for n, _ in M.AXIS_FIELDS], c["extras_json"].alias("extras_json"),
                               c["component_identity"].alias("component_identity"))))).alias("components"))
        import mapping_attach as A
        for table in lane.tables:                                                     # (1) bronze, key-scoped
            target = f"{target_schema}.{table}"
            if not spark.catalog.tableExists(target.replace("`", "")):
                continue
            mapping_col, key_col, comps_col = M.lane_columns(lane_id)
            if key_col not in spark.read.table(target).columns:
                # A >1bn-row table whose columns a full rebuild dropped (attach_changed returned HEAL_DEFERRED): it has
                # no lane column to MERGE into. Its re-attach (initial_attach_on_cluster) reads mapping_resolution,
                # which step (2) brings up to date, so the table still ends on this resolution.
                heal_pending.append(table)
                continue
            before = A._latest_version(spark, target)
            # The matched condition names <LANE>_MAPPING.resolution_hash (A.OWN_COMMIT_MARKER): readers recognise
            # this commit as R1's own from DESCRIBE HISTORY (pathology_incremental's optional skip, D13).
            (DeltaTable.forName(spark, target).alias("t")
             .merge(_maybe_broadcast(src, n_keys).alias("s"), f"t.{key_col} = s.map_key")
             .whenMatchedUpdate(condition=f"NOT (t.{mapping_col}.resolution_hash <=> s.new_hash)",
                                set={mapping_col: "s.mapping", comps_col: "s.components"}).execute())
            # R1's own commit: the next attach and incremental resolver skip it instead of re-reading up to 200M rows
            A.record_own_commit(spark, names, target, before, A._latest_version(spark, target), run_id)
        keys = pending.select("map_key")                                              # (2) resolution
        DeltaTable.forName(spark, names["resolution"]).alias("r").merge(
            _maybe_broadcast(keys, n_keys).alias("k"), f"r.lane_id = '{lane_id}' AND r.map_key = k.map_key").whenMatchedDelete().execute()
        lookup_json = spark.read.table(names["lane"]).where(F.col("lane_id") == lane_id).first()["input_maps_hash_json"]
        (comps.select("map_key", "new_hash", F.explode("_cs").alias("c"))
         .select(F.lit(lane_id).alias("lane_id"), "map_key", F.col("c.component_index").alias("component_index"),
                 *[F.col(f"c.{n}").alias(n) for n, _ in M.AXIS_FIELDS], F.col("c.extras_json").alias("extras_json"),
                 F.col("c.component_identity").alias("component_identity"), F.col("new_hash").alias("resolution_hash"),
                 F.lit(M.RESOLVER_VERSION).alias("resolver_version"), F.lit(lookup_json).alias("lookup_versions_json"),
                 F.current_timestamp().alias("resolved_at"))
         .write.mode("append").saveAsTable(names["resolution"]))
        spark.sql(f"UPDATE {names['change']} SET status = 'applied', applied_at = current_timestamp() "  # (3) ledger
                  f"WHERE lane_id = '{lane_id}' AND status = 'pending'")
    metrics = {"pending_keys": n_keys, "rows_by_table": rows_by_table, **decision}
    if heal_pending:
        metrics["heal_pending_tables"] = heal_pending
    _audit_backfill(spark, names, run_id, lane_id, status, metrics)
    return {"lane": lane_id, "status": status, **metrics}


def recount_usage(spark, names, target_schema="`4_prod`.bronze", silver_map=None):
    """Full recount of mapping_resolution_usage from the bronze key columns (and the silver-derived arms): one
    key-column aggregate per (table, lane); about $0.5 a run (R1 sizing: 1.9bn-row key scan 1,514 task-s)."""
    import pyspark.sql.functions as F
    import mapping_contract as M
    import mapping_lanes as L
    frames = []
    for lane_id, lane in L.LANES.items():
        for table in lane.tables:
            if not spark.catalog.tableExists(f"{target_schema}.{table}".replace("`", "")):
                continue
            df = spark.read.table(f"{target_schema}.{table}")
            ts = F.col("ADC_UPDT") if "ADC_UPDT" in df.columns else F.lit(None).cast("timestamp")
            key_col = M.lane_columns(lane_id)[1]
            if key_col not in df.columns:
                continue      # columns dropped by a full rebuild, heal deferred to the job cluster: counted after it
            frames.append(df.where(F.col(key_col).isNotNull()).groupBy(F.col(key_col).alias("map_key")).agg(
                F.count(F.lit(1)).alias("row_count"), F.min(ts).alias("first_seen"), F.max(ts).alias("last_seen"))
                .select(F.lit(lane_id).alias("lane_id"), "map_key", F.lit(table).alias("bronze_table"),
                        "row_count", "first_seen", "last_seen"))
        for source in lane.silver_sources:
            name = (silver_map or {}).get(source, f"`4_prod`.tmp.{source}")
            if not spark.catalog.tableExists(name.replace("`", "")):
                continue      # not published yet (R2 publishes the arm tables; dev has no specimen stand-in)
            rows = spark.read.table(name)
            keyed = L.key_frame(L.project(rows, source, lane.model), lane_id)
            key = F.col(f"_r1_key_{lane_id}")
            frames.append(keyed.where(key.isNotNull()).groupBy(key.alias("map_key")).agg(
                F.count(F.lit(1)).alias("row_count"), F.lit(None).cast("timestamp").alias("first_seen"),
                F.lit(None).cast("timestamp").alias("last_seen"))
                .select(F.lit(lane_id).alias("lane_id"), "map_key", F.lit(f"silver:{source}").alias("bronze_table"),
                        "row_count", "first_seen", "last_seen"))
    for arm in L.native_arms():                                       # native arms: registry usage only
        name = f"{target_schema}.{arm['table']}"
        if not spark.catalog.tableExists(name.replace("`", "")):
            continue
        df = spark.read.table(name)
        ts = F.col("ADC_UPDT") if "ADC_UPDT" in df.columns else F.lit(None).cast("timestamp")
        keyed = (df.select(F.expr(arm["coding_system"]).cast("string").alias("source_system"),
                           F.expr(arm["code"]).cast("string").alias("source_code"), ts.alias("_ts"))
                 .where(F.col("source_code").isNotNull())
                 .withColumn("map_key", M.map_key_expr(arm["lane_id"], ["source_system", "source_code"])))
        frames.append(keyed.groupBy("map_key").agg(F.count(F.lit(1)).alias("row_count"), F.min("_ts").alias("first_seen"),
                                                   F.max("_ts").alias("last_seen"))
                      .select(F.lit(arm["lane_id"]).alias("lane_id"), "map_key", F.lit(arm["table"]).alias("bronze_table"),
                              "row_count", "first_seen", "last_seen"))
    usage = frames[0]
    for frame in frames[1:]:
        usage = usage.unionByName(frame)
    # INSERT OVERWRITE, never saveAsTable(overwrite): the table carries P8 comments and tags (CTAS-style overwrite
    # drops them). Temp view and INSERT in the same call (serverless).
    usage.withColumn("counted_at", F.current_timestamp()).createOrReplaceTempView("_r1_usage_recount")
    spark.sql(f"INSERT OVERWRITE TABLE {names['usage']} SELECT lane_id, map_key, bronze_table, row_count, first_seen, "
              "last_seen, counted_at FROM _r1_usage_recount")
    spark.catalog.dropTempView("_r1_usage_recount")
    return spark.read.table(names["usage"]).count()
