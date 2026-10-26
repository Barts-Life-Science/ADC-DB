"""CDF state and match-group-scoped execution for pathology sidecars."""

from __future__ import annotations

from pathology_contracts import CONTRACT_VERSION
from pathology_pipeline import (
    TLC_SPLIT_PATTERN,
    PipelineConfig,
    ensure_contracts,
    run_core,
    specimen_map_table,
)


# STM_SPECIMEN_TYPE_V1/incremental: specimen type reads three more sources. A source without a state row forces FULL, so the
# first run after this change is the one-off full reconcile of both branches.
def _state_sources(config: PipelineConfig) -> dict[str, str]:
    return {
        "map_pathology": config.map_pathology_table,
        "path_patient_samplelevel": config.sample_table,
        "mill_order_detail": config.order_detail_table,
        "specimen_map": specimen_map_table(config),
        "path_master_orderables": config.master_orderable_table,
    }


def _imports():
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    return DeltaTable, F


def current_delta_version(spark, table_name: str) -> int:
    row = spark.sql(f"DESCRIBE HISTORY {table_name}").selectExpr("max(version) AS version").first()
    return int(row["version"])


def _state(spark, config: PipelineConfig) -> dict[str, dict[str, object]]:
    rows = spark.table(f"{config.bronze_schema}.pathology_expansion_state").collect()
    return {row["source_name"]: row.asDict() for row in rows}


def _read_changes(spark, table_name: str, start_version: int, end_version: int):
    if start_version > end_version:
        return None
    escaped = table_name.replace("'", "''")
    return spark.sql(
        f"SELECT * FROM table_changes('{escaped}', {start_version}, {end_version})"
    )



# PATH_EXPANSION_LANES_V1: map_pathology columns each expansion lane reads, derived from the code by AST (functions that
# read config.map_pathology_table, their same-module helpers, and the output contracts they pass mp.* into) and checked
# against Unity Catalog column lineage (~/path_expansion_hash_fix_20261011/analysis/derive_read_sets.py). A
# map_pathology update re-expands its parent only when a core column changed. When only an equivalence column changed
# (measurement_concept_id, ucum_code, unit_source_value), only map_pathology_result_equivalence is recomputed for the
# parent's accessions. Updates touching neither (concept names, SNOMED/LOINC codes, tiers, mapping provenance,
# canonical units, ...) re-expand nothing. No stored state changes: the hashes compare each update's pre- and post-image.
EXPANSION_CORE_COLUMNS = ('ENCNTR_ID', 'LIMSNo', 'MRN', 'NHS_Number', 'PERSON_ID', 'ReportDate', 'WkgCode', 'authentic_flag', 'body_site_code', 'code', 'description', 'lab_no', 'measurement_datetime', 'order_id', 'order_no', 'performed_dt_tm', 'person_match_status', 'report_section', 'request_dt', 'result_status', 'sample_dt', 'source_code', 'source_parent_key', 'source_record_key', 'source_sequence_end', 'source_sequence_start', 'source_table', 'specimen_category', 'specimen_type_code', 'urgent_flag', 'value_source_value', 'verified_dt_tm')
EXPANSION_EQUIVALENCE_COLUMNS = ('code', 'measurement_concept_id', 'measurement_datetime', 'source_parent_key', 'source_record_key', 'source_table', 'ucum_code', 'unit_source_value', 'value_source_value')
_EQUIVALENCE_ONLY = [None]


def _lane_payload(changes, columns):
    _, F = _imports()
    present = [column for column in columns if column in changes.columns]
    return F.xxhash64(*[F.col(column) for column in present])


def _equivalence_only_parent_changes(changes):
    """(source_table, source_parent_key, source_record_key) of map_pathology updates whose core columns are unchanged
    but whose equivalence columns changed."""
    if changes is None:
        return None
    _, F = _imports()
    prepared = changes.filter(F.col('_change_type').isin('update_preimage', 'update_postimage')).select(
        'source_record_key', 'source_table', 'source_parent_key',
        F.col('_commit_version').alias('_commit_version'), F.col('_change_type').alias('_change_type'),
        _lane_payload(changes, EXPANSION_CORE_COLUMNS).alias('_CORE'),
        _lane_payload(changes, EXPANSION_EQUIVALENCE_COLUMNS).alias('_EQ'),
    )
    before = prepared.filter(F.col('_change_type') == 'update_preimage').alias('b')
    after = prepared.filter(F.col('_change_type') == 'update_postimage').alias('a')
    return (
        before.join(after, F.col('b.source_record_key').eqNullSafe(F.col('a.source_record_key'))
                    & (F.col('b._commit_version') == F.col('a._commit_version')), 'inner')
        .where(F.col('b._CORE').eqNullSafe(F.col('a._CORE')) & ~F.col('b._EQ').eqNullSafe(F.col('a._EQ')))
        .select(F.col('a.source_table').alias('source_table'), F.col('a.source_parent_key').alias('source_parent_key'),
                F.col('a.source_record_key').alias('source_record_key'))
        .filter(F.col('source_parent_key').isNotNull()).dropDuplicates()
    )


def _run_equivalence_only_lane(spark, config, core_touched, run_id):
    """Recompute map_pathology_result_equivalence alone for the equivalence-only parents of this run (parents the
    core run already recomputed are skipped). Returns the lane metrics, or None when there is nothing to do."""
    changed = _EQUIVALENCE_ONLY[0]
    if changed is None:
        return None
    _, F = _imports()
    parents = changed.select(
        F.when(F.col('source_table') == 'raw', 'TFC_LIMS').otherwise('CERNER').alias('source_system'), 'source_parent_key'
    ).dropDuplicates()
    if core_touched is not None:
        parents = parents.join(core_touched.select('source_system', 'source_parent_key'),
                               ['source_system', 'source_parent_key'], 'left_anti')
    count = parents.count()
    if count == 0:
        return None
    from pathology_pipeline import run_result_equivalence_only
    records = changed.select('source_record_key').dropDuplicates()
    metrics = run_result_equivalence_only(spark, config, parents, record_keys=records)
    log_run(spark, config, run_id=run_id, mode='INCREMENTAL', status='SUCCESS', stage='result_equivalence_only',
            message=str(metrics)[:8000], source_parent_count=count)
    return metrics

def _semantic_map_parent_changes(changes):
    """Discard map_pathology updates that changed only operational provenance."""
    if changes is None:
        return None
    _, F = _imports()
    ignored = {
        '_change_type', '_commit_version', '_commit_timestamp',
        'ADC_UPDT', 'source_adc_updt', 'mapping_updated_at', 'loaded_at',
        'source_payload_hash',
    }
    semantic_columns = [column for column in EXPANSION_CORE_COLUMNS if column in changes.columns and column not in ignored]  # PATH_EXPANSION_LANES_V1
    payload = F.xxhash64(*[F.col(column) for column in semantic_columns])
    prepared = changes.select(
        'source_record_key', 'source_table', 'source_parent_key',
        F.col('_commit_version').alias('_commit_version'),
        F.col('_change_type').alias('_change_type'),
        payload.alias('_SEMANTIC_PAYLOAD'),
    )
    direct = prepared.filter(F.col('_change_type').isin('insert', 'delete')).select(
        'source_table', 'source_parent_key'
    )
    before = prepared.filter(F.col('_change_type') == 'update_preimage').alias('b')
    after = prepared.filter(F.col('_change_type') == 'update_postimage').alias('a')
    updates = (
        before.join(
            after,
            F.col('b.source_record_key').eqNullSafe(F.col('a.source_record_key'))
            & (F.col('b._commit_version') == F.col('a._commit_version')),
            'full',
        )
        .where(~F.col('b._SEMANTIC_PAYLOAD').eqNullSafe(F.col('a._SEMANTIC_PAYLOAD')))
        .select(
            F.coalesce(F.col('a.source_table'), F.col('b.source_table')).alias('source_table'),
            F.coalesce(F.col('a.source_parent_key'), F.col('b.source_parent_key')).alias('source_parent_key'),
        )
    )
    return direct.unionByName(updates).filter(F.col('source_parent_key').isNotNull()).dropDuplicates()


def _semantic_specimen_map_keys(changes):
    """(system, code) keys whose map payload changed; a key that became WkgCode-split also yields its LIMS base."""
    _, F = _imports()
    keys = ["specimen_type_source_system", "specimen_type_code"]
    ignored = {"_change_type", "_commit_version", "_commit_timestamp", "ADC_UPDT", "created_at", "row_weight"}
    payload = F.xxhash64(*[F.col(column) for column in changes.columns if column not in ignored])
    prepared = changes.select(*keys, "_commit_version", "_change_type", payload.alias("_SEMANTIC_PAYLOAD"))
    direct = prepared.filter(F.col("_change_type").isin("insert", "delete")).select(*keys)
    before = prepared.filter(F.col("_change_type") == "update_preimage").alias("b")
    after = prepared.filter(F.col("_change_type") == "update_postimage").alias("a")
    updates = (
        before.join(
            after,
            [F.col(f"b.{k}").eqNullSafe(F.col(f"a.{k}")) for k in keys]
            + [F.col("b._commit_version") == F.col("a._commit_version")],
            "full",
        )
        .where(~F.col("b._SEMANTIC_PAYLOAD").eqNullSafe(F.col("a._SEMANTIC_PAYLOAD")))
        .select(*[F.coalesce(F.col(f"a.{k}"), F.col(f"b.{k}")).alias(k) for k in keys])
    )
    changed = direct.unionByName(updates)
    base = changed.select(
        F.regexp_replace("specimen_type_source_system", r"(:tfc:lims[0-9]+):[^:]+$", "$1").alias("specimen_type_source_system"),
        "specimen_type_code",
    )
    return changed.unionByName(base).filter(F.col("specimen_type_code").isNotNull()).dropDuplicates()

# PATH_ORDERABLES_SEMANTIC_V1 (BRONZE_FIX_20261025): raw.path_master_orderables is rebuilt weekly with CREATE OR
# REPLACE, so its change feed removes and re-adds every row even when nothing changed. Compare what was removed with
# what was added, per (WkgCode, TLCCode), on the columns the specimen default reads; only keys that really differ count.
ORDERABLE_SPECIMEN_COLUMNS = ("CSpecTypeCode", "LastUpdatedDT")


def _semantic_orderable_keys(changes):
    """(_wkg, _tlc) keys whose specimen-default payload differs between removed and added change-feed rows."""
    _, F = _imports()
    keyed = changes.select(
        F.upper(F.trim("WkgCode")).alias("_wkg"),
        F.upper(F.trim("TLCCode")).alias("_tlc"),
        F.xxhash64(*[F.col(column) for column in ORDERABLE_SPECIMEN_COLUMNS]).alias("_payload"),
        F.col("_change_type").isin("delete", "update_preimage").alias("_removed"),
    )
    removed = keyed.where("_removed").groupBy("_wkg", "_tlc", "_payload").count().withColumnRenamed("count", "_n_removed")
    added = keyed.where("NOT _removed").groupBy("_wkg", "_tlc", "_payload").count().withColumnRenamed("count", "_n_added")
    same_key = (
        F.col("r._wkg").eqNullSafe(F.col("a._wkg"))
        & F.col("r._tlc").eqNullSafe(F.col("a._tlc"))
        & F.col("r._payload").eqNullSafe(F.col("a._payload"))
    )
    return (
        removed.alias("r")
        .join(added.alias("a"), same_key, "full")
        .where(F.coalesce(F.col("r._n_removed"), F.lit(0)) != F.coalesce(F.col("a._n_added"), F.lit(0)))
        .select(
            F.coalesce(F.col("r._wkg"), F.col("a._wkg")).alias("_wkg"),
            F.coalesce(F.col("r._tlc"), F.col("a._tlc")).alias("_tlc"),
        )
        .dropDuplicates()
    )


def changed_parent_keys(spark, config: PipelineConfig):
    """Return touched source parents and pending source versions.

    A missing state row means a full build is required. CDF failures are raised;
    this pipeline deliberately has no silent billion-row snapshot fallback.
    """

    _, F = _imports()
    _EQUIVALENCE_ONLY[0] = None  # PATH_EXPANSION_LANES_V1
    state = _state(spark, config)
    sources = _state_sources(config)
    versions = {name: current_delta_version(spark, table) for name, table in sources.items()}
    if any(name not in state or state[name]["last_delta_version"] is None for name in sources):
        return None, versions

    frames = []
    map_start = int(state["map_pathology"]["last_delta_version"]) + 1
    map_changes = _read_changes(
        spark, config.map_pathology_table, map_start, versions["map_pathology"]
    )
    if map_changes is not None:
        semantic_map_changes = _semantic_map_parent_changes(map_changes)
        _EQUIVALENCE_ONLY[0] = _equivalence_only_parent_changes(map_changes)  # PATH_EXPANSION_LANES_V1
        frames.append(
            semantic_map_changes.select(
                F.when(F.col("source_table") == "raw", "TFC_LIMS")
                .otherwise("CERNER")
                .alias("source_system"),
                "source_parent_key",
            )
        )

    sample_start = int(state["path_patient_samplelevel"]["last_delta_version"]) + 1
    sample_changes = _read_changes(
        spark, config.sample_table, sample_start, versions["path_patient_samplelevel"]
    )
    if sample_changes is not None:
        frames.append(
            sample_changes.filter(F.col("_change_type") != "update_preimage").select(
                F.lit("TFC_LIMS").alias("source_system"),
                F.concat_ws(
                    "|",
                    F.lit("raw"),
                    F.coalesce(F.col("LIMSNo").cast("string"), F.lit("∅")),
                    F.coalesce(F.col("LabNo"), F.lit("∅")),
                ).alias("source_parent_key"),
            )
        )
    # STM_SPECIMEN_TYPE_V1/incremental: order-detail, specimen-map and orderables-default changes reach existing accessions.
    current_sources = spark.table(
        f"{config.bronze_schema}.map_pathology_accession_source"
    ).filter(F.col("is_current") == True)
    detail_start = int(state["mill_order_detail"]["last_delta_version"]) + 1
    detail_changes = _read_changes(
        spark, config.order_detail_table, detail_start, versions["mill_order_detail"]
    )
    if detail_changes is not None:
        changed_orders = (
            detail_changes.filter(F.col("OE_FIELD_MEANING").isin("SPECIMEN TYPE", "BODYSITE"))
            .select(F.col("ORDER_ID").cast("long").alias("order_id"))
            .dropDuplicates()
        )
        frames.append(
            current_sources.filter(F.col("source_system") == "CERNER")
            .join(changed_orders, "order_id", "inner")
            .select("source_system", "source_parent_key")
        )
    specimen_start = int(state["specimen_map"]["last_delta_version"]) + 1
    specimen_changes = _read_changes(
        spark, specimen_map_table(config), specimen_start, versions["specimen_map"]
    )
    if specimen_changes is not None:
        frames.append(
            current_sources.join(
                _semantic_specimen_map_keys(specimen_changes),
                ["specimen_type_source_system", "specimen_type_code"],
                "inner",
            ).select("source_system", "source_parent_key")
        )
    orderables_start = int(state["path_master_orderables"]["last_delta_version"]) + 1
    orderables_changes = _read_changes(
        spark, config.master_orderable_table, orderables_start, versions["path_master_orderables"]
    )
    if orderables_changes is not None:
        changed_tlcs = _semantic_orderable_keys(orderables_changes)  # PATH_ORDERABLES_SEMANTIC_V1
        requested = spark.table(config.sample_table).select(
            F.col("LIMSNo").cast("int").alias("LIMSNo"), F.col("LabNo").alias("lab_no"), "TLCsRequested"
        )
        frames.append(
            current_sources.filter(
                (F.col("source_system") == "TFC_LIMS")
                & (
                    F.col("specimen_type_code").isNull()
                    | (F.col("specimen_type_derivation") == "orderables_default")
                )
            )
            .join(requested, ["LIMSNo", "lab_no"], "inner")
            .select(
                "source_system",
                "source_parent_key",
                F.upper(F.trim("wkg_code")).alias("_wkg"),
                F.explode(F.split(F.col("TLCsRequested"), TLC_SPLIT_PATTERN)).alias("_tlc"),
            )
            .withColumn("_tlc", F.upper(F.trim("_tlc")))
            .join(F.broadcast(changed_tlcs), ["_wkg", "_tlc"], "inner")
            .select("source_system", "source_parent_key")
        )
    if not frames:
        return spark.createDataFrame([], "source_system string, source_parent_key string"), versions
    output = frames[0]
    for frame in frames[1:]:
        output = output.unionByName(frame)
    return output.dropDuplicates(), versions


def commit_state(
    spark,
    config: PipelineConfig,
    versions: dict[str, int],
    run_id: str,
) -> None:
    DeltaTable, F = _imports()
    table_names = _state_sources(config)
    rows = [
        (name, table_names[name], int(version), run_id, CONTRACT_VERSION)
        for name, version in versions.items()
    ]
    stage = (
        spark.createDataFrame(
            rows,
            "source_name string, table_name string, last_delta_version long, run_id string, contract_version string",
        )
        .withColumn("last_success_at", F.current_timestamp())
    )
    (
        DeltaTable.forName(spark, f"{config.bronze_schema}.pathology_expansion_state")
        .alias("t")
        .merge(stage.alias("s"), "t.source_name=s.source_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def log_run(
    spark,
    config: PipelineConfig,
    *,
    run_id: str,
    mode: str,
    status: str,
    stage: str,
    message: str,
    source_parent_count: int | None = None,
    match_group_count: int | None = None,
) -> None:
    _, F = _imports()
    spark.createDataFrame(
        [
            (
                run_id,
                mode,
                status,
                stage,
                source_parent_count,
                match_group_count,
                message,
                CONTRACT_VERSION,
            )
        ],
        "run_id string, mode string, status string, stage string, source_parent_count long, match_group_count long, message string, contract_version string",
    ).withColumn("started_at", F.current_timestamp()).withColumn(
        "completed_at", F.current_timestamp()
    ).withColumn(
        "inserted_rows", F.lit(None).cast("long")
    ).withColumn(
        "updated_rows", F.lit(None).cast("long")
    ).withColumn(
        "deleted_rows", F.lit(None).cast("long")
    ).select(
        "run_id",
        "started_at",
        "completed_at",
        "mode",
        "status",
        "stage",
        "source_parent_count",
        "match_group_count",
        "inserted_rows",
        "updated_rows",
        "deleted_rows",
        "message",
        "contract_version",
    ).write.mode("append").saveAsTable(
        f"{config.bronze_schema}.pathology_expansion_run_log"
    )


def _stage_touched(spark, config: PipelineConfig, touched, run_id: str):
    """Write the touched-parent set once to a run-scoped Delta table and read it back.

    PATH_TOUCHED_STAGE_V1 (2026-10-18): the set is derived lazily from five change feeds. Unstaged, the
    count, the core and the equivalence lane each re-read those feeds.
    """
    table = f"{config.bronze_schema}._pathology_expansion_touched_{run_id.replace('-', '')}"
    (
        touched.select("source_system", "source_parent_key")
        .dropDuplicates()
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table)
    )
    return spark.table(table), table


def run_incremental_core(
    spark,
    config: PipelineConfig | None = None,
    *,
    validate_stage_keys: bool = True,
):
    config = config or PipelineConfig()
    ensure_contracts(spark, config)
    run_id = spark.sql("SELECT uuid() AS id").first()["id"]
    touched, versions = changed_parent_keys(spark, config)
    if touched is None:
        mode = "FULL"
        log_run(
            spark,
            config,
            run_id=run_id,
            mode=mode,
            status="STARTED",
            stage="core",
            message="No complete CDF state; running initial full reconciliation",
        )
        metrics = run_core(
            spark,
            config,
            full_reconcile=True,
            validate_stage_keys=validate_stage_keys,
        )
    else:
        touched, touched_table = _stage_touched(spark, config, touched, run_id)
        count = touched.count()
        mode = "INCREMENTAL"
        if count == 0:
            _run_equivalence_only_lane(spark, config, None, run_id)  # PATH_EXPANSION_LANES_V1
            commit_state(spark, config, versions, run_id)
            log_run(
                spark,
                config,
                run_id=run_id,
                mode=mode,
                status="SUCCESS",
                stage="core",
                message="No touched source parents",
                source_parent_count=0,
            )
            spark.sql(f"DROP TABLE IF EXISTS {touched_table}")
            return {"run_id": run_id, "mode": mode, "metrics": {}}
        log_run(
            spark,
            config,
            run_id=run_id,
            mode=mode,
            status="STARTED",
            stage="core",
            message="Recomputing complete match groups for touched parents",
            source_parent_count=count,
        )
        try:
            metrics = run_core(
                spark,
                config,
                full_reconcile=False,
                touched_parent_keys=touched,
                validate_stage_keys=validate_stage_keys,
            )
        except Exception as error:
            # A failed core used to leave only a STARTED row; record the failure and where it happened.
            log_run(
                spark,
                config,
                run_id=run_id,
                mode=mode,
                status="FAILED",
                stage="core",
                message=repr(error)[:8000],
                source_parent_count=count,
            )
            spark.sql(f"DROP TABLE IF EXISTS {touched_table}")
            raise
    if touched is not None:
        _run_equivalence_only_lane(spark, config, touched, run_id)  # PATH_EXPANSION_LANES_V1
    commit_state(spark, config, versions, run_id)
    log_run(
        spark,
        config,
        run_id=run_id,
        mode=mode,
        status="SUCCESS",
        stage="core",
        message=str(metrics)[:8000],
    )
    if touched is not None:
        spark.sql(f"DROP TABLE IF EXISTS {touched_table}")
    return {"run_id": run_id, "mode": mode, "metrics": metrics}
