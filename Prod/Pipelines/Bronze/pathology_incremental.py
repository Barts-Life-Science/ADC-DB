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
    semantic_columns = [column for column in changes.columns if column not in ignored]
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

def changed_parent_keys(spark, config: PipelineConfig):
    """Return touched source parents and pending source versions.

    A missing state row means a full build is required. CDF failures are raised;
    this pipeline deliberately has no silent billion-row snapshot fallback.
    """

    _, F = _imports()
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
        changed_tlcs = orderables_changes.select(
            F.upper(F.trim("WkgCode")).alias("_wkg"), F.upper(F.trim("TLCCode")).alias("_tlc")
        ).dropDuplicates()
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
        count = touched.count()
        mode = "INCREMENTAL"
        if count == 0:
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
        metrics = run_core(
            spark,
            config,
            full_reconcile=False,
            touched_parent_keys=touched,
            validate_stage_keys=validate_stage_keys,
        )
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
    return {"run_id": run_id, "mode": mode, "metrics": metrics}
