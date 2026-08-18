"""CDF state and match-group-scoped execution for pathology sidecars."""

from __future__ import annotations

from pathology_contracts import CONTRACT_VERSION
from pathology_pipeline import PipelineConfig, ensure_contracts, run_core


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


def changed_parent_keys(spark, config: PipelineConfig):
    """Return touched source parents and pending source versions.

    A missing state row means a full build is required. CDF failures are raised;
    this pipeline deliberately has no silent billion-row snapshot fallback.
    """

    _, F = _imports()
    state = _state(spark, config)
    sources = {
        "map_pathology": config.map_pathology_table,
        "path_patient_samplelevel": config.sample_table,
    }
    versions = {name: current_delta_version(spark, table) for name, table in sources.items()}
    if any(name not in state or state[name]["last_delta_version"] is None for name in sources):
        return None, versions

    frames = []
    map_start = int(state["map_pathology"]["last_delta_version"]) + 1
    map_changes = _read_changes(
        spark, config.map_pathology_table, map_start, versions["map_pathology"]
    )
    if map_changes is not None:
        frames.append(
            map_changes.filter(F.col("_change_type") != "update_preimage")
            .select(
                F.when(F.col("source_table") == "raw", "TFC_LIMS")
                .otherwise("CERNER")
                .alias("source_system"),
                "source_parent_key",
            )
            .filter(F.col("source_parent_key").isNotNull())
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
    table_names = {
        "map_pathology": config.map_pathology_table,
        "path_patient_samplelevel": config.sample_table,
    }
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
