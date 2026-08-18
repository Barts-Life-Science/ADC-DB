"""Deterministic microbiology isolate and AMR sidecar builder."""

from __future__ import annotations

from pathology_contracts import contract
from pathology_pipeline import PipelineConfig, ensure_contracts, merge_contract


def _imports():
    from pyspark.sql import Window, functions as F

    return Window, F


def normalize_interpretation(column):
    _, F = _imports()
    normalized = F.upper(F.trim(column.cast("string")))
    return (
        F.when(normalized.isin("S", "SUSCEPTIBLE", "SENSITIVE"), "S")
        .when(normalized.isin("I", "INTERMEDIATE", "SUSCEPTIBLE, INCREASED EXPOSURE"), "I")
        .when(normalized.isin("R", "RESISTANT"), "R")
        .otherwise("indeterminate")
    )


def build_amr_frames(spark, config: PipelineConfig, *, include_proposed_rules: bool = False):
    Window, F = _imports()
    statuses = ["APPROVED"] + (["PROPOSED"] if include_proposed_rules else [])
    source_map = (
        spark.table(f"{config.bronze_schema}.map_pathology_accession_source")
        .filter(F.col("is_current") == True)
        .select("source_system", "source_parent_key", "pathology_accession_id")
        .dropDuplicates(["source_system", "source_parent_key"])
    )
    reports = (
        spark.table(f"{config.bronze_schema}.map_pathology_report")
        .filter(F.col("report_role") == "microbiology")
        .select("source_record_key", "report_version_id", "lifecycle_status", "is_current")
        .dropDuplicates(["source_record_key"])
    )
    base = (
        spark.table(config.map_pathology_table)
        .withColumn("source_system", F.when(F.col("source_table") == "raw", "TFC_LIMS").otherwise("CERNER"))
        .join(source_map, ["source_system", "source_parent_key"], "inner")
        .join(reports, "source_record_key", "left")
    )

    organism_rules = spark.table(f"{config.lookup_schema}.pathology_micro_organism_rule").filter(
        F.upper(F.col("status")).isin(*statuses)
    )
    organism_candidates = base.alias("b").crossJoin(
        F.broadcast(organism_rules.alias("r"))
    ).filter(
        F.expr(
            """
            (r.code_pattern IS NULL OR coalesce(b.code,'') RLIKE r.code_pattern)
            AND (r.section_pattern IS NULL OR coalesce(b.report_section,'') RLIKE r.section_pattern)
            AND (NOT coalesce(r.require_result_concept, false) OR b.value_as_concept_id IS NOT NULL)
            """
        )
    )
    organism_priority = Window.partitionBy(F.col("b.source_record_key")).orderBy(
        F.col("r.reviewed_at").desc_nulls_last(), F.col("r.rule_id")
    )
    isolates = (
        organism_candidates.withColumn("_rn", F.row_number().over(organism_priority))
        .filter(F.col("_rn") == 1)
        .select(
            F.sha2(F.concat_ws("|", F.lit("microbiology_isolate"), F.col("b.pathology_accession_id"), F.col("b.source_record_key")), 256).alias("microbiology_isolate_id"),
            F.col("b.pathology_accession_id").alias("pathology_accession_id"),
            F.col("b.report_version_id").alias("report_version_id"),
            F.col("b.source_record_key").alias("source_record_key"),
            F.col("b.specimen_type_code").alias("specimen_type_code"),
            F.coalesce(F.col("b.result_concept_name"), F.col("b.value_source_value")).alias("organism_text"),
            F.col("b.result_snomed_code").cast("string").alias("organism_snomed_code"),
            F.col("b.value_as_concept_id").cast("long").alias("organism_omop_concept_id"),
            F.coalesce(F.col("b.result_is_suspected"), F.lit(False)).alias("suspected_ind"),
            F.col("b.result_growth_grade").alias("growth_grade"),
            F.coalesce(F.col("b.lifecycle_status"), F.lit("unknown")).alias("lifecycle_status"),
            F.coalesce(F.col("b.is_current"), F.lit(True)).alias("is_current"),
            F.lit(True).alias("research_qi_only"),
        )
    )

    antimicrobial_map = spark.table(f"{config.lookup_schema}.pathology_antimicrobial_map").filter(
        F.upper(F.col("status")).isin(*statuses)
    )
    susceptibility = (
        base.alias("b")
        .join(
            F.broadcast(antimicrobial_map.alias("a")),
            (F.upper(F.col("b.code_system")) == F.upper(F.col("a.code_system")))
            & (F.upper(F.trim(F.col("b.code"))) == F.upper(F.trim(F.col("a.code")))),
            "inner",
        )
        .withColumn("interpretation", normalize_interpretation(F.col("b.value_source_value")))
        .withColumn(
            "mic",
            F.when(
                F.col("b.value_source_value").rlike(r"^\s*[<>]?\s*\d+(?:\.\d+)?\s*$"),
                F.regexp_extract(F.col("b.value_source_value"), r"([0-9]+(?:\.[0-9]+)?)", 1).cast("double"),
            ),
        )
    )
    isolate_counts = isolates.groupBy("pathology_accession_id").agg(
        F.countDistinct("microbiology_isolate_id").alias("_isolate_count"),
        F.first("microbiology_isolate_id", ignorenulls=True).alias("_only_isolate_id"),
    )
    susceptibility = (
        susceptibility.join(isolate_counts, F.col("b.pathology_accession_id") == isolate_counts.pathology_accession_id, "left")
        .select(
            F.sha2(F.concat_ws("|", F.lit("antimicrobial_susceptibility"), F.col("b.source_record_key")), 256).alias("susceptibility_result_id"),
            F.when(F.col("_isolate_count") == 1, F.col("_only_isolate_id")).alias("microbiology_isolate_id"),
            F.col("b.pathology_accession_id").alias("pathology_accession_id"),
            F.col("b.source_record_key").alias("source_record_key"),
            F.col("a.antimicrobial_text").alias("antimicrobial_text"),
            F.col("b.code").alias("antimicrobial_code"),
            F.col("a.antimicrobial_omop_concept_id").cast("long").alias("antimicrobial_omop_concept_id"),
            F.col("b.value_source_value").alias("interpretation_raw"),
            "interpretation",
            F.when(F.col("mic").isNotNull(), F.col("b.value_source_value")).alias("mic_raw"),
            "mic",
            F.col("b.unit_source_value").alias("unit_source_value"),
            F.col("a.method").alias("method"),
            F.when(F.col("_isolate_count") == 1, "unique_isolate")
            .when(F.col("_isolate_count") > 1, "ambiguous_isolate")
            .otherwise("no_isolate")
            .alias("link_status"),
            F.coalesce(F.col("b.lifecycle_status"), F.lit("unknown")).alias("lifecycle_status"),
            F.coalesce(F.col("b.is_current"), F.lit(True)).alias("is_current"),
            F.lit(True).alias("research_qi_only"),
        )
    )
    return isolates, susceptibility


def run_amr(
    spark,
    config: PipelineConfig | None = None,
    *,
    include_proposed_rules: bool = False,
    full_reconcile: bool = True,
    validate_stage_keys: bool = True,
):
    config = config or PipelineConfig()
    ensure_contracts(spark, config)
    isolates, susceptibility = build_amr_frames(
        spark, config, include_proposed_rules=include_proposed_rules
    )
    outputs = {
        "map_pathology_microbiology_isolate": isolates,
        "map_pathology_antimicrobial_susceptibility": susceptibility,
    }
    return {
        name: merge_contract(
            spark,
            f"{config.bronze_schema}.{name}",
            frame,
            contract(name),
            delete_not_matched=full_reconcile,
            validate_stage_keys=validate_stage_keys,
        )
        for name, frame in outputs.items()
    }
