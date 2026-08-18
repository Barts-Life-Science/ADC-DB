"""Deterministic genetics sidecar builder."""

from __future__ import annotations

import uuid

from pathology_contracts import contract
from pathology_pipeline import PipelineConfig, ensure_contracts, merge_contract, table_exists
from pathology_rules import PARSER_VERSION, parse_genetic_report_json_worker


def _imports():
    from pyspark.sql import Window, functions as F, types as T

    return Window, F, T


def _parsed_schema(T):
    finding = T.StructType(
        [
            T.StructField("reported_gene_symbol", T.StringType()),
            T.StructField("partner_gene_symbol", T.StringType()),
            T.StructField("alteration_type", T.StringType()),
            T.StructField("detection_status", T.StringType()),
            T.StructField("hgvs_c_raw", T.StringType()),
            T.StructField("hgvs_p_raw", T.StringType()),
            T.StructField("transcript", T.StringType()),
            T.StructField("vaf_raw", T.StringType()),
            T.StructField("vaf", T.DoubleType()),
            T.StructField("reported_classification", T.StringType()),
            T.StructField("evidence_text", T.StringType()),
            T.StructField("evidence_start", T.IntegerType()),
            T.StructField("evidence_end", T.IntegerType()),
        ]
    )
    return T.StructType(
        [
            T.StructField("overall_result_status", T.StringType()),
            T.StructField("genes_tested", T.ArrayType(T.StringType())),
            T.StructField("findings", T.ArrayType(finding)),
            T.StructField("parser_version", T.StringType()),
        ]
    )


def _approved_profiles(spark, config: PipelineConfig, include_proposed: bool):
    _, F, _ = _imports()
    statuses = ["APPROVED"] + (["PROPOSED"] if include_proposed else [])
    return spark.table(f"{config.lookup_schema}.pathology_report_profile").filter(
        F.upper(F.col("status")).isin(*statuses)
    )


def _match_report_profiles(spark, config: PipelineConfig, include_proposed: bool):
    Window, F, _ = _imports()
    reports = spark.table(f"{config.bronze_schema}.map_pathology_report")
    source_system = (
        spark.table(f"{config.bronze_schema}.map_pathology_accession_source")
        .filter(F.col("is_current") == True)
        .groupBy("source_accession_id")
        .agg(F.first("source_system", ignorenulls=True).alias("source_system"))
    )
    reports = reports.join(source_system, "source_accession_id", "left")
    profiles = _approved_profiles(spark, config, include_proposed)
    reports = reports.alias("r")
    profiles = F.broadcast(profiles.alias("p"))
    # Spark's dynamic regexp operand is expressed in SQL; Column.rlike accepts a
    # literal pattern on older Databricks runtimes.
    condition = F.expr(
        """
        (coalesce(p.source_system, '*')='*' OR upper(p.source_system)=upper(r.source_system))
        AND (p.result_code_pattern IS NULL OR coalesce(r.report_code,'') RLIKE p.result_code_pattern)
        AND (p.report_section_pattern IS NULL OR coalesce(r.report_section,'') RLIKE p.report_section_pattern)
        AND (p.text_pattern IS NULL OR coalesce(r.report_text,'') RLIKE p.text_pattern)
        """
    )
    matched = reports.join(profiles, condition, "inner")
    priority = Window.partitionBy(F.col("r.report_version_id")).orderBy(
        F.col("p.priority").asc_nulls_last(),
        F.col("p.profile_id"),
        F.col("p.profile_version").desc(),
    )
    return matched.withColumn("_profile_rn", F.row_number().over(priority)).filter(
        F.col("_profile_rn") == 1
    )


def _gene_aliases(spark, config: PipelineConfig):
    _, F, _ = _imports()
    aliases = spark.table(f"{config.lookup_schema}.pathology_hgnc_alias")
    return aliases.filter(F.col("ambiguous_ind") == False).select(
        F.upper(F.trim("alias_symbol")).alias("alias_symbol"),
        "hgnc_id",
        "approved_symbol",
        "alias_type",
    )


def _normalize_gene(frame, aliases, symbol_column: str, prefix: str):
    _, F, _ = _imports()
    lookup = aliases.select(
        F.col("alias_symbol").alias(f"_{prefix}_alias"),
        F.col("hgnc_id").alias(f"{prefix}_hgnc_id"),
        F.col("approved_symbol").alias(f"{prefix}_normalized_symbol"),
        F.col("alias_type").alias(f"{prefix}_alias_type"),
    )
    return frame.join(
        lookup,
        F.upper(F.trim(F.col(symbol_column))) == F.col(f"_{prefix}_alias"),
        "left",
    ).drop(f"_{prefix}_alias")


def _quote_table(table_name: str) -> str:
    return ".".join(f"`{part.replace('`', '``')}`" for part in table_name.split("."))


def _materialize_tests(spark, frame, bronze_schema: str):
    """Persist the parser output in Delta so autoscaling cannot discard it."""

    stage_table = f"{bronze_schema}._a12_genetics_stage_{uuid.uuid4().hex}"
    frame.write.format("delta").mode("overwrite").saveAsTable(stage_table)
    return spark.table(stage_table), stage_table


def _drop_stage_table(spark, stage_table: str | None):
    if not stage_table:
        return
    try:
        spark.sql(f"DROP TABLE IF EXISTS {_quote_table(stage_table)}")
    except Exception:
        # The stage name is unique and the next cleanup sweep can remove it.
        pass


def build_genetic_frames(
    spark,
    config: PipelineConfig,
    *,
    include_proposed_profiles: bool = False,
):
    _, F, T = _imports()
    aliases = _gene_aliases(spark, config)
    gene_symbols = [row["alias_symbol"] for row in aliases.select("alias_symbol").distinct().collect()]
    if not gene_symbols:
        raise RuntimeError(
            "No unambiguous HGNC aliases are loaded in 8_dev.lookup.pathology_hgnc_alias"
        )

    matched = _match_report_profiles(spark, config, include_proposed_profiles).select(
        F.col("r.pathology_accession_id").alias("pathology_accession_id"),
        F.col("r.report_version_id").alias("report_version_id"),
        F.col("r.report_code").alias("report_code"),
        F.col("r.report_text").alias("report_text"),
        F.col("r.issued_dt").alias("issued_dt"),
        F.col("r.is_current").alias("is_current"),
        F.col("r.lifecycle_status").alias("lifecycle_status"),
        F.col("p.profile_id").alias("profile_id"),
        F.col("p.profile_version").alias("profile_version"),
        F.col("p.status").alias("profile_status"),
        F.col("p.parser_type").alias("parser_type"),
        F.col("p.assay_name").alias("profile_assay_name"),
        F.col("p.method").alias("profile_method"),
        F.col("p.panel_code").alias("panel_code"),
        F.col("p.analysis_context").alias("profile_analysis_context"),
        F.col("p.dedicated_gene_symbol").alias("dedicated_gene_symbol"),
    )
    parser_udf = F.udf(
        lambda text: parse_genetic_report_json_worker(text, gene_symbols), T.StringType()
    )
    parsed = matched.withColumn("_parse_json", parser_udf(F.col("report_text"))).withColumn(
        "_parsed", F.from_json("_parse_json", _parsed_schema(T))
    )

    panels = spark.table(f"{config.lookup_schema}.pathology_panel_definition").filter(
        F.upper(F.col("status")) == "APPROVED"
    )
    parsed = (
        parsed.alias("x")
        .join(
            F.broadcast(panels.alias("pd")),
            (F.col("x.panel_code") == F.col("pd.panel_code"))
            & (
                F.col("x.issued_dt")
                >= F.coalesce(
                    F.col("pd.effective_from"), F.lit("1900-01-01").cast("timestamp")
                )
            )
            & (
                F.col("x.issued_dt")
                < F.coalesce(
                    F.col("pd.effective_to"), F.lit("9999-12-31").cast("timestamp")
                )
            ),
            "left",
        )
        .select(
            "x.*",
            F.col("pd.panel_version").alias("_panel_version"),
            F.col("pd.analysis_context").alias("_panel_analysis_context"),
        )
    )

    tests = (
        parsed.withColumn(
            "genetic_test_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit("pathology_genetic_test"),
                    F.col("report_version_id"),
                    F.col("profile_id"),
                    F.col("profile_version"),
                ),
                256,
            ),
        )
        .select(
            "genetic_test_id",
            "pathology_accession_id",
            "report_version_id",
            F.lit(None).cast("string").alias("canonical_requested_test_id"),
            F.col("report_code").alias("assay_code"),
            F.coalesce(F.col("profile_assay_name"), F.col("report_code")).alias("assay_name"),
            F.coalesce(F.col("profile_method"), F.col("parser_type")).alias("method"),
            "panel_code",
            F.col("_panel_version").alias("panel_version"),
            F.col("_panel_version").isNotNull().alias("panel_version_inferred"),
            F.coalesce(
                F.col("profile_analysis_context"),
                F.col("_panel_analysis_context"),
                F.lit("unknown"),
            ).alias("analysis_context"),
            F.col("_parsed.overall_result_status").alias("overall_result_status"),
            F.lit(None).cast("string").alias("test_snomed_code"),
            F.lit(None).cast("string").alias("test_loinc_code"),
            F.lit(None).cast("long").alias("test_omop_concept_id"),
            F.concat_ws("@", F.col("profile_id"), F.col("profile_version")).alias("parser_profile_id"),
            "is_current",
            F.lit(True).alias("research_qi_only"),
            F.col("_parsed.genes_tested").alias("_genes_tested"),
            F.col("_parsed.findings").alias("_findings"),
            F.col("dedicated_gene_symbol").alias("_dedicated_gene_symbol"),
            F.upper(F.col("profile_status")).alias("_profile_status"),
            F.col("lifecycle_status").alias("_lifecycle_status"),
        )
    )

    # A normal cache is not durable on this autoscaling cluster: workers can be
    # removed after materialisation and Spark then reparses the full report
    # estate. A transient Delta stage preserves the one-parse invariant and is
    # supported by this cluster's Spark Connect session.
    stage_table = None
    try:
        tests, stage_table = _materialize_tests(spark, tests, config.bronze_schema)
    except Exception:
        _drop_stage_table(spark, stage_table)
        raise

    report_genes = (
        tests.select(
            "genetic_test_id",
            "panel_version_inferred",
            F.explode_outer("_genes_tested").alias("reported_gene_symbol"),
        )
        .filter(F.col("reported_gene_symbol").isNotNull())
        .withColumn("evidence_type", F.lit("report_gene_list"))
        .withColumn("test_scope", F.lit(None).cast("string"))
        .withColumn("confidence", F.lit(1.0))
    )
    dedicated_genes = (
        tests.select(
            "genetic_test_id",
            "panel_version_inferred",
            F.col("_dedicated_gene_symbol").alias("reported_gene_symbol"),
        )
        .filter(F.col("reported_gene_symbol").isNotNull())
        .withColumn("evidence_type", F.lit("dedicated_test"))
        .withColumn("test_scope", F.lit(None).cast("string"))
        .withColumn("confidence", F.lit(1.0))
    )
    panel_members = (
        tests.filter(F.col("panel_version").isNotNull()).alias("x")
        .join(
            spark.table(f"{config.lookup_schema}.pathology_panel_gene")
            .filter(F.upper(F.col("status")) == "APPROVED")
            .alias("pg"),
            (F.col("x.panel_code") == F.col("pg.panel_code"))
            & (F.col("x.panel_version") == F.col("pg.panel_version")),
            "inner",
        )
        .select(
            F.col("x.genetic_test_id").alias("genetic_test_id"),
            F.lit(True).alias("panel_version_inferred"),
            F.col("pg.gene_symbol").alias("reported_gene_symbol"),
            F.lit("panel_definition").alias("evidence_type"),
            F.col("pg.test_scope").alias("test_scope"),
            F.lit(1.0).alias("confidence"),
        )
    )
    gene_tested = report_genes.unionByName(dedicated_genes).unionByName(panel_members)
    gene_tested = _normalize_gene(gene_tested, aliases, "reported_gene_symbol", "gene")
    gene_tested = (
        gene_tested.withColumn("hgnc_id", F.col("gene_hgnc_id"))
        .withColumn("normalized_gene_symbol", F.col("gene_normalized_symbol"))
        .withColumn("alias_match_type", F.coalesce(F.col("gene_alias_type"), F.lit("unresolved")))
        .withColumn(
            "gene_tested_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit("pathology_gene_tested"),
                    "genetic_test_id",
                    F.upper("reported_gene_symbol"),
                    "evidence_type",
                ),
                256,
            ),
        )
        .dropDuplicates(["gene_tested_id"])
    )

    findings = (
        tests.select(
            "genetic_test_id",
            "report_version_id",
            "parser_profile_id",
            "is_current",
            "_profile_status",
            "_lifecycle_status",
            F.explode_outer("_findings").alias("finding"),
        )
        .filter(F.col("finding").isNotNull())
        .select(
            "genetic_test_id",
            "report_version_id",
            "parser_profile_id",
            "is_current",
            "_profile_status",
            "_lifecycle_status",
            "finding.*",
        )
    )
    findings = _normalize_gene(findings, aliases, "reported_gene_symbol", "gene")
    findings = _normalize_gene(findings, aliases, "partner_gene_symbol", "partner")
    findings = (
        findings.withColumn(
            "genetic_result_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit("pathology_genetic_result"),
                    "report_version_id",
                    F.col("evidence_start").cast("string"),
                    F.col("evidence_end").cast("string"),
                    F.coalesce("hgvs_c_raw", "hgvs_p_raw", "evidence_text"),
                ),
                256,
            ),
        )
        .withColumn("hgnc_id", F.col("gene_hgnc_id"))
        .withColumn("normalized_gene_symbol", F.col("gene_normalized_symbol"))
        .withColumn("partner_hgnc_id", F.col("partner_hgnc_id"))
        .withColumn("hgvs_c_parsed", F.lit(None).cast("string"))
        .withColumn("hgvs_p_parsed", F.lit(None).cast("string"))
        .withColumn("hgvs_validation_status", F.lit("not_validated"))
        .withColumn("genome_build", F.lit(None).cast("string"))
        .withColumn("chromosome", F.lit(None).cast("string"))
        .withColumn("position_start", F.lit(None).cast("long"))
        .withColumn("position_end", F.lit(None).cast("long"))
        .withColumn("zygosity", F.lit(None).cast("string"))
        .withColumn("reported_tier", F.lit(None).cast("string"))
        .withColumn("copy_number", F.lit(None).cast("double"))
        .withColumn("ratio_raw", F.lit(None).cast("string"))
        .withColumn("iscn_raw", F.lit(None).cast("string"))
        .withColumn("clinvar_concept_id", F.lit(None).cast("long"))
        .withColumn("omop_genomic_concept_id", F.lit(None).cast("long"))
        .withColumn("snomed_code", F.lit(None).cast("string"))
        .withColumn("parser_version", F.lit(PARSER_VERSION))
        .withColumn(
            "review_status",
            F.when(F.col("_profile_status") == "APPROVED", "auto_validated").otherwise("proposed"),
        )
        .withColumn("lifecycle_status", F.col("_lifecycle_status"))
        .withColumn("research_qi_only", F.lit(True))
    )
    return tests, gene_tested, findings, stage_table


def run_genetics(
    spark,
    config: PipelineConfig | None = None,
    *,
    include_proposed_profiles: bool = False,
    full_reconcile: bool = True,
    validate_stage_keys: bool = True,
):
    config = config or PipelineConfig()
    ensure_contracts(spark, config)
    tests, genes, findings, stage_table = build_genetic_frames(
        spark, config, include_proposed_profiles=include_proposed_profiles
    )
    outputs = {
        "map_pathology_genetic_test": tests,
        "map_pathology_gene_tested": genes,
        "map_pathology_genetic_result": findings,
    }
    try:
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
    finally:
        _drop_stage_table(spark, stage_table)
