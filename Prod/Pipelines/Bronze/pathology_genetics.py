"""Deterministic genetics sidecar builder."""

from __future__ import annotations

import uuid

from pathology_contracts import contract
from pathology_pipeline import PipelineConfig, ensure_contracts, merge_contract, reconcile_scoped_stale, table_exists
from pathology_rules import PARSER_VERSION, parse_genetic_report_json_worker
# GEN4_GENETICS_V1: assay components, report rows and structured extras come from the enrich worker;
# codes, coordinates and concepts from the coding module.
from pathology_genetics_enrich import ENRICH_VERSION, enrich_genetic_report_json_worker
from pathology_genetics_coding import code_findings, code_reports, code_tests


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
            T.StructField("hgvs_c_parsed", T.StringType()),
            T.StructField("hgvs_p_raw", T.StringType()),
            T.StructField("hgvs_p_parsed", T.StringType()),
            T.StructField("hgvs_validation_status", T.StringType()),
            T.StructField("transcript", T.StringType()),
            T.StructField("vaf_raw", T.StringType()),
            T.StructField("vaf", T.DoubleType()),
            T.StructField("reported_classification", T.StringType()),
            T.StructField("reported_tier", T.StringType()),
            T.StructField("zygosity", T.StringType()),
            T.StructField("ratio_raw", T.StringType()),
            T.StructField("evidence_text", T.StringType()),
            T.StructField("evidence_start", T.IntegerType()),
            T.StructField("evidence_end", T.IntegerType()),
        ]
    )
    return T.StructType(
        [
            T.StructField("overall_result_status", T.StringType()),
            T.StructField("genes_tested", T.ArrayType(T.StringType())),
            T.StructField("genes_reported", T.ArrayType(T.StringType())),
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


def _match_report_profiles(spark, config: PipelineConfig, include_proposed: bool, report_scope=None):
    Window, F, _ = _imports()
    reports = spark.table(f"{config.bronze_schema}.map_pathology_report")
    if report_scope is not None:  # GEN4: parse only the scoped report versions
        reports = reports.join(report_scope.select("report_version_id").distinct(), "report_version_id", "left_semi")
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
    """One HGNC row per symbol string.

    A current approved symbol always resolves to its own gene, even when the
    same string is also another gene's alias (SF3B1, MET and SMO are flagged
    ambiguous for that reason). Previous symbols and aliases resolve only when
    unambiguous.
    """

    Window, F, _ = _imports()
    aliases = spark.table(f"{config.lookup_schema}.pathology_hgnc_alias").select(
        F.upper(F.trim("alias_symbol")).alias("alias_symbol"),
        "hgnc_id",
        "approved_symbol",
        "alias_type",
        "ambiguous_ind",
    )
    aliases = aliases.filter(
        (F.col("alias_type") == "approved_symbol") | (F.col("ambiguous_ind") == False)
    )
    preference = Window.partitionBy("alias_symbol").orderBy(
        F.when(F.col("alias_type") == "approved_symbol", 0)
        .when(F.col("alias_type") == "previous_symbol", 1)
        .otherwise(2),
        F.col("hgnc_id").asc_nulls_last(),
    )
    return (
        aliases.withColumn("_alias_rn", F.row_number().over(preference))
        .filter(F.col("_alias_rn") == 1)
        .select("alias_symbol", "hgnc_id", "approved_symbol", "alias_type")
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


def _enrich_schema(T):
    """Spark schema of enrich_genetic_report_json_worker's JSON."""
    component = T.StructType([
        T.StructField("assay_component", T.StringType()),
        T.StructField("is_primary", T.BooleanType()),
        T.StructField("overall_result_status", T.StringType()),
        T.StructField("section_start", T.IntegerType()),
        T.StructField("section_end", T.IntegerType()),
    ])
    extra = T.StructType([
        T.StructField("assay_component", T.StringType()),
        T.StructField("cosmic_id", T.StringType()),
        T.StructField("itd_length_bp", T.IntegerType()),
        T.StructField("reported_pathogenicity", T.StringType()),
        T.StructField("transcript", T.StringType()),
        T.StructField("transcript_source", T.StringType()),
    ])
    gap = T.StructType([T.StructField("gene_symbol", T.StringType()), T.StructField("regions_raw", T.StringType())])
    tested_gene = T.StructType([T.StructField("assay_component", T.StringType()),
                                T.StructField("gene_symbol", T.StringType()),
                                T.StructField("test_scope", T.StringType())])
    fields = T.StructType([
        T.StructField(name, T.DoubleType() if name == "tumour_content_pct"
                      else T.BooleanType() if name == "panel_genes_not_assessed" else T.StringType())
        for name in ("sample_type_site_raw", "block_raw", "tumour_content_raw", "tumour_content_pct",
                     "clinical_details_raw", "interpretation_text", "predicted_response",
                     "predicted_response_drug_class", "predicted_response_text", "panel_genes_not_assessed")
    ])
    return T.StructType([
        T.StructField("components", T.ArrayType(component)),
        T.StructField("component_genes", T.ArrayType(tested_gene)),
        T.StructField("finding_extras", T.ArrayType(extra)),
        T.StructField("coverage_gaps", T.ArrayType(gap)),
        T.StructField("panel_transcripts", T.MapType(T.StringType(), T.StringType())),
        T.StructField("report_fields", fields),
        T.StructField("enrich_version", T.StringType()),
    ])


# Secondary assay components test one gene each; the primary keeps the profile's dedicated gene.
COMPONENT_DEDICATED_GENE = {"npm1": "NPM1"}
ADMIN_STATUSES = ("not_performed", "duplicate_request", "sent_away", "failed")


def _genetic_test_id(F, component, is_primary):
    """Primary components keep the pre-GEN4 id; secondary components add their name."""
    legacy = F.sha2(F.concat_ws("|", F.lit("pathology_genetic_test"), F.col("report_version_id"),
                                F.col("profile_id"), F.col("profile_version")), 256)
    secondary = F.sha2(F.concat_ws("|", F.lit("pathology_genetic_test"), F.col("report_version_id"),
                                   F.col("profile_id"), F.col("profile_version"), component), 256)
    return F.when(is_primary, legacy).otherwise(secondary)


def build_genetic_frames(
    spark,
    config: PipelineConfig,
    *,
    include_proposed_profiles: bool = False,
    report_scope=None,
    coding_version: str | None = None,
):
    """Parse matching reports once and return (tests, gene_tested, findings, reports, stage_table).

    coding_version is run_genetics' coding_fingerprint, recorded on each report row.

    report_scope: optional DataFrame with report_version_id; when given only those reports are parsed
    (incremental runs). Coding columns are NULL here and filled by pathology_genetics_coding.
    """
    _, F, T = _imports()
    aliases = _gene_aliases(spark, config)
    gene_symbols = [row["alias_symbol"] for row in aliases.select("alias_symbol").distinct().collect()]
    if not gene_symbols:
        raise RuntimeError(f"No HGNC symbols are loaded in {config.lookup_schema}.pathology_hgnc_alias")

    matched = _match_report_profiles(spark, config, include_proposed_profiles, report_scope).select(
        F.col("r.pathology_accession_id").alias("pathology_accession_id"),
        F.col("r.report_version_id").alias("report_version_id"),
        F.col("r.report_code").alias("report_code"),
        F.col("r.report_text").alias("report_text"),
        F.col("r.report_text_hash").alias("report_text_hash"),
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

    # Non-deterministic marks stop the optimizer from inlining one UDF into the other's input,
    # which would parse every report twice.
    parser_udf = F.udf(lambda text: parse_genetic_report_json_worker(text, gene_symbols), T.StringType()).asNondeterministic()
    enrich_udf = F.udf(
        lambda text, v3, profile: enrich_genetic_report_json_worker(text, v3, profile), T.StringType()
    ).asNondeterministic()
    matched = matched.withColumn("_profile", F.concat_ws("@", "profile_id", "profile_version"))
    parsed = matched.withColumn("_parse_json", parser_udf(F.col("report_text")))
    parsed = parsed.withColumn("_enrich_json", enrich_udf(F.col("report_text"), F.col("_parse_json"), F.col("profile_id")))

    # One durable stage holds both JSON strings, so each report is parsed exactly once per run.
    stage_table = None
    try:
        parsed, stage_table = _materialize_tests(spark, parsed.drop("report_text"), config.bronze_schema)
    except Exception:
        _drop_stage_table(spark, stage_table)
        raise
    parsed = (
        parsed.withColumn("_parsed", F.from_json("_parse_json", _parsed_schema(T)))
        .withColumn("_enriched", F.from_json("_enrich_json", _enrich_schema(T)))
        .withColumn("_primary_component",
                    F.element_at(F.filter("_enriched.components", lambda c: c["is_primary"]), 1)["assay_component"])
    )

    panels = spark.table(f"{config.lookup_schema}.pathology_panel_definition").filter(F.upper(F.col("status")) == "APPROVED")
    parsed = (
        parsed.alias("x")
        .join(
            F.broadcast(panels.alias("pd")),
            (F.col("x.panel_code") == F.col("pd.panel_code"))
            & (F.col("x.issued_dt") >= F.coalesce(F.col("pd.effective_from"), F.lit("1900-01-01").cast("timestamp")))
            & (F.col("x.issued_dt") < F.coalesce(F.col("pd.effective_to"), F.lit("9999-12-31").cast("timestamp"))),
            "left",
        )
        .select("x.*", F.col("pd.panel_version").alias("_panel_version"),
                F.col("pd.analysis_context").alias("_panel_analysis_context"))
    )

    # --- reports: one row per parsed report version -------------------------------------------------
    rf = "_enriched.report_fields"
    reports = parsed.select(
        "report_version_id",
        "pathology_accession_id",
        F.concat_ws("@", "profile_id", "profile_version").alias("parser_profile_id"),
        "report_text_hash",
        source_fingerprint(F).alias("source_fingerprint"),
        F.lit(coding_version).cast("string").alias("coding_fingerprint"),
        F.col("_parsed.parser_version").alias("parser_version"),
        F.col("_enriched.enrich_version").alias("enrich_version"),
        F.col(f"{rf}.sample_type_site_raw").alias("sample_type_site_raw"),
        F.lit(None).cast("string").alias("sample_site_snomed_code"),
        F.lit(None).cast("long").alias("sample_site_concept_id"),
        F.col(f"{rf}.block_raw").alias("block_raw"),
        F.col(f"{rf}.tumour_content_raw").alias("tumour_content_raw"),
        F.col(f"{rf}.tumour_content_pct").alias("tumour_content_pct"),
        F.col(f"{rf}.clinical_details_raw").alias("clinical_details_raw"),
        F.lit(None).cast("string").alias("indication_snomed_code"),
        F.lit(None).cast("long").alias("indication_concept_id"),
        F.lit(None).cast("string").alias("term_rule_ids"),
        F.col(f"{rf}.interpretation_text").alias("interpretation_text"),
        F.col(f"{rf}.predicted_response").alias("predicted_response"),
        F.col(f"{rf}.predicted_response_drug_class").alias("predicted_response_drug_class"),
        F.col(f"{rf}.predicted_response_text").alias("predicted_response_text"),
        F.to_json(F.col("_enriched.panel_transcripts")).alias("panel_transcripts_json"),
        F.col(f"{rf}.panel_genes_not_assessed").alias("panel_genes_not_assessed"),
        "is_current",
        F.lit(True).alias("research_qi_only"),
    )

    # --- tests: one row per assay component ---------------------------------------------------------
    components = parsed.withColumn("_component", F.explode("_enriched.components"))
    tests = components.select(
        _genetic_test_id(F, F.col("_component.assay_component"), F.col("_component.is_primary")).alias("genetic_test_id"),
        "pathology_accession_id",
        "report_version_id",
        F.lit(None).cast("string").alias("canonical_requested_test_id"),
        F.col("report_code").alias("assay_code"),
        F.when(F.col("_component.is_primary"), F.coalesce(F.col("profile_assay_name"), F.col("report_code")))
         .otherwise(F.concat_ws(" ", F.coalesce(F.col("profile_assay_name"), F.col("report_code")),
                                F.upper(F.col("_component.assay_component"))))
         .alias("assay_name"),
        F.coalesce(F.col("profile_method"), F.col("parser_type")).alias("method"),
        "panel_code",
        F.col("_panel_version").alias("panel_version"),
        F.col("_panel_version").isNotNull().alias("panel_version_inferred"),
        F.coalesce(F.col("profile_analysis_context"), F.col("_panel_analysis_context"), F.lit("unknown")).alias("analysis_context"),
        F.col("_component.overall_result_status").alias("overall_result_status"),
        F.lit(None).cast("string").alias("test_snomed_code"),
        F.lit(None).cast("string").alias("test_loinc_code"),
        F.lit(None).cast("long").alias("test_omop_concept_id"),
        F.concat_ws("@", F.col("profile_id"), F.col("profile_version")).alias("parser_profile_id"),
        F.col("_component.assay_component").alias("assay_component"),
        F.col("_component.is_primary").alias("is_primary_component"),
        F.lit(None).cast("string").alias("nhs_test_directory_code"),
        F.lit(None).cast("string").alias("test_code_method"),
        "is_current",
        F.lit(True).alias("research_qi_only"),
        # helper columns for gene_tested (dropped by the contract sync)
        F.col("_enriched.coverage_gaps").alias("_coverage_gaps"),
        F.col(f"{rf}.panel_genes_not_assessed").alias("_panel_not_assessed"),
        F.coalesce(F.element_at(F.create_map(*[F.lit(x) for kv in COMPONENT_DEDICATED_GENE.items() for x in kv]),
                                F.col("_component.assay_component")),
                   F.when(F.col("_component.is_primary"), F.col("dedicated_gene_symbol"))).alias("_dedicated_gene_symbol"),
        F.upper(F.col("profile_status")).alias("_profile_status"),
    )

    # --- findings: v3 findings with their component's test id and GEN4 extras -----------------------
    exploded = parsed.select(
        "report_version_id", "profile_id", "profile_version", "is_current", "lifecycle_status", "profile_status",
        "_primary_component",
        F.col("_parsed.parser_version").alias("_parser_version"),
        F.col("_enriched.finding_extras").alias("_extras"),
        F.posexplode("_parsed.findings").alias("_pos", "finding"),
    )
    extra = F.element_at(F.col("_extras"), F.col("_pos") + 1)
    findings = exploded.select(
        _genetic_test_id(F, extra["assay_component"], extra["assay_component"] == F.col("_primary_component")).alias("genetic_test_id"),
        "report_version_id",
        F.concat_ws("@", "profile_id", "profile_version").alias("parser_profile_id"),
        "is_current",
        F.upper(F.col("profile_status")).alias("_profile_status"),
        F.col("lifecycle_status").alias("_lifecycle_status"),
        "_parser_version",
        "finding.*",
        extra["assay_component"].alias("_assay_component"),
        extra["cosmic_id"].alias("cosmic_id"),
        extra["itd_length_bp"].alias("itd_length_bp"),
        extra["reported_pathogenicity"].alias("reported_pathogenicity"),
        extra["transcript"].alias("resolved_transcript"),
        extra["transcript_source"].alias("transcript_source"),
    )
    findings = _normalize_gene(findings, aliases, "reported_gene_symbol", "gene")
    findings = _normalize_gene(findings, aliases, "partner_gene_symbol", "partner")
    findings = (
        findings.withColumn(
            "genetic_result_id",
            F.sha2(F.concat_ws("|", F.lit("pathology_genetic_result"), "report_version_id",
                               F.col("evidence_start").cast("string"), F.col("evidence_end").cast("string"),
                               F.coalesce("hgvs_c_raw", "hgvs_p_raw", "evidence_text")), 256),
        )
        .withColumn("hgnc_id", F.col("gene_hgnc_id"))
        .withColumn("normalized_gene_symbol", F.col("gene_normalized_symbol"))
        .withColumn("genome_build", F.lit(None).cast("string"))
        .withColumn("chromosome", F.lit(None).cast("string"))
        .withColumn("position_start", F.lit(None).cast("long"))
        .withColumn("position_end", F.lit(None).cast("long"))
        .withColumn("copy_number", F.lit(None).cast("double"))
        .withColumn("iscn_raw", F.lit(None).cast("string"))
        .withColumn("clinvar_concept_id", F.lit(None).cast("long"))
        .withColumn("omop_genomic_concept_id", F.lit(None).cast("long"))
        .withColumn("snomed_code", F.lit(None).cast("string"))
        .withColumn("variant_key", F.lit(None).cast("string"))
        .withColumn("ref_allele", F.lit(None).cast("string"))
        .withColumn("alt_allele", F.lit(None).cast("string"))
        .withColumn("coordinate_method", F.lit(None).cast("string"))
        .withColumn("clinvar_variation_id", F.lit(None).cast("long"))
        .withColumn("omop_genomic_concept_level", F.lit(None).cast("string"))
        .withColumn("concept_method", F.lit(None).cast("string"))
        .withColumn("parser_version", F.coalesce(F.col("_parser_version"), F.lit(PARSER_VERSION)))
        .withColumn("review_status", F.when(F.col("_profile_status") == "APPROVED", "auto_validated").otherwise("proposed"))
        .withColumn("lifecycle_status", F.col("_lifecycle_status"))
        .withColumn("research_qi_only", F.lit(True))
    )

    # --- gene_tested: each assay's own denominator (enrich component_genes), plus dedicated/panel rows ----
    primary_tests = tests.filter(F.col("is_primary_component"))
    component_tests = tests.select("genetic_test_id", "report_version_id", "assay_component", "panel_version_inferred")
    report_genes = (
        parsed.select("report_version_id", F.explode("_enriched.component_genes").alias("_cg"))
        .select("report_version_id",
                F.col("_cg.assay_component").alias("assay_component"),
                F.col("_cg.gene_symbol").alias("reported_gene_symbol"),
                F.col("_cg.test_scope").alias("test_scope"))
        .join(component_tests, ["report_version_id", "assay_component"], "inner")
        .select("genetic_test_id", "panel_version_inferred", "reported_gene_symbol",
                F.lit("report_gene_list").alias("evidence_type"), "test_scope", F.lit(1.0).alias("confidence"))
    )
    dedicated_genes = (
        tests.select("genetic_test_id", "panel_version_inferred", F.col("_dedicated_gene_symbol").alias("reported_gene_symbol"))
        .filter(F.col("reported_gene_symbol").isNotNull())
        .withColumn("evidence_type", F.lit("dedicated_test"))
        .withColumn("test_scope", F.lit(None).cast("string"))
        .withColumn("confidence", F.lit(1.0))
    )
    panel_members = (
        tests.filter(F.col("panel_version").isNotNull() & F.col("is_primary_component")).alias("x")
        .join(spark.table(f"{config.lookup_schema}.pathology_panel_gene").filter(F.upper(F.col("status")) == "APPROVED").alias("pg"),
              (F.col("x.panel_code") == F.col("pg.panel_code")) & (F.col("x.panel_version") == F.col("pg.panel_version")), "inner")
        .select(F.col("x.genetic_test_id").alias("genetic_test_id"), F.lit(True).alias("panel_version_inferred"),
                F.col("pg.gene_symbol").alias("reported_gene_symbol"), F.lit("panel_definition").alias("evidence_type"),
                F.col("pg.test_scope").alias("test_scope"), F.lit(1.0).alias("confidence"))
    )
    gaps = primary_tests.select("genetic_test_id", F.explode("_coverage_gaps").alias("_gap")).select(
        "genetic_test_id", F.upper(F.col("_gap.gene_symbol")).alias("_gap_gene"), F.col("_gap.regions_raw").alias("coverage_gap_regions_raw")
    )
    gap_only_genes = (
        gaps.join(report_genes.select("genetic_test_id", F.upper("reported_gene_symbol").alias("_gap_gene")),
                  ["genetic_test_id", "_gap_gene"], "left_anti")
        .join(tests.select("genetic_test_id", "panel_version_inferred"), "genetic_test_id")
        .select("genetic_test_id", "panel_version_inferred", F.col("_gap_gene").alias("reported_gene_symbol"),
                F.lit("coverage_gap_list").alias("evidence_type"), F.lit(None).cast("string").alias("test_scope"),
                F.lit(1.0).alias("confidence"))
    )
    gene_tested = report_genes.unionByName(dedicated_genes).unionByName(panel_members).unionByName(gap_only_genes)
    status = tests.select("genetic_test_id", F.col("overall_result_status").alias("_test_status"), "_panel_not_assessed")
    gap_lookup = gaps.select(F.col("genetic_test_id").alias("_gap_test_id"), "_gap_gene", "coverage_gap_regions_raw")
    gene_tested = (
        gene_tested.join(status, "genetic_test_id", "left")
        .join(gap_lookup, (F.col("genetic_test_id") == F.col("_gap_test_id"))
              & (F.upper("reported_gene_symbol") == F.col("_gap_gene")), "left")
        .drop("_gap_test_id", "_gap_gene")
        .withColumn(
            "assessment_status",
            F.when(F.col("_test_status").isin(*ADMIN_STATUSES), F.lit("test_not_completed"))
             .when(F.col("coverage_gap_regions_raw").isNotNull(), F.lit("insufficient_coverage"))
             .when(F.col("evidence_type") == "dedicated_test", F.lit("dedicated_assay"))
             .when(F.col("test_scope") == "reported_result", F.lit("reported"))
             .when(F.coalesce(F.col("_panel_not_assessed"), F.lit(False)), F.lit("panel_not_assessed"))
             .otherwise(F.lit("panel_assessed")),
        )
        .drop("_test_status", "_panel_not_assessed")
    )
    gene_tested = _normalize_gene(gene_tested, aliases, "reported_gene_symbol", "gene")
    gene_tested = (
        gene_tested.withColumn("hgnc_id", F.col("gene_hgnc_id"))
        .withColumn("normalized_gene_symbol", F.col("gene_normalized_symbol"))
        .withColumn("alias_match_type", F.coalesce(F.col("gene_alias_type"), F.lit("unresolved")))
        .withColumn("gene_tested_id", F.sha2(F.concat_ws("|", F.lit("pathology_gene_tested"), "genetic_test_id",
                                                          F.upper("reported_gene_symbol"), "evidence_type"), 256))
        .dropDuplicates(["gene_tested_id"])
    )
    # GEN4 coding: deterministic codes with provenance (pathology_genetics_coding).
    tests = code_tests(spark, config, tests)
    findings = code_findings(spark, config, findings)
    reports = code_reports(spark, config, reports)
    return tests, gene_tested, findings, reports, stage_table


def _version_key(value: str) -> tuple[int, ...]:
    return tuple(int(part) if part.isdigit() else 0 for part in str(value).split("."))


def _guard_full_reconcile(spark, config: PipelineConfig, findings, *, allow_result_shrink: bool):
    """Refuse a reconcile that would delete findings a stale parser cannot see.

    A full reconcile deletes every target row the stage lacks. On 2026-09-11 a
    bundled copy of parser 1.0.0 produced no findings and the reconcile deleted
    all 2,842 rows written by 2.0.0.
    """

    _, F, _ = _imports()
    target = f"{config.bronze_schema}.map_pathology_genetic_result"
    if not table_exists(spark, target):
        return
    existing = spark.table(target).agg(
        F.count(F.lit(1)).alias("row_count"),
        F.collect_set("parser_version").alias("parser_versions"),
    ).first()
    newer = sorted(
        version
        for version in (existing["parser_versions"] or [])
        if version and _version_key(version) > _version_key(PARSER_VERSION)
    )
    if newer:
        raise RuntimeError(
            f"{target} holds rows from parser {newer}, newer than the running "
            f"parser {PARSER_VERSION}; refusing full reconcile (stale module on sys.path?)"
        )
    if allow_result_shrink or not existing["row_count"]:
        return
    staged = findings.count()
    if staged < existing["row_count"] * 0.5:
        raise RuntimeError(
            f"Parser {PARSER_VERSION} staged {staged} findings against {existing['row_count']} "
            f"in {target}; refusing a full reconcile that would delete over half. "
            "Pass allow_result_shrink=True if the reduction is intended."
        )


# Inputs that decide a report's parse and codes. A change to any of them puts the report version in scope:
# the text, which profile matched, whether it is current, its lifecycle (amended/cancelled), its accession
# and its issue date (panel effective dating). Lookup-table versions decide the codes (coding_fingerprint).
SOURCE_FINGERPRINT_COLUMNS = ("report_text_hash", "_profile", "is_current", "lifecycle_status",
                              "pathology_accession_id", "issued_dt")
CODING_LOOKUPS = ("pathology_genetic_assay_concept", "pathology_genetic_term_rule", "pathology_omop_genomic_synonym",
                  "pathology_transcript_model", "pathology_transcript_equivalence", "pathology_reference_window",
                  "pathology_clinvar_variant", "pathology_hgnc_alias", "pathology_report_profile")


def source_fingerprint(F):
    """SHA-256 over SOURCE_FINGERPRINT_COLUMNS (NULL-safe), the per-report change detector."""
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in SOURCE_FINGERPRINT_COLUMNS]), 256)


def coding_fingerprint(spark, config: PipelineConfig) -> str:
    """Delta versions of every lookup the parse and codes read; a change re-parses every report."""
    versions = [
        f"{name}@{spark.sql(f'DESCRIBE HISTORY {config.lookup_schema}.{name} LIMIT 1').first()['version']}"
        for name in CODING_LOOKUPS
    ]
    return ";".join(versions)


def genetics_change_scope(spark, config: PipelineConfig, *, include_proposed_profiles: bool, coding_version: str):
    """Report versions to re-parse and to remove since the last completed run, plus their counts.

    Changed = matching now, and absent from map_pathology_genetic_report or recorded with a different
    source_fingerprint, coding_fingerprint, parser_version or enrich_version. Vanished = recorded but no
    longer matching. The regex profile match runs once: its fingerprint is staged to Delta (serverless has
    no cache) and every count comes from one aggregate.
    Returns (scope DataFrame of report_version_id, stats dict, stage table name to drop).
    """
    _, F, _ = _imports()
    matched = _match_report_profiles(spark, config, include_proposed_profiles).select(
        F.col("r.report_version_id").alias("report_version_id"),
        F.col("r.report_text_hash").alias("report_text_hash"),
        F.concat_ws("@", F.col("p.profile_id"), F.col("p.profile_version")).alias("_profile"),
        F.col("r.is_current").alias("is_current"),
        F.col("r.lifecycle_status").alias("lifecycle_status"),
        F.col("r.pathology_accession_id").alias("pathology_accession_id"),
        F.col("r.issued_dt").alias("issued_dt"),
    ).select("report_version_id", "_profile", source_fingerprint(F).alias("_fingerprint"))
    matched, stage_table = _materialize_tests(spark, matched, config.bronze_schema)
    existing = spark.table(f"{config.bronze_schema}.map_pathology_genetic_report").select(
        "report_version_id",
        F.col("parser_profile_id").alias("_old_profile"),
        F.col("source_fingerprint").alias("_old_fingerprint"),
        F.col("coding_fingerprint").alias("_old_coding"),
        F.col("parser_version").alias("_old_parser"),
        F.col("enrich_version").alias("_old_enrich"),
    )
    compared = matched.join(existing, "report_version_id", "full_outer").withColumn(
        "_state",
        F.when(F.col("_profile").isNull(), F.lit("vanished"))
        .when(
            F.col("_old_profile").isNull()
            | ~F.col("_fingerprint").eqNullSafe(F.col("_old_fingerprint"))
            | ~F.col("_old_coding").eqNullSafe(F.lit(coding_version))
            | ~F.col("_old_parser").eqNullSafe(F.lit(PARSER_VERSION))
            | ~F.col("_old_enrich").eqNullSafe(F.lit(ENRICH_VERSION)),
            F.lit("changed"),
        )
        .otherwise(F.lit("unchanged")),
    )
    counts = compared.agg(
        F.sum(F.col("_profile").isNotNull().cast("int")).alias("matched"),
        F.sum(F.col("_old_profile").isNotNull().cast("int")).alias("existing"),
        F.sum((F.col("_state") == "changed").cast("int")).alias("changed"),
        F.sum((F.col("_state") == "vanished").cast("int")).alias("vanished"),
    ).first()
    stats = {key: int(counts[key] or 0) for key in ("matched", "existing", "changed", "vanished")}
    scope = compared.where(F.col("_state") != "unchanged").select("report_version_id")
    return scope, stats, stage_table


def _guard_parser_version(spark, config: PipelineConfig):
    """Refuse any write (either mode) when the tables hold output of a newer parser or enrich worker.

    The 2026-09-11 wipe came from a stale bundled parser. In incremental mode an older parser would see
    every newer fingerprint as changed and its scoped reconcile would replace newer findings, so this
    runs before both modes.
    """
    _, F, _ = _imports()
    checks = (("map_pathology_genetic_result", "parser_version", PARSER_VERSION),
              ("map_pathology_genetic_report", "parser_version", PARSER_VERSION),
              ("map_pathology_genetic_report", "enrich_version", ENRICH_VERSION))
    for table, column, running in checks:
        target = f"{config.bronze_schema}.{table}"
        if not table_exists(spark, target):
            continue
        seen = spark.table(target).agg(F.collect_set(column).alias("v")).first()["v"] or []
        newer = sorted(v for v in seen if v and _version_key(v) > _version_key(running))
        if newer:
            raise RuntimeError(f"{target}.{column} holds {newer}, newer than the running {running}; "
                               "refusing to write (stale module on sys.path?)")


def _guard_incremental(stats: dict):
    """Refuse an incremental run that would empty or gut the genetics tables."""
    if stats["matched"] == 0:
        raise RuntimeError("No genetics reports match any profile; refusing (profiles unapproved or lookup empty?)")
    if stats["existing"] and stats["vanished"] > 0.05 * stats["existing"]:
        raise RuntimeError(
            f"{stats['vanished']} of {stats['existing']} parsed report versions no longer match a profile; "
            "refusing to delete over 5% in an incremental run. Investigate, or run mode='full' with allow_result_shrink."
        )


# Dependent tables first; map_pathology_genetic_report last, because its fingerprints are the checkpoint:
# a run that fails before the report merge leaves the old fingerprints, so the retry re-scopes the same work.
GENETICS_TABLES = (
    ("map_pathology_genetic_test", "report_version_id"),
    ("map_pathology_genetic_result", "report_version_id"),
    ("map_pathology_gene_tested", "genetic_test_id"),
    ("map_pathology_genetic_report", "report_version_id"),
)


def run_genetics(
    spark,
    config: PipelineConfig | None = None,
    *,
    include_proposed_profiles: bool = False,
    mode: str = "incremental",
    validate_stage_keys: bool = True,
    allow_result_shrink: bool = False,
    _fail_after: str | None = None,
):
    """Parse genetics reports into the four map_pathology_genetic_* tables.

    mode='incremental' re-parses only changed report versions (source fingerprint, lookups, parser/enrich
    versions) and deletes rows of changed or vanished versions that the new parse no longer produces;
    mode='full' re-parses everything and deletes any row the stage lacks. Both refuse to run under an older
    parser than the tables hold. _fail_after (tests only) raises after the named table's merge.
    """
    if mode not in ("incremental", "full"):
        raise ValueError(f"mode must be incremental or full, got {mode!r}")
    config = config or PipelineConfig()
    ensure_contracts(spark, config)
    _guard_parser_version(spark, config)
    coding_version = coding_fingerprint(spark, config)
    report_table = f"{config.bronze_schema}.map_pathology_genetic_report"
    if mode == "incremental" and spark.table(report_table).limit(1).count() == 0:
        mode = "full"  # first GEN4 run: nothing recorded yet

    scope, stats, stages = None, {}, []
    try:
        if mode == "incremental":
            scope, stats, fingerprint_stage = genetics_change_scope(
                spark, config, include_proposed_profiles=include_proposed_profiles, coding_version=coding_version)
            stages.append(fingerprint_stage)
            _guard_incremental(stats)
            if stats["changed"] == 0 and stats["vanished"] == 0:
                return {"mode": mode, "status": "NO_CHANGES", **stats}
            # Freeze the scope before any write: it is derived from map_pathology_genetic_report, which this
            # run rewrites, so a lazy re-evaluation after that merge would see no changes at all.
            scope, scope_stage = _materialize_tests(spark, scope, config.bronze_schema)
            stages.append(scope_stage)

        tests, genes, findings, reports, parse_stage = build_genetic_frames(
            spark, config, include_proposed_profiles=include_proposed_profiles, report_scope=scope,
            coding_version=coding_version,
        )
        stages.append(parse_stage)
        outputs = {"map_pathology_genetic_test": tests, "map_pathology_gene_tested": genes,
                   "map_pathology_genetic_result": findings, "map_pathology_genetic_report": reports}
        if mode == "full":
            _guard_full_reconcile(spark, config, findings, allow_result_shrink=allow_result_shrink)
            test_scope = None
        else:
            # gene_tested is scoped by test id: the scoped reports' tests, old and new. Frozen before the test
            # merge deletes the old ids, or their gene rows would never be found.
            test_scope, test_scope_stage = _materialize_tests(
                spark,
                spark.table(f"{config.bronze_schema}.map_pathology_genetic_test")
                .join(scope, "report_version_id", "left_semi").select("genetic_test_id")
                .unionByName(tests.select("genetic_test_id")).distinct(),
                config.bronze_schema,
            )
            stages.append(test_scope_stage)
        merged = {}
        for name, scope_column in GENETICS_TABLES:
            table, item = f"{config.bronze_schema}.{name}", contract(name)
            merged[name] = merge_contract(spark, table, outputs[name], item, delete_not_matched=(mode == "full"),
                                          validate_stage_keys=validate_stage_keys)
            if mode == "incremental":
                scope_frame = test_scope if scope_column == "genetic_test_id" else scope
                merged[name]["deleted"] = reconcile_scoped_stale(spark, table, outputs[name], item, scope_frame, scope_column)
            if _fail_after == name:
                raise RuntimeError(f"_fail_after={name} (test hook)")
        return {"mode": mode, **stats, **merged}
    finally:
        for stage in stages:
            _drop_stage_table(spark, stage)
