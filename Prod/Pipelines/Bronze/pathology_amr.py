"""Microbiology sidecars from packed WinPath antibiograms."""

from __future__ import annotations

from pathology_antibiogram import ANTIBIOGRAM_PARSER_VERSION, parse_antibiogram_json
from pathology_contracts import contract
from pathology_pipeline import PipelineConfig, ensure_contracts, merge_contract


def _imports():
    from pyspark.sql import Window, functions as F, types as T
    return Window, F, T


def _parsed_schema(T):
    token = T.StructType([
        T.StructField("token_ordinal", T.IntegerType()), T.StructField("raw_token", T.StringType()),
        T.StructField("agent_code", T.StringType()), T.StructField("flag_raw", T.StringType()),
        T.StructField("token_class", T.StringType()), T.StructField("interpretation", T.StringType()),
    ])
    isolate = T.StructType([
        T.StructField("isolate_ordinal", T.IntegerType()), T.StructField("organism_code", T.StringType()),
        T.StructField("panel_code", T.StringType()), T.StructField("raw_block", T.StringType()),
        T.StructField("tokens", T.ArrayType(token)), T.StructField("isolate_comment", T.StringType()),
        T.StructField("parse_status", T.StringType()),
    ])
    return T.StructType([
        T.StructField("parse_status", T.StringType()), T.StructField("parser_version", T.StringType()),
        T.StructField("isolates", T.ArrayType(isolate)),
    ])


def antibiogram_rows(spark, config: PipelineConfig):
    """One row per source result value carrying a packed antibiogram."""
    _, F, _ = _imports()
    return (spark.table(config.map_pathology_table)
            .where((F.col("source_table") == "raw") & (F.col("WkgCode") == "INF")
                   & F.col("value_source_value").like("%[<*%"))
            .select("source_record_key", "source_parent_key", "LIMSNo", "specimen_type_code",
                    "valid_until_dt_tm", "value_source_value"))


def _dictionary(spark, config, name, code_column):
    """Published dictionary rows; only REJECTED and RETIRED are excluded."""
    _, F, _ = _imports()
    return (spark.table(f"{config.lookup_schema}.{name}")
            .where(~F.upper(F.coalesce(F.col("status"), F.lit(""))).isin("REJECTED", "RETIRED"))
            .withColumn("_lims_no", F.regexp_extract("code_system", r"^WINPATH_LIMS(\d+)$", 1).cast("int"))
            .where(F.col("_lims_no").isNotNull())
            .withColumnRenamed("code", code_column))


def _current_source_map(spark, config):
    Window, F, _ = _imports()
    rows = (spark.table(f"{config.bronze_schema}.map_pathology_accession_source")
            .where(F.col("is_current") == True)
            .where(F.col("source_system") == "TFC_LIMS"))
    order = Window.partitionBy("source_parent_key").orderBy(F.col("pathology_accession_id").asc_nulls_last())
    return (rows.withColumn("_rn", F.row_number().over(order)).where(F.col("_rn") == 1)
            .select("source_parent_key", "pathology_accession_id"))


def _microbiology_reports(spark, config):
    Window, F, _ = _imports()
    rows = spark.table(f"{config.bronze_schema}.map_pathology_report").where(F.col("report_role") == "microbiology")
    order = Window.partitionBy("source_record_key").orderBy(
        F.col("is_current").desc_nulls_last(), F.col("report_version_id").desc_nulls_last(),
        F.col("lifecycle_status").asc_nulls_last())
    return (rows.withColumn("_rn", F.row_number().over(order)).where(F.col("_rn") == 1)
            .select("source_record_key", "report_version_id", "lifecycle_status", "is_current"))


def build_amr_frames(spark, config: PipelineConfig, *, include_proposed_rules: bool = True):
    # Kept for caller compatibility. PROPOSED is provenance, never a publication gate.
    del include_proposed_rules
    _, F, T = _imports()
    parser = F.udf(parse_antibiogram_json, T.StringType())
    parsed = antibiogram_rows(spark, config).withColumn(
        "_parsed", F.from_json(parser(F.col("value_source_value")), _parsed_schema(T)))
    base = (parsed.join(_current_source_map(spark, config), "source_parent_key", "left")
            .join(_microbiology_reports(spark, config), "source_record_key", "left")
            .withColumn("lifecycle_status", F.coalesce("lifecycle_status", F.lit("unknown")))
            .withColumn("is_current", F.coalesce("is_current", F.col("valid_until_dt_tm").isNull())))

    isolates_raw = base.select("*", F.explode_outer("_parsed.isolates").alias("iso")).where(F.col("iso").isNotNull())
    organisms = _dictionary(spark, config, "pathology_micro_organism_map", "_organism_code")
    isolates = (isolates_raw.alias("b")
        .join(F.broadcast(organisms).alias("o"),
              (F.col("b.LIMSNo") == F.col("o._lims_no")) & (F.col("b.iso.organism_code") == F.col("o._organism_code")), "left")
        .select(
            F.sha2(F.concat_ws("|", F.lit("microbiology_isolate"),
                               F.coalesce(F.col("b.pathology_accession_id"), F.lit("∅")),
                               F.col("b.source_record_key"), F.col("b.iso.isolate_ordinal").cast("string")), 256).alias("microbiology_isolate_id"),
            F.col("b.pathology_accession_id"), F.col("b.report_version_id"), F.col("b.source_record_key"),
            F.col("b.specimen_type_code"),
            F.coalesce(F.col("o.organism_text"), F.col("b.iso.organism_code")).alias("organism_text"),
            F.col("o.organism_snomed_code").cast("string").alias("organism_snomed_code"),
            F.col("o.organism_omop_concept_id").cast("long").alias("organism_omop_concept_id"),
            F.lit(False).alias("suspected_ind"), F.lit(None).cast("string").alias("growth_grade"),
            F.col("b.lifecycle_status"), F.col("b.is_current"), F.lit(True).alias("research_qi_only"),
            F.col("b.iso.organism_code").alias("organism_code"), F.col("b.iso.panel_code").alias("panel_code"),
            F.col("b.iso.isolate_ordinal").alias("isolate_ordinal"), F.col("b.iso.isolate_comment").alias("isolate_comment"),
            F.col("b.LIMSNo").alias("lims_no"), F.col("b.iso.parse_status").alias("parse_status"),
            F.lit(ANTIBIOGRAM_PARSER_VERSION).alias("parser_version")))

    agents = _dictionary(spark, config, "pathology_antimicrobial_map", "_agent_code")
    tokens = isolates_raw.select(
        "source_record_key", "pathology_accession_id", "LIMSNo", "lifecycle_status", "is_current",
        F.col("iso.isolate_ordinal").alias("isolate_ordinal"), F.explode("iso.tokens").alias("tok"))
    susceptibility = (tokens.alias("t")
        .join(F.broadcast(agents).alias("a"),
              (F.col("t.LIMSNo") == F.col("a._lims_no")) & (F.col("t.tok.agent_code") == F.col("a._agent_code")), "left")
        .select(
            F.sha2(F.concat_ws("|", F.lit("antimicrobial_susceptibility"), F.col("t.source_record_key"),
                               F.col("t.isolate_ordinal").cast("string"), F.col("t.tok.token_ordinal").cast("string")), 256).alias("susceptibility_result_id"),
            F.sha2(F.concat_ws("|", F.lit("microbiology_isolate"),
                               F.coalesce(F.col("t.pathology_accession_id"), F.lit("∅")),
                               F.col("t.source_record_key"), F.col("t.isolate_ordinal").cast("string")), 256).alias("microbiology_isolate_id"),
            F.col("t.pathology_accession_id"), F.col("t.source_record_key"),
            F.coalesce(F.col("a.antimicrobial_text"), F.col("t.tok.agent_code"), F.col("t.tok.raw_token")).alias("antimicrobial_text"),
            F.coalesce(F.col("t.tok.agent_code"), F.col("t.tok.raw_token")).alias("antimicrobial_code"),
            F.col("a.antimicrobial_omop_concept_id").cast("long").alias("antimicrobial_omop_concept_id"),
            F.coalesce(F.col("t.tok.flag_raw"), F.col("t.tok.raw_token")).alias("interpretation_raw"),
            F.col("t.tok.interpretation").alias("interpretation"),
            F.lit(None).cast("string").alias("mic_raw"), F.lit(None).cast("double").alias("mic"),
            F.lit(None).cast("string").alias("unit_source_value"), F.col("a.method").alias("method"),
            F.lit("unique_isolate").alias("link_status"), F.col("t.lifecycle_status"), F.col("t.is_current"),
            F.lit(True).alias("research_qi_only"), F.col("t.tok.token_class").alias("token_class"),
            F.col("t.tok.token_ordinal").alias("token_ordinal"),
            F.lit(ANTIBIOGRAM_PARSER_VERSION).alias("parser_version")))
    return isolates, susceptibility


def validate_amr_stage(spark, config: PipelineConfig, isolates, susceptibility):
    """Validate source conservation before either reconciling MERGE can run."""
    _, F, _ = _imports()
    source = antibiogram_rows(spark, config).withColumn("u", F.regexp_replace("value_source_value", r">\]\s*\[<\.", ""))
    per_row = (source.select(
        "source_record_key",
        F.size(F.expr(r"regexp_extract_all(u, '\\[<\\*', 0)")).alias("blocks"),
        F.expr(r"regexp_extract_all(u, '\\[<\\*([^\\]]*)>\\]', 1)").alias("blks"))
        .withColumn("closed_blocks", F.size("blks"))
        .withColumn("tokens", F.aggregate(
            "blks", F.lit(0),
            lambda acc, block: acc + F.size(F.filter(
                F.split(F.regexp_replace(block, r"^[^/]*//[^/]*/", ""), "//"), lambda token: token != "")))))
    staged = (isolates.groupBy("source_record_key")
        .agg(F.count("*").alias("iso_rows"), F.sum((F.col("parse_status") != "ok").cast("int")).alias("flagged"))
        .join(susceptibility.groupBy("source_record_key").agg(F.count("*").alias("sus_rows")), "source_record_key", "full"))
    stats = (per_row.join(staged, "source_record_key", "full").na.fill(0).agg(
        F.count("*").alias("rows"),
        F.sum((F.col("blocks") != F.col("iso_rows")).cast("int")).alias("isolate_mismatch"),
        F.sum(((F.col("flagged") == 0) & (F.col("tokens") != F.col("sus_rows"))).cast("int")).alias("token_mismatch_clean_rows"),
        F.sum(((F.col("flagged") > 0) & (F.col("tokens") > F.col("sus_rows"))).cast("int")).alias("token_short_flagged_rows"),
        F.sum((F.col("flagged") > 0).cast("int")).alias("flagged_rows")).first().asDict())
    if stats["isolate_mismatch"] or stats["token_mismatch_clean_rows"] or stats["token_short_flagged_rows"]:
        raise RuntimeError(f"antibiogram stage does not conserve the source: {stats}")
    return stats


def run_amr(spark, config: PipelineConfig | None = None, *, include_proposed_rules: bool = True,
            full_reconcile: bool = True, validate_stage_keys: bool = True):
    config = config or PipelineConfig()
    ensure_contracts(spark, config)
    live_agents = _dictionary(spark, config, "pathology_antimicrobial_map", "_agent_code").limit(1).count()
    if live_agents == 0:
        return {"status": "DISABLED_BY_LOOKUP",
                "reason": "no non-retired WINPATH_LIMS* rows in pathology_antimicrobial_map"}
    isolates, susceptibility = build_amr_frames(spark, config, include_proposed_rules=include_proposed_rules)
    conservation = validate_amr_stage(spark, config, isolates, susceptibility)
    outputs = {"map_pathology_microbiology_isolate": isolates,
               "map_pathology_antimicrobial_susceptibility": susceptibility}
    metrics = {name: merge_contract(spark, f"{config.bronze_schema}.{name}", frame, contract(name),
                                    delete_not_matched=full_reconcile, validate_stage_keys=validate_stage_keys)
               for name, frame in outputs.items()}
    metrics["conservation"] = conservation
    return metrics
