"""GEN4 coding for the genetics frames: deterministic codes with provenance, set in bronze.

code_tests    - LOINC/SNOMED/OMOP/NHS test-directory codes per assay component (pathology_genetic_assay_concept).
code_reports  - SNOMED sample site and indication from report text (pathology_genetic_term_rule).
code_findings - GRCh38 coordinates (pathology_variant_mapper on the driver over distinct variants), then the
                most specific OMOP Genomic concept (dna > rna > protein > fusion > gene) and ClinVar ids.
"""

from __future__ import annotations

from pathology_variant_mapper import TranscriptModel, map_finding, window_fetcher

CONCEPT_LEVELS = ("dna", "rna", "protein", "fusion", "gene")
MAPPER_BATCH = 5_000


def _imports():
    from pyspark.sql import functions as F, types as T

    return F, T


def code_tests(spark, config, tests):
    """Join each test's assay component to its rule-based codes; record the rule id."""
    F, _ = _imports()
    assay = spark.table(f"{config.lookup_schema}.pathology_genetic_assay_concept").select(
        "assay_component",
        F.col("loinc_code").alias("_loinc"),
        F.col("snomed_code").alias("_snomed"),
        F.col("omop_concept_id").alias("_omop"),
        F.col("nhs_test_directory_code").alias("_nhs"),
        F.col("rule_id").alias("_assay_rule"),
    )
    return (
        tests.join(F.broadcast(assay), "assay_component", "left")
        .withColumn("test_loinc_code", F.col("_loinc"))
        .withColumn("test_snomed_code", F.col("_snomed"))
        .withColumn("test_omop_concept_id", F.col("_omop").cast("long"))
        .withColumn("nhs_test_directory_code", F.col("_nhs"))
        .withColumn("test_code_method", F.when(F.col("_assay_rule").isNotNull(), F.concat(F.lit("assay_rule:"), F.col("_assay_rule"))))
        .drop("_loinc", "_snomed", "_omop", "_nhs", "_assay_rule")
    )


def _first_rule_expr(F, column: str, rules: list):
    """CASE expression returning the id of the best-priority rule whose pattern matches column."""
    expression = F.lit(None).cast("string")
    for rule in sorted(rules, key=lambda r: (r["priority"], r["rule_id"]), reverse=True):
        expression = F.when(F.col(column).rlike(rule["pattern"]), F.lit(rule["rule_id"])).otherwise(expression)
    return expression


def code_reports(spark, config, reports):
    """SNOMED sample site and indication from the report's own text, with the rule ids that set them."""
    F, _ = _imports()
    rules = [row.asDict() for row in spark.table(f"{config.lookup_schema}.pathology_genetic_term_rule").collect()]
    details = spark.createDataFrame(
        [(r["rule_id"], r["snomed_code"], int(r["concept_id"])) for r in rules],
        "rule_id string, snomed_code string, concept_id bigint",
    )
    site_rules = [r for r in rules if r["target_field"] == "sample_site"]
    indication_rules = [r for r in rules if r["target_field"] == "indication"]
    coded = (
        reports.withColumn("_site_rule", _first_rule_expr(F, "sample_type_site_raw", site_rules))
        .withColumn("_ind_rule", _first_rule_expr(F, "clinical_details_raw", indication_rules))
    )
    site = details.select(F.col("rule_id").alias("_site_rule"), F.col("snomed_code").alias("_site_code"), F.col("concept_id").alias("_site_cid"))
    ind = details.select(F.col("rule_id").alias("_ind_rule"), F.col("snomed_code").alias("_ind_code"), F.col("concept_id").alias("_ind_cid"))
    return (
        coded.join(F.broadcast(site), "_site_rule", "left").join(F.broadcast(ind), "_ind_rule", "left")
        .withColumn("sample_site_snomed_code", F.col("_site_code"))
        .withColumn("sample_site_concept_id", F.col("_site_cid"))
        .withColumn("indication_snomed_code", F.col("_ind_code"))
        .withColumn("indication_concept_id", F.col("_ind_cid"))
        .withColumn("term_rule_ids", F.nullif(F.concat_ws(",", "_site_rule", "_ind_rule"), F.lit("")))
        .drop("_site_rule", "_ind_rule", "_site_code", "_site_cid", "_ind_code", "_ind_cid")
    )


def derive_coordinates(spark, config, variants: list):
    """Map distinct (gene, transcript, hgvs_c, hgvs_p) on the driver; returns a DataFrame keyed by those four."""
    F, T = _imports()
    genes = sorted({v["gene"] for v in variants if v["gene"]})
    bases = sorted({v["transcript"].split(".")[0] for v in variants if v["transcript"]})
    model_rows = (
        spark.table(f"{config.lookup_schema}.pathology_transcript_model")
        .where(F.col("gene_symbol").isin(genes) | F.col("transcript_base").isin(bases))
        .collect()
    )
    models = {
        r["transcript_accession"]: TranscriptModel(
            r["transcript_accession"], r["chromosome"], r["strand"], int(r["cds_start"]), int(r["cds_end"]),
            list(zip(map(int, r["exon_starts"].split(",")), map(int, r["exon_ends"].split(",")))), r["mane_status"],
        )
        for r in model_rows
    }
    mane = {r["gene_symbol"].upper(): r["transcript_accession"] for r in model_rows if r["mane_status"] == "MANE Select"}
    chromosomes = sorted({m.chromosome for m in models.values()})
    windows = {
        (r["chromosome"], int(r["chunk_start"])): r["sequence"]
        for r in spark.table(f"{config.lookup_schema}.pathology_reference_window").where(F.col("chromosome").isin(chromosomes)).collect()
    }
    fetch = window_fetcher(windows)
    # Stated versions without a model project only when their CDS is identical to a modelled version (B2b).
    equivalence = {
        r["stated_accession"]: r["model_accession"]
        for r in spark.table(f"{config.lookup_schema}.pathology_transcript_equivalence").where(F.col("cds_identical")).collect()
    }
    rows = []
    for v in variants:
        out = map_finding(v["gene"], v["transcript"], v["hgvs_c"], models, mane, fetch, equivalence, hgvs_p=v["hgvs_p"])
        rows.append((v["gene"], v["transcript"], v["hgvs_c"], v["hgvs_p"], out["used_transcript"], out.get("transcript_resolution"),
                     out["genome_build"], out["chromosome"], out["position_start"], out["position_end"],
                     out["ref_allele"], out["alt_allele"], out["variant_key"], out["coordinate_method"]))
    schema = ("_m_gene string, _m_tx string, _m_c string, _m_p string, _used_transcript string, _resolution string, _build string, "
              "_chrom string, _pos_start bigint, _pos_end bigint, _ref string, _alt string, _variant_key string, _coord_method string")
    frames = [spark.createDataFrame(rows[i:i + MAPPER_BATCH], schema) for i in range(0, max(len(rows), 1), MAPPER_BATCH)]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def code_findings(spark, config, findings):
    """Coordinates, OMOP Genomic concept (most specific level) and ClinVar ids for each finding."""
    F, _ = _imports()
    distinct = (
        findings.where(F.col("hgvs_c_parsed").isNotNull())
        .select(F.col("normalized_gene_symbol").alias("gene"), F.col("resolved_transcript").alias("transcript"),
                F.col("hgvs_c_parsed").alias("hgvs_c"), F.col("hgvs_p_parsed").alias("hgvs_p"))
        .distinct().collect()
    )
    variants = [row.asDict() for row in distinct]
    if variants:
        mapped = derive_coordinates(spark, config, variants)
        findings = findings.join(
            mapped,
            F.col("normalized_gene_symbol").eqNullSafe(F.col("_m_gene"))
            & F.col("resolved_transcript").eqNullSafe(F.col("_m_tx"))
            & (F.col("hgvs_c_parsed") == F.col("_m_c"))
            & F.col("hgvs_p_parsed").eqNullSafe(F.col("_m_p")),
            "left",
        )
    else:
        for name in ("_used_transcript", "_resolution", "_build", "_chrom", "_variant_key", "_ref", "_alt", "_coord_method"):
            findings = findings.withColumn(name, F.lit(None).cast("string"))
        findings = findings.withColumn("_pos_start", F.lit(None).cast("long")).withColumn("_pos_end", F.lit(None).cast("long"))
    findings = (
        findings.withColumn("genome_build", F.col("_build"))
        .withColumn("chromosome", F.col("_chrom"))
        .withColumn("position_start", F.col("_pos_start"))
        .withColumn("position_end", F.col("_pos_end"))
        .withColumn("ref_allele", F.col("_ref"))
        .withColumn("alt_allele", F.col("_alt"))
        .withColumn("variant_key", F.col("_variant_key"))
        .withColumn("coordinate_method",
                    F.when(F.col("hgvs_c_parsed").isNull(), F.lit("no_parsed_hgvs_c")).otherwise(F.col("_coord_method")))
        .withColumn("transcript_source",
                    F.when(F.col("_resolution") == "mane_select", F.lit("mane_select")).otherwise(F.col("transcript_source")))
        # A stated transcript is kept as stated (a projection is recorded in coordinate_method); only when nothing
        # was stated does the transcript the mapper used (MANE Select) become the resolved transcript.
        .withColumn("resolved_transcript", F.coalesce(F.col("resolved_transcript"), F.col("_used_transcript")))
    )

    # --- OMOP Genomic: one lookup per level, lowest concept_id per key -------------------------------------------
    synonyms = spark.table(f"{config.lookup_schema}.pathology_omop_genomic_synonym").groupBy("match_level", "match_key").agg(
        F.min("concept_id").alias("concept_id")
    )
    keys = {
        "dna": F.col("variant_key"),
        "rna": F.when(F.col("resolved_transcript").startswith("NM_") & F.col("hgvs_c_parsed").isNotNull(),
                      F.concat(F.split(F.col("resolved_transcript"), r"\.")[0], F.lit(":"), F.col("hgvs_c_parsed"))),
        "protein": F.when(F.col("hgvs_p_parsed").isNotNull(),
                          F.concat(F.col("normalized_gene_symbol"), F.lit(":"), F.regexp_replace("hgvs_p_parsed", r"[()]", ""))),
        "fusion": F.when(F.col("alteration_type") == "fusion",
                         F.concat(F.col("normalized_gene_symbol"), F.lit("::"),
                                  F.coalesce(F.col("partner_normalized_symbol"), F.upper(F.col("partner_gene_symbol"))))),
        "gene": F.when(F.col("hgnc_id").isNotNull(), F.concat(F.lit("HGNC:"), F.regexp_replace("hgnc_id", r"^HGNC:", ""))),
    }
    for level, key in keys.items():
        lookup = synonyms.where(F.col("match_level") == level).select(
            F.col("match_key").alias(f"_{level}_key"), F.col("concept_id").alias(f"_{level}_cid")
        )
        findings = findings.withColumn(f"_{level}_key", key).join(F.broadcast(lookup), f"_{level}_key", "left")
    level_expr = F.lit(None).cast("string")
    for level in reversed(CONCEPT_LEVELS):
        level_expr = F.when(F.col(f"_{level}_cid").isNotNull(), F.lit(level)).otherwise(level_expr)
    findings = (
        findings.withColumn("omop_genomic_concept_level", level_expr)
        .withColumn("omop_genomic_concept_id", F.coalesce(*[F.col(f"_{level}_cid") for level in CONCEPT_LEVELS]).cast("long"))
        .withColumn("concept_method", F.coalesce(*[
            F.when(F.col(f"_{level}_cid").isNotNull(), F.concat(F.lit(f"{level}:"), F.col(f"_{level}_key"))) for level in CONCEPT_LEVELS
        ]))
    )

    # --- ClinVar on the VCF key; OMOP ClinVar concept where the vocabulary has the VariationID --------------------
    clinvar = spark.table(f"{config.lookup_schema}.pathology_clinvar_variant").groupBy("variant_key").agg(
        F.min("variation_id").alias("_clinvar_id")
    )
    clinvar_concepts = spark.table(config.omop_concept_table).where(F.col("vocabulary_id") == "ClinVar").select(
        F.col("concept_code").alias("_clinvar_code"), F.col("concept_id").alias("_clinvar_cid")
    )
    findings = (
        findings.join(clinvar.withColumnRenamed("variant_key", "_cv_key"), F.col("variant_key") == F.col("_cv_key"), "left")
        .withColumn("clinvar_variation_id", F.col("_clinvar_id").cast("long"))
        .join(F.broadcast(clinvar_concepts), F.col("_clinvar_id").cast("string") == F.col("_clinvar_code"), "left")
        .withColumn("clinvar_concept_id", F.col("_clinvar_cid").cast("long"))
    )
    helper = [c for c in findings.columns if c.startswith(("_m_", "_dna_", "_rna_", "_protein_", "_fusion_", "_gene_", "_clinvar", "_cv_"))]
    helper += ["_used_transcript", "_resolution", "_build", "_chrom", "_pos_start", "_pos_end", "_ref", "_alt", "_variant_key", "_coord_method"]
    return findings.drop(*[c for c in helper if c in findings.columns])
