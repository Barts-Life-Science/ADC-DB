"""Deterministic indication and diagnosis-context builder."""

from __future__ import annotations

from pathology_contracts import contract
from pathology_pipeline import PipelineConfig, ensure_contracts, merge_contract


def _imports():
    from pyspark.sql import Window, functions as F, types as T

    return Window, F, T


def _active_filter(spark, table_name: str) -> str:
    return (
        "ADC_Deleted IS NULL"
        if "adc_deleted" in {column.lower() for column in spark.table(table_name).columns}
        else "TRUE"
    )


def build_explicit_source_fields(spark, config: PipelineConfig):
    Window, F, _ = _imports()
    source_map = spark.table(f"{config.bronze_schema}.map_pathology_accession_source").filter(
        F.col("is_current") == True
    )

    sample = spark.table(config.sample_table).filter(
        F.expr(_active_filter(spark, config.sample_table))
    )
    sample_window = Window.partitionBy("LIMSNo", "LabNo").orderBy(
        F.col("ADC_UPDT").desc_nulls_last(), F.col("SampleDT").desc_nulls_last()
    )
    sample = sample.withColumn("_rn", F.row_number().over(sample_window)).filter(
        F.col("_rn") == 1
    )
    raw = (
        sample.withColumn(
            "source_parent_key",
            F.concat_ws(
                "|",
                F.lit("raw"),
                F.coalesce(F.col("LIMSNo").cast("string"), F.lit("∅")),
                F.coalesce(F.col("LabNo"), F.lit("∅")),
            ),
        )
        .join(
            source_map.filter(F.col("source_system") == "TFC_LIMS").select(
                "source_parent_key", "source_accession_id", "pathology_accession_id"
            ),
            "source_parent_key",
            "inner",
        )
        .select(
            "source_accession_id",
            "pathology_accession_id",
            F.explode(
                F.array(
                    F.struct(F.lit("ClinicalDetails").alias("source_field"), F.col("ClinicalDetails").alias("source_text")),
                    F.struct(F.lit("Reason").alias("source_field"), F.col("Reason").alias("source_text")),
                    F.struct(F.lit("Conditions").alias("source_field"), F.col("Conditions").alias("source_text")),
                )
            ).alias("evidence"),
        )
        .select("source_accession_id", "pathology_accession_id", "evidence.*")
        .filter(F.col("source_text").isNotNull() & (F.length(F.trim("source_text")) > 0))
    )

    linked_accessions = (
        source_map.filter(
            (F.col("source_system") == "CERNER") & F.col("order_id").isNotNull()
        )
        .select("source_accession_id", "pathology_accession_id", "order_id")
        .dropDuplicates(["source_accession_id", "order_id"])
    )
    comments = (
        spark.table("4_prod.raw.mill_order_comment")
        .join(
            spark.table("4_prod.raw.mill_long_text").select(
                F.col("LONG_TEXT_ID").cast("long").alias("_long_text_id"), "LONG_TEXT"
            ),
            F.col("LONG_TEXT_ID").cast("long") == F.col("_long_text_id"),
            "inner",
        )
        .select(F.col("ORDER_ID").cast("long").alias("order_id"), F.col("LONG_TEXT").alias("source_text"))
        .filter(F.col("source_text").isNotNull() & (F.length(F.trim("source_text")) > 0))
        .withColumn("source_field", F.lit("OrderComment"))
    )
    details = (
        spark.table("4_prod.raw.mill_order_detail")
        .select(
            F.col("ORDER_ID").cast("long").alias("order_id"),
            F.coalesce("OE_FIELD_DISPLAY_VALUE_EXTEND", "OE_FIELD_DISPLAY_VALUE").alias("source_text"),
            F.concat_ws(":", F.lit("OrderDetail"), F.coalesce("OE_FIELD_MEANING", F.col("OE_FIELD_ID").cast("string"))).alias("source_field"),
        )
        .filter(F.col("source_text").isNotNull() & (F.length(F.trim("source_text")) > 0))
    )
    orders = (
        spark.table("4_prod.raw.mill_orders")
        .select(
            F.col("ORDER_ID").cast("long").alias("order_id"),
            F.explode(
                F.array(
                    F.struct(F.lit("OrderDetailDisplayLine").alias("source_field"), F.col("ORDER_DETAIL_DISPLAY_LINE").alias("source_text")),
                    F.struct(F.lit("ClinicalDisplayLine").alias("source_field"), F.col("CLINICAL_DISPLAY_LINE").alias("source_text")),
                )
            ).alias("evidence"),
        )
        .select("order_id", "evidence.*")
        .filter(F.col("source_text").isNotNull() & (F.length(F.trim("source_text")) > 0))
    )
    linked = (
        comments.unionByName(details).unionByName(orders)
        .join(linked_accessions, "order_id", "inner")
        .select("source_accession_id", "pathology_accession_id", "source_field", "source_text")
        .dropDuplicates()
    )
    return raw.unionByName(linked)


def build_rule_indications(
    spark,
    config: PipelineConfig,
    *,
    include_proposed_rules: bool = False,
):
    _, F, _ = _imports()
    statuses = ["APPROVED"] + (["PROPOSED"] if include_proposed_rules else [])
    rules_df = spark.table(f"{config.lookup_schema}.pathology_indication_rule").filter(
        F.upper(F.col("status")).isin(*statuses)
    )
    fields = build_explicit_source_fields(spark, config).alias("f")
    rules = F.broadcast(rules_df).alias("r")
    joined = fields.join(
        rules,
        (F.col("r.source_field") == F.lit("*"))
        | (F.col("r.source_field") == F.col("f.source_field")),
        "inner",
    )
    normalized_source = F.lower(
        F.trim(F.regexp_replace(F.col("f.source_text"), r"\s+", " "))
    )
    normalized_pattern = F.lower(
        F.trim(F.regexp_replace(F.col("r.pattern"), r"\s+", " "))
    )
    is_exact = F.col("r.match_type") == "exact_normalized"
    is_regex = F.col("r.match_type") == "bounded_regex"
    # Spark SQL's regexp functions accept a governed pattern column. Lookup
    # promotion validates syntax before any rule reaches APPROVED status.
    regex_match = F.expr("regexp_extract(f.source_text, r.pattern, 0)")
    regex_position = F.expr("regexp_instr(f.source_text, r.pattern)")
    matched = (
        joined.filter(
            (is_exact & (normalized_source == normalized_pattern))
            | (is_regex & F.expr("f.source_text RLIKE r.pattern"))
        )
        .withColumn(
            "evidence_text",
            F.when(is_exact, F.col("f.source_text")).otherwise(regex_match),
        )
        .withColumn(
            "evidence_start",
            F.when(is_exact, F.lit(0)).otherwise(regex_position - F.lit(1)),
        )
        .withColumn(
            "evidence_end",
            F.col("evidence_start") + F.length(F.col("evidence_text")),
        )
        .select(
            F.col("f.pathology_accession_id").alias("pathology_accession_id"),
            F.col("f.source_accession_id").alias("source_accession_id"),
            F.lit("explicit_indication").alias("relation_type"),
            F.col("f.source_field").alias("source_field"),
            F.col("f.source_text").alias("source_text"),
            "evidence_text",
            "evidence_start",
            "evidence_end",
            F.col("r.snomed_code").alias("snomed_code"),
            F.col("r.snomed_term").alias("snomed_term"),
            F.col("r.omop_concept_id").alias("omop_concept_id"),
            F.col("r.default_assertion").alias("assertion"),
            F.lit("unknown").alias("temporality"),
            F.lit("patient").alias("experiencer"),
            F.col("r.rule_id").alias("rule_id"),
            F.col("r.rule_version").alias("rule_version"),
            F.col("r.confidence").cast("double").alias("confidence"),
            F.col("r.status").alias("_rule_status"),
        )
    )
    return (
        matched.withColumn(
            "mapping_status",
            F.when(F.upper(F.col("_rule_status")) == "APPROVED", "approved_rule").otherwise(
                "proposed"
            ),
        )
        .drop("_rule_status")
        .withColumn("ig_release_status", F.lit("dev_only"))
        .withColumn("is_current", F.lit(True))
        .withColumn("research_qi_only", F.lit(True))
        .withColumn(
            "indication_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit("pathology_indication"),
                    "source_accession_id",
                    "source_field",
                    F.col("evidence_start").cast("string"),
                    F.col("evidence_end").cast("string"),
                    "rule_id",
                    "rule_version",
                ),
                256,
            ),
        )
    )


def build_diagnosis_context(spark, config: PipelineConfig):
    """Attach coded diagnoses as context without calling them indications."""

    Window, F, _ = _imports()
    accession_candidates = (
        spark.table(f"{config.bronze_schema}.map_pathology_accession_source")
        .filter(
            (F.col("is_current") == True)
            & (F.col("person_projection_status") == "eligible")
            & F.col("person_id").isNotNull()
        )
        .select(
            "pathology_accession_id",
            "source_accession_id",
            "person_id",
            "encounter_id",
            "source_system",
            "source_parent_key",
            "order_id",
            "ADC_UPDT",
            "sample_dt",
            "request_dt",
            "report_dt",
            F.coalesce("sample_dt", "request_dt", "report_dt").alias("accession_dt"),
        )
    )
    # map_pathology_accession_source is keyed by source parent, so a source
    # accession can legitimately have repeated candidate rows. An unordered
    # dropDuplicates() made the selected accession date depend on task order,
    # which changed diagnosis-window membership between identical runs.
    payload = F.struct(
        "pathology_accession_id",
        "person_id",
        "encounter_id",
        "accession_dt",
    )
    total_order = F.struct(
        "ADC_UPDT",
        "sample_dt",
        "request_dt",
        "report_dt",
        "pathology_accession_id",
        "person_id",
        "encounter_id",
        "source_system",
        "source_parent_key",
        "order_id",
    )
    accessions = (
        accession_candidates.groupBy("source_accession_id")
        .agg(F.max_by(payload, total_order).alias("_canonical"))
        .select("source_accession_id", "_canonical.*")
    )
    diagnosis = spark.table("4_prod.bronze.map_diagnosis").select(
        F.col("DIAGNOSIS_ID").cast("long").alias("diagnosis_id"),
        F.col("PERSON_ID").cast("long").alias("diag_person_id"),
        F.col("ENCNTR_ID").cast("long").alias("diag_encounter_id"),
        F.coalesce("DIAG_DT_TM", "ASSERTED_DT_TM", "BEG_EFFECTIVE_DT_TM").alias("diagnosis_dt"),
        F.col("DIAGNOSIS_TEXT").alias("source_text"),
        F.col("SNOMED_CODE").cast("string").alias("snomed_code"),
        F.col("SNOMED_TERM").alias("snomed_term"),
        F.col("OMOP_CONCEPT_ID").cast("long").alias("omop_concept_id"),
        F.col("confirmation_status_desc").alias("confirmation_status"),
    )
    joined = accessions.alias("a").join(
        diagnosis.alias("d"),
        (F.col("a.person_id") == F.col("d.diag_person_id"))
        & (
            (F.col("a.encounter_id").isNotNull() & (F.col("a.encounter_id") == F.col("d.diag_encounter_id")))
            | (
                F.col("a.accession_dt").isNotNull()
                & F.col("d.diagnosis_dt").between(
                    F.col("a.accession_dt") - F.expr("INTERVAL 30 DAYS"),
                    F.col("a.accession_dt") + F.expr("INTERVAL 7 DAYS"),
                )
            )
        ),
        "inner",
    )
    ranked = joined.withColumn(
        "_context_priority",
        F.when(F.col("a.encounter_id") == F.col("d.diag_encounter_id"), 0).otherwise(1),
    ).withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("a.source_accession_id", "d.diagnosis_id").orderBy(
                "_context_priority", F.abs(F.datediff("d.diagnosis_dt", "a.accession_dt"))
            )
        ),
    ).filter(F.col("_rn") == 1)
    return (
        ranked.select(
            F.sha2(F.concat_ws("|", F.lit("pathology_diagnosis_context"), F.col("a.source_accession_id"), F.col("d.diagnosis_id")), 256).alias("indication_id"),
            F.col("a.pathology_accession_id").alias("pathology_accession_id"),
            F.col("a.source_accession_id").alias("source_accession_id"),
            F.lit("clinical_context").alias("relation_type"),
            F.lit("map_diagnosis").alias("source_field"),
            F.col("d.source_text").alias("source_text"),
            F.col("d.source_text").alias("evidence_text"),
            F.lit(0).alias("evidence_start"),
            F.length(F.col("d.source_text")).alias("evidence_end"),
            F.col("d.snomed_code").alias("snomed_code"),
            F.col("d.snomed_term").alias("snomed_term"),
            F.col("d.omop_concept_id").alias("omop_concept_id"),
            F.when(F.lower(F.coalesce(F.col("d.confirmation_status"), F.lit(""))).rlike("rule out|suspect|possible"), "possible").otherwise("present").alias("assertion"),
            F.lit("current").alias("temporality"),
            F.lit("patient").alias("experiencer"),
            F.lit("diagnosis_context_window_v1").alias("rule_id"),
            F.lit("1").alias("rule_version"),
            F.lit(1.0).alias("confidence"),
            F.lit("approved_exact").alias("mapping_status"),
            F.lit("dev_only").alias("ig_release_status"),
            F.lit(True).alias("is_current"),
            F.lit(True).alias("research_qi_only"),
        )
    )


def run_indications(
    spark,
    config: PipelineConfig | None = None,
    *,
    include_proposed_rules: bool = False,
    include_diagnosis_context: bool = True,
    full_reconcile: bool = True,
    validate_stage_keys: bool = True,
):
    config = config or PipelineConfig()
    ensure_contracts(spark, config)
    explicit = build_rule_indications(
        spark, config, include_proposed_rules=include_proposed_rules
    )
    output = explicit
    if include_diagnosis_context:
        output = output.unionByName(build_diagnosis_context(spark, config), allowMissingColumns=True)
    return merge_contract(
        spark,
        f"{config.bronze_schema}.map_pathology_indication",
        output,
        contract("map_pathology_indication"),
        delete_not_matched=full_reconcile,
        validate_stage_keys=validate_stage_keys,
    )
