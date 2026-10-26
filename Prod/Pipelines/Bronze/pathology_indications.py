"""Deterministic indication and diagnosis-context builder."""

from __future__ import annotations

from pathology_contracts import contract
from pathology_pipeline import PipelineConfig, ensure_contracts, merge_contract, reconcile_scoped_stale

# INDICATIONS_INCREMENTAL_V1: the diagnosis context of an accession depends only on its own canonical row and its
# patient's diagnoses, so a run can rebuild just the accessions whose patient or row changed. Before this, every
# map_diagnosis version change rebuilt and full-merged all 3.39B context rows (6.5 h on 2026-10-05); a typical
# week touches ~4% of them. The full reconcile stays for forced/periodic runs, change-feed gaps and large changes.
DIAGNOSIS_TABLE = "4_prod.bronze.map_diagnosis"
INCREMENTAL_MAX_PATIENT_FRACTION = 0.25


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


def build_diagnosis_context(spark, config: PipelineConfig, accession_scope=None):
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
    if accession_scope is not None:
        # Incremental run: only the affected accessions (all their candidate rows, so the canonical pick is unchanged).
        accession_candidates = accession_candidates.join(accession_scope, "source_accession_id", "left_semi")
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
    diagnosis = spark.table(DIAGNOSIS_TABLE).select(
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
    if accession_scope is not None:
        # Only those accessions' patients' diagnoses can pair with them.
        patients = accessions.select(F.col("person_id").alias("diag_person_id")).distinct()
        diagnosis = diagnosis.join(patients, "diag_person_id", "left_semi")
    # INDICATIONS_KEYED_CONTEXT_V1: the OR join left person_id as the only equi-key, so every accession met every
    # diagnosis of the same person before the encounter/window test (2026-10-04: 30.25B candidate pairs, 56.9M for
    # one person; the full rebuild's indications lane spilled for hours). Two keyed joins give the same pairs: one on
    # (person, encounter), one on (person, week) with each accession spread over the weeks its -30/+7 day window
    # touches and the exact window applied after. A pair found by both is ranked once below, as before.
    by_encounter = accessions.alias("a").join(
        diagnosis.alias("d"),
        (F.col("a.person_id") == F.col("d.diag_person_id"))
        & F.col("a.encounter_id").isNotNull()
        & (F.col("a.encounter_id") == F.col("d.diag_encounter_id")),
        "inner",
    )
    day = lambda c: F.datediff(F.to_date(c), F.lit("1970-01-01"))
    windowed = accessions.filter(F.col("accession_dt").isNotNull()).withColumn(
        "_week", F.explode(F.sequence(F.floor((day("accession_dt") - 30) / 7), F.floor((day("accession_dt") + 7) / 7)))
    )
    dated = diagnosis.filter(F.col("diagnosis_dt").isNotNull()).withColumn("_week", F.floor(day("diagnosis_dt") / 7))
    by_window = (
        windowed.alias("a").join(
            dated.alias("d"),
            (F.col("a.person_id") == F.col("d.diag_person_id")) & (F.col("a._week") == F.col("d._week")),
            "inner",
        )
        .filter(
            F.col("d.diagnosis_dt").between(
                F.col("a.accession_dt") - F.expr("INTERVAL 30 DAYS"),
                F.col("a.accession_dt") + F.expr("INTERVAL 7 DAYS"),
            )
        )
        .select("a.*", "d.*")
        .drop("_week")
    )
    joined = by_encounter.unionByName(by_window)
    ranked = joined.withColumn(
        "_context_priority",
        F.when(F.col("encounter_id") == F.col("diag_encounter_id"), 0).otherwise(1),
    ).withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("source_accession_id", "diagnosis_id").orderBy(
                "_context_priority", F.abs(F.datediff("diagnosis_dt", "accession_dt"))
            )
        ),
    ).filter(F.col("_rn") == 1)
    return (
        ranked.select(
            F.sha2(F.concat_ws("|", F.lit("pathology_diagnosis_context"), F.col("source_accession_id"), F.col("diagnosis_id")), 256).alias("indication_id"),
            F.col("pathology_accession_id").alias("pathology_accession_id"),
            F.col("source_accession_id").alias("source_accession_id"),
            F.lit("clinical_context").alias("relation_type"),
            F.lit("map_diagnosis").alias("source_field"),
            F.col("source_text").alias("source_text"),
            F.col("source_text").alias("evidence_text"),
            F.lit(0).alias("evidence_start"),
            F.length(F.col("source_text")).alias("evidence_end"),
            F.col("snomed_code").alias("snomed_code"),
            F.col("snomed_term").alias("snomed_term"),
            F.col("omop_concept_id").alias("omop_concept_id"),
            F.when(F.lower(F.coalesce(F.col("confirmation_status"), F.lit(""))).rlike("rule out|suspect|possible"), "possible").otherwise("present").alias("assertion"),
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


def _change_feed(spark, table, first_version, last_version):
    """Rows of a Delta table's change feed between two versions (inclusive); None if there is nothing to read."""
    if first_version > last_version:
        return None
    return (spark.read.format("delta").option("readChangeFeed", "true")
            .option("startingVersion", int(first_version)).option("endingVersion", int(last_version)).table(table))


# CONTEXT_LINK_V1 (BRONZE_FIX_20261025C): the diagnosis context is a narrow link table, map_pathology_diagnosis_context,
# one row per (canonical accession, diagnosis), with no text. It used to be ~3.4B rows of relation_type
# 'clinical_context' in map_pathology_indication, each copying the diagnosis text twice, which silver then published
# as indications. Silver now joins text and codes through clinical_condition.

DIAGNOSIS_CONTEXT_RULE = "diagnosis_context_window_v1"
CONTEXT_TABLE = "map_pathology_diagnosis_context"

# The map_diagnosis columns the link reads. An update that leaves all of them unchanged (text, codes, display names,
# similarity scores) cannot change a link row, so it must not put its patient in scope.
DIAGNOSIS_CONTEXT_COLUMNS = ['DIAGNOSIS_ID', 'PERSON_ID', 'ENCNTR_ID', 'DIAG_DT_TM', 'ASSERTED_DT_TM', 'BEG_EFFECTIVE_DT_TM']

# The map_pathology_accession_source columns the link reads (keys, identity, encounter, dates, eligibility).
# ADC_UPDT only breaks ties between candidate rows of one accession, so a change to it alone changes no link row.
# Before this, every accession_source update counted: PEGP's 10-17 rewrite of all 348M rows forced a full rebuild.
ACCESSION_CONTEXT_COLUMNS = ['source_system', 'source_parent_key', 'source_accession_id', 'pathology_accession_id',
                             'person_id', 'encounter_id', 'order_id', 'sample_dt', 'request_dt', 'report_dt',
                             'is_current', 'person_projection_status']


def _changed_images(changes, keys, columns):
    """Change-feed rows that can alter the link: inserts, deletes, and both images of an update whose `columns`
    differ between its pre- and post-image (both images, so a row that moved patient or accession keeps both)."""
    _, F, _ = _imports()
    fingerprint = F.sha2(F.to_json(F.struct(*[F.col(c) for c in columns])), 256)
    marked = changes.withColumn("_context_fp", fingerprint)
    pre = marked.where(F.col("_change_type") == "update_preimage").select(
        *keys, "_commit_version", F.col("_context_fp").alias("_pre_fp"))
    post = marked.where(F.col("_change_type") == "update_postimage").select(
        *keys, "_commit_version", F.col("_context_fp").alias("_post_fp"))
    changed_updates = (pre.join(post, [*keys, "_commit_version"], "full_outer")
                       .where(~F.col("_pre_fp").eqNullSafe(F.col("_post_fp")))
                       .select(*keys, "_commit_version"))
    updates = (marked.where(F.col("_change_type").isin("update_preimage", "update_postimage"))
               .join(changed_updates, [*keys, "_commit_version"], "left_semi"))
    inserts_deletes = marked.where(F.col("_change_type").isin("insert", "delete"))
    return inserts_deletes.unionByName(updates).drop("_context_fp")


def _context_relevant_diagnosis_changes(changes):
    """map_diagnosis change-feed rows that can alter the link."""
    return _changed_images(changes, ["DIAGNOSIS_ID"], DIAGNOSIS_CONTEXT_COLUMNS)


def _context_relevant_accession_changes(changes):
    """map_pathology_accession_source change-feed rows that can alter the link."""
    return _changed_images(changes, ["source_system", "source_parent_key"], ACCESSION_CONTEXT_COLUMNS)


def changed_scope(spark, config: PipelineConfig, diagnosis_versions, accession_versions):
    """The canonical accessions (pathology_accession_id) whose diagnosis-context links may have changed.

    diagnosis_versions / accession_versions are (first, last) versions of map_diagnosis and
    map_pathology_accession_source to read changes over. Returns (scope DataFrame of pathology_accession_id, stats),
    or (None, stats) when only a full reconcile is safe: versions unknown, a change feed unreadable (vacuumed, or the
    table was replaced), or more than INCREMENTAL_MAX_PATIENT_FRACTION of patients changed. Only changes to the
    columns the link reads count (CONTEXT_LINK_V1).
    """
    _, F, _ = _imports()
    if diagnosis_versions is None or accession_versions is None:
        return None, {"reason": "no stored versions (first incremental run)"}
    source = f"{config.bronze_schema}.map_pathology_accession_source"
    relevant_accessions = None
    try:
        diagnosis_changes = _change_feed(spark, DIAGNOSIS_TABLE, *diagnosis_versions)
        accession_changes = _change_feed(spark, source, *accession_versions)
        frames = []
        if diagnosis_changes is not None:
            frames.append(_context_relevant_diagnosis_changes(diagnosis_changes).select(
                F.col("PERSON_ID").cast("long").alias("person_id")))
        if accession_changes is not None:
            relevant_accessions = _context_relevant_accession_changes(accession_changes)
            frames.append(relevant_accessions.select(F.col("person_id").cast("long").alias("person_id")))
        if not frames:
            return spark.createDataFrame([], "pathology_accession_id string"), {"reason": "no new versions", "patients": 0}
        changed = frames[0]
        for frame in frames[1:]:
            changed = changed.unionByName(frame)
        changed = changed.where(F.col("person_id").isNotNull()).distinct()
        changed_count = changed.count()
    except Exception as exc:  # AnalysisException for a vacuumed or replaced range
        return None, {"reason": f"change feed unreadable: {type(exc).__name__}: {str(exc)[:200]}"}
    current = spark.table(source).filter(
        (F.col("is_current") == True)
        & (F.col("person_projection_status") == "eligible")
        & F.col("person_id").isNotNull()
    )
    total = current.select("person_id").distinct().count()
    stats = {"reason": "incremental", "patients": changed_count, "eligible_patients": total}
    if changed_count > INCREMENTAL_MAX_PATIENT_FRACTION * max(total, 1):
        return None, {**stats, "reason": "too many changed patients for incremental"}
    # The changed patients' current accessions, plus every accession a changed source row left or joined.
    scope = current.join(changed, "person_id", "left_semi").select("pathology_accession_id")
    if relevant_accessions is not None:
        scope = scope.unionByName(relevant_accessions.select("pathology_accession_id"))
    return scope.where(F.col("pathology_accession_id").isNotNull()).distinct(), stats


def build_diagnosis_context_link(spark, config: PipelineConfig, accession_scope=None):
    """One narrow row per (canonical accession, diagnosis) pair of the same patient, where the diagnosis is in the
    encounter of one of the accession's current source rows, or dated 30 days before to 7 days after that row's date.
    No text.

    Every current, person-eligible source row of the accession takes part (a Cerner order may carry the encounter a
    LIMS sample lacks), and the pairs are then collapsed to one row per (accession, diagnosis) with an aggregation
    that keeps the strongest evidence: a same-encounter match first, then the closest date, then the lowest ids. The
    two joins are disjoint (a same-encounter pair is left out of the window join), and the collapse is a narrow
    aggregation, so there is no sort over wide rows.
    """
    _, F, _ = _imports()
    sources = (
        spark.table(f"{config.bronze_schema}.map_pathology_accession_source")
        .filter((F.col("is_current") == True)
                & (F.col("person_projection_status") == "eligible")
                & F.col("person_id").isNotNull())
        .select("pathology_accession_id",
                F.col("person_id").cast("long").alias("person_id"),
                F.col("encounter_id").cast("long").alias("encounter_id"),
                F.coalesce("sample_dt", "request_dt", "report_dt").alias("accession_dt"))
        .distinct()  # source rows of one accession that agree on these give the same pairs
    )
    if accession_scope is not None:
        sources = sources.join(accession_scope.select("pathology_accession_id"), "pathology_accession_id", "left_semi")

    diagnosis = spark.table(DIAGNOSIS_TABLE).select(
        F.col("DIAGNOSIS_ID").cast("long").alias("diagnosis_id"),
        F.col("PERSON_ID").cast("long").alias("diag_person_id"),
        F.col("ENCNTR_ID").cast("long").alias("diag_encounter_id"),
        F.coalesce("DIAG_DT_TM", "ASSERTED_DT_TM", "BEG_EFFECTIVE_DT_TM").alias("diagnosis_dt"),
    )
    if accession_scope is not None:
        patients = sources.select(F.col("person_id").alias("diag_person_id")).distinct()
        diagnosis = diagnosis.join(patients, "diag_person_id", "left_semi")

    # Same encounter.
    by_encounter = sources.alias("a").join(
        diagnosis.alias("d"),
        (F.col("a.person_id") == F.col("d.diag_person_id"))
        & F.col("a.encounter_id").isNotNull()
        & (F.col("a.encounter_id") == F.col("d.diag_encounter_id")),
        "inner",
    ).select("a.*", "d.*")
    # Date window, joined on (person, week): each source row is spread over the weeks its -30/+7 day window touches,
    # then the exact window is applied. Same-encounter pairs are by_encounter's, so they are left out here.
    day = lambda c: F.datediff(F.to_date(c), F.lit("1970-01-01"))
    windowed = sources.filter(F.col("accession_dt").isNotNull()).withColumn(
        "_week", F.explode(F.sequence(F.floor((day("accession_dt") - 30) / 7), F.floor((day("accession_dt") + 7) / 7))))
    dated = diagnosis.filter(F.col("diagnosis_dt").isNotNull()).withColumn("_week", F.floor(day("diagnosis_dt") / 7))
    by_window = (
        windowed.alias("a").join(
            dated.alias("d"),
            (F.col("a.person_id") == F.col("d.diag_person_id")) & (F.col("a._week") == F.col("d._week")),
            "inner",
        )
        .filter(F.col("d.diagnosis_dt").between(F.col("a.accession_dt") - F.expr("INTERVAL 30 DAYS"),
                                                F.col("a.accession_dt") + F.expr("INTERVAL 7 DAYS")))
        .filter(~F.coalesce(F.col("a.encounter_id") == F.col("d.diag_encounter_id"), F.lit(False)))
        .select("a.*", "d.*")
        .drop("_week")
    )
    evidence = (
        by_encounter.unionByName(by_window)
        .withColumn("same_encounter", F.coalesce(F.col("encounter_id") == F.col("diag_encounter_id"), F.lit(False)))
        .withColumn("day_offset", F.datediff(F.to_date("diagnosis_dt"), F.to_date("accession_dt")))
    )
    # One row per (accession, diagnosis): the strongest evidence wins. min_by over a total order (same encounter
    # first, then the smallest absolute day offset, then encounter and date) is deterministic and needs no sort:
    # two evidence rows equal on all of these have identical output columns.
    strength = F.struct(
        (~F.col("same_encounter")).alias("_not_same"),
        F.coalesce(F.abs(F.col("day_offset")), F.lit(1 << 30)).alias("_distance"),
        F.coalesce(F.col("encounter_id"), F.lit(-1)).alias("_enc"),
        F.coalesce(F.col("accession_dt").cast("string"), F.lit("")).alias("_dt"),
    )
    best = F.struct("person_id", "encounter_id", "diag_encounter_id", "accession_dt", "diagnosis_dt",
                    "same_encounter", "day_offset")
    pairs = (evidence.groupBy("pathology_accession_id", "diagnosis_id")
             .agg(F.min_by(best, strength).alias("_best"))
             .select("pathology_accession_id", "diagnosis_id", "_best.*"))
    return pairs.select(
        F.sha2(F.concat_ws("|", F.lit("pathology_diagnosis_context"), F.col("pathology_accession_id"),
                           F.col("diagnosis_id").cast("string")), 256).alias("context_id"),
        "pathology_accession_id",
        "diagnosis_id",
        "person_id",
        F.col("encounter_id").alias("accession_encounter_id"),
        F.col("diag_encounter_id").alias("diagnosis_encounter_id"),
        "accession_dt",
        "diagnosis_dt",
        "same_encounter",
        "day_offset",
        F.lit(DIAGNOSIS_CONTEXT_RULE).alias("rule_id"),
        F.lit("1").alias("rule_version"),
    )


def _purge_context_rows_from_indication(spark, table: str) -> int:
    """Delete the diagnosis-context rows from map_pathology_indication once; a no-op afterwards. They were never
    indications and now live in map_pathology_diagnosis_context."""
    if not spark.table(table).where("relation_type = 'clinical_context'").limit(1).collect():
        return 0
    result = spark.sql(f"DELETE FROM {table} WHERE relation_type = 'clinical_context'").first()
    affected = result.asDict().get("num_affected_rows") if result is not None else None
    return int(affected) if affected is not None else -1


def _ensure_context_table_features(spark, table: str) -> None:
    """Row tracking and deletion vectors on the link table (the contract DDL sets CDF): a Lakeflow materialized view
    reading it refreshes incrementally only with all three."""
    props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    wanted = {"delta.enableRowTracking": "true", "delta.enableDeletionVectors": "true", "delta.enableChangeDataFeed": "true"}
    missing = {k: v for k, v in wanted.items() if props.get(k) != v}
    if missing:
        settings = ", ".join(f"'{k}' = '{v}'" for k, v in missing.items())
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({settings})")


def _tag_context_columns(spark, config: PipelineConfig, table: str) -> int:
    """Copy ig_risk / ig_severity onto every link column from the bronze column it is derived from (the stricter
    value when it comes from several). No tag value is decided here; a missing source tag stops the lane."""
    accession = f"{config.bronze_schema}.map_pathology_accession_source"
    indication = f"{config.bronze_schema}.map_pathology_indication"
    lineage = {
        "context_id": [(indication, "indication_id")],
        "pathology_accession_id": [(accession, "pathology_accession_id")],
        "diagnosis_id": [(DIAGNOSIS_TABLE, "DIAGNOSIS_ID")],
        "person_id": [(accession, "person_id"), (DIAGNOSIS_TABLE, "PERSON_ID")],
        "accession_encounter_id": [(accession, "encounter_id")],
        "diagnosis_encounter_id": [(DIAGNOSIS_TABLE, "ENCNTR_ID")],
        "accession_dt": [(accession, "sample_dt"), (accession, "request_dt"), (accession, "report_dt")],
        "diagnosis_dt": [(DIAGNOSIS_TABLE, "DIAG_DT_TM"), (DIAGNOSIS_TABLE, "ASSERTED_DT_TM"),
                         (DIAGNOSIS_TABLE, "BEG_EFFECTIVE_DT_TM")],
        "same_encounter": [(accession, "encounter_id"), (DIAGNOSIS_TABLE, "ENCNTR_ID")],
        "day_offset": [(accession, "sample_dt"), (DIAGNOSIS_TABLE, "DIAG_DT_TM")],
        "rule_id": [(indication, "rule_id")],
        "rule_version": [(indication, "rule_version")],
        "source_payload_hash": [(indication, "source_payload_hash")],
        "contract_version": [(indication, "contract_version")],
        "ADC_UPDT": [(indication, "ADC_UPDT")],
    }

    def source_tags(full_name):
        """{(column, tag): value} of one table's ig tags."""
        catalog, schema, name = full_name.split(".")
        rows = spark.sql(f"""SELECT column_name, tag_name, tag_value FROM `{catalog}`.information_schema.column_tags
                             WHERE schema_name = '{schema}' AND table_name = '{name}'
                               AND tag_name IN ('ig_risk', 'ig_severity')""").collect()
        return {(r["column_name"], r["tag_name"]): r["tag_value"] for r in rows}

    tags = {}
    for full_name in {t for sources in lineage.values() for t, _ in sources}:
        tags[full_name] = source_tags(full_name)
    current = source_tags(table)
    written = 0
    for column, sources in lineage.items():
        for tag in ("ig_risk", "ig_severity"):
            values = [tags[t].get((c, tag)) for t, c in sources]
            if any(v is None for v in values):
                raise RuntimeError(f"{table}.{column}: source column without {tag}: {sources}")
            value = str(max(int(v) for v in values))
            if current.get((column, tag)) != value:
                spark.sql(f"ALTER TABLE {table} ALTER COLUMN `{column}` SET TAGS ('{tag}' = '{value}')")
                written += 1
    return written


def _has_active_rules(spark, config: PipelineConfig, include_proposed_rules: bool) -> bool:
    """Whether any indication rule is active (the rule branch then has rows an incremental run does not rebuild)."""
    _, F, _ = _imports()
    statuses = ["APPROVED"] + (["PROPOSED"] if include_proposed_rules else [])
    return bool(spark.table(f"{config.lookup_schema}.pathology_indication_rule")
                .filter(F.upper(F.col("status")).isin(*statuses)).limit(1).collect())


def run_indications(
    spark,
    config: PipelineConfig | None = None,
    *,
    include_proposed_rules: bool = False,
    include_diagnosis_context: bool = True,
    full_reconcile: bool = True,
    validate_stage_keys: bool = True,
    accession_scope=None,
):
    """Rule-based indications into map_pathology_indication; diagnosis context into map_pathology_diagnosis_context.

    accession_scope (pathology_accession_id, from changed_scope) makes the context incremental: only those
    accessions are rebuilt, merged, and their links that no longer hold are deleted. Without it the context is
    rebuilt in full (first build: a plain append into the empty table).
    """
    from pathology_pipeline import _sync_contract, _with_payload_hash

    config = config or PipelineConfig()
    ensure_contracts(spark, config)
    indication_table = f"{config.bronze_schema}.map_pathology_indication"
    context_table = f"{config.bronze_schema}.{CONTEXT_TABLE}"
    result = {"purged_context_rows_from_indication": _purge_context_rows_from_indication(spark, indication_table)}

    # Rule-based indications (none active today). An incremental run without active rules leaves them alone.
    if accession_scope is None or _has_active_rules(spark, config, include_proposed_rules):
        explicit = build_rule_indications(spark, config, include_proposed_rules=include_proposed_rules)
        result["indication"] = merge_contract(spark, indication_table, explicit, contract("map_pathology_indication"),
                                              delete_not_matched=full_reconcile, validate_stage_keys=validate_stage_keys)

    if not include_diagnosis_context:
        return result
    _ensure_context_table_features(spark, context_table)
    result["context_tags_written"] = _tag_context_columns(spark, config, context_table)
    item = contract(CONTEXT_TABLE)
    if accession_scope is not None:
        if not accession_scope.limit(1).collect():
            result["context"] = {"mode": "incremental", "affected_accessions": 0}
            return result
        link = build_diagnosis_context_link(spark, config, accession_scope=accession_scope)
        merged = merge_contract(spark, context_table, link, item, delete_not_matched=False,
                                validate_stage_keys=validate_stage_keys)
        merged["deleted"] = reconcile_scoped_stale(spark, context_table, link, item, accession_scope,
                                                   "pathology_accession_id")
        result["context"] = {"mode": "incremental", **merged}
        return result
    link = build_diagnosis_context_link(spark, config)
    if spark.table(context_table).limit(1).collect():
        result["context"] = {"mode": "full", **merge_contract(spark, context_table, link, item, delete_not_matched=True,
                                                              validate_stage_keys=validate_stage_keys)}
    else:
        # First build: nothing to compare against, so append instead of a MERGE against an empty table.
        _sync_contract(_with_payload_hash(link, item), item).write.mode("append").saveAsTable(context_table)
        result["context"] = {"mode": "first_build", "rows": spark.table(context_table).count()}
    return result
