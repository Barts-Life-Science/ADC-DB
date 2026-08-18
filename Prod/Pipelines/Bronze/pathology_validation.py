"""Executable release checks for deterministic pathology sidecars."""

from __future__ import annotations

from pathology_contracts import (
    AMR_CONTRACTS,
    CONTRACTS,
    CONTRACT_VERSION,
    LOOKUP_CONTRACTS,
)
from pathology_pipeline import PipelineConfig, ensure_contracts, table_exists


def _imports():
    from pyspark.sql import functions as F

    return F


def _result(spark, run_id, name, severity, passed, observed, threshold=None, detail=""):
    F = _imports()
    return (
        spark.createDataFrame(
            [
                (
                    run_id,
                    name,
                    severity,
                    bool(passed),
                    float(observed) if observed is not None else None,
                    float(threshold) if threshold is not None else None,
                    detail,
                    CONTRACT_VERSION,
                )
            ],
            "validation_run_id string, check_name string, severity string, passed boolean, observed_value double, threshold_value double, detail string, contract_version string",
        )
        .withColumn("validated_at", F.current_timestamp())
        .select(
            "validation_run_id",
            "check_name",
            "severity",
            "passed",
            "observed_value",
            "threshold_value",
            "detail",
            "validated_at",
            "contract_version",
        )
    )


def _duplicates(spark, table_name: str, keys: tuple[str, ...]) -> int:
    F = _imports()
    return (
        spark.table(table_name)
        .groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def validate_contract_shapes(spark, config: PipelineConfig, run_id: str):
    results = []
    for item in CONTRACTS + AMR_CONTRACTS:
        table_name = f"{config.bronze_schema}.{item.name}"
        actual = {field.name for field in spark.table(table_name).schema.fields}
        expected = {column.name for column in item.columns}
        missing = sorted(expected - actual)
        results.append(
            _result(
                spark,
                run_id,
                f"contract_columns:{item.name}",
                "ERROR",
                not missing,
                len(missing),
                0,
                f"missing={missing}",
            )
        )
        duplicate_count = _duplicates(spark, table_name, item.keys)
        results.append(
            _result(
                spark,
                run_id,
                f"duplicate_keys:{item.name}",
                "ERROR",
                duplicate_count == 0,
                duplicate_count,
                0,
                f"keys={item.keys}",
            )
        )
    return results


def validate_source_coverage(spark, config: PipelineConfig, run_id: str):
    F = _imports()
    sample = spark.table(config.sample_table)
    if "ADC_Deleted" in sample.columns:
        sample = sample.filter(F.col("ADC_Deleted").isNull())
    expected_raw = sample.select("LIMSNo", "LabNo").dropDuplicates().count()
    expected_linked = (
        spark.table(config.map_pathology_table)
        .filter(F.col("source_table") == "linked")
        .select("source_parent_key")
        .dropDuplicates()
        .count()
    )
    source = spark.table(f"{config.bronze_schema}.map_pathology_accession_source").filter(
        F.col("is_current") == True
    )
    actual_raw = source.filter(F.col("source_system") == "TFC_LIMS").select(
        "source_parent_key"
    ).dropDuplicates().count()
    actual_linked = source.filter(F.col("source_system") == "CERNER").select(
        "source_parent_key"
    ).dropDuplicates().count()
    return [
        _result(
            spark,
            run_id,
            "source_parent_coverage:raw",
            "ERROR",
            actual_raw == expected_raw,
            actual_raw,
            expected_raw,
        ),
        _result(
            spark,
            run_id,
            "source_parent_coverage:linked",
            "ERROR",
            actual_linked == expected_linked,
            actual_linked,
            expected_linked,
        ),
    ]


def validate_lifecycle_and_genetics(spark, config: PipelineConfig, run_id: str):
    F = _imports()
    reports = spark.table(f"{config.bronze_schema}.map_pathology_report")
    multi_current = (
        reports.filter(F.col("is_current") == True)
        .groupBy("report_series_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    tests = spark.table(f"{config.bronze_schema}.map_pathology_genetic_test")
    findings = spark.table(f"{config.bronze_schema}.map_pathology_genetic_result")
    negative_with_findings = (
        tests.filter(F.col("overall_result_status") == "not_detected")
        .join(findings, "genetic_test_id", "inner")
        .count()
    )
    hgvs = findings.filter(F.col("hgvs_c_raw").isNotNull() | F.col("hgvs_p_raw").isNotNull())
    hgvs_count = hgvs.count()
    hgvs_bad = hgvs.filter(
        (F.col("hgvs_c_raw").isNotNull() & (F.instr(F.col("evidence_text"), F.col("hgvs_c_raw")) == 0))
        | (F.col("hgvs_p_raw").isNotNull() & (F.instr(F.col("evidence_text"), F.col("hgvs_p_raw")) == 0))
    ).count()
    panel_leakage = findings.filter(
        F.lower(F.coalesce("evidence_text", F.lit(""))).rlike(
            r"^\s*(technical information|genes? tested|genes? covered|panel content)"
        )
    ).count()
    return [
        _result(spark, run_id, "single_current_report_version", "ERROR", multi_current == 0, multi_current, 0),
        _result(spark, run_id, "negative_panel_has_no_findings", "ERROR", negative_with_findings == 0, negative_with_findings, 0),
        _result(
            spark,
            run_id,
            "raw_hgvs_fidelity",
            "ERROR",
            hgvs_bad == 0,
            1.0 if hgvs_count == 0 else (hgvs_count - hgvs_bad) / hgvs_count,
            1.0,
        ),
        _result(spark, run_id, "panel_list_leakage", "ERROR", panel_leakage == 0, panel_leakage, 0),
    ]


def validate_tlc_mapping(spark, config: PipelineConfig, run_id: str):
    F = _imports()
    tests = spark.table(f"{config.bronze_schema}.map_pathology_requested_test")
    source = spark.table(f"{config.bronze_schema}.map_pathology_accession_source").select(
        "source_accession_id",
        F.to_date(F.coalesce("sample_dt", "request_dt", "report_dt")).alias("event_date"),
        F.coalesce(
            F.nullif(F.regexp_extract(F.upper("normalized_lab_no"), r"[A-Z]", 0), F.lit("")),
            F.lit("OTHER"),
        ).alias("lab_series"),
    ).dropDuplicates(["source_accession_id"])
    rows = tests.filter(F.col("source_system") == "TFC_LIMS").join(
        source, "source_accession_id", "left"
    )
    rows = rows.withColumn(
        "era",
        F.when(F.col("event_date").isNull(), "unknown")
        .when(F.col("event_date") >= F.add_months(F.current_date(), -12), "recent-12-months")
        .when(F.year("event_date") >= 2024, "2024-before-recent")
        .when(F.year("event_date") >= 2020, "2020-2023")
        .when(F.year("event_date") >= 2016, "2016-2019")
        .when(F.year("event_date") >= 2012, "2012-2015")
        .otherwise("pre-2012"),
    )
    metrics = rows.groupBy("era", "lab_series").agg(
        F.count("*").alias("tokens"),
        F.sum(F.when(F.col("mapping_status") == "mapped", 1).otherwise(0)).alias("mapped"),
    ).collect()
    legacy_floors = {
        "pre-2012": 0.70,
        "2012-2015": 0.75,
        "2016-2019": 0.85,
        "2020-2023": 0.95,
        "2024-before-recent": 0.98,
    }
    results = []
    for row in metrics:
        rate = row["mapped"] / row["tokens"] if row["tokens"] else 1.0
        if row["era"] == "recent-12-months":
            threshold = 0.999 if row["lab_series"] in {"S", "N"} else 0.98
            severity = "ERROR"
        elif row["era"] == "unknown":
            threshold = 0.0
            severity = "WARN"
        else:
            threshold = legacy_floors[row["era"]]
            severity = "ERROR"
        results.append(
            _result(
                spark,
                run_id,
                f"tlc_mapping_rate:{row['era']}:{row['lab_series']}",
                severity,
                rate >= threshold,
                rate,
                threshold,
                (
                    f"tokens={row['tokens']}, mapped={row['mapped']}; recent is "
                    "the rolling 12 months ending on validation date"
                ),
            )
        )
    return results


def validate_gold_set(spark, config: PipelineConfig, run_id: str):
    F = _imports()
    gold_reports = spark.table(f"{config.bronze_schema}.pathology_genetics_gold_report")
    if gold_reports.limit(1).count() == 0:
        return [
            _result(
                spark,
                run_id,
                "genetics_gold_set_present",
                "WARN",
                False,
                0,
                200,
                "Ben-owned gold set is empty; parser profiles cannot be approved",
            )
        ]
    gold_findings = spark.table(f"{config.bronze_schema}.pathology_genetics_gold_finding")
    actual = spark.table(f"{config.bronze_schema}.map_pathology_genetic_result")
    expected = gold_findings.join(
        gold_reports.select("gold_report_id", "report_version_id", "profile_id", "format_era"),
        "gold_report_id",
    )
    match_condition = (
        (F.upper(expected.reported_gene_symbol) == F.upper(actual.reported_gene_symbol))
        & expected.hgvs_c_raw.eqNullSafe(actual.hgvs_c_raw)
        & expected.hgvs_p_raw.eqNullSafe(actual.hgvs_p_raw)
    )
    matched = expected.join(
        actual,
        (expected.report_version_id == actual.report_version_id) & match_condition,
        "inner",
    ).select(expected.gold_finding_id).dropDuplicates()
    tp = matched.count()
    expected_count = expected.count()
    actual_in_gold = actual.join(
        gold_reports.select("report_version_id").dropDuplicates(), "report_version_id", "inner"
    ).count()
    fp = max(0, actual_in_gold - tp)
    fn = max(0, expected_count - tp)
    precision = tp / (tp + fp) if tp + fp else 1.0
    recall = tp / (tp + fn) if tp + fn else 1.0
    report_count = gold_reports.count()
    positive_count = gold_reports.filter(F.col("expected_overall_result_status") == "detected").count()
    negative_count = gold_reports.filter(F.col("expected_overall_result_status") == "not_detected").count()
    amendment_count = gold_reports.filter(F.col("amendment_ind") == True).count()
    sample_ok = report_count >= 200 and positive_count >= 50 and negative_count >= 50 and amendment_count >= 50
    passed = sample_ok and precision >= 0.99 and recall >= 0.95
    validation = spark.createDataFrame(
        [
            (
                run_id,
                "ALL",
                "ALL",
                report_count,
                positive_count,
                negative_count,
                amendment_count,
                tp,
                fp,
                fn,
                precision,
                recall,
                1.0,
                0,
                passed,
                "Ben",
                CONTRACT_VERSION,
            )
        ],
        "validation_run_id string, profile_id string, format_era string, report_count long, positive_count long, negative_count long, amendment_count long, true_positive_count long, false_positive_count long, false_negative_count long, precision double, recall double, raw_hgvs_fidelity double, panel_list_leakage_count long, passed boolean, accountable_owner string, contract_version string",
    ).withColumn("validated_at", F.current_timestamp())
    validation.write.mode("append").saveAsTable(
        f"{config.bronze_schema}.pathology_parser_validation"
    )
    return [
        _result(spark, run_id, "genetics_gold_sample_size", "ERROR", sample_ok, report_count, 200),
        _result(spark, run_id, "genetics_gold_precision", "ERROR", precision >= 0.99, precision, 0.99),
        _result(spark, run_id, "genetics_gold_recall", "ERROR", recall >= 0.95, recall, 0.95),
    ]


def run_validation(spark, config: PipelineConfig | None = None, *, fail_on_error: bool = True):
    config = config or PipelineConfig()
    ensure_contracts(spark, config)
    run_id = spark.sql("SELECT uuid() AS id").first()["id"]
    frames = []
    frames.extend(validate_contract_shapes(spark, config, run_id))
    frames.extend(validate_source_coverage(spark, config, run_id))
    frames.extend(validate_lifecycle_and_genetics(spark, config, run_id))
    frames.extend(validate_tlc_mapping(spark, config, run_id))
    frames.extend(validate_gold_set(spark, config, run_id))
    output = frames[0]
    for frame in frames[1:]:
        output = output.unionByName(frame)
    output.write.mode("append").saveAsTable(
        f"{config.bronze_schema}.pathology_validation_result"
    )
    failures = output.filter((output.severity == "ERROR") & (output.passed == False)).count()
    if fail_on_error and failures:
        raise RuntimeError(f"Pathology validation failed {failures} ERROR checks; run_id={run_id}")
    return run_id, output
