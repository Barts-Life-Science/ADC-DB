"""Full-history identity and equivalence audits required before rule approval."""

from __future__ import annotations

from pathology_contracts import CONTRACT_VERSION, contract
from pathology_pipeline import PipelineConfig, ensure_contracts, merge_contract, table_exists


def _imports():
    from pyspark.sql import Window, functions as F

    return Window, F


def _active_filter(spark, table_name: str) -> str:
    return (
        "ADC_Deleted IS NULL"
        if "adc_deleted" in {column.lower() for column in spark.table(table_name).columns}
        else "TRUE"
    )


def _audit_row(frame, audit_run_id: str, audit_name: str, audit_group: str, **metrics):
    _, F = _imports()
    values = (
        audit_run_id,
        audit_name,
        audit_group,
        metrics.get("row_count"),
        metrics.get("distinct_patient_count"),
        metrics.get("collision_count"),
        metrics.get("metric_value"),
        metrics.get("evidence_json"),
        CONTRACT_VERSION,
    )
    schema = (
        "audit_run_id string, audit_name string, audit_group string, row_count long, "
        "distinct_patient_count long, collision_count long, metric_value double, "
        "evidence_json string, contract_version string"
    )
    return frame.sparkSession.createDataFrame([values], schema).withColumn(
        "audited_at", F.current_timestamp()
    )


def run_identity_audits(spark, config: PipelineConfig | None = None, audit_run_id: str | None = None):
    """Run the collision, round-trip, date-window, and identity-conflict audits."""

    _, F = _imports()
    config = config or PipelineConfig()
    ensure_contracts(spark, config)
    audit_run_id = audit_run_id or spark.sql("SELECT uuid() AS id").first()["id"]

    sample = spark.table(config.sample_table).filter(F.expr(_active_filter(spark, config.sample_table)))
    result = spark.table(config.result_table).filter(F.expr(_active_filter(spark, config.result_table)))
    mapped = spark.table(config.map_pathology_table)

    normalized_lab = F.upper(F.regexp_replace(F.trim(F.col("LabNo")), "[^A-Za-z0-9]", ""))
    raw_base = sample.select(
        "LIMSNo",
        "LabNo",
        normalized_lab.alias("normalized_lab_no"),
        F.year(F.coalesce("SampleDT", "RequestDT", "ReportDate")).alias("accession_year"),
        "LegWkgCode",
        F.coalesce("SourceCode", F.col("ProcSiteNo").cast("string"), F.col("ReqSiteNo").cast("string")).alias("site_code"),
        "MRN",
        "NHSNo",
        "OrderNo",
        "SampleDT",
        "RequestDT",
        "ReportDate",
    )

    uniqueness = raw_base.groupBy("normalized_lab_no", "accession_year").agg(
        F.count("*").alias("rows"),
        F.countDistinct("LIMSNo", "LabNo").alias("source_accessions"),
        F.countDistinct("LegWkgCode").alias("working_codes"),
        F.countDistinct("site_code").alias("sites"),
        F.countDistinct("MRN").alias("mrns"),
        F.countDistinct("NHSNo").alias("nhs_numbers"),
    )
    uniqueness_collisions = uniqueness.filter(
        (F.col("source_accessions") > 1)
        | (F.col("working_codes") > 1)
        | (F.col("sites") > 1)
        | (F.col("mrns") > 1)
        | (F.col("nhs_numbers") > 1)
    )

    sample_duplicates = sample.groupBy("LIMSNo", "LabNo").count().filter(F.col("count") > 1)
    section_column = (
        F.col("master_section_code")
        if "master_section_code" in mapped.columns
        else (
            F.col("WkgCode")
            if "WkgCode" in mapped.columns
            else F.lit(None).cast("string")
        )
    )
    raw_person_conflict = (
        mapped.filter(F.col("source_table") == "raw")
        .groupBy("source_parent_key")
        .agg(
            F.countDistinct(F.when(F.col("PERSON_ID").isNotNull(), F.col("PERSON_ID"))).alias("person_count"),
            F.max(F.when(F.col("person_match_status") == "conflict", 1).otherwise(0)).alias("source_conflict"),
            F.max(section_column).alias("section"),
        )
        .filter((F.col("person_count") > 1) | (F.col("source_conflict") == 1))
    )

    linked_orders = (
        mapped.filter((F.col("source_table") == "linked") & F.col("order_id").isNotNull())
        .groupBy(F.col("order_id").cast("long").alias("order_id"))
        .agg(
            F.first("lab_no", ignorenulls=True).alias("linked_lab_no"),
            F.first("PERSON_ID", ignorenulls=True).cast("long").alias("linked_person_id"),
            F.min("measurement_datetime").alias("linked_result_dt"),
            F.first("code", ignorenulls=True).alias("order_mnemonic"),
        )
    )
    order_oracle = (
        raw_base.withColumn("order_id", F.expr("try_cast(OrderNo as bigint)"))
        .filter(F.col("order_id").isNotNull())
        .join(linked_orders, "order_id", "inner")
        .withColumn("raw_lab_norm", F.upper(F.regexp_replace(F.trim("LabNo"), "[^A-Za-z0-9]", "")))
        .withColumn("linked_lab_norm", F.upper(F.regexp_replace(F.trim("linked_lab_no"), "[^A-Za-z0-9]", "")))
        .withColumn("lab_roundtrip", F.col("raw_lab_norm") == F.col("linked_lab_norm"))
        .withColumn("sample_day_diff", F.abs(F.datediff("SampleDT", "linked_result_dt")))
        .withColumn("report_day_diff", F.abs(F.datediff("ReportDate", "linked_result_dt")))
    )

    audit_frames = []
    audit_frames.append(
        _audit_row(
            raw_base,
            audit_run_id,
            "lab_no_year_uniqueness",
            "all",
            row_count=raw_base.count(),
            distinct_patient_count=raw_base.select("MRN", "NHSNo").distinct().count(),
            collision_count=uniqueness_collisions.count(),
            metric_value=None,
            evidence_json=None,
        )
    )
    audit_frames.append(
        _audit_row(
            sample_duplicates,
            audit_run_id,
            "sample_parent_duplicates",
            "all",
            row_count=sample.count(),
            collision_count=sample_duplicates.count(),
        )
    )
    audit_frames.append(
        _audit_row(
            raw_person_conflict,
            audit_run_id,
            "raw_person_conflicts",
            "all",
            row_count=mapped.filter(F.col("source_table") == "raw").select("source_parent_key").distinct().count(),
            collision_count=raw_person_conflict.count(),
        )
    )
    oracle_count = order_oracle.count()
    roundtrip_count = order_oracle.filter(F.col("lab_roundtrip") == True).count()
    audit_frames.append(
        _audit_row(
            order_oracle,
            audit_run_id,
            "reference_number_roundtrip",
            "order_id_oracle",
            row_count=oracle_count,
            collision_count=oracle_count - roundtrip_count,
            metric_value=(roundtrip_count / oracle_count) if oracle_count else None,
        )
    )
    for date_field, metric in (("SampleDT", "sample_day_diff"), ("ReportDate", "report_day_diff")):
        quantiles = order_oracle.approxQuantile(metric, [0.5, 0.9, 0.95, 0.99], 0.001) if oracle_count else []
        audit_frames.append(
            _audit_row(
                order_oracle,
                audit_run_id,
                "cross_arm_date_difference",
                date_field,
                row_count=oracle_count,
                metric_value=quantiles[-1] if quantiles else None,
                evidence_json=(
                    '{"p50":%s,"p90":%s,"p95":%s,"p99":%s}' % tuple(quantiles)
                    if len(quantiles) == 4
                    else None
                ),
            )
        )

    output = audit_frames[0]
    for frame in audit_frames[1:]:
        output = output.unionByName(frame, allowMissingColumns=True)
    output.write.mode("append").saveAsTable(f"{config.bronze_schema}.pathology_identity_audit")

    # Produce a governed PROPOSED cross-arm code map from the order oracle. It is
    # never approved or activated by this audit job.
    raw_codes = result.select(
        "LIMSNo", "LabNo", F.coalesce("WkgCode", "LegWkgCode").alias("wkg_code"),
        F.coalesce("TFCCode", "LegTFCCode").alias("tfc_code")
    ).dropDuplicates()
    proposed = (
        order_oracle.select("LIMSNo", "LabNo", "order_mnemonic")
        .join(raw_codes, ["LIMSNo", "LabNo"], "inner")
        .groupBy("wkg_code", "tfc_code", "order_mnemonic")
        .agg(F.count("*").alias("evidence_count"))
    )
    totals = proposed.groupBy("wkg_code", "tfc_code").agg(F.sum("evidence_count").alias("_total"))
    proposed = (
        proposed.join(totals, ["wkg_code", "tfc_code"])
        .withColumn("precision_estimate", F.col("evidence_count") / F.col("_total"))
        .withColumn("status", F.lit("PROPOSED"))
        .withColumn("reviewed_by", F.lit(None).cast("string"))
        .withColumn("reviewed_at", F.lit(None).cast("timestamp"))
        .withColumn("valid_from", F.lit(None).cast("timestamp"))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
    )
    merge_contract(
        spark,
        f"{config.lookup_schema}.pathology_cross_arm_test_map",
        proposed,
        contract("pathology_cross_arm_test_map"),
        delete_not_matched=False,
    )
    return {
        "audit_run_id": audit_run_id,
        "audit_rows": len(audit_frames),
        "order_oracle_rows": oracle_count,
    }
