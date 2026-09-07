"""Spark implementation of the deterministic Bronze pathology sidecars.

The module is intentionally environment-parameterised. It defaults to `8_dev`
and contains no production publication or downstream activation code.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from pathology_contracts import (
    CONTRACTS,
    AMR_CONTRACTS,
    LOOKUP_CONTRACTS,
    AMR_LOOKUP_CONTRACTS,
    CONTRACT_VERSION,
    Contract,
    all_create_ddls,
    contract,
)
from pathology_rules import PARSER_VERSION


@dataclass(frozen=True)
class PipelineConfig:
    bronze_schema: str = "8_dev.bronze"
    lookup_schema: str = "8_dev.lookup"
    map_pathology_table: str = "4_prod.bronze.map_pathology"
    sample_table: str = "4_prod.raw.path_patient_samplelevel"
    result_table: str = "4_prod.raw.path_patient_resultlevel"
    master_orderable_table: str = "4_prod.raw.path_master_orderables"
    master_result_table: str = "4_prod.raw.path_master_resultable"
    omop_concept_table: str = "3_lookup.omop.concept"
    enable_approved_accession_merges: bool = False
    enable_result_equivalence: bool = False


def _imports():
    from delta.tables import DeltaTable
    from pyspark.sql import Window, functions as F, types as T

    return DeltaTable, Window, F, T


def qn(value: str) -> str:
    return ".".join(f"`{part.replace('`', '``')}`" for part in value.split("."))


def table_exists(spark, table_name: str) -> bool:
    return bool(spark.catalog.tableExists(table_name))


def _ensure_schema(spark, schema_name: str, comment: str) -> None:
    statement = f"CREATE SCHEMA IF NOT EXISTS {qn(schema_name)} COMMENT '{comment}'"
    try:
        spark.sql(statement)
    except Exception as create_error:
        # The weekly principal may have USE/CREATE TABLE rights in an existing schema
        # without CREATE SCHEMA. The idempotent statement is not required in that case.
        message = str(create_error).upper()
        permission_tokens = {
            "PERMISSION_DENIED",
            "INSUFFICIENT_PERMISSIONS",
            "INSUFFICIENT_PRIVILEGES",
            "NOT_AUTHORIZED",
        }
        if not any(token in message for token in permission_tokens):
            raise
        try:
            spark.sql(f"DESCRIBE SCHEMA {qn(schema_name)}").collect()
        except Exception:
            raise create_error


def ensure_contracts(spark, config: PipelineConfig) -> None:
    _ensure_schema(
        spark,
        config.bronze_schema,
        "Development pathology sidecars; production promotion is human-gated",
    )
    _ensure_schema(
        spark,
        config.lookup_schema,
        "Development twins of governed pathology lookup assets",
    )
    for ddl in all_create_ddls(config.bronze_schema, config.lookup_schema):
        spark.sql(ddl)
    # CREATE TABLE IF NOT EXISTS deliberately preserves comments and history. Apply
    # additive contract evolution explicitly rather than using CREATE OR REPLACE.
    for schema_name, items in (
        (config.bronze_schema, CONTRACTS + AMR_CONTRACTS),
        (config.lookup_schema, LOOKUP_CONTRACTS + AMR_LOOKUP_CONTRACTS),
    ):
        for item in items:
            table_name = f"{schema_name}.{item.name}"
            existing = {field.name.lower() for field in spark.table(table_name).schema.fields}
            for column in item.columns:
                if column.name.lower() not in existing:
                    comment = column.comment.replace("'", "''")
                    spark.sql(
                        f"ALTER TABLE {qn(table_name)} ADD COLUMNS "
                        f"(`{column.name}` {column.data_type} COMMENT '{comment}')"
                    )
    ensure_internal_tables(spark, config)
    create_views(spark, config)


def create_views(spark, config: PipelineConfig) -> None:
    """Publish only logical convenience views; never duplicate the 1.9B-row fact."""

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {qn(config.bronze_schema)}.map_pathology_enriched
        COMMENT 'Logical join of map_pathology to canonical accession/report/result sidecars; no result text is rematerialized.'
        AS
        SELECT
          mp.*,
          src.pathology_accession_id,
          src.source_accession_id,
          src.link_status AS accession_link_status,
          src.link_confidence AS accession_link_confidence,
          src.person_projection_status,
          acc.lab_series,
          acc.lab_series_desc,
          acc.discipline AS pathology_discipline,
          acc.clinical_details,
          acc.tlcs_requested,
          eq.canonical_result_id,
          eq.report_version_id AS canonical_report_version_id,
          eq.representation_role,
          eq.preferred_result_ind,
          eq.lifecycle_status AS canonical_result_lifecycle_status,
          eq.is_current AS canonical_result_is_current
        FROM {qn(config.map_pathology_table)} mp
        LEFT JOIN {qn(config.bronze_schema)}.map_pathology_accession_source src
          ON src.source_system = CASE WHEN mp.source_table='raw' THEN 'TFC_LIMS' ELSE 'CERNER' END
         AND src.source_parent_key=mp.source_parent_key
         AND src.is_current=TRUE
        LEFT JOIN {qn(config.bronze_schema)}.map_pathology_accession acc
          ON acc.pathology_accession_id=src.pathology_accession_id
        LEFT JOIN {qn(config.bronze_schema)}.map_pathology_result_equivalence eq
          ON eq.source_record_key=mp.source_record_key
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {qn(config.bronze_schema)}.map_pathology_preferred
        COMMENT 'Current preferred source representation per canonical pathology result; unresolved identity remains visible and flagged.'
        AS
        SELECT *
        FROM {qn(config.bronze_schema)}.map_pathology_enriched
        WHERE coalesce(preferred_result_ind, TRUE)=TRUE
          AND coalesce(canonical_result_is_current, TRUE)=TRUE
          AND coalesce(canonical_result_lifecycle_status, 'unknown')
              NOT IN ('entered_in_error','cancelled')
        """
    )


def ensure_internal_tables(spark, config: PipelineConfig) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qn(config.bronze_schema)}.pathology_expansion_state (
          source_name STRING COMMENT 'Logical CDF source name',
          table_name STRING COMMENT 'Fully-qualified Delta source table',
          last_delta_version BIGINT COMMENT 'Last successfully committed source version',
          last_success_at TIMESTAMP COMMENT 'Successful pipeline completion time',
          run_id STRING COMMENT 'Run that committed this state',
          contract_version STRING COMMENT 'Expansion contract version'
        ) USING DELTA
        COMMENT 'Internal restart state for the deterministic pathology expansion.'
        TBLPROPERTIES ('delta.enableChangeDataFeed'='true',
                       'delta.deletedFileRetentionDuration'='interval 30 days')
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qn(config.bronze_schema)}.pathology_expansion_run_log (
          run_id STRING,
          started_at TIMESTAMP,
          completed_at TIMESTAMP,
          mode STRING,
          status STRING,
          stage STRING,
          source_parent_count BIGINT,
          match_group_count BIGINT,
          inserted_rows BIGINT,
          updated_rows BIGINT,
          deleted_rows BIGINT,
          message STRING,
          contract_version STRING
        ) USING DELTA
        COMMENT 'Restartable stage log and CDF-churn evidence for pathology sidecars.'
        TBLPROPERTIES ('delta.enableChangeDataFeed'='true',
                       'delta.deletedFileRetentionDuration'='interval 30 days')
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qn(config.bronze_schema)}.pathology_identity_audit (
          audit_run_id STRING,
          audit_name STRING,
          audit_group STRING,
          row_count BIGINT,
          distinct_patient_count BIGINT,
          collision_count BIGINT,
          metric_value DOUBLE,
          evidence_json STRING,
          audited_at TIMESTAMP,
          contract_version STRING
        ) USING DELTA
        COMMENT 'Evidence required before approving accession-link and equivalence rules.'
        TBLPROPERTIES ('delta.enableChangeDataFeed'='true',
                       'delta.deletedFileRetentionDuration'='interval 30 days')
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qn(config.bronze_schema)}.pathology_genetics_gold_report (
          gold_report_id STRING COMMENT 'Stable labelled report identifier',
          report_version_id STRING COMMENT 'Report version under evaluation',
          profile_id STRING COMMENT 'Expected parser profile',
          format_era STRING COMMENT 'Named report-format era',
          site_code STRING COMMENT 'Source site stratum',
          expected_overall_result_status STRING COMMENT 'detected, not_detected, indeterminate, failed, or unknown',
          amendment_ind BOOLEAN COMMENT 'Whether the case tests amendment/supersession handling',
          labelled_by STRING COMMENT 'Accountable data owner; v1 owner is Ben',
          labelled_at TIMESTAMP COMMENT 'Labelling time',
          adjudication_notes STRING COMMENT 'Data-team adjudication notes'
        ) USING DELTA
        COMMENT 'Ben-owned report-level gold set for deterministic genetics validation.'
        TBLPROPERTIES ('delta.enableChangeDataFeed'='true',
                       'delta.deletedFileRetentionDuration'='interval 30 days')
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qn(config.bronze_schema)}.pathology_genetics_gold_finding (
          gold_finding_id STRING COMMENT 'Stable expected-finding identifier',
          gold_report_id STRING COMMENT 'Parent labelled report',
          reported_gene_symbol STRING COMMENT 'Expected reported symbol',
          hgvs_c_raw STRING COMMENT 'Expected raw coding HGVS',
          hgvs_p_raw STRING COMMENT 'Expected raw protein HGVS',
          detection_status STRING COMMENT 'Expected detection status',
          evidence_text STRING COMMENT 'Expected evidence text where labelled'
        ) USING DELTA
        COMMENT 'Finding-level labels for deterministic genetics precision, recall, and HGVS-fidelity gates.'
        TBLPROPERTIES ('delta.enableChangeDataFeed'='true',
                       'delta.deletedFileRetentionDuration'='interval 30 days')
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qn(config.bronze_schema)}.pathology_parser_validation (
          validation_run_id STRING,
          profile_id STRING,
          format_era STRING,
          report_count BIGINT,
          positive_count BIGINT,
          negative_count BIGINT,
          amendment_count BIGINT,
          true_positive_count BIGINT,
          false_positive_count BIGINT,
          false_negative_count BIGINT,
          precision DOUBLE,
          recall DOUBLE,
          raw_hgvs_fidelity DOUBLE,
          panel_list_leakage_count BIGINT,
          passed BOOLEAN,
          accountable_owner STRING,
          validated_at TIMESTAMP,
          contract_version STRING
        ) USING DELTA
        COMMENT 'Parser release evidence; release thresholds are 99% precision, 95% recall, 100% raw-HGVS fidelity, and zero panel-list leakage.'
        TBLPROPERTIES ('delta.enableChangeDataFeed'='true',
                       'delta.deletedFileRetentionDuration'='interval 30 days')
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qn(config.bronze_schema)}.pathology_validation_result (
          validation_run_id STRING,
          check_name STRING,
          severity STRING,
          passed BOOLEAN,
          observed_value DOUBLE,
          threshold_value DOUBLE,
          detail STRING,
          validated_at TIMESTAMP,
          contract_version STRING
        ) USING DELTA
        COMMENT 'Executable release and data-quality checks for pathology sidecars.'
        TBLPROPERTIES ('delta.enableChangeDataFeed'='true',
                       'delta.deletedFileRetentionDuration'='interval 30 days')
        """
    )


def _sync_contract(df, item: Contract):
    _, _, F, _ = _imports()
    data_types = {column.name: column.data_type for column in item.columns}
    for name, data_type in data_types.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(data_type))
        else:
            df = df.withColumn(name, F.col(name).cast(data_type))
    return df.select(*data_types)


def _with_payload_hash(df, item: Contract):
    _, _, F, _ = _imports()
    excluded = {"source_payload_hash", "contract_version", "ADC_UPDT"}
    payload_columns = [
        column.name for column in item.columns if column.name not in excluded
    ]
    return (
        df.withColumn(
            "source_payload_hash",
            F.xxhash64(*[F.col(name) for name in payload_columns]),
        )
        .withColumn("contract_version", F.lit(CONTRACT_VERSION))
        .withColumn("ADC_UPDT", F.current_timestamp())
    )


def _merge_condition(keys: Sequence[str]) -> str:
    return " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in keys)


def merge_contract(
    spark,
    table_name: str,
    frame,
    item: Contract,
    *,
    delete_not_matched: bool = False,
    stale_update: Mapping[str, object] | None = None,
    validate_stage_keys: bool = True,
) -> dict[str, int]:
    """Keyed, hash-guarded MERGE that never restamps unchanged rows."""

    DeltaTable, _, F, _ = _imports()
    frame = _sync_contract(_with_payload_hash(frame, item), item)
    if validate_stage_keys:
        duplicate = (
            frame.groupBy(*item.keys).count().filter(F.col("count") > 1).limit(10).collect()
        )
        if duplicate:
            raise RuntimeError(
                f"{item.name} stage contains duplicate keys: "
                + ", ".join(str(row.asDict()) for row in duplicate)
            )

    builder = (
        DeltaTable.forName(spark, table_name)
        .alias("t")
        .merge(frame.alias("s"), _merge_condition(item.keys))
        .whenMatchedUpdateAll(
            condition="NOT (t.source_payload_hash <=> s.source_payload_hash)"
        )
        .whenNotMatchedInsertAll()
    )
    if delete_not_matched:
        builder = builder.whenNotMatchedBySourceDelete()
    elif stale_update:
        builder = builder.whenNotMatchedBySourceUpdate(
            set={name: value for name, value in stale_update.items()}
        )
    result = builder.execute()
    result_rows = result.take(1) if result is not None else []
    row = result_rows[0].asDict() if result_rows else {}
    return {
        "inserted": int(row.get("num_inserted_rows", 0) or 0),
        "updated": int(row.get("num_updated_rows", 0) or 0),
        "deleted": int(row.get("num_deleted_rows", 0) or 0),
        "affected": int(row.get("num_affected_rows", 0) or 0),
    }


def reconcile_scoped_stale(
    spark,
    table_name: str,
    frame,
    item: Contract,
    scope_frame,
    scope_column: str,
    *,
    mark_inactive: bool = False,
) -> int:
    """Delete or inactivate stale rows inside recomputed match/accession groups."""

    DeltaTable, _, F, _ = _imports()
    scoped_target = spark.table(table_name).join(
        scope_frame.select(scope_column).dropDuplicates(), scope_column, "inner"
    )
    stale = scoped_target.join(frame.select(*item.keys), list(item.keys), "left_anti").select(
        *item.keys
    )
    builder = DeltaTable.forName(spark, table_name).alias("t").merge(
        stale.alias("s"), _merge_condition(item.keys)
    )
    if mark_inactive:
        result = builder.whenMatchedUpdate(
            set={"is_current": F.lit(False), "ADC_UPDT": F.current_timestamp()}
        ).execute()
    else:
        result = builder.whenMatchedDelete().execute()
    result_rows = result.take(1) if result is not None else []
    row = result_rows[0].asDict() if result_rows else {}
    return int(row.get("num_affected_rows", 0) or 0)


def _active_filter(spark, table_name: str) -> str:
    columns = {column.lower() for column in spark.table(table_name).columns}
    return "ADC_Deleted IS NULL" if "adc_deleted" in columns else "TRUE"


def _normalize_lab_no(column):
    _, _, F, _ = _imports()
    return F.upper(F.regexp_replace(F.trim(column.cast("string")), "[^A-Za-z0-9]", ""))


def _source_date(frame_alias: str = ""):
    _, _, F, _ = _imports()
    prefix = f"{frame_alias}." if frame_alias else ""
    return F.coalesce(
        F.col(prefix + "sample_dt"),
        F.col(prefix + "request_dt"),
        F.col(prefix + "report_dt"),
    )


def build_source_stage(spark, config: PipelineConfig):
    """Build one row per existing source parent plus accession-grain auxiliaries."""

    _, Window, F, _ = _imports()
    sample = spark.table(config.sample_table).filter(
        F.expr(_active_filter(spark, config.sample_table))
    )
    sample_window = Window.partitionBy("LIMSNo", "LabNo").orderBy(
        F.col("ADC_UPDT").desc_nulls_last(),
        F.col("SampleDT").desc_nulls_last(),
        F.col("ReportDate").desc_nulls_last(),
        F.col("LegWkgCode").asc_nulls_last(),
    )
    sample = (
        sample.withColumn("_rn", F.row_number().over(sample_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    mapped = spark.table(config.map_pathology_table)
    raw_map = (
        mapped.filter(F.col("source_table") == "raw")
        .groupBy("source_parent_key")
        .agg(
            F.max("LIMSNo").cast("int").alias("_mp_lims_no"),
            F.max("lab_no").alias("_mp_lab_no"),
            F.max("WkgCode").alias("_mp_wkg_code"),
            F.max("PERSON_ID").cast("long").alias("_mp_person_id"),
            F.max("ENCNTR_ID").cast("long").alias("_mp_encounter_id"),
            F.max("MRN").alias("_mp_mrn"),
            F.max("NHS_Number").alias("_mp_nhs"),
            F.max("person_match_status").alias("_mp_person_status"),
            F.max("source_adc_updt").alias("_mp_adc"),
        )
    )

    raw = (
        sample.alias("s")
        .join(
            raw_map.alias("m"),
            (F.col("m._mp_lims_no") == F.col("s.LIMSNo"))
            & (F.col("m._mp_lab_no") == F.col("s.LabNo")),
            "left",
        )
        .select(
            F.lit("TFC_LIMS").alias("source_system"),
            F.concat_ws(
                "|",
                F.lit("raw"),
                F.coalesce(F.col("s.LIMSNo").cast("string"), F.lit("∅")),
                F.coalesce(F.col("s.LabNo"), F.lit("∅")),
            ).alias("source_parent_key"),
            F.col("s.LIMSNo").cast("int").alias("LIMSNo"),
            F.col("s.LabNo").alias("lab_no"),
            F.coalesce(F.col("m._mp_wkg_code"), F.col("s.LegWkgCode")).alias("wkg_code"),
            F.coalesce(F.col("s.SourceCode"), F.col("s.ProcSiteNo").cast("string"), F.col("s.ReqSiteNo").cast("string")).alias("source_site_code"),
            F.col("m._mp_person_id").alias("person_id"),
            F.col("m._mp_encounter_id").alias("encounter_id"),
            F.coalesce(F.col("m._mp_mrn"), F.col("s.MRN")).alias("mrn"),
            F.coalesce(F.col("m._mp_nhs"), F.col("s.NHSNo")).alias("nhs_number"),
            F.coalesce(F.col("m._mp_person_status"), F.lit("unresolved")).alias("person_match_status"),
            F.col("s.RequestDT").alias("request_dt"),
            F.col("s.SampleDT").alias("sample_dt"),
            F.col("s.ReportDate").alias("report_dt"),
            F.col("s.ClinicalDetails").alias("clinical_details"),
            F.col("s.TLCsRequested").alias("tlcs_requested"),
            F.col("s.Conditions").alias("conditions"),
            F.col("s.Reason").alias("reason"),
            F.col("s.UrgentFlag").alias("urgent_flag"),
            F.col("s.BodySiteCode").alias("body_site_code"),
            F.col("s.CSpecTypeCode").alias("specimen_type_code"),
            F.col("s.SpecimenCategory").alias("specimen_category"),
            F.col("s.OrderNo").alias("order_no"),
            F.lit(None).cast("long").alias("order_id"),
            F.lit(None).cast("string").alias("order_mnemonic"),
            F.greatest(F.col("s.ADC_UPDT"), F.col("m._mp_adc")).alias("source_adc_updt"),
        )
    )

    linked = (
        mapped.filter(F.col("source_table") == "linked")
        .groupBy("source_parent_key")
        .agg(
            F.max("lab_no").alias("lab_no"),
            F.max("PERSON_ID").cast("long").alias("person_id"),
            F.max("ENCNTR_ID").cast("long").alias("encounter_id"),
            F.max("MRN").alias("mrn"),
            F.max("NHS_Number").alias("nhs_number"),
            F.max("person_match_status").alias("person_match_status"),
            F.min("request_dt").alias("request_dt"),
            F.min("measurement_datetime").alias("sample_dt"),
            F.max("ReportDate").alias("report_dt"),
            F.max("order_id").cast("long").alias("order_id"),
            F.max("code").alias("order_mnemonic"),
            F.max("source_code").alias("source_site_code"),
            F.max("source_adc_updt").alias("source_adc_updt"),
        )
        .select(
            F.lit("CERNER").alias("source_system"),
            "source_parent_key",
            F.lit(None).cast("int").alias("LIMSNo"),
            "lab_no",
            F.lit(None).cast("string").alias("wkg_code"),
            "source_site_code",
            "person_id",
            "encounter_id",
            "mrn",
            "nhs_number",
            F.lit("native").alias("person_match_status"),
            "request_dt",
            "sample_dt",
            "report_dt",
            F.lit(None).cast("string").alias("clinical_details"),
            F.lit(None).cast("string").alias("tlcs_requested"),
            F.lit(None).cast("string").alias("conditions"),
            F.lit(None).cast("string").alias("reason"),
            F.lit(None).cast("string").alias("urgent_flag"),
            F.lit(None).cast("string").alias("body_site_code"),
            F.lit(None).cast("string").alias("specimen_type_code"),
            F.lit(None).cast("string").alias("specimen_category"),
            F.lit(None).cast("string").alias("order_no"),
            "order_id",
            "order_mnemonic",
            "source_adc_updt",
        )
    )

    source = raw.unionByName(linked)
    source = source.withColumn("normalized_lab_no", _normalize_lab_no(F.col("lab_no")))
    source = source.withColumn(
        "source_accession_id",
        F.when(
            F.col("source_system") == "CERNER",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit("CERNER_ORDER"),
                    F.coalesce(F.col("order_id").cast("string"), F.col("source_parent_key")),
                ),
                256,
            ),
        ).otherwise(
            F.sha2(
                F.concat_ws("|", F.lit("TFC_LIMS"), F.col("source_parent_key")),
                256,
            )
        ),
    )
    source = source.withColumn(
        "match_group_key",
        F.sha2(
            F.concat_ws(
                "|",
                F.lit("pathology_match_group_v1"),
                F.coalesce(F.col("normalized_lab_no"), F.col("source_accession_id")),
            ),
            256,
        ),
    )
    source = source.withColumn(
        "person_projection_status",
        F.when(
            F.col("person_id").isNotNull()
            & ~F.lower(F.coalesce(F.col("person_match_status"), F.lit(""))).isin(
                "conflict", "ambiguous", "unresolved"
            ),
            F.lit("eligible"),
        )
        .when(F.lower(F.col("person_match_status")) == "conflict", F.lit("conflicting"))
        .otherwise(F.lit("unresolved")),
    )

    accession_source_table = f"{config.bronze_schema}.map_pathology_accession_source"
    existing = (
        spark.table(accession_source_table)
        .filter(F.col("is_current") == True)
        .groupBy("source_accession_id")
        .agg(F.min("pathology_accession_id").alias("_existing_accession_id"))
        if table_exists(spark, accession_source_table)
        else None
    )
    source_ids = source.select("source_accession_id").distinct()
    if existing is not None:
        source_ids = source_ids.join(existing, "source_accession_id", "left")
    else:
        source_ids = source_ids.withColumn("_existing_accession_id", F.lit(None).cast("string"))
    source_ids = source_ids.withColumn(
        "pathology_accession_id",
        F.coalesce(F.col("_existing_accession_id"), F.expr("uuid()")),
    ).select("source_accession_id", "pathology_accession_id")
    return source.join(source_ids, "source_accession_id", "inner")


def build_link_candidates(spark, source_stage, config: PipelineConfig):
    """Generate bounded raw↔Cerner candidates and approved unique merges."""

    _, Window, F, _ = _imports()
    grouped = source_stage.groupBy("source_accession_id", "source_system", "pathology_accession_id").agg(
        F.first("normalized_lab_no", ignorenulls=True).alias("normalized_lab_no"),
        F.first("source_site_code", ignorenulls=True).alias("source_site_code"),
        F.first("wkg_code", ignorenulls=True).alias("wkg_code"),
        F.first("person_id", ignorenulls=True).alias("person_id"),
        F.first("person_projection_status", ignorenulls=True).alias("person_projection_status"),
        F.min("request_dt").alias("request_dt"),
        F.min("sample_dt").alias("sample_dt"),
        F.max("report_dt").alias("report_dt"),
        F.first("match_group_key", ignorenulls=True).alias("match_group_key"),
    ).withColumn("match_dt", _source_date())

    raw = grouped.filter(F.col("source_system") == "TFC_LIMS").alias("r")
    linked = grouped.filter(F.col("source_system") == "CERNER").alias("l")
    candidates = raw.join(
        linked,
        (F.col("r.normalized_lab_no") == F.col("l.normalized_lab_no"))
        & F.col("r.normalized_lab_no").isNotNull()
        & (F.abs(F.datediff(F.col("r.match_dt"), F.col("l.match_dt"))) <= 7),
        "inner",
    ).select(
        F.col("r.source_accession_id").alias("left_source_accession_id"),
        F.col("l.source_accession_id").alias("right_source_accession_id"),
        F.col("r.pathology_accession_id").alias("left_pathology_accession_id"),
        F.col("l.pathology_accession_id").alias("right_pathology_accession_id"),
        F.col("r.match_group_key").alias("match_group_key"),
        F.abs(F.datediff(F.col("r.match_dt"), F.col("l.match_dt"))).cast("int").alias("day_difference"),
        (
            F.col("r.person_id").isNotNull()
            & F.col("l.person_id").isNotNull()
            & (F.col("r.person_id") == F.col("l.person_id"))
        ).alias("identifier_agreement"),
        (
            F.col("r.person_id").isNotNull()
            & F.col("l.person_id").isNotNull()
            & (F.col("r.person_id") != F.col("l.person_id"))
        ).alias("identifier_conflict"),
        F.col("r.source_site_code").alias("raw_site"),
        F.col("l.source_site_code").alias("linked_site"),
        F.col("r.wkg_code").alias("raw_wkg_code"),
        F.col("r.normalized_lab_no").alias("normalized_lab_no"),
    )
    left_counts = candidates.groupBy("left_source_accession_id").count().withColumnRenamed("count", "left_candidate_count")
    right_counts = candidates.groupBy("right_source_accession_id").count().withColumnRenamed("count", "right_candidate_count")
    candidates = candidates.join(left_counts, "left_source_accession_id").join(right_counts, "right_source_accession_id")

    approved_rule = None
    rules_table = f"{config.lookup_schema}.pathology_accession_link_rule"
    if table_exists(spark, rules_table):
        rules = (
            spark.table(rules_table)
            .filter(
                (F.upper(F.col("status")) == "APPROVED")
                & (F.col("auto_merge_ind") == True)
                & (F.current_timestamp() >= F.coalesce(F.col("valid_from"), F.lit("1900-01-01").cast("timestamp")))
                & (F.current_timestamp() < F.coalesce(F.col("valid_to"), F.lit("9999-12-31").cast("timestamp")))
            )
            .orderBy(F.col("reviewed_at").desc_nulls_last())
            .limit(1)
            .collect()
        )
        approved_rule = rules[0].asDict() if rules else None

    can_merge = (
        F.lit(bool(config.enable_approved_accession_merges and approved_rule))
        & (F.col("left_candidate_count") == 1)
        & (F.col("right_candidate_count") == 1)
        & ~F.col("identifier_conflict")
    )
    if approved_rule:
        can_merge = can_merge & (
            F.col("day_difference") <= F.lit(int(approved_rule["max_day_difference"]))
        )
        if approved_rule.get("require_identifier_agreement"):
            can_merge = can_merge & F.col("identifier_agreement")
        if approved_rule.get("require_site_agreement"):
            can_merge = can_merge & F.col("raw_site").eqNullSafe(F.col("linked_site"))

    rule_id = approved_rule["rule_id"] if approved_rule else "lab_no_date_candidate_v1"
    rule_version = approved_rule["rule_version"] if approved_rule else "1"
    candidates = (
        candidates.withColumn(
            "candidate_status",
            F.when(F.col("identifier_conflict"), F.lit("conflicted"))
            .when(can_merge, F.lit("approved"))
            .otherwise(F.lit("proposed")),
        )
        .withColumn("candidate_rule_id", F.lit(rule_id))
        .withColumn("rule_version", F.lit(rule_version))
        .withColumn(
            "confidence",
            F.when(F.col("identifier_agreement"), F.lit(1.0))
            .when(F.col("identifier_conflict"), F.lit(0.0))
            .otherwise(F.lit(0.8)),
        )
        .withColumn(
            "link_candidate_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit("pathology_link_candidate"),
                    F.col("left_source_accession_id"),
                    F.col("right_source_accession_id"),
                    F.col("candidate_rule_id"),
                    F.col("rule_version"),
                ),
                256,
            ),
        )
        .withColumn(
            "evidence_json",
            F.to_json(
                F.struct(
                    "normalized_lab_no",
                    "day_difference",
                    "identifier_agreement",
                    "identifier_conflict",
                    "raw_site",
                    "linked_site",
                    "raw_wkg_code",
                    "left_candidate_count",
                    "right_candidate_count",
                )
            ),
        )
    )

    approved = candidates.filter(F.col("candidate_status") == "approved")
    merge_map = (
        approved.select(
            F.col("left_pathology_accession_id").alias("member_id"),
            F.least("left_pathology_accession_id", "right_pathology_accession_id").alias("survivor_id"),
            F.col("candidate_rule_id").alias("merge_rule_id"),
            F.col("evidence_json").alias("merge_evidence_json"),
        )
        .unionByName(
            approved.select(
                F.col("right_pathology_accession_id").alias("member_id"),
                F.least("left_pathology_accession_id", "right_pathology_accession_id").alias("survivor_id"),
                F.col("candidate_rule_id").alias("merge_rule_id"),
                F.col("evidence_json").alias("merge_evidence_json"),
            )
        )
        .dropDuplicates(["member_id"])
    )
    return candidates, merge_map


def apply_merge_map(source_stage, merge_map):
    _, _, F, _ = _imports()
    joined = source_stage.alias("s").join(
        merge_map.alias("m"),
        F.col("s.pathology_accession_id") == F.col("m.member_id"),
        "left",
    )
    mapped = joined.select(
        *[
            F.col(f"s.`{column}`").alias(column)
            for column in source_stage.columns
            if column != "pathology_accession_id"
        ],
        F.coalesce(
            F.col("m.survivor_id"), F.col("s.pathology_accession_id")
        ).alias("pathology_accession_id"),
        F.col("s.pathology_accession_id").alias("_old_pathology_accession_id"),
        F.when(F.col("m.survivor_id").isNotNull(), F.lit("confirmed"))
        .otherwise(F.lit("source_local"))
        .alias("link_status"),
        F.col("m.merge_rule_id").alias("link_rule_id"),
        F.when(F.col("m.survivor_id").isNotNull(), F.lit(1.0))
        .otherwise(F.lit(None).cast("double"))
        .alias("link_confidence"),
        F.col("m.merge_evidence_json").alias("link_evidence_json"),
    )
    aliases = (
        merge_map.filter(F.col("member_id") != F.col("survivor_id"))
        .select(
            F.col("member_id").alias("retired_pathology_accession_id"),
            F.col("survivor_id").alias("survivor_pathology_accession_id"),
            "merge_rule_id",
            "merge_evidence_json",
        )
        .withColumn("merged_at", F.current_timestamp())
    )
    return mapped, aliases


def build_accession_source_rows(source_stage):
    _, _, F, _ = _imports()
    return source_stage.select(
        "source_accession_id",
        "pathology_accession_id",
        "source_system",
        "source_parent_key",
        "LIMSNo",
        "lab_no",
        "normalized_lab_no",
        "wkg_code",
        "source_site_code",
        "person_id",
        "encounter_id",
        "mrn",
        "nhs_number",
        "person_match_status",
        "person_projection_status",
        "request_dt",
        "sample_dt",
        "report_dt",
        "order_id",
        "order_mnemonic",
        F.coalesce(F.col("link_status"), F.lit("source_local")).alias("link_status"),
        "link_rule_id",
        "link_confidence",
        "link_evidence_json",
        "match_group_key",
        F.lit(True).alias("is_current"),
    )


def build_accession_rows(spark, source_stage, config: PipelineConfig):
    _, _, F, _ = _imports()
    eligible = F.when(F.col("person_projection_status") == "eligible", F.col("person_id"))
    grouped = source_stage.groupBy("pathology_accession_id").agg(
        F.min("source_accession_id").alias("primary_source_accession_id"),
        F.countDistinct(eligible).alias("_eligible_person_count"),
        F.max(eligible).alias("_eligible_person_id"),
        F.countDistinct("normalized_lab_no").alias("_lab_count"),
        F.first("normalized_lab_no", ignorenulls=True).alias("normalized_lab_no"),
        F.min("request_dt").alias("request_dt"),
        F.min("sample_dt").alias("sample_dt"),
        F.max("report_dt").alias("report_dt"),
        F.first("clinical_details", ignorenulls=True).alias("clinical_details"),
        F.first("tlcs_requested", ignorenulls=True).alias("tlcs_requested"),
        F.first("conditions", ignorenulls=True).alias("conditions"),
        F.first("reason", ignorenulls=True).alias("reason"),
        F.first("urgent_flag", ignorenulls=True).alias("urgent_flag"),
        F.first("body_site_code", ignorenulls=True).alias("body_site_code"),
        F.first("specimen_type_code", ignorenulls=True).alias("specimen_type_code"),
        F.first("source_site_code", ignorenulls=True).alias("source_site_code"),
        F.first("wkg_code", ignorenulls=True).alias("_wkg_code"),
        F.max(F.when(F.col("link_status") == "confirmed", F.lit(1)).otherwise(F.lit(0))).alias("_has_confirmed_link"),
        F.max(F.when(F.col("person_projection_status") == "conflicting", F.lit(1)).otherwise(F.lit(0))).alias("_has_conflict"),
    )
    series = F.regexp_extract(F.col("normalized_lab_no"), "[A-Z]", 0)
    grouped = (
        grouped.withColumn("lab_series", F.when(series == "", F.lit("UNKNOWN")).otherwise(series))
        .withColumn(
            "lab_series_desc",
            F.when(series == "S", "histopathology")
            .when(series == "N", "cytology")
            .when(series == "E", "SIHMDS")
            .when(series == "", "unknown")
            .otherwise("other"),
        )
        .withColumn(
            "discipline",
            F.when(series == "S", "cellular_pathology")
            .when(series == "N", "cytology")
            .when(series == "E", "sihmds")
            .when(F.lower(F.coalesce(F.col("_wkg_code"), F.lit(""))).rlike("micro|bact|virol"), "microbiology")
            .otherwise("other"),
        )
        .withColumn(
            "person_resolution_status",
            F.when(F.col("_has_conflict") == 1, "conflicting")
            .when(F.col("_eligible_person_count") == 1, "eligible")
            .otherwise("unresolved"),
        )
        .withColumn(
            "canonical_person_id",
            F.when(F.col("person_resolution_status") == "eligible", F.col("_eligible_person_id")),
        )
        .withColumn(
            "canonical_accession_status",
            F.when(F.col("_has_conflict") == 1, "ambiguous")
            .when(F.col("_has_confirmed_link") == 1, "active")
            .otherwise("unlinked"),
        )
        .withColumn("lifecycle_status", F.lit("active"))
        .withColumn("match_rule_version", F.lit("accession_match_v1"))
        .withColumn("research_qi_only", F.lit(True))
        .withColumn("body_site_snomed_code", F.lit(None).cast("string"))
        .withColumn("specimen_type_snomed_code", F.lit(None).cast("string"))
    )
    existing_table = f"{config.bronze_schema}.map_pathology_accession"
    if table_exists(spark, existing_table):
        existing = spark.table(existing_table).select(
            "pathology_accession_id", "created_at", "retired_at"
        )
        grouped = grouped.join(existing, "pathology_accession_id", "left")
    else:
        grouped = grouped.withColumn("created_at", F.lit(None).cast("timestamp")).withColumn("retired_at", F.lit(None).cast("timestamp"))
    return grouped.withColumn("created_at", F.coalesce(F.col("created_at"), F.current_timestamp()))


def build_requested_test_rows(spark, source_stage, config: PipelineConfig):
    _, Window, F, _ = _imports()
    raw = (
        source_stage.filter(
            (F.col("source_system") == "TFC_LIMS")
            & F.col("tlcs_requested").isNotNull()
            & (F.length(F.trim("tlcs_requested")) > 0)
        )
        .select(
            "pathology_accession_id",
            "source_accession_id",
            "source_parent_key",
            "wkg_code",
            F.posexplode(
                F.split(F.col("tlcs_requested"), r"\s*(?:[,;|]|\r?\n|\t)\s*")
            ).alias("request_ordinal", "raw_request_text"),
        )
        .filter(F.length(F.trim("raw_request_text")) > 0)
        .withColumn("tlc_code_normalized", F.upper(F.trim("raw_request_text")))
    )
    master = spark.table(config.master_orderable_table)
    master_window = Window.partitionBy(
        F.upper(F.trim("WkgCode")), F.upper(F.trim("TLCCode"))
    ).orderBy(F.col("LastUpdatedDT").desc_nulls_last(), F.col("ADC_UPDT").desc_nulls_last())
    master = (
        master.withColumn("_rn", F.row_number().over(master_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            F.upper(F.trim("WkgCode")).alias("_wkg"),
            F.upper(F.trim("TLCCode")).alias("_tlc"),
            F.col("TLCCode").alias("_tlc_code"),
            F.coalesce("TLCDesc_Full", "TLCDesc_WP", "EPRDisplayName").alias("_test_description"),
            F.col("SnomedCTCode").cast("string").alias("_snomed_code"),
            F.col("NLMC_ID").cast("string").alias("_nlmc_id"),
        )
    )
    raw = raw.join(
        master,
        (F.upper(F.trim(raw.wkg_code)) == master._wkg)
        & (raw.tlc_code_normalized == master._tlc),
        "left",
    )
    concepts = (
        spark.table(config.omop_concept_table)
        .filter(F.col("vocabulary_id") == "SNOMED")
        .select(F.col("concept_code").alias("_concept_code"), F.col("concept_id").cast("long").alias("_omop_concept_id"))
    )
    raw = raw.join(concepts, raw._snomed_code == concepts._concept_code, "left")
    raw = (
        raw.withColumn("source_system", F.lit("TFC_LIMS"))
        .withColumn("requested_test_occurrence_id", F.sha2(F.concat_ws("|", F.lit("raw_request"), "source_parent_key", F.col("request_ordinal"), "raw_request_text"), 256))
        .withColumn("tlc_code", F.coalesce("_tlc_code", "raw_request_text"))
        .withColumn("order_id", F.lit(None).cast("long"))
        .withColumn("order_mnemonic", F.lit(None).cast("string"))
        .withColumn("test_description", F.col("_test_description"))
        .withColumn("test_snomed_code", F.col("_snomed_code"))
        .withColumn("test_loinc_code", F.lit(None).cast("string"))
        .withColumn("test_omop_concept_id", F.col("_omop_concept_id"))
        .withColumn("mapping_status", F.when(F.col("_tlc_code").isNotNull(), "mapped").otherwise("unmapped"))
        .withColumn("mapping_rule_id", F.when(F.col("_tlc_code").isNotNull(), "path_master_orderables_exact_v1"))
    )

    linked = (
        source_stage.filter(
            (F.col("source_system") == "CERNER") & F.col("order_id").isNotNull()
        )
        .select(
            "pathology_accession_id",
            "source_accession_id",
            "order_id",
            "order_mnemonic",
        )
        .dropDuplicates(["source_accession_id", "order_id"])
        .withColumn("source_system", F.lit("CERNER"))
        .withColumn("request_ordinal", F.lit(0))
        .withColumn("raw_request_text", F.col("order_mnemonic"))
        .withColumn("wkg_code", F.lit(None).cast("string"))
        .withColumn("tlc_code", F.lit(None).cast("string"))
        .withColumn("requested_test_occurrence_id", F.sha2(F.concat_ws("|", F.lit("linked_order"), F.col("order_id")), 256))
        .withColumn("test_description", F.col("order_mnemonic"))
        .withColumn("test_snomed_code", F.lit(None).cast("string"))
        .withColumn("test_loinc_code", F.lit(None).cast("string"))
        .withColumn("test_omop_concept_id", F.lit(None).cast("long"))
        .withColumn("mapping_status", F.lit("unmapped"))
        .withColumn("mapping_rule_id", F.lit(None).cast("string"))
    )
    combined = raw.select(*[c.name for c in contract("map_pathology_requested_test").columns if c.name not in {"canonical_requested_test_id", "source_payload_hash", "contract_version", "ADC_UPDT"} and c.name in raw.columns]).unionByName(
        linked.select(*[c.name for c in contract("map_pathology_requested_test").columns if c.name not in {"canonical_requested_test_id", "source_payload_hash", "contract_version", "ADC_UPDT"} and c.name in linked.columns]),
        allowMissingColumns=True,
    )
    return combined.withColumn(
        "canonical_requested_test_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.lit("canonical_requested_test"),
                "pathology_accession_id",
                F.coalesce(
                    F.col("test_omop_concept_id").cast("string"),
                    "test_snomed_code",
                    "tlc_code",
                    "order_mnemonic",
                    "raw_request_text",
                ),
            ),
            256,
        ),
    )


def build_report_rows(spark, source_stage, config: PipelineConfig):
    _, Window, F, _ = _imports()
    source_map = source_stage.select(
        "source_system", "source_parent_key", "source_accession_id", "pathology_accession_id"
    ).dropDuplicates(["source_system", "source_parent_key"])
    mapped_source = spark.table(config.map_pathology_table).withColumn(
        "_source_system",
        F.when(F.col("source_table") == "raw", F.lit("TFC_LIMS")).otherwise(
            F.lit("CERNER")
        ),
    )
    mapped = (
        mapped_source.alias("mp")
        .join(
            source_map.alias("sm"),
            (F.col("mp._source_system") == F.col("sm.source_system"))
            & (F.col("mp.source_parent_key") == F.col("sm.source_parent_key")),
            "inner",
        )
        .select(
            "mp.*",
            F.col("sm.source_accession_id").alias("source_accession_id"),
            F.col("sm.pathology_accession_id").alias("pathology_accession_id"),
        )
        .drop("_source_system")
    )
    report_haystack = F.lower(
        F.concat_ws(
            " ",
            F.coalesce(F.col("code").cast("string"), F.lit("")),
            F.coalesce(F.col("report_section").cast("string"), F.lit("")),
            F.coalesce(F.col("description").cast("string"), F.lit("")),
            F.coalesce(F.substring(F.col("value_source_value").cast("string"), 1, 500), F.lit("")),
        )
    )
    report_role = (
        F.when(report_haystack.rlike(r"immunophenotyp|flow cytometr"), "immunophenotyping")
        .when(report_haystack.rlike(r"cytogen|karyotyp|\\bfish\\b|iscn"), "cytogenetics")
        .when(report_haystack.rlike(r"molecular|\\bngs\\b|sequenc|mutation|variant|bcr.?abl|pml.?rara"), "molecular")
        .when(report_haystack.rlike(r"morpholog|bone marrow aspirate|trephine"), "morphology")
        .when(report_haystack.rlike(r"histolog|histopath|resection|biopsy"), "histology")
        .when(report_haystack.rlike(r"cytolog|smear"), "cytology")
        .when(report_haystack.rlike(r"micro|culture|suscept|organism|no growth"), "microbiology")
        .when(report_haystack.rlike(r"technical|method|target genes|genes covered|limitations"), "technical")
        .when(report_haystack.rlike(r"administrative|specimen received|test cancelled|referred"), "administrative")
        .otherwise("unknown")
    )
    lifecycle_haystack = F.lower(
        F.concat_ws(
            " ",
            F.coalesce(F.col("result_status").cast("string"), F.lit("")),
            F.coalesce(F.substring(F.col("value_source_value").cast("string"), 1, 1000), F.lit("")),
        )
    )
    authentic = F.lower(F.trim(F.coalesce(F.col("authentic_flag").cast("string"), F.lit(""))))
    lifecycle = (
        F.when(
            authentic.isin("0", "false")
            | lifecycle_haystack.rlike(r"entered in error|report withdrawn|result withdrawn"),
            "entered_in_error",
        )
        .when(lifecycle_haystack.rlike(r"\\bcancelled|canceled|not processed"), "cancelled")
        .when(lifecycle_haystack.rlike(r"\\bcorrected|correction"), "corrected")
        .when(lifecycle_haystack.rlike(r"\\bamended|supplementary|addendum"), "amended")
        .when(lifecycle_haystack.rlike(r"\\bprelim"), "preliminary")
        .when(lifecycle_haystack.rlike(r"\\bfinal|authorised|authorized|verified"), "final")
        .otherwise("unknown")
    )
    reports = (
        mapped.withColumn("report_role", report_role)
        .filter(
            F.col("value_source_value").isNotNull()
            & (
                (F.length("value_source_value") >= 80)
                | (F.col("report_role") != "unknown")
            )
        )
        .withColumn(
            "discipline",
            F.when(F.col("report_role") == "microbiology", "microbiology")
            .when(F.col("report_role") == "cytology", "cytology")
            .when(F.col("report_role").isin("histology"), "cellular_pathology")
            .when(F.col("report_role").isin("morphology", "immunophenotyping", "molecular", "cytogenetics"), "sihmds")
            .otherwise("other"),
        )
        .withColumn("report_text", F.col("value_source_value"))
        .withColumn("report_text_hash", F.sha2(F.regexp_replace(F.trim("value_source_value"), r"\s+", " "), 256))
        .withColumn("issued_dt", F.coalesce("ReportDate", "verified_dt_tm", "performed_dt_tm", "measurement_datetime"))
        .withColumn("lifecycle_status", lifecycle)
        .withColumn(
            "report_series_id",
            F.sha2(F.concat_ws("|", F.lit("pathology_report_series"), "pathology_accession_id", "source_table", "code", "report_role"), 256),
        )
        .withColumn("report_version_id", F.sha2(F.concat_ws("|", F.lit("pathology_report_version"), "source_record_key"), 256))
        .withColumn("report_code", F.col("code"))
        .withColumn("research_qi_only", F.lit(True))
    )
    version_window = Window.partitionBy("report_series_id").orderBy(
        F.col("issued_dt").asc_nulls_first(), F.col("source_sequence_start").asc_nulls_first(), F.col("source_record_key")
    )
    current_window = Window.partitionBy("report_series_id").orderBy(
        F.col("issued_dt").desc_nulls_last(), F.col("source_sequence_end").desc_nulls_last(), F.col("source_record_key").desc()
    )
    return (
        reports.withColumn("version_ordinal", F.row_number().over(version_window))
        .withColumn("supersedes_report_version_id", F.lag("report_version_id").over(version_window))
        .withColumn("valid_from", F.col("issued_dt"))
        .withColumn("valid_to", F.lead("issued_dt").over(version_window))
        .withColumn(
            "is_current",
            (F.row_number().over(current_window) == 1)
            & ~F.col("lifecycle_status").isin("entered_in_error", "cancelled"),
        )
    )


def build_result_equivalence_rows(spark, source_stage, report_rows, config: PipelineConfig):
    _, Window, F, _ = _imports()
    source_map = source_stage.select(
        "source_system",
        "source_parent_key",
        "pathology_accession_id",
        "person_projection_status",
    ).dropDuplicates(["source_system", "source_parent_key"])
    mapped = spark.table(config.map_pathology_table).withColumn(
        "source_system",
        F.when(F.col("source_table") == "raw", "TFC_LIMS").otherwise("CERNER"),
    )
    base = (
        mapped.alias("mp")
        .join(
            source_map.alias("sm"),
            (F.col("mp.source_system") == F.col("sm.source_system"))
            & (F.col("mp.source_parent_key") == F.col("sm.source_parent_key")),
            "inner",
        )
        .select(
            "mp.*",
            F.col("sm.pathology_accession_id").alias("pathology_accession_id"),
            F.col("sm.person_projection_status").alias("person_projection_status"),
        )
        .withColumn("_value_norm", F.lower(F.regexp_replace(F.trim(F.coalesce("value_source_value", F.lit(""))), r"\s+", " ")))
        .withColumn("_unit_norm", F.lower(F.trim(F.coalesce("ucum_code", "unit_source_value", F.lit("")))))
        .withColumn("_result_dt", F.to_date("measurement_datetime"))
    )
    report_map = report_rows.select("source_record_key", "report_version_id", "lifecycle_status", "is_current").dropDuplicates(["source_record_key"])
    base = base.join(report_map, "source_record_key", "left")

    pair_map = None
    if config.enable_result_equivalence:
        raw = base.filter((F.col("source_table") == "raw") & F.col("measurement_concept_id").isNotNull()).alias("r")
        linked = base.filter((F.col("source_table") == "linked") & F.col("measurement_concept_id").isNotNull()).alias("l")
        pairs = raw.join(
            linked,
            (F.col("r.pathology_accession_id") == F.col("l.pathology_accession_id"))
            & (F.col("r.measurement_concept_id") == F.col("l.measurement_concept_id"))
            & (F.col("r._value_norm") == F.col("l._value_norm"))
            & (F.col("r._unit_norm") == F.col("l._unit_norm"))
            & (F.abs(F.datediff(F.col("r._result_dt"), F.col("l._result_dt"))) <= 1)
            & (F.col("r.person_projection_status") == "eligible")
            & (F.col("l.person_projection_status") == "eligible"),
            "inner",
        ).select(
            F.col("r.source_record_key").alias("raw_key"),
            F.col("l.source_record_key").alias("linked_key"),
            F.col("r.pathology_accession_id").alias("pathology_accession_id"),
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit("pathology_result_equivalence"),
                    F.col("r.pathology_accession_id"),
                    F.least(F.col("r.source_record_key"), F.col("l.source_record_key")),
                    F.greatest(F.col("r.source_record_key"), F.col("l.source_record_key")),
                ),
                256,
            ).alias("canonical_result_id"),
        )
        raw_count = pairs.groupBy("raw_key").count().withColumnRenamed("count", "raw_count")
        linked_count = pairs.groupBy("linked_key").count().withColumnRenamed("count", "linked_count")
        pairs = pairs.join(raw_count, "raw_key").join(linked_count, "linked_key").filter((F.col("raw_count") == 1) & (F.col("linked_count") == 1))
        pair_map = pairs.select(F.col("raw_key").alias("source_record_key"), "canonical_result_id", F.lit("preferred").alias("representation_role"), F.lit(True).alias("preferred_result_ind")).unionByName(
            pairs.select(F.col("linked_key").alias("source_record_key"), "canonical_result_id", F.lit("alternate").alias("representation_role"), F.lit(False).alias("preferred_result_ind"))
        )

    if pair_map is not None:
        base = base.join(pair_map, "source_record_key", "left")
    else:
        base = base.withColumn("canonical_result_id", F.lit(None).cast("string")).withColumn("representation_role", F.lit(None).cast("string")).withColumn("preferred_result_ind", F.lit(None).cast("boolean"))
    return (
        base.withColumn(
            "canonical_result_id",
            F.coalesce(
                "canonical_result_id",
                F.sha2(F.concat_ws("|", F.lit("pathology_result_unique"), "source_record_key"), 256),
            ),
        )
        .withColumn("match_group_key", F.sha2(F.concat_ws("|", F.lit("result_match_group"), "pathology_accession_id", F.coalesce(F.col("measurement_concept_id").cast("string"), "code"), "_value_norm", "_unit_norm"), 256))
        .withColumn("equivalence_rule_id", F.when(F.col("representation_role").isNotNull(), "same_omop_value_unit_date_v1").otherwise("unique_source_result_v1"))
        .withColumn("equivalence_confidence", F.when(F.col("representation_role").isNotNull(), 1.0).otherwise(1.0))
        .withColumn("representation_role", F.coalesce("representation_role", F.lit("unique")))
        .withColumn("preferred_result_ind", F.coalesce("preferred_result_ind", F.lit(True)))
        .withColumn("lifecycle_status", F.coalesce("lifecycle_status", F.lit("unknown")))
        .withColumn("is_current", F.coalesce("is_current", F.lit(True)))
    )


def _candidate_contract_rows(candidates):
    return candidates.select(
        "link_candidate_id",
        "left_source_accession_id",
        "right_source_accession_id",
        "match_group_key",
        "candidate_rule_id",
        "candidate_status",
        "day_difference",
        "identifier_agreement",
        "identifier_conflict",
        "confidence",
        "evidence_json",
        "rule_version",
    )


def run_core(
    spark,
    config: PipelineConfig | None = None,
    *,
    full_reconcile: bool = True,
    touched_parent_keys=None,
    validate_stage_keys: bool = True,
) -> dict[str, dict[str, int]]:
    """Build accession, request, report, and equivalence sidecars in the selected dev schemas."""

    from pyspark import StorageLevel

    config = config or PipelineConfig()
    ensure_contracts(spark, config)
    source_stage = build_source_stage(spark, config)
    scoped_match_groups = None
    if touched_parent_keys is not None:
        _, _, F, _ = _imports()
        touched = touched_parent_keys.select("source_system", "source_parent_key").dropDuplicates()
        new_groups = source_stage.join(
            touched, ["source_system", "source_parent_key"], "inner"
        ).select("match_group_key")
        previous_groups = (
            spark.table(f"{config.bronze_schema}.map_pathology_accession_source")
            .join(touched, ["source_system", "source_parent_key"], "inner")
            .select("match_group_key")
        )
        scoped_match_groups = new_groups.unionByName(previous_groups).dropDuplicates()
        source_stage = source_stage.join(scoped_match_groups, "match_group_key", "inner")
        full_reconcile = False
    identity_stage = source_stage.persist(StorageLevel.DISK_ONLY)
    identity_stage.count()
    candidates, merge_map = build_link_candidates(spark, identity_stage, config)
    source_stage, aliases = apply_merge_map(identity_stage, merge_map)
    source_stage = source_stage.persist(StorageLevel.DISK_ONLY)
    source_stage.count()
    identity_stage.unpersist()

    outputs = {
        "map_pathology_accession_source": build_accession_source_rows(source_stage),
        "map_pathology_accession_link_candidate": _candidate_contract_rows(candidates),
        "map_pathology_accession_alias": aliases,
        "map_pathology_accession": build_accession_rows(spark, source_stage, config),
    }
    outputs["map_pathology_requested_test"] = build_requested_test_rows(spark, source_stage, config)
    reports = build_report_rows(spark, source_stage, config).persist(StorageLevel.DISK_ONLY)
    reports.count()
    outputs["map_pathology_report"] = reports
    outputs["map_pathology_result_equivalence"] = build_result_equivalence_rows(spark, source_stage, reports, config)

    metrics: dict[str, dict[str, int]] = {}
    registry_tables = {"map_pathology_accession", "map_pathology_accession_alias"}
    for name, frame in outputs.items():
        item = contract(name)
        stale_update = None
        delete_not_matched = full_reconcile and name not in registry_tables and name != "map_pathology_accession_source"
        if full_reconcile and name == "map_pathology_accession_source":
            _, _, F, _ = _imports()
            stale_update = {"is_current": F.lit(False), "ADC_UPDT": F.current_timestamp()}
        metrics[name] = merge_contract(
            spark,
            f"{config.bronze_schema}.{name}",
            frame,
            item,
            delete_not_matched=delete_not_matched,
            stale_update=stale_update,
            validate_stage_keys=validate_stage_keys,
        )
    if scoped_match_groups is not None:
        scoped_accessions = source_stage.select("pathology_accession_id").dropDuplicates()
        scope_by_table = {
            "map_pathology_accession_source": (scoped_match_groups, "match_group_key", True),
            "map_pathology_accession_link_candidate": (scoped_match_groups, "match_group_key", False),
            "map_pathology_requested_test": (scoped_accessions, "pathology_accession_id", False),
            "map_pathology_report": (scoped_accessions, "pathology_accession_id", False),
            "map_pathology_result_equivalence": (scoped_accessions, "pathology_accession_id", False),
        }
        for name, (scope, scope_column, mark_inactive) in scope_by_table.items():
            stale = reconcile_scoped_stale(
                spark,
                f"{config.bronze_schema}.{name}",
                outputs[name],
                contract(name),
                scope,
                scope_column,
                mark_inactive=mark_inactive,
            )
            metrics[name]["stale_reconciled"] = stale
    reports.unpersist()
    source_stage.unpersist()
    return metrics
