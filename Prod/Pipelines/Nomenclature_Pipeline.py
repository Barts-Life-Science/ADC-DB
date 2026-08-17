# Databricks notebook source
import json
import uuid

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("lookback_days", "7")
dbutils.widgets.dropdown("force_full", "false", ["false", "true"])
dbutils.widgets.dropdown("enable_prefix_fallback", "false", ["false", "true"])
dbutils.widgets.dropdown("include_all_nomenclature", "false", ["false", "true"])
dbutils.widgets.dropdown(
    "allow_full_nomenclature_embedding_backfill", "false", ["false", "true"]
)
dbutils.widgets.dropdown("refresh_model_mismatch", "false", ["false", "true"])
dbutils.widgets.dropdown("prune_stale", "false", ["false", "true"])
dbutils.widgets.text("embedding_endpoint", "azure_openai_embedding_endpoint")
dbutils.widgets.text("embedding_model_version", "text-embedding-3-large")
dbutils.widgets.text("batch_size", "250")
dbutils.widgets.text("expected_dimension", "3072")
dbutils.widgets.text("similarity_threshold", "0.70")
dbutils.widgets.dropdown("full_replace", "false", ["false", "true"])
dbutils.widgets.text("vector_match_batch_size", "128")

PIPELINE_RUN_ID = dbutils.widgets.get("run_id").strip() or uuid.uuid4().hex

# COMMAND ----------

def run_mapping_section():
    import json
    import uuid
    from datetime import datetime, timedelta, timezone
    from functools import reduce

    from delta.tables import DeltaTable
    from pyspark.sql import DataFrame, functions as F
    from pyspark.sql.types import (
        BooleanType,
        DecimalType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    from pyspark.sql.window import Window



    RUN_ID = PIPELINE_RUN_ID
    LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
    FORCE_FULL = dbutils.widgets.get("force_full").lower() == "true"
    ENABLE_PREFIX_FALLBACK = dbutils.widgets.get("enable_prefix_fallback").lower() == "true"

    SOURCE_TABLE = "3_lookup.mill.mill_nomenclature"
    CONCEPT_TABLE = "3_lookup.omop.concept"
    RELATIONSHIP_TABLE = "3_lookup.omop.concept_relationship"
    ICD_MAP_TABLE = "3_lookup.trud.maps_icdsctmap"
    OPCS_MAP_TABLE = "3_lookup.trud.maps_opcssctmap"
    SNOMED_HIER_TABLE = "3_lookup.trud.snomed_scthier"

    TEMP_ONE = "4_prod.tmp.tempone_nomenclature"
    TEMP_TWO = "4_prod.tmp.temptwo_nomenclature"
    TEMP_THREE = "4_prod.tmp.tempthree_nomenclature"
    TEMP_FOUR = "4_prod.tmp.tempfour_nomenclature"
    STATE_TABLE = "4_prod.tmp.nomenclature_pipeline_state"
    RUN_ITEMS_TABLE = "4_prod.tmp.nomenclature_pipeline_run_items"

    SNOMED_VOCAB_CODES = [466776237, 673967]
    MULTUM_VOCAB_CODES = [64004559, 1238, 1237]
    ICD10_VOCAB_CODES = [2976507, 647081]
    OPCS4_VOCAB_CODES = [685812]


    def table_exists(table_name: str) -> bool:
        return spark.catalog.tableExists(table_name)


    def ensure_columns(table_name: str, source_df: DataFrame) -> None:
        existing = {field.name.lower() for field in spark.table(table_name).schema.fields}
        for field in source_df.schema.fields:
            if field.name.lower() not in existing:
                spark.sql(
                    f"ALTER TABLE {table_name} ADD COLUMNS (`{field.name}` {field.dataType.simpleString()})"
                )


    def merge_rows(
        source_df: DataFrame,
        target_table: str,
        primary_keys=("NOMENCLATURE_ID",),
        force_update: bool = True,
    ) -> None:
        order_cols = []
        if "SOURCE_CHANGE_TS" in source_df.columns:
            order_cols.append(F.col("SOURCE_CHANGE_TS").desc_nulls_last())
        if "ADC_UPDT" in source_df.columns:
            order_cols.append(F.col("ADC_UPDT").desc_nulls_last())
        if not order_cols:
            order_cols = [F.lit(1)]

        window = Window.partitionBy(*primary_keys).orderBy(*order_cols)
        latest = (
            source_df.withColumn("_merge_rank", F.row_number().over(window))
            .filter(F.col("_merge_rank") == 1)
            .drop("_merge_rank")
        )

        if not table_exists(target_table):
            latest.write.format("delta").mode("overwrite").saveAsTable(target_table)
            return

        ensure_columns(target_table, latest)
        source_columns = latest.columns
        on = " AND ".join([f"t.`{key}` = s.`{key}`" for key in primary_keys])
        updates = {column: f"s.`{column}`" for column in source_columns if column not in primary_keys}
        inserts = {column: f"s.`{column}`" for column in source_columns}

        builder = DeltaTable.forName(spark, target_table).alias("t").merge(latest.alias("s"), on)
        if force_update:
            builder = builder.whenMatchedUpdate(set=updates)
        else:
            condition = (
                "t.SOURCE_CHANGE_TS IS NULL OR s.SOURCE_CHANGE_TS > t.SOURCE_CHANGE_TS "
                "OR (s.SOURCE_CHANGE_TS = t.SOURCE_CHANGE_TS AND s.ADC_UPDT > t.ADC_UPDT)"
            )
            builder = builder.whenMatchedUpdate(condition=condition, set=updates)
        builder.whenNotMatchedInsert(values=inserts).execute()


    def normalize_code(column, code_type: str):
        value = F.upper(F.trim(column.cast(StringType())))
        if code_type == "MULTUM":
            return F.regexp_replace(value, "^D", "")
        if code_type in ("ICD10", "OPCS4"):
            return F.regexp_replace(value, "[^A-Z0-9]", "")
        return value


    def try_cast_column(column_name: str, data_type):
        escaped_name = column_name.replace("`", "``")
        return F.expr(f"try_cast(`{escaped_name}` AS {data_type.simpleString()})")


    def empty_candidates() -> DataFrame:
        schema = StructType(
            [
                StructField("NOMENCLATURE_ID", DecimalType(38, 18), False),
                StructField("BASE_OMOP_ID", IntegerType(), True),
                StructField("CANDIDATE_CODE", StringType(), True),
                StructField("BASE_TYPE", StringType(), False),
                StructField("PRIORITY", IntegerType(), False),
            ]
        )
        return spark.createDataFrame([], schema)


    def candidate_frame(df: DataFrame, code_column, base_type: str, priority: int) -> DataFrame:
        return (
            df.select(
                F.col("NOMENCLATURE_ID").cast(DecimalType(38, 18)).alias("NOMENCLATURE_ID"),
                F.col("OMOP_CONCEPT_ID").cast(IntegerType()).alias("BASE_OMOP_ID"),
                code_column.cast(StringType()).alias("CANDIDATE_CODE"),
                F.lit(base_type).alias("BASE_TYPE"),
                F.lit(priority).cast(IntegerType()).alias("PRIORITY"),
            )
            .filter(F.col("CANDIDATE_CODE").isNotNull() & (F.trim("CANDIDATE_CODE") != ""))
        )


    def union_candidates(*frames: DataFrame) -> DataFrame:
        valid = [frame for frame in frames if frame is not None]
        if not valid:
            return empty_candidates()
        return reduce(lambda left, right: left.unionByName(right), valid).dropDuplicates(
            ["NOMENCLATURE_ID", "CANDIDATE_CODE", "BASE_TYPE"]
        )


    def resolve_candidates(
        candidates: DataFrame,
        target_vocabularies,
        code_type: str,
        terms_table: str,
        code_output: str,
        type_output: str,
        count_output: str,
        term_output: str,
        output_data_type,
        valid_concepts: DataFrame,
        valid_maps: DataFrame,
    ) -> DataFrame:
        keys = candidates.select(
            normalize_code(F.col("CANDIDATE_CODE"), code_type).alias("TARGET_CODE")
        ).distinct()
        targets = (
            valid_concepts.filter(F.col("vocabulary_id").isin(target_vocabularies))
            .withColumn("TARGET_CODE", normalize_code(F.col("concept_code"), code_type))
            .join(F.broadcast(keys), "TARGET_CODE", "left_semi")
            .select(
                F.col("concept_id").alias("TARGET_CONCEPT_ID"),
                F.col("concept_name").alias("TARGET_CONCEPT_NAME"),
                F.col("standard_concept").alias("TARGET_STANDARD"),
                F.col("domain_id").alias("TARGET_DOMAIN"),
                "TARGET_CODE",
            )
        )

        expanded = candidates.join(
            targets,
            normalize_code(F.col("CANDIDATE_CODE"), code_type) == F.col("TARGET_CODE"),
            "left",
        )

        pairs = expanded.filter(
            F.col("BASE_OMOP_ID").isNotNull() & F.col("TARGET_CONCEPT_ID").isNotNull()
        ).select("BASE_OMOP_ID", "TARGET_CONCEPT_ID").distinct()

        forward = pairs.join(
            valid_maps,
            (F.col("BASE_OMOP_ID") == F.col("concept_id_1"))
            & (F.col("TARGET_CONCEPT_ID") == F.col("concept_id_2")),
            "inner",
        ).select("BASE_OMOP_ID", "TARGET_CONCEPT_ID")
        reverse = pairs.join(
            valid_maps,
            (F.col("TARGET_CONCEPT_ID") == F.col("concept_id_1"))
            & (F.col("BASE_OMOP_ID") == F.col("concept_id_2")),
            "inner",
        ).select("BASE_OMOP_ID", "TARGET_CONCEPT_ID")
        linked = forward.unionByName(reverse).distinct().withColumn("HAS_OMOP_LINK", F.lit(True))

        expanded = (
            expanded.join(linked, ["BASE_OMOP_ID", "TARGET_CONCEPT_ID"], "left")
            .withColumn("HAS_OMOP_LINK", F.coalesce(F.col("HAS_OMOP_LINK"), F.lit(False)))
        )

        counts = candidates.groupBy("NOMENCLATURE_ID").agg(
            F.countDistinct("CANDIDATE_CODE").cast(LongType()).alias(count_output)
        )

        ranking = Window.partitionBy("NOMENCLATURE_ID").orderBy(
            F.col("PRIORITY").asc(),
            F.col("HAS_OMOP_LINK").desc(),
            F.when(F.col("TARGET_STANDARD") == "S", 1).otherwise(0).desc(),
            F.col("TARGET_CONCEPT_ID").asc_nulls_last(),
            F.col("CANDIDATE_CODE").asc(),
            F.col("BASE_TYPE").asc(),
        )
        chosen = (
            expanded.withColumn("_rank", F.row_number().over(ranking))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
            .withColumn(
                type_output,
                F.when(
                    F.col("HAS_OMOP_LINK")
                    & ~F.col("BASE_TYPE").isin("NATIVE", "OMOP_DERIVED"),
                    F.concat(F.col("BASE_TYPE"), F.lit("_OMOP_ASSISTED")),
                ).otherwise(F.col("BASE_TYPE")),
            )
        )

        term_keys = candidates.select(
            normalize_code(F.col("CANDIDATE_CODE"), code_type).alias("TERM_CODE")
        ).distinct()
        terms = spark.table(terms_table).select(
            normalize_code(F.col("CUI"), code_type).alias("TERM_CODE"),
            F.col("TERM").cast(StringType()).alias("REFERENCE_TERM"),
        ).join(F.broadcast(term_keys), "TERM_CODE", "left_semi")
        term_rank = Window.partitionBy("TERM_CODE").orderBy(
            F.length("REFERENCE_TERM").asc_nulls_last(), F.col("REFERENCE_TERM").asc_nulls_last()
        )
        terms = (
            terms.filter(F.col("TERM_CODE").isNotNull())
            .withColumn("_term_rank", F.row_number().over(term_rank))
            .filter(F.col("_term_rank") == 1)
            .drop("_term_rank")
        )

        return (
            chosen.join(
                F.broadcast(terms),
                normalize_code(F.col("CANDIDATE_CODE"), code_type) == F.col("TERM_CODE"),
                "left",
            )
            .join(counts, "NOMENCLATURE_ID", "inner")
            .select(
                "NOMENCLATURE_ID",
                try_cast_column("CANDIDATE_CODE", output_data_type).alias(code_output),
                F.col(type_output),
                F.col(count_output),
                F.coalesce(F.col("REFERENCE_TERM"), F.col("TARGET_CONCEPT_NAME")).alias(term_output),
            )
        )


    def assert_unique(df: DataFrame, key: str, label: str) -> None:
        duplicate = df.groupBy(key).count().filter(F.col("count") > 1).limit(1).count()
        if duplicate:
            raise RuntimeError(f"{label} produced duplicate {key} values")


    run_item_schema = StructType(
        [
            StructField("RUN_ID", StringType(), False),
            StructField("NOMENCLATURE_ID", DecimalType(38, 18), False),
            StructField("SOURCE_CHANGE_TS", TimestampType(), False),
            StructField("STATUS", StringType(), False),
            StructField("CREATED_AT", TimestampType(), False),
            StructField("UPDATED_AT", TimestampType(), False),
        ]
    )
    if not table_exists(RUN_ITEMS_TABLE):
        spark.createDataFrame([], run_item_schema).write.format("delta").saveAsTable(RUN_ITEMS_TABLE)

    epoch = F.lit("1980-01-01 00:00:00").cast(TimestampType())
    source = (
        spark.table(SOURCE_TABLE)
        .withColumn(
            "SOURCE_CHANGE_TS",
            F.coalesce(F.greatest(F.col("UPDT_DT_TM"), F.col("LAST_UTC_TS")), F.col("ADC_UPDT"), epoch),
        )
        .withColumn(
            "IS_ACTIVE",
            (F.col("ACTIVE_IND") == 1)
            & (F.col("END_EFFECTIVE_DT_TM").isNull() | (F.col("END_EFFECTIVE_DT_TM") > F.current_timestamp())),
        )
        .filter(F.col("NOMENCLATURE_ID").isNotNull())
        .filter(F.col("SOURCE_CHANGE_TS") <= F.current_timestamp() + F.expr("INTERVAL 1 DAY"))
    )
    source_window = Window.partitionBy("NOMENCLATURE_ID").orderBy(
        F.col("SOURCE_CHANGE_TS").desc(), F.col("ADC_UPDT").desc_nulls_last()
    )
    source = source.withColumn("_source_rank", F.row_number().over(source_window)).filter(
        F.col("_source_rank") == 1
    ).drop("_source_rank")

    watermark = None
    if table_exists(STATE_TABLE):
        state_row = (
            spark.table(STATE_TABLE)
            .filter(F.col("PIPELINE_NAME") == "nomenclature")
            .orderBy(F.col("UPDATED_AT").desc())
            .first()
        )
        if state_row:
            watermark = state_row["LAST_SUCCESSFUL_SOURCE_CHANGE_TS"]

    if FORCE_FULL:
        candidates = source
    elif watermark is None:
        if table_exists(TEMP_ONE):
            raise RuntimeError(
                "Updated pipeline state is not initialized. Run 99_Nomenclature_Repair_Backfill once, "
                "or invoke this notebook with force_full=true."
            )
        candidates = source
    else:
        cutoff = watermark - timedelta(days=LOOKBACK_DAYS)
        candidates = source.filter(F.col("SOURCE_CHANGE_TS") > F.lit(cutoff))

    base_columns = [
        "NOMENCLATURE_ID",
        "SOURCE_IDENTIFIER",
        "SOURCE_STRING",
        "SOURCE_VOCABULARY_CD",
        "VOCAB_AXIS_CD",
        "CONCEPT_CKI",
        "ADC_UPDT",
        "SOURCE_CHANGE_TS",
        "IS_ACTIVE",
    ]
    candidates = candidates.select(*base_columns)
    candidate_count = candidates.count()
    print(f"Run {RUN_ID}: selected {candidate_count} source rows")

    DeltaTable.forName(spark, RUN_ITEMS_TABLE).delete(F.col("RUN_ID") == RUN_ID)
    if candidate_count:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        (
            candidates.select("NOMENCLATURE_ID", "SOURCE_CHANGE_TS")
            .withColumn("RUN_ID", F.lit(RUN_ID))
            .withColumn("STATUS", F.lit("STARTED"))
            .withColumn("CREATED_AT", F.lit(now).cast(TimestampType()))
            .withColumn("UPDATED_AT", F.lit(now).cast(TimestampType()))
            .select(
                *[
                    F.col(field.name).cast(field.dataType).alias(field.name)
                    for field in run_item_schema.fields
                ]
            )
            .write.format("delta").mode("append").saveAsTable(RUN_ITEMS_TABLE)
        )
    else:
        result = {"run_id": RUN_ID, "status": "NO_CHANGES", "candidate_count": 0}
        return result


    valid_concepts = spark.table(CONCEPT_TABLE).filter(
        F.col("concept_id").isNotNull() & F.col("invalid_reason").isNull()
    ).select(
        "concept_id",
        "concept_code",
        "concept_name",
        "vocabulary_id",
        "standard_concept",
        "domain_id",
        "concept_class_id",
    )
    valid_maps = spark.table(RELATIONSHIP_TABLE).filter(
        (F.col("relationship_id") == "Maps to") & F.col("invalid_reason").isNull()
    ).select("concept_id_1", "concept_id_2").distinct()

    vocab_code = try_cast_column("SOURCE_VOCABULARY_CD", LongType())
    base = (
        candidates.withColumn("IS_SNOMED", vocab_code.isin(SNOMED_VOCAB_CODES))
        .withColumn("IS_MULTUM", vocab_code.isin(MULTUM_VOCAB_CODES))
        .withColumn("IS_ICD10", vocab_code.isin(ICD10_VOCAB_CODES))
        .withColumn("IS_OPCS4", vocab_code.isin(OPCS4_VOCAB_CODES))
        .withColumn(
            "VOCABULARY_TYPE",
            F.when(F.col("IS_SNOMED"), "SNOMED")
            .when(F.col("IS_MULTUM"), "MULTUM")
            .when(F.col("IS_ICD10"), "ICD10")
            .when(F.col("IS_OPCS4"), "OPCS4")
            .otherwise("OTHER"),
        )
        .withColumn(
            "FOUND_CUI",
            F.when(
                F.col("IS_SNOMED"),
                F.when(
                    F.col("CONCEPT_CKI").rlike("(?i)^SNOMED!"),
                    F.regexp_extract(F.col("CONCEPT_CKI"), "(?i)^SNOMED!(.*)$", 1),
                ).otherwise(F.col("SOURCE_IDENTIFIER")),
            )
            .when(F.col("IS_MULTUM"), normalize_code(F.col("SOURCE_IDENTIFIER"), "MULTUM"))
            .when(F.col("IS_ICD10"), normalize_code(F.col("SOURCE_IDENTIFIER"), "ICD10"))
            .when(F.col("IS_OPCS4"), normalize_code(F.col("SOURCE_IDENTIFIER"), "OPCS4"))
            .otherwise(F.trim(F.col("SOURCE_IDENTIFIER"))),
        )
    )


    def initial_mapping(vocabulary_type: str, vocabularies, code_type: str) -> DataFrame:
        subset = base.filter(F.col("VOCABULARY_TYPE") == vocabulary_type)
        keys = subset.select(normalize_code(F.col("FOUND_CUI"), code_type).alias("MATCH_CODE")).distinct()
        concepts = (
            valid_concepts.filter(F.col("vocabulary_id").isin(vocabularies))
            .withColumn("MATCH_CODE", normalize_code(F.col("concept_code"), code_type))
            .join(F.broadcast(keys), "MATCH_CODE", "left_semi")
        )
        joined = subset.alias("b").join(
            concepts.alias("c"),
            normalize_code(F.col("b.FOUND_CUI"), code_type) == F.col("c.MATCH_CODE"),
            "left",
        )
        counts = joined.groupBy(
            F.col("b.NOMENCLATURE_ID").alias("COUNT_NOMENCLATURE_ID")
        ).agg(
            F.countDistinct("c.concept_id").cast(LongType()).alias("NUMBER_OF_OMOP_MATCHES")
        )
        rank = Window.partitionBy("b.NOMENCLATURE_ID").orderBy(
            F.when(F.col("c.standard_concept") == "S", 1).otherwise(0).desc(),
            F.col("c.concept_id").asc_nulls_last(),
        )
        best = joined.withColumn("_rank", F.row_number().over(rank)).filter(F.col("_rank") == 1)
        return best.join(
            counts,
            F.col("b.NOMENCLATURE_ID") == F.col("COUNT_NOMENCLATURE_ID"),
            "left",
        ).select(
            F.col("b.NOMENCLATURE_ID").alias("NOMENCLATURE_ID"),
            F.col("c.concept_id").cast(IntegerType()).alias("OMOP_CONCEPT_ID"),
            F.col("c.concept_name").alias("OMOP_CONCEPT_NAME"),
            F.col("c.standard_concept").alias("IS_STANDARD_OMOP_CONCEPT"),
            F.col("c.domain_id").alias("CONCEPT_DOMAIN"),
            F.col("c.concept_class_id").alias("CONCEPT_CLASS"),
            F.coalesce(counts.NUMBER_OF_OMOP_MATCHES, F.lit(0).cast(LongType())).alias(
                "NUMBER_OF_OMOP_MATCHES"
            ),
        )


    initial_maps = reduce(
        lambda left, right: left.unionByName(right),
        [
            initial_mapping("SNOMED", ["SNOMED"], "SNOMED"),
            initial_mapping("MULTUM", ["Multum"], "MULTUM"),
            initial_mapping("ICD10", ["ICD10", "ICD10CM"], "ICD10"),
            initial_mapping("OPCS4", ["OPCS4"], "OPCS4"),
        ],
    )

    stage_one = (
        base.join(initial_maps, "NOMENCLATURE_ID", "left")
        .fillna(0, subset=["NUMBER_OF_OMOP_MATCHES"])
        .select(
            *base_columns,
            "OMOP_CONCEPT_ID",
            "OMOP_CONCEPT_NAME",
            "IS_STANDARD_OMOP_CONCEPT",
            "CONCEPT_DOMAIN",
            "CONCEPT_CLASS",
            "FOUND_CUI",
            "NUMBER_OF_OMOP_MATCHES",
        )
    )
    assert_unique(stage_one, "NOMENCLATURE_ID", "initial mapping")
    merge_rows(stage_one, TEMP_ONE)


    run_ids = spark.table(RUN_ITEMS_TABLE).filter(F.col("RUN_ID") == RUN_ID).select("NOMENCLATURE_ID")
    stage_one_current = spark.table(TEMP_ONE).join(F.broadcast(run_ids), "NOMENCLATURE_ID", "inner")

    icd_map = spark.table(ICD_MAP_TABLE).select(
        normalize_code(F.col("SCUI"), "ICD10").alias("SCUI_NORM"),
        F.col("TCUI").cast(StringType()).alias("TCUI"),
    )
    opcs_map = spark.table(OPCS_MAP_TABLE).select(
        normalize_code(F.col("SCUI"), "OPCS4").alias("SCUI_NORM"),
        F.col("TCUI").cast(StringType()).alias("TCUI"),
    )

    native_snomed = candidate_frame(
        stage_one_current.filter(
            try_cast_column("SOURCE_VOCABULARY_CD", LongType()).isin(SNOMED_VOCAB_CODES)
            | F.col("CONCEPT_CKI").rlike("(?i)^SNOMED!")
        ),
        F.coalesce(
            F.when(
                F.col("CONCEPT_CKI").rlike("(?i)^SNOMED!"),
                F.regexp_extract(F.col("CONCEPT_CKI"), "(?i)^SNOMED!(.*)$", 1),
            ),
            F.col("FOUND_CUI"),
        ),
        "NATIVE",
        1,
    )
    direct_snomed_icd = candidate_frame(
        stage_one_current.filter(
            try_cast_column("SOURCE_VOCABULARY_CD", LongType()).isin(ICD10_VOCAB_CODES)
        ).join(
            F.broadcast(icd_map),
            normalize_code(F.col("FOUND_CUI"), "ICD10") == F.col("SCUI_NORM"),
            "inner",
        ),
        F.col("TCUI"),
        "EXACT",
        2,
    )
    direct_snomed_opcs = candidate_frame(
        stage_one_current.filter(
            try_cast_column("SOURCE_VOCABULARY_CD", LongType()).isin(OPCS4_VOCAB_CODES)
        ).join(
            F.broadcast(opcs_map),
            normalize_code(F.col("FOUND_CUI"), "OPCS4") == F.col("SCUI_NORM"),
            "inner",
        ),
        F.col("TCUI"),
        "EXACT",
        2,
    )

    prefix_frames = []
    if ENABLE_PREFIX_FALLBACK:
        prefix_frames.extend(
            [
                candidate_frame(
                    stage_one_current.filter(
                        try_cast_column("SOURCE_VOCABULARY_CD", LongType()).isin(ICD10_VOCAB_CODES)
                    ).join(
                        F.broadcast(icd_map.withColumn("PREFIX3", F.substring("SCUI_NORM", 1, 3))),
                        F.substring(normalize_code(F.col("FOUND_CUI"), "ICD10"), 1, 3)
                        == F.col("PREFIX3"),
                        "inner",
                    ),
                    F.col("TCUI"),
                    "PREFIX",
                    6,
                ),
                candidate_frame(
                    stage_one_current.filter(
                        try_cast_column("SOURCE_VOCABULARY_CD", LongType()).isin(OPCS4_VOCAB_CODES)
                    ).join(
                        F.broadcast(opcs_map.withColumn("PREFIX3", F.substring("SCUI_NORM", 1, 3))),
                        F.substring(normalize_code(F.col("FOUND_CUI"), "OPCS4"), 1, 3)
                        == F.col("PREFIX3"),
                        "inner",
                    ),
                    F.col("TCUI"),
                    "PREFIX",
                    6,
                ),
            ]
        )

    snomed_concepts = valid_concepts.filter(F.col("vocabulary_id") == "SNOMED")
    self_snomed = candidate_frame(
        stage_one_current.join(
            snomed_concepts,
            stage_one_current.OMOP_CONCEPT_ID == snomed_concepts.concept_id,
            "inner",
        ),
        F.col("concept_code"),
        "OMOP_DERIVED",
        4,
    )
    mapped_snomed = candidate_frame(
        stage_one_current.filter(F.col("OMOP_CONCEPT_ID").isNotNull())
        .join(valid_maps, F.col("OMOP_CONCEPT_ID") == F.col("concept_id_1"), "inner")
        .join(snomed_concepts, F.col("concept_id_2") == F.col("concept_id"), "inner"),
        F.col("concept_code"),
        "OMOP_DERIVED",
        4,
    )
    reverse_snomed = candidate_frame(
        stage_one_current.filter(F.col("OMOP_CONCEPT_ID").isNotNull())
        .join(valid_maps, F.col("OMOP_CONCEPT_ID") == F.col("concept_id_2"), "inner")
        .join(snomed_concepts, F.col("concept_id_1") == F.col("concept_id"), "inner"),
        F.col("concept_code"),
        "OMOP_DERIVED",
        4,
    )

    snomed_candidates = union_candidates(
        native_snomed,
        direct_snomed_icd,
        direct_snomed_opcs,
        self_snomed,
        mapped_snomed,
        reverse_snomed,
        *prefix_frames,
    ).filter(try_cast_column("CANDIDATE_CODE", LongType()).isNotNull())
    snomed_resolved = resolve_candidates(
        snomed_candidates,
        ["SNOMED"],
        "SNOMED",
        "3_lookup.trud.snomed_sct",
        "SNOMED_CODE",
        "SNOMED_TYPE",
        "SNOMED_MATCH_COUNT",
        "SNOMED_TERM",
        LongType(),
        valid_concepts,
        valid_maps,
    )
    stage_two = stage_one_current.join(snomed_resolved, "NOMENCLATURE_ID", "left")
    assert_unique(stage_two, "NOMENCLATURE_ID", "SNOMED mapping")
    merge_rows(stage_two, TEMP_TWO)


    stage_two_current = spark.table(TEMP_TWO).join(F.broadcast(run_ids), "NOMENCLATURE_ID", "inner")
    hierarchy = spark.table(SNOMED_HIER_TABLE).select(
        F.col("PARENT").cast(StringType()).alias("PARENT"),
        F.col("CHILD").cast(StringType()).alias("CHILD"),
    ).distinct()

    native_icd = candidate_frame(
        stage_two_current.filter(
            try_cast_column("SOURCE_VOCABULARY_CD", LongType()).isin(ICD10_VOCAB_CODES)
            | F.col("CONCEPT_CKI").rlike("(?i)^ICD10WHO!")
        ),
        F.coalesce(
            F.when(
                F.col("CONCEPT_CKI").rlike("(?i)^ICD10WHO!"),
                F.regexp_extract(F.col("CONCEPT_CKI"), "(?i)^ICD10WHO!(.*)$", 1),
            ),
            F.col("FOUND_CUI"),
        ),
        "NATIVE",
        1,
    )
    direct_icd = candidate_frame(
        stage_two_current.filter(F.col("SNOMED_CODE").isNotNull()).join(
            F.broadcast(icd_map),
            F.col("SNOMED_CODE").cast(StringType()) == F.col("TCUI"),
            "inner",
        ),
        F.col("SCUI_NORM"),
        "EXACT",
        2,
    )
    child_icd = candidate_frame(
        stage_two_current.filter(F.col("SNOMED_CODE").isNotNull())
        .join(
            hierarchy,
            F.col("SNOMED_CODE").cast(StringType()) == F.col("CHILD"),
            "inner",
        )
        .join(F.broadcast(icd_map), F.col("PARENT") == F.col("TCUI"), "inner"),
        F.col("SCUI_NORM"),
        "CHILDMATCH",
        3,
    )
    parent_icd = candidate_frame(
        stage_two_current.filter(F.col("SNOMED_CODE").isNotNull())
        .join(
            hierarchy,
            F.col("SNOMED_CODE").cast(StringType()) == F.col("PARENT"),
            "inner",
        )
        .join(F.broadcast(icd_map), F.col("CHILD") == F.col("TCUI"), "inner"),
        F.col("SCUI_NORM"),
        "PARENTMATCH",
        4,
    )
    icd_concepts = valid_concepts.filter(F.col("vocabulary_id").isin("ICD10", "ICD10CM"))
    self_icd = candidate_frame(
        stage_two_current.join(
            icd_concepts,
            stage_two_current.OMOP_CONCEPT_ID == icd_concepts.concept_id,
            "inner",
        ),
        F.col("concept_code"),
        "OMOP_DERIVED",
        5,
    )
    reverse_icd = candidate_frame(
        stage_two_current.filter(F.col("OMOP_CONCEPT_ID").isNotNull())
        .join(valid_maps, F.col("OMOP_CONCEPT_ID") == F.col("concept_id_2"), "inner")
        .join(icd_concepts, F.col("concept_id_1") == F.col("concept_id"), "inner"),
        F.col("concept_code"),
        "OMOP_DERIVED",
        5,
    )
    icd_resolved = resolve_candidates(
        union_candidates(native_icd, direct_icd, child_icd, parent_icd, self_icd, reverse_icd),
        ["ICD10", "ICD10CM"],
        "ICD10",
        "3_lookup.trud.maps_icd",
        "ICD10_CODE",
        "ICD10_CODE_TYPE",
        "ICD10_CODE_MATCH_COUNT",
        "ICD10_TERM",
        StringType(),
        valid_concepts,
        valid_maps,
    )
    stage_three = stage_two_current.join(icd_resolved, "NOMENCLATURE_ID", "left")
    assert_unique(stage_three, "NOMENCLATURE_ID", "ICD10 mapping")
    merge_rows(stage_three, TEMP_THREE)


    stage_three_current = spark.table(TEMP_THREE).join(F.broadcast(run_ids), "NOMENCLATURE_ID", "inner")

    native_opcs = candidate_frame(
        stage_three_current.filter(
            try_cast_column("SOURCE_VOCABULARY_CD", LongType()).isin(OPCS4_VOCAB_CODES)
            | F.col("CONCEPT_CKI").rlike("(?i)^OPCS4!")
        ),
        F.coalesce(
            F.when(
                F.col("CONCEPT_CKI").rlike("(?i)^OPCS4!"),
                F.regexp_extract(F.col("CONCEPT_CKI"), "(?i)^OPCS4!(.*)$", 1),
            ),
            F.col("FOUND_CUI"),
        ),
        "NATIVE",
        1,
    )
    direct_opcs = candidate_frame(
        stage_three_current.filter(F.col("SNOMED_CODE").isNotNull()).join(
            F.broadcast(opcs_map),
            F.col("SNOMED_CODE").cast(StringType()) == F.col("TCUI"),
            "inner",
        ),
        F.col("SCUI_NORM"),
        "EXACT",
        2,
    )
    child_opcs = candidate_frame(
        stage_three_current.filter(F.col("SNOMED_CODE").isNotNull())
        .join(
            hierarchy,
            F.col("SNOMED_CODE").cast(StringType()) == F.col("CHILD"),
            "inner",
        )
        .join(F.broadcast(opcs_map), F.col("PARENT") == F.col("TCUI"), "inner"),
        F.col("SCUI_NORM"),
        "CHILDMATCH",
        3,
    )
    parent_opcs = candidate_frame(
        stage_three_current.filter(F.col("SNOMED_CODE").isNotNull())
        .join(
            hierarchy,
            F.col("SNOMED_CODE").cast(StringType()) == F.col("PARENT"),
            "inner",
        )
        .join(F.broadcast(opcs_map), F.col("CHILD") == F.col("TCUI"), "inner"),
        F.col("SCUI_NORM"),
        "PARENTMATCH",
        4,
    )
    opcs_concepts = valid_concepts.filter(F.col("vocabulary_id") == "OPCS4")
    self_opcs = candidate_frame(
        stage_three_current.join(
            opcs_concepts,
            stage_three_current.OMOP_CONCEPT_ID == opcs_concepts.concept_id,
            "inner",
        ),
        F.col("concept_code"),
        "OMOP_DERIVED",
        5,
    )
    reverse_opcs = candidate_frame(
        stage_three_current.filter(F.col("OMOP_CONCEPT_ID").isNotNull())
        .join(valid_maps, F.col("OMOP_CONCEPT_ID") == F.col("concept_id_2"), "inner")
        .join(opcs_concepts, F.col("concept_id_1") == F.col("concept_id"), "inner"),
        F.col("concept_code"),
        "OMOP_DERIVED",
        5,
    )
    opcs_resolved = resolve_candidates(
        union_candidates(native_opcs, direct_opcs, child_opcs, parent_opcs, self_opcs, reverse_opcs),
        ["OPCS4"],
        "OPCS4",
        "3_lookup.trud.maps_opcs",
        "OPCS4_CODE",
        "OPCS4_CODE_TYPE",
        "OPCS4_CODE_MATCH_COUNT",
        "OPCS4_TERM",
        StringType(),
        valid_concepts,
        valid_maps,
    )
    stage_four_pre = stage_three_current.join(opcs_resolved, "NOMENCLATURE_ID", "left")


    def tertiary_candidates(code_type: str, code_column: str, vocabularies, priority: int) -> DataFrame:
        source_rows = stage_four_pre.filter(
            F.col("OMOP_CONCEPT_ID").isNull() & F.col(code_column).isNotNull()
        ).select("NOMENCLATURE_ID", code_column)
        keys = source_rows.select(normalize_code(F.col(code_column), code_type).alias("SOURCE_CODE")).distinct()
        source_concepts = (
            valid_concepts.filter(F.col("vocabulary_id").isin(vocabularies))
            .withColumn("SOURCE_CODE", normalize_code(F.col("concept_code"), code_type))
            .join(F.broadcast(keys), "SOURCE_CODE", "left_semi")
        )
        matched_sources = source_rows.join(
            source_concepts,
            normalize_code(F.col(code_column), code_type) == F.col("SOURCE_CODE"),
            "inner",
        )

        self_targets = matched_sources.filter(F.col("standard_concept") == "S").select(
            "NOMENCLATURE_ID",
            F.col("concept_id").alias("TARGET_ID"),
            F.col("concept_name").alias("TARGET_NAME"),
            F.col("standard_concept").alias("TARGET_STANDARD"),
            F.col("domain_id").alias("TARGET_DOMAIN"),
            F.col("concept_class_id").alias("TARGET_CLASS"),
            F.lit(code_type).alias("SOURCE_TYPE"),
            F.lit(priority).alias("SOURCE_PRIORITY"),
        )
        mapped_targets = (
            matched_sources.join(valid_maps, F.col("concept_id") == F.col("concept_id_1"), "inner")
            .join(
                valid_concepts.select(
                    F.col("concept_id").alias("TARGET_ID"),
                    F.col("concept_name").alias("TARGET_NAME"),
                    F.col("standard_concept").alias("TARGET_STANDARD"),
                    F.col("domain_id").alias("TARGET_DOMAIN"),
                    F.col("concept_class_id").alias("TARGET_CLASS"),
                ),
                F.col("concept_id_2") == F.col("TARGET_ID"),
                "inner",
            )
            .select(
                "NOMENCLATURE_ID",
                "TARGET_ID",
                "TARGET_NAME",
                "TARGET_STANDARD",
                "TARGET_DOMAIN",
                "TARGET_CLASS",
                F.lit(code_type).alias("SOURCE_TYPE"),
                F.lit(priority).alias("SOURCE_PRIORITY"),
            )
        )
        return self_targets.unionByName(mapped_targets).dropDuplicates(
            ["NOMENCLATURE_ID", "TARGET_ID", "SOURCE_TYPE"]
        )


    tertiary = reduce(
        lambda left, right: left.unionByName(right),
        [
            tertiary_candidates("SNOMED", "SNOMED_CODE", ["SNOMED"], 1),
            tertiary_candidates("ICD10", "ICD10_CODE", ["ICD10", "ICD10CM"], 2),
            tertiary_candidates("OPCS4", "OPCS4_CODE", ["OPCS4"], 3),
        ],
    )

    preferred_domain = (
        F.when(
            (F.col("SOURCE_TYPE") == "SNOMED")
            & F.col("TARGET_DOMAIN").isin("Condition", "Procedure", "Observation", "Measurement"),
            1,
        )
        .when((F.col("SOURCE_TYPE") == "ICD10") & (F.col("TARGET_DOMAIN") == "Condition"), 1)
        .when((F.col("SOURCE_TYPE") == "OPCS4") & (F.col("TARGET_DOMAIN") == "Procedure"), 1)
        .otherwise(0)
    )
    tertiary_counts = tertiary.groupBy("NOMENCLATURE_ID").agg(
        F.countDistinct("TARGET_ID").cast(LongType()).alias("TERTIARY_MATCH_COUNT")
    )
    tertiary_rank = Window.partitionBy("NOMENCLATURE_ID").orderBy(
        F.col("SOURCE_PRIORITY").asc(),
        F.when(F.col("TARGET_STANDARD") == "S", 1).otherwise(0).desc(),
        preferred_domain.desc(),
        F.col("TARGET_CLASS").asc_nulls_last(),
        F.col("TARGET_ID").asc(),
    )
    tertiary_best = (
        tertiary.withColumn("_rank", F.row_number().over(tertiary_rank))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .join(tertiary_counts, "NOMENCLATURE_ID", "inner")
    )

    existing_columns = [
        column
        for column in stage_four_pre.columns
        if column
        not in [
            "OMOP_CONCEPT_ID",
            "OMOP_CONCEPT_NAME",
            "IS_STANDARD_OMOP_CONCEPT",
            "CONCEPT_DOMAIN",
            "CONCEPT_CLASS",
            "NUMBER_OF_OMOP_MATCHES",
        ]
    ]
    stage_four = stage_four_pre.alias("b").join(
        tertiary_best.alias("n"), "NOMENCLATURE_ID", "left"
    ).select(
        *[F.col(f"b.`{column}`") for column in existing_columns],
        F.coalesce(F.col("b.OMOP_CONCEPT_ID"), F.col("n.TARGET_ID")).cast(IntegerType()).alias(
            "OMOP_CONCEPT_ID"
        ),
        F.coalesce(F.col("b.OMOP_CONCEPT_NAME"), F.col("n.TARGET_NAME")).alias("OMOP_CONCEPT_NAME"),
        F.coalesce(F.col("b.IS_STANDARD_OMOP_CONCEPT"), F.col("n.TARGET_STANDARD")).alias(
            "IS_STANDARD_OMOP_CONCEPT"
        ),
        F.coalesce(F.col("b.CONCEPT_DOMAIN"), F.col("n.TARGET_DOMAIN")).alias("CONCEPT_DOMAIN"),
        F.coalesce(F.col("b.CONCEPT_CLASS"), F.col("n.TARGET_CLASS")).alias("CONCEPT_CLASS"),
        F.when(F.col("n.TARGET_ID").isNotNull(), F.col("n.TERTIARY_MATCH_COUNT"))
        .otherwise(F.col("b.NUMBER_OF_OMOP_MATCHES"))
        .cast(LongType())
        .alias("NUMBER_OF_OMOP_MATCHES"),
    )
    assert_unique(stage_four, "NOMENCLATURE_ID", "final mapping stage")
    merge_rows(stage_four, TEMP_FOUR)

    DeltaTable.forName(spark, RUN_ITEMS_TABLE).update(
        condition=F.col("RUN_ID") == RUN_ID,
        set={"STATUS": F.lit("MAPPED"), "UPDATED_AT": F.current_timestamp()},
    )

    result = {
        "run_id": RUN_ID,
        "status": "MAPPED",
        "candidate_count": candidate_count,
        "force_full": FORCE_FULL,
        "lookback_days": LOOKBACK_DAYS,
        "prefix_fallback_enabled": ENABLE_PREFIX_FALLBACK,
    }
    print(json.dumps(result, default=str))
    return result


# COMMAND ----------

def run_embedding_section():
    import json
    import random
    import time
    import uuid
    from datetime import datetime, timezone
    from functools import reduce

    import requests
    from delta.tables import DeltaTable
    from pyspark.sql import DataFrame, functions as F
    from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType, TimestampType
    from pyspark.sql.window import Window



    RUN_ID = PIPELINE_RUN_ID
    FORCE_FULL = dbutils.widgets.get("force_full").lower() == "true"
    INCLUDE_ALL_NOMENCLATURE = (
        dbutils.widgets.get("include_all_nomenclature").lower() == "true"
        or FORCE_FULL
    )
    ALLOW_FULL_NOMENCLATURE_EMBEDDING_BACKFILL = (
        dbutils.widgets.get("allow_full_nomenclature_embedding_backfill").lower() == "true"
    )
    REFRESH_MODEL_MISMATCH = dbutils.widgets.get("refresh_model_mismatch").lower() == "true"
    PRUNE_STALE = dbutils.widgets.get("prune_stale").lower() == "true"
    EMBEDDING_ENDPOINT = dbutils.widgets.get("embedding_endpoint").strip()
    MODEL_VERSION = dbutils.widgets.get("embedding_model_version").strip()
    BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
    EXPECTED_DIMENSION = int(dbutils.widgets.get("expected_dimension"))

    RUN_ITEMS_TABLE = "4_prod.tmp.nomenclature_pipeline_run_items"
    NOMENCLATURE_STAGE = "4_prod.tmp.tempfour_nomenclature"

    TARGETS = {
        "nomenclature_terms": "3_lookup.embeddings.terms",
        "barts_ingredients": "3_lookup.embeddings.barts_ingredients",
        "omop_ingredients": "3_lookup.embeddings.omop_ingredients",
        "code_value_descriptions": "3_lookup.embeddings.code_value_descriptions",
    }


    def table_exists(table_name: str) -> bool:
        return spark.catalog.tableExists(table_name)


    def normalize_term(column):
        return F.initcap(F.lower(F.trim(column.cast(StringType()))))


    def ensure_embedding_table(table_name: str) -> None:
        schema = StructType(
            [
                StructField("term", StringType(), False),
                StructField("embedding_vector", ArrayType(FloatType()), True),
                StructField("model_version", StringType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("embedded_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
                StructField("ADC_UPDT", TimestampType(), True),
            ]
        )
        if not table_exists(table_name):
            spark.createDataFrame([], schema).write.format("delta").saveAsTable(table_name)
            return

        existing = {field.name.lower() for field in spark.table(table_name).schema.fields}
        for field in schema.fields:
            if field.name.lower() not in existing:
                spark.sql(
                    f"ALTER TABLE {table_name} ADD COLUMNS (`{field.name}` {field.dataType.simpleString()})"
                )


    def endpoint_context():
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        return context.apiUrl().get().rstrip("/"), context.apiToken().get()


    API_URL, API_TOKEN = endpoint_context()
    INVOCATION_URL = f"{API_URL}/serving-endpoints/{EMBEDDING_ENDPOINT}/invocations"


    def request_embeddings(terms, max_attempts: int = 6):
        payload = {"input": terms}
        headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
        last_error = None
        for attempt in range(max_attempts):
            try:
                response = requests.post(INVOCATION_URL, headers=headers, json=payload, timeout=180)
                if response.status_code == 429 or response.status_code >= 500:
                    raise RuntimeError(f"Transient endpoint response {response.status_code}: {response.text[:500]}")
                response.raise_for_status()
                body = response.json()
                rows = body.get("data") or body.get("predictions")
                if not isinstance(rows, list):
                    raise RuntimeError(f"Unexpected embedding response: {str(body)[:1000]}")
                if rows and isinstance(rows[0], dict) and "embedding" in rows[0]:
                    rows = sorted(rows, key=lambda row: row.get("index", 0))
                    vectors = [row["embedding"] for row in rows]
                else:
                    vectors = rows
                if len(vectors) != len(terms):
                    raise RuntimeError(f"Endpoint returned {len(vectors)} vectors for {len(terms)} terms")
                dimensions = {len(vector) for vector in vectors}
                if dimensions != {EXPECTED_DIMENSION}:
                    raise RuntimeError(f"Unexpected embedding dimensions: {sorted(dimensions)}")
                return vectors
            except Exception as exc:
                last_error = exc
                if attempt + 1 == max_attempts:
                    break
                delay = min(60.0, (2 ** attempt) + random.random())
                print(f"Embedding request failed (attempt {attempt + 1}/{max_attempts}); retrying in {delay:.1f}s: {exc}")
                time.sleep(delay)
        raise RuntimeError(f"Embedding endpoint failed after {max_attempts} attempts: {last_error}")


    def merge_term_keys(source_terms: DataFrame, target_table: str) -> None:
        now = F.current_timestamp()
        source = (
            source_terms.select(normalize_term(F.col("term")).alias("term"))
            .filter(F.col("term").isNotNull() & (F.col("term") != ""))
            .distinct()
            .withColumn("created_at", now)
            .withColumn("updated_at", now)
            .withColumn("ADC_UPDT", now)
        )
        DeltaTable.forName(spark, target_table).alias("t").merge(
            source.alias("s"), "t.term = s.term"
        ).whenNotMatchedInsert(
            values={
                "term": "s.term",
                "created_at": "s.created_at",
                "updated_at": "s.updated_at",
                "ADC_UPDT": "s.ADC_UPDT",
            }
        ).execute()


    def write_embedding_batch(target_table: str, batch_terms, vectors) -> None:
        timestamp = datetime.now(timezone.utc).replace(tzinfo=None)
        schema = StructType(
            [
                StructField("term", StringType(), False),
                StructField("embedding_vector", ArrayType(FloatType()), False),
                StructField("model_version", StringType(), False),
                StructField("embedded_at", TimestampType(), False),
                StructField("updated_at", TimestampType(), False),
                StructField("ADC_UPDT", TimestampType(), False),
            ]
        )
        rows = [
            (
                term,
                [float(value) for value in vector],
                MODEL_VERSION,
                timestamp,
                timestamp,
                timestamp,
            )
            for term, vector in zip(batch_terms, vectors)
        ]
        batch = spark.createDataFrame(rows, schema)
        DeltaTable.forName(spark, target_table).alias("t").merge(
            batch.alias("s"), "t.term = s.term"
        ).whenMatchedUpdate(
            set={
                "embedding_vector": "s.embedding_vector",
                "model_version": "s.model_version",
                "embedded_at": "s.embedded_at",
                "updated_at": "s.updated_at",
                "ADC_UPDT": "s.ADC_UPDT",
            }
        ).whenNotMatchedInsert(
            values={
                "term": "s.term",
                "embedding_vector": "s.embedding_vector",
                "model_version": "s.model_version",
                "created_at": "s.embedded_at",
                "embedded_at": "s.embedded_at",
                "updated_at": "s.updated_at",
                "ADC_UPDT": "s.ADC_UPDT",
            }
        ).execute()


    def sync_embeddings(
        label: str,
        source_terms: DataFrame,
        target_table: str,
        allow_prune: bool = False,
        generate_missing: bool = True,
    ):
        ensure_embedding_table(target_table)
        source = (
            source_terms.select(normalize_term(F.col("term")).alias("term"))
            .filter(F.col("term").isNotNull() & (F.col("term") != ""))
            .distinct()
        )
        source_count = source.count()
        print(f"{label}: {source_count} required terms")
        if not generate_missing:
            print(
                f"{label}: missing-vector generation skipped; set "
                "allow_full_nomenclature_embedding_backfill=true for an explicit full backfill"
            )
            return {
                "required": source_count,
                "embedded": 0,
                "unresolved": None,
                "generation_skipped": True,
            }

        existing_terms = (
            spark.table(target_table)
            .select(normalize_term(F.col("term")).alias("term"))
            .filter(F.col("term").isNotNull() & (F.col("term") != ""))
            .distinct()
        )
        new_terms = source.join(existing_terms, "term", "left_anti")
        new_term_count = new_terms.count()
        if new_term_count:
            print(f"{label}: adding {new_term_count} new term keys")
            merge_term_keys(new_terms, target_table)
        else:
            print(f"{label}: no new term keys")

        target = spark.table(target_table).select("term", "embedding_vector", "model_version")
        missing = target.join(source, "term", "left_semi").filter(
            F.col("embedding_vector").isNull()
            | (F.size("embedding_vector") == 0)
            | (F.lit(REFRESH_MODEL_MISMATCH) & ~F.col("model_version").eqNullSafe(F.lit(MODEL_VERSION)))
        ).select("term").distinct().orderBy("term")

        missing_count = missing.count()
        print(f"{label}: {missing_count} terms require embedding")
        batch_terms = []
        processed = 0
        for row in missing.toLocalIterator():
            batch_terms.append(row["term"])
            if len(batch_terms) >= BATCH_SIZE:
                vectors = request_embeddings(batch_terms)
                write_embedding_batch(target_table, batch_terms, vectors)
                processed += len(batch_terms)
                print(f"{label}: embedded {processed}/{missing_count}")
                batch_terms = []
        if batch_terms:
            vectors = request_embeddings(batch_terms)
            write_embedding_batch(target_table, batch_terms, vectors)
            processed += len(batch_terms)
            print(f"{label}: embedded {processed}/{missing_count}")

        unresolved = (
            spark.table(target_table)
            .join(source, "term", "left_semi")
            .filter(F.col("embedding_vector").isNull() | (F.size("embedding_vector") != EXPECTED_DIMENSION))
            .count()
        )
        if unresolved:
            raise RuntimeError(f"{label}: {unresolved} required terms still lack a valid embedding")

        if PRUNE_STALE and allow_prune:
            print(f"{label}: pruning terms not present in the authoritative source")
            DeltaTable.forName(spark, target_table).alias("t").merge(
                source.alias("s"), "t.term = s.term"
            ).whenNotMatchedBySourceDelete().execute()
        return {
            "required": source_count,
            "new_terms": new_term_count,
            "embedded": processed,
            "unresolved": unresolved,
        }


    if INCLUDE_ALL_NOMENCLATURE:
        nomenclature_source = spark.table(NOMENCLATURE_STAGE)
    else:
        run_ids = (
            spark.table(RUN_ITEMS_TABLE)
            .filter(F.col("RUN_ID") == RUN_ID)
            .select("NOMENCLATURE_ID")
        )
        nomenclature_source = spark.table(NOMENCLATURE_STAGE).join(
            F.broadcast(run_ids), "NOMENCLATURE_ID", "inner"
        )

    term_columns = ["SOURCE_STRING", "SNOMED_TERM", "ICD10_TERM", "OPCS4_TERM", "OMOP_CONCEPT_NAME"]
    nomenclature_terms = reduce(
        lambda left, right: left.unionByName(right),
        [nomenclature_source.select(F.col(column).alias("term")) for column in term_columns],
    ).filter(F.col("term").isNotNull() & (F.trim("term") != ""))


    catalog = spark.table("3_lookup.mill.mill_order_catalog").filter(
        F.col("CKI").like("MUL.ORD!d%")
    ).select("CATALOG_CD", "PRIMARY_MNEMONIC")
    synonyms = spark.table("3_lookup.mill.mill_order_catalog_synonym").select(
        "CATALOG_CD", "SYNONYM_ID", "MNEMONIC"
    )
    ingredient_stats = (
        spark.table("4_prod.raw.mill_order_ingredient")
        .filter(F.col("SYNONYM_ID").isNotNull())
        .select(
            "SYNONYM_ID",
            normalize_term(F.col("HNA_ORDER_MNEMONIC")).alias("INGREDIENT_TERM"),
            F.col("ADC_UPDT").alias("INGREDIENT_ADC_UPDT"),
        )
        .filter(F.col("INGREDIENT_TERM").isNotNull() & (F.col("INGREDIENT_TERM") != ""))
        .groupBy("SYNONYM_ID", "INGREDIENT_TERM")
        .agg(F.count(F.lit(1)).alias("TERM_FREQUENCY"), F.max("INGREDIENT_ADC_UPDT").alias("LAST_SEEN"))
    )
    ingredient_rank = Window.partitionBy("SYNONYM_ID").orderBy(
        F.col("TERM_FREQUENCY").desc(), F.col("LAST_SEEN").desc_nulls_last(), F.col("INGREDIENT_TERM").asc()
    )
    preferred_ingredient = (
        ingredient_stats.withColumn("_rank", F.row_number().over(ingredient_rank))
        .filter(F.col("_rank") == 1)
        .select("SYNONYM_ID", "INGREDIENT_TERM")
    )
    barts_terms = (
        catalog.join(synonyms, "CATALOG_CD", "inner")
        .join(preferred_ingredient, "SYNONYM_ID", "left")
        .select(
            F.coalesce(F.col("INGREDIENT_TERM"), F.col("MNEMONIC"), F.col("PRIMARY_MNEMONIC")).alias("term")
        )
    )

    omop_terms = (
        spark.table("3_lookup.omop.concept")
        .filter(
            F.col("invalid_reason").isNull()
            & (F.col("vocabulary_id") == "dm+d")
            & F.col("concept_class_id").isin("Ingredient", "Multiple Ingredients")
        )
        .select(F.col("concept_name").alias("term"))
    )

    code_value_terms = (
        spark.table("3_lookup.mill.mill_code_value")
        .filter(F.col("DESCRIPTION").isNotNull() & (F.trim("DESCRIPTION") != ""))
        .select(F.col("DESCRIPTION").alias("term"))
    )


    if PRUNE_STALE and not INCLUDE_ALL_NOMENCLATURE:
        raise ValueError("prune_stale=true requires include_all_nomenclature=true")

    results = {
        "nomenclature_terms": sync_embeddings(
            "nomenclature terms",
            nomenclature_terms,
            TARGETS["nomenclature_terms"],
            allow_prune=INCLUDE_ALL_NOMENCLATURE,
            generate_missing=(
                not FORCE_FULL or ALLOW_FULL_NOMENCLATURE_EMBEDDING_BACKFILL
            ),
        ),
        "barts_ingredients": sync_embeddings(
            "Barts ingredients", barts_terms, TARGETS["barts_ingredients"], allow_prune=True
        ),
        "omop_ingredients": sync_embeddings(
            "OMOP ingredients", omop_terms, TARGETS["omop_ingredients"], allow_prune=True
        ),
        "code_value_descriptions": sync_embeddings(
            "code value descriptions",
            code_value_terms,
            TARGETS["code_value_descriptions"],
            allow_prune=True,
        ),
    }

    result = {
        "run_id": RUN_ID,
        "status": "SUCCEEDED",
        "model_version": MODEL_VERSION,
        "endpoint": EMBEDDING_ENDPOINT,
        "tables": results,
    }
    print(json.dumps(result, default=str))
    return result


# COMMAND ----------

def run_finalize_section():
    import json
    import uuid
    from datetime import datetime, timezone
    from functools import reduce

    from delta.tables import DeltaTable
    from pyspark.sql import DataFrame, functions as F
    from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType
    from pyspark.sql.window import Window



    RUN_ID = PIPELINE_RUN_ID
    RUN_ITEMS_TABLE = "4_prod.tmp.nomenclature_pipeline_run_items"
    STATE_TABLE = "4_prod.tmp.nomenclature_pipeline_state"
    STAGE_TABLE = "4_prod.tmp.tempfour_nomenclature"
    EMBEDDINGS_TABLE = "3_lookup.embeddings.terms"
    PUBLISHED_TABLE = "3_lookup.mill.map_nomenclature"


    def table_exists(table_name: str) -> bool:
        return spark.catalog.tableExists(table_name)


    def ensure_columns(table_name: str, source_df: DataFrame) -> None:
        existing = {field.name.lower() for field in spark.table(table_name).schema.fields}
        for field in source_df.schema.fields:
            if field.name.lower() not in existing:
                spark.sql(
                    f"ALTER TABLE {table_name} ADD COLUMNS (`{field.name}` {field.dataType.simpleString()})"
                )


    def merge_rows(source_df: DataFrame, target_table: str) -> None:
        window = Window.partitionBy("NOMENCLATURE_ID").orderBy(
            F.col("SOURCE_CHANGE_TS").desc_nulls_last(), F.col("ADC_UPDT").desc_nulls_last()
        )
        source = (
            source_df.withColumn("_rank", F.row_number().over(window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )
        if not table_exists(target_table):
            source.write.format("delta").mode("overwrite").saveAsTable(target_table)
            return
        ensure_columns(target_table, source)
        updates = {column: f"s.`{column}`" for column in source.columns if column != "NOMENCLATURE_ID"}
        inserts = {column: f"s.`{column}`" for column in source.columns}
        (
            DeltaTable.forName(spark, target_table)
            .alias("t")
            .merge(source.alias("s"), "t.NOMENCLATURE_ID = s.NOMENCLATURE_ID")
            .whenMatchedUpdate(set=updates)
            .whenNotMatchedInsert(values=inserts)
            .execute()
        )


    def normalize_term(column):
        return F.initcap(F.lower(F.trim(column.cast(StringType()))))


    def cosine_similarity(left, right):
        zero = F.lit(0.0).cast("double")
        dot = F.aggregate(
            F.zip_with(left, right, lambda x, y: x.cast("double") * y.cast("double")),
            zero,
            lambda acc, value: acc + value,
        )
        left_norm = F.sqrt(
            F.aggregate(
                F.transform(left, lambda value: value.cast("double") * value.cast("double")),
                zero,
                lambda acc, value: acc + value,
            )
        )
        right_norm = F.sqrt(
            F.aggregate(
                F.transform(right, lambda value: value.cast("double") * value.cast("double")),
                zero,
                lambda acc, value: acc + value,
            )
        )
        return F.when(
            left.isNotNull()
            & right.isNotNull()
            & (F.size(left) == F.size(right))
            & (left_norm > 0)
            & (right_norm > 0),
            dot / (left_norm * right_norm),
        ).otherwise(F.lit(None).cast("double"))


    def assert_unique(df: DataFrame, key: str, label: str) -> None:
        if df.groupBy(key).count().filter(F.col("count") > 1).limit(1).count():
            raise RuntimeError(f"{label} contains duplicate {key} values")


    run_items = spark.table(RUN_ITEMS_TABLE).filter(F.col("RUN_ID") == RUN_ID).select(
        "NOMENCLATURE_ID", "SOURCE_CHANGE_TS"
    )
    run_count = run_items.count()
    if run_count == 0:
        result = {"run_id": RUN_ID, "status": "NO_CHANGES", "row_count": 0}
        return result

    base = spark.table(STAGE_TABLE).join(
        F.broadcast(run_items.select("NOMENCLATURE_ID")), "NOMENCLATURE_ID", "inner"
    )
    assert_unique(base, "NOMENCLATURE_ID", "finalization input")


    roles = {
        "source": "SOURCE_STRING",
        "snomed": "SNOMED_TERM",
        "icd10": "ICD10_TERM",
        "opcs4": "OPCS4_TERM",
        "omop": "OMOP_CONCEPT_NAME",
    }

    needed_terms = reduce(
        lambda left, right: left.unionByName(right),
        [
            base.select(normalize_term(F.col(column)).alias("term"))
            .filter(F.col("term").isNotNull() & (F.col("term") != ""))
            for column in roles.values()
        ],
    ).distinct()

    embedding_rank = Window.partitionBy("term").orderBy(
        F.when(F.col("embedding_vector").isNotNull(), 1).otherwise(0).desc(),
        F.col("embedded_at").desc_nulls_last(),
        F.col("ADC_UPDT").desc_nulls_last(),
        F.col("model_version").asc_nulls_last(),
    )
    embeddings = (
        spark.table(EMBEDDINGS_TABLE)
        .select("term", "embedding_vector", "model_version", "embedded_at", "ADC_UPDT")
        .join(needed_terms, "term", "left_semi")
        .withColumn("_rank", F.row_number().over(embedding_rank))
        .filter(F.col("_rank") == 1)
        .drop("_rank", "model_version", "embedded_at", "ADC_UPDT")
    )

    with_vectors = base
    for role, source_column in roles.items():
        role_vectors = (
            base.select(
                "NOMENCLATURE_ID", normalize_term(F.col(source_column)).alias("term")
            )
            .join(embeddings, "term", "left")
            .select(
                "NOMENCLATURE_ID", F.col("embedding_vector").alias(f"_{role}_embedding")
            )
        )
        with_vectors = with_vectors.join(role_vectors, "NOMENCLATURE_ID", "left")

    source_vec = F.col("_source_embedding")
    snomed_vec = F.col("_snomed_embedding")
    icd_vec = F.col("_icd10_embedding")
    opcs_vec = F.col("_opcs4_embedding")
    omop_vec = F.col("_omop_embedding")

    source_snomed = cosine_similarity(source_vec, snomed_vec)
    source_icd = cosine_similarity(source_vec, icd_vec)
    source_opcs = cosine_similarity(source_vec, opcs_vec)
    source_omop = cosine_similarity(source_vec, omop_vec)

    final_df = (
        with_vectors.withColumn("SNOMED_SIMILARITY", source_snomed)
        .withColumn("ICD10_SIMILARITY", source_icd)
        .withColumn("OPCS4_SIMILARITY", source_opcs)
        .withColumn("OMOP_SIMILARITY", source_omop)
        .withColumn("SIMILARITY_SOURCE_SNOMED", source_snomed)
        .withColumn("SIMILARITY_SOURCE_ICD10", source_icd)
        .withColumn("SIMILARITY_SOURCE_OPCS4", source_opcs)
        .withColumn("SIMILARITY_SOURCE_OMOP", source_omop)
        .withColumn("SIMILARITY_SNOMED_ICD10", cosine_similarity(snomed_vec, icd_vec))
        .withColumn("SIMILARITY_SNOMED_OPCS4", cosine_similarity(snomed_vec, opcs_vec))
        .withColumn("SIMILARITY_SNOMED_OMOP", cosine_similarity(snomed_vec, omop_vec))
        .withColumn("SIMILARITY_ICD10_OMOP", cosine_similarity(icd_vec, omop_vec))
        .withColumn("SIMILARITY_OPCS4_OMOP", cosine_similarity(opcs_vec, omop_vec))
        .drop(*[f"_{role}_embedding" for role in roles])
        .withColumn("NUMBER_OF_OMOP_MATCHES", F.col("NUMBER_OF_OMOP_MATCHES").cast(IntegerType()))
    )
    assert_unique(final_df, "NOMENCLATURE_ID", "finalized nomenclature")


    # Publish once to the governed lookup table. Bronze no longer carries a
    # duplicate nomenclature copy.
    merge_rows(final_df, PUBLISHED_TABLE)

    published_missing = (
        run_items.select("NOMENCLATURE_ID")
        .join(
            spark.table(PUBLISHED_TABLE).select("NOMENCLATURE_ID"),
            "NOMENCLATURE_ID",
            "left_anti",
        )
        .count()
    )
    if published_missing:
        raise RuntimeError(
            f"Nomenclature publication validation failed: missing={published_missing}"
        )

    state_schema = StructType(
        [
            StructField("PIPELINE_NAME", StringType(), False),
            StructField("LAST_SUCCESSFUL_SOURCE_CHANGE_TS", TimestampType(), False),
            StructField("LAST_RUN_ID", StringType(), False),
            StructField("LAST_ROW_COUNT", LongType(), False),
            StructField("UPDATED_AT", TimestampType(), False),
        ]
    )
    if not table_exists(STATE_TABLE):
        spark.createDataFrame([], state_schema).write.format("delta").saveAsTable(STATE_TABLE)

    max_source_change = run_items.agg(F.max("SOURCE_CHANGE_TS").alias("max_ts")).first()["max_ts"]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    state_df = spark.createDataFrame(
        [("nomenclature", max_source_change, RUN_ID, run_count, now)], state_schema
    )
    (
        DeltaTable.forName(spark, STATE_TABLE)
        .alias("t")
        .merge(state_df.alias("s"), "t.PIPELINE_NAME = s.PIPELINE_NAME")
        .whenMatchedUpdate(
            set={
                "LAST_SUCCESSFUL_SOURCE_CHANGE_TS": "greatest(t.LAST_SUCCESSFUL_SOURCE_CHANGE_TS, s.LAST_SUCCESSFUL_SOURCE_CHANGE_TS)",
                "LAST_RUN_ID": "s.LAST_RUN_ID",
                "LAST_ROW_COUNT": "s.LAST_ROW_COUNT",
                "UPDATED_AT": "s.UPDATED_AT",
            }
        )
        .whenNotMatchedInsert(
            values={
                "PIPELINE_NAME": "s.PIPELINE_NAME",
                "LAST_SUCCESSFUL_SOURCE_CHANGE_TS": "s.LAST_SUCCESSFUL_SOURCE_CHANGE_TS",
                "LAST_RUN_ID": "s.LAST_RUN_ID",
                "LAST_ROW_COUNT": "s.LAST_ROW_COUNT",
                "UPDATED_AT": "s.UPDATED_AT",
            }
        )
        .execute()
    )

    DeltaTable.forName(spark, RUN_ITEMS_TABLE).update(
        condition=F.col("RUN_ID") == RUN_ID,
        set={"STATUS": F.lit("COMPLETE"), "UPDATED_AT": F.current_timestamp()},
    )
    # Row-level manifests are operational scratch data, not permanent history.
    DeltaTable.forName(spark, RUN_ITEMS_TABLE).delete(
        (F.col("STATUS") == "COMPLETE")
        & (F.col("UPDATED_AT") < F.current_timestamp() - F.expr("INTERVAL 30 DAYS"))
    )
    result = {
        "run_id": RUN_ID,
        "status": "COMPLETE",
        "row_count": run_count,
        "watermark": str(max_source_change),
        "published_missing": published_missing,
    }
    print(json.dumps(result, default=str))
    return result


# COMMAND ----------

def run_medication_section():
    import hashlib
    import json
    import uuid
    from datetime import datetime, timezone

    import numpy as np
    from delta.tables import DeltaTable
    from pyspark.sql import DataFrame, functions as F
    from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType, TimestampType
    from pyspark.sql.window import Window



    RUN_ID = PIPELINE_RUN_ID
    FORCE_FULL = dbutils.widgets.get("force_full").lower() == "true"
    FULL_REPLACE = dbutils.widgets.get("full_replace").lower() == "true"
    SIMILARITY_THRESHOLD = float(dbutils.widgets.get("similarity_threshold"))
    MODEL_VERSION = dbutils.widgets.get("embedding_model_version").strip()
    VECTOR_MATCH_BATCH_SIZE = max(1, int(dbutils.widgets.get("vector_match_batch_size")))

    TARGET_TABLE = "3_lookup.mill.map_med_lookup"
    STATE_TABLE = "4_prod.tmp.medication_lookup_pipeline_state"

    REFERENCE_TABLES = [
        "3_lookup.rxnorm.rxnconso",
        "3_lookup.omop.concept",
        "3_lookup.omop.concept_relationship",
        "3_lookup.embeddings.barts_ingredients",
        "3_lookup.embeddings.omop_ingredients",
    ]


    def table_exists(table_name: str) -> bool:
        return spark.catalog.tableExists(table_name)


    def normalize_term(column):
        return F.initcap(F.lower(F.trim(column.cast(StringType()))))


    def current_table_version(table_name: str):
        row = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").select(
            "version", "timestamp"
        ).first()
        return {"table": table_name, "version": int(row["version"]), "timestamp": str(row["timestamp"])}


    reference_versions = [current_table_version(table) for table in REFERENCE_TABLES]
    REFERENCE_FINGERPRINT = hashlib.sha256(
        json.dumps(reference_versions, sort_keys=True).encode("utf-8")
    ).hexdigest()


    def ensure_columns(table_name: str, source_df: DataFrame) -> None:
        existing = {field.name.lower() for field in spark.table(table_name).schema.fields}
        for field in source_df.schema.fields:
            if field.name.lower() not in existing:
                spark.sql(
                    f"ALTER TABLE {table_name} ADD COLUMNS (`{field.name}` {field.dataType.simpleString()})"
                )


    def duplicate_id_count(table_name: str) -> int:
        if not table_exists(table_name):
            return 0
        return (
            spark.table(table_name)
            .groupBy("SYNONYM_ID")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )


    def assert_unique(df: DataFrame, key: str, label: str) -> None:
        if df.groupBy(key).count().filter(F.col("count") > 1).limit(1).count():
            raise RuntimeError(f"{label} contains duplicate {key} values")


    catalog = (
        spark.table("3_lookup.mill.mill_order_catalog")
        .filter(F.col("CKI").like("MUL.ORD!d%"))
        .select(
            "CATALOG_CD",
            "PRIMARY_MNEMONIC",
            "CKI",
            F.coalesce(F.greatest("UPDT_DT_TM", "LAST_UTC_TS"), F.col("ADC_UPDT")).alias(
                "CATALOG_CHANGE_TS"
            ),
            F.col("ADC_UPDT").alias("CATALOG_ADC_UPDT"),
        )
    )
    synonyms = spark.table("3_lookup.mill.mill_order_catalog_synonym").select(
        "CATALOG_CD",
        F.col("SYNONYM_ID").cast("long").alias("SYNONYM_ID"),
        "MNEMONIC",
        F.coalesce(F.greatest("UPDT_DT_TM", "LAST_UTC_TS"), F.col("ADC_UPDT")).alias(
            "SYNONYM_CHANGE_TS"
        ),
        F.col("ADC_UPDT").alias("SYNONYM_ADC_UPDT"),
    )

    ingredient_stats = (
        spark.table("4_prod.raw.mill_order_ingredient")
        .filter(F.col("SYNONYM_ID").isNotNull())
        .select(
            F.col("SYNONYM_ID").cast("long").alias("SYNONYM_ID"),
            normalize_term(F.col("HNA_ORDER_MNEMONIC")).alias("INGREDIENT_TERM"),
            F.coalesce(F.greatest("UPDT_DT_TM", "LAST_UTC_TS"), F.col("ADC_UPDT")).alias(
                "INGREDIENT_CHANGE_TS"
            ),
        )
        .filter(F.col("INGREDIENT_TERM").isNotNull() & (F.col("INGREDIENT_TERM") != ""))
        .groupBy("SYNONYM_ID", "INGREDIENT_TERM")
        .agg(
            F.count(F.lit(1)).alias("TERM_FREQUENCY"),
            F.max("INGREDIENT_CHANGE_TS").alias("INGREDIENT_CHANGE_TS"),
        )
    )
    ingredient_rank = Window.partitionBy("SYNONYM_ID").orderBy(
        F.col("TERM_FREQUENCY").desc(),
        F.col("INGREDIENT_CHANGE_TS").desc_nulls_last(),
        F.col("INGREDIENT_TERM").asc(),
    )
    preferred_ingredient = (
        ingredient_stats.withColumn("_rank", F.row_number().over(ingredient_rank))
        .filter(F.col("_rank") == 1)
        .drop("_rank", "TERM_FREQUENCY")
    )

    drug_universe = (
        synonyms.join(catalog, "CATALOG_CD", "inner")
        .join(preferred_ingredient, "SYNONYM_ID", "left")
        .withColumn(
            "VEC_SEARCH_TERM",
            normalize_term(
                F.coalesce(F.col("INGREDIENT_TERM"), F.col("MNEMONIC"), F.col("PRIMARY_MNEMONIC"))
            ),
        )
        .withColumn("MULTUM_CODE", F.substring_index(F.col("CKI"), "!", -1))
        .withColumn(
            "SOURCE_CHANGE_TS",
            F.greatest("SYNONYM_CHANGE_TS", "CATALOG_CHANGE_TS", "INGREDIENT_CHANGE_TS"),
        )
        .withColumn(
            "ADC_UPDT",
            F.greatest("SYNONYM_ADC_UPDT", "CATALOG_ADC_UPDT", "SOURCE_CHANGE_TS"),
        )
        .withColumn(
            "SOURCE_ROW_HASH",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("MNEMONIC"), F.lit("")),
                    F.coalesce(F.col("MULTUM_CODE"), F.lit("")),
                    F.coalesce(F.col("VEC_SEARCH_TERM"), F.lit("")),
                ),
                256,
            ),
        )
        .select(
            "SYNONYM_ID",
            F.col("MNEMONIC").alias("HNA_ORDER_MNEMONIC"),
            "VEC_SEARCH_TERM",
            "MULTUM_CODE",
            "SOURCE_CHANGE_TS",
            "SOURCE_ROW_HASH",
            "ADC_UPDT",
        )
    )
    source_rank = Window.partitionBy("SYNONYM_ID").orderBy(
        F.col("SOURCE_CHANGE_TS").desc_nulls_last(),
        F.col("ADC_UPDT").desc_nulls_last(),
        F.col("VEC_SEARCH_TERM").asc_nulls_last(),
    )
    drug_universe = (
        drug_universe.withColumn("_rank", F.row_number().over(source_rank))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    assert_unique(drug_universe, "SYNONYM_ID", "drug universe")


    previous_fingerprint = None
    if table_exists(STATE_TABLE):
        row = spark.table(STATE_TABLE).filter(F.col("PIPELINE_NAME") == "medication_lookup").first()
        if row:
            previous_fingerprint = row["REFERENCE_FINGERPRINT"]
    reference_changed = previous_fingerprint != REFERENCE_FINGERPRINT

    duplicates_before = duplicate_id_count(TARGET_TABLE)
    if duplicates_before and not FULL_REPLACE:
        raise RuntimeError(
            f"{TARGET_TABLE} contains {duplicates_before} duplicate SYNONYM_ID groups. "
            "Run 99_Nomenclature_Repair_Backfill or invoke this notebook with full_replace=true."
        )

    if FORCE_FULL or FULL_REPLACE or reference_changed or not table_exists(TARGET_TABLE):
        candidates = drug_universe
    else:
        target_columns = {column.lower() for column in spark.table(TARGET_TABLE).columns}
        target = spark.table(TARGET_TABLE).select(
            F.col("SYNONYM_ID").alias("TARGET_SYNONYM_ID"),
            (
                F.col("SOURCE_CHANGE_TS")
                if "source_change_ts" in target_columns
                else F.lit(None).cast(TimestampType())
            ).alias("TARGET_CHANGE_TS"),
            (
                F.col("SOURCE_ROW_HASH")
                if "source_row_hash" in target_columns
                else F.lit(None).cast(StringType())
            ).alias("TARGET_ROW_HASH"),
        )
        candidates = drug_universe.alias("s").join(
            target.alias("t"),
            F.col("s.SYNONYM_ID") == F.col("t.TARGET_SYNONYM_ID"),
            "left",
        ).filter(
            F.col("t.TARGET_SYNONYM_ID").isNull()
            | ~F.col("s.SOURCE_ROW_HASH").eqNullSafe(F.col("t.TARGET_ROW_HASH"))
            | (F.col("s.SOURCE_CHANGE_TS") > F.col("t.TARGET_CHANGE_TS"))
        ).select("s.*")

    candidate_count = candidates.count()
    print(
        f"Medication lookup: {candidate_count} candidates; reference_changed={reference_changed}; "
        f"full_replace={FULL_REPLACE}"
    )

    if candidate_count == 0:
        result = {
            "run_id": RUN_ID,
            "status": "NO_CHANGES",
            "candidate_count": 0,
            "reference_changed": reference_changed,
        }
        return result


    rxnorm = spark.table("3_lookup.rxnorm.rxnconso").filter(
        (F.col("SAB") == "MMSL") & (F.coalesce(F.col("SUPPRESS"), F.lit("N")) != "Y")
    )
    rxnorm_rank = Window.partitionBy("CODE").orderBy(
        F.when(F.col("ISPREF") == "Y", 1).otherwise(0).desc(),
        F.col("RXCUI").cast("long").asc_nulls_last(),
        F.col("RXCUI").asc(),
    )
    best_rxnorm = (
        rxnorm.withColumn("_rank", F.row_number().over(rxnorm_rank))
        .filter(F.col("_rank") == 1)
        .select(F.col("CODE").alias("MULTUM_CODE"), F.col("RXCUI").alias("RXNORM_CODE"))
    )

    snomed_rank = Window.partitionBy("RXCUI").orderBy(
        F.when(F.col("ISPREF") == "Y", 1).otherwise(0).desc(),
        F.length("CODE").asc(),
        F.col("CODE").asc(),
    )
    best_snomed = (
        spark.table("3_lookup.rxnorm.rxnconso")
        .filter(
            F.col("SAB").isin("SNOMEDCT_US", "SNOMEDCT")
            & (F.coalesce(F.col("SUPPRESS"), F.lit("N")) != "Y")
        )
        .withColumn("_rank", F.row_number().over(snomed_rank))
        .filter(F.col("_rank") == 1)
        .select("RXCUI", F.col("CODE").alias("SNOMED_CODE"))
    )

    base_mapping = (
        candidates.join(F.broadcast(best_rxnorm), "MULTUM_CODE", "left")
        .join(F.broadcast(best_snomed), F.col("RXNORM_CODE") == F.col("RXCUI"), "left")
        .drop("RXCUI")
    )

    valid_concepts = spark.table("3_lookup.omop.concept").filter(
        F.col("invalid_reason").isNull()
    )
    rxnorm_omop_rank = Window.partitionBy("concept_code").orderBy(F.col("concept_id").asc())
    rxnorm_to_omop = (
        valid_concepts.filter(
            (F.col("vocabulary_id") == "RxNorm") & (F.col("standard_concept") == "S")
        )
        .withColumn("_rank", F.row_number().over(rxnorm_omop_rank))
        .filter(F.col("_rank") == 1)
        .select(
            F.col("concept_code").alias("RXNORM_CODE"),
            F.col("concept_id").cast(IntegerType()).alias("MAPPED_OMOP_CONCEPT_ID"),
            F.col("concept_name").alias("MAPPED_OMOP_CONCEPT_TERM"),
        )
    )
    base_mapping = base_mapping.join(F.broadcast(rxnorm_to_omop), "RXNORM_CODE", "left")


    target_vectors = (
        spark.table("3_lookup.embeddings.omop_ingredients")
        .filter(F.col("embedding_vector").isNotNull() & (F.size("embedding_vector") > 0))
        .select(normalize_term(F.col("term")).alias("term"), "embedding_vector")
        .join(
            valid_concepts.filter(
                F.col("vocabulary_id").isin("dm+d", "RxNorm")
                & F.col("concept_class_id").isin("Ingredient", "Multiple Ingredients")
            ).select(
                normalize_term(F.col("concept_name")).alias("term"),
                F.col("concept_id").cast(IntegerType()).alias("RAW_OMOP_ID"),
                F.col("concept_name").alias("RAW_OMOP_TERM"),
                "standard_concept",
            ),
            "term",
            "inner",
        )
    )
    target_rank = Window.partitionBy("term").orderBy(
        F.when(F.col("standard_concept") == "S", 1).otherwise(0).desc(),
        F.col("RAW_OMOP_ID").asc(),
    )
    target_vectors = (
        target_vectors.withColumn("_rank", F.row_number().over(target_rank))
        .filter(F.col("_rank") == 1)
        .drop("_rank", "standard_concept", "term")
        .toPandas()
    )

    if len(target_vectors) > 100000:
        raise RuntimeError(
            f"Vector target contains {len(target_vectors)} rows; refuse to collect more than 100000 to the driver"
        )

    source_vector_rank = Window.partitionBy("VEC_SEARCH_TERM").orderBy(
        F.when(F.col("embedding_vector").isNotNull(), 1).otherwise(0).desc(),
        F.col("updated_at").desc_nulls_last(),
        F.col("model_version").asc_nulls_last(),
    )
    source_vectors = (
        spark.table("3_lookup.embeddings.barts_ingredients")
        .select(
            normalize_term(F.col("term")).alias("VEC_SEARCH_TERM"),
            "embedding_vector",
            "updated_at",
            "model_version",
        )
        .withColumn("_rank", F.row_number().over(source_vector_rank))
        .filter(F.col("_rank") == 1)
        .select("VEC_SEARCH_TERM", "embedding_vector")
    )

    if target_vectors.empty:
        raise RuntimeError("No OMOP ingredient vectors are available")

    target_ids = target_vectors["RAW_OMOP_ID"].to_numpy()
    target_terms = target_vectors["RAW_OMOP_TERM"].to_numpy()
    target_matrix = np.stack(target_vectors["embedding_vector"].to_numpy()).astype(np.float32)
    target_norm = target_matrix / np.maximum(np.linalg.norm(target_matrix, axis=1, keepdims=True), 1e-9)
    target_norm_transposed = np.ascontiguousarray(target_norm.T)

    vector_inputs = (
        base_mapping.filter(F.col("MAPPED_OMOP_CONCEPT_ID").isNull())
        .select("VEC_SEARCH_TERM")
        .filter(F.col("VEC_SEARCH_TERM").isNotNull())
        .distinct()
        .join(source_vectors, "VEC_SEARCH_TERM", "inner")
        .filter(
            F.col("embedding_vector").isNotNull()
            & (F.size("embedding_vector") == int(target_matrix.shape[1]))
        )
        .orderBy("VEC_SEARCH_TERM")
    )

    vector_match_schema = StructType(
        [
            StructField("VEC_SEARCH_TERM", StringType(), False),
            StructField("RAW_OMOP_ID", IntegerType(), True),
            StructField("RAW_OMOP_TERM", StringType(), True),
            StructField("SCORE", FloatType(), True),
        ]
    )


    def match_vector_batch(batch_terms, batch_vectors):
        source_matrix = np.asarray(batch_vectors, dtype=np.float32)
        if source_matrix.ndim != 2 or source_matrix.shape[1] != target_norm.shape[1]:
            raise RuntimeError(
                f"Unexpected source vector shape {source_matrix.shape}; "
                f"expected (*, {target_norm.shape[1]})"
            )
        source_norm = source_matrix / np.maximum(np.linalg.norm(source_matrix, axis=1, keepdims=True), 1e-9)
        scores = np.matmul(source_norm, target_norm_transposed)
        best_index = np.argmax(scores, axis=1)
        best_scores = np.max(scores, axis=1).astype(np.float32)
        return [
            (
                str(term),
                int(target_ids[index]),
                str(target_terms[index]),
                float(score),
            )
            for term, index, score in zip(batch_terms, best_index, best_scores)
        ]


    vector_match_rows = []
    batch_terms = []
    batch_vectors = []
    processed_terms = 0
    for row in vector_inputs.toLocalIterator():
        batch_terms.append(row["VEC_SEARCH_TERM"])
        batch_vectors.append(row["embedding_vector"])
        if len(batch_terms) >= VECTOR_MATCH_BATCH_SIZE:
            vector_match_rows.extend(match_vector_batch(batch_terms, batch_vectors))
            processed_terms += len(batch_terms)
            if processed_terms % (VECTOR_MATCH_BATCH_SIZE * 20) == 0:
                print(f"Medication vector fallback: matched {processed_terms} distinct terms")
            batch_terms = []
            batch_vectors = []
    if batch_terms:
        vector_match_rows.extend(match_vector_batch(batch_terms, batch_vectors))
        processed_terms += len(batch_terms)

    print(
        f"Medication vector fallback: matched {processed_terms} distinct terms "
        f"against {len(target_vectors)} OMOP ingredient vectors"
    )
    vector_matches = spark.createDataFrame(vector_match_rows, vector_match_schema)
    matches = base_mapping.join(vector_matches, "VEC_SEARCH_TERM", "left")


    valid_maps = spark.table("3_lookup.omop.concept_relationship").filter(
        (F.col("relationship_id") == "Maps to") & F.col("invalid_reason").isNull()
    ).select("concept_id_1", "concept_id_2").distinct()

    raw_ids = matches.select("RAW_OMOP_ID").filter(F.col("RAW_OMOP_ID").isNotNull()).distinct()
    self_standard = (
        valid_concepts.join(F.broadcast(raw_ids), F.col("concept_id") == F.col("RAW_OMOP_ID"), "inner")
        .filter(F.col("standard_concept") == "S")
        .select(
            F.col("RAW_OMOP_ID"),
            F.col("concept_id").cast(IntegerType()).alias("VECTOR_STANDARD_ID"),
            F.col("concept_name").alias("VECTOR_STANDARD_TERM"),
            F.col("domain_id").alias("VECTOR_STANDARD_DOMAIN"),
        )
    )
    mapped_standard = (
        raw_ids.join(valid_maps, F.col("RAW_OMOP_ID") == F.col("concept_id_1"), "inner")
        .join(
            valid_concepts.filter(F.col("standard_concept") == "S").select(
                F.col("concept_id").alias("VECTOR_STANDARD_ID"),
                F.col("concept_name").alias("VECTOR_STANDARD_TERM"),
                F.col("domain_id").alias("VECTOR_STANDARD_DOMAIN"),
            ),
            F.col("concept_id_2") == F.col("VECTOR_STANDARD_ID"),
            "inner",
        )
        .select("RAW_OMOP_ID", "VECTOR_STANDARD_ID", "VECTOR_STANDARD_TERM", "VECTOR_STANDARD_DOMAIN")
    )
    vector_standardization = self_standard.unionByName(mapped_standard).dropDuplicates(
        ["RAW_OMOP_ID", "VECTOR_STANDARD_ID"]
    )
    standard_rank = Window.partitionBy("RAW_OMOP_ID").orderBy(
        F.when(F.col("VECTOR_STANDARD_DOMAIN") == "Drug", 1).otherwise(0).desc(),
        F.col("VECTOR_STANDARD_ID").asc(),
    )
    vector_standardization = (
        vector_standardization.withColumn("_rank", F.row_number().over(standard_rank))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    matches = matches.join(vector_standardization, "RAW_OMOP_ID", "left").withColumn(
        "RAW_SIMILARITY_OMOP_CONCEPT_ID",
        F.col("RAW_OMOP_ID").cast(IntegerType()),
    ).withColumn(
        "RAW_SIMILARITY_OMOP_CONCEPT_TERM",
        F.col("RAW_OMOP_TERM"),
    ).withColumn(
        "STANDARDIZED_SIMILARITY_OMOP_CONCEPT_ID",
        F.col("VECTOR_STANDARD_ID").cast(IntegerType()),
    ).withColumn(
        "STANDARDIZED_SIMILARITY_OMOP_CONCEPT_TERM",
        F.col("VECTOR_STANDARD_TERM"),
    )

    matches = (
        matches.withColumn("RAW_SIMILARITY_SCORE", F.col("SCORE").cast(FloatType()))
        .withColumn(
            "SIMILARITY_STATUS",
            F.when(F.col("MAPPED_OMOP_CONCEPT_ID").isNotNull(), F.lit("NOT_REQUIRED"))
            .when(F.col("SCORE").isNull(), F.lit("NO_EMBEDDING"))
            .when(F.col("SCORE") < F.lit(SIMILARITY_THRESHOLD), F.lit("REJECTED"))
            .when(F.col("VECTOR_STANDARD_ID").isNull(), F.lit("NO_STANDARD_MAP"))
            .otherwise(F.lit("ACCEPTED")),
        )
        .withColumn(
            "SIMILARITY_OMOP_CONCEPT_ID",
            F.when(
                F.col("SIMILARITY_STATUS") == "ACCEPTED",
                F.col("STANDARDIZED_SIMILARITY_OMOP_CONCEPT_ID"),
            ).cast(IntegerType()),
        )
        .withColumn(
            "SIMILARITY_OMOP_CONCEPT_TERM",
            F.when(
                F.col("SIMILARITY_STATUS") == "ACCEPTED",
                F.col("STANDARDIZED_SIMILARITY_OMOP_CONCEPT_TERM"),
            ),
        )
        .withColumn(
            "SIMILARITY_SCORE",
            F.when(F.col("SIMILARITY_STATUS") == "ACCEPTED", F.col("SCORE")).cast(FloatType()),
        )
    )

    effective_standard_ids = matches.select(
        F.coalesce(F.col("MAPPED_OMOP_CONCEPT_ID"), F.col("SIMILARITY_OMOP_CONCEPT_ID")).alias(
            "EFFECTIVE_STANDARD_ID"
        )
    ).filter(F.col("EFFECTIVE_STANDARD_ID").isNotNull()).distinct()
    snomed_sources = valid_concepts.filter(
        (F.col("vocabulary_id") == "SNOMED")
        & (F.col("concept_class_id") == "Substance")
        & (F.col("domain_id") == "Drug")
    ).select(
        F.col("concept_id").alias("SNOMED_SOURCE_ID"),
        F.col("concept_code").alias("DERIVED_SNOMED_CODE"),
        F.col("concept_name").alias("SNOMED_SOURCE_NAME"),
    )
    best_snomed = (
        snomed_sources.join(valid_maps, F.col("SNOMED_SOURCE_ID") == F.col("concept_id_1"), "inner")
        .join(
            F.broadcast(effective_standard_ids),
            F.col("concept_id_2") == F.col("EFFECTIVE_STANDARD_ID"),
            "inner",
        )
    )
    snomed_choice = Window.partitionBy("EFFECTIVE_STANDARD_ID").orderBy(
        F.length("SNOMED_SOURCE_NAME").asc_nulls_last(), F.col("DERIVED_SNOMED_CODE").asc()
    )
    best_snomed = (
        best_snomed.withColumn("_rank", F.row_number().over(snomed_choice))
        .filter(F.col("_rank") == 1)
        .select("EFFECTIVE_STANDARD_ID", "DERIVED_SNOMED_CODE")
    )

    final_df = (
        matches.withColumn(
            "EFFECTIVE_STANDARD_ID",
            F.coalesce(F.col("MAPPED_OMOP_CONCEPT_ID"), F.col("SIMILARITY_OMOP_CONCEPT_ID")),
        )
        .join(best_snomed, "EFFECTIVE_STANDARD_ID", "left")
        .select(
            "SYNONYM_ID",
            "HNA_ORDER_MNEMONIC",
            "VEC_SEARCH_TERM",
            "MULTUM_CODE",
            "RXNORM_CODE",
            "SNOMED_CODE",
            "MAPPED_OMOP_CONCEPT_ID",
            "MAPPED_OMOP_CONCEPT_TERM",
            "SIMILARITY_OMOP_CONCEPT_ID",
            "SIMILARITY_OMOP_CONCEPT_TERM",
            "SIMILARITY_SCORE",
            "RAW_SIMILARITY_OMOP_CONCEPT_ID",
            "RAW_SIMILARITY_OMOP_CONCEPT_TERM",
            "STANDARDIZED_SIMILARITY_OMOP_CONCEPT_ID",
            "STANDARDIZED_SIMILARITY_OMOP_CONCEPT_TERM",
            "RAW_SIMILARITY_SCORE",
            "SIMILARITY_STATUS",
            F.col("DERIVED_SNOMED_CODE").alias("SNOMED_FROM_OMOP"),
            "SOURCE_CHANGE_TS",
            "SOURCE_ROW_HASH",
            "ADC_UPDT",
            F.lit(MODEL_VERSION).alias("EMBEDDING_MODEL_VERSION"),
            F.lit(SIMILARITY_THRESHOLD).cast(FloatType()).alias("SIMILARITY_THRESHOLD"),
        )
    )
    final_rank = Window.partitionBy("SYNONYM_ID").orderBy(
        F.col("SOURCE_CHANGE_TS").desc_nulls_last(),
        F.col("MAPPED_OMOP_CONCEPT_ID").asc_nulls_last(),
        F.col("RAW_SIMILARITY_SCORE").desc_nulls_last(),
        F.col("RAW_SIMILARITY_OMOP_CONCEPT_ID").asc_nulls_last(),
    )
    final_df = (
        final_df.withColumn("_rank", F.row_number().over(final_rank))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    assert_unique(final_df, "SYNONYM_ID", "medication output")


    if FULL_REPLACE:
        final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            TARGET_TABLE
        )
    else:
        if not table_exists(TARGET_TABLE):
            final_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
        else:
            ensure_columns(TARGET_TABLE, final_df)
            updates = {column: f"s.`{column}`" for column in final_df.columns if column != "SYNONYM_ID"}
            inserts = {column: f"s.`{column}`" for column in final_df.columns}
            (
                DeltaTable.forName(spark, TARGET_TABLE)
                .alias("t")
                .merge(final_df.alias("s"), "t.SYNONYM_ID = s.SYNONYM_ID")
                .whenMatchedUpdate(set=updates)
                .whenNotMatchedInsert(values=inserts)
                .execute()
            )

    duplicates_after = duplicate_id_count(TARGET_TABLE)
    if duplicates_after:
        raise RuntimeError(f"Medication target still contains {duplicates_after} duplicate ID groups")

    state_schema = StructType(
        [
            StructField("PIPELINE_NAME", StringType(), False),
            StructField("REFERENCE_FINGERPRINT", StringType(), False),
            StructField("LAST_RUN_ID", StringType(), False),
            StructField("UPDATED_AT", TimestampType(), False),
        ]
    )
    if not table_exists(STATE_TABLE):
        spark.createDataFrame([], state_schema).write.format("delta").saveAsTable(STATE_TABLE)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    state_df = spark.createDataFrame(
        [("medication_lookup", REFERENCE_FINGERPRINT, RUN_ID, now)], state_schema
    )
    (
        DeltaTable.forName(spark, STATE_TABLE)
        .alias("t")
        .merge(state_df.alias("s"), "t.PIPELINE_NAME = s.PIPELINE_NAME")
        .whenMatchedUpdate(
            set={
                "REFERENCE_FINGERPRINT": "s.REFERENCE_FINGERPRINT",
                "LAST_RUN_ID": "s.LAST_RUN_ID",
                "UPDATED_AT": "s.UPDATED_AT",
            }
        )
        .whenNotMatchedInsert(
            values={
                "PIPELINE_NAME": "s.PIPELINE_NAME",
                "REFERENCE_FINGERPRINT": "s.REFERENCE_FINGERPRINT",
                "LAST_RUN_ID": "s.LAST_RUN_ID",
                "UPDATED_AT": "s.UPDATED_AT",
            }
        )
        .execute()
    )

    result = {
        "run_id": RUN_ID,
        "status": "SUCCEEDED",
        "candidate_count": candidate_count,
        "reference_changed": reference_changed,
        "duplicates_before": duplicates_before,
        "duplicates_after": duplicates_after,
        "full_replace": FULL_REPLACE,
        "similarity_threshold": SIMILARITY_THRESHOLD,
    }
    print(json.dumps(result, default=str))
    return result

# COMMAND ----------

pipeline_results = {}
for section_name, section_function in [
    ("mapping", run_mapping_section),
    ("embeddings", run_embedding_section),
    ("finalize", run_finalize_section),
    ("medication_lookup", run_medication_section),
]:
    print(f"Starting inline section: {section_name}")
    pipeline_results[section_name] = section_function()
    print(f"Completed inline section: {section_name}: {pipeline_results[section_name]}")

pipeline_result = {
    "run_id": PIPELINE_RUN_ID,
    "status": "SUCCEEDED",
    "sections": pipeline_results,
}
print(json.dumps(pipeline_result, default=str))
dbutils.notebook.exit(json.dumps(pipeline_result, default=str))
