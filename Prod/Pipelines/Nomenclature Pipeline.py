# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *
from pyspark.sql import DataFrame, functions as F
from functools import reduce
from delta.tables import DeltaTable
import uuid

# COMMAND ----------



def table_exists(table_name):
    """
    Checks if a table exists in the Spark catalog.
    """
    try:
        spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except AnalysisException:
        return False

def get_max_timestamp(table_name):
    """
    Retrieves the maximum timestamp from a given table.
    If the table doesn't exist or has no data, returns January 1, 1980.
    
    Args:
        table_name (str): Name of the table to query
    
    Returns:
        datetime: Maximum timestamp or default date
    """
    default_date = datetime(1980, 1, 1)
    if not table_exists(table_name):
        return default_date
        
    try:
        max_date_row = spark.sql(f"SELECT MAX(ADC_UPDT) AS max_date FROM {table_name}").first()
        return max_date_row["max_date"] if max_date_row and max_date_row["max_date"] is not None else default_date
    except Exception as e:
        print(f"Error getting max timestamp from {table_name}: {str(e)}")
        return default_date


def update_nomen_table(source_df, target_table, primary_keys=["NOMENCLATURE_ID"]):
    if source_df is None:
        print(f"No records to update in {target_table}")
        return
    
    # Keep the latest row per PK without a global sort/window:
    # Use max(struct(...)) so the whole row for the max ADC_UPDT is kept.
    cols = source_df.columns
    if "ADC_UPDT" not in cols:
        raise ValueError("ADC_UPDT column required for update-if-newer logic")
    
    # Build struct with ADC_UPDT first so max() picks latest row
    max_struct = F.struct(F.col("ADC_UPDT"), *[F.col(c) for c in cols if c != "ADC_UPDT"])
    latest = (source_df
              .groupBy(*primary_keys)
              .agg(F.max(max_struct).alias("_r"))
              # Put PKs back plus all other columns from the max struct
              .select(
                  *primary_keys,
                  *[F.col(f"_r.{c}").alias(c) for c in cols if c not in primary_keys]
              ))
    
    # Repartition by PK to balance write/MERGE
    latest = latest.repartition(400, *[F.col(k) for k in primary_keys])
    
    # Stage as a short-lived Delta table in the same catalog.schema
    cat, sch, tbl = target_table.split(".")
    stage_tbl = f"{cat}.{sch}.{tbl}__stage_{uuid.uuid4().hex[:8]}"
    (latest.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(stage_tbl))
    
    try:
        n = spark.table(stage_tbl).count()
        if n == 0:
            print(f"No records to update in {target_table}")
            return
        
        
        if spark.catalog.tableExists(target_table):
            delta_t = DeltaTable.forName(spark, target_table)
            src = spark.table(stage_tbl)
            on = " AND ".join([f"t.{k} = s.{k}" for k in primary_keys])
            
            # Use '>' not '>=' to avoid re-writing when ADC_UPDT is equal
            (delta_t.alias("t")
                .merge(src.alias("s"), on)
                .whenMatchedUpdate(
                    condition="s.ADC_UPDT > t.ADC_UPDT",
                    set={c: f"s.{c}" for c in src.columns if c not in primary_keys}
                )
                .whenNotMatchedInsert(values={c: f"s.{c}" for c in src.columns})
                .execute())
        else:
            spark.table(stage_tbl).write.format("delta").mode("overwrite").saveAsTable(target_table)
        
        print(f"Updated {target_table} from {n} staged rows")
    
    finally:
        spark.sql(f"DROP TABLE {stage_tbl}")

# COMMAND ----------


# =============================================================================
# GENERIC HELPER FUNCTIONS
# =============================================================================

def prepare_symmetric_relationships(concept_relationship):
    """Create symmetric relationship table for efficient equi-joins."""
    rels = concept_relationship.select("concept_id_1", "concept_id_2", "relationship_id") \
                               .filter(F.col("relationship_id").isin("Maps to", "Mapped from"))
    forward = rels.select(F.col("concept_id_1").alias("source_concept_id"),
                         F.col("concept_id_2").alias("target_concept_id"))
    backward = rels.select(F.col("concept_id_2").alias("source_concept_id"),
                          F.col("concept_id_1").alias("target_concept_id"))
    return forward.unionByName(backward).distinct()

def filter_concepts(concepts_df, vocab_ids):
    """Filter concepts by vocabulary IDs and return standardized columns."""
    return concepts_df.filter(F.col("invalid_reason").isNull() & F.col("vocabulary_id").isin(vocab_ids)) \
                      .select(F.col("concept_id"), 
                             F.col("concept_code").cast(StringType()).alias("concept_code"),
                             F.col("concept_name"), 
                             F.col("standard_concept"), 
                             F.col("domain_id"), 
                             F.col("concept_class_id"),
                             F.col("vocabulary_id"))

def normalize_code(col_expr, code_type):
    """Normalize codes for matching - handle case, whitespace, and ICD10 dots."""
    c = F.upper(F.trim(col_expr))
    if code_type == "ICD10":
        c = F.regexp_replace(c, r"\.", "")
    elif code_type == "OPCS4":
        c = F.regexp_replace(c, "[^A-Z0-9]", "")
    return c

def add_omop_preference_generic(df, source_code_col, target_code_col,
                                source_vocab_ids, target_vocab_ids,
                                concepts_df, symmetric_rels):
    """Generic OMOP preference checker for any vocabulary pair."""
    src = F.broadcast(filter_concepts(concepts_df, source_vocab_ids)) \
          .select(F.col("concept_id").alias("src_id"), F.col("concept_code").alias("src_code"))
    tgt = F.broadcast(filter_concepts(concepts_df, target_vocab_ids)) \
          .select(F.col("concept_id").alias("tgt_id"), F.col("concept_code").alias("tgt_code"))
    
    joined = df.join(src, F.col(source_code_col).cast(StringType()) == F.col("src_code"), "left") \
               .join(tgt, F.col(target_code_col).cast(StringType()) == F.col("tgt_code"), "left") \
               .join(F.broadcast(symmetric_rels),
                    (F.col("src_id") == F.col("source_concept_id")) & 
                    (F.col("tgt_id") == F.col("target_concept_id")),
                    "left")
    
    return joined.withColumn("HAS_OMOP_MAP", 
                           F.col("source_concept_id").isNotNull() & 
                           F.col("target_concept_id").isNotNull())

def select_best_match(df, nom_id_col, candidate_code_col, has_omop_col,
                     out_code_col, base_type, omop_type):
    """Select best match from candidates with OMOP preference."""
    candidates = df.filter(F.col(candidate_code_col).isNotNull())
    if candidates.isEmpty():
        return None
    
    win = Window.partitionBy(nom_id_col).orderBy(F.col(has_omop_col).desc(), F.col(candidate_code_col))
    best = candidates.withColumn("match_rank", F.row_number().over(win)) \
                    .filter(F.col("match_rank") == 1)
    
    counts = candidates.groupBy(nom_id_col).agg(
        F.count(candidate_code_col).alias(f"{out_code_col}_MATCH_COUNT"),
        F.max(F.col(has_omop_col)).alias("HAS_OMOP_MAP")
    )
    
    typed = best.select(F.col(nom_id_col),
                       F.col(candidate_code_col).alias(out_code_col),
                       F.when(F.col(has_omop_col), F.lit(omop_type))
                       .otherwise(F.lit(base_type)).alias(f"{out_code_col}_TYPE"))
    
    return typed.join(counts, nom_id_col, "inner")

def combine_matches_generic(code_col, type_col, match_count_col, *dfs):
    """Combine and prioritize matches from multiple sources."""
    valid = [df for df in dfs if df is not None and not df.isEmpty()]
    if not valid:
        return None
    
    combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), valid)
    
    priority = F.when(F.col(type_col) == "NATIVE", 1) \
               .when(F.col(type_col).isin("EXACT", "OMOP_ASSISTED", "CHILDMATCH_OMOP_ASSISTED", 
                                         "PARENTMATCH_OMOP_ASSISTED"), 2) \
               .when(F.col(type_col).contains("CHILD"), 3) \
               .when(F.col(type_col).contains("PARENT"), 4) \
               .otherwise(5)
    
    win = Window.partitionBy("NOMENCLATURE_ID").orderBy(priority, F.col(match_count_col).desc())
    
    return combined.withColumn("row_num", F.row_number().over(win)) \
                   .filter(F.col("row_num") == 1) \
                   .drop("row_num")

def add_terms_generic(matches_df, terms_df, code_col, term_out_col, base_df):
    """Add terms from reference table to matches."""
    if matches_df is None:
        return base_df.withColumn(code_col, F.lit(None).cast(StringType())) \
                     .withColumn(f"{code_col}_TYPE", F.lit(None).cast(StringType())) \
                     .withColumn(f"{code_col}_MATCH_COUNT", F.lit(None).cast(IntegerType())) \
                     .withColumn(term_out_col, F.lit(None).cast(StringType()))
    
    terms_clean = F.broadcast(
        terms_df.select(F.col("CUI").cast(StringType()).alias("CUI"),
                       F.col("TERM").alias(term_out_col))
                .dropDuplicates(["CUI"])
    )
    
    with_terms = matches_df.join(terms_clean, F.col(code_col) == F.col("CUI"), "left") \
                          .select("NOMENCLATURE_ID", code_col, f"{code_col}_TYPE", 
                                 f"{code_col}_MATCH_COUNT", term_out_col)
    
    return base_df.join(with_terms, "NOMENCLATURE_ID", "left")

def process_secondary_mappings_generic(df, concepts_df, symmetric_rels,
                                      code_col, type_col, match_count_col, term_col,
                                      target_vocab_ids, new_type="OMOP_DERIVED"):
    """Process secondary mappings via OMOP for any vocabulary."""
    candidates = df.filter(F.col("OMOP_CONCEPT_ID").isNotNull() & F.col(code_col).isNull()) \
                  .select("NOMENCLATURE_ID", "OMOP_CONCEPT_ID")
    
    if candidates.isEmpty():
        return df
    
    targets = filter_concepts(concepts_df, target_vocab_ids) \
             .select(F.col("concept_id").alias("tgt_id"),
                    F.col("concept_code").alias("tgt_code"),
                    F.col("concept_name").alias("tgt_name"))
    
    mapping = candidates.join(symmetric_rels, 
                            F.col("OMOP_CONCEPT_ID") == F.col("source_concept_id"), 
                            "inner") \
                      .join(F.broadcast(targets), 
                           F.col("target_concept_id") == F.col("tgt_id"), 
                           "inner") \
                      .groupBy("NOMENCLATURE_ID").agg(
                          F.first("tgt_code").alias("new_code"),
                          F.first("tgt_name").alias("new_term"),
                          F.count("tgt_code").alias("new_match_count")
                      )
    
    cols_to_keep = [c for c in df.columns if c not in [code_col, type_col, match_count_col, term_col]]
    
    # Check which columns actually exist in the dataframe
    existing_cols = df.columns
    
    result = df.join(mapping, "NOMENCLATURE_ID", "left")
    
    # Build the select list dynamically based on what columns exist
    select_list = cols_to_keep.copy()
    
    # Handle code column
    if code_col in existing_cols:
        select_list.append(F.coalesce(F.col(code_col), F.col("new_code")).alias(code_col))
    else:
        select_list.append(F.col("new_code").alias(code_col))
    
    # Handle type column
    if type_col in existing_cols:
        select_list.append(F.coalesce(F.col(type_col), F.lit(new_type)).alias(type_col))
    else:
        select_list.append(F.lit(new_type).alias(type_col))
    
    # Handle match count column
    if match_count_col in existing_cols:
        select_list.append(F.coalesce(F.col(match_count_col), F.col("new_match_count")).alias(match_count_col))
    else:
        select_list.append(F.col("new_match_count").alias(match_count_col))
    
    # Handle term column
    if term_col in existing_cols:
        select_list.append(F.coalesce(F.col(term_col), F.col("new_term")).alias(term_col))
    else:
        select_list.append(F.col("new_term").alias(term_col))
    
    return result.select(*select_list)

def materialize(df, temp_table):
    """Materialize DataFrame to break complex lineage."""
    # Properly format the DROP statement for catalog.schema.table format
    spark.sql(f"DROP TABLE IF EXISTS `{temp_table}`")
    df.write.mode("overwrite").saveAsTable(temp_table)
    out = spark.table(temp_table)
    return out

def process_native_codes_generic(base_df, code_values_df, code_col, vocab_filter_col, cki_pattern=None):
    """Generic processor for native codes."""
    # Filter by vocabulary code values
    vocab_codes = F.broadcast(code_values_df.select("CODE_VALUE").distinct())
    
    vocab_matches = base_df.join(
        vocab_codes,
        base_df[vocab_filter_col] == vocab_codes.CODE_VALUE,
        "left_semi"
    ).select(
        "NOMENCLATURE_ID",
        F.col("FOUND_CUI").alias(code_col),
        F.lit(1).alias(f"{code_col}_MATCH_COUNT"),
        F.lit(False).alias("HAS_OMOP_MAP"),
        F.lit("NATIVE").alias(f"{code_col}_TYPE")
    )
    
    # Handle CKI pattern if provided
    if cki_pattern:
        cki_matches = base_df.filter(F.col("CONCEPT_CKI").like(f"{cki_pattern}%")).select(
            "NOMENCLATURE_ID",
            F.regexp_replace(F.col("CONCEPT_CKI"), f"{cki_pattern}!", "").alias(code_col),
            F.lit(1).alias(f"{code_col}_MATCH_COUNT"),
            F.lit(False).alias("HAS_OMOP_MAP"),
            F.lit("NATIVE").alias(f"{code_col}_TYPE")
        )
        return vocab_matches.unionByName(cki_matches).distinct()
    
    return vocab_matches

# COMMAND ----------


# =============================================================================
# NOMENCLATURE MAPPING CREATION
# =============================================================================

def create_nomenclature_mapping_incr():
    """
    Creates an incremental mapping table that combines all vocabulary mappings.
    
    Returns:
        DataFrame: Incremental nomenclature mappings
    """
    # Get cutoff timestamp for incremental processing
    max_adc_updt = get_max_timestamp("4_prod.tmp.tempone_nomenclature")
    print(f"Processing records updated after: {max_adc_updt}")
    
    # Use fixed timestamp for reproducibility within the job
    job_timestamp = current_timestamp()

    # Get base nomenclature data
    base_nomenclature = (
        spark.table("3_lookup.mill.mill_nomenclature")
        .filter(
            (col("ACTIVE_IND") == 1) & 
            (col("END_EFFECTIVE_DT_TM").isNull() | (col("END_EFFECTIVE_DT_TM") > job_timestamp)) &
            (F.greatest(col("UPDT_DT_TM"), col("LAST_UTC_TS")) > max_adc_updt)
        )
    )
    
    # Check for new records efficiently
    if base_nomenclature.isEmpty():
        print("No new records found in source nomenclature.")
        return None
    
    record_count = base_nomenclature.count()
    print(f"Found {record_count} new records to process.")

    # Get reference tables and prepare them for efficient joins
    all_concepts = spark.table("3_lookup.omop.concept").filter(
        (col("concept_id").isNotNull()) & (col("invalid_reason").isNull())
    )
    
    # Split concepts by vocabulary for accurate matching
    snomed_concepts = all_concepts.filter(col("vocabulary_id") == "SNOMED")
    multum_concepts = all_concepts.filter(col("vocabulary_id") == "Multum")
    icd10_concepts = all_concepts.filter(col("vocabulary_id").isin(["ICD10", "ICD10CM"]))
    opcs4_concepts = all_concepts.filter(col("vocabulary_id") == "OPCS4")
    
    # Standardize concept codes once for each vocabulary
    # Add standardized codes to concepts for cleaner joins
    snomed_concepts = snomed_concepts.withColumn(
        "standardized_concept_code", col("concept_code")
    )
    
    multum_concepts = multum_concepts.withColumn(
        "standardized_concept_code", 
        regexp_replace(upper(col("concept_code")), "^D", "")
    )
    
    icd10_concepts = icd10_concepts.withColumn(
        "standardized_concept_code", 
        regexp_replace(upper(col("concept_code")), "[^A-Z0-9]", "")
    )
    
    opcs4_concepts = opcs4_concepts.withColumn(
        "standardized_concept_code", 
        regexp_replace(upper(trim(col("concept_code"))), "[^A-Z0-9]", "")
    )

    # Define vocabulary code groups
    base_with_vocab_type = (
        base_nomenclature
        .withColumn("is_snomed", col("SOURCE_VOCABULARY_CD").isin(466776237, 673967))
        .withColumn("is_multum", col("SOURCE_VOCABULARY_CD").isin(64004559, 1238, 1237))
        .withColumn("is_icd10", col("SOURCE_VOCABULARY_CD").isin(2976507, 647081))
        .withColumn("is_opcs4", col("SOURCE_VOCABULARY_CD") == 685812)
    )

    # Add standardized codes based on vocabulary type
    base_with_vocab_type = base_with_vocab_type.withColumn(
        "standardized_code",
        when(col("is_snomed"), regexp_extract(col("CONCEPT_CKI"), "SNOMED!(.*)", 1))
        .when(col("is_multum"), regexp_replace(upper(col("SOURCE_IDENTIFIER")), "^D", ""))
        .when(col("is_icd10"), regexp_replace(upper(col("SOURCE_IDENTIFIER")), "[^A-Z0-9]", ""))
        .when(col("is_opcs4"), regexp_replace(upper(col("SOURCE_IDENTIFIER")), "[^A-Z0-9]", ""))
        .otherwise(col("SOURCE_IDENTIFIER"))
    )
    

    # Process matches for each vocabulary type with broadcast hints where appropriate
    # SNOMED matches
    snomed_base = base_with_vocab_type.filter(col("is_snomed")).alias("base")
    snomed_matches = snomed_base.join(
        broadcast(snomed_concepts).alias("concepts"),
        col("base.standardized_code") == col("concepts.standardized_concept_code"),
        "left"
    ).withColumn("vocabulary_type", lit("SNOMED"))
    
    # Multum matches
    multum_base = base_with_vocab_type.filter(col("is_multum")).alias("base")
    multum_matches = multum_base.join(
        broadcast(multum_concepts).alias("concepts"),
        col("base.standardized_code") == col("concepts.standardized_concept_code"),
        "left"
    ).withColumn("vocabulary_type", lit("MULTUM"))
    
    # ICD10 matches
    icd10_base = base_with_vocab_type.filter(col("is_icd10")).alias("base")
    icd10_matches = icd10_base.join(
        broadcast(icd10_concepts).alias("concepts"),
        col("base.standardized_code") == col("concepts.standardized_concept_code"),
        "left"
    ).withColumn("vocabulary_type", lit("ICD10"))

    # OPCS4 matches
    opcs4_base = base_with_vocab_type.filter(col("is_opcs4")).alias("base")
    opcs4_matches = opcs4_base.join(
        broadcast(opcs4_concepts).alias("concepts"),
        col("base.standardized_code") == col("concepts.standardized_concept_code"),
        "left"
    ).withColumn("vocabulary_type", lit("OPCS4"))
    
    # Handle unmatched/other vocabulary types
    other_base = base_with_vocab_type.filter(
        ~col("is_snomed") & ~col("is_multum") & ~col("is_icd10") & ~col("is_opcs4")
    ).alias("base")
    other_matches = other_base.withColumn("vocabulary_type", lit("OTHER")).select(
        col("base.*"),
        lit(None).alias("concept_id"),
        lit(None).alias("concept_name"),
        lit(None).alias("standard_concept"),
        lit(None).alias("domain_id"),
        lit(None).alias("concept_class_id"),
        lit("OTHER").alias("vocabulary_type")
    )

    # Define common columns for union (consistent across all DataFrames)
    common_columns = [
        col("base.NOMENCLATURE_ID").alias("NOMENCLATURE_ID"),
        col("base.SOURCE_IDENTIFIER").alias("SOURCE_IDENTIFIER"),
        col("base.SOURCE_STRING").alias("SOURCE_STRING"),
        col("base.SOURCE_VOCABULARY_CD").alias("SOURCE_VOCABULARY_CD"),
        col("base.VOCAB_AXIS_CD").alias("VOCAB_AXIS_CD"),
        col("base.CONCEPT_CKI").alias("CONCEPT_CKI"),
        col("base.ADC_UPDT").alias("ADC_UPDT"),
        col("concepts.concept_id").alias("OMOP_CONCEPT_ID"),
        col("concepts.concept_name").alias("OMOP_CONCEPT_NAME"),
        col("concepts.standard_concept").alias("IS_STANDARD_OMOP_CONCEPT"),
        col("concepts.domain_id").alias("CONCEPT_DOMAIN"),
        col("concepts.concept_class_id").alias("CONCEPT_CLASS"),
        col("base.standardized_code").alias("FOUND_CUI"),
        col("vocabulary_type")
    ]
    
    # Select columns for other_matches (which doesn't have concepts columns)
    other_columns = [
        "NOMENCLATURE_ID", "SOURCE_IDENTIFIER", "SOURCE_STRING", "SOURCE_VOCABULARY_CD",
        "VOCAB_AXIS_CD", "CONCEPT_CKI", "ADC_UPDT",
        col("concept_id").alias("OMOP_CONCEPT_ID"),
        col("concept_name").alias("OMOP_CONCEPT_NAME"),
        col("standard_concept").alias("IS_STANDARD_OMOP_CONCEPT"),
        col("domain_id").alias("CONCEPT_DOMAIN"),
        col("concept_class_id").alias("CONCEPT_CLASS"),
        col("standardized_code").alias("FOUND_CUI"),
        "vocabulary_type"
    ]

    # Combine all matches using unionByName for robustness
    matches = (
        snomed_matches.select(*common_columns)
        .unionByName(multum_matches.select(*common_columns), allowMissingColumns=True)
        .unionByName(icd10_matches.select(*common_columns), allowMissingColumns=True)
        .unionByName(opcs4_matches.select(*common_columns), allowMissingColumns=True)
        .unionByName(other_matches.select(*other_columns), allowMissingColumns=True)
    )

    # First, get the best match for each NOMENCLATURE_ID
    window_spec = Window.partitionBy("NOMENCLATURE_ID").orderBy(
        when(col("vocabulary_type") == "SNOMED", 1)
        .when(col("vocabulary_type") == "MULTUM", 2)
        .when(col("vocabulary_type") == "ICD10", 3)
        .when(col("vocabulary_type") == "OPCS4", 4)
        .otherwise(5),
        when(col("IS_STANDARD_OMOP_CONCEPT") == "S", 1).otherwise(2),
        col("OMOP_CONCEPT_ID").asc_nulls_last()  # Stable tie-breaker
    )
    
    best_matches = (
        matches
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    # Calculate match counts separately for better performance
    match_counts = (
        matches
        .filter(col("OMOP_CONCEPT_ID").isNotNull())
        .groupBy("NOMENCLATURE_ID")
        .agg(F.count("OMOP_CONCEPT_ID").alias("NUMBER_OF_OMOP_MATCHES"))
    )
    
    # Join the counts back to the best matches
    final_df = (
        best_matches
        .join(match_counts, "NOMENCLATURE_ID", "left")
        .fillna(0, subset=["NUMBER_OF_OMOP_MATCHES"])
        .select(
            "NOMENCLATURE_ID", "SOURCE_IDENTIFIER", "SOURCE_STRING", "SOURCE_VOCABULARY_CD",
            "VOCAB_AXIS_CD", "OMOP_CONCEPT_ID", "OMOP_CONCEPT_NAME", "IS_STANDARD_OMOP_CONCEPT",
            "CONCEPT_DOMAIN", "CONCEPT_CLASS", "FOUND_CUI", "NUMBER_OF_OMOP_MATCHES",
            "CONCEPT_CKI", "ADC_UPDT"
        )
    )
    
    
    return final_df


# COMMAND ----------


# =============================================================================
# SNOMED MAPPING
# =============================================================================

def create_snomed_mapping_incr():
    """Creates an incremental SNOMED mapping table processing only new/modified records."""
    max_adc_updt = get_max_timestamp("4_prod.tmp.temptwo_nomenclature")
    print(f"Processing records updated after: {max_adc_updt}")
    
    base_df = spark.table("4_prod.tmp.tempone_nomenclature").filter(F.col("ADC_UPDT") > max_adc_updt)
    
    if base_df.isEmpty():
        print("No new records to process for SNOMED mapping.")
        return None

    # Add normalized columns
    base_df = base_df.withColumn("original_snomed_code", 
                                 F.when(F.col("CONCEPT_CKI").startswith("SNOMED!"), 
                                       F.substring(F.col("CONCEPT_CKI"), 8, 9999))) \
                     .withColumn("standardized_found_cui", normalize_code(F.col("FOUND_CUI"), "SNOMED")) \
                     .withColumn("found_prefix3", F.substring(normalize_code(F.col("FOUND_CUI"), "SNOMED"), 1, 3))

    # Load reference tables
    icd_map = spark.table("3_lookup.trud.maps_icdsctmap").select("SCUI", "TCUI")
    opcs_map = spark.table("3_lookup.trud.maps_opcssctmap").select("SCUI", "TCUI")
    snomed_terms = spark.table("3_lookup.trud.snomed_sct")
    omop_concepts = spark.table("3_lookup.omop.concept")
    concept_relationship = spark.table("3_lookup.omop.concept_relationship")
    
    # Prepare symmetric relationships
    symmetric_rels = prepare_symmetric_relationships(concept_relationship)
    symmetric_rels = materialize(symmetric_rels, "4_prod.tmp.snomed_symmetric_rels_temp")

    # Precompute standardized columns for maps
    icd_map = icd_map.withColumn("std_scui", normalize_code(F.col("SCUI"), "ICD10")) \
                     .withColumn("prefix3", F.substring(normalize_code(F.col("SCUI"), "ICD10"), 1, 3))
    
    opcs_map = opcs_map.withColumn("std_scui", normalize_code(F.col("SCUI"), "OPCS4")) \
                      .withColumn("prefix3", F.substring(normalize_code(F.col("SCUI"), "OPCS4"), 1, 3))
    
    icd_map_broadcast = F.broadcast(icd_map)
    opcs_map_broadcast = F.broadcast(opcs_map)

    # Filter for records needing mapping
    icd10_base = base_df.filter((F.col("SOURCE_VOCABULARY_CD").isin(2976507, 647081)) & 
                                (F.col("original_snomed_code").isNull()))
    opcs4_base = base_df.filter((F.col("SOURCE_VOCABULARY_CD") == 685812) & 
                                (F.col("original_snomed_code").isNull()))

    # Process direct matches
    icd10_direct = icd10_base.join(icd_map_broadcast, 
                                   F.col("standardized_found_cui") == F.col("std_scui"), "left") \
                             .select(icd10_base["*"], icd_map_broadcast.SCUI, 
                                    F.col("TCUI").alias("matched_snomed_code"))
    
    opcs4_direct = opcs4_base.join(opcs_map_broadcast, 
                                   F.col("standardized_found_cui") == F.col("std_scui"), "left") \
                             .select(opcs4_base["*"], opcs_map_broadcast.SCUI, 
                                    F.col("TCUI").alias("matched_snomed_code"))
    
    direct_matches = icd10_direct.unionByName(opcs4_direct, allowMissingColumns=True)
    
    # Add OMOP preference using generic function
    direct_with_omop = add_omop_preference_generic(
        direct_matches, "SCUI", "matched_snomed_code",
        ["ICD10", "ICD10CM", "OPCS4"], ["SNOMED"],
        omop_concepts, symmetric_rels
    )

    # Process prefix matches
    icd10_prefix = icd10_base.join(icd_map_broadcast, 
                                   F.col("found_prefix3") == F.col("prefix3"), "left") \
                             .select(icd10_base["*"], icd_map_broadcast.SCUI, 
                                    F.col("TCUI").alias("matched_snomed_code"))
    
    opcs4_prefix = opcs4_base.join(opcs_map_broadcast, 
                                   F.col("found_prefix3") == F.col("prefix3"), "left") \
                             .select(opcs4_base["*"], opcs_map_broadcast.SCUI, 
                                    F.col("TCUI").alias("matched_snomed_code"))
    
    prefix_matches = icd10_prefix.unionByName(opcs4_prefix, allowMissingColumns=True)
    
    prefix_with_omop = add_omop_preference_generic(
        prefix_matches, "SCUI", "matched_snomed_code",
        ["ICD10", "ICD10CM", "OPCS4"], ["SNOMED"],
        omop_concepts, symmetric_rels
    )

    # Process native codes - use consistent column naming
    native_codes = base_df.filter(F.col("original_snomed_code").isNotNull()) \
                          .select(F.col("NOMENCLATURE_ID"), 
                                 F.col("original_snomed_code").alias("temp_snomed_code"), 
                                 F.lit(1).alias("temp_snomed_code_MATCH_COUNT"), 
                                 F.lit(False).alias("HAS_OMOP_MAP"), 
                                 F.lit("NATIVE").alias("temp_snomed_code_TYPE"))
    
    # Use generic functions for processing matches
    direct_final = select_best_match(direct_with_omop, "NOMENCLATURE_ID", "matched_snomed_code", 
                                    "HAS_OMOP_MAP", "temp_snomed_code", "EXACT", "OMOP_ASSISTED")
    prefix_final = select_best_match(prefix_with_omop, "NOMENCLATURE_ID", "matched_snomed_code", 
                                    "HAS_OMOP_MAP", "temp_snomed_code", "PREFIX", "PREFIX_OMOP_ASSISTED")
    
    # Combine all matches - use actual column names
    all_matches = combine_matches_generic("temp_snomed_code", "temp_snomed_code_TYPE", "temp_snomed_code_MATCH_COUNT",
                                         native_codes, direct_final, prefix_final)
    
    # Clean up temporary columns
    base_df_clean = base_df.drop("original_snomed_code", "standardized_found_cui", "found_prefix3")
    
    if all_matches is None:
        # Clean up temp table before returning
        spark.sql("DROP TABLE IF EXISTS `4_prod.tmp.snomed_symmetric_rels_temp`")
        return base_df_clean.withColumn("SNOMED_CODE", F.lit(None).cast(StringType())) \
                           .withColumn("SNOMED_TYPE", F.lit(None).cast(StringType())) \
                           .withColumn("SNOMED_MATCH_COUNT", F.lit(None).cast(IntegerType())) \
                           .withColumn("SNOMED_TERM", F.lit(None).cast(StringType()))

    # Add terms and process secondary mappings
    final_df_with_terms = add_terms_generic(all_matches, snomed_terms, "temp_snomed_code", 
                                           "SNOMED_TERM", base_df_clean)
    
    # Rename all temp_snomed_code columns to SNOMED columns
    final_df_with_terms = final_df_with_terms.withColumnRenamed("temp_snomed_code", "SNOMED_CODE") \
                                             .withColumnRenamed("temp_snomed_code_TYPE", "SNOMED_TYPE") \
                                             .withColumnRenamed("temp_snomed_code_MATCH_COUNT", "SNOMED_MATCH_COUNT")
    
    final_df = process_secondary_mappings_generic(
        final_df_with_terms, omop_concepts, symmetric_rels,
        "SNOMED_CODE", "SNOMED_TYPE", "SNOMED_MATCH_COUNT", "SNOMED_TERM",
        ["SNOMED"], "OMOP_DERIVED"
    )
    
    # Materialize the result before cleaning up temp tables to avoid lazy evaluation issues
    final_df = materialize(final_df, "4_prod.tmp.snomed_mapping_result_temp")
    
    # NOW we can safely clean up temp tables
    spark.sql("DROP TABLE IF EXISTS `4_prod.tmp.snomed_symmetric_rels_temp`")
    spark.sql("DROP TABLE IF EXISTS `4_prod.tmp.snomed_mapping_result_temp`")
    
    return final_df


# COMMAND ----------


# =============================================================================
# ICD10 MAPPING
# =============================================================================

def create_icd10_mapping_incr():
    """Creates an incremental ICD10 mapping table with optimizations."""
    max_adc_updt = get_max_timestamp("4_prod.tmp.tempthree_nomenclature")
    print(f"Processing records updated after: {max_adc_updt}")
    
    base_df = spark.table("4_prod.tmp.temptwo_nomenclature").filter(col("ADC_UPDT") > max_adc_updt)
    
    if base_df.isEmpty():
        print("No new records to process for ICD10 mapping.")
        return None
    
    # Get unique SNOMED codes for pre-filtering
    snomed_keys = base_df.select("SNOMED_CODE").distinct()
    
    # Load and pre-filter reference tables
    icd_map_full = spark.table("3_lookup.trud.maps_icdsctmap")
    icd_map = icd_map_full.alias("im").join(
        broadcast(snomed_keys), 
        col("im.TCUI") == col("SNOMED_CODE"), 
        "inner"
    ).select("im.*")
    
    snomed_hier_full = spark.table("3_lookup.trud.snomed_scthier")
    snomed_hier_child = snomed_hier_full.alias("sh").join(
        broadcast(snomed_keys),
        col("sh.CHILD") == col("SNOMED_CODE"),
        "inner"
    ).select("sh.*")
    
    snomed_hier_parent = snomed_hier_full.alias("sh").join(
        broadcast(snomed_keys),
        col("sh.PARENT") == col("SNOMED_CODE"),
        "inner"
    ).select("sh.*")
    
    # Load other reference tables
    icd_terms = spark.table("3_lookup.trud.maps_icd")
    code_value = spark.table("3_lookup.mill.mill_code_value")
    omop_concepts = spark.table("3_lookup.omop.concept").filter(col("invalid_reason").isNull())
    concept_relationship = spark.table("3_lookup.omop.concept_relationship")
    
    # Prepare symmetric relationships
    symmetric_rels = prepare_symmetric_relationships(concept_relationship)
    symmetric_rels = materialize(symmetric_rels, "4_prod.tmp.icd10_symmetric_rels_temp")
    
    # Process direct matches
    direct_matches = base_df.alias("b").join(
        icd_map.alias("im"), 
        col("b.SNOMED_CODE") == col("im.TCUI"), 
        "left"
    ).filter(col("im.SCUI").isNotNull())
    
    direct_with_omop = add_omop_preference_generic(
        direct_matches, "SNOMED_CODE", "SCUI",
        ["SNOMED"], ["ICD10", "ICD10CM"],
        omop_concepts, symmetric_rels
    )
    direct_final = select_best_match(direct_with_omop, "NOMENCLATURE_ID", "SCUI", 
                                    "HAS_OMOP_MAP", "ICD10_CODE", "EXACT", "OMOP_ASSISTED")
    
    # Process child matches
    child_matches = base_df.alias("b").join(
        snomed_hier_child.alias("sh"),
        col("b.SNOMED_CODE") == col("sh.CHILD"),
        "left"
    ).join(
        icd_map.alias("im"),
        col("sh.PARENT") == col("im.TCUI"),
        "left"
    ).filter(col("im.SCUI").isNotNull())
    
    child_with_omop = add_omop_preference_generic(
        child_matches, "PARENT", "SCUI",
        ["SNOMED"], ["ICD10", "ICD10CM"],
        omop_concepts, symmetric_rels
    )
    child_final = select_best_match(child_with_omop, "NOMENCLATURE_ID", "SCUI", 
                                   "HAS_OMOP_MAP", "ICD10_CODE", "CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED")
    
    # Process parent matches
    parent_matches = base_df.alias("b").join(
        snomed_hier_parent.alias("sh"),
        col("b.SNOMED_CODE") == col("sh.PARENT"),
        "left"
    ).join(
        icd_map.alias("im"),
        col("sh.CHILD") == col("im.TCUI"),
        "left"
    ).filter(col("im.SCUI").isNotNull())
    
    parent_with_omop = add_omop_preference_generic(
        parent_matches, "CHILD", "SCUI",
        ["SNOMED"], ["ICD10", "ICD10CM"],
        omop_concepts, symmetric_rels
    )
    parent_final = select_best_match(parent_with_omop, "NOMENCLATURE_ID", "SCUI", 
                                    "HAS_OMOP_MAP", "ICD10_CODE", "PARENTMATCH", "PARENTMATCH_OMOP_ASSISTED")
    
    # Process native codes
    icd10_code_values = code_value.filter(col("CDF_MEANING").isin("ICD10", "ICD10WHO"))
    native_codes = process_native_codes_generic(base_df, icd10_code_values, "ICD10_CODE", 
                                               "SOURCE_VOCABULARY_CD", "ICD10WHO")
    
    # Combine all matches - use actual column names
    all_matches = combine_matches_generic("ICD10_CODE", "ICD10_CODE_TYPE", "ICD10_CODE_MATCH_COUNT",
                                         native_codes, direct_final, child_final, parent_final)
    
    if all_matches is None:
        # Clean up temp table before returning
        spark.sql("DROP TABLE IF EXISTS `4_prod.tmp.icd10_symmetric_rels_temp`")
        return base_df.withColumn("ICD10_CODE", lit(None).cast(StringType())) \
                     .withColumn("ICD10_TYPE", lit(None).cast(StringType())) \
                     .withColumn("ICD10_MATCH_COUNT", lit(None).cast(IntegerType())) \
                     .withColumn("ICD10_TERM", lit(None).cast(StringType()))
    
    # Add terms
    final_df_with_terms = add_terms_generic(all_matches, icd_terms, "ICD10_CODE", 
                                           "ICD10_TERM", base_df)
    
    # Process secondary mappings
    final_df = process_secondary_mappings_generic(
        final_df_with_terms, omop_concepts, symmetric_rels,
        "ICD10_CODE", "ICD10_CODE_TYPE", "ICD10_CODE_MATCH_COUNT", "ICD10_TERM",
        ["ICD10", "ICD10CM"], "OMOP_DERIVED"
    )
    
    # Materialize the result before cleaning up temp tables
    final_df = materialize(final_df, "4_prod.tmp.icd10_mapping_result_temp")
    
    # NOW we can safely clean up temp tables
    spark.sql("DROP TABLE IF EXISTS `4_prod.tmp.icd10_symmetric_rels_temp`")
    spark.sql("DROP TABLE IF EXISTS `4_prod.tmp.icd10_mapping_result_temp`")
    
    return final_df

# COMMAND ----------


# =============================================================================
# OPCS4 MAPPING
# =============================================================================

def create_opcs4_mapping_incr():
    """Creates an incremental OPCS4 mapping table with optimizations."""
    max_adc_updt = get_max_timestamp("4_prod.tmp.tempfour_nomenclature")
    print(f"Processing records updated after: {max_adc_updt}")
    
    base_df = spark.table("4_prod.tmp.tempthree_nomenclature").filter(
        F.col("ADC_UPDT") > max_adc_updt
    )
    
    if base_df.isEmpty():
        print("No new records to process for OPCS4 mapping.")
        return None
    
    # Pre-cast SNOMED_CODE and FOUND_CUI
    base_df = base_df.withColumn(
        "SNOMED_CODE_STR", F.col("SNOMED_CODE").cast(StringType())
    ).withColumn(
        "FOUND_CUI_STR", F.col("FOUND_CUI").cast(StringType())
    )
    
    print("Loading reference tables...")
    
    # OPCS mapping table
    opcs_map = spark.table("3_lookup.trud.maps_opcssctmap").select(
        F.col("TCUI").cast(StringType()).alias("TCUI"),
        F.col("SCUI").cast(StringType()).alias("SCUI")
    )
    
    # SNOMED hierarchy - pre-filter
    base_snomed_codes = base_df.select("SNOMED_CODE_STR").distinct()
    snomed_hier = spark.table("3_lookup.trud.snomed_scthier").select(
        F.col("PARENT").cast(StringType()).alias("PARENT"),
        F.col("CHILD").cast(StringType()).alias("CHILD")
    ).join(
        F.broadcast(base_snomed_codes),
        (F.col("PARENT") == F.col("SNOMED_CODE_STR")) | (F.col("CHILD") == F.col("SNOMED_CODE_STR")),
        "left_semi"
    )
    
    # Materialize hierarchy for reuse
    snomed_hier = materialize(snomed_hier, "4_prod.tmp.opcs4_snomed_hier_temp")
    
    # OPCS terms
    opcs_terms = spark.table("3_lookup.trud.maps_opcs").select(
        F.col("CUI").cast(StringType()).alias("CUI"),
        "TERM"
    )
    
    # Code values for OPCS4
    opcs4_code_values = spark.table("3_lookup.mill.mill_code_value").filter(
        F.col("CDF_MEANING") == "OPCS4"
    ).select("CODE_VALUE")
    
    # OMOP concepts
    omop_concepts = spark.table("3_lookup.omop.concept").filter(
        F.col("invalid_reason").isNull()
    )
    
    # Prepare symmetric relationships
    concept_relationship = spark.table("3_lookup.omop.concept_relationship")
    symmetric_rels = prepare_symmetric_relationships(concept_relationship)
    symmetric_rels = materialize(symmetric_rels, "4_prod.tmp.opcs4_symmetric_rels_temp")
    
    print("Processing direct matches...")
    # Direct matches
    direct_matches = base_df.alias("b").join(
        opcs_map.alias("om"),
        F.col("b.SNOMED_CODE_STR") == F.col("om.TCUI"),
        "left"
    ).filter(
        F.col("om.SCUI").isNotNull()
    ).select(
        "b.*",
        F.col("om.SCUI")
    )
    
    direct_with_omop = add_omop_preference_generic(
        direct_matches, "SNOMED_CODE", "SCUI",
        ["SNOMED", "SNOMED CT"], ["OPCS4"],
        omop_concepts, symmetric_rels
    )
    direct_final = select_best_match(direct_with_omop, "NOMENCLATURE_ID", "SCUI",
                                    "HAS_OMOP_MAP", "OPCS4_CODE", "EXACT", "OMOP_ASSISTED")
    
    print("Processing child matches...")
    # Child matches
    child_matches = base_df.alias("b").join(
        snomed_hier.alias("sh"),
        F.col("b.SNOMED_CODE_STR") == F.col("sh.CHILD"),
        "inner"
    ).join(
        opcs_map.alias("om"),
        F.col("sh.PARENT") == F.col("om.TCUI"),
        "inner"
    ).filter(
        F.col("om.SCUI").isNotNull()
    ).select(
        "b.*",
        F.col("sh.PARENT"),
        F.col("om.SCUI")
    )
    
    child_with_omop = add_omop_preference_generic(
        child_matches, "PARENT", "SCUI",
        ["SNOMED", "SNOMED CT"], ["OPCS4"],
        omop_concepts, symmetric_rels
    )
    child_final = select_best_match(child_with_omop, "NOMENCLATURE_ID", "SCUI",
                                   "HAS_OMOP_MAP", "OPCS4_CODE", "CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED")
    
    print("Processing parent matches...")
    # Parent matches
    parent_matches = base_df.alias("b").join(
        snomed_hier.alias("sh"),
        F.col("b.SNOMED_CODE_STR") == F.col("sh.PARENT"),
        "inner"
    ).join(
        opcs_map.alias("om"),
        F.col("sh.CHILD") == F.col("om.TCUI"),
        "inner"
    ).filter(
        F.col("om.SCUI").isNotNull()
    ).select(
        "b.*",
        F.col("sh.CHILD"),
        F.col("om.SCUI")
    )
    
    parent_with_omop = add_omop_preference_generic(
        parent_matches, "CHILD", "SCUI",
        ["SNOMED", "SNOMED CT"], ["OPCS4"],
        omop_concepts, symmetric_rels
    )
    parent_final = select_best_match(parent_with_omop, "NOMENCLATURE_ID", "SCUI",
                                    "HAS_OMOP_MAP", "OPCS4_CODE", "PARENTMATCH", "PARENTMATCH_OMOP_ASSISTED")
    
    print("Processing native codes...")
    # Process native codes
    native_codes = process_native_codes_generic(base_df, opcs4_code_values, "OPCS4_CODE",
                                               "SOURCE_VOCABULARY_CD", "OPCS4")
    
    print("Combining all matches...")
    # Combine all matches - use actual column names
    all_matches = combine_matches_generic("OPCS4_CODE", "OPCS4_CODE_TYPE", "OPCS4_CODE_MATCH_COUNT",
                                         native_codes, direct_final, child_final, parent_final)
    
    # Materialize combined matches
    if all_matches is not None:
        all_matches = materialize(all_matches, "4_prod.tmp.opcs4_all_matches_temp")
    
    print("Adding OPCS4 terms...")
    # Add terms
    final_df_with_terms = add_terms_generic(all_matches, opcs_terms, "OPCS4_CODE",
                                           "OPCS4_TERM", base_df)
    
    print("Processing secondary OMOP mappings...")
    # Process secondary mappings
    final_df = process_secondary_mappings_generic(
        final_df_with_terms, omop_concepts, symmetric_rels,
        "OPCS4_CODE", "OPCS4_CODE_TYPE", "OPCS4_CODE_MATCH_COUNT", "OPCS4_TERM",
        ["OPCS4"], "OMOP_DERIVED"
    )
    
    # Remove temporary pre-cast columns
    columns_to_drop = ["SNOMED_CODE_STR", "FOUND_CUI_STR"]
    for col in columns_to_drop:
        if col in final_df.columns:
            final_df = final_df.drop(col)
    
    # Ensure we keep the most recent record per NOMENCLATURE_ID
    window_spec = Window.partitionBy("NOMENCLATURE_ID").orderBy(F.col("ADC_UPDT").desc())
    final_df = final_df.withColumn("rn", F.row_number().over(window_spec)) \
                      .filter(F.col("rn") == 1) \
                      .drop("rn")
    
    # Materialize the result before cleaning up temp tables to avoid lazy evaluation issues
    final_df = materialize(final_df, "4_prod.tmp.opcs4_mapping_result_temp")
    
    # NOW we can safely clean up all intermediate temp tables
    spark.sql("DROP TABLE IF EXISTS `4_prod.tmp.opcs4_snomed_hier_temp`")
    spark.sql("DROP TABLE IF EXISTS `4_prod.tmp.opcs4_symmetric_rels_temp`")
    spark.sql("DROP TABLE IF EXISTS `4_prod.tmp.opcs4_all_matches_temp`")
    spark.sql("DROP TABLE IF EXISTS `4_prod.tmp.opcs4_mapping_result_temp`")
    
    return final_df

# COMMAND ----------


# =============================================================================
# FINAL NOMENCLATURE AND TERTIARY MAPPINGS
# =============================================================================

def get_preferred_domains(source_type):
    """Returns preferred OMOP domains based on source vocabulary type."""
    domain_preferences = {
        "SNOMED": ["Condition", "Procedure", "Observation", "Measurement"],
        "ICD10": ["Condition"],
        "OPCS4": ["Procedure"]
    }
    return domain_preferences.get(source_type, [])

def process_code_to_omop_mapping(candidates_df, source_code_col, concepts_df, relationships_df, source_type):
    """Helper function to map a specific code type to OMOP, selecting the best match."""
    
    # Normalize the source code for matching
    normalized_source = normalize_code(col(source_code_col).cast(StringType()), source_type)
    normalized_concept = normalize_code(col("source_concept_code").cast(StringType()), source_type)
    
    # Join with concepts first to get source concept IDs
    source_matches = candidates_df.join(
        concepts_df.select(
            col("concept_id").alias("source_concept_id"),
            col("concept_code").alias("source_concept_code")
        ),
        normalized_source == normalized_concept,
        "inner"
    )
    
    # Join with relationships (only "Maps to" direction for correctness)
    with_relationships = source_matches.join(
        relationships_df.select(
            col("concept_id_1").alias("rel_source_id"),
            col("concept_id_2").alias("rel_target_id")
        ),
        col("source_concept_id") == col("rel_source_id"),
        "inner"
    ).join(
        concepts_df.select(
            col("concept_id").alias("target_concept_id"),
            col("concept_name").alias("target_concept_name"),
            col("standard_concept").alias("target_standard"),
            col("domain_id").alias("target_domain"),
            col("concept_class_id").alias("target_class")
        ),
        col("rel_target_id") == col("target_concept_id"),
        "inner"
    )
    
    # Improved window ordering with deterministic tie-breaking
    preferred_domains = get_preferred_domains(source_type)
    window_spec = Window.partitionBy("NOMENCLATURE_ID").orderBy(
        when(col("target_standard") == "S", 1).otherwise(0).desc(),
        when(col("target_domain").isin(preferred_domains), 1).otherwise(0).desc(),
        col("target_class"),
        col("target_concept_id")
    )
    
    return with_relationships.withColumn("row_num", row_number().over(window_spec)) \
                             .filter(col("row_num") == 1) \
                             .select(
                                 "NOMENCLATURE_ID",
                                 col("target_concept_id").alias("OMOP_CONCEPT_ID"),
                                 col("target_concept_name").alias("OMOP_CONCEPT_NAME")
                             )

def process_tertiary_omop_mappings(df, omop_concepts, concept_relationship):
    """Processes tertiary OMOP mappings efficiently for records without existing OMOP mappings."""
    
    # Filter valid concepts and relationships once
    valid_concepts = omop_concepts.filter(col("invalid_reason").isNull()) \
                                  .select("concept_id", "concept_code", "vocabulary_id", 
                                          "standard_concept", "domain_id", "concept_name", "concept_class_id")
    
    # Only use "Maps to" relationship for correctness
    valid_relationships = concept_relationship.filter(col("relationship_id") == "Maps to") \
                                              .select("concept_id_1", "concept_id_2")
    
    # Identify records needing mapping once
    need_mapping_df = df.filter(col("OMOP_CONCEPT_ID").isNull())
    
    if need_mapping_df.isEmpty():
        return df
    
    result_df = df
    
    # Process each code type
    for code_type, code_col, vocab_filter in [
        ("SNOMED", "SNOMED_CODE", col("vocabulary_id") == "SNOMED"),
        ("ICD10", "ICD10_CODE", col("vocabulary_id").isin(["ICD10", "ICD10CM"])),
        ("OPCS4", "OPCS4_CODE", col("vocabulary_id") == "OPCS4")
    ]:
        # Get candidates from the pre-filtered set
        candidates = need_mapping_df.filter(col(code_col).isNotNull()) \
                                   .select("NOMENCLATURE_ID", code_col)
        
        # Skip if no candidates
        if candidates.isEmpty():
            continue
        
        # Filter concepts for this vocabulary once
        vocab_concepts = valid_concepts.filter(vocab_filter)
        
        # If small enough, broadcast the filtered concepts
        vocab_concepts_count = vocab_concepts.count()
        if vocab_concepts_count < 100000:
            vocab_concepts = broadcast(vocab_concepts)
        
        # Get mappings
        mappings = process_code_to_omop_mapping(
            candidates, 
            code_col, 
            vocab_concepts, 
            valid_relationships, 
            code_type
        )
        
        # Update result with coalesce to preserve existing values
        result_df = result_df.join(mappings.alias("new"), "NOMENCLATURE_ID", "left_outer") \
                             .select(
                                 *[c for c in result_df.columns if c not in ["OMOP_CONCEPT_ID", "OMOP_CONCEPT_NAME"]],
                                 coalesce(result_df.OMOP_CONCEPT_ID, col("new.OMOP_CONCEPT_ID")).alias("OMOP_CONCEPT_ID"),
                                 coalesce(result_df.OMOP_CONCEPT_NAME, col("new.OMOP_CONCEPT_NAME")).alias("OMOP_CONCEPT_NAME")
                             )
        
        # Update need_mapping_df for next iteration (only unmapped records)
        need_mapping_df = result_df.filter(col("OMOP_CONCEPT_ID").isNull())
    
    return result_df

def create_final_nomenclature_incr():
    """
    Creates the final incremental nomenclature table with tertiary OMOP mappings.
    """
    max_adc_updt = get_max_timestamp("4_prod.bronze.nomenclature")
    print(f"Processing records updated after: {max_adc_updt}")

    # Get incremental records
    base_df = spark.table("4_prod.tmp.tempfour_nomenclature").filter(col("ADC_UPDT") > max_adc_updt)
    
    if base_df.isEmpty():
        print("No new records to process for finalization.")
        return None
    
    # Load OMOP tables once
    omop_concepts = spark.table("3_lookup.omop.concept")
    concept_relationship = spark.table("3_lookup.omop.concept_relationship")
    
    # Process mappings
    final_df = process_tertiary_omop_mappings(base_df, omop_concepts, concept_relationship)
    
    return final_df

def replicate_to_lookup_incremental(source_table, target_table, updates_df):
    """
    Incrementally replicate changes to lookup table instead of full overwrite.
    """
    if not spark.catalog.tableExists(target_table):
        # Initial load - copy full table
        print(f"Initial replication of {source_table} to {target_table}")
        spark.table(source_table).write \
             .format("delta") \
             .mode("overwrite") \
             .option("overwriteSchema", "true") \
             .saveAsTable(target_table)
    else:
        # Incremental update using merge
        print(f"Incremental replication to {target_table}")
        delta_table = DeltaTable.forName(spark, target_table)
        merge_condition = "target.NOMENCLATURE_ID = source.NOMENCLATURE_ID"
        all_columns = updates_df.columns
        update_columns = {c: f"source.{c}" for c in all_columns if c != "NOMENCLATURE_ID"}
        insert_columns = {c: f"source.{c}" for c in all_columns}
        
        (delta_table.alias("target")
            .merge(updates_df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_columns)
            .whenNotMatchedInsert(values=insert_columns)
            .execute())

# COMMAND ----------


# =============================================================================
# MAIN EXECUTION
# =============================================================================

try:
    # Process initial OMOP mapping
    updates_df = create_nomenclature_mapping_incr()
    update_nomen_table(
        updates_df, 
        "4_prod.tmp.tempone_nomenclature",
        primary_keys=["NOMENCLATURE_ID"]
    )
except Exception as e:
    print(f"Error in mapping process: {str(e)}")
    raise

# Process SNOMED mapping
updates_df = create_snomed_mapping_incr()
if updates_df is not None:
    update_nomen_table(updates_df, "4_prod.tmp.temptwo_nomenclature", primary_keys=["NOMENCLATURE_ID"])
else:
    print("No updates to process.")

# Process ICD10 mapping
updates_df = create_icd10_mapping_incr()
update_nomen_table(updates_df, "4_prod.tmp.tempthree_nomenclature")

# Process OPCS4 mapping
print("Starting OPCS4 mapping process...")
opcs_updates_df = create_opcs4_mapping_incr()

if opcs_updates_df is not None:
    # Materialize to break complex lineage before update
    temp_table = "4_prod.tmp.opcs4_updates_temp"
    spark.sql(f"DROP TABLE IF EXISTS `{temp_table}`")
    
    opcs_updates_df.write.mode("overwrite").saveAsTable(temp_table)
    materialized_df = spark.table(temp_table)
    
    record_count = materialized_df.count()
    print(f"Processing {record_count} records...")
    
    update_nomen_table(materialized_df, "4_prod.tmp.tempfour_nomenclature")
    
    spark.sql(f"DROP TABLE IF EXISTS `{temp_table}`")
    print("OPCS4 mapping completed successfully.")
else:
    print("No updates to process.")

# Finalization and replication
print("Starting finalization and replication process...")

updates_df = create_final_nomenclature_incr()

if updates_df is not None:
    temp_table = "4_prod.tmp.final_nomenclature_temp"
    spark.sql(f"DROP TABLE IF EXISTS `{temp_table}`")
    
    updates_df.write.mode("overwrite").saveAsTable(temp_table)
    materialized_updates = spark.table(temp_table)
    
    try:
        has_updates = materialized_updates.first() is not None
    except:
        has_updates = False
    
    if has_updates:
        update_nomen_table(materialized_updates, "4_prod.bronze.nomenclature")
        
        print("Replicating nomenclature updates to lookup location...")
        replicate_to_lookup_incremental(
            "4_prod.bronze.nomenclature",
            "3_lookup.mill.map_nomenclature",
            materialized_updates
        )
        print("Successfully completed finalization and replication.")
    
    spark.sql(f"DROP TABLE IF EXISTS `{temp_table}`")
else:
    print("No updates were made, skipping replication.")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE 4_prod.tmp.tempone_nomenclature CLUSTER BY (NOMENCLATURE_ID);
# MAGIC OPTIMIZE 4_prod.tmp.tempone_nomenclature;
# MAGIC ALTER TABLE 4_prod.tmp.temptwo_nomenclature CLUSTER BY (NOMENCLATURE_ID);
# MAGIC OPTIMIZE 4_prod.tmp.temptwo_nomenclature;
# MAGIC ALTER TABLE 4_prod.tmp.tempthree_nomenclature CLUSTER BY (NOMENCLATURE_ID);
# MAGIC OPTIMIZE 4_prod.tmp.tempthree_nomenclature;
# MAGIC ALTER TABLE 4_prod.bronze.nomenclature CLUSTER BY (NOMENCLATURE_ID);
# MAGIC OPTIMIZE 4_prod.bronze.nomenclature;
# MAGIC ALTER TABLE 3_lookup.mill.map_nomenclature CLUSTER BY (NOMENCLATURE_ID);
# MAGIC OPTIMIZE 3_lookup.mill.map_nomenclature;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
