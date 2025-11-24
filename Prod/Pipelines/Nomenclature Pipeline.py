# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %pip install openai

# COMMAND ----------

# MAGIC %restart_python

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
from scipy.spatial.distance import cosine
import numpy as np
from pyspark.ml.functions import vector_to_array
from openai import OpenAI
from pyspark.sql import SparkSession
import time

# COMMAND ----------

def calculate_cosine_similarity_udf(vec1, vec2):
    """Calculate cosine similarity between two vectors for use in UDF"""
    if vec1 is None or vec2 is None:
        return None
    if len(vec1) == 0 or len(vec2) == 0:
        return None
    try:
        arr1 = np.array(vec1)
        arr2 = np.array(vec2)
        similarity = float(1 - cosine(arr1, arr2))
        return similarity
    except Exception as e:
        return None

def fq_name(qualified):
    """Return properly formatted fully qualified table name for DROP statements."""
    parts = qualified.split(".")
    if len(parts) != 3:
        return qualified
    return f"{parts[0]}.{parts[1]}.{parts[2]}"

def cosine_sim(vec1, vec2):
    """Calculate cosine similarity between two vectors safely."""
    if vec1 is None or vec2 is None:
        return None
    if len(vec1) == 0 or len(vec2) == 0:
        return None
    try:
        a = np.array(vec1, dtype=float)
        b = np.array(vec2, dtype=float)
        denom = np.linalg.norm(a) * np.linalg.norm(b)
        if denom == 0:
            return None
        return float(np.dot(a, b) / denom)
    except Exception:
        return None

similarity_udf = udf(cosine_sim, DoubleType())


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
    
    # Keep the latest row per PK
    cols = source_df.columns
    if "ADC_UPDT" not in cols:
        raise ValueError("ADC_UPDT column required for update-if-newer logic")
    
    max_struct = F.struct(F.col("ADC_UPDT"), *[F.col(c) for c in cols if c != "ADC_UPDT"])
    latest = (source_df
              .groupBy(*primary_keys)
              .agg(F.max(max_struct).alias("_r"))
              .select(
                  *primary_keys,
                  *[F.col(f"_r.{c}").alias(c) for c in cols if c not in primary_keys]
              ))
    
    latest = latest.repartition(400, *[F.col(k) for k in primary_keys])
    
    # Stage as a short-lived Delta table
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
            
            # Use all columns for update and insert
            update_cols = {c: f"s.{c}" for c in src.columns if c not in primary_keys}
            insert_cols = {c: f"s.{c}" for c in src.columns}
            
            # Perform merge with schema evolution enabled
            (delta_t.alias("t")
                .merge(src.alias("s"), on)
                .whenMatchedUpdate(
                    condition="s.ADC_UPDT > t.ADC_UPDT",
                    set=update_cols
                )
                .whenNotMatchedInsert(values=insert_cols)
                .execute())
            
        else:
            spark.table(stage_tbl).write.format("delta").mode("overwrite").saveAsTable(target_table)
        
        print(f"Updated {target_table} from {n} staged rows")
    
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {fq_name(stage_tbl)}")  

# COMMAND ----------


# =============================================================================
# GENERIC HELPER FUNCTIONS
# =============================================================================
def add_similarity_scores(df, embeddings_table="3_lookup.embeddings.terms"):  
    print(f"Loading embeddings from {embeddings_table}...")  
    embeddings_df = spark.table(embeddings_table)  
  
    # Only keep rows with non-null embeddings to avoid vector_to_array(null)  
    embeddings_prepared = (  
        embeddings_df  
        .filter(F.col("embedding_vector").isNotNull())  
        .select(  
            F.lower(F.trim(F.col("term"))).alias("term_lower"),  
            F.when(  
                F.col("embedding_vector").cast(ArrayType(DoubleType())).isNotNull(),  
                F.col("embedding_vector").cast(ArrayType(DoubleType()))  
            ).otherwise(  
                vector_to_array(F.col("embedding_vector"))  # pass Column, not string  
            ).alias("embedding_vector")  
        )  
        .dropDuplicates(["term_lower"])  
    )  
  
    # Optional: broadcast if small  
    try:  
        emb_count = embeddings_prepared.count()  
        if emb_count < 1000000:  
            embeddings_prepared = F.broadcast(embeddings_prepared)  
            print(f"Broadcasting {emb_count} embeddings...")  
    except:  
        pass  
  
    print("Joining embeddings for SOURCE_STRING...")  
    result_df = df.join(  
        embeddings_prepared.withColumnRenamed("embedding_vector", "source_embedding")  
                          .withColumnRenamed("term_lower", "source_term_lower"),  
        F.lower(F.trim(F.col("SOURCE_STRING"))) == F.col("source_term_lower"),  
        "left"  
    ).drop("source_term_lower")  
  
    print("Joining embeddings for SNOMED_TERM...")  
    result_df = result_df.join(  
        embeddings_prepared.withColumnRenamed("embedding_vector", "snomed_embedding")  
                          .withColumnRenamed("term_lower", "snomed_term_lower"),  
        F.lower(F.trim(F.col("SNOMED_TERM"))) == F.col("snomed_term_lower"),  
        "left"  
    ).drop("snomed_term_lower")  
  
    print("Joining embeddings for ICD10_TERM...")  
    result_df = result_df.join(  
        embeddings_prepared.withColumnRenamed("embedding_vector", "icd10_embedding")  
                          .withColumnRenamed("term_lower", "icd10_term_lower"),  
        F.lower(F.trim(F.col("ICD10_TERM"))) == F.col("icd10_term_lower"),  
        "left"  
    ).drop("icd10_term_lower")  
  
    print("Joining embeddings for OPCS4_TERM...")  
    result_df = result_df.join(  
        embeddings_prepared.withColumnRenamed("embedding_vector", "opcs4_embedding")  
                          .withColumnRenamed("term_lower", "opcs4_term_lower"),  
        F.lower(F.trim(F.col("OPCS4_TERM"))) == F.col("opcs4_term_lower"),  
        "left"  
    ).drop("opcs4_term_lower")  
  
    print("Joining embeddings for OMOP_CONCEPT_NAME...")  
    result_df = result_df.join(  
        embeddings_prepared.withColumnRenamed("embedding_vector", "omop_embedding")  
                          .withColumnRenamed("term_lower", "omop_term_lower"),  
        F.lower(F.trim(F.col("OMOP_CONCEPT_NAME"))) == F.col("omop_term_lower"),  
        "left"  
    ).drop("omop_term_lower")  
  
    print("Calculating cosine similarities...")  
    result_df = (  
        result_df  
        .withColumn("SNOMED_SIMILARITY",  
                    similarity_udf(F.col("source_embedding"), F.col("snomed_embedding")))  
        .withColumn("ICD10_SIMILARITY",  
                    similarity_udf(F.col("source_embedding"), F.col("icd10_embedding")))  
        .withColumn("OPCS4_SIMILARITY",  
                    similarity_udf(F.col("source_embedding"), F.col("opcs4_embedding")))  
        .withColumn("OMOP_SIMILARITY",  
                    similarity_udf(F.col("source_embedding"), F.col("omop_embedding")))  
        .drop("source_embedding", "snomed_embedding", "icd10_embedding",  
              "opcs4_embedding", "omop_embedding")  
    )  
    return result_df  

    
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
    
    match_counts = candidates.join(
        symmetric_rels, 
        F.col("OMOP_CONCEPT_ID") == F.col("source_concept_id"), 
        "inner"
    ).join(
        F.broadcast(targets), 
        F.col("target_concept_id") == F.col("tgt_id"), 
        "inner"
    ).groupBy("NOMENCLATURE_ID").agg(
        F.count("tgt_code").alias("new_match_count")
    )

    # Define window for deterministic best match selection
    window_spec = Window.partitionBy("NOMENCLATURE_ID").orderBy(
    F.col("tgt_id")  # Stable tie-breaker
    )

    # Select best match per NOMENCLATURE_ID
    best_matches = candidates.join(
        symmetric_rels, 
        F.col("OMOP_CONCEPT_ID") == F.col("source_concept_id"), 
        "inner"
    ).join(
        F.broadcast(targets), 
        F.col("target_concept_id") == F.col("tgt_id"), 
        "inner"
    ).withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .select(
        "NOMENCLATURE_ID",
        F.col("tgt_code").alias("new_code"),
        F.col("tgt_name").alias("new_term")
    )

    # Combine best matches with counts
    mapping = best_matches.join(match_counts, "NOMENCLATURE_ID", "inner")
    
    cols_to_keep = [c for c in df.columns if c not in [code_col, type_col, match_count_col, term_col]]
    
    # Check which columns actually exist in the dataframe
    existing_cols = df.columns
    
    result = df.join(mapping, "NOMENCLATURE_ID", "left")
    
    # Build the select list dynamically based on what columns exist
    select_list = cols_to_keep.copy()
    
    # Handle code column
    if code_col in existing_cols:
        select_list.append(
        F.when(F.col("new_code").isNotNull(), F.col("new_code"))
        .otherwise(F.col(code_col))
        .alias(code_col)
        )
    else:
        select_list.append(F.col("new_code").alias(code_col))

    # Handle type column
    if type_col in existing_cols:
        select_list.append(
            F.when(F.col("new_code").isNotNull(), F.lit(new_type))
            .otherwise(F.col(type_col))
            .alias(type_col)
        )
    else:
        select_list.append(F.lit(new_type).alias(type_col))
    
    # Handle match count column
    if match_count_col in existing_cols:
        select_list.append(
        F.when(F.col("new_code").isNotNull(), F.col("new_match_count"))
        .otherwise(F.col(match_count_col))
        .alias(match_count_col))
    else:
        select_list.append(F.col("new_match_count").alias(match_count_col))
    
    # Handle term column
    if term_col in existing_cols:
        select_list.append(
            F.when(F.col("new_code").isNotNull(), F.col("new_term"))
            .otherwise(F.col(term_col))
            .alias(term_col)
        )
    else:
        select_list.append(F.col("new_term").alias(term_col))

    return result.select(*select_list)

def materialize(df, temp_table):
    """Materialize DataFrame to break complex lineage."""
    spark.sql(f"DROP TABLE IF EXISTS {fq_name(temp_table)}") 
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


# Configuration
CATALOG_NAME = "3_lookup"
SCHEMA_NAME = "embeddings"
TABLE_NAME = "terms"
FULL_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"
INDEX_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}_index"
ENDPOINT_NAME = "nomenclature"

# Case standardization (Title Case for consistency)
STANDARDIZE_CASE = True
CASE_FORMAT = "title"  # Options: "title", "upper", "lower"

# Source table
SOURCE_TABLE = "4_prod.bronze.nomenclature"

# Columns to extract terms from
COLUMNS_TO_PROCESS = [
    "source_string",
    "snomed_term", 
    "ICD10_TERM",
    "OPCS4_TERM",
    "OMOP_CONCEPT_NAME"
]

def deduplicate_existing_embeddings():
    """
    Clean up any existing duplicate terms in the embeddings table
    Keep only one version per term (title case)
    """
    print(f"Checking for duplicate terms in {FULL_TABLE_NAME}...")
    
    try:
        # Check if table exists and has duplicates
        check_query = f"""
            SELECT 
                INITCAP(LOWER(TRIM(term))) as term_normalized,
                COUNT(*) as count
            FROM {FULL_TABLE_NAME}
            GROUP BY INITCAP(LOWER(TRIM(term)))
            HAVING COUNT(*) > 1
        """
        
        duplicates_df = spark.sql(check_query).toPandas()
        
        if len(duplicates_df) > 0:
            print(f"  ⚠ Found {len(duplicates_df)} terms with duplicates")
            print(f"  Deduplicating by keeping first occurrence of each term...")
            
            # Use CREATE OR REPLACE to directly replace the table
            dedup_query = f"""
                CREATE OR REPLACE TABLE {FULL_TABLE_NAME} AS
                SELECT 
                    INITCAP(LOWER(TRIM(term))) as term,
                    FIRST(embedding_vector) as embedding_vector,
                    FIRST(model_version) as model_version,
                    FIRST(created_at) as created_at,
                    FIRST(embedded_at) as embedded_at,
                    FIRST(ADC_UPDT) as ADC_UPDT
                FROM {FULL_TABLE_NAME}
                GROUP BY INITCAP(LOWER(TRIM(term)))
            """
            
            spark.sql(dedup_query)
            
            print(f"  ✓ Deduplication complete")
        else:
            print(f"  ✓ No duplicates found")
            
    except Exception as e:
        print(f"  ℹ Could not check for duplicates (table may not exist yet): {e}")

def extract_unique_terms_from_nomenclature():
    """
    Extract all unique terms from the nomenclature table using Spark SQL
    This ensures consistent normalization with the database
    """
    print(f"Extracting unique terms from {SOURCE_TABLE}...")
    print(f"Processing columns: {', '.join(COLUMNS_TO_PROCESS)}")
    print("Standardizing all terms to Title Case using SQL INITCAP...")
    
    # Build a UNION query to get all terms at once with consistent normalization
    union_parts = []
    for column in COLUMNS_TO_PROCESS:
        union_parts.append(f"""
            SELECT DISTINCT 
                INITCAP(LOWER(TRIM({column}))) as term,
                '{column}' as source_column
            FROM {SOURCE_TABLE}
            WHERE {column} IS NOT NULL 
            AND SOURCE_VOCABULARY_CD IN ('673967', '466776237', '2976507', '647081', '685812')
            AND TRIM({column}) != ''
        """)
    
    query = " UNION ".join(union_parts)
    
    try:
        df = spark.sql(query)
        
        # Get counts per column for reporting
        for column in COLUMNS_TO_PROCESS:
            count = df.filter(F.col("source_column") == column).count()
            print(f"  - {column}: {count} unique terms found")
        
        # Get distinct terms
        terms_df = df.select("term").distinct().toPandas()
        unique_terms = sorted(terms_df['term'].tolist())
        
        print(f"\n✓ Total unique terms extracted (title case): {len(unique_terms)}")
        return unique_terms
        
    except Exception as e:
        print(f"  ✗ Error extracting terms: {e}")
        return []

def get_existing_terms():
    """
    Get all existing terms from the embeddings table (normalized to title case)
    """
    try:
        df = spark.sql(f"""
            SELECT DISTINCT term
            FROM {FULL_TABLE_NAME}
        """).toPandas()
        
        existing = set(df['term'].tolist())
        print(f"  ✓ Retrieved {len(existing)} existing terms")
        return existing
    except Exception as e:
        print(f"  ℹ No existing terms found (table may be empty or not exist): {e}")
        return set()

def filter_new_terms(all_terms, existing_terms):
    """
    Filter out terms that already exist
    Since both are now normalized the same way, direct comparison works
    """
    new_terms = [term for term in all_terms if term not in existing_terms]
    return new_terms

def batch_insert_terms_sql(terms_list, batch_size=1000):
    """
    Insert terms in batches using SQL directly (no Python string manipulation)
    """
    total_terms = len(terms_list)
    print(f"Inserting {total_terms} new terms in batches of {batch_size}...")
    
    for i in range(0, total_terms, batch_size):
        batch = terms_list[i:i+batch_size]
        
        # Create a temporary view from the batch
        batch_df = spark.createDataFrame([(term,) for term in batch], ["term"])
        batch_df.createOrReplaceTempView("temp_batch_terms")
        
        # Insert using SQL to avoid any Python string manipulation
        insert_query = f"""
            INSERT INTO {FULL_TABLE_NAME} (term)
            SELECT term FROM temp_batch_terms
        """
        
        try:
            spark.sql(insert_query)
            print(f"  ✓ Inserted batch {i//batch_size + 1}/{(total_terms + batch_size - 1)//batch_size} ({min(i+batch_size, total_terms)}/{total_terms} terms)")
        except Exception as e:
            # Check if it's a duplicate key error
            if "duplicate" in str(e).lower() or "unique" in str(e).lower() or "constraint" in str(e).lower():
                print(f"  ℹ Batch {i//batch_size + 1} had some duplicates (skipped)")
            else:
                print(f"  ✗ Error inserting batch {i//batch_size + 1}: {e}")
    
    print("✓ All terms inserted")

def verify_no_duplicates():
    """
    Final verification that no duplicates exist
    """
    print("\nVerifying no duplicates exist...")
    
    try:
        check_query = f"""
            SELECT 
                term,
                COUNT(*) as count
            FROM {FULL_TABLE_NAME}
            GROUP BY term
            HAVING COUNT(*) > 1
        """
        
        duplicates_df = spark.sql(check_query).toPandas()
        
        if len(duplicates_df) > 0:
            print(f"  ⚠ WARNING: Found {len(duplicates_df)} terms with duplicates after insertion!")
            print("  Top duplicates:")
            print(duplicates_df.head(10))
        else:
            print(f"  ✓ Verification passed - no duplicates found")
            
    except Exception as e:
        print(f"  ✗ Error during verification: {e}")

# COMMAND ----------

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
EMBEDDING_MODEL = "azure_openai_embedding_endpoint"
FULL_TABLE_NAME = "3_lookup.embeddings.terms"

# Initialize OpenAI client
client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url="https://adb-3660342888273328.8.azuredatabricks.net/serving-endpoints"
)

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

def get_embedding(texts, model=EMBEDDING_MODEL):
    """Get embeddings for text(s)"""
    response = client.embeddings.create(
        model=model,
        input=texts,
        encoding_format="float"
    )
    
    if isinstance(texts, str):
        return response.data[0].embedding
    else:
        return [item.embedding for item in response.data]

def process_missing_embeddings(batch_size=250, delay_between_batches=0.2):
    """Find terms without embeddings and generate them, using 'term' as the key."""

    # Find records with missing embeddings - ADD DISTINCT to avoid duplicates
    missing_df = spark.sql(f"""
        SELECT DISTINCT term
        FROM {FULL_TABLE_NAME}
        WHERE embedding_vector IS NULL
           OR SIZE(embedding_vector) = 0
    """)

    missing_records = missing_df.collect()

    if not missing_records:
        print("No terms with missing embeddings found.")
        return 0

    print(f"Found {len(missing_records)} terms with missing embeddings")

    # Process in batches
    processed_count = 0

    for i in range(0, len(missing_records), batch_size):
        batch = missing_records[i:i + batch_size]
        batch_terms = [record['term'] for record in batch]

        print(f"Processing batch {i//batch_size + 1}: {len(batch_terms)} terms...")

        try:
            # Get embeddings
            embeddings = get_embedding(batch_terms)

            # Prepare update data
            update_data = []
            for term, embedding in zip(batch_terms, embeddings):
                update_data.append({
                    "term": term,
                    "embedding_vector": embedding,
                    "model_version": "text-embedding-3-large"
                })

            # Update the table
            update_df = spark.createDataFrame(update_data)

            delta_table = DeltaTable.forName(spark, FULL_TABLE_NAME)

            delta_table.alias("target") \
                .merge(
                    update_df.alias("source"),
                    "target.term = source.term"
                ) \
                .whenMatchedUpdate(set={
                    "embedding_vector": "source.embedding_vector",
                    "model_version": "source.model_version"
                }) \
                .execute()

            processed_count += len(update_data)
            print(f"  Updated {len(update_data)} embeddings")

        except Exception as e:
            print(f"  Error: {e}")

        # Delay between batches
        if i + batch_size < len(missing_records):
            time.sleep(delay_between_batches)

    print(f"\nCompleted: {processed_count} embeddings updated")
    return processed_count

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
        spark.sql(f"DROP TABLE IF EXISTS {fq_name('4_prod.tmp.snomed_symmetric_rels_temp')}")
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
    
    # Now safe to drop only the upstream temp inputs used by the lineage
    spark.sql(f"DROP TABLE IF EXISTS {fq_name('4_prod.tmp.snomed_symmetric_rels_temp')}")
    
    # Do NOT drop snomed_mapping_result_temp here; let the caller drop it after update_nomen_table runs
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
    
    # Combine all matches
    all_matches = combine_matches_generic("ICD10_CODE", "ICD10_CODE_TYPE", "ICD10_CODE_MATCH_COUNT",
                                         native_codes, direct_final, child_final, parent_final)
    
    if all_matches is None:
        # Clean up temp table before returning
        spark.sql(f"DROP TABLE IF EXISTS {fq_name('4_prod.tmp.icd10_symmetric_rels_temp')}")
        return base_df.withColumn("ICD10_CODE", lit(None).cast(StringType())) \
                     .withColumn("ICD10_CODE_TYPE", lit(None).cast(StringType())) \
                     .withColumn("ICD10_CODE_MATCH_COUNT", lit(None).cast(IntegerType())) \
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
    
    # Materialize before cleanup
    final_df = materialize(final_df, "4_prod.tmp.icd10_mapping_result_temp")
    
    # Safe to drop only upstream temp inputs used by lineage
    spark.sql(f"DROP TABLE IF EXISTS {fq_name('4_prod.tmp.icd10_symmetric_rels_temp')}")
    
    # Do NOT drop icd10_mapping_result_temp here
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
    
    # Pre-cast SNOMED_CODE once
    base_df = base_df.withColumn(
        "SNOMED_CODE_STR", F.col("SNOMED_CODE").cast(StringType())
    )
    
    print("Loading and pre-filtering reference tables...")
    
    # Get unique SNOMED codes for pre-filtering
    snomed_keys = base_df.select("SNOMED_CODE_STR").distinct()
    
    # Pre-filter and broadcast OPCS mapping table
    opcs_map_full = spark.table("3_lookup.trud.maps_opcssctmap")
    opcs_map = opcs_map_full.alias("om").join(
        F.broadcast(snomed_keys),
        F.col("om.TCUI").cast(StringType()) == F.col("SNOMED_CODE_STR"),
        "inner"
    ).select(
        F.col("om.TCUI").cast(StringType()).alias("TCUI"),
        F.col("om.SCUI").cast(StringType()).alias("SCUI")
    )
    opcs_map = F.broadcast(opcs_map)
    
    # Split hierarchy into two separate filtered sets
    snomed_hier_full = spark.table("3_lookup.trud.snomed_scthier")
    
    # Child hierarchy: where base SNOMED codes are children
    snomed_hier_child = snomed_hier_full.alias("sh").join(
        F.broadcast(snomed_keys),
        F.col("sh.CHILD").cast(StringType()) == F.col("SNOMED_CODE_STR"),
        "inner"
    ).select(
        F.col("sh.PARENT").cast(StringType()).alias("PARENT"),
        F.col("sh.CHILD").cast(StringType()).alias("CHILD")
    )
    snomed_hier_child = F.broadcast(snomed_hier_child)
    
    # Parent hierarchy: where base SNOMED codes are parents
    snomed_hier_parent = snomed_hier_full.alias("sh").join(
        F.broadcast(snomed_keys),
        F.col("sh.PARENT").cast(StringType()) == F.col("SNOMED_CODE_STR"),
        "inner"
    ).select(
        F.col("sh.PARENT").cast(StringType()).alias("PARENT"),
        F.col("sh.CHILD").cast(StringType()).alias("CHILD")
    )
    snomed_hier_parent = F.broadcast(snomed_hier_parent)
    
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
        opcs_map,
        F.col("b.SNOMED_CODE_STR") == F.col("TCUI"),
        "left"
    ).filter(
        F.col("SCUI").isNotNull()
    ).select(
        "b.NOMENCLATURE_ID",
        "b.SNOMED_CODE",
        "b.SOURCE_VOCABULARY_CD",
        "b.FOUND_CUI",
        "SCUI"
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
        snomed_hier_child,
        F.col("b.SNOMED_CODE_STR") == F.col("CHILD"),
        "inner"
    ).select(
        "b.NOMENCLATURE_ID",
        "b.SOURCE_VOCABULARY_CD",
        "b.FOUND_CUI",
        "PARENT"
    ).join(
        opcs_map,
        F.col("PARENT") == F.col("TCUI"),
        "inner"
    ).filter(
        F.col("SCUI").isNotNull()
    ).select(
        "NOMENCLATURE_ID",
        "PARENT",
        "SOURCE_VOCABULARY_CD",
        "FOUND_CUI",
        "SCUI"
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
        snomed_hier_parent,
        F.col("b.SNOMED_CODE_STR") == F.col("PARENT"),
        "inner"
    ).select(
        "b.NOMENCLATURE_ID",
        "b.SOURCE_VOCABULARY_CD",
        "b.FOUND_CUI",
        "CHILD"
    ).join(
        opcs_map,
        F.col("CHILD") == F.col("TCUI"),
        "inner"
    ).filter(
        F.col("SCUI").isNotNull()
    ).select(
        "NOMENCLATURE_ID",
        "CHILD",
        "SOURCE_VOCABULARY_CD",
        "FOUND_CUI",
        "SCUI"
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
    # Combine all matches
    all_matches = combine_matches_generic("OPCS4_CODE", "OPCS4_CODE_TYPE", "OPCS4_CODE_MATCH_COUNT",
                                         native_codes, direct_final, child_final, parent_final)
    
    # Remove temporary column from base_df
    base_df = base_df.drop("SNOMED_CODE_STR")
    
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
    
    # Ensure we keep the most recent record per NOMENCLATURE_ID
    window_spec = Window.partitionBy("NOMENCLATURE_ID").orderBy(F.col("ADC_UPDT").desc())
    final_df = final_df.withColumn("rn", F.row_number().over(window_spec)) \
                      .filter(F.col("rn") == 1) \
                      .drop("rn")
    
    # Materialize the result
    final_df = materialize(final_df, "4_prod.tmp.opcs4_mapping_result_temp")
    
    # Safe to drop upstream temp inputs
    spark.sql(f"DROP TABLE IF EXISTS {fq_name('4_prod.tmp.opcs4_symmetric_rels_temp')}")
    
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
    
    # Count matches per NOMENCLATURE_ID before selecting best
    match_counts = with_relationships.groupBy("NOMENCLATURE_ID").agg(
        F.count("target_concept_id").alias("tertiary_match_count")
    )
    
    # Improved window ordering with deterministic tie-breaking
    preferred_domains = get_preferred_domains(source_type)
    window_spec = Window.partitionBy("NOMENCLATURE_ID").orderBy(
        when(col("target_standard") == "S", 1).otherwise(0).desc(),
        when(col("target_domain").isin(preferred_domains), 1).otherwise(0).desc(),
        col("target_class"),
        col("target_concept_id")
    )
    
    best_matches = with_relationships.withColumn("row_num", row_number().over(window_spec)) \
                             .filter(col("row_num") == 1) \
                             .select(
                                 "NOMENCLATURE_ID",
                                 col("target_concept_id").alias("OMOP_CONCEPT_ID"),
                                 col("target_concept_name").alias("OMOP_CONCEPT_NAME")
                             )
    
    # Join match counts back to best matches
    return best_matches.join(match_counts, "NOMENCLATURE_ID", "inner")

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
        
        # Get mappings (now includes tertiary_match_count)
        mappings = process_code_to_omop_mapping(
            candidates, 
            code_col, 
            vocab_concepts, 
            valid_relationships, 
            code_type
        )
        
        # Update result with coalesce to preserve existing values
        # Now also update NUMBER_OF_OMOP_MATCHES
        result_df = result_df.join(mappings.alias("new"), "NOMENCLATURE_ID", "left_outer") \
                     .select(
                         *[c for c in result_df.columns if c not in ["OMOP_CONCEPT_ID", "OMOP_CONCEPT_NAME", "NUMBER_OF_OMOP_MATCHES"]],
                         coalesce(result_df.OMOP_CONCEPT_ID, col("new.OMOP_CONCEPT_ID")).alias("OMOP_CONCEPT_ID"),
                         coalesce(result_df.OMOP_CONCEPT_NAME, col("new.OMOP_CONCEPT_NAME")).alias("OMOP_CONCEPT_NAME"),
                         when(col("new.OMOP_CONCEPT_ID").isNotNull(), col("new.tertiary_match_count"))
                         .otherwise(result_df.NUMBER_OF_OMOP_MATCHES)
                         .cast(IntegerType()).alias("NUMBER_OF_OMOP_MATCHES")
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
    
    print("Adding similarity scores...")
    final_df_with_similarities = add_similarity_scores(final_df)
    
    return final_df_with_similarities

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
    # Now it's safe to drop the materialized result
    spark.sql(f"DROP TABLE IF EXISTS {fq_name('4_prod.tmp.snomed_mapping_result_temp')}")
else:
    print("No updates to process.")

# Process ICD10 mapping
updates_df = create_icd10_mapping_incr()
if updates_df is not None:
    update_nomen_table(updates_df, "4_prod.tmp.tempthree_nomenclature")
    spark.sql(f"DROP TABLE IF EXISTS {fq_name('4_prod.tmp.icd10_mapping_result_temp')}")
else:
    print("No updates to process for ICD10.")

# Process OPCS4 mapping
print("Starting OPCS4 mapping process...")
opcs_updates_df = create_opcs4_mapping_incr()

if opcs_updates_df is not None:
    # The function already materialized to opcs4_mapping_result_temp
    # So we can directly use it
    update_nomen_table(opcs_updates_df, "4_prod.tmp.tempfour_nomenclature")
    
    # Now safe to drop the result temp
    spark.sql(f"DROP TABLE IF EXISTS {fq_name('4_prod.tmp.opcs4_mapping_result_temp')}")
    print("OPCS4 mapping completed successfully.")
else:
    print("No updates to process.")

print("\nDeduplicating existing embeddings...")
deduplicate_existing_embeddings()
    
print("\nExtracting terms from nomenclature...")
all_terms = extract_unique_terms_from_nomenclature()
    
if all_terms:    
    print("\nChecking for existing terms...")
    existing_terms = get_existing_terms()
    
    new_terms = filter_new_terms(all_terms, existing_terms)
    print(f"  - {len(new_terms)} new terms to add")
    
    if new_terms:
        print("\nInserting new terms...")
        batch_insert_terms_sql(new_terms)
        verify_no_duplicates()
        
print("Processing Embeddings")
process_missing_embeddings()

# Finalization and replication
print("Starting finalization and replication process...")

updates_df = create_final_nomenclature_incr()

if updates_df is not None:
    temp_table = "4_prod.tmp.final_nomenclature_temp"
    spark.sql(f"DROP TABLE IF EXISTS {fq_name(temp_table)}")
    
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
    
    spark.sql(f"DROP TABLE IF EXISTS {fq_name(temp_table)}")
else:
    print("No updates were made, skipping replication.")

# COMMAND ----------

# =============================================================================
# INGREDIENT EMBEDDING TABLES CREATION & UPDATE
# =============================================================================

def sync_and_embed_ingredients(source_df, target_table_name, batch_size=500):
    """
    Syncs unique terms from a source DataFrame to a target Delta table and generates embeddings.
    
    1. Normalizes source terms to Title Case.
    2. Creates target table if it doesn't exist.
    3. Identifies terms present in source but missing in target.
    4. Generates embeddings in batches.
    5. Merges new records into target table.
    """
    print(f"\nProcessing {target_table_name}...")
    
    # 1. Normalize Source Data (Title Case for Case Insensitivity)
    # Expects source_df to have a column named 'term'
    source_clean = source_df.select(
        F.initcap(F.lower(F.trim(F.col("term")))).alias("term")
    ).filter(
        F.col("term").isNotNull() & (F.col("term") != "")
    ).distinct()
    
    # 2. Create Target Table if not exists
    if not SparkSession.getActiveSession().catalog.tableExists(target_table_name):
        print(f"  Creating new table: {target_table_name}")
        empty_schema = StructType([
            StructField("term", StringType(), False),
            StructField("embedding_vector", ArrayType(FloatType()), True),
            StructField("model_version", StringType(), True),
            StructField("updated_at", TimestampType(), True)
        ])
        spark.createDataFrame([], empty_schema).write \
            .format("delta") \
            .mode("error") \
            .saveAsTable(target_table_name)
    
    # 3. Identify New Terms (Left Anti Join)
    target_df = spark.table(target_table_name)
    
    # Find terms in source that are NOT in target
    new_terms_df = source_clean.join(
        target_df, 
        source_clean.term == target_df.term, 
        "left_anti"
    )
    
    new_terms_list = [row['term'] for row in new_terms_df.collect()]
    count = len(new_terms_list)
    
    if count == 0:
        print("  ✓ Table is up to date. No new terms to embed.")
        return

    print(f"  Found {count} new terms to embed.")
    
    # 4. Process in Batches
    processed_count = 0
    
    # Initialize Delta Table wrapper for merging
    delta_table = DeltaTable.forName(spark, target_table_name)
    
    for i in range(0, count, batch_size):
        batch_terms = new_terms_list[i:i + batch_size]
        print(f"  Batch {i//batch_size + 1}: Embedding {len(batch_terms)} terms...")
        
        try:
            # Generate Embeddings
            embeddings = get_embedding(batch_terms)
            
            # Prepare Data for Merge
            # Note: casting embedding to float array to match schema
            batch_data = []
            timestamp = datetime.now()
            
            for term, vec in zip(batch_terms, embeddings):
                batch_data.append({
                    "term": term,
                    "embedding_vector": [float(x) for x in vec],
                    "model_version": "text-embedding-3-large", 
                    "updated_at": timestamp
                })
                
            batch_df = spark.createDataFrame(batch_data)
            
            # 5. Merge into Delta Table
            (delta_table.alias("tgt")
                .merge(
                    batch_df.alias("src"),
                    "tgt.term = src.term"
                )
                .whenMatchedUpdate(set={
                    "embedding_vector": "src.embedding_vector",
                    "updated_at": "src.updated_at"
                })
                .whenNotMatchedInsert(values={
                    "term": "src.term",
                    "embedding_vector": "src.embedding_vector",
                    "model_version": "src.model_version",
                    "updated_at": "src.updated_at"
                })
                .execute()
            )
            
            processed_count += len(batch_data)
            
            # Rate limit protection
            time.sleep(0.2)
            
        except Exception as e:
            print(f"  ✗ Error processing batch: {str(e)}")
            # Continue to next batch rather than failing entire job
            continue

    print(f"  ✓ Finished. Added {processed_count} new embeddings to {target_table_name}.")

# -------------------------------------------------------
# 1. Process Barts Ingredients (Enhanced Fallback Logic)
# -------------------------------------------------------
BARTS_TABLE = "3_lookup.embeddings.barts_ingredients"

def get_enhanced_barts_source():
    # 1. Get Base Drug Catalog (Multum Drugs only)
    # Filtering for CKI 'MUL.ORD!d%' ensures we target drugs
    df_catalog = spark.table("3_lookup.mill.mill_order_catalog") \
        .filter(F.col("CKI").like("MUL.ORD!d%")) \
        .select("CATALOG_CD", "PRIMARY_MNEMONIC")

    # 2. Get Synonyms
    df_synonyms = spark.table("3_lookup.mill.mill_order_catalog_synonym") \
        .select("CATALOG_CD", "SYNONYM_ID", "MNEMONIC")

    # 3. Get Ingredients
    # Note: HNA_ORDER_MNEMONIC in this table usually holds the ingredient name
    df_ingredients = spark.table("4_prod.raw.mill_order_ingredient") \
        .select("SYNONYM_ID", "HNA_ORDER_MNEMONIC")

    # 4. Join and Apply Fallback Logic
    # Priority: Ingredient Name -> Synonym Mnemonic -> Catalog Primary Mnemonic
    df_enhanced_source = df_catalog.join(df_synonyms, "CATALOG_CD", "inner") \
        .join(df_ingredients, "SYNONYM_ID", "left") \
        .select(
            F.coalesce(
                F.col("mill_order_ingredient.HNA_ORDER_MNEMONIC"), 
                F.col("mill_order_catalog_synonym.MNEMONIC"), 
                F.col("mill_order_catalog.PRIMARY_MNEMONIC")
            ).alias("term")
        ) \
        .filter(F.col("term").isNotNull() & (F.col("term") != "")) \
        .distinct()
        
    return df_enhanced_source

df_barts_source_enhanced = get_enhanced_barts_source()
sync_and_embed_ingredients(df_barts_source_enhanced, BARTS_TABLE)

# -------------------------------------------------------
# 2. Process OMOP Ingredients (dm+d specific)
# -------------------------------------------------------
OMOP_TABLE = "3_lookup.embeddings.omop_ingredients"

# Get distinct concept names for dm+d Ingredients
df_omop_source = spark.table("3_lookup.omop.concept") \
    .filter(
        (F.col("vocabulary_id") == "dm+d") & 
        (F.col("concept_class_id").isin("Ingredient", "Multiple Ingredients"))
    ) \
    .select(F.col("concept_name").alias("term"))

sync_and_embed_ingredients(df_omop_source, OMOP_TABLE)



# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
import numpy as np
import pandas as pd

TARGET_TABLE = "3_lookup.mill.map_med_lookup"

def update_med_lookup_table_incr():
    print(f"Starting optimized intelligent update for {TARGET_TABLE}...")
    
    # ---------------------------------------------------------
    # 0. ENSURE TABLE EXISTS
    # ---------------------------------------------------------
    if not SparkSession.getActiveSession().catalog.tableExists(TARGET_TABLE):
        print(f"  Creating empty table: {TARGET_TABLE}")
        schema = StructType([
            StructField("SYNONYM_ID", LongType(), True),
            StructField("HNA_ORDER_MNEMONIC", StringType(), True),
            StructField("MULTUM_CODE", StringType(), True),
            StructField("RXNORM_CODE", StringType(), True),
            StructField("SNOMED_CODE", StringType(), True),
            StructField("MAPPED_OMOP_CONCEPT_ID", IntegerType(), True),
            StructField("MAPPED_OMOP_CONCEPT_TERM", StringType(), True),
            StructField("SIMILARITY_OMOP_CONCEPT_ID", IntegerType(), True),
            StructField("SIMILARITY_OMOP_CONCEPT_TERM", StringType(), True),
            StructField("SIMILARITY_SCORE", FloatType(), True),
            StructField("SNOMED_FROM_OMOP", StringType(), True),
            StructField("ADC_UPDT", TimestampType(), True)
        ])
        spark.createDataFrame([], schema).write.format("delta").saveAsTable(TARGET_TABLE)

    # ---------------------------------------------------------
    # 1. DEFINE DRUG UNIVERSE
    # ---------------------------------------------------------
    print("  Step 1: Defining Drug Universe...")
    
    drug_universe = (
        spark.table("3_lookup.mill.mill_order_catalog_synonym").alias("S")
        .join(spark.table("3_lookup.mill.mill_order_catalog").alias("C"), F.col("S.CATALOG_CD") == F.col("C.CATALOG_CD"), "inner")
        .join(spark.table("4_prod.raw.mill_order_ingredient").alias("I"), "SYNONYM_ID", "left")
        .filter(F.col("C.CKI").like("MUL.ORD!d%")) 
        .select(
            F.col("S.SYNONYM_ID"),
            F.col("S.MNEMONIC").alias("HNA_ORDER_MNEMONIC"),
            F.col("C.CKI"),
            F.col("S.ADC_UPDT"),
            F.coalesce(F.col("I.HNA_ORDER_MNEMONIC"), F.col("S.MNEMONIC"), F.col("C.PRIMARY_MNEMONIC")).alias("VEC_SEARCH_TERM"),
            F.when(F.col("C.CKI").like("MUL.ORD%"), F.substring_index(F.col("C.CKI"), "!", -1)).alias("MULTUM_CODE")
        )
        .dropDuplicates(["SYNONYM_ID"]) 
    )

    target_ids = spark.table(TARGET_TABLE).select("SYNONYM_ID")
    new_records = drug_universe.join(target_ids, "SYNONYM_ID", "left_anti")
    
    if new_records.isEmpty():
        print("  ✓ No new drug records found.")
        return

    print(f"  Processing {new_records.count()} new drug records...")

    # ---------------------------------------------------------
    # 2. DETERMINISTIC MAPPING
    # ---------------------------------------------------------
    rxnorm = spark.table("3_lookup.rxnorm.rxnconso")
    
    shortest_snomed = (
        rxnorm.filter(F.col("SAB") == "SNOMEDCT_US")
        .withColumn("len", F.length("CODE"))
        .withColumn("rn", F.row_number().over(Window.partitionBy("RXCUI").orderBy("len")))
        .filter(F.col("rn") == 1).select("RXCUI", F.col("CODE").alias("SNOMED_CODE"))
    )

    base_mapping = (
        new_records.alias("BASE")
        .join(rxnorm.filter(F.col("SAB") == "MMSL").alias("RXN"), F.col("BASE.MULTUM_CODE") == F.col("RXN.CODE"), "left")
        .join(shortest_snomed.alias("SNO"), F.col("RXN.RXCUI") == F.col("SNO.RXCUI"), "left")
        .select("BASE.SYNONYM_ID", "BASE.HNA_ORDER_MNEMONIC", "BASE.VEC_SEARCH_TERM", "BASE.MULTUM_CODE", 
                F.col("RXN.RXCUI").alias("RXNORM_CODE"), "SNO.SNOMED_CODE", "BASE.ADC_UPDT")
    )

    # ---------------------------------------------------------
    # 3. OPTIMIZED VECTOR MATCHING
    # ---------------------------------------------------------
    print("  Step 3: Optimizing Vector Similarity...")

    # Prepare Target Vectors
    target_vec_df = (
        spark.table("3_lookup.embeddings.omop_ingredients").alias("TGT")
        .join(spark.table("3_lookup.omop.concept").alias("C"), 
              (F.lower(F.trim(F.col("TGT.term"))) == F.lower(F.trim(F.col("C.concept_name")))) &
              (F.col("C.vocabulary_id").isin(["dm+d", "RxNorm"])) & 
              (F.col("C.concept_class_id").isin("Ingredient", "Multiple Ingredients")))
        .select(F.col("TGT.term"), F.col("TGT.embedding_vector"), F.col("C.concept_id"))
        .toPandas()
    )

    if target_vec_df.empty:
        print("  No target vectors found. Skipping similarity step.")
        return

    tgt_ids = target_vec_df['concept_id'].values
    tgt_terms = target_vec_df['term'].values
    tgt_matrix = np.stack(target_vec_df['embedding_vector'].values)
    tgt_norms = np.linalg.norm(tgt_matrix, axis=1, keepdims=True)
    tgt_matrix_norm = tgt_matrix / (tgt_norms + 1e-9)

    schema_match = StructType([
        StructField("RAW_OMOP_ID", IntegerType(), True),
        StructField("RAW_OMOP_TERM", StringType(), True),
        StructField("SCORE", FloatType(), True)
    ])

    @F.pandas_udf(schema_match)
    def find_best_match_udf(vectors: pd.Series) -> pd.DataFrame:
        src_matrix = np.vstack(vectors.values)
        src_norms = np.linalg.norm(src_matrix, axis=1, keepdims=True)
        src_matrix_norm = src_matrix / (src_norms + 1e-9)
        scores = np.matmul(src_matrix_norm, tgt_matrix_norm.T)
        best_idx = np.argmax(scores, axis=1)
        best_scores = np.max(scores, axis=1)
        return pd.DataFrame({
            "RAW_OMOP_ID": tgt_ids[best_idx],
            "RAW_OMOP_TERM": tgt_terms[best_idx],
            "SCORE": best_scores
        })

    source_vecs = spark.table("3_lookup.embeddings.barts_ingredients")
    
    df_ready = base_mapping.join(
        source_vecs, 
        F.lower(F.trim(base_mapping.VEC_SEARCH_TERM)) == F.lower(F.trim(source_vecs.term)), 
        "left"
    )
    
    df_matches = (
        df_ready
        .withColumn("match", F.when(F.col("embedding_vector").isNotNull(), 
                                   find_best_match_udf(F.col("embedding_vector")))
                              .otherwise(None))
        .select(
            "SYNONYM_ID", "HNA_ORDER_MNEMONIC", "MULTUM_CODE", "RXNORM_CODE", "SNOMED_CODE", "ADC_UPDT",
            "match.RAW_OMOP_ID", "match.RAW_OMOP_TERM", "match.SCORE"
        )
    )

    # ---------------------------------------------------------
    # 4. STANDARDIZE & FINALIZE (UPDATED LOGIC)
    # ---------------------------------------------------------
    print("  Step 4: Standardizing, Mapping RxNorm & Linking SNOMED...")
    
    concept_rel = spark.table("3_lookup.omop.concept_relationship")
    omop = spark.table("3_lookup.omop.concept")

    # --- Logic A: Populate MAPPED_OMOP_CONCEPT_ID via RxNorm Code ---
    # Direct lookup: RxNorm Code -> Standard OMOP Concept
    rxnorm_to_omop = omop.filter(
        (F.col("vocabulary_id") == "RxNorm") & 
        (F.col("standard_concept") == "S")
    ).select(
        F.col("concept_code").alias("RXNORM_CODE"),
        F.col("concept_id").alias("DIRECT_STD_ID"),
        F.col("concept_name").alias("DIRECT_STD_NAME")
    )

    # --- Logic B: Standardize Vector Results ---
    # Map Vector Result (RAW) -> Standard Concept (STD)
    # Supports 'Maps to' AND 'Source - RxNorm eq' (handling the ID 21234887 case)
    valid_relationships = ["Maps to", "Source - RxNorm eq", "Source - RxNorm"]
    
    vector_standardization = (
        concept_rel.filter(
            F.col("relationship_id").isin(valid_relationships) & 
            F.col("invalid_reason").isNull()
        )
        .join(omop.alias("T"), (F.col("concept_id_2") == F.col("T.concept_id")) & (F.col("T.standard_concept") == "S"))
        .select(
            F.col("concept_id_1").alias("RAW_OMOP_ID"),
            F.col("concept_id_2").alias("VEC_STD_ID"),
            F.col("T.concept_name").alias("VEC_STD_NAME")
        )
    )

    # --- Logic C: SNOMED Reverse Lookup ---
    # Find a SNOMED Substance that maps TO the Standard Concept
    # Rules: Vocabulary=SNOMED, Class=Substance, Domain=Drug
    # Tie-breaker: Shortest Name
    
    snomed_candidates = omop.filter(
        (F.col("vocabulary_id") == "SNOMED") &
        (F.col("concept_class_id") == "Substance") &
        (F.col("domain_id") == "Drug") &
        (F.col("invalid_reason").isNull())
    ).alias("SNO_C")

    snomed_to_std_rel = (
        snomed_candidates
        .join(concept_rel.alias("R"), F.col("SNO_C.concept_id") == F.col("R.concept_id_1"))
        .filter(F.col("R.relationship_id").isin(valid_relationships))
    )

    # Window to find the single best SNOMED code per Standard ID
    w_shortest = Window.partitionBy("R.concept_id_2").orderBy(F.length("SNO_C.concept_name"), "SNO_C.concept_code")

    best_snomed_map = (
        snomed_to_std_rel
        .withColumn("rn", F.row_number().over(w_shortest))
        .filter("rn = 1")
        .select(
            F.col("R.concept_id_2").alias("TARGET_STD_ID"),
            F.col("SNO_C.concept_code").alias("DERIVED_SNOMED_CODE")
        )
    )

    # --- Logic D: JOIN ALL ---
    final_df = (
        df_matches.alias("M")
        # 1. Attach Direct RxNorm Map
        .join(rxnorm_to_omop, "RXNORM_CODE", "left")
        # 2. Attach Standardized Vector Map
        .join(vector_standardization, "RAW_OMOP_ID", "left")
        # 3. Determine "Effective" Standard ID for SNOMED lookup (Prefer Direct Map, else Vector Map)
        .withColumn("EFFECTIVE_STD_ID", F.coalesce(F.col("DIRECT_STD_ID"), F.col("VEC_STD_ID")))
        # 4. Attach Derived SNOMED based on Effective ID
        .join(best_snomed_map, F.col("EFFECTIVE_STD_ID") == F.col("TARGET_STD_ID"), "left")
        .select(
            "SYNONYM_ID", 
            "HNA_ORDER_MNEMONIC", 
            "MULTUM_CODE", 
            "RXNORM_CODE", 
            "SNOMED_CODE",
            # Mapped (from RxNorm Code)
            F.col("DIRECT_STD_ID").alias("MAPPED_OMOP_CONCEPT_ID"), 
            F.col("DIRECT_STD_NAME").alias("MAPPED_OMOP_CONCEPT_TERM"),
            # Similarity (Standardized from Vector)
            F.coalesce(F.col("VEC_STD_ID"), F.col("RAW_OMOP_ID")).alias("SIMILARITY_OMOP_CONCEPT_ID"),
            F.coalesce(F.col("VEC_STD_NAME"), F.col("RAW_OMOP_TERM")).alias("SIMILARITY_OMOP_CONCEPT_TERM"),
            F.col("SCORE").alias("SIMILARITY_SCORE"),
            # Derived Snomed
            F.col("DERIVED_SNOMED_CODE").alias("SNOMED_FROM_OMOP"),
            "ADC_UPDT"
        )
    )

    # Merge
    DeltaTable.forName(spark, TARGET_TABLE).alias("t").merge(final_df.alias("s"), "t.SYNONYM_ID = s.SYNONYM_ID") \
        .whenNotMatchedInsertAll().execute()

    print("✓ Update complete.")

update_med_lookup_table_incr()

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
# MAGIC OPTIMIZE 3_lookup.embeddings.barts_ingredients ZORDER BY (term);
# MAGIC OPTIMIZE 3_lookup.embeddings.omop_ingredients ZORDER BY (term);
# MAGIC
