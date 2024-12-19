# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import max as spark_max
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *
from pyspark.sql import functions as F
from functools import reduce

# COMMAND ----------



def get_max_timestamp(table_name):
    """
    Retrieves the maximum timestamp from a given table.
    Falls back to ADC_UPDT if _row_modified doesn't exist.
    If no date is found, returns January 1, 1980 as a default date.
    
    Args:
        table_name (str): Name of the table to query
    
    Returns:
        datetime: Maximum timestamp or default date
    """
    try:
        default_date = datetime(1980, 1, 1)
        
        if not table_exists(table_name):
            return default_date
            
        # Try to get ADC_UPDT directly
        result = spark.sql(f"SELECT MAX(ADC_UPDT) AS max_date FROM {table_name}")
        max_date = result.select(max("max_date").alias("max_date")).first()["max_date"]
        
        return max_date if max_date is not None else default_date
        
    except Exception as e:
        print(f"Error getting max timestamp from {table_name}: {str(e)}")
        return default_date
    


def table_exists(table_name):
    try:
        result = spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
        return result.first() is not None
    except:
        return False

# COMMAND ----------

def update_nomen_table(source_df, target_table):
    """
    Updates the nomenclature table using SQL MERGE
    
    Args:
        source_df: Source DataFrame with new/updated records
        target_table: Target table name to update
    """
    
    if table_exists(target_table):
        # Create temporary view of source data
        temp_view_name = f"temp_source_{target_table.replace('.', '_')}"
        source_df.createOrReplaceTempView(temp_view_name)
        
        # Construct and execute MERGE statement
        merge_sql = f"""
        MERGE INTO {target_table} as target
        USING {temp_view_name} as source
        ON target.NOMENCLATURE_ID = source.NOMENCLATURE_ID
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        try:
            spark.sql(merge_sql)
        finally:
            # Clean up temporary view
            spark.sql(f"DROP VIEW IF EXISTS {temp_view_name}")
    else:
        # If table doesn't exist, create it from source
        source_df.write.format("delta") \
            .option("delta.enableChangeDataFeed", "true") \
            .option("delta.enableRowTracking", "true") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .mode("overwrite") \
            .saveAsTable(target_table)

# COMMAND ----------



# COMMAND ----------


def process_snomed_matches(snomed_df, snomed_sct, omop_concepts):
    """
    Processes SNOMED code matches using multiple matching strategies (TUI, CUI, and Term matching).
    
    Args:
        snomed_df: DataFrame containing source SNOMED codes
        snomed_sct: SNOMED reference table
        omop_concepts: OMOP concept reference table
    
    Returns:
        DataFrame: Combined results of all matching strategies with standardized columns
    """
    # Alias the main dataframe for clear join conditions
    base_df = snomed_df.alias("base")
    
    # Extract CUI (Concept Unique Identifier) from CONCEPT_CKI field
    # Format expected: "SNOMED!{CUI}"
    base_df = base_df.withColumn(
        "extracted_cui",
        when(
            col("base.CONCEPT_CKI").contains("SNOMED!"),
            regexp_extract(col("base.CONCEPT_CKI"), "SNOMED!(.*)", 1)
        )
    )

    # Apply matching strategies in order of preference:
    # 1. TUI (Type Unique Identifier) matches
    tui_matches = process_tui_matches(base_df, snomed_sct, omop_concepts)
    
    # 2. CUI matches for records that didn't match by TUI
    unmatched_for_cui = base_df.join(tui_matches, col("base.NOMENCLATURE_ID") == tui_matches.NOMENCLATURE_ID, "left_anti")
    cui_matches = process_cui_matches(unmatched_for_cui, snomed_sct, omop_concepts)
    
    # 3. Term matches for records that didn't match by TUI or CUI
    unmatched_for_term = unmatched_for_cui.join(cui_matches, col("base.NOMENCLATURE_ID") == cui_matches.NOMENCLATURE_ID, "left_anti")
    term_matches = process_term_matches(unmatched_for_term, snomed_sct, omop_concepts)
    
    # Combine all matches into a single result set
    all_matches = tui_matches.unionAll(cui_matches).unionAll(term_matches)
    
    # Join back to original dataframe to preserve all records and standardize output
    return (
        base_df
        .join(
            all_matches,
            col("base.NOMENCLATURE_ID") == all_matches.NOMENCLATURE_ID,
            "left"
        )
        .select(
            # Original fields
            col("base.NOMENCLATURE_ID").alias("NOMENCLATURE_ID"),
            col("base.SOURCE_IDENTIFIER").alias("SOURCE_IDENTIFIER"),
            col("base.SOURCE_STRING").alias("SOURCE_STRING"),
            col("base.SOURCE_VOCABULARY_CD").alias("SOURCE_VOCABULARY_CD"),
            col("base.CONCEPT_CKI").alias("CONCEPT_CKI"),
            col("base.ADC_UPDT").alias("ADC_UPDT"),
            # Matched fields with null handling
            coalesce(all_matches.concept_id, lit(None)).alias("concept_id"),
            coalesce(all_matches.concept_name, lit(None)).alias("concept_name"),
            coalesce(all_matches.standard_concept, lit(None)).alias("standard_concept"),
            coalesce(all_matches.domain_id, lit(None)).alias("domain_id"),
            coalesce(all_matches.concept_class_id, lit(None)).alias("concept_class_id"),
            coalesce(all_matches.match_count, lit(0)).alias("match_count"),
            coalesce(all_matches.match_type, lit(None)).alias("match_type")
        )
    )

def process_tui_matches(df, snomed_sct, snomed_concepts):
    """
    Matches records based on Type Unique Identifier (TUI).
    This is typically the most precise matching method.
    
    Args:
        df: Source DataFrame with base records
        snomed_sct: SNOMED reference table
        snomed_concepts: OMOP concept reference table
    
    Returns:
        DataFrame: Matched records with standardized columns
    """
    return (
        # Join source data with SNOMED reference using TUI
        df.join(
            snomed_sct.alias("trud1"),
            col("base.SOURCE_IDENTIFIER") == col("trud1.TUI"),
            "inner"
        )
        # Join with OMOP concepts to get standardized concept information
        .join(
            snomed_concepts.alias("concept1"),
            col("trud1.CUI") == col("concept1.concept_code"),
            "inner"
        )
        # Calculate how many matches were found for each source identifier
        .withColumn(
            "match_count",
            count("concept1.concept_id").over(Window.partitionBy("base.SOURCE_IDENTIFIER", "base.SOURCE_VOCABULARY_CD"))
        )
        .withColumn("match_type", lit("TUI"))
        # Select standardized output columns
        .select(
            col("base.NOMENCLATURE_ID").alias("NOMENCLATURE_ID"),
            col("concept1.concept_id").alias("concept_id"),
            col("concept1.concept_name").alias("concept_name"),
            col("concept1.standard_concept").alias("standard_concept"),
            col("concept1.domain_id").alias("domain_id"),
            col("concept1.concept_class_id").alias("concept_class_id"),
            col("match_count").alias("match_count"),
            col("match_type").alias("match_type")
        )
    )

def process_cui_matches(df, snomed_sct, snomed_concepts):
    """
    Matches records based on Concept Unique Identifier (CUI).
    Used as a secondary matching strategy when TUI matching fails.
    
    Args:
        df: Source DataFrame with unmatched records
        snomed_sct: SNOMED reference table
        snomed_concepts: OMOP concept reference table
    
    Returns:
        DataFrame: Matched records with standardized columns
    """
    return (
        # Join with SNOMED reference using extracted CUI
        df.join(
            snomed_sct.alias("trud2"),
            col("extracted_cui") == col("trud2.CUI"),
            "inner"
        )
        # Join with OMOP concepts for standardized terminology
        .join(
            snomed_concepts.alias("concept2"),
            col("trud2.CUI") == col("concept2.concept_code"),
            "inner"
        )
        # Track number of matches found
        .withColumn(
            "match_count",
            count("concept2.concept_id").over(Window.partitionBy("base.SOURCE_IDENTIFIER", "base.SOURCE_VOCABULARY_CD"))
        )
        .withColumn("match_type", lit("CUI"))
        # Standardize output columns
        .select(
            col("base.NOMENCLATURE_ID").alias("NOMENCLATURE_ID"),
            col("concept2.concept_id").alias("concept_id"),
            col("concept2.concept_name").alias("concept_name"),
            col("concept2.standard_concept").alias("standard_concept"),
            col("concept2.domain_id").alias("domain_id"),
            col("concept2.concept_class_id").alias("concept_class_id"),
            col("match_count").alias("match_count"),
            col("match_type").alias("match_type")
        )
    )

def process_term_matches(df, snomed_sct, snomed_concepts):
    """
    Matches records based on term text similarity.
    Used as a last resort when both TUI and CUI matching fail.
    Includes text normalization to improve match quality.
    
    Args:
        df: Source DataFrame with unmatched records
        snomed_sct: SNOMED reference table
        snomed_concepts: OMOP concept reference table
    
    Returns:
        DataFrame: Matched records with standardized columns
    """
    return (
        # Normalize source terms by removing special characters and converting to uppercase
        df.withColumn("clean_source", 
            regexp_replace(upper(col("base.SOURCE_STRING")), '[^A-Z0-9]', ''))
        .join(
            # Normalize SNOMED terms in the same way
            snomed_sct.alias("trud3")
            .withColumn("clean_term",
                regexp_replace(upper(col("TERM")), '[^A-Z0-9]', '')),
            col("clean_source") == col("clean_term"),
            "inner"
        )
        # Join with OMOP concepts
        .join(
            snomed_concepts.alias("concept3"),
            col("trud3.CUI") == col("concept3.concept_code"),
            "inner"
        )
        .withColumn(
            "match_count",
            count("concept3.concept_id").over(Window.partitionBy("base.SOURCE_IDENTIFIER", "base.SOURCE_VOCABULARY_CD"))
        )
        .withColumn("match_type", lit("TERM"))
        .select(
            col("base.NOMENCLATURE_ID").alias("NOMENCLATURE_ID"),
            col("concept3.concept_id").alias("concept_id"),
            col("concept3.concept_name").alias("concept_name"),
            col("concept3.standard_concept").alias("standard_concept"),
            col("concept3.domain_id").alias("domain_id"),
            col("concept3.concept_class_id").alias("concept_class_id"),
            col("match_count").alias("match_count"),
            col("match_type").alias("match_type")
        )
    )


def standardize_multum(code):
    """
    Standardizes Multum drug codes by removing 'd' prefix and converting to string.
    
    Args:
        code: Raw Multum code
    
    Returns:
        str: Standardized code
    """
    if not code:
        return code
    numeric_part = code.lower().replace('d', '')
    try:
        return str(int(numeric_part))
    except ValueError:
        return numeric_part

standardize_multum_udf = udf(standardize_multum)

def process_multum_matches(df, multum_codes, omop_concepts):
    """
    Processes matches for Multum drug codes against OMOP concepts.
    Includes code standardization and specialized drug vocabulary matching.
    
    Args:
        df: Source DataFrame with Multum codes
        multum_codes: Reference table of valid Multum codes
        omop_concepts: OMOP concept reference table
    
    Returns:
        DataFrame: Matched records with standardized columns
    """

    base_df = df.alias("base")
    multum_codes_df = multum_codes.alias("multum")
    
    # Join with Multum reference codes to validate source codes
    multum_df = (
        base_df
        .join(
            multum_codes_df,
            col("base.SOURCE_VOCABULARY_CD") == col("multum.CODE_VALUE")
        )
    )
    
    # Filter OMOP concepts to only include Multum vocabulary
    multum_concepts = omop_concepts.filter(col("vocabulary_id") == "Multum")
    
    # Perform matching with standardized codes
    matches = (
        multum_df
        .withColumn(
            "standardized_source_id",
            standardize_multum_udf(col("base.SOURCE_IDENTIFIER"))
        )
        .join(
            multum_concepts,
            col("standardized_source_id") == col("concept_code"),
            "left"
        )
        # Track number of matches found
        .withColumn(
            "match_count",
            count("concept_id").over(Window.partitionBy("base.SOURCE_IDENTIFIER"))
        )
        .withColumn("match_type", lit("MULTUM"))
    )
    

    return (
        matches
        .select(
            col("base.NOMENCLATURE_ID"),
            col("base.SOURCE_IDENTIFIER"),
            col("base.SOURCE_STRING"),
            col("base.SOURCE_VOCABULARY_CD"),
            col("base.CONCEPT_CKI"),

            col("base.ADC_UPDT"),
            coalesce(col("concept_id"), lit(None)).alias("concept_id"),
            coalesce(col("concept_name"), lit(None)).alias("concept_name"),
            coalesce(col("standard_concept"), lit(None)).alias("standard_concept"),
            coalesce(col("domain_id"), lit(None)).alias("domain_id"),
            coalesce(col("concept_class_id"), lit(None)).alias("concept_class_id"),
            coalesce(col("match_count"), lit(0)).alias("match_count"),
            coalesce(col("match_type"), lit(None)).alias("match_type")
        )
    )

def standardize_icd10(code):
    """
    Standardizes ICD10 codes by removing dots and spaces, converting to uppercase.
    
    Args:
        code: Raw ICD10 code
    
    Returns:
        str: Standardized ICD10 code
    """
    if not code:
        return code
    return code.replace('.', '').replace(' ', '').upper()

standardize_icd10_udf = udf(standardize_icd10)

def is_valid_icd10(code_column):
    """
    Validates ICD10 code format using regex patterns.
    Valid formats: 'A99' or 'A99.9'
    
    Args:
        code_column: Column containing ICD10 codes
    
    Returns:
        Column: Boolean column indicating validity
    """
    return (
        code_column.rlike('^[A-Z][0-9]{2}$') |  
        code_column.rlike('^[A-Z][0-9]{2}\\..+$')
    )

def process_icd10_matches(df, icd10_codes, omop_concepts):
    """
    Processes matches for ICD10 diagnosis codes against OMOP concepts.
    Includes code validation, standardization, and hierarchical matching.
    
    Args:
        df: Source DataFrame with ICD10 codes
        icd10_codes: Reference table of valid ICD10 codes
        omop_concepts: OMOP concept reference table
    
    Returns:
        DataFrame: Matched records with standardized columns
    """
    # Add explicit aliases
    base_df = df.alias("base")
    icd10_codes_df = icd10_codes.alias("icd10")
    
    # Join with ICD10 reference codes and validate format
    icd10_df = (
        base_df
        .join(
            icd10_codes_df,
            col("base.SOURCE_VOCABULARY_CD") == col("icd10.CODE_VALUE")
        )
        .filter(is_valid_icd10(col("base.SOURCE_IDENTIFIER")))
    )
    
    # Filter OMOP concepts for ICD10 vocabularies
    icd10_concepts = omop_concepts.filter(col("vocabulary_id").isin(["ICD10", "ICD10CM"]))
    
    # Perform hierarchical matching with standardized codes
    matches = (
        icd10_df
        .withColumn(
            "standardized_source_id",
            standardize_icd10_udf(col("base.SOURCE_IDENTIFIER"))
        )
        .join(
            icd10_concepts.withColumn(
                "standardized_concept_code",
                standardize_icd10_udf(col("concept_code"))
            ),
            # Use startswith to support hierarchical matching
            col("standardized_source_id").startswith(col("standardized_concept_code")),
            "left"
        )
        .withColumn(
            "match_count",
            count("concept_id").over(Window.partitionBy("base.SOURCE_IDENTIFIER"))
        )
        .withColumn("match_type", lit("ICD10"))
    )

def standardize_opcs4(code):
    """
    Standardizes OPCS4 procedure codes by trimming whitespace and converting to uppercase.
    
    Args:
        code: Raw OPCS4 code
    
    Returns:
        str: Standardized OPCS4 code
    """
    if not code:
        return code
    return code.strip().upper()

standardize_opcs4_udf = udf(standardize_opcs4)

def process_opcs4_matches(df, opcs4_codes, omop_concepts):
    """
    Processes matches for OPCS4 procedure codes against OMOP concepts.
    Includes code standardization and hierarchical matching support.
    
    Args:
        df: Source DataFrame with OPCS4 codes
        opcs4_codes: Reference table of valid OPCS4 codes
        omop_concepts: OMOP concept reference table
    
    Returns:
        DataFrame: Matched records with standardized columns
    """

    base_df = df.alias("base")
    opcs4_codes_df = opcs4_codes.alias("opcs4")
    
    # Join with OPCS4 reference codes
    opcs4_df = (
        base_df
        .join(
            opcs4_codes_df,
            col("base.SOURCE_VOCABULARY_CD") == col("opcs4.CODE_VALUE")
        )
    )
    
    # Filter OMOP concepts to only include OPCS4 vocabulary
    opcs4_concepts = omop_concepts.filter(col("vocabulary_id") == "OPCS4")
    
    # Perform hierarchical matching with standardized codes
    matches = (
        opcs4_df
        .withColumn(
            "standardized_source_id",
            standardize_opcs4_udf(col("base.SOURCE_IDENTIFIER"))
        )
        .join(
            opcs4_concepts.withColumn(
                "standardized_concept_code",
                standardize_opcs4_udf(col("concept_code"))
            ),
            # Use startswith to support hierarchical matching
            col("standardized_source_id").startswith(col("standardized_concept_code")),
            "left"
        )
        # Track number of matches found
        .withColumn(
            "match_count",
            count("concept_id").over(Window.partitionBy("base.SOURCE_IDENTIFIER"))
        )
        .withColumn("match_type", lit("OPCS4"))
    )
    
    # Return standardized output with match information
    return (
        matches
        .select(
            col("base.NOMENCLATURE_ID"),
            col("base.SOURCE_IDENTIFIER"),
            col("base.SOURCE_STRING"),
            col("base.SOURCE_VOCABULARY_CD"),
            col("base.CONCEPT_CKI"),
            col("base.ADC_UPDT"),
            coalesce(col("concept_id"), lit(None)).alias("concept_id"),
            coalesce(col("concept_name"), lit(None)).alias("concept_name"),
            coalesce(col("standard_concept"), lit(None)).alias("standard_concept"),
            coalesce(col("domain_id"), lit(None)).alias("domain_id"),
            coalesce(col("concept_class_id"), lit(None)).alias("concept_class_id"),
            coalesce(col("match_count"), lit(0)).alias("match_count"),
            coalesce(col("match_type"), lit(None)).alias("match_type")
        )
    )

# COMMAND ----------

def create_nomenclature_mapping_incr():
    """
    Creates an incremental mapping table that combines all vocabulary mappings.

    
    Returns:
        DataFrame: Incremental nomenclature mappings
    """

    code_value = spark.table("3_lookup.mill.mill_code_value").select(
        "CODE_VALUE", "CDF_MEANING", "DESCRIPTION", "DISPLAY"
    )
    
    snomed_sct = spark.table("3_lookup.trud.snomed_sct").filter(
        (col("CUI").isNotNull()) &
        (col("TUI").isNotNull()) &
        (col("TERM").isNotNull())
    )
    
    omop_concepts = spark.table("3_lookup.omop.concept").filter(
        (col("concept_id").isNotNull()) &
        (col("invalid_reason").isNull())
    )

    max_adc_updt = get_max_timestamp("4_prod.bronze.tempone_nomenclature")

    # Create vocabulary-specific reference subsets
    snomed_vocab_codes = code_value.filter(col("CDF_MEANING").isin(["SNMCT", "SNMUKEMED"]))
    multum_vocab_codes = code_value.filter(col("CDF_MEANING") == "MUL.DRUG")
    icd10_vocab_codes = code_value.filter(col("CDF_MEANING").isin(["ICD10", "ICD10WHO"]))
    opcs4_vocab_codes = code_value.filter(col("CDF_MEANING") == "OPCS4")

    # Get base nomenclature data
    base_nomenclature = (
        spark.table("3_lookup.mill.mill_nomenclature")
        .filter(
            (col("ACTIVE_IND") == 1) & 
            (col("END_EFFECTIVE_DT_TM").isNull() | (col("END_EFFECTIVE_DT_TM") > current_timestamp())) &
            (col("ADC_UPDT") > max_adc_updt)
        )
    )

    # Initialize vocabulary type indicators
    base_with_vocab_type = (
        base_nomenclature
        .withColumn("is_snomed", lit(False))
        .withColumn("is_multum", lit(False))
        .withColumn("is_icd10", lit(False))
        .withColumn("is_opcs4", lit(False))
    )

    # Update vocabulary type flags using broadcast joins for efficiency
    base_with_vocab_type = (
        base_with_vocab_type
        .join(broadcast(snomed_vocab_codes.select("CODE_VALUE").distinct()), 
              base_with_vocab_type.SOURCE_VOCABULARY_CD == snomed_vocab_codes.CODE_VALUE, 
              "left")
        .join(broadcast(multum_vocab_codes.select("CODE_VALUE").distinct()), 
              base_with_vocab_type.SOURCE_VOCABULARY_CD == multum_vocab_codes.CODE_VALUE, 
              "left")
        .join(broadcast(icd10_vocab_codes.select("CODE_VALUE").distinct()), 
              base_with_vocab_type.SOURCE_VOCABULARY_CD == icd10_vocab_codes.CODE_VALUE, 
              "left")
        .join(broadcast(opcs4_vocab_codes.select("CODE_VALUE").distinct()), 
              base_with_vocab_type.SOURCE_VOCABULARY_CD == opcs4_vocab_codes.CODE_VALUE, 
              "left")
    )

    # Add standardized codes and match types based on vocabulary
    base_with_vocab_type = base_with_vocab_type.withColumn(
        "standardized_code",
        when(col("is_snomed"), regexp_extract(col("CONCEPT_CKI"), "SNOMED!(.*)", 1))
        .when(col("is_multum"), regexp_replace(lower(col("SOURCE_IDENTIFIER")), "d", ""))
        .when(col("is_icd10"), regexp_replace(upper(col("SOURCE_IDENTIFIER")), "[^A-Z0-9]", ""))
        .when(col("is_opcs4"), trim(upper(col("SOURCE_IDENTIFIER"))))
        .otherwise(col("SOURCE_IDENTIFIER"))
    ).withColumn(
        "match_type",
        when(col("is_snomed"), "SNOMED")
        .when(col("is_multum"), "MULTUM")
        .when(col("is_icd10"), "ICD10")
        .when(col("is_opcs4"), "OPCS4")
        .otherwise(None)
    )

    # Perform matching and prioritize results
    matches = (
        base_with_vocab_type
        .join(
            omop_concepts,
            base_with_vocab_type.standardized_code == omop_concepts.concept_code,
            "left"
        )
        # Calculate match statistics
        .withColumn(
            "match_count",
            count("concept_id").over(Window.partitionBy("SOURCE_IDENTIFIER"))
        )
        # Assign priority to different vocabulary types
        .withColumn(
            "match_priority",
            when(col("match_type") == "SNOMED", 1)
            .when(col("match_type") == "MULTUM", 2)
            .when(col("match_type") == "ICD10", 3)
            .when(col("match_type") == "OPCS4", 4)
            .otherwise(5)
        )
        # Rank matches based on priority and standard concept status
        .withColumn(
            "row_num",
            row_number().over(
                Window.partitionBy("NOMENCLATURE_ID")
                .orderBy(
                    col("match_priority"),
                    when(col("standard_concept") == "S", 1).otherwise(2),
                    col("match_count")
                )
            )
        )
    )

    # Return only the best match for each nomenclature ID
    final_df = (
        matches
        .filter(col("row_num") == 1)
        .select(
            col("NOMENCLATURE_ID"),
            col("SOURCE_IDENTIFIER"),
            col("SOURCE_STRING"),
            col("SOURCE_VOCABULARY_CD"),
            col("VOCAB_AXIS_CD"),
            col("concept_id").alias("OMOP_CONCEPT_ID"),
            col("concept_name").alias("OMOP_CONCEPT_NAME"),
            col("standard_concept").alias("IS_STANDARD_OMOP_CONCEPT"),
            col("domain_id").alias("CONCEPT_DOMAIN"),
            col("concept_class_id").alias("CONCEPT_CLASS"),
            col("standardized_code").alias("FOUND_CUI"),
            col("match_count").alias("NUMBER_OF_OMOP_MATCHES"),
            col("CONCEPT_CKI"),
            col("ADC_UPDT")
        )
    )

    return final_df

# COMMAND ----------


updates_df = create_nomenclature_mapping_incr()
    

update_nomen_table(updates_df, "4_prod.bronze.tempone_nomenclature")

# COMMAND ----------


def standardize_code(code):
    """
    Standardizes clinical codes by removing dots, spaces, and converting to uppercase.
    
    Args:
        code (str): Raw clinical code
        
    Returns:
        str: Standardized code format
    """
    if not code:
        return code
    return code.replace('.', '').replace(' ', '').upper()

standardize_code_udf = F.udf(standardize_code)

def generate_match_levels(df, code_col):
    """
    Generates multiple matching levels for hierarchical code matching.
    Creates four levels of matching granularity from most to least specific.
    
    Args:
        df (DataFrame): Input DataFrame containing codes
        code_col (str): Name of the column containing the codes
        
    Returns:
        DataFrame: Original DataFrame with additional match level columns
    """
    return df.withColumn(
        "match_level_1", 
        standardize_code_udf(F.col(code_col))
    ).withColumn(
        "match_level_2",  # First three characters for broad category matching
        F.expr("substring(match_level_1, 1, 3)")
    ).withColumn(
        "match_level_3",  # Remove last character for parent category matching
        F.expr("substring(match_level_1, 1, length(match_level_1) - 1)")
    ).withColumn(
        "match_level_4",  # Remove last two characters for grandparent category matching
        F.expr("substring(match_level_1, 1, length(match_level_1) - 2)")
    )

def add_omop_preference(df, source_code_col, target_code_col, source_concepts, target_concepts, omop_rels):
    """
    Adds OMOP mapping preferences to a DataFrame by joining with OMOP concept and relationship tables.
    
    Args:
        df: Source DataFrame containing codes to be mapped
        source_code_col: Name of the column containing source codes
        target_code_col: Name of the column containing target codes
        source_concepts: OMOP source concepts DataFrame
        target_concepts: OMOP target concepts DataFrame
        omop_rels: OMOP relationship reference DataFrame
    
    Returns:
        DataFrame: Original DataFrame enriched with OMOP mapping preferences
    """
    # Prepare concepts DataFrames
    source_concepts = source_concepts.select(
        F.col("concept_id").alias("source_concept_id"),
        F.col("concept_code").alias("source_concept_code")
    )
    
    target_concepts = target_concepts.select(
        F.col("concept_id").alias("target_concept_id"),
        F.col("concept_code").alias("target_concept_code")
    )
    
    # Join source codes with OMOP concepts
    with_source_concept = df.join(
        source_concepts,
        F.col(source_code_col) == F.col("source_concept_code"),
        "left"
    )
    
    # Join target codes with OMOP concepts
    with_target_concept = with_source_concept.join(
        target_concepts,
        F.col(target_code_col) == F.col("target_concept_code"),
        "left"
    )
    
    # Validate relationships and add preference flag
    return with_target_concept.join(
        omop_rels,
        (
            # Check for direct mapping relationship
            (F.col("source_concept_id") == omop_rels.concept_id_1) &
            (F.col("target_concept_id") == omop_rels.concept_id_2)
        ) |
        (
            # Check for reverse mapping relationship
            (F.col("source_concept_id") == omop_rels.concept_id_2) &
            (F.col("target_concept_id") == omop_rels.concept_id_1)
        ),
        "left"
    ).select(
        df["*"],
        # Add binary flag for OMOP-validated mappings
        F.when(
            F.col("relationship_id").isNotNull(),
            F.lit(1)
        ).otherwise(F.lit(0)).alias("omop_mapped")
    )


# COMMAND ----------

def process_matches(df, base_type, omop_type):
    """Helper function to process matches and aggregate results"""
    df = df.alias("matches")
    return df.withColumn(
        "match_rank",
        F.row_number().over(
            Window.partitionBy("matches.NOMENCLATURE_ID")
            .orderBy(
                F.col("matches.omop_mapped").desc(),
                F.col("matches.SCUI")
            )
        )
    ).groupBy(F.col("matches.NOMENCLATURE_ID")).agg(
        F.first(F.when(F.col("match_rank") == 1, F.col("matches.SCUI"))).alias("temp_snomed_code"),
        F.count("matches.SCUI").alias("SNOMED_MATCH_COUNT"),
        F.max(F.col("matches.omop_mapped")).alias("HAS_OMOP_MAP"),
        F.when(F.max(F.col("matches.omop_mapped")) > 0, F.lit(omop_type))
         .otherwise(F.lit(base_type))
         .alias("SNOMED_TYPE")
    )

def combine_matches(*dfs):
    """Helper function to combine and prioritize matches"""
    # Add unique aliases to each DataFrame
    aliased_dfs = []
    for i, df in enumerate(dfs):
        aliased_dfs.append(
            df.select(
                F.col("NOMENCLATURE_ID"),
                F.col("temp_snomed_code"),
                F.col("SNOMED_MATCH_COUNT"),
                F.col("HAS_OMOP_MAP"),
                F.col("SNOMED_TYPE")
            ).alias(f"df_{i}")
        )
    
    combined = reduce(lambda df1, df2: df1.unionAll(df2), aliased_dfs)
    
    return combined.withColumn(
        "priority",
        F.when(F.col("SNOMED_TYPE") == "NATIVE", 1)
        .when(F.col("SNOMED_TYPE").isin("EXACT", "OMOP_ASSISTED"), 2)
        .when(F.col("SNOMED_TYPE").isin("CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED"), 3)
        .when(F.col("SNOMED_TYPE").isin("PARENTMATCH", "PARENTMATCH_OMOP_ASSISTED"), 4)
        .otherwise(5)
    ).withColumn(
        "row_num",
        F.row_number().over(
            Window.partitionBy("NOMENCLATURE_ID")
            .orderBy("priority", "SNOMED_MATCH_COUNT")
        )
    ).filter(F.col("row_num") == 1).drop("priority", "row_num")

def add_snomed_terms(matches_df, snomed_terms, base_df):
    """Helper function to add SNOMED terms to final mappings"""
    # First join matches with terms
    matches_df = matches_df.alias("matches")
    snomed_terms = snomed_terms.alias("terms")
    
    # Add row number to pick just one term (preferably the shortest one)
    snomed_terms_deduplicated = snomed_terms.withColumn(
        "term_rank",
        F.row_number().over(
            Window.partitionBy("CUI")
            .orderBy(F.length("TERM"))  # Order by term length to get the shortest term
        )
    ).filter(F.col("term_rank") == 1)  # Keep only one term per CUI
    
    with_terms = matches_df.join(
        snomed_terms_deduplicated,
        F.col("matches.temp_snomed_code") == F.col("terms.CUI"),
        "left"
    ).select(
        F.col("matches.NOMENCLATURE_ID"),
        F.col("matches.temp_snomed_code").alias("SNOMED_CODE"), 
        F.col("matches.SNOMED_TYPE"),
        F.col("matches.SNOMED_MATCH_COUNT"),
        F.col("terms.TERM").alias("SNOMED_TERM")
    )
    
    # Then join with base DataFrame
    base_df = base_df.alias("base")
    return base_df.join(
        with_terms.alias("with_terms"),
        F.col("base.NOMENCLATURE_ID") == F.col("with_terms.NOMENCLATURE_ID"),
        "left"
    ).select(
        *[F.col(f"base.{c}") for c in base_df.columns if c != "SNOMED_CODE"],
        F.col("with_terms.SNOMED_CODE"),
        F.col("with_terms.SNOMED_TYPE"),
        F.col("with_terms.SNOMED_MATCH_COUNT"),
        F.col("with_terms.SNOMED_TERM")
    )

# COMMAND ----------

def create_snomed_mapping_incr():
    """Creates an incremental SNOMED mapping table processing only new/modified records."""

    max_adc_updt = get_max_timestamp("4_prod.bronze.temptwo_nomenclature")
    
    # Get base and reference tables
    base_df = spark.table("4_prod.bronze.tempone_nomenclature") \
        .filter(F.col("ADC_UPDT") > max_adc_updt)

    # Extract SNOMED code and create a new DataFrame with all columns
    base_df = base_df.select(
        "*",  # Keep all original columns
        F.when(
            F.col("CONCEPT_CKI").like("SNOMED!%"),
            F.regexp_extract(F.col("CONCEPT_CKI"), "SNOMED!(.*)", 1)
        ).otherwise(F.col("FOUND_CUI")).alias("SNOMED_CODE")
    )

    # Load and prepare reference tables
    icd_map = spark.table("3_lookup.trud.maps_icdsctmap")
    snomed_hier = spark.table("3_lookup.trud.snomed_scthier")
    snomed_terms = spark.table("3_lookup.trud.snomed_sct")
    code_value = spark.table("3_lookup.mill.mill_code_value")
    
    # Get OMOP reference tables with explicit column selection
    source_concepts = spark.table("3_lookup.omop.concept") \
        .filter(F.col("invalid_reason").isNull()) \
        .select("concept_id", "concept_code")
    
    target_concepts = spark.table("3_lookup.omop.concept") \
        .filter(F.col("invalid_reason").isNull()) \
        .select("concept_id", "concept_code")
    
    omop_rels = spark.table("3_lookup.omop.concept_relationship") \
        .filter(F.col("relationship_id").isin(["Maps to", "Mapped from"])) \
        .select("concept_id_1", "concept_id_2", "relationship_id")

    # Process direct matches
    direct_matches = base_df.join(
        icd_map,
        base_df.SNOMED_CODE == icd_map.TCUI,
        "left"
    ).select(
        base_df["*"],
        icd_map.SCUI,
        icd_map.TCUI
    )
    
    direct_with_omop = add_omop_preference(
        df=direct_matches,
        source_code_col="SNOMED_CODE",
        target_code_col="SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    # Process child matches
    child_matches = base_df.join(
        snomed_hier.select(
            F.col("CHILD"),
            F.col("PARENT")
        ),
        base_df.SNOMED_CODE == F.col("CHILD"),
        "left"
    ).join(
        icd_map,
        F.col("PARENT") == icd_map.TCUI,
        "left"
    ).select(
        base_df["*"],
        icd_map.SCUI,
        icd_map.TCUI,
        F.col("PARENT")
    ).filter(F.col("SCUI").isNotNull())
    
    child_with_omop = add_omop_preference(
        df=child_matches,
        source_code_col="PARENT",
        target_code_col="SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    # Process parent matches
    parent_matches = base_df.join(
        snomed_hier.select(
            F.col("CHILD"),
            F.col("PARENT")
        ),
        base_df.SNOMED_CODE == F.col("PARENT"),
        "left"
    ).join(
        icd_map,
        F.col("CHILD") == icd_map.TCUI,
        "left"
    ).select(
        base_df["*"],
        icd_map.SCUI,
        icd_map.TCUI,
        F.col("CHILD")
    ).filter(F.col("SCUI").isNotNull())
    
    parent_with_omop = add_omop_preference(
        df=parent_matches,
        source_code_col="CHILD",
        target_code_col="SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    # Process native SNOMED codes
    snomed_code_values = code_value.filter(
        F.col("CDF_MEANING").isin(["SNMCT", "SNMUKEMED"])
    )
    
    native_codes = base_df.filter(
        (F.col("SOURCE_VOCABULARY_CD").isin(
            [row.CODE_VALUE for row in snomed_code_values.collect()]
        )) |
        (F.col("CONCEPT_CKI").like("SNOMED!%"))
    ).select(
        F.col("NOMENCLATURE_ID"),
        F.col("SNOMED_CODE").alias("temp_snomed_code"),
        F.lit(1).alias("SNOMED_MATCH_COUNT"),
        F.lit(0).alias("HAS_OMOP_MAP"),
        F.lit("NATIVE").alias("SNOMED_TYPE")
    )

    # Process the results
    direct_final = process_matches(direct_with_omop, "EXACT", "OMOP_ASSISTED")
    child_final = process_matches(child_with_omop, "CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED")
    parent_final = process_matches(parent_with_omop, "PARENTMATCH", "PARENTMATCH_OMOP_ASSISTED")

    # Combine all matches
    all_matches = combine_matches(native_codes, direct_final, child_final, parent_final)

    # Add SNOMED terms
    final_df = add_snomed_terms(all_matches, snomed_terms, base_df)

    return final_df

# COMMAND ----------



updates_df = create_snomed_mapping_incr()
    

update_nomen_table(updates_df, "4_prod.bronze.temptwo_nomenclature")

# COMMAND ----------

def add_omop_preference_icd10(df, source_code_col, target_code_col, source_concepts, target_concepts, omop_rels):
    """
    Adds OMOP mapping preferences to help prioritize mappings.
    
    Args:
        df: Source DataFrame
        source_code_col: Column name for source codes
        target_code_col: Column name for target codes
        source_concepts: OMOP source concepts DataFrame
        target_concepts: OMOP target concepts DataFrame
        omop_rels: OMOP relationships DataFrame
    
    Returns:
        DataFrame with added OMOP mapping preference
    """
    return df.join(
        source_concepts,
        F.col(source_code_col) == source_concepts.concept_code,
        "left"
    ).join(
        target_concepts,
        F.col(target_code_col) == target_concepts.concept_code,
        "left"
    ).join(
        omop_rels,
        (
            (F.col("source_concepts.concept_id") == F.col("rel.concept_id_1")) &
            (F.col("target_concepts.concept_id") == F.col("rel.concept_id_2")) |
            (F.col("source_concepts.concept_id") == F.col("rel.concept_id_2")) &
            (F.col("target_concepts.concept_id") == F.col("rel.concept_id_1"))
        ),
        "left"
    ).withColumn(
        "has_omop_map",
        F.when(F.col("rel.relationship_id").isNotNull(), True).otherwise(False)
    )


def process_hierarchical_matches(df, base_type, omop_type):
    """Helper function to process hierarchical matches (child/parent)"""
    return df.withColumn(
        "match_rank",
        F.row_number().over(
            Window.partitionBy("NOMENCLATURE_ID")
            .orderBy(
                F.col("has_omop_map").desc(),
                F.col("icd_map.SCUI")
            )
        )
    ).groupBy("NOMENCLATURE_ID").agg(
        F.first(F.when(F.col("match_rank") == 1, F.col("icd_map.SCUI"))).alias("ICD10_CODE"),
        F.count("icd_map.SCUI").alias("ICD10_MATCH_COUNT"),
        F.max(F.col("has_omop_map")).alias("HAS_OMOP_MAP"),
        F.when(F.max(F.col("has_omop_map")), F.lit(omop_type))
         .otherwise(F.lit(base_type))
         .alias("ICD10_TYPE")
    )

def process_native_codes(base_df, icd10_code_values):
    """Helper function to process native ICD10 codes"""
    return base_df.filter(
        (F.col("SOURCE_VOCABULARY_CD").isin(
            [row.CODE_VALUE for row in icd10_code_values.collect()]
        )) |
        (F.col("CONCEPT_CKI").like("ICD10WHO!%"))
    ).select(
        "NOMENCLATURE_ID",
        F.when(F.col("CONCEPT_CKI").like("ICD10WHO!%"),
            F.regexp_replace(F.col("CONCEPT_CKI"), "ICD10WHO!", "")
        ).otherwise(F.col("FOUND_CUI")).alias("ICD10_CODE"),
        F.lit(1).alias("ICD10_MATCH_COUNT"),
        F.lit(False).alias("HAS_OMOP_MAP"),
        F.lit("NATIVE").alias("ICD10_TYPE")
    )

def combine_matches(*dfs):
    """Helper function to combine and prioritize matches"""
    combined = reduce(
        lambda df1, df2: df1.unionAll(df2),
        [df.select("NOMENCLATURE_ID", "ICD10_CODE", "ICD10_MATCH_COUNT", 
                  "HAS_OMOP_MAP", "ICD10_TYPE") for df in dfs]
    )
    
    return combined.withColumn(
        "priority",
        F.when(F.col("ICD10_TYPE") == "NATIVE", 1)
        .when(F.col("ICD10_TYPE").isin("EXACT", "OMOP_ASSISTED"), 2)
        .when(F.col("ICD10_TYPE").isin("CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED"), 3)
        .when(F.col("ICD10_TYPE").isin("PARENTMATCH", "PARENTMATCH_OMOP_ASSISTED"), 4)
        .otherwise(5)
    ).withColumn(
        "row_num",
        F.row_number().over(
            Window.partitionBy("NOMENCLATURE_ID")
            .orderBy("priority", "ICD10_MATCH_COUNT")
        )
    ).filter(F.col("row_num") == 1).drop("priority", "row_num")

def combine_matches_icd10(*dfs):
    """
    Helper function to combine and prioritize ICD10 matches
    
    Args:
        *dfs: Variable number of DataFrames to combine
        
    Returns:
        DataFrame: Combined and prioritized matches
    """
    # Add unique aliases to each DataFrame
    aliased_dfs = []
    for i, df in enumerate(dfs):
        aliased_dfs.append(
            df.select(
                F.col("NOMENCLATURE_ID"),
                F.col("ICD10_CODE"),
                F.col("ICD10_MATCH_COUNT"),
                F.col("HAS_OMOP_MAP"),
                F.col("ICD10_TYPE")
            ).alias(f"df_{i}")
        )
    
    combined = reduce(lambda df1, df2: df1.unionAll(df2), aliased_dfs)
    
    return combined.withColumn(
        "priority",
        F.when(F.col("ICD10_TYPE") == "NATIVE", 1)
        .when(F.col("ICD10_TYPE").isin("EXACT", "OMOP_ASSISTED"), 2)
        .when(F.col("ICD10_TYPE").isin("CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED"), 3)
        .when(F.col("ICD10_TYPE").isin("PARENTMATCH", "PARENTMATCH_OMOP_ASSISTED"), 4)
        .otherwise(5)
    ).withColumn(
        "row_num",
        F.row_number().over(
            Window.partitionBy("NOMENCLATURE_ID")
            .orderBy("priority", "ICD10_MATCH_COUNT")
        )
    ).filter(F.col("row_num") == 1).drop("priority", "row_num")

def add_icd10_terms(matches_df, icd_terms, base_df):
    """
    Helper function to add ICD10 terms to final mappings
    
    Args:
        matches_df: DataFrame with ICD10 matches
        icd_terms: DataFrame with ICD10 terms
        base_df: Original base DataFrame
        
    Returns:
        DataFrame: Final mappings with ICD10 terms
    """
    # First join matches with terms
    matches_df = matches_df.alias("matches")
    icd_terms = icd_terms.alias("terms")
    
    with_terms = matches_df.join(
        icd_terms,
        F.col("matches.ICD10_CODE") == F.col("terms.CUI"),
        "left"
    ).select(
        F.col("matches.NOMENCLATURE_ID"),
        F.col("matches.ICD10_CODE"),
        F.col("matches.ICD10_TYPE"),
        F.col("matches.ICD10_MATCH_COUNT"),
        F.col("terms.TERM").alias("ICD10_TERM")
    )
    
    # Then join with base DataFrame
    base_df = base_df.alias("base")
    return base_df.join(
        with_terms.alias("with_terms"),
        F.col("base.NOMENCLATURE_ID") == F.col("with_terms.NOMENCLATURE_ID"),
        "left"
    ).select(
        *[F.col(f"base.{c}") for c in base_df.columns],
        F.col("with_terms.ICD10_CODE"),
        F.col("with_terms.ICD10_TYPE"),
        F.col("with_terms.ICD10_MATCH_COUNT"),
        F.col("with_terms.ICD10_TERM")
    )

def process_matches_icd10(df, base_type, omop_type):
    """
    Helper function to process ICD10 matches and aggregate results
    
    Args:
        df: DataFrame containing matches
        base_type: Base match type string
        omop_type: OMOP-assisted match type string
        
    Returns:
        DataFrame: Processed and aggregated matches
    """
    df = df.alias("matches")
    return df.withColumn(
        "match_rank",
        F.row_number().over(
            Window.partitionBy("matches.NOMENCLATURE_ID")
            .orderBy(
                F.col("matches.has_omop_map").desc(),
                F.col("matches.SCUI")
            )
        )
    ).groupBy(F.col("matches.NOMENCLATURE_ID")).agg(
        F.first(F.when(F.col("match_rank") == 1, F.col("matches.SCUI"))).alias("ICD10_CODE"),
        F.count("matches.SCUI").alias("ICD10_MATCH_COUNT"),
        F.max(F.col("matches.has_omop_map")).alias("HAS_OMOP_MAP"),
        F.when(F.max(F.col("matches.has_omop_map")), F.lit(omop_type)) 
         .otherwise(F.lit(base_type))
         .alias("ICD10_TYPE")
    )

# COMMAND ----------

def create_icd10_mapping_incr():
    """
    Creates an incremental ICD10 mapping table processing only new/modified records.
        
    Returns:
        DataFrame: Incremental ICD10 mappings
    """

    max_adc_updt = get_max_timestamp("4_prod.bronze.tempthree_nomenclature")
    
    # Get base and reference tables
    base_df = spark.table("4_prod.bronze.temptwo_nomenclature") \
        .filter(col("ADC_UPDT") > max_adc_updt) \
        .alias("base")

    icd_map = spark.table("3_lookup.trud.maps_icdsctmap").alias("icd_map")
    snomed_hier = spark.table("3_lookup.trud.snomed_scthier").alias("hier")
    icd_terms = spark.table("3_lookup.trud.maps_icd").alias("terms")
    code_value = spark.table("3_lookup.mill.mill_code_value")
    
    # Get OMOP reference tables
    source_concepts = spark.table("3_lookup.omop.concept") \
        .filter(col("invalid_reason").isNull()) \
        .alias("source_concepts")
    
    target_concepts = spark.table("3_lookup.omop.concept") \
        .filter(col("invalid_reason").isNull()) \
        .alias("target_concepts")
    
    omop_rels = spark.table("3_lookup.omop.concept_relationship") \
        .filter(col("relationship_id").isin(["Maps to", "Mapped from"])) \
        .alias("rel")

    # Process direct matches
    direct_matches = base_df.join(
        icd_map,
        col("base.SNOMED_CODE") == col("icd_map.TCUI"),
        "left"
    )
    
    direct_with_omop = add_omop_preference_icd10(
        df=direct_matches,
        source_code_col="base.SNOMED_CODE",
        target_code_col="icd_map.SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    # Process child matches
    child_matches = base_df.join(
        snomed_hier,
        col("base.SNOMED_CODE") == col("hier.CHILD"),
        "left"
    ).join(
        icd_map,
        col("hier.PARENT") == col("icd_map.TCUI"),
        "left"
    ).filter(col("icd_map.SCUI").isNotNull())
    
    child_with_omop = add_omop_preference_icd10(
        df=child_matches,
        source_code_col="hier.PARENT",
        target_code_col="icd_map.SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    # Process parent matches
    parent_matches = base_df.join(
        snomed_hier,
        col("base.SNOMED_CODE") == col("hier.PARENT"),
        "left"
    ).join(
        icd_map,
        col("hier.CHILD") == col("icd_map.TCUI"),
        "left"
    ).filter(col("icd_map.SCUI").isNotNull())
    
    parent_with_omop = add_omop_preference_icd10(
        df=parent_matches,
        source_code_col="hier.CHILD",
        target_code_col="icd_map.SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    # Process the results
    direct_final = process_matches_icd10(direct_with_omop, "EXACT", "OMOP_ASSISTED")
    child_final = process_matches_icd10(child_with_omop, "CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED")
    parent_final = process_matches_icd10(parent_with_omop, "PARENTMATCH", "PARENTMATCH_OMOP_ASSISTED")

    # Process native ICD10 codes
    icd10_code_values = code_value.filter(
        col("CDF_MEANING").isin(["ICD10", "ICD10WHO"])
    )
    
    native_codes = base_df.filter(
        (col("base.SOURCE_VOCABULARY_CD").isin(
            [row.CODE_VALUE for row in icd10_code_values.collect()]
        )) |
        (col("base.CONCEPT_CKI").like("ICD10WHO!%"))
    ).select(
        col("base.NOMENCLATURE_ID"),
        when(col("base.CONCEPT_CKI").like("ICD10WHO!%"),
            regexp_replace(col("base.CONCEPT_CKI"), "ICD10WHO!", "")
        ).otherwise(col("base.SNOMED_CODE")).alias("ICD10_CODE"),
        lit(1).alias("ICD10_MATCH_COUNT"),
        lit(False).alias("HAS_OMOP_MAP"),
        lit("NATIVE").alias("ICD10_TYPE")
    )

    # Combine all matches
    all_matches = combine_matches_icd10(native_codes, direct_final, child_final, parent_final)

    # Add ICD10 terms and return final result
    final_df = add_icd10_terms(all_matches, icd_terms, base_df)

    return final_df

# COMMAND ----------


updates_df = create_icd10_mapping_incr()
    

update_nomen_table(updates_df, "4_prod.bronze.tempthree_nomenclature")

# COMMAND ----------



def add_omop_preference_opcs4(df, source_code_col, target_code_col, source_concepts, target_concepts, omop_rels):
    """
    Adds OMOP mapping preferences to help prioritize mappings.
    Args:
     df: Source DataFrame
     source_code_col: Column name for source codes
     target_code_col: Column name for target codes
     source_concepts: OMOP source concepts DataFrame
     target_concepts: OMOP target concepts DataFrame
     omop_rels: OMOP relationships DataFrame

    Returns:
     DataFrame with added OMOP mapping preference
    """
    return df.join(
        source_concepts,
        F.col(source_code_col) == source_concepts.concept_code,
        "left"
    ).join(
        target_concepts,
        F.col(target_code_col) == target_concepts.concept_code,
        "left"
    ).join(
         omop_rels,
        (
        (F.col("source_concepts.concept_id") == F.col("rel.concept_id_1")) &
        (F.col("target_concepts.concept_id") == F.col("rel.concept_id_2")) |
        (F.col("source_concepts.concept_id") == F.col("rel.concept_id_2")) &
        (F.col("target_concepts.concept_id") == F.col("rel.concept_id_1"))
        ),
        "left"
    ).withColumn(
        "has_omop_map",
        F.when(F.col("rel.relationship_id").isNotNull(), True).otherwise(False)
    )



def process_matches_opcs4(df, base_type, omop_type):
    """
Helper function to process matches and aggregate results
CopyArgs:
    df: DataFrame containing matches
    base_type: Base match type string
    omop_type: OMOP-assisted match type string

Returns:
    DataFrame: Processed and aggregated matches
    """
    return df.withColumn(
    "match_rank",
    F.row_number().over(
        Window.partitionBy("NOMENCLATURE_ID")
        .orderBy(
            F.col("has_omop_map").desc(),
            F.col("opcs_map.SCUI")
        )
    )
    ).groupBy("NOMENCLATURE_ID").agg(
    F.first(F.when(F.col("match_rank") == 1, F.col("opcs_map.SCUI"))).alias("OPCS4_CODE"),
    F.count("opcs_map.SCUI").alias("OPCS4_MATCH_COUNT"),
    F.max(F.col("has_omop_map")).alias("HAS_OMOP_MAP"),
    F.when(F.max(F.col("has_omop_map")), F.lit(omop_type))
     .otherwise(F.lit(base_type))
     .alias("OPCS4_TYPE")
    ).withColumn(
    "OPCS4_TYPE",
    F.when(F.col("OPCS4_CODE").isNotNull(), F.col("OPCS4_TYPE"))
    )

def process_native_codes_opcs4(base_df, opcs4_code_values):
    """
Helper function to process native OPCS4 codes
CopyArgs:
    base_df: Base DataFrame
    opcs4_code_values: DataFrame containing valid OPCS4 codes

Returns:
    DataFrame: Processed native OPCS4 codes
    """
    return base_df.filter(
    (F.col("SOURCE_VOCABULARY_CD").isin(
        [row.CODE_VALUE for row in opcs4_code_values.collect()]
    )) |
    (F.col("CONCEPT_CKI").like("OPCS4!%"))
    ).select(
    "NOMENCLATURE_ID",
    F.when(F.col("CONCEPT_CKI").like("OPCS4!%"),
        F.regexp_replace(F.col("CONCEPT_CKI"), "OPCS4!", "")
    ).otherwise(F.col("FOUND_CUI")).alias("OPCS4_CODE"),
    F.lit(1).alias("OPCS4_MATCH_COUNT"),
    F.lit(False).alias("HAS_OMOP_MAP"),
    F.lit("NATIVE").alias("OPCS4_TYPE")
    )

def combine_matches_opcs4(*dfs):
    """
Helper function to combine and prioritize matches
CopyArgs:
    *dfs: Variable number of DataFrames to combine

Returns:
    DataFrame: Combined and prioritized matches
    """
    combined = reduce(
    lambda df1, df2: df1.unionAll(df2),
    [df.select(
        "NOMENCLATURE_ID", 
        "OPCS4_CODE", 
        "OPCS4_MATCH_COUNT", 
        "HAS_OMOP_MAP", 
        "OPCS4_TYPE"
    ) for df in dfs]
    )

    return combined.withColumn(
    "priority",
    F.when(F.col("OPCS4_TYPE") == "NATIVE", 1)
    .when(F.col("OPCS4_TYPE").isin("EXACT", "OMOP_ASSISTED"), 2)
    .when(F.col("OPCS4_TYPE").isin("CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED"), 3)
    .when(F.col("OPCS4_TYPE").isin("PARENTMATCH", "PARENTMATCH_OMOP_ASSISTED"), 4)
    .otherwise(5)
    ).withColumn(
    "row_num",
    F.row_number().over(
        Window.partitionBy("NOMENCLATURE_ID")
        .orderBy("priority", "OPCS4_MATCH_COUNT")
    )
    ).filter(F.col("row_num") == 1).drop("priority", "row_num")

def add_opcs4_terms(matches_df, opcs_terms, base_df):
    """
    Helper function to add OPCS4 terms to final mappings
    Args:
        matches_df: DataFrame with OPCS4 matches
        opcs_terms: DataFrame with OPCS4 terms
        base_df: Original base DataFrame

    Returns:
        DataFrame: Final mappings with OPCS4 terms
    """
    # First join matches with terms
    with_terms = matches_df.join(
        opcs_terms,
        matches_df.OPCS4_CODE == opcs_terms.CUI,
        "left"
    ).select(
        "NOMENCLATURE_ID",
        "OPCS4_CODE",
        "OPCS4_TYPE",
        "OPCS4_MATCH_COUNT",
        F.col("TERM").alias("OPCS4_TERM")
    )

    # Then join with base DataFrame and preserve all columns
    return base_df.join(
        with_terms,
        "NOMENCLATURE_ID",
        "left"
    ).select(
        # Keep all original columns from base_df
        *[F.col(c) for c in base_df.columns],
        # Add new OPCS4 columns
        F.col("OPCS4_CODE"),
        F.col("OPCS4_TYPE"),
        F.col("OPCS4_MATCH_COUNT"),
        F.col("OPCS4_TERM")
    )

# COMMAND ----------

def create_opcs4_mapping_incr():
    """
    Creates an incremental OPCS4 mapping table processing only new/modified records.
    
        
    Returns:
        DataFrame: Incremental OPCS4 mappings
    """

    max_adc_updt = get_max_timestamp("4_prod.bronze.nomenclature")

    # Get base and reference tables
    base_df = spark.table("4_prod.bronze.tempthree_nomenclature") \
        .filter(col("ADC_UPDT") > max_adc_updt) \
        .alias("base")

    opcs_map = spark.table("3_lookup.trud.maps_opcssctmap").alias("opcs_map")
    snomed_hier = spark.table("3_lookup.trud.snomed_scthier").alias("hier")
    opcs_terms = spark.table("3_lookup.trud.maps_opcs").alias("terms")
    code_value = spark.table("3_lookup.mill.mill_code_value")

    # Get OMOP reference tables
    source_concepts = spark.table("3_lookup.omop.concept") \
        .filter(col("invalid_reason").isNull()) \
        .alias("source_concepts")
    
    target_concepts = spark.table("3_lookup.omop.concept") \
        .filter(col("invalid_reason").isNull()) \
        .alias("target_concepts")
    
    omop_rels = spark.table("3_lookup.omop.concept_relationship") \
        .filter(col("relationship_id").isin(["Maps to", "Mapped from"])) \
        .alias("rel")

    # Process direct matches
    direct_matches = base_df.join(
        opcs_map,
        col("base.SNOMED_CODE") == col("opcs_map.TCUI"),
        "left"
    )

    direct_with_omop = add_omop_preference_opcs4(
        df=direct_matches,
        source_code_col="SNOMED_CODE",
        target_code_col="opcs_map.SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    direct_final = process_matches_opcs4(direct_with_omop, "EXACT", "OMOP_ASSISTED")

    # Process hierarchical matches
    child_matches = base_df.join(
        snomed_hier,
        col("base.SNOMED_CODE") == col("hier.CHILD"),
        "left"
    ).join(
        opcs_map,
        col("hier.PARENT") == col("opcs_map.TCUI"),
        "left"
    ).filter(col("opcs_map.SCUI").isNotNull())

    child_with_omop = add_omop_preference_opcs4(
        df=child_matches,
        source_code_col="hier.PARENT",
        target_code_col="opcs_map.SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    child_final = process_matches_opcs4(child_with_omop, "CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED")

    parent_matches = base_df.join(
        snomed_hier,
        col("base.SNOMED_CODE") == col("hier.PARENT"),
        "left"
    ).join(
        opcs_map,
        col("hier.CHILD") == col("opcs_map.TCUI"),
        "left"
    ).filter(col("opcs_map.SCUI").isNotNull())

    parent_with_omop = add_omop_preference_opcs4(
        df=parent_matches,
        source_code_col="hier.CHILD",
        target_code_col="opcs_map.SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    parent_final = process_matches_opcs4(parent_with_omop, "PARENTMATCH", "PARENTMATCH_OMOP_ASSISTED")

    # Process native OPCS4 codes
    opcs4_code_values = code_value.filter(col("CDF_MEANING") == "OPCS4")
    native_codes = process_native_codes_opcs4(base_df, opcs4_code_values)

    # Combine all matches
    all_matches = combine_matches_opcs4(native_codes, direct_final, child_final, parent_final)

    # Add OPCS4 terms and return final result
    final_df = add_opcs4_terms(all_matches, opcs_terms, base_df)

    return final_df

# COMMAND ----------


updates_df = create_opcs4_mapping_incr()
    

update_nomen_table(updates_df, "4_prod.bronze.nomenclature")
