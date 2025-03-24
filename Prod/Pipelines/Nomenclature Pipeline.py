# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *
from pyspark.sql import functions as F
from functools import reduce
from delta.tables import DeltaTable

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
        max_date = spark.sql(f"SELECT MAX(ADC_UPDT) AS max_date FROM {table_name}").first()["max_date"]
        
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
        # Check if source_df is empty

    if source_df is None:
        return
    if source_df.count() == 0:
        return
    source_df = source_df.distinct()
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
    #return code.strip().upper()
    return code.replace('.', '').replace(' ', '').upper()

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


    
    max_adc_updt = get_max_timestamp("4_prod.bronze.tempone_nomenclature")



    # Get base nomenclature data
    base_nomenclature = (
        spark.table("3_lookup.mill.mill_nomenclature")
        .filter(
            (col("ACTIVE_IND") == 1) & 
            (col("END_EFFECTIVE_DT_TM").isNull() | (col("END_EFFECTIVE_DT_TM") > current_timestamp())) &
            (col("ADC_UPDT") > max_adc_updt) &
            (col("source_identifier").isNotNull()) &
            (trim(col("source_identifier")) != "")
        )
    )
    
    if(base_nomenclature.count() == 0):
        return base_nomenclature
    

    code_value = spark.table("3_lookup.mill.mill_code_value").select(
        "CODE_VALUE", "CDF_MEANING", "DESCRIPTION", "DISPLAY"
    )

    # Create vocabulary-specific reference subsets
    snomed_vocab_codes = code_value.filter(col("CDF_MEANING").isin(["SNMCT", "SNMUKEMED"]))
    multum_vocab_codes = code_value.filter(col("CDF_MEANING") == "MUL.DRUG")
    icd10_vocab_codes = code_value.filter(col("CDF_MEANING").isin(["ICD10", "ICD10WHO"]))
    opcs4_vocab_codes = code_value.filter(col("CDF_MEANING") == "OPCS4")
    
    snomed_sct = spark.table("3_lookup.trud.snomed_sct").filter(
        (col("CUI").isNotNull()) &
        (col("TUI").isNotNull()) &
        (col("TERM").isNotNull())
    )
    
    omop_concepts = spark.table("3_lookup.omop.concept").filter(
        (col("concept_id").isNotNull()) &
        (col("invalid_reason").isNull())
    )

    icd10_concepts = spark.table("3_lookup.omop.concept").filter(
        (col("concept_id").isNotNull()) &
        (col("invalid_reason").isNull()) &
        (col("vocabulary_id").isin(["ICD10", "ICD10CM"]))
    )

    opcs4_concepts = spark.table("3_lookup.omop.concept").filter(
        (col("concept_id").isNotNull()) &
        (col("invalid_reason").isNull()) &
        (col("vocabulary_id") == "OPCS4")
    )

    # Initialize vocabulary type indicators
    base_with_vocab_type = (
        base_nomenclature
        .withColumn("is_snomed", col("SOURCE_VOCABULARY_CD").isin(466776237, 673967))
        .withColumn("is_multum", col("SOURCE_VOCABULARY_CD").isin(64004559, 1238, 1237))
        .withColumn("is_icd10", col("SOURCE_VOCABULARY_CD").isin(2976507, 647081))
        .withColumn("is_opcs4", col("SOURCE_VOCABULARY_CD") == 685812)
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
        .when(col("is_opcs4"), regexp_replace(upper(col("SOURCE_IDENTIFIER")), "[^A-Z0-9]", ""))
        .otherwise(col("SOURCE_IDENTIFIER"))
    )

    # Process ICD10 matches with standardized concept codes
    icd10_matches = (
        base_with_vocab_type.filter(col("is_icd10"))
        .join(
            icd10_concepts.withColumn(
                "standardized_concept_code",
                regexp_replace(upper(col("concept_code")), "[^A-Z0-9]", "")
            ),
            col("standardized_code") == col("standardized_concept_code"),
            "left"
        )
        .withColumn("vocabulary_type", lit("ICD10"))
    )

    opcs4_matches = (
        base_with_vocab_type.filter(col("is_opcs4"))
        .join(
            opcs4_concepts.withColumn(
                "standardized_concept_code",
                regexp_replace(upper(trim(col("concept_code"))), "[^A-Z0-9]", "")
            ),
            col("standardized_code") == col("standardized_concept_code"),
            "left"
        )
        .withColumn("vocabulary_type", lit("OPCS4"))
    )

    other_matches = (
        base_with_vocab_type.filter(~col("is_icd10") & ~col("is_opcs4"))
        .join(
            spark.table("3_lookup.omop.concept")
            .filter(
                (col("concept_id").isNotNull()) &
                (col("invalid_reason").isNull()) &
                (~col("vocabulary_id").isin(["ICD10", "ICD10CM", "OPCS4"]))
            ),
            base_with_vocab_type.standardized_code == col("concept_code"),
            "left"
        )
        .withColumn("vocabulary_type", 
            when(col("is_snomed"), lit("SNOMED"))
            .when(col("is_multum"), lit("MULTUM"))
            .otherwise(lit("OTHER"))
        )
    )

    # Define common columns for all matches
    common_columns = [
        col("NOMENCLATURE_ID"),
        col("SOURCE_IDENTIFIER"),
        col("SOURCE_STRING"),
        col("SOURCE_VOCABULARY_CD"),
        col("VOCAB_AXIS_CD"),
        col("CONCEPT_CKI"),
        col("ADC_UPDT"),
        col("concept_id").alias("OMOP_CONCEPT_ID"),
        col("concept_name").alias("OMOP_CONCEPT_NAME"),
        col("standard_concept").alias("IS_STANDARD_OMOP_CONCEPT"),
        col("domain_id").alias("CONCEPT_DOMAIN"),
        col("concept_class_id").alias("CONCEPT_CLASS"),
        col("standardized_code").alias("FOUND_CUI"),
        col("vocabulary_type")
    ]

    # Select common columns for each match type
    icd10_matches = icd10_matches.select(*common_columns)
    opcs4_matches = opcs4_matches.select(*common_columns)
    other_matches = other_matches.select(*common_columns)

    # Combine all matches
    matches = icd10_matches.unionAll(opcs4_matches).unionAll(other_matches)
    matches = matches.withColumn(
        "match_count",
        count("OMOP_CONCEPT_ID").over(Window.partitionBy("SOURCE_IDENTIFIER"))
    ).withColumn(
        "match_priority",
        when(col("vocabulary_type") == "SNOMED", 1)
        .when(col("vocabulary_type") == "MULTUM", 2)
        .when(col("vocabulary_type") == "ICD10", 3)
        .when(col("vocabulary_type") == "OPCS4", 4)
        .otherwise(5)
    ).withColumn(
        "row_num",
        row_number().over(
            Window.partitionBy("NOMENCLATURE_ID")
            .orderBy(
                col("match_priority"),
                when(col("IS_STANDARD_OMOP_CONCEPT") == "S", 1).otherwise(2),
                col("match_count")
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
            col("OMOP_CONCEPT_ID"),
            col("OMOP_CONCEPT_NAME"),
            col("IS_STANDARD_OMOP_CONCEPT"),
            col("CONCEPT_DOMAIN"),
            col("CONCEPT_CLASS"),
            col("FOUND_CUI"),
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
    print(f"Count of with source concepts: {with_source_concept.count()}")
    with_source_concept.show(5)
    target_concepts.show(5)
    # Join target codes with OMOP concepts
    with_target_concept = with_source_concept.join(
        target_concepts,
        F.col(target_code_col).cast(StringType()) == F.col("target_concept_code").cast(StringType()),
        "left"
    )
    print(f"Count of with target concepts: {with_target_concept.count()}")
    
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
    print(f"Processing {omop_type} matches of base {base_type} records")

    
    # First filter to only records that actually got a match
    matched_df = df.filter(F.col("matched_snomed_code").isNotNull())
    
    if matched_df.count() == 0:  # No matches found
        return matched_df
        
    return matched_df.withColumn(
        "match_rank",
        F.row_number().over(
            Window.partitionBy("matches.NOMENCLATURE_ID")
            .orderBy(
                F.col("matches.has_omop_map").desc(),
                F.col("matches.matched_snomed_code")
            )
        )
    ).groupBy(F.col("matches.NOMENCLATURE_ID")).agg(
        F.first(F.when(F.col("match_rank") == 1, F.col("matches.matched_snomed_code"))).alias("temp_snomed_code"),
        F.count("matches.matched_snomed_code").alias("SNOMED_MATCH_COUNT"),
        F.max(F.col("matches.has_omop_map")).alias("HAS_OMOP_MAP"),
        F.when(F.max(F.col("matches.has_omop_map")), F.lit(omop_type))
         .otherwise(F.lit(base_type))
         .alias("SNOMED_TYPE")
    )

def combine_matches(*dfs):
    """Helper function to combine and prioritize matches"""
    # Check if all DataFrames are empty
    if all(df.count() == 0 for df in dfs):
        # Return an empty DataFrame with the expected schema
        return spark.createDataFrame(
            [],
            schema=StructType([
                StructField("NOMENCLATURE_ID", StringType(), True),
                StructField("temp_snomed_code", StringType(), True),
                StructField("SNOMED_MATCH_COUNT", IntegerType(), True),
                StructField("HAS_OMOP_MAP", BooleanType(), True),
                StructField("SNOMED_TYPE", StringType(), True)
            ])
        )

    # Add unique aliases to each DataFrame
    aliased_dfs = []
    for i, df in enumerate(dfs):
        if df.count() > 0:  # Only process non-empty DataFrames
            aliased_dfs.append(
                df.select(
                    F.col("NOMENCLATURE_ID"),
                    F.col("temp_snomed_code"),
                    F.col("SNOMED_MATCH_COUNT"),
                    F.col("HAS_OMOP_MAP"),
                    F.col("SNOMED_TYPE")
                ).alias(f"df_{i}")
            )
    
    if not aliased_dfs:  # If no valid DataFrames after filtering
        return spark.createDataFrame(
            [],
            schema=StructType([
                StructField("NOMENCLATURE_ID", StringType(), True),
                StructField("temp_snomed_code", StringType(), True),
                StructField("SNOMED_MATCH_COUNT", IntegerType(), True),
                StructField("HAS_OMOP_MAP", BooleanType(), True),
                StructField("SNOMED_TYPE", StringType(), True)
            ])
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

def process_secondary_snomed_mappings(df, omop_concepts, concept_relationship):
    """
    Processes secondary SNOMED mappings by going through OMOP for records that don't have direct SNOMED mappings.
    This specifically handles the ICD10 -> OMOP -> SNOMED path.
    
    Args:
        df: DataFrame with existing mappings
        omop_concepts: OMOP concepts reference table
        concept_relationship: OMOP concept relationships table
        
    Returns:
        DataFrame: Updated mappings with additional SNOMED codes found via OMOP
    """
    # First identify records that have OMOP mappings but no SNOMED mappings
    candidates = df.filter(
        (F.col("OMOP_CONCEPT_ID").isNotNull()) &
        (F.col("SNOMED_CODE").isNull())
    ).select(
        "NOMENCLATURE_ID",
        "OMOP_CONCEPT_ID",
        "NUMBER_OF_OMOP_MATCHES"
    ).alias("candidates")
    
    if candidates.count() == 0:
        return df
        
    # Get SNOMED target concepts
    snomed_targets = omop_concepts.filter(
        F.col("vocabulary_id") == "SNOMED"
    ).select(
        F.col("concept_id").alias("snomed_concept_id"),
        F.col("concept_code").alias("snomed_concept_code"),
        F.col("concept_name").alias("snomed_concept_name")
    ).alias("snomed")
    
    # Get valid relationships
    valid_relationships = concept_relationship.filter(
        F.col("relationship_id").isin(["Maps to", "Mapped from"])
    ).select(
        F.col("concept_id_1").alias("source_concept_id"),
        F.col("concept_id_2").alias("target_concept_id")
    ).alias("rels")
    
    # Add aliases to source concepts
    source_concepts = omop_concepts.select(
        F.col("concept_id").alias("source_id"),
        F.col("concept_code").alias("source_code")
    ).alias("source")
    
    # Get mappings from OMOP to SNOMED using concept_relationship
    omop_to_snomed = source_concepts.join(
        valid_relationships,
        F.col("source.source_id") == F.col("rels.source_concept_id"),
        "inner"
    ).join(
        snomed_targets,
        (F.col("rels.target_concept_id") == F.col("snomed.snomed_concept_id")) &
        (F.col("source.source_id") != F.col("snomed.snomed_concept_id")),
        "inner"
    ).select(
        F.col("source.source_id").alias("omop_id"),
        F.col("snomed.snomed_concept_code").alias("mapped_snomed_code"),
        F.col("snomed.snomed_concept_name").alias("mapped_snomed_name")
    )
    
    # Join candidates with OMOP-to-SNOMED mappings
    secondary_matches = candidates.join(
        omop_to_snomed,
        F.col("candidates.OMOP_CONCEPT_ID") == F.col("omop_id"),
        "inner"
    ).select(
        "NOMENCLATURE_ID",
        "NUMBER_OF_OMOP_MATCHES",
        "mapped_snomed_code",
        "mapped_snomed_name"
    )
    
    # Calculate combined match count
    secondary_matches = secondary_matches.withColumn(
        "match_count_per_nom",
        F.count("mapped_snomed_code").over(Window.partitionBy("NOMENCLATURE_ID"))
    ).withColumn(
        "combined_match_count",
        F.col("NUMBER_OF_OMOP_MATCHES") * F.col("match_count_per_nom")
    )
    
    # Select best match for each record
    best_matches = secondary_matches.withColumn(
        "row_num",
        F.row_number().over(
            Window.partitionBy("NOMENCLATURE_ID")
            .orderBy(F.col("combined_match_count"))
        )
    ).filter(F.col("row_num") == 1)
    
    # Update original records with secondary matches
    updated_records = best_matches.select(
        "NOMENCLATURE_ID",
        F.col("mapped_snomed_code").alias("SNOMED_CODE"),
        F.lit("OMOP_DERIVED").alias("SNOMED_TYPE"),
        F.col("combined_match_count").alias("SNOMED_MATCH_COUNT"),
        F.col("mapped_snomed_name").alias("SNOMED_TERM")
    )
    
    # Get all columns from original DataFrame except the ones we're updating
    original_columns = [col for col in df.columns if col not in 
                       ["SNOMED_CODE", "SNOMED_TYPE", "SNOMED_MATCH_COUNT", "SNOMED_TERM"]]
    
    # Combine original mappings with new ones
    result_df = df.alias("orig").join(
        updated_records.alias("updates"),
        "NOMENCLATURE_ID",
        "left_outer"
    )
    
    # Build the final selection
    select_expr = []
    
    # Add all original columns except the ones we're updating
    for col in original_columns:
        select_expr.append(F.col(f"orig.{col}"))
    
    # Add the updated/coalesced columns
    select_expr.extend([
        F.coalesce(F.col("orig.SNOMED_CODE"), F.col("updates.SNOMED_CODE")).alias("SNOMED_CODE"),
        F.coalesce(F.col("orig.SNOMED_TYPE"), F.col("updates.SNOMED_TYPE")).alias("SNOMED_TYPE"),
        F.coalesce(F.col("orig.SNOMED_MATCH_COUNT"), F.col("updates.SNOMED_MATCH_COUNT")).alias("SNOMED_MATCH_COUNT"),
        F.coalesce(F.col("orig.SNOMED_TERM"), F.col("updates.SNOMED_TERM")).alias("SNOMED_TERM")
    ])
    
    # Return the final DataFrame with the correct column selection
    return result_df.select(*select_expr)

# COMMAND ----------

def create_snomed_mapping_incr():
    """Creates an incremental SNOMED mapping table processing only new/modified records."""
    
    print("Starting SNOMED mapping process...")

    max_adc_updt = get_max_timestamp("4_prod.bronze.temptwo_nomenclature")
    print(f"Max ADC_UPDT timestamp: {max_adc_updt}")
    
    # Get base and reference tables
    base_df = spark.table("4_prod.bronze.tempone_nomenclature") \
        .filter(F.col("ADC_UPDT") > max_adc_updt)
    
    record_count = base_df.count()
    print(f"Number of records to process: {record_count}")
    
    if record_count == 0:
        print("No records to process")
        return base_df  # Return empty DataFrame with original schema

    # Extract SNOMED code and standardize FOUND_CUI
    base_df = base_df.select(
        "*",
        F.when(
            F.col("CONCEPT_CKI").like("SNOMED!%"),
            F.regexp_extract(F.col("CONCEPT_CKI"), "SNOMED!(.*)", 1)
        ).otherwise(F.lit(None)).alias("original_snomed_code"),
        standardize_code_udf(F.col("FOUND_CUI")).alias("standardized_found_cui")
    )

    # Count and print SNOMED codes found vs not found
    snomed_counts = base_df.select(
        F.sum(F.when(F.col("CONCEPT_CKI").like("SNOMED!%"), 1).otherwise(0)).alias("native_count"),
        F.sum(F.when(F.col("CONCEPT_CKI").like("SNOMED!%"), 0).otherwise(1)).alias("not_found_count")
    ).collect()[0]

    print(f"Native SNOMED codes found: {snomed_counts['native_count']:,}")
    print(f"SNOMED codes not found: {snomed_counts['not_found_count']:,}")

    # Load reference tables
    icd_map = spark.table("3_lookup.trud.maps_icdsctmap")
    opcs_map = spark.table("3_lookup.trud.maps_opcssctmap")
    snomed_hier = spark.table("3_lookup.trud.snomed_scthier")
    snomed_terms = spark.table("3_lookup.trud.snomed_sct")
    code_value = spark.table("3_lookup.mill.mill_code_value")
    
    # Get OMOP reference tables
    source_concepts = spark.table("3_lookup.omop.concept") \
        .filter(F.col("invalid_reason").isNull()) \
        .select("concept_id", "concept_code")
    
    target_concepts = source_concepts
    
    omop_rels = spark.table("3_lookup.omop.concept_relationship") \
        .filter(F.col("relationship_id").isin(["Maps to", "Mapped from"])) \
        .select("concept_id_1", "concept_id_2", "relationship_id")

    # Filter for ICD10 and OPCS4 source codes and those without existing SNOMED codes
    icd10_base_df = base_df.filter(
        (F.col("SOURCE_VOCABULARY_CD").isin([2976507, 647081])) &
        (F.col("original_snomed_code").isNull())
    )
    
    opcs4_base_df = base_df.filter(
        (F.col("SOURCE_VOCABULARY_CD") == 685812) &
        (F.col("original_snomed_code").isNull())
    )
    
    print(f"Number of ICD10 records to process: {icd10_base_df.count()}")
    print(f"Number of OPCS4 records to process: {opcs4_base_df.count()}")

    # Process direct matches for ICD10
    icd10_direct_matches = icd10_base_df.join(
        icd_map.withColumn("standardized_scui", standardize_code_udf(F.col("SCUI"))),
        F.col("standardized_found_cui") == F.col("standardized_scui"),
        "left"
    ).select(
        icd10_base_df["*"],
        icd_map.SCUI.alias("SCUI"),
        icd_map.TCUI.alias("matched_snomed_code"),
        F.lit(False).alias("has_omop_map")
    )

    # Process direct matches for OPCS4
    opcs4_direct_matches = opcs4_base_df.join(
        opcs_map.withColumn("standardized_scui", standardize_code_udf(F.col("SCUI"))),
        F.col("standardized_found_cui") == F.col("standardized_scui"),
        "left"
    ).select(
        opcs4_base_df["*"],
        opcs_map.SCUI.alias("SCUI"),
        opcs_map.TCUI.alias("matched_snomed_code"),
        F.lit(False).alias("has_omop_map")
    )
    
    direct_matches = icd10_direct_matches.unionAll(opcs4_direct_matches)
    print(f"Direct matches found: {direct_matches.filter(F.col('matched_snomed_code').isNotNull()).count()}")
    
    direct_with_omop = add_omop_preference(
        df=direct_matches,
        source_code_col="SCUI",
        target_code_col="matched_snomed_code",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )
    print(f"Direct matches with OMOP mappings found: {direct_with_omop.count()}")

    # Process prefix matches for ICD10
    icd10_prefix_matches = icd10_base_df.join(
        icd_map,
        F.substring(F.col("standardized_found_cui"), 1, 3) == F.substring(F.col("SCUI"), 1, 3),
        "left"
    ).select(
        icd10_base_df["*"],
        icd_map.SCUI.alias("SCUI"),
        icd_map.TCUI.alias("matched_snomed_code"),
        F.lit(False).alias("has_omop_map")
    ).filter(F.col("matched_snomed_code").isNotNull())

    # Process prefix matches for OPCS4
    opcs4_prefix_matches = opcs4_base_df.join(
        opcs_map,
        F.substring(F.col("standardized_found_cui"), 1, 3) == F.substring(F.col("SCUI"), 1, 3),
        "left"
    ).select(
        opcs4_base_df["*"],
        opcs_map.SCUI.alias("SCUI"),
        opcs_map.TCUI.alias("matched_snomed_code"),
        F.lit(False).alias("has_omop_map")
    ).filter(F.col("matched_snomed_code").isNotNull())
    
    prefix_matches = icd10_prefix_matches.unionAll(opcs4_prefix_matches)
    print(f"Prefix matches found: {prefix_matches.count()}")
    
    prefix_with_omop = add_omop_preference(
        df=prefix_matches,
        source_code_col="SCUI",
        target_code_col="matched_snomed_code",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )
    print("Here1")

    # Process native SNOMED codes (keep original ones)
    native_codes = base_df.filter(F.col("original_snomed_code").isNotNull()).select(
        F.col("NOMENCLATURE_ID"),
        F.col("original_snomed_code").alias("temp_snomed_code"),
        F.lit(1).alias("SNOMED_MATCH_COUNT"),
        F.lit(False).alias("HAS_OMOP_MAP"),
        F.lit("NATIVE").alias("SNOMED_TYPE")
    )
    print("Here2")
    # Process results with match types
    direct_final = process_matches(direct_with_omop, "EXACT", "OMOP_ASSISTED")
    print("Here3")
    prefix_final = process_matches(prefix_with_omop, "PREFIX", "PREFIX_OMOP_ASSISTED")

    print(f"Final direct matches: {direct_final.count()}")
    print(f"Final prefix matches: {prefix_final.count()}")

    # Combine all matches with priority ordering
    all_matches = combine_matches(
        native_codes,  # Priority 1
        direct_final,  # Priority 2
        prefix_final   # Priority 3
    )
    if all_matches.count() == 0:
        # Return the original DataFrame with null SNOMED columns
        return base_df.withColumn("SNOMED_CODE", F.lit(None)) \
                     .withColumn("SNOMED_TYPE", F.lit(None)) \
                     .withColumn("SNOMED_MATCH_COUNT", F.lit(None)) \
                     .withColumn("SNOMED_TERM", F.lit(None)) \
                     .drop("original_snomed_code", "standardized_found_cui")
    print(f"Total combined matches: {all_matches.count()}")

    # Add SNOMED terms to final result
    final_df = add_snomed_terms(all_matches, snomed_terms, base_df)
    # Process secondary mappings through OMOP for records without SNOMED codes
    omop_concepts = spark.table("3_lookup.omop.concept").filter(F.col("invalid_reason").isNull())
    concept_relationship = spark.table("3_lookup.omop.concept_relationship")
    final_df = process_secondary_snomed_mappings(final_df, omop_concepts, omop_rels)
    
    # Drop intermediate columns
    final_df = final_df.drop("original_snomed_code", "standardized_found_cui")
    
    print(f"Final record count: {final_df.count()}")

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
        F.col(source_code_col).cast(StringType()) == source_concepts.concept_code.cast(StringType()),
        "left"
    ).join(
        target_concepts,
        F.col(target_code_col).cast(StringType()) == target_concepts.concept_code.cast(StringType()),
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

def process_native_codes_icd10(base_df, icd10_code_values, icd_terms):
    """
    Process native ICD10 codes with standardization and term lookup.
    
    Args:
        base_df: Base DataFrame containing source records
        icd10_code_values: Reference table of valid ICD10 codes
        icd_terms: Reference table containing ICD10 terms
        
    Returns:
        DataFrame: Processed native ICD10 codes with standardized format and terms
    """
    native_codes = base_df.filter(
        (col("SOURCE_VOCABULARY_CD").isin(
            [row.CODE_VALUE for row in icd10_code_values.collect()]
        )) |
        (col("CONCEPT_CKI").like("ICD10WHO!%"))
    )
    
    # First standardize the codes
    with_standard_codes = native_codes.select(
        col("NOMENCLATURE_ID"),
        standardize_icd10_udf(
            when(col("CONCEPT_CKI").like("ICD10WHO!%"),
                regexp_replace(col("CONCEPT_CKI"), "ICD10WHO!", "")
            ).otherwise(col("SOURCE_IDENTIFIER"))
        ).alias("ICD10_CODE"),
        lit(1).alias("ICD10_MATCH_COUNT"),
        lit(False).alias("HAS_OMOP_MAP"),
        lit("NATIVE").alias("ICD10_TYPE")
    )
    
    # Then join with terms
    return with_standard_codes.join(
        icd_terms,
        col("ICD10_CODE") == col("CUI"),
        "left"
    ).select(
        col("NOMENCLATURE_ID"),
        col("ICD10_CODE"),
        col("ICD10_MATCH_COUNT"),
        col("HAS_OMOP_MAP"),
        col("ICD10_TYPE"),
        col("TERM").alias("ICD10_TERM")
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
        .orderBy(
            F.col("ICD10_CODE").isNull().asc(),  
            "priority", 
            "ICD10_MATCH_COUNT"
        )
    )
).filter(F.col("row_num") == 1)

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

def process_secondary_icd10_mappings(df, omop_concepts, concept_relationship):
    """
    Processes secondary ICD10 mappings by going through OMOP for records that don't have direct ICD10 mappings.
    This specifically handles the SNOMED -> OMOP -> ICD10 path.
    
    Args:
        df: DataFrame with existing mappings
        omop_concepts: OMOP concepts reference table
        concept_relationship: OMOP concept relationships table
        
    Returns:
        DataFrame: Updated mappings with additional ICD10 codes found via OMOP
    """
    # First identify records that have OMOP mappings but no ICD10 mappings
    candidates = df.filter(
        (F.col("OMOP_CONCEPT_ID").isNotNull()) &
        (F.col("ICD10_CODE").isNull())
    ).select(
        "NOMENCLATURE_ID",
        "OMOP_CONCEPT_ID",
        "NUMBER_OF_OMOP_MATCHES"
    ).alias("candidates")
    
    if candidates.count() == 0:
        return df
        
    # Get ICD10 target concepts
    icd10_targets = omop_concepts.filter(
        F.col("vocabulary_id").isin(["ICD10", "ICD10CM"])
    ).select(
        F.col("concept_id").alias("icd10_concept_id"),
        F.col("concept_code").alias("icd10_concept_code"),
        F.col("concept_name").alias("icd10_concept_name")
    ).alias("icd10")
    
    # Get valid relationships
    valid_relationships = concept_relationship.filter(
        F.col("relationship_id").isin(["Maps to", "Mapped from"])
    ).select(
        F.col("concept_id_1").alias("source_concept_id"),
        F.col("concept_id_2").alias("target_concept_id")
    ).alias("rels")
    
    # Add aliases to source concepts
    source_concepts = omop_concepts.select(
        F.col("concept_id").alias("source_id"),
        F.col("concept_code").alias("source_code")
    ).alias("source")
    
    # Get mappings from OMOP to ICD10 using concept_relationship
    omop_to_icd10 = source_concepts.join(
        valid_relationships,
        F.col("source.source_id") == F.col("rels.source_concept_id"),
        "inner"
    ).join(
        icd10_targets,
        (F.col("rels.target_concept_id") == F.col("icd10.icd10_concept_id")) &
        (F.col("source.source_id") != F.col("icd10.icd10_concept_id")),
        "inner"
    ).select(
        F.col("source.source_id").alias("omop_id"),
        F.col("icd10.icd10_concept_code").alias("mapped_icd10_code"),
        F.col("icd10.icd10_concept_name").alias("mapped_icd10_name")
    )
    
    # Join candidates with OMOP-to-ICD10 mappings
    secondary_matches = candidates.join(
        omop_to_icd10,
        F.col("candidates.OMOP_CONCEPT_ID") == F.col("omop_id"),
        "inner"
    ).select(
        "NOMENCLATURE_ID",
        "NUMBER_OF_OMOP_MATCHES",
        "mapped_icd10_code",
        "mapped_icd10_name"
    )
    
    # Calculate combined match count
    secondary_matches = secondary_matches.withColumn(
        "match_count_per_nom",
        F.count("mapped_icd10_code").over(Window.partitionBy("NOMENCLATURE_ID"))
    ).withColumn(
        "combined_match_count",
        F.col("NUMBER_OF_OMOP_MATCHES") * F.col("match_count_per_nom")
    )
    
    # Select best match for each record
    best_matches = secondary_matches.withColumn(
        "row_num",
        F.row_number().over(
            Window.partitionBy("NOMENCLATURE_ID")
            .orderBy(F.col("combined_match_count"))
        )
    ).filter(F.col("row_num") == 1)
    
    # Update original records with secondary matches
    updated_records = best_matches.select(
        "NOMENCLATURE_ID",
        F.col("mapped_icd10_code").alias("ICD10_CODE"),
        F.lit("OMOP_DERIVED").alias("ICD10_TYPE"),
        F.col("combined_match_count").alias("ICD10_MATCH_COUNT"),
        F.col("mapped_icd10_name").alias("ICD10_TERM")
    )
    
    # Get all columns from original DataFrame except the ones we're updating
    original_columns = [col for col in df.columns if col not in 
                       ["ICD10_CODE", "ICD10_TYPE", "ICD10_MATCH_COUNT", "ICD10_TERM"]]
    
    # Combine original mappings with new ones
    result_df = df.alias("orig").join(
        updated_records.alias("updates"),
        "NOMENCLATURE_ID",
        "left_outer"
    )
    
    # Build the final selection
    select_expr = []
    
    # Add all original columns except the ones we're updating
    for col in original_columns:
        select_expr.append(F.col(f"orig.{col}"))
    
    # Add the updated/coalesced columns
    select_expr.extend([
        F.coalesce(F.col("orig.ICD10_CODE"), F.col("updates.ICD10_CODE")).alias("ICD10_CODE"),
        F.coalesce(F.col("orig.ICD10_TYPE"), F.col("updates.ICD10_TYPE")).alias("ICD10_TYPE"),
        F.coalesce(F.col("orig.ICD10_MATCH_COUNT"), F.col("updates.ICD10_MATCH_COUNT")).alias("ICD10_MATCH_COUNT"),
        F.coalesce(F.col("orig.ICD10_TERM"), F.col("updates.ICD10_TERM")).alias("ICD10_TERM")
    ])
    
    # Return the final DataFrame with the correct column selection
    return result_df.select(*select_expr)

# COMMAND ----------

def create_icd10_mapping_incr():
    """
    Creates an incremental ICD10 mapping table processing only new/modified records.
        
    Returns:
        DataFrame: Incremental ICD10 mappings with standardized codes and terms
    """
    max_adc_updt = get_max_timestamp("4_prod.bronze.tempthree_nomenclature")
    
    # Get base and reference tables
    base_df = spark.table("4_prod.bronze.temptwo_nomenclature") \
        .filter(col("ADC_UPDT") > max_adc_updt) \
        .alias("base")

    if(base_df.count() == 0):
        return base_df

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
    print(direct_matches.count())
    
    direct_with_omop = add_omop_preference_icd10(
        df=direct_matches,
        source_code_col="base.SNOMED_CODE",
        target_code_col="icd_map.SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )
    print(direct_with_omop.count()) 

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


    print(child_matches.count())
    
    child_with_omop = add_omop_preference_icd10(
        df=child_matches,
        source_code_col="hier.PARENT",
        target_code_col="icd_map.SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    print(child_with_omop.count())

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
    
    print(parent_matches.count())

    parent_with_omop = add_omop_preference_icd10(
        df=parent_matches,
        source_code_col="hier.CHILD",
        target_code_col="icd_map.SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )
    print(parent_with_omop.count())
    # Process the results
    direct_final = process_matches_icd10(direct_with_omop, "EXACT", "OMOP_ASSISTED")
    print(direct_final.count())
    child_final = process_matches_icd10(child_with_omop, "CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED")
    print(child_final.count())
    parent_final = process_matches_icd10(parent_with_omop, "PARENTMATCH", "PARENTMATCH_OMOP_ASSISTED")
    print(parent_final.count())

    # Process native ICD10 codes with the new function
    icd10_code_values = code_value.filter(
        col("CDF_MEANING").isin(["ICD10", "ICD10WHO"])
    )
    
    native_codes = process_native_codes_icd10(base_df, icd10_code_values, icd_terms)
    print(native_codes.count())
    # Combine all matches
    all_matches = combine_matches_icd10(native_codes, direct_final, child_final, parent_final)
    print(all_matches.count())

    # Add ICD10 terms and return final result
    final_df = add_icd10_terms(all_matches, icd_terms, base_df)
    print(final_df.count())
    # Process secondary mappings through OMOP for records without ICD10 codes
    omop_concepts = spark.table("3_lookup.omop.concept").filter(F.col("invalid_reason").isNull())
    concept_relationship = spark.table("3_lookup.omop.concept_relationship")
    final_df = process_secondary_icd10_mappings(final_df, omop_concepts, concept_relationship)
    print(final_df.count())
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
        F.col(source_code_col).cast(StringType()) == source_concepts.concept_code.cast(StringType()),
        "left"
    ).join(
        target_concepts,
        F.col(target_code_col).cast(StringType()) == target_concepts.concept_code.cast(StringType()),
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
    
    Args:
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
            .orderBy(
                F.col("OPCS4_CODE").isNull().asc(),
                "priority", 
                "OPCS4_MATCH_COUNT"
            )
        )
    ).filter(F.col("row_num") == 1).drop("priority", "row_num")

def add_opcs4_terms(matches_df, opcs_terms, base_df):
    """
    Helper function to add OPCS4 terms to final mappings
    """
    print("Step 1: Before first join")
    # First join matches with terms
    with_terms = matches_df.join(
        opcs_terms,
        matches_df.OPCS4_CODE.cast(StringType()) == opcs_terms.CUI.cast(StringType()),
        "left"
    )
    
    print("Step 2: After first join, before select")
    with_terms.show(2)
    
    with_terms = with_terms.select(
        "NOMENCLATURE_ID",
        "OPCS4_CODE",
        "OPCS4_TYPE",
        "OPCS4_MATCH_COUNT",
        F.col("TERM").alias("OPCS4_TERM")
    )
    
    print("Step 3: After select, before final join")
    with_terms.show(2)

    # Then join with base DataFrame and preserve all columns
    final_df = base_df.join(
        with_terms,
        "NOMENCLATURE_ID",
        "left"
    )
    
    print("Step 4: After final join, before final select")
    final_df.show(2)
    
    return final_df.select(
        # Keep all original columns from base_df
        *[F.col(c) for c in base_df.columns],
        # Add new OPCS4 columns
        F.col("OPCS4_CODE"),
        F.col("OPCS4_TYPE"),
        F.col("OPCS4_MATCH_COUNT"),
        F.col("OPCS4_TERM")
    )

# COMMAND ----------

def process_secondary_opcs4_mappings(df, omop_concepts, concept_relationship):
    """
    Processes secondary OPCS4 mappings by going through OMOP for records that don't have direct OPCS4 mappings.
    This specifically handles the SNOMED -> OMOP -> OPCS4 path.
    
    Args:
        df: DataFrame with existing mappings
        omop_concepts: OMOP concepts reference table
        concept_relationship: OMOP concept relationships table
        
    Returns:
        DataFrame: Updated mappings with additional OPCS4 codes found via OMOP
    """
    # First identify records that have OMOP mappings but no OPCS4 mappings
    candidates = df.filter(
        (F.col("OMOP_CONCEPT_ID").isNotNull()) &
        (F.col("OPCS4_CODE").isNull())
    ).select(
        "NOMENCLATURE_ID",
        "OMOP_CONCEPT_ID",
        "NUMBER_OF_OMOP_MATCHES"
    ).alias("candidates")
    
    if candidates.count() == 0:
        return df
        
    # Get OPCS4 target concepts
    opcs4_targets = omop_concepts.filter(
        F.col("vocabulary_id") == "OPCS4"
    ).select(
        F.col("concept_id").alias("opcs4_concept_id"),
        F.col("concept_code").alias("opcs4_concept_code"),
        F.col("concept_name").alias("opcs4_concept_name")
    ).alias("opcs4")
    
    # Get valid relationships
    valid_relationships = concept_relationship.filter(
        F.col("relationship_id").isin(["Maps to", "Mapped from"])
    ).select(
        F.col("concept_id_1").alias("source_concept_id"),
        F.col("concept_id_2").alias("target_concept_id")
    ).alias("rels")
    
    # Add aliases to source concepts
    source_concepts = omop_concepts.select(
        F.col("concept_id").alias("source_id"),
        F.col("concept_code").alias("source_code")
    ).alias("source")
    
    # Get mappings from OMOP to OPCS4 using concept_relationship
    omop_to_opcs4 = source_concepts.join(
        valid_relationships,
        F.col("source.source_id") == F.col("rels.source_concept_id"),
        "inner"
    ).join(
        opcs4_targets,
        (F.col("rels.target_concept_id") == F.col("opcs4.opcs4_concept_id")) &
        (F.col("source.source_id") != F.col("opcs4.opcs4_concept_id")),
        "inner"
    ).select(
        F.col("source.source_id").alias("omop_id"),
        F.col("opcs4.opcs4_concept_code").alias("mapped_opcs4_code"),
        F.col("opcs4.opcs4_concept_name").alias("mapped_opcs4_name")
    )
    
    # Join candidates with OMOP-to-OPCS4 mappings
    secondary_matches = candidates.join(
        omop_to_opcs4,
        F.col("candidates.OMOP_CONCEPT_ID") == F.col("omop_id"),
        "inner"
    ).select(
        "NOMENCLATURE_ID",
        "NUMBER_OF_OMOP_MATCHES",
        "mapped_opcs4_code",
        "mapped_opcs4_name"
    )
    
    # Calculate combined match count
    secondary_matches = secondary_matches.withColumn(
        "match_count_per_nom",
        F.count("mapped_opcs4_code").over(Window.partitionBy("NOMENCLATURE_ID"))
    ).withColumn(
        "combined_match_count",
        F.col("NUMBER_OF_OMOP_MATCHES") * F.col("match_count_per_nom")
    )
    
    # Select best match for each record
    best_matches = secondary_matches.withColumn(
        "row_num",
        F.row_number().over(
            Window.partitionBy("NOMENCLATURE_ID")
            .orderBy(F.col("combined_match_count"))
        )
    ).filter(F.col("row_num") == 1)
    
    # Update original records with secondary matches
    updated_records = best_matches.select(
        "NOMENCLATURE_ID",
        F.col("mapped_opcs4_code").alias("OPCS4_CODE"),
        F.lit("OMOP_DERIVED").alias("OPCS4_TYPE"),
        F.col("combined_match_count").alias("OPCS4_MATCH_COUNT"),
        F.col("mapped_opcs4_name").alias("OPCS4_TERM")
    )
    
    # Get all columns from original DataFrame except the ones we're updating
    original_columns = [col for col in df.columns if col not in 
                       ["OPCS4_CODE", "OPCS4_TYPE", "OPCS4_MATCH_COUNT", "OPCS4_TERM"]]
    
    # Combine original mappings with new ones
    result_df = df.alias("orig").join(
        updated_records.alias("updates"),
        "NOMENCLATURE_ID",
        "left_outer"
    )
    
    # Build the final selection
    select_expr = []
    
    # Add all original columns except the ones we're updating
    for col in original_columns:
        select_expr.append(F.col(f"orig.{col}"))
    
    # Add the updated/coalesced columns
    select_expr.extend([
        F.coalesce(F.col("orig.OPCS4_CODE"), F.col("updates.OPCS4_CODE")).alias("OPCS4_CODE"),
        F.coalesce(F.col("orig.OPCS4_TYPE"), F.col("updates.OPCS4_TYPE")).alias("OPCS4_TYPE"),
        F.coalesce(F.col("orig.OPCS4_MATCH_COUNT"), F.col("updates.OPCS4_MATCH_COUNT")).alias("OPCS4_MATCH_COUNT"),
        F.coalesce(F.col("orig.OPCS4_TERM"), F.col("updates.OPCS4_TERM")).alias("OPCS4_TERM")
    ])
    
    # Return the final DataFrame with the correct column selection
    return result_df.select(*select_expr)

# COMMAND ----------

def create_opcs4_mapping_incr():
    """
    Creates an incremental OPCS4 mapping table processing only new/modified records.
    
        
    Returns:
        DataFrame: Incremental OPCS4 mappings
    """

    max_adc_updt = get_max_timestamp("4_prod.bronze.tempfour_nomenclature")

    # Get base and reference tables
    base_df = spark.table("4_prod.bronze.tempthree_nomenclature") \
        .filter(col("ADC_UPDT") > max_adc_updt) \
        .alias("base")

    if(base_df.count() == 0):
        return base_df
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
    print("Counter A {}.".format(direct_matches.count()))

    direct_with_omop = add_omop_preference_opcs4(
        df=direct_matches,
        source_code_col="SNOMED_CODE",
        target_code_col="opcs_map.SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    direct_final = process_matches_opcs4(direct_with_omop, "EXACT", "OMOP_ASSISTED")
    print("Counter B {}.".format(direct_final.count()))

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

    print("Counter D {}.".format(child_matches.count()))

    child_with_omop = add_omop_preference_opcs4(
        df=child_matches,
        source_code_col="hier.PARENT",
        target_code_col="opcs_map.SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    print("Counter E {}.".format(child_with_omop.count()))

    child_final = process_matches_opcs4(child_with_omop, "CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED")
    print("Counter C {}.".format(child_final.count()))
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

    opcs4_code_values = code_value.filter(col("CDF_MEANING") == "OPCS4")
    native_codes = process_native_codes_opcs4(base_df, opcs4_code_values)

    # Combine all matches
    all_matches = combine_matches_opcs4(native_codes, direct_final, child_final, parent_final)

    final_df = add_opcs4_terms(all_matches, opcs_terms, base_df)

    # Process secondary mappings through OMOP for records without OPCS4 codes
    omop_concepts = spark.table("3_lookup.omop.concept").filter(F.col("invalid_reason").isNull())
    concept_relationship = spark.table("3_lookup.omop.concept_relationship")
    final_df = process_secondary_opcs4_mappings(final_df, omop_concepts, concept_relationship)
    return final_df

# COMMAND ----------


opcs_updates_df = create_opcs4_mapping_incr()

if(opcs_updates_df is None):
    print("No new records to process.")
else:
    print("Processing {} new records.".format(opcs_updates_df.count()))

update_nomen_table(opcs_updates_df, "4_prod.bronze.tempfour_nomenclature")

# COMMAND ----------

def process_tertiary_omop_mappings(df, omop_concepts, concept_relationship):
    """
    Processes tertiary OMOP mappings sequentially: SNOMED first, then ICD10, then OPCS4
    only for records that don't already have OMOP mappings.
    """
    result_df = df
    
    # Get valid concepts and relationships once
    valid_concepts = omop_concepts.filter(F.col("invalid_reason").isNull())
    valid_relationships = concept_relationship.filter(
        F.col("relationship_id").isin(["Maps to", "Mapped from"])
    )
    
    # Process SNOMED mappings first
    snomed_candidates = result_df.alias("r").filter(
        (F.col("r.OMOP_CONCEPT_ID").isNull()) &
        (F.col("r.SNOMED_CODE").isNotNull()) & 
        (F.col("r.SNOMED_TYPE") != "NATIVE")
    )
    
    if snomed_candidates.count() > 0:
        snomed_mappings = process_code_to_omop_mapping(
            candidates_df=snomed_candidates,
            source_code_col="SNOMED_CODE",
            concepts_df=valid_concepts.filter(F.col("vocabulary_id") == "SNOMED"),
            relationships_df=valid_relationships,
            source_type="SNOMED"
        )
        
        # Update result_df with SNOMED mappings
        result_df = result_df.alias("orig").join(
            snomed_mappings.alias("new"),
            "NOMENCLATURE_ID",
            "left_outer"
        ).select(
            *[F.col("orig." + c) for c in result_df.columns if c not in ["OMOP_CONCEPT_ID", "OMOP_CONCEPT_NAME"]],
            F.coalesce("orig.OMOP_CONCEPT_ID", "new.OMOP_CONCEPT_ID").alias("OMOP_CONCEPT_ID"),
            F.coalesce("orig.OMOP_CONCEPT_NAME", "new.OMOP_CONCEPT_NAME").alias("OMOP_CONCEPT_NAME")
        )
    
    # Process ICD10 mappings for remaining unmapped records
    icd10_candidates = result_df.alias("r").filter(
        (F.col("r.OMOP_CONCEPT_ID").isNull()) &
        (F.col("r.ICD10_CODE").isNotNull()) & 
        (F.col("r.ICD10_TYPE") != "NATIVE")
    )
    
    if icd10_candidates.count() > 0:
        icd10_mappings = process_code_to_omop_mapping(
            candidates_df=icd10_candidates,
            source_code_col="ICD10_CODE",
            concepts_df=valid_concepts.filter(F.col("vocabulary_id").isin(["ICD10", "ICD10CM"])),
            relationships_df=valid_relationships,
            source_type="ICD10"
        )
        
        # Update result_df with ICD10 mappings
        result_df = result_df.alias("orig").join(
            icd10_mappings.alias("new"),
            "NOMENCLATURE_ID",
            "left_outer"
        ).select(
            *[F.col("orig." + c) for c in result_df.columns if c not in ["OMOP_CONCEPT_ID", "OMOP_CONCEPT_NAME"]],
            F.coalesce("orig.OMOP_CONCEPT_ID", "new.OMOP_CONCEPT_ID").alias("OMOP_CONCEPT_ID"),
            F.coalesce("orig.OMOP_CONCEPT_NAME", "new.OMOP_CONCEPT_NAME").alias("OMOP_CONCEPT_NAME")
        )
    
    # Process OPCS4 mappings for remaining unmapped records
    opcs4_candidates = result_df.alias("r").filter(
        (F.col("r.OMOP_CONCEPT_ID").isNull()) &
        (F.col("r.OPCS4_CODE").isNotNull()) & 
        (F.col("r.OPCS4_TYPE") != "NATIVE")
    )
    
    if opcs4_candidates.count() > 0:
        opcs4_mappings = process_code_to_omop_mapping(
            candidates_df=opcs4_candidates,
            source_code_col="OPCS4_CODE",
            concepts_df=valid_concepts.filter(F.col("vocabulary_id") == "OPCS4"),
            relationships_df=valid_relationships,
            source_type="OPCS4"
        )
        
        # Update result_df with OPCS4 mappings
        result_df = result_df.alias("orig").join(
            opcs4_mappings.alias("new"),
            "NOMENCLATURE_ID",
            "left_outer"
        ).select(
            *[F.col("orig." + c) for c in result_df.columns if c not in ["OMOP_CONCEPT_ID", "OMOP_CONCEPT_NAME"]],
            F.coalesce("orig.OMOP_CONCEPT_ID", "new.OMOP_CONCEPT_ID").alias("OMOP_CONCEPT_ID"),
            F.coalesce("orig.OMOP_CONCEPT_NAME", "new.OMOP_CONCEPT_NAME").alias("OMOP_CONCEPT_NAME")
        )
    
    return result_df

def process_code_to_omop_mapping(candidates_df, source_code_col, concepts_df, relationships_df, source_type):
    """
    Helper function to process mappings from a specific code type to OMOP,
    ensuring only one mapping per nomenclature_id
    """
    # Join candidates with OMOP concepts to get concept_ids
    source_matches = candidates_df.join(
        concepts_df.select(
            F.col("concept_id").alias("source_concept_id"),
            F.col("concept_code").alias("source_concept_code"),
            F.col("standard_concept").alias("source_standard")
        ),
        F.col(source_code_col).cast(StringType()) == F.col("source_concept_code").cast(StringType()),
        "inner"
    )
    
    # Use relationships to find target OMOP concepts
    with_relationships = source_matches.join(
        relationships_df.select(
            F.col("concept_id_1").alias("rel_source_id"),
            F.col("concept_id_2").alias("rel_target_id")
        ),
        (F.col("source_concept_id") == F.col("rel_source_id")),
        "inner"
    ).join(
        concepts_df.select(
            F.col("concept_id").alias("target_concept_id"),
            F.col("concept_name").alias("target_concept_name"),
            F.col("standard_concept").alias("target_standard"),
            F.col("domain_id").alias("target_domain")
        ),
        F.col("rel_target_id") == F.col("target_concept_id"),
        "inner"
    )
    
    # Select best match for each record using window function
    window_spec = Window.partitionBy("NOMENCLATURE_ID").orderBy(
        # Prefer standard concepts
        F.when(F.col("target_standard") == "S", 1)
         .otherwise(0).desc(),
        # Prefer certain domains based on source type
        F.when(F.col("target_domain").isin(get_preferred_domains(source_type)), 1)
         .otherwise(0).desc(),
        # Use concept_id as final tiebreaker for deterministic results
        F.col("target_concept_id")
    )
    
    return with_relationships.withColumn(
        "row_num", F.row_number().over(window_spec)
    ).filter(
        F.col("row_num") == 1
    ).select(
        "NOMENCLATURE_ID",
        F.col("target_concept_id").alias("OMOP_CONCEPT_ID"),
        F.col("target_concept_name").alias("OMOP_CONCEPT_NAME")
    )

def get_preferred_domains(source_type):
    """
    Returns preferred OMOP domains based on source vocabulary type
    """
    domain_preferences = {
        "SNOMED": ["Condition", "Procedure", "Observation"],
        "ICD10": ["Condition"],
        "OPCS4": ["Procedure"]
    }
    return domain_preferences.get(source_type, [])

# COMMAND ----------

def create_final_nomenclature_incr():
    """
    Creates an incremental final nomenclature table with additional derived OMOP mappings.
    Only processes records that have been added or modified since the last run.
    
    Returns:
        DataFrame: Incremental final nomenclature mappings with all possible OMOP codes
    """
    # Get max timestamp from final table
    max_adc_updt = get_max_timestamp("4_prod.bronze.nomenclature")
    
    # Get base nomenclature table with original column names
    base_df = spark.table("4_prod.bronze.tempfour_nomenclature").filter(
        F.col("ADC_UPDT") > max_adc_updt
    )
    
    # If no new records, return empty DataFrame with correct schema
    if base_df.count() == 0:
        return base_df
    
    # Get OMOP reference tables
    omop_concepts = spark.table("3_lookup.omop.concept").filter(
        F.col("invalid_reason").isNull()
    )
    
    concept_relationship = spark.table("3_lookup.omop.concept_relationship")
    
    # Process tertiary mappings
    final_df = process_tertiary_omop_mappings(
        df=base_df,
        omop_concepts=omop_concepts,
        concept_relationship=concept_relationship
    )
    
    return final_df

# Create and save incremental final nomenclature table
updates_df = create_final_nomenclature_incr()
update_nomen_table(updates_df, "4_prod.bronze.nomenclature")

# Only proceed if updates_df exists and has records
if updates_df and updates_df.count() > 0:
    # Read the full nomenclature table
    full_nomenclature = spark.table("4_prod.bronze.nomenclature")
    
    # Write to destination, overwriting if exists
    full_nomenclature.write \
        .mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .saveAsTable("3_lookup.mill.map_nomenclature")
    
    print("Successfully replicated nomenclature table to lookup location")
else:
    print("No updates to replicate - skipping replication")
