# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

import dlt
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
    For row tracking enabled tables, uses _row_modified.
    For other tables, falls back to ADC_UPDT.
    If no date is found, returns January 1, 1980 as a default date.
    
    Args:
        table_name (str): Name of the table to query
    
    Returns:
        datetime: Maximum timestamp or default date
    """
    try:
        default_date = lit(datetime(1980, 1, 1)).cast(TimestampType())
        
        # Check if table has row tracking enabled
        table_properties = spark.sql(f"DESCRIBE DETAIL {table_name}").select("properties").collect()[0]["properties"]
        has_row_tracking = table_properties.get("delta.enableRowTracking", "false").lower() == "true"
        
        if has_row_tracking:
            result = spark.sql(f"SELECT MAX(_row_modified) AS max_date FROM {table_name}")
        else:
            result = spark.sql(f"SELECT MAX(ADC_UPDT) AS max_date FROM {table_name}")
            
        max_date = result.select(max("max_date").alias("max_date")).first()["max_date"]
        return max_date if max_date is not None else default_date
        
    except Exception as e:
        print(f"Error getting max timestamp from {table_name}: {str(e)}")
        return lit(datetime(1980, 1, 1)).cast(TimestampType())

# COMMAND ----------

# Reference table definitions using Delta Live Tables (DLT)
@dlt.table(
    name="code_value",
    comment="Code Value Reference Table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect("valid_codes", "CODE_VALUE IS NOT NULL")
@dlt.expect("valid_meanings", "CDF_MEANING IS NOT NULL")
def lookup_code_value():
    """
    Creates a reference table for code values and their meanings.
    Includes data quality expectations for non-null values.
    """
    return (
        spark.table("3_lookup.mill.mill_code_value")
        .select("CODE_VALUE", "CDF_MEANING", "DESCRIPTION", "DISPLAY")
    )

@dlt.table(
    name="snomed_sct",
    comment="SNOMED Reference Table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect("valid_cui", "CUI IS NOT NULL")
@dlt.expect("valid_tui", "TUI IS NOT NULL")
@dlt.expect("valid_term", "TERM IS NOT NULL")
def lookup_snomed():
    """
    Creates a reference table for SNOMED clinical terms.
    Includes data quality expectations for required fields.
    """
    return spark.table("3_lookup.trud.snomed_sct")

@dlt.table(
    name="omop_concept",
    comment="OMOP Concept Reference Table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect("valid_concept", "concept_id IS NOT NULL")
@dlt.expect("active_concepts", "invalid_reason IS NULL")
def lookup_omop_concept():
    """
    Creates a reference table for OMOP concepts.
    Filters out invalid concepts and includes data quality expectations.
    """
    return (
        spark.table("3_lookup.omop.concept")
        .filter(col("invalid_reason").isNull())
    )

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

# Helper function to standardize Multum drug codes
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
    # Add explicit aliases for clarity in joins
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
    # Add explicit aliases for clarity in joins
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

@dlt.table(
    name="nomenclature_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def nomenclature_mapping_incr():
    """
    Creates an incremental mapping table that combines all vocabulary mappings.
    This is a temporary table used as an intermediate step in the pipeline and
    will only include rows which have been changes since the base table was last updated.
    """
    # Get reference tables from previous pipeline steps
    code_values = dlt.read("code_value")
    snomed_sct = dlt.read("snomed_sct")
    omop_concepts = dlt.read("omop_concept")

    # Find the last updated time for the base table
    max_adc_updt = get_max_timestamp("4_prod.dlt.tempone_nomenclature")
    
    # Create vocabulary-specific reference subsets
    snomed_vocab_codes = code_values.filter(col("CDF_MEANING").isin(["SNMCT", "SNMUKEMED"]))
    multum_vocab_codes = code_values.filter(col("CDF_MEANING") == "MUL.DRUG")
    icd10_vocab_codes = code_values.filter(col("CDF_MEANING").isin(["ICD10", "ICD10WHO"]))
    opcs4_vocab_codes = code_values.filter(col("CDF_MEANING") == "OPCS4")

    # Get base nomenclature data, filtering for active records only which have been altered since the last update
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
    return (
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

@dlt.view(name="nomenclature_update")
def nomenclature_update():
    """
    Creates a streaming view of the incremental nomenclature mappings, this is a version of the incr_table created above which allows it to be fed into a dlt apply_changes function.
    """
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.nomenclature_mapping_incr")
    )

# Define the final target table with optimization settings
dlt.create_target_table(
    name = "tempone_nomenclature",
    comment = "Nomenclature mappings with OMOP concepts",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "NOMENCLATURE_ID"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "tempone_nomenclature",
    source = "nomenclature_update",
    keys = ["NOMENCLATURE_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(
    name="maps_icdsctmap",
    comment="ICD10 to SNOMED-CT mapping table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect("valid_scui", "SCUI IS NOT NULL")
@dlt.expect("valid_tcui", "TCUI IS NOT NULL")
def lookup_icd_snomed_map():
    """
    Creates a reference table for ICD10 to SNOMED-CT mappings.
    Includes data quality expectations for source and target concept identifiers.
    
    Returns:
        DataFrame: ICD10 to SNOMED-CT mapping table
    """
    return spark.table("3_lookup.trud.maps_icdsctmap")

@dlt.table(
    name="maps_opcssctmap",
    comment="OPCS4 to SNOMED-CT mapping table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect("valid_scui", "SCUI IS NOT NULL")
@dlt.expect("valid_tcui", "TCUI IS NOT NULL")
def lookup_opcs_snomed_map():
    """
    Creates a reference table for OPCS4 to SNOMED-CT mappings.
    Includes data quality expectations for source and target concept identifiers.
    
    Returns:
        DataFrame: OPCS4 to SNOMED-CT mapping table
    """
    return spark.table("3_lookup.trud.maps_opcssctmap")


@dlt.table(
    name="omop_concept_relationship",
    comment="OMOP Concept Relationship Reference Table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect("valid_concept_id_1", "concept_id_1 IS NOT NULL")
@dlt.expect("valid_concept_id_2", "concept_id_2 IS NOT NULL")
@dlt.expect("valid_relationship", "relationship_id IS NOT NULL")
def lookup_omop_relationship():
    """
    Creates a reference table for OMOP concept relationships, specifically focusing
    on mapping relationships between concepts ('Maps to' and 'Mapped from').
    
    Data quality expectations:
    - Both source and target concept IDs must be non-null
    - Relationship type must be specified
    
    Filters:
    - Only includes 'Maps to' and 'Mapped from' relationships
    - Excludes other relationship types (e.g., 'Is a', 'Part of')
    
    Returns:
        DataFrame: Filtered OMOP concept relationships containing only mapping relationships
    """
    return (
        spark.table("3_lookup.omop.concept_relationship")
        .filter(col("relationship_id").isin(["Maps to", "Mapped from"]))
    )

# COMMAND ----------

# Helper functions
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

def add_omop_preference(df, source_code_col, target_code_col, omop_concepts, omop_relationships):
    """
    Adds OMOP mapping preferences to a DataFrame by joining with OMOP concept and relationship tables.
    This function helps prioritize mappings that align with standard OMOP relationships.
    
    Args:
        df (DataFrame): Source DataFrame containing codes to be mapped
        source_code_col (str): Name of the column containing source codes
        target_code_col (str): Name of the column containing target codes
        omop_concepts (DataFrame): OMOP concept reference table
        omop_relationships (DataFrame): OMOP relationship reference table
    
    Process:
    1. Joins source codes with OMOP concepts
    2. Joins target codes with OMOP concepts
    3. Validates relationships between concepts
    4. Adds preference flag for OMOP-validated mappings
    
    Returns:
        DataFrame: Original DataFrame enriched with OMOP mapping preferences
    """
    # Join source codes with OMOP concepts
    with_source_concept = df.join(
        omop_concepts,
        F.col(source_code_col) == omop_concepts.concept_code,
        "left"
    ).select(
        df["*"],
        omop_concepts.concept_id.alias("source_concept_id")
    )
    
    # Join target codes with OMOP concepts
    with_target_concept = with_source_concept.join(
        omop_concepts.alias("target_concepts"),
        F.col(target_code_col) == F.col("target_concepts.concept_code"),
        "left"
    ).select(
        with_source_concept["*"],
        F.col("target_concepts.concept_id").alias("target_concept_id")
    )
    
    # Validate relationships and add preference flag
    return with_target_concept.join(
        omop_relationships,
        (
            # Check for direct mapping relationship
            (F.col("source_concept_id") == omop_relationships.concept_id_1) &
            (F.col("target_concept_id") == omop_relationships.concept_id_2)
        ) |
        (
            # Check for reverse mapping relationship
            (F.col("source_concept_id") == omop_relationships.concept_id_2) &
            (F.col("target_concept_id") == omop_relationships.concept_id_1)
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

@dlt.table(
    name="rde_snomed_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def snomed_mapping_incr():
    """
    Creates an incremental SNOMED mapping table that processes only new or modified records.
    This temporary table serves as an intermediate step in the mapping pipeline.
    
    Process flow:
    1. Identifies records modified since last update
    2. Loads required reference tables
    3. Processes different code types (SNOMED, ICD10, OPCS4)
    4. Applies mapping logic with prioritization
    
    Dependencies:
    - tempone_nomenclature: Base nomenclature table
    - Reference tables: SNOMED, ICD10, OPCS4 mappings
    - OMOP concept and relationship tables
    
    Returns:
        DataFrame: Incremental SNOMED mappings for new/modified records
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.rde.temptwo_nomenclature")
    
    # Get required reference tables
    base_df = dlt.read("tempone_nomenclature").filter(F.col("ADC_UPDT") > max_adc_updt)

    # Early exit if no new records
    #if base_df.limit(1).count() == 0:
        # Return empty DataFrame with same schema
        #return spark.createDataFrame([], base_df.schema)



    icd_snomed_map = dlt.read("maps_icdsctmap")
    opcs_snomed_map = dlt.read("maps_opcssctmap")
    snomed_terms = dlt.read("snomed_sct")
    code_value = dlt.read("code_value")
    omop_concepts = dlt.read("omop_concept")
    omop_relationships = dlt.read("omop_concept_relationship")

    # Define base columns that should be present in all mappings
    base_columns = [col for col in dlt.read("tempone_nomenclature").columns]

     # Process native SNOMED codes
    snomed_code_values = code_value.filter(F.col("CDF_MEANING").isin(["SNMCT", "SNMUKEMED"]))
    native_snomed = (
        # Filter base dataset for native SNOMED codes
        base_df.filter(F.col("SOURCE_VOCABULARY_CD").isin([row.CODE_VALUE for row in snomed_code_values.collect()]))
        .select(
            *base_columns,
            # Extract SNOMED code from CONCEPT_CKI field using regex
            F.regexp_extract(F.col("CONCEPT_CKI"), "SNOMED!(.*)", 1).alias("SNOMED_CODE"),
            # Mark these as native SNOMED mappings
            F.lit("NATIVE").alias("SNOMED_TYPE"),
            # Preserve original match count
            F.col("NUMBER_OF_OMOP_MATCHES").alias("NUMBER_SNOMED_MATCHES")
        )
    )

    # Process ICD10 codes
    # Identify valid ICD10 vocabulary codes
    icd10_code_values = code_value.filter(F.col("CDF_MEANING").isin(["ICD10", "ICD10WHO"]))
    # Filter base dataset for ICD10 codes and validate format
    icd10_df = base_df.filter(
        F.col("SOURCE_VOCABULARY_CD").isin([row.CODE_VALUE for row in icd10_code_values.collect()])
    ).filter(is_valid_icd10(F.col("SOURCE_IDENTIFIER")))

    # Generate different levels of ICD10 codes for hierarchical matching
    icd10_with_levels = generate_match_levels(icd10_df, "SOURCE_IDENTIFIER")
    
    # Perform ICD10 to SNOMED mapping with multiple matching strategies
    icd10_mapped = icd10_with_levels.join(
        icd_snomed_map,
        (
            # Exact match
            (icd10_with_levels.match_level_1 == icd_snomed_map.SCUI) |
            # Match ignoring suffixes
            (F.regexp_replace(icd_snomed_map.SCUI, '[0A-Z]+$', '') == icd10_with_levels.match_level_1) |
            # Category level match (first 3 characters)
            (icd10_with_levels.match_level_2 == F.substring(icd_snomed_map.SCUI, 1, 3)) |
            # Parent level match (excluding last character)
            (icd10_with_levels.match_level_3 == F.regexp_replace(icd_snomed_map.SCUI, '[0A-Z]+$', '')) |
            # Grandparent level match (excluding last 2 characters)
            (icd10_with_levels.match_level_4 == F.regexp_replace(icd_snomed_map.SCUI, '[0A-Z]+$', ''))
        ),
        "left"
    ).withColumn(
        # Count total number of matches found
        "match_count",
        F.count("TCUI").over(Window.partitionBy("NOMENCLATURE_ID"))
    ).withColumn(
        # Classify match type based on matching strategy used
        "match_type",
        F.when(icd10_with_levels.match_level_1 == icd_snomed_map.SCUI, "EXACT")
         .when(F.regexp_replace(icd_snomed_map.SCUI, '[0A-Z]+$', '') == icd10_with_levels.match_level_1, "EXACT")
         .when(icd10_with_levels.match_level_2 == F.substring(icd_snomed_map.SCUI, 1, 3), "PARTIAL")
         .when(icd10_with_levels.match_level_3 == F.regexp_replace(icd_snomed_map.SCUI, '[0A-Z]+$', ''), "PARTIAL")
         .when(icd10_with_levels.match_level_4 == F.regexp_replace(icd_snomed_map.SCUI, '[0A-Z]+$', ''), "PARTIAL")
         .otherwise(None)
    )

    # Add OMOP relationship validation for ICD10 mappings
    icd10_with_omop = add_omop_preference(
        icd10_mapped,
        "SOURCE_IDENTIFIER",
        "TCUI",
        omop_concepts,
        omop_relationships
    )

    # Select best ICD10 mapping based on match type and OMOP validation
    icd10_final = icd10_with_omop.withColumn(
        "match_rank",
        F.row_number().over(
            Window.partitionBy("NOMENCLATURE_ID")
            .orderBy(
                # Prioritize exact matches over partial matches
                F.when(F.col("match_type") == "EXACT", 0)
                 .when(F.col("match_type") == "PARTIAL", 1)
                 .otherwise(2),
                # Prefer OMOP-validated mappings
                F.col("omop_mapped").desc(),
                # Use SNOMED code as final tiebreaker
                F.col("TCUI")
            )
        )
    # Keep only the best match for each code
    ).filter(F.col("match_rank") == 1).select(
        *base_columns,
        F.col("TCUI").alias("SNOMED_CODE"),
        # Determine final match type, considering OMOP validation
        F.when(F.col("match_count") > 1,
            F.when(F.col("omop_mapped") == 1, "OMOP_ASSISTED")
            .otherwise(F.col("match_type"))
        ).otherwise(F.col("match_type")).alias("SNOMED_TYPE"),
        F.col("match_count").alias("NUMBER_SNOMED_MATCHES")
    )

    # Process OPCS4 codes (similar pattern to ICD10)
    opcs4_code_values = code_value.filter(F.col("CDF_MEANING") == "OPCS4")
    opcs4_df = base_df.filter(
        F.col("SOURCE_VOCABULARY_CD").isin([row.CODE_VALUE for row in opcs4_code_values.collect()])
    )

    # Generate matching levels for OPCS4 codes
    opcs4_with_levels = generate_match_levels(opcs4_df, "SOURCE_IDENTIFIER")
    
    # Perform OPCS4 to SNOMED mapping with multiple matching strategies
    opcs4_mapped = opcs4_with_levels.join(
        opcs_snomed_map,
        (
            # Same matching hierarchy as ICD10
            (opcs4_with_levels.match_level_1 == opcs_snomed_map.SCUI) |
            (F.regexp_replace(opcs_snomed_map.SCUI, '[0A-Z]+$', '') == opcs4_with_levels.match_level_1) |
            (opcs4_with_levels.match_level_2 == F.substring(opcs_snomed_map.SCUI, 1, 3)) |
            (opcs4_with_levels.match_level_3 == F.regexp_replace(opcs_snomed_map.SCUI, '[0A-Z]+$', '')) |
            (opcs4_with_levels.match_level_4 == F.regexp_replace(opcs_snomed_map.SCUI, '[0A-Z]+$', ''))
        ),
        "left"
    ).withColumn(
        "match_count",
        F.count("TCUI").over(Window.partitionBy("NOMENCLATURE_ID"))
    ).withColumn(
        "match_type",
        F.when(opcs4_with_levels.match_level_1 == opcs_snomed_map.SCUI, "EXACT")
         .when(F.regexp_replace(opcs_snomed_map.SCUI, '[0A-Z]+$', '') == opcs4_with_levels.match_level_1, "EXACT")
         .when(opcs4_with_levels.match_level_2 == F.substring(opcs_snomed_map.SCUI, 1, 3), "PARTIAL")
         .when(opcs4_with_levels.match_level_3 == F.regexp_replace(opcs_snomed_map.SCUI, '[0A-Z]+$', ''), "PARTIAL")
         .when(opcs4_with_levels.match_level_4 == F.regexp_replace(opcs_snomed_map.SCUI, '[0A-Z]+$', ''), "PARTIAL")
         .otherwise(None)
    )

    # Add OMOP relationship validation for OPCS4 mappings
    opcs4_with_omop = add_omop_preference(
        opcs4_mapped,
        "SOURCE_IDENTIFIER",
        "TCUI",
        omop_concepts,
        omop_relationships
    )

    # Select best OPCS4 mapping using same logic as ICD10
    opcs4_final = opcs4_with_omop.withColumn(
        "match_rank",
        F.row_number().over(
            Window.partitionBy("NOMENCLATURE_ID")
            .orderBy(
                F.when(F.col("match_type") == "EXACT", 0)
                 .when(F.col("match_type") == "PARTIAL", 1)
                 .otherwise(2),
                F.col("omop_mapped").desc(),
                F.col("TCUI")
            )
        )
    ).filter(F.col("match_rank") == 1).select(
        *base_columns,
        F.col("TCUI").alias("SNOMED_CODE"),
        F.when(F.col("match_count") > 1,
            F.when(F.col("omop_mapped") == 1, "OMOP_ASSISTED")
            .otherwise(F.col("match_type"))
        ).otherwise(F.col("match_type")).alias("SNOMED_TYPE"),
        F.col("match_count").alias("NUMBER_SNOMED_MATCHES")
    )

    # Handle remaining records that don't match any vocabulary
    all_mapped_code_values = [row.CODE_VALUE for row in 
        code_value.filter(F.col("CDF_MEANING").isin(["SNMCT", "SNMUKEMED", "ICD10", "ICD10WHO", "OPCS4"]))
        .select("CODE_VALUE").collect()
    ]
    
    # Create placeholder records for unmapped codes
    other_records = (
        base_df.filter(~F.col("SOURCE_VOCABULARY_CD").isin(all_mapped_code_values))
        .select(
            *base_columns,
            F.lit(None).cast("string").alias("SNOMED_CODE"),
            F.lit(None).cast("string").alias("SNOMED_TYPE"),
            F.lit(None).cast("integer").alias("NUMBER_SNOMED_MATCHES")
        )
    )

    # Combine all mappings with prioritization
    combined_df = native_snomed.unionAll(icd10_final)\
                              .unionAll(opcs4_final)\
                              .unionAll(other_records)\
                              .withColumn(
        # Assign priority to different mapping types
        "priority",
        F.when(F.col("SNOMED_TYPE") == "NATIVE", 1)
         .when(F.col("SNOMED_TYPE") == "EXACT", 2)
         .when(F.col("SNOMED_TYPE") == "OMOP_ASSISTED", 3)
         .when(F.col("SNOMED_TYPE") == "PARTIAL", 4)
         .otherwise(5)
    ).withColumn(
        # Select best overall mapping for each nomenclature ID
        "row_num",
        F.row_number().over(
            Window.partitionBy("NOMENCLATURE_ID")
            .orderBy(
                "priority",
                "NUMBER_SNOMED_MATCHES",
                "SNOMED_CODE"
            )
        )
    ).filter(F.col("row_num") == 1).drop("priority", "row_num")

    # Add SNOMED terms to final mappings
    latest_terms = snomed_terms.select("CUI", "TERM").withColumn(
        "row_num",
        F.row_number().over(Window.partitionBy("CUI").orderBy(F.col("TERM").desc()))
    ).filter(F.col("row_num") == 1).drop("row_num")

    # Return final dataset with SNOMED terms included
    return combined_df.join(
        latest_terms,
        F.col("SNOMED_CODE") == F.col("CUI"),
        "left"
    ).select(
        # First select all original columns
        *[F.col(c) for c in base_df.columns],
        # Then add the new SNOMED columns
        F.col("SNOMED_CODE"),
        F.col("SNOMED_TYPE"),
        F.col("NUMBER_SNOMED_MATCHES"),
        F.col("TERM").alias("SNOMED_TERM")
    )

@dlt.view(name="snomed_mapping_update")
def snomed_mapping_update():
    """
    Creates a streaming view of the incremental SNOMED mappings.
    """
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_snomed_mapping_incr")
    )

# Define the final target table with optimization settings
dlt.create_target_table(
    name = "temptwo_nomenclature",
    comment = "Extended nomenclature mappings with SNOMED concepts",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "NOMENCLATURE_ID,SNOMED_CODE"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "temptwo_nomenclature",
    source = "snomed_mapping_update",
    keys = ["NOMENCLATURE_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(
    name="maps_icd",
    comment="ICD10 Terms Reference Table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect("valid_cui", "CUI IS NOT NULL")
@dlt.expect("valid_term", "TERM IS NOT NULL")
def lookup_icd_terms():
    """Creates a reference table for ICD10 terms."""
    return spark.table("3_lookup.trud.maps_icd")

@dlt.table(
    name="snomed_hierarchy",
    comment="SNOMED Hierarchy Reference Table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect("valid_parent", "PARENT IS NOT NULL")
@dlt.expect("valid_child", "CHILD IS NOT NULL")
def lookup_snomed_hierarchy():
    """Creates a reference table for SNOMED concept hierarchies."""
    return spark.table("3_lookup.trud.snomed_scthier")

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

def combine_matches(*dfs):
    """
    Helper function to combine and prioritize matches
    
    Args:
        *dfs: Variable number of DataFrames to combine
        
    Returns:
        DataFrame: Combined and prioritized matches
    """
    def union_dfs(df1, df2):
        return df1.unionAll(df2)
    
    combined = reduce(
        union_dfs,
        [df.select(
            "NOMENCLATURE_ID", 
            "ICD10_CODE", 
            "ICD10_MATCH_COUNT", 
            "HAS_OMOP_MAP", 
            "ICD10_TYPE"
        ) for df in dfs]
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
    with_terms = matches_df.alias("matches").join(
        icd_terms.alias("terms"),
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
    return base_df.alias("base").join(
        with_terms.alias("with_terms"),
        F.col("base.NOMENCLATURE_ID") == F.col("with_terms.NOMENCLATURE_ID"),
        "left"
    ).select(
        # Get all columns from base_df
        *[F.col(f"base.{c}") for c in base_df.columns],
        # Add new ICD10 columns
        F.col("with_terms.ICD10_CODE"),
        F.col("with_terms.ICD10_TYPE"),
        F.col("with_terms.ICD10_MATCH_COUNT"),
        F.col("with_terms.ICD10_TERM")
    )

def process_matches(df, base_type, omop_type):
    """Helper function to process matches and aggregate results"""
    return df.withColumn(
        "match_rank",
        F.row_number().over(
            Window.partitionBy("base.NOMENCLATURE_ID")
            .orderBy(
                F.col("has_omop_map").desc(),
                F.col("icd_map.SCUI")
            )
        )
    ).groupBy(F.col("base.NOMENCLATURE_ID")).agg(
        F.first(F.when(F.col("match_rank") == 1, F.col("icd_map.SCUI"))).alias("ICD10_CODE"),
        F.count("icd_map.SCUI").alias("ICD10_MATCH_COUNT"),
        F.max(F.col("has_omop_map")).alias("HAS_OMOP_MAP"),
        F.when(F.max(F.col("has_omop_map")), F.lit(omop_type))
         .otherwise(F.lit(base_type))
         .alias("ICD10_TYPE")
    )

# COMMAND ----------

@dlt.table(
    name="icd10_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def icd10_mapping_incr():
    """
    Creates an incremental ICD10 mapping table processing only new/modified records.
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.tempthree_nomenclature")
    
    # Get base and reference tables
    base_df = dlt.read("temptwo_nomenclature").filter(F.col("ADC_UPDT") > max_adc_updt).alias("base")


    

    icd_map = dlt.read("maps_icdsctmap").alias("icd_map")
    snomed_hier = dlt.read("snomed_hierarchy").alias("hier")
    icd_terms = dlt.read("maps_icd").alias("terms")
    code_value = dlt.read("code_value")
    
    # Get OMOP reference tables
    source_concepts = dlt.read("omop_concept").filter(F.col("invalid_reason").isNull()).alias("source_concepts")
    target_concepts = dlt.read("omop_concept").filter(F.col("invalid_reason").isNull()).alias("target_concepts")
    omop_rels = dlt.read("omop_concept_relationship")\
        .filter(F.col("relationship_id").isin(["Maps to", "Mapped from"]))\
        .alias("rel")

    # Process direct matches using SNOMED_CODE instead of FOUND_CUI
    direct_matches = base_df.join(
        icd_map,
        F.col("base.SNOMED_CODE") == F.col("icd_map.TCUI"),
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
        F.col("base.SNOMED_CODE") == F.col("hier.CHILD"),
        "left"
    ).join(
        icd_map,
        F.col("hier.PARENT") == F.col("icd_map.TCUI"),
        "left"
    ).filter(F.col("icd_map.SCUI").isNotNull())
    
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
        F.col("base.SNOMED_CODE") == F.col("hier.PARENT"),
        "left"
    ).join(
        icd_map,
        F.col("hier.CHILD") == F.col("icd_map.TCUI"),
        "left"
    ).filter(F.col("icd_map.SCUI").isNotNull())
    
    parent_with_omop = add_omop_preference_icd10(
        df=parent_matches,
        source_code_col="hier.CHILD",
        target_code_col="icd_map.SCUI",
        source_concepts=source_concepts,
        target_concepts=target_concepts,
        omop_rels=omop_rels
    )

    # Process the results
    direct_final = process_matches(direct_with_omop, "EXACT", "OMOP_ASSISTED")
    child_final = process_matches(child_with_omop, "CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED")
    parent_final = process_matches(parent_with_omop, "PARENTMATCH", "PARENTMATCH_OMOP_ASSISTED")

    # Process native ICD10 codes
    icd10_code_values = code_value.filter(
        F.col("CDF_MEANING").isin(["ICD10", "ICD10WHO"])
    )
    
    native_codes = base_df.filter(
        (F.col("base.SOURCE_VOCABULARY_CD").isin(
            [row.CODE_VALUE for row in icd10_code_values.collect()]
        )) |
        (F.col("base.CONCEPT_CKI").like("ICD10WHO!%"))
    ).select(
        F.col("base.NOMENCLATURE_ID"),
        F.when(F.col("base.CONCEPT_CKI").like("ICD10WHO!%"),
            F.regexp_replace(F.col("base.CONCEPT_CKI"), "ICD10WHO!", "")
        ).otherwise(F.col("base.SNOMED_CODE")).alias("ICD10_CODE"),
        F.lit(1).alias("ICD10_MATCH_COUNT"),
        F.lit(False).alias("HAS_OMOP_MAP"),
        F.lit("NATIVE").alias("ICD10_TYPE")
    )

    # Combine all matches
    all_matches = combine_matches(native_codes, direct_final, child_final, parent_final)

    # Add ICD10 terms and return final result
    return add_icd10_terms(all_matches, icd_terms, base_df)



@dlt.view(name="icd10_mapping_update")
def icd10_mapping_update():
    """Creates a streaming view of the incremental ICD10 mappings."""
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.icd10_mapping_incr")
    )

# Define the final target table
dlt.create_target_table(
    name = "tempthree_nomenclature",
    comment = "Extended nomenclature mappings with ICD10 concepts",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "NOMENCLATURE_ID,ICD10_CODE"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "tempthree_nomenclature",
    source = "icd10_mapping_update",
    keys = ["NOMENCLATURE_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(
name="maps_opcs",
comment="OPCS4 Terms Reference Table",
table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect("valid_cui", "CUI IS NOT NULL")
@dlt.expect("valid_term", "TERM IS NOT NULL")
def lookup_opcs_terms():
    """Creates a reference table for OPCS4 terms."""
    return spark.table("3_lookup.trud.maps_opcs")

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

@dlt.table(
name="opcs4_mapping_incr",
temporary=True,
table_properties={"skipChangeCommits": "true"}
)
def opcs4_mapping_incr():
    """
Creates an incremental OPCS4 mapping table processing only new/modified records.
    """
# Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.nomenclature")

    base_df = dlt.read("tempthree_nomenclature").filter(F.col("ADC_UPDT") > max_adc_updt).alias("base")



    opcs_map = dlt.read("maps_opcssctmap").alias("opcs_map")
    snomed_hier = dlt.read("snomed_hierarchy").alias("hier")
    opcs_terms = dlt.read("maps_opcs").alias("terms")
    code_value = dlt.read("code_value")

    # Get OMOP reference tables
    source_concepts = dlt.read("omop_concept").filter(F.col("invalid_reason").isNull()).alias("source_concepts")
    target_concepts = dlt.read("omop_concept").filter(F.col("invalid_reason").isNull()).alias("target_concepts")
    omop_rels = dlt.read("omop_concept_relationship").filter(
        F.col("relationship_id").isin(["Maps to", "Mapped from"])
    ).alias("rel")

    # Process direct matches
    direct_matches = base_df.join(
        opcs_map,
        F.col("base.SNOMED_CODE") == F.col("opcs_map.TCUI"),
        "left"
    )

    direct_with_omop = add_omop_preference_opcs4(
    direct_matches,
    "SNOMED_CODE",
    "opcs_map.SCUI",
    source_concepts,
    target_concepts,
    omop_rels
    )

    direct_final = process_matches_opcs4(direct_with_omop, "EXACT", "OMOP_ASSISTED")

    # Process hierarchical matches
    child_matches = base_df.join(
    snomed_hier,
    F.col("base.SNOMED_CODE") == F.col("hier.CHILD"),
    "left"
    ).join(
    opcs_map,
    F.col("hier.PARENT") == F.col("opcs_map.TCUI"),
    "left"
    ).filter(F.col("opcs_map.SCUI").isNotNull())

    child_with_omop = add_omop_preference_opcs4(
    child_matches,
    "hier.PARENT",
    "opcs_map.SCUI",
    source_concepts,
    target_concepts,
    omop_rels
    )

    child_final = process_matches_opcs4(child_with_omop, "CHILDMATCH", "CHILDMATCH_OMOP_ASSISTED")

    parent_matches = base_df.join(
    snomed_hier,
    F.col("base.SNOMED_CODE") == F.col("hier.PARENT"),
    "left"
    ).join(
    opcs_map,
    F.col("hier.CHILD") == F.col("opcs_map.TCUI"),
    "left"
    ).filter(F.col("opcs_map.SCUI").isNotNull())

    parent_with_omop = add_omop_preference_opcs4(
    parent_matches,
    "hier.CHILD",
    "opcs_map.SCUI",
    source_concepts,
    target_concepts,
    omop_rels
    )

    parent_final = process_matches_opcs4(parent_with_omop, "PARENTMATCH", "PARENTMATCH_OMOP_ASSISTED")

    # Process native OPCS4 codes
    opcs4_code_values = code_value.filter(F.col("CDF_MEANING") == "OPCS4")
    native_codes = process_native_codes_opcs4(base_df, opcs4_code_values)

    # Combine all matches
    all_matches = combine_matches_opcs4(native_codes, direct_final, child_final, parent_final)

    # Add OPCS4 terms and return final result
    return add_opcs4_terms(all_matches, opcs_terms, base_df)

@dlt.view(name="opcs4_mapping_update")
def opcs4_mapping_update():
    """Creates a streaming view of the incremental OPCS4 mappings."""
    return (
spark.readStream
.option("forceDeleteReadCheckpoint", "true")
.option("ignoreDeletes", "true")
.option("ignoreChanges", "true")
.table("LIVE.opcs4_mapping_incr")
    )

dlt.create_target_table(
name = "nomenclature",
comment = "Final nomenclature mappings with all vocabulary concepts",
table_properties = {
"delta.enableChangeDataFeed": "true",
"delta.enableRowTracking": "true",
"pipelines.autoOptimize.managed": "true",
"pipelines.autoOptimize.zOrderCols": "NOMENCLATURE_ID,OPCS4_CODE"
}
)


dlt.apply_changes(
target = "nomenclature",
source = "opcs4_mapping_update",
keys = ["NOMENCLATURE_ID"],
sequence_by = "ADC_UPDT",
apply_as_deletes = None,
except_column_list = [],
stored_as_scd_type = 1
)

# COMMAND ----------


