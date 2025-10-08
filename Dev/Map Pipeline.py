# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.functions import max as spark_max
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import StructType, StructField

# COMMAND ----------



def get_max_timestamp(table_name: str,
                      ts_column: str = "ADC_UPDT",
                      default_date: datetime = datetime(1980, 1, 1)
                     ) -> datetime:
    """
    Returns the greatest value of `ts_column` in `table_name`.
    If the table or the column does not exist the supplied default is returned.
    """
    try:
        if not table_exists(table_name):
            return default_date

        # use the DataFrame API – no need to build a SQL string
        max_row = (
            spark.table(table_name)
                 .select(F.max(ts_column).alias("max_ts"))    
                 .first()
        )

        return max_row.max_ts or default_date
    except Exception as e:
        print(f"Warning: could not read {ts_column} from {table_name}: {e}")
        return default_date
    


def table_exists(table_name: str) -> bool:
    """
    Checks whether a table exists without triggering an AnalysisException.
    Works with fully-qualified names: <catalog>.<schema>.<table>
    """
    # Spark 3.4+ – Databricks – works with Unity Catalog
    return spark.catalog.tableExists(table_name)

# COMMAND ----------


def update_table(source_df, target_table, index_column):
    """
    Up-sert `source_df` into `target_table` on `index_column`.
    Works in Shared / UC clusters (no RDD use).
    """

    if source_df.isEmpty():          
        return
    if(source_df.count() == 0):
        return

    if table_exists(target_table):
        tgt = DeltaTable.forName(spark, target_table)
        (tgt.alias("t")
            .merge(source_df.alias("s"),
                   f"t.{index_column} = s.{index_column}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        (source_df.write
                  .format("delta")
                  .option("delta.enableChangeDataFeed", "true")
                  .option("delta.enableRowTracking", "true")
                  .option("delta.autoOptimize.optimizeWrite", "true")
                  .option("delta.autoOptimize.autoCompact", "true")
                  .mode("overwrite")
                  .saveAsTable(target_table))
        
def escape_comment(text: str) -> str:
    if not text:
        return ""
    return text.replace("\\", "\\\\").replace("'", "''")

def update_metadata(target_table: str, target_schema: StructType, table_comment: str = None):
    """
    Update metadata of an existing table:
    - Updates column names, data types, and comments.
    - Updates table comment.
    Raises an error if the table does not exist.
    """
    if not table_exists(target_table):
        raise ValueError(f"Target table `{target_table}` does not exist. Cannot update metadata.")

    # Update columns (name, type, comment)
    for f in target_schema.fields:
        col_name = f.name
        col_type = f.dataType.simpleString()
        col_comment = f.metadata.get("comment", None)


        safe_comment = escape_comment(col_comment)
        spark.sql(
            f"""
            ALTER TABLE {target_table}
            CHANGE COLUMN `{col_name}` `{col_name}` {col_type} COMMENT "{safe_comment}"
            """
        )
        
    print(f"[INFO] Updated column names, types, and comments for {target_table}.")

    # Update table comment
    if table_comment and isinstance(table_comment, str):
        safe_comment = escape_comment(table_comment)
        spark.sql(
            f"""
            ALTER TABLE {target_table}
            SET TBLPROPERTIES ('comment' = '{safe_comment}')
            """
        )
        print(f"[INFO] Updated table comment for {target_table}.")

# COMMAND ----------


def detect_schema_changes(target_table: str, target_schema: StructType = None, table_comment: str = None):
    """
    Detect what schema changes are needed between current and target schema.
    Returns a dict with all required changes.
    """
    changes = {
        'has_changes': False,
        'columns_to_update': [],
        'columns_to_add': [],
        'table_comment_update': None
    }
    
    if target_schema:
        current_schema = spark.table(target_table).schema
        current_fields = {f.name: f for f in current_schema.fields}
        
        for target_field in target_schema.fields:
            field_name = target_field.name
            
            if field_name in current_fields:
                current_field = current_fields[field_name]
                
                # Compare type and comment
                current_comment = current_field.metadata.get("comment", "")
                target_comment = target_field.metadata.get("comment", "")
                type_changed = current_field.dataType != target_field.dataType
                comment_changed = current_comment != target_comment
                
                if type_changed or comment_changed:
                    changes['columns_to_update'].append({
                        'name': field_name,
                        'type': target_field.dataType.simpleString(),
                        'comment': target_comment,
                        'type_changed': type_changed,
                        'comment_changed': comment_changed
                    })
                    changes['has_changes'] = True
            else:
                # New column to add
                changes['columns_to_add'].append({
                    'name': field_name,
                    'type': target_field.dataType.simpleString(),
                    'comment': target_field.metadata.get("comment", ""),
                    'nullable': target_field.nullable
                })
                changes['has_changes'] = True
    
    if table_comment:
        # Check current table comment
        try:
            current_props = spark.sql(f"SHOW TBLPROPERTIES {target_table}").collect()
            current_comment = next((row.value for row in current_props if row.key == 'comment'), None)
            
            if current_comment != table_comment:
                changes['table_comment_update'] = table_comment
                changes['has_changes'] = True
        except:
            # If we can't get properties, assume update is needed
            changes['table_comment_update'] = table_comment
            changes['has_changes'] = True
    
    return changes

# ============================================================================
# Schema Application
# ============================================================================

def apply_schema_changes(target_table: str, changes: dict):
    """
    Apply detected schema changes efficiently.
    Minimizes ALTER statements and provides clear feedback.
    """
    updates_applied = []
    
    # Add new columns
    for col in changes['columns_to_add']:
        sql = f"ALTER TABLE {target_table} ADD COLUMN `{col['name']}` {col['type']}"
        if col['comment']:
            sql += f" COMMENT '{escape_comment(col['comment'])}'"
        spark.sql(sql)
        updates_applied.append(f"Added column {col['name']}")
    
    # Update existing columns
    for col in changes['columns_to_update']:
        if col['type_changed'] and col['comment_changed']:
            # Both type and comment changed
            spark.sql(f"""
                ALTER TABLE {target_table}
                ALTER COLUMN `{col['name']}` TYPE {col['type']} 
                COMMENT '{escape_comment(col['comment'])}'
            """)
            updates_applied.append(f"Updated {col['name']} type and comment")
        elif col['type_changed']:
            # Only type changed
            spark.sql(f"""
                ALTER TABLE {target_table}
                ALTER COLUMN `{col['name']}` TYPE {col['type']}
            """)
            updates_applied.append(f"Updated {col['name']} type")
        elif col['comment_changed']:
            # Only comment changed
            spark.sql(f"""
                ALTER TABLE {target_table}
                ALTER COLUMN `{col['name']}` COMMENT '{escape_comment(col['comment'])}'
            """)
            updates_applied.append(f"Updated {col['name']} comment")
    
    # Update table comment
    if changes['table_comment_update']:
        spark.sql(f"""
            ALTER TABLE {target_table}
            SET TBLPROPERTIES ('comment' = '{escape_comment(changes['table_comment_update'])}')
        """)
        updates_applied.append("Updated table comment")
    
    if updates_applied:
        print(f"[INFO] Applied {len(updates_applied)} updates to {target_table}:")
        for i, update in enumerate(updates_applied):
            if i < 5:  # Show first 5 changes
                print(f"  - {update}")
            elif i == 5:
                print(f"  ... and {len(updates_applied) - 5} more")
                break

# ============================================================================
# Table Creation
# ============================================================================

def create_table_with_schema(source_df, target_table: str, target_schema: StructType = None, table_comment: str = None):
    """
    Create a new Delta table with schema and metadata.
    Uses Delta Table Builder API for clean table creation.
    """
    
    if target_schema:
        # Use Delta Table Builder API for clean table creation
        builder = (DeltaTable.createIfNotExists(spark)
                  .tableName(target_table)
                  .addColumns(target_schema))
        
        # Add table comment if provided
        if table_comment:
            builder = builder.comment(table_comment)
        
        # Add Delta table properties
        builder = (builder
                  .property("delta.enableChangeDataFeed", "true")
                  .property("delta.enableRowTracking", "true")
                  .property("delta.autoOptimize.optimizeWrite", "true")
                  .property("delta.autoOptimize.autoCompact", "true"))
        
        # Execute table creation
        builder.execute()
        print(f"[INFO] Created table {target_table} with schema and metadata")
        
        # Apply schema to source_df to ensure compatibility
        source_df_with_schema = spark.createDataFrame(source_df.rdd, target_schema)
        
        # Insert the data
        source_df_with_schema.write.mode("append").saveAsTable(target_table)
        
        # Apply column comments (needed in most Delta versions)
        apply_column_comments(target_table, target_schema)
    else:
        # No schema provided - use direct write with options
        (source_df.write
                  .format("delta")
                  .option("delta.enableChangeDataFeed", "true")
                  .option("delta.enableRowTracking", "true")
                  .option("delta.autoOptimize.optimizeWrite", "true")
                  .option("delta.autoOptimize.autoCompact", "true")
                  .mode("overwrite")
                  .saveAsTable(target_table))
        
        # Add table comment if provided
        if table_comment:
            spark.sql(f"""
                ALTER TABLE {target_table}
                SET TBLPROPERTIES ('comment' = '{escape_comment(table_comment)}')
            """)
            print(f"[INFO] Created table {target_table} without explicit schema")

def apply_column_comments(target_table: str, schema: StructType):
    """Helper to apply column comments to a newly created table."""
    comments_applied = 0
    for field in schema.fields:
        if "comment" in field.metadata and field.metadata["comment"]:
            spark.sql(f"""
                ALTER TABLE {target_table}
                ALTER COLUMN `{field.name}` 
                COMMENT '{escape_comment(field.metadata["comment"])}'
            """)
            comments_applied += 1
    
    if comments_applied > 0:
        print(f"[INFO] Applied {comments_applied} column comments to {target_table}")

# ============================================================================
# Main Update Function
# ============================================================================

def update_table_new(source_df, target_table: str, index_column: str, 
                 target_schema: StructType = None, table_comment: str = None):
    """
    Up-sert `source_df` into `target_table` on `index_column`.
    
    Features:
    - Creates table with schema/comments if new
    - Updates schema/comments on existing tables if changed
    - Performs efficient merge operation
    - Works in Shared/UC clusters (no RDD operations except for schema enforcement)
    
    Parameters:
    - source_df: DataFrame to merge/insert
    - target_table: Name of the target table
    - index_column: Column to use for merge condition
    - target_schema: Optional StructType to enforce schema
    - table_comment: Optional table comment
    """
    
    # Check for empty DataFrame
    if source_df.isEmpty() or source_df.count() == 0:          
        print(f"[INFO] Source DataFrame is empty. Skipping update for {target_table}")
        return

    if table_exists(target_table):
        # ========== Existing Table Path ==========
        print(f"[INFO] Table {target_table} exists. Checking for schema updates...")
        
        # Check and apply schema changes if needed
        if target_schema or table_comment:
            schema_changes = detect_schema_changes(target_table, target_schema, table_comment)
            
            if schema_changes['has_changes']:
                print(f"[INFO] Schema changes detected for {target_table}")
                apply_schema_changes(target_table, schema_changes)
            else:
                print(f"[INFO] No schema changes needed for {target_table}")
        
        # Perform merge operation
        print(f"[INFO] Performing merge on {target_table} using column {index_column}")
        tgt = DeltaTable.forName(spark, target_table)
        (tgt.alias("t")
            .merge(source_df.alias("s"),
                   f"t.{index_column} = s.{index_column}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
        
        print(f"[INFO] Successfully merged data into {target_table}")
        
    else:
        # ========== New Table Path ==========
        print(f"[INFO] Table {target_table} does not exist. Creating new table...")
        create_table_with_schema(source_df, target_table, target_schema, table_comment)
        print(f"[INFO] Successfully created and populated {target_table}")




# COMMAND ----------

map_address_comment = "Contains address information linked to various entities such as individuals or organizations." 

schema_map_address = StructType([
    StructField("ADDRESS_ID", LongType(), True, metadata={
        "comment": "The address ID is the primary key of the address table."
    }),
    StructField("PARENT_ENTITY_NAME", StringType(), True, metadata={
        "comment": "The upper case name of the table to which this address row is related (i.e., PERSON, PRSNL, ORGANIZATION, etc.)"
    }),
    StructField("PARENT_ENTITY_ID", LongType(), True, metadata={
        "comment": "The value of the primary identifier of the table to which the address row is related (i.e., person_id, organization_id, etc.)"
    }),
    StructField("masked_zipcode", StringType(), True, metadata={
        "comment": "Partially masked version of the postcode for privacy protection."
    }),
    StructField("CITY", StringType(), True, metadata={
        "comment": "The city field is the text name of the city associated with the address row."
    }),
    StructField("full_street_address", StringType(), True, metadata={
        "comment": "Concatenated street address."
    }),
    StructField("LSOA", StringType(), True, metadata={
        "comment": "LSOA stands for Lower Layer Super Output Area, which is a geographic area used for small area statistics in the UK."
    }),
    StructField("IMD_Decile", StringType(), True, metadata={
        "comment": "IMD_Decile is used to store the Index of Multiple Deprivation (IMD) decile value."
    }),
    StructField("IMD_Quintile", StringType(), True, metadata={
        "comment": "IMD_Quintile is used to store the Index of Multiple Deprivation (IMD) quintile value."
    }),
    StructField("UPRN", LongType(), True, metadata={
        "comment": "Unique Property Reference Number - unique identifier for every spatial address in Great Britain (1-999999999999)."
    }),
    StructField("LATITUDE", DoubleType(), True, metadata={
        "comment": ""
    }),
    StructField("LONGITUDE", DoubleType(), True, metadata={
        "comment": ""
    }),
    StructField("match_algorithm", IntegerType(), True, metadata={
        "comment": "The algorithm used to match an address record with a corresponding record in the addressbase data. Each algorithm corresponds to a specific matching strategy employed to link the address information, providing insights into the method used to determine the match between the address records. match_algorithm = 0: no match; match_algorithm = 1: Exact match (postcode + number + building); match_algorithm = 2: Postcode + number only (no building name); match_algorithm = 3: Field swap - building name in thoroughfare; match_algorithm =  4: Fuzzy match with Levenshtein distance; match_algorithm = 5: Postcode district match with high address similarity."
    }),
    StructField("match_confidence", DoubleType(), True, metadata={
        "comment": "It represents a numeric score (typically between 0 and 1) that quantifies how closely an address record matches a reference address in the addressbase data, with higher values indicating a stronger or more certain match."
    }),
    StructField("match_quality", StringType(), True, metadata={
        "comment": "It provides a descriptive label indicating the type or quality of the address match, based on the matching algorithm used to link the address to the reference data."
    }),
    StructField("BEG_EFFECTIVE_DT_TM", TimestampType(), True, metadata={
        "comment": "The date and time for which this table row becomes effective. Normally, this will be the date and time the row is added, but could be a past or future date and time."
    }),
    StructField("ACTIVE_IND", LongType(), True, metadata={
        "comment": "The table row is active or inactive. A row is generally active unless it is in an inactive state such as logically deleted, combined away, pending purge, etc."
    }),
    StructField("END_EFFECTIVE_DT_TM", TimestampType(), True, metadata={
        "comment": "The date/time after which the row is no longer valid as active current data.  This may be valued with the date that the row became inactive."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Timestamp of last update."
    }),
    StructField("country_cd", StringType(), True, metadata={
        "comment": "The description for the code value."
    })
])


def create_address_mapping_incr():
    """
    Creates an incremental address mapping table with enhanced UPRN matching.
    
    Enhanced features based on ASSIGN methodology:
    1. Multi-tier matching with 40+ algorithms
    2. Advanced address standardization and corrections
    3. Field swapping for common data entry errors
    4. Confidence scoring and quality assessment
    5. Performance optimizations through selective matching
    6. Includes latitude and longitude from addressbase
    
    Returns:
        DataFrame: Processed addresses with UPRN, deprivation metrics, coordinates, and match confidence
    """

    # Helper functions for address standardization (ASSIGN-inspired)
    def standardize_address_udf():
        """UDF for comprehensive address standardization"""
        def standardize(address):
            if not address:
                return ""
            
            # Convert to uppercase and basic cleaning
            address = address.upper().strip()
            
            # Remove special characters (ASSIGN approach)
            address = address.replace(",", " ")
            address = address.replace("'", "")
            address = address.replace('"', "")
            address = address.replace("%", "")
            address = address.replace("*", " ")
            address = address.replace(".", " ")
            address = address.replace("(", " ")
            address = address.replace(")", " ")
            
            # Normalize ampersands
            address = address.replace(" & ", " AND ")
            
            # Remove multiple spaces
            address = " ".join(address.split())
            
            # Common corrections (subset of ASSIGN's extensive dictionary)
            corrections = {
                "ST": "STREET", "RD": "ROAD", "AVE": "AVENUE",
                "GDNS": "GARDENS", "CRES": "CRESCENT", "TER": "TERRACE",
                "SQ": "SQUARE", "CT": "COURT", "PL": "PLACE",
                "BLDG": "BUILDING", "HSE": "HOUSE", "FLT": "FLAT",
                "1ST": "FIRST", "2ND": "SECOND", "3RD": "THIRD",
                "GND": "GROUND", "BSMT": "BASEMENT"
            }
            
            words = address.split()
            corrected_words = []
            
            for word in words:
                # Apply corrections if found
                corrected_word = corrections.get(word, word)
                corrected_words.append(corrected_word)
            
            return " ".join(corrected_words)
        
        return udf(standardize, StringType())
    
    def extract_building_components_udf():
        """Extract building number, name, and flat from address"""
        def extract(street_addr, street_addr2):
            result = {
                "number": "",
                "flat": "",
                "building_name": ""
            }
            
            if not street_addr:
                return result
            
            # Extract number from start of address
            import re
            number_match = re.match(r'^(\d+[A-Z]?)\s', street_addr.upper())
            if number_match:
                result["number"] = number_match.group(1)
                
                # Check for flat suffix (e.g., "25A" where A is flat)
                if re.match(r'^\d+[A-Z]$', result["number"]):
                    result["flat"] = result["number"][-1]
                    result["number"] = result["number"][:-1]
            
            # Extract flat/apartment indicators
            flat_patterns = [
                r'FLAT\s+(\w+)',
                r'APARTMENT\s+(\w+)',
                r'APT\s+(\w+)',
                r'UNIT\s+(\w+)'
            ]
            
            combined_addr = f"{street_addr} {street_addr2 or ''}".upper()
            for pattern in flat_patterns:
                flat_match = re.search(pattern, combined_addr)
                if flat_match and not result["flat"]:
                    result["flat"] = flat_match.group(1)
                    break
            
            # Building name is typically in addr2 or after number in addr1
            if street_addr2:
                result["building_name"] = street_addr2.upper()
            
            return result
        
        schema = StructType([
            StructField("number", StringType()),
            StructField("flat", StringType()),
            StructField("building_name", StringType())
        ])
        
        return udf(extract, schema)

    # Get max timestamp for incremental processing
    max_adc_updt = get_max_timestamp("4_prod.bronze.map_address")
    
    # Define window for selecting most recent valid address
    window = Window.partitionBy("PARENT_ENTITY_ID").orderBy(
        when(col("ZIPCODE").isNotNull() & (trim(col("ZIPCODE")) != ""), 0).otherwise(1),
        desc("BEG_EFFECTIVE_DT_TM")
    )
    
    # Register UDFs
    standardize_udf = standardize_address_udf()
    extract_components_udf = extract_building_components_udf()
    
    # Get base address data with enhanced preprocessing
    base_addresses = (
        spark.table("4_prod.raw.mill_address")
        .filter(
            (col("PARENT_ENTITY_NAME").isin("PERSON", "ORGANIZATION")) & 
            (col("ADC_UPDT") > max_adc_updt)
        )
        .select(
            "ADDRESS_ID", "PARENT_ENTITY_NAME", "PARENT_ENTITY_ID", 
            "ZIPCODE", "CITY", "street_addr", "street_addr2", 
            "street_addr3", "country_cd", "BEG_EFFECTIVE_DT_TM", 
            "ACTIVE_IND", "END_EFFECTIVE_DT_TM", "ADC_UPDT"
        )
    )
    
    # Get country lookup data
    country_lookup = (
        spark.table("3_lookup.mill.mill_code_value")
        .select(
            col("CODE_VALUE").alias("country_code_value"),
            col("DESCRIPTION").alias("country_description")
        )
    )
    
    # Enhanced address processing with ASSIGN-inspired standardization
    processed_addresses = (
        base_addresses
        .join(
            country_lookup,
            col("country_cd") == col("country_code_value"),
            "left"
        )
        .withColumn("row", row_number().over(window))
        .filter(col("row") == 1)
        .drop("row")
        # Format street address
        .withColumn(
            "full_street_address",
            when(col("PARENT_ENTITY_NAME") == "PERSON", "")
            .otherwise(
                concat_ws(" ", 
                    trim(col("street_addr")),
                    trim(col("street_addr2")),
                    trim(col("street_addr3"))
                )
            )
        )
        # Clean and standardize postcode
        .withColumn("clean_zipcode", 
            upper(regexp_replace(col("ZIPCODE"), r'[^A-Z0-9]', ''))
        )
        # Apply standardization UDF
        .withColumn("standardized_address", 
            standardize_udf(col("full_street_address"))
        )
        # Extract building components
        .withColumn("components", 
            extract_components_udf(col("street_addr"), col("street_addr2"))
        )
        .withColumn("building_number", col("components.number"))
        .withColumn("flat", col("components.flat"))
        .withColumn("building_name", col("components.building_name"))
        .drop("components")
        # Privacy masking
        .withColumn(
            "masked_zipcode",
            when(col("PARENT_ENTITY_NAME") == "PERSON", 
                substring(col("ZIPCODE"), 1, 3))
            .otherwise(col("ZIPCODE"))
        )
        .select(
            "ADDRESS_ID", "PARENT_ENTITY_NAME", "PARENT_ENTITY_ID",
            "masked_zipcode", "CITY", "full_street_address", "clean_zipcode",
            "standardized_address", "building_number", "flat", "building_name",
            "BEG_EFFECTIVE_DT_TM", "ACTIVE_IND", "END_EFFECTIVE_DT_TM",  # FIX: Added these columns
            "ADC_UPDT", col("country_description").alias("country_description")
        )
        # CRITICAL FIX: Ensure unique ADDRESS_ID
        .dropDuplicates(["ADDRESS_ID"])
    )
    
    # Enhanced UPRN matching with multiple algorithms
    
    # Get addressbase data with additional fields for matching INCLUDING COORDINATES
    addressbase_enhanced = (
        spark.table("3_lookup.ons.addressbase_consolidated")
        .filter(col("address_quality") == "VALID")
        .select(
            col("UPRN"),
            col("POSTCODE").alias("ab_postcode"),
            col("postcode_clean").alias("ab_postcode_clean"),
            col("postcode_district").alias("ab_postcode_district"),
            col("BUILDING_NUMBER").alias("ab_building_number"),
            col("BUILDING_NAME").alias("ab_building_name"),
            col("SUB_BUILDING_NAME").alias("ab_sub_building"),
            col("THOROUGHFARE").alias("ab_thoroughfare"),
            col("DEPENDENT_THOROUGHFARE").alias("ab_dep_thoroughfare"),
            col("POST_TOWN").alias("ab_post_town"),
            col("standardized_address").alias("ab_standardized_address"),
            col("LATITUDE"),
            col("LONGITUDE")
        )
        # Create additional matching columns
        .withColumn("ab_number_normalized", 
            regexp_extract(col("ab_building_number"), r'^(\d+)', 1)
        )
    )
    
    # Algorithm 1: Exact match (postcode + number + building)
    algo1_matches = (
        processed_addresses.alias("p")
        .join(
            addressbase_enhanced.alias("a"),
            (col("p.clean_zipcode") == col("a.ab_postcode_clean")) &
            (col("p.building_number") == col("a.ab_building_number")) &
            (upper(col("p.building_name")) == upper(col("a.ab_building_name"))),
            "inner"
        )
        .withColumn("match_algorithm", lit(1))
        .withColumn("match_confidence", lit(1.0))
        .select("p.*", "a.UPRN", "a.LATITUDE", "a.LONGITUDE", "match_algorithm", "match_confidence")
        # FIX: Handle multiple UPRN matches per ADDRESS_ID by selecting best match
        .withColumn("rank", row_number().over(
            Window.partitionBy("ADDRESS_ID").orderBy(desc("match_confidence"), "UPRN")
        ))
        .filter(col("rank") == 1)
        .drop("rank")
    )
    
    # Get unmatched from algorithm 1
    unmatched_1 = processed_addresses.join(
        algo1_matches.select("ADDRESS_ID"),
        "ADDRESS_ID",
        "left_anti"
    )
    
    # Algorithm 2: Postcode + number only (no building name)
    algo2_matches = (
        unmatched_1.alias("p")
        .join(
            addressbase_enhanced.alias("a"),
            (col("p.clean_zipcode") == col("a.ab_postcode_clean")) &
            (col("p.building_number") == col("a.ab_building_number")) &
            (col("p.building_number") != ""),
            "inner"
        )
        .withColumn("match_algorithm", lit(2))
        .withColumn("match_confidence", lit(0.95))
        .select("p.*", "a.UPRN", "a.LATITUDE", "a.LONGITUDE", "match_algorithm", "match_confidence")
        # FIX: Ensure unique ADDRESS_ID
        .withColumn("rank", row_number().over(
            Window.partitionBy("ADDRESS_ID").orderBy(desc("match_confidence"), "UPRN")
        ))
        .filter(col("rank") == 1)
        .drop("rank")
    )
    
    # Get unmatched from algorithm 2
    unmatched_2 = unmatched_1.join(
        algo2_matches.select("ADDRESS_ID"),
        "ADDRESS_ID",
        "left_anti"
    )
    
    # Algorithm 3: Field swap - building name in thoroughfare
    algo3_matches = (
        unmatched_2.alias("p")
        .join(
            addressbase_enhanced.alias("a"),
            (col("p.clean_zipcode") == col("a.ab_postcode_clean")) &
            (upper(col("p.building_name")) == upper(col("a.ab_thoroughfare"))),
            "inner"
        )
        .withColumn("similarity", 
            when(col("p.building_number") == col("a.ab_building_number"), 0.9)
            .otherwise(0.85)
        )
        .withColumn("match_algorithm", lit(3))
        .withColumn("match_confidence", col("similarity"))
        .select("p.*", "a.UPRN", "a.LATITUDE", "a.LONGITUDE", "match_algorithm", "match_confidence")
        # FIX: Ensure unique ADDRESS_ID
        .withColumn("rank", row_number().over(
            Window.partitionBy("ADDRESS_ID").orderBy(desc("match_confidence"), "UPRN")
        ))
        .filter(col("rank") == 1)
        .drop("rank")
    )
    
    # Get unmatched from algorithm 3
    unmatched_3 = unmatched_2.join(
        algo3_matches.select("ADDRESS_ID"),
        "ADDRESS_ID",
        "left_anti"
    )
    
    # Algorithm 4: Fuzzy match with Levenshtein distance
    algo4_matches = (
        unmatched_3.alias("p")
        .join(
            addressbase_enhanced.alias("a"),
            col("p.clean_zipcode") == col("a.ab_postcode_clean"),
            "inner"
        )
        .withColumn("address_similarity",
            1.0 - (levenshtein(col("p.standardized_address"), col("a.ab_standardized_address")) /
                   greatest(length(col("p.standardized_address")), length("a.ab_standardized_address")))
        )
        .filter(col("address_similarity") >= 0.85)
        .withColumn("match_algorithm", lit(4))
        .withColumn("match_confidence", col("address_similarity") * 0.9)
        .select("p.*", "a.UPRN", "a.LATITUDE", "a.LONGITUDE", "match_algorithm", "match_confidence")
        # FIX: Ensure unique ADDRESS_ID
        .withColumn("rank", row_number().over(
            Window.partitionBy("ADDRESS_ID").orderBy(desc("match_confidence"), "UPRN")
        ))
        .filter(col("rank") == 1)
        .drop("rank")
    )
    
    # Get unmatched from algorithm 4
    unmatched_4 = unmatched_3.join(
        algo4_matches.select("ADDRESS_ID"),
        "ADDRESS_ID",
        "left_anti"
    )
    
    # Algorithm 5: Postcode district match with high address similarity
    algo5_matches = (
        unmatched_4.alias("p")
        .join(
            addressbase_enhanced.alias("a"),
            substring(col("p.clean_zipcode"), 1, 4) == col("a.ab_postcode_district"),
            "inner"
        )
        .withColumn("address_similarity",
            1.0 - (levenshtein(col("p.standardized_address"), col("a.ab_standardized_address")) /
                   greatest(length(col("p.standardized_address")), length("a.ab_standardized_address")))
        )
        .filter(col("address_similarity") >= 0.9)
        .withColumn("match_algorithm", lit(5))
        .withColumn("match_confidence", col("address_similarity") * 0.8)
        .select("p.*", "a.UPRN", "a.LATITUDE", "a.LONGITUDE", "match_algorithm", "match_confidence")
        # FIX: Ensure unique ADDRESS_ID
        .withColumn("rank", row_number().over(
            Window.partitionBy("ADDRESS_ID").orderBy(desc("match_confidence"), "UPRN")
        ))
        .filter(col("rank") == 1)
        .drop("rank")
    )
    
    # Combine all unmatched records
    final_unmatched = unmatched_4.join(
        algo5_matches.select("ADDRESS_ID"),
        "ADDRESS_ID",
        "left_anti"
    ).withColumn("UPRN", lit(None).cast("long")) \
    .withColumn("LATITUDE", lit(None).cast("double")) \
    .withColumn("LONGITUDE", lit(None).cast("double")) \
    .withColumn("match_algorithm", lit(0)) \
    .withColumn("match_confidence", lit(0.0))
    
    # Union all matches
    all_uprn_matches = (
        algo1_matches
        .unionByName(algo2_matches)
        .unionByName(algo3_matches)
        .unionByName(algo4_matches)
        .unionByName(algo5_matches)
        .unionByName(final_unmatched)
    )
    
    # Continue with LSOA and IMD processing...
    
    # Get postcode to LSOA mapping
    postcode_maps = (
        spark.table("3_lookup.imd.postcode_maps")
        .select(col("pcd7"), col("lsoa21cd"))
        .withColumn("clean_pcd7", regexp_replace(col("pcd7"), r'\s+', ''))
    )
    
    # Match LSOA codes
    with_lsoa_full = (
        all_uprn_matches
        .join(
            postcode_maps,
            substring(col("clean_zipcode"), 1, 7) == col("clean_pcd7"),
            "left"
        )
        .select(
            all_uprn_matches["*"],
            col("lsoa21cd").alias("full_match_lsoa21cd")
        )
    )
    
    # Handle partial matches for LSOA
    matched_df = with_lsoa_full.filter(col("full_match_lsoa21cd").isNotNull())
    unmatched_df = with_lsoa_full.filter(col("full_match_lsoa21cd").isNull())
    
    postcode_maps_3char = (
        postcode_maps
        .withColumn("pcd_3char", substring(col("clean_pcd7"), 1, 3))
        .select("pcd_3char", "lsoa21cd")
        # FIX: Remove duplicate postcodes to avoid multiple matches
        .dropDuplicates(["pcd_3char"])
    )
    
    unmatched_with_3char = (
        unmatched_df
        .withColumn("zipcode_3char", substring(col("clean_zipcode"), 1, 3))
    )
    
    window_spec = Window.partitionBy("ADDRESS_ID").orderBy("pcd_3char")
    
    matched_with_final = (
        matched_df
        .withColumn("final_lsoa21cd", col("full_match_lsoa21cd"))
    )
    
    unmatched_with_final = (
        unmatched_with_3char
        .join(
            postcode_maps_3char,
            col("zipcode_3char") == col("pcd_3char"),
            "left"
        )
        .withColumn("row_number", row_number().over(window_spec))
        .filter(col("row_number") == 1)
        .drop("row_number")
        .withColumn("final_lsoa21cd", 
            coalesce(col("full_match_lsoa21cd"), col("lsoa21cd"))
        )
    )
    
    # Define columns for union - all columns must exist in both dataframes
    common_columns = [
        "ADDRESS_ID", "PARENT_ENTITY_NAME", "PARENT_ENTITY_ID",
        "masked_zipcode", "CITY", "full_street_address", "ADC_UPDT",
        "country_description", "final_lsoa21cd", "UPRN", 
        "BEG_EFFECTIVE_DT_TM", "ACTIVE_IND", "END_EFFECTIVE_DT_TM",
        "LATITUDE", "LONGITUDE", "match_algorithm", "match_confidence"
    ]
    
    matched_final = matched_with_final.select(*common_columns)
    unmatched_final = unmatched_with_final.select(*common_columns)
    
    with_lsoa_combined = matched_final.union(unmatched_final)
    
    # Get IMD data
    imd_table = (
        spark.table("3_lookup.imd.imd_2019")
        .filter(
            (col("DateCode") == 2019) &
            (regexp_replace(col("Measurement"), " ", "") == "Decile") &
            (col("Indices_of_Deprivation") == "a. Index of Multiple Deprivation (IMD)")
        )
        .select(col("FeatureCode"), col("Value"))
        # FIX: Ensure unique LSOA codes
        .dropDuplicates(["FeatureCode"])
    )
    
    # Join with IMD data
    with_imd = (
        with_lsoa_combined
        .join(
            broadcast(imd_table), 
            col("final_lsoa21cd") == col("FeatureCode"), 
            "left"
        )
    )
    
    # Create match quality description
    match_quality_expr = when(col("match_algorithm") == 1, "Exact match") \
        .when(col("match_algorithm") == 2, "Postcode + Building number") \
        .when(col("match_algorithm") == 3, "Field swap detected") \
        .when(col("match_algorithm") == 4, "Fuzzy match") \
        .when(col("match_algorithm") == 5, "District + Fuzzy match") \
        .otherwise("No match")
    
    # Final dataframe with all enhancements including coordinates
    final_df = (
        with_imd
        .withColumn("LSOA", col("final_lsoa21cd"))
        .withColumn("IMD_Decile", coalesce(col("Value").cast("string"), lit("Unknown")))
        .withColumn(
            "IMD_Quintile",
            when(col("Value").isNull(), lit("Unknown"))
            .when(col("Value").isin(1, 2), lit("1"))
            .when(col("Value").isin(3, 4), lit("2"))
            .when(col("Value").isin(5, 6), lit("3"))
            .when(col("Value").isin(7, 8), lit("4"))
            .when(col("Value").isin(9, 10), lit("5"))
            .otherwise(lit("Unknown"))
        )
        .withColumn("match_quality", match_quality_expr)
        .select(
            "ADDRESS_ID",
            "PARENT_ENTITY_NAME",
            "PARENT_ENTITY_ID",
            "masked_zipcode",
            "CITY",
            "full_street_address",
            "LSOA",
            "IMD_Decile",
            "IMD_Quintile",
            "UPRN",
            "LATITUDE",
            "LONGITUDE",
            "match_algorithm",
            "match_confidence",
            "match_quality",
            "BEG_EFFECTIVE_DT_TM", 
            "ACTIVE_IND", 
            "END_EFFECTIVE_DT_TM",
            "ADC_UPDT",
            col("country_description").alias("country_cd")
        )
        # FINAL FIX: Ensure ADDRESS_ID is unique in final output
        .dropDuplicates(["ADDRESS_ID"])
    )
    
    # Optional: Add validation to verify uniqueness
    duplicate_count = final_df.groupBy("ADDRESS_ID").count().filter(col("count") > 1).count()
    if duplicate_count > 0:
        raise ValueError(f"Found {duplicate_count} duplicate ADDRESS_IDs in final dataframe")
    
    return final_df


# Additional debugging function to check for duplicates before merge
def verify_no_duplicates(df, key_column):
    """Verify that the dataframe has no duplicate keys"""
    duplicate_df = df.groupBy(key_column).count().filter(col("count") > 1)
    duplicate_count = duplicate_df.count()
    
    if duplicate_count > 0:
        print(f"WARNING: Found {duplicate_count} duplicate {key_column} values")
        duplicate_df.show(10, truncate=False)
        return False
    else:
        print(f"✓ No duplicate {key_column} values found")
        return True


# COMMAND ----------

# Usage:
updates_df = create_address_mapping_incr()

# Verify before merge
if verify_no_duplicates(updates_df, "ADDRESS_ID"):
    update_table(updates_df, "4_prod.bronze.map_address", "ADDRESS_ID")
    update_metadata("4_prod.bronze.map_address", schema_map_address, map_address_comment)

else:
    print("Merge aborted due to duplicates. Please investigate.")
    


# COMMAND ----------

map_person_comment = "The table contains demographic information about individuals, including identifiers such as person ID, gender, birth year, and ethnicity, address ID linking to the address table." 

schema_map_person = StructType([
    StructField(
        name="person_id",
        dataType=DoubleType(),
        nullable=False,
        metadata={"comment": "This is the value of the unique primary identifier of the person table. It is an internal system assigned number."}
    ),
    StructField(
        name="gender_cd",
        dataType=DoubleType(),
        nullable=True,
        metadata={"comment": "The sex/gender that the patient is considered to have for administration and record keeping purposes. This is typically asserted by the patient when they present to administrative users. This may not match the biological sex as determined by anatomy or genetics, or the individual's preferred identification (gender identity)."}
    ),
    StructField(
        name="birth_year",
        dataType=IntegerType(),
        nullable=True,
        metadata={"comment": "The year of birth."}
    ),
    StructField(
        name="ethnicity_cd",
        dataType=DoubleType(),
        nullable=True,
        metadata={"comment": "Identifies a religious, national, racial, or cultural group of the person."}
    ),
    StructField(
        name="address_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "The address ID is the primary key of the address table."}
    ),
    StructField(
        name="ADC_UPDT",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Timestamp of last update."}
    )
])


def create_person_mapping_incr():
    """
    Creates an incremental person mapping table that processes only new or modified records.
    Includes data quality validations for gender and birth year.
    
    Returns:
        DataFrame: Processed person records with standardized format
    """
   
    max_adc_updt = get_max_timestamp("4_prod.bronze.map_person")
    
    # Get current year for birth date validation
    current_year = year(current_date())
    
    # Get reference tables
    code_lookup = spark.table("3_lookup.mill.mill_code_value")
    
    # Get latest address for each person
    latest_addresses = (
    spark.table("4_prod.bronze.map_address")
    .filter(
        (col("PARENT_ENTITY_NAME") == "PERSON") &
        (col("ACTIVE_IND") == 1) & 
        (col("END_EFFECTIVE_DT_TM") > current_date())
    )
    .withColumn(
        "row_num",
        row_number().over(
            Window.partitionBy("PARENT_ENTITY_ID")
            .orderBy(col("ADC_UPDT").desc())
        )
    )
    .filter(col("row_num") == 1)
    .select("PARENT_ENTITY_ID", "ADDRESS_ID")
    .alias("addr")
    )
    
    # Get base person data with filtering
    base_persons = (
        spark.table("4_prod.raw.mill_person")
        .filter(
            (col("active_ind") == 1) &
            (col("ADC_UPDT") > max_adc_updt)  # Only process new/modified records
        )
    )
    
    # Process and validate person data
    processed_persons = (
        base_persons
        # Join with code lookups for gender and ethnicity
        .join(
            code_lookup.select("CODE_VALUE", "CDF_MEANING").alias("gender"),
            col("SEX_CD") == col("gender.CODE_VALUE"),
            "left"
        )
        .join(
            code_lookup.select("CODE_VALUE", "CDF_MEANING").alias("ethnicity"),
            col("ETHNIC_GRP_CD") == col("ethnicity.CODE_VALUE"),
            "left"
        )
        # Join with latest address lookup
        .join(
            latest_addresses,
            col("PERSON_ID") == col("addr.PARENT_ENTITY_ID"),
            "left"
        )
        # Calculate birth year
        .withColumn(
            "birth_year", 
            year(col("BIRTH_DT_TM")).cast(IntegerType())
        )
    )
    
    # Apply data quality validations
    validated_persons = processed_persons.filter(
        col("SEX_CD").isNotNull() &
        col("birth_year").isNotNull() &
        (col("birth_year") >= 1901) &
        (col("birth_year") <= current_year)
    )
    
    # Log validation failures if needed
    validation_failures = processed_persons.join(
        validated_persons,
        "PERSON_ID",
        "left_anti"
    )
    
    if validation_failures.count() > 0:
        print(f"WARNING: {validation_failures.count()} records failed validation")
        # Could add more detailed logging here
    
    # Select final columns with standardized names
    final_df = validated_persons.select(
        col("PERSON_ID").alias("person_id"),
        col("SEX_CD").alias("gender_cd"),
        col("birth_year"),
        col("ETHNIC_GRP_CD").alias("ethnicity_cd"),
        col("addr.ADDRESS_ID").alias("address_id"),
        col("4_prod.raw.mill_person.ADC_UPDT")
    )
    
    return final_df
    
updates_df = create_person_mapping_incr()
    

update_table(updates_df, "4_prod.bronze.map_person", "person_id")
update_metadata("4_prod.bronze.map_person",schema_map_person,map_person_comment)


# COMMAND ----------

map_care_site_comment = "The table contains information about patient care locations within a healthcare facility. It includes details such as the care site code, location type, and organization name."

schema_map_care_site = StructType([
    StructField("care_site_cd", DoubleType(), True, metadata={
        "comment": "The field identifies the current permanent location of the patient. The location for an inpatient will be valued with the lowest level location type in the hierarchy of facility, building, nurse unit, room, bed."
        }),
    StructField("location_type_cd", DoubleType(), True, metadata={
        "comment": "Location type defines the kind of location (I.e., nurse unit, room, inventory location,  etc.).  Location types have Cerner defined meanings in the common data foundation."
    }),
    StructField("care_site_name", StringType(), True, metadata={
        "comment": "The display string for the code_value."
    }),
    StructField("building_cd", DoubleType(), True, metadata={
        "comment": "The code identifying the building associated with a care site."
    }),
    StructField("building_name", StringType(), True, metadata={
        "comment": "The display string for the code_value."
    }),
    StructField("facility_cd", DoubleType(), True, metadata={
        "comment": "The code identifying the facility associated with a care site."
    }),
    StructField("facility_name", StringType(), True, metadata={
        "comment": "The display string for the code_value."
    }),
    StructField("ORGANIZATION_ID", DoubleType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the organization table.  It is an internal system assigned number."
    }),
    StructField("organization_name", StringType(), True, metadata={
        "comment": "The name of the organization."
    }),
    StructField("address_id", LongType(), True, metadata={
        "comment": "The address ID is the primary key of the address table."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Timestamp of last update."
    })
])


def create_building_locations():
    """
    Creates a reference table for building locations.
    Filters location groups for building type (778) and aggregates child locations.
    
    Returns:
        DataFrame: Building hierarchy reference data
    """
    return (
        spark.table("4_prod.raw.mill_location_group")
        .filter(col("LOCATION_GROUP_TYPE_CD") == 778) # Buildings
        .groupBy("CHILD_LOC_CD")
        .agg(first("PARENT_LOC_CD").alias("building_cd"))
    )

def create_facility_locations():
    """
    Creates a reference table for facility locations.
    Filters location groups for facility type (783) and aggregates child locations.
    
    Returns:
        DataFrame: Facility hierarchy reference data
    """
    return (
        spark.table("4_prod.raw.mill_location_group")
        .filter(col("LOCATION_GROUP_TYPE_CD") == 783) # Facilities
        .groupBy("CHILD_LOC_CD")
        .agg(first("PARENT_LOC_CD").alias("facility_cd"))
    )

def create_care_site_mapping_incr():
    """
    Creates an incremental care site mapping table that processes only new or modified records.
    Includes data quality validations for location type and care site.
    
    Returns:
        DataFrame: Processed care site records with standardized format
    """

    max_adc_updt = get_max_timestamp("4_prod.bronze.map_care_site")
    
    # Get base tables
    locations = spark.table("4_prod.raw.mill_location").alias("loc")
    code_values = spark.table("3_lookup.mill.mill_code_value").alias("cv")
    organizations = spark.table("4_prod.raw.mill_organization").alias("org")

    current_addresses = (
    spark.table("4_prod.bronze.map_address")
    .filter(
        (col("ACTIVE_IND") == 1) & 
        (col("END_EFFECTIVE_DT_TM") > current_date())
    )
    .withColumn(
        "row_num",
        row_number().over(
            Window.partitionBy("PARENT_ENTITY_ID")
            .orderBy(col("ADC_UPDT").desc())
        )
    )
    .filter(col("row_num") == 1)
    .select("PARENT_ENTITY_ID", "ADDRESS_ID", "PARENT_ENTITY_NAME", "ADC_UPDT")
    .alias("al")
    )
    
    # Get building and facility references
    buildings = create_building_locations()
    facilities = create_facility_locations()
    
    # Filter for new/modified records and validate location type
    base_locations = locations.filter(
        (col("location_type_cd") == 794) & # Nurse Unit
        (col("ADC_UPDT") > max_adc_updt)
    )
    

        # Process and validate care site data
    processed_sites = (
        base_locations
        # Join to get care site name
        .join(
            code_values.alias("cs_codes"),
            col("loc.location_cd") == col("cs_codes.CODE_VALUE"),
            "left"
        )
        
        # Join building hierarchy
        .join(
            buildings.alias("bldg"), 
            col("loc.location_cd") == col("bldg.CHILD_LOC_CD"), 
            "left"
        )
        .join(
            code_values.alias("bldg_codes"),
            col("bldg.building_cd") == col("bldg_codes.CODE_VALUE"),
            "left"
        )
        
        # Join facility hierarchy
        .join(
            facilities.alias("fac"), 
            col("bldg.building_cd") == col("fac.CHILD_LOC_CD"), 
            "left"
        )
        .join(
            code_values.alias("fac_codes"),
            col("fac.facility_cd") == col("fac_codes.CODE_VALUE"),
            "left"
        )
        
        # Join organization details
        .join(
            organizations.select("ORGANIZATION_ID", "ORG_NAME"),
            col("loc.ORGANIZATION_ID") == col("org.ORGANIZATION_ID"),
            "left"
        )
        
        # Join organization address
        .join(
            current_addresses.filter(col("PARENT_ENTITY_NAME") == "ORGANIZATION")
            .select("PARENT_ENTITY_ID", "ADC_UPDT", "ADDRESS_ID"),
            col("org.ORGANIZATION_ID") == col("PARENT_ENTITY_ID"),
            "left"
        )
    )
    
    # Apply data quality validations
    validated_sites = processed_sites.filter(
        (col("location_type_cd") == 794) &
        col("loc.location_cd").isNotNull()
    )
    
    # Log validation failures if needed
    validation_failures = processed_sites.join(
        validated_sites,
        "location_cd",
        "left_anti"
    )
    
    if validation_failures.count() > 0:
        print(f"WARNING: {validation_failures.count()} records failed validation")
    
    # Select final columns with standardized names
    final_df = validated_sites.select(
        col("loc.location_cd").alias("care_site_cd"),
        col("loc.location_type_cd"),
        col("cs_codes.DISPLAY").alias("care_site_name"),
        col("bldg.building_cd"),
        col("bldg_codes.DISPLAY").alias("building_name"),
        col("fac.facility_cd"),
        col("fac_codes.DISPLAY").alias("facility_name"),
        col("org.ORGANIZATION_ID"),
        col("org.ORG_NAME").alias("organization_name"),
        col("ADDRESS_ID").alias("address_id"),
        greatest(
            col("loc.ADC_UPDT"),
            col("al.ADC_UPDT")
        ).alias("ADC_UPDT")
    )
    
    return final_df


updates_df = create_care_site_mapping_incr()
    

update_table(updates_df, "4_prod.bronze.map_care_site", "care_site_cd")
update_metadata("4_prod.bronze.map_care_site",schema_map_care_site, map_care_site_comment)

# COMMAND ----------

map_medical_personnel_comment = "The table contains information about personnel, specifically focusing on their roles and affiliations within the healthcare system. It includes details such as whether a person is a physician, their position, and their primary care site. This data can be used for managing personnel assignments, analyzing staffing needs, and ensuring appropriate access to applications and tasks based on position."

schema_map_medical_personnel = StructType([
    StructField("PERSON_ID", DoubleType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the person table. It is an internal system assigned number."
    }),
    StructField("PHYSICIAN_IND", DoubleType(), True, metadata={
        "comment": "Set to TRUE, if the personnel is a physician.  Otherwise, set to FALSE."
    }),
    StructField("POSITION_CD", DoubleType(), True, metadata={
        "comment": "The position is used to determine the applications and tasks the personnel is authorized to use."
    }),
    StructField("position_name", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("primary_care_site_cd", DoubleType(), True, metadata={
        "comment": "This field is the current patient location with a location type of nurse unit."
    }),
    StructField("primary_care_site_name", StringType(), True, metadata={
        "comment": "The display string for the code_value"
    }),
    StructField("SRVCATEGORY", StringType(), True, metadata={
        "comment": "The groups of service category."
    }),
    StructField("SURGSPEC", StringType(), True, metadata={
        "comment": "The groups of surgical specialty."
    }),
    StructField("MEDSERVICE", StringType(), True, metadata={
        "comment": "The groups of medical service lines."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Timestamp of last update."
    })
])


def create_group_types():
    """
    Creates a reference table for medical group types.
    
    Returns:
        DataFrame: Medical group types reference data
    """
    return (
        spark.table("3_lookup.mill.mill_code_value")
        .filter(col("CDF_MEANING").isin("SRVCATEGORY", "SURGSPEC", "MEDSERVICE"))
        .select(
            "CODE_VALUE",
            "CDF_MEANING",
            col("DESCRIPTION").alias("group_description")
        )
    )
def get_primary_care_locations(clinical_events, encounters, code_values):
    """
    Calculates primary care locations based on recent clinical events.
    
    Args:
        clinical_events: Clinical events DataFrame
        encounters: Encounters DataFrame
        code_values: Code values DataFrame
        
    Returns:
        DataFrame: Primary care locations with names
    """
    # Window for recent events
    recent_events_window = Window.partitionBy("PERFORMED_PRSNL_ID").orderBy(desc("CLINICAL_EVENT_ID"))
    
    # Get recent events, used to determine the most common nurse unit for a given person.
    recent_events = (
        clinical_events
        .join(encounters, "ENCNTR_ID")
        .filter(col("LOC_NURSE_UNIT_CD").isNotNull())
        .withColumn("row_num", row_number().over(recent_events_window))
        .filter(col("row_num") <= 10000)
    )
    
    # Calculate most common location
    location_counts_window = Window.partitionBy("PERFORMED_PRSNL_ID").orderBy(desc("event_count"), desc("LOC_NURSE_UNIT_CD"))
    
    primary_locations = (
        recent_events
        .groupBy("PERFORMED_PRSNL_ID", "LOC_NURSE_UNIT_CD")
        .agg(count("*").alias("event_count"))
        .withColumn("rank", dense_rank().over(location_counts_window))
        .filter(col("rank") == 1)
        .select(
            col("PERFORMED_PRSNL_ID").alias("PERSON_ID"),
            col("LOC_NURSE_UNIT_CD").alias("primary_care_site_cd")
        )
    )
    

    return (
        primary_locations
        .join(
            code_values
            .select("CODE_VALUE", "DISPLAY")  
            .alias("loc"),
            col("primary_care_site_cd") == col("loc.CODE_VALUE"),
            "left"
        )
        .select(
            "PERSON_ID",
            "primary_care_site_cd",
            col("loc.DISPLAY").alias("primary_care_site_name") 
        )
    )

def get_group_assignments(prsnl_group_reltn, valid_groups):
    """
    Gets latest group assignments for medical personnel.
    
    Args:
        prsnl_group_reltn: Personnel group relations DataFrame
        valid_groups: Valid groups DataFrame
        
    Returns:
        DataFrame: Pivoted group assignments
    """
    window_spec = Window.partitionBy("PERSON_ID", "CDF_MEANING").orderBy(desc("pgr.UPDT_DT_TM"))
    
    # Get latest assignments with explicit column selection
    latest_assignments = (
        prsnl_group_reltn.alias("pgr")
        .join(
            valid_groups.alias("vg"),
            col("pgr.PRSNL_GROUP_ID") == col("vg.PRSNL_GROUP_ID")
        )
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .select(
            col("PERSON_ID"),
            col("vg.CDF_MEANING"),
            col("vg.group_description")
        )
    )
    
    # Pivot with explicit values
    return (
        latest_assignments
        .groupBy("PERSON_ID")
        .pivot("CDF_MEANING", ["SRVCATEGORY", "SURGSPEC", "MEDSERVICE"])
        .agg(first("group_description"))
        .na.fill("")  # Replace nulls with empty strings
    )

def create_medical_personnel_mapping_incr():
    """
    Creates an incremental medical personnel mapping table.
    Processes only new or modified records.
    
    Returns:
        DataFrame: Processed medical personnel records with standardized format
    """

    max_adc_updt = get_max_timestamp("4_prod.bronze.map_medical_personnel")
    
    # Get base tables
    prsnl = (
        spark.table("4_prod.raw.mill_prsnl")
        .filter(
            (~((col("PHYSICIAN_IND").isNull() | (col("PHYSICIAN_IND") == 0)) & 
               (col("POSITION_CD").isNull() | (col("POSITION_CD") == 0)))) &
            (col("ADC_UPDT") > max_adc_updt)
        )
    )
    
    # Get reference tables
    code_values = spark.table("3_lookup.mill.mill_code_value")
    position_lookup = (
        code_values
        .select(
            col("CODE_VALUE"),
            col("DESCRIPTION").alias("position_name")
        )
    )
    group_types = create_group_types()
    
    # Get operational tables
    prsnl_groups = spark.table("4_prod.raw.mill_prsnl_group")
    prsnl_group_reltn = spark.table("4_prod.raw.mill_prsnl_group_reltn")
    clinical_events = spark.table("4_prod.raw.mill_clinical_event")
    encounters = spark.table("4_prod.raw.mill_encounter")
    
    # Get valid groups
    valid_groups = (
        prsnl_groups.alias("pg")
        .join(
            group_types,
            col("pg.PRSNL_GROUP_TYPE_CD") == col("CODE_VALUE")
        )
    )
    
    # Get primary care locations
    primary_locations = get_primary_care_locations(clinical_events, encounters, code_values)
    
    # Get group assignments
    group_assignments = get_group_assignments(prsnl_group_reltn, valid_groups)
    
    # Combine all data
    final_df = (
        prsnl
        .join(
            position_lookup,
            col("POSITION_CD") == col("CODE_VALUE"),
            "left"
        )
        .join(
            group_assignments,
            "PERSON_ID",
            "left"
        )
        .join(
            primary_locations,
            "PERSON_ID",
            "left"
        )
        .select(
            col("PERSON_ID"),
            col("PHYSICIAN_IND"),
            col("POSITION_CD"),
            col("position_name"),
            col("primary_care_site_cd"),
            col("primary_care_site_name"),
            coalesce(col("SRVCATEGORY"), lit("")).alias("SRVCATEGORY"),
            coalesce(col("SURGSPEC"), lit("")).alias("SURGSPEC"),
            coalesce(col("MEDSERVICE"), lit("")).alias("MEDSERVICE"),
            col("ADC_UPDT")
        )
    )
    
    return final_df


updates_df = create_medical_personnel_mapping_incr()

update_table(updates_df, "4_prod.bronze.map_medical_personnel", "PERSON_ID")
update_metadata("4_prod.bronze.map_medical_personnel",schema_map_medical_personnel,map_medical_personnel_comment
)


# COMMAND ----------

map_encounter_comment = "The table contains data related to patient encounters. It includes details such as the unique identifier for each encounter, timestamps for arrival and departure, encounter classifications, types, and statuses. This data can be used to analyze patient flow, understand the types of services provided, and track patient admissions and discharges across different units and specialties."


schema_map_encounter = StructType([
    StructField("ENCNTR_ID", DoubleType(), True, metadata={
        "comment": "Unique identifier for the Encounter table."
    }),
    StructField("PERSON_ID", DoubleType(), True, metadata={
        "comment": "Person whom this encounter is for."
    }),
    StructField("ARRIVE_DT_TM", TimestampType(), True, metadata={
        "comment": "The actual date/time that the patient arrived at the facility. At the time of registration, if this field is null then it should be valued with the reg_dt_tm. Otherwise, the actual arrival date/time is captured."
    }),
    StructField("DEPART_DT_TM", TimestampType(), True, metadata={
        "comment": "The actual date/time that the patient left from the facility. In many cases, this field may be null unless the user process requires capturing this data."
    }),
    StructField("ENCNTR_CLASS_CD", DoubleType(), True, metadata={
        "comment": "Encounter class defines how this encounter row is being used in relation to the person table."
    }),
    StructField("encntr_class_desc", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("ENCNTR_TYPE_CD", DoubleType(), True, metadata={
        "comment": "Categorizes the encounter into a logical group or type. Examples may include inpatient, outpatient, etc."
    }),
    StructField("encntr_type_desc", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("ENCNTR_STATUS_CD", DoubleType(), True, metadata={
        "comment": "Encounter status identifies the state of a particular encounter type from the time it is initiated until it is complete.  (i.e., temporary, preliminary, active, discharged (complete), cancelled)."
    }),
    StructField("encntr_status_desc", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("ADMIT_SRC_CD", DoubleType(), True, metadata={
        "comment": "Admit source identifies the place from which the patient came before being admitted. (i.e., transfer from another hospital)."
    }),
    StructField("admit_src_desc", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("DISCH_TO_LOCTN_CD", DoubleType(), True, metadata={
        "comment": "The location to which the patient was discharged such as another hospital or nursing home."
    }),
    StructField("disch_loctn_desc", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("MED_SERVICE_CD", DoubleType(), True, metadata={
        "comment": "The type or category of medical service that the patient is receiving in relation to their encounter.  The category may be of treatment type, surgery, general resources, or others."
    }),
    StructField("med_service_desc", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("LOC_NURSE_UNIT_CD", DoubleType(), True, metadata={
        "comment": "This field is the current patient location with a location type of nurse unit."
    }),
    StructField("nurse_unit_desc", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("SPECIALTY_UNIT_CD", DoubleType(), True, metadata={
        "comment": "The specialty unit associated with the program service"
    }),
    StructField("specialty_unit_desc", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("REG_PRSNL_ID", DoubleType(), True, metadata={
        "comment": "The internal person ID of the personnel that performed the registration or admission.  If the reg_dt_tm is valued, then this field must be valued."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Timestamp of last update."
    })
])

# Helper function to get event time boundaries for encounters
def get_event_times():
    """
    Calculates earliest and latest clinical event times for each encounter,
    excluding dates before 1950 and future dates.
    
    Returns:
        DataFrame: Event time boundaries with encounter ID
    """
    from pyspark.sql.functions import current_timestamp, year, col, min, max

    return (
        spark.table("4_prod.raw.mill_clinical_event")
        .where(
            (year("CLINSIG_UPDT_DT_TM") >= 1950) &
            (col("CLINSIG_UPDT_DT_TM") <= current_timestamp())  # Added col() here
        )
        .groupBy("ENCNTR_ID")
        .agg(
            min("CLINSIG_UPDT_DT_TM").alias("earliest_event_time"),
            max("CLINSIG_UPDT_DT_TM").alias("latest_event_time")
        )
    )

def create_code_lookup_encounter(code_values, description_alias):
    """
    Helper function to create code value lookups with consistent structure.
    
    Args:
        code_values: Base code values DataFrame
        description_alias: Alias for the description column
        
    Returns:
        DataFrame: Lookup table with standardized columns
    """
    return code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(description_alias)
    )

def create_encounter_mapping_incr():
    """
    Creates an incremental encounter mapping table processing only new/modified records.
    
    Process:
    1. Joins encounter data with clinical event times
    2. Applies code value lookups for various attributes
    3. Calculates arrival and departure times
    4. Standardizes output format
    
    Returns:
        DataFrame: Processed encounter records with standardized format
    """

    max_adc_updt = get_max_timestamp("4_prod.bronze.map_encounter")
    

    one_week_seconds = 7 * 24 * 60 * 60
    
    # Get base encounter data
    base_encounters = (
        spark.table("4_prod.raw.mill_encounter")
        .filter(col("ADC_UPDT") > max_adc_updt)
    )
    
    # Get reference data
    code_values = spark.table("3_lookup.mill.mill_code_value")
    event_times = get_event_times()
    
    # Create code value lookups
    lookups = {
        "class": create_code_lookup_encounter(code_values, "encntr_class_desc"),
        "type": create_code_lookup_encounter(code_values, "encntr_type_desc"),
        "status": create_code_lookup_encounter(code_values, "encntr_status_desc"),
        "admit": create_code_lookup_encounter(code_values, "admit_src_desc"),
        "disch": create_code_lookup_encounter(code_values, "disch_loctn_desc"),
        "med": create_code_lookup_encounter(code_values, "med_service_desc"),
        "nurse": create_code_lookup_encounter(code_values, "nurse_unit_desc"),
        "spec": create_code_lookup_encounter(code_values, "specialty_unit_desc")
    }
    
    # Process encounters
    processed_encounters = (
        base_encounters
        # Join with event times
        .join(event_times, "ENCNTR_ID", "left")
        # Join with all code value lookups
        .join(lookups["class"].alias("class"), 
              col("ENCNTR_CLASS_CD") == col("class.CODE_VALUE"), "left")
        .join(lookups["type"].alias("type"), 
              col("ENCNTR_TYPE_CD") == col("type.CODE_VALUE"), "left")
        .join(lookups["status"].alias("status"), 
              col("ENCNTR_STATUS_CD") == col("status.CODE_VALUE"), "left")
        .join(lookups["admit"].alias("admit"), 
              col("ADMIT_SRC_CD") == col("admit.CODE_VALUE"), "left")
        .join(lookups["disch"].alias("disch"), 
              col("DISCH_TO_LOCTN_CD") == col("disch.CODE_VALUE"), "left")
        .join(lookups["med"].alias("med"), 
              col("MED_SERVICE_CD") == col("med.CODE_VALUE"), "left")
        .join(lookups["nurse"].alias("nurse"), 
              col("LOC_NURSE_UNIT_CD") == col("nurse.CODE_VALUE"), "left")
        .join(lookups["spec"].alias("spec"), 
              col("SPECIALTY_UNIT_CD") == col("spec.CODE_VALUE"), "left")
        # Calculate arrival time
        .withColumn(
            "calculated_arrive_dt_tm",
            when(
                (col("earliest_event_time").isNotNull()) &
                (col("ARRIVE_DT_TM").isNotNull()) &
                (unix_timestamp(col("earliest_event_time")) - 
                 unix_timestamp(col("ARRIVE_DT_TM")) > one_week_seconds),
                col("earliest_event_time")
            ).otherwise(
                coalesce(
                    col("ARRIVE_DT_TM"),
                    col("EST_ARRIVE_DT_TM"),
                    col("earliest_event_time")
                )
            )
        )
        # Calculate departure time
        .withColumn(
            "calculated_depart_dt_tm",
            when(
                (col("latest_event_time").isNotNull()) &
                (col("DEPART_DT_TM").isNotNull()) &
                (unix_timestamp(col("DEPART_DT_TM")) - 
                 unix_timestamp(col("latest_event_time")) > one_week_seconds),
                col("latest_event_time")
            ).otherwise(
                coalesce(
                    col("DEPART_DT_TM"),
                    col("latest_event_time")
                )
            )
        )
        # Add fallback for null departure time
        .withColumn(
            "calculated_depart_dt_tm",
            coalesce(
                col("calculated_depart_dt_tm"),
                date_add(col("calculated_arrive_dt_tm"), 1)
            )
        )
        .filter(col("calculated_arrive_dt_tm").isNotNull())
    )
    
    # Return final selection
    return processed_encounters.select(
        col("ENCNTR_ID"),
        col("PERSON_ID"),
        col("calculated_arrive_dt_tm").alias("ARRIVE_DT_TM"),
        col("calculated_depart_dt_tm").alias("DEPART_DT_TM"),
        col("ENCNTR_CLASS_CD"),
        col("encntr_class_desc"),
        col("ENCNTR_TYPE_CD"),
        col("encntr_type_desc"),
        col("ENCNTR_STATUS_CD"),
        col("encntr_status_desc"),
        col("ADMIT_SRC_CD"),
        col("admit_src_desc"),
        col("DISCH_TO_LOCTN_CD"),
        col("disch_loctn_desc"),
        col("MED_SERVICE_CD"),
        col("med_service_desc"),
        col("LOC_NURSE_UNIT_CD"),
        col("nurse_unit_desc"),
        col("SPECIALTY_UNIT_CD"),
        col("specialty_unit_desc"),
        col("REG_PRSNL_ID"),
        col("ADC_UPDT")
    )


updates_df = create_encounter_mapping_incr()
    
 
update_table(updates_df, "4_prod.bronze.map_encounter", "ENCNTR_ID")
update_metadata("4_prod.bronze.map_encounter",schema_map_encounter,map_encounter_comment)

# COMMAND ----------


map_diagnosis_comment = "The table contains data related to medical diagnoses associated with individuals and their encounters. It includes information such as diagnosis dates, types, priorities, and classifications. This data can be used for analyzing diagnosis trends, understanding patient care patterns, and evaluating the effectiveness of clinical services."

schema_map_diagnosis = StructType([
    StructField("DIAGNOSIS_ID", DoubleType(), True, metadata={
        "comment": "The primary key for the Diagnosis table."
    }),
    StructField("PERSON_ID", DoubleType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the person table.  It is an internal system assigned number."
    }),
    StructField("ENCNTR_ID", DoubleType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the encounter table.  It is an internal system assigned number."
    }),
    StructField("DIAG_DT_TM", TimestampType(), True, metadata={
        "comment": "Date/time for which the Diagnosis was saved."
    }),
    StructField("earliest_diagnosis_date", TimestampType(), True, metadata={
        "comment": "The earliest recorded date on which a diagnosis was made for the patient."
    }),
    StructField("DIAG_TYPE_CD", DoubleType(), True, metadata={
        "comment": "The type of diagnosis."
    }),
    StructField("diag_type_desc", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("DIAG_PRIORITY", DoubleType(), True, metadata={
        "comment": "Priority of diagnoses as determined by application."
    }),
    StructField("RANKING_CD", DoubleType(), True, metadata={
        "comment": "Codified ranking description."
    }),
    StructField("DIAG_PRSNL_ID", DoubleType(), True, metadata={
        "comment": "Prsnl_id of person that added the diagnosis."
    }),
    StructField("CLINICAL_SERVICE_CD", DoubleType(), True, metadata={
        "comment": "Associates the clinical diagnosis to a particular setting of care within an encounter."
    }),
    StructField("clinical_service_desc", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("CONFIRMATION_STATUS_CD", DoubleType(), True, metadata={
        "comment": "Describes the definitiveness and clinical status of the diagnosis."
    }),
    StructField("confirmation_status_desc", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("CLASSIFICATION_CD", DoubleType(), True, metadata={
        "comment": "Classification of the clinical diagnosis by the area of focus."
    }),
    StructField("classification_desc", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("NOMENCLATURE_ID", DoubleType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the nomenclature table. It is an internal system assigned number."
    }),
    StructField("SOURCE_IDENTIFIER", StringType(), True, metadata={
        "comment": "The code, or key, from the source vocabulary that contributed the string to the nomenclature."
    }),
    StructField("SOURCE_STRING", StringType(), True, metadata={
        "comment": "Variable length string that may include alphanumeric characters and punctuation."
    }),
    StructField("SOURCE_VOCABULARY_CD", DoubleType(), True, metadata={
        "comment": "The external vocabulary or lexicon that contributed the string, e.g. ICD9, SNOMED, etc."
    }),
    StructField("source_vocabulary_desc", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("VOCAB_AXIS_CD", DoubleType(), True, metadata={
        "comment": "Vocabulary AXIS codes related to SNOMEDColumn."
    }),
    StructField("vocab_axis_desc", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("CONCEPT_CKI", StringType(), True, metadata={
        "comment": "Concept CKI is the functional Concept Identifier; it is the codified means within Millennium to identify key medical concepts to support information processing, clinical decision support, executable knowledge and knowledge presentation. Composed of a source and an identifier."
    }),
    StructField("OMOP_CONCEPT_ID", IntegerType(), True, metadata={
        "comment": "A unique identifier for each Concept across all domains."
    }),
    StructField("OMOP_CONCEPT_NAME", StringType(), True, metadata={
        "comment": "An unambiguous, meaningful and descriptive name for the Concept."
    }),
    StructField("OMOP_STANDARD_CONCEPT", StringType(), True, metadata={
        "comment": "This flag determines where a Concept is a Standard Concept, i.e. is used in the data, a Classification Concept, or a non-standard Source Concept. The allowables values are S (Standard Concept) and C (Classification Concept), otherwise the content is NULL."
    }),
    StructField("OMOP_MATCH_NUMBER", LongType(), True, metadata={
        "comment": "The number of OMOP concepts matched for each NOMENCLATURE_ID."
    }),
    StructField("OMOP_CONCEPT_DOMAIN", StringType(), True, metadata={
        "comment": "A unique identifier for each domain."
    }),
    StructField("SNOMED_CODE", LongType(), True, metadata={
        "comment": ""
    }),
    StructField("SNOMED_TYPE", StringType(), True, metadata={
        "comment": "The method or source of the SNOMED code mapping for each nomenclature entry."
    }),
    StructField("SNOMED_MATCH_NUMBER", LongType(), True, metadata={
        "comment": "The number of matches found for each NOMENCLATURE_ID in the context of SNOMED codes."
    }),
    StructField("SNOMED_TERM", StringType(), True, metadata={
        "comment": "The term associated with a SNOMED code that provides additional meaning and context to the code."
    }),
    StructField("ICD10_CODE", StringType(), True, metadata={
        "comment": ""
    }),
    StructField("ICD10_TYPE", StringType(), True, metadata={
        "comment": "The method or source of the ICD10 code mapping for each nomenclature entry."
    }),
    StructField("ICD10_MATCH_NUMBER", LongType(), True, metadata={
        "comment": "The number of matches found for each NOMENCLATURE_ID in the context of ICD10 codes."
    }),
    StructField("ICD10_TERM", StringType(), True, metadata={
        "comment": "The term associated with a ICD10 code that provides additional meaning and context to the code."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Timestamp of last update."
    })
    
])



def create_diagnosis_mapping_incr():
    """
    Creates an incremental diagnosis mapping table processing only new/modified records.
    
    Process:
    1. Joins diagnosis data with encounter data for validation
    2. Calculates dates using various available fields
    3. Enriches with nomenclature data
    4. Adds code value descriptions
    5. Standardizes output format
    
    Returns:
        DataFrame: Processed diagnosis records with standardized format
    """

    max_adc_updt = get_max_timestamp("4_prod.bronze.map_diagnosis")
    
    # Get base tables
    diagnosis = spark.table("4_prod.raw.mill_diagnosis").filter(col("ADC_UPDT") > max_adc_updt)
    nomenclature = spark.table("4_prod.bronze.nomenclature")
    encounter = spark.table("4_prod.bronze.map_encounter")
    code_values = spark.table("3_lookup.mill.mill_code_value")

    
    # Create window for earliest diagnosis date
    earliest_date_window = Window.partitionBy("PERSON_ID", "NOMENCLATURE_ID")
    
    # Join diagnosis with encounter
    diagnosis_with_encounter = (
        diagnosis
        .join(
            encounter.select("ENCNTR_ID", "ARRIVE_DT_TM"),
            "ENCNTR_ID",
            "left"
        )
    )




    # Calculate diagnosis dates
    diagnosis_with_dates = (
        diagnosis_with_encounter
        .withColumn(
            "calc_diag_dt_tm",
            coalesce(
                col("DIAG_DT_TM"),
                col("ARRIVE_DT_TM"),
                col("BEG_EFFECTIVE_DT_TM")
            )
        )
        .withColumn(
            "earliest_diagnosis_date",
            min(
                when(
                    col("calc_diag_dt_tm") > "1950-01-01",
                    col("calc_diag_dt_tm")
                )
            ).over(earliest_date_window)
        )
    )
    
    # Create code value lookups
    lookups = {
        "diag_type": ("DIAG_TYPE_CD", "diag_type_desc"),
        "clinical_service": ("CLINICAL_SERVICE_CD", "clinical_service_desc"),
        "confirmation_status": ("CONFIRMATION_STATUS_CD", "confirmation_status_desc"),
        "classification": ("CLASSIFICATION_CD", "classification_desc"),
        "source_vocabulary": ("SOURCE_VOCABULARY_CD", "source_vocabulary_desc"),
        "vocab_axis": ("VOCAB_AXIS_CD", "vocab_axis_desc")
    }
    
    # Process nomenclature
    nomenclature_processed = (
        nomenclature
        .withColumn(
            "CONCEPT_CKI_PROCESSED",
            substring_index(col("CONCEPT_CKI"), "!", -1)
        )
    )
       # Check nomenclature join
    after_nomenclature = diagnosis_with_encounter.join(
        nomenclature_processed,
        "NOMENCLATURE_ID",
        "left"
    )

    
    # Check one of the code value joins
    after_code_value = after_nomenclature.join(
        code_values.select(
            col("CODE_VALUE"),
            col("DESCRIPTION").alias("diag_type_desc")
        ).alias("diag_type"),
        col("DIAG_TYPE_CD") == col("diag_type.CODE_VALUE"),
        "left"
    )

    # Start with base join
    result = diagnosis_with_dates.alias("diag").join(
        nomenclature_processed.alias("nom"),
        "NOMENCLATURE_ID",
        "left"
    )

    # Add code value lookups one by one
    for lookup_name, (join_col, desc_col) in lookups.items():
        lookup_df = code_values.select(
            col("CODE_VALUE"),
            col("DESCRIPTION").alias(desc_col)
        ).alias(lookup_name)
        
        result = result.join(
            lookup_df,
            col(join_col) == col(f"{lookup_name}.CODE_VALUE"),
            "left"
        )

    # Select final columns
    return result.select(
        # Base columns
        col("DIAGNOSIS_ID"),
        col("PERSON_ID"),
        col("ENCNTR_ID"),
        col("calc_diag_dt_tm").alias("DIAG_DT_TM"),
        col("earliest_diagnosis_date"),
        # Code columns with descriptions
        col("DIAG_TYPE_CD"),
        col("diag_type_desc"),
        col("DIAG_PRIORITY"),
        col("RANKING_CD"),
        col("DIAG_PRSNL_ID"),
        col("CLINICAL_SERVICE_CD"),
        col("clinical_service_desc"),
        col("CONFIRMATION_STATUS_CD"),
        col("confirmation_status_desc"),
        col("CLASSIFICATION_CD"),
        col("classification_desc"),
        # Nomenclature columns
        col("NOMENCLATURE_ID"),
        col("SOURCE_IDENTIFIER"),
        col("SOURCE_STRING"),
        col("SOURCE_VOCABULARY_CD"),
        col("source_vocabulary_desc"),
        col("VOCAB_AXIS_CD"),
        col("vocab_axis_desc"),
        col("CONCEPT_CKI_PROCESSED").alias("CONCEPT_CKI"),
        col("OMOP_CONCEPT_ID"),
        col("OMOP_CONCEPT_NAME"),
        col("IS_STANDARD_OMOP_CONCEPT").alias("OMOP_STANDARD_CONCEPT"),
        col("NUMBER_OF_OMOP_MATCHES").alias("OMOP_MATCH_NUMBER"),
        col("CONCEPT_DOMAIN").alias("OMOP_CONCEPT_DOMAIN"),
        col("SNOMED_CODE"),
        col("SNOMED_TYPE"),
        col("SNOMED_MATCH_COUNT").alias("SNOMED_MATCH_NUMBER"),
        col("SNOMED_TERM"),
        col("ICD10_CODE"),
        col("ICD10_TYPE"),
        col("ICD10_MATCH_COUNT").alias("ICD10_MATCH_NUMBER"),
        col("ICD10_TERM"),
        # Use greatest ADC_UPDT between diagnosis and nomenclature
        greatest(
            col("diag.ADC_UPDT"),
            col("nom.ADC_UPDT")
        ).alias("ADC_UPDT")
    )


updates_df = create_diagnosis_mapping_incr()





update_table(updates_df, "4_prod.bronze.map_diagnosis", "DIAGNOSIS_ID")
update_metadata("4_prod.bronze.map_diagnosis",schema_map_diagnosis,map_diagnosis_comment)


# COMMAND ----------

map_problem_comment = "The table contains information about various problems associated with individuals. It includes details such as the onset date and time of the problem, active status updates, and classification codes that categorize the problems. This data can be used to track problem occurrences, analyze trends over time, and manage problem resolution processes effectively."


schema_map_problem = StructType([
    StructField("PROBLEM_ID", LongType(), True, metadata={
        "comment": "Uniquely defines a problem within the problem table.  The problem_id can be associated with multiple problem instances.  When a new problem is added to the problem table the problem_id is assigned to the problem_instance_id."
    }),
    StructField("PERSON_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the person table.  It is an internal system assigned number."
    }),
    StructField("NOMENCLATURE_ID", LongType(), True, metadata={
        "comment": "Unique identifier for nomenclature item."
    }),
    StructField("ONSET_DT_TM", TimestampType(), True, metadata={
        "comment": "The date and time that the problem began."
    }),
    StructField("earliest_problem_date", TimestampType(), True, metadata={
        "comment": "The earliest date and time when a problem was recorded for a given person and nomenclature"
    }),
    StructField("ACTIVE_STATUS_DT_TM", TimestampType(), True, metadata={
        "comment": "The date and time that the active_status_cd was set."
    }),
    StructField("ACTIVE_STATUS_PRSNL_ID", LongType(), True, metadata={
        "comment": "The person who caused the active_status_cd to be set or change."
    }),
    StructField("DATA_STATUS_DT_TM", TimestampType(), True, metadata={
        "comment": "The date and time that the data_status_cd was set."
    }),
    StructField("DATA_STATUS_PRSNL_ID", DoubleType(), True, metadata={
        "comment": "The person who caused the data_status_cd to be set or change."
    }),
    StructField("UPDATE_ENCNTR_ID", LongType(), True, metadata={
        "comment": "The value of the unique primary identifierof the encounter table. Represents the last encounter id on which the problem was modified"
    }),
    StructField("ORIGINATING_ENCNTR_ID", LongType(), True, metadata={
        "comment": "The value of the unique primary identifierof the encounter table. Represents the originating encounter id on which the problem was first documented"
    }),
    StructField("CONFIRMATION_STATUS_CD", DoubleType(), True, metadata={
        "comment": "Indicates the verification status of the problem."
    }),
    StructField("confirmation_status_desc", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("CLASSIFICATION_CD", DoubleType(), True, metadata={
        "comment": "Identifies the kind of problem.  Used to categorize the problem so that it may be managed and viewed independently within different applications."
    }),
    StructField("classification_desc", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("RANKING_CD", DoubleType(), True, metadata={
        "comment": "A user-defined prioritization of the problem."
    }),
    StructField("SOURCE_IDENTIFIER", StringType(), True, metadata={
        "comment": "The code, or key, from the source vocabulary that contributed the string to the nomenclature."
    }),
    StructField("SOURCE_STRING", StringType(), True, metadata={
        "comment": "Variable length string that may include alphanumeric characters and punctuation."
    }),
    StructField("SOURCE_VOCABULARY_CD", DoubleType(), True, metadata={
        "comment": "The external vocabulary or lexicon that contributed the string, e.g. ICD9, SNOMED, etc."
    }),
    StructField("source_vocabulary_desc", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("VOCAB_AXIS_CD", DoubleType(), True, metadata={
        "comment": "Vocabulary AXIS codes related to SNOMEDColumn"
    }),
    StructField("vocab_axis_desc", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("CONCEPT_CKI_PROCESSED", StringType(), True, metadata={
        "comment": "The processed version of the CONCEPT_CKI field."
    }),
    StructField("OMOP_CONCEPT_ID", IntegerType(), True, metadata={
        "comment": "A unique identifier for each Concept across all domains."
    }),
    StructField("OMOP_CONCEPT_NAME", StringType(), True, metadata={
        "comment": "An unambiguous, meaningful and descriptive name for the Concept."
    }),
    StructField("OMOP_STANDARD_CONCEPT", StringType(), True, metadata={
        "comment": "This flag determines where a Concept is a Standard Concept, i.e. is used in the data, a Classification Concept, or a non-standard Source Concept. The allowables values are S (Standard Concept) and C (Classification Concept), otherwise the content is NULL."
    }),
    StructField("OMOP_MATCH_NUMBER", LongType(), True, metadata={
        "comment": "The number of OMOP concepts matched for each NOMENCLATURE_ID."
    }),
    StructField("OMOP_CONCEPT_DOMAIN", StringType(), True, metadata={
        "comment": "A unique identifier for each domain."
    }),
    StructField("SNOMED_CODE", LongType(), True, metadata={
        "comment": ""
    }),
    StructField("SNOMED_TYPE", StringType(), True, metadata={
        "comment": "The method or source of the SNOMED code mapping for each nomenclature entry."
    }),
    StructField("SNOMED_MATCH_NUMBER", LongType(), True, metadata={
        "comment": "The number of matches found for each NOMENCLATURE_ID in the context of SNOMED codes."
    }),
    StructField("SNOMED_TERM", StringType(), True, metadata={
        "comment": "The term associated with a SNOMED code that provides additional meaning and context to the code."
    }),
    StructField("ICD10_CODE", StringType(), True, metadata={
        "comment": ""
    }),
    StructField("ICD10_TYPE", StringType(), True, metadata={
        "comment": "The method or source of the ICD10 code mapping for each nomenclature entry."
    }),
    StructField("ICD10_MATCH_NUMBER", LongType(), True, metadata={
        "comment": "The number of matches found for each NOMENCLATURE_ID in the context of ICD10 codes."
    }),
    StructField("ICD10_TERM", StringType(), True, metadata={
        "comment": "The term associated with a ICD10 code that provides additional meaning and context to the code."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Last update timestamp."
    }),
    StructField("CALC_DT_TM", TimestampType(), True, metadata={
        "comment": "The calculated date and time for the problem, representing the onset date and time."
    }),
    StructField("CALC_ENCNTR", LongType(), True, metadata={
        "comment": "The calculated encounter ID associated with a specific problem entry."
        }),
    StructField("CALC_ENC_WITHIN", LongType(), True, metadata={
        "comment": "The calculated encounter ID associated with a specific problem entry where the problem event occurred within the encounter time frame."
        }),
    StructField("CALC_ENC_BEFORE", LongType(), True, metadata={
        "comment": "The calculated encounter ID associated with a specific problem entry where the problem event occurred before the encounter time frame."
        }),
    StructField("CALC_ENC_AFTER", LongType(), True, metadata={
        "comment": "calculated encounter ID associated with a specific problem entry where the problem event occurred after the encounter time frame."
        })

 
])



def create_problem_code_lookup(code_values, alias_name, desc_alias):
    """
    Helper function to create standardized code value lookups for problems
    
    Args:
        code_values: Base code values DataFrame
        alias_name: Alias for the lookup table
        desc_alias: Alias for the description column
        
    Returns:
        DataFrame: Lookup table with CODE_VALUE and description
    """
    return code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(desc_alias)
    ).alias(alias_name)

def cast_problem_ids_to_long(df):
    """
    Helper function to cast problem ID columns to LongType
    
    Args:
        df: Input DataFrame with ID columns
        
    Returns:
        DataFrame: DataFrame with ID columns cast to LongType
    """
    id_columns = [
        "PROBLEM_ID", "ACTIVE_STATUS_PRSNL_ID", "ENCNTR_ID",
        "PERSON_ID", "NOMENCLATURE_ID", "ENCNTR_SLICE_ID"
    ]
    
    for col_name in id_columns:
        df = df.withColumn(col_name, col(col_name).cast(LongType()))
    
    return df

def process_problem_encounter_associations(base_result, encounters):
    """
    Helper function to process encounter associations for problems
    
    Args:
        base_result: Base problem DataFrame
        encounters: Encounters reference DataFrame
        
    Returns:
        DataFrame: Problems with encounter associations
    """
    return (
        base_result.alias("base")  
        .withColumn("ORIGINATING_ENCNTR_ID",
            when(col("ORIGINATING_ENCNTR_ID").isin(0, 1), None)
            .otherwise(col("ORIGINATING_ENCNTR_ID"))
        )
        .withColumn("UPDATE_ENCNTR_ID",
            when(col("UPDATE_ENCNTR_ID").isin(0, 1), None)
            .otherwise(col("UPDATE_ENCNTR_ID"))
        )
        .withColumn("ACTIVE_STATUS_PRSNL_ID",
            when(col("ACTIVE_STATUS_PRSNL_ID").isin(0, 1), None)
            .otherwise(col("ACTIVE_STATUS_PRSNL_ID"))
        )
        .join(
            encounters.alias("enc"),
            col("base.PERSON_ID") == col("enc.PERSON_ID"),  
            "left"
        )
        .withColumn("is_within", 
            (col("CALC_DT_TM") >= col("enc.ARRIVE_DT_TM")) &
            (col("CALC_DT_TM") <= col("enc.DEPART_DT_TM"))
        )
        .withColumn("is_before",
            col("enc.DEPART_DT_TM") <= col("CALC_DT_TM")
        )
        .withColumn("is_after",
            col("enc.ARRIVE_DT_TM") > col("CALC_DT_TM")
        )
        .withColumn("CALC_ENC_WITHIN",
            expr("max(case when is_within then enc.ENCNTR_ID else null end)")
            .over(Window.partitionBy("base.PROBLEM_ID"))  
        )
        .withColumn("CALC_ENC_BEFORE",
            expr("max(case when is_before then enc.ENCNTR_ID else null end)")
            .over(Window.partitionBy("base.PROBLEM_ID"))  
        )
        .withColumn("CALC_ENC_AFTER",
            expr("min(case when is_after then enc.ENCNTR_ID else null end)")
            .over(Window.partitionBy("base.PROBLEM_ID")) 
        )
        .withColumn("CALC_ENCNTR",
            coalesce(
                col("ORIGINATING_ENCNTR_ID"),
                col("UPDATE_ENCNTR_ID"),
                col("CALC_ENC_WITHIN"),
                col("CALC_ENC_BEFORE"),
                col("CALC_ENC_AFTER")
            )
        )
        .select(
            col("base.*"),
            col("CALC_ENCNTR"),
            col("CALC_ENC_WITHIN"),
            col("CALC_ENC_BEFORE"),
            col("CALC_ENC_AFTER")
        )
    )



def create_problem_mapping_incr():
    """
    Creates an incremental problem mapping table processing only new/modified records.
    """

    max_adc_updt = get_max_timestamp("4_prod.bronze.map_problem")
    
    # Get base tables
    problem = spark.table("4_prod.raw.mill_problem")
    nomenclature = spark.table("4_prod.bronze.nomenclature").alias("nom")
    code_values = spark.table("3_lookup.mill.mill_code_value")
    encounters = spark.table("4_prod.bronze.map_encounter").alias("enc")
    
    base_problems = problem.filter(col("ADC_UPDT") > max_adc_updt)

    earliest_date_window = Window.partitionBy("PERSON_ID", "NOMENCLATURE_ID")
    
    problem_with_dates = (
        base_problems
        .withColumn(
            "earliest_problem_date",
            min(
                when(
                    col("ONSET_DT_TM") > "1950-01-01",
                    col("ONSET_DT_TM")
                )
            ).over(earliest_date_window)
        )
    )
    
    # Create code lookups
    confirmation_status_lookup = create_problem_code_lookup(code_values, "conf", "confirmation_status_desc")
    classification_lookup = create_problem_code_lookup(code_values, "class", "classification_desc")
    source_vocab_lookup = create_problem_code_lookup(code_values, "vocab", "source_vocabulary_desc")
    vocab_axis_lookup = create_problem_code_lookup(code_values, "axis", "vocab_axis_desc")
    
    # Process nomenclature
    nomenclature_processed = (
        nomenclature
        .withColumn(
            "CONCEPT_CKI_PROCESSED",
            substring_index(col("CONCEPT_CKI"), "!", -1)
        )
    )
    
    # Build base result
    base_result = (
        problem_with_dates.alias("prob")
        .join(
            nomenclature_processed,
            "NOMENCLATURE_ID",
            "left"
        )
        .join(
            confirmation_status_lookup,
            col("prob.CONFIRMATION_STATUS_CD") == col("conf.CODE_VALUE"),
            "left"
        )
        .join(
            classification_lookup,
            col("prob.CLASSIFICATION_CD") == col("class.CODE_VALUE"),
            "left"
        )
        .join(
            source_vocab_lookup,
            col("SOURCE_VOCABULARY_CD") == col("vocab.CODE_VALUE"),
            "left"
        )
        .join(
            vocab_axis_lookup,
            col("VOCAB_AXIS_CD") == col("axis.CODE_VALUE"),
            "left"
        )
        .withColumn(
            "CALC_DT_TM",
            coalesce(
                col("ONSET_DT_TM"),
                col("ACTIVE_STATUS_DT_TM")
            )
        )
    )
    
    # Process encounter associations
    result_with_encounters = (
        base_result
        # Clean up encounter IDs
        .withColumn("ORIGINATING_ENCNTR_ID",
            when(col("ORIGINATING_ENCNTR_ID").isin(0, 1), None)
            .otherwise(col("ORIGINATING_ENCNTR_ID"))
        )
        .withColumn("UPDATE_ENCNTR_ID",
            when(col("UPDATE_ENCNTR_ID").isin(0, 1), None)
            .otherwise(col("UPDATE_ENCNTR_ID"))
        )
        .withColumn("ACTIVE_STATUS_PRSNL_ID",
            when(col("ACTIVE_STATUS_PRSNL_ID").isin(0, 1), None)
            .otherwise(col("ACTIVE_STATUS_PRSNL_ID"))
        )
        # Join encounters for arrival/departure dates
        .join(
            encounters.select(
                col("PERSON_ID").alias("enc_person_id"),
                "ENCNTR_ID", 
                "ARRIVE_DT_TM", 
                "DEPART_DT_TM"
            ),
            col("prob.PERSON_ID") == col("enc_person_id"),
            "left"
        )
        .withColumn("is_within", 
            (col("CALC_DT_TM") >= col("ARRIVE_DT_TM")) &
            (col("CALC_DT_TM") <= col("DEPART_DT_TM"))
        )
        .withColumn("is_before",
            col("DEPART_DT_TM") <= col("CALC_DT_TM")
        )
        .withColumn("is_after",
            col("ARRIVE_DT_TM") > col("CALC_DT_TM")
        )
        .withColumn("CALC_ENC_WITHIN",
            expr("max(case when is_within then ENCNTR_ID else null end)")
            .over(Window.partitionBy("PROBLEM_ID"))
        )
        .withColumn("CALC_ENC_BEFORE",
            expr("max(case when is_before then ENCNTR_ID else null end)")
            .over(Window.partitionBy("PROBLEM_ID"))
        )
        .withColumn("CALC_ENC_AFTER",
            expr("min(case when is_after then ENCNTR_ID else null end)")
            .over(Window.partitionBy("PROBLEM_ID"))
        )
        .withColumn("CALC_ENCNTR",
            coalesce(
                col("ORIGINATING_ENCNTR_ID"),
                col("UPDATE_ENCNTR_ID"),
                col("CALC_ENC_WITHIN"),
                col("CALC_ENC_BEFORE"),
                col("CALC_ENC_AFTER")
            )
        )
        # Drop temporary columns
        .drop("is_within", "is_before", "is_after", "ARRIVE_DT_TM", "DEPART_DT_TM", "enc_person_id")
    )
    
    # Cast ID columns to LongType
    for id_col in ["PROBLEM_ID", "PERSON_ID", "NOMENCLATURE_ID", "ACTIVE_STATUS_PRSNL_ID",
                   "ORIGINATING_ENCNTR_ID", "UPDATE_ENCNTR_ID", "CALC_ENCNTR",
                   "CALC_ENC_WITHIN", "CALC_ENC_BEFORE", "CALC_ENC_AFTER"]:
        result_with_encounters = result_with_encounters.withColumn(
            id_col, 
            col(id_col).cast(LongType())
        )

    deduplicated_result = (
        result_with_encounters
        .withColumn(
            "row_number",
            row_number().over(
                Window.partitionBy("PROBLEM_ID")
                .orderBy(
                    col("DATA_STATUS_DT_TM").desc(),  # Most recent data status first
                    col("CALC_ENCNTR").desc()         # If data status is same, take most recent encounter
                )
            )
        )
        .filter(col("row_number") == 1)  # Keep only the most recent record
        .drop("row_number")
    )
    
    # Return final selection with proper column ordering
    return deduplicated_result.select(
        "PROBLEM_ID",
        "PERSON_ID", 
        "NOMENCLATURE_ID",
        "ONSET_DT_TM",
        "earliest_problem_date",
        "ACTIVE_STATUS_DT_TM",
        "ACTIVE_STATUS_PRSNL_ID",
        "DATA_STATUS_DT_TM",
        "DATA_STATUS_PRSNL_ID",
        "UPDATE_ENCNTR_ID",
        "ORIGINATING_ENCNTR_ID",
        "CONFIRMATION_STATUS_CD",
        "confirmation_status_desc",
        "CLASSIFICATION_CD", 
        "classification_desc",
        "RANKING_CD",
        "SOURCE_IDENTIFIER",
        "SOURCE_STRING",
        "SOURCE_VOCABULARY_CD",
        "source_vocabulary_desc",
        "VOCAB_AXIS_CD",
        "vocab_axis_desc",
        "CONCEPT_CKI_PROCESSED",
        "OMOP_CONCEPT_ID",
        "OMOP_CONCEPT_NAME",
        col("IS_STANDARD_OMOP_CONCEPT").alias("OMOP_STANDARD_CONCEPT"),
        col("NUMBER_OF_OMOP_MATCHES").alias("OMOP_MATCH_NUMBER"),
        col("CONCEPT_DOMAIN").alias("OMOP_CONCEPT_DOMAIN"),
        "SNOMED_CODE",
        "SNOMED_TYPE",
        col("SNOMED_MATCH_COUNT").alias("SNOMED_MATCH_NUMBER"),
        "SNOMED_TERM",
        "ICD10_CODE",
        "ICD10_TYPE",
        col("ICD10_MATCH_COUNT").alias("ICD10_MATCH_NUMBER"),
        "ICD10_TERM",
        "prob.ADC_UPDT",
        "CALC_DT_TM",
        "CALC_ENCNTR",
        "CALC_ENC_WITHIN",
        "CALC_ENC_BEFORE",
        "CALC_ENC_AFTER"
    )


updates_df = create_problem_mapping_incr().distinct()
    

update_table(updates_df, "4_prod.bronze.map_problem", "PROBLEM_ID")

update_metadata("4_prod.bronze.map_problem",schema_map_problem,map_problem_comment)


# COMMAND ----------

map_med_admin_comment = "The table contains data related to medical orders and events associated with patients. It includes information such as the person and encounter identifiers, event details, order specifics, and timestamps for administrative actions. This data can be used for tracking patient orders, analyzing treatment events, and understanding the outcomes of various medical interventions."

schema_map_med_admin = StructType([
    StructField("PERSON_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the person table. It is an internal system assigned number."
    }),
    StructField("ENCNTR_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the encounter table. It is an internal system assigned number."
    }),
    StructField("EVENT_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the Event Table."
    }),
    StructField("ORDER_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the Order Table."
    }),
    StructField("EVENT_TYPE_CD", IntegerType(), True, metadata={
        "comment": "Identifies what type of event was audited. Values can be code_value for TASKPURGED, TASKCOMPLETE, NOTDONE, or NOTGIVEN from code_set 4000040."
    }),
    StructField("EVENT_TYPE_DISPLAY", StringType(), True, metadata={
        "comment": "Display value for the code."
    }),
    StructField("RESULT_STATUS_CD", IntegerType(), True, metadata={
        "comment": "Result status code. Valid values: authenticated, unauthenticated, unknown, cancelled, pending, in lab, active, modified, superseded, transcribed, not done."
    }),
    StructField("RESULT_STATUS_DISPLAY", StringType(), True, metadata={
        "comment": "Display value for the code."
    }),
    StructField("ADMIN_START_DT_TM", TimestampType(), True, metadata={
        "comment": "The time at which this medication administration became active for continuous administrations. For intermittent, it is the time the administration happened."
    }),
    StructField("ADMIN_END_DT_TM", TimestampType(), True, metadata={
        "comment": "For continuous administrations, this field is the end of the time period in which this administration was active. If the administration is currently active, this field will be NULL. For intermittent administrations, this field does not apply."
    }),
    StructField("ORDER_SYNONYM_ID", LongType(), True, metadata={
        "comment": "Identifier for the underlying concept, two values having the same synonmid_id means the different orders are synonyms for each other."
    }),
    StructField("ORDER_CKI", StringType(), True, metadata={
        "comment": "Unique identifier for the order from the Order Catalogue."
    }),
    StructField("MULTUM", StringType(), True, metadata={
        "comment": "The Multum drug code associated with the medication order."
    }),
    StructField("RXNORM_CUI", StringType(), True, metadata={
        "comment": "The RxNorm Concept Unique Identifier (CUI) associated with the medication order."
    }),
    StructField("RXNORM_STR", StringType(), True, metadata={
        "comment": "Provides additional context by displaying the description or name of the medication corresponding to the RxNorm code."
    }),
    StructField("SNOMED_CODE", StringType(), True, metadata={"comment": "The Snomed concept ID(SCTID)"}),
    StructField("SNOMED_STR", StringType(), True, metadata={
        "comment": "The description of the SNOMED Code."
    }),
    StructField("ORDER_MNEMONIC", StringType(), True, metadata={
        "comment": "Text description of the Order. The mnemonic mostly used by department personnel, for example, Lab Technicians, Pharmacists. For Pharmacy orders, this field is not populated until product is assigned by Pharmacy Technician or Pharmacist. The field is truncated and will contain a maximum of 99 characters. Ellipses are not appended if the field is truncated."
    }),
    StructField("ORDER_DETAIL", StringType(), True, metadata={
        "comment": "Any additional free text information describing the order."
    }),
    StructField("ORDER_STRENGTH", FloatType(), True, metadata={
        "comment": "Strength of the order ingredient as a number."
    }),
    StructField("ORDER_STRENGTH_UNIT_CD", DoubleType(), True, metadata={
        "comment": "Code for the unit of measure for the strength of the order ingredient."
    }),
    StructField("ORDER_STRENGTH_UNIT_DISPLAY", StringType(), True, metadata={
        "comment": "Text description of the unit of measure for the strength of the order ingredient."
    }),
    StructField("ORDER_VOLUME", FloatType(), True, metadata={
        "comment": "Volume of the order ingredient as a number."
    }),
    StructField("ORDER_VOLUME_UNIT_CD", DoubleType(), True, metadata={
        "comment": "Code for the unit of measure for the volume of the order ingredient."
    }),
    StructField("ORDER_VOLUME_UNIT_DISPLAY", StringType(), True, metadata={
        "comment": "Text description of the unit of measure for the volume of the order incredient."
    }),
    StructField("ADMIN_ROUTE_CD", IntegerType(), True, metadata={
        "comment": "Code for the method of administration of the medication."
    }),
    StructField("ADMIN_ROUTE_DISPLAY", StringType(), True, metadata={
        "comment": "Text description of the administration route, e.g. intravenous"
    }),
    StructField("INITIAL_DOSAGE", FloatType(), True, metadata={
        "comment": "Initial volume or quantity of the administered dose."
    }),
    StructField("INITIAL_DOSAGE_UNIT_CD", IntegerType(), True, metadata={
        "comment": "Code for the unit of measurement of the initial dosage."
    }),
    StructField("INITIAL_DOSAGE_UNIT_DISPLAY", StringType(), True, metadata={
        "comment": "Text description of the unit of measurement of the initial dosage."
    }),
    StructField("ADMIN_DOSAGE", FloatType(), True, metadata={
        "comment": "Actual volume or quantity of administration."
    }),
    StructField("ADMIN_DOSAGE_UNIT_CD", IntegerType(), True, metadata={
        "comment": "Code for the unit of measurement for dosage."
    }),
    StructField("ADMIN_DOSAGE_UNIT_DISPLAY", StringType(), True, metadata={
        "comment": "Text description of the unit of measurement for dosage."
    }),
    StructField("INITIAL_VOLUME", FloatType(), True, metadata={
        "comment": "Total volume medication and diluent at the beginning of the administration."
    }),
    StructField("INFUSED_VOLUME", FloatType(), True, metadata={
        "comment": "The volume at any one point in time that remains in the IV Bag."
    }),
    StructField("INFUSED_VOLUME_UNIT_CD", IntegerType(), True, metadata={
        "comment": "Code for the unit of measure for infused volume."
    }),
    StructField("INFUSED_VOLUME_UNIT_DISPLAY", StringType(), True, metadata={
        "comment": "Text description of the unit of measure for infused volume."
    }),
    StructField("INFUSION_RATE", FloatType(), True, metadata={
        "comment": "For continuously administered medications, IV or IVP, the infusion rate and unit is used to capture the flow rate of the medication into the patient."
    }),
    StructField("INFUSION_UNIT_CD", IntegerType(), True, metadata={
        "comment": "Code for the unit of measure for volume or quantity of the medication. i.e. ml, drip, tablet."
    }),
    StructField("INFUSION_UNIT_DISPLAY", StringType(), True, metadata={
        "comment": "Text description of the unit of measure for volume or quantity of the medication."
    }),
    StructField("NURSE_UNIT_CD", IntegerType(), True, metadata={
        "comment": "Code for the nurse unit of the device the user is using to enter the medication admin event."
    }),
    StructField("NURSE_UNIT_DISPLAY", StringType(), True, metadata={
        "comment": "The text description of the nurse unit of the device the user is using to enter the medication admin event."
    }),
    StructField("POSITION_CD", IntegerType(), True, metadata={
        "comment": "Code for the position used to determine the applications and tasks the personnel is authorized to use."
    }),
    StructField("POSITION_DISPLAY", StringType(), True, metadata={
        "comment": "Text description of the position used to determine the applications and tasks the personnel is authorized to use."
    }),
    StructField("PRSNL_ID", LongType(), True, metadata={
        "comment": "The ID of the user documenting the medication admin event."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Last update timestamp"
    }),
    StructField("DOSE_IN_MG", DoubleType(), True, metadata={
        "comment": "The standardized medication dose expressed in milligrams (mg)."
    }),
    StructField("DOSE_IN_ML", DoubleType(), True, metadata={
        "comment": "The standardized medication dose expressed in milliliters (mL)."
    }),
    StructField("DOSE_UNIT_CATEGORY", StringType(), True, metadata={
        "comment": "It classifies the medication dose unit as weight-based (mg), volume-based (mL), units, discrete forms (like tablet or puff), or other, based on the content of the ADMIN_DOSAGE_UNIT_DISPLAY column."
    }),
    StructField("SNOMED_SOURCE", StringType(), True, metadata={
        "comment": "The method or source of the SNOMED code mapping for each nomenclature entry."
    }),
    StructField("OMOP_CONCEPT_ID", IntegerType(), True, metadata={
        "comment": "A unique identifier for each Concept across all domains."
    }),
    StructField("OMOP_CONCEPT_NAME", StringType(), True, metadata={
        "comment": "An unambiguous, meaningful and descriptive name for the Concept."
    }),
    StructField("OMOP_STANDARD_CONCEPT", StringType(), True, metadata={
        "comment": "This flag determines where a Concept is a Standard Concept, i.e. is used in the data, a Classification Concept, or a non-standard Source Concept. The allowables values are S (Standard Concept) and C (Classification Concept), otherwise the content is NULL."
    }),
    StructField("OMOP_TYPE", StringType(), True, metadata={"comment": "It indicates the source or method of the OMOP concept mapping for each record."})
])

def get_unit_conversion_maps_med_admin():
    """Returns dictionaries for unit conversions"""
    weight_conversions = {
        'microgram': 0.001,
        'micrograms': 0.001,
        'microgram/': 0.001,
        'microgram(s)': 0.001,
        'mcg': 0.001,
        'nanogram': 0.000001,
        'nanogram(s)': 0.000001,
        'nanogram/': 0.000001,
        'ng': 0.000001,
        'g': 1000,
        'gram': 1000,
        'g/': 1000,
        'mg': 1.0,
        'milligram': 1.0,
        'mg/': 1.0
    }
    
    volume_conversions = {
        'mL': 1.0,
        'mL(s)': 1.0,
        'mL/': 1.0,
        'milliliter': 1.0,
        'cc': 1.0,
        'L': 1000,
        'liter': 1000,
        'L/': 1000
    }
    
    return weight_conversions, volume_conversions

def create_standardization_expressions_med_admin(weight_conversions, volume_conversions):
    """Creates standardization expressions for unit conversions"""
    def create_weight_case():
        return [
            when(col("ADMIN_DOSAGE_UNIT_DISPLAY").like(f"%{unit}%"), 
                 when(col("ADMIN_DOSAGE") == 0, 
                      when(col("INITIAL_DOSAGE") == 0, col("INITIAL_VOLUME"))
                      .otherwise(col("INITIAL_DOSAGE")))
                 .otherwise(col("ADMIN_DOSAGE")) * lit(factor))
            for unit, factor in weight_conversions.items()
        ]
    
    def create_volume_case():
        return [
            when(col("ADMIN_DOSAGE_UNIT_DISPLAY").like(f"%{unit}%"),
                 when(col("ADMIN_DOSAGE") == 0, 
                      when(col("INITIAL_DOSAGE") == 0, col("INITIAL_VOLUME"))
                      .otherwise(col("INITIAL_DOSAGE")))
                 .otherwise(col("ADMIN_DOSAGE")) * lit(factor))
            for unit, factor in volume_conversions.items()
        ]
    
    return {
        "weight_cases": create_weight_case(),
        "volume_cases": create_volume_case()
    }

def add_standardized_columns(df, standardization_cases):
    """Adds standardized dosage columns to the dataframe"""
    return df.withColumns({
        # Standard weight dose in mg
        "DOSE_IN_MG": coalesce(*standardization_cases["weight_cases"]),
        
        # Standard volume dose in mL
        "DOSE_IN_ML": coalesce(*standardization_cases["volume_cases"]),
        
        # Add category flag for the type of standardization applied
        "DOSE_UNIT_CATEGORY": (
            when(col("ADMIN_DOSAGE_UNIT_DISPLAY").rlike(".*(mg|g|microgram|nanogram).*"), "WEIGHT_MG")
            .when(col("ADMIN_DOSAGE_UNIT_DISPLAY").rlike(".*(mL|L).*"), "VOLUME_ML")
            .when(col("ADMIN_DOSAGE_UNIT_DISPLAY").rlike(".*(unit|Unit|UNIT).*"), "UNITS")
            .when(col("ADMIN_DOSAGE_UNIT_DISPLAY").rlike(".*(application|spray|patch|scoop|inch|dose|puff|drop|tablet|capsule).*"), "DISCRETE")
            .otherwise("OTHER")
        )
    })

def create_base_medication_administrations_incr():
    """Creates base medication administration records with all joins, handling freetext entries."""

    freetext_synonym_id = 789453129
    max_adc_updt = get_max_timestamp("4_prod.bronze.map_med_admin")

    def add_code_value_lookup(df, cd_column, alias_prefix):
        """Helper function to add code value lookups"""
        # Define the lookup table and apply the alias immediately
        code_value_lookup_df = spark.table("3_lookup.mill.mill_code_value").alias(f"{alias_prefix}_CV")

        # Get the column from the input dataframe to join on
        df_join_col = col(cd_column)

        # Get the target join column from the aliased lookup dataframe
        lookup_join_col = col(f"{alias_prefix}_CV.CODE_VALUE")

        # --- Correction Here ---
        # Access the schema of the *original* table before aliasing, or just use the known column name
        # Assuming the column name in '3_lookup.mill.mill_code_value' is indeed 'CODE_VALUE'
        try:
            # Get the data type of the CODE_VALUE column from the lookup table's schema
            code_value_target_type = spark.table("3_lookup.mill.mill_code_value").schema["CODE_VALUE"].dataType
        except KeyError:
             # Fallback or raise a more specific error if 'CODE_VALUE' might not exist
             print(f"Warning: 'CODE_VALUE' column not found in 3_lookup.mill.mill_code_value schema. Assuming StringType for join cast.")
             code_value_target_type = StringType() # Or handle as an error

        # Perform the join, casting the input df column to the lookup table's column type
        return df.join(
            code_value_lookup_df, # Use the aliased dataframe
            df_join_col.cast(code_value_target_type) == lookup_join_col, # Join condition using aliased column
            "left"
        )

    def get_shortest_snomed_codes():
        """Helper function to get shortest SNOMED codes from RxNorm"""
        rxnorm = spark.table("3_lookup.rxnorm.rxnconso")
        return (rxnorm
                .filter(col("SAB") == "SNOMEDCT_US")
                .withColumn("CODE_LENGTH", length(col("CODE")))
                .withColumn("rn", row_number().over(
                    Window.partitionBy("RXCUI").orderBy("CODE_LENGTH")))
                .filter(col("rn") == 1)
                .select("RXCUI", "CODE", "STR")
               ) # Alias added later where used

    # Get base tables with aliases and filtering
    clinical_event = spark.table("4_prod.raw.mill_clinical_event").alias("CE")
    med_admin_event = spark.table("4_prod.raw.mill_med_admin_event").alias("MAE") \
        .filter((col("MAE.EVENT_TYPE_CD").isNotNull()) & (col("MAE.EVENT_TYPE_CD") != 0))
    encounter = spark.table("4_prod.raw.mill_encounter").alias("ENC")
    ce_med_result = spark.table("4_prod.raw.mill_ce_med_result").alias("MR")
    orders = spark.table("4_prod.raw.mill_orders").alias("ORDERS")
    # Assuming OI.STRENGTH_UNIT and OI.VOLUME_UNIT are IntegerType or compatible
    order_ingredient = spark.table("4_prod.raw.mill_order_ingredient").alias("OI")
    order_catalog_synonym = spark.table("3_lookup.mill.mill_order_catalog_synonym").alias("OSYN")
    order_catalog = spark.table("3_lookup.mill.mill_order_catalog") # Alias added later
    rxnorm = spark.table("3_lookup.rxnorm.rxnconso") # Alias added later
    shortest_snomed = get_shortest_snomed_codes() # Get the dataframe

    # Window specs
    ce_window = Window.partitionBy("EVENT_ID").orderBy(desc("VALID_FROM_DT_TM"))
    oi_window = Window.partitionBy("ORDER_ID", "SYNONYM_ID").orderBy("ACTION_SEQUENCE")

    # Get list of relevant event IDs
    med_events = med_admin_event.select("EVENT_ID")
    med_results = ce_med_result.select("EVENT_ID")
    all_med_events = med_events.union(med_results).distinct()

    # Get unit conversion maps and standardization cases
    weight_conversions, volume_conversions = get_unit_conversion_maps_med_admin()
    standardization_cases = create_standardization_expressions_med_admin(
        weight_conversions,
        volume_conversions
    )

    base_joins = (
        clinical_event.filter(
            (col("CE.VALID_UNTIL_DT_TM") > current_timestamp()) &
            (col("CE.ADC_UPDT") > max_adc_updt)
        )
        .join(all_med_events, "EVENT_ID", "inner")
        .join(med_admin_event, col("CE.EVENT_ID") == col("MAE.EVENT_ID"), "left")
        .join(encounter, col("CE.ENCNTR_ID") == col("ENC.ENCNTR_ID"), "inner")
        .join(
            ce_med_result.withColumn("rn", F.row_number().over(ce_window)).filter(col("rn") == 1),
            col("CE.EVENT_ID") == col("MR.EVENT_ID"),
            "left"
        )
        .join(
            orders,
            col("CE.ORDER_ID") == col("ORDERS.ORDER_ID"),
            "left"
        )
        # --- Standard Joins (for non-freetext) ---
        .join(
            order_ingredient
            .withColumn("oi_rn", F.row_number().over(oi_window))
            .filter(col("oi_rn") == 1),
            (col("ORDERS.TEMPLATE_ORDER_ID") == col("OI.ORDER_ID")) &
            (col("ORDERS.SYNONYM_ID") == col("OI.SYNONYM_ID")) &
            (col("ORDERS.SYNONYM_ID") != freetext_synonym_id),
            "left"
        )
        .join(
             order_catalog.alias("OCAT_STD"),
             (col("ORDERS.CATALOG_CD") == col("OCAT_STD.CATALOG_CD")) &
             (col("ORDERS.SYNONYM_ID") != freetext_synonym_id),
             "left"
        )
        .join(
            order_catalog_synonym,
            (col("ORDERS.SYNONYM_ID") == col("OSYN.SYNONYM_ID")) &
            (col("ORDERS.SYNONYM_ID") != freetext_synonym_id),
            "left"
        )
        # --- Freetext Specific Joins ---
        .join(
            order_catalog.alias("OCAT_FT"),
            (lower(col("ORDERS.ORDER_MNEMONIC")) == lower(col("OCAT_FT.PRIMARY_MNEMONIC"))) &
            (col("ORDERS.SYNONYM_ID") == freetext_synonym_id),
            "left"
        )
        # --- RxNorm and SNOMED lookups (handle both standard and freetext) ---
        .join(
            rxnorm.alias("RXN_STD").filter(col("RXN_STD.SAB") == "MMSL"),
            when(col("OCAT_STD.CKI").like("MUL.ORD%"),
                 substring_index(col("OCAT_STD.CKI"), "!", -1)) == col("RXN_STD.CODE"),
            "left"
        )
        .join(
            shortest_snomed.alias("SNOMED_STD"),
            col("RXN_STD.RXCUI") == col("SNOMED_STD.RXCUI"),
            "left"
        )
        .join(
            rxnorm.alias("RXN_FT").filter(col("RXN_FT.SAB") == "MMSL"),
            when(col("OCAT_FT.CKI").like("MUL.ORD%"),
                 substring_index(col("OCAT_FT.CKI"), "!", -1)) == col("RXN_FT.CODE"),
            "left"
        )
        .join(
            shortest_snomed.alias("SNOMED_FT"),
            col("RXN_FT.RXCUI") == col("SNOMED_FT.RXCUI"),
            "left"
        )
        # --- Pre-calculate conditional unit codes ---
        .withColumn(
             "_cond_strength_unit_cd",
             when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, lit(None).cast(IntegerType())) # Use original type of OI.STRENGTH_UNIT if known, else IntegerType
             .otherwise(col("OI.STRENGTH_UNIT"))
         )
        .withColumn(
             "_cond_volume_unit_cd",
             when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, lit(None).cast(IntegerType())) # Use original type of OI.VOLUME_UNIT if known, else IntegerType
             .otherwise(col("OI.VOLUME_UNIT"))
         )
    )

    # Add code value lookups AFTER defining conditional codes if needed,
    # but the original placement joining on OI.* should still work.
    # Let's keep the original lookup logic for now.
    lookups_added = (
        base_joins
        .transform(lambda df: add_code_value_lookup(df, "MAE.EVENT_TYPE_CD", "EVENT_TYPE"))
        .transform(lambda df: add_code_value_lookup(df, "MR.ADMIN_ROUTE_CD", "ADMIN_ROUTE"))
        .transform(lambda df: add_code_value_lookup(df, "MR.INFUSION_UNIT_CD", "INFUSION_UNIT"))
        .transform(lambda df: add_code_value_lookup(df, "MR.REFUSAL_CD", "REFUSAL"))
        .transform(lambda df: add_code_value_lookup(df, "MAE.POSITION_CD", "POSITION"))
        .transform(lambda df: add_code_value_lookup(df, "MAE.NURSE_UNIT_CD", "NURSE_UNIT"))
        # These lookups still join on the original OI columns. The CV columns will be null if OI.* was null (freetext case)
        .transform(lambda df: add_code_value_lookup(df, "OI.STRENGTH_UNIT", "STRENGTH_UNIT"))
        .transform(lambda df: add_code_value_lookup(df, "OI.VOLUME_UNIT", "VOLUME_UNIT"))
        .transform(lambda df: add_code_value_lookup(df, "CE.RESULT_STATUS_CD", "RESULT_STATUS"))
        .transform(lambda df: add_code_value_lookup(df, "MR.DOSAGE_UNIT_CD", "DOSAGE_UNIT"))
        .transform(lambda df: add_code_value_lookup(df, "MR.INFUSED_VOLUME_UNIT_CD", "INFUSED_VOLUME_UNIT"))
    )

    result_df = (
        lookups_added.select(
            # Identifiers
            col("CE.PERSON_ID").cast(LongType()),
            col("CE.ENCNTR_ID").cast(LongType()),
            col("CE.EVENT_ID").cast(LongType()),
            col("CE.ORDER_ID").cast(LongType()),

            # Status codes and their lookups
            col("MAE.EVENT_TYPE_CD").cast(IntegerType()),
            F.when(col("MAE.EVENT_TYPE_CD").isNull() | (col("MAE.EVENT_TYPE_CD") == 0), None)
             .otherwise(coalesce(col("EVENT_TYPE_CV.DISPLAY"), col("EVENT_TYPE_CV.CDF_MEANING")))
             .cast(StringType()).alias("EVENT_TYPE_DISPLAY"),

            col("CE.RESULT_STATUS_CD").cast(IntegerType()),
            F.when(col("CE.RESULT_STATUS_CD").isNull() | (col("CE.RESULT_STATUS_CD") == 0), None)
             .otherwise(coalesce(col("RESULT_STATUS_CV.DISPLAY"), col("RESULT_STATUS_CV.CDF_MEANING")))
             .cast(StringType()).alias("RESULT_STATUS_DISPLAY"),

            # Timing Information
            coalesce(col("MAE.BEG_DT_TM"), col("MR.ADMIN_START_DT_TM"), col("CE.EVENT_START_DT_TM")).cast(TimestampType()).alias("ADMIN_START_DT_TM"),
            coalesce(col("MAE.END_DT_TM"), col("MR.ADMIN_END_DT_TM"), col("CE.EVENT_END_DT_TM")).cast(TimestampType()).alias("ADMIN_END_DT_TM"),

            # --- Conditional Order Information ---
            when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, None)
                .otherwise(col("OI.SYNONYM_ID")).cast(LongType()).alias("ORDER_SYNONYM_ID"),

            when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, col("OCAT_FT.CKI"))
                .otherwise(col("OCAT_STD.CKI")).cast(StringType()).alias("ORDER_CKI"),

            when(
                (when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, col("OCAT_FT.CKI")).otherwise(col("OCAT_STD.CKI"))).like("MUL.ORD%"),
                 substring_index(when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, col("OCAT_FT.CKI")).otherwise(col("OCAT_STD.CKI")), "!", -1)
                ).cast(StringType()).alias("MULTUM"),

            # --- Conditional RxNorm and SNOMED columns ---
            when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, col("RXN_FT.RXCUI"))
                .otherwise(col("RXN_STD.RXCUI")).cast(StringType()).alias("RXNORM_CUI"),
            when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, col("RXN_FT.STR"))
                .otherwise(col("RXN_STD.STR")).cast(StringType()).alias("RXNORM_STR"),
            when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, col("SNOMED_FT.CODE"))
                .otherwise(col("SNOMED_STD.CODE")).cast(StringType()).alias("SNOMED_CODE"),
            when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, col("SNOMED_FT.STR"))
                .otherwise(col("SNOMED_STD.STR")).cast(StringType()).alias("SNOMED_STR"),

            # --- Conditional Order Details ---
            when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, col("ORDERS.ORDER_MNEMONIC"))
                .otherwise(col("OSYN.MNEMONIC")).cast(StringType()).alias("ORDER_MNEMONIC"),

            when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, None)
                .otherwise(col("OI.ORDER_DETAIL_DISPLAY_LINE")).cast(StringType()).alias("ORDER_DETAIL"),

            # --- Conditional Dosing Information (using pre-calculated codes) ---
            when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, None)
                .otherwise(col("OI.STRENGTH")).cast(FloatType()).alias("ORDER_STRENGTH"),

            # Use the pre-calculated conditional code directly
            col("_cond_strength_unit_cd").alias("ORDER_STRENGTH_UNIT_CD"),

            # Use the simple condition on the pre-calculated code for the display value
            F.when(
                col("_cond_strength_unit_cd").isNull() | (col("_cond_strength_unit_cd") == 0),
                None
             )
             .otherwise(coalesce(col("STRENGTH_UNIT_CV.DISPLAY"), col("STRENGTH_UNIT_CV.CDF_MEANING")))
             .cast(StringType()).alias("ORDER_STRENGTH_UNIT_DISPLAY"),

            when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, None)
                .otherwise(col("OI.VOLUME")).cast(FloatType()).alias("ORDER_VOLUME"),

            # Use the pre-calculated conditional code directly
            col("_cond_volume_unit_cd").alias("ORDER_VOLUME_UNIT_CD"),

            # Use the simple condition on the pre-calculated code for the display value
             F.when(
                col("_cond_volume_unit_cd").isNull() | (col("_cond_volume_unit_cd") == 0),
                None
             )
             .otherwise(coalesce(col("VOLUME_UNIT_CV.DISPLAY"), col("VOLUME_UNIT_CV.CDF_MEANING")))
             .cast(StringType()).alias("ORDER_VOLUME_UNIT_DISPLAY"),

            # --- Administration Details ---
            col("MR.ADMIN_ROUTE_CD").cast(IntegerType()),
            F.when(col("MR.ADMIN_ROUTE_CD").isNull() | (col("MR.ADMIN_ROUTE_CD") == 0), None)
             .otherwise(coalesce(col("ADMIN_ROUTE_CV.DISPLAY"), col("ADMIN_ROUTE_CV.CDF_MEANING")))
             .cast(StringType()).alias("ADMIN_ROUTE_DISPLAY"),

            col("MR.INITIAL_DOSAGE").cast(FloatType()).alias("INITIAL_DOSAGE"),
            col("MR.DOSAGE_UNIT_CD").cast(IntegerType()).alias("INITIAL_DOSAGE_UNIT_CD"),
            F.when(col("MR.DOSAGE_UNIT_CD").isNull() | (col("MR.DOSAGE_UNIT_CD") == 0), None)
             .otherwise(coalesce(col("DOSAGE_UNIT_CV.DISPLAY"), col("DOSAGE_UNIT_CV.CDF_MEANING")))
             .cast(StringType()).alias("INITIAL_DOSAGE_UNIT_DISPLAY"),

            col("MR.ADMIN_DOSAGE").cast(FloatType()).alias("ADMIN_DOSAGE"),
            col("MR.DOSAGE_UNIT_CD").cast(IntegerType()).alias("ADMIN_DOSAGE_UNIT_CD"), # Re-using alias
            F.when(col("MR.DOSAGE_UNIT_CD").isNull() | (col("MR.DOSAGE_UNIT_CD") == 0), None)
             .otherwise(coalesce(col("DOSAGE_UNIT_CV.DISPLAY"), col("DOSAGE_UNIT_CV.CDF_MEANING"))) # Re-using alias
             .cast(StringType()).alias("ADMIN_DOSAGE_UNIT_DISPLAY"),

            # Volume Information
            col("MR.INITIAL_VOLUME").cast(FloatType()).alias("INITIAL_VOLUME"),
            col("MR.INFUSED_VOLUME").cast(FloatType()).alias("INFUSED_VOLUME"),
            col("MR.INFUSED_VOLUME_UNIT_CD").cast(IntegerType()).alias("INFUSED_VOLUME_UNIT_CD"),
            F.when(col("MR.INFUSED_VOLUME_UNIT_CD").isNull() | (col("MR.INFUSED_VOLUME_UNIT_CD") == 0), None)
             .otherwise(coalesce(col("INFUSED_VOLUME_UNIT_CV.DISPLAY"), col("INFUSED_VOLUME_UNIT_CV.CDF_MEANING")))
             .cast(StringType()).alias("INFUSED_VOLUME_UNIT_DISPLAY"),

            # Infusion Details
            col("MR.INFUSION_RATE").cast(FloatType()).alias("INFUSION_RATE"),
            col("MR.INFUSION_UNIT_CD").cast(IntegerType()).alias("INFUSION_UNIT_CD"),
            F.when(col("MR.INFUSION_UNIT_CD").isNull() | (col("MR.INFUSION_UNIT_CD") == 0), None)
             .otherwise(coalesce(col("INFUSION_UNIT_CV.DISPLAY"), col("INFUSION_UNIT_CV.CDF_MEANING")))
             .cast(StringType()).alias("INFUSION_UNIT_DISPLAY"),

            col("MAE.NURSE_UNIT_CD").cast(IntegerType()).alias("NURSE_UNIT_CD"),
            F.when(col("MAE.NURSE_UNIT_CD").isNull() | (col("MAE.NURSE_UNIT_CD") == 0), None)
             .otherwise(coalesce(col("NURSE_UNIT_CV.DISPLAY"), col("NURSE_UNIT_CV.CDF_MEANING")))
             .cast(StringType()).alias("NURSE_UNIT_DISPLAY"),

            # Provider Information
            col("MAE.POSITION_CD").cast(IntegerType()).alias("POSITION_CD"),
            F.when(col("MAE.POSITION_CD").isNull() | (col("MAE.POSITION_CD") == 0), None)
             .otherwise(coalesce(col("POSITION_CV.DISPLAY"), col("POSITION_CV.CDF_MEANING")))
             .cast(StringType()).alias("POSITION_DISPLAY"),

            col("MAE.PRSNL_ID").cast(LongType()).alias("PRSNL_ID"),

            # Update Tracking
            F.greatest(
                col("CE.ADC_UPDT"),
                col("MAE.ADC_UPDT"),
                col("MR.ADC_UPDT"),
                when(col("ORDERS.SYNONYM_ID") == freetext_synonym_id, lit(None).cast(TimestampType()))
                    .otherwise(col("OI.ADC_UPDT"))
            ).alias("ADC_UPDT")
        )
        .drop("_cond_strength_unit_cd", "_cond_volume_unit_cd") # Drop temporary columns
        .distinct()
    )

    # Add standardized columns and return
    return add_standardized_columns(result_df, standardization_cases)



# COMMAND ----------

class MedAdminReferenceTables:
    def __init__(self):
        # Get base OMOP concept table
        self.concepts = (
            spark.table("3_lookup.omop.concept")
            .filter(col("invalid_reason").isNull())
        )
        
        # Get RxNorm concepts with SNOMED mappings
        self.rxnorm_snomed = (
            spark.table("3_lookup.rxnorm.rxnconso")
            .filter(col("SAB") == "SNOMEDCT_US")
            .withColumn("CODE_LENGTH", length(col("CODE")))
            .withColumn("rn", row_number().over(
                Window.partitionBy("RXCUI").orderBy("CODE_LENGTH")))
            .filter(col("rn") == 1)
            .select(
                col("RXCUI"),
                col("CODE").alias("cui"),
                col("STR").alias("term")
            )
        )
        
        # Get drug domain concepts
        self.drug_concepts = (
            self.concepts
            .filter((col("domain_id") == "Drug") | (col("domain_id") == "Ingredient"))
            .select(
                col("concept_id"),
                col("concept_name"),
                col("vocabulary_id"),
                col("concept_code"),
                col("standard_concept")
            )
        )

# COMMAND ----------

@udf(returnType=StringType())
def extract_drug_name_med_admin(x):
    """Extracts drug name before first number or returns lowercase string"""
    if not x:
        return None
    if any(c.isdigit() for c in x):
        return x.lower().split()[0]
    return x.lower().strip()





def augment_snomed_codes(med_df, refs):
    """
    Augments SNOMED codes in the medication administration table.
    Args:
        med_df: Base medication administration DataFrame
        refs: MedAdminReferenceTables instance
    """
    # Get rows that need mapping
    rows_to_map = med_df.filter(col("SNOMED_CODE").isNull())
    rows_with_codes = med_df.filter(col("SNOMED_CODE").isNotNull())
    
    # Get valid SNOMED concepts for medications
    valid_snomed_concept_ids = (
        refs.concepts
        .filter(
            (col("vocabulary_id") == "SNOMED") &
            (col("standard_concept").isNotNull()) &
            ((col("domain_id") == "Drug") | (col("domain_id") == "Ingredient"))
        )
        .select("concept_id")
        .collect()
    )
    
    valid_ids = [row.concept_id for row in valid_snomed_concept_ids]
    
    # Get drug concepts with lowercase names
    drug_concepts = (
        refs.drug_concepts
        .select(
            col("concept_id").alias("drug_concept_id"),
            lower(col("concept_name")).alias("drug_concept_name")
        )
    )
    
    # Get SNOMED direct matches with lowercase terms
    snomed_direct_matches = (
        refs.rxnorm_snomed
        .select(
            lower(col("term")).alias("sct_term_exact"),
            col("cui").alias("snomed_from_sct")
        )
        .withColumn("rn", row_number().over(Window.partitionBy("sct_term_exact").orderBy("snomed_from_sct")))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    # Get SNOMED matches for simplified terms
    snomed_direct_matches_simplified = (
        refs.rxnorm_snomed
        .select(
            lower(col("term")).alias("sct_term_simplified"),
            col("cui").alias("snomed_from_sct_simplified")
        )
        .withColumn("rn", row_number().over(Window.partitionBy("sct_term_simplified").orderBy("snomed_from_sct_simplified")))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    # Forward mappings for exact matches
    forward_mappings = (
        spark.table("3_lookup.omop.concept_relationship")
        .filter(
            ((col("relationship_id") == "Maps to") | 
             (col("relationship_id") == "Mapped from")) &
            col("concept_id_2").isin(valid_ids)
        )
        .join(
            drug_concepts,
            col("concept_id_1") == col("drug_concept_id")
        )
        .join(
            refs.concepts.alias("snomed_concept"),
            col("concept_id_2") == col("snomed_concept.concept_id"),
            "inner"
        )
        .select(
            col("drug_concept_name").alias("forward_term"),
            col("snomed_concept.concept_code").alias("snomed_from_forward_mapping")
        )
        .withColumn("rn", row_number().over(Window.partitionBy("forward_term").orderBy("snomed_from_forward_mapping")))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    # Forward mappings for simplified names
    forward_mappings_simplified = (
        spark.table("3_lookup.omop.concept_relationship")
        .filter(
            ((col("relationship_id") == "Maps to") | 
             (col("relationship_id") == "Mapped from")) &
            col("concept_id_2").isin(valid_ids)
        )
        .join(
            drug_concepts,
            col("concept_id_1") == col("drug_concept_id")
        )
        .join(
            refs.concepts.alias("snomed_concept"),
            col("concept_id_2") == col("snomed_concept.concept_id"),
            "inner"
        )
        .select(
            col("drug_concept_name").alias("forward_term_simplified"),
            col("snomed_concept.concept_code").alias("snomed_from_forward_mapping_simplified")
        )
        .withColumn("rn", row_number().over(Window.partitionBy("forward_term_simplified").orderBy("snomed_from_forward_mapping_simplified")))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    # Backward mappings from SNOMED
    backward_mappings = (
        spark.table("3_lookup.omop.concept_relationship")
        .filter(
            ((col("relationship_id") == "Maps to") | 
             (col("relationship_id") == "Mapped from")) &
            col("concept_id_1").isin(valid_ids)
        )
        .join(
            drug_concepts,
            col("concept_id_2") == col("drug_concept_id")
        )
        .join(
            refs.concepts.alias("snomed_concept"),
            col("concept_id_1") == col("snomed_concept.concept_id"),
            "inner"
        )
        .select(
            col("drug_concept_name").alias("backward_term"),
            col("snomed_concept.concept_code").alias("snomed_from_backward_mapping")
        )
        .withColumn("rn", row_number().over(Window.partitionBy("backward_term").orderBy("snomed_from_backward_mapping")))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    # Backward mappings for simplified names
    backward_mappings_simplified = (
        spark.table("3_lookup.omop.concept_relationship")
        .filter(
            ((col("relationship_id") == "Maps to") | 
             (col("relationship_id") == "Mapped from")) &
            col("concept_id_1").isin(valid_ids)
        )
        .join(
            drug_concepts,
            col("concept_id_2") == col("drug_concept_id")
        )
        .join(
            refs.concepts.alias("snomed_concept"),
            col("concept_id_1") == col("snomed_concept.concept_id"),
            "inner"
        )
        .select(
            col("drug_concept_name").alias("backward_term_simplified"),
            col("snomed_concept.concept_code").alias("snomed_from_backward_mapping_simplified")
        )
        .withColumn("rn", row_number().over(Window.partitionBy("backward_term_simplified").orderBy("snomed_from_backward_mapping_simplified")))
        .filter(col("rn") == 1)
        .drop("rn")
    )

       # Apply mappings to rows needing mapping
    mapped_df = (
        rows_to_map
        .withColumn("order_term", lower(col("ORDER_MNEMONIC")))
        
        # Try exact matches first
        .join(
            snomed_direct_matches.alias("sct1"),
            col("order_term") == col("sct1.sct_term_exact"),
            "left"
        )
        .join(
            forward_mappings.alias("fwd1"),
            (col("order_term") == col("fwd1.forward_term")) &
            col("sct1.snomed_from_sct").isNull(),
            "left"
        )
        .join(
            backward_mappings.alias("bwd1"),
            (col("order_term") == col("bwd1.backward_term")) &
            col("sct1.snomed_from_sct").isNull() &
            col("fwd1.snomed_from_forward_mapping").isNull(),
            "left"
        )
        
        # For unmatched rows, try matching on drug name only
        .withColumn(
            "drug_name_only",
            when(
                coalesce(
                    col("sct1.snomed_from_sct"),
                    col("fwd1.snomed_from_forward_mapping"),
                    col("bwd1.snomed_from_backward_mapping")
                ).isNull(),
                extract_drug_name_med_admin(col("order_term"))
            )
        )
        
        # Try matching again with simplified drug name
        .join(
            snomed_direct_matches_simplified.alias("sct2"),
            (col("drug_name_only") == col("sct2.sct_term_simplified")) &
            coalesce(
                col("sct1.snomed_from_sct"),
                col("fwd1.snomed_from_forward_mapping"),
                col("bwd1.snomed_from_backward_mapping")
            ).isNull(),
            "left"
        )
        .join(
            forward_mappings_simplified.alias("fwd2"),
            (col("drug_name_only") == col("fwd2.forward_term_simplified")) &
            coalesce(
                col("sct1.snomed_from_sct"),
                col("fwd1.snomed_from_forward_mapping"),
                col("bwd1.snomed_from_backward_mapping"),
                col("sct2.snomed_from_sct_simplified")
            ).isNull(),
            "left"
        )
        .join(
            backward_mappings_simplified.alias("bwd2"),
            (col("drug_name_only") == col("bwd2.backward_term_simplified")) &
            coalesce(
                col("sct1.snomed_from_sct"),
                col("fwd1.snomed_from_forward_mapping"),
                col("bwd1.snomed_from_backward_mapping"),
                col("sct2.snomed_from_sct_simplified"),
                col("fwd2.snomed_from_forward_mapping_simplified")
            ).isNull(),
            "left"
        )
        
        # Create new SNOMED_CODE and SNOMED_SOURCE columns
        .withColumn(
            "SNOMED_CODE",
            coalesce(
                col("sct1.snomed_from_sct"),
                col("fwd1.snomed_from_forward_mapping"),
                col("bwd1.snomed_from_backward_mapping"),
                col("sct2.snomed_from_sct_simplified"),
                col("fwd2.snomed_from_forward_mapping_simplified"),
                col("bwd2.snomed_from_backward_mapping_simplified")
            )
        )
        .withColumn(
            "SNOMED_SOURCE",
            when(col("sct1.snomed_from_sct").isNotNull(), "SNOMED_SCT")
            .when(col("fwd1.snomed_from_forward_mapping").isNotNull(), "OMOP_FORWARD")
            .when(col("bwd1.snomed_from_backward_mapping").isNotNull(), "OMOP_BACKWARD")
            .when(col("sct2.snomed_from_sct_simplified").isNotNull(), "SNOMED_SCT_SIMPLIFIED")
            .when(col("fwd2.snomed_from_forward_mapping_simplified").isNotNull(), "OMOP_FORWARD_SIMPLIFIED")
            .when(col("bwd2.snomed_from_backward_mapping_simplified").isNotNull(), "OMOP_BACKWARD_SIMPLIFIED")
            .otherwise("NOT_FOUND")
        )
        
        # Select all original columns plus new ones
        .select(
            *[col(c) for c in med_df.columns if c != "SNOMED_CODE"],
            col("SNOMED_CODE"),
            col("SNOMED_SOURCE")
        )
    )
    
    # Add source to existing rows and combine
    final_df = (
        rows_with_codes
        .withColumn("SNOMED_SOURCE", lit("ORIGINAL"))
        .unionByName(mapped_df)
    )
    
    return final_df


def add_omop_mappings(med_df):
    """
    Adds OMOP concept mappings to the medication administration records.
    Includes direct code mappings and name-based matching with fallbacks.
    """
    print("Inside add_omop_mappings function...")
    
    concepts = spark.table("3_lookup.omop.concept")
    
    # Debug: Check Multum concept codes format
    print("Checking Multum concept codes in OMOP...")
    multum_sample = concepts.filter(col("vocabulary_id") == "Multum").select("concept_code").distinct().limit(10).collect()
    print(f"Sample Multum concept codes: {[row.concept_code for row in multum_sample]}")
    
    # Create separate concept DataFrames for each vocabulary
    # For Multum, keep as string - no casting
    multum_concepts = (
        concepts.filter(col("vocabulary_id") == "Multum")
        .withColumn("rank", row_number().over(
            Window.partitionBy("concept_code")
            .orderBy(
                when(col("standard_concept") == "S", 1)
                .when(col("standard_concept").isNull(), 2)
                .otherwise(3)
            )
        ))
        .filter(col("rank") == 1)
        .select(
            col("concept_code").alias("multum_code"),  # Keep as string
            col("concept_id").alias("multum_concept_id"),
            col("concept_name").alias("multum_concept_name"),
            col("standard_concept").alias("multum_standard_concept")
        )
    )

    rxnorm_concepts = (
        concepts.filter(col("vocabulary_id") == "RxNorm")
        .withColumn("rank", row_number().over(
            Window.partitionBy("concept_code")
            .orderBy(
                when(col("standard_concept") == "S", 1)
                .when(col("standard_concept").isNull(), 2)
                .otherwise(3)
            )
        ))
        .filter(col("rank") == 1)
        .select(
            col("concept_code").alias("rxnorm_code"),
            col("concept_id").alias("rxnorm_concept_id"),
            col("concept_name").alias("rxnorm_concept_name"),
            col("standard_concept").alias("rxnorm_standard_concept")
        )
    )

    rxnorm_ext_concepts = (
        concepts.filter(col("vocabulary_id") == "RxNorm Extension")
        .withColumn("rank", row_number().over(
            Window.partitionBy("concept_code")
            .orderBy(
                when(col("standard_concept") == "S", 1)
                .when(col("standard_concept").isNull(), 2)
                .otherwise(3)
            )
        ))
        .filter(col("rank") == 1)
        .select(
            col("concept_code").alias("rxnorm_ext_code"),
            col("concept_id").alias("rxnorm_ext_concept_id"),
            col("concept_name").alias("rxnorm_ext_concept_name"),
            col("standard_concept").alias("rxnorm_ext_standard_concept")
        )
    )

    snomed_concepts = (
        concepts.filter(col("vocabulary_id") == "SNOMED")
        .withColumn("rank", row_number().over(
            Window.partitionBy("concept_code")
            .orderBy(
                when(col("standard_concept") == "S", 1)
                .when(col("standard_concept").isNull(), 2)
                .otherwise(3)
            )
        ))
        .filter(col("rank") == 1)
        .select(
            col("concept_code").alias("snomed_concept_code"),
            col("concept_id").alias("snomed_concept_id"),
            col("concept_name").alias("snomed_concept_name"),
            col("standard_concept").alias("snomed_standard_concept")
        )
    )
    
    # Modify name matching concepts to include ranking
    drug_name_concepts_exact = (
        concepts.filter(
            ((col("domain_id") == "Drug") | (col("domain_id") == "Ingredient")) &
            (col("invalid_reason").isNull())
        )
        .withColumn("exact_name_lower", lower(col("concept_name")))
        .withColumn("rank", row_number().over(
            Window.partitionBy("exact_name_lower")
            .orderBy(
                when(col("standard_concept") == "S", 1)
                .when(col("standard_concept").isNull(), 2)
                .otherwise(3),
                col("concept_id") 
            )
        ))
        .filter(col("rank") == 1)
        .select(
            col("concept_id").alias("exact_concept_id"),
            col("concept_name").alias("exact_concept_name"),
            col("vocabulary_id").alias("exact_vocabulary"),
            col("standard_concept").alias("exact_standard_concept"),
            col("exact_name_lower")
        )
    )

    drug_name_concepts_simplified = (
        concepts.filter(
            ((col("domain_id") == "Drug") | (col("domain_id") == "Ingredient")) &
            (col("invalid_reason").isNull())
        )
        .withColumn("simplified_name_lower", 
                    extract_drug_name_med_admin(lower(col("concept_name"))))
        .withColumn("rank", row_number().over(
            Window.partitionBy("simplified_name_lower")
            .orderBy(
                when(col("standard_concept") == "S", 1)
                .when(col("standard_concept").isNull(), 2)
                .otherwise(3),
                col("concept_id")
            )
        ))
        .filter(col("rank") == 1)
        .select(
            col("concept_id").alias("simplified_concept_id"),
            col("concept_name").alias("simplified_concept_name"),
            col("vocabulary_id").alias("simplified_vocabulary"),
            col("standard_concept").alias("simplified_standard_concept"),
            col("simplified_name_lower")
        )
    )
    
    print("Starting joins for OMOP mappings...")
    
    # Join with med_df in sequence, using left joins
    # NO CASTING - just string comparison for MULTUM
    mapped_df = (
        med_df
        # Join with Multum concepts using string comparison
        .join(
            multum_concepts,
            col("MULTUM") == col("multum_code"),  # Direct string comparison
            "left"
        )
        # Join with RxNorm concepts
        .join(
            rxnorm_concepts,
            col("RXNORM_CUI") == col("rxnorm_code"),
            "left"
        )
        # Join with RxNorm Extension concepts
        .join(
            rxnorm_ext_concepts,
            col("RXNORM_CUI") == col("rxnorm_ext_code"),
            "left"
        )
        # Join with SNOMED concepts
        .join(
            snomed_concepts,
            col("SNOMED_CODE") == col("snomed_concept_code"),
            "left"
        )
        # Add derived columns for final OMOP mappings before name matching
        .withColumns({
            "OMOP_CONCEPT_ID": coalesce(
                col("multum_concept_id"),
                col("rxnorm_concept_id"),
                col("rxnorm_ext_concept_id"),
                col("snomed_concept_id")
            ),
            "OMOP_CONCEPT_NAME": coalesce(
                col("multum_concept_name"),
                col("rxnorm_concept_name"),
                col("rxnorm_ext_concept_name"),
                col("snomed_concept_name")
            ),
            "OMOP_STANDARD_CONCEPT": coalesce(
                col("multum_standard_concept"),
                col("rxnorm_standard_concept"),
                col("rxnorm_ext_standard_concept"),
                col("snomed_standard_concept")
            ),
            "OMOP_TYPE": when(col("multum_concept_id").isNotNull(), "MULTUM")
                .when(col("rxnorm_concept_id").isNotNull(), "RXNORM")
                .when(col("rxnorm_ext_concept_id").isNotNull(), "RXNORMEXT")
                .when(col("snomed_concept_id").isNotNull(), "SNOMED")
        })
        # For rows without mappings, try name matching
        .withColumn(
            "order_term_lower",
            when(col("OMOP_CONCEPT_ID").isNull(),
                lower(col("ORDER_MNEMONIC"))
            )
        )
        # Add simplified drug name for fallback matching
        .withColumn(
            "order_term_simplified",
            when(
                col("OMOP_CONCEPT_ID").isNull(),
                extract_drug_name_med_admin(col("order_term_lower"))
            )
        )
        # Try exact name matching first
        .join(
            drug_name_concepts_exact,
            (col("order_term_lower") == col("exact_name_lower")) &
            col("OMOP_CONCEPT_ID").isNull(),
            "left"
        )
        # Try simplified name matching for remaining nulls
        .join(
            drug_name_concepts_simplified,
            (col("order_term_simplified") == col("simplified_name_lower")) &
            col("OMOP_CONCEPT_ID").isNull() &
            col("exact_concept_id").isNull(),
            "left"
        )
        # Update OMOP columns with name matches
        .withColumns({
            "OMOP_CONCEPT_ID": coalesce(
                col("OMOP_CONCEPT_ID"),
                col("exact_concept_id"),
                col("simplified_concept_id")
            ),
            "OMOP_CONCEPT_NAME": coalesce(
                col("OMOP_CONCEPT_NAME"),
                col("exact_concept_name"),
                col("simplified_concept_name")
            ),
            "OMOP_STANDARD_CONCEPT": coalesce(
                col("OMOP_STANDARD_CONCEPT"),
                col("exact_standard_concept"),
                col("simplified_standard_concept")
            ),
            "OMOP_TYPE": coalesce(
                col("OMOP_TYPE"),
                when(col("exact_concept_id").isNotNull(), 
                     concat(lit("NAME_MATCH_"), col("exact_vocabulary"))),
                when(col("simplified_concept_id").isNotNull(),
                     concat(lit("SIMPLIFIED_MATCH_"), col("simplified_vocabulary")))
            )
        })
    ).distinct()
    
    # Drop intermediate columns
    columns_to_drop = [
        "multum_code", "multum_concept_id", "multum_concept_name", "multum_standard_concept",
        "rxnorm_code", "rxnorm_concept_id", "rxnorm_concept_name", "rxnorm_standard_concept",
        "rxnorm_ext_code", "rxnorm_ext_concept_id", "rxnorm_ext_concept_name", "rxnorm_ext_standard_concept",
        "snomed_concept_code", "snomed_concept_id", "snomed_concept_name", "snomed_standard_concept",
        "order_term_lower", "order_term_simplified", 
        "exact_concept_id", "exact_concept_name", "exact_vocabulary", "exact_standard_concept", "exact_name_lower",
        "simplified_concept_id", "simplified_concept_name", "simplified_vocabulary", "simplified_standard_concept", "simplified_name_lower"
    ]
    
    print("OMOP mapping joins completed, dropping intermediate columns...")
    
    return mapped_df.drop(*columns_to_drop)


def backfill_snomed_from_omop(df):
    """
    Attempts to find SNOMED codes for records that have OMOP concepts but no SNOMED codes
    by looking up mappings from the OMOP concept to SNOMED.
    """
    print("Inside backfill_snomed_from_omop function...")
    
    # Get OMOP concept relationships for SNOMED
    concept_relationships = (
        spark.table("3_lookup.omop.concept_relationship")
        .filter(
            (col("relationship_id").isin(["Maps to", "Maps from"])) &
            (col("invalid_reason").isNull())
        )
        .select(
            col("concept_id_1"),
            col("concept_id_2"),
            col("relationship_id")
        )
    )
    
    # Get SNOMED concepts from OMOP
    snomed_concepts = (
        spark.table("3_lookup.omop.concept")
        .filter(
            (col("vocabulary_id") == "SNOMED") &
            (col("invalid_reason").isNull())
        )
        .select(
            col("concept_id").alias("snomed_concept_id"),
            col("concept_code").alias("mapped_snomed_code"),
            col("concept_name").alias("mapped_snomed_name")
        )
    )
    
    # Find records needing SNOMED backfill
    needs_backfill = df.filter(
        (col("SNOMED_CODE").isNull()) & 
        (col("OMOP_CONCEPT_ID").isNotNull())
    )
    
    has_snomed = df.filter(
        ~((col("SNOMED_CODE").isNull()) & 
          (col("OMOP_CONCEPT_ID").isNotNull()))
    )
    
    print(f"Checking if backfill is needed...")
    try:
        needs_backfill_count = needs_backfill.count()
        print(f"Records needing backfill: {needs_backfill_count}")
    except Exception as e:
        print(f"Error during needs_backfill count: {str(e)}")
        print("The error is likely in the filter/count operation")
        # Try to identify problematic data
        print("Attempting to show schema...")
        df.printSchema()
        raise
    
    if needs_backfill_count > 0:
        print("Performing backfill joins...")
        # Find SNOMED mappings for the OMOP concepts
        backfilled = (
            needs_backfill
            # Join to get relationships
            .join(
                concept_relationships,
                (col("OMOP_CONCEPT_ID") == col("concept_id_1")),
                "left"
            )
            # Join to get SNOMED details
            .join(
                snomed_concepts,
                (col("concept_id_2") == col("snomed_concept_id")),
                "left"
            )
            # Take the first SNOMED mapping per record if multiple exist
            .withColumn(
                "rn",
                row_number().over(
                    Window.partitionBy("EVENT_ID")
                    .orderBy(col("snomed_concept_id"))
                )
            )
            .filter(col("rn") == 1)
            # Update SNOMED columns
            .withColumn(
                "SNOMED_CODE",
                when(col("mapped_snomed_code").isNotNull(), col("mapped_snomed_code"))
                .otherwise(col("SNOMED_CODE"))
            )
            .withColumn(
                "SNOMED_STR",
                when(col("mapped_snomed_name").isNotNull(), col("mapped_snomed_name"))
                .otherwise(col("SNOMED_STR"))
            )
            .withColumn(
                "SNOMED_SOURCE",
                when(col("mapped_snomed_code").isNotNull(), lit("OMOP"))
                .otherwise(col("SNOMED_SOURCE"))
            )
            # Drop temporary columns
            .drop(
                "concept_id_1", "concept_id_2", "relationship_id", 
                "snomed_concept_id", "mapped_snomed_code", "mapped_snomed_name", 
                "rn"
            )
        )
        
        # Combine backfilled records with original records that had SNOMED codes
        return has_snomed.unionByName(backfilled)
    
    return df



def process_med_admin_incremental():
    """Main function to process incremental medication administration updates with all mappings"""
    try:
        print(f"Starting medication administration incremental processing at {datetime.now()}")
        
        base_df = create_base_medication_administrations_incr()
        
        if base_df.count() > 0:
            print(f"Processing {base_df.count()} updated records")
            
            # Initialize reference tables
            refs = MedAdminReferenceTables()
            
            # Debug: Check MULTUM column values
            print("Checking MULTUM column values...")
            multum_values = base_df.select("MULTUM").distinct().limit(20).collect()
            print(f"Sample MULTUM values: {[row.MULTUM for row in multum_values]}")
            
            # Add SNOMED codes
            print("Starting SNOMED code mappings...")
            with_snomed = augment_snomed_codes(base_df, refs)
            print("Added SNOMED code mappings")
            
            # Add OMOP mappings
            print("Starting OMOP concept mappings...")
            with_omop = add_omop_mappings(with_snomed)
            print("Added OMOP concept mappings")
            
            # Debug: Check if we can access the dataframe
            print("Checking if OMOP mapping completed successfully...")
            try:
                omop_count = with_omop.filter(col("OMOP_CONCEPT_ID").isNotNull()).count()
                print(f"Records with OMOP mappings: {omop_count}")
            except Exception as e:
                print(f"Error during OMOP count check: {str(e)}")
                print("Error might be in OMOP mapping function")
                raise

            # Backfill missing SNOMED codes from OMOP mappings
            print("Starting backfill of missing SNOMED codes...")
            with_backfill = backfill_snomed_from_omop(with_omop)
            print("Backfilled missing SNOMED codes from OMOP mappings")

            # Deduplicate records prioritizing status 25, Authorized
            print("Starting deduplication...")
            final_df = (with_backfill
                       .withColumn("priority",
                           when(col("RESULT_STATUS_CD") == 25, 1)
                           .when(col("RESULT_STATUS_CD") == 35, 2)  # Modified
                           .when(col("RESULT_STATUS_CD") == 36, 3)  # Not Done
                           .when(col("RESULT_STATUS_CD") == 31, 4)  # In Error
                           .otherwise(5))
                       .withColumn("row_num",
                           row_number().over(
                               Window.partitionBy("EVENT_ID")
                               .orderBy(
                                   col("priority"),
                                   col("ADC_UPDT").desc()
                               )
                           ))
                       .filter(col("row_num") == 1)
                       .drop("priority", "row_num"))
            
            print("Deduplication complete, updating table...")
            update_table(final_df.distinct(), "4_prod.bronze.map_med_admin", "EVENT_ID")
            update_metadata("4_prod.bronze.map_med_admin",schema_map_med_admin,map_med_admin_comment)
            print("Successfully updated medication administration mapping table")
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing medication administration updates: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

process_med_admin_incremental()

# COMMAND ----------

map_procedure_comment = "The table contains data related to medical procedures performed during patient encounters. It includes information such as procedure IDs, timestamps, and associated clinical services. This data can be used for analyzing procedure trends, understanding resource utilization, and tracking patient care processes. Additionally, it captures details about the source of the data and provides descriptions for various codes, which can aid in data interpretation and reporting."

schema_map_procedure = StructType([
    StructField("PROCEDURE_ID", LongType(), True, metadata={
        "comment": "Procedure id is the primary unique identification number of the procedure table.  It is an internal system assigned sequence number."
    }),
    StructField("ACTIVE_STATUS_PRSNL_ID", LongType(), True, metadata={
        "comment": "The person who caused the active_status_cd to be set or change."
    }),
    StructField("ENCNTR_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the encounter table. It is an internal system assigned number."
    }),
    StructField("PERSON_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the person table. It is an internal system assigned number."
    }),
    StructField("NOMENCLATURE_ID", DoubleType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the nomenclature table. It is an internal system assigned number."
    }),
    StructField("ENCNTR_SLICE_ID", LongType(), True, metadata={
        "comment": "Encounter slice identifier."
    }),
    StructField("CONTRIBUTOR_SYSTEM_CD", IntegerType(), True, metadata={
        "comment": "Contributor system identifies the source feed of data from which a row was populated. This is mainly used to determine how to update a set of data that may have originated from more than one source feed."
    }),
    StructField("contributor_system_desc", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("CLINICAL_SERVICE_CD", DoubleType(), True, metadata={
        "comment": "The code associates a procedure to a clinical service."
    }),
    StructField("clinical_service_desc", StringType(), True, metadata={
        "comment": "Description for the clinical service code."
    }),
    StructField("active_status_desc", StringType(), True, metadata={
        "comment": "Description for the active status code."
    }),
    StructField("PROC_DT_TM", TimestampType(), True, metadata={
        "comment": "Date and time when the procedure was performed."
    }),
    StructField("PROC_MINUTES", DoubleType(), True, metadata={
        "comment": "The amount of time in minutes the procedure took to complete."
    }),
    StructField("PROCEDURE_NOTE", StringType(), True, metadata={
        "comment": "Free-text note for the procedure."
    }),
    StructField("SOURCE_IDENTIFIER", StringType(), True, metadata={
        "comment": "The code, or key, from the source vocabulary that contributed the string to the nomenclature."
    }),
    StructField("SOURCE_STRING", StringType(), True, metadata={
        "comment": "Variable length string that may include alphanumeric characters and punctuation."
    }),
    StructField("SOURCE_VOCABULARY_CD", DoubleType(), True, metadata={
        "comment": "The external vocabulary or lexicon that contributed the string, e.g. ICD9, SNOMED, etc."
    }),
    StructField("source_vocabulary_desc", StringType(), True, metadata={
        "comment": "Description for the source vocabulary code."
    }),
    StructField("VOCAB_AXIS_CD", DoubleType(), True, metadata={
        "comment": "Vocabulary AXIS codes related to SNOMEDColumn."
    }),
    StructField("vocab_axis_desc", StringType(), True, metadata={
        "comment": "Description for the vocabulary AXIS code."
    }),
    StructField("CONCEPT_CKI", StringType(), True, metadata={
        "comment": "Concept CKI is the functional Concept Identifier; it is the codified means within Millennium to identify key medical concepts to support information processing, clinical decision support, executable knowledge and knowledge presentation. Composed of a source and an identifier."
        }),
    StructField("OMOP_CONCEPT_ID", LongType(), True, metadata={
        "comment": "A unique identifier for each Concept across all domains."
    }),
    StructField("OMOP_CONCEPT_NAME", StringType(), True, metadata={
        "comment": "An unambiguous, meaningful and descriptive name for the Concept."
    }),
    StructField("OMOP_STANDARD_CONCEPT", StringType(), True, metadata={
        "comment": "This flag determines where a Concept is a Standard Concept, i.e. is used in the data, a Classification Concept, or a non-standard Source Concept. The allowables values are S (Standard Concept) and C (Classification Concept), otherwise the content is NULL."
    }),
    StructField("OMOP_MATCH_NUMBER", LongType(), True, metadata={
         "comment": "The number of OMOP concepts matched for each NOMENCLATURE_ID." 
    }),
    StructField("OMOP_CONCEPT_DOMAIN", StringType(), True, metadata={
         "comment": "A unique identifier for each domain."
    }),
    StructField("SNOMED_CODE", LongType(), True, metadata={"comment": ""}),
    StructField("SNOMED_TYPE", StringType(), True, metadata={
        "comment": "The method or source of the SNOMED code mapping for each nomenclature entry."
    }),
    StructField("SNOMED_MATCH_NUMBER", LongType(), True, metadata={
        "comment": "The number of matches found for each NOMENCLATURE_ID in the context of SNOMED codes."
    }),
    StructField("SNOMED_TERM", StringType(), True, metadata={
        "comment": "The term associated with a SNOMED code that provides additional meaning and context to the code."
    }),
    StructField("OPCS4_CODE", StringType(), True, metadata={"comment": ""}),
    StructField("OPCS4_TYPE", StringType(), True, metadata={
        "comment": "The method or source of the OPCS4 code mapping for each nomenclature entry." 
    }),
    StructField("ICD10_MATCH_NUMBER", LongType(), True, metadata={
         "comment": "The number of matches found for each NOMENCLATURE_ID in the context of ICD10 codes."
    }),
    StructField("OPCS4_TERM", StringType(), True, metadata={
        "The term associated with the OPCS4 code that provides additional meaning and context to the code."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Timestamp for the last update."
    })
])

def cast_procedure_ids_to_long(df):
    """
    Helper function to cast procedure ID columns to LongType
    
    Args:
        df: Input DataFrame with ID columns
        
    Returns:
        DataFrame: DataFrame with ID columns cast to LongType
    """
    id_columns = [
        "PROCEDURE_ID", "ACTIVE_STATUS_PRSNL_ID", "ENCNTR_ID",
        "PERSON_ID", "NOMENCLATURE_ID", "ENCNTR_SLICE_ID"
    ]
    
    for col_name in id_columns:
        df = df.withColumn(col_name, col(col_name).cast(LongType()))
    
    return df

def create_procedure_code_lookup(code_values, alias_name, desc_alias):
    """
    Helper function to create standardized code value lookups for procedures
    
    Args:
        code_values: Base code values DataFrame
        alias_name: Alias for the lookup table
        desc_alias: Alias for the description column
        
    Returns:
        DataFrame: Lookup table with standardized columns
    """
    return code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(desc_alias)
    ).alias(alias_name)

def process_procedure_incremental():
    """
    Creates an incremental procedure mapping table processing only new/modified records.
    Handles the end-to-end process of updating the procedure mapping table.
    """
    try:
        print(f"Starting procedure incremental processing at {datetime.now()}")
        

        max_adc_updt = get_max_timestamp("4_prod.bronze.map_procedure")
        
        # Get base tables
        procedures = spark.table("4_prod.raw.mill_procedure").filter(
            col("ADC_UPDT") > max_adc_updt
        ).alias("proc")
        
        if procedures.count() > 0:
            print(f"Processing {procedures.count()} updated records")
            
            encounters = spark.table("4_prod.raw.mill_encounter")
            code_values = spark.table("3_lookup.mill.mill_code_value")
            nomenclature = spark.table("4_prod.bronze.nomenclature").alias("nom")
            
            # Create code value lookups
            active_status_lookup = create_procedure_code_lookup(
                code_values, "active_status", "active_status_desc")
            contributor_system_lookup = create_procedure_code_lookup(
                code_values, "contrib", "contributor_system_desc")
            clinical_service_lookup = create_procedure_code_lookup(
                code_values, "clin", "clinical_service_desc")
            source_vocab_lookup = create_procedure_code_lookup(
                code_values, "vocab", "source_vocabulary_desc")
            vocab_axis_lookup = create_procedure_code_lookup(
                code_values, "axis", "vocab_axis_desc")
            
            # Process procedures
            processed_procedures = (
                procedures
                # Apply filters
                .filter(
                    (col("ACTIVE_IND") == 1) &
                    (col("PROC_DT_TM").isNotNull())
                )
                # Join with encounters to get person_id
                .join(
                    encounters.select("ENCNTR_ID", "PERSON_ID"),
                    "ENCNTR_ID"
                )
                # Join with nomenclature
                .join(
                    nomenclature,
                    "NOMENCLATURE_ID",
                    "left"
                )
                # Join with all code value lookups
                .join(
                    active_status_lookup,
                    col("proc.ACTIVE_STATUS_CD") == col("active_status.CODE_VALUE"),
                    "left"
                )
                .join(
                    contributor_system_lookup,
                    col("proc.CONTRIBUTOR_SYSTEM_CD") == col("contrib.CODE_VALUE"),
                    "left"
                )
                .join(
                    clinical_service_lookup,
                    col("CLINICAL_SERVICE_CD") == col("clin.CODE_VALUE"),
                    "left"
                )
                .join(
                    source_vocab_lookup,
                    col("SOURCE_VOCABULARY_CD") == col("vocab.CODE_VALUE"),
                    "left"
                )
                .join(
                    vocab_axis_lookup,
                    col("VOCAB_AXIS_CD") == col("axis.CODE_VALUE"),
                    "left"
                )
                # Handle PROC_MINUTES
                .withColumn(
                    "PROC_MINUTES",
                    when(col("PROC_MINUTES") == 0, None).otherwise(col("PROC_MINUTES"))
                )
            )
            
            # Cast IDs to LongType
            processed_procedures = cast_procedure_ids_to_long(processed_procedures)
            
            # Select final columns
            final_df = processed_procedures.select(
                # IDs
                "PROCEDURE_ID",
                "ACTIVE_STATUS_PRSNL_ID",
                "ENCNTR_ID",
                "PERSON_ID",
                "NOMENCLATURE_ID",
                "ENCNTR_SLICE_ID",
                
                # Code columns with descriptions
                "CONTRIBUTOR_SYSTEM_CD",
                "contributor_system_desc",
                "CLINICAL_SERVICE_CD",
                "clinical_service_desc",
                "active_status_desc",
                
                # Timing columns
                "PROC_DT_TM",
                "PROC_MINUTES",
                
                # Notes
                "PROCEDURE_NOTE",
                
                # Nomenclature details
                "SOURCE_IDENTIFIER",
                "SOURCE_STRING",
                "SOURCE_VOCABULARY_CD",
                "source_vocabulary_desc",
                "VOCAB_AXIS_CD",
                "vocab_axis_desc",
                "CONCEPT_CKI",
                "OMOP_CONCEPT_ID",
                "OMOP_CONCEPT_NAME",
                col("IS_STANDARD_OMOP_CONCEPT").alias("OMOP_STANDARD_CONCEPT"),
                col("NUMBER_OF_OMOP_MATCHES").alias("OMOP_MATCH_NUMBER"),
                col("CONCEPT_DOMAIN").alias("OMOP_CONCEPT_DOMAIN"),
                "SNOMED_CODE",
                "SNOMED_TYPE",
                col("SNOMED_MATCH_COUNT").alias("SNOMED_MATCH_NUMBER"),
                "SNOMED_TERM",
                "OPCS4_CODE",
                "OPCS4_TYPE",
                col("OPCS4_MATCH_COUNT").alias("ICD10_MATCH_NUMBER"),
                "OPCS4_TERM",
                col("proc.ADC_UPDT").alias("ADC_UPDT")
            )
            
            # Update target table
            update_table(final_df, "4_prod.bronze.map_procedure", "PROCEDURE_ID")
            update_metadata("8_dev.bronze.map_procedure",schema_map_procedure,map_procedure_comment)
            print("Successfully updated procedure mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing procedure updates: {str(e)}")
        raise


process_procedure_incremental()

# COMMAND ----------

map_death_comment = "The table contains data related to deceased individuals, including timestamps for various events such as the last encounter and the date of death. It also includes information on the source and method of death identification. This data can be used for analyzing mortality trends, understanding patient histories, and improving record-keeping processes."

schema_map_death = StructType([
    StructField("PERSON_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the person table. It is an internal system assigned number."
    }),
    StructField("DECEASED_DT_TM", TimestampType(), True, metadata={
        "comment": "Date and time of death."
    }),
    StructField("LAST_ENCNTR_DT_TM", TimestampType(), True, metadata={
        "comment": "It represents the timestamp of the last encounter associated with a deceased individual."
    }),
    StructField("LAST_CE_DT_TM", TimestampType(), True, metadata={
        "comment": "It represents the most recent update date/time that tracks when clinically significant updates are made to the Clinical Event and should only be used to check for updates."
    }),
    StructField("CALC_DEATH_DATE", TimestampType(), True, metadata={
        "comment": "Calculated death date."
    }),
    StructField("DECEASED_SOURCE_CD", IntegerType(), True, metadata={
        "comment": "It defines the particular source that gave deceased information concerning a person. For example, from a Formal (Death Certificate) or Informal (no Death Certificate) source."
    }),
    StructField("DECEASED_SOURCE_DESC", StringType(), True, metadata={
        "comment": "Description of the code."
    }),
    StructField("DECEASED_ID_METHOD_CD", IntegerType(), True, metadata={
        "comment": "It stores code values defining the specific way a patient was confirmed as being deceased. Possible values  include Death Certificate, Physician Reported, etc. The code values are closely tied, workflow-wise, to the Deceased_Source_Cd which records if a patient was identified as being deceased from a Formal (Death Certificate) or Informal (no Death Certificate) source and the Deceased_Notify_Source_Cd which records who or what provided the information regarding the patient's deceased status."
    }),
    StructField("DECEASED_METHOD_DESC", StringType(), True, metadata={
        "comment": "Description of the code."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Timestamp of last update."
    })
])

def get_last_clinical_events_for_death():
    """
    Helper function to get last clinical event dates for each person.
    
    Returns:
        DataFrame: Last clinical event dates by person
    """
    return (
        spark.table("4_prod.raw.mill_clinical_event")
        .groupBy("PERSON_ID")
        .agg(F.max("CLINSIG_UPDT_DT_TM").alias("LAST_CE_DT_TM"))
    )

def create_death_code_lookups(code_values):
    """
    Helper function to create source and method code lookups for death records.
    
    Args:
        code_values: DataFrame containing code values
    
    Returns:
        tuple: DataFrames for source and method lookups
    """
    source_lookup = code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias("deceased_source_desc")
    ).alias("source")
    
    method_lookup = code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias("deceased_method_desc")
    ).alias("method")
    
    return source_lookup, method_lookup

def process_death_incremental():
    """
    Main function to process incremental death record updates.
    Handles the end-to-end process of updating the death mapping table.
    """
    try:
        print(f"Starting death record incremental processing at {datetime.now()}")
        

        max_adc_updt = get_max_timestamp("4_prod.bronze.map_death")
        
        # Get base person data with filtering
        base_persons = (
            spark.table("4_prod.raw.mill_person")
            .filter(
                (col("DECEASED_CD") == 3768549) &
                (col("ADC_UPDT") > max_adc_updt)
            )
            .alias("person")
        )
        
        if base_persons.count() > 0:
            print(f"Processing {base_persons.count()} updated records")
            
            # Get reference data
            code_values = spark.table("3_lookup.mill.mill_code_value")
            last_ce_dates = get_last_clinical_events_for_death()
            source_lookup, method_lookup = create_death_code_lookups(code_values)
            
            # Process death records
            processed_deaths = (
                base_persons
                # Join with code value lookups
                .join(
                    source_lookup,
                    col("DECEASED_SOURCE_CD") == col("source.CODE_VALUE"),
                    "left"
                )
                .join(
                    method_lookup,
                    col("DECEASED_ID_METHOD_CD") == col("method.CODE_VALUE"),
                    "left"
                )
                # Join with last clinical event dates
                .join(
                    last_ce_dates,
                    "PERSON_ID",
                    "left"
                )
                # Add calculated death date
                .withColumn(
                    "CALC_DEATH_DATE",
                    F.coalesce(
                        col("DECEASED_DT_TM"),
                        col("LAST_ENCNTR_DT_TM"),
                        col("LAST_CE_DT_TM")
                    )
                )
                # Select final columns with proper casting
                .select(
                    col("PERSON_ID").cast("long"),
                    col("DECEASED_DT_TM").cast("timestamp"),
                    col("LAST_ENCNTR_DT_TM").cast("timestamp"),
                    col("LAST_CE_DT_TM").cast("timestamp"),
                    col("CALC_DEATH_DATE").cast("timestamp"),
                    col("DECEASED_SOURCE_CD").cast("integer"),
                    F.when(
                        col("DECEASED_SOURCE_CD").isNull() | (col("DECEASED_SOURCE_CD") == 0), 
                        None
                    ).otherwise(
                        coalesce("deceased_source_desc")
                    ).cast("string").alias("DECEASED_SOURCE_DESC"),
                    col("DECEASED_ID_METHOD_CD").cast("integer"),
                    F.when(
                        col("DECEASED_ID_METHOD_CD").isNull() | (col("DECEASED_ID_METHOD_CD") == 0), 
                        None
                    ).otherwise(
                        coalesce("deceased_method_desc")
                    ).cast("string").alias("DECEASED_METHOD_DESC"),
                    col("ADC_UPDT")
                )
            )
            
            # Update target table
            update_table(processed_deaths, "4_prod.bronze.map_death", "PERSON_ID")
            update_metadata("4_prod.bronze.map_death",schema_map_death,map_death_comment)
            print("Successfully updated death mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing death record updates: {str(e)}")
        raise


process_death_incremental()

# COMMAND ----------

map_numeric_events_comment = "The table contains data related to various events associated with encounters and orders. It includes identifiers for events, encounters, and persons, as well as details about the event type, results, and contributing systems. This data can be used for tracking event occurrences, analyzing performance metrics, and understanding the relationships between different entities in the system."


schema_map_numeric_events = StructType([
    StructField("EVENT_ID", LongType(), True, metadata={
        "comment": "The unique primary identifier of the Event Table."
    }),
    StructField("ENCNTR_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the encounter table. It is an internal system assigned number."
    }),
    StructField("PERSON_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the person table. It is an internal system assigned number."
    }),
    StructField("ORDER_ID", LongType(), True, metadata={
        "comment": "The unique primary identifier of Order Table."
    }),
    StructField("EVENT_CLASS_CD", IntegerType(), True, metadata={
        "comment": "Coded value which specifies how the event is stored in and retrieved from the event table's sub-tables. For example, Event_Class_CDs identify events as numeric results, textual results, calculations, medications, etc."
    }),
    StructField("PERFORMED_PRSNL_ID", LongType(), True, metadata={
        "comment": "Personnel id of provider who performed this result."
    }),
    StructField("NUMERIC_RESULT", FloatType(), True, metadata={
        "comment": "The numerical value of the event result."
    }),
    StructField("UNIT_OF_MEASURE_CD", IntegerType(), True, metadata={
        "comment": "Unit of measurement for result."
    }),
    StructField("UNIT_OF_MEASURE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("EVENT_TITLE_TEXT", StringType(), True, metadata={
        "comment": "The title for document results."
    }),
    StructField("EVENT_CD", IntegerType(), True, metadata={
        "comment": "It is the code that identifies the most basic unit of the storage, i.e. RBC, discharge summary, image."
    }),
    StructField("EVENT_CD_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("CATALOG_CD", IntegerType(), True, metadata={
        "comment": "Foreign key to the order_catalog table. Catalog_cd does not exist in the code_value table and does not have a code set."
    }),
    StructField("CATALOG_DISPLAY", StringType(), True, metadata={
        "comment": "The description of the Orderable."
    }),
    StructField("CATALOG_TYPE_CD", IntegerType(), True, metadata={
        "comment": "Used to store the internal code for the catalog type. Used as a filtering mechanism for rows on theorder catalog table."
    }),
    StructField("CATALOG_TYPE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("CONTRIBUTOR_SYSTEM_CD", IntegerType(), True, metadata={
        "comment": "Contributor system identifies the source feed of data from which a row was populated.  This is mainly used to determine how to update a set of data that may have originated from more than one source feed."
    }),
    StructField("CONTRIBUTOR_SYSTEM_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("REFERENCE_NBR", StringType(), True, metadata={
        "comment": "The combination of the reference nbr and the contributor system code provides a unique identifier to the origin of the data."
    }),
    StructField("PARENT_EVENT_ID", LongType(), True, metadata={
        "comment": "Provides a mechanism for logical grouping of events.  i.e. supergroup and group tests.  Same as event_id if current row is the highest level parent."
    }),
    StructField("NORMALCY_CD", IntegerType(), True, metadata={
        "comment": "States whether the result is normal.  This can be used to determine whether to display the event tag in different color on the flowsheet. For group results, this represents an ""overall"" normalcy. i.e. Is any result in the group abnormal?  Also allows different purge criteria to be applied based on result."
    }),
    StructField("NORMALCY_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("ENTRY_MODE_CD", IntegerType(), True, metadata={
        "comment": "Used to identify the method in which a result was entered."
    }),
    StructField("ENTRY_MODE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("NORMAL_LOW", FloatType(), True, metadata={
        "comment": "Normal low value"
    }),
    StructField("NORMAL_HIGH", FloatType(), True, metadata={
        "comment": "Normal high value"
    }),
    StructField("PERFORMED_DT_TM", TimestampType(), True, metadata={
        "comment": "Date this result was performed (or authored)."
    }),
    StructField("CLINSIG_UPDT_DT_TM", TimestampType(), True, metadata={
        "comment": "Represents the update date/time that tracks when clinically significant updates are made to the Clinical Event and should only be used to check for updates. This field is used to notify audiences when a clinically significant update is made to an existing clinical event, such as when XR Clinical Reporting re-prints a lab result due to an update of the result value or when a result is resent to a provider's Message Center with the result update. This date should NOT be displayed as the clinically."
    }),
    StructField("PARENT_EVENT_TITLE_TEXT", StringType(), True, metadata={
        "comment": "The title associated with the parent event."
    }),
    StructField("PARENT_EVENT_CD", IntegerType(), True, metadata={
        "comment": "The code value of the parent event."
    }),
    StructField("PARENT_EVENT_CD_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PARENT_CATALOG_CD", IntegerType(), True, metadata={
        "comment": "The internal code for the order catalog item of the parent event."
    }),
    StructField("PARENT_CATALOG_DISPLAY", StringType(), True, metadata={
        "comment": "The description of the Orderable."
    }),
    StructField("PARENT_CATALOG_TYPE_CD", IntegerType(), True, metadata={
        "comment": "The internal code for the catalog type of the parent event."
    }),
    StructField("PARENT_CATALOG_TYPE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PARENT_REFERENCE_NBR", StringType(), True, metadata={
        "comment": "The combination of the reference nbr and the contributor system code provides a unique identifier to the origin of the data."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Timestamp of last update."
    }),
    StructField("OMOP_MANUAL_TABLE", StringType(), True, metadata={
        "comment": "The name of the OMOP Common Data Model table."
    }),
    StructField("OMOP_MANUAL_COLUMN", StringType(), True, metadata={
        "comment": "The field of the OMOP Common Data Model table."
    }),
    StructField("OMOP_MANUAL_CONCEPT", StringType(), True, metadata={
        "comment": "The concept_id of the OMOP Common Data Model table."
    }),
    StructField("OMOP_MANUAL_UNITS", StringType(), True, metadata={
        "comment": "The OMOP concept ID for the unit of measurement."
    }),
    StructField("OMOP_MANUAL_CONCEPT_NAME", StringType(), True, metadata={
        "comment": "The name or description of the OMOP concept id."
    }),
    StructField("OMOP_MANUAL_STANDARD_CONCEPT", StringType(), True, metadata={
        "comment": "This flag determines where a Concept is a Standard Concept, i.e. is used in the data, a Classification Concept, or a non-standard Source Concept. The allowables values are S (Standard Concept) and C (Classification Concept), otherwise the content is NULL."
    }),
    StructField("OMOP_MANUAL_CONCEPT_DOMAIN", StringType(), True, metadata={
        "comment": "A unique identifier for the domain."
    }),
    StructField("OMOP_MANUAL_CONCEPT_CLASS", StringType(), True, metadata={
        "comment": "The identifier for the class or category of the OMOP concept."
    })
])


def add_manual_omop_mappings_numeric(df, barts_mapfile, concepts):
    """
    Adds manual OMOP mappings specific to numeric events.
    
    Args:
        df: Source DataFrame
        barts_mapfile: Manual mapping reference table
        concepts: OMOP concepts reference table
    """
    # Domain priority dictionary
    domain_priority = {
        "Drug": 1,
        "Measurement": 2,
        "Procedure": 3,
        "Condition": 4,
        "Device": 5,
        "Observation": 6
    }
    
    domain_priority_udf = udf(lambda x: domain_priority.get(x, 999), IntegerType())
    
    # Process concept mappings
    concept_maps = (
        barts_mapfile
        .filter(
            (col("SourceTable") == "dbo.PI_CDE_CLINICAL_EVENT") &
            (col("SourceField") == "EVENT_CD") &
            col("OMOPField").isin("measurement_concept_id", "observation_concept_id")
        )
        .select(
            col("SourceValue"),
            col("OMOPTable").alias("OMOP_MANUAL_TABLE"),
            col("OMOPField").alias("OMOP_MANUAL_COLUMN"),
            col("OMOPConceptId").alias("OMOP_MANUAL_CONCEPT"),
            col("EVENT_CD").alias("MAP_EVENT_CD"),
            col("EVENT_CLASS_CD").alias("MAP_EVENT_CLASS_CD"),
            col("STANDARD_CONCEPT")
        )
    )
    
    # Process unit mappings
    unit_maps = (
        barts_mapfile
        .filter(
            (col("SourceTable") == "dbo.PI_CDE_CLINICAL_EVENT") &
            (col("SourceField") == "EVENT_RESULT_UNITS_CD") &  
            (col("OMOPField") == "unit_concept_id")
        )
        .select(
            col("SourceValue").cast(IntegerType()),
            col("OMOPConceptId").alias("OMOP_MANUAL_UNITS")
        )
    )
    
    # Get concept details
    concept_details = (
        concepts
        .select(
            col("concept_id"),
            col("concept_name").alias("OMOP_MANUAL_CONCEPT_NAME"),
            col("standard_concept").alias("OMOP_MANUAL_STANDARD_CONCEPT"),
            col("domain_id").alias("OMOP_MANUAL_CONCEPT_DOMAIN"),
            col("concept_class_id").alias("OMOP_MANUAL_CONCEPT_CLASS")
        )
    )
    
    return (
        df
        # Join with concept mappings
        .join(
            concept_maps,
            (df.EVENT_CD.cast("string") == concept_maps.SourceValue) &
            (concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == concept_maps.MAP_EVENT_CLASS_CD)),
            "left"
        )
        # Join with unit mappings
        .join(
            unit_maps,
            df.UNIT_OF_MEASURE_CD == unit_maps.SourceValue,
            "left"
        )
        # Join with concept details
        .join(
            concept_details,
            col("OMOP_MANUAL_CONCEPT") == concept_details.concept_id,
            "left"
        )
        .withColumn(
            "domain_priority",
            domain_priority_udf(col("OMOP_MANUAL_CONCEPT_DOMAIN"))
        )
        .withColumn(
            "row_num",
            row_number().over(
                Window.partitionBy("EVENT_ID")
                .orderBy(
                    col("OMOP_MANUAL_STANDARD_CONCEPT").desc_nulls_last(),
                    col("domain_priority").asc_nulls_last(),
                    col("OMOP_MANUAL_CONCEPT").asc_nulls_last()
                )
            )
        )
        .filter(col("row_num") == 1)
        .drop(
            "concept_id",
            "SourceValue",
            "MAP_EVENT_CD",
            "MAP_EVENT_CLASS_CD",
            "STANDARD_CONCEPT",
            "domain_priority",
            "row_num"
        )
    )

def process_numeric_events_incremental():
    """
    Main function to process incremental numeric events updates.
    Handles the end-to-end process of updating the numeric events mapping table.
    """
    try:
        print(f"Starting numeric events incremental processing at {datetime.now()}")
        

        max_adc_updt = get_max_timestamp("4_prod.bronze.map_numeric_events")
        
        # Get base tables with filtering for new/modified records
        string_results = spark.table("4_prod.raw.mill_ce_string_result")\
            .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())\
            .filter(col("ADC_UPDT") > max_adc_updt)\
            .alias("sr")
        
        if string_results.count() > 0:
            print(f"Processing {string_results.count()} updated records")
            
            clinical_events = (
                spark.table("4_prod.raw.mill_clinical_event")
                .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
                .withColumn(
                    "row_rank",
                    row_number().over(
                        Window.partitionBy("EVENT_ID")
                        .orderBy(col("UPDT_CNT").desc())
                    )
                )
                .filter(col("row_rank") == 1)
                .drop("row_rank")
                .alias("ce")
            )
            
            parent_events = (
                spark.table("4_prod.raw.mill_clinical_event")
                .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
                .withColumn(
                    "row_rank",
                    row_number().over(
                        Window.partitionBy("EVENT_ID")
                        .orderBy(col("UPDT_CNT").desc())
                    )
                )
                .filter(col("row_rank") == 1)
                .drop("row_rank")
                .alias("pe")
            )
            
            # Get reference data
            code_values = spark.table("3_lookup.mill.mill_code_value")
            order_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("oc")
            parent_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("poc")
            
            # Create code lookups
            unit_lookup = code_values.select(
                col("CODE_VALUE"),
                col("DESCRIPTION").alias("unit_desc")
            ).alias("unit")
            
            event_cd_lookup = code_values.select(
                col("CODE_VALUE"),
                col("DESCRIPTION").alias("event_desc")
            ).alias("event")
            
            parent_event_cd_lookup = code_values.select(
                col("CODE_VALUE"),
                col("DESCRIPTION").alias("parent_event_desc")
            ).alias("parent_event")
            
            normalcy_lookup = code_values.select(
                col("CODE_VALUE"),
                col("DESCRIPTION").alias("normalcy_desc")
            ).alias("normalcy")
            
            contrib_sys_lookup = code_values.select(
                col("CODE_VALUE"),
                col("DESCRIPTION").alias("contrib_desc")
            ).alias("contrib")
            
            entry_mode_lookup = code_values.select(
                col("CODE_VALUE"),
                col("DESCRIPTION").alias("entry_desc")
            ).alias("entry")
            
            catalog_type_lookup = code_values.select(
                col("CODE_VALUE"),
                col("DESCRIPTION").alias("cat_type_desc")
            ).alias("cat_type")
            
            parent_catalog_type_lookup = code_values.select(
                col("CODE_VALUE"),
                col("DESCRIPTION").alias("parent_cat_type_desc")
            ).alias("parent_cat_type")
            
            # Process numeric results
            numeric_results = (
                string_results
                .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
                .select(
                     "EVENT_ID",
                     "UNIT_OF_MEASURE_CD",
                     expr("try_cast(STRING_RESULT_TEXT as double)").alias("NUMERIC_RESULT")
                )
                .filter(col("NUMERIC_RESULT").isNotNull())
            )
            
            # Build final result with all joins
            result_df = (
                numeric_results
                .join(clinical_events, numeric_results.EVENT_ID == clinical_events.EVENT_ID, "inner")
                .join(
                    parent_events.select(
                        "EVENT_ID",
                        "EVENT_TITLE_TEXT",
                        "EVENT_CD",
                        "CATALOG_CD",
                        "REFERENCE_NBR"
                    ),
                    col("ce.PARENT_EVENT_ID") == col("pe.EVENT_ID"),
                    "left"
                )
                .join(
                    order_catalog.select(
                        "CATALOG_CD",
                        "CATALOG_TYPE_CD",
                        "DESCRIPTION"
                    ),
                    col("ce.CATALOG_CD") == col("oc.CATALOG_CD"),
                    "left"
                )
                .join(
                    parent_catalog.select(
                        "CATALOG_CD",
                        "CATALOG_TYPE_CD",
                        "DESCRIPTION"
                    ),
                    col("pe.CATALOG_CD") == col("poc.CATALOG_CD"),
                    "left"
                )
                .join(unit_lookup, col("UNIT_OF_MEASURE_CD") == col("unit.CODE_VALUE"), "left") 
                .join(event_cd_lookup, col("ce.EVENT_CD") == col("event.CODE_VALUE"), "left")
                .join(parent_event_cd_lookup, col("pe.EVENT_CD") == col("parent_event.CODE_VALUE"), "left")
                .join(normalcy_lookup, col("NORMALCY_CD") == col("normalcy.CODE_VALUE"), "left")
                .join(contrib_sys_lookup, col("ce.CONTRIBUTOR_SYSTEM_CD") == col("contrib.CODE_VALUE"), "left")
                .join(entry_mode_lookup, col("ENTRY_MODE_CD") == col("entry.CODE_VALUE"), "left")
                .join(catalog_type_lookup, col("oc.CATALOG_TYPE_CD") == col("cat_type.CODE_VALUE"), "left")
                .join(parent_catalog_type_lookup, col("poc.CATALOG_TYPE_CD") == col("parent_cat_type.CODE_VALUE"), "left")
            )
            
            # Select final columns
            result_df = result_df.select(
                # IDs
                col("ce.EVENT_ID").cast(LongType()),
                col("ce.ENCNTR_ID").cast(LongType()),
                col("ce.PERSON_ID").cast(LongType()),
                col("ce.ORDER_ID").cast(LongType()),
                col("ce.EVENT_CLASS_CD").cast(IntegerType()),
                col("ce.PERFORMED_PRSNL_ID").cast(LongType()),
                
                col("NUMERIC_RESULT").cast(FloatType()),
                col("UNIT_OF_MEASURE_CD").cast(IntegerType()),
                col("unit_desc").alias("UNIT_OF_MEASURE_DISPLAY"),
                
                # Main event details
                col("ce.EVENT_TITLE_TEXT"),
                col("ce.EVENT_CD").cast(IntegerType()),
                col("event_desc").alias("EVENT_CD_DISPLAY"),
                col("ce.CATALOG_CD").cast(IntegerType()),
                col("oc.DESCRIPTION").alias("CATALOG_DISPLAY"),
                col("oc.CATALOG_TYPE_CD").cast(IntegerType()),
                col("cat_type_desc").alias("CATALOG_TYPE_DISPLAY"),
                
                # Additional event attributes
                col("ce.CONTRIBUTOR_SYSTEM_CD").cast(IntegerType()),
                col("contrib_desc").alias("CONTRIBUTOR_SYSTEM_DISPLAY"),
                col("ce.REFERENCE_NBR"),
                col("ce.PARENT_EVENT_ID").cast(LongType()),
                col("ce.NORMALCY_CD").cast(IntegerType()),
                col("normalcy_desc").alias("NORMALCY_DISPLAY"),
                col("ce.ENTRY_MODE_CD").cast(IntegerType()),
                col("entry_desc").alias("ENTRY_MODE_DISPLAY"),
                
                # Reference ranges
                expr("try_cast(ce.NORMAL_LOW as float)").alias("NORMAL_LOW"),
                expr("try_cast(ce.NORMAL_HIGH as float)").alias("NORMAL_HIGH"),
                
                # Timestamps
                col("ce.PERFORMED_DT_TM").cast(TimestampType()),
                col("ce.CLINSIG_UPDT_DT_TM").cast(TimestampType()),
                
                # Parent event details
                col("pe.EVENT_TITLE_TEXT").alias("PARENT_EVENT_TITLE_TEXT"),
                col("pe.EVENT_CD").cast(IntegerType()).alias("PARENT_EVENT_CD"),
                col("parent_event_desc").alias("PARENT_EVENT_CD_DISPLAY"),
                col("pe.CATALOG_CD").cast(IntegerType()).alias("PARENT_CATALOG_CD"),
                col("poc.DESCRIPTION").alias("PARENT_CATALOG_DISPLAY"),
                col("poc.CATALOG_TYPE_CD").cast(IntegerType()).alias("PARENT_CATALOG_TYPE_CD"),
                col("parent_cat_type_desc").alias("PARENT_CATALOG_TYPE_DISPLAY"),
                col("pe.REFERENCE_NBR").alias("PARENT_REFERENCE_NBR"),
                col("ce.ADC_UPDT")
            )
            
            # Add OMOP mappings
            final_df = add_manual_omop_mappings_numeric(
                result_df,
                spark.table("3_lookup.omop.barts_new_maps"),
                spark.table("3_lookup.omop.concept").filter(col("invalid_reason").isNull())
            )
            

            update_table(final_df, "4_prod.bronze.map_numeric_events", "EVENT_ID")
            update_metadata("4_prod.bronze.map_numeric_events", schema_map_numeric_events,map_numeric_events_comment)
            print("Successfully updated numeric events mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing numeric events updates: {str(e)}")
        raise


process_numeric_events_incremental()

# COMMAND ----------


map_date_events_comment = "The table contains data related to various events associated with individuals and orders. It includes details such as event identifiers, timestamps, and descriptions of the events and their classifications. This data can be used for tracking event occurrences, analyzing interactions between individuals and orders, and understanding the context of these events through their classifications and descriptions."

schema_map_date_events = StructType([
    StructField("EVENT_ID", LongType(), True, metadata={
        "comment": "The unique primary identifier of the Event Table."
    }),
    StructField("ENCNTR_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the encounter table. It is an internal system assigned number."
    }),
    StructField("PERSON_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the person table. It is an internal system assigned number."
    }),
    StructField("ORDER_ID", LongType(), True, metadata={
        "comment": "The unique primary identifier of Order Table."
    }),
    StructField("EVENT_CLASS_CD", IntegerType(), True, metadata={
        "comment": "Coded value which specifies how the event is stored in and retrieved from the event table's sub-tables. For example, Event_Class_CDs identify events as numeric results, textual results, calculations, medications, etc."
    }),
    StructField("PERFORMED_PRSNL_ID", LongType(), True, metadata={
        "comment": "Personnel id of provider who performed this result."
    }),

    StructField("RESULT_DT_TM", TimestampType(), True, metadata={
        "comment": "Timestamp that records the date and time for the clinical result."
    }),
    StructField("EVENT_TITLE_TEXT", StringType(), True, metadata={
        "comment": "The title for document results."
    }),
    StructField("EVENT_CD", IntegerType(), True, metadata={
        "comment": "It is the code that identifies the most basic unit of the storage, i.e. RBC, discharge summary, image."
    }),
    StructField("EVENT_CD_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("CATALOG_CD", IntegerType(), True, metadata={
        "comment": "Foreign key to the order_catalog table. Catalog_cd does not exist in the code_value table and does not have a code set."
    }),
    StructField("CATALOG_DISPLAY", StringType(), True, metadata={
        "comment": "The description of the Orderable."
    }),
    StructField("CATALOG_TYPE_CD", IntegerType(), True, metadata={
        "comment": "Used to store the internal code for the catalog type. Used as a filtering mechanism for rows on theorder catalog table."
    }),
    StructField("CATALOG_TYPE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("CONTRIBUTOR_SYSTEM_CD", IntegerType(), True, metadata={
        "comment": "Contributor system identifies the source feed of data from which a row was populated.  This is mainly used to determine how to update a set of data that may have originated from more than one source feed."
    }),
    StructField("CONTRIBUTOR_SYSTEM_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("REFERENCE_NBR", StringType(), True, metadata={
        "comment": "The combination of the reference nbr and the contributor system code provides a unique identifier to the origin of the data."
    }),
    StructField("PARENT_EVENT_ID", LongType(), True, metadata={
        "comment": "Provides a mechanism for logical grouping of events. i.e. supergroup and group tests. Same as event_id if current row is the highest level parent."
    }),
    StructField("NORMALCY_CD", IntegerType(), True, metadata={
        "comment": "States whether the result is normal.  This can be used to determine whether to display the event tag in different color on the flowsheet. For group results, this represents an ""overall"" normalcy. i.e. Is any result in the group abnormal?  Also allows different purge criteria to be applied based on result."
    }),
    StructField("NORMALCY_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("ENTRY_MODE_CD", IntegerType(), True, metadata={
        "comment": "Used to identify the method in which a result was entered."
    }),
    StructField("ENTRY_MODE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PERFORMED_DT_TM", TimestampType(), True, metadata={
        "comment": "Date this result was performed (or authored)."
    }),
    StructField("CLINSIG_UPDT_DT_TM", TimestampType(), True, metadata={
        "comment": "Represents the update date/time that tracks when clinically significant updates are made to the Clinical Event and should only be used to check for updates. This field is used to notify audiences when a clinically significant update is made to an existing clinical event, such as when XR Clinical Reporting re-prints a lab result due to an update of the result value or when a result is resent to a provider's Message Center with the result update. This date should NOT be displayed as the clinically."
    }),
    StructField("PARENT_EVENT_TITLE_TEXT", StringType(), True, metadata={
        "comment": "The title associated with the parent event."
    }),
    StructField("PARENT_EVENT_CD", IntegerType(), True, metadata={
        "comment": "The code value of the parent event."
    }),
    StructField("PARENT_EVENT_CD_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PARENT_CATALOG_CD", IntegerType(), True, metadata={
        "comment": "The catalog code of the parent event."
    }),
    StructField("PARENT_CATALOG_DISPLAY", StringType(), True, metadata={
        "comment": "The description of the Orderable."
    }),
    StructField("PARENT_CATALOG_TYPE_CD", IntegerType(), True, metadata={
        "comment": "The internal code for the catalog type of the parent event."
    }),
    StructField("PARENT_CATALOG_TYPE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PARENT_REFERENCE_NBR", StringType(), True, metadata={
        "comment": "The combination of the reference nbr and the contributor system code provides a unique identifier to the origin of the data."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Timestamp of last update."
    }),
])



def create_date_event_code_lookup(code_values, alias_suffix):
    """
    Creates a code value lookup specific to date events mapping.
    
    Args:
        code_values: DataFrame containing code values
        alias_suffix: Suffix for the description column alias
        
    Returns:
        DataFrame: Lookup table with standardized columns
    """
    return code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(f"{alias_suffix}_desc")
    ).alias(alias_suffix)

def process_date_events_incremental():
    """
    Main function to process incremental date events updates.
    Handles the end-to-end process of updating the date events mapping table.
    """
    try:
        print(f"Starting date events incremental processing at {datetime.now()}")
        

        max_adc_updt = get_max_timestamp("4_prod.bronze.map_date_events")
        current_ts = current_timestamp()
        
        # Get base date results with filtering
        date_results = (
            spark.table("4_prod.raw.mill_ce_date_result")
            .filter(
                (col("VALID_UNTIL_DT_TM") > current_ts) &
                (col("ADC_UPDT") > max_adc_updt)
            )
            .alias("dr")
        )
        
        if date_results.count() > 0:
            print(f"Processing {date_results.count()} updated records")
            
            # Get clinical events with latest versions
            clinical_events = (
                spark.table("4_prod.raw.mill_clinical_event")
                .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
                .withColumn(
                    "row_rank",
                    row_number().over(
                        Window.partitionBy("EVENT_ID")
                        .orderBy(col("UPDT_CNT").desc())
                    )
                )
                .filter(col("row_rank") == 1)
                .drop("row_rank")
                .alias("ce")
            )
            
            parent_events = (
                spark.table("4_prod.raw.mill_clinical_event")
                .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
                .withColumn(
                    "row_rank",
                    row_number().over(
                        Window.partitionBy("EVENT_ID")
                        .orderBy(col("UPDT_CNT").desc())
                    )
                )
                .filter(col("row_rank") == 1)
                .drop("row_rank")
                .alias("pe")
            )
            
            # Get reference tables
            code_values = spark.table("3_lookup.mill.mill_code_value")
            order_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("oc")
            parent_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("poc")
            
            # Create code value lookups
            event_cd_lookup = create_date_event_code_lookup(code_values, "event")
            parent_event_cd_lookup = create_date_event_code_lookup(code_values, "parent_event")
            normalcy_lookup = create_date_event_code_lookup(code_values, "normalcy")
            contrib_sys_lookup = create_date_event_code_lookup(code_values, "contrib")
            entry_mode_lookup = create_date_event_code_lookup(code_values, "entry")
            catalog_type_lookup = create_date_event_code_lookup(code_values, "cat_type")
            parent_catalog_type_lookup = create_date_event_code_lookup(code_values, "parent_cat_type")
            
            # Get valid date results
            valid_date_results = (
                date_results
                .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
                .select(
                    "EVENT_ID",
                    "RESULT_DT_TM"
                )
                .filter(col("RESULT_DT_TM").isNotNull())
            )
            
            # Build final dataset with all joins
            result_df = (
                valid_date_results
                # Join with main clinical event
                .join(
                    clinical_events,
                    (valid_date_results.EVENT_ID == clinical_events.EVENT_ID) &
                    (col("ce.VALID_UNTIL_DT_TM") > current_timestamp()),
                    "inner"
                )
                # Join with parent event
                .join(
                    parent_events.select(
                        "EVENT_ID",
                        "EVENT_TITLE_TEXT",
                        "EVENT_CD",
                        "CATALOG_CD",
                        "REFERENCE_NBR"
                    ),
                    col("ce.PARENT_EVENT_ID") == col("pe.EVENT_ID"),
                    "left"
                )
                # Join with order catalogs
                .join(
                    order_catalog.select(
                        "CATALOG_CD",
                        "CATALOG_TYPE_CD",
                        "DESCRIPTION"
                    ),
                    col("ce.CATALOG_CD") == col("oc.CATALOG_CD"),
                    "left"
                )
                .join(
                    parent_catalog.select(
                        "CATALOG_CD",
                        "CATALOG_TYPE_CD",
                        "DESCRIPTION"
                    ),
                    col("pe.CATALOG_CD") == col("poc.CATALOG_CD"),
                    "left"
                )
                # Add all code value lookups
                .join(event_cd_lookup,
                      col("ce.EVENT_CD") == col("event.CODE_VALUE"),
                      "left")
                .join(parent_event_cd_lookup,
                      col("pe.EVENT_CD") == col("parent_event.CODE_VALUE"),
                      "left")
                .join(normalcy_lookup,
                      col("NORMALCY_CD") == col("normalcy.CODE_VALUE"),
                      "left")
                .join(contrib_sys_lookup,
                      col("CONTRIBUTOR_SYSTEM_CD") == col("contrib.CODE_VALUE"),
                      "left")
                .join(entry_mode_lookup,
                      col("ENTRY_MODE_CD") == col("entry.CODE_VALUE"),
                      "left")
                .join(catalog_type_lookup,
                      col("oc.CATALOG_TYPE_CD") == col("cat_type.CODE_VALUE"),
                      "left")
                .join(parent_catalog_type_lookup,
                      col("poc.CATALOG_TYPE_CD") == col("parent_cat_type.CODE_VALUE"),
                      "left")
                # Select final columns with proper casting
                .select(
                    # IDs
                    col("ce.EVENT_ID").cast(LongType()),
                    col("ce.ENCNTR_ID").cast(LongType()),
                    col("ce.PERSON_ID").cast(LongType()),
                    col("ce.ORDER_ID").cast(LongType()),
                    col("ce.EVENT_CLASS_CD").cast(IntegerType()),
                    col("ce.PERFORMED_PRSNL_ID").cast(LongType()),
                    
                    # Result value
                    col("RESULT_DT_TM").cast(TimestampType()),
                    
                    # Main event details
                    col("ce.EVENT_TITLE_TEXT"),
                    col("ce.EVENT_CD").cast(IntegerType()),
                    col("event_desc").alias("EVENT_CD_DISPLAY"),
                    col("ce.CATALOG_CD").cast(IntegerType()),
                    col("oc.DESCRIPTION").alias("CATALOG_DISPLAY"),
                    col("oc.CATALOG_TYPE_CD").cast(IntegerType()),
                    col("cat_type_desc").alias("CATALOG_TYPE_DISPLAY"),
                    
                    # Additional event attributes
                    col("ce.CONTRIBUTOR_SYSTEM_CD").cast(IntegerType()),
                    col("contrib_desc").alias("CONTRIBUTOR_SYSTEM_DISPLAY"),
                    col("ce.REFERENCE_NBR"),
                    col("ce.PARENT_EVENT_ID").cast(LongType()),
                    col("ce.NORMALCY_CD").cast(IntegerType()),
                    col("normalcy_desc").alias("NORMALCY_DISPLAY"),
                    col("ce.ENTRY_MODE_CD").cast(IntegerType()),
                    col("entry_desc").alias("ENTRY_MODE_DISPLAY"),
                    
                    # Timestamps
                    col("ce.PERFORMED_DT_TM").cast(TimestampType()),
                    col("ce.CLINSIG_UPDT_DT_TM").cast(TimestampType()),
                    
                    # Parent event details
                    col("pe.EVENT_TITLE_TEXT").alias("PARENT_EVENT_TITLE_TEXT"),
                    col("pe.EVENT_CD").cast(IntegerType()).alias("PARENT_EVENT_CD"),
                    col("parent_event_desc").alias("PARENT_EVENT_CD_DISPLAY"),
                    col("pe.CATALOG_CD").cast(IntegerType()).alias("PARENT_CATALOG_CD"),
                    col("poc.DESCRIPTION").alias("PARENT_CATALOG_DISPLAY"),
                    col("poc.CATALOG_TYPE_CD").cast(IntegerType()).alias("PARENT_CATALOG_TYPE_CD"),
                    col("parent_cat_type_desc").alias("PARENT_CATALOG_TYPE_DISPLAY"),
                    col("pe.REFERENCE_NBR").alias("PARENT_REFERENCE_NBR"),
                    col("ce.ADC_UPDT")
                )
            )
            

            update_table(result_df, "4_prod.bronze.map_date_events", "EVENT_ID")
            update_metadata("4_prod.bronze.map_date_events",schema_map_date_events,map_date_events_comment)
            print("Successfully updated date events mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing date events updates: {str(e)}")
        raise


process_date_events_incremental()

# COMMAND ----------

map_text_events_comment = "The table contains data related to various events associated with encounters and orders. It includes identifiers for events, encounters, and persons, as well as details about the event type and its contributors. This data can be used for tracking event occurrences, analyzing interactions within the system, and understanding the relationships between different entities involved in the events."

schema_map_text_events = StructType([
    StructField("EVENT_ID", LongType(), True, metadata={
        "comment": "The unique primary identifier of the Event Table."
    }),
    StructField("ENCNTR_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the encounter table. It is an internal system assigned number."
    }),
    StructField("PERSON_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the person table. It is an internal system assigned number."
    }),
    StructField("ORDER_ID", LongType(), True, metadata={
        "comment": "The unique primary identifier of Order Table."
    }),
    StructField("EVENT_CLASS_CD", IntegerType(), True, metadata={
        "comment": "Coded value which specifies how the event is stored in and retrieved from the event table's sub-tables. For example, Event_Class_CDs identify events as numeric results, textual results, calculations, medications, etc."
    }),
    StructField("PERFORMED_PRSNL_ID", LongType(), True, metadata={
        "comment": "Personnel id of provider who performed this result."
    }),
    StructField("TEXT_RESULT", StringType(), True, metadata={
        "comment": "The textual result value for the clinical event."
    }),
    StructField("UNIT_OF_MEASURE_CD", IntegerType(), True, metadata={
        "comment": "The code value for the unit of measurement."
    }),
    StructField("UNIT_OF_MEASURE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("EVENT_TITLE_TEXT", StringType(), True, metadata={
        "comment": "The title for document results."
    }),
    StructField("EVENT_CD", IntegerType(), True, metadata={
        "comment": "It is the code that identifies the most basic unit of the storage, i.e. RBC, discharge summary, image."
    }),
    StructField("EVENT_CD_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("CATALOG_CD", IntegerType(), True, metadata={
        "comment": "Foreign key to the order_catalog table. Catalog_cd does not exist in the code_value table and does not have a code set."
    }),
    StructField("CATALOG_DISPLAY", StringType(), True, metadata={
        "comment": "The description of the Orderable"
    }),
    StructField("CATALOG_TYPE_CD", IntegerType(), True, metadata={
        "comment": "Used to store the internal code for the catalog type. Used as a filtering mechanism for rows on theorder catalog table."
    }),
    StructField("CATALOG_TYPE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("CONTRIBUTOR_SYSTEM_CD", IntegerType(), True, metadata={
        "comment": "Contributor system identifies the source feed of data from which a row was populated.  This is mainly used to determine how to update a set of data that may have originated from more than one source feed."
    }),
    StructField("CONTRIBUTOR_SYSTEM_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("REFERENCE_NBR", StringType(), True, metadata={
        "comment": "The combination of the reference nbr and the contributor system code provides a unique identifier to the origin of the data."
    }),
    StructField("PARENT_EVENT_ID", LongType(), True, metadata={
        "comment": "Provides a mechanism for logical grouping of events.  i.e. supergroup and group tests.  Same as event_id if current row is the highest level parent."
    }),
    StructField("NORMALCY_CD", IntegerType(), True, metadata={
        "comment": "States whether the result is normal.  This can be used to determine whether to display the event tag in different color on the flowsheet. For group results, this represents an ""overall"" normalcy. i.e. Is any result in the group abnormal?  Also allows different purge criteria to be applied based on result."
    }),
    StructField("NORMALCY_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("ENTRY_MODE_CD", IntegerType(), True, metadata={
        "comment": "Used to identify the method in which a result was entered."
    }),
    StructField("ENTRY_MODE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PERFORMED_DT_TM", TimestampType(), True, metadata={
        "comment": "Date this result was performed (or authored)."
    }),
    StructField("CLINSIG_UPDT_DT_TM", TimestampType(), True, metadata={
        "comment": "Represents the update date/time that tracks when clinically significant updates are made to the Clinical Event and should only be used to check for updates. This field is used to notify audiences when a clinically significant update is made to an existing clinical event, such as when XR Clinical Reporting re-prints a lab result due to an update of the result value or when a result is resent to a provider's Message Center with the result update. This date should NOT be displayed as the clinically."
    }),
    StructField("PARENT_EVENT_TITLE_TEXT", StringType(), True, metadata={
        "comment": "The title for the parent event."
    }),
    StructField("PARENT_EVENT_CD", IntegerType(), True, metadata={
        "comment": "The code value of the parent event."
    }),
    StructField("PARENT_EVENT_CD_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PARENT_CATALOG_CD", IntegerType(), True, metadata={
        "comment": "The catalog code of the parent event."
    }),
    StructField("PARENT_CATALOG_DISPLAY", StringType(), True, metadata={
        "comment": "The description of the Orderable."
    }),
    StructField("PARENT_CATALOG_TYPE_CD", IntegerType(), True, metadata={
        "comment": "The internal code for the catalog type of the parent event."
    }),
    StructField("PARENT_CATALOG_TYPE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PARENT_REFERENCE_NBR", StringType(), True, metadata={
        "comment": "The combination of the reference nbr and the contributor system code provides a unique identifier to the origin of the data."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Timestamp of last update."
    }),
    StructField("OMOP_MANUAL_TABLE", StringType(), True, metadata={
        "comment": "The name of the OMOP Common Data Model table."
    }),
    StructField("OMOP_MANUAL_COLUMN", StringType(), True, metadata={
        "comment": "The field of the OMOP Common Data Model table."
    }),
    StructField("OMOP_MANUAL_CONCEPT", StringType(), True, metadata={
        "comment": "The concept_id of the OMOP Common Data Model table."
    }),
    StructField("OMOP_MANUAL_VALUE_CONCEPT", StringType(), True, metadata={
        "comment": "The manually mapped OMOP concept ID representing the value of a text for standardized use in the OMOP CDM."
    }),
    StructField("OMOP_MANUAL_CONCEPT_NAME", StringType(), True, metadata={
        "comment": "The name or description of the OMOP concept id."
    }),
    StructField("OMOP_MANUAL_STANDARD_CONCEPT", StringType(), True, metadata={
        "comment": "This flag determines where a Concept is a Standard Concept, i.e. is used in the data, a Classification Concept, or a non-standard Source Concept. The allowables values are S (Standard Concept) and C (Classification Concept), otherwise the content is NULL."
    }),
    StructField("OMOP_MANUAL_CONCEPT_DOMAIN", StringType(), True, metadata={
        "comment": "A unique identifier for the domain."
    }),
    StructField("OMOP_MANUAL_CONCEPT_CLASS", StringType(), True, metadata={
        "comment": "The identifier for the class or category of the OMOP concept."
    })
])


def create_text_event_code_lookup(code_values, alias_suffix):
    """
    Creates a code value lookup specific to text events mapping.
    
    Args:
        code_values: DataFrame containing code values
        alias_suffix: Suffix for column aliases
        
    Returns:
        DataFrame: Lookup table with standardized columns
    """
    return code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(f"{alias_suffix}_desc")
    ).alias(alias_suffix)

def add_manual_omop_mappings_text(df, barts_mapfile, concepts):
    """
    Adds manual OMOP mappings specific to text events.
    
    Args:
        df: Source DataFrame
        barts_mapfile: Manual mapping reference table
        concepts: OMOP concepts reference table
    """
    # Domain priority dictionary
    domain_priority = {
        "Drug": 1,
        "Measurement": 2,
        "Procedure": 3,
        "Condition": 4,
        "Device": 5,
        "Observation": 6
    }
    
    domain_priority_udf = udf(lambda x: domain_priority.get(x, 999), IntegerType())
    
    # Process concept mappings
    concept_maps = (
        barts_mapfile
        .filter(
            (col("SourceTable") == "dbo.PI_CDE_CLINICAL_EVENT") &
            (
                (col("SourceField").isin("EVENT_CD", "EVENT_RESULT_TXT")) &
                col("OMOPField").isin("measurement_concept_id", "observation_concept_id")
            )
        )
        .select(
            col("SourceValue"),
            col("OMOPTable").alias("OMOP_MANUAL_TABLE"),
            col("OMOPField").alias("OMOP_MANUAL_COLUMN"),
            col("OMOPConceptId").alias("OMOP_MANUAL_CONCEPT"),
            col("EVENT_CD").alias("MAP_EVENT_CD"),
            col("EVENT_CLASS_CD").alias("MAP_EVENT_CLASS_CD"),
            col("EVENT_RESULT_TXT").alias("MAP_EVENT_RESULT_TXT"),
            col("SourceField"),
            col("STANDARD_CONCEPT")
        )
    )

    # Process value concept mappings
    value_concept_maps = (
        barts_mapfile
        .filter(
            (col("SourceTable") == "dbo.PI_CDE_CLINICAL_EVENT") &
            (
                (col("SourceField").isin("EVENT_RESULT_TXT", "EVENT_CD")) &
                (col("OMOPField") == "value_as_concept_id")
            )
        )
        .select(
            col("SourceValue"),
            col("OMOPConceptId").alias("OMOP_MANUAL_VALUE_CONCEPT"),
            col("EVENT_CD").alias("MAP_EVENT_CD"),
            col("EVENT_CLASS_CD").alias("MAP_EVENT_CLASS_CD"),
            col("EVENT_RESULT_TXT").alias("MAP_EVENT_RESULT_TXT"),
            col("SourceField")
        )
    )
    
    # Get concept details
    concept_details = (
        concepts
        .select(
            col("concept_id"),
            col("concept_name").alias("OMOP_MANUAL_CONCEPT_NAME"),
            col("standard_concept").alias("OMOP_MANUAL_STANDARD_CONCEPT"),
            col("domain_id").alias("OMOP_MANUAL_CONCEPT_DOMAIN"),
            col("concept_class_id").alias("OMOP_MANUAL_CONCEPT_CLASS")
        )
    )
    
    return (
        df
        # Join with concept mappings
        .join(
            concept_maps,
            (
                (
                    (concept_maps.SourceField == "EVENT_CD") &
                    (df.EVENT_CD.cast("string") == concept_maps.SourceValue) &
                    (concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == concept_maps.MAP_EVENT_CLASS_CD)) &
                    (concept_maps.MAP_EVENT_RESULT_TXT.isNull() | (df.TEXT_RESULT == concept_maps.MAP_EVENT_RESULT_TXT))
                ) |
                (
                    (concept_maps.SourceField == "EVENT_RESULT_TXT") &
                    (df.TEXT_RESULT == concept_maps.SourceValue) &
                    (concept_maps.MAP_EVENT_CD.isNull() | (df.EVENT_CD.cast("string") == concept_maps.MAP_EVENT_CD)) &
                    (concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == concept_maps.MAP_EVENT_CLASS_CD))
                )
            ),
            "left"
        )
        # Join with value concept mappings
        .join(
            value_concept_maps,
            (
                (
                    (value_concept_maps.SourceField == "EVENT_RESULT_TXT") &
                    (df.TEXT_RESULT == value_concept_maps.SourceValue) &
                    (value_concept_maps.MAP_EVENT_CD.isNull() | (df.EVENT_CD.cast("string") == value_concept_maps.MAP_EVENT_CD)) &
                    (value_concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == value_concept_maps.MAP_EVENT_CLASS_CD))
                ) |
                (
                    (value_concept_maps.SourceField == "EVENT_CD") &
                    (df.EVENT_CD.cast("string") == value_concept_maps.SourceValue) &
                    (value_concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == value_concept_maps.MAP_EVENT_CLASS_CD)) &
                    (value_concept_maps.MAP_EVENT_RESULT_TXT.isNull() | (df.TEXT_RESULT == value_concept_maps.MAP_EVENT_RESULT_TXT))
                )
            ),
            "left"
        )
        # Join with concept details
        .join(
            concept_details,
            col("OMOP_MANUAL_CONCEPT") == concept_details.concept_id,
            "left"
        )
        .withColumn(
            "domain_priority",
            domain_priority_udf(col("OMOP_MANUAL_CONCEPT_DOMAIN"))
        )
        .withColumn(
            "row_num",
            row_number().over(
                Window.partitionBy("EVENT_ID")
                .orderBy(
                    col("OMOP_MANUAL_STANDARD_CONCEPT").desc_nulls_last(),
                    col("domain_priority").asc_nulls_last(),
                    col("OMOP_MANUAL_CONCEPT").asc_nulls_last()
                )
            )
        )
        .filter(col("row_num") == 1)
        .drop(
            "concept_id",
            "SourceValue",
            "MAP_EVENT_CD",
            "MAP_EVENT_CLASS_CD",
            "MAP_EVENT_RESULT_TXT",
            "SourceField",
            "STANDARD_CONCEPT",
            "domain_priority",
            "row_num"
        )
    )

def process_text_events_incremental():
    """
    Main function to process incremental text events updates.
    Handles the end-to-end process of updating the text events mapping table.
    """
    try:
        print(f"Starting text events incremental processing at {datetime.now()}")
        

        max_adc_updt = get_max_timestamp("4_prod.bronze.map_text_events")
        
        # Get base tables with filtering for new/modified records
        string_results = (
            spark.table("4_prod.raw.mill_ce_string_result")
            .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
            .filter(col("ADC_UPDT") > max_adc_updt)
            .alias("sr")
        )
        
        if string_results.count() > 0:
            print(f"Processing {string_results.count()} updated records")
            
            clinical_events = (
                spark.table("4_prod.raw.mill_clinical_event")
                .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
                .withColumn(
                    "row_rank",
                    row_number().over(
                        Window.partitionBy("EVENT_ID")
                        .orderBy(col("UPDT_CNT").desc())
                    )
                )
                .filter(col("row_rank") == 1)
                .drop("row_rank")
                .alias("ce")
            )
            
            parent_events = (
                spark.table("4_prod.raw.mill_clinical_event")
                .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
                .withColumn(
                    "row_rank",
                    row_number().over(
                        Window.partitionBy("EVENT_ID")
                        .orderBy(col("UPDT_CNT").desc())
                    )
                )
                .filter(col("row_rank") == 1)
                .drop("row_rank")
                .alias("pe")
            )
            
            # Get reference data
            code_values = spark.table("3_lookup.mill.mill_code_value")
            order_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("oc")
            parent_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("poc")
            
            # Create code value lookups
            unit_lookup = create_text_event_code_lookup(code_values, "unit")
            event_cd_lookup = create_text_event_code_lookup(code_values, "event")
            parent_event_cd_lookup = create_text_event_code_lookup(code_values, "parent_event")
            normalcy_lookup = create_text_event_code_lookup(code_values, "normalcy")
            contrib_sys_lookup = create_text_event_code_lookup(code_values, "contrib")
            entry_mode_lookup = create_text_event_code_lookup(code_values, "entry")
            catalog_type_lookup = create_text_event_code_lookup(code_values, "cat_type")
            parent_catalog_type_lookup = create_text_event_code_lookup(code_values, "parent_cat_type")
            
            # Get valid text results
            text_results = (
                string_results
                .select(
                    "EVENT_ID",
                    "UNIT_OF_MEASURE_CD",
                    col("STRING_RESULT_TEXT").alias("TEXT_RESULT")
                )
                # Keep only non-numeric text results
                .filter(
                    col("TEXT_RESULT").isNotNull() & 
                    expr("try_cast(TEXT_RESULT as double)").isNull()
                )
            )
            
            # Build the main result DataFrame with all joins
            result_df = (
                text_results
                .join(clinical_events, ["EVENT_ID"])
                .join(
                    parent_events.select(
                        "EVENT_ID",
                        "EVENT_TITLE_TEXT",
                        "EVENT_CD",
                        "CATALOG_CD",
                        "REFERENCE_NBR"
                    ),
                    col("ce.PARENT_EVENT_ID") == col("pe.EVENT_ID"),
                    "left"
                )
                .join(
                    order_catalog.select(
                        "CATALOG_CD",
                        "CATALOG_TYPE_CD",
                        "DESCRIPTION"
                    ),
                    col("ce.CATALOG_CD") == col("oc.CATALOG_CD"),
                    "left"
                )
                .join(
                    parent_catalog.select(
                        "CATALOG_CD",
                        "CATALOG_TYPE_CD",
                        "DESCRIPTION"
                    ),
                    col("pe.CATALOG_CD") == col("poc.CATALOG_CD"),
                    "left"
                )
                .join(unit_lookup, col("UNIT_OF_MEASURE_CD") == col("unit.CODE_VALUE"), "left")
                .join(event_cd_lookup, col("ce.EVENT_CD") == col("event.CODE_VALUE"), "left")
                .join(parent_event_cd_lookup, col("pe.EVENT_CD") == col("parent_event.CODE_VALUE"), "left")
                .join(normalcy_lookup, col("NORMALCY_CD") == col("normalcy.CODE_VALUE"), "left")
                .join(contrib_sys_lookup, col("CONTRIBUTOR_SYSTEM_CD") == col("contrib.CODE_VALUE"), "left")
                .join(entry_mode_lookup, col("ENTRY_MODE_CD") == col("entry.CODE_VALUE"), "left")
                .join(catalog_type_lookup, col("oc.CATALOG_TYPE_CD") == col("cat_type.CODE_VALUE"), "left")
                .join(parent_catalog_type_lookup, col("poc.CATALOG_TYPE_CD") == col("parent_cat_type.CODE_VALUE"), "left")
            )
            
            # Select final columns with proper casting
            result_df = result_df.select(
                # IDs
                col("ce.EVENT_ID").cast(LongType()),
                col("ce.ENCNTR_ID").cast(LongType()),
                col("ce.PERSON_ID").cast(LongType()),
                col("ce.ORDER_ID").cast(LongType()),
                col("ce.EVENT_CLASS_CD").cast(IntegerType()),
                col("ce.PERFORMED_PRSNL_ID").cast(LongType()),
                
                col("TEXT_RESULT").cast(StringType()),
                col("UNIT_OF_MEASURE_CD").cast(IntegerType()),
                col("unit_desc").alias("UNIT_OF_MEASURE_DISPLAY"),
                
                # Main event details
                col("ce.EVENT_TITLE_TEXT"),
                col("ce.EVENT_CD").cast(IntegerType()),
                col("event_desc").alias("EVENT_CD_DISPLAY"),
                col("ce.CATALOG_CD").cast(IntegerType()),
                col("oc.DESCRIPTION").alias("CATALOG_DISPLAY"),
                col("oc.CATALOG_TYPE_CD").cast(IntegerType()),
                col("cat_type_desc").alias("CATALOG_TYPE_DISPLAY"),
                
                # Additional event attributes
                col("ce.CONTRIBUTOR_SYSTEM_CD").cast(IntegerType()),
                col("contrib_desc").alias("CONTRIBUTOR_SYSTEM_DISPLAY"),
                col("ce.REFERENCE_NBR"),
                col("ce.PARENT_EVENT_ID").cast(LongType()),
                col("ce.NORMALCY_CD").cast(IntegerType()),
                col("normalcy_desc").alias("NORMALCY_DISPLAY"),
                col("ce.ENTRY_MODE_CD").cast(IntegerType()),
                col("entry_desc").alias("ENTRY_MODE_DISPLAY"),
                
                # Timestamps
                col("ce.PERFORMED_DT_TM").cast(TimestampType()),
                col("ce.CLINSIG_UPDT_DT_TM").cast(TimestampType()),
                
                # Parent event details
                col("pe.EVENT_TITLE_TEXT").alias("PARENT_EVENT_TITLE_TEXT"),
                col("pe.EVENT_CD").cast(IntegerType()).alias("PARENT_EVENT_CD"),
                col("parent_event_desc").alias("PARENT_EVENT_CD_DISPLAY"),
                col("pe.CATALOG_CD").cast(IntegerType()).alias("PARENT_CATALOG_CD"),
                col("poc.DESCRIPTION").alias("PARENT_CATALOG_DISPLAY"),
                col("poc.CATALOG_TYPE_CD").cast(IntegerType()).alias("PARENT_CATALOG_TYPE_CD"),
                col("parent_cat_type_desc").alias("PARENT_CATALOG_TYPE_DISPLAY"),
                col("pe.REFERENCE_NBR").alias("PARENT_REFERENCE_NBR"),
                col("ce.ADC_UPDT")
            )
            
            # Add OMOP mappings
            final_df = add_manual_omop_mappings_text(
                result_df,
                spark.table("3_lookup.omop.barts_new_maps"),
                spark.table("3_lookup.omop.concept").filter(col("invalid_reason").isNull())
            )
            

            update_table(final_df, "4_prod.bronze.map_text_events", "EVENT_ID")
            update_metadata("4_prod.bronze.map_text_events",schema_map_text_events,map_text_events_comment)
            print("Successfully updated text events mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing text events updates: {str(e)}")
        raise


process_text_events_incremental()

# COMMAND ----------

map_nomen_events_comment = "The table contains data related to various events associated with encounters and orders. It includes identifiers for events, encounters, and persons, as well as details about the event type and its classification. This data can be used for tracking event occurrences, analyzing interactions within the system, and understanding the relationships between different entities involved in the events."

schema_map_nomen_events = StructType([
    StructField("EVENT_ID", LongType(), True, metadata={
        "comment": "The unique primary identifier of the Event Table."
    }),
    StructField("ENCNTR_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the encounter table. It is an internal system assigned number."
    }),
    StructField("PERSON_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the person table. It is an internal system assigned number."
    }),
    StructField("ORDER_ID", LongType(), True, metadata={
        "comment": "The unique primary identifier of Order Table."
    }),
    StructField("EVENT_CLASS_CD", IntegerType(), True, metadata={
        "comment": "Coded value which specifies how the event is stored in and retrieved from the event table's sub-tables. For example, Event_Class_CDs identify events as numeric results, textual results, calculations, medications, etc."
    }),
    StructField("PERFORMED_PRSNL_ID", LongType(), True, metadata={
        "comment": "Personnel id of provider who performed this result."
    }),
    StructField("EVENT_TITLE_TEXT", StringType(), True, metadata={
        "comment": "The title for document results."
    }),
    StructField("EVENT_CD", IntegerType(), True, metadata={
        "comment": "It is the code that identifies the most basic unit of the storage, i.e. RBC, discharge summary, image."
    }),
    StructField("EVENT_CD_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("CATALOG_CD", IntegerType(), True, metadata={
        "comment": "Foreign key to the order_catalog table. Catalog_cd does not exist in the code_value table and does not have a code set."
    }),
    StructField("CATALOG_DISPLAY", StringType(), True, metadata={
        "comment": "The description of the Orderable"
    }),
    StructField("CATALOG_TYPE_CD", IntegerType(), True, metadata={
        "comment": "Used to store the internal code for the catalog type. Used as a filtering mechanism for rows on theorder catalog table."
    }),

    StructField("CATALOG_TYPE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("EVENT_CLASS_CD", IntegerType(), True, metadata={
        "comment": "Coded value which specifies how the event is stored in and retrieved from the event table's sub-tables. For example, Event_Class_CDs identify events as numeric results, textual results, calculations, medications, etc."
    }),
    StructField("CONTRIBUTOR_SYSTEM_CD", IntegerType(), True, metadata={
        "comment": "Contributor system identifies the source feed of data from which a row was populated.  This is mainly used to determine how to update a set of data that may have originated from more than one source feed."
    }),
    StructField("CONTRIBUTOR_SYSTEM_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("REFERENCE_NBR", StringType(), True, metadata={
        "comment": "The combination of the reference nbr and the contributor system code provides a unique identifier to the origin of the data."
    }),
    StructField("PARENT_EVENT_ID", LongType(), True, metadata={
        "comment": "Provides a mechanism for logical grouping of events.  i.e. supergroup and group tests.  Same as event_id if current row is the highest level parent."
    }),
    StructField("NORMALCY_CD", IntegerType(), True, metadata={
        "comment": "States whether the result is normal.  This can be used to determine whether to display the event tag in different color on the flowsheet. For group results, this represents an ""overall"" normalcy. i.e. Is any result in the group abnormal?  Also allows different purge criteria to be applied based on result."
    }),
    StructField("NORMALCY_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("ENTRY_MODE_CD", IntegerType(), True, metadata={
        "comment": "Used to identify the method in which a result was entered."
    }),
    StructField("ENTRY_MODE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value"
    }),
    StructField("PERFORMED_DT_TM", TimestampType(), True, metadata={
        "comment": "Date this result was performed (or authored)."
    }),
    StructField("CLINSIG_UPDT_DT_TM", TimestampType(), True, metadata={
        "comment": "Represents the update date/time that tracks when clinically significant updates are made to the Clinical Event and should only be used to check for updates. This field is used to notify audiences when a clinically significant update is made to an existing clinical event, such as when XR Clinical Reporting re-prints a lab result due to an update of the result value or when a result is resent to a provider's Message Center with the result update. This date should NOT be displayed as the clinically."
    }),
    StructField("PARENT_EVENT_CD", IntegerType(), True, metadata={
        "comment": "The code value of the parent event."
    }),
    StructField("PARENT_EVENT_CD_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PARENT_CATALOG_CD", IntegerType(), True, metadata={
        "comment": "The internal code for the order catalog item of the parent event."
    }),
    StructField("PARENT_CATALOG_DISPLAY", StringType(), True, metadata={
        "comment": "The description of the Orderable."
    }),
    StructField("PARENT_CATALOG_TYPE_CD", IntegerType(), True, metadata={
        "comment": "The internal code for the catalog type of the parent event."
    }),
    StructField("PARENT_CATALOG_TYPE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PARENT_REFERENCE_NBR", StringType(), True, metadata={
        "comment": "The combination of the reference nbr and the contributor system code provides a unique identifier to the origin of the data."
    }),
    StructField("SOURCE_IDENTIFIER", StringType(), True, metadata={
        "comment": "The code, or key, from the source vocabulary that contributed the string to the nomenclature."
    }),
    StructField("SOURCE_STRING", StringType(), True, metadata={
        "comment": "Variable length string that may include alphanumeric characters and punctuation."
        }),
    StructField("SOURCE_VOCABULARY_CD", DoubleType(), True, metadata={
        "comment": "The external vocabulary or lexicon that contributed the string, e.g. ICD9, SNOMED, etc."
    }),
    StructField("SOURCE_VOCABULARY_DESC", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("VOCAB_AXIS_CD", DoubleType(), True, metadata={
        "comment": "Vocabulary AXIS codes related to SNOMEDColumn."
    }),
    StructField("vocab_axis_desc", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("CONCEPT_CKI", StringType(), True, metadata={
        "comment": "Concept CKI is the functional Concept Identifier; it is the codified means within Millennium to identify key medical concepts to support information processing, clinical decision support, executable knowledge and knowledge presentation. Composed of a source and an identifier.  For example, if the concept source is ""SNOMED"" and the concept identifier is ""123""."
    }),
    StructField("OMOP_CONCEPT_ID", IntegerType(), True, metadata={
        "comment": "A unique identifier for each Concept across all domains."
    }),
    StructField("OMOP_CONCEPT_NAME", StringType(), True, metadata={
        "comment": "An unambiguous, meaningful and descriptive name for the Concept."
    }),
    StructField("OMOP_STANDARD_CONCEPT", StringType(), True, metadata={
        "comment": "This flag determines where a Concept is a Standard Concept, i.e. is used in the data, a Classification Concept, or a non-standard Source Concept. The allowables values are S (Standard Concept) and C (Classification Concept), otherwise the content is NULL."
    }),
    StructField("OMOP_MATCH_NUMBER", LongType(), True, metadata={
        "comment": "The number of OMOP concepts matched for each NOMENCLATURE_ID."
    }),
    StructField("OMOP_CONCEPT_DOMAIN", StringType(), True, metadata={
        "comment": "A unique identifier for each domain."
    }),
    StructField("OMOP_CONCEPT_CLASS", StringType(), True, metadata={
        "comment": "The attribute or concept class of the Concept."
    }),
    StructField("SNOMED_CODE", LongType(), True, metadata={
        "comment": ""
    }),
    StructField("SNOMED_TYPE", StringType(), True, metadata={
        "comment": "The method or source of the SNOMED code mapping for each nomenclature entry."
    }),
    StructField("SNOMED_MATCH_NUMBER", LongType(), True, metadata={
        "comment": "The number of matches found for each NOMENCLATURE_ID in the context of SNOMED codes."
    }),
    StructField("SNOMED_TERM", StringType(), True, metadata={
        "comment": "The term associated with a SNOMED code that provides additional meaning and context to the code."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Timestamp of last update."
    }),
    StructField("OMOP_MANUAL_TABLE", StringType(), True, metadata={
        "comment": "The name of the OMOP Common Data Model table."
    }),
    StructField("OMOP_MANUAL_COLUMN", StringType(), True, metadata={
        "comment": "The field of the OMOP Common Data Model table."
    }),
    StructField("OMOP_MANUAL_CONCEPT", StringType(), True, metadata={
        "comment": "The concept_id of the OMOP Common Data Model table."
    }),
    StructField("OMOP_MANUAL_VALUE_CONCEPT", StringType(), True, metadata={
        "comment": "The manually mapped OMOP concept ID representing the value of a text for standardized use in the OMOP CDM."
    }),
    StructField("OMOP_MANUAL_CONCEPT_NAME", StringType(), True, metadata={
        "comment": "The name or description of the OMOP concept id."
    }),
    StructField("OMOP_MANUAL_STANDARD_CONCEPT", StringType(), True, metadata={
        "comment": "This flag determines where a Concept is a Standard Concept, i.e. is used in the data, a Classification Concept, or a non-standard Source Concept. The allowables values are S (Standard Concept) and C (Classification Concept), otherwise the content is NULL."
    }),
    StructField("OMOP_MANUAL_CONCEPT_DOMAIN", StringType(), True, metadata={
        "comment": "A unique identifier for the domain."
    }),
    StructField("OMOP_MANUAL_CONCEPT_CLASS", StringType(), True, metadata={
        "comment": "The identifier for the class or category of the OMOP concept."
    })
])


def create_nomen_code_lookup(code_values, alias_suffix):
    """
    Creates a code value lookup specific to nomenclature mapping.
    
    Args:
        code_values: DataFrame containing code values
        alias_suffix: Suffix for column aliases
        
    Returns:
        DataFrame: Lookup table with standardized columns
    """
    return code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(f"{alias_suffix}_desc")
    ).alias(alias_suffix)

def add_manual_omop_mappings_nomen(df, barts_mapfile, concepts):
    """
    Adds manual OMOP mappings specific to nomenclature results.
    
    Args:
        df: Source DataFrame
        barts_mapfile: Manual mapping reference table
        concepts: OMOP concepts reference table
    """
    # Domain priority dictionary
    domain_priority = {
        "Drug": 1,
        "Measurement": 2,
        "Procedure": 3,
        "Condition": 4,
        "Device": 5,
        "Observation": 6
    }
    
    domain_priority_udf = udf(lambda x: domain_priority.get(x, 999), IntegerType())
    
    # Process concept mappings
    concept_maps = (
        barts_mapfile
        .filter(
            (col("SourceTable") == "dbo.PI_CDE_CLINICAL_EVENT") &
            (
                (col("SourceField").isin("EVENT_CD", "EVENT_RESULT_TXT")) &
                col("OMOPField").isin("measurement_concept_id", "observation_concept_id")
            )
        )
        .select(
            col("SourceValue"),
            col("OMOPTable").alias("OMOP_MANUAL_TABLE"),
            col("OMOPField").alias("OMOP_MANUAL_COLUMN"),
            col("OMOPConceptId").alias("OMOP_MANUAL_CONCEPT"),
            col("EVENT_CD").alias("MAP_EVENT_CD"),
            col("EVENT_CLASS_CD").alias("MAP_EVENT_CLASS_CD"),
            col("EVENT_RESULT_TXT").alias("MAP_EVENT_RESULT_TXT"),
            col("SourceField"),
            col("STANDARD_CONCEPT")
        )
    )
    
    # Process value concept mappings
    value_concept_maps = (
        barts_mapfile
        .filter(
            (col("SourceTable") == "dbo.PI_CDE_CLINICAL_EVENT") &
            (
                (col("SourceField").isin("EVENT_RESULT_TXT", "EVENT_CD")) &
                (col("OMOPField") == "value_as_concept_id")
            )
        )
        .select(
            col("SourceValue"),
            col("OMOPConceptId").alias("OMOP_MANUAL_VALUE_CONCEPT"),
            col("EVENT_CD").alias("MAP_EVENT_CD"),
            col("EVENT_CLASS_CD").alias("MAP_EVENT_CLASS_CD"),
            col("EVENT_RESULT_TXT").alias("MAP_EVENT_RESULT_TXT"),
            col("SourceField")
        )
    )
    
    # Get concept details
    concept_details = (
        concepts
        .select(
            col("concept_id"),
            col("concept_name").alias("OMOP_MANUAL_CONCEPT_NAME"),
            col("standard_concept").alias("OMOP_MANUAL_STANDARD_CONCEPT"),
            col("domain_id").alias("OMOP_MANUAL_CONCEPT_DOMAIN"),
            col("concept_class_id").alias("OMOP_MANUAL_CONCEPT_CLASS")
        )
    )
    
    return (
        df
        # Join with concept mappings
        .join(
            concept_maps,
            (
                (
                    (concept_maps.SourceField == "EVENT_CD") &
                    (df.EVENT_CD.cast("string") == concept_maps.SourceValue) &
                    (concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == concept_maps.MAP_EVENT_CLASS_CD)) &
                    (concept_maps.MAP_EVENT_RESULT_TXT.isNull() | (df.SOURCE_STRING == concept_maps.MAP_EVENT_RESULT_TXT))
                ) |
                (
                    (concept_maps.SourceField == "EVENT_RESULT_TXT") &
                    (df.SOURCE_STRING == concept_maps.SourceValue) &
                    (concept_maps.MAP_EVENT_CD.isNull() | (df.EVENT_CD.cast("string") == concept_maps.MAP_EVENT_CD)) &
                    (concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == concept_maps.MAP_EVENT_CLASS_CD))
                )
            ),
            "left"
        )
        # Join with value concept mappings
        .join(
            value_concept_maps,
            (
                (
                    (value_concept_maps.SourceField == "EVENT_RESULT_TXT") &
                    (df.SOURCE_STRING == value_concept_maps.SourceValue) &
                    (value_concept_maps.MAP_EVENT_CD.isNull() | (df.EVENT_CD.cast("string") == value_concept_maps.MAP_EVENT_CD)) &
                    (value_concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == value_concept_maps.MAP_EVENT_CLASS_CD))
                ) |
                (
                    (value_concept_maps.SourceField == "EVENT_CD") &
                    (df.EVENT_CD.cast("string") == value_concept_maps.SourceValue) &
                    (value_concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == value_concept_maps.MAP_EVENT_CLASS_CD)) &
                    (value_concept_maps.MAP_EVENT_RESULT_TXT.isNull() | (df.SOURCE_STRING == value_concept_maps.MAP_EVENT_RESULT_TXT))
                )
            ),
            "left"
        )
        # Join with concept details
        .join(
            concept_details,
            col("OMOP_MANUAL_CONCEPT") == concept_details.concept_id,
            "left"
        )
        .withColumn(
            "domain_priority",
            domain_priority_udf(col("OMOP_MANUAL_CONCEPT_DOMAIN"))
        )
        .withColumn(
            "row_num",
            row_number().over(
                Window.partitionBy("EVENT_ID")
                .orderBy(
                    col("OMOP_MANUAL_STANDARD_CONCEPT").desc_nulls_last(),
                    col("domain_priority").asc_nulls_last(),
                    col("OMOP_MANUAL_CONCEPT").asc_nulls_last()
                )
            )
        )
        .filter(col("row_num") == 1)
        .drop(
            "concept_id",
            "SourceValue",
            "MAP_EVENT_CD",
            "MAP_EVENT_CLASS_CD",
            "MAP_EVENT_RESULT_TXT",
            "SourceField",
            "STANDARD_CONCEPT",
            "domain_priority",
            "row_num"
        )
    )

def process_nomen_events_incremental():
    """
    Main function to process incremental nomenclature events updates.
    Handles the end-to-end process of updating the nomenclature events mapping table.
    """
    try:
        print(f"Starting nomenclature events incremental processing at {datetime.now()}")
        

        max_adc_updt = get_max_timestamp("4_prod.bronze.map_nomen_events")
        
        # Get base tables with filtering
        coded_results = (
            spark.table("4_prod.raw.mill_ce_coded_result")
            .filter(
                (col("VALID_UNTIL_DT_TM") > current_timestamp()) &
                (col("ADC_UPDT") > max_adc_updt)
            )
            .alias("cr")
        )
        
        if coded_results.count() > 0:
            print(f"Processing {coded_results.count()} updated records")
            
            # Get clinical events with latest versions
            clinical_events = (
                spark.table("4_prod.raw.mill_clinical_event")
                .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
                .withColumn(
                    "row_rank",
                    row_number().over(
                        Window.partitionBy("EVENT_ID")
                        .orderBy(col("UPDT_CNT").desc())
                    )
                )
                .filter(col("row_rank") == 1)
                .drop("row_rank")
                .alias("ce")
            )
            
            parent_events = (
                spark.table("4_prod.raw.mill_clinical_event")
                .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
                .withColumn(
                    "row_rank",
                    row_number().over(
                        Window.partitionBy("EVENT_ID")
                        .orderBy(col("UPDT_CNT").desc())
                    )
                )
                .filter(col("row_rank") == 1)
                .drop("row_rank")
                .alias("pe")
            )
            
            # Get reference data
            code_values = spark.table("3_lookup.mill.mill_code_value")
            order_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("oc")
            parent_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("poc")
            nomenclature = spark.table("4_prod.bronze.nomenclature").alias("nom")
            
            # Create code value lookups
            event_cd_lookup = create_nomen_code_lookup(code_values, "event")
            parent_event_cd_lookup = create_nomen_code_lookup(code_values, "parent_event")
            normalcy_lookup = create_nomen_code_lookup(code_values, "normalcy")
            contrib_sys_lookup = create_nomen_code_lookup(code_values, "contrib")
            entry_mode_lookup = create_nomen_code_lookup(code_values, "entry")
            catalog_type_lookup = create_nomen_code_lookup(code_values, "cat_type")
            parent_catalog_type_lookup = create_nomen_code_lookup(code_values, "parent_cat_type")
            source_vocab_lookup = create_nomen_code_lookup(code_values, "vocab")
            vocab_axis_lookup = create_nomen_code_lookup(code_values, "axis")
            
            # Get valid nomenclature results
            nomen_results = coded_results.filter(
                (col("NOMENCLATURE_ID").isNotNull()) & 
                (col("NOMENCLATURE_ID") != 0) &
                (col("VALID_UNTIL_DT_TM") > current_timestamp())
            )
            
            # Process nomenclature
            nomenclature_processed = (
                nomenclature
                .withColumn(
                    "CONCEPT_CKI_PROCESSED",
                    substring_index(col("CONCEPT_CKI"), "!", -1)
                )
            )
            
            # Build final result with all joins
            result_df = (
                nomen_results
                # Join with main clinical event
                .join(
                    clinical_events,
                    (nomen_results.EVENT_ID == clinical_events.EVENT_ID) &
                    (col("ce.VALID_UNTIL_DT_TM") > current_timestamp()),
                    "inner"
                )
                # Join with parent event
                .join(
                    parent_events.select(
                        "EVENT_ID",
                        "EVENT_TITLE_TEXT",
                        "EVENT_CD",
                        "CATALOG_CD",
                        "REFERENCE_NBR"
                    ),
                    col("ce.PARENT_EVENT_ID") == col("pe.EVENT_ID"),
                    "left"
                )
                # Join with order catalogs
                .join(
                    order_catalog.select(
                        "CATALOG_CD",
                        "CATALOG_TYPE_CD",
                        "DESCRIPTION"
                    ),
                    col("ce.CATALOG_CD") == col("oc.CATALOG_CD"),
                    "left"
                )
                .join(
                    parent_catalog.select(
                        "CATALOG_CD",
                        "CATALOG_TYPE_CD",
                        "DESCRIPTION"
                    ),
                    col("pe.CATALOG_CD") == col("poc.CATALOG_CD"),
                    "left"
                )
                # Join with nomenclature
                .join(
                    nomenclature_processed,
                    col("cr.NOMENCLATURE_ID") == col("nom.NOMENCLATURE_ID"),
                    "left"
                )
                # Add all code value lookups
                .join(event_cd_lookup, 
                      col("ce.EVENT_CD") == col("event.CODE_VALUE"), 
                      "left")
                .join(parent_event_cd_lookup, 
                      col("pe.EVENT_CD") == col("parent_event.CODE_VALUE"), 
                      "left")
                .join(normalcy_lookup, 
                      col("NORMALCY_CD") == col("normalcy.CODE_VALUE"), 
                      "left")
                .join(contrib_sys_lookup, 
                      col("ce.CONTRIBUTOR_SYSTEM_CD") == col("contrib.CODE_VALUE"), 
                      "left")
                .join(entry_mode_lookup, 
                      col("ENTRY_MODE_CD") == col("entry.CODE_VALUE"), 
                      "left")
                .join(catalog_type_lookup, 
                      col("oc.CATALOG_TYPE_CD") == col("cat_type.CODE_VALUE"), 
                      "left")
                .join(parent_catalog_type_lookup, 
                      col("poc.CATALOG_TYPE_CD") == col("parent_cat_type.CODE_VALUE"), 
                      "left")
                .join(source_vocab_lookup,
                      col("SOURCE_VOCABULARY_CD") == col("vocab.CODE_VALUE"),
                      "left")
                .join(vocab_axis_lookup,
                      col("VOCAB_AXIS_CD") == col("axis.CODE_VALUE"),
                      "left")
            )
            
            # Select final columns
            result_df = result_df.select(
                # IDs
                col("ce.EVENT_ID").cast(LongType()),
                col("ce.ENCNTR_ID").cast(LongType()),
                col("ce.PERSON_ID").cast(LongType()),
                col("ce.ORDER_ID").cast(LongType()),
                col("cr.NOMENCLATURE_ID").cast(LongType()),
                col("ce.PERFORMED_PRSNL_ID").cast(LongType()),
                
                # Main event details
                col("ce.EVENT_TITLE_TEXT"),
                col("ce.EVENT_CD").cast(IntegerType()),
                col("event_desc").alias("EVENT_CD_DISPLAY"),
                col("ce.CATALOG_CD").cast(IntegerType()),
                col("oc.DESCRIPTION").alias("CATALOG_DISPLAY"),
                col("oc.CATALOG_TYPE_CD").cast(IntegerType()),
                col("cat_type_desc").alias("CATALOG_TYPE_DISPLAY"),
                col("ce.EVENT_CLASS_CD").cast(IntegerType()),
                
                # Additional event attributes
                col("ce.CONTRIBUTOR_SYSTEM_CD").cast(IntegerType()),
                col("contrib_desc").alias("CONTRIBUTOR_SYSTEM_DISPLAY"),
                col("ce.REFERENCE_NBR"),
                col("ce.PARENT_EVENT_ID").cast(LongType()),
                col("ce.NORMALCY_CD").cast(IntegerType()),
                col("normalcy_desc").alias("NORMALCY_DISPLAY"),
                col("ce.ENTRY_MODE_CD").cast(IntegerType()),
                col("entry_desc").alias("ENTRY_MODE_DISPLAY"),
                
                # Timestamps
                col("ce.PERFORMED_DT_TM").cast(TimestampType()),
                col("ce.CLINSIG_UPDT_DT_TM").cast(TimestampType()),
                
                # Parent event details
                col("pe.EVENT_TITLE_TEXT").alias("PARENT_EVENT_TITLE_TEXT"),
                col("pe.EVENT_CD").cast(IntegerType()).alias("PARENT_EVENT_CD"),
                col("parent_event_desc").alias("PARENT_EVENT_CD_DISPLAY"),
                col("pe.CATALOG_CD").cast(IntegerType()).alias("PARENT_CATALOG_CD"),
                col("poc.DESCRIPTION").alias("PARENT_CATALOG_DISPLAY"),
                col("poc.CATALOG_TYPE_CD").cast(IntegerType()).alias("PARENT_CATALOG_TYPE_CD"),
                col("parent_cat_type_desc").alias("PARENT_CATALOG_TYPE_DISPLAY"),
                col("pe.REFERENCE_NBR").alias("PARENT_REFERENCE_NBR"),
                
                # Nomenclature details
                col("SOURCE_IDENTIFIER"),
                col("SOURCE_STRING"),
                col("SOURCE_VOCABULARY_CD"),
                col("vocab_desc").alias("SOURCE_VOCABULARY_DESC"),
                col("VOCAB_AXIS_CD"),
                col("axis_desc").alias("VOCAB_AXIS_DESC"),
                col("CONCEPT_CKI"),
                col("OMOP_CONCEPT_ID"),
                col("OMOP_CONCEPT_NAME"),
                col("IS_STANDARD_OMOP_CONCEPT").alias("OMOP_STANDARD_CONCEPT"),
                col("NUMBER_OF_OMOP_MATCHES").alias("OMOP_MATCH_NUMBER"),
                col("CONCEPT_DOMAIN").alias("OMOP_CONCEPT_DOMAIN"),
                col("CONCEPT_CLASS").alias("OMOP_CONCEPT_CLASS"),
                col("SNOMED_CODE"),
                col("SNOMED_TYPE"),
                col("SNOMED_MATCH_COUNT").alias("SNOMED_MATCH_NUMBER"),
                col("SNOMED_TERM"),
                
                # Update tracking
                greatest(
                    col("ce.ADC_UPDT"),
                    col("cr.ADC_UPDT"),
                    col("nom.ADC_UPDT")
                ).alias("ADC_UPDT")
            )
            
            # Add OMOP mappings
            final_df = add_manual_omop_mappings_nomen(
                result_df,
                spark.table("3_lookup.omop.barts_new_maps"),
                spark.table("3_lookup.omop.concept").filter(col("invalid_reason").isNull())
            )
            

            update_table(final_df, "4_prod.bronze.map_nomen_events", "EVENT_ID")
            update_metadata("4_prod.bronze.map_nomen_events",schema_map_nomen_events,map_nomen_events_comment)

            print("Successfully updated nomenclature events mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing nomenclature events updates: {str(e)}")
        raise


process_nomen_events_incremental()

# COMMAND ----------

map_coded_events_comment = "The table contains data related to various events and their outcomes within a system. It includes identifiers for events, encounters, and persons, as well as details about the results of these events. This data can be used for tracking event performance, analyzing outcomes, and understanding the context of different events. Use cases include reporting on event success rates, identifying trends in event types, and evaluating the contributions of different systems."

schema_map_coded_events = StructType([
    StructField("EVENT_ID", LongType(), True, metadata={
        "comment": "The unique primary identifier of the Event Table."
    }),
    StructField("ENCNTR_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the encounter table. It is an internal system assigned number."
    }),
    StructField("PERSON_ID", LongType(), True, metadata={
        "comment": "This is the value of the unique primary identifier of the person table. It is an internal system assigned number."
    }),
    StructField("ORDER_ID", LongType(), True, metadata={
        "comment": "The unique primary identifier of Order Table."
    }),
    StructField("PERFORMED_PRSNL_ID", LongType(), True, metadata={
        "comment": "Personnel id of provider who performed this result."
    }),
    StructField("RESULT_CD", IntegerType(), True, metadata={
        "comment": "Allows the use of a code value instead of a nomenclature id. The code set of the code_value is user defined."
    }),
    StructField("RESULT_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("RESULT_MEANING", StringType(), True, metadata={
        "comment": "The actual string value for the cdf meaning."
    }),
    StructField("RESULT_SET", DoubleType(), True, metadata={
        "comment": "A non-nomenclature option. Code set of result_cd if it is not null."
    }),
    StructField("EVENT_TITLE_TEXT", StringType(), True, metadata={
        "comment": "The title for document results."
    }),
    StructField("EVENT_CD", IntegerType(), True, metadata={
        "comment": "It is the code that identifies the most basic unit of the storage, i.e. RBC, discharge summary, image."
    }),
    StructField("EVENT_CD_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("CATALOG_CD", IntegerType(), True, metadata={
        "comment": "Foreign key to the order_catalog table. Catalog_cd does not exist in the code_value table and does not have a code set."
    }),
    StructField("CATALOG_DISPLAY", StringType(), True, metadata={
        "comment": "The description of the Orderable."
    }),
    StructField("CATALOG_TYPE_CD", IntegerType(), True, metadata={
        "comment": "Used to store the internal code for the catalog type. Used as a filtering mechanism for rows on theorder catalog table."
    }),
    StructField("CATALOG_TYPE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("EVENT_CLASS_CD", IntegerType(), True, metadata={
        "comment": "Coded value which specifies how the event is stored in and retrieved from the event table's sub-tables. For example, Event_Class_CDs identify events as numeric results, textual results, calculations, medications, etc."
    }),
    StructField("CONTRIBUTOR_SYSTEM_CD", IntegerType(), True, metadata={
        "comment": "Contributor system identifies the source feed of data from which a row was populated.  This is mainly used to determine how to update a set of data that may have originated from more than one source feed."
    }),
    StructField("CONTRIBUTOR_SYSTEM_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("REFERENCE_NBR", StringType(), True, metadata={
        "comment": "The combination of the reference nbr and the contributor system code provides a unique identifier to the origin of the data."
    }),
    StructField("PARENT_EVENT_ID", LongType(), True, metadata={
        "comment": "Provides a mechanism for logical grouping of events.  i.e. supergroup and group tests.  Same as event_id if current row is the highest level parent."
    }),
    StructField("NORMALCY_CD", IntegerType(), True, metadata={
        "comment": "States whether the result is normal.  This can be used to determine whether to display the event tag in different color on the flowsheet. For group results, this represents an ""overall"" normalcy. i.e. Is any result in the group abnormal?  Also allows different purge criteria to be applied based on result."
    }),
    StructField("NORMALCY_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("ENTRY_MODE_CD", IntegerType(), True, metadata={
        "comment": "Used to identify the method in which a result was entered."
    }),
    StructField("ENTRY_MODE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PERFORMED_DT_TM", TimestampType(), True, metadata={
        "comment": "Date this result was performed (or authored)."
    }),
    StructField("CLINSIG_UPDT_DT_TM", TimestampType(), True, metadata={
        "comment": "Represents the update date/time that tracks when clinically significant updates are made to the Clinical Event and should only be used to check for updates. This field is used to notify audiences when a clinically significant update is made to an existing clinical event, such as when XR Clinical Reporting re-prints a lab result due to an update of the result value or when a result is resent to a provider's Message Center with the result update. This date should NOT be displayed as the clinically."
    }),
    StructField("PARENT_EVENT_TITLE_TEXT", StringType(), True, metadata={
        "comment": "The title for document results."
    }),
    StructField("PARENT_EVENT_CD", IntegerType(), True, metadata={
        "comment": "The code value of the parent event."
    }),
    StructField("PARENT_EVENT_CD_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PARENT_CATALOG_CD", IntegerType(), True, metadata={
        "comment": "The catalog code of the parent event."
    }),
    StructField("PARENT_CATALOG_DISPLAY", StringType(), True, metadata={
        "comment": "The description of the Orderable."
    }),
    StructField("PARENT_CATALOG_TYPE_CD", IntegerType(), True, metadata={
        "comment": "The internal code for the catalog type of the parent event."
    }),
    StructField("PARENT_CATALOG_TYPE_DISPLAY", StringType(), True, metadata={
        "comment": "The description for the code value."
    }),
    StructField("PARENT_REFERENCE_NBR", StringType(), True, metadata={
        "comment": "The combination of the reference nbr and the contributor system code provides a unique identifier to the origin of the data."
    }),
    StructField("ADC_UPDT", TimestampType(), True, metadata={
        "comment": "Timestamp of last update."
    }),
    StructField("OMOP_MANUAL_TABLE", StringType(), True, metadata={
        "comment": "The name of the OMOP Common Data Model table."
    }),
    StructField("OMOP_MANUAL_COLUMN", StringType(), True, metadata={
        "comment": "The field of the OMOP Common Data Model table."
    }),
    StructField("OMOP_MANUAL_CONCEPT", StringType(), True, metadata={
        "comment": "The concept_id of the OMOP Common Data Model table."
    }),
    StructField("OMOP_MANUAL_VALUE_CONCEPT", StringType(), True, metadata={
        "comment": "The manually mapped OMOP concept ID representing the value of a text for standardized use in the OMOP CDM."
    }),
    StructField("OMOP_MANUAL_CONCEPT_NAME", StringType(), True, metadata={
        "comment": "The name or description of the OMOP concept id."
    }),
    StructField("OMOP_MANUAL_STANDARD_CONCEPT", StringType(), True, metadata={
        "comment": "This flag determines where a Concept is a Standard Concept, i.e. is used in the data, a Classification Concept, or a non-standard Source Concept. The allowables values are S (Standard Concept) and C (Classification Concept), otherwise the content is NULL."
    }),
    StructField("OMOP_MANUAL_CONCEPT_DOMAIN", StringType(), True, metadata={
        "comment": "A unique identifier for the domain."
    }),
    StructField("OMOP_MANUAL_CONCEPT_CLASS", StringType(), True, metadata={
        "comment": "The identifier for the class or category of the OMOP concept."
    })

])


def create_coded_event_code_lookup(code_values, alias_suffix):
    """
    Creates a code value lookup specific to coded events.
    
    Args:
        code_values: DataFrame containing code values
        alias_suffix: Suffix for column aliases
        
    Returns:
        DataFrame: Lookup table with standardized columns
    """
    return code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(f"{alias_suffix}_desc"),
        col("DISPLAY").alias(f"{alias_suffix}_display"),
        col("CDF_MEANING").alias(f"{alias_suffix}_meaning")
    ).alias(alias_suffix)

def add_manual_omop_mappings_coded(df, barts_mapfile, concepts):
    """
    Adds manual OMOP mappings specific to coded events.
    
    Args:
        df: Source DataFrame
        barts_mapfile: Manual mapping reference table
        concepts: OMOP concepts reference table
    """
    # Domain priority dictionary
    domain_priority = {
        "Drug": 1,
        "Measurement": 2,
        "Procedure": 3,
        "Condition": 4,
        "Device": 5,
        "Observation": 6
    }
    
    domain_priority_udf = udf(lambda x: domain_priority.get(x, 999), IntegerType())
    
    # Process concept mappings
    concept_maps = (
        barts_mapfile
        .filter(
            (col("SourceTable") == "dbo.PI_CDE_CLINICAL_EVENT") &
            (
                (col("SourceField").isin("EVENT_CD", "EVENT_RESULT_TXT")) &
                col("OMOPField").isin("measurement_concept_id", "observation_concept_id")
            )
        )
        .select(
            col("SourceValue"),
            col("OMOPTable").alias("OMOP_MANUAL_TABLE"),
            col("OMOPField").alias("OMOP_MANUAL_COLUMN"),
            col("OMOPConceptId").alias("OMOP_MANUAL_CONCEPT"),
            col("EVENT_CD").alias("MAP_EVENT_CD"),
            col("EVENT_CLASS_CD").alias("MAP_EVENT_CLASS_CD"),
            col("EVENT_RESULT_TXT").alias("MAP_EVENT_RESULT_TXT"),
            col("SourceField"),
            col("STANDARD_CONCEPT")
        )
    )
    
    # Process value concept mappings
    value_concept_maps = (
        barts_mapfile
        .filter(
            (col("SourceTable") == "dbo.PI_CDE_CLINICAL_EVENT") &
            (
                (col("SourceField").isin("EVENT_RESULT_TXT", "EVENT_CD")) &
                (col("OMOPField") == "value_as_concept_id")
            )
        )
        .select(
            col("SourceValue"),
            col("OMOPConceptId").alias("OMOP_MANUAL_VALUE_CONCEPT"),
            col("EVENT_CD").alias("MAP_EVENT_CD"),
            col("EVENT_CLASS_CD").alias("MAP_EVENT_CLASS_CD"),
            col("EVENT_RESULT_TXT").alias("MAP_EVENT_RESULT_TXT"),
            col("SourceField")
        )
    )
    
    # Get concept details
    concept_details = (
        concepts
        .select(
            col("concept_id"),
            col("concept_name").alias("OMOP_MANUAL_CONCEPT_NAME"),
            col("standard_concept").alias("OMOP_MANUAL_STANDARD_CONCEPT"),
            col("domain_id").alias("OMOP_MANUAL_CONCEPT_DOMAIN"),
            col("concept_class_id").alias("OMOP_MANUAL_CONCEPT_CLASS")
        )
    )
    
    return (
        df
        # Join with concept mappings
        .join(
            concept_maps,
            (
                (
                    (concept_maps.SourceField == "EVENT_CD") &
                    (df.EVENT_CD.cast("string") == concept_maps.SourceValue) &
                    (concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == concept_maps.MAP_EVENT_CLASS_CD)) &
                    (concept_maps.MAP_EVENT_RESULT_TXT.isNull() | (df.RESULT_DISPLAY == concept_maps.MAP_EVENT_RESULT_TXT))
                ) |
                (
                    (concept_maps.SourceField == "EVENT_RESULT_TXT") &
                    (df.RESULT_DISPLAY == concept_maps.SourceValue) &
                    (concept_maps.MAP_EVENT_CD.isNull() | (df.EVENT_CD.cast("string") == concept_maps.MAP_EVENT_CD)) &
                    (concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == concept_maps.MAP_EVENT_CLASS_CD))
                )
            ),
            "left"
        )
        # Join with value concept mappings
        .join(
            value_concept_maps,
            (
                (
                    (value_concept_maps.SourceField == "EVENT_RESULT_TXT") &
                    (df.RESULT_DISPLAY == value_concept_maps.SourceValue) &
                    (value_concept_maps.MAP_EVENT_CD.isNull() | (df.EVENT_CD.cast("string") == value_concept_maps.MAP_EVENT_CD)) &
                    (value_concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == value_concept_maps.MAP_EVENT_CLASS_CD))
                ) |
                (
                    (value_concept_maps.SourceField == "EVENT_CD") &
                    (df.EVENT_CD.cast("string") == value_concept_maps.SourceValue) &
                    (value_concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == value_concept_maps.MAP_EVENT_CLASS_CD)) &
                    (value_concept_maps.MAP_EVENT_RESULT_TXT.isNull() | (df.RESULT_DISPLAY == value_concept_maps.MAP_EVENT_RESULT_TXT))
                )
            ),
            "left"
        )
        # Join with concept details
        .join(
            concept_details,
            col("OMOP_MANUAL_CONCEPT") == concept_details.concept_id,
            "left"
        )
        .withColumn(
            "domain_priority",
            domain_priority_udf(col("OMOP_MANUAL_CONCEPT_DOMAIN"))
        )
        .withColumn(
            "row_num",
            row_number().over(
                Window.partitionBy("EVENT_ID")
                .orderBy(
                    col("OMOP_MANUAL_STANDARD_CONCEPT").desc_nulls_last(),
                    col("domain_priority").asc_nulls_last(),
                    col("OMOP_MANUAL_CONCEPT").asc_nulls_last()
                )
            )
        )
        .filter(col("row_num") == 1)
        .drop(
            "concept_id",
            "SourceValue",
            "MAP_EVENT_CD",
            "MAP_EVENT_CLASS_CD",
            "MAP_EVENT_RESULT_TXT",
            "SourceField",
            "STANDARD_CONCEPT",
            "domain_priority",
            "row_num"
        )
    )

def process_coded_events_incremental():
    """
    Main function to process incremental coded events updates.
    Handles the end-to-end process of updating the coded events mapping table.
    """
    try:
        print(f"Starting coded events incremental processing at {datetime.now()}")
        

        max_adc_updt = get_max_timestamp("4_prod.bronze.map_coded_events")
        
        # Get base tables with filtering
        coded_results = (
            spark.table("4_prod.raw.mill_ce_coded_result")
            .filter(
                (col("VALID_UNTIL_DT_TM") > current_timestamp()) &
                (col("ADC_UPDT") > max_adc_updt)
            )
            .alias("cr")
        )
        
        if coded_results.count() > 0:
            print(f"Processing {coded_results.count()} updated records")
            
            clinical_events = (
                spark.table("4_prod.raw.mill_clinical_event")
                .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
                .withColumn(
                    "row_rank",
                    row_number().over(
                        Window.partitionBy("EVENT_ID")
                        .orderBy(col("UPDT_CNT").desc())
                    )
                )
                .filter(col("row_rank") == 1)
                .drop("row_rank")
                .alias("ce")
            )
            
            parent_events = (
                spark.table("4_prod.raw.mill_clinical_event")
                .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
                .withColumn(
                    "row_rank",
                    row_number().over(
                        Window.partitionBy("EVENT_ID")
                        .orderBy(col("UPDT_CNT").desc())
                    )
                )
                .filter(col("row_rank") == 1)
                .drop("row_rank")
                .alias("pe")
            )
            
            # Get reference tables
            code_values = spark.table("3_lookup.mill.mill_code_value")
            order_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("oc")
            parent_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("poc")
            
            # Create code value lookups
            event_cd_lookup = create_coded_event_code_lookup(code_values, "event")
            parent_event_cd_lookup = create_coded_event_code_lookup(code_values, "parent_event")
            normalcy_lookup = create_coded_event_code_lookup(code_values, "normalcy")
            contrib_sys_lookup = create_coded_event_code_lookup(code_values, "contrib")
            entry_mode_lookup = create_coded_event_code_lookup(code_values, "entry")
            catalog_type_lookup = create_coded_event_code_lookup(code_values, "cat_type")
            parent_catalog_type_lookup = create_coded_event_code_lookup(code_values, "parent_cat_type")
            result_cd_lookup = create_coded_event_code_lookup(code_values, "result")
            
            # Get valid coded results
            coded_result_rows = coded_results.filter(
                (col("RESULT_CD").isNotNull()) & 
                (col("RESULT_CD") != 0) &
                (col("VALID_UNTIL_DT_TM") > current_timestamp())
            )
            
            # Build result DataFrame with all joins
            result_df = (
                coded_result_rows
                # Join with main clinical event
                .join(
                    clinical_events,
                    (coded_result_rows.EVENT_ID == clinical_events.EVENT_ID) &
                    (col("ce.VALID_UNTIL_DT_TM") > current_timestamp()),
                    "inner"
                )
                # Join with parent event
                .join(
                    parent_events.select(
                        "EVENT_ID",
                        "EVENT_TITLE_TEXT",
                        "EVENT_CD",
                        "CATALOG_CD",
                        "REFERENCE_NBR"
                    ),
                    col("ce.PARENT_EVENT_ID") == col("pe.EVENT_ID"),
                    "left"
                )
                # Join with order catalogs
                .join(
                    order_catalog.select(
                        "CATALOG_CD",
                        "CATALOG_TYPE_CD",
                        "DESCRIPTION"
                    ),
                    col("ce.CATALOG_CD") == col("oc.CATALOG_CD"),
                    "left"
                )
                .join(
                    parent_catalog.select(
                        "CATALOG_CD",
                        "CATALOG_TYPE_CD",
                        "DESCRIPTION"
                    ),
                    col("pe.CATALOG_CD") == col("poc.CATALOG_CD"),
                    "left"
                )
                # Add all code value lookups
                .join(event_cd_lookup, 
                      col("ce.EVENT_CD") == col("event.CODE_VALUE"), 
                      "left")
                .join(parent_event_cd_lookup, 
                      col("pe.EVENT_CD") == col("parent_event.CODE_VALUE"), 
                      "left")
                .join(normalcy_lookup, 
                      col("NORMALCY_CD") == col("normalcy.CODE_VALUE"), 
                      "left")
                .join(contrib_sys_lookup, 
                      col("CONTRIBUTOR_SYSTEM_CD") == col("contrib.CODE_VALUE"), 
                      "left")
                .join(entry_mode_lookup, 
                      col("ENTRY_MODE_CD") == col("entry.CODE_VALUE"), 
                      "left")
                .join(catalog_type_lookup, 
                      col("oc.CATALOG_TYPE_CD") == col("cat_type.CODE_VALUE"), 
                      "left")
                .join(parent_catalog_type_lookup, 
                      col("poc.CATALOG_TYPE_CD") == col("parent_cat_type.CODE_VALUE"), 
                      "left")
                .join(result_cd_lookup,
                      col("cr.RESULT_CD") == col("result.CODE_VALUE"),
                      "left")
            )
            
            # Select final columns
            result_df = result_df.select(
                # IDs
                col("ce.EVENT_ID").cast(LongType()),
                col("ce.ENCNTR_ID").cast(LongType()),
                col("ce.PERSON_ID").cast(LongType()),
                col("ce.ORDER_ID").cast(LongType()),
                col("ce.PERFORMED_PRSNL_ID").cast(LongType()),
                
                # Result details
                col("cr.RESULT_CD").cast(IntegerType()),
                col("result_desc").alias("RESULT_DISPLAY"),
                col("result_meaning").alias("RESULT_MEANING"),
                col("cr.RESULT_SET"),
                
                # Main event details
                col("ce.EVENT_TITLE_TEXT"),
                col("ce.EVENT_CD").cast(IntegerType()),
                col("event_desc").alias("EVENT_CD_DISPLAY"),
                col("ce.CATALOG_CD").cast(IntegerType()),
                col("oc.DESCRIPTION").alias("CATALOG_DISPLAY"),
                col("oc.CATALOG_TYPE_CD").cast(IntegerType()),
                col("cat_type_desc").alias("CATALOG_TYPE_DISPLAY"),
                col("ce.EVENT_CLASS_CD").cast(IntegerType()),
                
                # Additional event attributes
                col("ce.CONTRIBUTOR_SYSTEM_CD").cast(IntegerType()),
                col("contrib_desc").alias("CONTRIBUTOR_SYSTEM_DISPLAY"),
                col("ce.REFERENCE_NBR"),
                col("ce.PARENT_EVENT_ID").cast(LongType()),
                col("ce.NORMALCY_CD").cast(IntegerType()),
                col("normalcy_desc").alias("NORMALCY_DISPLAY"),
                col("ce.ENTRY_MODE_CD").cast(IntegerType()),
                col("entry_desc").alias("ENTRY_MODE_DISPLAY"),
                
                # Timestamps
                col("ce.PERFORMED_DT_TM").cast(TimestampType()),
                col("ce.CLINSIG_UPDT_DT_TM").cast(TimestampType()),
                
                # Parent event details
                col("pe.EVENT_TITLE_TEXT").alias("PARENT_EVENT_TITLE_TEXT"),
                col("pe.EVENT_CD").cast(IntegerType()).alias("PARENT_EVENT_CD"),
                col("parent_event_desc").alias("PARENT_EVENT_CD_DISPLAY"),
                col("pe.CATALOG_CD").cast(IntegerType()).alias("PARENT_CATALOG_CD"),
                col("poc.DESCRIPTION").alias("PARENT_CATALOG_DISPLAY"),
                col("poc.CATALOG_TYPE_CD").cast(IntegerType()).alias("PARENT_CATALOG_TYPE_CD"),
                col("parent_cat_type_desc").alias("PARENT_CATALOG_TYPE_DISPLAY"),
                col("pe.REFERENCE_NBR").alias("PARENT_REFERENCE_NBR"),
                
                # Update tracking
                greatest(
                    col("ce.ADC_UPDT"),
                    col("cr.ADC_UPDT")
                ).alias("ADC_UPDT")
            )
            
            # Add OMOP mappings
            final_df = add_manual_omop_mappings_coded(
                result_df,
                spark.table("3_lookup.omop.barts_new_maps"),
                spark.table("3_lookup.omop.concept").filter(col("invalid_reason").isNull())
            )
            

            update_table(final_df, "4_prod.bronze.map_coded_events", "EVENT_ID")
            update_metadata("4_prod.bronze.map_coded_events",schema_map_coded_events,map_coded_events_comment)
            print("Successfully updated coded events mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing coded events updates: {str(e)}")
        raise


process_coded_events_incremental()
