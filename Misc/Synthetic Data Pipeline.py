# Databricks notebook source
# MAGIC %pip install sdv

# COMMAND ----------

from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer
import time
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import col, rand, unix_timestamp, lit, when, substring

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# COMMAND ----------

def table_exist_check(table_name):
    try:
        result = spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
        return result.first() is not None
    except:
        return False

# COMMAND ----------

def align_datatypes_with_schema(synthetic_data, original_schema):
    """
    Aligns datatypes in synthetic_data with the original Spark schema.
    """
    print("Aligning datatypes with original schema...")
    synthetic_data = synthetic_data.copy()
    
    for field in original_schema.fields:
        column_name = field.name
        if column_name not in synthetic_data.columns:
            print(f"Warning: Column {column_name} not found in synthetic data")
            continue
            
        print(f"\nProcessing column: {column_name}")
        print(f"Original type: {field.dataType}")
        print(f"Current synthetic type: {synthetic_data[column_name].dtype}")
        
        try:
            # Handle DateType or TimestampType
            if isinstance(field.dataType, (DateType, TimestampType)):
                synthetic_data[column_name] = pd.to_datetime(synthetic_data[column_name], errors='coerce')
                
                # Ensure dates are within valid range
                min_date = pd.Timestamp('1970-01-01')
                max_date = pd.Timestamp('2262-04-11')
                
                synthetic_data.loc[synthetic_data[column_name] < min_date, column_name] = min_date
                synthetic_data.loc[synthetic_data[column_name] > max_date, column_name] = max_date
                
                print(f"Converted {column_name} to datetime")
                
            # Handle StringType
            elif isinstance(field.dataType, StringType):
                # Convert to string, handling NaN values
                synthetic_data[column_name] = synthetic_data[column_name].fillna('')
                synthetic_data[column_name] = synthetic_data[column_name].astype(str)
                # Replace 'nan' strings with empty string
                synthetic_data.loc[synthetic_data[column_name] == 'nan', column_name] = ''
                
            # Handle IntegerType
            elif isinstance(field.dataType, IntegerType):
                # Fill NaN with 0 or another appropriate value
                synthetic_data[column_name] = synthetic_data[column_name].fillna(0)
                synthetic_data[column_name] = synthetic_data[column_name].astype('int32')
                
            # Handle LongType
            elif isinstance(field.dataType, LongType):
                # Fill NaN with 0 or another appropriate value
                synthetic_data[column_name] = synthetic_data[column_name].fillna(0)
                synthetic_data[column_name] = synthetic_data[column_name].astype('int64')
                
            # Handle FloatType or DoubleType
            elif isinstance(field.dataType, (FloatType, DoubleType)):
                synthetic_data[column_name] = synthetic_data[column_name].astype('float64')
                
            # Handle BooleanType
            elif isinstance(field.dataType, BooleanType):
                synthetic_data[column_name] = synthetic_data[column_name].astype('bool')
                
            print(f"New type: {synthetic_data[column_name].dtype}")
            print("Sample values after conversion:")
            print(synthetic_data[column_name].head())
            
        except Exception as e:
            print(f"Error converting column {column_name}: {str(e)}")
            print("Attempting fallback conversion...")
            try:
                # Fallback to string conversion for problematic columns
                synthetic_data[column_name] = synthetic_data[column_name].fillna('')
                synthetic_data[column_name] = synthetic_data[column_name].astype(str)
                synthetic_data.loc[synthetic_data[column_name] == 'nan', column_name] = ''
                print("Fallback conversion successful")
            except Exception as e2:
                print(f"Fallback conversion failed: {str(e2)}")
            
    return synthetic_data

# COMMAND ----------

def generate_synthetic_data(catalog, schema, table_prefix, overwrite=True):
    """
    Generate synthetic data for tables matching the prefix in the specified catalog and schema.
    
    Parameters:
    catalog (str): The catalog name
    schema (str): The schema name
    table_prefix (str): The prefix of table names to process
    overwrite (bool): If False, skip tables that already exist in destination
    """
    
    # Get list of tables matching the prefix
    tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    matching_tables = tables_df.filter(f"tableName LIKE '{table_prefix}%'").select("tableName").collect()
    
    for table_row in matching_tables:
        table_name = table_row['tableName']
        
        try:
            # Check if table exists in destination
            destination_table = f"a_synth.{schema}.{table_name}"
            table_exists = False
            if(table_exist_check(destination_table)):
                table_exists = True

            # Skip if table exists and overwrite is False
            if table_exists and not overwrite:
                print(f"Skipping {table_name} as it already exists and overwrite=False")
                continue
                
            print(f"Processing table: {table_name}")
            
            # Load source data
            source_df = spark.table(f"{catalog}.{schema}.{table_name}")

            timestamp_cols = [f.name for f in source_df.schema.fields 
                            if isinstance(f.dataType, TimestampType)]
            
            text_cols = [f.name for f in source_df.schema.fields 
             if isinstance(f.dataType, StringType)]
            
            binary_cols = [f.name for f in source_df.schema.fields 
               if isinstance(f.dataType, BinaryType)]
            # Sample up to 100k random rows
            sampled_df = source_df.orderBy(rand()).limit(100000) 


            if timestamp_cols:
                for ts_col in timestamp_cols:
                    sampled_df = sampled_df.withColumn(
                        ts_col,
                    when(
                        (col(ts_col).isNotNull()) &
                        (unix_timestamp(col(ts_col)) >= unix_timestamp(lit('1677-09-21'))) &
                        (unix_timestamp(col(ts_col)) <= unix_timestamp(lit('2262-04-11'))),
                        col(ts_col)
                    ).otherwise(lit(None))
                    )

            # Handle text columns - trim to 256 characters
            if text_cols:
                for col_name in text_cols:
                    sampled_df = sampled_df.withColumn(
                        col_name,
                        when(
                            col(col_name).isNotNull(),
                            substring(col(col_name), 1, 256)
                        ).otherwise(lit(None))
                    )

            # Handle binary columns - set to null
            if binary_cols:
                for col_name in binary_cols:
                    sampled_df = sampled_df.withColumn(
                        col_name,
                        lit(None)
                    )

            # Convert to pandas
            pdf = sampled_df.toPandas()
            print(len(pdf))

            # Initialize metadata
            pdf = clean_boolean_data(pdf)
            metadata = SingleTableMetadata()
            metadata.detect_from_dataframe(pdf)
            
            # Get sdtypes for this table
            sdtypes_df = spark.table("6_mgmt.default.sdtypes") \
                             .where(f"table_name = '{table_name}'") \
                             .toPandas()
            
            # Create dictionaries for sdtype and pii
            sdtype_dict = dict(zip(sdtypes_df['column_name'], sdtypes_df['sd_type']))
            pii_dict = {k: bool(v) for k, v in zip(sdtypes_df['column_name'], sdtypes_df['PII'])}

            primary_key = metadata.primary_key
            primary_key_metadata = metadata.columns.get(primary_key, {}).copy() if primary_key else None
            
            # Update metadata for each column
            for column_name, sdtype in sdtype_dict.items():
                try:
                    if column_name in metadata.columns:
                        if column_name == primary_key:
                            continue
                        is_pii = pii_dict.get(column_name, False)
                        
                        # If PII is True, always set as unknown type with pii=True
                        if is_pii:
                            metadata.update_column(
                                column_name=column_name,
                                sdtype='unknown',
                                pii=True
                            )
                        else:
                            column_metadata = metadata.columns[column_name]
                            current_sdtype = column_metadata.get('sdtype', None)
                            if current_sdtype != sdtype:
                                if sdtype == 'boolean':
                                    if is_actually_boolean(pdf[column_name]):
                                        metadata.update_column(
                                            column_name=column_name,
                                            sdtype=sdtype
                                        )
                    
                                elif sdtype == 'datetime':
                                    if is_actually_datetime(pdf[column_name]):
                                        datetime_format = detect_datetime_format(pdf[column_name])
                                        if datetime_format:
                                            metadata.update_column(
                                                column_name=column_name,
                                                sdtype=sdtype,
                                                datetime_format=datetime_format
                                            )
                                        else:
                                            metadata.update_column(
                                                column_name=column_name,
                                                sdtype='unknown',
                                                pii=False
                                            )
                    
                                elif sdtype in ['numerical', 'integer']:
                                    if is_actually_numerical(pdf[column_name]):
                                        metadata.update_column(
                                            column_name=column_name,
                                            sdtype=sdtype
                                        )
                                elif sdtype == 'unknown':
                                    metadata.update_column(
                                        column_name=column_name,
                                        sdtype=sdtype,
                                        pii=False
                                    )
                                else:
                                    metadata.update_column(
                                        column_name=column_name,
                                        sdtype=sdtype
                                    )
                except Exception as e:
                    print(f"Error updating column {column_name}: {repr(e)}")

            for column_name, column_metadata in metadata.columns.items():
                if column_metadata.get('sdtype') in ['numerical', 'integer']:
                    try:
                        # Convert to numeric, setting invalid values to NaN
                        pdf[column_name] = pd.to_numeric(pdf[column_name], errors='coerce')
                    except Exception as e:
                        print(f"Error cleaning numerical column {column_name}: {repr(e)}")

            # Validate metadata
            metadata.validate()
            metadata.validate_data(data=pdf)
            
            # Generate synthetic data
            synthesizer = GaussianCopulaSynthesizer(metadata)
            synthesizer.fit(pdf)
            synthetic_data = synthesizer.sample(num_rows=10000)

            # Before converting synthetic data to Spark DataFrame, get original schema
            original_schema = source_df.schema
            synthetic_data = align_datatypes_with_schema(synthetic_data, original_schema)

            # Convert synthetic data to Spark DataFrame with original schema
            synthetic_spark_df = spark.createDataFrame(synthetic_data, schema=original_schema)
            # Convert back to Spark DataFrame
            #synthetic_spark_df = spark.createDataFrame(synthetic_data)
            print("Dataframe created")
            # Save to destination
            synthetic_spark_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite") \
                .saveAsTable(f"a_synth.{schema}.{table_name}")
            
            print(f"Successfully processed {table_name}")
            
        except Exception as e:
            print(f"Error processing table {table_name}: {repr(e)}")
            continue

def is_actually_boolean(series):
    """
    Check if a series contains boolean-like values
    """
    # Drop NA values and convert to string
    clean_series = series.dropna().astype(str).str.lower()
    
    # Define valid boolean values
    valid_boolean_values = {'true', 'false', '1', '0', 't', 'f', 'yes', 'no', 'y', 'n'}
    
    # Check if all non-null values are valid boolean representations
    return all(val in valid_boolean_values for val in clean_series.unique())

def clean_boolean_data(df):
    """
    Clean boolean columns in the dataframe by converting various boolean representations to True/False
    """
    for column in df.columns:
        try:
            # Check if column contains boolean-like values
            if df[column].dtype == bool or (
                df[column].dtype in [object, str] and 
                is_actually_boolean(df[column])
            ):
                # Create a mapping dictionary for boolean conversion
                bool_map = {
                    'true': True, 
                    't': True, 
                    'yes': True, 
                    'y': True, 
                    '1': True, 
                    'false': False, 
                    'f': False, 
                    'no': False, 
                    'n': False, 
                    '0': False
                }
                
                # Convert to string, lowercase, and map to boolean
                df[column] = df[column].astype(str).str.lower().map(bool_map)
                
                # Fill any NaN values that resulted from the mapping with None
                df[column] = df[column].where(pd.notnull(df[column]), None)
                
        except Exception as e:
            print(f"Warning: Could not clean boolean column {column}: {str(e)}")
            continue
            
    return df

def is_actually_numerical(series):
    try:
        # Drop NA values and convert to numeric
        numeric_series = pd.to_numeric(series.dropna(), errors='raise')
        # Check if all values are finite (not inf or -inf)
        return np.all(np.isfinite(numeric_series))
    except:
        return False
    
def detect_datetime_format(series):
    """
    Detect the datetime format of a series and return the strftime format string.
    Returns None if format cannot be detected.
    """
    # Common datetime formats to check
    formats = [
        '%Y-%m-%d %H:%M:%S',  # 2023-01-01 12:34:56
        '%Y-%m-%d %H:%M:%S.%f',  # 2023-01-01 12:34:56.789
        '%Y-%m-%d',  # 2023-01-01
        '%m/%d/%Y %H:%M:%S',  # 01/01/2023 12:34:56
        '%m/%d/%Y',  # 01/01/2023
        '%Y%m%d',  # 20230101
        '%Y%m%d%H%M%S'  # 20230101123456
    ]
    
    # Get a sample of non-null values
    sample = series.dropna().astype(str).unique()
    if len(sample) == 0:
        return None
    
    # Try each format
    for fmt in formats:
        try:
            # Try to parse all unique values with the current format
            all_valid = all(bool(datetime.strptime(str(x), fmt)) for x in sample[:10])  # Test first 10 values
            if all_valid:
                return fmt
        except ValueError:
            continue
    
    return None

def is_actually_datetime(series):
    try:
        pd.to_datetime(series.dropna(), errors='raise')
        return True
    except:
        return False
    
def is_valid_timestamp(ts):
    """Check if timestamp is within pandas valid range"""
    try:
        min_ts = pd.Timestamp.min
        max_ts = pd.Timestamp.max
        return min_ts <= pd.Timestamp(ts) <= max_ts
    except:
        return False

# COMMAND ----------

generate_synthetic_data("4_prod", "raw", "mill_", False)
generate_synthetic_data("4_prod", "dlt", "omop_", False)
generate_synthetic_data("4_prod", "rde", "rde_", False)


# COMMAND ----------

def convert_binary_columns_to_string(catalog, schema, table_prefix):
    """
    Convert binary columns to string type for tables matching the prefix in the specified catalog and schema.
    
    Parameters:
    catalog (str): The catalog name
    schema (str): The schema name 
    table_prefix (str): The prefix of table names to process
    """
    
    # Get list of tables matching the prefix
    tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    matching_tables = tables_df.filter(f"tableName LIKE '{table_prefix}%'").select("tableName").collect()
    
    for table_row in matching_tables:
        table_name = table_row['tableName']
        
        try:
            print(f"Checking table: {table_name}")
            
            # Load table
            df = spark.table(f"{catalog}.{schema}.{table_name}")
            
            # Get binary columns
            binary_cols = [f.name for f in df.schema.fields 
                          if isinstance(f.dataType, BinaryType)]
            
            if binary_cols:
                print(f"Found binary columns in {table_name}: {binary_cols}")
                
                # Create new columns with string type
                for col_name in binary_cols:
                    df = df.withColumn(
                        col_name,
                        col(col_name).cast("string")
                    )
                
                # Save back to table with updated schema
                df.write.format("delta").option("overwriteSchema", "true").mode("overwrite") \
                    .saveAsTable(f"{catalog}.{schema}.{table_name}")
                
                print(f"Successfully converted binary columns in {table_name}")
            
        except Exception as e:
            print(f"Error processing table {table_name}: {repr(e)}")
            continue



# COMMAND ----------

# Usage example:
convert_binary_columns_to_string("a_synth", "raw", "mill")
convert_binary_columns_to_string("a_synth", "dlt", "omop")
convert_binary_columns_to_string("a_synth", "rde", "rde")



# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def create_synthetic_schema_tables(source_catalog, schema, table_prefix):
    """
    Create schema tables for each source schema, saved in the corresponding synthetic data location
    with only tables matching the specified prefix.
    
    Parameters:
    source_catalog (str): The source catalog name
    schema (str): The source schema name
    table_prefix (str): The prefix of table names to process
    """
    
    try:
        # Get schema information for tables matching the prefix
        schema_query = f"""
        SELECT 
            table_name,
            column_name,
            data_type,
            is_nullable,
            COALESCE(comment, '') as column_comment
        FROM {source_catalog}.information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name LIKE '{table_prefix}%'
        ORDER BY table_name, column_name
        """
        
        schema_df = spark.sql(schema_query)
        
        # Add timestamp column
        schema_df = schema_df.withColumn(
            "created_at", 
            current_timestamp()
        )
        
        # Define the schema table name
        schema_table_name = f"{table_prefix}schema"
        
        # Write the schema information to the destination
        schema_df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(f"a_synth.{schema}.{schema_table_name}")
        
        print(f"Successfully created schema table: a_synth.{schema}.{schema_table_name}")
            
    except Exception as e:
        print(f"Error creating schema table: {repr(e)}")

# Example usage:
# After running generate_synthetic_data, call:
create_synthetic_schema_tables("4_prod", "raw", "mill_")
create_synthetic_schema_tables("4_prod", "dlt", "omop_")
create_synthetic_schema_tables("4_prod", "rde", "rde_")
