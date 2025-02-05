# Databricks notebook source
# MAGIC %pip install sdv

# COMMAND ----------

from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer
import time
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col, rand, unix_timestamp, lit

# COMMAND ----------

def generate_synthetic_data(catalog, schema, table_prefix):
    """
    Generate synthetic data for tables matching the prefix in the specified catalog and schema.
    
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
            print(f"Processing table: {table_name}")
            
            # Load source data
            source_df = spark.table(f"{catalog}.{schema}.{table_name}")

            timestamp_cols = [f.name for f in source_df.schema.fields 
                            if isinstance(f.dataType, TimestampType)]
            
            
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


            # Convert to pandas
            pdf = sampled_df.toPandas()
            print(len(pdf))
            
            # Initialize metadata
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
            
            # Validate metadata
            metadata.validate()
            metadata.validate_data(data=pdf)
            
            # Generate synthetic data
            synthesizer = GaussianCopulaSynthesizer(metadata)
            synthesizer.fit(pdf)
            synthetic_data = synthesizer.sample(num_rows=10000)

            
            # Convert back to Spark DataFrame
            synthetic_spark_df = spark.createDataFrame(synthetic_data)
            
            # Save to destination
            synthetic_spark_df.write.format("delta").mode("overwrite") \
                .saveAsTable(f"a_synth.{schema}.{table_name}")
            
            print(f"Successfully processed {table_name}")
            
        except Exception as e:
            print(f"Error processing table {table_name}: {repr(e)}")
            continue

def is_actually_boolean(series):
    # Convert to string and check if only 'True' or 'False' values
    unique_values = set(series.dropna().astype(str).str.lower().unique())
    return unique_values.issubset({'true', 'false'})

    

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

generate_synthetic_data("4_prod", "raw", "mill_")
generate_synthetic_data("4_prod", "dlt", "omop_")
generate_synthetic_data("4_prod", "rde", "rde_")

