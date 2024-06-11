# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
from pyspark.sql.utils import AnalysisException
import time

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CopyDeltaTables") \
    .getOrCreate()

# Specify the source and target catalogues and schema
source_catalogue = "4_prod"
source_schema = "raw"
target_catalogue = "8_dev"
target_schema = "raw"

# Delete all tables in the target schema
tables_to_delete = spark.catalog.listTables(f"{target_catalogue}.{target_schema}")
for table_to_delete in tables_to_delete:
    spark.sql(f"DROP TABLE IF EXISTS {target_catalogue}.{target_schema}.{table_to_delete.name}")

# Get the list of tables in the source catalogue and schema
tables = spark.catalog.listTables(f"{source_catalogue}.{source_schema}")

# Iterate over each table
for table in tables:
    # Get the table name
    table_name = table.name
    
    # Read the source table with retry logic
    max_retries = 3
    retry_delay = 60  # seconds
    
    for attempt in range(max_retries):
        try:
            source_table = spark.table(f"{source_catalogue}.{source_schema}.{table_name}")
            break
        except AnalysisException as e:
            if attempt < max_retries - 1:
                print(f"Error reading table {table_name}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Error reading table {table_name} after {max_retries} attempts. Skipping...")
                continue
    
    # Select 1000 random rows from the source table with retry logic
    for attempt in range(max_retries):
        try:
            sampled_table = source_table.orderBy(rand()).limit(1000)
            break
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Error sampling table {table_name}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Error sampling table {table_name} after {max_retries} attempts. Skipping...")
                continue
    
    # Write the sampled table to the target catalogue and schema, overwriting if exists, with retry logic
    for attempt in range(max_retries):
        try:
            sampled_table.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(f"{target_catalogue}.{target_schema}.{table_name}")
            print(f"Copied table {table_name} from {source_catalogue}.{source_schema} to {target_catalogue}.{target_schema}")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Error writing table {table_name}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Error writing table {table_name} after {max_retries} attempts. Skipping...")
                continue

# Stop the SparkSession
spark.stop()
