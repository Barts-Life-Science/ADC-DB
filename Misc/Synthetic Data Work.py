# Databricks notebook source
# MAGIC %pip install sdv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 6_mgmt.default.sdtypes WHERE table_name = 'mill_address'

# COMMAND ----------


from pyspark.sql.functions import rand
import pandas as pd
import numpy as np
from sdv.metadata import SingleTableMetadata
from datetime import datetime

# Specify the catalog, schema, and table name
catalog = "4_prod"
schema = "raw"
table_name = "mill_organization"

# Construct the path to the Delta table
delta_table_path = f"{catalog}.{schema}.{table_name}"

# Load the Delta table as a Spark DataFrame
df = spark.table(f"{catalog}.{schema}.{table_name}")


#Take only up to 100k  rows, selected randomly.
sdf = df.orderBy(rand()).limit(1000)

# Convert the Spark DataFrame to a pandas DataFrame
pdf = sdf.toPandas()



# COMMAND ----------

from sdv.metadata import SingleTableMetadata

metadata = SingleTableMetadata()

metadata.detect_from_dataframe(pdf)

print(metadata)
#metadata.visualize()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct(sd_type) FROM 6_mgmt.default.sdtypes

# COMMAND ----------

# First, let's get the sdtypes data into a DataFrame
sdtypes_df = spark.table("6_mgmt.default.sdtypes").where("table_name = 'mill_clinical_event'").toPandas()

# Create a dictionary of column_name to sdtype
sdtype_dict = dict(zip(sdtypes_df['column_name'], sdtypes_df['sd_type']))

# Create a dictionary of column_name to pii status
pii_dict = {k: bool(v) for k, v in zip(sdtypes_df['column_name'], sdtypes_df['PII'])}

# Function to check if data is actually boolean or datetime
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
    
# Update the metadata for each column
for column_name, sdtype in sdtype_dict.items():
    try:
        if column_name in metadata.columns:
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
                                pii=pii_dict.get(column_name, False)
                            )
                
                elif sdtype in ['numerical', 'integer']:
                    if is_actually_numerical(pdf[column_name]):
                        metadata.update_column(
                            column_name=column_name,
                            sdtype=sdtype
                        )
                elif sdtype == 'categorical':
                    metadata.update_column(
                        column_name=column_name,
                        sdtype=sdtype,
                        pii=pii_dict.get(column_name, False)
                    )
                elif sdtype == 'unknown':
                    metadata.update_column(
                        column_name=column_name,
                        sdtype=sdtype,
                        pii=pii_dict.get(column_name, False)
                    )
                
                else:
                    metadata.update_column(
                        column_name=column_name,
                        sdtype=sdtype
                    )
    except Exception as e:
        print(f"Error updating column {column_name}: {str(e)}")

# COMMAND ----------

print(metadata)

# COMMAND ----------

metadata.validate()
metadata.validate_data(data=pdf)

# COMMAND ----------

from sdv.single_table import GaussianCopulaSynthesizer

#https://docs.sdv.dev/sdv/single-table-data/modeling/synthesizers/gaussiancopulasynthesizer
synthesizer = GaussianCopulaSynthesizer(metadata)
synthesizer.fit(pdf)

                


# COMMAND ----------

synthetic_data = synthesizer.sample(num_rows=1000)
synthetic_data.head()
