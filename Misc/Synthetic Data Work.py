# Databricks notebook source
# MAGIC %pip install sdv
# MAGIC from pyspark.sql.functions import rand
# MAGIC
# MAGIC # Specify the catalog, schema, and table name
# MAGIC catalog = "4_prod"
# MAGIC schema = "raw"
# MAGIC table_name = "cds_apc_icd_diag"
# MAGIC
# MAGIC # Construct the path to the Delta table
# MAGIC delta_table_path = f"{catalog}.{schema}.{table_name}"
# MAGIC
# MAGIC # Load the Delta table as a Spark DataFrame
# MAGIC df = spark.table(f"{catalog}.{schema}.{table_name}")
# MAGIC
# MAGIC
# MAGIC #Take only up to 1 million rows, selected randomly.
# MAGIC sdf = df.orderBy(rand()).limit(1000000)
# MAGIC
# MAGIC # Convert the Spark DataFrame to a pandas DataFrame
# MAGIC pdf = sdf.toPandas()
# MAGIC
# MAGIC

# COMMAND ----------

from sdv.metadata import SingleTableMetadata

metadata = SingleTableMetadata()

metadata.detect_from_dataframe(pdf)

print(metadata)
#metadata.visualize()


# COMMAND ----------


#Types: https://docs.sdv.dev/sdv/reference/metadata-spec/sdtypes
metadata.update_column(
    column_name='CDS_APC_ID',
    sdtype='id'
)

# COMMAND ----------

metadata.validate()
metadata.validate_data(data=pdf)

# COMMAND ----------

from sdv.single_table import GaussianCopulaSynthesizer

#https://docs.sdv.dev/sdv/single-table-data/modeling/synthesizers/gaussiancopulasynthesizer
synthesizer = GaussianCopulaSynthesizer(metadata)
synthesizer.fit(pdf)

                


# COMMAND ----------

synthetic_data = synthesizer.sample(num_rows=100000)
synthetic_data.head()
