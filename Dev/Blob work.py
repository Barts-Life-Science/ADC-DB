# Databricks notebook source
# MAGIC %pip install PyMuPDF
# MAGIC %pip install striprtf
# MAGIC %pip install PyPDF2
# MAGIC %pip install pdfminer.six
# MAGIC %pip install python-magic
# MAGIC %pip install ocflzw_decompress
# MAGIC

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from ocflzw_decompress.lzw import LzwDecompress
from striprtf.striprtf import rtf_to_text
from bs4 import BeautifulSoup
from pyspark.sql.window import Window
import re
import chardet
import magic
import fitz
import io
import PyPDF2
from pdfminer.high_level import extract_text_to_fp
from pdfminer.layout import LAParams
from datetime import datetime

# COMMAND ----------

def get_max_adc_updt(table_name):
    try:
        default_date = lit(datetime(1980, 1, 1)).cast(DateType())
        result = spark.sql(f"SELECT MAX(ADC_UPDT) AS max_date FROM {table_name}")
        max_date = result.select(max("max_date").alias("max_date")).first()["max_date"]
        return max_date if max_date is not None else default_date
    except:
        return lit(datetime(1980, 1, 1)).cast(DateType())
    

def table_exists(table_name):
    try:
        result = spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
        return result.first() is not None
    except:
        return False
    
def table_exists_with_rows(table_name):
    try:
        result = spark.sql(f"SELECT COUNT(*) AS row_count FROM {table_name}")
        row_count = result.first()["row_count"]
        return row_count > 0
    except:
        return False

# COMMAND ----------





# Define helper functions
def combine_and_decompress_blob(blob_chunks):
    if not blob_chunks:
        return None
    
    sorted_chunks = sorted(blob_chunks, key=lambda x: x['BLOB_SEQ_NUM'])
    
    combined = bytearray()
    for chunk in sorted_chunks:
        combined.extend(chunk['BLOB_CONTENTS'])
    
    compression_cd = sorted_chunks[0]['COMPRESSION_CD']
    
    try:
        if compression_cd == 728:  # LZW compression
            lzw = LzwDecompress()
            return bytes(lzw.decompress(combined))
        elif compression_cd == 727:  # No compression
            return bytes(combined)
        else:
            return None
    except Exception as e:
        print(f"Decompression error: {str(e)}")
        return None
    
def parse_blob_content(content):
    if not content:
        return None
    
    mime = magic.Magic(mime=True)
    content_type = mime.from_buffer(content)
    
    if content_type.startswith('image/') or content_type == 'application/zip':
        return f"[{content_type} Content]"

    if content_type == 'application/pdf':
        pdf_file = io.BytesIO(content)
        text = ""
        
        # Try PyMuPDF (fitz)
        try:
            with fitz.open(stream=pdf_file, filetype="pdf") as doc:
                for page in doc:
                    text += page.get_text()
        except Exception:
            pass

        # If PyMuPDF failed, try PyPDF2
        if not text.strip():
            try:
                pdf_file.seek(0)
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
            except Exception:
                pass

        # If PyPDF2 failed, try pdfminer
        if not text.strip():
            try:
                pdf_file.seek(0)
                output_string = io.StringIO()
                extract_text_to_fp(pdf_file, output_string, laparams=LAParams(), output_type='text', codec='utf-8')
                text = output_string.getvalue()
            except Exception:
                pass

        return text.strip() if text.strip() else "[PDF Content - Error extracting text]"

    # Try multiple encodings
    encodings = ['utf-8', 'iso-8859-1', 'windows-1252', 'ascii']
    decoded = None
    
    for encoding in encodings:
        try:
            decoded = content.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    
    if decoded is None:
        detected = chardet.detect(content)
        try:
            decoded = content.decode(detected['encoding'])
        except:
            return f"[Binary data, unable to decode. Detected encoding: {detected['encoding']}]"

    if content_type == "text/rtf":
        return rtf_to_text(decoded)
    elif content_type == "text/html" or content_type == "text/xml":
        soup = BeautifulSoup(decoded, 'html.parser')
        return soup.get_text(separator='\n', strip=True)
    else:
        # clean custom format
        cleaned = re.sub(r'<%.*?%>', '', decoded)
        cleaned = cleaned.replace('|', '\n')
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)

        cleaned = re.sub(r'\n+', '\n', cleaned)  # Remove multiple consecutive newlines
        return cleaned.strip()

def determine_content_type(content):
    if not content:
        return None
    
    mime = magic.Magic(mime=True)
    return mime.from_buffer(content)

# Update the process_partition function to include content type
def process_partition(iterator):
    # Convert the partition to a Pandas DataFrame
    pdf = pd.DataFrame(iterator)
    
    if not pdf.empty:
        # Process the data using apply
        pdf['BLOB_BINARY'] = pdf['blob_chunks'].apply(combine_and_decompress_blob)
        pdf['CONTENT_TYPE'] = pdf['BLOB_BINARY'].apply(determine_content_type)
        pdf['BLOB_TEXT'] = pdf['BLOB_BINARY'].apply(parse_blob_content)
        pdf['BINARY_SIZE'] = pdf['BLOB_BINARY'].apply(lambda x: len(x) if x is not None else 0)
        pdf['TEXT_LENGTH'] = pdf['BLOB_TEXT'].apply(lambda x: len(x) if x is not None else 0)

        # Drop the temporary column
        pdf = pdf.drop(columns=['blob_chunks'])

    # Return the processed data as an iterator of rows
    return pdf.itertuples(index=False, name=None)

@dlt.table(
    name="mill_dir_ce_blob_processed_incr",
    comment="Incrementally processed BLOB data from mill_dir_ce_blob",
    temporary=True,
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "EVENT_ID",
        "skipChangeCommits": "true"
    }
)
def create_mill_dir_ce_blob_processed_incr():
    # Get the maximum ADC_UPDT from the target table
    max_adc_updt = get_max_adc_updt("8_dev.5_preproc.mill_dir_ce_blob_processed")

    df = spark.table("8_dev.raw.mill_dir_ce_blob")
    
    # Apply the filter condition
    df_filtered = df.filter(df.ADC_UPDT > max_adc_updt)

    # Group by EVENT_ID and collect all blob chunks
    window = Window.partitionBy("EVENT_ID").orderBy("BLOB_SEQ_NUM")
    grouped_df = df.withColumn("row_number", row_number().over(window)) \
                   .groupBy("EVENT_ID") \
                   .agg(
                       collect_list(struct("BLOB_CONTENTS", "COMPRESSION_CD", "BLOB_SEQ_NUM", "row_number")).alias("blob_chunks"),
                       first("VALID_UNTIL_DT_TM").alias("VALID_UNTIL_DT_TM"),
                       first("VALID_FROM_DT_TM").alias("VALID_FROM_DT_TM"),
                       first("UPDT_DT_TM").alias("UPDT_DT_TM"),
                       first("UPDT_ID").alias("UPDT_ID"),
                       first("UPDT_TASK").alias("UPDT_TASK"),
                       first("UPDT_CNT").alias("UPDT_CNT"),
                       first("UPDT_APPLCTX").alias("UPDT_APPLCTX"),
                       first("LAST_UTC_TS").alias("LAST_UTC_TS"),
                       first("ADC_UPDT").alias("ADC_UPDT")
                   )

    # Define the schema for the processed DataFrame
    schema = StructType([
        StructField("EVENT_ID", LongType(), True),
        StructField("VALID_UNTIL_DT_TM", TimestampType(), True),
        StructField("VALID_FROM_DT_TM", TimestampType(), True),
        StructField("UPDT_DT_TM", TimestampType(), True),
        StructField("UPDT_ID", LongType(), True),
        StructField("UPDT_TASK", LongType(), True),
        StructField("UPDT_CNT", LongType(), True),
        StructField("UPDT_APPLCTX", LongType(), True),
        StructField("LAST_UTC_TS", TimestampType(), True),
        StructField("ADC_UPDT", TimestampType(), True),
        StructField("BLOB_BINARY", BinaryType(), True),
        StructField("CONTENT_TYPE", StringType(), True),
        StructField("BLOB_TEXT", StringType(), True),
        StructField("BINARY_SIZE", LongType(), True),
        StructField("TEXT_LENGTH", LongType(), True)
    ])

    # Process the data in batches
    batch_size = 10  # Adjust this value based on your cluster's capacity
    total_rows = grouped_df.count()
    num_batches = (total_rows + batch_size - 1) // batch_size

    processed_dfs = []

    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = (i + 1) * batch_size
        if end_idx > total_rows:
            end_idx = total_rows

        batch_df = grouped_df.limit(end_idx).subtract(grouped_df.limit(start_idx))
        
        # Convert to Pandas for processing
        pdf = batch_df.toPandas()

        # Process the data
        pdf['BLOB_BINARY'] = pdf['blob_chunks'].apply(combine_and_decompress_blob)
        pdf['CONTENT_TYPE'] = pdf['BLOB_BINARY'].apply(determine_content_type)
        pdf['BLOB_TEXT'] = pdf['BLOB_BINARY'].apply(parse_blob_content)
        pdf['BINARY_SIZE'] = pdf['BLOB_BINARY'].apply(lambda x: len(x) if x is not None else 0)
        pdf['TEXT_LENGTH'] = pdf['BLOB_TEXT'].apply(lambda x: len(x) if x is not None else 0)

        # Ensure integer columns are of type int64
        for col in ['EVENT_ID', 'UPDT_ID', 'UPDT_TASK', 'UPDT_CNT', 'UPDT_APPLCTX', 'BINARY_SIZE', 'TEXT_LENGTH']:
            pdf[col] = pdf[col].astype('int64')

        # Drop the temporary column
        pdf = pdf.drop(columns=['blob_chunks'])

        # Convert back to Spark DataFrame with the defined schema
        processed_df = spark.createDataFrame(pdf, schema=schema)
        processed_dfs.append(processed_df)

    # Union all processed batches
    final_df = processed_dfs[0]
    for df in processed_dfs[1:]:
        final_df = final_df.unionAll(df)

        # Deduplicate the final DataFrame
    final_df = final_df.dropDuplicates(["EVENT_ID"])


    error_messages = [
        "[PDF Content - Error extracting text]",
        "[Binary data, unable to decode. Detected encoding: None]",
        "[image/jpeg Content]",
        "[image/png Content]",
        "[application/zip Content]"
    ]
    error_array = array(*[lit(msg) for msg in error_messages])
    
    final_df = final_df.withColumn("STATUS", 
        when(array_contains(error_array, final_df["BLOB_TEXT"]), final_df["BLOB_TEXT"])
        .otherwise(lit("Decoded"))
    )

    final_df = final_df.withColumn("BLOB_TEXT", 
        when(array_contains(error_array, final_df["BLOB_TEXT"]), lit(None))
        .otherwise(final_df["BLOB_TEXT"])
    )

    return final_df

@dlt.view(name="ce_blob_update")
def ce_blob_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.mill_dir_ce_blob_processed_incr")
    )

dlt.create_target_table(
    name="mill_dir_ce_blob_processed",
    comment="Incrementally updated processed BLOB data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "EVENT_ID"
    }
)

dlt.apply_changes(
    target="mill_dir_ce_blob_processed",
    source="ce_blob_update",
    keys=["EVENT_ID"],
    sequence_by="ADC_UPDT",
    apply_as_deletes=None,
    except_column_list=[],
    stored_as_scd_type=1
)
