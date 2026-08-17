# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import *
from pyspark.sql.functions import max as spark_max
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *
from pyspark.sql import functions as F
from functools import reduce

# COMMAND ----------

# ============================================================================
# Environment Configuration
# ============================================================================
ENVIRONMENT = "dev"  # Change to "prod" for production

if ENVIRONMENT == "dev":
    TARGET_CATALOG = "8_dev"
    TARGET_SCHEMA = "silver"
elif ENVIRONMENT == "prod":
    TARGET_CATALOG = "4_prod"
    TARGET_SCHEMA = "silver"
else:
    raise ValueError(f"Unknown ENVIRONMENT: {ENVIRONMENT}")

def get_target_table(table_name: str) -> str:
    return f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}"

# COMMAND ----------

def get_max_timestamp(table_name: str,
                      ts_column: str = "ADC_UPDT",
                      default_date: datetime = datetime(1980, 1, 1)
                     ) -> datetime:
    """
    Returns the greatest value of `ts_column` in `table_name`.
    If CDF is enabled, uses the earlier of max(ts_column) or table's last update time
    to guard against future-dated timestamps.
    If the table or the column does not exist the supplied default is returned.
    """
    try:
        if not table_exists(table_name):
            return default_date

        # Get max value from the timestamp column
        max_row = (
            spark.table(table_name)
                 .select(F.max(ts_column).alias("max_ts"))
                 .first()
        )
        max_ts_value = max_row.max_ts or default_date

        # If CDF is enabled, check table's actual last update time as a safeguard
        if has_cdf_enabled(table_name):
            try:
                # Get the table's last modification timestamp from Delta history
                history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()

                if history and len(history) > 0:
                    table_last_update = history[0].timestamp

                    # Convert to datetime if it's not already
                    if isinstance(table_last_update, str):
                        from datetime import datetime
                        table_last_update = datetime.fromisoformat(table_last_update.replace('Z', '+00:00'))

                    # Use the earlier of the two timestamps to be conservative
                    # This protects against future-dated ADC_UPDT values
                    if table_last_update < max_ts_value:
                        print(f"[INFO] {table_name}: Using table update time {table_last_update} instead of max {ts_column} {max_ts_value}")
                        return table_last_update

            except Exception as cdf_error:
                print(f"[WARN] Could not check CDF history for {table_name}: {cdf_error}")
                print(f"[INFO] Falling back to max {ts_column} value")

        return max_ts_value

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

# Barts-specific trust filter: this pipeline is purpose-built for Barts Health NHS Trust data,
# so coupling the trust value here is intentional rather than a design oversight.
def get_trust_filter():
    """Returns the trust filter condition for Barts Health NHS Trust"""
    return col("Trust") == "Barts"

def has_cdf_enabled(table_name: str) -> bool:
    """
    Check if a table has Change Data Feed enabled.

    Args:
        table_name: Fully qualified table name

    Returns:
        bool: True if CDF is enabled, False otherwise
    """
    try:
        table_props = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
        cdf_prop = next((row.value for row in table_props if row.key == 'delta.enableChangeDataFeed'), None)
        return (cdf_prop or "").lower() == "true"
    except Exception as e:
        print(f"Warning: Could not check CDF status for {table_name}: {e}")
        return False

def get_incremental_data_with_cdf(
    source_table: str,
    target_table: str,
    timestamp_column: str = "ADC_UPDT",
    key_columns: list = None,
    apply_trust_filter: bool = True,
    additional_filters = None
) -> tuple:
    """
    Get incremental data using CDF when available, falling back to timestamp filtering.

    Args:
        source_table: Source Delta table name.
        target_table: Target Delta table name (used to read the watermark).
        timestamp_column: Column used for watermark comparison.
        key_columns: Columns that uniquely identify a row. Used to deduplicate
                     CDF changes. If None, falls back to all non-meta columns
                     (legacy behaviour, but risks 'multiple source rows matched'
                     errors on merge).
        apply_trust_filter: Whether to filter for Barts trust.
        additional_filters: Extra filter condition to apply.

    Returns:
        tuple: (DataFrame, record_count) - the incremental data and its count
    """
    max_timestamp = get_max_timestamp(target_table, timestamp_column)

    if has_cdf_enabled(source_table):
        try:
            print(f"[INFO] Attempting CDF read for {source_table}")

            changes_df = (
                spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingTimestamp", max_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
                .table(source_table)
            )

            if key_columns:
                partition_cols = key_columns
            else:
                partition_cols = [c for c in changes_df.columns if not c.startswith("_")]
            window_spec = Window.partitionBy(*partition_cols).orderBy(col("_commit_timestamp").desc())

            incremental_df = (
                changes_df
                .filter(col("_change_type").isin("insert", "update_postimage"))
                .withColumn("_row_num", row_number().over(window_spec))
                .filter(col("_row_num") == 1)
                .drop("_change_type", "_commit_version", "_commit_timestamp", "_row_num")
            )

            # Apply trust filter before counting
            if apply_trust_filter:
                schema_fields = [f.name for f in incremental_df.schema.fields]
                if "Trust" in schema_fields:
                    incremental_df = incremental_df.filter(col("Trust") == "Barts")
                    print(f"[INFO] Applied Barts trust filter")

            # Apply additional filters before counting
            if additional_filters is not None:
                incremental_df = incremental_df.filter(additional_filters)

            record_count = incremental_df.count()
            print(f"[SUCCESS] Retrieved {record_count} records via CDF from {source_table}")

            return incremental_df, record_count

        except Exception as e:
            error_msg = str(e).split('\n')[0]
            if "FILE_NOT_FOUND" in error_msg or "PathNotFound" in error_msg:
                print(f"[WARN] CDF files unavailable for {source_table} (possibly due to file cleanup or shallow clone)")
            else:
                print(f"[WARN] CDF read failed for {source_table}: {error_msg}")

            print(f"[INFO] Using timestamp-based incremental load instead")
            incremental_df = spark.table(source_table).filter(col(timestamp_column) > max_timestamp)
    else:
        print(f"[INFO] Using timestamp-based incremental load for {source_table}")
        incremental_df = spark.table(source_table).filter(col(timestamp_column) > max_timestamp)

    # Timestamp-based path: apply filters then count
    if apply_trust_filter:
        schema_fields = [f.name for f in incremental_df.schema.fields]
        if "Trust" in schema_fields:
            incremental_df = incremental_df.filter(col("Trust") == "Barts")
            print(f"[INFO] Applied Barts trust filter")

    if additional_filters is not None:
        incremental_df = incremental_df.filter(additional_filters)

    record_count = incremental_df.count()
    print(f"[INFO] Retrieved {record_count} records via timestamp filter from {source_table}")

    return incremental_df, record_count

def apply_trust_filter_to_df(df, table_name: str = None):
    """
    Apply Barts trust filtering to a DataFrame.

    Args:
        df: DataFrame to filter
        table_name: Optional table name for logging

    Returns:
        DataFrame: Filtered DataFrame
    """
    schema_fields = [f.name for f in df.schema.fields]

    if "Trust" not in schema_fields:
        if table_name:
            print(f"[WARN] Trust column not found in {table_name}, returning unfiltered")
        return df

    if table_name:
        print(f"[INFO] Applied Barts trust filter to {table_name}")

    return df.filter(col("Trust") == "Barts")

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
        target_fields = {f.name for f in target_schema.fields}

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
def escape_comment(text: str) -> str:
    if not text:
        return ""
    return text.replace("\\", "\\\\").replace("'", "''")


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

    # Check if any column has a type change
    type_change_detected = any(col['type_changed'] for col in changes['columns_to_update'])

    if type_change_detected:
        print(f"Type change detected. Recreating table {target_table}...")
        df = spark.table(target_table)
        cols_to_cast = [col['name'] for col in changes['columns_to_update'] if col['type_changed']]
        pre_counts = df.select([F.count(F.col(c)).alias(c) for c in cols_to_cast]).collect()[0].asDict()

        for col_info in changes['columns_to_update']:
            if col_info['type_changed']:
                current_type = df.schema[col_info['name']].dataType
                target_type_str = col_info['type']
                # Handle string → integer types: cast through double first to handle "123.0" or "1.23E5" formats
                if isinstance(current_type, StringType) and target_type_str in ('bigint', 'int', 'smallint', 'tinyint'):
                    df = df.withColumn(col_info['name'], df[col_info['name']].cast('double').cast(target_type_str))
                else:
                    df = df.withColumn(col_info['name'], df[col_info['name']].cast(target_type_str))

        # Now check the data loss after changing the data type
        post_counts = df.select([F.count(F.col(c)).alias(c) for c in cols_to_cast]).collect()[0].asDict()
        losses = {c: pre_counts[c] - post_counts[c] for c in cols_to_cast if pre_counts[c] - post_counts[c] > 0}

        if losses:
            raise ValueError(f"Data loss detected during cast: {losses}")
        else:
            print("All type casts completed without introducing NULLs.")

        # Drop and recreate the table, preserving CDF and row tracking
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("delta.enableChangeDataFeed", "true") \
            .option("delta.enableRowTracking", "true") \
            .saveAsTable(target_table)

        updates_applied.append(f"Recreated {target_table} due to type change")

    # Handle column comment updates separately
    comment_change_detected = any(col['comment_changed'] for col in changes['columns_to_update'])

    if comment_change_detected:
        for col in changes['columns_to_update']:
            if col['comment_changed']:
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
    """
    if target_schema:
        # 1. Create the empty table first
        builder = (DeltaTable.createIfNotExists(spark)
                  .tableName(target_table)
                  .addColumns(target_schema))

        if table_comment:
            builder = builder.comment(table_comment)

        builder = (builder
                  .property("delta.enableChangeDataFeed", "true")
                  .property("delta.enableRowTracking", "true"))
        builder.execute()
        print(f"[INFO] Created table {target_table} with schema and metadata")

        # 2. Enforce schema using Select/Cast (Avoiding RDD conversion)
        # This keeps the operation inside the JVM and preserves Catalyst optimizations
        select_expr = []
        for field in target_schema.fields:
            if field.name in source_df.columns:
                select_expr.append(F.col(field.name).cast(field.dataType))
            else:
                # Handle missing columns if necessary, or let it fail depending on requirements
                select_expr.append(F.lit(None).cast(field.dataType).alias(field.name))

        source_df_aligned = source_df.select(*select_expr)

        # 3. Append data
        source_df_aligned.write.mode("append").saveAsTable(target_table)

        apply_column_comments(target_table, target_schema)
    else:
        # Fallback for no schema
        (source_df.write
                  .format("delta")
                  .option("delta.enableChangeDataFeed", "true")
                  .mode("overwrite")
                  .saveAsTable(target_table))

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

# COMMAND ----------

# ============================================================================
# Main Update Function
# ============================================================================
def update_table(source_df, target_table: str, index_columns,
                 target_schema: StructType = None, table_comment: str = None):
    """
    Merge source_df into target_table using the given index column(s).
    Creates the table if it does not exist.

    Args:
        source_df: DataFrame with new/updated rows.
        target_table: Fully-qualified Delta table name.
        index_columns: Column name (str) or list of column names for the merge key.
        target_schema: Optional StructType to enforce on the target table.
        table_comment: Optional table-level comment.
    """
    # Normalize to list upfront
    if isinstance(index_columns, str):
        index_keys = [index_columns]
    else:
        index_keys = list(index_columns)

    if table_exists(target_table):
        print(f"[INFO] Table {target_table} exists. Checking for schema updates...")

        if target_schema or table_comment:
            schema_changes = detect_schema_changes(target_table, target_schema, table_comment)
            if schema_changes['has_changes']:
                print(f"[INFO] Schema changes detected for {target_table}")
                apply_schema_changes(target_table, schema_changes)

        # Check if empty using take(1) to avoid full table scan
        if len(source_df.take(1)) == 0:
            print(f"[INFO] Source DataFrame is empty. Skipping update for {target_table}")
            return

        # Perform merge operation
        # Deduplicate source on merge keys to prevent DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
        source_df = source_df.dropDuplicates(index_keys)
        print(f"[INFO] Performing merge on {target_table} using columns {index_keys}")

        merge_condition = " AND ".join([f"t.`{c}` <=> s.`{c}`" for c in index_keys])
        tgt = DeltaTable.forName(spark, target_table)
        (
            tgt.alias("t")
            .merge(source_df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[INFO] Successfully merged data into {target_table}")

    else:
        print(f"[INFO] Table {target_table} does not exist. Creating new table...")
        create_table_with_schema(source_df, target_table, target_schema, table_comment)
        print(f"[INFO] Successfully created and populated {target_table}")


# COMMAND ----------

pharos_person_comment = """
The table contains demographic information about patients, including identifiers
such as person ID, gender, birth year, and ethnicity.
"""

schema_pharos_person = StructType([
    StructField(
        name="person_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Assigned unique ID for each participant."}
    ),
    StructField(
        name="pharosid",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Assigned unique ID for each participant."}
    ),
    StructField(
        name="clinical_record_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Unique clinical record PharosID."}
    ),
    StructField(
        name="tumour_group",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Primary tumour group: Breast, Lung, Pancreas."}
    ),
    StructField(
        name="site",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Center identifier: QMUL, KCL, Public data."}
    ),
    StructField(
        name="yob",
        dataType=IntegerType(),
        nullable=True,
        metadata={"comment": "Year of birth. Dates not shared outside each site."}
    ),
    StructField(
        name="sex",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Sex at birth."}
    ),
    StructField(
        name="ethnicity",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Ethnicity category (broad group)."}
    ),
    StructField(
        name="ethnic_group",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Ethnicity category (granular group)."}
    ),
    StructField(
        name="ADC_UPDT",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Max source ADC_UPDT for incremental watermarking."}
    ),
    StructField(
        name="date_checked",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Date the relevant medical record was last checked."}
    ),
    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )
])


def create_pharos_person_incr():

    max_adc_updt = get_max_timestamp(
        table_name=get_target_table("pharos_person"),
        ts_column="ADC_UPDT"
    )

    map_person = spark.table(
        "4_prod.bronze.map_person"
    )

    processed_person = (
        map_person
        .filter(
            col("ADC_UPDT") > max_adc_updt
            if max_adc_updt is not None
            else lit(True)
        )
        .withColumn(
            "sex",
            when(col("gender_cd") == 362.0, lit("001 - Female"))
            .when(col("gender_cd") == 363.0, lit("002 - Male"))
            .otherwise(lit("G09 Unknown"))
        )
        .withColumn(
            "ethnicity",
            when(
                col("ethnicity_cd").isin(3767643, 3767645, 3767650),
                lit("010 - White")
            )
            .when(
                col("ethnicity_cd").isin(3767649, 3767652, 3767653, 3767654),
                lit("020 - Mixed")
            )
            .when(
                col("ethnicity_cd").isin(3767640, 3767644, 3767651, 3767647),
                lit("030 - Asian")
            )
            .when(
                col("ethnicity_cd").isin(3767638, 3767641, 3767648),
                lit("040 - Black")
            )
            .when(
                col("ethnicity_cd").isin(312508, 3767639, 3767642),
                lit("050 - Other")
            )
            .otherwise(lit("G09 - Unknown"))
        )
        .withColumn(
            "ethnic_group",
            when(col("ethnicity_cd") == 3767643, lit("011 White - British"))
            .when(col("ethnicity_cd") == 3767645, lit("012 White - Irish"))
            .when(col("ethnicity_cd") == 3767650, lit("013 White - White Other"))
            .when(col("ethnicity_cd") == 3767654, lit("021 Mixed - White and Black Caribbean"))
            .when(col("ethnicity_cd") == 3767653, lit("022 Mixed - White and Black African"))
            .when(col("ethnicity_cd") == 3767652, lit("023 Mixed - White and Asian"))
            .when(col("ethnicity_cd") == 3767649, lit("024 Mixed - Other"))
            .when(col("ethnicity_cd") == 3767644, lit("031 Asian - Indian"))
            .when(col("ethnicity_cd") == 3767651, lit("032 Asian - Pakistani"))
            .when(col("ethnicity_cd") == 3767640, lit("033 Asian - Bangladeshi"))
            .when(col("ethnicity_cd") == 3767647, lit("034 Asian - Other"))
            .when(col("ethnicity_cd") == 3767641, lit("041 Black - Caribbean"))
            .when(col("ethnicity_cd") == 3767638, lit("042 Black - African"))
            .when(col("ethnicity_cd") == 3767648, lit("043 Black - Other"))
            .when(col("ethnicity_cd") == 3767642, lit("051 Other - Chinese"))
            .when(col("ethnicity_cd").isin(3767639, 312508), lit("054 Other"))
            .when(col("ethnicity_cd").isin(0, 3767646), lit("G09 Unknown"))
            .otherwise(lit("G09 Unknown"))
        )
        .dropDuplicates()
    )

    map_diagnosis = (
        spark.table("4_prod.bronze.map_diagnosis")
        .select(
            "PERSON_ID",
            "OMOP_CONCEPT_ID",
            "ICD10_CODE"
        )
    )

    # OMOP concept IDs for breast cancer subtypes
    brc_add_ids = [
        45768522,  # Triple-negative breast cancer
        35624616,  # Germline BRCA-mutated, HER2-negative metastatic breast cancer
        602331     # Metastatic malignant neoplasm to left breast
    ]

    site = (
        map_diagnosis
        .withColumn(
            "tumour_group",
            when(
                col("ICD10_CODE").like("C50%")
                | col("OMOP_CONCEPT_ID").isin(brc_add_ids),
                lit("001 Breast")
            )
        )
        .filter(col("tumour_group").isNotNull())
        .dropDuplicates(["PERSON_ID"])
    )

    final_df = (
        processed_person.alias("p")
        .join(
            site.alias("s"),
            col("p.person_id") == col("s.PERSON_ID"),
            "inner"
        )
        .select(
            col("p.person_id").cast(LongType()).alias("person_id"),
            lit(None).cast(StringType()).alias("pharosid"),
            lit(None).cast(LongType()).alias("clinical_record_id"),
            col("s.tumour_group").cast(StringType()).alias("tumour_group"),
            lit(None).cast(StringType()).alias("site"),
            col("p.birth_year").cast(IntegerType()).alias("yob"),
            col("p.sex").cast(StringType()).alias("sex"),
            col("p.ethnicity").cast(StringType()).alias("ethnicity"),
            col("p.ethnic_group").cast(StringType()).alias("ethnic_group"),
            col("p.ADC_UPDT"),
            current_timestamp().alias("date_checked"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
        .dropDuplicates(["person_id"])
    )

    return final_df


updates_df = create_pharos_person_incr()

update_table(updates_df,get_target_table("pharos_person"),"person_id",schema_pharos_person,pharos_person_comment)

# COMMAND ----------

pharos_cohort_comment = "The table contains demographic information about patients, including identifiers such as person ID, gender, birth year, and ethnicity, etc."

schema_pharos_cohort = StructType([
    StructField(
        name="person_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Assigned unique ID for each participant."}
    ),
    StructField(
        name="pharosid",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Assigned unique ID for each participant."}
    ),
    StructField(
        name="clinical_record_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Unique clincial record PharosID."}
    ),    
    StructField(
        name="cohort",
        dataType=StringType(),
        nullable=True,
        metadata={
            "comment": "Pharos cohort groups."}
    )
])

def create_pharos_cohort_incr():

    max_adc_updt = get_max_timestamp(
        table_name=get_target_table("pharos_metastasis"),
        ts_column="ADC_UPDT"
    )

    map_diagnosis = spark.table("4_prod.bronze.map_diagnosis")

    # Get the breast cancer cohort
    breast_cancer_cohort = (
        map_diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (
                col("ICD10_CODE").like("C50%") |
                col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331)
            )
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("brc_diag_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )

    final_df = (
        breast_cancer_cohort
        .select(
            col("PERSON_ID")
                .cast(LongType())
                .alias("person_id"),
            lit(None)
                .cast(StringType())
                .alias("pharosid"),
            lit(None)
                .cast(LongType())
                .alias("clinical_record_id"),
            lit(None)
                .cast(StringType())
                .alias("cohort"),
            current_timestamp()
                .cast(StringType())
                .alias("date_checked_met_sites"),
            col("_src_adc_updt")
                .alias("ADC_UPDT")
        )
    )

    return final_df


updates_df = create_pharos_cohort_incr()

update_table(updates_df,get_target_table("pharos_cohort"),["person_id", "cohort"],schema_pharos_cohort,pharos_cohort_comment)


# COMMAND ----------

tumour_group_comment = (
    "This table records the tumour group associated with each Pharos participant. It links the participant to their primary tumour group, such as Breast, Pancreatic, or Lung, based on clinical diagnosis records."
)


schema_tumour_group = StructType([
    StructField(
        "tumour_group_id",
        LongType(),
        True,
        {"comment": "Surrogate primary key for each tumour group record."}
    ),
    StructField(
        "person_id",
        LongType(),
        True,
        {"comment": "Assigned unique ID for each participant."}
    ),
    StructField(
        "pharosid",
        StringType(),
        True,
        {"comment": "FK to person."}
    ),
    StructField(
        "clinical_record_id",
        LongType(),
        True,
        {"comment": "Unique clinical record PharosID."}
    ),
    StructField(
        "tumour_group",
        StringType(),
        True,
        {"comment": "Tumour group, e.g. Breast, Pancreatic, Lung."}
    ),
    StructField(
        "ADC_UPDT",
        TimestampType(),
        True,
        {"comment": "Last update timestamp."}
    ),
    StructField(
        "date_checked",
        TimestampType(),
        True,
        {"comment": "Last update timestamp."}
    ),
    StructField(
        "created_at",
        TimestampType(),
        True,
        {"comment": "Row creation timestamp."}
    ),
    StructField(
        "updated_at",
        TimestampType(),
        True,
        {"comment": "Row last-updated timestamp."}
    )
])


def create_tumour_group_incr():

    max_adc_updt = get_max_timestamp(
        table_name=get_target_table("tumour_group"),
        ts_column="ADC_UPDT"
    )

    map_diagnosis = spark.table("4_prod.bronze.map_diagnosis")

    tumour_group_cohort = (
        map_diagnosis
        .filter(col("ADC_UPDT") > max_adc_updt)
        .filter(col("PERSON_ID").isNotNull())
        .withColumn(
            "tumour_group",
            F.when(
                (col("ICD10_CODE").like("C50%")) |
                (col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331)),
                "Breast"
            )
            .when(
                col("ICD10_CODE").like("C25%"),
                "Pancreatic"
            )
            .when(
                col("ICD10_CODE").like("C33%") |
                col("ICD10_CODE").like("C34%"),
                "Lung"
            )
        )
        .filter(col("tumour_group").isNotNull())
        .groupBy("PERSON_ID", "tumour_group")
        .agg(
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )

    final_df = (
        tumour_group_cohort
        .select(
            lit(None).cast(LongType()).alias("tumour_group_id"),
            col("PERSON_ID").cast(LongType()).alias("person_id"),
            lit(None).cast(StringType()).alias("pharosid"),
            lit(None).cast(LongType()).alias("clinical_record_id"),
            col("tumour_group"),
            col("_src_adc_updt").alias("ADC_UPDT"),
            current_timestamp().alias("date_checked"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
    )

    return final_df


updates_df = create_tumour_group_incr()

update_table(updates_df,"8_dev.silver.pharos_tumour_group",["person_id"],schema_tumour_group,tumour_group_comment)

# COMMAND ----------

pharos_medical_history_comment = "The table contains the medical history of partcipants."
schema_pharos_medical_history = StructType([
    StructField(
        name="person_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Assigned unique ID for each participant (TBC)."}
    ),
    StructField(
        name="clinical_record_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Unique clincial record PharosID."}
    ), 
    StructField(
        name="familyhistory_bca",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Family history of breast cancer - assessed at time of first diagnosis."}
    ),
    StructField(
        name="familyhistory_ovarian",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Family history of breast cancer - assessed at time of first diagnosis."}
    ),

    StructField(
        name="familyhistory_cancer",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Family history of any cancer - assessed at time of first diagnosis."}
    ),
    StructField(
        name="genetic_testing",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Whether patient underwent genetic testing."}
    ),
    StructField(
        name="any_germline_mutation",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "The presence of any pathological mutation found through genetic testing Mandatory only where genetic_testing =Yes."}
    ),
    StructField(
        name="brca1",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Results from BRCA1 testing."}
    ),
    StructField(
        name="brca2",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Results from BRCA2 testing."}
    ),
    StructField(
        name="tp53",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Results from TP53 testing."}
    ),
    StructField(
        name="palb2",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Results from PALB2 testing."}
    ),
    StructField(
        name="chek2",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Results from CHEK2 testing."}
    ),
    StructField(
        name="atm",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Results from ATM testing."}
    ),
    StructField(
        name="rad51c",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Results from RAD51C testing."}
    ),
    StructField(
        name="rad51d",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Results from RAD51D testing."}
    ),
    StructField(
        name="genetic_testing_details",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Record any information on the details of the genetic mutations reported."}
    ),
    StructField(
        name="menopausal_status",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Recorded menopausal status at time of first diagnosis."}
    ),
    StructField(
        name="menopause_age",
        dataType=IntegerType(),
        nullable=True,
        metadata={"comment": "Age in years at menopause - leave blank if unknown."}
    ),
    StructField(
        name="inferred_menopausal_status",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Inferred menopausal status based on age and treatment patterns:Postmenopausal if menstrual period had stopped naturally or surgically by bilateral oopherectomy. Those with unknown menopausal age, who reported irregular menses, hysterectomy, or MHT use, considered postmenopausal at age 53.Those taking aromatase inhibitors considered postmenopausal."}
    ),
    StructField(
        name="hrt",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "If patient is currently taking HRT or has in the past  - assessed at time of first diagnosis."}
    ),
    StructField(
        name="hrt_years",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Time in years of HRT use."}
    ),
    StructField(
        name="contraception_use",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Use of contraception."}
    ),
    StructField(
        name="contraception_details",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Details of contraception."}
    ),
    StructField(
        name="presentation",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Method of presentation to oncology."}
    ),
    StructField(
        name="height_diagnosis",
        dataType=FloatType(),
        nullable=True,
        metadata={"comment": "Height at primary cancer diagnosis (cm)."}
    ),
    StructField(
        name="weight_diagnosis",
        dataType=FloatType(),
        nullable=True,
        metadata={"comment": "Weight at primary cancer diagnosis (kg)."}
    ),
    StructField(
        name="bmi_diagnosis",
        dataType=FloatType(),
        nullable=True,
        metadata={"comment": "BMI at primary cancer diagnosis - closest availabile BMI to diagnosis date within 6 months, with priority given to BMI prior to diagnosis."}
    ),
    StructField(
        name="smoking",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Smoking status at time of diagnosis."}
    ),
    StructField(
        name="smoking_no",
        dataType=FloatType(),
        nullable=True,
        metadata={"comment": "Number of cigarettes per day. If a range is given e.g. 5-10 per day, then the highest number is recorded."}
    ),
    StructField(
        name="smoking_years",
        dataType=IntegerType(),
        nullable=True,
        metadata={"comment": "Number of years smoked."}
    ),
    StructField(
        name="vape",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "E-cigarettes used at time of diagnosis."}
    ),
    StructField(
        name="alcohol",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Alcohol use at time of diagnosis."}
    ),
    StructField(
        name="alcohol_no",
        dataType=FloatType(),
        nullable=True,
        metadata={"comment": "Units per week.  If a range is given, then the highest number is recorded."}
    ),
    StructField(
        name="drug",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Recreational drugs used."}
    ),
    StructField(
        name="drug_details",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Recreational drugs details."}
    ),
    StructField(
        name="performance_diagnosis",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Performance status (ECOG) closest to diagnosis, within 30 days."}
    ),
    StructField(
        name="parous",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Patient has had pregnancies carried to viable gestational age."}
    ),
    StructField(
        name="gravidity_no",
        dataType=FloatType(),
        nullable=True,
        metadata={"comment": "Total number of pregnancies, regardless of outcome."}
    ),
    StructField(
        name="parity_no",
        dataType=FloatType(),
        nullable=True,
        metadata={"comment": "Number of live births."}
    ),
    StructField(
        name="age_first_pregnancy",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Age of patient (years) at first pregnancy."}
    ),
    StructField(
        name="breastfeed",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "If any children were breastfed."}
    ),
    StructField(
        name="pabc",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Pregnancy associated breast cancer, defined as breast cancer diagnosed during pregnancy, or within 12 months after giving birth."}
    ),
    StructField(
        name="time_pregnancytobc",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Time calculated between last live birth and breast cancer diagnosis (in years)"}
    ),
    StructField(
        name="ADC_UPDT",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
    ),
    StructField(
        name="date_checked",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Timestamp of the update"}
    ),
    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )
])

def create_medical_history_incr():

    max_adc_updt = get_max_timestamp(
        table_name=get_target_table("pharos_medical_history"),
        ts_column="ADC_UPDT")

    map_diagnosis = spark.table("4_prod.bronze.map_diagnosis")
    map_problem = spark.table("4_prod.bronze.map_problem")
    map_numeric_events = spark.table("4_prod.bronze.map_numeric_events")
    map_family_history = spark.table("4_prod.bronze.map_family_history")
    map_birth = spark.table("4_prod.bronze.map_mat_birth")
    pharos_person_sex = (
        spark.table("4_prod.silver.pharos_person")
        .select("person_id","sex"))

    # Get the breast cancer cohort
    breast_cancer_cohort = (
        map_diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (
                col("ICD10_CODE").like("C50%") |
                col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331)
            )
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("brc_diag_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )

    # Narrow down the cohort to only those who have a diagnosis of breast cancer for the data range of interest

    diagnosis = map_diagnosis.join(breast_cancer_cohort, ["PERSON_ID"], "inner")
    problem = map_problem.join(breast_cancer_cohort, "PERSON_ID", "inner")
    numeric_events = map_numeric_events.join(breast_cancer_cohort, "PERSON_ID", "inner")
    family_history = map_family_history.join(breast_cancer_cohort, "PERSON_ID", "semi").alias("f")
    birth = map_birth.join(breast_cancer_cohort, map_birth["MotherPerson_ID"] == breast_cancer_cohort["PERSON_ID"], "semi")

    comb_prob_diag = (
        problem
        .select("PERSON_ID", "SOURCE_STRING", "SOURCE_IDENTIFIER", "OMOP_CONCEPT_ID", "SNOMED_CODE", "ICD10_CODE", col("ONSET_DT_TM").alias("condition_date"))
        .unionByName(
            diagnosis
            .select("PERSON_ID", "SOURCE_STRING", "SOURCE_IDENTIFIER", "OMOP_CONCEPT_ID", "SNOMED_CODE", "ICD10_CODE", col("DIAG_DT_TM").alias("condition_date")))
        .dropDuplicates()
    ).alias("c")

    #-------------FAMILY_CANCER_HISTORY

    # Captures Family History using OMOP concept IDs to supplement standard ICD-10 Z80 (Family history of primary malignant neoplasm) codes.
    family_history_add_ids = [
        4175994,   # Family history of malignant neoplasm of cervix uteri
        43530673,  # Family history of colorectal cancer
        4329111,   # Family history of breast cancer 2 gene mutation
        4160695,   # Family history of breast cancer 1 gene mutation
        4195970,   # Family history of cancer of colon
        37117109,  # Family history of malignant neoplasm of ovary in first degree relative
        42535500,  # Family history of breast cancer gene BRCA mutation
        3078338013,# Family history of breast cancer gene mutation in first degree relative
        45884753,  # Family history of colon cancer
        46273481,  # Family history of hereditary nonpolyposis colon cancer
        1243977,   # Family history of hereditary diffuse gastric cancer
        4326336,   # Family history of cancer of esophagus
        37311977,  # Family history of malignant neoplasm of urinary tract
        4176765,   # Family history of male breast cancer
        4334494,   # Family history of digestive organ cancer
        4179232,   # Family history of pancreatic cancer
        46273150,  # Family history of ureter cancer
        4328801,   # Family history of thyroid cancer
        4324202,   # Family history of liver cancer
        46273151,  # Family history of urethra cancer
        42535054,  # Family history of colon cancer over age 50
        4334339,   # Family history of thoracic cavity structure cancer
        4323762,   # Family history of brain cancer
        4322902,   # Family history of vagina cancer
        4177058,   # Family history of ileum cancer
        4328583,   # Family history of breast cancer in first degree relative <50
        35624517,  # Family history of breast cancer <50 in second degree female relative
        46274041,  # Family history of endometrium cancer
        4179082,   # Family history of eye cancer
        4324203,   # Family history of bone cancer
        764948,    # Family history of oral cavity cancer
        4327415    # Family history of testis cancer
    ]

    family_history = (
        comb_prob_diag
        .join(family_history, ["PERSON_ID"], "left")
        .withColumn(
            "familyhistory_bca_flag",
            when(
                # ICD-10 Z803: Family history of malignant neoplasm of breast
                (col("c.ICD10_CODE").like("Z803")) |
                # OMOP concept_ids containing for family history breast cancer
                (col("c.OMOP_CONCEPT_ID").isin(4179963, 4329111, 4160695, 42535500, 46270135, 4210263, 4176765, 4328583, 35624517, 46270155, 46270130)) |
                (col("f.CONDITION_DESC") == "Breast cancer"),
                "G01 - Yes")
            .when (
                # Concept_id for "No FH: breast carcinoma": 4209112
                (col("c.OMOP_CONCEPT_ID") == 4209112), "G00 - No")
        )
        .withColumn(
            "familyhistory_ovarian_flag",
            ## OMOP concept_ids containing for family history ovarian cancer
            when(col("c.OMOP_CONCEPT_ID").isin(4326681, 37117109, 37109210), "G01 - Yes")
            # Concept_id for No family history of ovarian cancer: 44804658
            .when(col("c.OMOP_CONCEPT_ID") == 44804658, "G00 - No")
        )
        .withColumn(
            "familyhistory_cancer_flag",
            when(
                (col("c.OMOP_CONCEPT_ID").isin(family_history_add_ids)) |
                (col("c.ICD10_CODE").like("Z80%")) |
                (col("f.ICD10_CODE").rlike("^[CD]")) |
                (col("familyhistory_bca_flag") == "1 Breast") |
                (col("familyhistory_ovarian_flag") == "1 Ovarian"),
                "G01 - Yes"
            ).otherwise("G09 - Unknown")
        )
        .groupBy("PERSON_ID")
        .agg(
            F.max("familyhistory_bca_flag").alias("familyhistory_bca"),
            F.max("familyhistory_ovarian_flag").alias("familyhistory_ovarian"),
            F.min("familyhistory_cancer_flag").alias("familyhistory_cancer")
            )
    )
        # Restrict lifestyle and biometric features to the peri-diagnostic period. # Window: [Diagnosis - 1 year] to [Diagnosis + 7 days].

    check_date_range = (
        breast_cancer_cohort
        .withColumn("check_onset_date", date_sub(col("brc_diag_date"), 365))
        .withColumn("check_offset_date", date_add(col("brc_diag_date"), 7))
        .select("PERSON_ID", "check_onset_date", "check_offset_date")
    )

    #-------------SMOKING, ALCOHOL & DRUGS
    # Get the range of dates to check for the personal lifestyles

    # List of Source Identifiers representing smoking status.
    never_smoker = [
        "397731016",   # Never smoked
        "397732011"    # Never smoked tobacco
    ]
    non_smoker = [
        "14866014",    # Non-smoker
        "169636015",   # Non-smoker for medical reasons
        "2157522017"  # Current non smoker but past smoking history unknown
    ]
    current_smoker = [
        "128130017",   # Smoker
        "503483019",   # Current smoker
        "4589211015",  # Smoking
        "2912728015",  # Smokes tobacco daily
        "108938018",   # Cigarette smoker
        "344798019",   # Light cigarette smoker
        "344801013",   # Moderate cigarette smoker
        "344802018",   # Heavy cigarette smoker
        "344803011",   # Very heavy cigarette smoker
        "344804017",   # Chain smoker
        "136515019",   # Pipe smoker
        "99639019",    # Cigar smoker
        "344797012",   # Occasional cigarette smoker
        "3308893017",  # Occasional tobacco smoker
        "2973829011"   # Water pipe smoker
    ]
    past_smoker = [
        "15047015",          # Ex-smoker
        "15046012",          # Former smoker
        "250373019",         # Stopped smoking
        "418914010",         # Ex-cigarette smoker
        "397737017",         # Ex-light cigarette smoker (1-9/day)
        "397738010",         # Ex-moderate cigarette smoker (10-19/day)
        "397739019",         # Ex-heavy cigarette smoker (20-39/day)
        "397740017",         # Ex-very heavy cigarette smoker (40+/day)
        "3513199018",        # Ex-smoker for less than 1 year
        "3047429015",        # Ex-smoker for more than 1 year
        "2735181000000113",  # Ex-smoker amount unknown
        "397744014",         # Ex-cigarette smoker amount unknown
        "250376010",         # Ex-pipe smoker
        "250377018",         # Ex-cigar smoker
        "F17.20"             # previousSmoker
    ]
    smoking_unknown_usage = [
        "172772016",   # Smoking AND/OR drinking habits
        "3084413016",  # Smoking/drinking/substance abuse habits
        "489484014",   # Tobacco smoking consumption - finding
        "106720017"    # Smoke (General/Unspecified)
    ]
    # List of Source Identifiers representing drinking status.
    alcoholic = [
        "1462019",    # Alcohol user
        "481158013",  # Drinks alcohol
        "Z72.1",      # Alcohol use
        "3012209018", # Admits alcohol use
        "2765571000000113", # History of alcohol use
        "1206561011", # Details of alcohol drinking behaviour
        "489467015",  # Alcohol intake - finding
        "2695747019", # Alcohol intake exceeds recommended limit
        "250338016",  # XS - Excessive alcohol consumption
        "342370018",  # Alcoholic binges exceeding sensible amounts
        "299061000000117", # Hazardous alcohol use
        "3322030015", # Unhealthy alcohol drinking behaviour
        "1216142012", # Alcohol drinking behaviour
        "478024019",  # Alcohol-induced epilepsy
        "2694326014", # Alcohol intake within daily limit
        "250340014",  # Alcohol intake within sensible limits
        "1154571000000110", # Alcohol misuse enhanced service completed
    ]

    non_alcoholic = [
        "291354017",  # 4022664 - Does not drink alcohol
        "2659854017", # 4022664 - Non - drinker alcohol
        "442138014",  # 4116983 - Abstinent alcoholic
        "250317010",  # 4052945 - Stopped drinking alcohol
        "339752015",  # 4022703 - Alcohol-free diet
    ]
    # List of Source Identifiers representing vaping status.
    current_vaper = [
        "5169833015", # Daily vaper
        "5169699016", # Daily vape user
        "3747608014", # Vaper
        "3773975015", # Vaper with nicotine
        "3769372019", # Nicotine-filled electronic cigarette vaper
        "3770507018", # Non-nicotine-filled electronic cigarette vaper
    ]

    vaping_usage_unknown = [
        "3747483013", # Vape
        "3850395015", # Vaping
    ]
    # List of Source Identifiers representing drug using status.

    recreational_drug_use = [
        "45692019",   # Recreational drug user
        "169644015",  # Occasional drug user
        "2548201014", # Episodic use of drugs
        "2159187014", # H/O: recreational drug use
        "2986925015", # History of recreational drug use
        "74848019",   # Ex-drug user
        "2548992019", # Previously injecting drug user (if no dependence specified)
    ]

    drug_abuse = [
        "450216018",  # Illicit drug use
        "2548235012", # Current drug user
        "11819014",   # Drug addiction
        "342438017",  # Drug addict
        "1216143019", # Drug misuse behaviour
        "342426014",  # Drug abuse behaviour
        "342461015",  # IVDU - Intravenous drug user
        "342465012",  # Intravenous drug user
        "339576014",  # Injecting drug user
        "339577017",  # Drug injector
        "342457014",  # Injects drugs intramuscularly
        "342455018",  # Injects drugs subcutaneously
        "427785011",  # Hypodermic drug injection
        "342443012",  # Smokes drugs
        "342448015",  # Intranasal drug use
        "342441014",  # Misuses drugs orally
        "342451010",  # Misuses drugs rectally
        "1210093012", # History of drug abuse
        "2476701016", # H/O: drug abuse
        "494054010",  # Ex-drug addict
        "494055011",  # Ex-drug misuser
        "3766542013", # Abuse of ecstasy type drug
        "342435019",  # Poly-drug misuser
        "295322016",  # Nondependent mixed drug abuse
        "253490011",  # Suspected abuse hard drugs
    ]

    smoke_alcohol_drug = (
        comb_prob_diag
        .join(check_date_range, ["PERSON_ID"], "left")
        .filter(
            (col("condition_date").between(col("check_onset_date"), col("check_offset_date"))))

        #SMOKING ----------------
        .withColumn(
            "smoking",
            when(
                col("SOURCE_IDENTIFIER").isin(never_smoker),
                "000 - Never smoked"
            )
            .when(
                col("SOURCE_IDENTIFIER").isin(non_smoker),
                "001 - Non-smoker"
            )
            .when(
                col("SOURCE_IDENTIFIER").isin(current_smoker),
                "002 - Yes, current smoker"
            )
            .when(
                col("SOURCE_IDENTIFIER").isin(past_smoker),
                "003 - Yes, past smoker"
            )
            .when(
                col("SOURCE_IDENTIFIER").isin(smoking_unknown_usage),
                "004 - Yes, unknown usage"
            )
        )

        # VAPING ----------------
        .withColumn(
            "vape",
            when(col("SOURCE_IDENTIFIER").isin(current_vaper),
                    "002 - Yes, current smoker"
            )
            .when(col("SOURCE_IDENTIFIER") == "5169692013", "003 - Yes, past smoker")
            .when(col("SOURCE_IDENTIFIER").isin(vaping_usage_unknown),
                    "004 - Yes, unknown usage"
            )
        )

        # ALCOHOL ----------------
        .withColumn(
            "alcohol",
            when(
                col("SOURCE_IDENTIFIER").isin(non_alcoholic),
                "000 - No alcohol use"
            )
            .when(
                col("SOURCE_IDENTIFIER").isin(alcoholic),
                "001 - Drinks alcohol"
            )
            .when(
                # 2988881016 "Drinks alcohol daily"
                col("SOURCE_IDENTIFIER") == "2988881016",
                "002 - Drinks alcohol - Regularly"
            )
        )

        # DRUGS ----------------
        .withColumn(
            "drug",
            when(col("SOURCE_IDENTIFIER").isin(recreational_drug_use),
            "001 Yes - Recreationally")
            .when(col("SOURCE_IDENTIFIER").isin(drug_abuse),
            "002 Yes - Abuses drugs")
        )
    )
    smoke_alcohol_drug = (
        smoke_alcohol_drug
        .groupBy("PERSON_ID")
        .agg(
            F.max("smoking").alias("smoking"),
            F.max("vape").alias("vape"),
            F.max("alcohol").alias("alcohol"),
            F.max("drug").alias("drug")
        )
        # Fill missing values with "9 Unknown" only where there is no data
        .fillna({
            "smoking": "G09 Unknown",
            "vape": "G09 Unknown",
            "alcohol": "G09 Unknown",
            "drug": "G09 Unknown"
        })
    )

    # MENOPAUSE ----------------

    pre_menop = ["Before menopause", "Premenopausal state", "Excessive bleeding in the premenopausal period"]

    post_menop = [
        "Post-menopausal", "Postmenopausal state", "Postmenopausal", "Postmenopausal bleeding",
        "PMB - Postmenopausal bleeding", "Bleeding after menopause", "History of postmenopausal bleeding",
        "H/O: postmenopausal bleeding", "Postartificial menopausal syndrome", "Premature menopause",
        "Postmenopausal osteoporosis", "Postmenopausal osteoporosis; Multiple sites",
        "Postmenopausal osteoporosis with pathological fracture", "Postmenopausal atrophic vaginitis",
        "Post menopausal depression", "Postsurgical menopause", "Post-hysterectomy menopause",
        "States associated with artificial menopause", "History of natural age related menopause",
        "Postmenopausal hormone replacement therapy", "Postmenopausal urethral atrophy",
        "Postmenopausal endometrium", "Postmenopausal postcoital bleeding",
        "Endometrial cells, cytologically benign, in a postmenopausal woman"
    ]

    peri_menop = ["Peri-menopausal", "Perimenopausal", "Perimenopause", "Perimenopausal state",
                "Menopausal and female climacteric states", "Abnormal perimenopausal bleeding",
                "Perimenopausal disorder", "Perimenopausal atrophic vaginitis"]

    # Pre-compute lowered/trimmed lists for case-insensitive matching
    pre_menop_lower = [s.lower().strip() for s in pre_menop]
    post_menop_lower = [s.lower().strip() for s in post_menop]
    peri_menop_lower = [s.lower().strip() for s in peri_menop]

    menopause = (
        comb_prob_diag.alias("c")
        .join(
            breast_cancer_cohort.alias("b"),
            col("c.PERSON_ID") == col("b.PERSON_ID"),
            "inner"
        )
        .join(
            pharos_person_sex.alias("s"),
            col("s.person_id") == col("b.PERSON_ID"),
            "left"
        )
        .filter(col("condition_date") <= col("brc_diag_date"))
        .withColumn(
            "menopausal_status_flag",
            when(col("sex") == "M", "G04 - Not applicable")
            .when(lower(trim(col("SOURCE_STRING"))).isin(pre_menop_lower), "001 - Pre-menopausal")
            .when(lower(trim(col("SOURCE_STRING"))).isin(peri_menop_lower), "002 - Peri-menopausal")
            .when(lower(trim(col("SOURCE_STRING"))).isin(post_menop_lower), "003 - Post-menopausal")
        )
        .groupBy(col("b.PERSON_ID"))
        .agg(max("menopausal_status_flag").alias("menopausal_status"))
    )

    processed_numeric_events = (
        numeric_events
        .join(check_date_range, ["PERSON_ID"], "left")
        .filter(
            col("PERFORMED_DT_TM").between(col("check_onset_date"), col("check_offset_date"))
        )
        # Get the absolute time difference between the event and the earliest diagnosis date to help identify the most recent events.
        .withColumn(
            "abs_diff",
            abs(datediff(col("PERFORMED_DT_TM"),col("brc_diag_date")))
        )
    )

    # Get source codes for the smoking amount and aclcohol amount
    smoke_codes = [
        4127902,    # Number of Cigarettes Per Day Now
        71834925,   # Cigarettes Per Day at Booking
        71835447,   # Cigarettes Per Day at Delivery
        472635529,  # CCO cigarettes per day
        662214545,  # CCO cigarettes day
        999498499   # How many cigarettes do you usually smoke
    ]
    alcohol_codes = [
        71839205,   # Standard lifestyle measure of weekly consumption.
        71835023,   # Alcohol Units Pre Pregnancy Per Week
        71834759,   # Alcohol Units at Booking Per Week
        71835548,   # Alcohol Units at Delivery Per Week
        71844122    # (M) Alcohol Units Pre Pregnancy Per Week
        ]
    
    # Get the smoking and alcohol numbers for each patient if it exists
    window = Window.partitionBy("PERSON_ID", "lifestyle").orderBy(col("abs_diff").asc())

    smoke_alcohol_no = (
        processed_numeric_events
        .withColumn("lifestyle",
            when(col("EVENT_CD").isin(smoke_codes), "smoking_no")
            .when(col("EVENT_CD").isin(alcohol_codes), "alcohol_no")
        )
        .filter(col("lifestyle").isNotNull())
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .groupBy("PERSON_ID")
        .pivot("lifestyle", ["smoking_no", "alcohol_no"])
        .agg(first("NUMERIC_RESULT"))
    )

    # HEIGHT, WEIGHT, BMI

    # Closest measurement per PERSON
    window = (
        Window
        .partitionBy("PERSON_ID", "OMOP_MANUAL_CONCEPT_NAME")
        .orderBy(col("abs_diff").asc())
    )

    height_weight = (
        processed_numeric_events
        .filter(
            col("OMOP_MANUAL_CONCEPT_NAME").isin("Body height measure","Body weight measure")
        )
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .groupBy("PERSON_ID")
        .pivot("OMOP_MANUAL_CONCEPT_NAME",["Body height measure", "Body weight measure"])
        .agg(first("NUMERIC_RESULT"))
        .withColumn("height", col("Body height measure") / 100)
        .withColumnRenamed("Body weight measure", "weight")
        # Get bmi using height and weight
        .withColumn("bmi",col("weight") / (col("height") * col("height")))
        .select("PERSON_ID", "height", "weight", "bmi")
    )

    #PREGNANCY
    
    # Get the largest parity & gravidity number for each person
    window = Window.partitionBy("PERSON_ID", "EVENT_CD_DISPLAY").orderBy(col("NUMERIC_RESULT").desc())
    parity_gravidity = (
        numeric_events
        .filter(
            (col("PERFORMED_DT_TM") < col("brc_diag_date")) &
            (col("EVENT_CD_DISPLAY").isin("Parity", "Gravida"))
        )
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .groupBy("PERSON_ID")
        .pivot("EVENT_CD_DISPLAY",["Parity", "Gravida"])
        .agg(first("NUMERIC_RESULT"))
    )

    processed_pregnancy = (
        parity_gravidity.alias("p")
        .join(
            pharos_person_sex.alias("s"),
            col("s.person_id") == col("p.PERSON_ID"),
            "left"
        )
        .withColumn("parous",
                    when((col("Parity") == 0), "G00 No")
                    .when((col("Parity") > 0), "G01 Yes")
                    .when(col("sex") == "M", "G04 Not applicable")
                    )
        .select("p.PERSON_ID", "parous", col("Parity").alias("parity_no"), col("Gravida").alias("gravidity_no"))
        .dropDuplicates()
    )

    breastfeed_items = [
        "Partial Breastfeeding", "Exclusive Breastfeeding", "Breast and complementary feeds",
        "Partially breast milk feeding", "Exclusively breast milk feeding", "Other: BREASTFEEDING",
        "Other: MIX FEEDINJG", "Other: MIX FEEDIMG", "Other: Expressed breast milk",
        "Other: Mixed as per maternal request", "Other: Mixfeeding. Explained to always offer breast first and bottle afterwards.",
        "Other: MIXED", "Other: MIX  FEEDING", "Other: breastfeeding and topping up with EBM",
        "Other: EBM via bottle", "Other: ebm", "Other: mix feedings", "Other: mix feeeding",
        "Other: BF+bottle", "Other: MIX FEDING", "Other: Expressed Breast Milk", "Other: MIX FEEDIN",
        "Other: EBM + formula feeding", "Other: mixed", "Other: NMIX FEEDING", "Other: Mix feeding",
        "Other: Breastfeeds and tops up with formular milk.", "Other: B/F", "Other: bottle EBM",
        "Other: MX FEEDING", "Other: Mixed Feeding", "Other: MIX FEEDIG", "Other: Breast borderline IGUR",
        "Other: MIX FEEEDING", "Other: gGiving EBM from her finger", "Other: BREAST FEEDING PLUS EBM BY BOTTLE",
        "Other: mix feeing", "Other: MIX", "Other: MI X FEEDING", "Other: EBM and complementary feeds",
        "Other: expressed milk via bottle", "Other: Breast and formulae", "Other: EBM + artificial",
        "Other: Expressed breastmilk via bottle", "Other: N/MIX FEEDING", "Other: finger feeding",
        "Other: mixfeeding", "Other: MIX FEEDINBG", "Other: MMIX FEEDING", "Other: Mixed feeding",
        "Other: CUP FEEDING", "Other: MIX  FEDDING", "Other: breastfeeding and giving EBM 90mls 3hrly",
        "Other: EBM"
    ]
    breastfeed  = (
        birth.alias("bi")
        .join(
            pharos_person_sex.alias("s"),
            col("s.person_id") == col("bi.MotherPerson_ID"),
            "left"
        )
        .withColumn(
            "bf_flag",
            when(col("FeedingMethod").isin("Artificial", "No breast feeding at all", "atifical feeding",), "G00 No")
            .when(col("FeedingMethod").isin(breastfeed_items), "G01 Yes")
            .when(col("sex") == "M", "G04 Not applicable")
        )
        .groupBy("MotherPerson_ID")
        .agg(F.max("bf_flag").alias("breastfeed"))
        .select(col("MotherPerson_ID").alias("PERSON_ID"), "breastfeed")
    )


    pregnancy = (
        processed_pregnancy
        .join(breastfeed, ["PERSON_ID"], "left")
        .fillna("G09 Unknown", ["parous","breastfeed"])
    )

    processed_df = (
        breast_cancer_cohort
        .join(family_history, ["PERSON_ID"], "left")
        .join(smoke_alcohol_drug, ["PERSON_ID"], "left")
        .join(smoke_alcohol_no, ["PERSON_ID"], "left")
        .join(height_weight, ["PERSON_ID"], "left")
        .join(menopause,["PERSON_ID"], "left")
        .join(pregnancy, ["PERSON_ID"], "left")
        .fillna("G09 Unknown", ["familyhistory_bca","familyhistory_ovarian","familyhistory_cancer","menopausal_status"])
        .drop("brc_diag_date")
        .dropDuplicates())

    final_df = (
        processed_df
        .select(
            col("PERSON_ID").cast(LongType()).alias("person_id"),
            lit(None).cast(StringType()).alias("pharosid"),
            lit(None).cast(LongType()).alias("clinical_record_id"),
            col("familyhistory_bca").cast(StringType()),
            col("familyhistory_ovarian").cast(StringType()),
            col("familyhistory_cancer").cast(StringType()),
            lit(None).cast(StringType()).alias("genetic_testing"),
            lit(None).cast(StringType()).alias("any_germline_mutation"),
            lit(None).cast(StringType()).alias("brca1"),
            lit(None).cast(StringType()).alias("brca2"),
            lit(None).cast(StringType()).alias("tp53"),
            lit(None).cast(StringType()).alias("palb2"),
            lit(None).cast(StringType()).alias("chek2"),
            lit(None).cast(StringType()).alias("atm"),
            lit(None).cast(StringType()).alias("rad51c"),
            lit(None).cast(StringType()).alias("rad51d"),
            lit(None).cast(StringType()).alias("genetic_testing_details"),
            col("menopausal_status").cast(StringType()),
            lit(None).cast(IntegerType()).alias("menopause_age"),
            lit(None).cast(StringType()).alias("inferred_menopausal_status"),
            lit(None).cast(StringType()).alias("hrt"),
            lit(None).cast(StringType()).alias("hrt_years"),
            lit(None).cast(StringType()).alias("contraception_use"),
            lit(None).cast(StringType()).alias("contraception_details"),
            lit(None).cast(StringType()).alias("presentation"),
            col("height").cast(FloatType()).alias("height_diagnosis"),
            col("weight").cast(FloatType()).alias("weight_diagnosis"),
            col("bmi").cast(FloatType()).alias("bmi_diagnosis"),
            col("smoking").cast(StringType()),
            col("smoking_no").cast(FloatType()),
            lit(None).cast(IntegerType()).alias("smoking_years"),
            col("vape").cast(StringType()),
            col("alcohol").cast(StringType()),
            col("alcohol_no").cast(FloatType()),
            col("drug").cast(StringType()),
            lit(None).cast(StringType()).alias("drug_details"),
            lit(None).cast(StringType()).alias("performance_diagnosis"),
            col("parous").cast(StringType()),
            col("gravidity_no").cast(FloatType()),
            col("parity_no").cast(FloatType()),
            lit(None).cast(StringType()).alias("age_first_pregnancy"),
            col("breastfeed").cast(StringType()),
            lit(None).cast(StringType()).alias("pabc"),
            lit(None).cast(StringType()).alias("time_pregnancytobc"),
            col("_src_adc_updt").alias("ADC_UPDT"),
            current_timestamp().alias("date_checked"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
    )

    return final_df

updates_df = create_medical_history_incr()


# update_table(updates_df, get_target_table("pharos_medical_history"), ["person_id"], schema_pharos_medical_history, pharos_medical_history_comment)
update_table(updates_df,"8_dev.silver.pharos_medical_history", ["person_id"], schema_pharos_medical_history, pharos_medical_history_comment)

# COMMAND ----------

pharos_fh_comment = "This table captures family history of cancer."

schema_pharos_fh = StructType([
    StructField(
        name="family_history_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Surrogate PK."}
    ), 
    StructField(
        name="person_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Assigned unique ID for each participant."}
    ),   
    StructField(
        name="pharosid",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "FK to person."}
    ),
    StructField(
        name="clinical_record_id",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Unique clinical record PharosID."}
    ),
    StructField(
        name="cancer_type",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Type of cancer in family member."}
    ),
    StructField(
        name="relation_degree",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Highest degree relation."}
    ),
    StructField(
        name="relation_detail",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "Specific relation (e.g. Mother, Sister, Aunt)."}
    ),
    StructField(
        name="date_checked",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Timestamp of the update"}
    ),
    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )
])

# Family history is derived from two sources:
# 1. The map_family_history table
# 2. The map_problem and map_diagnosis tables using relevant family history codes (OMOP and ICD10)

def create_fh_incr():
    max_adc_updt = get_max_timestamp(
        table_name=get_target_table("pharos_family_cancer_history"),
        ts_column="ADC_UPDT")

    map_diagnosis = spark.table("4_prod.bronze.map_diagnosis")
    map_problem = spark.table("4_prod.bronze.map_problem")
    map_family_history = spark.table("4_prod.bronze.map_family_history")

    # Get the breast cancer cohort
    breast_cancer_cohort = (
        map_diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (
                col("ICD10_CODE").like("C50%") |
                col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331)
            )
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("brc_diag_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )

    comb_prob_diag = (
    map_problem
    .select("PERSON_ID", "SOURCE_STRING", "SOURCE_IDENTIFIER", "OMOP_CONCEPT_ID", "SNOMED_CODE", "ICD10_CODE", col("ONSET_DT_TM").alias("condition_date"))
    .unionByName(
        map_diagnosis
        .select("PERSON_ID", "SOURCE_STRING", "SOURCE_IDENTIFIER", "OMOP_CONCEPT_ID", "SNOMED_CODE", "ICD10_CODE", col("DIAG_DT_TM").alias("condition_date")))
    .dropDuplicates())
    # Get data from map_family_history

    cancer_type = (
        when(col("ICD10_CODE").rlike("^C74"), "001 - Adrenal cancer")
        .when(col("ICD10_CODE").rlike("^C21"), "002 - Anal cancer")
        .when(col("ICD10_CODE").rlike("^(C22\\.1|C24\\.0)"), "003 - Bile duct cancer")
        .when(col("ICD10_CODE").rlike("^C67"), "004 - Bladder cancer")
        .when(col("ICD10_CODE").rlike("^(C40|C41)"), "005 - Bone cancer")
        .when(col("ICD10_CODE").rlike("^C71"), "006 - Brain cancer")
        .when(col("ICD10_CODE").rlike("^C50"), "007 - Breast cancer")
        .when(col("ICD10_CODE").rlike("^C53"), "008 - Cervical cancer")
        .when(col("ICD10_CODE").rlike("^C18"), "010 - Colon cancer")
        .when(col("ICD10_CODE").rlike("^(C18|C19|C20)"), "011 - Colorectal cancer")
        .when(col("ICD10_CODE").rlike("^C54\\.1"), "012 - Endometrial cancer")
        .when(col("ICD10_CODE").rlike("^C69"), "013 - Eye cancer")
        .when(col("ICD10_CODE").rlike("^C57\\.0"), "014 - Fallopian tube cancer")
        .when(col("ICD10_CODE").rlike("^C23"), "015 - Gallbladder cancer")
        .when(col("ICD10_CODE").rlike("^C16"), "016 - Gastric cancer")
        .when(col("ICD10_CODE").rlike("^C64"), "019 - Kidney cancer")
        .when(col("ICD10_CODE").rlike("^C32"), "020 - Laryngeal cancer")
        .when(col("ICD10_CODE").rlike("^(C91|C92|C93|C94|C95)"), "021 - Leukaemia")
        .when(col("ICD10_CODE").rlike("^C22"), "022 - Liver cancer")
        .when(col("ICD10_CODE").rlike("^C34"), "023 - Lung cancer")
        .when(col("ICD10_CODE").rlike("^(C81|C82|C83|C84|C85|C86|C88)"), "024 - Lymphoma")
        .when(col("ICD10_CODE").rlike("^C38"), "025 - Mediastinal cancer")
        .when(col("ICD10_CODE").rlike("^C43"), "026 - Melanoma")
        .when(col("ICD10_CODE").rlike("^D46"), "027 - Myelodysplastic syndrome")
        .when(col("ICD10_CODE").rlike("^C90"), "028 - Myeloma")
        .when(col("ICD10_CODE").rlike("^(D45|D47\\.1|D47\\.3|D47\\.4)"), "029 - Myeloproliferative neoplasm")
        .when(col("ICD10_CODE").rlike("^C11"), "030 - Nasopharyngeal cancer")
        .when(col("ICD10_CODE").rlike("^C15"), "033 - Oesophageal cancer")
        .when(col("ICD10_CODE").rlike("^(C00|C01|C02|C03|C04|C05|C06)"), "034 - Oral cancer")
        .when(col("ICD10_CODE").rlike("^(C09|C10)"), "035 - Oropharyngeal cancer")
        .when(col("ICD10_CODE").rlike("^C56"), "036 - Ovarian cancer")
        .when(col("ICD10_CODE").rlike("^C25"), "037 - Pancreatic cancer")
        .when(col("ICD10_CODE").rlike("^C75\\.0"), "038 - Parathyroid cancer")
        .when(col("ICD10_CODE").rlike("^C60"), "039 - Penile cancer")
        .when(col("ICD10_CODE").rlike("^(C38\\.4|C45\\.0)"), "040 - Pleural cancer")
        .when(col("ICD10_CODE").rlike("^C48"), "041 - Primary peritoneal cancer")
        .when(col("ICD10_CODE").rlike("^C61"), "042 - Prostate cancer")
        .when(col("ICD10_CODE").rlike("^C20"), "043 - Rectal cancer")
        .when(col("ICD10_CODE").rlike("^(C07|C08)"), "044 - Salivary gland cancer")
        .when(col("ICD10_CODE").rlike("^(C30\\.0|C31)"), "045 - Sinonasal cancer")
        .when(col("ICD10_CODE").rlike("^(C43|C44)"), "046 - Skin cancer")
        .when(col("ICD10_CODE").rlike("^C17"), "047 - Small bowel cancer")
        .when(col("ICD10_CODE").rlike("^(C47|C49)"), "048 - Soft tissue sarcoma")
        .when(col("ICD10_CODE").rlike("^C72\\.0"), "049 - Spinal cord cancer")
        .when(col("ICD10_CODE").rlike("^C62"), "050 - Testicular cancer")
        .when(col("ICD10_CODE").rlike("^C37"), "051 - Thymus cancer")
        .when(col("ICD10_CODE").rlike("^C73"), "052 - Thyroid cancer")
        .when(col("ICD10_CODE").rlike("^(C65|C66)"), "053 - Upper urinary tract cancer")
        .when(col("ICD10_CODE").rlike("^C68\\.0"), "054 - Urethral cancer")
        .when(col("ICD10_CODE").rlike("^C52"), "055 - Vaginal cancer")
        .when(col("ICD10_CODE").rlike("^C51"), "056 - Vulvar cancer")
        .otherwise("G09 - Unknown")
    )
    family_history_1 = (
        map_family_history
        .filter(col("ICD10_CODE").rlike(r"^(C|D)"))
        .join(breast_cancer_cohort, "PERSON_ID", "left")
        .withColumn(
            "familyhistory_re",
            # 1st degree relatives: Mother, Father, Sister, Brother, Daughter, Son
            when(col("RELATION_CD").isin(81849783,81849776,81849760,81849757,81849790,81849795,153,160), 1)
            # 2nd degree relatives: Grandmother, Grandfather, Aunt, Uncle, Niece, Nephew
            .when(col("RELATION_CD").isin(81849796,81849770,81849784,81849774,81849785,81849791,81849786), 2)
            # 3rd degree relatives: Cousin
            .when(col("RELATION_CD") == 81849761, 3)
            # 7 None: Spouse, Partner, Wife, Husband, Step Parents, Foster Parents
            .when(col("RELATION_CD").isin(634771,81849794,81849793,81849762,81849777,81849797),7)
            # 9 Unknown: Not Specified, Null
            .otherwise(lit(9))
        )
        .withColumn(
            "relation_degree",
            when(col("familyhistory_re") == 1, "001 - First degree")
            .when(col("familyhistory_re") == 2, "002 - Second degree")
            .when(col("familyhistory_re") == 3, "003 - Third degree")
            .when(col("familyhistory_re") == 7, "000 - None")
            .otherwise("G09 - Unknown")
        )
        .withColumn("cancer_type", cancer_type)
        .select("PERSON_ID", "relation_degree", col("RELATION_DESC").alias("relation_detail"),"cancer_type", "_src_adc_updt")
    )


    # OMOP concept mappings
    omop_to_category = {
        # 005 - Bone cancer
        4324203: "005 - Bone cancer",      # Family history of bone cancer

        # 006 - Brain cancer
        4323762: "006 - Brain cancer",     # Family history of brain cancer

        # 007 - Breast cancer
        42535500: "007 - Breast cancer",   # Family history of breast cancer gene BRCA mutation
        3078338013: "007 - Breast cancer", # Family history of breast cancer gene mutation in first degree relative
        4176765: "007 - Breast cancer",    # Family history of male breast cancer
        4328583: "007 - Breast cancer",    # Family history of breast cancer in first degree relative <50
        35624517: "007 - Breast cancer",   # Family history of breast cancer <50 in second degree female relative
        4179963: "007 - Breast cancer",    # Family history of breast cancer
        4329111: "007 - Breast cancer",    # Family history of breast cancer 2 gene mutation
        4160695: "007 - Breast cancer",    # Family history of breast cancer 1 gene mutation
        46270135: "007 - Breast cancer",   # Family history of breast cancer gene mutation in first degree relative
        46270155: "007 - Breast cancer",   # Family history of malignant neoplasm of breast diagnosed before 45 years of age
        46270130: "007 - Breast cancer",   # Family history of malignant neoplasm of breast at under age 50 in second degree female relative

        # 008 - Cervical cancer
        4175994: "008 - Cervical cancer",  # Family history of malignant neoplasm of cervix uteri

        # 010 - Colon cancer
        4195970: "010 - Colon cancer",     # Family history of cancer of colon
        45884753: "010 - Colon cancer",    # Family history of colon cancer
        42535054: "010 - Colon cancer",    # Family history of colon cancer over age 50

        # 011 - Colorectal cancer
        43530673: "011 - Colorectal cancer",   # Family history of colorectal cancer
        46273481: "011 - Colorectal cancer",   # Family history of hereditary nonpolyposis colon cancer

        # 012 - Endometrial cancer
        46274041: "012 - Endometrial cancer",  # Family history of endometrium cancer

        # 013 - Eye cancer
        4179082: "013 - Eye cancer",       # Family history of eye cancer

        # 016 - Gastric cancer
        1243977: "016 - Gastric cancer",   # Family history of hereditary diffuse gastric cancer

        # 022 - Liver cancer
        4324202: "022 - Liver cancer",     # Family history of liver cancer

        # 033 - Oesophageal cancer
        4326336: "033 - Oesophageal cancer",   # Family history of cancer of esophagus

        # 034 - Oral cancer
        764948: "034 - Oral cancer",       # Family history of oral cavity cancer

        # 036 - Ovarian cancer
        37117109: "036 - Ovarian cancer",  # Family history of malignant neoplasm of ovary in first degree relative
        4326681: "036 - Ovarian cancer",   # Family history of malignant neoplasm of ovary
        37109210: "036 - Ovarian cancer",  # Family history of malignant neoplasm of ovary in second degree relative

        # 037 - Pancreatic cancer
        4179232: "037 - Pancreatic cancer",    # Family history of pancreatic cancer

        # 042 - Prostate cancer
        4210263: "042 - Prostate cancer",  # Family history of malignant neoplasm of prostate

        # 047 - Small bowel cancer
        4177058: "047 - Small bowel cancer",   # Family history of ileum cancer

        # 050 - Testicular cancer
        4327415: "050 - Testicular cancer",    # Family history of testis cancer

        # 052 - Thyroid cancer
        4328801: "052 - Thyroid cancer",   # Family history of thyroid cancer

        # 053 - Upper urinary tract cancer
        37311977: "053 - Upper urinary tract cancer",   # Family history of malignant neoplasm of urinary tract
        46273150: "053 - Upper urinary tract cancer",   # Family history of ureter cancer

        # 054 - Urethral cancer
        46273151: "054 - Urethral cancer", # Family history of urethra cancer

        # 055 - Vaginal cancer
        4322902: "055 - Vaginal cancer",   # Family history of vagina cancer

        # G09 - Unknown
        4334494: "G09 - Unknown",          # Family history of digestive organ cancer
        4334339: "G09 - Unknown",          # Family history of thoracic cavity structure cancer
    }

    icd10_to_category = {
        "Z80.0": "G09 - Unknown",
        "Z80.1": "023 - Lung cancer",
        "Z80.2": "G09 - Unknown",
        "Z80.3": "007 - Breast cancer",
        "Z80.4": "036 - Ovarian cancer",
        "Z80.5": "053 - Upper urinary tract cancer",
        "Z80.6": "021 - Leukaemia",
        "Z80.7": "024 - Lymphoma",
        "Z80.8": "G09 - Unknown",
        "Z80.9": "G09 - Unknown",
    }
    # Convert Python dictionaries to Spark maps
    omop_map = F.create_map(
        *[F.lit(x) for kv in omop_to_category.items() for x in kv]
    )

    icd10_map = F.create_map(
        *[F.lit(x) for kv in icd10_to_category.items() for x in kv]
    )

    family_history_2 = (
        comb_prob_diag
        .join(breast_cancer_cohort, "PERSON_ID", "left")
        .withColumn(
            "cancer_type",
            F.coalesce(
                omop_map[F.col("OMOP_CONCEPT_ID")],
                icd10_map[F.col("ICD10_CODE")]
            )
        )
        .filter(col("cancer_type").isNotNull())
        .withColumn(
            "relation_degree",
            when(col("OMOP_CONCEPT_ID").isin(3078338013, 46270135, 4328583, 37117109), "001 - 1st degree")
            .when(col("OMOP_CONCEPT_ID").isin(35624517, 46270130, 37109210), "002 - 2nd degree")
            .otherwise("G09 - Unknown")
        )
        .withColumn("relation_detail", F.lit(None))
        .select("PERSON_ID", "relation_degree", "relation_detail","cancer_type","_src_adc_updt")
    )
    # Combine the two data source together
    combined_family_history = (
        family_history_1
        .unionByName(family_history_2)
        .dropDuplicates()
    )
    # Select the highest degree relation
    ranked_family_history = (
        combined_family_history
        .withColumn(
            "relation_rank",
            F.when(F.col("relation_degree").startswith("001"), 1)
            .when(F.col("relation_degree").startswith("002"), 2)
            .when(F.col("relation_degree").startswith("003"), 3)
            .when(F.col("relation_degree") == "G09 - Unknown", 4)
            .when(F.col("relation_degree") == "000 - None", 5)
            .otherwise(99)
        )
    )

    w = Window.partitionBy("PERSON_ID", "cancer_type") \
            .orderBy(F.col("relation_rank"))

    result = (
        ranked_family_history
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn", "relation_rank")
    )


    final_df = (
        result
        .select(
            lit(None).alias("family_history_id"),
            lit(None).alias("pharosid"),
            lit(None).alias("clinical_record_id"),
            col("PERSON_ID").alias("person_id"),
            col("cancer_type"),
            col("relation_degree"),
            col("relation_detail"),
            col("_src_adc_updt").alias("ADC_UPDT"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
            current_timestamp().alias("date_checked"),            
            )
    )
    return final_df


updated_df = create_fh_incr()

update_table(updated_df, get_target_table("pharos_family_cancer_history"), ["person_id", "cancer_type"], schema_pharos_fh, pharos_fh_comment)


# COMMAND ----------

pharos_tumour_comment = "Clinical characteristics at breast cancer diagnosis for participants in the PHAROS cohort."

schema_pharos_tumour = StructType([
    StructField(
        "tumour_id",
        LongType(),
        True,
        {"comment": "Surrogate PK"}
    ),
    StructField(
        "person_id",
        LongType(),
        True,
        {"comment": "Assigned unique ID for each participant"}
    ),
    StructField(
        "pharosid",
        StringType(),
        True,
        {"comment": "Surrogate PK"}
    ),
    StructField(
        name="clinical_record_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Unique clincial record PharosID."}
    ),
    StructField(
        "date_of_diagnosis",
        DateType(),
        True,
        {"comment": "Patient's initial cancer diagnosis date (biopsy or scan if metastatic)"}
    ),
    StructField(
        "year_of_diagnosis",
        IntegerType(),
        True,
        {"comment": "Patient's initial cancer diagnosis year"}
    ),
    StructField(
        "age_at_diagnosis",
        IntegerType(),
        True,
        {"comment": "Age at date of diagnosis"}
    ),
    StructField(
        "disease_status",
        StringType(),
        True,
        {"comment": "Type of disease: Primary, Recurrence, New Primary, Metastasis, High Risk, Asymmetry, Completion, Cosmetic, Atypia, Benign, Normal Control"}
    ),
    StructField(
        "laterality",
        StringType(),
        True,
        {"comment": "Laterality of the cancer. If bilateral, create a new row for each side"}
    ),
    StructField(
        "bilateral",
        BooleanType(),
        True,
        {"comment": "Indication of if cancer is bilateral or not"}
    ),
    StructField(
        "invasive_size_clinical",
        DoubleType(),
        True,
        {"comment": "Size of invasive tumour in mm from radiology. If multifocal, size of largest focus"}
    ),
    StructField(
        "total_size_clinical",
        DoubleType(),
        True,
        {"comment": "Total size of the tumour in mm from radiology, including in-situ disease"}
    ),
    StructField(
        "c_ajcc_edition",
        StringType(),
        True,
        {"comment": "TNM staging edition used"}
    ),
    StructField(
        "clinical_t_stage",
        StringType(),
        True,
        {"comment": "Clinical/pre-treatment T stage from TNM"}
    ),
    StructField(
        "clinical_n_stage",
        StringType(),
        True,
        {"comment": "Clinical/pre-treatment N stage from TNM"}
    ),
    StructField(
        "clinical_m_stage",
        StringType(),
        True,
        {"comment": "Clinical/pre-treatment M stage from TNM"}
    ),
    StructField(
        "clinical_stage",
        StringType(),
        True,
        {"comment": "Clinical/pre-treatment stage I-IV"}
    ),
    StructField(
        "diag_npi",
        DoubleType(),
        True,
        {"comment": "Nottingham Prognostics Index (NPI)"}
    ),
    StructField(
        "disease_inflammatory",
        BooleanType(),
        True,
        {"comment": "Inflammatory Breast cancer (If yes, T stage should be T4)"}
    ),
    StructField(
        "neoadjuvant_indication",
        StringType(),
        True,
        {"comment": "Whether or not patient had neo-adjuvant treatment"}
    ),
    StructField(
        "dx_test",
        StringType(),
        True,
        {"comment": "Oncotype DX Testing performed (Yes/No)"}
    ),
    StructField(
        "dx_node_status",
        StringType(),
        True,
        {"comment": "Oncotype DX Nodal Status"}
    ),
    StructField(
        "dx_er_score",
        DoubleType(),
        True,
        {"comment": "ER Gene Score as recorded on Oncotype DX results report"}
    ),
    StructField(
        "dx_pr_score",
        DoubleType(),
        True,
        {"comment": "PR Gene Score as recorded on Oncotype DX results"}
    ),
    StructField(
        "dx_her2_score",
        DoubleType(),
        True,
        {"comment": "HER2 Gene Score as recorded on Oncotype DX results"}
    ),
    StructField(
        "oncotype_dx_score",
        IntegerType(),
        True,
        {"comment": "Recurrence Score from Oncotype DX test"}
    ),
    StructField(
        "clin_response",
        StringType(),
        True,
        {"comment": "Response to treatment reported in EOT imaging"}
    ),
    StructField(
        "ADC_UPDT",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
    ),
    StructField(
        "date_checked",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Last update timestamp."}
    ),
    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )
])

def create_pharos_tumour_incr():

    max_adc_updt = get_max_timestamp(get_target_table("pharos_tumour"), "ADC_UPDT")

    diagnosis = spark.table("4_prod.bronze.map_diagnosis")
    person = spark.table("4_prod.bronze.map_person").select(col("person_id").alias("PERSON_ID"),"birth_year")

    # Get the breast cancer cohort
    breast_cancer_cohort = (
        diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (col("ICD10_CODE").like("C50%") |
            col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
        )
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("date_of_diagnosis"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
        .withColumn("year_of_diagnosis", year(col("date_of_diagnosis")))
        .join(person, ["PERSON_ID"], "left")
        .withColumn("age_at_diagnosis", col("year_of_diagnosis") - col("birth_year"))
        .select("PERSON_ID", "date_of_diagnosis", "year_of_diagnosis", "age_at_diagnosis", "_src_adc_updt")
        .dropDuplicates()
    )

    final_df = (
        breast_cancer_cohort
        .select(
            lit(None).cast(LongType()).alias("tumour_id"),
            col("PERSON_ID").cast(LongType()).alias("person_id"),
            lit(None).cast(StringType()).alias("pharosid"),
            lit(None).cast(LongType()).alias("clinical_record_id"),
            col("date_of_diagnosis").cast(DateType()),
            col("year_of_diagnosis").cast(IntegerType()),
            col("age_at_diagnosis").cast(IntegerType()),
            lit(None).cast(StringType()).alias("disease_status"),
            lit(None).cast(StringType()).alias("laterality"),
            lit(None).cast(BooleanType()).alias("bilateral"),
            lit(None).cast(DoubleType()).alias("invasive_size_clinical"),
            lit(None).cast(DoubleType()).alias("total_size_clinical"),
            lit(None).cast(StringType()).alias("c_ajcc_edition"),
            lit(None).cast(StringType()).alias("clinical_t_stage"),
            lit(None).cast(StringType()).alias("clinical_n_stage"),
            lit(None).cast(StringType()).alias("clinical_m_stage"),
            lit(None).cast(StringType()).alias("clinical_stage"),
            lit(None).cast(DoubleType()).alias("diag_npi"),
            lit(None).cast(BooleanType()).alias("disease_inflammatory"),
            lit(None).cast(StringType()).alias("neoadjuvant_indication"),
            lit(None).cast(StringType()).alias("dx_test"),
            lit(None).cast(StringType()).alias("dx_node_status"),
            lit(None).cast(DoubleType()).alias("dx_er_score"),
            lit(None).cast(DoubleType()).alias("dx_pr_score"),
            lit(None).cast(DoubleType()).alias("dx_her2_score"),
            lit(None).cast(IntegerType()).alias("oncotype_dx_score"),
            lit(None).cast(StringType()).alias("clin_response"),
            col("_src_adc_updt").alias("ADC_UPDT"),
            current_timestamp().alias("date_checked"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
    )
    return final_df


updated_df = create_pharos_tumour_incr()

update_table(updated_df, get_target_table("pharos_tumour"), ["person_id", "tumour_id"], schema_pharos_tumour, pharos_tumour_comment)


# COMMAND ----------

pharos_imaging_comment = "This table contains longitudinal diagnostic imaging data for patients within the Pharos breast cancer cohort. It captures specific radiology metrics including lesion counts, breast density, ACR BI-RADS scores, and timing relative to the initial diagnosis date. Each row represents a unique imaging event per patient."

schema_pharos_imaging = StructType([
    StructField(
        "imaging_id",
        LongType(),
        True,
        {"comment": "Assigned unique ID for each participant"}
    ),
    StructField(
        "person_id",
        LongType(),
        True,
        {"comment": "Assigned unique ID for each participant"}
    ),
    StructField(
        name="pharosid",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "FK to person."}
    ),
    StructField(
        name="clinical_record_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Unique clincial record PharosID."}
    ),
    StructField(
        "image_date",
        DateType(),
        True,
        {"comment": "Imaging date"}
    ),
        StructField(
        "imaging_session_id",
        StringType(),
        True,
        {"comment": "The session ID for the imaging event"}
    ),
    StructField(
        "days_diagnosis_imaging",
        IntegerType(),
        True,
        {"comment": "Time in days between date diagnosis and date of imaging/radiology"}
    ),
    StructField(
        "imaging_type",
        StringType(),
        True,
        {"comment": "Type of diagnostic imaging"}
    ),
    StructField(
        "image_status",
        StringType(),
        True,
        {"comment": "Imaging rationale"}
    ),
    StructField(
        "image_site",
        StringType(),
        True,
        {"comment": "Imaging site. If multiple present, to be listed separated by a semi-colon (;)"}
    ),
    StructField(
        "image_contrast",
        StringType(),
        True,
        {"comment": "Contrast used"}
    ),
    StructField(
        "image_lesions",
        IntegerType(),
        True,
        {"comment": "Number of lesions"}
    ),
    StructField(
        "image_cal",
        StringType(),
        True,
        {"comment": "Calcification as recorded in the imaging report"}
    ),
    StructField(
        "breast_density",
        StringType(),
        True,
        {"comment": "Either clinician assessed or machine assessed breast density as recorded in the imaging report"}
    ),
    StructField(
        "acr_score",
        StringType(),
        True,
        {"comment": "ACR BI-RADS Score e.g. bilateral: M1; right: M2, left: M3"}
    ),
    StructField(
        "rcr_score",
        StringType(),
        True,
        {"comment": "RCR UK Score e.g. bilateral: M1; right: M2, left: M3 (mammogram); U1, U2, U3 (ultrasound)"}
    ),
    StructField(
        "index_lesion_size",
        IntegerType(),
        True,
        {"comment": "Size of the largest tumour in mm from radiology (invasive/in-situ not specified)"}
    ),
    StructField(
        "index_lesion_quadrant",
        StringType(),
        True,
        {"comment": "Quadrant of the largest tumor (index lesion) e.g. upper left, lower right"}
    ),
    StructField(
        "index_lesion_distance",
        FloatType(),
        True,
        {"comment": "Distance of the largest tumour (index lesion) from nipple"}
    ),
    StructField(
        "index_lesion_laterality",
        StringType(),
        True,
        {"comment": "Laterality of the largest tumor (index lesion) e.g. left, right"}
    ),
    StructField(
        "image_quality_notes",
        StringType(),
        True,
        {"comment": "Any notes from the radiology report on image quality including image artefacts such as motion "}
    ),
    StructField(
        "ADC_UPDT",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
    ),
    StructField(
        "date_checked",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Last update timestamp."}
    ),
    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )
])

def create_pharos_imaging_incr():

    max_adc_updt = get_max_timestamp(get_target_table("pharos_imaging"), "ADC_UPDT")
    diagnosis = spark.table("4_prod.bronze.map_diagnosis")
    imaging_meta = spark.table("4_prod.pacs.imaging_metadata")

    # Get the breast cancer cohort
    breast_cancer_cohort = (
        diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (col("ICD10_CODE").like("C50%") | col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("brc_diag_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )


    # IMAGING: Modality aggregation within diagnostic window
    imaging = (
        imaging_meta
        .join(breast_cancer_cohort, col("PERSON_ID") == col("PersonId"), "inner")
        # Lower bound: 6 months before diagnosis; upper bound: 5 years after diagnosis
        .filter(
            (col("MillEventDate") > date_sub(col("brc_diag_date"), 180)) &
            (col("MillEventDate") <= date_add(col("brc_diag_date"), 1825))
        )
        .withColumn("days_diagnosis_imaging", datediff(col("MillEventDate"), col("brc_diag_date")))
        .withColumn(
            "imaging_type",
            # 0: Mammogram (Standard 2D)
            when((lower(col("ExaminationModality")) == "mg") & (~lower(col("ExaminationModality")).contains("tomo")), "000 - Mammogram")
            # 1: Ultrasound
            .when(lower(col("ExaminationModality")).rlike("us|ivus"), "001 - Ultrasound")
            # 2: PET
            .when(lower(col("ExaminationModality")).rlike("pt|pet|gems pet raw"), "002 - PET")
            # 3: CT
            .when(lower(col("ExaminationModality")) == "ct", "003 - CT")
            # 4: MRI
            .when(lower(col("ExaminationModality")).rlike("mr"), "004 - MRI")
            # 5: Tomosynthesis (3D Mammography)
            .when(lower(col("ExaminationModality")).contains("tomo"), "005 - Tomosynthesis")
            # 6: X-ray
            .when(lower(col("ExaminationModality")).rlike("dx|cr|xa|fl|rf|px|io"), "006 - X-ray")
            # 7: Nuclear medicine (NM)
            .when(lower(col("ExaminationModality")).rlike("nm"), "007 - Nuclear medicine (NM)")
            .otherwise("G09 - Unknown")
        )
        .select(col("PERSON_ID").alias("person_id"), col("MillEventDate").alias("image_date"), "days_diagnosis_imaging", "imaging_type", col("ExaminationBodyPart").alias("image_site"), col("_src_adc_updt"))
        .dropDuplicates()
    )

    final_df = (
        imaging
        .select(
            col("person_id").cast(LongType()),
            col("image_date").cast(DateType()),
            lit(None).alias("pharosid").cast(StringType()),
            lit(None).alias("clinical_record_id").cast(LongType()),
            col("days_diagnosis_imaging").cast(IntegerType()),
            col("imaging_type").cast(StringType()),
            lit(None).alias("image_status").cast(StringType()),
            col("image_site").cast(StringType()),
            lit(None).alias("image_contrast").cast(StringType()),
            lit(None).alias("image_lesions").cast(IntegerType()),
            lit(None).alias("image_cal").cast(StringType()),
            lit(None).alias("breast_density").cast(StringType()),
            lit(None).alias("acr_score").cast(StringType()),
            lit(None).alias("rcr_score").cast(StringType()),
            lit(None).alias("index_lesion_size").cast(StringType()),
            lit(None).alias("index_lesion_quadrant").cast(StringType()),
            lit(None).alias("index_lesion_distance").cast(FloatType()),
            lit(None).alias("index_lesion_laterality").cast(StringType()),
            lit(None).alias("image_quality_notes").cast(StringType()),
            col("_src_adc_updt").alias("ADC_UPDT").cast(TimestampType()),
            current_timestamp().alias("date_checked").cast(TimestampType()),
            current_timestamp().alias("created_at").cast(TimestampType()),
            current_timestamp().alias("updated_at").cast(TimestampType())
        )
    )
    return final_df

updated_df = create_pharos_imaging_incr()

update_table(updated_df, get_target_table("pharos_imaging"), ["person_id", "image_date", "imaging_type"], schema_pharos_imaging, pharos_imaging_comment)


# COMMAND ----------

pharos_sample_comment = "This table serves as the biospecimen inventory for the Pharos cohort. It tracks the lineage of tissue samples from collection date through sequential laboratory procedures. It includes critical clinical context such as tissue pathology (e.g., tumour bed vs. benign), anatomical site, and timing relative to the patient's cancer diagnosis."

schema_pharos_sample = StructType([
    StructField(
        "sample_id",
        StringType(),
        True,
        {"comment": "Unique ID for each sample"}
    ),
    StructField(
        "person_id",
        LongType(),
        True,
        {"comment": "Assigned unique ID for each participant"}
    ),
    StructField(
        "pharosid",
        StringType(),
        True,
        {"comment": "FK to person"}
    ),
    StructField(
        name="clinical_record_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Unique clincial record PharosID."}
    ),
    StructField(
        "procedure_id",
        StringType(),
        True,
        {"comment": "FK to pathology"}
    ),
    StructField(
        "sample_collection_date",
        DateType(),
        True,
        {"comment": "Date of sample collection (DD-MM-YYYY)"}
    ),
    StructField(
        "days_diagnosistosample",
        IntegerType(),
        True,
        {"comment": "Time in days between diagnosis date and sample date"}
    ),
    StructField(
        "tissue_type",
        StringType(),
        True,
        {"comment": "Type of tissue collected (e.g., Non-tumour, Tumour bed, Normal, Benign)"}
    ),
    StructField(
        "tumour_sample",
        StringType(),
        True,
        {"comment": "Type of sampling specifically for tissue samples"}
    ),
    StructField(
        "sample_type",
        StringType(),
        True,
        {"comment": "Sample type stored in the Biobank"}
    ),
    StructField(
        "tissue_site",
        StringType(),
        True,
        {"comment": "Anatomical site where sample was taken from (e.g., Axillary lymph node for Sentinel nodes)"}
    ),
    StructField(
        "sample_procedure_1",
        StringType(),
        True,
        {"comment": "The first procedure performed on this specific sample"}
    ),
    StructField(
        "sample_procedure_2",
        StringType(),
        True,
        {"comment": "The second procedure performed on this specific sample"}
    ),
    StructField(
        "sample_procedure_3",
        StringType(),
        True,
        {"comment": "The third procedure performed on this specific sample"}
    ),
    StructField(
        "sample_laterality",
        StringType(),
        True,
        {"comment": "Side that the sample was taken from (Left, Right, Bilateral)"}
    ),
    StructField(
        "ADC_UPDT",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Last update timestamp."}
    ),
    StructField(
        "date_checked",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Last update timestamp."}
    ),
    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )
])

def create_pharos_sample_incr():
    max_adc_updt = get_max_timestamp(
        table_name=get_target_table("pharos_sample"),
        ts_column="ADC_UPDT")

    map_diagnosis = spark.table("4_prod.bronze.map_diagnosis")    
    breast_cancer_cohort = (
        map_diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (col("ICD10_CODE").like("C50%") | col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("brc_diag_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )
    
    final_df = (
        breast_cancer_cohort
        .select(
            lit(None).cast(StringType()).alias("sample_id"),
            col("PERSON_ID").cast(LongType()).alias("person_id"),
            lit(None).cast(StringType()).alias("pharosid"),
            lit(None).cast(LongType()).alias("clinical_record_id"),
            lit(None).cast(StringType()).alias("procedure_id"),
            lit(None).cast(TimestampType()).alias("sample_collection_date"),
            lit(None).cast(IntegerType()).alias("days_diagnosistosample"),
            lit(None).cast(StringType()).alias("tissue_type"),
            lit(None).cast(StringType()).alias("tumour_sample"),
            lit(None).cast(StringType()).alias("sample_type"),
            lit(None).cast(StringType()).alias("tissue_site"),
            lit(None).cast(StringType()).alias("sample_procedure_1"),
            lit(None).cast(StringType()).alias("sample_procedure_2"),
            lit(None).cast(StringType()).alias("sample_procedure_3"),
            lit(None).cast(StringType()).alias("sample_laterality"),
            col("_src_adc_updt").alias("ADC_UPDT"),
            current_timestamp().alias("date_checked"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
    )

    return final_df


updates_df = create_pharos_sample_incr()

update_table(updates_df,"8_dev.silver.pharos_sample", ["person_id","sample_id"], schema_pharos_sample, pharos_sample_comment)

# COMMAND ----------

pharos_pathology_comment = "This table contains detailed pathology and surgical information related to breast cancer samples collected from participants."

schema_pharos_pathology = StructType([
    StructField(
        "pathology_id",
        LongType(),
        nullable=True,
        metadata={"comment": "Surrogate PK"}
    ),
    StructField(
        "person_id",
        LongType(),
        nullable=True,
        metadata={"comment": "Assigned unique ID for each participant"}
    ),
    StructField(
        name="pharosid",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "FK to person."}
    ),
    StructField(
        name="clinical_record_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Unique clincial record PharosID."}
    ),
    StructField(
        name="procedure_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "ID for the surgical or biopsy procedure that generated this pathology record."}
    ),
    StructField(
        "laterality_of_surgery",
        StringType(),
        nullable=True,
        metadata={"comment": "Laterality: Unilateral, Bilateral, Not applicable, Unknown"}
    ),
    StructField(
        "side_of_surgery",
        StringType(),
        nullable=True,
        metadata={"comment": "Side: L Left, R Right, N Not applicable, U Unknown"}
    ),
    StructField(
        "surgery_date",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Date of breast procedure"}
    ),
    StructField(
        "age_at_surgery",
        IntegerType(),
        nullable=True,
        metadata={"comment": "Age at surgery (years)"}
    ),
    StructField(
        "days_diagnosis_surgery",
        IntegerType(),
        nullable=True,
        metadata={"comment": "Days between primary diagnosis and surgery"}
    ),
    StructField(
        "breast_procedure",
        StringType(),
        nullable=True,
        metadata={"comment": "Type: Mastectomy, WLE, Excision, Biopsy, etc.)"}
    ),
    StructField(
        "somatic_mutations",
        StringType(),
        nullable=True,
        metadata={"comment": "Presence of somatic genetic mutations"}
    ),
    StructField(
        "somatic_mutations_how_detected",
        StringType(),
        nullable=True,
        metadata={"comment": "Source of sample for somatic mutation testing"}
    ),
    StructField(
        "nodal_procedure",
        StringType(),
        nullable=True,
        metadata={"comment": "Axillary procedure type"}
    ),
    StructField(
        "path_atypical",
        StringType(),
        nullable=True,
        metadata={"comment": "Atypical changes as recorded in pathology report"}
    ),
    StructField(
        "path_benign",
        StringType(),
        nullable=True,
        metadata={"comment": "Benign changes as recorded in pathology report"}
    ),
    StructField(
        "total_nodes_removed",
        IntegerType(),
        nullable=True,
        metadata={"comment": "Total number of nodes removed during surgery"}
    ),
    StructField(
        "total_positive_nodes",
        IntegerType(),
        nullable=True,
        metadata={"comment": "Total number of positive nodes (including micrometastasis; ITC is negative)"}
    ),
    StructField(
        "surgery_nodes_status",
        StringType(),
        nullable=True,
        metadata={"comment": "General Node Status"}
    ),
    StructField(
        "biopsy_bcat",
        StringType(),
        nullable=True,
        metadata={"comment": "Biopsy category (B1-B5c) or FNA category (C1-C5)"}
    ),
    StructField(
        "multifocal",
        StringType(),
        nullable=True,
        metadata={"comment": "Whether tumour was multifocal or not. Unifocal = one focus of either invasive carcinoma or pure DCIS; Multifocal = two or more foci of either invasive carcinoma or pure DCIS"}
    ),
    StructField(
        "lvi",
        StringType(),
        nullable=True,
        metadata={"comment": "Presence of lymphovascular invasion"}
    ),
    StructField(
        "margin",
        StringType(),
        nullable=True,
        metadata={"comment": "Distance to closest surgical margin in mm (excluding anterior)"}
    ),
    StructField(
        "inflammatory_infiltrate",
        StringType(),
        nullable=True,
        metadata={"comment": "Presence of inflammatory infiltrate"}
    ),
    StructField(
        "inflammatory_type",
        StringType(),
        nullable=True,
        metadata={"comment": "Type of Inflammatory Infiltrate"}
    ),
    StructField(
        "p_ajcc_edition",
        StringType(),
        nullable=True,
        metadata={"comment": "TNM staging edition used"}
    ),
    StructField(
        "pathological_t_stage",
        StringType(),
        nullable=True,
        metadata={"comment": "Pathological T stage. See ref_pathological_t_stage."}
    ),
    StructField(
        "pathological_n_stage",
        StringType(),
        nullable=True,
        metadata={"comment": "Pathological N stage. See ref_pathological_n_stage."}
    ),
    StructField(
        "pathological_m_stage",
        StringType(),
        nullable=True,
        metadata={"comment": "Pathological M stage. See ref_pathological_m_stage."}
    ),
    StructField(
        "pathological_stage",
        StringType(),
        nullable=True,
        metadata={"comment": "Pathological overall stage. See ref_pathological_stage."}
    ),
    StructField(
        "path_response",
        StringType(),
        nullable=True,
        metadata={"comment": "Path response: PR, SD, DP, CR, O Other, U Unknown"}
    ),
    StructField(
        "rcb_group",
        StringType(),
        nullable=True,
        metadata={"comment": "RCB group reported after neo-adjuvant therapy"}
    ),
    StructField(
        "rcb_volume",
        StringType(),
        nullable=True,
        metadata={"comment": "RCB volume reported after neo-adjuvant therapy"}
    ),
    StructField(
        "nodes_showing_prev_involvement",
        IntegerType(),
        nullable=True,
        metadata={"comment": "Number of nodes showing response to treatment after neoadjuvant therapy"}
    ),
    StructField(
        "ADC_UPDT",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
    ),
    StructField(
        "date_checked",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Last update timestamp."}
    ),
    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )

])


def create_pharos_pathology_incr():

    max_adc_updt = get_max_timestamp(get_target_table("pharos_pathology"), "ADC_UPDT")

    diagnosis = spark.table("4_prod.bronze.map_diagnosis")
    person = spark.table("4_prod.bronze.map_person").select(col("person_id").alias("PERSON_ID"),"birth_year")
    procedure = spark.table("4_prod.bronze.map_procedure")


    # Get the breast cancer cohort
    breast_cancer_cohort = (
        diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (col("ICD10_CODE").like("C50%") | col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("brc_diag_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )

    breast_procedure = (
        procedure
        .filter(col("ADC_UPDT") > max_adc_updt)
        .join(breast_cancer_cohort, ["PERSON_ID"], "right")
        .join(person, ["PERSON_ID"],"left")
        .withColumn("breast_procedure",
                    # The available data do not provide sufficient detail to identify specific sub-categories of mastectomy or excision, and a free-text review is required.
                    when(col("OPCS4_CODE").like("%B27%") | col("OPCS4_CODE").like("%B28%"), "001 - Mastectomy")
                    .when(col("OPCS4_CODE").rlike("B311"), "006 - Reduction mammoplasty")
                    .when(col("OPCS4_CODE") == "B374", "012 - Capsulectomy")
                    .when(col("OPCS4_CODE") == "B321", "013 - Core Needle Biopsy")
                    .when(col("OPCS4_CODE") == "B324", "014 - Vacuum-assisted Biopsy")
                    .when(col("OPCS4_CODE").isin(
                        "B323",  # Image-guided Biopsy
                        "B322",  # Biopsy of lesion of breast NEC
                        "B328",  # Other specified biopsy of breast
                        "B329"   # Unspecified biopsy of breast
                        ), "015 - Biopsy Other")
        )
        .withColumn("days_diagnosis_surgery", datediff(col("PROC_DT_TM"), col("brc_diag_date")))
        .withColumn("age_at_surgery",year(col("PROC_DT_TM")) - col("birth_year"))
        # .filter(col("breast_procedure").isNotNull())
        .select(col("PERSON_ID").alias("person_id"), col("PROC_DT_TM").alias("surgery_date"),"age_at_surgery","days_diagnosis_surgery","breast_procedure", "_src_adc_updt")
        .dropDuplicates()
    )

    final_df = (
        breast_procedure
        .select(
            lit(None).cast(LongType()).alias("pathology_id"),
            col("person_id").cast(LongType()),
            lit(None).cast(StringType()).alias("pharosid"),
            lit(None).cast(LongType()).alias("clinical_record_id"),
            lit(None).cast(StringType()).alias("procedure_id"),
            lit(None).cast(StringType()).alias("laterality_of_surgery"),
            lit(None).cast(StringType()).alias("side_of_surgery"),
            col("surgery_date").cast(TimestampType()),
            col("age_at_surgery").cast(IntegerType()),
            col("days_diagnosis_surgery").cast(IntegerType()),
            col("breast_procedure").cast(StringType()),
            lit(None).cast(StringType()).alias("somatic_mutations"),
            lit(None).cast(StringType()).alias("somatic_mutations_how_detected"),
            lit(None).cast(StringType()).alias("nodal_procedure"),
            lit(None).cast(StringType()).alias("path_atypical"),
            lit(None).cast(StringType()).alias("path_benign"),
            lit(None).cast(IntegerType()).alias("total_nodes_removed"),
            lit(None).cast(IntegerType()).alias("total_positive_nodes"),
            lit(None).cast(StringType()).alias("surgery_nodes_status"),
            lit(None).cast(StringType()).alias("biopsy_bcat"),
            lit(None).cast(StringType()).alias("multifocal"),
            lit(None).cast(StringType()).alias("lvi"),
            lit(None).cast(StringType()).alias("margin"),
            lit(None).cast(StringType()).alias("inflammatory_infiltrate"),
            lit(None).cast(StringType()).alias("inflammatory_type"),
            lit(None).cast(StringType()).alias("p_ajcc_edition"),
            lit(None).cast(StringType()).alias("pathological_t_stage"),
            lit(None).cast(StringType()).alias("pathological_n_stage"),
            lit(None).cast(StringType()).alias("pathological_m_stage"),
            lit(None).cast(StringType()).alias("pathological_stage"),
            lit(None).cast(StringType()).alias("path_response"),
            lit(None).cast(StringType()).alias("rcb_group"),
            lit(None).cast(StringType()).alias("rcb_volume"),
            lit(None).cast(IntegerType()).alias("nodes_showing_prev_involvement"),
            col("_src_adc_updt").alias("ADC_UPDT"),
            current_timestamp().cast(TimestampType()).alias("date_checked"),
            current_timestamp().cast(TimestampType()).alias("created_at"),
            current_timestamp().cast(TimestampType()).alias("updated_at")
        )
    )

    return final_df

updated_df = create_pharos_pathology_incr()

update_table(updated_df, get_target_table("pharos_pathology"), ["person_id","surgery_date","breast_procedure"], schema_pharos_pathology, pharos_pathology_comment)


# COMMAND ----------

pharos_pathology_tf_comment = "This table contains detailed pathology and surgical information related to breast cancer samples collected from participants."

schema_pharos_pathology_tf = StructType([
    StructField(
        "pathology_focus_id",
        LongType(),
        nullable=True,
        metadata={"comment": "Surrogate PK"}
    ),
    StructField(
        "pathology_id",
        LongType(),
        nullable=True,
        metadata={"comment": "FK to pathology"}
    ),
    StructField(
        name="person_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Assigned unique ID for each participant."}
    ),
    StructField(
        name="pharosid",
        dataType=StringType(),
        nullable=True,
        metadata={"comment": "FK to person."}
    ),
    StructField(
        name="clinical_record_id",
        dataType=LongType(),
        nullable=True,
        metadata={"comment": "Assigned unique ID for each participant."}
    ),
    StructField(
        "invasive_present",
        StringType(),
        nullable=True,
        metadata={"comment": "Is invasive disease present?"}
    ),
    StructField(
        "morphology",
        StringType(),
        nullable=True,
        metadata={"comment": "Morphological subtype of invasive cancer"}
    ),
    StructField(
        "invasive_size_path",
        StringType(),
        nullable=True,
        metadata={"comment": "Size of invasive tumour in mm from pathology report"}
    ),
    StructField(
        "grade",
        StringType(),
        nullable=True,
        metadata={"comment": "Histological grade of invasive cancer"}
    ),
    StructField(
        "total_size_path",
        StringType(),
        nullable=True,
        metadata={"comment": "Total size of tumour in mm including in-situ disease"}
    ),
    StructField(
        "er_status",
        StringType(),
        nullable=True,
        metadata={"comment": "ER status"}
    ),
    StructField(
        "er_score_sample",
        StringType(),
        nullable=True,
        metadata={"comment": "ER score reported per sample"}
    ),
    StructField(
        "pr_status",
        StringType(),
        nullable=True,
        metadata={"comment": "PR status"}
    ),
    StructField(
        "pr_score_sample",
        StringType(),
        nullable=True,
        metadata={"comment": "PR score reported per sample"}
    ),
    StructField(
        "her2_status",
        StringType(),
        nullable=True,
        metadata={"comment": "HER2 status"}
    ),
    StructField(
        "her2_score_sample",
        StringType(),
        nullable=True,
        metadata={"comment": "HER2 score reported per sample"}
    ),
    StructField(
        "her2_fish",
        StringType(),
        nullable=True,
        metadata={"comment": "Whether or not FISH testing was undertaken"}
    ),
    StructField(
        "insitu",
        StringType(),
        nullable=True,
        metadata={"comment": "Presence of in-situ disease (includes 'Possible' for lobular neoplasia)"}
    ),
    StructField(
        "insitu_type",
        StringType(),
        nullable=True,
        metadata={"comment": "Morphological type of in-situ disease"}
    ),
    StructField(
        "dcis_subtype",
        StringType(),
        nullable=True,
        metadata={"comment": "Subtype of DCIS; multiple values separated by semi-colon (;)"}
    ),
    StructField(
        "lcis_subtype",
        StringType(),
        nullable=True,
        metadata={"comment": "Subtype of LCIS; multiple values separated by semi-colon (;)"}
    ),
    StructField(
        "insitu_grade",
        StringType(),
        nullable=True,
        metadata={"comment": "Grade of in-situ disease"}
    ),
    StructField(
        "microinvasion",
        StringType(),
        nullable=True,
        metadata={"comment": "Microinvasion < 1mm (corresponds to T1mi)"}
    ),
    StructField(
        "necrosis",
        StringType(),
        nullable=True,
        metadata={"comment": "Presence of necrosis"}
    ),
    StructField(
        name="date_checked",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Date the relevant medical record was last checked."}
    ),
    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )
])

def create_pharos_pathology_incr():

    max_adc_updt = get_max_timestamp(get_target_table("pharos_pathology"), "ADC_UPDT")

    diagnosis = spark.table("4_prod.bronze.map_diagnosis")
    person = spark.table("4_prod.bronze.map_person").select(col("person_id").alias("PERSON_ID"),"birth_year")
    procedure = spark.table("4_prod.bronze.map_procedure")


    # Get the breast cancer cohort
    breast_cancer_cohort = (
        diagnosis
        .filter(
            (col("ICD10_CODE").like("C50%") | col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("brc_diag_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )


    final_df = (
        breast_cancer_cohort

        .select(
            lit(None).cast(LongType()).alias("pathology_focus_id"),
            lit(None).cast(LongType()).alias("pathology_id"),
            col("person_id").cast(LongType()),
            lit(None).cast(StringType()).alias("pharosid"),
            lit(None).cast(LongType()).alias("clinical_record_id"),
            lit(None).cast(StringType()).alias("invasive_present"),
            lit(None).cast(StringType()).alias("morphology"),
            lit(None).cast(StringType()).alias("invasive_size_path"),
            lit(None).cast(StringType()).alias("grade"),
            lit(None).cast(StringType()).alias("total_size_path"),
            lit(None).cast(StringType()).alias("er_status"),
            lit(None).cast(StringType()).alias("er_score_sample"),
            lit(None).cast(StringType()).alias("pr_status"),
            lit(None).cast(StringType()).alias("pr_score_sample"),
            lit(None).cast(StringType()).alias("her2_status"),
            lit(None).cast(StringType()).alias("her2_score_sample"),
            lit(None).cast(StringType()).alias("her2_fish"),
            lit(None).cast(StringType()).alias("insitu"),
            lit(None).cast(StringType()).alias("insitu_type"),
            lit(None).cast(StringType()).alias("dcis_subtype"),
            lit(None).cast(StringType()).alias("lcis_subtype"),
            lit(None).cast(StringType()).alias("insitu_grade"),
            lit(None).cast(StringType()).alias("microinvasion"),
            lit(None).cast(StringType()).alias("necrosis"),
            col("_src_adc_updt").alias("ADC_UPDT"),
            current_timestamp().cast(TimestampType()).alias("date_checked"),
            current_timestamp().cast(TimestampType()).alias("created_at"),
            current_timestamp().cast(TimestampType()).alias("updated_at")
        )
    )

    return final_df

updated_df = create_pharos_pathology_incr()

update_table(updated_df, get_target_table("pharos_pathology_tumourfocus"), ["person_id","pathology_focus_id"], schema_pharos_pathology_tf, pharos_pathology_tf_comment)


# COMMAND ----------

pharos_treatment_comment = "This table captures comprehensive treatment for breast cancer patients. It includes details on systemic therapies (type, drugs, intent, cycles, start/end dates, and reasons for stopping), as well as radiotherapy administration (sites, dose, fractions, and boost)."

schema_pharos_treatment = StructType([
    StructField(
        "treatment_id", 
        LongType(), 
        nullable=True, 
        metadata={"comment": "Assigned unique ID for each participant (TBC)"}
    ),
    StructField(
        "person_id", 
        LongType(), 
        nullable=True, 
        metadata={"comment": "Assigned unique ID for each participant (TBC)"}
    ),
    StructField(
        "pharosid", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "FK to person"}
    ),
    StructField(
        "clinical_record_id", 
        LongType(), 
        nullable=True, 
        metadata={"comment": "Unique clincial record PharosID"}
    ),
    StructField(
        "tumour_id", 
        LongType(), 
        nullable=True, 
        metadata={"comment": "FK to tumour (if linkable)"}
    ),
    StructField(
        "treatment_type", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Type of treatment given"}
    ),
    StructField(
        "treatment_regimen", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Name of drug or treatment given"}
    ),
    StructField(
        "treatment_name", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Name of drug or treatment given"}
    ),
    StructField(
        "treatment_name_other", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Name of drug given if other selected in treatment_name"}
    ),
    StructField(
        "treatment_intent", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Treatment intent (e.g. Neoadjuvant, Adjuvant, Advanced)"}
    ),
    StructField(
        "treatment_start_date", 
        TimestampType(), 
        nullable=True, 
        metadata={"comment": "Start date of treatment"}
    ),
    StructField(
        "days_diagnosis_treatmentstart", 
        IntegerType(), 
        nullable=True, 
        metadata={"comment": "Difference in days between primary diagnosis and start of treatment"}
    ),
    StructField(
        "therapy_ongoing", 
        BooleanType(), 
        nullable=True, 
        metadata={"comment": "Is the therapy ongoing?"}
    ),
    StructField(
        "months_therapy_length", 
        IntegerType(), 
        nullable=True, 
        metadata={"comment": "Planned duration of therapy in days"}
    ),
    StructField(
        "treatment_end_date", 
        TimestampType(), 
        nullable=True, 
        metadata={"comment": "End date of treatment"}
    ),
    StructField(
        "days_diagnosis_treatmentend", 
        IntegerType(), 
        nullable=True, 
        metadata={"comment": "Difference in days between primary diagnosis and end of treatment"}
    ),
    StructField(
        "treatment_end_reason", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Reason why the treatment was stopped (e.g. Toxicity, Progression, Protocol end, Patient choice, Death, Other, Unknown)"}
    ),
    StructField(
        "treatment_side_effects", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Any side effects from administered treatment, if multiple then  separated by semicolon (;)"}
    ),
    StructField(
        "date_treatment_last_given", 
        TimestampType(), 
        nullable=True, 
        metadata={"comment": "Date of last record of treatment administration where end date is missing"}
    ),
    StructField(
        "days_treatmentlastgiven", 
        IntegerType(), 
        nullable=True, 
        metadata={"comment": "Difference in days between primary diagnosis and date of last given treatment"}
    ),
    StructField(
        "treatment_duration", 
        IntegerType(), 
        nullable=True, 
        metadata={"comment": "Time in days between start and end date of treatment"}
    ),
    StructField(
        "treatment_cycles", 
        IntegerType(), 
        nullable=True, 
        metadata={"comment": "For systemic therapy given in cycles; total number of cycles given"}
    ),
    StructField(
        "clinical_trial", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Whether patient was enrolled in an interventional trial"}
    ),
    StructField(
        "radiotherapy_site", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Anatomical site(s) where radiotherapy was given"}
    ),
    StructField(
        "radiotherapy_dose", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Dose of radiotherapy administered"}
    ),
    StructField(
        "radiotherapy_fraction", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Number of radiotherapy fractions delivered"}
    ),
    StructField(
        "radiotherapy_boost", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Indicates if a boost dose of radiotherapy was given"}
    ),
    StructField(
        "radiotherapy_cd", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Procedure codes related to radiology (Barts internal use)"}
    ),
    StructField(
        "radiotherapy_desc", 
        StringType(), 
        nullable=True, 
        metadata={"comment": "Description of the procedure codes related to radiology (Barts internal use)"}
    ),
    StructField(
        "date_checked", 
        TimestampType(), 
        nullable=True, 
        metadata={"comment": "Last update timestamp."}
    ),    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )

])


def create_pharos_treatment_incr():

    max_adc_updt = get_max_timestamp("4_prod.bronze.pharos_treatment", "date_checked_treatment")

        # Load Tables
    diagnosis = spark.table("4_prod.bronze.map_diagnosis")
    procedure = spark.table("4_prod.bronze.map_procedure")
    drug = spark.table("4_prod.bronze.map_med_admin")
    chemotherapy = spark.table("4_prod.rde.rde_iqemo")

    # Get the breast cancer cohort
    breast_cancer_cohort = (
        diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (col("ICD10_CODE").like("C50%") | col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("brc_diag_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )

    # Process Chemotherapy data

    treatment_chemo = (
        chemotherapy
        .join(breast_cancer_cohort, "PERSON_ID", "inner")
        .withColumn("treatment_type", lit("chemotherapy"))
        .withColumn("treatment_cycles", 
                    when(
                        (col("PlannedCycles") > col("CycleCancelledFrom")) & (col("CourseFinished") == "true"), 
                        F.coalesce(F.col("PlannedCycles"), F.lit(0)) - F.coalesce(F.col("CycleCancelledFrom"), F.lit(0)))
                    .otherwise(col("PlannedCycles"))
                    )
        .select(
            col("PERSON_ID"),
            col("treatment_type"),
            col("SactName").alias("treatment_regimen"),
            col("Name").alias("treatment_name"),
            col("StartDate").alias("treatment_start_date"),
            col("CourseFinished").alias("therapy_ongoing"),
            col("EndDate").alias("treatment_end_date"),
            col("FinalTreatmentDate").alias("date_treatment_last_given"),
            col("treatment_cycles"),
            col("brc_diag_date"),
            col("_src_adc_updt")
        )
        .dropDuplicates()
    )

    # Other Systemic Therapies (Admin-based data)

    # Drug Lists for classification (Endocrine, Antibody, Inhibitors, and Supportive Care)
    endocrine_drugs = [
        72965,   # Letrozole
        10324,   # Tamoxifen
        258494,  # Exemestane
        50610,   # Goserelin
        72143,   # Raloxifene
        282357   # Fulvestrant
    ]

    antibody_drugs = [
        224905,   # Trastuzumab
        1298944,  # Pertuzumab
        253337,   # Bevacizumab
        1597876,  # Nivolumab
        993449    # Denosumab (Bone Health)
    ]
    chemotherapy_drugs = [
        194000,   # Capecitabine
        40048,    # Carboplatin
        3002,     # Cyclophosphamide
        1045453,  # Eribulin
        1160832,  # Fluorouracil (Topical)
        12574,    # Gemcitabine
        6851,     # Methotrexate
        632,      # Mitomycin C
    ]

    cdk4_6_inhibitors = [
        1601374,  # Palbociclib
        1873916,  # Ribociclib
        1740938   # Abemaciclib
    ]

    parp_inhibitors = [
        1597582,  # Olaparib
        1918231   # Niraparib
    ]

    # These are included because they showed up in treatment_name values in the cdm.
    #other_drugs = [ 
    #    141704,   # Everolimus (mTOR Inhibitor) 
    #    3264,     # Dexamethasone (Steroid) 
    #    68442,    # G-CSF (Growth Factor) 
    #    358255,   # Aprepitant (Antiemetic) 
    #    26225,    # Ondansetron (Antiemetic)  
    #    77655,    # Zoledronic acid (Bone Health) 
    #   73056,    # Risedronate (Bone Health) 
    #    11473,    # Pamidronate (Bone Health) 
    #    32915,    # Teriparatide (Bone Health) 
    #    1894,     # Calcitriol (Bone Health/Vitamin) 
    #   39786,    # Venlafaxine (Supportive Care) 
    #   25480,    # Gabapentin (Supportive Care) 
    #  6313,     # Folinic acid (Supportive Care) 
    #   8638,     # Prednisolone (Steroid) 
    #  5492,     # Hydrocortisone (Steroid) 
    #  6902      # Methylprednisolone (Steroid) 
    #]

    treatment_other = (
        drug
        .filter(
            (col("EVENT_TYPE_DISPLAY") == "Administered")
        )
        .join(breast_cancer_cohort, "PERSON_ID", "inner")
        .withColumn(
            "treatment_type",
            when(col("RXNORM_CUI").isin(endocrine_drugs), "001 - Endocrine")
            .when(col("RXNORM_CUI").isin(antibody_drugs), "003 - Antibody")
            .when(col("RXNORM_CUI").isin(cdk4_6_inhibitors), "004 - CDK4/6 Inhibitor")
            .when(col("RXNORM_CUI").isin(parp_inhibitors), "006 - PARP Inhibitor")
            .when(col("RXNORM_CUI").isin(chemotherapy_drugs), "002 - Chemotherapy")
            #.when(col("RXNORM_CUI").isin(other_drugs), "Other")
            .otherwise(lit(None))
        )
        .filter(col("treatment_type").isNotNull())
        .select(
            "PERSON_ID", 
            "treatment_type", 
            col("RXNORM_STR").alias("treatment_name"), 
            # WARNING: ADMIN_START/END dates represent single-day drug administrations, not the full therapy course. 
            # A freetext audit is required to validate clinical 'Treatment Start/End' dates.
            col("ADMIN_START_DT_TM").alias("treatment_start_date"), 
            col("ADMIN_END_DT_TM").alias("treatment_end_date"),
            col("ADMIN_END_DT_TM").alias("date_treatment_last_given"),
            lit(None).alias("therapy_ongoing"),
            lit(None).alias("treatment_cycles"), # All records in this dataset have EndDates in the past. Freetext audit is required to determine if the treatment cycles.
            col("brc_diag_date"),
            col("_src_adc_updt")
        )
    )
    
    # Radiotherapy
    snomed_radiotherapy_codes = [

        485444010,            # External beam radiotherapy
        485446012,            # EB - External beam radiotherapy

        2693115017,           # Radiotherapy
        5260791015,           # Radiotherapy
        2579750017,           # Radiotherapy
        1219393012,           # RT - Radiotherapy
        1784774011,           # XRT - Radiotherapy
        496531014,            # DXT - Radiotherapy

        372591000000111,      # Radiotherapy delivery (procedure)
        372601000000117,      # Radiotherapy delivery
        5034790019,           # Radiotherapy course of treatment
        261699013,            # Radiotherapy completed
        261687018,            # Post-operative course of radiotherapy
        550255014,            # Post-operative course of radiotherapy (procedure)
        261691011,            # Palliative course of radiotherapy
        550258011,            # Palliative course of radiotherapy (procedure)

        5172441014,           # Chemoradiotherapy
        3008687012,           # Combined chemotherapy and radiation therapy
        1488636014,           # Combined pre-operative chemotherapy and radiotherapy
        1468875011,           # Combined post-operative chemotherapy and radiotherapy
        262799017,            # Combined radiotherapy
        4551480018,           # Radiotherapy after chemotherapy

        1469036012,           # Stereotactic radiotherapy (procedure)
        1488795016,           # Stereotactic radiotherapy
        1894781000000115,     # SABR of prostate

        1786801012,           # Internal radiotherapy
        1786803010,           # Radiotherapy - internal
        3654594010,           # Selective internal radiotherapy (SIRT)

        236999019,            # Brachytherapy
        1786800013,           # Brachytherapy
        1490935011,           # Brachytherapy
        1490936012,           # Brachytherapy procedure
        2310961000000118,     # High dose rate brachytherapy
        4575691014,           # Low dose rate brachytherapy
        1479016010,           # Intracavitary brachytherapy
        2901374011,           # Intracavitary brachytherapy of female genital tract
        342822018,            # Gammamed brachytherapy
        342832013,            # Tantalum-182 brachytherapy
        342830017,            # Ruthenium-106 brachytherapy
        616308010,            # Iodine-125 brachytherapy
        2897350016,           # Brachytherapy using radioiodine
        2900753017,           # Brachytherapy using radioiodine
        2343691000000115,     # Ultrasound-guided prostate brachytherapy
        2343711000000118,     # Ultrasound-guided prostate brachytherapy
        2769780012,           # Fluoroscopy-guided prostate brachytherapy
        701171000000117,      # Fluoroscopy-guided prostate brachytherapy
        2981444017,           # Insertion of brachytherapy device

        2692176019,           # Radiotherapy to breast
        2694527018,           # Radiotherapy to head
        2694899018,           # Radiotherapy to neck
        2694686018,           # Radiotherapy to pelvis
        3077942015,           # Radiotherapy to thorax
        266761014,            # Radiotherapy to lacrimal gland
        261693014,            # Radiotherapy for tumour palliation
        2695397018,           # Radiotherapy by body site
        2689480017,           # Plaque radiotherapy of retina
        2695785016,           # Plaque radiotherapy of retina
        2623096012,           # Radiotherapy using radioactive plaque on eye
        262834017             # Radiotherapy seeds implanted into brain
    ]

    radiotherapy = (
        procedure
        .filter(
            col("SOURCE_IDENTIFIER").startswith("X65")  # Radiotherapy delivery
            | col("SOURCE_IDENTIFIER").startswith("Y89")  # Brachytherapy treatment
            | col("SOURCE_IDENTIFIER").startswith("Y91")  # External beam radiotherapy
            # additional OPCS4 codes
            | col("SOURCE_IDENTIFIER").isin(
                "Y90.2",  # Radiotherapy NEC
                "J12.3",  # Selective Internal Radiation Therapy (SIRT)
                "C82.3",  # External beam radiotherapy to retina
                "C82.4",  # Plaque radiotherapy to retina
                "A61.3",  # Radiotherapy to peripheral nerve lesion
                "C24.2",  # Radiotherapy to lacrimal gland
                "C39.5"   # Radiotherapy to conjunctival lesion
            )

            # additional SNOMED CT radiotherapy codes
            | col("SOURCE_IDENTIFIER").isin(
                *[str(x) for x in snomed_radiotherapy_codes]
            )
        )
        .select("PERSON_ID", col("SOURCE_IDENTIFIER").alias("radiotherapy_cd"),col("SOURCE_STRING").alias("radiotherapy_desc"), col("PROC_DT_TM"))
    )
    treatment_radio = (
        radiotherapy
        .join(breast_cancer_cohort, "PERSON_ID", "inner")
        .withColumn("treatment_type", lit("006 - Radiotherapy"))
        .select(
            "PERSON_ID", 
            "treatment_type", 
            lit(None).alias("treatment_name"), 
            "radiotherapy_cd",
            "radiotherapy_desc",
            # WARNING: ADMIN_START/END dates represent single-day drug administrations, not the full therapy course. 
            # A freetext audit is required to validate clinical 'Treatment Start/End' dates.
            col("PROC_DT_TM").alias("treatment_start_date"), 
            col("PROC_DT_TM").alias("treatment_end_date"),
            col("PROC_DT_TM").alias("date_treatment_last_given"),
            lit(None).alias("therapy_ongoing"),
            lit(None).alias("treatment_cycles"), # All records in this dataset have EndDates in the past. Freetext audit is required to determine if the treatment cycles.
            col("brc_diag_date"),
            col("_src_adc_updt")
        )
    )



    # Combine the treatment and calculate date differences
    treatment = (
        treatment_chemo
        .unionByName(treatment_other, allowMissingColumns=True)
        .unionByName(treatment_radio, allowMissingColumns=True)
        .filter(col("treatment_start_date") >= col("brc_diag_date"))
        .withColumn("days_diagnosis_treatmentstart", datediff(col("treatment_start_date"), col("brc_diag_date")))
        .withColumn("days_diagnosis_treatmentend", datediff(col("treatment_end_date"), col("brc_diag_date")))
        .withColumn("days_treatmentlastgiven", datediff(col("date_treatment_last_given"), col("brc_diag_date")))
        .withColumn("treatment_duration", datediff(col("treatment_end_date"), col("treatment_start_date")))
        .dropDuplicates()
        )

    final_df = (
        treatment
        .select(
            lit(None).alias("treatment_id").cast(StringType()),
            col("PERSON_ID").cast(LongType()).alias("person_id"),
            lit(None).alias("pharosid").cast(StringType()),
            lit(None).alias("clinical_record_id").cast(LongType()),
            lit(None).alias("tumour_id").cast(LongType()),
            col("treatment_type").cast(StringType()),
            col("treatment_regimen").cast(StringType()),
            col("treatment_name").cast(StringType()),
            lit(None).alias("treatment_name_other").cast(StringType()),
            lit(None).alias("treatment_intent").cast(StringType()),
            col("treatment_start_date").cast(TimestampType()),
            col("days_diagnosis_treatmentstart").cast(IntegerType()),
            col("therapy_ongoing").cast(StringType()),
            lit(None).alias("months_therapy_length").cast(IntegerType()),
            col("treatment_end_date").cast(TimestampType()),
            col("days_diagnosis_treatmentend").cast(IntegerType()),
            lit(None).alias("treatment_end_reason").cast(StringType()),
            lit(None).alias("treatment_side_effects").cast(StringType()),
            col("date_treatment_last_given").cast(TimestampType()),
            col("days_treatmentlastgiven").cast(IntegerType()),
            col("treatment_duration").cast(IntegerType()),
            col("treatment_cycles").cast(IntegerType()),
            lit(None).alias("clinical_trial").cast(StringType()),
            lit(None).alias("radiotherapy_site").cast(StringType()),
            lit(None).alias("radiotherapy_dose").cast(StringType()),
            lit(None).alias("radiotherapy_fraction").cast(StringType()),
            lit(None).alias("radiotherapy_boost").cast(StringType()),
            col("radiotherapy_cd").cast(StringType()),
            col("radiotherapy_desc").cast(StringType()),
            col("_src_adc_updt").cast(TimestampType()).alias("ADC_UPDT"),
            current_timestamp().cast(TimestampType()).alias("date_checked"),
            current_timestamp().cast(TimestampType()).alias("created_at"),
            current_timestamp().cast(TimestampType()).alias("updated_at")
        )
    )

    return final_df 

updates_df = create_pharos_treatment_incr()

update_table(updates_df, "8_dev.silver.pharos_treatment",["person_id", "treatment_type", "treatment_name", "treatment_start_date", "treatment_end_date"], schema_pharos_treatment, pharos_treatment_comment)


# COMMAND ----------

pharos_followup_comment = "This table tracks longitudinal outcomes for breast cancer patients, including detailed recurrence data (local and distant metastasis), survival metrics, and vital status. It captures the chronology of disease progression through event-specific dates and site locations, alongside calculated time-to-event intervals (years from diagnosis)."

schema_pharos_followup= StructType([
    StructField(
        "followup_id",
        LongType(),
        metadata={"comment": "Surrogate PK"}
    ),
    StructField(
        "person_id",
        LongType(),
        metadata={"comment": "Assigned unique ID for each participant (TBC)"}
    ),
    StructField(
        "clinical_record_id", 
        LongType(), 
        nullable=True, 
        metadata={"comment": "Unique clincial record PharosID"}
    ),
    StructField(
        "local_recurrence",
        StringType(),
        metadata={"comment": "Whether or not patient had local recurrence"}
    ),
    StructField(
        "distant_metastasis",
        StringType(),
        metadata={"comment": "Whether or not patient had a distant recurrence"}
    ),
    StructField(
        "denovo_metastasis",
        StringType(),
        metadata={"comment": "Indication of distant metastasis at time of cancer diagnosis or within 6 months"}
    ),
    StructField(
        "vital_status",
        StringType(),
        metadata={"comment": "Whether patient is alive at date of last follow-up"}
    ),
    StructField(
        "date_of_death",
        TimestampType(),
        metadata={"comment": "Date of death recorded in the medical record (DD-MM-YYYY)"}
    ),
    StructField(
        "years_diagnosistodeath",
        IntegerType(),
        metadata={"comment": "Time (in years) between first diagnosis date and date of death"}
    ),
    StructField(
        "cause_of_death",
        StringType(),
        metadata={"comment": "Cause of death as recorded on death certificate or medical records"}
    ),
    StructField(
        "date_of_last_followup",
        TimestampType(),
        metadata={"comment": "Last date patient was recorded to have contact with hospital staff (DD-MM-YYYY)"}
    ),
    StructField(
        "followup_status",
        StringType(),
        metadata={"comment": "Follow up status at date last seen"}
    ),
    StructField(
        "years_lastfollowup",
        IntegerType(),
        metadata={"comment": "Time (in years) between first diagnosis and last follow-up (alive) or death (deceased)"}
    ),
    StructField(
        "lost_to_followup",
        StringType(),
        metadata={"comment": "Indication of whether patient has been lost to follow-up"}
    ),
    StructField(
        "ADC_UPDT",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
    ),
    StructField(
        "date_checked",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Last update timestamp."}
    ),
    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )
])

def create_pharos_followup_incr():
    # Incremental update based on silver table timestamp
    max_adc_updt = get_max_timestamp(get_target_table("pharos_followup"), "ADC_UPDT")
    death = spark.table("4_prod.bronze.map_death")
    diagnosis = spark.table("4_prod.bronze.map_diagnosis")

    # Identify latest contact date from hospital encounter records
    encounter = (
        spark.table("4_prod.bronze.map_encounter")
        .withColumn("date_of_last_followup", F.to_timestamp(F.coalesce("DEPART_DT_TM", "ARRIVE_DT_TM")))
        .groupBy("PERSON_ID")
        .agg(max("date_of_last_followup").alias("date_of_last_followup"))
    )

        # Filter cohort by ICD-10 and incremental update logic
    breast_cancer_cohort = (
        diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (col("ICD10_CODE").like("C50%") | col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("earliest_diagnosis_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )


    # Deduplicate death records: keep most recent per person
    death_deduped = (
        death
        .filter(col("DECEASED_DT_TM").isNotNull())
        .withColumn("_rn", row_number().over(
            Window.partitionBy("PERSON_ID").orderBy(col("DECEASED_DT_TM").desc())
        ))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # Process death records and calculate survival time (years)
    death_processed = (
        death_deduped
        .join(breast_cancer_cohort, "PERSON_ID", "right")
        .withColumn(
            "vital_status",
            when(col("DECEASED_DT_TM").isNotNull(), lit("001 - Deceased"))
            .otherwise(lit("000 - Alive"))
        )
        .withColumn(
            "years_diagnosistodeath",
            when(col("DECEASED_DT_TM").isNotNull(),
                datediff(col("DECEASED_DT_TM"), col("earliest_diagnosis_date")).cast(DoubleType()) / 365)
            .otherwise(lit(None))
        )
        .select(
            "PERSON_ID",
            "vital_status",
            col("DECEASED_DT_TM").alias("date_of_death"),
            "years_diagnosistodeath",
            "earliest_diagnosis_date",
            "_src_adc_updt"
        )
    )


    # Merge mortality data with encounter history
    final_df = (
        death_processed
        .join(encounter, "PERSON_ID", "left")
        # Follow-up duration calculation (Diagnosis to last contact)
        .withColumn(
            "years_lastfollowup",
            datediff(col("date_of_last_followup"), col("earliest_diagnosis_date")).cast(DoubleType()) / 365
        )
        .select(
            # Recurrence/Metastasis placeholders reserved for future free-text audit results
            lit(None).cast(StringType()).alias("followup_id"),
            col("PERSON_ID").cast(LongType()).alias("person_id"),
            lit(None).cast(StringType()).alias("pharosid"),
            lit(None).cast(LongType()).alias("clinical_record_id"),
            lit(None).cast(StringType()).alias("local_recurrence"),
            lit(None).cast(StringType()).alias("distant_metastasis"),
            lit(None).cast(StringType()).alias("denovo_metastasis"),
            col("vital_status").cast(StringType()),
            col("date_of_death").cast(TimestampType()),
            col("years_diagnosistodeath").cast(IntegerType()),
            lit(None).cast(StringType()).alias("cause_of_death"),
            col("date_of_last_followup").cast(TimestampType()),
            lit(None).cast(StringType()).alias("followup_status"),
            col("years_lastfollowup").cast(IntegerType()),
            lit(None).cast(StringType()).alias("lost_to_followup"),
            col("_src_adc_updt").alias("ADC_UPDT"),
            lit(current_timestamp()).cast(TimestampType()).alias("date_checked"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
    )

    return final_df

updates_df = create_pharos_followup_incr()
update_table(updates_df, get_target_table("pharos_followup"), "person_id", schema_pharos_followup, pharos_followup_comment)

# COMMAND ----------

pharos_recurrence_comment = "Stores cancer recurrence information for each person, including recurrence details and record audit timestamps."

schema_pharos_recurrence = StructType([
    StructField(
        "recurrence_id",
        IntegerType(),
        True,
        {"comment": "Surrogate PK"}
    ),
    StructField(
        "person_id",
        LongType(),
        True,
        {"comment": "Assigned unique ID for each participant"}
    ),
    StructField(
        "pharosid",
        StringType(),
        True,
        {"comment": "FK to person"}
    ),
    StructField(
        "recurrence_date",
        TimestampType(),
        metadata={"comment": "Date of local recurrence"}
    ),
    StructField(
        "days_diagnosis_recurrence",
        IntegerType(),
        metadata={"comment": "Date of local recurrence"}
    ),
    StructField(
        "recurrence_sites",
        StringType(),
        metadata={"comment": "Site: Ipsilateral breast, Ipsilateral axilla, Chest wall, etc."}
    ),
    StructField(
        "ADC_UPDT",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Last update timestamp."}
    ),
    StructField(
        "date_checked",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Last update timestamp."}
    ),
    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )
])

def create_pharos_recurrence_incr():
    max_adc_updt = get_max_timestamp(
        table_name=get_target_table("pharos_sample"),
        ts_column="ADC_UPDT")

    map_diagnosis = spark.table("4_prod.bronze.map_diagnosis")    
    breast_cancer_cohort = (
        map_diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (col("ICD10_CODE").like("C50%") | col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("brc_diag_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )
    
    final_df = (
        breast_cancer_cohort
        .select(
            lit(None).cast(IntegerType()).alias("recurrence_id"),
            col("PERSON_ID").cast(LongType()).alias("person_id"),
            lit(None).cast(StringType()).alias("pharosid"),
            lit(None).cast(TimestampType()).alias("recurrence_date"),
            lit(None).cast(IntegerType()).alias("days_diagnosis_recurrence"),
            lit(None).cast(StringType()).alias("recurrence_site"),
            col("_src_adc_updt").alias("ADC_UPDT"),
            current_timestamp().alias("date_checked"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
    )

    return final_df


updates_df = create_pharos_recurrence_incr()

update_table(updates_df, get_target_table("pharos_recurrence"), "person_id", schema_pharos_recurrence,pharos_recurrence_comment)



# COMMAND ----------

pharos_metastasis_comment = (
    "Stores cancer metastasis information for each person, including metastasis details and record audit timestamps."
)

schema_pharos_metastasis = StructType([
    StructField(
        "metastasis_id",
        IntegerType(),
        True,
        {"comment": "Surrogate PK"}
    ),
        StructField(
        "person_id",
        LongType(),
        True,
        {"comment": "Assigned unique ID for each participant"}
    ),
    StructField(
        "pharosid",
        StringType(),
        True,
        {"comment": "FK to followup/person"}
    ),
    StructField(
        "metastasis_date",
        TimestampType(),
        True,
        {"comment": "Date of distant recurrence episode"}
    ),
    StructField(
        "days_diagnosis_metastasis",
        IntegerType(),
        True,
        {"comment": "Days between primary diagnosis and this metastasis"}
    ),
    StructField(
        "metastasis_site",
        StringType(),
        True,
        {"comment": "Anatomical site of metastasis"}
    ),
    StructField(
        "ADC_UPDT",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Last update timestamp."}
    ),
    StructField(
        "date_checked",
        TimestampType(),
        True,
        {"comment": "Date the relevant medical record was last checked"}
    ),
    StructField(
        "created_at",
        TimestampType(),
        True,
        {"comment": "Row creation timestamp"}
    ),
    StructField(
        "updated_at",
        TimestampType(),
        True,
        {"comment": "Row last-updated timestamp"}
    )
])


def create_pharos_metastasis_incr():
    max_adc_updt = get_max_timestamp(
        table_name=get_target_table("pharos_metastasis"),
        ts_column="ADC_UPDT"
    )

    map_diagnosis = spark.table("4_prod.bronze.map_diagnosis")

    breast_cancer_cohort = (
        map_diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (
                col("ICD10_CODE").like("C50%") |
                col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331)
            )
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("brc_diag_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )

    final_df = (
        breast_cancer_cohort
        .select(
            lit(None).cast(IntegerType()).alias("metastasis_id"),
            col("PERSON_ID").cast(LongType()).alias("person_id"),
            lit(None).cast(StringType()).alias("pharosid"),
            lit(None).cast(TimestampType()).alias("metastasis_date"),
            lit(None).cast(IntegerType()).alias("days_diagnosis_metastasis"),
            lit(None).cast(StringType()).alias("metastasis_site"),
            col("_src_adc_updt").alias("ADC_UPDT"),
            current_timestamp().alias("date_checked"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
    )

    return final_df


updates_df = create_pharos_metastasis_incr()

update_table(
    updates_df,get_target_table("pharos_metastasis"),"person_id",schema_pharos_metastasis,pharos_metastasis_comment
)

# COMMAND ----------

pharos_comorbidities_comment = "This table records the co-existing medical conditions (comorbidities) of participants in the Pharos cohort, with a specialized focus on diabetes management. It captures diagnostic data using ICD-10 standards, tracks the temporal relationship (pre- vs. post-cancer diagnosis) of each condition, and documents longitudinal diabetes care including associated medications. The data is structured to support analysis of how underlying health status impacts cancer treatment outcomes and patient survival."

schema_pharos_comorbidities = StructType([
    StructField(
        "comorbidity_id",
        StringType(),
        True,
        {"comment": "Surrogate PK"}
    ),
    StructField(
        "person_id", 
        LongType(), 
        True, 
        {"comment": "Assigned unique ID for each participant (TBC)"}
    ),
    StructField(
        "pharosid",
        StringType(),
        True,
        {"comment": "FK to person"}
    ),
    StructField(
        "clinical_record_id", 
        LongType(), 
        nullable=True, 
        metadata={"comment": "Unique clincial record PharosID"}
    ),
    StructField(
        "comorbidities", 
        StringType(), 
        True, 
        {"comment": "Whether comorbidities are present"}
    ),
    StructField(
        "icd_code", 
        StringType(), 
        True, 
        {"comment": "ICD code for each condition (from ICD-10)"}
    ),
    StructField(
        "comorbidity_name", 
        StringType(), 
        True, 
        {"comment": "ICD description name for condition (from ICD-10)"}
    ),
    StructField(
        "comorbidity_temporality", 
        StringType(), 
        True, 
        {"comment": "Whether comorbidity was diagnosed before or after cancer diagnosis"}
    ),
    StructField(
        "diabetes", 
        StringType(), 
        True, 
        {"comment": "Whether patient is diabetic according to medical record"}
    ),
    StructField(
        "diabetes_temporality", 
        StringType(), 
        True, 
        {"comment": "Whether diabetes was diagnosed before or after cancer diagnosis"}
    ),
    StructField(
        "date_assessed", 
        TimestampType(), 
        True, 
        {"comment": "Date that this condition was entered"}
    ),
    StructField(
        "days_diagnosis_comor", 
        IntegerType(), 
        True, 
        {"comment": "Time in days between date diagnosis and date of comorbidity condition assessment"}
    ),
    StructField(
        "ADC_UPDT",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
    ),
    StructField(
        "date_checked", 
        TimestampType(), 
        nullable=True, 
        metadata={"comment": "Date the relevant medical record was last checked."}
    ),
    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )

])

def create_pharos_comorbidity_incr():

    max_adc_updt = get_max_timestamp("8_dev.silver.pharos_comorbidities", "date_checked_comorbidities")

    diagnosis = spark.table("4_prod.bronze.map_diagnosis")
    treatment = (
        spark.table("8_dev.silver.pharos_treatment")
        .select(col("person_id").alias("PERSON_ID"),"treatment_start_date","treatment_end_date")
        .groupBy("person_id")
        .agg(
            min("treatment_start_date").alias("treatment_start_date"),
            max("treatment_end_date").alias("treatment_end_date")
        )
    )

    breast_cancer_cohort = (
        diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (col("ICD10_CODE").like("C50%") | col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("earliest_diagnosis_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )

    # The following medical condition with ICD10 codes are excluded,  
    code_excluded = [
        "C5*",  # Breast cancer
        "C77", "C78", "C79",  # Metastasis
        "S", "T",  # Injury, poisoning and certain other consequences of external causes
        "V", "W", "X", "Y",  # External causes of morbidity and mortality
        "Z", # Factors influencing health status and contact with health services
        "U" # XXII Codes for special purposes
    ]

    # Convert the wildcard list into a single regex for rlike  
    excluded_regex = r"^(?:{})".format("|".join(code_excluded))  
    window = Window.partitionBy("PERSON_ID","ICD10_CODE").orderBy(col("earliest_diagnosis_date").asc())
    comorbidity = (
        diagnosis
        .filter((~col("ICD10_CODE").rlike(excluded_regex)))
        .join(breast_cancer_cohort, "PERSON_ID", "inner")
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
        .join(treatment, ["PERSON_ID"], "left") 
        
        # COMORBIDITIES --------------- 
        .withColumn(
            "comorbidities",
            when(col("ICD10_CODE").isNotNull(), lit("1 Yes"))
            .otherwise(lit("9 Unknown"))
        )
        .withColumn(
            "comorbidity_temporality",
            when(col("ICD10_CODE").isNull(), lit("9 Unknown temporality"))
            .when(col("earliest_diagnosis_date") < col("brc_diag_date"), lit("0 Pre-diagnosis"))
            .when(
                (col("earliest_diagnosis_date") >= col("brc_diag_date")) & 
                (col("earliest_diagnosis_date") <= col("treatment_start_date")), lit("1 Post-diagnosis"))
            .when(
                (col("earliest_diagnosis_date") >= col("treatment_start_date")) & 
                (col("earliest_diagnosis_date") <= col("treatment_end_date")), lit("2 During treatment"))
            .when(col("earliest_diagnosis_date") > col("treatment_end_date"), lit("1 Post-diagnosis"))
        )

        # DIABETES ----------------
        .withColumn(
            "diabetes",
            when(col("ICD10_CODE").like("E10%"), "1 Type I diabetes")
            .when(
                (col("ICD10_CODE").like("E11%")) |
                (col("OMOP_CONCEPT_ID") == 45757508),
                "2 Type II diabetes"
            )
            .when(
                (col("ICD10_CODE").like("R73%")) |
                (col("OMOP_CONCEPT_ID").isin(44808385, 37018196)),
                "3 Pre-diabetic/borderline"
            )
            # Probably this other type of diabetes can be included in the comorbidities (check)
            # .when(
            #     (col("ICD10_CODE").like("E14%")) | # Unspecified diabetes mellitus
            #     (col("ICD10_CODE").like("E13%")) | # Other specified diabetes mellitus
            #     (col("ICD10_CODE").like("E12%")) | # Malnutrition-related diabetes mellitus,
            #     "4 Other diabetes types"
            # )
            .otherwise(lit("9 Unknown"))
        )
        .withColumn(
            "diabetes_temporality",
            when(
                (col("diabetes")!= "9 Unknown") & (col("earliest_diagnosis_date") < col("brc_diag_date")), "0 Pre-diagnosis")
            .when(
                (col("diabetes")!= "9 Unknown") &
                (col("earliest_diagnosis_date") >= col("brc_diag_date")) & 
                (col("earliest_diagnosis_date") <= col("treatment_start_date")), lit("1 Post-diagnosis"))
            .when(
                (col("diabetes")!= "9 Unknown") &
                (col("earliest_diagnosis_date") >= col("treatment_start_date")) & 
                (col("earliest_diagnosis_date") <= col("treatment_end_date")), lit("2 During treatment"))
            .when(
                (col("diabetes")!= "9 Unknown") &
                (col("earliest_diagnosis_date") > col("treatment_end_date")), lit("1 Post-diagnosis"))
            .otherwise(lit("9 Unknown"))
        )
        .withColumn("days_diagnosis_comor", datediff(col("earliest_diagnosis_date"), col("brc_diag_date")))
    )


    final_df = (
        comorbidity
        .select(
            lit(None).cast(StringType()).alias("comorbidity_id"),
            col("PERSON_ID").alias("person_id").cast(LongType()),
            lit(None).cast(StringType()).alias("pharosid"),
            lit(None).cast(LongType()).alias("clinical_record_id"),
            col("comorbidities").cast(StringType()),
            col("ICD10_CODE").alias("icd_code").cast(StringType()),
            col("ICD10_TERM").alias("comorbidity_name").cast(StringType()),
            col("comorbidity_temporality").cast(StringType()),
            col("diabetes").cast(StringType()),
            col("diabetes_temporality").cast(StringType()),
            col("earliest_diagnosis_date").alias("date_assessed").cast(TimestampType()),
            col("days_diagnosis_comor").cast(IntegerType()),
            col("_src_adc_updt").alias("ADC_UPDT"),
            current_timestamp().alias("date_checked").cast(TimestampType()),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
        .dropDuplicates()  
    )
    return final_df

updated_df = create_pharos_comorbidity_incr()
update_table(updated_df, get_target_table("pharos_comorbidities"),["person_id", "icd_code"], schema_pharos_comorbidities, pharos_comorbidities_comment)


# COMMAND ----------

med =spark.table("8_dev.bronze.map_med_admin")


med1 = med.filter(
    lower(col("RXNORM_STR")).rlike(
        "(?i)(ambrisentan|bosentan|clopidogrel|dornase alfa|enoxaparin|enoximone|eplerenone|epoprostenol|flecainide acetate|furosemide|glibenclamide|heparin|iloprost|lisinopril|macitentan|mannitol|milrinone|nintedanib|pirfenidone|pravastatin|riociguat|salbutamol|selexipag|sildenafil|tadalafil|treprostinil)"
    )
)
med1.select("RXNORM_STR","RXNORM_CUI").distinct().display()


# COMMAND ----------

pharos_meds_comment = "This table records the co-existing medical conditions (comorbidities) of participants in the Pharos cohort, with a specialized focus on diabetes management. It captures diagnostic data using ICD-10 standards, tracks the temporal relationship (pre- vs. post-cancer diagnosis) of each condition, and documents longitudinal diabetes care including associated medications. The data is structured to support analysis of how underlying health status impacts cancer treatment outcomes and patient survival."

schema_pharos_meds = StructType([
    StructField(
        "medication_id", 
        LongType(), 
        True, 
        {"comment": "Surrogate PK"}
    ),
    StructField(
        "person_id", 
        LongType(), 
        True, 
        {"comment": "Assigned unique ID for each participant (TBC)"}
    ),
    StructField(
        "clinical_record_id", 
        LongType(), 
        True, 
        {"comment": "Unique clincial record PharosID"}
    ),
    StructField(
        "medication_name", 
        StringType(), 
        True, 
        {"comment": "Name of the non-cancer medication the patient was taking at the time of cancer diagnosis, one medication per row, only one patient"}
    ),
    StructField(
        "medication_category", 
        StringType(), 
        True, 
        {"comment": "Category of the non-cancer medication the patient was taking at the time of cancer diagnosis"}
    ),
    StructField(
        "ADC_UPDT",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
    ),
    StructField(
        "date_checked", 
        TimestampType(), 
        True, 
        {"comment": "Date medical record regarding Non-cancer Medications data was checked DD-MM-YYYY"}
    )
])


def create_pharos_med_incr():

    diagnosis = spark.table("4_prod.bronze.map_diagnosis")
    max_adc_updt = get_max_timestamp("8_dev.silver.pharos_noncancer_meds", "date_checked_non_cancer_meds")

    medicine = spark.table("4_prod.bronze.map_med_admin")

    # Get the breast cancer cohort
    breast_cancer_cohort = (
        diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (col("ICD10_CODE").like("C50%") | col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("earliest_diagnosis_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )

    T1DM = [
        "51428",    # Insulin Aspart
        "86009",    # Insulin Lispro
        "400008",   # Insulin Glulisine
        "253181",   # Insulin Isophane
        "253182",   # Human Regular Insulin
        "139825",   # Insulin Detemir
        "274783",   # Insulin Glargine
        "1670007",  # Insulin Degludec
        "261420",   # Insulin Lispro Protamine Mix
        "2004040",  # Insulin Aspart Protamine Mix
        "1650023"   # Insulin Isophane–Regular Mix
    ]

    T2DM = [
        "4815",      # Glyburide
        "4821",      # Glipizide
        "6809",      # Metformin
        "10635",     # Tolbutamide
        "16681",     # Acarbose
        "25789",     # Glimepiride
        "33738",     # Pioglitazone
        "60548",     # Exenatide
        "73044",     # Repaglinide
        "475968",    # Liraglutide
        "593411",    # Sitagliptin
        "607999",    # Metformin and pioglitazone
        "729717",    # Metformin and sitagliptin
        "857974",    # Saxagliptin
        "1100699",   # Linagliptin
        "1243019",   # Linagliptin / metformin
        "1368001",   # Alogliptin
        "1368384",   # Alogliptin-metformin
        "1373458",   # Canagliflozin
        "1440051",   # Lixisenatide
        "1488564",   # Dapagliflozin
        "1545653",   # Empagliflozin
        "1551291",   # Dulaglutide
        "1991302",   # Semaglutide
        "2601723"    # Tirzepatide
    ]

    OTHER_MEDS = [
        "67108",    # Enoxaparin
        "32968",    # Clopidogrel
        "4603",     # Furosemide
        "235473",   # Heparin
        "298869",   # Eplerenone
        "104462",   # Heparin Flush
        "42463",    # Pravastatin
        "29046",    # Lisinopril
        "52769",    # Milrinone
        "6628",     # Mannitol
        "337623",   # Dornase Alfa
        "1592737",  # Nintedanib
        "8814",     # Epoprostenol
        "136411",   # Sildenafil
        "358263",   # Tadalafil
        "214618"    # Hydrochlorothiazide-Lisinopril
    ]

    all_diabetes_meds = T1DM + T2DM + OTHER_MEDS

    med = (
        medicine
        .join(breast_cancer_cohort, "PERSON_ID", "inner")
        .filter(col("RXNORM_CUI").isin(all_diabetes_meds))
        # Time window for the medication record selection (<=90 days)
        .filter(
            (col("ADMIN_START_DT_TM") <= col("earliest_diagnosis_date")) &
            (datediff(col("earliest_diagnosis_date"), col("ADMIN_START_DT_TM")) <= 90)
        )
        .withColumn(
            "medication_category",
            when(col("RXNORM_CUI").isin("32968", "52769"), "001 - Heart related medications")  # Clopidogrel, Milrinone
            .when(col("RXNORM_CUI").isin("42463"), "002 - Statins")  # Pravastatin
            .when(
                col("RXNORM_CUI").isin(
                    "29046",   # Lisinopril
                    "214618",  # Hydrochlorothiazide-Lisinopril
                    "298869",  # Eplerenone
                    "8814",    # Epoprostenol
                    "136411",  # Sildenafil
                    "358263"   # Tadalafil
                ),
                "003 - Hypertension drugs"
            )
            .when(col("RXNORM_CUI").isin("67108", "235473", "104462"), "004 - Anticoagulants")  # Enoxaparin, Heparin, Heparin Flush
            .when(col("RXNORM_CUI").isin("6628"), "005 - Inhalers and respiratory meds")  # Mannitol
            .when(col("RXNORM_CUI").isin("337623", "1592737"), "006 - COPD medications")  # Dornase Alfa, Nintedanib
            .when(col("RXNORM_CUI").isin(T1DM), "007 - T1DM Meds")
            .when(col("RXNORM_CUI").isin(T2DM), "008 - T2DM Meds")
        )
        .select(
            col("PERSON_ID").alias("person_id"),
            col("RXNORM_STR").alias("medication_name"),
            col("medication_category"),
            col("_src_adc_updt")
        )  
        .dropDuplicates()
    )

    final_df =(
        med
        .select(
            lit(None).alias("medication_id").cast(LongType()),
            col("person_id").cast(LongType()),
            lit(None).alias("pharosid").cast(StringType()),
            lit(None).alias("clinical_record_id").cast(LongType()),
            col("medication_name").cast(StringType()),
            col("medication_category").cast(StringType()),
            col("_src_adc_updt").alias("ADC_UPDT"),
            current_timestamp().alias("date_checked").cast(TimestampType()),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )  
    )

    return final_df

updated_df = create_pharos_med_incr()
update_table(updated_df, get_target_table("pharos_medication"), ["person_id", "medication_name"], schema_pharos_meds, pharos_meds_comment)

# COMMAND ----------

pharos_ph_comment = "This table contains a harmonised and standardised record of personal history of cancer diagnoses."

schema_pharos_ph = StructType([
    StructField(
        "history_id", 
        LongType(), 
        True, 
        {"comment": "Surrogate PK"}
    ),
    StructField(
        "person_id", 
        LongType(), 
        True, 
        {"comment": "Assigned unique ID for each participant (TBC)"}
    ),
    StructField(
        "pharosid", 
        StringType(), 
        True, 
        {"comment": "FK to person"}
    ),
    StructField(
        "clinical_record_id", 
        LongType(), 
        True, 
        {"comment": "Unique clincial record PharosID"}
    ),
    StructField(
        "cancer_type_code", 
        StringType(), 
        True, 
        {"comment": "Anatomical or diagnostic category of the historical cancer"}
    ),
    StructField(
        "Source_cd", 
        StringType(), 
        True, 
        {"comment": "Original code for the person history (Barts internal use)."}
    ),
    StructField(
        "cancer_type_detail", 
        StringType(), 
        True, 
        {"comment": "Free-text detail if not in reference list"}
    ),
    StructField(
        "date_checked", 
        StringType(), 
        True, 
        {"comment": "Date the medical record was reviewed for cancer history data DD-MM-YYYY"}
    ),
    StructField(
        "ADC_UPDT",
        TimestampType(),
        nullable=True,
        metadata={"comment": "Last update timestamp."}
    ),
    StructField(
        name="created_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row creation timestamp."}
    ),
    StructField(
        name="updated_at",
        dataType=TimestampType(),
        nullable=True,
        metadata={"comment": "Row last-updated timestamp."}
    )
])

def create_pharos_ph_incr():

    diagnosis = spark.table("4_prod.bronze.map_diagnosis")
    max_adc_updt = get_max_timestamp("4_prod.silver.pharos_personal_history", "date_checked_personal_history")

    # Get the breast cancer cohort
    breast_cancer_cohort = (
        diagnosis
        .filter(
            (col("ADC_UPDT") > max_adc_updt) &
            (col("ICD10_CODE").like("C50%") | col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
        )
        .filter(col("PERSON_ID").isNotNull())
        .groupBy("PERSON_ID")
        .agg(
            min("earliest_diagnosis_date").alias("brc_diag_date"),
            F.max("ADC_UPDT").alias("_src_adc_updt")
        )
    )

    omop_map = {
        4333465:  "006 - Brain cancer",                  # History of malignant neoplasm of nervous system
        46269969: "006 - Brain cancer",                  # History of cancer metastatic to brain
        3175032:  "018 - Head and neck cancer",          # History of cancer of ear
        3190039:  "018 - Head and neck cancer",          # History of cancer of head and neck
        4333345:  "018 - Head and neck cancer",          # History of malignant neoplasm of head and/or neck
        3189194:  "034 - Oral cancer",                   # History of cancer of mouth
        3184244:  "020 - Laryngeal cancer",              # History of cancer of larynx
        46270611: "044 - Salivary gland cancer",         # History of malignant neoplasm of parotid gland
        1246982:  "G09 - Unknown",                       # History of metastatic cancer
        3181707:  "045 - Sinonasal cancer",              # History of cancer of nares
        3184017:  "045 - Sinonasal cancer",              # History of sinus cancer
        4212564:  "023 - Lung cancer",                   # History of malignant neoplasm of bronchus
        4324190:  "007 - Breast cancer",                 # History of malignant neoplasm of breast
        4187205:  "023 - Lung cancer",                   # History of malignant neoplasm of lung
        4187206:  "023 - Lung cancer",                   # History of malignant neoplasm of trachea
        4323367:  "040 - Pleural cancer",                # History of malignant mesothelioma
        4178640:  "040 - Pleural cancer",                # History of malignant neoplasm of pleura
        4179069:  "G09 - Unknown",                       # History of malignant neoplasm of thoracic cavity structure
        4177071:  "025 - Mediastinal cancer",            # History of malignant neoplasm of mediastinum
        46273376: "047 - Small bowel cancer",            # History of cancer of ampulla of duodenum
        35610791: "G09 - Unknown",                       # History of malignant neoplasm of digestive organ
        4190633:  "011 - Colorectal cancer",             # History of malignant neoplasm of gastrointestinal tract
        3184515:  "G09 - Unknown",                       # History of splenic cancer
        46273480: "011 - Colorectal cancer",             # History of rectosigmoid junction cancer
        4327107:  "010 - Colon cancer",                  # History of malignant neoplasm of colon
        4324189:  "043 - Rectal cancer",                 # History of malignant neoplasm of rectum
        37016142: "002 - Anal cancer",                   # History of malignant neoplasm of anus
        3189077:  "022 - Liver cancer",                  # History of liver cancer
        4180749:  "042 - Prostate cancer",               # History of malignant neoplasm of prostate
        3169330:  "042 - Prostate cancer",               # Personal history of prostate cancer
        43021271: "050 - Testicular cancer",             # History of malignant neoplasm of testis
        4327872:  "039 - Penile cancer",                 # History of malignant neoplasm of penis
        4187203:  "G09 - Unknown",                       # History of malignant neoplasm of female genital organ
        4323346:  "G09 - Unknown",                       # History of malignant neoplasm of uterine adnexa
        46273417: "041 - Primary peritoneal cancer",     # History of malignant neoplasm of peritoneum
        4178782:  "008 - Cervical cancer",               # History of malignant neoplasm of cervix
        45763611: "012 - Endometrial cancer",            # History of carcinosarcoma of uterus
        37159719:  "012 - Endometrial cancer",           # History of choriocarcinoma
        4179084:  "012 - Endometrial cancer",            # History of malignant neoplasm of uterine body
        4325186:  "055 - Vaginal cancer",                # History of malignant neoplasm of vagina
        4181024:  "056 - Vulvar cancer",                 # History of malignant neoplasm of vulva
        4216132:  "004 - Bladder cancer",                # History of malignant neoplasm of urinary system
        4325868:  "053 - Upper urinary tract cancer",    # History of malignant neoplasm of ureter
        46273500: "054 - Urethral cancer",               # History of cancer of urethra
        3174258:  "052 - Thyroid cancer",                # History of thyroid cancer
        4197758:  "G09 - Unknown",                       # History of malignant neoplasm of endocrine gland
        46270077: "031 - Neuroendocrine tumour",         # History of neuroendocrine malignant neoplasm
        4058705:  "026 - Melanoma",                      # H/O Malignant melanoma
        4179242:  "032 - Non-melanoma skin cancer",      # History of malignant neoplasm of skin
        3176981:  "005 - Bone cancer",                   # History of bone cancer
        4180113:  "005 - Bone cancer",                   # History of malignant neoplasm of bone
        4325206:  "005 - Bone cancer",                   # History of osteosarcoma
        4323212:  "048 - Soft tissue sarcoma",           # History of Kaposi's sarcoma
        46273501: "048 - Soft tissue sarcoma",           # History of malignant neoplasm of retroperitoneum
        46273380: "048 - Soft tissue sarcoma",           # History of rhabdomyosarcoma
        3176052:  "048 - Soft tissue sarcoma",           # History of sarcoma
        46270545: "048 - Soft tissue sarcoma",           # History of soft tissue sarcoma
        44782983: "G09 - Unknown",                       # History of malignant hematologic neoplasm
        3180053:  "024 - Lymphoma",                      # History of Hodgkins disease
        4180131:  "017 - Germ cell tumour",              # History of malignant neoplasm of epididymis
        4190635:  "G09 - Unknown",                       # History of malignant neoplasm of male genital organ
        4332932:  "009 - Childhood cancer",              # History of neuroblastoma
        44782997: "009 - Childhood cancer"               # History of primitive neuroectodermal tumor
    }

    icd10_map = {
        "Z85.0": "G09 - Unknown",                       # digestive organs
        "Z85.1": "023 - Lung cancer",                   # trachea, bronchus, lung
        "Z85.2": "G09 - Unknown",                       # other respiratory/intrathoracic
        "Z85.3": "007 - Breast cancer",                 # breast
        "Z85.4": "G09 - Unknown",                       # genital organs
        "Z85.5": "G09 - Unknown",                       # urinary tract
        "Z85.6": "021 - Leukaemia",                     # leukaemia
        "Z85.7": "G09 - Unknown",                       # lymphoid/haematopoietic
        "Z85.8": "G09 - Unknown",                       # other organs
        "Z85.9": "G09 - Unknown"                        # unspecified
    }



    combined_cancer_map = {
        **{str(k): v for k, v in omop_map.items()},
        **icd10_map
    }

    mapping_df = spark.createDataFrame([
        Row(code=str(k), cancer_type_code=v) for k, v in combined_cancer_map.items()
    ])

    # Normalize diagnosis codes
    diagnosis = diagnosis.withColumn(
        "code",
        col("OMOP_CONCEPT_ID").cast("string")
    )

    person_history = (
        diagnosis
        .join(breast_cancer_cohort, "PERSON_ID", "left")
        .join(mapping_df, "code", "inner")
    )

    final_df = (
        person_history
        .filter(col("cancer_type_code").isNotNull())
        .select(
            col("person_id"),
            lit(None).alias("pharosid"),
            lit(None).alias("clinical_record_id"),
            col("cancer_type_code"),
            col("code").alias("Source_cd"),
            col("SOURCE_STRING").alias("cancer_type_detail"),
            col("_src_adc_updt").alias("ADC_UPDT"),
            current_timestamp().alias("date_checked"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
    )

    return final_df

updated_df = create_pharos_ph_incr()

update_table(updated_df, get_target_table("pharos_personal_history"), ["person_id","cancer_type_code"], schema_pharos_ph, pharos_ph_comment)


