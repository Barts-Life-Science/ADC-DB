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

@dlt.table(
    name="address_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def address_mapping_incr():
    """
    Creates an incremental address mapping table that processes only new or modified records.
    This temporary table serves as an intermediate step in the mapping pipeline.
    
    Process:
    1. Filters for active addresses for persons and organizations
    2. Joins with country lookup table
    3. Applies privacy masking for personal addresses
    4. Formats street addresses for organizations
    
    Data quality checks:
    - Validates entity types
    - Ensures active status
    - Verifies effective dates
    
    Returns:
        DataFrame: Processed address records with standardized format
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_address")
    
    # Define window for selecting most recent valid address
    window = Window.partitionBy("PARENT_ENTITY_ID").orderBy(
        when(col("ZIPCODE").isNotNull() & (trim(col("ZIPCODE")) != ""), 0).otherwise(1),
        desc("BEG_EFFECTIVE_DT_TM")
    )
    
    # Get base address data with filtering
    base_addresses = (
        spark.table("4_prod.raw.mill_address")
        .filter(
            (col("PARENT_ENTITY_NAME").isin("PERSON", "ORGANIZATION")) & 
            (col("ACTIVE_IND") == 1) & 
            (col("END_EFFECTIVE_DT_TM") > current_date()) &
            (col("ADC_UPDT") > max_adc_updt)  # Only process new/modified records
        )
    )
    
    # Get country lookup data
    country_lookup = dlt.read("code_value")
    
    return (
        base_addresses
        # Join with country lookup
        .join(
            country_lookup.select("CODE_VALUE", "DESCRIPTION"),
            col("country_cd") == col("CODE_VALUE"),
            "left"
        )
        # Select most recent valid address
        .withColumn("row", row_number().over(window))
        .filter(col("row") == 1)
        # Format street address for organizations
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
        # Apply privacy masking for personal addresses
        .withColumn(
            "masked_zipcode",
            when(col("PARENT_ENTITY_NAME") == "PERSON", 
                substring(col("ZIPCODE"), 1, 3))
            .otherwise(col("ZIPCODE"))
        )
        # Select final columns
        .select(
            "ADDRESS_ID",
            "PARENT_ENTITY_NAME",
            "PARENT_ENTITY_ID",
            "masked_zipcode",
            "CITY",
            "full_street_address",
            "ADC_UPDT",
            coalesce(col("DESCRIPTION"), col("country_cd")).alias("country_cd")
        )
    )

@dlt.view(name="address_mapping_update")
def address_mapping_update():
    """
    Creates a streaming view of the incremental address mappings.
    This view is used to feed the apply_changes function.
    """
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.address_mapping_incr")
    )

# Define the final target table with optimization settings
dlt.create_target_table(
    name = "map_address",
    comment = "Standardized address mappings with privacy controls",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "ADDRESS_ID,PARENT_ENTITY_ID"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "map_address",
    source = "address_mapping_update",
    keys = ["ADDRESS_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(
    name="person_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
@dlt.expect("valid_gender", "gender_cd IS NOT NULL")
@dlt.expect("valid_birth_year", """
    birth_year IS NOT NULL AND 
    birth_year >= 1901 AND 
    birth_year <= year(current_date())
""")
def person_mapping_incr():
    """
    Creates an incremental person mapping table that processes only new or modified records.
    
    Data quality expectations:
    - Valid gender code must be present
    - Birth year must be present and within reasonable range (1901 to current year)
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_person")
    
    # Get current year for birth date validation
    current_year = year(current_date())
    
    # Get reference tables
    code_lookup = dlt.read("code_value")
    address_lookup = dlt.read("map_address")
    
    # Get base person data with filtering
    base_persons = (
        spark.table("4_prod.raw.mill_person")
        .filter(
            (col("active_ind") == 1) &
            (col("ADC_UPDT") > max_adc_updt)  # Only process new/modified records
        )
    )
    

    
    return (
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
        # Join with address lookup
        .join(
            address_lookup
            .filter(col("PARENT_ENTITY_NAME") == "PERSON")
            .select("PARENT_ENTITY_ID", "ADC_UPDT", "ADDRESS_ID")
            .alias("addr"),
            col("PERSON_ID") == col("addr.PARENT_ENTITY_ID"),
            "left"
        )
        # Calculate birth year
        .withColumn(
            "birth_year", 
            year(col("BIRTH_DT_TM")).cast(IntegerType())
        )
        # Apply data quality filters
        .filter(
            col("SEX_CD").isNotNull() &
            col("birth_year").isNotNull() &
            (col("birth_year") >= 1901) &
            (col("birth_year") <= current_year)
        )
        # Select final columns with standardized names
        .select(
            col("PERSON_ID").alias("person_id"),
            col("SEX_CD").alias("gender_cd"),
            col("birth_year"),
            col("ETHNIC_GRP_CD").alias("ethnicity_cd"),
            col("addr.ADDRESS_ID").alias("address_id"),
            col("4_prod.raw.mill_person.ADC_UPDT")
        )
    )

@dlt.view(name="person_mapping_update")
def person_mapping_update():
    """
    Creates a streaming view of the incremental person mappings.
    This view is used to feed the apply_changes function.
    """
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.person_mapping_incr")
    )

# Define the final target table with optimization settings
dlt.create_target_table(
    name = "map_person",
    comment = "Standardized person mappings with demographic information",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "person_id,address_id"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "map_person",
    source = "person_mapping_update",
    keys = ["person_id"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)


# COMMAND ----------

@dlt.table(
    name="building_locations",
    comment="Building hierarchy reference table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def lookup_buildings():
    """
    Creates a reference table for building locations.
    Filters location groups for building type (778) and aggregates child locations.
    """
    return (
        spark.table("4_prod.raw.mill_location_group")
        .filter(col("LOCATION_GROUP_TYPE_CD") == 778)
        .groupBy("CHILD_LOC_CD")
        .agg(first("PARENT_LOC_CD").alias("building_cd"))
    )

@dlt.table(
    name="facility_locations",
    comment="Facility hierarchy reference table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def lookup_facilities():
    """
    Creates a reference table for facility locations.
    Filters location groups for facility type (783) and aggregates child locations.
    """
    return (
        spark.table("4_prod.raw.mill_location_group")
        .filter(col("LOCATION_GROUP_TYPE_CD") == 783)
        .groupBy("CHILD_LOC_CD")
        .agg(first("PARENT_LOC_CD").alias("facility_cd"))
    )

# COMMAND ----------

@dlt.table(
    name="care_site_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
@dlt.expect("valid_location_type", "location_type_cd = 794")
@dlt.expect("valid_care_site", "care_site_cd IS NOT NULL")
def care_site_mapping_incr():
    """
    Creates an incremental care site mapping table that processes only new or modified records.
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_care_site")
    
    # Get base tables
    locations = spark.table("4_prod.raw.mill_location").alias("loc")
    code_values = dlt.read("code_value").alias("cv")
    organizations = spark.table("4_prod.raw.mill_organization").alias("org")
    address_lookup = dlt.read("map_address").alias("al")
    
    # Get building and facility references
    buildings = dlt.read("building_locations")
    facilities = dlt.read("facility_locations")
    
    # Filter for new/modified records
    base_locations = locations.filter(
        (col("location_type_cd") == 794) &
        (col("ADC_UPDT") > max_adc_updt)
    )
    
    
    return (
        base_locations
        # Join to get care site name
        .join(
            code_values.select("CODE_VALUE", "CDF_MEANING").alias("cs"),
            col("loc.location_cd") == col("cs.CODE_VALUE"),
            "left"
        )
        
        # Join building hierarchy
        .join(
            buildings.alias("bldg"), 
            col("loc.location_cd") == col("bldg.CHILD_LOC_CD"), 
            "left"
        )
        .join(
            code_values.select("CODE_VALUE", "CDF_MEANING").alias("bldg_desc"),
            col("bldg.building_cd") == col("bldg_desc.CODE_VALUE"),
            "left"
        )
        
        # Join facility hierarchy
        .join(
            facilities.alias("fac"), 
            col("bldg.building_cd") == col("fac.CHILD_LOC_CD"), 
            "left"
        )
        .join(
            code_values.select("CODE_VALUE", "CDF_MEANING").alias("fac_desc"),
            col("fac.facility_cd") == col("fac_desc.CODE_VALUE"),
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
            address_lookup.filter(col("PARENT_ENTITY_NAME") == "ORGANIZATION")
            .select("PARENT_ENTITY_ID", "ADC_UPDT", "ADDRESS_ID"),
            col("org.ORGANIZATION_ID") == col("PARENT_ENTITY_ID"),
            "left"
        )
        
        # Select final columns with standardized names
        .select(
            col("loc.location_cd").alias("care_site_cd"),
            col("loc.location_type_cd"),
            col("cs.CDF_MEANING").alias("care_site_name"),
            col("bldg.building_cd"),
            col("bldg_desc.CDF_MEANING").alias("building_name"),
            col("fac.facility_cd"),
            col("fac_desc.CDF_MEANING").alias("facility_name"),
            col("org.ORGANIZATION_ID"),
            col("org.ORG_NAME").alias("organization_name"),
            col("ADDRESS_ID").alias("address_id"),
            greatest(
                col("loc.ADC_UPDT"),
                col("al.ADC_UPDT")
            ).alias("ADC_UPDT")
        )
    )

@dlt.view(name="care_site_mapping_update")
def care_site_mapping_update():
    """Creates a streaming view of the incremental care site mappings."""
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.care_site_mapping_incr")
    )

# Define the final target table
dlt.create_target_table(
    name = "map_care_site",
    comment = "Care site mappings with full location hierarchy",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "care_site_cd,building_cd,facility_cd"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "map_care_site",
    source = "care_site_mapping_update",
    keys = ["care_site_cd"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------



@dlt.table(
    name="group_types",
    comment="Medical group types reference table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect("valid_code", "CODE_VALUE IS NOT NULL")
@dlt.expect("valid_meaning", "CDF_MEANING IS NOT NULL")
def lookup_group_types():
    """Creates a reference table for medical group types."""
    return (
        dlt.read("code_value")
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
    
    # Get recent events
    recent_events = (
        clinical_events
        .join(encounters, "ENCNTR_ID")
        .filter(col("LOC_NURSE_UNIT_CD").isNotNull())
        .withColumn("row_num", row_number().over(recent_events_window))
        .filter(col("row_num") <= 100)
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
    
    # Add location names
    return (
        primary_locations
        .join(
            code_values.select("CODE_VALUE", "CDF_MEANING").alias("loc"),
            col("primary_care_site_cd") == col("loc.CODE_VALUE"),
            "left"
        )
        .select(
            "PERSON_ID",
            "primary_care_site_cd",
            col("loc.CDF_MEANING").alias("primary_care_site_name") 
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

# COMMAND ----------

@dlt.table(
    name="medical_personnel_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def medical_personnel_mapping_incr():
    """
    Creates an incremental medical personnel mapping table.
    Processes only new or modified records.
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_medical_personnel")
    
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
    code_values = dlt.read("code_value")
    position_lookup = (
        dlt.read("code_value")
        .select(
            col("CODE_VALUE"),
            col("DESCRIPTION").alias("position_name")
        )
    )
    group_types = dlt.read("group_types")
    
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
    
    # Get group assignments with explicit column selection
    group_assignments = get_group_assignments(prsnl_group_reltn, valid_groups)
    
    # Combine all data with explicit column selection
    return (
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

@dlt.view(name="medical_personnel_mapping_update")
def medical_personnel_mapping_update():
    """Creates a streaming view of the incremental medical personnel mappings."""
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.medical_personnel_mapping_incr")
    )

# Define the final target table
dlt.create_target_table(
    name = "map_medical_personnel",
    comment = "Medical personnel mappings with positions and assignments",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID,POSITION_CD"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "map_medical_personnel",
    source = "medical_personnel_mapping_update",
    keys = ["PERSON_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------



@dlt.table(
    name="encounter_event_times",
    comment="Earliest and latest clinical event times for encounters",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def get_event_times():
    """Calculates event time boundaries for each encounter."""
    return (
        spark.table("4_prod.raw.mill_clinical_event")
        .groupBy("ENCNTR_ID")
        .agg(
            min("CLINSIG_UPDT_DT_TM").alias("earliest_event_time"),
            max("CLINSIG_UPDT_DT_TM").alias("latest_event_time")
        )
    )

def create_code_lookup(code_values, description_alias):
    """Helper function to create code value lookups with consistent structure."""
    return code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(description_alias)
    )

# COMMAND ----------

@dlt.table(
    name="encounter_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def encounter_mapping_incr():
    """
    Creates an incremental encounter mapping table.
    
    Process:
    1. Joins encounter data with clinical event times
    2. Applies code value lookups for various attributes
    3. Calculates arrival and departure times
    4. Standardizes output format
    
    Returns:
        DataFrame: Processed encounter records with standardized format
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_encounter")
    
    # Constants
    one_week_seconds = 7 * 24 * 60 * 60
    
    # Get base encounter data
    base_encounters = (
        spark.table("4_prod.raw.mill_encounter")
        .filter(col("ADC_UPDT") > max_adc_updt)
    )
    
    
    # Get reference data
    code_values = dlt.read("code_value")
    event_times = dlt.read("encounter_event_times")
    
    # Create code value lookups
    lookups = {
        "class": create_code_lookup(code_values, "encntr_class_desc"),
        "type": create_code_lookup(code_values, "encntr_type_desc"),
        "status": create_code_lookup(code_values, "encntr_status_desc"),
        "admit": create_code_lookup(code_values, "admit_src_desc"),
        "disch": create_code_lookup(code_values, "disch_loctn_desc"),
        "med": create_code_lookup(code_values, "med_service_desc"),
        "nurse": create_code_lookup(code_values, "nurse_unit_desc"),
        "spec": create_code_lookup(code_values, "specialty_unit_desc")
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

@dlt.view(name="encounter_mapping_update")
def encounter_mapping_update():
    """Creates a streaming view of the incremental encounter mappings."""
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.encounter_mapping_incr")
    )

# Define the final target table
dlt.create_target_table(
    name = "map_encounter",
    comment = "Standardized encounter mappings with calculated timestamps",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "ENCNTR_ID,PERSON_ID"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "map_encounter",
    source = "encounter_mapping_update",
    keys = ["ENCNTR_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

def create_code_lookup_diag(code_values, alias_name):
    """Helper function to create specific code value lookups"""
    return code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(f"{alias_name}_desc")
    ).alias(alias_name)



# COMMAND ----------

@dlt.table(
    name="diagnosis_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def diagnosis_mapping_incr():
    """
    Creates an incremental diagnosis mapping table processing only new or modified records.
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_diagnosis")
    
    # Get required tables
    diagnosis = spark.table("4_prod.raw.mill_diagnosis").filter(col("ADC_UPDT") > max_adc_updt)
    
    
    nomenclature = spark.table("4_prod.bronze.nomenclature")
    encounter = dlt.read("map_encounter")
    code_values = dlt.read("code_value")
    
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

@dlt.view(name="diagnosis_mapping_update")
def diagnosis_mapping_update():
    """Creates a streaming view of the incremental diagnosis mappings."""
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.diagnosis_mapping_incr")
    )

# Define the final target table
dlt.create_target_table(
    name = "map_diagnosis",
    comment = "Standardized diagnosis mappings with code descriptions",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "DIAGNOSIS_ID,PERSON_ID,ENCNTR_ID"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "map_diagnosis",
    source = "diagnosis_mapping_update",
    keys = ["DIAGNOSIS_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

def create_problem_lookup(code_values, alias_name):
    return code_values.select(
        col("CODE_VALUE"),
        col("CDF_MEANING").alias(alias_name)
    )


def process_problem_nomenclature(nomenclature):
    return (
        nomenclature
        .withColumn(
            "CONCEPT_CKI_PROCESSED",
            substring_index(col("CONCEPT_CKI"), "!", -1)
        )
    )

def create_problem_base_result(problem_df, nomenclature_df, confirmation_lookup, classification_lookup, vocab_lookup, axis_lookup):
    return (
        problem_df.alias("prob")
        .join(
            nomenclature_df.alias("nom"),
            "NOMENCLATURE_ID",
            "left"
        )
        .join(
            confirmation_lookup.alias("conf"),
            col("CONFIRMATION_STATUS_CD") == col("conf.CODE_VALUE"),
            "left"
        )
        .join(
            classification_lookup.alias("class"),
            col("CLASSIFICATION_CD") == col("class.CODE_VALUE"),
            "left"
        )
        .join(
            vocab_lookup.alias("vocab"),
            col("SOURCE_VOCABULARY_CD") == col("vocab.CODE_VALUE"),
            "left"
        )
        .join(
            axis_lookup.alias("axis"),
            col("VOCAB_AXIS_CD") == col("axis.CODE_VALUE"),
            "left"
        )
        .select(
            col("prob.PROBLEM_ID"),
            col("prob.PERSON_ID"),
            col("prob.NOMENCLATURE_ID"),
            col("prob.ONSET_DT_TM"),
            col("earliest_problem_date"),
            col("prob.ACTIVE_STATUS_DT_TM"),
            col("prob.ACTIVE_STATUS_PRSNL_ID"),
            col("prob.DATA_STATUS_DT_TM"),
            col("prob.DATA_STATUS_PRSNL_ID"),
            col("prob.UPDATE_ENCNTR_ID"),
            col("prob.ORIGINATING_ENCNTR_ID"),
            col("CONFIRMATION_STATUS_CD"),
            col("confirmation_status_desc"),
            col("CLASSIFICATION_CD"),
            col("classification_desc"),
            col("RANKING_CD"),
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
            col("CONCEPT_CLASS").alias("OMOP_CONCEPT_CLASS"),
            col("SNOMED_CODE"),
            col("SNOMED_TYPE"),
            col("SNOMED_MATCH_COUNT").alias("SNOMED_MATCH_NUMBER"),
            col("SNOMED_TERM"),
            col("ICD10_CODE"),
            col("ICD10_TYPE"),
            col("ICD10_MATCH_COUNT").alias("ICD10_MATCH_NUMBER"),
            col("ICD10_TERM"),
            greatest(
                col("prob.ADC_UPDT"),
                col("nom.ADC_UPDT")
            ).alias("ADC_UPDT"),
            coalesce(
                col("prob.ONSET_DT_TM"),
                col("prob.ACTIVE_STATUS_DT_TM")
            ).alias("CALC_DT_TM")
        )
    )

def process_problem_encounter_associations(base_result, encounters):
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
            col("base.PROBLEM_ID"),  
            col("base.PERSON_ID"),
            col("base.NOMENCLATURE_ID"),
            col("base.ONSET_DT_TM"),
            col("earliest_problem_date"),
            col("base.ACTIVE_STATUS_DT_TM"),
            col("ACTIVE_STATUS_PRSNL_ID"),
            col("base.DATA_STATUS_DT_TM"),
            col("base.DATA_STATUS_PRSNL_ID"),
            col("UPDATE_ENCNTR_ID"),
            col("ORIGINATING_ENCNTR_ID"),
            col("base.CONFIRMATION_STATUS_CD"),
            col("confirmation_status_desc"),
            col("base.CLASSIFICATION_CD"),
            col("classification_desc"),
            col("base.RANKING_CD"),
            col("base.SOURCE_IDENTIFIER"),
            col("base.SOURCE_STRING"),
            col("base.SOURCE_VOCABULARY_CD"),
            col("source_vocabulary_desc"),
            col("base.VOCAB_AXIS_CD"),
            col("vocab_axis_desc"),
            col("CONCEPT_CKI"),
            col("OMOP_CONCEPT_ID"),
            col("OMOP_CONCEPT_NAME"),
            col("OMOP_STANDARD_CONCEPT"),
            col("OMOP_MATCH_NUMBER"),
            col("OMOP_CONCEPT_DOMAIN"),
            col("OMOP_CONCEPT_CLASS"),
            col("SNOMED_CODE"),
            col("SNOMED_TYPE"),
            col("SNOMED_MATCH_NUMBER"),
            col("SNOMED_TERM"),
            col("ICD10_CODE"),
            col("ICD10_TYPE"),
            col("ICD10_MATCH_NUMBER"),
            col("ICD10_TERM"),
            col("base.ADC_UPDT"),
            col("CALC_DT_TM"),
            col("CALC_ENCNTR"),
            col("CALC_ENC_WITHIN"),
            col("CALC_ENC_BEFORE"),
            col("CALC_ENC_AFTER")
        )
    )

# COMMAND ----------

@dlt.table(
    name="problem_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def problem_mapping_incr():
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_problem")
    
    problem = spark.table("4_prod.raw.mill_problem")
    nomenclature = spark.table("4_prod.bronze.nomenclature")
    code_values = dlt.read("code_value")
    encounters = dlt.read("map_encounter")
    
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
    
    confirmation_status_lookup = create_problem_lookup(code_values, "confirmation_status_desc")
    classification_lookup = create_problem_lookup(code_values, "classification_desc")
    source_vocab_lookup = create_problem_lookup(code_values, "source_vocabulary_desc")
    vocab_axis_lookup = create_problem_lookup(code_values, "vocab_axis_desc")
    
    nomenclature_processed = process_problem_nomenclature(nomenclature)
    
    base_result = create_problem_base_result(
        problem_with_dates,
        nomenclature_processed,
        confirmation_status_lookup,
        classification_lookup,
        source_vocab_lookup,
        vocab_axis_lookup
    )
    
    return process_problem_encounter_associations(base_result, encounters)

@dlt.view(name="problem_mapping_update")
def problem_mapping_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.problem_mapping_incr")
    )

dlt.create_target_table(
    name = "map_problem",
    comment = "Problem mappings with encounter associations",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PROBLEM_ID,PERSON_ID,NOMENCLATURE_ID"
    }
)

dlt.apply_changes(
    target = "map_problem",
    source = "problem_mapping_update",
    keys = ["PROBLEM_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
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



@dlt.table(
    name="med_admin_rxnorm",
    comment="RxNorm reference table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def create_rxnorm_reference():
    """Creates reference table for RxNorm concepts"""
    return spark.table("3_lookup.rxnorm.rxnconso")

@dlt.table(
    name="med_admin_snomed",
    comment="SNOMED reference table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def create_snomed_reference():
    """Creates reference table for SNOMED concepts"""
    return (
        dlt.read("med_admin_rxnorm")
        .filter(col("SAB") == "SNOMEDCT_US")
        .withColumn("CODE_LENGTH", length(col("CODE")))
        .withColumn("rn", row_number().over(
            Window.partitionBy("RXCUI").orderBy("CODE_LENGTH")))
        .filter(col("rn") == 1)
        .select("RXCUI", "CODE", "STR")
    )


def add_standardized_columns(df, standardization_cases):
    """
    Adds standardized dosage columns to the dataframe
    
    Args:
        df: DataFrame containing medication administration records
        standardization_cases: Dictionary containing weight and volume standardization cases
        
    Returns:
        DataFrame: Original data with added standardized dosage columns
    """
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

# COMMAND ----------

def augment_snomed_codes(med_admin_df):
    """
    Augments SNOMED codes in the medication administration table by finding matches
    through various lookup tables when the SNOMED_CODE is null.
    Includes additional logic to match on drug name before first number.
    All matching is done case-insensitively.
    """
    # Get base lookup tables
    snomed_sct = dlt.read("snomed_sct")
    omop_concept = dlt.read("omop_concept")
    concept_relationship = dlt.read("omop_concept_relationship")
    
    # Get the list of all columns from input dataframe
    all_columns = med_admin_df.columns
    
    # Get rows that need mapping - keep all columns
    rows_to_map = med_admin_df.filter(col("SNOMED_CODE").isNull())
    rows_with_codes = med_admin_df.filter(col("SNOMED_CODE").isNotNull())
    
    # Get valid SNOMED concept IDs
    valid_snomed_concept_ids = (
        omop_concept
        .filter(
            (col("vocabulary_id") == "SNOMED") &
            (col("standard_concept").isNotNull()) &
            (col("invalid_reason").isNull()) &
            ((col("domain_id") == "Drug") | (col("domain_id") == "Ingredient"))
        )
        .select("concept_id")
        .collect()
    )
    
    valid_ids = [row.concept_id for row in valid_snomed_concept_ids]
    
    # Get drug concepts with lowercase names
    drug_concepts = (
        omop_concept
        .filter(
            ((col("domain_id") == "Drug") | (col("domain_id") == "Ingredient"))
        )
        .select(
            col("concept_id").alias("drug_concept_id"),
            lower(col("concept_name")).alias("drug_concept_name")
        )
    )
    
    # Get SNOMED direct matches with lowercase terms for exact matching
    snomed_direct_matches = (
        snomed_sct
        .select(
            lower(col("term")).alias("sct_term_exact"),
            col("cui").alias("snomed_from_sct")
        )
        .withColumn("rn", row_number().over(Window.partitionBy("sct_term_exact").orderBy("snomed_from_sct")))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    # Get SNOMED direct matches with lowercase terms for simplified matching
    snomed_direct_matches_simplified = (
        snomed_sct
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
        concept_relationship
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
            omop_concept.alias("snomed_concept"),
            col("concept_id_2") == col("snomed_concept.concept_id")
        )
        .select(
            col("drug_concept_name").alias("forward_term"),
            col("snomed_concept.concept_code").alias("snomed_from_forward_mapping")
        )
        .withColumn("rn", row_number().over(Window.partitionBy("forward_term").orderBy("snomed_from_forward_mapping")))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    # Forward mappings for simplified matches with different column names
    forward_mappings_simplified = (
        concept_relationship
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
            omop_concept.alias("snomed_concept"),
            col("concept_id_2") == col("snomed_concept.concept_id")
        )
        .select(
            col("drug_concept_name").alias("forward_term_simplified"),
            col("snomed_concept.concept_code").alias("snomed_from_forward_mapping_simplified")
        )
        .withColumn("rn", row_number().over(Window.partitionBy("forward_term_simplified").orderBy("snomed_from_forward_mapping_simplified")))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    # Backward mappings with lowercase terms
    backward_mappings = (
        concept_relationship
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
            omop_concept.alias("snomed_concept"),
            col("concept_id_1") == col("snomed_concept.concept_id")
        )
        .select(
            col("drug_concept_name").alias("backward_term"),
            col("snomed_concept.concept_code").alias("snomed_from_backward_mapping")
        )
        .withColumn("rn", row_number().over(Window.partitionBy("backward_term").orderBy("snomed_from_backward_mapping")))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    # Backward mappings for simplified matches
    backward_mappings_simplified = (
        concept_relationship
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
            omop_concept.alias("snomed_concept"),
            col("concept_id_1") == col("snomed_concept.concept_id")
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
        
        # Create new SNOMED_CODE column with properly qualified column references
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
        
        # Select all original columns plus new ones in the correct order
        .select(
            *[col(c) for c in all_columns if c != "SNOMED_CODE"],  # All original columns except SNOMED_CODE
            col("SNOMED_CODE"),  # Newly created SNOMED_CODE
            col("SNOMED_SOURCE")  # New source column
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
    
    Args:
        med_df: DataFrame containing medication administration records
        
    Returns:
        DataFrame: Original data enriched with OMOP concept mappings
    """
    # Get OMOP concepts reference table
    concepts = dlt.read("omop_concept")
    
    # Create separate concept DataFrames for each vocabulary
    multum_concepts = (
        concepts.filter(col("vocabulary_id") == "Multum")
        .select(
            col("concept_code").alias("multum_code"),
            col("concept_id").alias("multum_concept_id"),
            col("concept_name").alias("multum_concept_name"),
            col("standard_concept").alias("multum_standard_concept")
        )
    )
    
    rxnorm_concepts = (
        concepts.filter(col("vocabulary_id") == "RxNorm")
        .select(
            col("concept_code").alias("rxnorm_code"),
            col("concept_id").alias("rxnorm_concept_id"),
            col("concept_name").alias("rxnorm_concept_name"),
            col("standard_concept").alias("rxnorm_standard_concept")
        )
    )
    
    rxnorm_ext_concepts = (
        concepts.filter(col("vocabulary_id") == "RxNorm Extension")
        .select(
            col("concept_code").alias("rxnorm_ext_code"),
            col("concept_id").alias("rxnorm_ext_concept_id"),
            col("concept_name").alias("rxnorm_ext_concept_name"),
            col("standard_concept").alias("rxnorm_ext_standard_concept")
        )
    )
    
    snomed_concepts = (
        concepts.filter(col("vocabulary_id") == "SNOMED")
        .select(
            col("concept_code").alias("snomed_concept_code"),
            col("concept_id").alias("snomed_concept_id"),
            col("concept_name").alias("snomed_concept_name"),
            col("standard_concept").alias("snomed_standard_concept")
        )
    )

    # Create drug name matching concepts with distinct column names for exact and simplified matches
    drug_name_concepts_exact = (
        concepts.filter(
            ((col("domain_id") == "Drug") | (col("domain_id") == "Ingredient")) &
            (col("invalid_reason").isNull())
        )
        .select(
            col("concept_id").alias("exact_concept_id"),
            col("concept_name").alias("exact_concept_name"),
            col("vocabulary_id").alias("exact_vocabulary"),
            col("standard_concept").alias("exact_standard_concept")
        )
        .withColumn("exact_name_lower", lower(col("exact_concept_name")))
    )

    drug_name_concepts_simplified = (
        concepts.filter(
            ((col("domain_id") == "Drug") | (col("domain_id") == "Ingredient")) &
            (col("invalid_reason").isNull())
        )
        .select(
            col("concept_id").alias("simplified_concept_id"),
            col("concept_name").alias("simplified_concept_name"),
            col("vocabulary_id").alias("simplified_vocabulary"),
            col("standard_concept").alias("simplified_standard_concept")
        )
        .withColumn("simplified_name_lower", lower(col("simplified_concept_name")))
    )
    
    # Join with med_df in sequence, using left joins
    mapped_df = (
        med_df
        # Convert MULTUM to integer for joining
        .withColumn("MULTUM_INT", 
            when(col("MULTUM").isNotNull(), 
                col("MULTUM").cast("integer"))
        )
        # Join with Multum concepts
        .join(
            multum_concepts,
            col("MULTUM_INT") == col("multum_code").cast("integer"),
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
    )
    
    # Drop intermediate columns
    columns_to_drop = [
        "MULTUM_INT", "multum_code", "multum_concept_id", "multum_concept_name", "multum_standard_concept",
        "rxnorm_code", "rxnorm_concept_id", "rxnorm_concept_name", "rxnorm_standard_concept",
        "rxnorm_ext_code", "rxnorm_ext_concept_id", "rxnorm_ext_concept_name", "rxnorm_ext_standard_concept",
        "snomed_concept_code", "snomed_concept_id", "snomed_concept_name", "snomed_standard_concept",
        "order_term_lower", "order_term_simplified", 
        "exact_concept_id", "exact_concept_name", "exact_vocabulary", "exact_standard_concept", "exact_name_lower",
        "simplified_concept_id", "simplified_concept_name", "simplified_vocabulary", "simplified_standard_concept", "simplified_name_lower"
    ]
    
    return mapped_df.drop(*columns_to_drop)

# COMMAND ----------

@dlt.table(
    name="base_medication_administrations",
    comment="Base medication administration records",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def create_base_medication_administrations():
    """Creates base medication administration records with all joins"""
    
    def add_code_value_lookup(df, cd_column, alias_prefix):
        """Helper function to add code value lookups"""
        code_value = dlt.read("code_value")
        return df.join(
            code_value.alias(f"{alias_prefix}_CV"),
            col(cd_column) == col(f"{alias_prefix}_CV.CODE_VALUE"),
            "left"
        )

    def get_shortest_snomed_codes():
        """Helper function to get shortest SNOMED codes"""
        rxnorm = dlt.read("med_admin_rxnorm")
        return (rxnorm
                .filter(col("SAB") == "SNOMEDCT_US")
                .withColumn("CODE_LENGTH", length(col("CODE")))
                .withColumn("rn", row_number().over(
                    Window.partitionBy("RXCUI").orderBy("CODE_LENGTH")))
                .filter(col("rn") == 1)
                .select("RXCUI", "CODE", "STR")
                .alias("SHORTEST_SNOMED"))

    # Get base tables with aliases
    clinical_event = spark.table("4_prod.raw.mill_clinical_event").alias("CE")
    med_admin_event = spark.table("4_prod.raw.mill_med_admin_event").alias("MAE") \
        .filter((col("EVENT_TYPE_CD").isNotNull()) & (col("EVENT_TYPE_CD") != 0))
    encounter = spark.table("4_prod.raw.mill_encounter").alias("ENC")
    ce_med_result = spark.table("4_prod.raw.mill_ce_med_result").alias("MR")
    orders = spark.table("4_prod.raw.mill_orders").alias("ORDERS")
    order_ingredient = spark.table("4_prod.raw.mill_order_ingredient").alias("OI")
    order_catalog_synonym = spark.table("3_lookup.mill.mill_order_catalog_synonym").alias("OSYN")
    order_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("OCAT")
    rxnorm = dlt.read("med_admin_rxnorm").alias("RXN")

    # Window specs
    ce_window = Window.partitionBy("EVENT_ID").orderBy(desc("VALID_FROM_DT_TM"))

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

    # Main query
    result_df = (
        clinical_event.filter(col("CE.VALID_UNTIL_DT_TM") > current_timestamp())
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
        .join(
            order_ingredient.alias("OI")
            .withColumn(
                "oi_rn",
                F.row_number().over(
                    Window.partitionBy("ORDER_ID", "SYNONYM_ID")
                    .orderBy("ACTION_SEQUENCE")
                )
            )
            .filter(col("oi_rn") == 1),
            (col("ORDERS.TEMPLATE_ORDER_ID") == col("OI.ORDER_ID")) & 
            (col("ORDERS.SYNONYM_ID") == col("OI.SYNONYM_ID")),
            "left"
        )
        .join(order_catalog.alias("OCAT"), 
              col("ORDERS.CATALOG_CD") == col("OCAT.CATALOG_CD"), 
              "left")
        .join(order_catalog_synonym.alias("OSYN"), 
              col("ORDERS.SYNONYM_ID") == col("OSYN.SYNONYM_ID"), 
              "left")
        
        # Add RxNorm and SNOMED lookups
        .join(
            rxnorm.filter(col("SAB") == "MMSL"),
            when(col("OCAT.CKI").like("MUL.ORD%"), 
                 substring_index(col("OCAT.CKI"), "!", -1)) == col("RXN.CODE"),
            "left"
        )
        .join(
            get_shortest_snomed_codes(),
            col("RXN.RXCUI") == col("SHORTEST_SNOMED.RXCUI"),
            "left"
        )
        
        # Add all code value lookups
        .transform(lambda df: add_code_value_lookup(df, "MAE.EVENT_TYPE_CD", "EVENT_TYPE"))
        .transform(lambda df: add_code_value_lookup(df, "MR.ADMIN_ROUTE_CD", "ADMIN_ROUTE"))
        .transform(lambda df: add_code_value_lookup(df, "MR.INFUSION_UNIT_CD", "INFUSION_UNIT"))
        .transform(lambda df: add_code_value_lookup(df, "MR.REFUSAL_CD", "REFUSAL"))
        .transform(lambda df: add_code_value_lookup(df, "MAE.POSITION_CD", "POSITION"))
        .transform(lambda df: add_code_value_lookup(df, "MAE.NURSE_UNIT_CD", "NURSE_UNIT"))
        .transform(lambda df: add_code_value_lookup(df, "OI.STRENGTH_UNIT", "STRENGTH_UNIT"))
        .transform(lambda df: add_code_value_lookup(df, "OI.VOLUME_UNIT", "VOLUME_UNIT"))
        .transform(lambda df: add_code_value_lookup(df, "CE.RESULT_STATUS_CD", "RESULT_STATUS"))
        .transform(lambda df: add_code_value_lookup(df, "MR.DOSAGE_UNIT_CD", "DOSAGE_UNIT"))
        .transform(lambda df: add_code_value_lookup(df, "MR.INFUSED_VOLUME_UNIT_CD", "INFUSED_VOLUME_UNIT"))
        
        .select(
            # Identifiers
            col("CE.PERSON_ID").cast(LongType()).alias("PERSON_ID"),
            col("CE.ENCNTR_ID").cast(LongType()).alias("ENCNTR_ID"),
            col("CE.EVENT_ID").cast(LongType()).alias("EVENT_ID"),
            col("CE.ORDER_ID").cast(LongType()).alias("ORDER_ID"),
            
            # Status codes and their lookups
            col("MAE.EVENT_TYPE_CD").cast(IntegerType()).alias("EVENT_TYPE_CD"),
            F.when(col("EVENT_TYPE_CD").isNull() | (col("EVENT_TYPE_CD") == 0), None)
             .otherwise(coalesce("EVENT_TYPE_CV.DISPLAY", "EVENT_TYPE_CV.CDF_MEANING"))
             .cast(StringType()).alias("EVENT_TYPE_DISPLAY"),
            
            col("CE.RESULT_STATUS_CD").cast(IntegerType()).alias("RESULT_STATUS_CD"),
            F.when(col("RESULT_STATUS_CD").isNull() | (col("RESULT_STATUS_CD") == 0), None)
             .otherwise(coalesce("RESULT_STATUS_CV.DISPLAY", "RESULT_STATUS_CV.CDF_MEANING"))
             .cast(StringType()).alias("RESULT_STATUS_DISPLAY"),
            
            # Timing Information
            coalesce("MAE.BEG_DT_TM", "MR.ADMIN_START_DT_TM", "CE.EVENT_START_DT_TM").cast(TimestampType()).alias("ADMIN_START_DT_TM"),
            coalesce("MAE.END_DT_TM", "MR.ADMIN_END_DT_TM", "CE.EVENT_END_DT_TM").cast(TimestampType()).alias("ADMIN_END_DT_TM"),
            
            # Order Information
            col("OI.SYNONYM_ID").cast(LongType()).alias("ORDER_SYNONYM_ID"),
            col("OCAT.CKI").cast(StringType()).alias("ORDER_CKI"),
            when(col("OCAT.CKI").like("MUL.ORD%"), 
                 substring_index(col("OCAT.CKI"), "!", -1)).cast(StringType()).alias("MULTUM"),
            
            # RxNorm and SNOMED columns
            col("RXN.RXCUI").cast(StringType()).alias("RXNORM_CUI"),
            col("RXN.STR").cast(StringType()).alias("RXNORM_STR"),
            col("SHORTEST_SNOMED.CODE").cast(StringType()).alias("SNOMED_CODE"),
            col("SHORTEST_SNOMED.STR").cast(StringType()).alias("SNOMED_STR"),
            
            col("OSYN.MNEMONIC").cast(StringType()).alias("ORDER_MNEMONIC"),
            col("OI.ORDER_DETAIL_DISPLAY_LINE").cast(StringType()).alias("ORDER_DETAIL"),
            
            # Dosing Information
            col("OI.STRENGTH").cast(FloatType()).alias("ORDER_STRENGTH"),
            col("OI.STRENGTH_UNIT").cast(IntegerType()).alias("ORDER_STRENGTH_UNIT_CD"),
            F.when(col("ORDER_STRENGTH_UNIT_CD").isNull() | (col("ORDER_STRENGTH_UNIT_CD") == 0), None)
             .otherwise(coalesce("STRENGTH_UNIT_CV.DISPLAY", "STRENGTH_UNIT_CV.CDF_MEANING"))
             .cast(StringType()).alias("ORDER_STRENGTH_UNIT_DISPLAY"),
            
            col("OI.VOLUME").cast(FloatType()).alias("ORDER_VOLUME"),
            col("OI.VOLUME_UNIT").cast(IntegerType()).alias("ORDER_VOLUME_UNIT_CD"),
            F.when(col("ORDER_VOLUME_UNIT_CD").isNull() | (col("ORDER_VOLUME_UNIT_CD") == 0), None)
             .otherwise(coalesce("VOLUME_UNIT_CV.DISPLAY", "VOLUME_UNIT_CV.CDF_MEANING"))
             .cast(StringType()).alias("ORDER_VOLUME_UNIT_DISPLAY"),
            
            # Administration Details
            col("MR.ADMIN_ROUTE_CD").cast(IntegerType()).alias("ADMIN_ROUTE_CD"),
            F.when(col("ADMIN_ROUTE_CD").isNull() | (col("ADMIN_ROUTE_CD") == 0), None)
             .otherwise(coalesce("ADMIN_ROUTE_CV.DISPLAY", "ADMIN_ROUTE_CV.CDF_MEANING"))
             .cast(StringType()).alias("ADMIN_ROUTE_DISPLAY"),
            
            col("MR.INITIAL_DOSAGE").cast(FloatType()).alias("INITIAL_DOSAGE"),
            col("MR.DOSAGE_UNIT_CD").cast(IntegerType()).alias("INITIAL_DOSAGE_UNIT_CD"),
            F.when(col("INITIAL_DOSAGE_UNIT_CD").isNull() | (col("INITIAL_DOSAGE_UNIT_CD") == 0), None)
             .otherwise(coalesce("DOSAGE_UNIT_CV.DISPLAY", "DOSAGE_UNIT_CV.CDF_MEANING"))
             .cast(StringType()).alias("INITIAL_DOSAGE_UNIT_DISPLAY"),
            
            col("MR.ADMIN_DOSAGE").cast(FloatType()).alias("ADMIN_DOSAGE"),
            col("MR.DOSAGE_UNIT_CD").cast(IntegerType()).alias("ADMIN_DOSAGE_UNIT_CD"),
            F.when(col("ADMIN_DOSAGE_UNIT_CD").isNull() | (col("ADMIN_DOSAGE_UNIT_CD") == 0), None)
             .otherwise(coalesce("DOSAGE_UNIT_CV.DISPLAY", "DOSAGE_UNIT_CV.CDF_MEANING"))
             .cast(StringType()).alias("ADMIN_DOSAGE_UNIT_DISPLAY"),
            
            # Volume Information
            col("MR.INITIAL_VOLUME").cast(FloatType()).alias("INITIAL_VOLUME"),
            col("MR.INFUSED_VOLUME").cast(FloatType()).alias("INFUSED_VOLUME"),
            col("MR.INFUSED_VOLUME_UNIT_CD").cast(IntegerType()).alias("INFUSED_VOLUME_UNIT_CD"),
            F.when(col("INFUSED_VOLUME_UNIT_CD").isNull() | (col("INFUSED_VOLUME_UNIT_CD") == 0), None)
             .otherwise(coalesce("INFUSED_VOLUME_UNIT_CV.DISPLAY", "INFUSED_VOLUME_UNIT_CV.CDF_MEANING"))
             .cast(StringType()).alias("INFUSED_VOLUME_UNIT_DISPLAY"),
            
           # Infusion Details
            col("MR.INFUSION_RATE").cast(FloatType()).alias("INFUSION_RATE"),
            col("MR.INFUSION_UNIT_CD").cast(IntegerType()).alias("INFUSION_UNIT_CD"),
            F.when(col("INFUSION_UNIT_CD").isNull() | (col("INFUSION_UNIT_CD") == 0), None)
             .otherwise(coalesce("INFUSION_UNIT_CV.DISPLAY", "INFUSION_UNIT_CV.CDF_MEANING"))
             .cast(StringType()).alias("INFUSION_UNIT_DISPLAY"),

            col("MAE.NURSE_UNIT_CD").cast(IntegerType()).alias("NURSE_UNIT_CD"),
            F.when(col("NURSE_UNIT_CD").isNull() | (col("NURSE_UNIT_CD") == 0), None)
             .otherwise(coalesce("NURSE_UNIT_CV.DISPLAY", "NURSE_UNIT_CV.CDF_MEANING"))
             .cast(StringType()).alias("NURSE_UNIT_DISPLAY"),

            # Provider Information
            col("MAE.POSITION_CD").cast(IntegerType()).alias("POSITION_CD"),
            F.when(col("POSITION_CD").isNull() | (col("POSITION_CD") == 0), None)
             .otherwise(coalesce("POSITION_CV.DISPLAY", "POSITION_CV.CDF_MEANING"))
             .cast(StringType()).alias("POSITION_DISPLAY"),
            
            col("MAE.PRSNL_ID").cast(LongType()).alias("PRSNL_ID"),
            
            # Update Tracking
            F.greatest(
                col("CE.ADC_UPDT"),
                col("MAE.ADC_UPDT"),
                col("MR.ADC_UPDT"),
                col("OI.ADC_UPDT")
            ).alias("ADC_UPDT")
        )
    )
    
    # Add standardized columns and return
    return add_standardized_columns(result_df, standardization_cases)

@dlt.table(
    name="med_admin_with_snomed",
    comment="Medication administrations with augmented SNOMED codes",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def augment_med_admin_snomed():
    """Augments medication administrations with SNOMED codes"""
    base_med_admin = dlt.read("base_medication_administrations")
    return augment_snomed_codes(base_med_admin)

@dlt.table(
    name="map_med_admin",
    comment="Final medication administration mappings with OMOP concepts",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def create_final_med_admin_mappings():
    """Creates final medication administration mappings with OMOP concepts"""
    med_admin_with_snomed = dlt.read("med_admin_with_snomed")
    return add_omop_mappings(med_admin_with_snomed).distinct()

# COMMAND ----------



def create_procedure_code_lookup(code_values, alias_name, desc_alias):
    """
    Helper function to create standardized code value lookups for procedures
    
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

# COMMAND ----------

@dlt.table(
    name="procedure_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def procedure_mapping_incr():
    """
    Creates an incremental procedure mapping table processing only new/modified records.
    
    Process:
    1. Filters active procedures with valid dates
    2. Joins with encounters for person IDs
    3. Enriches with nomenclature data
    4. Adds code value descriptions
    5. Standardizes data types
    
    Returns:
        DataFrame: Processed procedure records with standardized format
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_procedure")
    
    # Get base tables
    procedures = spark.table("4_prod.raw.mill_procedure").filter(
        col("ADC_UPDT") > max_adc_updt
    ).alias("proc")
    
    
    encounters = spark.table("4_prod.raw.mill_encounter")
    code_values = dlt.read("code_value")
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
    
    # Return final selection
    return processed_procedures.select(
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

@dlt.view(name="procedure_mapping_update")
def procedure_mapping_update():
    """Creates a streaming view of the incremental procedure mappings."""
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.procedure_mapping_incr")
    )

# Define the final target table
dlt.create_target_table(
    name = "map_procedure",
    comment = "Standardized procedure mappings with nomenclature",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PROCEDURE_ID,ENCNTR_ID,PERSON_ID"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "map_procedure",
    source = "procedure_mapping_update",
    keys = ["PROCEDURE_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------



def get_last_clinical_events_for_death():
    """Helper function to get last clinical event dates for each person"""
    return (
        spark.table("4_prod.raw.mill_clinical_event")
        .groupBy("PERSON_ID")
        .agg(F.max("CLINSIG_UPDT_DT_TM").alias("LAST_CE_DT_TM"))
    )

def create_death_code_lookups(code_values):
    """Helper function to create source and method code lookups for death records"""
    source_lookup = code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias("deceased_source_desc")
    ).alias("source")
    
    method_lookup = code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias("deceased_method_desc")
    ).alias("method")
    
    return source_lookup, method_lookup

# COMMAND ----------

@dlt.table(
    name="death_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def death_mapping_incr():
    """
    Creates an incremental death records mapping table.
    
    Process:
    1. Identifies deceased persons
    2. Joins with code value lookups for source and method
    3. Adds last clinical event dates
    4. Calculates death date using multiple sources
    5. Standardizes output format
    
    Returns:
        DataFrame: Processed death records with standardized format
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_death")
    
    # Get base person data
    base_persons = (
        spark.table("4_prod.raw.mill_person")
        .filter(
            (col("DECEASED_CD") == 3768549) &
            (col("ADC_UPDT") > max_adc_updt)
        )
        .alias("person")
    )
    
    
    # Get reference data
    code_values = dlt.read("code_value")
    last_ce_dates = get_last_clinical_events_for_death()
    source_lookup, method_lookup = create_death_code_lookups(code_values)
    
    return (
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

@dlt.view(name="death_mapping_update")
def death_mapping_update():
    """Creates a streaming view of the incremental death mappings"""
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.death_mapping_incr")
    )

# Define the final target table
dlt.create_target_table(
    name = "map_death",
    comment = "Standardized death records with source tracking",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID,CALC_DEATH_DATE"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "map_death",
    source = "death_mapping_update",
    keys = ["PERSON_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(
    name="barts_manual_mappings",
    comment="BARTS manual mappings reference table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def lookup_barts_mappings():
    """Creates a reference table for BARTS manual mappings."""
    return spark.table("3_lookup.omop.barts_new_maps")


@dlt.table(
    name="mill_order_catalog",
    comment="Mill order catalog reference table",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def lookup_mill_catalog():
    """Creates a reference table for Mill order catalog."""
    return spark.table("3_lookup.mill.mill_order_catalog")

def create_numeric_events_code_lookup(code_values, alias_suffix):
    """
    Helper function to create code value lookups for numeric events.
    
    Args:
        code_values: DataFrame containing code values
        alias_suffix: Suffix for column aliases
        
    Returns:
        DataFrame: Formatted code lookup table
    """
    return code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(f"{alias_suffix}_desc")
    ).alias(alias_suffix)

def add_numeric_events_omop_mappings(df, barts_mappings, omop_concepts):
    """
    Helper function to add OMOP manual mappings to numeric events.
    
    Args:
        df: Source DataFrame
        barts_mappings: BARTS mapping reference table
        omop_concepts: OMOP concepts reference table
        
    Returns:
        DataFrame: Source data enriched with OMOP mappings
    """
    # Filter for concept mappings
    concept_maps = (
        barts_mappings
        .filter(
            (col("SourceTable") == "dbo.PI_CDE_CLINICAL_EVENT") &
            (col("SourceField") == "EVENT_CD") &
            col("OMOPField").isin("measurement_concept_id", "observation_concept_id")
        )
        .select(
            col("SourceValue").cast(IntegerType()),
            col("OMOPTable").alias("OMOP_MANUAL_TABLE"),
            col("OMOPField").alias("OMOP_MANUAL_COLUMN"),
            col("OMOPConceptId").alias("OMOP_MANUAL_CONCEPT"),
            col("EVENT_CD").alias("MAP_EVENT_CD"),
            col("EVENT_CLASS_CD").alias("MAP_EVENT_CLASS_CD"),
            col("EVENT_RESULT_TXT").alias("MAP_EVENT_RESULT_TXT"),
            col("STANDARD_CONCEPT")
        )
    )
    
    # Filter for unit mappings
    unit_maps = (
        barts_mappings
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
        omop_concepts
        .select(
            col("concept_id"),
            col("concept_name").alias("OMOP_MANUAL_CONCEPT_NAME"),
            col("standard_concept").alias("OMOP_MANUAL_STANDARD_CONCEPT"),
            col("domain_id").alias("OMOP_MANUAL_CONCEPT_DOMAIN"),
            col("concept_class_id").alias("OMOP_MANUAL_CONCEPT_CLASS")
        )
    )
    
    # Domain priority mapping
    domain_priority = {
        "Drug": 1,
        "Measurement": 2,
        "Procedure": 3,
        "Condition": 4,
        "Device": 5,
        "Observation": 6
    }
    
    domain_priority_udf = udf(lambda x: domain_priority.get(x, 999), IntegerType())
    
    return (
        df
        .join(
            concept_maps,
            (df.EVENT_CD == concept_maps.SourceValue) &
            (
                (concept_maps.MAP_EVENT_CD.isNull() | (df.EVENT_CD == concept_maps.MAP_EVENT_CD)) &
                (concept_maps.MAP_EVENT_CLASS_CD.isNull() | (df.EVENT_CLASS_CD == concept_maps.MAP_EVENT_CLASS_CD)) &
                (concept_maps.MAP_EVENT_RESULT_TXT.isNull() | (df.EVENT_TITLE_TEXT == concept_maps.MAP_EVENT_RESULT_TXT))
            ),
            "left"
        )
        .join(
            unit_maps,
            df.UNIT_OF_MEASURE_CD == unit_maps.SourceValue,
            "left"
        )
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
            "STANDARD_CONCEPT",
            "domain_priority",
            "row_num"
        )
    )

# COMMAND ----------

@dlt.table(
    name="numeric_events_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def numeric_events_mapping_incr():
    """
    Creates an incremental numeric events mapping table.
    Processes only new or modified records.
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_numeric_events")
    
    # Get base tables with filtering
    string_results = spark.table("4_prod.raw.mill_ce_string_result")\
        .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())\
        .filter(col("ADC_UPDT") > max_adc_updt)\
        .alias("sr")
    
    
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
    code_values = dlt.read("code_value")
    order_catalog = dlt.read("mill_order_catalog").alias("oc")
    parent_catalog = dlt.read("mill_order_catalog").alias("poc")
    
    # Create code lookups
    unit_lookup = create_numeric_events_code_lookup(code_values, "unit")
    event_cd_lookup = create_numeric_events_code_lookup(code_values, "event")
    parent_event_cd_lookup = create_numeric_events_code_lookup(code_values, "parent_event")
    normalcy_lookup = create_numeric_events_code_lookup(code_values, "normalcy")
    contrib_sys_lookup = create_numeric_events_code_lookup(code_values, "contrib")
    entry_mode_lookup = create_numeric_events_code_lookup(code_values, "entry")
    catalog_type_lookup = create_numeric_events_code_lookup(code_values, "cat_type")
    parent_catalog_type_lookup = create_numeric_events_code_lookup(code_values, "parent_cat_type")
    
    # Process numeric results
    numeric_results = (
        string_results
        .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
        .select(
            "EVENT_ID",
            "UNIT_OF_MEASURE_CD",
            when(
                col("STRING_RESULT_TEXT").cast("double").isNotNull(),
                col("STRING_RESULT_TEXT").cast("double")
            ).alias("NUMERIC_RESULT")
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
        .join(contrib_sys_lookup, col("CONTRIBUTOR_SYSTEM_CD") == col("contrib.CODE_VALUE"), "left")
        .join(entry_mode_lookup, col("ENTRY_MODE_CD") == col("entry.CODE_VALUE"), "left")
        .join(catalog_type_lookup, col("oc.CATALOG_TYPE_CD") == col("cat_type.CODE_VALUE"), "left")
        .join(parent_catalog_type_lookup, col("poc.CATALOG_TYPE_CD") == col("parent_cat_type.CODE_VALUE"), "left")
    )
    result_df = result_df.select(
        # IDs
        col("ce.EVENT_ID").cast(LongType()),
        col("ce.ENCNTR_ID").cast(LongType()),
        col("ce.PERSON_ID").cast(LongType()),
        col("ce.ORDER_ID").cast(LongType()),
        col("ce.EVENT_CLASS_CD").cast(IntegerType()),
        col("ce.PERFORMED_PRSNL_ID").cast(LongType()),
        
        # Result value and unit
        col("NUMERIC_RESULT").cast(DoubleType()),
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
        col("ce.NORMAL_LOW").cast(DoubleType()),
        col("ce.NORMAL_HIGH").cast(DoubleType()),
        
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
    return add_numeric_events_omop_mappings(
        result_df,
        dlt.read("barts_manual_mappings"),
        dlt.read("omop_concept")
    )

@dlt.view(name="numeric_events_update")
def numeric_events_update():
    """Creates a streaming view of the incremental numeric events mappings."""
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.numeric_events_mapping_incr")
    )

# Define the final target table
dlt.create_target_table(
    name = "map_numeric_events",
    comment = "Numeric events mappings with OMOP concepts",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "EVENT_ID,PERSON_ID,ENCNTR_ID"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "map_numeric_events",
    source = "numeric_events_update",
    keys = ["EVENT_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)


# COMMAND ----------





def create_date_event_code_lookup(code_values_df, alias_suffix):
    """
    Creates code value lookup DataFrame with standardized aliases for date events.
    
    Args:
        code_values_df: Source code values DataFrame
        alias_suffix: Suffix to append to description column alias
        
    Returns:
        DataFrame: Lookup table with CODE_VALUE and aliased description
    """
    return code_values_df.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(f"{alias_suffix}_desc")
    ).alias(alias_suffix)

def get_latest_clinical_events(events_df, valid_until_timestamp=None):
    """
    Gets latest version of clinical events by ranking on UPDT_CNT.
    
    Args:
        events_df: Clinical events DataFrame
        valid_until_timestamp: Optional timestamp to filter valid records
        
    Returns:
        DataFrame: Latest version of each clinical event
    """
    base_df = events_df
    if valid_until_timestamp is not None:
        base_df = base_df.filter(col("VALID_UNTIL_DT_TM") > valid_until_timestamp)
        
    return (
        base_df
        .withColumn(
            "row_rank",
            row_number().over(
                Window.partitionBy("EVENT_ID")
                .orderBy(col("UPDT_CNT").desc())
            )
        )
        .filter(col("row_rank") == 1)
        .drop("row_rank")
    )


# COMMAND ----------

@dlt.table(
    name="date_event_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def date_event_mapping_incr():
    """
    Creates incremental date event mapping table processing only new/modified records.
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_date_events")
    current_ts = current_timestamp()
    
    # Get base date results
    date_results = (
        spark.table("4_prod.raw.mill_ce_date_result")
        .filter(
            (col("VALID_UNTIL_DT_TM") > current_ts) &
            (col("ADC_UPDT") > max_adc_updt)
        )
        .alias("dr")
    )
    
    
    # Get clinical events with latest versions
    clinical_events = get_latest_clinical_events(
        spark.table("4_prod.raw.mill_clinical_event")
    ).alias("ce")
    
    parent_events = get_latest_clinical_events(
        spark.table("4_prod.raw.mill_clinical_event"),
        current_ts
    ).alias("pe")
    
    # Get reference tables
    code_values = dlt.read("code_value")
    order_catalog = dlt.read("mill_order_catalog").alias("oc")
    parent_catalog = dlt.read("mill_order_catalog").alias("poc")
    
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
    return (
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

@dlt.view(name="date_event_mapping_update")
def date_event_mapping_update():
    """Creates streaming view of incremental date event mappings."""
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.date_event_mapping_incr")
    )

# Define final target table
dlt.create_target_table(
    name = "map_date_events",
    comment = "Date event mappings with reference data",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "EVENT_ID,PERSON_ID,ENCNTR_ID"
    }
)

# Apply changes to target table
dlt.apply_changes(
    target = "map_date_events",
    source = "date_event_mapping_update", 
    keys = ["EVENT_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------





def text_events_create_code_lookup(code_values, alias_suffix):
    """
    Helper function to create code value lookups specifically for text events processing.
    
    Args:
        code_values: Base code values DataFrame
        alias_suffix: Suffix for column aliases
    
    Returns:
        DataFrame: Formatted lookup table
    """
    return code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(f"{alias_suffix}_desc")
    ).alias(alias_suffix)

def text_events_add_omop_manual_mappings(df, barts_mapfile, concepts):
    """
    Helper function to add OMOP manual mappings to text events.
    
    Args:
        df: Base DataFrame with text events
        barts_mapfile: BARTS manual mappings reference
        concepts: OMOP concepts reference
    
    Returns:
        DataFrame: Enhanced with OMOP mappings
    """
    # Filter for concept mappings
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
    
    # Filter for value concept mappings
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
    
    return (
        df
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

# COMMAND ----------

@dlt.table(
    name="text_events_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def text_events_mapping_incr():
    """
    Creates an incremental text events mapping table.
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_text_events")
    
    # Get reference tables
    string_results = spark.table("4_prod.raw.mill_ce_string_result")\
        .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())\
        .filter(col("ADC_UPDT") > max_adc_updt)\
        .alias("sr")
    
    
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
    
    code_values = dlt.read("code_value")
    order_catalog = dlt.read("mill_order_catalog").alias("oc")
    parent_catalog = dlt.read("mill_order_catalog").alias("poc")
    
    # Create code lookups
    unit_lookup = text_events_create_code_lookup(code_values, "unit")
    event_cd_lookup = text_events_create_code_lookup(code_values, "event")
    parent_event_cd_lookup = text_events_create_code_lookup(code_values, "parent_event")
    normalcy_lookup = text_events_create_code_lookup(code_values, "normalcy")
    contrib_sys_lookup = text_events_create_code_lookup(code_values, "contrib")
    entry_mode_lookup = text_events_create_code_lookup(code_values, "entry")
    catalog_type_lookup = text_events_create_code_lookup(code_values, "cat_type")
    parent_catalog_type_lookup = text_events_create_code_lookup(code_values, "parent_cat_type")
    
    # Get valid text results
    text_results = (
        string_results
        .filter(col("STRING_RESULT_TEXT").cast("double").isNull())
        .select(
            "EVENT_ID",
            "UNIT_OF_MEASURE_CD",
            col("STRING_RESULT_TEXT").alias("TEXT_RESULT")
        )
    )
    
    # Build base result set
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
    return text_events_add_omop_manual_mappings(
        result_df,
        dlt.read("barts_manual_mappings"),
        dlt.read("omop_concept")
    )

@dlt.view(name="text_events_mapping_update")
def text_events_mapping_update():
    """Creates a streaming view of the incremental text events mappings."""
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.text_events_mapping_incr")
    )

# Define the final target table
dlt.create_target_table(
    name = "map_text_events",
    comment = "Text events with OMOP mappings",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "EVENT_ID,PERSON_ID,ENCNTR_ID"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "map_text_events",
    source = "text_events_mapping_update",
    keys = ["EVENT_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------



def nomen_create_code_lookup_specific(code_values, alias_suffix):
    """
    Creates a code value lookup specific to nomenclature mapping.
    
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

def nomen_add_manual_omop_mappings_specific(df, barts_mapfile, concepts):
    """
    Adds manual OMOP mappings specific to nomenclature results.
    
    Args:
        df: Source DataFrame
        barts_mapfile: Manual mapping reference table
        concepts: OMOP concepts reference table
        
    Returns:
        DataFrame: Enhanced with manual OMOP mappings
    """
    # Domain priority dictionary specific to nomenclature mapping
    nomen_domain_priority = {
        "Drug": 1,
        "Measurement": 2,
        "Procedure": 3,
        "Condition": 4,
        "Device": 5,
        "Observation": 6
    }
    
    nomen_domain_priority_udf = udf(lambda x: nomen_domain_priority.get(x, 999), IntegerType())
    
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
            nomen_domain_priority_udf(col("OMOP_MANUAL_CONCEPT_DOMAIN"))
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

# COMMAND ----------

@dlt.table(
    name="nomen_results_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def nomen_results_mapping_incr():
    """
    Creates an incremental nomenclature results mapping table.
    Processes only new or modified records since last update.
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_nomen_events")
    
    # Get base tables with filtering for new/modified records
    coded_results = (
        spark.table("4_prod.raw.mill_ce_coded_result")
        .filter(
            (col("VALID_UNTIL_DT_TM") > current_timestamp()) &
            (col("ADC_UPDT") > max_adc_updt)
        )
        .alias("cr")
    )

    
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
    code_values = dlt.read("code_value")
    order_catalog = dlt.read("mill_order_catalog").alias("oc")
    parent_catalog = dlt.read("mill_order_catalog").alias("poc")
    nomenclature = spark.table("4_prod.bronze.nomenclature").alias("nom")
    barts_mapfile = dlt.read("barts_manual_mappings")
    omop_concepts = dlt.read("omop_concept").filter(col("invalid_reason").isNull())
    
    # Create code value lookups
    event_cd_lookup = nomen_create_code_lookup_specific(code_values, "event")
    parent_event_cd_lookup = nomen_create_code_lookup_specific(code_values, "parent_event")
    normalcy_lookup = nomen_create_code_lookup_specific(code_values, "normalcy")
    contrib_sys_lookup = nomen_create_code_lookup_specific(code_values, "contrib")
    entry_mode_lookup = nomen_create_code_lookup_specific(code_values, "entry")
    catalog_type_lookup = nomen_create_code_lookup_specific(code_values, "cat_type")
    parent_catalog_type_lookup = nomen_create_code_lookup_specific(code_values, "parent_cat_type")
    source_vocab_lookup = nomen_create_code_lookup_specific(code_values, "vocab")
    vocab_axis_lookup = nomen_create_code_lookup_specific(code_values, "axis")
    
    # Get valid nomenclature results
    nomen_results = coded_results.filter(
        (col("NOMENCLATURE_ID").isNotNull()) & 
        (col("NOMENCLATURE_ID") != 0) &
        (col("VALID_UNTIL_DT_TM") > current_timestamp())
    )


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
        # Join with order catalog for main event
        .join(
            order_catalog.select(
                "CATALOG_CD",
                "CATALOG_TYPE_CD",
                "DESCRIPTION"
            ),
            col("ce.CATALOG_CD") == col("oc.CATALOG_CD"),
            "left"
        )
        # Join with order catalog for parent event
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
            nomenclature,
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
        
        # Select final columns
        .select(
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
    )


    # Add manual OMOP mappings
    final_df = nomen_add_manual_omop_mappings_specific(
        result_df,
        barts_mapfile,
        omop_concepts
    )
    
    return final_df

@dlt.view(name="nomen_results_update")
def nomen_results_update():
    """Creates a streaming view of the incremental nomenclature results mappings."""
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.nomen_results_mapping_incr")
    )

# Define the final target table
dlt.create_target_table(
    name = "map_nomen_events",
    comment = "Mapped nomenclature events with OMOP and SNOMED concepts",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "EVENT_ID,NOMENCLATURE_ID",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "map_nomen_events",
    source = "nomen_results_update",
    keys = ["EVENT_ID", "NOMENCLATURE_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------



def coded_add_manual_omop_mappings_specific(df, barts_mapfile, concepts):
    """
    Adds manual OMOP mappings specific to coded results.
    
    Args:
        df: Source DataFrame with coded results
        barts_mapfile: Manual mapping reference table
        concepts: OMOP concepts reference table
        
    Returns:
        DataFrame: Enhanced with manual OMOP mappings
    """
    # Domain priority dictionary specific to coded results
    coded_domain_priority = {
        "Drug": 1,
        "Measurement": 2,
        "Procedure": 3,
        "Condition": 4,
        "Device": 5,
        "Observation": 6
    }
    
    coded_domain_priority_udf = udf(lambda x: coded_domain_priority.get(x, 999), IntegerType())
    
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
    
    # Join everything together and apply the mapping logic with ranking
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
            coded_domain_priority_udf(col("OMOP_MANUAL_CONCEPT_DOMAIN"))
        )
        .withColumn(
            "row_num",
            row_number().over(
                Window.partitionBy("EVENT_ID")
                .orderBy(
                    # First prioritize non-null standard concepts
                    col("OMOP_MANUAL_STANDARD_CONCEPT").desc_nulls_last(),
                    # Then by domain priority
                    col("domain_priority").asc_nulls_last(),
                    # Finally, take the first one if still multiple matches
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

def coded_create_code_lookup_specific(code_values, alias_suffix):
    """
    Creates a code value lookup specific to coded results mapping.
    
    Args:
        code_values: DataFrame containing code values
        alias_suffix: Suffix for the column aliases
        
    Returns:
        DataFrame: Lookup table with standardized columns
    """
    return code_values.select(
        col("CODE_VALUE"),
        col("DESCRIPTION").alias(f"{alias_suffix}_desc"),
        col("DISPLAY").alias(f"{alias_suffix}_display"),
        col("CDF_MEANING").alias(f"{alias_suffix}_meaning")
    ).alias(alias_suffix)

# COMMAND ----------

@dlt.table(
    name="coded_results_mapping_incr",
    temporary=True,
    table_properties={"skipChangeCommits": "true"}
)
def coded_results_mapping_incr():
    """
    Creates an incremental coded results mapping table.
    Processes only new or modified records since last update.
    """
    # Get timestamp of last processed record
    max_adc_updt = get_max_timestamp("4_prod.dlt.map_coded_events")


    # Get base tables with filtering for new/modified records
    coded_results = (
        spark.table("4_prod.raw.mill_ce_coded_result")
        .filter(
            (col("VALID_UNTIL_DT_TM") > current_timestamp()) &
            (col("ADC_UPDT") > max_adc_updt)
        )
        .alias("cr")
    )
    
    
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
    code_values = dlt.read("code_value")
    order_catalog = dlt.read("mill_order_catalog").alias("oc")
    parent_catalog = dlt.read("mill_order_catalog").alias("poc")
    barts_mapfile = dlt.read("barts_manual_mappings")
    omop_concepts = dlt.read("omop_concept").filter(col("invalid_reason").isNull())
    
    # Create code value lookups
    event_cd_lookup = coded_create_code_lookup_specific(code_values, "event")
    parent_event_cd_lookup = coded_create_code_lookup_specific(code_values, "parent_event")
    normalcy_lookup = coded_create_code_lookup_specific(code_values, "normalcy")
    contrib_sys_lookup = coded_create_code_lookup_specific(code_values, "contrib")
    entry_mode_lookup = coded_create_code_lookup_specific(code_values, "entry")
    catalog_type_lookup = coded_create_code_lookup_specific(code_values, "cat_type")
    parent_catalog_type_lookup = coded_create_code_lookup_specific(code_values, "parent_cat_type")
    result_cd_lookup = coded_create_code_lookup_specific(code_values, "result")
    
    # Get valid coded results
    coded_result_rows = coded_results.filter(
        (col("RESULT_CD").isNotNull()) & 
        (col("RESULT_CD") != 0) &
        (col("VALID_UNTIL_DT_TM") > current_timestamp())
    )


    # Build the main result DataFrame with all joins
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
        # Join with order catalog for main event
        .join(
            order_catalog.select(
                "CATALOG_CD",
                "CATALOG_TYPE_CD",
                "DESCRIPTION"
            ),
            col("ce.CATALOG_CD") == col("oc.CATALOG_CD"),
            "left"
        )
        # Join with order catalog for parent event
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
        .select(
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
    )
    
    # Add manual OMOP mappings
    return coded_add_manual_omop_mappings_specific(
        result_df,
        barts_mapfile,
        omop_concepts
    )

@dlt.view(name="coded_results_update")
def coded_results_update():
    """Creates a streaming view of the incremental coded results mappings."""
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.coded_results_mapping_incr")
    )

# Define the final target table
dlt.create_target_table(
    name = "map_coded_events",
    comment = "Mapped coded events with OMOP concepts",
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "EVENT_ID,RESULT_CD",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

# Apply changes to the target table
dlt.apply_changes(
    target = "map_coded_events",
    source = "coded_results_update",
    keys = ["EVENT_ID", "RESULT_CD"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------


