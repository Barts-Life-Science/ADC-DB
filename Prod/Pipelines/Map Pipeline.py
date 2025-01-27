# Databricks notebook source
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
    Falls back to ADC_UPDT if _row_modified doesn't exist.
    If no date is found, returns January 1, 1980 as a default date.
    
    Args:
        table_name (str): Name of the table to query
    
    Returns:
        datetime: Maximum timestamp or default date
    """
    try:
        default_date = datetime(1980, 1, 1)
        
        if not table_exists(table_name):
            return default_date
            
        # Try to get ADC_UPDT directly
        result = spark.sql(f"SELECT MAX(ADC_UPDT) AS max_date FROM {table_name}")
        max_date = result.select(max("max_date").alias("max_date")).first()["max_date"]
        
        return max_date if max_date is not None else default_date
        
    except Exception as e:
        print(f"Error getting max timestamp from {table_name}: {str(e)}")
        return default_date
    


def table_exists(table_name):
    try:
        result = spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
        return result.first() is not None
    except:
        return False

# COMMAND ----------

def update_table(source_df, target_table, index_column):
    """
    Generic function to update a Delta table using SQL MERGE
    
    Args:
        source_df (DataFrame): Source DataFrame with new/updated records
        target_table (str): Target table name to update (fully qualified)
        index_column (str): Name of the column to use as merge key
    """
    if(source_df.count() == 0):
        return
    if table_exists(target_table):
        # Create temporary view of source data
        temp_view_name = f"temp_source_{target_table.replace('.', '_')}"
        source_df.createOrReplaceTempView(temp_view_name)
        
        # Construct and execute MERGE statement
        merge_sql = f"""
        MERGE INTO {target_table} as target
        USING {temp_view_name} as source
        ON target.{index_column} = source.{index_column}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        try:
            spark.sql(merge_sql)
        finally:
            # Clean up temporary view
            spark.sql(f"DROP VIEW IF EXISTS {temp_view_name}")
    else:
        # If table doesn't exist, create it from source
        source_df.write.format("delta") \
            .option("delta.enableChangeDataFeed", "true") \
            .option("delta.enableRowTracking", "true") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .mode("overwrite") \
            .saveAsTable(target_table)

# COMMAND ----------

def create_address_mapping_incr():
    """
    Creates an incremental address mapping table that processes only new or modified records.
    
    Returns:
        DataFrame: Processed address records with standardized format
    """

    max_adc_updt = get_max_timestamp("4_prod.bronze.map_address")
    
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
    country_lookup = spark.table("3_lookup.mill.mill_code_value")
    
    processed_addresses = (
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
    
    return processed_addresses


# COMMAND ----------


updates_df = create_address_mapping_incr()
    

update_table(updates_df, "4_prod.bronze.map_address", "ADDRESS_ID")

# COMMAND ----------

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
        .filter(col("PARENT_ENTITY_NAME") == "PERSON")
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


# COMMAND ----------

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
    address_lookup = spark.table("4_prod.bronze.map_address").alias("al")
    
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
            address_lookup.filter(col("PARENT_ENTITY_NAME") == "ORGANIZATION")
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


# COMMAND ----------

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


# COMMAND ----------

# Helper function to get event time boundaries for encounters
def get_event_times():
    """
    Calculates earliest and latest clinical event times for each encounter,
    excluding dates before 1950 and future dates.
    
    Returns:
        DataFrame: Event time boundaries with encounter ID
    """
    from pyspark.sql.functions import current_timestamp, year

    return (
        spark.table("4_prod.raw.mill_clinical_event")
        .where(
            (year("CLINSIG_UPDT_DT_TM") >= 1950) &
            ("CLINSIG_UPDT_DT_TM" <= current_timestamp())
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


# COMMAND ----------

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


# COMMAND ----------

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



# COMMAND ----------

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
    """Creates base medication administration records with all joins"""
    

    max_adc_updt = get_max_timestamp("4_prod.bronze.map_med_admin")
    
    def add_code_value_lookup(df, cd_column, alias_prefix):
        """Helper function to add code value lookups"""
        code_value = spark.table("3_lookup.mill.mill_code_value")
        return df.join(
            code_value.alias(f"{alias_prefix}_CV"),
            col(cd_column) == col(f"{alias_prefix}_CV.CODE_VALUE"),
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
                .alias("SHORTEST_SNOMED"))

    # Get base tables with aliases and filtering
    clinical_event = spark.table("4_prod.raw.mill_clinical_event").alias("CE")
    med_admin_event = spark.table("4_prod.raw.mill_med_admin_event").alias("MAE") \
        .filter((col("EVENT_TYPE_CD").isNotNull()) & (col("EVENT_TYPE_CD") != 0))
    encounter = spark.table("4_prod.raw.mill_encounter").alias("ENC")
    ce_med_result = spark.table("4_prod.raw.mill_ce_med_result").alias("MR")
    orders = spark.table("4_prod.raw.mill_orders").alias("ORDERS")
    order_ingredient = spark.table("4_prod.raw.mill_order_ingredient").alias("OI")
    order_catalog_synonym = spark.table("3_lookup.mill.mill_order_catalog_synonym").alias("OSYN")
    order_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("OCAT")
    rxnorm = spark.table("3_lookup.rxnorm.rxnconso").alias("RXN")

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



    result_df = (
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
        
        # RxNorm and SNOMED lookups through Multum
        .join(
            rxnorm.filter(col("SAB") == "MMSL"),  # Get Multum mappings from RxNorm
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
            col("CE.PERSON_ID").cast(LongType()),
            col("CE.ENCNTR_ID").cast(LongType()),
            col("CE.EVENT_ID").cast(LongType()),
            col("CE.ORDER_ID").cast(LongType()),
            
            # Status codes and their lookups
            col("MAE.EVENT_TYPE_CD").cast(IntegerType()),
            F.when(col("EVENT_TYPE_CD").isNull() | (col("EVENT_TYPE_CD") == 0), None)
             .otherwise(coalesce("EVENT_TYPE_CV.DISPLAY", "EVENT_TYPE_CV.CDF_MEANING"))
             .cast(StringType()).alias("EVENT_TYPE_DISPLAY"),
            
            col("CE.RESULT_STATUS_CD").cast(IntegerType()),
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
            col("MR.ADMIN_ROUTE_CD").cast(IntegerType()),
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
        ).distinct()
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

    concepts = spark.table("3_lookup.omop.concept")
    
    # Create separate concept DataFrames for each vocabulary
    # Add ranking within each vocabulary based on standard_concept
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
            col("concept_code").alias("multum_code"),
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
    ).distinct()
    
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

def backfill_snomed_from_omop(df):
    """
    Attempts to find SNOMED codes for records that have OMOP concepts but no SNOMED codes
    by looking up mappings from the OMOP concept to SNOMED.
    """
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
    
    if needs_backfill.count() > 0:
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
            
            # Add SNOMED codes
            with_snomed = augment_snomed_codes(base_df, refs)
            print("Added SNOMED code mappings")
            
            # Add OMOP mappings
            with_omop = add_omop_mappings(with_snomed)
            print("Added OMOP concept mappings")


            # Backfill missing SNOMED codes from OMOP mappings
            with_backfill = backfill_snomed_from_omop(with_omop)
            print("Backfilled missing SNOMED codes from OMOP mappings")

            # Deduplicate records prioritizing status 25, Authorized
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
            

            update_table(final_df.distinct(), "4_prod.bronze.map_med_admin", "EVENT_ID")
            print("Successfully updated medication administration mapping table")
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing medication administration updates: {str(e)}")
        raise

process_med_admin_incremental()

# COMMAND ----------

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
            print("Successfully updated procedure mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing procedure updates: {str(e)}")
        raise


process_procedure_incremental()

# COMMAND ----------

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
            print("Successfully updated death mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing death record updates: {str(e)}")
        raise


process_death_incremental()

# COMMAND ----------

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
                col("ce.NORMAL_LOW").cast(FloatType()),
                col("ce.NORMAL_HIGH").cast(FloatType()),
                
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
            print("Successfully updated numeric events mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing numeric events updates: {str(e)}")
        raise


process_numeric_events_incremental()

# COMMAND ----------

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
            print("Successfully updated date events mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing date events updates: {str(e)}")
        raise


process_date_events_incremental()

# COMMAND ----------

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
                .filter(col("STRING_RESULT_TEXT").cast("double").isNull())
                .select(
                    "EVENT_ID",
                    "UNIT_OF_MEASURE_CD",
                    col("STRING_RESULT_TEXT").alias("TEXT_RESULT")
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
            print("Successfully updated text events mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing text events updates: {str(e)}")
        raise


process_text_events_incremental()

# COMMAND ----------

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
            print("Successfully updated nomenclature events mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing nomenclature events updates: {str(e)}")
        raise


process_nomen_events_incremental()

# COMMAND ----------

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
            print("Successfully updated coded events mapping table")
            
        else:
            print("No new records to process")
            
    except Exception as e:
        print(f"Error processing coded events updates: {str(e)}")
        raise


process_coded_events_incremental()
