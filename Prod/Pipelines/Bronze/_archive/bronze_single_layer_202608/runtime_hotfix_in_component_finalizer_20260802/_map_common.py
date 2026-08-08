# Databricks notebook source
# MAGIC %md
# MAGIC # Map Pipeline Replacement
# MAGIC
# MAGIC Interface-compatible replacement with improved data quality, late-arrival handling and validation. Production table names and required downstream columns remain stable.

# COMMAND ----------

from pyspark.sql.functions import max as spark_max
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
from functools import reduce

# Capture runtime objects in notebook-owned private names. Component notebooks
# executed with %run share a mutable global namespace and may redefine public
# names such as table_exists, F, DeltaTable, or datetime.
_PIPELINE_SPARK = spark
_PIPELINE_DBUTILS = dbutils
_pipeline_DeltaTable = DeltaTable
_pipeline_F = F
_pipeline_datetime = datetime
_PIPELINE_ASSERT_ALL_PRIMARY_CONTRACTS = bronze_assert_all_primary_contracts

import builtins as _pipeline_builtins
import json as _pipeline_json
import traceback as _pipeline_traceback
import uuid as _pipeline_uuid

_PIPELINE_MODE = "production"
try:
    _PIPELINE_REQUESTED_RUN_ID = str(dbutils.widgets.get("pipeline_run_id")).strip()
except Exception:
    _PIPELINE_REQUESTED_RUN_ID = ""
_PIPELINE_RUN_ID = _PIPELINE_REQUESTED_RUN_ID or str(_pipeline_uuid.uuid4())
_PIPELINE_TARGET_PREFIX = "4_prod.bronze"
_PIPELINE_CHECKPOINT_TABLE = "4_prod.bronze.map_pipeline_source_checkpoints"
_PIPELINE_AUDIT_TABLE = "4_prod.bronze.map_pipeline_audit"
_PIPELINE_CONTRACTS = {'4_prod.bronze.map_address': {'columns': [{'comment': 'The address ID is the primary key of the '
                                                       'address table.',
                                            'name': 'ADDRESS_ID',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'The upper case name of the table to which '
                                                       'this address row is related (i.e., PERSON, '
                                                       'PRSNL, ORGANIZATION, etc.)',
                                            'name': 'PARENT_ENTITY_NAME',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'The value of the primary identifier of the '
                                                       'table to which the address row is related '
                                                       '(i.e., person_id, organization_id, etc.)',
                                            'name': 'PARENT_ENTITY_ID',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'Partially masked version of the postcode '
                                                       'for privacy protection.',
                                            'name': 'masked_zipcode',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'The city field is the text name of the '
                                                       'city associated with the address row.',
                                            'name': 'CITY',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'Concatenated street address.',
                                            'name': 'full_street_address',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'LSOA stands for Lower Layer Super Output '
                                                       'Area, which is a geographic area used for '
                                                       'small area statistics in the UK.',
                                            'name': 'LSOA',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'IMD_Decile is used to store the Index of '
                                                       'Multiple Deprivation (IMD) decile value.',
                                            'name': 'IMD_Decile',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'IMD_Quintile is used to store the Index of '
                                                       'Multiple Deprivation (IMD) quintile value.',
                                            'name': 'IMD_Quintile',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'Unique Property Reference Number - unique '
                                                       'identifier for every spatial address in '
                                                       'Great Britain (1-999999999999).',
                                            'name': 'UPRN',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': None,
                                            'name': 'LATITUDE',
                                            'nullable': True,
                                            'type': 'double'},
                                           {'comment': None,
                                            'name': 'LONGITUDE',
                                            'nullable': True,
                                            'type': 'double'},
                                           {'comment': 'The algorithm used to match an address '
                                                       'record with a corresponding record in the '
                                                       'addressbase data. Each algorithm '
                                                       'corresponds to a specific matching '
                                                       'strategy employed to link the address '
                                                       'information, providing insights into the '
                                                       'method used to determine the match between '
                                                       'the address records. match_algorithm = 0: '
                                                       'no match; match_algorithm = 1: Exact match '
                                                       '(postcode + number + building); '
                                                       'match_algorithm = 2: Postcode + number '
                                                       'only (no building name); match_algorithm = '
                                                       '3: Field swap - building name in '
                                                       'thoroughfare; match_algorithm =  4: Fuzzy '
                                                       'match with Levenshtein distance; '
                                                       'match_algorithm = 5: Postcode district '
                                                       'match with high address similarity.',
                                            'name': 'match_algorithm',
                                            'nullable': True,
                                            'type': 'int'},
                                           {'comment': 'It represents a numeric score (typically '
                                                       'between 0 and 1) that quantifies how '
                                                       'closely an address record matches a '
                                                       'reference address in the addressbase data, '
                                                       'with higher values indicating a stronger '
                                                       'or more certain match.',
                                            'name': 'match_confidence',
                                            'nullable': True,
                                            'type': 'double'},
                                           {'comment': 'It provides a descriptive label indicating '
                                                       'the type or quality of the address match, '
                                                       'based on the matching algorithm used to '
                                                       'link the address to the reference data.',
                                            'name': 'match_quality',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'The date and time for which this table row '
                                                       'becomes effective. Normally, this will be '
                                                       'the date and time the row is added, but '
                                                       'could be a past or future date and time.',
                                            'name': 'BEG_EFFECTIVE_DT_TM',
                                            'nullable': True,
                                            'type': 'timestamp'},
                                           {'comment': 'The table row is active or inactive. A row '
                                                       'is generally active unless it is in an '
                                                       'inactive state such as logically deleted, '
                                                       'combined away, pending purge, etc.',
                                            'name': 'ACTIVE_IND',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'The date/time after which the row is no '
                                                       'longer valid as active current data.  This '
                                                       'may be valued with the date that the row '
                                                       'became inactive.',
                                            'name': 'END_EFFECTIVE_DT_TM',
                                            'nullable': True,
                                            'type': 'timestamp'},
                                           {'comment': 'Timestamp of last update.',
                                            'name': 'ADC_UPDT',
                                            'nullable': True,
                                            'type': 'timestamp'},
                                           {'comment': 'The description for the code value.',
                                            'name': 'country_cd',
                                            'nullable': True,
                                            'type': 'string'}],
                               'keys': ['ADDRESS_ID'],
                               'table_name': 'map_address'},
 '4_prod.bronze.map_address_epc': {'columns': [{'comment': 'Primary key. Foreign key to '
                                                           'map_address.',
                                                'name': 'ADDRESS_ID',
                                                'nullable': False,
                                                'type': 'bigint'},
                                               {'comment': 'Unique Property Reference Number used '
                                                           'to join to EPC data.',
                                                'name': 'UPRN',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'EPC certificate unique identifier '
                                                           '(LMK_KEY from the EPC register).',
                                                'name': 'EPC_LMK_KEY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'EPC band A-G. Core fuel poverty '
                                                           'indicator; bands D-G flag risk under '
                                                           'LILEE definition.',
                                                'name': 'CURRENT_ENERGY_RATING',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Achievable EPC band after recommended '
                                                           'improvements.',
                                                'name': 'POTENTIAL_ENERGY_RATING',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'SAP score (1-100+). Continuous energy '
                                                           'efficiency measure underpinning the '
                                                           'EPC band.',
                                                'name': 'CURRENT_ENERGY_EFFICIENCY',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'Achievable SAP score after '
                                                           'improvements.',
                                                'name': 'POTENTIAL_ENERGY_EFFICIENCY',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'Wall construction and insulation '
                                                           'description. Solid uninsulated walls '
                                                           'indicate cold/damp risk.',
                                                'name': 'WALLS_DESCRIPTION',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Wall energy efficiency rating (Very '
                                                           'Good to Very Poor).',
                                                'name': 'WALLS_ENERGY_EFF',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Roof type and insulation depth.',
                                                'name': 'ROOF_DESCRIPTION',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Roof energy efficiency rating.',
                                                'name': 'ROOF_ENERGY_EFF',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Floor construction and insulation.',
                                                'name': 'FLOOR_DESCRIPTION',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Floor energy efficiency rating.',
                                                'name': 'FLOOR_ENERGY_EFF',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Glazing description. Single glazing '
                                                           'indicates cold surfaces and '
                                                           'condensation risk.',
                                                'name': 'WINDOWS_DESCRIPTION',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Window energy efficiency rating.',
                                                'name': 'WINDOWS_ENERGY_EFF',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Glazing type (single/double/triple). '
                                                           'Also serves as noise insulation proxy.',
                                                'name': 'GLAZED_TYPE',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Main heating system description. '
                                                           'Identifies combustion-based heating '
                                                           'producing indoor NOx.',
                                                'name': 'MAINHEAT_DESCRIPTION',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Main heating energy efficiency rating.',
                                                'name': 'MAINHEAT_ENERGY_EFF',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Fuel type (mains gas, electricity, '
                                                           'oil, etc). Critical for indoor air '
                                                           'quality and fuel poverty.',
                                                'name': 'MAIN_FUEL',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Heating controls description '
                                                           '(programmer, thermostat, TRVs).',
                                                'name': 'MAINHEATCONT_DESCRIPTION',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Secondary heating. Open fires and '
                                                           'solid fuel stoves identifiable here.',
                                                'name': 'SECONDHEAT_DESCRIPTION',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Whether mains gas is available. '
                                                           'Off-gas-grid homes face higher fuel '
                                                           'costs.',
                                                'name': 'MAINS_GAS_FLAG',
                                                'nullable': True,
                                                'type': 'boolean'},
                                               {'comment': 'Hot water system description.',
                                                'name': 'HOTWATER_DESCRIPTION',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'House/Flat/Bungalow/Maisonette/Park '
                                                           'home.',
                                                'name': 'PROPERTY_TYPE',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Detached/Semi/Terrace etc. Affects '
                                                           'heat loss via surface-area-to-volume '
                                                           'ratio.',
                                                'name': 'BUILT_FORM',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Total floor area in mÂ². Overcrowding '
                                                           'proxy when linked to household size.',
                                                'name': 'TOTAL_FLOOR_AREA',
                                                'nullable': True,
                                                'type': 'double'},
                                               {'comment': 'Count of habitable rooms. Overcrowding '
                                                           'denominator.',
                                                'name': 'NUMBER_HABITABLE_ROOMS',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'Count of heated rooms. Fewer than '
                                                           'habitable rooms indicates spatial '
                                                           'heating poverty.',
                                                'name': 'NUMBER_HEATED_ROOMS',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'Building age band. Strongest single '
                                                           'predictor of fabric quality. Pre-1919 '
                                                           'homes avg SAP ~45.',
                                                'name': 'CONSTRUCTION_AGE_BAND',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Ventilation type (natural/mechanical). '
                                                           'Only ventilation field in EPC data.',
                                                'name': 'MECHANICAL_VENTILATION',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Open fireplace count. Draughtiness and '
                                                           'ventilation proxy.',
                                                'name': 'NUMBER_OPEN_FIREPLACES',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'Modelled annual heating cost (GBP).',
                                                'name': 'HEATING_COST_CURRENT',
                                                'nullable': True,
                                                'type': 'double'},
                                               {'comment': 'Modelled annual hot water cost (GBP).',
                                                'name': 'HOT_WATER_COST_CURRENT',
                                                'nullable': True,
                                                'type': 'double'},
                                               {'comment': 'Modelled annual lighting cost (GBP).',
                                                'name': 'LIGHTING_COST_CURRENT',
                                                'nullable': True,
                                                'type': 'double'},
                                               {'comment': 'Modelled energy consumption '
                                                           '(kWh/m2/year).',
                                                'name': 'ENERGY_CONSUMPTION_CURRENT',
                                                'nullable': True,
                                                'type': 'double'},
                                               {'comment': 'Modelled CO2 emissions (tonnes/year).',
                                                'name': 'CO2_EMISSIONS_CURRENT',
                                                'nullable': True,
                                                'type': 'double'},
                                               {'comment': 'Date the EPC assessment was carried '
                                                           'out.',
                                                'name': 'EPC_INSPECTION_DATE',
                                                'nullable': True,
                                                'type': 'date'},
                                               {'comment': 'Date the EPC was lodged on the '
                                                           'register.',
                                                'name': 'EPC_LODGEMENT_DATE',
                                                'nullable': True,
                                                'type': 'date'},
                                               {'comment': 'Tenure at time of EPC: Owner-occupied '
                                                           '/ Rented (private) / Rented (social).',
                                                'name': 'TENURE',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'EPC trigger: marketed sale, rental, '
                                                           'new dwelling, ECO assessment, etc.',
                                                'name': 'TRANSACTION_TYPE',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'True if EPC band D-G. Flags fuel '
                                                           'poverty risk under LILEE definition.',
                                                'name': 'fuel_poverty_risk',
                                                'nullable': True,
                                                'type': 'boolean'},
                                               {'comment': 'True if EPC band F or G. Approximates '
                                                           'HHSRS Category 1 excessive cold '
                                                           'hazard.',
                                                'name': 'hhsrs_cold_hazard_proxy',
                                                'nullable': True,
                                                'type': 'boolean'},
                                               {'comment': 'True if heated rooms < habitable '
                                                           'rooms, indicating partial home '
                                                           'heating.',
                                                'name': 'spatial_heating_poverty',
                                                'nullable': True,
                                                'type': 'boolean'},
                                               {'comment': 'True if no mains gas connection. '
                                                           'Off-grid homes face higher fuel costs.',
                                                'name': 'off_gas_grid',
                                                'nullable': True,
                                                'type': 'boolean'},
                                               {'comment': 'Timestamp of last update.',
                                                'name': 'ADC_UPDT',
                                                'nullable': True,
                                                'type': 'timestamp'}],
                                   'keys': ['ADDRESS_ID'],
                                   'table_name': 'map_address_epc'},
 '4_prod.bronze.map_care_site': {'columns': [{'comment': 'The field identifies the current '
                                                         'permanent location of the patient. The '
                                                         'location for an inpatient will be valued '
                                                         'with the lowest level location type in '
                                                         'the hierarchy of facility, building, '
                                                         'nurse unit, room, bed.',
                                              'name': 'care_site_cd',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'Location type defines the kind of '
                                                         'location (I.e., nurse unit, room, '
                                                         'inventory location,  etc.).  Location '
                                                         'types have Cerner defined meanings in '
                                                         'the common data foundation.',
                                              'name': 'location_type_cd',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The display string for the code_value.',
                                              'name': 'care_site_name',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The code identifying the building '
                                                         'associated with a care site.',
                                              'name': 'building_cd',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The display string for the code_value.',
                                              'name': 'building_name',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The code identifying the facility '
                                                         'associated with a care site.',
                                              'name': 'facility_cd',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The display string for the code_value.',
                                              'name': 'facility_name',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'This is the value of the unique primary '
                                                         'identifier of the organization table.  '
                                                         'It is an internal system assigned '
                                                         'number.',
                                              'name': 'ORGANIZATION_ID',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The name of the organization.',
                                              'name': 'organization_name',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The address ID is the primary key of the '
                                                         'address table.',
                                              'name': 'address_id',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'Timestamp of last update.',
                                              'name': 'ADC_UPDT',
                                              'nullable': True,
                                              'type': 'timestamp'}],
                                 'keys': ['care_site_cd'],
                                 'table_name': 'map_care_site'},
 '4_prod.bronze.map_coded_events': {'columns': [{'comment': 'The unique primary identifier of the '
                                                            'Event Table.',
                                                 'name': 'EVENT_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'This is the value of the unique '
                                                            'primary identifier of the encounter '
                                                            'table. It is an internal system '
                                                            'assigned number.',
                                                 'name': 'ENCNTR_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'This is the value of the unique '
                                                            'primary identifier of the person '
                                                            'table. It is an internal system '
                                                            'assigned number.',
                                                 'name': 'PERSON_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'The unique primary identifier of '
                                                            'Order Table.',
                                                 'name': 'ORDER_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'Personnel id of provider who '
                                                            'performed this result.',
                                                 'name': 'PERFORMED_PRSNL_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'Allows the use of a code value '
                                                            'instead of a nomenclature id. The '
                                                            'code set of the code_value is user '
                                                            'defined.',
                                                 'name': 'RESULT_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'RESULT_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The actual string value for the cdf '
                                                            'meaning.',
                                                 'name': 'RESULT_MEANING',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'A non-nomenclature option. Code set '
                                                            'of result_cd if it is not null.',
                                                 'name': 'RESULT_SET',
                                                 'nullable': True,
                                                 'type': 'double'},
                                                {'comment': 'The title for document results.',
                                                 'name': 'EVENT_TITLE_TEXT',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'It is the code that identifies the '
                                                            'most basic unit of the storage, i.e. '
                                                            'RBC, discharge summary, image.',
                                                 'name': 'EVENT_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'EVENT_CD_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Foreign key to the order_catalog '
                                                            'table. Catalog_cd does not exist in '
                                                            'the code_value table and does not '
                                                            'have a code set.',
                                                 'name': 'CATALOG_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description of the Orderable.',
                                                 'name': 'CATALOG_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Used to store the internal code for '
                                                            'the catalog type. Used as a filtering '
                                                            'mechanism for rows on theorder '
                                                            'catalog table.',
                                                 'name': 'CATALOG_TYPE_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'CATALOG_TYPE_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Coded value which specifies how the '
                                                            'event is stored in and retrieved from '
                                                            "the event table's sub-tables. For "
                                                            'example, Event_Class_CDs identify '
                                                            'events as numeric results, textual '
                                                            'results, calculations, medications, '
                                                            'etc.',
                                                 'name': 'EVENT_CLASS_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'Contributor system identifies the '
                                                            'source feed of data from which a row '
                                                            'was populated.  This is mainly used '
                                                            'to determine how to update a set of '
                                                            'data that may have originated from '
                                                            'more than one source feed.',
                                                 'name': 'CONTRIBUTOR_SYSTEM_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'CONTRIBUTOR_SYSTEM_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The combination of the reference nbr '
                                                            'and the contributor system code '
                                                            'provides a unique identifier to the '
                                                            'origin of the data.',
                                                 'name': 'REFERENCE_NBR',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Provides a mechanism for logical '
                                                            'grouping of events.  i.e. supergroup '
                                                            'and group tests.  Same as event_id if '
                                                            'current row is the highest level '
                                                            'parent.',
                                                 'name': 'PARENT_EVENT_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'States whether the result is normal.  '
                                                            'This can be used to determine whether '
                                                            'to display the event tag in different '
                                                            'color on the flowsheet. For group '
                                                            'results, this represents an overall '
                                                            'normalcy. i.e. Is any result in the '
                                                            'group abnormal?  Also allows '
                                                            'different purge criteria to be '
                                                            'applied based on result.',
                                                 'name': 'NORMALCY_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'NORMALCY_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Used to identify the method in which '
                                                            'a result was entered.',
                                                 'name': 'ENTRY_MODE_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'ENTRY_MODE_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Date this result was performed (or '
                                                            'authored).',
                                                 'name': 'PERFORMED_DT_TM',
                                                 'nullable': True,
                                                 'type': 'timestamp'},
                                                {'comment': 'Represents the update date/time that '
                                                            'tracks when clinically significant '
                                                            'updates are made to the Clinical '
                                                            'Event and should only be used to '
                                                            'check for updates. This field is used '
                                                            'to notify audiences when a clinically '
                                                            'significant update is made to an '
                                                            'existing clinical event, such as when '
                                                            'XR Clinical Reporting re-prints a lab '
                                                            'result due to an update of the result '
                                                            'value or when a result is resent to a '
                                                            "provider's Message Center with the "
                                                            'result update. This date should NOT '
                                                            'be displayed as the clinically.',
                                                 'name': 'CLINSIG_UPDT_DT_TM',
                                                 'nullable': True,
                                                 'type': 'timestamp'},
                                                {'comment': 'The title for document results.',
                                                 'name': 'PARENT_EVENT_TITLE_TEXT',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The code value of the parent event.',
                                                 'name': 'PARENT_EVENT_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'PARENT_EVENT_CD_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The catalog code of the parent event.',
                                                 'name': 'PARENT_CATALOG_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description of the Orderable.',
                                                 'name': 'PARENT_CATALOG_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The internal code for the catalog '
                                                            'type of the parent event.',
                                                 'name': 'PARENT_CATALOG_TYPE_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'PARENT_CATALOG_TYPE_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The combination of the reference nbr '
                                                            'and the contributor system code '
                                                            'provides a unique identifier to the '
                                                            'origin of the data.',
                                                 'name': 'PARENT_REFERENCE_NBR',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Timestamp of last update.',
                                                 'name': 'ADC_UPDT',
                                                 'nullable': True,
                                                 'type': 'timestamp'},
                                                {'comment': 'The name of the OMOP Common Data '
                                                            'Model table.',
                                                 'name': 'OMOP_MANUAL_TABLE',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The field of the OMOP Common Data '
                                                            'Model table.',
                                                 'name': 'OMOP_MANUAL_COLUMN',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The concept_id of the OMOP Common '
                                                            'Data Model table.',
                                                 'name': 'OMOP_MANUAL_CONCEPT',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The manually mapped OMOP concept ID '
                                                            'representing the value of a text for '
                                                            'standardized use in the OMOP CDM.',
                                                 'name': 'OMOP_MANUAL_VALUE_CONCEPT',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The name or description of the OMOP '
                                                            'concept id.',
                                                 'name': 'OMOP_MANUAL_CONCEPT_NAME',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'This flag determines where a Concept '
                                                            'is a Standard Concept, i.e. is used '
                                                            'in the data, a Classification '
                                                            'Concept, or a non-standard Source '
                                                            'Concept. The allowables values are S '
                                                            '(Standard Concept) and C '
                                                            '(Classification Concept), otherwise '
                                                            'the content is NULL.',
                                                 'name': 'OMOP_MANUAL_STANDARD_CONCEPT',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'A unique identifier for the domain.',
                                                 'name': 'OMOP_MANUAL_CONCEPT_DOMAIN',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The identifier for the class or '
                                                            'category of the OMOP concept.',
                                                 'name': 'OMOP_MANUAL_CONCEPT_CLASS',
                                                 'nullable': True,
                                                 'type': 'string'}],
                                    'keys': ['EVENT_ID'],
                                    'table_name': 'map_coded_events'},
 '4_prod.bronze.map_date_events': {'columns': [{'comment': 'The unique primary identifier of the '
                                                           'Event Table.',
                                                'name': 'EVENT_ID',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'This is the value of the unique '
                                                           'primary identifier of the encounter '
                                                           'table. It is an internal system '
                                                           'assigned number.',
                                                'name': 'ENCNTR_ID',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'This is the value of the unique '
                                                           'primary identifier of the person '
                                                           'table. It is an internal system '
                                                           'assigned number.',
                                                'name': 'PERSON_ID',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'The unique primary identifier of Order '
                                                           'Table.',
                                                'name': 'ORDER_ID',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'Coded value which specifies how the '
                                                           'event is stored in and retrieved from '
                                                           "the event table's sub-tables. For "
                                                           'example, Event_Class_CDs identify '
                                                           'events as numeric results, textual '
                                                           'results, calculations, medications, '
                                                           'etc.',
                                                'name': 'EVENT_CLASS_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'Personnel id of provider who performed '
                                                           'this result.',
                                                'name': 'PERFORMED_PRSNL_ID',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'Timestamp that records the date and '
                                                           'time for the clinical result.',
                                                'name': 'RESULT_DT_TM',
                                                'nullable': True,
                                                'type': 'timestamp'},
                                               {'comment': 'The title for document results.',
                                                'name': 'EVENT_TITLE_TEXT',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'It is the code that identifies the '
                                                           'most basic unit of the storage, i.e. '
                                                           'RBC, discharge summary, image.',
                                                'name': 'EVENT_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value.',
                                                'name': 'EVENT_CD_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Foreign key to the order_catalog '
                                                           'table. Catalog_cd does not exist in '
                                                           'the code_value table and does not have '
                                                           'a code set.',
                                                'name': 'CATALOG_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description of the Orderable.',
                                                'name': 'CATALOG_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Used to store the internal code for '
                                                           'the catalog type. Used as a filtering '
                                                           'mechanism for rows on theorder catalog '
                                                           'table.',
                                                'name': 'CATALOG_TYPE_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value.',
                                                'name': 'CATALOG_TYPE_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Contributor system identifies the '
                                                           'source feed of data from which a row '
                                                           'was populated.  This is mainly used to '
                                                           'determine how to update a set of data '
                                                           'that may have originated from more '
                                                           'than one source feed.',
                                                'name': 'CONTRIBUTOR_SYSTEM_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value.',
                                                'name': 'CONTRIBUTOR_SYSTEM_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The combination of the reference nbr '
                                                           'and the contributor system code '
                                                           'provides a unique identifier to the '
                                                           'origin of the data.',
                                                'name': 'REFERENCE_NBR',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Provides a mechanism for logical '
                                                           'grouping of events. i.e. supergroup '
                                                           'and group tests. Same as event_id if '
                                                           'current row is the highest level '
                                                           'parent.',
                                                'name': 'PARENT_EVENT_ID',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'States whether the result is normal.  '
                                                           'This can be used to determine whether '
                                                           'to display the event tag in different '
                                                           'color on the flowsheet. For group '
                                                           'results, this represents an overall '
                                                           'normalcy. i.e. Is any result in the '
                                                           'group abnormal?  Also allows different '
                                                           'purge criteria to be applied based on '
                                                           'result.',
                                                'name': 'NORMALCY_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value',
                                                'name': 'NORMALCY_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Used to identify the method in which a '
                                                           'result was entered.',
                                                'name': 'ENTRY_MODE_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value.',
                                                'name': 'ENTRY_MODE_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Date this result was performed (or '
                                                           'authored).',
                                                'name': 'PERFORMED_DT_TM',
                                                'nullable': True,
                                                'type': 'timestamp'},
                                               {'comment': 'Represents the update date/time that '
                                                           'tracks when clinically significant '
                                                           'updates are made to the Clinical Event '
                                                           'and should only be used to check for '
                                                           'updates. This field is used to notify '
                                                           'audiences when a clinically '
                                                           'significant update is made to an '
                                                           'existing clinical event, such as when '
                                                           'XR Clinical Reporting re-prints a lab '
                                                           'result due to an update of the result '
                                                           'value or when a result is resent to a '
                                                           "provider's Message Center with the "
                                                           'result update. This date should NOT be '
                                                           'displayed as the clinically.',
                                                'name': 'CLINSIG_UPDT_DT_TM',
                                                'nullable': True,
                                                'type': 'timestamp'},
                                               {'comment': 'The title associated with the parent '
                                                           'event.',
                                                'name': 'PARENT_EVENT_TITLE_TEXT',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The code value of the parent event.',
                                                'name': 'PARENT_EVENT_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value.',
                                                'name': 'PARENT_EVENT_CD_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The catalog code of the parent event.',
                                                'name': 'PARENT_CATALOG_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description of the Orderable.',
                                                'name': 'PARENT_CATALOG_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The internal code for the catalog type '
                                                           'of the parent event.',
                                                'name': 'PARENT_CATALOG_TYPE_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value.',
                                                'name': 'PARENT_CATALOG_TYPE_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The combination of the reference nbr '
                                                           'and the contributor system code '
                                                           'provides a unique identifier to the '
                                                           'origin of the data.',
                                                'name': 'PARENT_REFERENCE_NBR',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Timestamp of last update.',
                                                'name': 'ADC_UPDT',
                                                'nullable': True,
                                                'type': 'timestamp'}],
                                   'keys': ['EVENT_ID'],
                                   'table_name': 'map_date_events'},
 '4_prod.bronze.map_death': {'columns': [{'comment': 'This is the value of the unique primary '
                                                     'identifier of the person table. It is an '
                                                     'internal system assigned number.',
                                          'name': 'PERSON_ID',
                                          'nullable': True,
                                          'type': 'bigint'},
                                         {'comment': 'Date and time of death.',
                                          'name': 'DECEASED_DT_TM',
                                          'nullable': True,
                                          'type': 'timestamp'},
                                         {'comment': 'It represents the timestamp of the last '
                                                     'encounter associated with a deceased '
                                                     'individual.',
                                          'name': 'LAST_ENCNTR_DT_TM',
                                          'nullable': True,
                                          'type': 'timestamp'},
                                         {'comment': 'It represents the most recent update '
                                                     'date/time that tracks when clinically '
                                                     'significant updates are made to the Clinical '
                                                     'Event and should only be used to check for '
                                                     'updates.',
                                          'name': 'LAST_CE_DT_TM',
                                          'nullable': True,
                                          'type': 'timestamp'},
                                         {'comment': 'Calculated death date.',
                                          'name': 'CALC_DEATH_DATE',
                                          'nullable': True,
                                          'type': 'timestamp'},
                                         {'comment': 'It defines the particular source that gave '
                                                     'deceased information concerning a person. '
                                                     'For example, from a Formal (Death '
                                                     'Certificate) or Informal (no Death '
                                                     'Certificate) source.',
                                          'name': 'DECEASED_SOURCE_CD',
                                          'nullable': True,
                                          'type': 'int'},
                                         {'comment': 'Description of the code.',
                                          'name': 'DECEASED_SOURCE_DESC',
                                          'nullable': True,
                                          'type': 'string'},
                                         {'comment': 'It stores code values defining the specific '
                                                     'way a patient was confirmed as being '
                                                     'deceased. Possible values  include Death '
                                                     'Certificate, Physician Reported, etc. The '
                                                     'code values are closely tied, workflow-wise, '
                                                     'to the Deceased_Source_Cd which records if a '
                                                     'patient was identified as being deceased '
                                                     'from a Formal (Death Certificate) or '
                                                     'Informal (no Death Certificate) source and '
                                                     'the Deceased_Notify_Source_Cd which records '
                                                     'who or what provided the information '
                                                     "regarding the patient's deceased status.",
                                          'name': 'DECEASED_ID_METHOD_CD',
                                          'nullable': True,
                                          'type': 'int'},
                                         {'comment': 'Description of the code.',
                                          'name': 'DECEASED_METHOD_DESC',
                                          'nullable': True,
                                          'type': 'string'},
                                         {'comment': 'Timestamp of last update.',
                                          'name': 'ADC_UPDT',
                                          'nullable': True,
                                          'type': 'timestamp'}],
                             'keys': ['PERSON_ID'],
                             'table_name': 'map_death'},
 '4_prod.bronze.map_diagnosis': {'columns': [{'comment': 'The primary key for the Diagnosis table.',
                                              'name': 'DIAGNOSIS_ID',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'This is the value of the unique primary '
                                                         'identifier of the person table.  It is '
                                                         'an internal system assigned number.',
                                              'name': 'PERSON_ID',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'This is the value of the unique primary '
                                                         'identifier of the encounter table.  It '
                                                         'is an internal system assigned number.',
                                              'name': 'ENCNTR_ID',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'Date/time for which the Diagnosis was '
                                                         'saved.',
                                              'name': 'DIAG_DT_TM',
                                              'nullable': True,
                                              'type': 'timestamp'},
                                             {'comment': 'The earliest recorded date on which a '
                                                         'diagnosis was made for the patient.',
                                              'name': 'earliest_diagnosis_date',
                                              'nullable': True,
                                              'type': 'timestamp'},
                                             {'comment': 'The type of diagnosis.',
                                              'name': 'DIAG_TYPE_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value',
                                              'name': 'diag_type_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Priority of diagnoses as determined by '
                                                         'application.',
                                              'name': 'DIAG_PRIORITY',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'Codified ranking description.',
                                              'name': 'RANKING_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'Prsnl_id of person that added the '
                                                         'diagnosis.',
                                              'name': 'DIAG_PRSNL_ID',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'Associates the clinical diagnosis to a '
                                                         'particular setting of care within an '
                                                         'encounter.',
                                              'name': 'CLINICAL_SERVICE_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value.',
                                              'name': 'clinical_service_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Describes the definitiveness and '
                                                         'clinical status of the diagnosis.',
                                              'name': 'CONFIRMATION_STATUS_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value.',
                                              'name': 'confirmation_status_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Classification of the clinical diagnosis '
                                                         'by the area of focus.',
                                              'name': 'CLASSIFICATION_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value.',
                                              'name': 'classification_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'This is the value of the unique primary '
                                                         'identifier of the nomenclature table. It '
                                                         'is an internal system assigned number.',
                                              'name': 'NOMENCLATURE_ID',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The code, or key, from the source '
                                                         'vocabulary that contributed the string '
                                                         'to the nomenclature.',
                                              'name': 'SOURCE_IDENTIFIER',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Variable length string that may include '
                                                         'alphanumeric characters and punctuation.',
                                              'name': 'SOURCE_STRING',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The external vocabulary or lexicon that '
                                                         'contributed the string, e.g. ICD9, '
                                                         'SNOMED, etc.',
                                              'name': 'SOURCE_VOCABULARY_CD',
                                              'nullable': True,
                                              'type': 'decimal(38,18)'},
                                             {'comment': 'The description for the code value.',
                                              'name': 'source_vocabulary_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Vocabulary AXIS codes related to '
                                                         'SNOMEDColumn.',
                                              'name': 'VOCAB_AXIS_CD',
                                              'nullable': True,
                                              'type': 'decimal(38,18)'},
                                             {'comment': 'The description for the code value.',
                                              'name': 'vocab_axis_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Concept CKI is the functional Concept '
                                                         'Identifier; it is the codified means '
                                                         'within Millennium to identify key '
                                                         'medical concepts to support information '
                                                         'processing, clinical decision support, '
                                                         'executable knowledge and knowledge '
                                                         'presentation. Composed of a source and '
                                                         'an identifier.',
                                              'name': 'CONCEPT_CKI',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'A unique identifier for each Concept '
                                                         'across all domains.',
                                              'name': 'OMOP_CONCEPT_ID',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'An unambiguous, meaningful and '
                                                         'descriptive name for the Concept.',
                                              'name': 'OMOP_CONCEPT_NAME',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'This flag determines where a Concept is '
                                                         'a Standard Concept, i.e. is used in the '
                                                         'data, a Classification Concept, or a '
                                                         'non-standard Source Concept. The '
                                                         'allowables values are S (Standard '
                                                         'Concept) and C (Classification Concept), '
                                                         'otherwise the content is NULL.',
                                              'name': 'OMOP_STANDARD_CONCEPT',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The number of OMOP concepts matched for '
                                                         'each NOMENCLATURE_ID.',
                                              'name': 'OMOP_MATCH_NUMBER',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'A unique identifier for each domain.',
                                              'name': 'OMOP_CONCEPT_DOMAIN',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': None,
                                              'name': 'SNOMED_CODE',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'The method or source of the SNOMED code '
                                                         'mapping for each nomenclature entry.',
                                              'name': 'SNOMED_TYPE',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The number of matches found for each '
                                                         'NOMENCLATURE_ID in the context of SNOMED '
                                                         'codes.',
                                              'name': 'SNOMED_MATCH_NUMBER',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'The term associated with a SNOMED code '
                                                         'that provides additional meaning and '
                                                         'context to the code.',
                                              'name': 'SNOMED_TERM',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': None,
                                              'name': 'ICD10_CODE',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The method or source of the ICD10 code '
                                                         'mapping for each nomenclature entry.',
                                              'name': 'ICD10_TYPE',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The number of matches found for each '
                                                         'NOMENCLATURE_ID in the context of ICD10 '
                                                         'codes.',
                                              'name': 'ICD10_MATCH_NUMBER',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'The term associated with a ICD10 code '
                                                         'that provides additional meaning and '
                                                         'context to the code.',
                                              'name': 'ICD10_TERM',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Timestamp of last update.',
                                              'name': 'ADC_UPDT',
                                              'nullable': True,
                                              'type': 'timestamp'},
                                             {'comment': 'COSIGN Similarity between the Source '
                                                         'term and the OMOP Term.',
                                              'name': 'OMOP_SIMILARITY',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'COSIGN Similarity between the Source '
                                                         'term and the SNOMED Term.',
                                              'name': 'SNOMED_SIMILARITY',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'COSIGN Similarity between the Source '
                                                         'term and the ICD10 Term.',
                                              'name': 'ICD10_SIMILARITY',
                                              'nullable': True,
                                              'type': 'double'}],
                                 'keys': ['DIAGNOSIS_ID'],
                                 'table_name': 'map_diagnosis'},
 '4_prod.bronze.map_encounter': {'columns': [{'comment': 'Unique identifier for the Encounter '
                                                         'table.',
                                              'name': 'ENCNTR_ID',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'Person whom this encounter is for.',
                                              'name': 'PERSON_ID',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The actual date/time that the patient '
                                                         'arrived at the facility. At the time of '
                                                         'registration, if this field is null then '
                                                         'it should be valued with the reg_dt_tm. '
                                                         'Otherwise, the actual arrival date/time '
                                                         'is captured.',
                                              'name': 'ARRIVE_DT_TM',
                                              'nullable': True,
                                              'type': 'timestamp'},
                                             {'comment': 'The actual date/time that the patient '
                                                         'left from the facility. In many cases, '
                                                         'this field may be null unless the user '
                                                         'process requires capturing this data.',
                                              'name': 'DEPART_DT_TM',
                                              'nullable': True,
                                              'type': 'timestamp'},
                                             {'comment': 'Encounter class defines how this '
                                                         'encounter row is being used in relation '
                                                         'to the person table.',
                                              'name': 'ENCNTR_CLASS_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value',
                                              'name': 'encntr_class_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Categorizes the encounter into a logical '
                                                         'group or type. Examples may include '
                                                         'inpatient, outpatient, etc.',
                                              'name': 'ENCNTR_TYPE_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value',
                                              'name': 'encntr_type_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Encounter status identifies the state of '
                                                         'a particular encounter type from the '
                                                         'time it is initiated until it is '
                                                         'complete.  (i.e., temporary, '
                                                         'preliminary, active, discharged '
                                                         '(complete), cancelled).',
                                              'name': 'ENCNTR_STATUS_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value.',
                                              'name': 'encntr_status_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Admit source identifies the place from '
                                                         'which the patient came before being '
                                                         'admitted. (i.e., transfer from another '
                                                         'hospital).',
                                              'name': 'ADMIT_SRC_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value.',
                                              'name': 'admit_src_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The location to which the patient was '
                                                         'discharged such as another hospital or '
                                                         'nursing home.',
                                              'name': 'DISCH_TO_LOCTN_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value.',
                                              'name': 'disch_loctn_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The type or category of medical service '
                                                         'that the patient is receiving in '
                                                         'relation to their encounter.  The '
                                                         'category may be of treatment type, '
                                                         'surgery, general resources, or others.',
                                              'name': 'MED_SERVICE_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value',
                                              'name': 'med_service_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'This field is the current patient '
                                                         'location with a location type of nurse '
                                                         'unit.',
                                              'name': 'LOC_NURSE_UNIT_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value',
                                              'name': 'nurse_unit_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The specialty unit associated with the '
                                                         'program service',
                                              'name': 'SPECIALTY_UNIT_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value',
                                              'name': 'specialty_unit_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The internal person ID of the personnel '
                                                         'that performed the registration or '
                                                         'admission.  If the reg_dt_tm is valued, '
                                                         'then this field must be valued.',
                                              'name': 'REG_PRSNL_ID',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'Timestamp of last update.',
                                              'name': 'ADC_UPDT',
                                              'nullable': True,
                                              'type': 'timestamp'}],
                                 'keys': ['ENCNTR_ID'],
                                 'table_name': 'map_encounter'},
 '4_prod.bronze.map_family_history': {'columns': [{'comment': 'Primary unique identifier for the '
                                                              'family history activity record from '
                                                              'FHX_ACTIVITY.',
                                                   'name': 'FHX_ACTIVITY_ID',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'Unique identifier for the patient '
                                                              'whose family history is being '
                                                              'recorded.',
                                                   'name': 'PERSON_ID',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'Local Medical Record Number for the '
                                                              'patient.',
                                                   'name': 'MRN',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'NHS Number for the patient.',
                                                   'name': 'NHS_Number',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Identifier for the related family '
                                                              'member. May be 0 for '
                                                              'virtual/unnamed relatives.',
                                                   'name': 'RELATED_PERSON_ID',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'Code identifying the type of family '
                                                              'relationship (Code Set 40). E.g., '
                                                              'Mother, Father, Sibling.',
                                                   'name': 'RELATION_CD',
                                                   'nullable': True,
                                                   'type': 'double'},
                                                  {'comment': 'Display description of the '
                                                              "relationship type (e.g., 'Mother', "
                                                              "'Father', 'Grandparent').",
                                                   'name': 'RELATION_DESC',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Code for the category of '
                                                              'relationship (Code Set 351). E.g., '
                                                              'Family History, Emergency Contact.',
                                                   'name': 'RELATION_TYPE_CD',
                                                   'nullable': True,
                                                   'type': 'double'},
                                                  {'comment': 'Description of the relationship '
                                                              'category.',
                                                   'name': 'RELATION_TYPE_DESC',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Indicates if the relationship is '
                                                              'genetic/blood-related. 1=Genetic, '
                                                              '0=Non-genetic (e.g., step-parent, '
                                                              'adoptive).',
                                                   'name': 'GENETIC_IND',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'Unique identifier for the '
                                                              'person-to-person relationship '
                                                              'record from PERSON_PERSON_RELTN.',
                                                   'name': 'PERSON_PERSON_RELTN_ID',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'Identifier linking to NOMENCLATURE '
                                                              'for coded conditions. 0 indicates '
                                                              'free-text entry.',
                                                   'name': 'NOMENCLATURE_ID',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'Coded condition description from '
                                                              'NOMENCLATURE.SOURCE_STRING. NULL if '
                                                              'free-text entry.',
                                                   'name': 'CONDITION_DESC_CODED',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Free-text description of the '
                                                              'condition from LONG_TEXT. NULL if '
                                                              'coded entry.',
                                                   'name': 'CONDITION_DESC_FREETEXT',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Combined condition description. '
                                                              'Uses coded description if '
                                                              'available, otherwise free-text.',
                                                   'name': 'CONDITION_DESC',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Indicates source of condition '
                                                              "description: 'CODED' (from "
                                                              "nomenclature), 'FREE_TEXT' (from "
                                                              "long_text), or 'UNKNOWN'.",
                                                   'name': 'CONDITION_SOURCE',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'The code/key from the source '
                                                              'vocabulary (e.g., SNOMED code, ICD '
                                                              'code) as stored in NOMENCLATURE.',
                                                   'name': 'SOURCE_IDENTIFIER',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Code indicating the source '
                                                              'vocabulary (Code Set 400). E.g., '
                                                              'SNOMED CT, ICD-9, ICD-10.',
                                                   'name': 'SOURCE_VOCABULARY_CD',
                                                   'nullable': True,
                                                   'type': 'double'},
                                                  {'comment': 'Description of the source '
                                                              "vocabulary (e.g., 'SNOMED CT', "
                                                              "'ICD-10-CM').",
                                                   'name': 'SOURCE_VOCABULARY_DESC',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Vocabulary axis code (Code Set '
                                                              '15849), primarily used for SNOMED '
                                                              'CT axes.',
                                                   'name': 'VOCAB_AXIS_CD',
                                                   'nullable': True,
                                                   'type': 'double'},
                                                  {'comment': 'Description of the vocabulary axis.',
                                                   'name': 'VOCAB_AXIS_DESC',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Cerner Knowledge Index (CKI) '
                                                              'concept identifier. Extracted '
                                                              "portion after '!' delimiter.",
                                                   'name': 'CONCEPT_CKI',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'OMOP CDM Concept ID mapped from the '
                                                              'source nomenclature.',
                                                   'name': 'OMOP_CONCEPT_ID',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'OMOP Concept Name/description.',
                                                   'name': 'OMOP_CONCEPT_NAME',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'OMOP standard concept flag: '
                                                              "'S'=Standard Concept, "
                                                              "'C'=Classification Concept, "
                                                              'NULL=Non-standard.',
                                                   'name': 'OMOP_STANDARD_CONCEPT',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Number of OMOP concepts matched for '
                                                              'this NOMENCLATURE_ID. >1 indicates '
                                                              'multiple possible mappings.',
                                                   'name': 'OMOP_MATCH_NUMBER',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'Cosine similarity score (0-1) '
                                                              'between source term and matched '
                                                              'OMOP term. Higher = better match.',
                                                   'name': 'OMOP_SIMILARITY',
                                                   'nullable': True,
                                                   'type': 'double'},
                                                  {'comment': 'OMOP domain of the concept (e.g., '
                                                              "'Condition', 'Observation', "
                                                              "'Procedure').",
                                                   'name': 'OMOP_CONCEPT_DOMAIN',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'SNOMED CT concept code.',
                                                   'name': 'SNOMED_CODE',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'Method/source of the SNOMED mapping '
                                                              "(e.g., 'DIRECT', 'CKI', "
                                                              "'SIMILARITY').",
                                                   'name': 'SNOMED_TYPE',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Number of SNOMED matches for this '
                                                              'NOMENCLATURE_ID. >1 indicates '
                                                              'multiple possible mappings.',
                                                   'name': 'SNOMED_MATCH_NUMBER',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'Cosine similarity score (0-1) '
                                                              'between source term and matched '
                                                              'SNOMED term.',
                                                   'name': 'SNOMED_SIMILARITY',
                                                   'nullable': True,
                                                   'type': 'double'},
                                                  {'comment': 'SNOMED CT preferred term '
                                                              'description.',
                                                   'name': 'SNOMED_TERM',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'ICD-10 diagnosis code.',
                                                   'name': 'ICD10_CODE',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Method/source of ICD-10 mapping '
                                                              "(e.g., 'DIRECT', 'SNOMED_MAP', "
                                                              "'SIMILARITY').",
                                                   'name': 'ICD10_TYPE',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Number of ICD-10 matches for this '
                                                              'NOMENCLATURE_ID. >1 indicates '
                                                              'multiple possible mappings.',
                                                   'name': 'ICD10_MATCH_NUMBER',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'Cosine similarity score (0-1) '
                                                              'between source term and matched '
                                                              'ICD-10 term.',
                                                   'name': 'ICD10_SIMILARITY',
                                                   'nullable': True,
                                                   'type': 'double'},
                                                  {'comment': 'ICD-10 code description/term.',
                                                   'name': 'ICD10_TERM',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Type of family history entry: '
                                                              "'PERSON' (patient overview), "
                                                              "'RELTN' (family member), "
                                                              "'CONDITION' (specific condition).",
                                                   'name': 'FHX_TYPE',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Indicates condition status for the '
                                                              'family member: 0=Negative, '
                                                              '1=Positive, 2=Unknown, 3=Unable to '
                                                              'Obtain, 4=Patient Adopted.',
                                                   'name': 'FHX_VALUE_FLAG',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'Age at which the condition started '
                                                              'in the family member. Critical for '
                                                              'hereditary risk assessment.',
                                                   'name': 'ONSET_AGE',
                                                   'nullable': True,
                                                   'type': 'double'},
                                                  {'comment': 'Unit of measurement for onset age '
                                                              "(e.g., 'Years', 'Months', 'Days').",
                                                   'name': 'ONSET_AGE_UNIT',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Severity of the condition (Code Set '
                                                              "12022). E.g., 'Mild', 'Moderate', "
                                                              "'Severe'.",
                                                   'name': 'SEVERITY',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Course/progression of the condition '
                                                              '(Code Set 12039). E.g., '
                                                              "'Improving', 'Stable', 'Worsening'.",
                                                   'name': 'COURSE',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Current status of the family '
                                                              'history record (Code Set 12030). '
                                                              "E.g., 'Active', 'Inactive', "
                                                              "'Resolved', 'Cancelled'.",
                                                   'name': 'LIFE_CYCLE_STATUS',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Date/time when this family history '
                                                              'record becomes effective.',
                                                   'name': 'BEG_EFFECTIVE_DT_TM',
                                                   'nullable': True,
                                                   'type': 'timestamp'},
                                                  {'comment': 'Date/time after which this record '
                                                              'is no longer valid/active.',
                                                   'name': 'END_EFFECTIVE_DT_TM',
                                                   'nullable': True,
                                                   'type': 'timestamp'},
                                                  {'comment': 'Latest update timestamp across '
                                                              'joined tables (FHX_ACTIVITY, '
                                                              'PERSON_PERSON_RELTN, NOMENCLATURE). '
                                                              'Used for incremental processing.',
                                                   'name': 'ADC_UPDT',
                                                   'nullable': True,
                                                   'type': 'timestamp'}],
                                      'keys': ['FHX_ACTIVITY_ID'],
                                      'table_name': 'map_family_history'},
 '4_prod.bronze.map_implant_details': {'columns': [{'comment': 'Primary key - the implant '
                                                               'description event ID',
                                                    'name': 'EVENT_ID',
                                                    'nullable': True,
                                                    'type': 'bigint'},
                                                   {'comment': 'Encounter identifier',
                                                    'name': 'ENCNTR_ID',
                                                    'nullable': True,
                                                    'type': 'bigint'},
                                                   {'comment': 'Patient identifier',
                                                    'name': 'PERSON_ID',
                                                    'nullable': True,
                                                    'type': 'bigint'},
                                                   {'comment': 'Primary surgical procedure '
                                                               'description',
                                                    'name': 'PRIMARY_PROCEDURE',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Procedure catalog code',
                                                    'name': 'PROCEDURE_CAT',
                                                    'nullable': True,
                                                    'type': 'bigint'},
                                                   {'comment': 'Personnel who performed/recorded '
                                                               'the implant',
                                                    'name': 'PERFORMED_PRSNL_ID',
                                                    'nullable': True,
                                                    'type': 'bigint'},
                                                   {'comment': 'Clinically significant update '
                                                               'datetime (implant date)',
                                                    'name': 'CLINSIG_UPDT_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Manufacturer catalogue/part number',
                                                    'name': 'CATALOGUE_NUMBER',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Compatibility confirmation',
                                                    'name': 'COMPATIBILITY',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Manufacturer confirmation',
                                                    'name': 'CONFIRM_MANUFACTURER',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Sterility confirmation',
                                                    'name': 'CONFIRM_STERILITY',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Device expiration date',
                                                    'name': 'EXPIRY_DATE',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Free text description of the '
                                                               'implant device',
                                                    'name': 'IMPLANT_DESCRIPTION',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Implant manufacturer name',
                                                    'name': 'MANUFACTURER',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Number of devices implanted',
                                                    'name': 'QUANTITY',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Scrub nurse confirmation',
                                                    'name': 'SCRUB_NURSE_CONFIRMS',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Scrub nurse read confirmation',
                                                    'name': 'SCRUB_NURSE_READ',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Device serial number',
                                                    'name': 'SERIAL_NUMBER',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Laterality (left/right/bilateral)',
                                                    'name': 'SIDE_OF_PROCEDURE',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Device size',
                                                    'name': 'SIZE',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Device sterilization date',
                                                    'name': 'STERILISATION_DATE',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Surgeon confirmation',
                                                    'name': 'SURGEON_CONFIRM',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'GS1 UDI Device Identifier (UDI-DI)',
                                                    'name': 'GS1_IDENTIFIER',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'GS1 production date from barcode',
                                                    'name': 'GS1_PRODUCTION_DATE',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'GS1 expiry date from barcode',
                                                    'name': 'GS1_EXPIRY_DATE',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'GS1 batch/lot number from barcode',
                                                    'name': 'GS1_BATCH_NUMBER',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'GS1 serial number from barcode '
                                                               '(UDI-PI)',
                                                    'name': 'GS1_SERIAL_NUMBER',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'HIBCC UDI Device Identifier',
                                                    'name': 'HIBCC_DEVICE_ID',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'HIBCC Production Identifier',
                                                    'name': 'HIBCC_PROD_ID',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Timestamp of last update',
                                                    'name': 'ADC_UPDT',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'SNOMED CT concept ID for the '
                                                               'device',
                                                    'name': 'SNOMED_DEVICE_CONCEPT_ID',
                                                    'nullable': True,
                                                    'type': 'int'},
                                                   {'comment': 'SNOMED CT concept name for the '
                                                               'device',
                                                    'name': 'SNOMED_DEVICE_CONCEPT_NAME',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Device type category from SNOMED '
                                                               'hierarchy',
                                                    'name': 'DEVICE_TYPE',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Confidence score of the device '
                                                               'mapping (0-1)',
                                                    'name': 'MAPPING_CONFIDENCE',
                                                    'nullable': True,
                                                    'type': 'double'}],
                                       'keys': ['EVENT_ID'],
                                       'table_name': 'map_implant_details'},
 '4_prod.bronze.map_mat_birth': {'columns': [{'comment': 'The unique identifier for the mother.',
                                              'name': 'MotherPerson_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'The unique identifier allocated to each '
                                                         'Pregnancy Episode.',
                                              'name': 'Pregnancy_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'Internal identifier for the baby.',
                                              'name': 'BabyPerson_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'MRN for the baby.',
                                              'name': 'Baby_MRN',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'NHS Number for the baby.',
                                              'name': 'Baby_NHS',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Order in which this baby was born for '
                                                         'this labor.',
                                              'name': 'BirthOrder',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Total number of babies born this labor.',
                                              'name': 'BirthNumber',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Total number of fetus during the '
                                                         'pregnancy.',
                                              'name': 'FetusNumber',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Location code for the birth.',
                                              'name': 'BirthLocation_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Location description for the birth.',
                                              'name': 'BirthLocation_DESC',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Date and time of the birth.',
                                              'name': 'BirthDateTime',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Delivery method code.',
                                              'name': 'DeliveryMethod_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Description of the method of delivery.',
                                              'name': 'DeliveryMethod_DESC',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Delivery outcome code.',
                                              'name': 'DeliveryOutcome_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Description of the outcome of delivery.',
                                              'name': 'DeliveryOutcome_DESC',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Neonatal outcome code.',
                                              'name': 'NeonatalOutcome_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Description of the outcome of birth.',
                                              'name': 'NeonatalOutcome_DESC',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Pregnancy outcome code.',
                                              'name': 'PregOutcome_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Pregnancy outcome description.',
                                              'name': 'PregOutcome_DESC',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Presentation at delivery code.',
                                              'name': 'PresDel_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Description of the presentation at '
                                                         'delivery, e.g., vertex.',
                                              'name': 'PresDel_DESC',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Gestation in weeks.',
                                              'name': 'GestationWeeks',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Gestation in additional days.',
                                              'name': 'GestationDays',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Weight of baby.',
                                              'name': 'BirthWeight',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Sex of baby.',
                                              'name': 'BirthSex',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'APGAR score at 1 minute.',
                                              'name': 'APGAR1Min',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'APGAR score at 5 minutes.',
                                              'name': 'APGAR5Min',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Method of feeding.',
                                              'name': 'FeedingMethod',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Any congenital anomalies recorded.',
                                              'name': 'CongenitalAnomalies',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Details of any complications for the '
                                                         'mother.',
                                              'name': 'MotherComplications',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Details of any complications for the '
                                                         'baby.',
                                              'name': 'FetalComplications',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Details of any neonatal complications.',
                                              'name': 'NeonatalComplications',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Resuscitation method if applicable.',
                                              'name': 'ResMethod',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Marital status of the mother at the time '
                                                         'of the pregnancy.',
                                              'name': 'MaritalStatusMother',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Timestamp of last update.',
                                              'name': 'ADC_UPDT',
                                              'nullable': True,
                                              'type': 'timestamp'}],
                                 'keys': ['Pregnancy_ID', 'BirthOrder'],
                                 'table_name': 'map_mat_birth'},
 '4_prod.bronze.map_mat_pregnancy': {'columns': [{'comment': 'The unique identifier for the '
                                                             'mother.',
                                                  'name': 'Person_ID',
                                                  'nullable': False,
                                                  'type': 'bigint'},
                                                 {'comment': 'The unique identifier allocated to '
                                                             'each Pregnancy Episode.',
                                                  'name': 'Pregnancy_ID',
                                                  'nullable': True,
                                                  'type': 'bigint'},
                                                 {'comment': 'Unique local identifier to identify '
                                                             'the person.',
                                                  'name': 'MRN',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'The NHS NUMBER, the primary '
                                                             'identifier of a PERSON, is a unique '
                                                             'identifier for a PATIENT within the '
                                                             'NHS in England and Wales. Based on '
                                                             'this field we identify the COHORT '
                                                             'patients from the DWH.',
                                                  'name': 'NHS_Number',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Date of Birth for the mother.',
                                                  'name': 'Mother_DOB',
                                                  'nullable': True,
                                                  'type': 'timestamp'},
                                                 {'comment': 'The total number of times a woman '
                                                             'has been pregnant.',
                                                  'name': 'Gravida_NBR',
                                                  'nullable': True,
                                                  'type': 'double'},
                                                 {'comment': 'The number of pregnancies that have '
                                                             'resulted in the birth of one or more '
                                                             'viable (living or stillborn) '
                                                             'infants.',
                                                  'name': 'Parity',
                                                  'nullable': True,
                                                  'type': 'double'},
                                                 {'comment': 'Number of previous livebirths.',
                                                  'name': 'PrevLiveBirth_NBR',
                                                  'nullable': True,
                                                  'type': 'double'},
                                                 {'comment': 'Number of previous miscarriages.',
                                                  'name': 'PrevMiscarriages_NBR',
                                                  'nullable': True,
                                                  'type': 'double'},
                                                 {'comment': 'Number of previous stillbirths.',
                                                  'name': 'PrevStillBirth_NBR',
                                                  'nullable': True,
                                                  'type': 'double'},
                                                 {'comment': 'Date of first antenatal appointment.',
                                                  'name': 'FirstAntenatalAPPTDate',
                                                  'nullable': True,
                                                  'type': 'timestamp'},
                                                 {'comment': 'Gestational age at start of '
                                                             'pregnancy record (weeks).',
                                                  'name': 'GestAgePregStart',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Gestational age at end of pregnancy '
                                                             '(weeks).',
                                                  'name': 'GestAgePregEnd',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Date on which last menstrual period '
                                                             'began.',
                                                  'name': 'LastMensPeriodDate',
                                                  'nullable': True,
                                                  'type': 'timestamp'},
                                                 {'comment': 'Units of alcohol per week.',
                                                  'name': 'AlcoholUnitsPerWeek',
                                                  'nullable': True,
                                                  'type': 'double'},
                                                 {'comment': 'Smoking status at booking code.',
                                                  'name': 'SmokingBooking_CD',
                                                  'nullable': True,
                                                  'type': 'int'},
                                                 {'comment': 'Smoking status at booking '
                                                             'description.',
                                                  'name': 'SmokingBooking_DESC',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Smoking status at delivery code.',
                                                  'name': 'SmokingDelivery_CD',
                                                  'nullable': True,
                                                  'type': 'int'},
                                                 {'comment': 'Smoking status at delivery '
                                                             'description.',
                                                  'name': 'SmokingDelivery_DESC',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Substance use code.',
                                                  'name': 'SubstanceUse_CD',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Description of substance use.',
                                                  'name': 'SubstanceUse_DESC',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Expected delivery date.',
                                                  'name': 'ExpectedDeliveryDate',
                                                  'nullable': True,
                                                  'type': 'timestamp'},
                                                 {'comment': 'Height in cm.',
                                                  'name': 'Height_CM',
                                                  'nullable': True,
                                                  'type': 'float'},
                                                 {'comment': 'Weight in kg.',
                                                  'name': 'Weight_KG',
                                                  'nullable': True,
                                                  'type': 'float'},
                                                 {'comment': 'BMI.',
                                                  'name': 'BMI',
                                                  'nullable': True,
                                                  'type': 'float'},
                                                 {'comment': 'Folic acid supplement code during '
                                                             'pregnancy.',
                                                  'name': 'FolicAcidSupp_CD',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Description of folic acid supplement '
                                                             'usage.',
                                                  'name': 'FolicAcidSupp_DESC',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Labor onset method code.',
                                                  'name': 'LaborOnsetMethod_CD',
                                                  'nullable': True,
                                                  'type': 'int'},
                                                 {'comment': 'Method for labor onset.',
                                                  'name': 'LaborOnsetMethod_DESC',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Augmentation code if labor was '
                                                             'augmented.',
                                                  'name': 'Augmentation_CD',
                                                  'nullable': True,
                                                  'type': 'int'},
                                                 {'comment': 'Description of augmentation.',
                                                  'name': 'Augmentation_DESC',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Analgesia code used during delivery.',
                                                  'name': 'AnalgesiaDelivery_CD',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Details of analgesia used during '
                                                             'delivery.',
                                                  'name': 'AnalgesiaDelivery_DESC',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Analgesia code used during labor.',
                                                  'name': 'AnalgesiaLabour_CD',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Details of analgesia used during '
                                                             'labor.',
                                                  'name': 'AnalgesiaLabour_DESC',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Anaesthesia code used during labor.',
                                                  'name': 'AnaesthesiaLabour_CD',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Details of anaesthesia used during '
                                                             'labor.',
                                                  'name': 'AnaesthesiaLabour_DESC',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Perineal trauma code.',
                                                  'name': 'PerinealTrauma_CD',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Details of any perineal trauma from '
                                                             'delivery.',
                                                  'name': 'PerinealTrauma_DESC',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Episiotomy code.',
                                                  'name': 'Episiotomy_CD',
                                                  'nullable': True,
                                                  'type': 'int'},
                                                 {'comment': 'Details of any episiotomy performed.',
                                                  'name': 'Episiotomy_DESC',
                                                  'nullable': True,
                                                  'type': 'string'},
                                                 {'comment': 'Amount of blood lost.',
                                                  'name': 'BloodLoss',
                                                  'nullable': True,
                                                  'type': 'float'},
                                                 {'comment': 'Timestamp of last update.',
                                                  'name': 'ADC_UPDT',
                                                  'nullable': True,
                                                  'type': 'timestamp'}],
                                     'keys': ['Person_ID', 'Pregnancy_ID'],
                                     'table_name': 'map_mat_pregnancy'},
 '4_prod.bronze.map_mat_vte_assessment': {'columns': [{'comment': 'Unique identifier for the '
                                                                  'pregnancy.',
                                                       'name': 'Pregnancy_ID',
                                                       'nullable': True,
                                                       'type': 'bigint'},
                                                      {'comment': 'Unique identifier for the '
                                                                  'person.',
                                                       'name': 'PERSON_ID',
                                                       'nullable': True,
                                                       'type': 'bigint'},
                                                      {'comment': 'Identifier for the Obstetric '
                                                                  'VTE Risk Assessment.',
                                                       'name': 'Event_ID',
                                                       'nullable': True,
                                                       'type': 'bigint'},
                                                      {'comment': 'Identifier for the encounter.',
                                                       'name': 'ENCNTR_ID',
                                                       'nullable': True,
                                                       'type': 'bigint'},
                                                      {'comment': 'Date of submission of the form.',
                                                       'name': 'FormDate',
                                                       'nullable': True,
                                                       'type': 'timestamp'},
                                                      {'comment': 'Section description.',
                                                       'name': 'Section',
                                                       'nullable': True,
                                                       'type': 'string'},
                                                      {'comment': 'Assessment element captured in '
                                                                  'the Obstetric VTE Risk '
                                                                  'Assessment.',
                                                       'name': 'Element',
                                                       'nullable': True,
                                                       'type': 'string'},
                                                      {'comment': 'Response for each element in '
                                                                  'the Obstetric VTE Risk '
                                                                  'Assessment.',
                                                       'name': 'Response',
                                                       'nullable': True,
                                                       'type': 'string'},
                                                      {'comment': 'The provider associated with '
                                                                  'the assessment record.',
                                                       'name': 'PERFORMED_PRSNL_ID',
                                                       'nullable': True,
                                                       'type': 'string'},
                                                      {'comment': 'Date of update of the record',
                                                       'name': 'ADC_UPDT',
                                                       'nullable': True,
                                                       'type': 'timestamp'}],
                                          'keys': ['Pregnancy_ID',
                                                   'PERSON_ID',
                                                   'ENCNTR_ID',
                                                   'Event_ID',
                                                   'Element'],
                                          'table_name': 'map_mat_vte_assessment'},
 '4_prod.bronze.map_med_admin': {'columns': [{'comment': 'This is the value of the unique primary '
                                                         'identifier of the person table. It is an '
                                                         'internal system assigned number.',
                                              'name': 'PERSON_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'This is the value of the unique primary '
                                                         'identifier of the encounter table. It is '
                                                         'an internal system assigned number.',
                                              'name': 'ENCNTR_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'This is the value of the unique primary '
                                                         'identifier of the Event Table.',
                                              'name': 'EVENT_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'This is the value of the unique primary '
                                                         'identifier of the Order Table.',
                                              'name': 'ORDER_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'Identifies what type of event was '
                                                         'audited. Values can be code_value for '
                                                         'TASKPURGED, TASKCOMPLETE, NOTDONE, or '
                                                         'NOTGIVEN from code_set 4000040.',
                                              'name': 'EVENT_TYPE_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Display value for the code.',
                                              'name': 'EVENT_TYPE_DISPLAY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Result status code. Valid values: '
                                                         'authenticated, unauthenticated, unknown, '
                                                         'cancelled, pending, in lab, active, '
                                                         'modified, superseded, transcribed, not '
                                                         'done.',
                                              'name': 'RESULT_STATUS_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Display value for the code.',
                                              'name': 'RESULT_STATUS_DISPLAY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The time at which this medication '
                                                         'administration became active for '
                                                         'continuous administrations. For '
                                                         'intermittent, it is the time the '
                                                         'administration happened.',
                                              'name': 'ADMIN_START_DT_TM',
                                              'nullable': True,
                                              'type': 'timestamp'},
                                             {'comment': 'For continuous administrations, this '
                                                         'field is the end of the time period in '
                                                         'which this administration was active. If '
                                                         'the administration is currently active, '
                                                         'this field will be NULL. For '
                                                         'intermittent administrations, this field '
                                                         'does not apply.',
                                              'name': 'ADMIN_END_DT_TM',
                                              'nullable': True,
                                              'type': 'timestamp'},
                                             {'comment': 'Identifier for the underlying concept, '
                                                         'two values having the same synonmid_id '
                                                         'means the different orders are synonyms '
                                                         'for each other.',
                                              'name': 'ORDER_SYNONYM_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'Unique identifier for the order from the '
                                                         'Order Catalogue.',
                                              'name': 'ORDER_CKI',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The Multum drug code associated with the '
                                                         'medication order.',
                                              'name': 'MULTUM',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The RxNorm Concept Unique Identifier '
                                                         '(CUI) associated with the medication '
                                                         'order.',
                                              'name': 'RXNORM_CUI',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Provides additional context by '
                                                         'displaying the description or name of '
                                                         'the medication corresponding to the '
                                                         'RxNorm code.',
                                              'name': 'RXNORM_STR',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The Snomed concept ID(SCTID). Sourced '
                                                         'either from direct RxNorm mapping or '
                                                         'derived from the OMOP Standard Concept.',
                                              'name': 'SNOMED_CODE',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The description of the SNOMED Code.',
                                              'name': 'SNOMED_STR',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Text description of the Order. The '
                                                         'mnemonic mostly used by department '
                                                         'personnel.',
                                              'name': 'ORDER_MNEMONIC',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Any additional free text information '
                                                         'describing the order.',
                                              'name': 'ORDER_DETAIL',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Strength of the order ingredient as a '
                                                         'number.',
                                              'name': 'ORDER_STRENGTH',
                                              'nullable': True,
                                              'type': 'float'},
                                             {'comment': 'Code for the unit of measure for the '
                                                         'strength of the order ingredient.',
                                              'name': 'ORDER_STRENGTH_UNIT_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'Text description of the unit of measure '
                                                         'for the strength of the order '
                                                         'ingredient.',
                                              'name': 'ORDER_STRENGTH_UNIT_DISPLAY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Volume of the order ingredient as a '
                                                         'number.',
                                              'name': 'ORDER_VOLUME',
                                              'nullable': True,
                                              'type': 'float'},
                                             {'comment': 'Code for the unit of measure for the '
                                                         'volume of the order ingredient.',
                                              'name': 'ORDER_VOLUME_UNIT_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'Text description of the unit of measure '
                                                         'for the volume of the order incredient.',
                                              'name': 'ORDER_VOLUME_UNIT_DISPLAY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Code for the method of administration of '
                                                         'the medication.',
                                              'name': 'ADMIN_ROUTE_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Text description of the administration '
                                                         'route, e.g. intravenous',
                                              'name': 'ADMIN_ROUTE_DISPLAY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Initial volume or quantity of the '
                                                         'administered dose.',
                                              'name': 'INITIAL_DOSAGE',
                                              'nullable': True,
                                              'type': 'float'},
                                             {'comment': 'Code for the unit of measurement of the '
                                                         'initial dosage.',
                                              'name': 'INITIAL_DOSAGE_UNIT_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Text description of the unit of '
                                                         'measurement of the initial dosage.',
                                              'name': 'INITIAL_DOSAGE_UNIT_DISPLAY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Actual volume or quantity of '
                                                         'administration.',
                                              'name': 'ADMIN_DOSAGE',
                                              'nullable': True,
                                              'type': 'float'},
                                             {'comment': 'Code for the unit of measurement for '
                                                         'dosage.',
                                              'name': 'ADMIN_DOSAGE_UNIT_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Text description of the unit of '
                                                         'measurement for dosage.',
                                              'name': 'ADMIN_DOSAGE_UNIT_DISPLAY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Total volume medication and diluent at '
                                                         'the beginning of the administration.',
                                              'name': 'INITIAL_VOLUME',
                                              'nullable': True,
                                              'type': 'float'},
                                             {'comment': 'The volume at any one point in time that '
                                                         'remains in the IV Bag.',
                                              'name': 'INFUSED_VOLUME',
                                              'nullable': True,
                                              'type': 'float'},
                                             {'comment': 'Code for the unit of measure for infused '
                                                         'volume.',
                                              'name': 'INFUSED_VOLUME_UNIT_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Text description of the unit of measure '
                                                         'for infused volume.',
                                              'name': 'INFUSED_VOLUME_UNIT_DISPLAY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'For continuously administered '
                                                         'medications, IV or IVP, the infusion '
                                                         'rate and unit is used to capture the '
                                                         'flow rate of the medication into the '
                                                         'patient.',
                                              'name': 'INFUSION_RATE',
                                              'nullable': True,
                                              'type': 'float'},
                                             {'comment': 'Code for the unit of measure for volume '
                                                         'or quantity of the medication. i.e. ml, '
                                                         'drip, tablet.',
                                              'name': 'INFUSION_UNIT_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Text description of the unit of measure '
                                                         'for volume or quantity of the '
                                                         'medication.',
                                              'name': 'INFUSION_UNIT_DISPLAY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Code for the nurse unit of the device '
                                                         'the user is using to enter the '
                                                         'medication admin event.',
                                              'name': 'NURSE_UNIT_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'The text description of the nurse unit '
                                                         'of the device the user is using to enter '
                                                         'the medication admin event.',
                                              'name': 'NURSE_UNIT_DISPLAY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Code for the position used to determine '
                                                         'the applications and tasks the personnel '
                                                         'is authorized to use.',
                                              'name': 'POSITION_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'Text description of the position used to '
                                                         'determine the applications and tasks the '
                                                         'personnel is authorized to use.',
                                              'name': 'POSITION_DISPLAY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The ID of the user documenting the '
                                                         'medication admin event.',
                                              'name': 'PRSNL_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'Last update timestamp',
                                              'name': 'ADC_UPDT',
                                              'nullable': True,
                                              'type': 'timestamp'},
                                             {'comment': 'The standardized medication dose '
                                                         'expressed in milligrams (mg).',
                                              'name': 'DOSE_IN_MG',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The standardized medication dose '
                                                         'expressed in milliliters (mL).',
                                              'name': 'DOSE_IN_ML',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'It classifies the medication dose unit '
                                                         'as weight-based (mg), volume-based (mL), '
                                                         'units, discrete forms, or other.',
                                              'name': 'DOSE_UNIT_CATEGORY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Indicates the origin of the SNOMED code. '
                                                         "Values: 'Direct Map' (via RxNorm) or "
                                                         "'Derived from OMOP' (reverse lookup from "
                                                         'Standard Concept).',
                                              'name': 'SNOMED_SOURCE',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The Standard OMOP Concept ID for the '
                                                         'medication ingredient.',
                                              'name': 'OMOP_CONCEPT_ID',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'The name of the Standard OMOP Concept.',
                                              'name': 'OMOP_CONCEPT_NAME',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': "Flag 'S' indicating this is a Standard "
                                                         'Concept.',
                                              'name': 'OMOP_STANDARD_CONCEPT',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': "Method of mapping. Values: 'Standard "
                                                         "Map' (direct code match), 'Vector "
                                                         "Similarity' (embedding match >= 0.6), or "
                                                         "'Unmapped'.",
                                              'name': 'OMOP_TYPE',
                                              'nullable': True,
                                              'type': 'string'}],
                                 'keys': ['EVENT_ID'],
                                 'table_name': 'map_med_admin'},
 '4_prod.bronze.map_medical_personnel': {'columns': [{'comment': 'This is the value of the unique '
                                                                 'primary identifier of the person '
                                                                 'table. It is an internal system '
                                                                 'assigned number.',
                                                      'name': 'PERSON_ID',
                                                      'nullable': True,
                                                      'type': 'double'},
                                                     {'comment': 'Set to TRUE, if the personnel is '
                                                                 'a physician.  Otherwise, set to '
                                                                 'FALSE.',
                                                      'name': 'PHYSICIAN_IND',
                                                      'nullable': True,
                                                      'type': 'double'},
                                                     {'comment': 'The position is used to '
                                                                 'determine the applications and '
                                                                 'tasks the personnel is '
                                                                 'authorized to use.',
                                                      'name': 'POSITION_CD',
                                                      'nullable': True,
                                                      'type': 'double'},
                                                     {'comment': 'The description for the code '
                                                                 'value.',
                                                      'name': 'position_name',
                                                      'nullable': True,
                                                      'type': 'string'},
                                                     {'comment': 'This field is the current '
                                                                 'patient location with a location '
                                                                 'type of nurse unit.',
                                                      'name': 'primary_care_site_cd',
                                                      'nullable': True,
                                                      'type': 'double'},
                                                     {'comment': 'The display string for the '
                                                                 'code_value',
                                                      'name': 'primary_care_site_name',
                                                      'nullable': True,
                                                      'type': 'string'},
                                                     {'comment': 'The groups of service category.',
                                                      'name': 'SRVCATEGORY',
                                                      'nullable': True,
                                                      'type': 'string'},
                                                     {'comment': 'The groups of surgical '
                                                                 'specialty.',
                                                      'name': 'SURGSPEC',
                                                      'nullable': True,
                                                      'type': 'string'},
                                                     {'comment': 'The groups of medical service '
                                                                 'lines.',
                                                      'name': 'MEDSERVICE',
                                                      'nullable': True,
                                                      'type': 'string'},
                                                     {'comment': 'Timestamp of last update.',
                                                      'name': 'ADC_UPDT',
                                                      'nullable': True,
                                                      'type': 'timestamp'}],
                                         'keys': ['PERSON_ID'],
                                         'table_name': 'map_medical_personnel'},
 '4_prod.bronze.map_nomen_events': {'columns': [{'comment': 'The unique primary identifier of the '
                                                            'Event Table.',
                                                 'name': 'EVENT_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'This is the value of the unique '
                                                            'primary identifier of the encounter '
                                                            'table. It is an internal system '
                                                            'assigned number.',
                                                 'name': 'ENCNTR_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'This is the value of the unique '
                                                            'primary identifier of the person '
                                                            'table. It is an internal system '
                                                            'assigned number.',
                                                 'name': 'PERSON_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'The unique primary identifier of '
                                                            'Order Table.',
                                                 'name': 'ORDER_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'This is the value of the unique '
                                                            'primary identifier of the '
                                                            'nomenclature table. It is an internal '
                                                            'system assigned number.',
                                                 'name': 'NOMENCLATURE_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'Personnel id of provider who '
                                                            'performed this result.',
                                                 'name': 'PERFORMED_PRSNL_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'The title for document results.',
                                                 'name': 'EVENT_TITLE_TEXT',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'It is the code that identifies the '
                                                            'most basic unit of the storage, i.e. '
                                                            'RBC, discharge summary, image.',
                                                 'name': 'EVENT_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'EVENT_CD_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Foreign key to the order_catalog '
                                                            'table. Catalog_cd does not exist in '
                                                            'the code_value table and does not '
                                                            'have a code set.',
                                                 'name': 'CATALOG_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description of the Orderable',
                                                 'name': 'CATALOG_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Used to store the internal code for '
                                                            'the catalog type. Used as a filtering '
                                                            'mechanism for rows on theorder '
                                                            'catalog table.',
                                                 'name': 'CATALOG_TYPE_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'CATALOG_TYPE_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Coded value which specifies how the '
                                                            'event is stored in and retrieved from '
                                                            "the event table's sub-tables. For "
                                                            'example, Event_Class_CDs identify '
                                                            'events as numeric results, textual '
                                                            'results, calculations, medications, '
                                                            'etc.',
                                                 'name': 'EVENT_CLASS_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'Contributor system identifies the '
                                                            'source feed of data from which a row '
                                                            'was populated.  This is mainly used '
                                                            'to determine how to update a set of '
                                                            'data that may have originated from '
                                                            'more than one source feed.',
                                                 'name': 'CONTRIBUTOR_SYSTEM_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'CONTRIBUTOR_SYSTEM_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The combination of the reference nbr '
                                                            'and the contributor system code '
                                                            'provides a unique identifier to the '
                                                            'origin of the data.',
                                                 'name': 'REFERENCE_NBR',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Provides a mechanism for logical '
                                                            'grouping of events.  i.e. supergroup '
                                                            'and group tests.  Same as event_id if '
                                                            'current row is the highest level '
                                                            'parent.',
                                                 'name': 'PARENT_EVENT_ID',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'States whether the result is normal.  '
                                                            'This can be used to determine whether '
                                                            'to display the event tag in different '
                                                            'color on the flowsheet. For group '
                                                            'results, this represents an overall '
                                                            'normalcy. i.e. Is any result in the '
                                                            'group abnormal?  Also allows '
                                                            'different purge criteria to be '
                                                            'applied based on result.',
                                                 'name': 'NORMALCY_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'NORMALCY_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Used to identify the method in which '
                                                            'a result was entered.',
                                                 'name': 'ENTRY_MODE_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value',
                                                 'name': 'ENTRY_MODE_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Date this result was performed (or '
                                                            'authored).',
                                                 'name': 'PERFORMED_DT_TM',
                                                 'nullable': True,
                                                 'type': 'timestamp'},
                                                {'comment': 'Represents the update date/time that '
                                                            'tracks when clinically significant '
                                                            'updates are made to the Clinical '
                                                            'Event and should only be used to '
                                                            'check for updates. This field is used '
                                                            'to notify audiences when a clinically '
                                                            'significant update is made to an '
                                                            'existing clinical event, such as when '
                                                            'XR Clinical Reporting re-prints a lab '
                                                            'result due to an update of the result '
                                                            'value or when a result is resent to a '
                                                            "provider's Message Center with the "
                                                            'result update. This date should NOT '
                                                            'be displayed as the clinically.',
                                                 'name': 'CLINSIG_UPDT_DT_TM',
                                                 'nullable': True,
                                                 'type': 'timestamp'},
                                                {'comment': 'The title for document results.',
                                                 'name': 'PARENT_EVENT_TITLE_TEXT',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The code value of the parent event.',
                                                 'name': 'PARENT_EVENT_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'PARENT_EVENT_CD_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The internal code for the order '
                                                            'catalog item of the parent event.',
                                                 'name': 'PARENT_CATALOG_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description of the Orderable.',
                                                 'name': 'PARENT_CATALOG_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The internal code for the catalog '
                                                            'type of the parent event.',
                                                 'name': 'PARENT_CATALOG_TYPE_CD',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'PARENT_CATALOG_TYPE_DISPLAY',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The combination of the reference nbr '
                                                            'and the contributor system code '
                                                            'provides a unique identifier to the '
                                                            'origin of the data.',
                                                 'name': 'PARENT_REFERENCE_NBR',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The code, or key, from the source '
                                                            'vocabulary that contributed the '
                                                            'string to the nomenclature.',
                                                 'name': 'SOURCE_IDENTIFIER',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Variable length string that may '
                                                            'include alphanumeric characters and '
                                                            'punctuation.',
                                                 'name': 'SOURCE_STRING',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The external vocabulary or lexicon '
                                                            'that contributed the string, e.g. '
                                                            'ICD9, SNOMED, etc.',
                                                 'name': 'SOURCE_VOCABULARY_CD',
                                                 'nullable': True,
                                                 'type': 'decimal(38,18)'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'SOURCE_VOCABULARY_DESC',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Vocabulary AXIS codes related to '
                                                            'SNOMEDColumn.',
                                                 'name': 'VOCAB_AXIS_CD',
                                                 'nullable': True,
                                                 'type': 'decimal(38,18)'},
                                                {'comment': 'The description for the code value.',
                                                 'name': 'VOCAB_AXIS_DESC',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Concept CKI is the functional Concept '
                                                            'Identifier; it is the codified means '
                                                            'within Millennium to identify key '
                                                            'medical concepts to support '
                                                            'information processing, clinical '
                                                            'decision support, executable '
                                                            'knowledge and knowledge presentation. '
                                                            'Composed of a source and an '
                                                            'identifier.  For example, if the '
                                                            'concept source is SNOMED and the '
                                                            'concept identifier is 123.',
                                                 'name': 'CONCEPT_CKI',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'A unique identifier for each Concept '
                                                            'across all domains.',
                                                 'name': 'OMOP_CONCEPT_ID',
                                                 'nullable': True,
                                                 'type': 'int'},
                                                {'comment': 'An unambiguous, meaningful and '
                                                            'descriptive name for the Concept.',
                                                 'name': 'OMOP_CONCEPT_NAME',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'This flag determines where a Concept '
                                                            'is a Standard Concept, i.e. is used '
                                                            'in the data, a Classification '
                                                            'Concept, or a non-standard Source '
                                                            'Concept. The allowables values are S '
                                                            '(Standard Concept) and C '
                                                            '(Classification Concept), otherwise '
                                                            'the content is NULL.',
                                                 'name': 'OMOP_STANDARD_CONCEPT',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The number of OMOP concepts matched '
                                                            'for each NOMENCLATURE_ID.',
                                                 'name': 'OMOP_MATCH_NUMBER',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'A unique identifier for each domain.',
                                                 'name': 'OMOP_CONCEPT_DOMAIN',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The attribute or concept class of the '
                                                            'Concept.',
                                                 'name': 'OMOP_CONCEPT_CLASS',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': None,
                                                 'name': 'SNOMED_CODE',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'The method or source of the SNOMED '
                                                            'code mapping for each nomenclature '
                                                            'entry.',
                                                 'name': 'SNOMED_TYPE',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The number of matches found for each '
                                                            'NOMENCLATURE_ID in the context of '
                                                            'SNOMED codes.',
                                                 'name': 'SNOMED_MATCH_NUMBER',
                                                 'nullable': True,
                                                 'type': 'bigint'},
                                                {'comment': 'The term associated with a SNOMED '
                                                            'code that provides additional meaning '
                                                            'and context to the code.',
                                                 'name': 'SNOMED_TERM',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'Timestamp of last update.',
                                                 'name': 'ADC_UPDT',
                                                 'nullable': True,
                                                 'type': 'timestamp'},
                                                {'comment': 'The name of the OMOP Common Data '
                                                            'Model table.',
                                                 'name': 'OMOP_MANUAL_TABLE',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The field of the OMOP Common Data '
                                                            'Model table.',
                                                 'name': 'OMOP_MANUAL_COLUMN',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The concept_id of the OMOP Common '
                                                            'Data Model table.',
                                                 'name': 'OMOP_MANUAL_CONCEPT',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The manually mapped OMOP concept ID '
                                                            'representing the value of a text for '
                                                            'standardized use in the OMOP CDM.',
                                                 'name': 'OMOP_MANUAL_VALUE_CONCEPT',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The name or description of the OMOP '
                                                            'concept id.',
                                                 'name': 'OMOP_MANUAL_CONCEPT_NAME',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'This flag determines where a Concept '
                                                            'is a Standard Concept, i.e. is used '
                                                            'in the data, a Classification '
                                                            'Concept, or a non-standard Source '
                                                            'Concept. The allowables values are S '
                                                            '(Standard Concept) and C '
                                                            '(Classification Concept), otherwise '
                                                            'the content is NULL.',
                                                 'name': 'OMOP_MANUAL_STANDARD_CONCEPT',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'A unique identifier for the domain.',
                                                 'name': 'OMOP_MANUAL_CONCEPT_DOMAIN',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'The identifier for the class or '
                                                            'category of the OMOP concept.',
                                                 'name': 'OMOP_MANUAL_CONCEPT_CLASS',
                                                 'nullable': True,
                                                 'type': 'string'},
                                                {'comment': 'COSIGN Similarity between the Source '
                                                            'term and the OMOP Term.',
                                                 'name': 'OMOP_SIMILARITY',
                                                 'nullable': True,
                                                 'type': 'double'},
                                                {'comment': 'COSIGN Similarity between the Source '
                                                            'term and the SNOMED Term.',
                                                 'name': 'SNOMED_SIMILARITY',
                                                 'nullable': True,
                                                 'type': 'double'}],
                                    'keys': ['EVENT_ID'],
                                    'table_name': 'map_nomen_events'},
 '4_prod.bronze.map_numeric_events': {'columns': [{'comment': 'The unique primary identifier of '
                                                              'the Event Table.',
                                                   'name': 'EVENT_ID',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'This is the value of the unique '
                                                              'primary identifier of the encounter '
                                                              'table. It is an internal system '
                                                              'assigned number.',
                                                   'name': 'ENCNTR_ID',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'This is the value of the unique '
                                                              'primary identifier of the person '
                                                              'table. It is an internal system '
                                                              'assigned number.',
                                                   'name': 'PERSON_ID',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'The unique primary identifier of '
                                                              'Order Table.',
                                                   'name': 'ORDER_ID',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'Coded value which specifies how the '
                                                              'event is stored in and retrieved '
                                                              "from the event table's sub-tables. "
                                                              'For example, Event_Class_CDs '
                                                              'identify events as numeric results, '
                                                              'textual results, calculations, '
                                                              'medications, etc.',
                                                   'name': 'EVENT_CLASS_CD',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'Personnel id of provider who '
                                                              'performed this result.',
                                                   'name': 'PERFORMED_PRSNL_ID',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'The numerical value of the event '
                                                              'result.',
                                                   'name': 'NUMERIC_RESULT',
                                                   'nullable': True,
                                                   'type': 'float'},
                                                  {'comment': 'Unit of measurement for result.',
                                                   'name': 'UNIT_OF_MEASURE_CD',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'The description for the code value.',
                                                   'name': 'UNIT_OF_MEASURE_DISPLAY',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'The title for document results.',
                                                   'name': 'EVENT_TITLE_TEXT',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'It is the code that identifies the '
                                                              'most basic unit of the storage, '
                                                              'i.e. RBC, discharge summary, image.',
                                                   'name': 'EVENT_CD',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'The description for the code value.',
                                                   'name': 'EVENT_CD_DISPLAY',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Foreign key to the order_catalog '
                                                              'table. Catalog_cd does not exist in '
                                                              'the code_value table and does not '
                                                              'have a code set.',
                                                   'name': 'CATALOG_CD',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'The description of the Orderable.',
                                                   'name': 'CATALOG_DISPLAY',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Used to store the internal code for '
                                                              'the catalog type. Used as a '
                                                              'filtering mechanism for rows on '
                                                              'theorder catalog table.',
                                                   'name': 'CATALOG_TYPE_CD',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'The description for the code value.',
                                                   'name': 'CATALOG_TYPE_DISPLAY',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Contributor system identifies the '
                                                              'source feed of data from which a '
                                                              'row was populated.  This is mainly '
                                                              'used to determine how to update a '
                                                              'set of data that may have '
                                                              'originated from more than one '
                                                              'source feed.',
                                                   'name': 'CONTRIBUTOR_SYSTEM_CD',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'The description for the code value',
                                                   'name': 'CONTRIBUTOR_SYSTEM_DISPLAY',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'The combination of the reference '
                                                              'nbr and the contributor system code '
                                                              'provides a unique identifier to the '
                                                              'origin of the data.',
                                                   'name': 'REFERENCE_NBR',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Provides a mechanism for logical '
                                                              'grouping of events.  i.e. '
                                                              'supergroup and group tests.  Same '
                                                              'as event_id if current row is the '
                                                              'highest level parent.',
                                                   'name': 'PARENT_EVENT_ID',
                                                   'nullable': True,
                                                   'type': 'bigint'},
                                                  {'comment': 'States whether the result is '
                                                              'normal.  This can be used to '
                                                              'determine whether to display the '
                                                              'event tag in different color on the '
                                                              'flowsheet. For group results, this '
                                                              'represents an overall normalcy. '
                                                              'i.e. Is any result in the group '
                                                              'abnormal?  Also allows different '
                                                              'purge criteria to be applied based '
                                                              'on result.',
                                                   'name': 'NORMALCY_CD',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'The description for the code value.',
                                                   'name': 'NORMALCY_DISPLAY',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Used to identify the method in '
                                                              'which a result was entered.',
                                                   'name': 'ENTRY_MODE_CD',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'The description for the code value',
                                                   'name': 'ENTRY_MODE_DISPLAY',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Normal low value',
                                                   'name': 'NORMAL_LOW',
                                                   'nullable': True,
                                                   'type': 'float'},
                                                  {'comment': 'Normal high value',
                                                   'name': 'NORMAL_HIGH',
                                                   'nullable': True,
                                                   'type': 'float'},
                                                  {'comment': 'Date this result was performed (or '
                                                              'authored).',
                                                   'name': 'PERFORMED_DT_TM',
                                                   'nullable': True,
                                                   'type': 'timestamp'},
                                                  {'comment': 'Represents the update date/time '
                                                              'that tracks when clinically '
                                                              'significant updates are made to the '
                                                              'Clinical Event and should only be '
                                                              'used to check for updates. This '
                                                              'field is used to notify audiences '
                                                              'when a clinically significant '
                                                              'update is made to an existing '
                                                              'clinical event, such as when XR '
                                                              'Clinical Reporting re-prints a lab '
                                                              'result due to an update of the '
                                                              'result value or when a result is '
                                                              "resent to a provider's Message "
                                                              'Center with the result update. This '
                                                              'date should NOT be displayed as the '
                                                              'clinically.',
                                                   'name': 'CLINSIG_UPDT_DT_TM',
                                                   'nullable': True,
                                                   'type': 'timestamp'},
                                                  {'comment': 'The title associated with the '
                                                              'parent event.',
                                                   'name': 'PARENT_EVENT_TITLE_TEXT',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'The code value of the parent event.',
                                                   'name': 'PARENT_EVENT_CD',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'The description for the code value.',
                                                   'name': 'PARENT_EVENT_CD_DISPLAY',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'The internal code for the order '
                                                              'catalog item of the parent event.',
                                                   'name': 'PARENT_CATALOG_CD',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'The description of the Orderable.',
                                                   'name': 'PARENT_CATALOG_DISPLAY',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'The internal code for the catalog '
                                                              'type of the parent event.',
                                                   'name': 'PARENT_CATALOG_TYPE_CD',
                                                   'nullable': True,
                                                   'type': 'int'},
                                                  {'comment': 'The description for the code value.',
                                                   'name': 'PARENT_CATALOG_TYPE_DISPLAY',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'The combination of the reference '
                                                              'nbr and the contributor system code '
                                                              'provides a unique identifier to the '
                                                              'origin of the data.',
                                                   'name': 'PARENT_REFERENCE_NBR',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'Timestamp of last update.',
                                                   'name': 'ADC_UPDT',
                                                   'nullable': True,
                                                   'type': 'timestamp'},
                                                  {'comment': 'The name of the OMOP Common Data '
                                                              'Model table.',
                                                   'name': 'OMOP_MANUAL_TABLE',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'The field of the OMOP Common Data '
                                                              'Model table.',
                                                   'name': 'OMOP_MANUAL_COLUMN',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'The concept_id of the OMOP Common '
                                                              'Data Model table.',
                                                   'name': 'OMOP_MANUAL_CONCEPT',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'The OMOP concept ID for the unit of '
                                                              'measurement.',
                                                   'name': 'OMOP_MANUAL_UNITS',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'The name or description of the OMOP '
                                                              'concept id.',
                                                   'name': 'OMOP_MANUAL_CONCEPT_NAME',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'This flag determines where a '
                                                              'Concept is a Standard Concept, i.e. '
                                                              'is used in the data, a '
                                                              'Classification Concept, or a '
                                                              'non-standard Source Concept. The '
                                                              'allowables values are S (Standard '
                                                              'Concept) and C (Classification '
                                                              'Concept), otherwise the content is '
                                                              'NULL.',
                                                   'name': 'OMOP_MANUAL_STANDARD_CONCEPT',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'A unique identifier for the domain.',
                                                   'name': 'OMOP_MANUAL_CONCEPT_DOMAIN',
                                                   'nullable': True,
                                                   'type': 'string'},
                                                  {'comment': 'The identifier for the class or '
                                                              'category of the OMOP concept.',
                                                   'name': 'OMOP_MANUAL_CONCEPT_CLASS',
                                                   'nullable': True,
                                                   'type': 'string'}],
                                      'keys': ['EVENT_ID'],
                                      'table_name': 'map_numeric_events'},
 '4_prod.bronze.map_pathology': {'columns': [{'comment': 'linked | raw',
                                              'name': 'source_table',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'EVENT_ID (linked) | TFCResultSeq or '
                                                         'synthetic hash (raw)',
                                              'name': 'source_event_id',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'TRUE when source_event_id is a synthetic '
                                                         'hash (raw rows w/ NULL TFCResultSeq)',
                                              'name': 'is_synthetic_key',
                                              'nullable': True,
                                              'type': 'boolean'},
                                             {'comment': 'LabNo: SUBSTRING(REFERENCE_NBR,1,11) for '
                                                         'linked; resultlevel LabNo for raw',
                                              'name': 'lab_no',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Cerner PERSON_ID; native for linked; raw '
                                                         'branch resolves via MRN alias (type 10), '
                                                         'NHS-number (type 18) fallback (may be '
                                                         'NULL â\x80\x94 unresolved rows kept)',
                                              'name': 'PERSON_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'native for linked; NULL for raw',
                                              'name': 'ENCNTR_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'MRN: mill_person_alias type 10 by '
                                                         'PERSON_ID (canonical); samplelevel MRN '
                                                         'fallback (raw, unresolved)',
                                              'name': 'MRN',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'NHS number: mill_person_alias type 18 by '
                                                         'PERSON_ID (canonical); samplelevel NHSNo '
                                                         'fallback (raw, unresolved)',
                                              'name': 'NHS_Number',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'linked only',
                                              'name': 'EVENT_CD',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'linked: mill_code_value DESCRIPTION of '
                                                         'EVENT_CD; NULL for raw',
                                              'name': 'EVENT_CD_DISPLAY',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'linked: PERFORMED_DT_TM; raw: SampleDT',
                                              'name': 'measurement_datetime',
                                              'nullable': True,
                                              'type': 'timestamp'},
                                             {'comment': 'CERNER_TESTCODE | TFC',
                                              'name': 'code_system',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'test code = map join key: ORDER_MNEMONIC '
                                                         '(linked) | TFCCode (raw)',
                                              'name': 'code',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'test description: code_value DESCRIPTION '
                                                         '(linked) | TFCDesc_Full/Rep/WP (raw)',
                                              'name': 'description',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'SNOMED source code of the '
                                                         'test/measurement concept (concept_code '
                                                         'where vocab=SNOMED; NULL otherwise)',
                                              'name': 'test_snomed_code',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'LOINC source code of the test concept '
                                                         '(concept_code where vocab=LOINC; NULL '
                                                         'otherwise)',
                                              'name': 'test_loinc_code',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'OMOP concept_id of the test concept (== '
                                                         'measurement_concept_id; the OMOP-coded '
                                                         'column, distinct identifier from the '
                                                         'SNOMED/LOINC source code)',
                                              'name': 'test_omop_concept_id',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'standard_concept flag (S/C/NULL) of the '
                                                         'test concept',
                                              'name': 'test_omop_standard_concept',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'vocabulary_id of the test concept '
                                                         '(SNOMED | LOINC)',
                                              'name': 'test_vocabulary_id',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'mapped OMOP test/measurement concept_id '
                                                         '(NULL if unmapped)',
                                              'name': 'measurement_concept_id',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'name of the mapped measurement concept',
                                              'name': 'measurement_concept_name',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'curated | auto_high | auto_low (NULL if '
                                                         'unmapped)',
                                              'name': 'test_confidence_tier',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'numeric result parsed from result_txt '
                                                         '(NULL if non-numeric)',
                                              'name': 'value_as_number',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'OMOP comparator for thresholded '
                                                         'numerics; NULL = equals',
                                              'name': 'operator_concept_id',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'categorical result-value concept; NULL '
                                                         'when the result is numeric',
                                              'name': 'value_as_concept_id',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'name of the mapped result-value concept',
                                              'name': 'result_concept_name',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'curated | auto_high | auto_low | '
                                                         'auto_anchor | auto_value | auto_genpos '
                                                         '(NULL if unmapped)',
                                              'name': 'result_confidence_tier',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': "micro arm: analyzer 'suspected' flag "
                                                         '(trailing-? or hedge language '
                                                         'possible/suggests/presumptive); NULL '
                                                         'unless an organism/suspected result',
                                              'name': 'result_is_suspected',
                                              'nullable': True,
                                              'type': 'boolean'},
                                             {'comment': 'micro arm: analyzer growth grade '
                                                         '(heavy/moderate/scanty/light/++/3+ etc.) '
                                                         'captured off the result text; NULL if '
                                                         'none',
                                              'name': 'result_growth_grade',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'SNOMED source code of the result-value '
                                                         'concept (concept_code where '
                                                         'vocab=SNOMED; NULL otherwise)',
                                              'name': 'result_snomed_code',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'LOINC source code of the result-value '
                                                         'concept (concept_code where vocab=LOINC; '
                                                         'NULL otherwise)',
                                              'name': 'result_loinc_code',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'OMOP concept_id of the result-value '
                                                         'concept (== value_as_concept_id)',
                                              'name': 'result_omop_concept_id',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'standard_concept flag (S/C/NULL) of the '
                                                         'result-value concept',
                                              'name': 'result_omop_standard_concept',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'vocabulary_id of the result-value '
                                                         'concept (SNOMED | LOINC)',
                                              'name': 'result_vocabulary_id',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'numeric | mapped | excluded | free_text',
                                              'name': 'result_status',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'raw result text (result_txt) as received '
                                                         'from source',
                                              'name': 'value_source_value',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'raw unit string: code_value DESCRIPTION '
                                                         'of RESULT_UNITS_CD (linked) | master '
                                                         'Units (raw)',
                                              'name': 'unit_source_value',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'mapped UCUM unit concept_id (via '
                                                         'pathology_unit_map)',
                                              'name': 'unit_concept_id',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'UCUM code for the mapped unit',
                                              'name': 'ucum_code',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'reference-range low: NORMAL_LOW (linked '
                                                         'only; NULL for raw)',
                                              'name': 'range_low',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'reference-range high: NORMAL_HIGH '
                                                         '(linked only; NULL for raw)',
                                              'name': 'range_high',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'normalcy flag: code_value DESCRIPTION of '
                                                         'NORMALCY_CD (linked only; NULL for raw)',
                                              'name': 'normalcy',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'raw branch: LIMS working code (part of '
                                                         'master join key)',
                                              'name': 'WkgCode',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'raw branch: National Lab Medicine '
                                                         'Catalogue code',
                                              'name': 'nlmc_id',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'linked: PERFORMED_DT_TM; raw: '
                                                         'samplelevel ReportDate',
                                              'name': 'ReportDate',
                                              'nullable': True,
                                              'type': 'timestamp'},
                                             {'comment': 'build/load timestamp '
                                                         '(CURRENT_TIMESTAMP() at write); the '
                                                         'incremental watermark column',
                                              'name': 'ADC_UPDT',
                                              'nullable': True,
                                              'type': 'timestamp'}],
                                 'keys': ['source_table',
                                          'source_event_id',
                                          'lab_no',
                                          'code',
                                          'description',
                                          'value_source_value'],
                                 'table_name': 'map_pathology'},
 '4_prod.bronze.map_patient_journey': {'columns': [{'comment': 'Primary unique identifier for this '
                                                               'location history record. Each row '
                                                               "represents one 'stop' in the "
                                                               "patient's journey.",
                                                    'name': 'ENCNTR_LOC_HIST_ID',
                                                    'nullable': True,
                                                    'type': 'bigint'},
                                                   {'comment': 'Unique identifier for the '
                                                               'encounter/visit. Links all '
                                                               'location movements for a single '
                                                               'hospital stay.',
                                                    'name': 'ENCNTR_ID',
                                                    'nullable': True,
                                                    'type': 'bigint'},
                                                   {'comment': 'Unique identifier for the patient '
                                                               'from the PERSON table.',
                                                    'name': 'PERSON_ID',
                                                    'nullable': True,
                                                    'type': 'bigint'},
                                                   {'comment': 'Local Medical Record Number for '
                                                               'the patient.',
                                                    'name': 'MRN',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'NHS Number for the patient (UK '
                                                               'national identifier).',
                                                    'name': 'NHS_NUMBER',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for encounter type (Code Set '
                                                               '71). Categorizes the encounter '
                                                               'into logical groups.',
                                                    'name': 'ENCNTR_TYPE_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of encounter type. '
                                                               "Examples: 'Inpatient', "
                                                               "'Outpatient', 'Emergency', 'Day "
                                                               "Case'.",
                                                    'name': 'ENCNTR_TYPE_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for encounter type class '
                                                               '(Code Set 69). Higher-level '
                                                               'categorization than encounter '
                                                               'type.',
                                                    'name': 'ENCNTR_TYPE_CLASS_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of encounter class. '
                                                               "Standard values: 'Inpatient', "
                                                               "'Outpatient', 'Emergency', "
                                                               "'Recurring Outpatient'.",
                                                    'name': 'ENCNTR_TYPE_CLASS_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for current encounter status '
                                                               '(Code Set 261).',
                                                    'name': 'ENCNTR_STATUS_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of encounter status. '
                                                               "Examples: 'Active', 'Discharged', "
                                                               "'Preadmit', 'Cancelled'.",
                                                    'name': 'ENCNTR_STATUS_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code identifying where the patient '
                                                               'came from before admission (Code '
                                                               'Set 2).',
                                                    'name': 'ADMIT_SRC_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of admission source. '
                                                               "Examples: 'GP Referral', 'Transfer "
                                                               "from Another Hospital', 'A&E'.",
                                                    'name': 'ADMIT_SRC_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for admission type at '
                                                               'encounter level (Code Set 3).',
                                                    'name': 'ENCOUNTER_ADMIT_TYPE_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of admission '
                                                               'circumstances. Examples: '
                                                               "'Emergency', 'Elective', "
                                                               "'Routine', 'Urgent'.",
                                                    'name': 'ENCOUNTER_ADMIT_TYPE_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code identifying how the patient '
                                                               'arrived (Code Set 68).',
                                                    'name': 'ADMIT_MODE_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of arrival method. '
                                                               "Examples: 'Ambulance', "
                                                               "'Helicopter', 'Private Transport', "
                                                               "'Walk-in'.",
                                                    'name': 'ADMIT_MODE_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code identifying the source of '
                                                               'referral (Code Set 30385).',
                                                    'name': 'REFERRAL_SOURCE_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of referral source. '
                                                               "Examples: 'GP', 'Consultant', "
                                                               "'Self', 'A&E', 'NHS 111'.",
                                                    'name': 'REFERRAL_SOURCE_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code indicating if this is a '
                                                               'readmission (Code Set 47).',
                                                    'name': 'READMIT_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of readmission status.',
                                                    'name': 'READMIT_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Free-text description of why the '
                                                               'patient presented (presenting '
                                                               'complaint).',
                                                    'name': 'REASON_FOR_VISIT',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for the facility/hospital '
                                                               '(Code Set 220). Top level of '
                                                               'location hierarchy.',
                                                    'name': 'LOC_FACILITY_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Name of the facility/hospital '
                                                               'where the patient is located.',
                                                    'name': 'FACILITY_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for the building within the '
                                                               'facility (Code Set 220).',
                                                    'name': 'LOC_BUILDING_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Name of the building within the '
                                                               'facility.',
                                                    'name': 'BUILDING_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for the nursing unit/ward '
                                                               '(Code Set 220). Key location for '
                                                               'patient flow analysis.',
                                                    'name': 'LOC_NURSE_UNIT_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Name of the nursing unit/ward. '
                                                               "Examples: 'ICU', 'Ward 5A', "
                                                               "'Medical Assessment Unit'.",
                                                    'name': 'NURSE_UNIT_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for the room within the '
                                                               'nursing unit (Code Set 220).',
                                                    'name': 'LOC_ROOM_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Room number or name within the '
                                                               'nursing unit.',
                                                    'name': 'ROOM_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for the specific bed (Code '
                                                               'Set 220). Lowest level of location '
                                                               'hierarchy.',
                                                    'name': 'LOC_BED_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Bed identifier within the room.',
                                                    'name': 'BED_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Encounter type at the time of this '
                                                               'location record (Code Set 71).',
                                                    'name': 'LOC_ENCNTR_TYPE_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of encounter type at '
                                                               'this location.',
                                                    'name': 'LOC_ENCNTR_TYPE_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Encounter class at the time of '
                                                               'this location record (Code Set '
                                                               '69).',
                                                    'name': 'LOC_ENCNTR_TYPE_CLASS_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of encounter class at '
                                                               'this location.',
                                                    'name': 'LOC_ENCNTR_TYPE_CLASS_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for the medical '
                                                               'service/specialty providing care '
                                                               '(Code Set 34).',
                                                    'name': 'MED_SERVICE_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of medical service. '
                                                               "Examples: 'General Medicine', "
                                                               "'General Surgery', 'Cardiology'.",
                                                    'name': 'MED_SERVICE_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Admission type at this specific '
                                                               'location (Code Set 3).',
                                                    'name': 'LOC_ADMIT_TYPE_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of admission type at '
                                                               'this location.',
                                                    'name': 'LOC_ADMIT_TYPE_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code indicating why the patient '
                                                               'was transferred to this location '
                                                               '(Code Set 285).',
                                                    'name': 'TRANSFER_REASON_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of transfer reason. '
                                                               "Examples: 'Clinical Need', 'Bed "
                                                               "Management', 'Step Down'.",
                                                    'name': 'TRANSFER_REASON_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for the type of accommodation '
                                                               'provided (Code Set 10).',
                                                    'name': 'ACCOMMODATION_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of accommodation type. '
                                                               "Examples: 'Private Room', "
                                                               "'Semi-Private', 'Ward'.",
                                                    'name': 'ACCOMMODATION_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code indicating why this '
                                                               'accommodation was given (Code Set '
                                                               '14767).',
                                                    'name': 'ACCOMMODATION_REASON_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of accommodation '
                                                               'reason.',
                                                    'name': 'ACCOMMODATION_REASON_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for alternate level of care '
                                                               '(Code Set 22589).',
                                                    'name': 'ALT_LVL_CARE_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of alternate level of '
                                                               'care.',
                                                    'name': 'ALT_LVL_CARE_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for reason alternate level of '
                                                               'care was assigned (Code Set '
                                                               '28443).',
                                                    'name': 'ALC_REASON_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of ALC reason.',
                                                    'name': 'ALC_REASON_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for the category of service '
                                                               '(Code Set 3394).',
                                                    'name': 'SERVICE_CATEGORY_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of service category.',
                                                    'name': 'SERVICE_CATEGORY_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for the program service '
                                                               'associated with this location '
                                                               '(Code Set 27900).',
                                                    'name': 'PROGRAM_SERVICE_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of program service.',
                                                    'name': 'PROGRAM_SERVICE_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for the specialty unit (Code '
                                                               'Set 27901).',
                                                    'name': 'SPECIALTY_UNIT_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of specialty unit.',
                                                    'name': 'SPECIALTY_UNIT_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code indicating isolation or '
                                                               'restricted access requirements '
                                                               '(Code Set 70).',
                                                    'name': 'ISOLATION_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of isolation status. '
                                                               "Examples: 'Contact Precautions', "
                                                               "'Droplet Isolation'.",
                                                    'name': 'ISOLATION_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Date/time the patient arrived at '
                                                               'this location.',
                                                    'name': 'LOC_ARRIVE_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Date/time the patient departed '
                                                               'from this location. NULL if '
                                                               'patient is still at this location.',
                                                    'name': 'LOC_DEPART_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Date/time this location record '
                                                               'became effective.',
                                                    'name': 'LOC_BEG_EFFECTIVE_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Date/time this location record was '
                                                               'no longer effective.',
                                                    'name': 'LOC_END_EFFECTIVE_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Date/time the transaction that '
                                                               'triggered this history record '
                                                               'occurred.',
                                                    'name': 'LOC_TRANSACTION_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'System date/time when the history '
                                                               'row was inserted.',
                                                    'name': 'LOC_ACTIVITY_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Calculated length of stay at this '
                                                               'specific location in hours.',
                                                    'name': 'LOC_LENGTH_OF_STAY_HOURS',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Date/time pre-registration or '
                                                               'pre-admission was performed.',
                                                    'name': 'PRE_REG_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Date/time registration or '
                                                               'admission was performed.',
                                                    'name': 'REG_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Date/time the patient arrived at '
                                                               'the facility for this encounter.',
                                                    'name': 'ENCNTR_ARRIVE_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Date/time of inpatient admission.',
                                                    'name': 'INPATIENT_ADMIT_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Date/time the patient was '
                                                               'discharged from the facility.',
                                                    'name': 'DISCH_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Date/time the patient physically '
                                                               'left the facility.',
                                                    'name': 'ENCNTR_DEPART_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Estimated length of stay in days, '
                                                               'as recorded at admission.',
                                                    'name': 'EST_LENGTH_OF_STAY',
                                                    'nullable': True,
                                                    'type': 'int'},
                                                   {'comment': 'Calculated total encounter length '
                                                               'of stay in days.',
                                                    'name': 'ENCNTR_LENGTH_OF_STAY_DAYS',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Code for discharge disposition '
                                                               '(Code Set 19).',
                                                    'name': 'DISCH_DISPOSITION_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of discharge '
                                                               "disposition. Examples: 'Discharged "
                                                               "Home', 'Deceased', 'Transferred'.",
                                                    'name': 'DISCH_DISPOSITION_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for discharge destination '
                                                               'location (Code Set 20).',
                                                    'name': 'DISCH_TO_LOCTN_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of discharge '
                                                               "destination. Examples: 'Home', "
                                                               "'Nursing Home', 'Rehabilitation "
                                                               "Facility'.",
                                                    'name': 'DISCH_TO_LOCTN_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for triage category/acuity '
                                                               '(Code Set 14753).',
                                                    'name': 'TRIAGE_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of triage category.',
                                                    'name': 'TRIAGE_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Date/time triage was performed.',
                                                    'name': 'TRIAGE_DT_TM',
                                                    'nullable': True,
                                                    'type': 'timestamp'},
                                                   {'comment': 'Code indicating type of trauma if '
                                                               'applicable (Code Set 14752).',
                                                    'name': 'TRAUMA_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of trauma type.',
                                                    'name': 'TRAUMA_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': "Code for patient's ambulatory "
                                                               'condition on arrival (Code Set 5).',
                                                    'name': 'AMBULATORY_COND_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of ambulatory '
                                                               "condition. Examples: 'Ambulatory', "
                                                               "'Wheelchair', 'Stretcher'.",
                                                    'name': 'AMBULATORY_COND_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for financial classification '
                                                               '(Code Set 354).',
                                                    'name': 'FINANCIAL_CLASS_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of financial class. '
                                                               "Examples: 'NHS', 'Private', "
                                                               "'Overseas Visitor'.",
                                                    'name': 'FINANCIAL_CLASS_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code indicating VIP status (Code '
                                                               'Set 67).',
                                                    'name': 'VIP_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of VIP status if '
                                                               'applicable.',
                                                    'name': 'VIP_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Code for confidentiality level '
                                                               '(Code Set 87).',
                                                    'name': 'CONFID_LEVEL_CD',
                                                    'nullable': True,
                                                    'type': 'double'},
                                                   {'comment': 'Description of confidentiality '
                                                               'level.',
                                                    'name': 'CONFID_LEVEL_DESC',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Sequential number of this location '
                                                               'within the encounter journey. 1 = '
                                                               'first location.',
                                                    'name': 'LOCATION_SEQUENCE',
                                                    'nullable': True,
                                                    'type': 'int'},
                                                   {'comment': 'Name of the previous nursing unit '
                                                               'the patient was in. NULL for first '
                                                               'location.',
                                                    'name': 'PREV_NURSE_UNIT',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Name of the next nursing unit the '
                                                               'patient moved to. NULL for last '
                                                               'location.',
                                                    'name': 'NEXT_NURSE_UNIT',
                                                    'nullable': True,
                                                    'type': 'string'},
                                                   {'comment': 'Flag indicating if this is the '
                                                               "first location in the patient's "
                                                               'journey. 1=Yes, 0=No.',
                                                    'name': 'IS_FIRST_LOCATION',
                                                    'nullable': True,
                                                    'type': 'int'},
                                                   {'comment': 'Flag indicating if this is the '
                                                               'last/current location in the '
                                                               "patient's journey. 1=Yes, 0=No.",
                                                    'name': 'IS_LAST_LOCATION',
                                                    'nullable': True,
                                                    'type': 'int'},
                                                   {'comment': 'Total number of locations visited '
                                                               'during this encounter.',
                                                    'name': 'TOTAL_LOCATIONS_VISITED',
                                                    'nullable': True,
                                                    'type': 'int'},
                                                   {'comment': 'Identifier for the organization '
                                                               'associated with this location '
                                                               'record.',
                                                    'name': 'ORGANIZATION_ID',
                                                    'nullable': True,
                                                    'type': 'bigint'},
                                                   {'comment': 'Latest update timestamp across '
                                                               'joined tables. Used for '
                                                               'incremental processing.',
                                                    'name': 'ADC_UPDT',
                                                    'nullable': True,
                                                    'type': 'timestamp'}],
                                       'keys': ['ENCNTR_LOC_HIST_ID'],
                                       'table_name': 'map_patient_journey'},
 '4_prod.bronze.map_person': {'columns': [{'comment': 'This is the value of the unique primary '
                                                      'identifier of the person table. It is an '
                                                      'internal system assigned number.',
                                           'name': 'person_id',
                                           'nullable': False,
                                           'type': 'double'},
                                          {'comment': 'The sex/gender that the patient is '
                                                      'considered to have for administration and '
                                                      'record keeping purposes. This is typically '
                                                      'asserted by the patient when they present '
                                                      'to administrative users. This may not match '
                                                      'the biological sex as determined by anatomy '
                                                      "or genetics, or the individual's preferred "
                                                      'identification (gender identity).',
                                           'name': 'gender_cd',
                                           'nullable': True,
                                           'type': 'double'},
                                          {'comment': 'The year of birth.',
                                           'name': 'birth_year',
                                           'nullable': True,
                                           'type': 'int'},
                                          {'comment': 'The month of birth (1-12).',
                                           'name': 'birth_month',
                                           'nullable': True,
                                           'type': 'int'},
                                          {'comment': 'The day of birth (1-31).',
                                           'name': 'birth_day',
                                           'nullable': True,
                                           'type': 'int'},
                                          {'comment': 'The full birth date and time.',
                                           'name': 'birth_datetime',
                                           'nullable': True,
                                           'type': 'timestamp'},
                                          {'comment': 'Identifies a religious, national, racial, '
                                                      'or cultural group of the person.',
                                           'name': 'ethnicity_cd',
                                           'nullable': True,
                                           'type': 'double'},
                                          {'comment': 'The address ID is the primary key of the '
                                                      'address table.',
                                           'name': 'address_id',
                                           'nullable': True,
                                           'type': 'bigint'},
                                          {'comment': 'Timestamp of last update.',
                                           'name': 'ADC_UPDT',
                                           'nullable': True,
                                           'type': 'timestamp'}],
                              'keys': ['person_id'],
                              'table_name': 'map_person'},
 '4_prod.bronze.map_problem': {'columns': [{'comment': 'Uniquely defines a problem within the '
                                                       'problem table.  The problem_id can be '
                                                       'associated with multiple problem '
                                                       'instances.  When a new problem is added to '
                                                       'the problem table the problem_id is '
                                                       'assigned to the problem_instance_id.',
                                            'name': 'PROBLEM_ID',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'This is the value of the unique primary '
                                                       'identifier of the person table.  It is an '
                                                       'internal system assigned number.',
                                            'name': 'PERSON_ID',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'Unique identifier for nomenclature item.',
                                            'name': 'NOMENCLATURE_ID',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'The date and time that the problem began.',
                                            'name': 'ONSET_DT_TM',
                                            'nullable': True,
                                            'type': 'timestamp'},
                                           {'comment': 'The earliest date and time when a problem '
                                                       'was recorded for a given person and '
                                                       'nomenclature',
                                            'name': 'earliest_problem_date',
                                            'nullable': True,
                                            'type': 'timestamp'},
                                           {'comment': 'The date and time that the '
                                                       'active_status_cd was set.',
                                            'name': 'ACTIVE_STATUS_DT_TM',
                                            'nullable': True,
                                            'type': 'timestamp'},
                                           {'comment': 'The person who caused the active_status_cd '
                                                       'to be set or change.',
                                            'name': 'ACTIVE_STATUS_PRSNL_ID',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'The date and time that the data_status_cd '
                                                       'was set.',
                                            'name': 'DATA_STATUS_DT_TM',
                                            'nullable': True,
                                            'type': 'timestamp'},
                                           {'comment': 'The person who caused the data_status_cd '
                                                       'to be set or change.',
                                            'name': 'DATA_STATUS_PRSNL_ID',
                                            'nullable': True,
                                            'type': 'double'},
                                           {'comment': 'The value of the unique primary '
                                                       'identifierof the encounter table. '
                                                       'Represents the last encounter id on which '
                                                       'the problem was modified',
                                            'name': 'UPDATE_ENCNTR_ID',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'The value of the unique primary '
                                                       'identifierof the encounter table. '
                                                       'Represents the originating encounter id on '
                                                       'which the problem was first documented',
                                            'name': 'ORIGINATING_ENCNTR_ID',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'Indicates the verification status of the '
                                                       'problem.',
                                            'name': 'CONFIRMATION_STATUS_CD',
                                            'nullable': True,
                                            'type': 'double'},
                                           {'comment': 'The description for the code value.',
                                            'name': 'confirmation_status_desc',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'Identifies the kind of problem.  Used to '
                                                       'categorize the problem so that it may be '
                                                       'managed and viewed independently within '
                                                       'different applications.',
                                            'name': 'CLASSIFICATION_CD',
                                            'nullable': True,
                                            'type': 'double'},
                                           {'comment': 'The description for the code value.',
                                            'name': 'classification_desc',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'A user-defined prioritization of the '
                                                       'problem.',
                                            'name': 'RANKING_CD',
                                            'nullable': True,
                                            'type': 'double'},
                                           {'comment': 'The code, or key, from the source '
                                                       'vocabulary that contributed the string to '
                                                       'the nomenclature.',
                                            'name': 'SOURCE_IDENTIFIER',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'Variable length string that may include '
                                                       'alphanumeric characters and punctuation.',
                                            'name': 'SOURCE_STRING',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'The external vocabulary or lexicon that '
                                                       'contributed the string, e.g. ICD9, SNOMED, '
                                                       'etc.',
                                            'name': 'SOURCE_VOCABULARY_CD',
                                            'nullable': True,
                                            'type': 'decimal(38,18)'},
                                           {'comment': 'The description for the code value.',
                                            'name': 'source_vocabulary_desc',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'Vocabulary AXIS codes related to '
                                                       'SNOMEDColumn',
                                            'name': 'VOCAB_AXIS_CD',
                                            'nullable': True,
                                            'type': 'decimal(38,18)'},
                                           {'comment': 'The description for the code value.',
                                            'name': 'vocab_axis_desc',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'The processed version of the CONCEPT_CKI '
                                                       'field.',
                                            'name': 'CONCEPT_CKI_PROCESSED',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'A unique identifier for each Concept '
                                                       'across all domains.',
                                            'name': 'OMOP_CONCEPT_ID',
                                            'nullable': True,
                                            'type': 'int'},
                                           {'comment': 'An unambiguous, meaningful and descriptive '
                                                       'name for the Concept.',
                                            'name': 'OMOP_CONCEPT_NAME',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'This flag determines where a Concept is a '
                                                       'Standard Concept, i.e. is used in the '
                                                       'data, a Classification Concept, or a '
                                                       'non-standard Source Concept. The '
                                                       'allowables values are S (Standard Concept) '
                                                       'and C (Classification Concept), otherwise '
                                                       'the content is NULL.',
                                            'name': 'OMOP_STANDARD_CONCEPT',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'The number of OMOP concepts matched for '
                                                       'each NOMENCLATURE_ID.',
                                            'name': 'OMOP_MATCH_NUMBER',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'A unique identifier for each domain.',
                                            'name': 'OMOP_CONCEPT_DOMAIN',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': None,
                                            'name': 'SNOMED_CODE',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'The method or source of the SNOMED code '
                                                       'mapping for each nomenclature entry.',
                                            'name': 'SNOMED_TYPE',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'The number of matches found for each '
                                                       'NOMENCLATURE_ID in the context of SNOMED '
                                                       'codes.',
                                            'name': 'SNOMED_MATCH_NUMBER',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'The term associated with a SNOMED code '
                                                       'that provides additional meaning and '
                                                       'context to the code.',
                                            'name': 'SNOMED_TERM',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': None,
                                            'name': 'ICD10_CODE',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'The method or source of the ICD10 code '
                                                       'mapping for each nomenclature entry.',
                                            'name': 'ICD10_TYPE',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'The number of matches found for each '
                                                       'NOMENCLATURE_ID in the context of ICD10 '
                                                       'codes.',
                                            'name': 'ICD10_MATCH_NUMBER',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'The term associated with a ICD10 code that '
                                                       'provides additional meaning and context to '
                                                       'the code.',
                                            'name': 'ICD10_TERM',
                                            'nullable': True,
                                            'type': 'string'},
                                           {'comment': 'Last update timestamp.',
                                            'name': 'ADC_UPDT',
                                            'nullable': True,
                                            'type': 'timestamp'},
                                           {'comment': 'The calculated date and time for the '
                                                       'problem, representing the onset date and '
                                                       'time.',
                                            'name': 'CALC_DT_TM',
                                            'nullable': True,
                                            'type': 'timestamp'},
                                           {'comment': 'The calculated encounter ID associated '
                                                       'with a specific problem entry.',
                                            'name': 'CALC_ENCNTR',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'The calculated encounter ID associated '
                                                       'with a specific problem entry where the '
                                                       'problem event occurred within the '
                                                       'encounter time frame.',
                                            'name': 'CALC_ENC_WITHIN',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'The calculated encounter ID associated '
                                                       'with a specific problem entry where the '
                                                       'problem event occurred before the '
                                                       'encounter time frame.',
                                            'name': 'CALC_ENC_BEFORE',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'calculated encounter ID associated with a '
                                                       'specific problem entry where the problem '
                                                       'event occurred after the encounter time '
                                                       'frame.',
                                            'name': 'CALC_ENC_AFTER',
                                            'nullable': True,
                                            'type': 'bigint'},
                                           {'comment': 'COSIGN Similarity between the Source term '
                                                       'and the OMOP Term.',
                                            'name': 'OMOP_SIMILARITY',
                                            'nullable': True,
                                            'type': 'double'},
                                           {'comment': 'COSIGN Similarity between the Source term '
                                                       'and the SNOMED Term.',
                                            'name': 'SNOMED_SIMILARITY',
                                            'nullable': True,
                                            'type': 'double'},
                                           {'comment': 'COSIGN Similarity between the Source term '
                                                       'and the ICD10 Term.',
                                            'name': 'ICD10_SIMILARITY',
                                            'nullable': True,
                                            'type': 'double'}],
                               'keys': ['PROBLEM_ID'],
                               'table_name': 'map_problem'},
 '4_prod.bronze.map_procedure': {'columns': [{'comment': 'Procedure id is the primary unique '
                                                         'identification number of the procedure '
                                                         'table.  It is an internal system '
                                                         'assigned sequence number.',
                                              'name': 'PROCEDURE_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'The person who caused the '
                                                         'active_status_cd to be set or change.',
                                              'name': 'ACTIVE_STATUS_PRSNL_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'This is the value of the unique primary '
                                                         'identifier of the encounter table. It is '
                                                         'an internal system assigned number.',
                                              'name': 'ENCNTR_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'This is the value of the unique primary '
                                                         'identifier of the person table. It is an '
                                                         'internal system assigned number.',
                                              'name': 'PERSON_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'This is the value of the unique primary '
                                                         'identifier of the nomenclature table. It '
                                                         'is an internal system assigned number.',
                                              'name': 'NOMENCLATURE_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'Encounter slice identifier.',
                                              'name': 'ENCNTR_SLICE_ID',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'Contributor system identifies the source '
                                                         'feed of data from which a row was '
                                                         'populated. This is mainly used to '
                                                         'determine how to update a set of data '
                                                         'that may have originated from more than '
                                                         'one source feed.',
                                              'name': 'CONTRIBUTOR_SYSTEM_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'The description for the code value.',
                                              'name': 'contributor_system_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The code associates a procedure to a '
                                                         'clinical service.',
                                              'name': 'CLINICAL_SERVICE_CD',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'Description for the clinical service '
                                                         'code.',
                                              'name': 'clinical_service_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Description for the active status code.',
                                              'name': 'active_status_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Date and time when the procedure was '
                                                         'performed.',
                                              'name': 'PROC_DT_TM',
                                              'nullable': True,
                                              'type': 'timestamp'},
                                             {'comment': 'The amount of time in minutes the '
                                                         'procedure took to complete.',
                                              'name': 'PROC_MINUTES',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'Free-text note for the procedure.',
                                              'name': 'PROCEDURE_NOTE',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The code, or key, from the source '
                                                         'vocabulary that contributed the string '
                                                         'to the nomenclature.',
                                              'name': 'SOURCE_IDENTIFIER',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Variable length string that may include '
                                                         'alphanumeric characters and punctuation.',
                                              'name': 'SOURCE_STRING',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The external vocabulary or lexicon that '
                                                         'contributed the string, e.g. ICD9, '
                                                         'SNOMED, etc.',
                                              'name': 'SOURCE_VOCABULARY_CD',
                                              'nullable': True,
                                              'type': 'decimal(38,18)'},
                                             {'comment': 'Description for the source vocabulary '
                                                         'code.',
                                              'name': 'source_vocabulary_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Vocabulary AXIS codes related to '
                                                         'SNOMEDColumn.',
                                              'name': 'VOCAB_AXIS_CD',
                                              'nullable': True,
                                              'type': 'decimal(38,18)'},
                                             {'comment': 'Description for the vocabulary AXIS '
                                                         'code.',
                                              'name': 'vocab_axis_desc',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Concept CKI is the functional Concept '
                                                         'Identifier; it is the codified means '
                                                         'within Millennium to identify key '
                                                         'medical concepts to support information '
                                                         'processing, clinical decision support, '
                                                         'executable knowledge and knowledge '
                                                         'presentation. Composed of a source and '
                                                         'an identifier.',
                                              'name': 'CONCEPT_CKI',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'A unique identifier for each Concept '
                                                         'across all domains.',
                                              'name': 'OMOP_CONCEPT_ID',
                                              'nullable': True,
                                              'type': 'int'},
                                             {'comment': 'An unambiguous, meaningful and '
                                                         'descriptive name for the Concept.',
                                              'name': 'OMOP_CONCEPT_NAME',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'This flag determines where a Concept is '
                                                         'a Standard Concept, i.e. is used in the '
                                                         'data, a Classification Concept, or a '
                                                         'non-standard Source Concept. The '
                                                         'allowables values are S (Standard '
                                                         'Concept) and C (Classification Concept), '
                                                         'otherwise the content is NULL.',
                                              'name': 'OMOP_STANDARD_CONCEPT',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The number of OMOP concepts matched for '
                                                         'each NOMENCLATURE_ID.',
                                              'name': 'OMOP_MATCH_NUMBER',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'A unique identifier for each domain.',
                                              'name': 'OMOP_CONCEPT_DOMAIN',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': None,
                                              'name': 'SNOMED_CODE',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'The method or source of the SNOMED code '
                                                         'mapping for each nomenclature entry.',
                                              'name': 'SNOMED_TYPE',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The number of matches found for each '
                                                         'NOMENCLATURE_ID in the context of SNOMED '
                                                         'codes.',
                                              'name': 'SNOMED_MATCH_NUMBER',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'The term associated with a SNOMED code '
                                                         'that provides additional meaning and '
                                                         'context to the code.',
                                              'name': 'SNOMED_TERM',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': None,
                                              'name': 'OPCS4_CODE',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The method or source of the OPCS4 code '
                                                         'mapping for each nomenclature entry.',
                                              'name': 'OPCS4_TYPE',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'The number of matches found for each '
                                                         'NOMENCLATURE_ID in the context of OPCS4 '
                                                         'codes.',
                                              'name': 'OPCS4_MATCH_NUMBER',
                                              'nullable': True,
                                              'type': 'bigint'},
                                             {'comment': 'The term associated with the OPCS4 code '
                                                         'that provides additional meaning and '
                                                         'context to the code.',
                                              'name': 'OPCS4_TERM',
                                              'nullable': True,
                                              'type': 'string'},
                                             {'comment': 'Timestamp for the last update.',
                                              'name': 'ADC_UPDT',
                                              'nullable': True,
                                              'type': 'timestamp'},
                                             {'comment': 'COSIGN Similarity between the Source '
                                                         'term and the OMOP Term.',
                                              'name': 'OMOP_SIMILARITY',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'COSIGN Similarity between the Source '
                                                         'term and the SNOMED Term.',
                                              'name': 'SNOMED_SIMILARITY',
                                              'nullable': True,
                                              'type': 'double'},
                                             {'comment': 'COSIGN Similarity between the Source '
                                                         'term and the OPCS4 Term.',
                                              'name': 'OPCS4_SIMILARITY',
                                              'nullable': True,
                                              'type': 'double'}],
                                 'keys': ['PROCEDURE_ID'],
                                 'table_name': 'map_procedure'},
 '4_prod.bronze.map_text_events': {'columns': [{'comment': 'The unique primary identifier of the '
                                                           'Event Table.',
                                                'name': 'EVENT_ID',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'This is the value of the unique '
                                                           'primary identifier of the encounter '
                                                           'table. It is an internal system '
                                                           'assigned number.',
                                                'name': 'ENCNTR_ID',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'This is the value of the unique '
                                                           'primary identifier of the person '
                                                           'table. It is an internal system '
                                                           'assigned number.',
                                                'name': 'PERSON_ID',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'The unique primary identifier of Order '
                                                           'Table.',
                                                'name': 'ORDER_ID',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'Coded value which specifies how the '
                                                           'event is stored in and retrieved from '
                                                           "the event table's sub-tables. For "
                                                           'example, Event_Class_CDs identify '
                                                           'events as numeric results, textual '
                                                           'results, calculations, medications, '
                                                           'etc.',
                                                'name': 'EVENT_CLASS_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'Personnel id of provider who performed '
                                                           'this result.',
                                                'name': 'PERFORMED_PRSNL_ID',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'The textual result value for the '
                                                           'clinical event.',
                                                'name': 'TEXT_RESULT',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The code value for the unit of '
                                                           'measurement.',
                                                'name': 'UNIT_OF_MEASURE_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value.',
                                                'name': 'UNIT_OF_MEASURE_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The title for document results.',
                                                'name': 'EVENT_TITLE_TEXT',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'It is the code that identifies the '
                                                           'most basic unit of the storage, i.e. '
                                                           'RBC, discharge summary, image.',
                                                'name': 'EVENT_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value',
                                                'name': 'EVENT_CD_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Foreign key to the order_catalog '
                                                           'table. Catalog_cd does not exist in '
                                                           'the code_value table and does not have '
                                                           'a code set.',
                                                'name': 'CATALOG_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description of the Orderable',
                                                'name': 'CATALOG_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Used to store the internal code for '
                                                           'the catalog type. Used as a filtering '
                                                           'mechanism for rows on theorder catalog '
                                                           'table.',
                                                'name': 'CATALOG_TYPE_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value',
                                                'name': 'CATALOG_TYPE_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Contributor system identifies the '
                                                           'source feed of data from which a row '
                                                           'was populated.  This is mainly used to '
                                                           'determine how to update a set of data '
                                                           'that may have originated from more '
                                                           'than one source feed.',
                                                'name': 'CONTRIBUTOR_SYSTEM_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value',
                                                'name': 'CONTRIBUTOR_SYSTEM_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The combination of the reference nbr '
                                                           'and the contributor system code '
                                                           'provides a unique identifier to the '
                                                           'origin of the data.',
                                                'name': 'REFERENCE_NBR',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Provides a mechanism for logical '
                                                           'grouping of events.  i.e. supergroup '
                                                           'and group tests.  Same as event_id if '
                                                           'current row is the highest level '
                                                           'parent.',
                                                'name': 'PARENT_EVENT_ID',
                                                'nullable': True,
                                                'type': 'bigint'},
                                               {'comment': 'States whether the result is normal.  '
                                                           'This can be used to determine whether '
                                                           'to display the event tag in different '
                                                           'color on the flowsheet. For group '
                                                           'results, this represents an overall '
                                                           'normalcy. i.e. Is any result in the '
                                                           'group abnormal?  Also allows different '
                                                           'purge criteria to be applied based on '
                                                           'result.',
                                                'name': 'NORMALCY_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value.',
                                                'name': 'NORMALCY_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Used to identify the method in which a '
                                                           'result was entered.',
                                                'name': 'ENTRY_MODE_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value.',
                                                'name': 'ENTRY_MODE_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Date this result was performed (or '
                                                           'authored).',
                                                'name': 'PERFORMED_DT_TM',
                                                'nullable': True,
                                                'type': 'timestamp'},
                                               {'comment': 'Represents the update date/time that '
                                                           'tracks when clinically significant '
                                                           'updates are made to the Clinical Event '
                                                           'and should only be used to check for '
                                                           'updates. This field is used to notify '
                                                           'audiences when a clinically '
                                                           'significant update is made to an '
                                                           'existing clinical event, such as when '
                                                           'XR Clinical Reporting re-prints a lab '
                                                           'result due to an update of the result '
                                                           'value or when a result is resent to a '
                                                           "provider's Message Center with the "
                                                           'result update. This date should NOT be '
                                                           'displayed as the clinically.',
                                                'name': 'CLINSIG_UPDT_DT_TM',
                                                'nullable': True,
                                                'type': 'timestamp'},
                                               {'comment': 'The title for the parent event.',
                                                'name': 'PARENT_EVENT_TITLE_TEXT',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The code value of the parent event.',
                                                'name': 'PARENT_EVENT_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value.',
                                                'name': 'PARENT_EVENT_CD_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The catalog code of the parent event.',
                                                'name': 'PARENT_CATALOG_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description of the Orderable.',
                                                'name': 'PARENT_CATALOG_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The internal code for the catalog type '
                                                           'of the parent event.',
                                                'name': 'PARENT_CATALOG_TYPE_CD',
                                                'nullable': True,
                                                'type': 'int'},
                                               {'comment': 'The description for the code value.',
                                                'name': 'PARENT_CATALOG_TYPE_DISPLAY',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The combination of the reference nbr '
                                                           'and the contributor system code '
                                                           'provides a unique identifier to the '
                                                           'origin of the data.',
                                                'name': 'PARENT_REFERENCE_NBR',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'Timestamp of last update.',
                                                'name': 'ADC_UPDT',
                                                'nullable': True,
                                                'type': 'timestamp'},
                                               {'comment': 'The name of the OMOP Common Data Model '
                                                           'table.',
                                                'name': 'OMOP_MANUAL_TABLE',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The field of the OMOP Common Data '
                                                           'Model table.',
                                                'name': 'OMOP_MANUAL_COLUMN',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The concept_id of the OMOP Common Data '
                                                           'Model table.',
                                                'name': 'OMOP_MANUAL_CONCEPT',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The manually mapped OMOP concept ID '
                                                           'representing the value of a text for '
                                                           'standardized use in the OMOP CDM.',
                                                'name': 'OMOP_MANUAL_VALUE_CONCEPT',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The name or description of the OMOP '
                                                           'concept id.',
                                                'name': 'OMOP_MANUAL_CONCEPT_NAME',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'This flag determines where a Concept '
                                                           'is a Standard Concept, i.e. is used in '
                                                           'the data, a Classification Concept, or '
                                                           'a non-standard Source Concept. The '
                                                           'allowables values are S (Standard '
                                                           'Concept) and C (Classification '
                                                           'Concept), otherwise the content is '
                                                           'NULL.',
                                                'name': 'OMOP_MANUAL_STANDARD_CONCEPT',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'A unique identifier for the domain.',
                                                'name': 'OMOP_MANUAL_CONCEPT_DOMAIN',
                                                'nullable': True,
                                                'type': 'string'},
                                               {'comment': 'The identifier for the class or '
                                                           'category of the OMOP concept.',
                                                'name': 'OMOP_MANUAL_CONCEPT_CLASS',
                                                'nullable': True,
                                                'type': 'string'}],
                                   'keys': ['EVENT_ID'],
                                   'table_name': 'map_text_events'}}
_PIPELINE_REQUIRED_TABLES = ['3_lookup.dwh.pi_lkp_cde_doc_ref',
 '3_lookup.embeddings.terms',
 '3_lookup.geo.epc_certificates',
 '3_lookup.imd.imd_2019',
 '3_lookup.imd.postcode_maps',
 '3_lookup.mill.map_med_lookup',
 '3_lookup.mill.mill_code_value',
 '3_lookup.mill.mill_order_catalog',
 '3_lookup.mill.mill_order_catalog_synonym',
 '3_lookup.omop.barts_new_maps',
 '3_lookup.omop.concept',
 '3_lookup.omop.concept_answer_lists',
 '3_lookup.omop.concept_index_result',
 '3_lookup.omop.concept_index_test',
 '3_lookup.omop.pathology_result_concept_map',
 '3_lookup.omop.pathology_result_map_overrides',
 '3_lookup.omop.pathology_result_value_exclusions',
 '3_lookup.omop.pathology_test_concept_map',
 '3_lookup.omop.pathology_test_map_overrides',
 '3_lookup.omop.pathology_unit_map',
 '3_lookup.ons.addressbase_consolidated',
 '3_lookup.rxnorm.rxnconso',

 '4_prod.bronze.nomenclature',
 '4_prod.raw.mat_birth',
 '4_prod.raw.mat_pregnancy',
 '4_prod.raw.mill_address',
 '4_prod.raw.mill_ce_coded_result',
 '4_prod.raw.mill_ce_date_result',
 '4_prod.raw.mill_ce_med_result',
 '4_prod.raw.mill_ce_string_result',
 '4_prod.raw.mill_clinical_event',
 '4_prod.raw.mill_diagnosis',
 '4_prod.raw.mill_encntr_loc_hist',
 '4_prod.raw.mill_encounter',
 '4_prod.raw.mill_fhx_activity',
 '4_prod.raw.mill_fhx_long_text_r',
 '4_prod.raw.mill_location',
 '4_prod.raw.mill_location_group',
 '4_prod.raw.mill_long_text',
 '4_prod.raw.mill_med_admin_event',
 '4_prod.raw.mill_order_ingredient',
 '4_prod.raw.mill_orders',
 '4_prod.raw.mill_organization',
 '4_prod.raw.mill_person',
 '4_prod.raw.mill_person_alias',
 '4_prod.raw.mill_person_person_reltn',
 '4_prod.raw.mill_problem',
 '4_prod.raw.mill_procedure',
 '4_prod.raw.mill_prsnl',
 '4_prod.raw.mill_prsnl_group',
 '4_prod.raw.mill_prsnl_group_reltn',
 '4_prod.raw.msds101pregbook',
 '4_prod.raw.nnu_episodes',
 '4_prod.raw.path_master_resultable',
 '4_prod.raw.path_patient_resultlevel',
 '4_prod.raw.path_patient_samplelevel',
 '4_prod.raw.pi_cde_doc_response']
_PIPELINE_PENDING_CHECKPOINTS = {}
_PIPELINE_UPDATED_TARGETS = []
_PIPELINE_CURRENT_COMPONENT = None
_PIPELINE_CHECKPOINTS_ENABLED = True
_PIPELINE_AUDIT_ENABLED = True
_PIPELINE_SKIP_TARGETS = set()
_PIPELINE_CUTOVER_BACKUPS = {}
_PIPELINE_COMPONENT_COMPLETE_PROPERTY = (
    "pipeline.map_pipeline.component_complete"
)
_PIPELINE_TARGET_STATE_PROPERTY = "pipeline.map_pipeline.target_state"
_PIPELINE_PROJECTION_VERSION_PROPERTY = (
    "pipeline.map_pipeline.compatibility_projection_version"
)
_PIPELINE_RELEASE_ID = "20260726_v1"
_PIPELINE_FROZEN_CONTRACT_SUFFIX = (
    "__pre_map_pipeline_replacement_945df7fc45a94b4d"
)
_PIPELINE_SOURCE_MANIFEST = {}
_PIPELINE_SOURCE_MANIFEST_HASH = ""

# Bumping an entry rebuilds that one canonical table on the next run and
# leaves every other target on its incremental path.
#
# Every entry of _PIPELINE_CANONICAL_TARGETS must appear here. A target that is
# absent defaults to version 1 and can therefore never be rebuilt by a bump,
# which silently exempts it from the corrective mechanism -- so the load-time
# check below refuses to start rather than let that happen again.
#
# 2 is the release baseline: bronze_release_prebuild brings every canonical table to
# this version, so they all begin the release on the same version whether or not
# their own build logic changed. Most are rebuilt outright from pinned sources. The
# three high-volume event lanes -- numeric, text and coded -- are instead cloned from
# the live canonical, purged of the rows their new scope excludes, and advanced by
# source CDF; that reaches the same content without re-parsing 1.5 billion rows.
# Either route must leave the state stamped at this version. A cloned lane still
# carrying version 1 would look stale to the first ordinary weekly run, which would
# then force a full rebuild of a 1.5 billion row table inside the weekly window --
# the exact cost the clone route exists to avoid.
_PIPELINE_CANONICAL_LOGIC_VERSIONS = {
    "map_address": 2,
    "map_address_epc": 2,
    "map_care_site": 2,
    "map_coded_events": 2,
    "map_date_events": 2,
    "map_death": 2,
    "map_diagnosis": 2,
    "map_encounter": 2,
    "map_family_history": 2,
    "map_implant_details": 2,
    "map_mat_birth": 2,
    "map_mat_pregnancy": 2,
    "map_mat_vte_assessment": 2,
    "map_med_admin": 2,
    "map_medical_personnel": 2,
    "map_nomen_events": 2,
    "map_numeric_events": 2,
    "map_pathology": 2,
    "map_patient_journey": 2,
    "map_person": 2,
    "map_problem": 2,
    "map_procedure": 2,
    "map_text_events": 2,
}
# Canonical keeps one row per (EVENT_ID, SEQUENCE_NBR); the legacy tables are keyed on
# EVENT_ID alone, so publishing has to choose one row. Highest SEQUENCE_NBR is a
# deliberate NEW rule, not a restoration -- the legacy Map Pipeline never mentions
# SEQUENCE_NBR anywhere (zero occurrences in 10,279 lines) and never ordered coded-result
# rows per event. Its tables came out one-row-per-EVENT_ID only because the publish merged
# on EVENT_ID, so the surviving row was whichever the merge happened to write last. There
# is no legacy order to return to; do not "correct" this rule towards one.
_PIPELINE_COMPATIBILITY_COLLAPSE = {
    "map_nomen_events": [
        ("SEQUENCE_NBR", True),
        ("ADC_UPDT", True),
        ("SOURCE_ROW_KEY", False),
    ],
    "map_coded_events": [
        ("SEQUENCE_NBR", True),
        ("ADC_UPDT", True),
        ("SOURCE_ROW_KEY", False),
    ],
    "map_mat_birth": [
        ("BirthSourceRecordUpdatedDateTime", True),
        ("BirthSourceADC_UPDT", True),
        ("BirthSourceCtrl_ID", True),
        ("BirthRow_ID", False),
    ],
    "map_mat_vte_assessment": [
        ("Source_ADC_UPDT", True),
        ("Reference_ADC_UPDT", True),
        ("DOC_RESPONSE_KEY", False),
    ],
    "map_pathology": [
        ("source_update_datetime", True),
        ("load_datetime", True),
        ("mapping_update_datetime", True),
        ("source_record_key", False),
    ],
}
# Superseded by the column_expressions of _PIPELINE_COMPATIBILITY_SPECS. Nothing
# reads this any more -- unlike _PIPELINE_COMPATIBILITY_COLLAPSE, which is still
# the single source of truth for collapse tiebreaks -- and its map_encounter
# entry named the canonical *_EFFECTIVE columns that the specification has since
# moved away from, so leaving it populated would mislead the next reader. Kept as
# an empty dict rather than deleted so that any reference elsewhere still
# resolves, and resolves to "no overrides", which is now the correct answer.
_PIPELINE_COMPATIBILITY_COLUMN_OVERRIDES = {}

# map_med_admin's legacy dose columns, transcribed from the production Map
# Pipeline: get_unit_conversion_maps_med_admin (its L3290-3321) and
# create_standardization_expressions_med_admin (its L3323-3348). The arm ORDER
# is the contract. coalesce takes the first arm whose LIKE matches, and '%g%'
# stands ahead of '%mg%', so every milligram display is scaled by 1000 and the
# last three weight arms can never fire at all. That is legacy's answer, and a
# legacy-named column publishes legacy's answer; the corrected figure belongs in
# a new canonical column, not here. LIKE is case-sensitive in Spark, which is
# why '%mL%' does not also swallow 'milliliter'.
_MED_ADMIN_LEGACY_WEIGHT_UNITS = (
    ("microgram", "1E-3"),
    ("micrograms", "1E-3"),
    ("microgram/", "1E-3"),
    ("microgram(s)", "1E-3"),
    ("mcg", "1E-3"),
    ("nanogram", "1E-6"),
    ("nanogram(s)", "1E-6"),
    ("nanogram/", "1E-6"),
    ("ng", "1E-6"),
    ("g", "1E3"),
    ("gram", "1E3"),
    ("g/", "1E3"),
    ("mg", "1E0"),
    ("milligram", "1E0"),
    ("mg/", "1E0"),
)
_MED_ADMIN_LEGACY_VOLUME_UNITS = (
    ("mL", "1E0"),
    ("mL(s)", "1E0"),
    ("mL/", "1E0"),
    ("milliliter", "1E0"),
    ("cc", "1E0"),
    ("L", "1E3"),
    ("liter", "1E3"),
    ("L/", "1E3"),
)

# The value legacy standardises: the administered dose, falling back to the
# initial dose and then to the initial volume. The FLOAT casts are load-bearing,
# not cosmetic -- legacy read all three columns as FloatType (Map Pipeline
# L3606-3618) and the frozen table stores them as FLOAT, while the canonical
# table stores them as DOUBLE. Without the casts the projection differs from
# legacy on 32-bit rounding alone.
_MED_ADMIN_LEGACY_DOSE_VALUE = (
    "CASE WHEN CAST(ADMIN_DOSAGE AS FLOAT) = 0 "
    "THEN CASE WHEN CAST(INITIAL_DOSAGE AS FLOAT) = 0 "
    "THEN CAST(INITIAL_VOLUME AS FLOAT) "
    "ELSE CAST(INITIAL_DOSAGE AS FLOAT) END "
    "ELSE CAST(ADMIN_DOSAGE AS FLOAT) END"
)


def _med_admin_legacy_dose_expression(units):
    # coalesce(dose * factor) over the first unit the display matches, in legacy
    # arm order. Factors are written as double literals (1E-3, not 0.001) so the
    # multiplication is FLOAT * DOUBLE, which is what lit(0.001) gave legacy; a
    # bare 0.001 is DECIMAL(4,3) in Spark SQL and coerces differently.
    arms = [
        "CASE WHEN ADMIN_DOSAGE_UNIT_DISPLAY LIKE '%" + unit + "%' THEN ("
        + _MED_ADMIN_LEGACY_DOSE_VALUE + ") * " + factor + " END"
        for unit, factor in units
    ]
    return "coalesce(" + ", ".join(arms) + ")"


# The unsuffixed tables are a curated legacy interface over the canonical
# tables, not a lossless copy of them. Each entry states how one legacy table
# is derived from its canonical table:
#
#   row_predicate       SQL selecting the canonical rows the legacy table keeps
#   additional_keys     columns added to the frozen contract keys to set the
#                       publication grain
#   column_expressions  SQL evaluated against the canonical table, for legacy
#                       columns whose legacy meaning differs from the canonical
#                       column of the same name
#   projection_version  bump to republish this legacy table from scratch
#
# Deterministic ordering for a collapsing grain stays in
# _PIPELINE_COMPATIBILITY_COLLAPSE, which remains the single source of truth
# for tiebreaks.
_PIPELINE_COMPATIBILITY_SPECS = {
    "map_address": {
        "row_predicate": (
            "PARENT_ENTITY_NAME IN ('PERSON', 'ORGANIZATION') "
            "AND ACTIVE_IND = 1 "
            "AND (END_EFFECTIVE_DT_TM IS NULL "
            "OR END_EFFECTIVE_DT_TM > current_timestamp())"
        ),
        # Legacy masking applied to PERSON rows only and took the first three
        # characters of the RAW postcode with the space intact, so 'E4 8XX'
        # masks to 'E4 ' and 'ZZ99 3VZ' to 'ZZ9'. ORGANIZATION rows kept the
        # whole postcode. On the frozen pre-replacement clone, of 42,406 joinable
        # organisation rows 41,674 match SOURCE_ZIPCODE verbatim and 1,555 match
        # a three-character prefix -- but that 1,555 is 794 null-null matches
        # plus 761 sources that are ALREADY three characters or fewer, and zero
        # genuine truncations. Legacy never masked an organisation.
        # SOURCE_ZIPCODE cannot reproduce the person form either, because for
        # person rows it is already the privacy-aware outward code with the
        # space removed, which loses every two-character outward code -- 2.2M
        # of 11.4M rows. map_10_foundation therefore publishes the verbatim
        # three-character prefix of the raw value as SOURCE_ZIPCODE_MASK3.
        "column_expressions": {
            "masked_zipcode": (
                "CASE WHEN PARENT_ENTITY_NAME = 'PERSON' "
                "THEN SOURCE_ZIPCODE_MASK3 "
                "ELSE SOURCE_ZIPCODE END"
            ),
        },
        "projection_version": 2,
    },
    "map_coded_events": {
        # DISPLAY is a 40 character source column; DESCRIPTION carries the full
        # term the legacy table always published.
        "column_expressions": {
            "RESULT_DISPLAY": "RESULT_DESCRIPTION",
        },
        "projection_version": 2,
    },
    "map_diagnosis": {
        # map_problem and map_family_history publish dotless ICD-10, so
        # map_diagnosis has to as well for the three to join to each other.
        "column_expressions": {
            "ICD10_CODE": "replace(ICD10_CODE, '.', '')",
        },
        "projection_version": 2,
    },
    "map_encounter": {
        # The canonical table keeps encounters that were booked but never
        # arrived; the legacy table has always described arrivals only.
        #
        # Legacy published DERIVED values under the raw column names. Arrival
        # was coalesce(ARRIVE_DT_TM, EST_ARRIVE_DT_TM, first clinical event) --
        # note there is no REG_DT_TM arm, despite what the release plan says --
        # unless the first clinical event was more than seven days after the
        # recorded arrival, in which case the event time replaced it. Departure
        # mirrored that rule against the last clinical event and fell back to
        # arrival plus one day. ARRIVE_DT_TM_COMPAT / DEPART_DT_TM_COMPAT
        # reproduce those chains with one deliberate divergence: map_10 rejects
        # the 2100-12-31 sentinel before the chain runs, where legacy carried it
        # through. That divergence is currently inert -- no ARRIVE_DT_TM,
        # EST_ARRIVE_DT_TM, DEPART_DT_TM or EST_DEPART_DT_TM value is at or past
        # 2100, and the only four sentinel-flagged rows come from REG_DT_TM and
        # DISCH_DT_TM, neither of which has an arm in this chain.
        #
        # It does NOT stop a departure preceding its arrival, and nothing here
        # does. Arrival and departure derive from independent sources, both
        # outlier rules shrink the interval, and no rule constrains their order,
        # so the chain emits 2,660,407 negative intervals against legacy's own
        # 2,658,885 -- faithful reproduction, which is the contract. The
        # row_predicate cannot remove them either: it is implied by ARRIVE_DT_TM
        # IS NOT NULL, so it drops no row a negative-interval check could see.
        # It is a null-arrival guard, serving encounter_compat_missing_arrival.
        # The canonical best-guess columns -- ARRIVAL_DT_TM_BEST,
        # DEPARTURE_DT_TM_BEST and LENGTH_OF_STAY_MINUTES -- are the evidenced
        # versions and deliberately differ. The REG_DT_TM arm, the refusal to
        # read a booking slot as an arrival, and the refusal to synthesise a
        # departure all live there, not here, because a legacy-named column
        # keeps its legacy meaning. Canonical ARRIVE_DT_TM_COMPAT_FABRICATED_IND
        # and DEPART_DT_TM_COMPAT_FABRICATED_IND mark exactly which values
        # published here have no observational basis -- the estimated-arrival
        # arm and the arrival-plus-one-day fallback -- so a consumer can drop
        # them without leaving the legacy schema.
        "row_predicate": "ARRIVE_DT_TM_COMPAT IS NOT NULL",
        "column_expressions": {
            "ARRIVE_DT_TM": "ARRIVE_DT_TM_COMPAT",
            "DEPART_DT_TM": "DEPART_DT_TM_COMPAT",
        },
        "projection_version": 2,
    },
    "map_mat_birth": {
        # Without the baby in the grain, every birth after the first in a
        # multiple birth collapses into its sibling.
        "additional_keys": ["BabyPerson_ID"],
        "projection_version": 2,
    },
    "map_mat_vte_assessment": {
        # The canonical grain is one row per response; the legacy grain is one
        # row per element, so a re-answered element has to collapse. Report how
        # often collapsing discards a differing answer.
        "report_collapse_conflicts": ["Response"],
    },
    "map_med_admin": {
        # Legacy-named columns keep legacy semantics, so these four reproduce
        # the production Map Pipeline's arithmetic rather than correcting it --
        # including the '%g%'-before-'%mg%' ordering that scales every milligram
        # by 1000. See _MED_ADMIN_LEGACY_WEIGHT_UNITS above.
        #
        # ORDER_CKI is legacy's coalesce(MULTUM_CODE, OSYN.MNEMONIC): the order
        # SYNONYM mnemonic, which the canonical table publishes as
        # ORDERED_AS_MNEMONIC. ORDER_MNEMONIC is deliberately NOT projected from
        # it. The synonym mnemonic drops the dose suffix and collapses a compound
        # infusion to a single ingredient -- 'propofol 1 g [100 mg/hour] + Neat
        # 100 mL(s)' becomes 'Neat', 'electrolyte solution (Plasma-Lyte 148)
        # intravenous solution 1000 mL(s)' becomes 'Plasma-lyte 148 intravenous
        # infusion' -- and preserving combinations is a locked decision of this
        # release. The consequence is that compat ORDER_MNEMONIC differs from
        # legacy on 17,944,153 rows (31%), which the validate assertion
        # mapping_med_admin_lookup_column_divergence reports.
        #
        # The DOSE_UNIT_CATEGORY patterns drop legacy's '.*' wrappers. rlike is a
        # search, so '.*(a|b).*' and '(a|b)' accept exactly the same strings, and
        # '.' matches CR/LF on the Photon path but not on the Spark-JVM path -- a
        # difference that can vary within a single run. Same answer, one fewer
        # engine dependency.
        #
        # Measured over the 57,884,827 EVENT_IDs shared with the frozen table,
        # these four expressions leave 2,211,090 rows (3.82%) differing from
        # legacy, and ZERO of them are unexplained: every one has an input the
        # canonical build resolved and legacy did not -- ADMIN_DOSAGE_UNIT_CD,
        # the order-synonym mnemonic, ADMIN_DOSAGE or MULTUM. On 369,107 of them
        # the frozen row's entire dose block is null because legacy's
        # med-admin-result join missed it. No projection can close that gap.
        "column_expressions": {
            "ORDER_CKI": "coalesce(MULTUM, ORDERED_AS_MNEMONIC)",
            "DOSE_UNIT_CATEGORY": (
                "CASE WHEN ADMIN_DOSAGE_UNIT_DISPLAY RLIKE "
                "'(mg|g|microgram|nanogram)' THEN 'WEIGHT_MG' "
                "WHEN ADMIN_DOSAGE_UNIT_DISPLAY RLIKE '(mL|L)' THEN 'VOLUME_ML' "
                "WHEN ADMIN_DOSAGE_UNIT_DISPLAY RLIKE '(unit|Unit|UNIT)' "
                "THEN 'UNITS' "
                "WHEN ADMIN_DOSAGE_UNIT_DISPLAY RLIKE '(application|spray|patch"
                "|scoop|inch|dose|puff|drop|tablet|capsule)' THEN 'DISCRETE' "
                "ELSE 'OTHER' END"
            ),
            "DOSE_IN_MG": _med_admin_legacy_dose_expression(
                _MED_ADMIN_LEGACY_WEIGHT_UNITS),
            "DOSE_IN_ML": _med_admin_legacy_dose_expression(
                _MED_ADMIN_LEGACY_VOLUME_UNITS),
        },
        "projection_version": 2,
    },
    "map_medical_personnel": {
        # The legacy table publishes empty strings, not nulls, for these three.
        "column_expressions": {
            "SRVCATEGORY": "coalesce(SRVCATEGORY, '')",
            "SURGSPEC": "coalesce(SURGSPEC, '')",
            "MEDSERVICE": "coalesce(MEDSERVICE, '')",
        },
        "projection_version": 2,
    },
    "map_nomen_events": {
        "projection_version": 2,
    },
    "map_pathology": {
        # The canonical table keeps results whose patient could not be
        # resolved, together with the candidate identifiers and the linkage
        # status. The legacy table is person keyed and cannot carry them.
        "row_predicate": "PERSON_ID IS NOT NULL",
        "projection_version": 2,
    },
    "map_person": {
        # Legacy kept active people of type Person only, and required a sex
        # code and a plausible birth year. Canonical keeps the rest behind
        # quality flags. The person-type test is redundant once the canonical
        # build is scoped to 903, and is kept as a guard so this legacy table
        # cannot silently regain the 96,273 'Freetext' stubs.
        #
        # Verified against the frozen legacy table: this predicate yields
        # 6,247,703 rows, every one of which the 6,363,327-row frozen table
        # also has, and it invents nothing. The 115,624 rows it drops are
        # 96,273 Freetext stubs, 19,330 people since made inactive that the
        # upsert-only legacy loader had no way to delete, 16 with no birth
        # timestamp and 5 with person_type_cd 0.
        "row_predicate": (
            "person_type_cd = 903 "
            "AND active_ind = 1 "
            "AND gender_cd IS NOT NULL "
            "AND birth_datetime_utc IS NOT NULL "
            "AND year(birth_datetime_utc) >= 1901 "
            "AND year(birth_datetime_utc) <= year(current_date())"
        ),
        # Legacy derived all four birth fields from the SAME column,
        # BIRTH_DT_TM, which is birth_datetime_utc here. Canonical prefers
        # ABS_BIRTH_DT_TM and so disagrees by a day for roughly 2.4 million
        # people. All four must move together, or the year, month and day would
        # contradict the timestamp beside them.
        "column_expressions": {
            "birth_datetime": "birth_datetime_utc",
            "birth_year": "year(birth_datetime_utc)",
            "birth_month": "month(birth_datetime_utc)",
            "birth_day": "dayofmonth(birth_datetime_utc)",
        },
        "projection_version": 2,
    },
}


def _pipeline_active_spark():
    """Return Databricks' injected Spark session captured by this notebook."""
    return _PIPELINE_SPARK


def table_exists(table_name: str) -> bool:
    return _pipeline_active_spark().catalog.tableExists(table_name)


def _pipeline_sql_name(name: str) -> str:
    quote = chr(96)
    return ".".join(
        quote + part.replace(quote, quote * 2) + quote
        for part in name.split(".")
    )


def _pipeline_table_property(
    table_name: str,
    property_name: str,
):
    if not table_exists(table_name):
        return None
    try:
        rows = spark.sql(
            f"SHOW TBLPROPERTIES "
            f"{_pipeline_sql_name(table_name)}"
        ).collect()
        return next(
            (
                row.value
                for row in rows
                if row.key == property_name
            ),
            None,
        )
    except Exception:
        return None


def _pipeline_target_base_name(target_table: str) -> str:
    return target_table.rsplit(".", 1)[-1]


def _pipeline_canonical_logic_version(target_table: str) -> int:
    return int(
        _PIPELINE_CANONICAL_LOGIC_VERSIONS.get(
            _pipeline_target_base_name(target_table),
            1,
        )
    )


def _pipeline_expected_target_state(component_name: str, target_table: str):
    return {
        "component": component_name,
        "release": _PIPELINE_RELEASE_ID,
        "run_id": _PIPELINE_RUN_ID,
        "source_manifest_hash": _PIPELINE_SOURCE_MANIFEST_HASH,
        "canonical_logic_version": _pipeline_canonical_logic_version(
            target_table
        ),
    }


def _pipeline_target_state(target_table: str):
    raw = _pipeline_table_property(
        target_table,
        _PIPELINE_TARGET_STATE_PROPERTY,
    )
    if not raw:
        return None
    try:
        return _pipeline_json.loads(raw)
    except Exception:
        return None


def _pipeline_target_state_literal(
    component_name: str,
    target_table: str,
) -> str:
    state = _pipeline_expected_target_state(component_name, target_table)
    return _pipeline_json.dumps(state, sort_keys=True).replace("'", "")


def _pipeline_resume_component_complete(
    component_name: str,
    target_tables,
) -> bool:
    """True only when this run already finished this component.

    Resume is scoped to one run against one pinned source manifest at one
    logic version. A marker left by an earlier run, an earlier release, or a
    run whose sources have since advanced does not count, so no component can
    be skipped while the data it reads has moved on.
    """
    if not target_tables:
        return False
    for target_table in target_tables:
        state = _pipeline_target_state(target_table)
        if state is None:
            return False
        expected = _pipeline_expected_target_state(
            component_name,
            target_table,
        )
        if any(state.get(key) != value for key, value in expected.items()):
            return False
    return True


# Components whose canonical target is not named after the component. Empty
# because nothing currently breaks the convention; it exists so that the day
# something does, the fix is one line here rather than a silent loss of version
# enforcement for that component.
_PIPELINE_COMPONENT_TARGET_OVERRIDES = {}


def _pipeline_component_canonical_targets(component_name: str):
    """The canonical tables a component owns, by naming convention.

    Every call site pairs the component name 'map_x' with a target list of
    ['4_prod.bronze.map_x'], so the component name is enough and the
    call sites do not have to repeat themselves.
    """
    override = _PIPELINE_COMPONENT_TARGET_OVERRIDES.get(component_name)
    if override is not None:
        return list(override)
    return [f"{_PIPELINE_TARGET_PREFIX}.{component_name}"]


def _pipeline_component_rebuild_reason(component_name: str, targets=None):
    """Why this component must rebuild rather than top up, or None.

    Absent targets are skipped rather than treated as needing a rebuild. An
    absent target is either genuinely new, in which case every component's
    incremental path already performs its own first-run full build, or the
    naming convention above guessed wrong -- and a wrong guess must never cost
    an unannounced billion-row rebuild.

    A target that exists but records no version is reported, not rebuilt. See
    the notebook header: the live tables predate this property, and the release
    prebuild is what performs the corrective rebuilds.
    """
    if targets is None:
        targets = _pipeline_component_canonical_targets(component_name)
    for target_table in targets:
        if not table_exists(target_table):
            continue
        expected = _pipeline_canonical_logic_version(target_table)
        recorded = (_pipeline_target_state(target_table) or {}).get(
            "canonical_logic_version"
        )
        if recorded is None:
            print(
                f"[LOGIC-VERSION] {component_name}: {target_table} records no "
                f"canonical_logic_version (expected {expected}); topping up. "
                "Seed target state via the release cutover to enforce this."
            )
            _pipeline_audit(
                target_table,
                "TARGET_STATE_MISSING",
                {
                    "component": component_name,
                    "expected_canonical_logic_version": expected,
                },
            )
            continue
        if int(recorded) != int(expected):
            return (
                f"{target_table} canonical_logic_version "
                f"{recorded} -> {expected}"
            )
    return None


def _pipeline_component_force_full_refresh(
    component_name: str,
    targets=None,
) -> bool:
    """The rebuild gate in predicate form, for components that self-refresh.

    map_diagnosis, map_problem and map_procedure pass their own
    force_full_refresh argument rather than routing through
    _pipeline_run_recoverable, so they consult this directly. Reports and
    audits its reason here so that every forced rebuild is explained once,
    whichever of the two paths the component takes.
    """
    if _PIPELINE_FULL_REFRESH:
        return True
    reason = _pipeline_component_rebuild_reason(component_name, targets)
    if not reason:
        return False
    # A corrected component must not top up on top of rows its old logic
    # produced, so a version bump costs one rebuild of that target alone and
    # leaves every other target on its incremental path.
    print(
        f"[LOGIC-VERSION] {component_name}: full canonical rebuild ({reason})"
    )
    _pipeline_audit(
        None,
        "COMPONENT_REBUILD_FORCED",
        {"component": component_name, "reason": reason},
    )
    return True


def _pipeline_mark_component_complete(
    component_name: str,
    target_tables,
):
    for target_table in target_tables:
        spark.sql(
            f"ALTER TABLE {_pipeline_sql_name(target_table)} "
            "SET TBLPROPERTIES "
            "('delta.enableChangeDataFeed' = 'true', "
            "'delta.enableRowTracking' = 'true', "
            f"'{_PIPELINE_COMPONENT_COMPLETE_PROPERTY}' = "
            f"'{component_name}', "
            f"'{_PIPELINE_TARGET_STATE_PROPERTY}' = "
            f"'{_pipeline_target_state_literal(component_name, target_table)}'"
            ")"
        )


def _pipeline_optional_bool_parameter(
    name: str,
    default: bool = False,
) -> bool:
    try:
        return str(_PIPELINE_DBUTILS.widgets.get(name)).strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
        }
    except Exception:
        return bool(default)


def _pipeline_contract(target_table: str):
    return _PIPELINE_CONTRACTS.get(target_table.lower())


def _pipeline_schema_signature(schema):
    return [
        (
            field.name,
            field.dataType.simpleString().lower(),
            bool(field.nullable),
        )
        for field in schema.fields
    ]


def _pipeline_expected_signature(target_table: str):
    contract = _pipeline_contract(target_table)
    if contract is None:
        raise RuntimeError(
            f"No frozen output contract is registered for {target_table}"
        )
    return [
        (
            field["name"],
            field["type"],
            bool(field.get("nullable", True)),
        )
        for field in contract["columns"]
    ]


def _pipeline_adopt_existing_contracts(reference_prefix: str):
    """Freeze each contract against the interface downstream actually saw.

    The generated manifest is the minimum known contract, and an existing
    table is authoritative for column order and types so a safely added
    downstream column survives. For a table this pipeline has already
    replaced the live table is its own output, and adopting that would let a
    regression become the contract, so the pre-replacement backup is
    preferred wherever one exists.
    """
    adopted = []
    frozen = []
    for contract in _PIPELINE_CONTRACTS.values():
        live_table = (
            reference_prefix
            + "."
            + contract["table_name"]
        )
        frozen_table = live_table + _PIPELINE_FROZEN_CONTRACT_SUFFIX
        if table_exists(frozen_table):
            reference_table = frozen_table
            frozen.append(frozen_table)
        elif table_exists(live_table):
            reference_table = live_table
        else:
            continue
        schema = spark.table(reference_table).schema
        actual_names = {field.name.lower() for field in schema.fields}
        missing_keys = [
            key
            for key in contract["keys"]
            if key.lower() not in actual_names
        ]
        if missing_keys:
            raise RuntimeError(
                f"Live contract {reference_table} is missing key columns: "
                f"{missing_keys}"
            )
        contract["columns"] = [
            {
                "name": field.name,
                "type": field.dataType.simpleString().lower(),
                "nullable": bool(field.nullable),
                "comment": field.metadata.get("comment"),
            }
            for field in schema.fields
        ]
        try:
            contract["table_comment"] = spark.catalog.getTable(
                reference_table
            ).description
        except Exception:
            contract["table_comment"] = None
        adopted.append(reference_table)
    print(
        f"[CONTRACT] adopted {len(adopted)} table schemas "
        f"({len(frozen)} frozen from pre-replacement backups)"
    )
    return adopted


def _pipeline_assert_contract(
    target_table: str,
    allow_missing: bool = True,
):
    if not table_exists(target_table):
        if allow_missing:
            return
        raise RuntimeError(f"Required target is missing: {target_table}")

    actual = _pipeline_schema_signature(spark.table(target_table).schema)
    expected = _pipeline_expected_signature(target_table)
    if actual == expected:
        return

    limit = _pipeline_builtins.max(
        len(actual),
        len(expected),
    )
    differences = []
    for index in range(limit):
        actual_field = actual[index] if index < len(actual) else None
        expected_field = expected[index] if index < len(expected) else None
        if actual_field != expected_field:
            differences.append(
                {
                    "ordinal": index,
                    "actual": actual_field,
                    "expected": expected_field,
                }
            )
        if len(differences) >= 20:
            break
    raise RuntimeError(
        f"Output contract mismatch for {target_table}; refusing to alter or "
        "overwrite it. "
        + _pipeline_json.dumps(differences)
    )


def _pipeline_preflight():
    failures = []
    for table_name in _PIPELINE_REQUIRED_TABLES:
        try:
            if not table_exists(table_name):
                failures.append(f"missing table/view: {table_name}")
                continue
            spark.table(table_name).limit(0).collect()
        except Exception as exc:
            failures.append(
                f"unreadable table/view {table_name}: {str(exc)[:500]}"
            )

    if failures:
        message = (
            "Map Pipeline dependency preflight found:\n- "
            + "\n- ".join(failures)
        )
        raise RuntimeError(message)

    print(
        f"[PREFLIGHT] passed: {len(_PIPELINE_REQUIRED_TABLES)} dependencies "
        "using read-only checks"
    )
    return []


def _pipeline_cleanup_orphan_staging():
    """Remove only this pipeline's known crash-left staging tables."""
    catalog_name, schema_name = _PIPELINE_TARGET_PREFIX.split(".", 1)
    rows = spark.sql(
        f"SHOW TABLES IN {_pipeline_sql_name(catalog_name + '.' + schema_name)}"
    ).collect()
    removed = []
    for row in rows:
        table_name = row["tableName"]
        is_pipeline_stage = (
            table_name.startswith("__map_care_site_stage_")
            or table_name.startswith("__map_death_rebuild_")
            or table_name.startswith("_tmp_map_pathology_v2_")
            or (
                table_name.startswith("map_")
                and (
                    "__rebuild_" in table_name
                    or "__merge_stage_" in table_name
                    or "__changes_stage_" in table_name
                    or "__source_stage_" in table_name
                    or "__current_deaths_stage_" in table_name
                    or "__code_lookup_stage_" in table_name
                    or "_history__rebuild_" in table_name
                )
            )
        )
        if not is_pipeline_stage:
            continue
        full_name = _PIPELINE_TARGET_PREFIX + "." + table_name
        spark.sql(f"DROP TABLE IF EXISTS {_pipeline_sql_name(full_name)}")
        removed.append(full_name)
    if removed:
        print(
            "[CLEANUP] removed orphan pipeline staging tables: "
            + ", ".join(removed)
        )
    return removed


def _pipeline_ensure_control_tables():
    global _PIPELINE_CHECKPOINTS_ENABLED
    global _PIPELINE_AUDIT_ENABLED
    try:
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {_PIPELINE_CHECKPOINT_TABLE} (
              target_table STRING NOT NULL,
              source_table STRING NOT NULL,
              source_version BIGINT NOT NULL,
              run_id STRING NOT NULL,
              committed_at TIMESTAMP NOT NULL
            ) USING DELTA
            """
        )
    except Exception as exc:
        _PIPELINE_CHECKPOINTS_ENABLED = False
        print(
            "[WARN] Version checkpoints disabled for this run: "
            + str(exc)[:1000]
        )
    try:
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {_PIPELINE_AUDIT_TABLE} (
              run_id STRING NOT NULL,
              target_table STRING,
              event_type STRING NOT NULL,
              event_timestamp TIMESTAMP NOT NULL,
              target_version BIGINT,
              details_json STRING
            ) USING DELTA
            """
        )
    except Exception as exc:
        _PIPELINE_AUDIT_ENABLED = False
        print(
            "[WARN] Pipeline audit disabled for this run: "
            + str(exc)[:1000]
        )


def _pipeline_latest_version(table_name: str):
    try:
        row = (
            spark.sql(
                f"DESCRIBE HISTORY "
                f"{_pipeline_sql_name(table_name)} LIMIT 1"
            )
            .select("version")
            .first()
        )
        return int(row["version"]) if row else None
    except Exception:
        return None


def _pipeline_checkpoint_version(
    target_table: str,
    source_table: str,
):
    if (
        not _PIPELINE_CHECKPOINTS_ENABLED
        or not table_exists(_PIPELINE_CHECKPOINT_TABLE)
    ):
        return None
    try:
        row = (
            spark.table(_PIPELINE_CHECKPOINT_TABLE)
            .where(
                (F.col("target_table") == target_table)
                & (F.col("source_table") == source_table)
            )
            .agg(
                F.max("source_version").alias("source_version")
            )
            .first()
        )
        if row and row["source_version"] is not None:
            return int(row["source_version"])
    except Exception as exc:
        print(
            "[WARN] Could not read source checkpoint; "
            "using production watermark behavior: "
            + str(exc)[:1000]
        )
    return None


def _pipeline_stage_checkpoint(
    target_table: str,
    source_table: str,
    version,
):
    if (
        version is None
        or not _PIPELINE_CHECKPOINTS_ENABLED
    ):
        return
    key = (target_table, source_table)
    current = _PIPELINE_PENDING_CHECKPOINTS.get(key)
    _PIPELINE_PENDING_CHECKPOINTS[key] = _pipeline_builtins.max(
        int(version),
        int(current or -1),
    )


def _pipeline_commit_all_checkpoints():
    runtime_spark = _pipeline_active_spark()
    if (
        not _PIPELINE_CHECKPOINTS_ENABLED
        or not _PIPELINE_PENDING_CHECKPOINTS
    ):
        return
    rows = [
        (target, source, version, _PIPELINE_RUN_ID)
        for (target, source), version
        in sorted(_PIPELINE_PENDING_CHECKPOINTS.items())
    ]
    updates = (
        runtime_spark.createDataFrame(
            rows,
            "target_table STRING, source_table STRING, "
            "source_version BIGINT, run_id STRING",
        )
        .withColumn(
            "committed_at",
            _pipeline_F.current_timestamp(),
        )
    )
    try:
        (
            _pipeline_DeltaTable.forName(
                runtime_spark,
                _PIPELINE_CHECKPOINT_TABLE,
            )
            .alias("t")
            .merge(
                updates.alias("s"),
                "t.target_table = s.target_table "
                "AND t.source_table = s.source_table",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        _PIPELINE_PENDING_CHECKPOINTS.clear()
    except Exception as exc:
        print(
            "[WARN] Checkpoint commit failed; data writes remain valid "
            "and will be safely replayed next run: "
            + str(exc)[:1000]
        )


def _pipeline_audit(
    target_table: str,
    event_type: str,
    details=None,
):
    runtime_spark = _pipeline_active_spark()
    if not _PIPELINE_AUDIT_ENABLED:
        return
    try:
        target_version = (
            _pipeline_latest_version(target_table)
            if target_table
            else None
        )
        row = [(
            _PIPELINE_RUN_ID,
            target_table,
            event_type,
            target_version,
            _pipeline_json.dumps(
                details or {},
                default=str,
            ),
        )]
        (
            runtime_spark.createDataFrame(
                row,
                "run_id STRING, target_table STRING, "
                "event_type STRING, target_version BIGINT, "
                "details_json STRING",
            )
            .withColumn(
                "event_timestamp",
                _pipeline_F.current_timestamp(),
            )
            .select(
                "run_id",
                "target_table",
                "event_type",
                "event_timestamp",
                "target_version",
                "details_json",
            )
            .write.mode("append")
            .saveAsTable(_PIPELINE_AUDIT_TABLE)
        )
    except Exception as exc:
        print(
            "[WARN] Audit write failed but pipeline execution continues: "
            + str(exc)[:1000]
        )


def _pipeline_rebuild_recovery_reason(exc: Exception):
    exception_name = type(exc).__name__.lower().replace("_", "")
    if "fullrebuildrequired" in exception_name:
        return "explicit full rebuild required"
    if "fullrefreshrequired" in exception_name:
        return "explicit full refresh required"
    message = str(exc).lower()
    normalized_message = message.replace("_", " ")
    markers = (
        "full rebuild",
        "full refresh",
        "full corrective refresh",
        "not deployed",
        "missing source-version checkpoint",
        "missing source checkpoints",
        "missing tables",
        "schema mismatch",
        "target and bridge must be initialized",
        "cdf is unavailable",
        "cdf could not be read",
        "change data feed",
        "delta_change",
        "startingversion",
        "cannot time travel beyond",
        "deletedfileretentionduration",
        "delta unsupported time travel",
        "version not available",
        "is before the earliest version",
        "file not found",
    )
    return next((marker for marker in markers if marker in message or marker in normalized_message), None)


def _pipeline_run_recoverable(
    component_name: str,
    force_full_refresh: bool,
    incremental,
    rebuild,
    targets=None,
):
    if force_full_refresh:
        print(f"[BOOTSTRAP] {component_name}: full canonical rebuild")
        return rebuild()
    if _pipeline_component_force_full_refresh(component_name, targets):
        return rebuild()
    try:
        return incremental()
    except Exception as exc:
        reason = _pipeline_rebuild_recovery_reason(exc)
        if reason is None:
            raise
        print(
            f"[RECOVERY] {component_name}: {reason}; rebuilding the "
            "canonical target from version-pinned source snapshots"
        )
        _pipeline_audit(
            None,
            "CANONICAL_REBUILD_RECOVERY",
            {
                "component": component_name,
                "reason": reason,
                "message": str(exc)[:2000],
            },
        )
        return rebuild()


def _pipeline_component_start(
    component_name: str,
    set_current: bool = True,
):
    global _PIPELINE_CURRENT_COMPONENT
    if set_current:
        _PIPELINE_CURRENT_COMPONENT = component_name
    print(f"[COMPONENT] {component_name}")
    _pipeline_audit(
        None,
        "COMPONENT_START",
        {"component": component_name},
    )


def _pipeline_record_error(
    target_table: str,
    exc: Exception,
    event_type: str = "TARGET_ERROR",
):
    details = {
        "component": _PIPELINE_CURRENT_COMPONENT,
        "exception_type": type(exc).__name__,
        "message": str(exc)[:4000],
        "traceback": _pipeline_traceback.format_exc()[-12000:],
    }
    print(
        f"[ERROR] {_PIPELINE_CURRENT_COMPONENT or target_table}: "
        f"{type(exc).__name__}: {str(exc)}"
    )
    try:
        _pipeline_audit(
            target_table,
            event_type,
            details,
        )
    except Exception as audit_exc:
        print(
            "[WARN] Could not persist failure audit: "
            + str(audit_exc)[:1000]
        )


def get_max_timestamp(
    table_name: str,
    ts_column: str = "ADC_UPDT",
    default_date: datetime = datetime(1980, 1, 1),
) -> datetime:
    try:
        if not table_exists(table_name):
            return default_date
        max_row = (
            spark.table(table_name)
            .select(F.max(ts_column).alias("max_ts"))
            .first()
        )
        max_ts_value = max_row["max_ts"] or default_date
        if has_cdf_enabled(table_name):
            try:
                history = spark.sql(
                    f"DESCRIBE HISTORY "
                    f"{_pipeline_sql_name(table_name)} LIMIT 1"
                ).collect()
                if history:
                    table_last_update = history[0]["timestamp"]
                    if (
                        table_last_update is not None
                        and table_last_update < max_ts_value
                    ):
                        print(
                            f"[INFO] {table_name}: using Delta update time "
                            f"{table_last_update} instead of future-dated "
                            f"{ts_column} {max_ts_value}"
                        )
                        return table_last_update
            except Exception as history_exc:
                print(
                    f"[WARN] Could not apply future-date guard for "
                    f"{table_name}: {str(history_exc)[:500]}"
                )
        return max_ts_value
    except Exception as exc:
        print(
            f"[WARN] Could not read {ts_column} "
            f"from {table_name}: {exc}"
        )
        return default_date


def has_cdf_enabled(table_name: str) -> bool:
    try:
        rows = spark.sql(
            f"SHOW TBLPROPERTIES "
            f"{_pipeline_sql_name(table_name)}"
        ).collect()
        value = next(
            (
                row.value
                for row in rows
                if row.key == "delta.enableChangeDataFeed"
            ),
            None,
        )
        return str(value).lower() == "true"
    except Exception:
        return False


def _pipeline_timestamp_fallback(
    source_table: str,
    target_table: str,
    timestamp_column: str,
):
    watermark = get_max_timestamp(
        target_table,
        timestamp_column,
    )
    print(
        f"[WARN] Using production-compatible timestamp fallback for "
        f"{source_table}: {timestamp_column} > {watermark}. "
        "The source-version checkpoint will not advance."
    )
    return (
        spark.table(source_table)
        .where(
            F.col(timestamp_column) > F.lit(watermark)
        )
    )


def get_incremental_data_with_cdf(
    source_table: str,
    target_table: str,
    timestamp_column: str = "ADC_UPDT",
    apply_trust_filter: bool = True,
    additional_filters=None,
) -> tuple:
    ending_version = _pipeline_latest_version(source_table)
    checkpoint_safe = False
    checkpoint = _pipeline_checkpoint_version(
        target_table,
        source_table,
    )

    if not table_exists(target_table):
        if ending_version is not None:
            incremental_df = (
                spark.read.format("delta")
                .option("versionAsOf", ending_version)
                .table(source_table)
            )
            checkpoint_safe = True
        else:
            incremental_df = spark.table(source_table)
        mode = "full_missing_target"
    elif (
        has_cdf_enabled(source_table)
        and ending_version is not None
    ):
        try:
            if (
                checkpoint is not None
                and checkpoint > ending_version
            ):
                print(
                    f"[WARN] {source_table} version reset detected "
                    f"({checkpoint} -> {ending_version}); rebasing CDF "
                    "from the protected target watermark."
                )
                checkpoint = None
            if checkpoint is not None:
                starting_version = checkpoint + 1
                if starting_version > ending_version:
                    incremental_df = (
                        spark.table(source_table)
                        .limit(0)
                    )
                else:
                    incremental_df = (
                        spark.read.format("delta")
                        .option(
                            "readChangeFeed",
                            "true",
                        )
                        .option(
                            "startingVersion",
                            starting_version,
                        )
                        .option(
                            "endingVersion",
                            ending_version,
                        )
                        .table(source_table)
                    )
                mode = "version_pinned_cdf"
                checkpoint_safe = True
            else:
                watermark = get_max_timestamp(
                    target_table,
                    timestamp_column,
                )
                incremental_df = (
                    spark.read.format("delta")
                    .option(
                        "readChangeFeed",
                        "true",
                    )
                    .option(
                        "startingTimestamp",
                        watermark.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    )
                    .option(
                        "endingVersion",
                        ending_version,
                    )
                    .table(source_table)
                )
                mode = (
                    "cdf_bootstrap_from_target_watermark"
                )
                checkpoint_safe = True

            if "_change_type" in incremental_df.columns:
                incremental_df = (
                    incremental_df
                    .where(
                        F.col("_change_type").isin(
                            "insert",
                            "update_postimage",
                        )
                    )
                    .drop(
                        "_change_type",
                        "_commit_version",
                        "_commit_timestamp",
                    )
                    .dropDuplicates()
                )
        except Exception as exc:
            print(
                f"[WARN] Version-pinned CDF unavailable "
                f"for {source_table}: {str(exc)[:500]}"
            )
            incremental_df = _pipeline_timestamp_fallback(
                source_table,
                target_table,
                timestamp_column,
            )
            mode = "timestamp_fallback"
            checkpoint_safe = False
    else:
        incremental_df = _pipeline_timestamp_fallback(
            source_table,
            target_table,
            timestamp_column,
        )
        mode = "timestamp_fallback"
        checkpoint_safe = False

    if (
        apply_trust_filter
        and "Trust" in incremental_df.columns
    ):
        incremental_df = incremental_df.where(
            F.col("Trust") == F.lit("Barts")
        )
    if additional_filters is not None:
        incremental_df = incremental_df.where(
            additional_filters
        )

    record_count = incremental_df.count()
    if checkpoint_safe:
        _pipeline_stage_checkpoint(
            target_table,
            source_table,
            ending_version,
        )
    print(
        f"[INCREMENTAL] {source_table} -> {target_table}: "
        f"mode={mode}, rows={record_count}, "
        f"ending_version={ending_version}"
    )
    return incremental_df, record_count



def escape_comment(text: str) -> str:
    if not text:
        return ""
    return text.replace("\\", "\\\\").replace("'", "''")


def detect_schema_changes(
    target_table: str,
    target_schema=None,
    table_comment: str = None,
):
    changes = {
        "has_changes": False,
        "columns_to_update": [],
        "columns_to_add": [],
        "table_comment_update": None,
    }
    if target_schema:
        current_fields = {
            field.name: field
            for field in spark.table(target_table).schema.fields
        }
        for target_field in target_schema.fields:
            current = current_fields.get(target_field.name)
            if current is None:
                changes["columns_to_add"].append({
                    "name": target_field.name,
                    "type": target_field.dataType.simpleString(),
                    "comment": target_field.metadata.get("comment", ""),
                })
                changes["has_changes"] = True
                continue
            current_comment = current.metadata.get("comment", "")
            target_comment = target_field.metadata.get("comment", "")
            type_changed = current.dataType != target_field.dataType
            comment_changed = current_comment != target_comment
            if type_changed or comment_changed:
                changes["columns_to_update"].append({
                    "name": target_field.name,
                    "type": target_field.dataType.simpleString(),
                    "comment": target_comment,
                    "type_changed": type_changed,
                    "comment_changed": comment_changed,
                })
                changes["has_changes"] = True
    if table_comment:
        try:
            detail = spark.sql(
                f"DESCRIBE DETAIL {_pipeline_sql_name(target_table)}"
            ).first()
            current_comment = (
                detail.asDict().get("description")
                if detail
                else None
            )
        except Exception:
            current_comment = None
        if current_comment != table_comment:
            changes["table_comment_update"] = table_comment
            changes["has_changes"] = True
    return changes


def apply_column_comments(target_table: str, schema):
    for field in schema.fields:
        comment = field.metadata.get("comment", "")
        if comment:
            spark.sql(
                f"ALTER TABLE {_pipeline_sql_name(target_table)} "
                f"ALTER COLUMN `{field.name}` COMMENT "
                f"'{escape_comment(comment)}'"
            )


def apply_schema_changes(target_table: str, changes: dict):
    for column in changes["columns_to_add"]:
        sql = (
            f"ALTER TABLE {_pipeline_sql_name(target_table)} "
            f"ADD COLUMN `{column['name']}` {column['type']}"
        )
        if column["comment"]:
            sql += (
                " COMMENT '"
                + escape_comment(column["comment"])
                + "'"
            )
        spark.sql(sql)

    type_changes = [
        column
        for column in changes["columns_to_update"]
        if column["type_changed"]
    ]
    if type_changes:
        print(
            f"[WARN] Applying production-compatible type migration "
            f"to {target_table}: "
            + ", ".join(column["name"] for column in type_changes)
        )
        migrated = spark.table(target_table)
        for column in type_changes:
            migrated = migrated.withColumn(
                column["name"],
                F.col(column["name"]).cast(column["type"]),
            )
        (
            migrated.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )

    for column in changes["columns_to_update"]:
        if column["comment_changed"]:
            spark.sql(
                f"ALTER TABLE {_pipeline_sql_name(target_table)} "
                f"ALTER COLUMN `{column['name']}` COMMENT "
                f"'{escape_comment(column['comment'])}'"
            )

    if changes["table_comment_update"]:
        spark.sql(
            f"COMMENT ON TABLE {_pipeline_sql_name(target_table)} IS "
            f"'{escape_comment(changes['table_comment_update'])}'"
        )


def create_table_with_schema(
    source_df,
    target_table: str,
    target_schema=None,
    table_comment: str = None,
):
    if target_schema:
        builder = (
            DeltaTable.createIfNotExists(spark)
            .tableName(target_table)
            .addColumns(target_schema)
            .property("delta.enableChangeDataFeed", "true")
            .property("delta.enableRowTracking", "true")
        )
        if table_comment:
            builder = builder.comment(table_comment)
        builder.execute()
        aligned = source_df.select(
            *[
                (
                    F.col(field.name).cast(field.dataType)
                    if field.name in source_df.columns
                    else F.lit(None).cast(field.dataType)
                ).alias(field.name, metadata=field.metadata)
                for field in target_schema.fields
            ]
        )
        aligned.write.mode("append").saveAsTable(target_table)
        apply_column_comments(target_table, target_schema)
    else:
        (
            source_df.write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.enableRowTracking", "true")
            .mode("overwrite")
            .saveAsTable(target_table)
        )


def update_table(
    source_df,
    target_table: str,
    index_column,
    target_schema=None,
    table_comment: str = None,
    update_condition: str = None,
):
    if source_df is None:
        print(f"[INFO] {target_table}: source is None; no write")
        return
    if bronze_is_primary_table(target_table):
        source_df = bronze_project_contract(source_df, target_table)
        target_schema = bronze_contract_schema(target_table)
    try:
        if table_exists(target_table):
            if target_schema or table_comment:
                changes = detect_schema_changes(
                    target_table,
                    target_schema,
                    table_comment,
                )
                if changes["has_changes"]:
                    apply_schema_changes(target_table, changes)
            if len(source_df.take(1)) == 0:
                print(f"[INFO] {target_table}: source is empty")
                _pipeline_audit(target_table, "NO_DATA")
                return
            keys = (
                [index_column]
                if isinstance(index_column, str)
                else list(index_column)
            )
            merge_condition = " AND ".join(
                f"t.`{key}` <=> s.`{key}`"
                for key in keys
            )
            assignments = {
                column_name: f"s.`{column_name}`"
                for column_name in source_df.columns
            }
            merge = (
                DeltaTable.forName(spark, target_table)
                .alias("t")
                .merge(source_df.alias("s"), merge_condition)
            )
            if update_condition:
                merge = merge.whenMatchedUpdate(
                    condition=update_condition,
                    set=assignments,
                )
            else:
                merge = merge.whenMatchedUpdate(set=assignments)
            merge.whenNotMatchedInsert(values=assignments).execute()
            event_type = "MERGE"
        else:
            if len(source_df.take(1)) == 0:
                print(
                    f"[INFO] {target_table}: source is empty; "
                    "target not created"
                )
                _pipeline_audit(target_table, "NO_DATA")
                return
            create_table_with_schema(
                source_df,
                target_table,
                target_schema,
                table_comment,
            )
            event_type = "CREATE"

        _PIPELINE_UPDATED_TARGETS.append(target_table)
        history = (
            spark.sql(
                f"DESCRIBE HISTORY "
                f"{_pipeline_sql_name(target_table)} LIMIT 1"
            )
            .select("operation", "operationMetrics")
            .first()
        )
        _pipeline_audit(
            target_table,
            event_type,
            history.asDict(recursive=True) if history else {},
        )
        print(f"[SUCCESS] {target_table}: {event_type.lower()} completed")
    except Exception as exc:
        _pipeline_record_error(target_table, exc)
        raise


def _pipeline_compatibility_spec(table_name: str):
    return _PIPELINE_COMPATIBILITY_SPECS.get(table_name, {})


def _pipeline_publication_keys(contract):
    """The legacy grain: frozen contract keys plus any spec additions."""
    spec = contract.get("_compatibility_spec")
    if spec is None:
        spec = _pipeline_compatibility_spec(contract["table_name"])
    keys = list(contract["keys"])
    for key in spec.get("additional_keys", []):
        if key not in keys:
            keys.append(key)
    return keys


def _pipeline_projection_version(contract) -> int:
    spec = contract.get("_compatibility_spec")
    if spec is None:
        spec = _pipeline_compatibility_spec(contract["table_name"])
    return int(spec.get("projection_version", 1))


def _pipeline_compatibility_plan(
    canonical_table: str,
    target_table: str,
):
    contract = _pipeline_contract(target_table)
    if contract is None:
        raise RuntimeError(
            f"No compatibility contract registered for {target_table}"
        )
    if not table_exists(canonical_table):
        raise RuntimeError(f"Missing canonical target: {canonical_table}")
    spec = _pipeline_compatibility_spec(contract["table_name"])
    contract["_compatibility_spec"] = spec
    canonical_columns = {
        name.lower(): name
        for name in spark.table(canonical_table).columns
    }
    column_expressions = spec.get("column_expressions", {})
    missing = [
        field["name"]
        for field in contract["columns"]
        if field["name"].lower() not in canonical_columns
        and field["name"] not in column_expressions
    ]
    if missing:
        raise RuntimeError(
            f"{canonical_table} cannot publish {target_table}; "
            f"missing compatibility columns: {missing}"
        )
    publication_keys = _pipeline_publication_keys(contract)
    source_keys = [
        canonical_columns[key.lower()]
        for key in publication_keys
        if key.lower() in canonical_columns
    ]
    if len(source_keys) != len(publication_keys):
        raise RuntimeError(
            f"{canonical_table} is missing compatibility keys "
            f"{publication_keys}"
        )
    # Resolve every declared expression against the canonical schema now, so a
    # specification that has drifted fails the plan instead of failing part
    # way through a publish.
    probe = spark.table(canonical_table).limit(0)
    unresolved = {}
    for column_name, expression in sorted(column_expressions.items()):
        try:
            probe.selectExpr(f"({expression}) AS `{column_name}`")
        except Exception as exc:
            unresolved[column_name] = str(exc)[:300]
    row_predicate = spec.get("row_predicate")
    if row_predicate:
        try:
            probe.where(row_predicate)
        except Exception as exc:
            unresolved["<row_predicate>"] = str(exc)[:300]
    if unresolved:
        raise RuntimeError(
            f"Compatibility specification for {contract['table_name']} does "
            f"not resolve against {canonical_table}: {unresolved}"
        )
    collapse = _PIPELINE_COMPATIBILITY_COLLAPSE.get(
        contract["table_name"]
    )
    collapse_order = []
    for requested_name, descending in collapse or []:
        actual_name = canonical_columns.get(requested_name.lower())
        if actual_name is not None:
            collapse_order.append((actual_name, descending))
    if collapse and not collapse_order:
        raise RuntimeError(
            f"No declared compatibility tiebreaker exists in "
            f"{canonical_table}"
        )
    return contract, canonical_columns, source_keys, collapse, collapse_order


def _pipeline_scope_current_canonical(
    canonical_table: str,
    source_keys,
    affected_keys,
):
    current = spark.table(canonical_table).alias("c")
    if affected_keys is None:
        return current
    affected = affected_keys.alias("a")
    condition = None
    for key in source_keys:
        term = F.col(f"c.`{key}`").eqNullSafe(F.col(f"a.`{key}`"))
        condition = term if condition is None else condition & term
    return current.join(affected, condition, "left_semi")


def _pipeline_project_compatibility(
    canonical,
    contract,
    canonical_columns,
    source_keys,
    collapse_order,
):
    spec = contract.get("_compatibility_spec")
    if spec is None:
        spec = _pipeline_compatibility_spec(contract["table_name"])
    row_predicate = spec.get("row_predicate")
    if row_predicate:
        # Scope before the collapse. A row the legacy table does not carry
        # must not be able to win the tiebreak and hide the row that should
        # have won it.
        canonical = canonical.where(F.expr(row_predicate))
    if collapse_order:
        order = [
            (
                F.col(name).desc_nulls_last()
                if descending
                else F.col(name).asc_nulls_last()
            )
            for name, descending in collapse_order
        ]
        window = Window.partitionBy(
            *[F.col(name) for name in source_keys]
        ).orderBy(*order)
        canonical = (
            canonical.withColumn(
                "_compatibility_row_number",
                F.row_number().over(window),
            )
            .where(F.col("_compatibility_row_number") == 1)
            .drop("_compatibility_row_number")
        )
    column_expressions = dict(spec.get("column_expressions", {}))
    for field in contract["columns"]:
        if field["name"] in column_expressions:
            continue
        actual_name = canonical_columns.get(field["name"].lower())
        if actual_name is None:
            raise RuntimeError(
                f"Compatibility projection for {contract['table_name']} "
                f"cannot resolve column {field['name']}"
            )
        column_expressions[field["name"]] = f"`{actual_name}`"
    projected = canonical.select(
        *[
            F.expr(column_expressions[field["name"]])
            .cast(field["type"])
            .alias(
                field["name"],
                metadata=(
                    {"comment": field["comment"]}
                    if field.get("comment")
                    else {}
                ),
            )
            for field in contract["columns"]
        ]
    )
    required_non_null = [
        field["name"]
        for field in contract["columns"]
        if not field.get("nullable", True)
    ]
    if required_non_null:
        null_condition = None
        for name in required_non_null:
            term = F.col(name).isNull()
            null_condition = (
                term
                if null_condition is None
                else null_condition | term
            )
        if projected.where(null_condition).limit(1).count():
            raise RuntimeError(
                f"Compatibility projection for {contract['table_name']} "
                f"contains nulls in NOT NULL columns: {required_non_null}"
            )
    publication_keys = _pipeline_publication_keys(contract)
    duplicate = (
        projected.groupBy(*publication_keys)
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .collect()
    )
    if duplicate:
        raise RuntimeError(
            f"Compatibility projection for {contract['table_name']} "
            f"violates grain {publication_keys}; sample duplicate: "
            f"{duplicate[0].asDict(recursive=True)}"
        )
    return projected


def _pipeline_apply_published_constraints(target_table: str, contract):
    current = {
        field.name.lower(): field
        for field in spark.table(target_table).schema.fields
    }
    for field in contract["columns"]:
        if field.get("nullable", True):
            continue
        existing = current.get(field["name"].lower())
        if existing is not None and existing.nullable:
            spark.sql(
                f"ALTER TABLE {_pipeline_sql_name(target_table)} "
                f"ALTER COLUMN `{field['name']}` SET NOT NULL"
            )


def _pipeline_full_compatibility_publish(
    canonical_table: str,
    target_table: str,
    contract,
    canonical_columns,
    source_keys,
    collapse_order,
    reason: str,
):
    projected = _pipeline_project_compatibility(
        spark.table(canonical_table),
        contract,
        canonical_columns,
        source_keys,
        collapse_order,
    )
    row_count = projected.count()
    (
        projected.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .option("delta.enableRowTracking", "true")
        .saveAsTable(target_table)
    )
    if contract.get("table_comment"):
        spark.sql(
            f"COMMENT ON TABLE {_pipeline_sql_name(target_table)} IS "
            f"'{escape_comment(contract['table_comment'])}'"
        )
    apply_column_comments(target_table, projected.schema)
    _pipeline_apply_published_constraints(target_table, contract)
    return {
        "mode": "FULL_REBUILD",
        "reason": reason,
        "compatibility_rows": row_count,
    }


def _pipeline_incremental_compatibility_publish(
    canonical_table: str,
    target_table: str,
    contract,
    canonical_columns,
    source_keys,
    collapse_order,
    previous_version: int,
    ending_version: int,
):
    changes = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", int(previous_version) + 1)
        .option("endingVersion", int(ending_version))
        .table(canonical_table)
    )
    affected_keys = changes.select(
        *[F.col(key) for key in source_keys]
    ).dropDuplicates()
    affected_count = affected_keys.count()
    if affected_count == 0:
        return {
            "mode": "NO_CHANGE",
            "affected_keys": 0,
            "upsert_rows": 0,
            "deleted_rows": 0,
        }

    current = _pipeline_scope_current_canonical(
        canonical_table,
        source_keys,
        affected_keys,
    )
    projected = _pipeline_project_compatibility(
        current,
        contract,
        canonical_columns,
        source_keys,
        collapse_order,
    )
    upsert_count = projected.count()
    keys = _pipeline_publication_keys(contract)
    merge_condition = " AND ".join(
        f"t.`{key}` <=> s.`{key}`"
        for key in keys
    )
    change_condition = " OR ".join(
        f"NOT (t.`{field['name']}` <=> s.`{field['name']}`)"
        for field in contract["columns"]
    )
    if upsert_count:
        (
            DeltaTable.forName(spark, target_table)
            .alias("t")
            .merge(projected.alias("s"), merge_condition)
            .whenMatchedUpdateAll(condition=change_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )

    affected_contract_keys = affected_keys.select(
        *[
            F.col(source_name).alias(contract_name)
            for source_name, contract_name
            in zip(source_keys, keys)
        ]
    )
    current_keys = projected.select(*keys).dropDuplicates()
    delete_condition = None
    for key in keys:
        term = F.col(f"a.`{key}`").eqNullSafe(F.col(f"c.`{key}`"))
        delete_condition = (
            term
            if delete_condition is None
            else delete_condition & term
        )
    deleted_keys = (
        affected_contract_keys.alias("a")
        .join(current_keys.alias("c"), delete_condition, "left_anti")
        .dropDuplicates(keys)
    )
    deleted_count = deleted_keys.count()
    if deleted_count:
        (
            DeltaTable.forName(spark, target_table)
            .alias("t")
            .merge(deleted_keys.alias("s"), merge_condition)
            .whenMatchedDelete()
            .execute()
        )
    return {
        "mode": "INCREMENTAL",
        "affected_keys": affected_count,
        "upsert_rows": upsert_count,
        "deleted_rows": deleted_count,
    }


def _pipeline_validate_all_compatibility_sources():
    checked = []
    for target_table, contract in sorted(_PIPELINE_CONTRACTS.items()):
        if contract["table_name"] in _PIPELINE_SKIP_TARGETS:
            continue
        canonical_table = (
            _PIPELINE_TARGET_PREFIX
            + "."
            + contract["table_name"]
            + ""
        )
        _pipeline_compatibility_plan(canonical_table, target_table)
        checked.append(target_table)
    print(
        f"[COMPATIBILITY] schema plans validated for {len(checked)} targets"
    )
    return checked


def _pipeline_create_cutover_backups():
    if (
        _PIPELINE_MODE != "production"
        or not _PIPELINE_FULL_REFRESH
        or not _pipeline_optional_bool_parameter(
            "create_cutover_backups",
            True,
        )
    ):
        return {}
    suffix = _PIPELINE_RUN_ID.replace("-", "")[:16]
    for target_table, contract in sorted(_PIPELINE_CONTRACTS.items()):
        if contract["table_name"] in _PIPELINE_SKIP_TARGETS:
            continue
        if not table_exists(target_table):
            continue
        backup_table = (
            target_table
            + "__pre_map_pipeline_replacement_"
            + suffix
        )
        spark.sql(
            f"CREATE TABLE {_pipeline_sql_name(backup_table)} "
            f"SHALLOW CLONE {_pipeline_sql_name(target_table)}"
        )
        _PIPELINE_CUTOVER_BACKUPS[target_table] = backup_table
    _pipeline_audit(
        None,
        "CUTOVER_BACKUPS_CREATED",
        _PIPELINE_CUTOVER_BACKUPS,
    )
    print(
        f"[BACKUP] created {len(_PIPELINE_CUTOVER_BACKUPS)} "
        "pre-cutover shallow clones"
    )
    return dict(_PIPELINE_CUTOVER_BACKUPS)


def _pipeline_publish_compatibility_target(
    canonical_table: str,
    target_table: str,
):
    (
        contract,
        canonical_columns,
        source_keys,
        collapse,
        collapse_order,
    ) = _pipeline_compatibility_plan(canonical_table, target_table)
    ending_version = _pipeline_latest_version(canonical_table)
    if ending_version is None:
        raise RuntimeError(
            f"Could not determine canonical Delta version: {canonical_table}"
        )
    previous_version = _pipeline_checkpoint_version(
        target_table,
        canonical_table,
    )
    full_reason = None
    if _PIPELINE_FULL_REFRESH:
        full_reason = "pipeline_full_refresh"
    elif not table_exists(target_table):
        full_reason = "missing_published_target"
    elif (
        _pipeline_schema_signature(spark.table(target_table).schema)
        != _pipeline_expected_signature(target_table)
    ):
        full_reason = "published_contract_drift"
    elif previous_version is None:
        full_reason = "missing_compatibility_checkpoint"
    elif previous_version > ending_version:
        full_reason = "canonical_version_reset"
    elif _pipeline_table_property(
        target_table,
        _PIPELINE_PROJECTION_VERSION_PROPERTY,
    ) != str(_pipeline_projection_version(contract)):
        full_reason = "compatibility_projection_version"

    if full_reason is not None:
        metrics = _pipeline_full_compatibility_publish(
            canonical_table,
            target_table,
            contract,
            canonical_columns,
            source_keys,
            collapse_order,
            full_reason,
        )
    elif previous_version == ending_version:
        metrics = {
            "mode": "NO_CHANGE",
            "affected_keys": 0,
            "upsert_rows": 0,
            "deleted_rows": 0,
        }
    else:
        try:
            metrics = _pipeline_incremental_compatibility_publish(
                canonical_table,
                target_table,
                contract,
                canonical_columns,
                source_keys,
                collapse_order,
                previous_version,
                ending_version,
            )
        except Exception as exc:
            print(
                f"[WARN] {canonical_table}: compatibility CDF range "
                f"could not be replayed ({str(exc)[:500]}); rebuilding "
                "the published projection from the canonical snapshot"
            )
            metrics = _pipeline_full_compatibility_publish(
                canonical_table,
                target_table,
                contract,
                canonical_columns,
                source_keys,
                collapse_order,
                "compatibility_cdf_unavailable",
            )

    # Validate and stamp the projection version before the source position is
    # staged. A checkpoint recorded ahead of validation would let the next run
    # believe a failed publish had succeeded.
    validation = _pipeline_validate_published_target(target_table, contract)
    spark.sql(
        f"ALTER TABLE {_pipeline_sql_name(target_table)} "
        "SET TBLPROPERTIES ("
        f"'{_PIPELINE_PROJECTION_VERSION_PROPERTY}' = "
        f"'{_pipeline_projection_version(contract)}')"
    )
    _pipeline_stage_checkpoint(
        target_table,
        canonical_table,
        ending_version,
    )
    metrics.update({
        "canonical_table": canonical_table,
        "canonical_ending_version": ending_version,
        "previous_compatibility_version": previous_version,
        "collapse_rule": collapse or [],
        "publication_keys": _pipeline_publication_keys(contract),
        "projection_version": _pipeline_projection_version(contract),
        "row_predicate": (
            contract.get("_compatibility_spec") or {}
        ).get("row_predicate"),
        "published_rows": validation["published_rows"],
    })
    _pipeline_audit(target_table, "COMPATIBILITY_PUBLISH", metrics)
    print(
        f"[COMPATIBILITY] {canonical_table} -> {target_table}: "
        f"{metrics['mode']}"
    )
    return metrics


def _pipeline_publish_all_compatibility_targets():
    raise RuntimeError(
        "The canonical-to-compatibility publisher is retired. "
        "Primary components now write the unsuffixed single layer directly."
    )

# COMMAND ----------

import hashlib as _pipeline_hashlib


def _pipeline_utc_now() -> str:
    return _pipeline_datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _pipeline_capture_source_manifest():
    """Pin every tracked source to the Delta version this run will read.

    The hash of that manifest is what makes resume safe: a component may only
    be skipped when it completed in this run against exactly these versions.
    """
    global _PIPELINE_SOURCE_MANIFEST
    global _PIPELINE_SOURCE_MANIFEST_HASH
    manifest = {}
    for source_table in sorted(set(_PIPELINE_REQUIRED_TABLES)):
        if not table_exists(source_table):
            continue
        try:
            version = _pipeline_latest_version(source_table)
        except Exception:
            # A source that cannot report a Delta version is left out
            # of the manifest rather than failing startup. It then
            # never contributes to a resume decision, which is the
            # safe direction: the component reruns.
            version = None
        if version is not None:
            manifest[source_table] = int(version)
    _PIPELINE_SOURCE_MANIFEST = manifest
    _PIPELINE_SOURCE_MANIFEST_HASH = _pipeline_hashlib.sha256(
        _pipeline_json.dumps(manifest, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    print(
        f"[MANIFEST] pinned {len(manifest)} source tables at "
        f"{_PIPELINE_SOURCE_MANIFEST_HASH}"
    )
    return _PIPELINE_SOURCE_MANIFEST_HASH


def _pipeline_validate_published_target(target_table: str, contract):
    """Structural check that a published table is usable.

    Row level scope and grain assertions belong in the post deployment
    checks; this runs on every publish so it stays metadata only.
    """
    published = spark.table(target_table)
    signature = _pipeline_schema_signature(published.schema)
    expected = _pipeline_expected_signature(target_table)
    if signature != expected:
        raise RuntimeError(
            f"Published {target_table} does not match its frozen contract "
            f"after publish; expected {expected} but found {signature}"
        )
    return {"published_rows": published.count()}


# Assertions run against the live tables after a successful publish, and
# reused by bronze_release_validate against candidate tables. Each entry is a
# query returning a single violations column that must be zero.
_PIPELINE_POST_DEPLOYMENT_ASSERTIONS = [
    {
        "name": "coded_events_zero_result_padding",
        "tables": ["{prefix}.map_coded_events{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_coded_events{suffix} "
            "WHERE RESULT_CD IS NULL OR RESULT_CD = 0"
        ),
    },
    {
        # Retargeted from encounter_sentinel_departure, which tested
        # ENCOUNTER_END_DT_TM_EFFECTIVE -- a column this release removes,
        # because its coalesce ladder read EST_DEPART_DT_TM, a booking slot.
        # The sentinel test survives; only its subject changes.
        "name": "encounter_sentinel_timestamps",
        "tables": ["{prefix}.map_encounter{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_encounter{suffix} "
            "WHERE ARRIVAL_DT_TM_BEST >= TIMESTAMP'2100-01-01' "
            "OR DEPARTURE_DT_TM_BEST >= TIMESTAMP'2100-01-01' "
            "OR ENCOUNTER_COMPLETE_DT_TM_EFFECTIVE >= TIMESTAMP'2100-01-01'"
        ),
    },
    {
        # 406,239 future-dated arrivals on candidate 20260726_v1, latest
        # 2032-06-03, every one from the EST_ARRIVE_DT_TM arm. The plausibility
        # gate rejects any arm value past now, so this holds by construction --
        # and stays a check precisely because that kind of regression is silent.
        "name": "encounter_best_arrival_future",
        "tables": ["{prefix}.map_encounter{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_encounter{suffix} "
            "WHERE ARRIVAL_DT_TM_BEST > current_timestamp()"
        ),
    },
    {
        # 700,917 rows carried EFFECTIVE_END_BEFORE_START_IND on candidate
        # 20260726_v1, 683,479 of them caused by the same EST arm. The departure
        # ladder now nulls the departure and records DEPARTURE_REJECTED_REASON
        # rather than publishing an interval that runs backwards.
        "name": "encounter_best_departure_before_arrival",
        "tables": ["{prefix}.map_encounter{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_encounter{suffix} "
            "WHERE DEPARTURE_DT_TM_BEST < ARRIVAL_DT_TM_BEST"
        ),
    },
    {
        # The headline defect this release removes: 3,172,028 encounters on
        # candidate 20260726_v1 with no arrival, no registration and no clinical
        # event were nonetheless asserted an arrival, every one of them read off
        # a booking slot. An arrival now requires a witness.
        "name": "encounter_best_arrival_without_evidence",
        "tables": ["{prefix}.map_encounter{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_encounter{suffix} "
            "WHERE ARRIVAL_DT_TM_BEST IS NOT NULL "
            "AND ATTENDANCE_EVIDENCE IN ('NONE', 'CANCELLED', 'FUTURE_SCHEDULED')"
        ),
    },
    {
        # Domain and null-consistency in one pass. Every method value must name
        # a declared arm -- so no scheduled, estimated, complete or create
        # column can ever creep back in under a new arm name -- and a method
        # must be present exactly when a value is, which is the count
        # reconciliation the plan asks for stated as an invariant rather than a
        # report.
        "name": "encounter_time_model_method_domain",
        "tables": ["{prefix}.map_encounter{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM ("
            "SELECT count(*) AS total, "
            "sum(CASE WHEN ARRIVAL_METHOD IS NULL OR ARRIVAL_METHOD IN "
            "('ARRIVE_DT_TM', 'REG_DT_TM', 'FIRST_ACTIVITY') "
            "THEN 1 ELSE 0 END) AS arrival_known, "
            "sum(CASE WHEN DEPARTURE_METHOD IS NULL OR DEPARTURE_METHOD IN "
            "('DEPART_DT_TM', 'DISCH_DT_TM', 'WARD_OUT', 'LAST_ACTIVITY') "
            "THEN 1 ELSE 0 END) AS departure_known, "
            "sum(CASE WHEN (ARRIVAL_METHOD IS NULL) <> (ARRIVAL_DT_TM_BEST IS NULL) "
            "THEN 1 ELSE 0 END) AS arrival_mismatch, "
            "sum(CASE WHEN (DEPARTURE_METHOD IS NULL) <> (DEPARTURE_DT_TM_BEST IS NULL) "
            "THEN 1 ELSE 0 END) AS departure_mismatch "
            "FROM {prefix}.map_encounter{suffix}"
            ") t WHERE t.total <> t.arrival_known "
            "OR t.total <> t.departure_known "
            "OR t.arrival_mismatch > 0 OR t.departure_mismatch > 0"
        ),
    },
    {
        # Emitted only from two direct-observation endpoints, so a negative
        # length of stay is unreachable without a logic regression. Legacy
        # reaches -6,612 days.
        "name": "encounter_length_of_stay_negative",
        "tables": ["{prefix}.map_encounter{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_encounter{suffix} "
            "WHERE LENGTH_OF_STAY_MINUTES < 0"
        ),
    },
    {
        # An activity-derived endpoint is accurate to roughly plus or minus 45
        # days between the 1st and 95th percentiles: fit to assert that someone
        # attended, not to subtract. Letting one reach a length of stay would
        # reproduce the very defect this release removes, merely with better
        # provenance.
        "name": "encounter_length_of_stay_from_activity",
        "tables": ["{prefix}.map_encounter{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_encounter{suffix} "
            "WHERE LENGTH_OF_STAY_MINUTES IS NOT NULL "
            "AND (ARRIVAL_METHOD = 'FIRST_ACTIVITY' "
            "OR DEPARTURE_METHOD = 'LAST_ACTIVITY')"
        ),
    },
    {
        # A rejection reason is a closed vocabulary; an unrecognised value means
        # a consumer filtering on it would silently keep a refused departure.
        "name": "encounter_departure_rejected_reason_domain",
        "tables": ["{prefix}.map_encounter{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_encounter{suffix} "
            "WHERE DEPARTURE_REJECTED_REASON IS NOT NULL "
            "AND DEPARTURE_REJECTED_REASON NOT IN "
            "('BEFORE_ARRIVAL', 'ADMINISTRATIVE_CLOSE', 'FIXED_WINDOW_CLOSE')"
        ),
    },
    {
        "name": "encounter_compat_missing_arrival",
        "tables": ["{prefix}.map_encounter{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_encounter{suffix} WHERE ARRIVE_DT_TM IS NULL"
        ),
    },
    {
        # Legacy carries 2,658,885 negative arrive/depart intervals of its own,
        # 5.6917% of 46,715,560 rows, measured on the pre-replacement clone
        # map_encounter__pre_map_pipeline_replacement_945df7fc45a94b4d -- whose
        # DESCRIBE HISTORY records sourceVersion 316, an OPTIMIZE two seconds
        # after the last legacy MERGE, so it is genuine legacy content and not
        # an earlier compat build. These columns are contracted to reproduce
        # that chain and they do, at row level rather than in aggregate:
        # 2,626,129 ENCNTR_IDs violate in both tables, 2,621,632 of them with
        # byte-identical ARRIVE_DT_TM and DEPART_DT_TM. A zero-tolerance gate
        # here failed the release for being correct.
        #
        # The intervals are structural. Arrival and departure derive from
        # independent sources and both outlier rules shrink the interval --
        # arrival forward to the first clinical event, departure back to the
        # last -- with nothing constraining depart >= arrive. 79.5% of violators
        # have a null raw ARRIVE_DT_TM and take compat arrival from
        # EST_ARRIVE_DT_TM, a scheduled slot that can postdate the recorded
        # departure; 42% are Outpatient Pre-registration. map_10_foundation
        # already publishes COMPAT_END_BEFORE_START_IND for this exact state.
        #
        # A rate ceiling, not a differential against the frozen clone: that
        # clone is SHALLOW over a table that is routinely VACUUMed, and
        # _pipeline_post_deployment_checks SKIPs an assertion whose tables are
        # missing, so losing it would present as a silent pass rather than an
        # error. A ceiling also covers rows with no legacy counterpart -- 4,221
        # of the violations sit in the 76,034 rows an inner join cannot see.
        # Anchors: legacy 0.056917, candidate 20260726_v1 0.056857. The ceiling
        # leaves 14% relative headroom, so ordinary drift passes and a chain
        # regression that doubles the rate fails immediately.
        "name": "encounter_compat_negative_interval_rate",
        "tables": ["{prefix}.map_encounter{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM ("
            "SELECT count(*) AS total, "
            "sum(CASE WHEN DEPART_DT_TM IS NOT NULL "
            "AND ARRIVE_DT_TM IS NOT NULL "
            "AND DEPART_DT_TM < ARRIVE_DT_TM THEN 1 ELSE 0 END) AS negative "
            "FROM {prefix}.map_encounter{suffix}) t "
            "WHERE t.total = 0 OR (t.negative / t.total) > 0.065"
        ),
    },
    {
        "name": "address_compat_out_of_scope",
        "tables": ["{prefix}.map_address{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_address{suffix} "
            "WHERE PARENT_ENTITY_NAME NOT IN ('PERSON', 'ORGANIZATION') "
            "OR ACTIVE_IND <> 1"
        ),
    },
    {
        # Only PERSON rows are masked. Organisations keep the whole postcode,
        # which is what legacy did and what the compatibility specification now
        # reproduces, so the old unscoped form of this assertion would have
        # failed the release for being correct.
        "name": "address_compat_person_mask_too_wide",
        "tables": ["{prefix}.map_address{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_address{suffix} "
            "WHERE PARENT_ENTITY_NAME = 'PERSON' "
            "AND length(masked_zipcode) > 3"
        ),
    },
    {
        # The other half of the same contract: an organisation whose published
        # postcode is the three-character TRUNCATION of its own source value
        # means the mask has leaked across the entity boundary.
        #
        # The earlier form flagged any organisation value of three characters or
        # fewer, on the premise that short genuine postcodes do not exist. They
        # do: 763 organisation rows carry a raw mill_address.ZIPCODE that is
        # itself three characters or fewer and is published byte-identically --
        # London districts typed into the postcode field for schools, nurseries
        # and PRUs (N16 x71, E1 x70, E14 x51), case variants, junk sentinels
        # like ZZZ, and one Dublin postal district. Two independent proofs that
        # nothing was masked: 296 of them are one or two characters, which
        # substring(ZIPCODE, 1, 3) cannot produce from any input; and on the
        # frozen pre-replacement clone, genuine organisation truncations number
        # zero across all 42,406 joinable rows, so legacy never masked one.
        #
        # Matching the truncation signature is the actual leak test, and unlike
        # a bare equality against the spec it demonstrably fires: on candidate
        # 20260726_v1 it returns 0 for ORGANIZATION and 2,862,200 for PERSON,
        # where masking is intended. ADDRESS_ID is unique in both tables, so the
        # join cannot fan out.
        "name": "address_compat_organisation_truncated",
        "tables": [
            "{prefix}.map_address{suffix}",
            "{prefix}.map_address{suffix}",
        ],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_address{suffix} v JOIN "
            "{prefix}.map_address{suffix} c USING (ADDRESS_ID) "
            "WHERE v.PARENT_ENTITY_NAME = 'ORGANIZATION' "
            "AND v.masked_zipcode IS NOT NULL "
            "AND c.SOURCE_ZIPCODE IS NOT NULL "
            "AND length(trim(c.SOURCE_ZIPCODE)) > 3 "
            "AND v.masked_zipcode = substr(trim(c.SOURCE_ZIPCODE), 1, 3)"
        ),
    },
    {
        "name": "diagnosis_compat_dotted_icd10",
        "tables": ["{prefix}.map_diagnosis{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_diagnosis{suffix} WHERE ICD10_CODE LIKE '%.%'"
        ),
    },
    {
        "name": "medical_personnel_compat_null_service",
        "tables": ["{prefix}.map_medical_personnel{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_medical_personnel{suffix} "
            "WHERE SRVCATEGORY IS NULL OR SURGSPEC IS NULL "
            "OR MEDSERVICE IS NULL"
        ),
    },
    {
        "name": "mat_birth_babies_lost_at_publish",
        "tables": [
            "{prefix}.map_mat_birth{suffix}",
            "{prefix}.map_mat_birth{suffix}",
        ],
        "sql": (
            "SELECT count(*) AS violations FROM ("
            "SELECT BabyPerson_ID FROM "
            "{prefix}.map_mat_birth{suffix} "
            "WHERE BabyPerson_ID IS NOT NULL "
            "EXCEPT "
            "SELECT BabyPerson_ID FROM {prefix}.map_mat_birth{suffix} "
            "WHERE BabyPerson_ID IS NOT NULL)"
        ),
    },
    {
        "name": "pathology_compat_unresolved_person",
        "tables": ["{prefix}.map_pathology{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_pathology{suffix} WHERE PERSON_ID IS NULL"
        ),
    },
    {
        # person_type_cd 903 is 'Person'. 900 is 'Freetext' and accounts for
        # 4,992,968 stub rows that the curated canonical table must not carry.
        "name": "person_canonical_non_person_types",
        "tables": ["{prefix}.map_person{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_person{suffix} "
            "WHERE person_type_cd IS NULL OR person_type_cd <> 903"
        ),
    },
    {
        # Legacy derived the timestamp and the three calendar parts from one
        # column, so they can never disagree. The canonical table derives them
        # from different columns, which is the whole reason the compatibility
        # specification overrides all four together.
        "name": "person_compat_birth_parts_disagree",
        "tables": ["{prefix}.map_person{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_person{suffix} "
            "WHERE birth_datetime IS NOT NULL AND ("
            "birth_year <> year(birth_datetime) "
            "OR birth_month <> month(birth_datetime) "
            "OR birth_day <> dayofmonth(birth_datetime))"
        ),
    },
    {
        # 5,839,061 of 19,577,601 Barts procedure rows are inactive or undated.
        "name": "procedure_canonical_out_of_scope",
        "tables": ["{prefix}.map_procedure{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_procedure{suffix} "
            "WHERE PROC_DT_TM IS NULL"
        ),
    },
    {
        # A SNOMED type with no SNOMED code is the signature of the old 2^53
        # integer guard destroying UK extension codes: 1,265,889 diagnosis rows
        # and 7,267 problem rows were affected.
        "name": "diagnosis_snomed_type_without_code",
        "tables": ["{prefix}.map_diagnosis{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_diagnosis{suffix} "
            "WHERE SNOMED_TYPE IS NOT NULL AND SNOMED_CODE IS NULL"
        ),
    },
    {
        "name": "procedure_snomed_type_without_code",
        "tables": ["{prefix}.map_procedure{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_procedure{suffix} "
            "WHERE SNOMED_TYPE IS NOT NULL AND SNOMED_CODE IS NULL"
        ),
    },
    {
        # location_type_cd 794 is Nurse Unit. Retired units deliberately survive
        # when retained facts still reference them, so this checks the type
        # without publishing a separate presence marker.
        "name": "care_site_canonical_non_nurse_unit",
        "tables": ["{prefix}.map_care_site{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_care_site{suffix} "
            "WHERE location_type_cd IS NULL OR location_type_cd <> 794"
        ),
    },
    {
        # Placeholder rows carry neither a UPRN nor an EPC certificate key and
        # have no business in the final certificate table.
        "name": "address_epc_unmatched_placeholders",
        "tables": ["{prefix}.map_address_epc{suffix}"],
        "sql": (
            "SELECT count(*) AS violations FROM "
            "{prefix}.map_address_epc{suffix} "
            "WHERE UPRN IS NULL OR EPC_LMK_KEY IS NULL"
        ),
    },
]


def _pipeline_post_deployment_checks(
    prefix: str = None,
    suffix: str = "",
    raise_on_failure: bool = True,
):
    """Run every acceptance assertion, reporting all of them not just the first."""
    runtime_spark = _pipeline_active_spark()
    prefix = prefix or _PIPELINE_TARGET_PREFIX
    results = []
    for assertion in _PIPELINE_POST_DEPLOYMENT_ASSERTIONS:
        tables = [
            table.format(prefix=prefix, suffix=suffix)
            for table in assertion["tables"]
        ]
        missing = [
            table
            for table in tables
            if not runtime_spark.catalog.tableExists(table)
        ]
        if missing:
            results.append({
                "name": assertion["name"],
                "status": "SKIPPED",
                "detail": f"missing tables: {missing}",
            })
            continue
        query = assertion["sql"].format(prefix=prefix, suffix=suffix)
        try:
            violations = int(runtime_spark.sql(query).collect()[0]["violations"])
        except Exception as exc:
            results.append({
                "name": assertion["name"],
                "status": "ERROR",
                "detail": str(exc)[:500],
            })
            continue
        results.append({
            "name": assertion["name"],
            "status": "PASS" if violations == 0 else "FAIL",
            "violations": violations,
        })
    try:
        for contract_result in _PIPELINE_ASSERT_ALL_PRIMARY_CONTRACTS(prefix):
            results.append({
                "name": "single_layer_schema_" + contract_result["table"].rsplit(".", 1)[-1],
                "status": "PASS",
                "columns": contract_result["columns"],
            })
    except Exception as exc:
        results.append({
            "name": "single_layer_schema_contracts",
            "status": "ERROR",
            "detail": str(exc)[:2000],
        })

    failed = [
        result
        for result in results
        if result["status"] in ("FAIL", "ERROR")
    ]
    for result in results:
        print(f"[CHECK] {result['status']} {result['name']} {result}")
    _pipeline_audit(
        None,
        "POST_DEPLOYMENT_CHECKS",
        {"prefix": prefix, "suffix": suffix, "results": results},
    )
    if failed and raise_on_failure:
        raise RuntimeError(f"Post deployment checks failed: {failed}")
    return results


# COMMAND ----------

try:
    _pipeline_capture_source_manifest()
    _pipeline_adopt_existing_contracts("4_prod.bronze")
    _pipeline_preflight()
except Exception as exc:
    _pipeline_record_error(
        None,
        exc,
        "STARTUP_ERROR",
    )
    raise

# COMMAND ----------

# MAGIC # pathology_embed_increment is folded into map_50_pathology, the only component that calls it

# COMMAND ----------

# Loading this notebook is normally the first act of a pipeline run, so it
# creates the control tables and audits RUN_START -- loading it is a production
# write. The release tooling needs the compatibility specifications and the
# acceptance assertions defined above without taking that write, so it loads
# with map_common_bootstrap=false. The read-only path announces itself, because
# a pipeline that quietly did not audit its own start would be worse than one
# that failed.
_PIPELINE_BOOTSTRAP = _pipeline_optional_bool_parameter(
    "map_common_bootstrap",
    True,
)
if _PIPELINE_BOOTSTRAP:
    try:
        _pipeline_cleanup_orphan_staging()
        _pipeline_ensure_control_tables()
        _pipeline_audit(
            None,
            "RUN_START",
            {"mode": _PIPELINE_MODE},
        )
    except Exception as exc:
        _pipeline_record_error(
            None,
            exc,
            "STARTUP_ERROR",
        )
        raise
else:
    print(
        "[MAP-COMMON] read-only load: control tables not ensured and no "
        "RUN_START audit written. Do not run a pipeline component from this "
        "session."
    )

# COMMAND ----------

_PIPELINE_RUN_PATHOLOGY = True
_PIPELINE_RUN_EMBEDDINGS = True
_PIPELINE_CANONICAL_TARGETS = ['4_prod.bronze.map_address', '4_prod.bronze.map_address_epc', '4_prod.bronze.map_person', '4_prod.bronze.map_care_site', '4_prod.bronze.map_medical_personnel', '4_prod.bronze.map_encounter', '4_prod.bronze.map_diagnosis', '4_prod.bronze.map_problem', '4_prod.bronze.map_med_admin', '4_prod.bronze.map_procedure', '4_prod.bronze.map_death', '4_prod.bronze.map_numeric_events', '4_prod.bronze.map_date_events', '4_prod.bronze.map_text_events', '4_prod.bronze.map_nomen_events', '4_prod.bronze.map_coded_events', '4_prod.bronze.map_mat_pregnancy', '4_prod.bronze.map_mat_birth', '4_prod.bronze.map_mat_vte_assessment', '4_prod.bronze.map_family_history', '4_prod.bronze.map_patient_journey', '4_prod.bronze.map_implant_details', '4_prod.bronze.map_pathology']
_PIPELINE_SKIP_TARGETS = set()
_PIPELINE_ACTIVE_CANONICAL_TARGETS = [
    table_name for table_name in _PIPELINE_CANONICAL_TARGETS
    if table_name.rsplit('.', 1)[-1]
    not in _PIPELINE_SKIP_TARGETS
]
_PIPELINE_TARGETS_MISSING_LOGIC_VERSION = sorted(
    base_name
    for base_name in (
        table_name.rsplit('.', 1)[-1]
        for table_name in _PIPELINE_CANONICAL_TARGETS
    )
    if base_name not in _PIPELINE_CANONICAL_LOGIC_VERSIONS
)
if _PIPELINE_TARGETS_MISSING_LOGIC_VERSION:
    raise RuntimeError(
        "These canonical targets have no _PIPELINE_CANONICAL_LOGIC_VERSIONS "
        "entry and would default to version 1, exempting them from the "
        "rebuild gate: "
        f"{_PIPELINE_TARGETS_MISSING_LOGIC_VERSION}"
    )
_PIPELINE_FORCE_FULL_REFRESH = _pipeline_optional_bool_parameter('force_full_refresh', False)
_PIPELINE_FULL_REFRESH = (
    _PIPELINE_FORCE_FULL_REFRESH
    or any(
    not table_exists(table_name)
    for table_name in _PIPELINE_ACTIVE_CANONICAL_TARGETS
    )
)
print(
    f'[MODE] improved canonical full_refresh={_PIPELINE_FULL_REFRESH}'
)

# COMMAND ----------

def finalize_map_pipeline_run():
    """Finish a map run inside this notebook's defining namespace.

    Databricks %run reliably exposes public names to the caller, while private
    underscore-prefixed implementation names can remain scoped to the notebook
    module after nested component runs. Keep every private finalization detail
    behind this public boundary.
    """
    _pipeline_active_spark()

    post_deployment_report = None
    if _pipeline_optional_bool_parameter(
        "run_post_deployment_checks",
        True,
    ):
        post_deployment_report = _pipeline_post_deployment_checks()
    else:
        print(
            "[INFO] Post-deployment checks disabled by "
            "run_post_deployment_checks"
        )

    # Advance source checkpoints only after all primary writes and acceptance
    # assertions have succeeded.
    _pipeline_commit_all_checkpoints()

    updated_targets = sorted(set(_PIPELINE_UPDATED_TARGETS))
    run_summary = {
        "pipeline": "map_pipeline",
        "release": _PIPELINE_RELEASE_ID,
        "run_id": _PIPELINE_RUN_ID,
        "mode": "single_layer",
        "full_refresh": _PIPELINE_FULL_REFRESH,
        "updated_targets": updated_targets,
        "post_deployment_checks": post_deployment_report,
        "finished_at": _pipeline_utc_now(),
    }
    _pipeline_audit(
        None,
        "RUN_SUCCESS",
        {
            "updated_targets": updated_targets,
            "post_deployment_checks": post_deployment_report,
        },
    )
    result_json = _pipeline_json.dumps(
        run_summary,
        default=str,
        sort_keys=True,
    )
    print(result_json)
    return result_json


# Persist the callable outside the mutable %run globals. The component
# notebooks can leave caller commands without names defined by _map_common,
# while Python's builtins module remains process-wide for this interpreter.
setattr(
    _pipeline_builtins,
    "_adc_bronze_map_pipeline_finalizer_20260726_v1",
    finalize_map_pipeline_run,
)


