# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION to_col_json_str(
# MAGIC   col_name STRING, data_type STRING, sensitive STRING, description STRING
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC /*
# MAGIC RETURN CONCAT(
# MAGIC   '{', '''name'':''', col_name, ''',',
# MAGIC   '''values'':null,',
# MAGIC   '''dataType'':''', data_type, ''',',
# MAGIC   '''sensitive'':', sensitive, ',',
# MAGIC   '''description'':''', description, '''','}')*/
# MAGIC
# MAGIC RETURN TO_JSON(named_struct(
# MAGIC   'name', col_name,
# MAGIC   'values', null,
# MAGIC   'dataType', data_type,
# MAGIC   'sensitive', sensitive,
# MAGIC   'description', description))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE 1_inland.evan_demo.hdruk_metadata_col_mill(
# MAGIC   table_catalog VARCHAR(50),
# MAGIC   table_schema VARCHAR(50),
# MAGIC   table_name VARCHAR(100),
# MAGIC   column_name VARCHAR(200),
# MAGIC   ordinal_position INT,
# MAGIC   dataType VARCHAR(50),
# MAGIC   ig_risk INT,
# MAGIC   ig_severity INT,
# MAGIC   sensitive BOOLEAN,
# MAGIC   column_description STRING,
# MAGIC   json_str STRING,
# MAGIC   json_struct STRUCT<name STRING, values STRING, dataType STRING, sensitive BOOLEAN, description STRING>
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH ig_risk AS (
# MAGIC   SELECT *
# MAGIC   FROM system.information_schema.column_tags AS t
# MAGIC   WHERE LOWER(tag_name) = 'ig_risk'
# MAGIC ),
# MAGIC ig_severity AS (
# MAGIC   SELECT *
# MAGIC   FROM system.information_schema.column_tags AS t
# MAGIC   WHERE LOWER(tag_name) = 'ig_severity'
# MAGIC ),
# MAGIC ig_sensitive AS (
# MAGIC   SELECT
# MAGIC     r.catalog_name,
# MAGIC     r.schema_name,
# MAGIC     r.table_name,
# MAGIC     r.column_name,
# MAGIC     r.tag_value AS ig_risk,
# MAGIC     s.tag_value AS ig_severity,
# MAGIC     CASE 
# MAGIC       WHEN UPPER(r.column_name) = 'ADC_UPDT'
# MAGIC         THEN FALSE
# MAGIC       WHEN r.tag_value IS NULL OR s.tag_value IS NULL
# MAGIC         THEN NULL
# MAGIC       WHEN r.tag_value >= 3 OR s.tag_value >= 2
# MAGIC         THEN TRUE
# MAGIC       ELSE FALSE
# MAGIC     END AS sensitive
# MAGIC   FROM ig_risk AS r
# MAGIC  INNER JOIN ig_severity AS s
# MAGIC   ON
# MAGIC     r.catalog_name = s.catalog_name
# MAGIC     AND r.schema_name = s.schema_name
# MAGIC     AND r.table_name = s.table_name
# MAGIC     AND r.column_name = s.column_name
# MAGIC )
# MAGIC INSERT INTO 1_inland.evan_demo.hdruk_metadata_col_mill
# MAGIC SELECT 
# MAGIC   c.table_catalog,
# MAGIC   c.table_schema,
# MAGIC   c.table_name,
# MAGIC   c.column_name AS name,
# MAGIC   c.ordinal_position,
# MAGIC   c.data_type AS dataType,
# MAGIC   s.ig_risk,
# MAGIC   s.ig_severity,
# MAGIC   s.sensitive,
# MAGIC   c.comment AS description,
# MAGIC   NULL,
# MAGIC   NULL
# MAGIC FROM system.information_schema.columns AS c
# MAGIC LEFT JOIN ig_sensitive AS s
# MAGIC ON
# MAGIC   c.table_catalog = s.catalog_name
# MAGIC   AND c.table_schema = s.schema_name
# MAGIC   AND c.table_name = s.table_name
# MAGIC   AND c.column_name = s.column_name
# MAGIC WHERE
# MAGIC
# MAGIC   (
# MAGIC     c.table_catalog = '4_prod'
# MAGIC     AND c.table_schema = 'raw'
# MAGIC     AND c.table_name ILIKE 'mill_%'
# MAGIC   ) 
# MAGIC   OR
# MAGIC   (
# MAGIC     c.table_catalog = '3_lookup'
# MAGIC     AND c.table_schema = 'mill'
# MAGIC     AND c.table_name ILIKE 'mill_%'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO 1_inland.evan_demo.hdruk_metadata_col_mill AS target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     table_name,
# MAGIC     column_name, 
# MAGIC     CASE
# MAGIC       WHEN sensitive IS NULL AND column_description IS NULL
# MAGIC         THEN to_col_json_str(column_name, dataType, 'null', 'null')
# MAGIC       WHEN sensitive IS NULL
# MAGIC         THEN to_col_json_str(column_name, dataType, 'null', column_description)
# MAGIC       WHEN column_description IS NULL
# MAGIC         THEN to_col_json_str(column_name, dataType, sensitive, 'null')
# MAGIC       ELSE to_col_json_str(column_name, dataType, sensitive, column_description)
# MAGIC     END AS json_str
# MAGIC   FROM 1_inland.evan_demo.hdruk_metadata_col_mill
# MAGIC   WHERE
# MAGIC     (
# MAGIC       table_catalog = '4_prod'
# MAGIC       AND table_schema = 'raw'
# MAGIC       AND table_name ILIKE 'mill_%'
# MAGIC     ) 
# MAGIC     OR
# MAGIC     (
# MAGIC       table_catalog = '3_lookup'
# MAGIC       AND table_schema = 'mill'
# MAGIC       AND table_name ILIKE 'mill_%'
# MAGIC     )
# MAGIC ) AS source
# MAGIC ON target.column_name = source.column_name
# MAGIC AND target.table_name = source.table_name
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET target.json_str = source.json_str

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO 1_inland.evan_demo.hdruk_metadata_col_mill AS target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     table_name,
# MAGIC     column_name, 
# MAGIC     from_json(json_str, 'name STRING, values STRING, dataType STRING, sensitive BOOLEAN, description STRING') AS json_struct
# MAGIC   FROM 1_inland.evan_demo.hdruk_metadata_col_mill
# MAGIC   WHERE
# MAGIC     (
# MAGIC       table_catalog = '4_prod'
# MAGIC       AND table_schema = 'raw'
# MAGIC       AND table_name ILIKE 'mill_%'
# MAGIC     ) 
# MAGIC     OR
# MAGIC     (
# MAGIC       table_catalog = '4_prod'
# MAGIC       AND table_schema = 'rde'
# MAGIC       AND table_name ILIKE 'rde_%'
# MAGIC     )
# MAGIC     OR
# MAGIC     (
# MAGIC       table_catalog = '3_lookup'
# MAGIC       AND table_schema = 'mill'
# MAGIC       AND table_name ILIKE 'mill_%'
# MAGIC     )
# MAGIC ) AS source
# MAGIC ON target.column_name = source.column_name
# MAGIC AND target.table_name = source.table_name
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET target.json_struct = source.json_struct

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if any json_str is null
# MAGIC SELECT * 
# MAGIC FROM 1_inland.evan_demo.hdruk_metadata_col_mill
# MAGIC WHERE json_str IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE 1_inland.evan_demo.hdruk_metadata_tab_mill(
# MAGIC   `name` VARCHAR(100),
# MAGIC   --`columns` STRING,
# MAGIC   `columns` ARRAY<STRUCT<name STRING, values STRING, dataType STRING, sensitive BOOLEAN, description STRING>>,
# MAGIC   description STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO 1_inland.evan_demo.hdruk_metadata_tab_mill
# MAGIC SELECT
# MAGIC   c.table_name,
# MAGIC   --CONCAT('[', array_join(collect_set(c.json_str), ','), ']'),
# MAGIC   collect_set(c.json_struct),
# MAGIC   NULL
# MAGIC FROM 1_inland.evan_demo.hdruk_metadata_col_mill AS c
# MAGIC GROUP BY c.table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO 1_inland.evan_demo.hdruk_metadata_tab_mill AS target
# MAGIC USING (
# MAGIC   SELECT table_name, comment
# MAGIC   FROM system.information_schema.tables
# MAGIC   WHERE
# MAGIC     (
# MAGIC       table_catalog = '4_prod'
# MAGIC       AND table_schema = 'raw'
# MAGIC       AND table_name ILIKE 'mill_%'
# MAGIC     ) 
# MAGIC     OR
# MAGIC     (
# MAGIC       table_catalog = '3_lookup'
# MAGIC       AND table_schema = 'mill'
# MAGIC       AND table_name ILIKE 'mill_%'
# MAGIC     )
# MAGIC ) AS source
# MAGIC ON target.name = source.table_name
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET target.description = source.comment

# COMMAND ----------



# COMMAND ----------

import json




# COMMAND ----------

import requests

api_path = "https://api.dev.hdruk.cloud/api/v1/integrations/datasets/992"
headers = {
    "x-application-id": dbutils.secrets.get(scope="adc_store", key="hdruk_app_id"),
    "x-client-id": dbutils.secrets.get(scope="adc_store", key="hdruk_client_id"),
    "Content-Type": "application/json"
}
response = requests.get(
    "https://api.healthdatagateway.org/api/v1/datasets/992",
    headers=headers
)
print(response)

# COMMAND ----------

response_json = json.loads(response.text)
response_json

# COMMAND ----------

version_nbr_str = response_json["data"]["versions"][0]["metadata"]["metadata"]["required"]["version"]
version_nbrs = version_nbr_str.split(".")
version_nbrs = [int(x) for x in version_nbrs]
new_version_nbr_str = f'{version_nbrs[0]}.{version_nbrs[1]+1 if version_nbrs[2]>=12 else version_nbrs[1]}.{0 if version_nbrs[2]>= 12 else version_nbrs[2]+1}'
print("old:", version_nbr_str)
print("new:", new_version_nbr_str)

# COMMAND ----------

# TODO: Change this template
hdruk_300_str = \
'''
{
    "identifier": "https://web.www.healthdatagateway.org/f948711f-b176-44e4-b57e-5776997a2e75",
    "version": "###VER_NUM_STR###",
    "issued": "2024-11-26T00:00:00.000Z",
    "modified": "###CURR_DATE_STR###T00:00:00.000Z",
    "revisions": [{"url": "https://web.dev.hdruk.cloud//dataset/992?version=1.0.0","version": "1.0.0"}],
    "summary": {
        "title": "Barts Health NHS Cerner Millenium Electronic Patient Record",
        "abstract": "Barts Health NHS Cerner Millenium Electronic Patient Record.",
        "contactPoint": "BartsHealth.ResearchDataRequest@nhs.net",
        "keywords": ["Hospital Inpatient data","Outpatient","Pathology","Radiology","Maternity","Critical Care","Pharmacy"],
        "alternateIdentifiers": null,
        "doiName": null,
        "populationSize": ###PATIENT_COUNT_INT###,
        "dataCustodian": {
            "identifier": "https://ror.org/00b31g692",
            "name": "Barts Health NHS Trust",
            "logo": "https://media.prod.hdruk.cloud/teams/nhs-barts-health.jpg",
            "description": null,
            "contactPoint": "BartsHealth.ResearchDataRequest@nhs.net",
            "memberOf": "Alliance"
        }
    },
    "documentation": {
        "description": "Barts Health NHS Trust Cerner Millenium Electronic Patient Record.",
        "associatedMedia": null,
        "inPipeline": null
    },
    "coverage": {
        "spatial": "United Kingdom,England",
        "followUp": null,
        "pathway": null,
        "typicalAgeRangeMin": 0,
        "typicalAgeRangeMax": 150,
        "datasetCompleteness": null,
        "materialType": [
            "None/not available"
        ]
    },
    "provenance": {
        "origin": {
            "purpose": [
                "Other",
                "Administrative"
            ],
            "source": [
                "EPR"
            ],
            "datasetType": ["Health and disease"],
            "datasetSubType": null,
            "collectionSource": null,
            "imageContrast": null
        },
        "temporal": {
            "distributionReleaseDate": null,
            "startDate": "2008-01-01",
            "endDate": null,
            "timeLag": "Variable",
            "publishingFrequency": "Daily"
        }
    },
    "accessibility": {
        "usage": {
            "dataUseLimitation": ["General research use"],
            "dataUseRequirements": ["Ethics approval required","Project-specific restrictions","User-specific restriction"],
            "resourceCreator": "Barts Health"
        },
        "access": {
            "accessRights": null,
            "accessService":"Barts Health has a secure data environment since 2024. Projects requiring access to data can make an application on the Data Portal (data.bartshealth.nhs.uk).",
            "accessRequestCost": "Cost Recovery Model",
            "deliveryLeadTime": null,
            "jurisdiction": ["GB-ENG"],
            "dataProcessor": null,
            "dataController": "Barts Health",
            "accessServiceCategory": null
        },
        "formatAndStandards": {
            "vocabularyEncodingScheme": [
                "ODS",
                "SNOMED CT",
                "NHS NATIONAL CODES",
                "ICD10"
            ],
            "conformsTo": [
                "NHS DATA DICTIONARY"
            ],
            "language": ["en"],
            "format": ["CSV"]
        }
    },
    "enrichmentAndLinkage": {
        "tools": null,
        "derivedFrom": null,
        "isPartOf": null,
        "linkableDatasets": null,
        "similarToDatasets": null,
        "publicationAboutDataset": null,
        "investigations": null,
        "publicationUsingDataset": null
    },
    "observations": [
        {
            "observedNode": "Persons",
            "measuredValue": ###PATIENT_COUNT_INT###,
            "measuredProperty": "COUNT",
            "observationDate": "###CURR_DATE_STR###",
            "disambiguatingDescription": "Total number of distinct PERSON_ID in the Millenium Encounter table"
        }
    ],
    "structuralMetadata": {
        "tables": [
            {
                "name": "IAPT.iapt.Rep_Referral",
                "description": "IAPT.iapt.Rep_Referral",
                "columns": [
                    {
                        "name": "Count of number of Non-guided Self Help (Computer) sessions (derived)",
                        "description": "Count of number of Non-guided Self Help (Computer) sessions (derived)",
                        "dataType": "Number",
                        "sensitive": false,
                        "values": null
                    }
                ]
            },
            {
                "name": "IAPT.iapt.Rep_Referral",
                "description": "IAPT.iapt.Rep_Referral",
                "columns": [
                    {
                        "name": "Pseudonymised Service Request Identifier",
                        "description": "A request for the provision of care services to a PATIENT.",
                        "dataType": "String",
                        "sensitive": false,
                        "values": null
                    }
                ]
            }
        ],
        "syntheticDataWebLink": null
    }
}
'''

# COMMAND ----------

patientcount = spark.sql("SELECT COUNT(DISTINCT PERSON_ID) AS patientcount FROM 4_prod.raw.mill_encounter").collect()[0]["patientcount"]
patientcount = round(patientcount,-5)
print(patientcount)
hdruk_300_str = hdruk_300_str.replace("###PATIENT_COUNT_INT###", str(patientcount))

# COMMAND ----------

hdruk_300_str = hdruk_300_str.replace("###VER_NUM_STR###", new_version_nbr_str)

# COMMAND ----------

currentdate = spark.sql("SELECT CAST(CURRENT_DATE() AS STRING) AS curr_date").collect()[0]["curr_date"]
print(currentdate)
hdruk_300_str = hdruk_300_str.replace("###CURR_DATE_STR###", currentdate)

# COMMAND ----------

hdruk_300_json = json.loads(hdruk_300_str)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

def update_struct_metadata(metadata_json):
    df = spark.read.table("1_inland.evan_demo.hdruk_metadata_tab_mill")
    struct_metadata_dict = df.toPandas().to_dict(orient="records")

    for i, d in enumerate(struct_metadata_dict):
        try:
            d["columns"] = d["columns"].tolist()
        except:
            print(i)
            
    metadata_json["structuralMetadata"]["tables"] = struct_metadata_dict

# COMMAND ----------

update_struct_metadata(hdruk_300_json)

# COMMAND ----------

hdruk_300_json["structuralMetadata"]["tables"]

# COMMAND ----------



# COMMAND ----------

import requests

api_path = "https://api.dev.hdruk.cloud/api/v1/integrations/datasets/992"
headers = {
    "x-application-id": dbutils.secrets.get(scope="adc_store", key="hdruk_app_id"),
    "x-client-id": dbutils.secrets.get(scope="adc_store", key="hdruk_client_id"),
    "Content-Type": "application/json"
}
response = requests.get(
    "https://api.healthdatagateway.org/api/v1/datasets/992",
    headers=headers
)
print(response)

# COMMAND ----------

import json
response_json = json.loads(response.text)
response_json

# COMMAND ----------

import requests

api_path = "https://api.healthdatagateway.org/api/v1/integrations/datasets/992"
headers = {
    "x-application-id": dbutils.secrets.get(scope="adc_store", key="hdruk_app_id"),
    "x-client-id": dbutils.secrets.get(scope="adc_store", key="hdruk_client_id"),
    "Content-Type": "application/json"
}
response = requests.put(
    f"{api_path}",
    headers=headers,
    json={"metadata":hdruk_300_json}
)
print(response.status_code)
