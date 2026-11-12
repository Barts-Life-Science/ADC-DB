"""Silver specialist.device_mapping: 1 dataset(s).

- datasets: reference_device_mapping
- moved verbatim from: 1_silver_spine_and_reference:6374, 1_silver_spine_and_reference:6436, 1_silver_spine_and_reference:6439, 1_silver_spine_and_reference:6444, 1_silver_spine_and_reference:6501

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / device mapping
SRC_DEVICE_MAPPING = "4_prod.bronze.map_device_mapping"


DEVICE_MAPPING_LIFECYCLE_FIELDS = [
]


DEVICE_MAPPING_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_device_mapping():
    # Assemble device mapping rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    s = read_source(SRC_DEVICE_MAPPING)
    # contract v2: rename the mapping hash as a key and publish native person and encounter ids
    return s.select(
        stable_id("device_mapping:mill", s.EVENT_ID).alias("device_mapping_key"),
        s.EVENT_ID.cast("string").alias("source_event_id"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.IMPLANT_DESCRIPTION.alias("implant_description"),
        s.NORMALIZED_DESCRIPTION.alias("normalized_description"),
        s.CLEANED_DESCRIPTION.alias("cleaned_description"),
        s.UDI_DI.alias("udi_di"), s.UDI_ISSUER.alias("udi_issuer"),
        s.SOURCE_GS1_IDENTIFIER.alias("gs1_identifier"),
        s.SOURCE_HIBCC_DEVICE_ID.alias("hibcc_device_id"),
        s.EFFECTIVE_SERIAL_NUMBER.alias("serial_number"),
        s.EFFECTIVE_EXPIRY_DATE.alias("expiry_date"),
        s.gmdncode.cast("string").alias("gmdn_code"), s.gmdn_name.alias("gmdn_name"),
        s.snomed_concept_id.cast("string").alias("snomed_concept_id"),
        s.snomed_name.alias("snomed_name"),
        s.STANDARD_CONCEPT_ID.cast("string").alias("standard_concept_id"),
        s.STANDARD_CONCEPT_NAME.alias("standard_concept_name"),
        s.STANDARD_VOCABULARY_ID.alias("standard_vocabulary_id"),
        s.device_type.alias("device_type"), s.mapping_layer.alias("mapping_layer"),
        s.MAPPING_STATUS.alias("mapping_status"), s.mapping_confidence.alias("mapping_confidence"),
        s.confidence_tier.alias("confidence_tier"), s.MAPPING_RULE_ID.alias("mapping_rule_id"),
        s.MATCHED_FIELD.alias("matched_field"), s.MATCHED_VALUE.alias("matched_value"),
        s.MAPPING_CANDIDATE_COUNT.alias("mapping_candidate_count"),
        s.MAPPING_DISTINCT_CONCEPT_COUNT.alias("mapping_distinct_concept_count"),
        s.MAPPING_AMBIGUOUS_IND.alias("mapping_ambiguous_ind"),
        s.MATCHED_OPCS_CODE.alias("matched_opcs_code"),
        s.PROCEDURE_SUPPORT_IND.alias("procedure_support_ind"),
        s.MAPPING_SCHEMA_VERSION.alias("mapping_schema_version"),
        s.NORMALIZATION_VERSION.alias("normalization_version"),
        s.BRAND_RULES_VERSION.alias("brand_rules_version"), s.MAPPED_AT.alias("mapped_at"),
        # PMS_DEVICE_IDENTITY_V1: device identity columns already computed in bronze.
        s.GMDN_IMPLANTABLE.alias("gmdn_implantable"),
        s.GMDN_DEFINITION.alias("gmdn_definition"),
        s.GMDN_CODE_STATUS.alias("gmdn_code_status"),
        s.GMDN_MAPPING_PROVENANCE.alias("gmdn_mapping_provenance"),
        s.GMDN_REFERENCE_PRESENT_IND.alias("gmdn_reference_present_ind"),
        s.GUDID_PRIMARY_DI.alias("gudid_primary_di"),
        s.GUDID_BRAND_NAME.alias("gudid_brand_name"),
        s.GUDID_MODEL_NUMBER.alias("gudid_model_number"),
        s.GUDID_COMPANY_NAME.alias("gudid_company_name"),
        s.GUDID_CATALOGUE_NUMBER.alias("gudid_catalogue_number"),
        s.GUDID_DEVICE_STATUS.alias("gudid_device_status"),
        s.GUDID_DISTRIBUTION_STATUS.alias("gudid_distribution_status"),
        s.SOURCE_CATALOGUE_NUMBER.alias("source_catalogue_number"),
        s.SOURCE_MANUFACTURER.alias("source_manufacturer"),
        s.MAPPED_DEVICE_NAME.alias("mapped_device_name"),
        s.PIPELINE_LOADED_AT.alias("loaded_at"), F.lit("millennium").alias("_source_system"),
        F.lit(SRC_DEVICE_MAPPING).alias("_source_table"),
        s.EVENT_ID.cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.device_mapping"),
    cluster_by=["person_id"], refresh_policy="auto",
)
def device_mapping():
    # Build the declared dataset: Best-effort implant-device decode with complete mapping
    # provenance; identifiers are IG-sensitive.
    return _lifecycle_source_device_mapping().drop(*DEVICE_MAPPING_LIFECYCLE_FIELDS, *DEVICE_MAPPING_RETIRED_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_device_mapping': {'columns': {'_source_row_id': 'Source implant description event identifier.',
                                          '_source_system': 'Value describing source system for the device '
                                                            'mapping record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Whitespace and source sentinel '
                                                            'text are retained unless the pipeline '
                                                            'explicitly normalizes them; null means no value '
                                                            'was supplied.',
                                          '_source_table': 'Value describing source table for the device '
                                                           'mapping record. It is produced by the silver '
                                                           'transformation and has no direct bronze-column '
                                                           'lineage entry. Whitespace and source sentinel '
                                                           'text are retained unless the pipeline explicitly '
                                                           'normalizes them; null means no value was '
                                                           'supplied.',
                                          'brand_rules_version': 'Boundary-aware phrase-rule version.',
                                          'cleaned_description': 'Versioned fallback-matching description.',
                                          'confidence_tier': 'HIGH, MEDIUM, LOW or UNMAPPED.',
                                          'device_mapping_key': 'Deterministic SHA-256 key for the '
                                                                'Millennium device-mapping row.',
                                          'device_type': 'Selected device category/type.',
                                          'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT.',
                                          'expiry_date': 'GS1 expiry date coalesced with raw expiry date.',
                                          'gmdn_code': 'Selected GMDN code.',
                                          'gmdn_code_status': 'GMDN reference status (active/obsolete) of '
                                                              'the mapped term.',
                                          'gmdn_definition': 'GMDN reference definition text for the mapped '
                                                             'GMDN term.',
                                          'gmdn_implantable': 'GMDN reference implantable flag for the '
                                                              'mapped GMDN term, carried from bronze; null '
                                                              'when the term has no reference row.',
                                          'gmdn_mapping_provenance': 'How the GMDN term was reached (method '
                                                                     'and source), carried from bronze.',
                                          'gmdn_name': 'Selected GMDN preferred term name.',
                                          'gmdn_reference_present_ind': 'True when the mapped GMDN code '
                                                                        'exists in the loaded GMDN '
                                                                        'reference.',
                                          'gs1_identifier': 'Source GS1 UDI device identifier.',
                                          'gudid_brand_name': 'GUDID brand name for the matched device '
                                                              'identifier.',
                                          'gudid_catalogue_number': 'GUDID catalogue number for the matched '
                                                                    'device identifier.',
                                          'gudid_company_name': 'GUDID labeler (company) name for the '
                                                                'matched device identifier.',
                                          'gudid_device_status': 'GUDID device record status for the matched '
                                                                 'identifier.',
                                          'gudid_distribution_status': 'GUDID commercial distribution status '
                                                                       'for the matched identifier.',
                                          'gudid_model_number': 'GUDID version or model number for the '
                                                                'matched device identifier.',
                                          'gudid_primary_di': 'FDA GUDID primary device identifier matched '
                                                              'for this implant; null when no GUDID match.',
                                          'hibcc_device_id': 'Source HIBCC UDI device identifier.',
                                          'implant_description': 'Raw source implant description.',
                                          'loaded_at': 'map_device_mapping.PIPELINE_LOADED_AT carried '
                                                       'unchanged. This is the contributing bronze row-write '
                                                       'clock, distinct from MAPPED_AT evaluation time and '
                                                       'from Silver refresh time; a missing value remains '
                                                       'null.',
                                          'mapped_at': 'Timestamp when this event was last evaluated.',
                                          'mapped_device_name': 'Device name chosen by the mapping (GUDID or '
                                                                'GMDN side) for display.',
                                          'mapping_ambiguous_ind': 'Whether multiple credible candidates '
                                                                   'existed.',
                                          'mapping_candidate_count': 'Number of distinct mapping candidates '
                                                                     'retained.',
                                          'mapping_confidence': 'Deterministic evidence score between zero '
                                                                'and one.',
                                          'mapping_distinct_concept_count': 'Number of distinct selected '
                                                                            'standard/GMDN tuples.',
                                          'mapping_layer': 'Selected mapping evidence layer.',
                                          'mapping_rule_id': 'Rule or algorithm identifier for the selected '
                                                             'candidate.',
                                          'mapping_schema_version': 'Output schema and mapping '
                                                                    'implementation version.',
                                          'mapping_status': 'MAPPED, AMBIGUOUS_SELECTED or UNMAPPED.',
                                          'matched_field': 'Source field that produced the selected '
                                                           'candidate.',
                                          'matched_opcs_code': 'OPCS code supporting the selected candidate.',
                                          'matched_value': 'Normalized source value used for the selected '
                                                           'candidate.',
                                          'normalization_version': 'Description normalization version.',
                                          'normalized_description': 'Versioned punctuation/whitespace '
                                                                    'normalized description.',
                                          'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                          'procedure_support_ind': 'Whether procedure context supports the '
                                                                   'selected candidate.',
                                          'serial_number': 'GS1 serial number coalesced with raw serial '
                                                           'number.',
                                          'snomed_concept_id': 'Compatibility field populated only for '
                                                               'SNOMED concepts.',
                                          'snomed_name': 'Compatibility field populated only for SNOMED '
                                                         'concepts.',
                                          'source_catalogue_number': 'Catalogue number recorded in SurgiNet '
                                                                     'for this implant, as entered.',
                                          'source_event_id': 'Source implant description event identifier.',
                                          'source_manufacturer': 'Manufacturer recorded in SurgiNet for this '
                                                                 'implant, as entered.',
                                          'standard_concept_id': 'Selected standard OMOP device concept '
                                                                 'identifier.',
                                          'standard_concept_name': 'Selected standard OMOP device concept '
                                                                   'name.',
                                          'standard_vocabulary_id': 'Vocabulary of STANDARD_CONCEPT_ID.',
                                          'udi_di': 'Coalesced source UDI-DI; issuer is retained separately.',
                                          'udi_issuer': 'GS1, HIBCC or null.'},
                              'table': 'Best-effort implant-device decode with complete mapping provenance; '
                                       'identifiers are IG-sensitive.'}}
