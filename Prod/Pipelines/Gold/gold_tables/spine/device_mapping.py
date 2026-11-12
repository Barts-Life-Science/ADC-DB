"""Gold spine.device_mapping: 2 dataset(s).

- datasets: _qc_reference_device_mapping, reference_device_mapping
- moved verbatim from: 1_gold_spine_and_reference:2315, 1_gold_spine_and_reference:2376, 1_gold_spine_and_reference:2451, 1_gold_spine_and_reference:2455, 1_gold_spine_and_reference:2467

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.device_mapping ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_DEVICE_MAPPING_SELECT = [
    '`device_mapping_key` AS `device_mapping_key`',
    '`source_event_id` AS `source_event_id`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`implant_description` AS `implant_description`',
    '`normalized_description` AS `normalized_description`',
    '`cleaned_description` AS `cleaned_description`',
    '`udi_di` AS `udi_di`',
    '`udi_issuer` AS `udi_issuer`',
    '`gs1_identifier` AS `gs1_identifier`',
    '`hibcc_device_id` AS `hibcc_device_id`',
    '`serial_number` AS `serial_number`',
    '`expiry_date` AS `expiry_date`',
    '`gmdn_code` AS `gmdn_code`',
    '`gmdn_name` AS `gmdn_name`',
    '`snomed_concept_id` AS `snomed_concept_id`',
    '`snomed_name` AS `snomed_name`',
    '`standard_concept_id` AS `standard_concept_id`',
    '`standard_concept_name` AS `standard_concept_name`',
    '`standard_vocabulary_id` AS `standard_vocabulary_id`',
    '`device_type` AS `device_type`',
    '`mapping_layer` AS `mapping_layer`',
    '`mapping_status` AS `mapping_status`',
    '`mapping_confidence` AS `mapping_confidence`',
    '`confidence_tier` AS `confidence_tier`',
    '`mapping_rule_id` AS `mapping_rule_id`',
    '`matched_field` AS `matched_field`',
    '`matched_value` AS `matched_value`',
    '`mapping_candidate_count` AS `mapping_candidate_count`',
    '`mapping_distinct_concept_count` AS `mapping_distinct_concept_count`',
    '`mapping_ambiguous_ind` AS `mapping_ambiguous_ind`',
    '`matched_opcs_code` AS `matched_opcs_code`',
    '`procedure_support_ind` AS `procedure_support_ind`',
    '`mapping_schema_version` AS `mapping_schema_version`',
    '`normalization_version` AS `normalization_version`',
    '`brand_rules_version` AS `brand_rules_version`',
    '`mapped_at` AS `mapped_at`',
    # PMS_DEVICE_IDENTITY_V1
    '`gmdn_implantable` AS `gmdn_implantable`',
    '`gmdn_definition` AS `gmdn_definition`',
    '`gmdn_code_status` AS `gmdn_code_status`',
    '`gmdn_mapping_provenance` AS `gmdn_mapping_provenance`',
    '`gmdn_reference_present_ind` AS `gmdn_reference_present_ind`',
    '`gudid_primary_di` AS `gudid_primary_di`',
    '`gudid_brand_name` AS `gudid_brand_name`',
    '`gudid_model_number` AS `gudid_model_number`',
    '`gudid_company_name` AS `gudid_company_name`',
    '`gudid_catalogue_number` AS `gudid_catalogue_number`',
    '`gudid_device_status` AS `gudid_device_status`',
    '`gudid_distribution_status` AS `gudid_distribution_status`',
    '`source_catalogue_number` AS `source_catalogue_number`',
    '`source_manufacturer` AS `source_manufacturer`',
    '`mapped_device_name` AS `mapped_device_name`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_DEVICE_MAPPING_ADVISORY_RULES = {
    # INSR_W4 W4.06 cross_field (estate 2026-10-29): A device mapping with confidence 0 should carry no concept (L3836).
    'gold.reference.device_mapping.table.insr_f19_zero_confidence_mapped':
        'NOT COALESCE((`mapping_confidence` = 0 AND (`snomed_concept_id` IS NOT NULL OR `standard_concept_id` IS NOT NULL)), FALSE)',
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 106,883 of 248,449
    # rows (43%) when profiled on 2026-08-24.
    # INSR_W3 W3.08 (F16): an expiry date is a planned future date; warn only outside 1990..loaded_at+15y.
    "gold.reference.device_mapping.expiry_date.expiry_window":
        "NOT COALESCE(CAST(`expiry_date` AS DATE) < DATE'1990-01-01' OR CAST(`expiry_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 15 YEARS, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
REFERENCE_DEVICE_MAPPING_PERSON_SCOPE_RULES = {
    "gold.reference.device_mapping.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_device_mapping"),
    refresh_policy="auto",
)
@dp.expect_all(REFERENCE_DEVICE_MAPPING_ADVISORY_RULES)
@dp.expect_all_or_drop(REFERENCE_DEVICE_MAPPING_PERSON_SCOPE_RULES)
def _gold_qc_reference_device_mapping():
    """Quality-controlled twin of journey_reference.device_mapping."""
    df = _qc("reference_device_mapping", REFERENCE_DEVICE_MAPPING_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.device_mapping"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_device_mapping():
    """Contract-v2 public twin of reference_device_mapping; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_device_mapping")).selectExpr(
        '`device_mapping_key` AS `device_mapping_key`',
        '`source_event_id` AS `source_event_id`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`implant_description` AS `implant_description`',
        '`normalized_description` AS `normalized_description`',
        '`cleaned_description` AS `cleaned_description`',
        '`udi_di` AS `udi_di`',
        '`udi_issuer` AS `udi_issuer`',
        '`gs1_identifier` AS `gs1_identifier`',
        '`hibcc_device_id` AS `hibcc_device_id`',
        '`serial_number` AS `serial_number`',
        '`expiry_date` AS `expiry_date`',
        '`gmdn_code` AS `gmdn_code`',
        '`gmdn_name` AS `gmdn_name`',
        '`snomed_concept_id` AS `snomed_concept_id`',
        '`snomed_name` AS `snomed_name`',
        '`standard_concept_id` AS `standard_concept_id`',
        '`standard_concept_name` AS `standard_concept_name`',
        '`standard_vocabulary_id` AS `standard_vocabulary_id`',
        '`device_type` AS `device_type`',
        '`mapping_layer` AS `mapping_layer`',
        '`mapping_status` AS `mapping_status`',
        '`mapping_confidence` AS `mapping_confidence`',
        '`confidence_tier` AS `confidence_tier`',
        '`mapping_rule_id` AS `mapping_rule_id`',
        '`matched_field` AS `matched_field`',
        '`matched_value` AS `matched_value`',
        '`mapping_candidate_count` AS `mapping_candidate_count`',
        '`mapping_distinct_concept_count` AS `mapping_distinct_concept_count`',
        '`mapping_ambiguous_ind` AS `mapping_ambiguous_ind`',
        '`matched_opcs_code` AS `matched_opcs_code`',
        '`procedure_support_ind` AS `procedure_support_ind`',
        '`mapping_schema_version` AS `mapping_schema_version`',
        '`normalization_version` AS `normalization_version`',
        '`brand_rules_version` AS `brand_rules_version`',
        '`mapped_at` AS `mapped_at`',
        # PMS_DEVICE_IDENTITY_V1
        '`gmdn_implantable` AS `gmdn_implantable`',
        '`gmdn_definition` AS `gmdn_definition`',
        '`gmdn_code_status` AS `gmdn_code_status`',
        '`gmdn_mapping_provenance` AS `gmdn_mapping_provenance`',
        '`gmdn_reference_present_ind` AS `gmdn_reference_present_ind`',
        '`gudid_primary_di` AS `gudid_primary_di`',
        '`gudid_brand_name` AS `gudid_brand_name`',
        '`gudid_model_number` AS `gudid_model_number`',
        '`gudid_company_name` AS `gudid_company_name`',
        '`gudid_catalogue_number` AS `gudid_catalogue_number`',
        '`gudid_device_status` AS `gudid_device_status`',
        '`gudid_distribution_status` AS `gudid_distribution_status`',
        '`source_catalogue_number` AS `source_catalogue_number`',
        '`source_manufacturer` AS `source_manufacturer`',
        '`mapped_device_name` AS `mapped_device_name`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_device_mapping': GoldRules(
        table='reference_device_mapping',
        select=REFERENCE_DEVICE_MAPPING_SELECT,
        advisory=REFERENCE_DEVICE_MAPPING_ADVISORY_RULES,
        person_scope=REFERENCE_DEVICE_MAPPING_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['device_mapping_key'], 'support_columns': ['loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_device_mapping': {'columns': {'_source_row_id': 'Source implant description event '
                                                                'identifier.',
                                              '_source_system': 'Value describing source system for the '
                                                                'device mapping record. It is produced by '
                                                                'the silver transformation and has no direct '
                                                                'bronze-column lineage entry. Whitespace and '
                                                                'source sentinel text are retained unless '
                                                                'the pipeline explicitly normalizes them; '
                                                                'null means no value was supplied.',
                                              '_source_table': 'Value describing source table for the device '
                                                               'mapping record. It is produced by the silver '
                                                               'transformation and has no direct '
                                                               'bronze-column lineage entry. Whitespace and '
                                                               'source sentinel text are retained unless the '
                                                               'pipeline explicitly normalizes them; null '
                                                               'means no value was supplied.',
                                              'brand_rules_version': 'Boundary-aware phrase-rule version.',
                                              'cleaned_description': 'Versioned fallback-matching '
                                                                     'description.',
                                              'confidence_tier': 'HIGH, MEDIUM, LOW or UNMAPPED.',
                                              'device_mapping_key': 'Deterministic SHA-256 key for the '
                                                                    'Millennium device-mapping row.',
                                              'device_type': 'Selected device category/type.',
                                              'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT.',
                                              'expiry_date': 'GS1 expiry date coalesced with raw expiry '
                                                             'date.',
                                              'gmdn_code': 'Selected GMDN code.',
                                              'gmdn_code_status': 'GMDN reference status (active/obsolete) '
                                                                  'of the mapped term.',
                                              'gmdn_definition': 'GMDN reference definition text for the '
                                                                 'mapped GMDN term.',
                                              'gmdn_implantable': 'GMDN reference implantable flag for the '
                                                                  'mapped GMDN term, carried from bronze; '
                                                                  'null when the term has no reference row.',
                                              'gmdn_mapping_provenance': 'How the GMDN term was reached '
                                                                         '(method and source), carried from '
                                                                         'bronze.',
                                              'gmdn_name': 'Selected GMDN preferred term name.',
                                              'gmdn_reference_present_ind': 'True when the mapped GMDN code '
                                                                            'exists in the loaded GMDN '
                                                                            'reference.',
                                              'gs1_identifier': 'Source GS1 UDI device identifier.',
                                              'gudid_brand_name': 'GUDID brand name for the matched device '
                                                                  'identifier.',
                                              'gudid_catalogue_number': 'GUDID catalogue number for the '
                                                                        'matched device identifier.',
                                              'gudid_company_name': 'GUDID labeler (company) name for the '
                                                                    'matched device identifier.',
                                              'gudid_device_status': 'GUDID device record status for the '
                                                                     'matched identifier.',
                                              'gudid_distribution_status': 'GUDID commercial distribution '
                                                                           'status for the matched '
                                                                           'identifier.',
                                              'gudid_model_number': 'GUDID version or model number for the '
                                                                    'matched device identifier.',
                                              'gudid_primary_di': 'FDA GUDID primary device identifier '
                                                                  'matched for this implant; null when no '
                                                                  'GUDID match.',
                                              'hibcc_device_id': 'Source HIBCC UDI device identifier.',
                                              'implant_description': 'Raw source implant description.',
                                              'loaded_at': 'map_device_mapping.PIPELINE_LOADED_AT carried '
                                                           'unchanged. This is the contributing bronze '
                                                           'row-write clock, distinct from MAPPED_AT '
                                                           'evaluation time and from Silver refresh time; a '
                                                           'missing value remains null.',
                                              'mapped_at': 'Timestamp when this event was last evaluated.',
                                              'mapped_device_name': 'Device name chosen by the mapping '
                                                                    '(GUDID or GMDN side) for display.',
                                              'mapping_ambiguous_ind': 'Whether multiple credible candidates '
                                                                       'existed.',
                                              'mapping_candidate_count': 'Number of distinct mapping '
                                                                         'candidates retained.',
                                              'mapping_confidence': 'Deterministic evidence score between '
                                                                    'zero and one.',
                                              'mapping_distinct_concept_count': 'Number of distinct selected '
                                                                                'standard/GMDN tuples.',
                                              'mapping_layer': 'Selected mapping evidence layer.',
                                              'mapping_rule_id': 'Rule or algorithm identifier for the '
                                                                 'selected candidate.',
                                              'mapping_schema_version': 'Output schema and mapping '
                                                                        'implementation version.',
                                              'mapping_status': 'MAPPED, AMBIGUOUS_SELECTED or UNMAPPED.',
                                              'matched_field': 'Source field that produced the selected '
                                                               'candidate.',
                                              'matched_opcs_code': 'OPCS code supporting the selected '
                                                                   'candidate.',
                                              'matched_value': 'Normalized source value used for the '
                                                               'selected candidate.',
                                              'normalization_version': 'Description normalization version.',
                                              'normalized_description': 'Versioned punctuation/whitespace '
                                                                        'normalized description.',
                                              'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                              'procedure_support_ind': 'Whether procedure context supports '
                                                                       'the selected candidate.',
                                              'serial_number': 'GS1 serial number coalesced with raw serial '
                                                               'number.',
                                              'snomed_concept_id': 'Compatibility field populated only for '
                                                                   'SNOMED concepts.',
                                              'snomed_name': 'Compatibility field populated only for SNOMED '
                                                             'concepts.',
                                              'source_catalogue_number': 'Catalogue number recorded in '
                                                                         'SurgiNet for this implant, as '
                                                                         'entered.',
                                              'source_event_id': 'Source implant description event '
                                                                 'identifier.',
                                              'source_manufacturer': 'Manufacturer recorded in SurgiNet for '
                                                                     'this implant, as entered.',
                                              'standard_concept_id': 'Selected standard OMOP device concept '
                                                                     'identifier.',
                                              'standard_concept_name': 'Selected standard OMOP device '
                                                                       'concept name.',
                                              'standard_vocabulary_id': 'Vocabulary of STANDARD_CONCEPT_ID.',
                                              'udi_di': 'Coalesced source UDI-DI; issuer is retained '
                                                        'separately.',
                                              'udi_issuer': 'GS1, HIBCC or null.'},
                                  'quantity_override': True,
                                  'table': 'Internal quality-controlled twin of reference_device_mapping: '
                                           'the Gold repairs, nulls and expectations are applied here and '
                                           'the published Gold MV reads this object. Materialized rather '
                                           'than temporary so the published MV can refresh incrementally.'},
 'reference_device_mapping': {'columns': {'_source_row_id': 'Source implant description event identifier.',
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
                              'quantity_override': False,
                              'table': 'Device-to-GMDN/GUDID mapping for implant catalogue items, best '
                                       'effort with provenance (method, score, rule). A UDI is recorded on a '
                                       'minority of implants, so most mappings come from catalogue and '
                                       'manufacturer text; unmapped items stay unmapped rather than '
                                       'guessed.'}}
