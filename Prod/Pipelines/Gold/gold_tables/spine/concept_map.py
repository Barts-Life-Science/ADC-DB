"""Gold spine.concept_map: 2 dataset(s).

- datasets: _qc_reference_concept_map, reference_concept_map
- moved verbatim from: 1_gold_spine_and_reference:2131, 1_gold_spine_and_reference:2158, 1_gold_spine_and_reference:2193, 1_gold_spine_and_reference:2204

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.concept_map ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_CONCEPT_MAP_SELECT = [
    '`concept_map_key` AS `concept_map_key`',
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = '0' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`target_coding_system` AS `target_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`target_code` AS STRING))) = '0' THEN NULL ELSE `target_code` END AS `target_code`",
    '`target_display` AS `target_display`',
    '`target_domain` AS `target_domain`',
    '`equivalence` AS `equivalence`',
    '`map_source` AS `map_source`',
    '`map_version` AS `map_version`',
    '`mapping_rule_id` AS `mapping_rule_id`',
    '`mapping_rank` AS `mapping_rank`',
    '`map_method` AS `map_method`',
    '`map_confidence` AS `map_confidence`',
    '`map_rule_id` AS `map_rule_id`',
    '`map_candidate_count` AS `map_candidate_count`',
    '`review_status` AS `review_status`',
    '`valid_from` AS `valid_from`',
    '`valid_to` AS `valid_to`',
    '`source_table` AS `source_table`',
    '`source_row_count` AS `source_row_count`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_CONCEPT_MAP_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 33 of 1,080,442
    # rows (0.00305%) when profiled on 2026-08-24.
    "gold.reference.concept_map.valid_to.future_owner":
        "NOT COALESCE((CAST(`valid_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_concept_map"),
    refresh_policy="auto",
)
@dp.expect_all(REFERENCE_CONCEPT_MAP_ADVISORY_RULES)
def _gold_qc_reference_concept_map():
    """Quality-controlled twin of journey_reference.concept_map."""
    df = _qc("reference_concept_map", REFERENCE_CONCEPT_MAP_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.concept_map"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_concept_map():
    """Contract-v2 public twin of reference_concept_map; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_concept_map")).selectExpr(
        '`concept_map_key` AS `concept_map_key`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`target_coding_system` AS `target_coding_system`',
        '`target_code` AS `target_code`',
        '`target_display` AS `target_display`',
        '`target_domain` AS `target_domain`',
        '`equivalence` AS `equivalence`',
        '`map_source` AS `map_source`',
        '`map_version` AS `map_version`',
        '`mapping_rule_id` AS `mapping_rule_id`',
        '`mapping_rank` AS `mapping_rank`',
        '`map_method` AS `map_method`',
        '`map_confidence` AS `map_confidence`',
        '`map_rule_id` AS `map_rule_id`',
        '`map_candidate_count` AS `map_candidate_count`',
        '`review_status` AS `review_status`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`source_table` AS `source_table`',
        '`source_row_count` AS `source_row_count`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_concept_map': GoldRules(
        table='reference_concept_map',
        select=REFERENCE_CONCEPT_MAP_SELECT,
        advisory=REFERENCE_CONCEPT_MAP_ADVISORY_RULES,
        lifecycle={'join_keys': ['concept_map_key'], 'support_columns': ['loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_concept_map': {'columns': {'concept_map_key': 'Deterministic SHA-256 terminology-map row '
                                                              'key.',
                                           'equivalence': 'FHIR equivalence when explicitly supplied; null '
                                                          'rather than inferred.',
                                           'loaded_at': 'Maximum contributing clock within each '
                                                        'source/target mapping group, with no further clock '
                                                        'aggregation across the unioned mapping arms. '
                                                        'Diagnosis and the ADC-backed lookup arms use '
                                                        'ADC_UPDT; coded-event mappings first coalesce '
                                                        'MAPPING_ADC_UPDT with PIPELINE_UPDT_DT_TM; '
                                                        'pathology first coalesces mapping_updated_at, '
                                                        'ADC_UPDT and source_adc_updt. LUNA and seed maps '
                                                        'use updated_at; iWeb, BloodTrack and Mediconnect '
                                                        'maps use CURATED_AT. Each chosen clock is cast to '
                                                        'TIMESTAMP before the group maximum. These are mixed '
                                                        'ingestion/mapping-update clocks, not a uniform '
                                                        'native modification time or Silver refresh time.',
                                           'map_candidate_count': 'Number of target candidates for the '
                                                                  'source code and mapping source.',
                                           'map_confidence': 'Mapping confidence in [0,1] where the source '
                                                             'supplied one.',
                                           'map_method': 'How the mapping was reached: source_native or rule '
                                                         'for legacy reference mappings.',
                                           'map_rule_id': 'Stable rule identifier copied from '
                                                          'mapping_rule_id for the S3 flat contract.',
                                           'map_source': 'Governed mapping product and route.',
                                           'map_version': 'Pinned mapping release.',
                                           'mapping_rank': 'Source-supplied candidate rank where available.',
                                           'mapping_rule_id': 'Source mapping rule or confidence '
                                                              'discriminator.',
                                           'review_status': 'Review state carried verbatim from the mapping '
                                                            'source; source_carried rows are bronze-carried '
                                                            'without review',
                                           'source_code': 'Source code or mapped source value.',
                                           'source_coding_system': 'Source code-system namespace.',
                                           'source_display': 'Source display carried from bronze.',
                                           'source_row_count': 'Number of bronze mapping observations '
                                                               'represented by the row.',
                                           'source_table': 'Configured bronze source or explicitly '
                                                           'authorized development fixture.',
                                           'target_code': 'Target code or concept identifier.',
                                           'target_coding_system': 'Target code-system namespace.',
                                           'target_display': 'Target display carried from bronze.',
                                           'target_domain': 'Intended downstream semantic domain.',
                                           'valid_from': 'Target mapping validity start where supplied.',
                                           'valid_to': 'Target mapping validity end where supplied.'},
                               'quantity_override': True,
                               'table': 'Internal quality-controlled twin of reference_concept_map: the Gold '
                                        'repairs, nulls and expectations are applied here and the published '
                                        'Gold MV reads this object. Materialized rather than temporary so '
                                        'the published MV can refresh incrementally.'},
 'reference_concept_map': {'columns': {'concept_map_key': 'Deterministic SHA-256 terminology-map row key.',
                                       'equivalence': 'FHIR equivalence when explicitly supplied; null '
                                                      'rather than inferred.',
                                       'loaded_at': 'Maximum contributing clock within each source/target '
                                                    'mapping group, with no further clock aggregation across '
                                                    'the unioned mapping arms. Diagnosis and the ADC-backed '
                                                    'lookup arms use ADC_UPDT; coded-event mappings first '
                                                    'coalesce MAPPING_ADC_UPDT with PIPELINE_UPDT_DT_TM; '
                                                    'pathology first coalesces mapping_updated_at, ADC_UPDT '
                                                    'and source_adc_updt. LUNA and seed maps use updated_at; '
                                                    'iWeb, BloodTrack and Mediconnect maps use CURATED_AT. '
                                                    'Each chosen clock is cast to TIMESTAMP before the group '
                                                    'maximum. These are mixed ingestion/mapping-update '
                                                    'clocks, not a uniform native modification time or '
                                                    'Silver refresh time.',
                                       'map_candidate_count': 'Number of candidate concepts the mapper '
                                                              'scored for this source code before choosing; '
                                                              'not the number of rows this code maps to.',
                                       'map_confidence': 'Mapping confidence in [0,1] where the source '
                                                         'supplied one.',
                                       'map_method': 'How the mapping was reached: source_native or rule for '
                                                     'legacy reference mappings.',
                                       'map_rule_id': 'Stable rule identifier copied from mapping_rule_id '
                                                      'for the S3 flat contract.',
                                       'map_source': 'Governed mapping product and route.',
                                       'map_version': 'Pinned mapping release.',
                                       'mapping_rank': 'Source-supplied candidate rank where available.',
                                       'mapping_rule_id': 'Source mapping rule or confidence discriminator.',
                                       'review_status': 'Review state carried verbatim from the mapping '
                                                        'source; source_carried rows are bronze-carried '
                                                        'without review',
                                       'source_code': 'Source code or mapped source value.',
                                       'source_coding_system': 'Source code-system namespace.',
                                       'source_display': 'Source display carried from bronze.',
                                       'source_row_count': 'Number of bronze mapping observations '
                                                           'represented by the row.',
                                       'source_table': 'Configured bronze source or explicitly authorized '
                                                       'development fixture.',
                                       'target_code': 'Target code or concept identifier.',
                                       'target_coding_system': 'Target code-system namespace.',
                                       'target_display': 'Target display carried from bronze.',
                                       'target_domain': 'Intended downstream semantic domain.',
                                       'valid_from': 'Target mapping validity start where supplied.',
                                       'valid_to': 'Target mapping validity end where supplied.'},
                           'quantity_override': False,
                           'table': 'One versioned source-to-target concept mapping carried by a governed '
                                    'bronze product; one-to-many mappings remain separate rows. Gold QC twin '
                                    'of the silver product: 2 columns are repaired or nulled, 1 check(s) are '
                                    'advisory. Each rule states its reason in the pipeline notebook, and '
                                    'Lakeflow expectation metrics report what every rule matched on each '
                                    'update.'}}
