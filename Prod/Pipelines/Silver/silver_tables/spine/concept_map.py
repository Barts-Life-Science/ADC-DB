"""Silver spine.concept_map: 1 dataset(s).

- datasets: reference_concept_map
- moved verbatim from: 1_silver_spine_and_reference:5246, 1_silver_spine_and_reference:5249, 1_silver_spine_and_reference:5259

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables.spine._shared import _lifecycle_source_concept_map


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


CONCEPT_MAP_LIFECYCLE_FIELDS = [
]


CONCEPT_MAP_RETIRED_COLUMNS = [

]


@materialized_view(
    name=_n("journey_reference.concept_map"),
    refresh_policy="auto",
)
def concept_map():
    # Build the declared dataset: Versioned one-to-many source-to-target mappings observed in
    # governed bronze products; no winner is selected.
    return _lifecycle_source_concept_map().drop(*CONCEPT_MAP_LIFECYCLE_FIELDS, *CONCEPT_MAP_RETIRED_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_concept_map': {'columns': {'concept_map_key': 'Deterministic SHA-256 terminology-map row key.',
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
                                       'map_candidate_count': 'Number of target candidates for the source '
                                                              'code and mapping source.',
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
                           'table': 'Versioned one-to-many source-to-target mappings observed in governed '
                                    'bronze products; no winner is selected.'}}
