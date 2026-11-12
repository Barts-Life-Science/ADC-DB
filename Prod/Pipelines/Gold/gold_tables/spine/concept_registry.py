"""Gold spine.concept_registry: 2 dataset(s).

- datasets: _qc_reference_concept_registry, reference_concept_registry
- moved verbatim from: 1_gold_spine_and_reference:2250, 1_gold_spine_and_reference:2274, 1_gold_spine_and_reference:2284

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.concept_registry ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_CONCEPT_REGISTRY_SELECT = [
    '`concept_registry_key` AS `concept_registry_key`',
    '`coding_system` AS `coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`code` AS STRING))) = '0' OR UPPER(TRIM(CAST(`code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `code` END AS `code`",
    '`preferred_display` AS `preferred_display`',
    '`status` AS `status`',
    '`source_use_count` AS `source_use_count`',
    '`target_use_count` AS `target_use_count`',
    '`first_observed_at` AS `first_observed_at`',
    '`last_observed_at` AS `last_observed_at`',
]


@dp.materialized_view(
    name=_n("gold_qc._reference_concept_registry"),
    refresh_policy="auto",
)
def _gold_qc_reference_concept_registry():
    """Quality-controlled twin of journey_reference.concept_registry."""
    df = _qc("reference_concept_registry", REFERENCE_CONCEPT_REGISTRY_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.concept_registry"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_concept_registry():
    """Contract-v2 public twin of reference_concept_registry; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_concept_registry")).selectExpr(
        '`concept_registry_key` AS `concept_registry_key`',
        '`coding_system` AS `coding_system`',
        '`code` AS `code`',
        '`preferred_display` AS `preferred_display`',
        '`status` AS `status`',
        '`source_use_count` AS `source_use_count`',
        '`target_use_count` AS `target_use_count`',
        '`first_observed_at` AS `first_observed_at`',
        '`last_observed_at` AS `last_observed_at`',
    )
    return df


RULES = {
    'reference_concept_registry': GoldRules(
        table='reference_concept_registry',
        select=REFERENCE_CONCEPT_REGISTRY_SELECT),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_concept_registry': {'columns': {'code': 'OMOP standard vocabulary concept identifier '
                                                        'assigned to the mapped source value.',
                                                'coding_system': 'Identifier namespace for the observed '
                                                                 'source or target code, taken from event or '
                                                                 'mapping evidence; event codes with no '
                                                                 'source namespace use urn:unknown.',
                                                'concept_registry_key': 'Deterministic SHA-256 '
                                                                        'observed-concept key.',
                                                'first_observed_at': 'Earliest represented bronze '
                                                                     'observation timestamp.',
                                                'last_observed_at': 'Latest represented bronze observation '
                                                                    'timestamp.',
                                                'preferred_display': 'Deterministically selected observed '
                                                                     'display.',
                                                'source_use_count': 'Source-side event and mapping '
                                                                    'observation count.',
                                                'status': 'Registry observation status.',
                                                'target_use_count': 'Target-side mapping observation count.'},
                                    'quantity_override': True,
                                    'table': 'Internal quality-controlled twin of '
                                             'reference_concept_registry: the Gold repairs, nulls and '
                                             'expectations are applied here and the published Gold MV reads '
                                             'this object. Materialized rather than temporary so the '
                                             'published MV can refresh incrementally.'},
 'reference_concept_registry': {'columns': {'code': 'OMOP standard vocabulary concept identifier assigned to '
                                                    'the mapped source value.',
                                            'coding_system': 'Identifier namespace for the observed source '
                                                             'or target code, taken from event or mapping '
                                                             'evidence; event codes with no source namespace '
                                                             'use urn:unknown.',
                                            'concept_registry_key': 'Deterministic SHA-256 observed-concept '
                                                                    'key.',
                                            'first_observed_at': 'Earliest represented bronze observation '
                                                                 'timestamp.',
                                            'last_observed_at': 'Latest represented bronze observation '
                                                                'timestamp.',
                                            'preferred_display': 'Deterministically selected observed '
                                                                 'display.',
                                            'source_use_count': 'Source-side event and mapping observation '
                                                                'count.',
                                            'status': 'Registry observation status.',
                                            'target_use_count': 'Target-side mapping observation count.'},
                                'quantity_override': False,
                                'table': 'One observed coding-system and code pair across event sources and '
                                         'mapping targets. Gold QC twin of the silver product: 1 columns are '
                                         'repaired or nulled, 0 check(s) are advisory. Each rule states its '
                                         'reason in the pipeline notebook, and Lakeflow expectation metrics '
                                         'report what every rule matched on each update.'}}
