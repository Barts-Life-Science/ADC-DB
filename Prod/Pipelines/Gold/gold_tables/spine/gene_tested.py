"""Gold spine.gene_tested: 2 dataset(s).

- datasets: _qc_reference_gene_tested, reference_gene_tested
- moved verbatim from: 1_gold_spine_and_reference:2816, 1_gold_spine_and_reference:2852, 1_gold_spine_and_reference:2862

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.gene_tested ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_GENE_TESTED_SELECT = [
    '`gene_tested_row_key` AS `gene_tested_row_key`',
    '`source_gene_tested_id` AS `source_gene_tested_id`',
    '`genomic_test_key` AS `genomic_test_key`',
    '`source_genetic_test_id` AS `source_genetic_test_id`',
    '`hgnc_id` AS `hgnc_id`',
    '`reported_gene_symbol` AS `reported_gene_symbol`',
    '`normalized_gene_symbol` AS `normalized_gene_symbol`',
    '`alias_match_type` AS `alias_match_type`',
    '`evidence_type` AS `evidence_type`',
    '`test_scope` AS `test_scope`',
    '`panel_version_inferred` AS `panel_version_inferred`',
    '`confidence` AS `confidence`',
    '`assessment_status` AS `assessment_status`',  # GEN4_GOLD_V1
    '`coverage_gap_regions_raw` AS `coverage_gap_regions_raw`',  # GEN4_GOLD_V1
    '`loaded_at` AS `loaded_at`',
]


@dp.materialized_view(
    name=_n("gold_qc._reference_gene_tested"),
    refresh_policy="auto",
)
def _gold_qc_reference_gene_tested():
    """Quality-controlled twin of journey_reference.gene_tested."""
    df = _qc("reference_gene_tested", REFERENCE_GENE_TESTED_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.gene_tested"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_gene_tested():
    """Contract-v2 public twin of reference_gene_tested; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_gene_tested")).selectExpr(
        '`gene_tested_row_key` AS `gene_tested_row_key`',
        '`source_gene_tested_id` AS `source_gene_tested_id`',
        '`genomic_test_key` AS `genomic_test_key`',
        '`source_genetic_test_id` AS `source_genetic_test_id`',
        '`hgnc_id` AS `hgnc_id`',
        '`reported_gene_symbol` AS `reported_gene_symbol`',
        '`normalized_gene_symbol` AS `normalized_gene_symbol`',
        '`alias_match_type` AS `alias_match_type`',
        '`evidence_type` AS `evidence_type`',
        '`test_scope` AS `test_scope`',
        '`panel_version_inferred` AS `panel_version_inferred`',
        '`confidence` AS `confidence`',
        '`assessment_status` AS `assessment_status`',  # GEN4_GOLD_V1
        '`coverage_gap_regions_raw` AS `coverage_gap_regions_raw`',  # GEN4_GOLD_V1
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_gene_tested': GoldRules(
        table='reference_gene_tested',
        select=REFERENCE_GENE_TESTED_SELECT,
        lifecycle={'join_keys': ['gene_tested_row_key'], 'support_columns': ['loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_gene_tested': {'columns': {'alias_match_type': 'HGNC symbol-resolution route.',
                                           'assessment_status': 'What the lab says about this gene for this '
                                                                'assay: reported, dedicated_assay, '
                                                                'panel_assessed, panel_not_assessed, '
                                                                'insufficient_coverage, test_not_completed.',
                                           'confidence': 'Deterministic normalization confidence.',
                                           'coverage_gap_regions_raw': 'Regions of this gene the lab could '
                                                                       'not assess, as reported '
                                                                       '(chr:start-end;...; coordinates as '
                                                                       'printed by the lab).',
                                           'evidence_type': 'Source of assay-gene membership.',
                                           'gene_tested_row_key': 'Deterministic SHA-256 assay-gene row key.',
                                           'genomic_test_key': 'Deterministic SHA-256 parent genomic-test '
                                                               'key.',
                                           'hgnc_id': 'Governed HGNC identifier.',
                                           'loaded_at': 'map_pathology_gene_tested.ADC_UPDT carried '
                                                        'unchanged for the assay-gene membership row. The '
                                                        'parent assay key is derived without joining or '
                                                        'adding its timestamp; this is bronze load '
                                                        'provenance, not a genomic-test event time or Silver '
                                                        'refresh time.',
                                           'normalized_gene_symbol': 'Current approved HGNC symbol.',
                                           'panel_version_inferred': 'Whether membership used an inferred '
                                                                     'panel version.',
                                           'reported_gene_symbol': 'Gene symbol as reported or configured.',
                                           'source_gene_tested_id': 'Bronze gene-tested identifier.',
                                           'source_genetic_test_id': 'Bronze parent genetic-test identifier.',
                                           'test_scope': 'Reported exon/region/coverage scope when '
                                                         'available.'},
                               'quantity_override': True,
                               'table': 'Internal quality-controlled twin of reference_gene_tested: the Gold '
                                        'repairs, nulls and expectations are applied here and the published '
                                        'Gold MV reads this object. Materialized rather than temporary so '
                                        'the published MV can refresh incrementally.'},
 'reference_gene_tested': {'columns': {'alias_match_type': 'HGNC symbol-resolution route.',
                                       'assessment_status': 'What the lab says about this gene for this '
                                                            'assay: reported, dedicated_assay, '
                                                            'panel_assessed, panel_not_assessed, '
                                                            'insufficient_coverage, test_not_completed.',
                                       'confidence': 'Deterministic normalization confidence.',
                                       'coverage_gap_regions_raw': 'Regions of this gene the lab could not '
                                                                   'assess, as reported (chr:start-end;...; '
                                                                   'coordinates as printed by the lab).',
                                       'evidence_type': 'Source of assay-gene membership.',
                                       'gene_tested_row_key': 'Deterministic SHA-256 assay-gene row key.',
                                       'genomic_test_key': 'Deterministic SHA-256 parent genomic-test key.',
                                       'hgnc_id': 'Governed HGNC identifier.',
                                       'loaded_at': 'map_pathology_gene_tested.ADC_UPDT carried unchanged '
                                                    'for the assay-gene membership row. The parent assay key '
                                                    'is derived without joining or adding its timestamp; '
                                                    'this is bronze load provenance, not a genomic-test '
                                                    'event time or Silver refresh time.',
                                       'normalized_gene_symbol': 'Current approved HGNC symbol.',
                                       'panel_version_inferred': 'Whether membership used an inferred panel '
                                                                 'version.',
                                       'reported_gene_symbol': 'Gene symbol as reported or configured.',
                                       'source_gene_tested_id': 'Bronze gene-tested identifier.',
                                       'source_genetic_test_id': 'Bronze parent genetic-test identifier.',
                                       'test_scope': 'Reported exon/region/coverage scope when available.'},
                           'quantity_override': False,
                           'table': 'Assay-gene denominator at gene_tested_id grain. The current build '
                                    'covers 42.6% of assays; BCRL and FLT3 are complete while MNGS is 10.5% '
                                    'and TNGS is 16.6%. Gold QC twin of the silver product: 0 columns are '
                                    'repaired or nulled, 0 check(s) are advisory. Each rule states its '
                                    'reason in the pipeline notebook, and Lakeflow expectation metrics '
                                    'report what every rule matched on each update.'}}
