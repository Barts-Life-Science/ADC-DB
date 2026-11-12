"""Silver pathology.gene_tested: 1 dataset(s).

- datasets: reference_gene_tested
- moved verbatim from: 2_silver_clinical_events:2974, 2_silver_clinical_events:2994, 2_silver_clinical_events:2997, 2_silver_clinical_events:3002, 2_silver_clinical_events:3019

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n, read_source, stable_id


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / gene tested
SRC_PATHOLOGY_GENE_TESTED = "4_prod.bronze.map_pathology_gene_tested"


GENE_TESTED_LIFECYCLE_FIELDS = [
]


GENE_TESTED_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_gene_tested():
    # Assemble gene tested rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    g = read_source(SRC_PATHOLOGY_GENE_TESTED)
    # contract v2: name assay-gene and parent-assay deterministic identities as keys
    return g.select(
        stable_id("gene_tested:assay-gene", g.gene_tested_id).alias("gene_tested_row_key"),
        g.gene_tested_id.alias("source_gene_tested_id"),
        stable_id("genomic_test:assay", g.genetic_test_id).alias("genomic_test_key"),
        g.genetic_test_id.alias("source_genetic_test_id"),
        g.hgnc_id, g.reported_gene_symbol, g.normalized_gene_symbol,
        g.alias_match_type, g.evidence_type, g.test_scope,
        g.panel_version_inferred, g.confidence,
        g.assessment_status, g.coverage_gap_regions_raw,  # GEN4
        g.ADC_UPDT.alias("loaded_at"),
    )


@materialized_view(
    name=_n("journey_reference.gene_tested"),
    refresh_policy="auto",
)
def gene_tested():
    # Build the declared dataset: Assay-gene denominator, HGNC-normalized; reference surface, no
    # event index. Covers 42.6% of assays in the current build (BCRL/FLT3 complete; MNGS 10.5%,
    # TNGS 16.6%) — recorded, not gated.
    return _lifecycle_source_gene_tested().drop(*GENE_TESTED_LIFECYCLE_FIELDS, *GENE_TESTED_RETIRED_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_gene_tested': {'columns': {'alias_match_type': 'HGNC symbol-resolution route.',
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
                           'table': 'Assay-gene denominator, HGNC-normalized; reference surface, no event '
                                    'index. Covers 42.6% of assays in the current build (BCRL/FLT3 complete; '
                                    'MNGS 10.5%, TNGS 16.6%) — recorded, not gated.'}}
