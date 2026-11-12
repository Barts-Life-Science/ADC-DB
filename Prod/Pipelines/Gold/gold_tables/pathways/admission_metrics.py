"""Gold pathways.admission_metrics: 2 dataset(s).

- datasets: _qc_reference_admission_metrics, reference_admission_metrics
- moved verbatim from: 4_gold_care_pathways:3945, 4_gold_care_pathways:3963, 4_gold_care_pathways:3973

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.admission_metrics ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_ADMISSION_METRICS_SELECT = [
    '`feed_id` AS `feed_id`',
    '`route_id` AS `route_id`',
    '`admitted` AS `admitted`',
    '`load_date` AS `load_date`',
    '`row_count` AS `row_count`',
    '`latest_loaded_at` AS `latest_loaded_at`',
]


@dp.materialized_view(
    name=_n("gold_qc._reference_admission_metrics"),
    refresh_policy="auto",
)
def _gold_qc_reference_admission_metrics():
    """Quality-controlled twin of journey_reference.admission_metrics."""
    df = _qc("reference_admission_metrics", REFERENCE_ADMISSION_METRICS_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.admission_metrics"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_admission_metrics():
    """Contract-v2 public twin of reference_admission_metrics; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_admission_metrics")).selectExpr(
        '`feed_id` AS `feed_id`',
        '`route_id` AS `route_id`',
        '`admitted` AS `admitted`',
        '`load_date` AS `load_date`',
        '`row_count` AS `row_count`',
        '`latest_loaded_at` AS `latest_loaded_at`',
    )
    return df


RULES = {
    'reference_admission_metrics': GoldRules(
        table='reference_admission_metrics',
        select=REFERENCE_ADMISSION_METRICS_SELECT),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_admission_metrics': {'columns': {'admitted': 'Whether these rows entered silver.',
                                                 'feed_id': 'Source-system identifier for the feed '
                                                            'associated with each admission metrics record. '
                                                            'It is produced by the silver transformation and '
                                                            'has no direct bronze-column lineage entry. Its '
                                                            'uniqueness is limited to the originating source '
                                                            'namespace; null means no identifier was '
                                                            'supplied.',
                                                 'latest_loaded_at': 'Latest bronze load time observed in '
                                                                     'the bucket.',
                                                 'load_date': 'Bronze load date bucket for per-load '
                                                              'accounting.',
                                                 'route_id': 'Route or exclusion reason.',
                                                 'row_count': 'Rows in this bucket.'},
                                     'quantity_override': True,
                                     'table': 'Internal quality-controlled twin of '
                                              'reference_admission_metrics: the Gold repairs, nulls and '
                                              'expectations are applied here and the published Gold MV reads '
                                              'this object. Materialized rather than temporary so the '
                                              'published MV can refresh incrementally.'},
 'reference_admission_metrics': {'columns': {'admitted': 'Whether these rows entered silver.',
                                             'feed_id': 'Source-system identifier for the feed associated '
                                                        'with each admission metrics record. It is produced '
                                                        'by the silver transformation and has no direct '
                                                        'bronze-column lineage entry. Its uniqueness is '
                                                        'limited to the originating source namespace; null '
                                                        'means no identifier was supplied.',
                                             'latest_loaded_at': 'Latest bronze load time observed in the '
                                                                 'bucket.',
                                             'load_date': 'Bronze load date bucket for per-load accounting.',
                                             'route_id': 'Route or exclusion reason.',
                                             'row_count': 'Rows in this bucket.'},
                                 'quantity_override': False,
                                 'table': 'Admission-gate accounting per registered route; route_id doubles '
                                          'as the exclusion reason for admitted=false rows. The product-wide '
                                          'usable-code gate covers every source_code-bearing fact lane; '
                                          'document is exempt as a narrative route, and structured admin '
                                          'facts without source_code are exempt; registry_entry rows carry '
                                          'no source codes by construction and are exempt as a class. Gold '
                                          'QC twin of the silver product: 0 columns are repaired or nulled, '
                                          '0 check(s) are advisory. Each rule states its reason in the '
                                          'pipeline notebook, and Lakeflow expectation metrics report what '
                                          'every rule matched on each update.'}}
