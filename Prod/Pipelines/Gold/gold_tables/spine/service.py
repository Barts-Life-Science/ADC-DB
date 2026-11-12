"""Gold spine.service: 2 dataset(s).

- datasets: _qc_reference_service, reference_service
- moved verbatim from: 1_gold_spine_and_reference:4403, 1_gold_spine_and_reference:4413, 1_gold_spine_and_reference:4432, 1_gold_spine_and_reference:4442

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.service ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_SERVICE_SELECT = [
    '`service_key` AS `service_key`',
    '`service_id` AS `service_id`',
    '`source_service_code` AS `source_service_code`',
    '`name` AS `name`',
    '`status` AS `status`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


REFERENCE_SERVICE_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`member_source` AS `member_source`',
]


@dp.materialized_view(
    name=_n("gold_qc._reference_service"),
    refresh_policy="auto",
)
def _gold_qc_reference_service():
    """Quality-controlled twin of journey_reference.service."""
    df = _qc("reference_service", REFERENCE_SERVICE_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.service"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_service():
    """Contract-v2 public twin of reference_service; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_service")).selectExpr(
        '`service_key` AS `service_key`',
        '`service_id` AS `service_id`',
        '`source_service_code` AS `source_service_code`',
        '`name` AS `name`',
        '`status` AS `status`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`loaded_at` AS `loaded_at`',
        '`member_source` AS `member_source`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'reference_service': GoldRules(
        table='reference_service',
        select=REFERENCE_SERVICE_SELECT,
        lifecycle={'join_keys': ['service_key'], 'support_columns': ['load_batch_id', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_service': {'columns': {'loaded_at': 'Maximum map_medical_personnel.ADC_UPDT over source rows '
                                                    'sharing the non-null MEDSERVICE_GROUP_ID. This includes '
                                                    'all contributing group assignments, not only active '
                                                    'assignments or the row supplying the selected service '
                                                    'name; it is bronze ingestion provenance, not Silver '
                                                    'refresh time.',
                                       'member_source': 'personnel_group (MEDSERVICE_GROUP_ID from personnel '
                                                        'assignments) or med_service_code (encounter '
                                                        'MED_SERVICE_CD, named by its bronze description). '
                                                        'INSR_W2 W2.11 (L4164).',
                                       'name': 'Source service name.',
                                       'service_id': 'Millennium MEDSERVICE_GROUP_ID; primary key of this '
                                                     'table.',
                                       'service_key': 'Deterministic SHA-256 service key.',
                                       'source_row_id': 'Stable source service identifier.',
                                       'source_service_code': 'Source personnel-group service code.',
                                       'source_table': 'Fully qualified bronze source table.',
                                       'status': 'Source-derived service status.'},
                           'quantity_override': True,
                           'table': 'Internal quality-controlled twin of reference_service: the Gold '
                                    'repairs, nulls and expectations are applied here and the published Gold '
                                    'MV reads this object. Materialized rather than temporary so the '
                                    'published MV can refresh incrementally.'},
 'reference_service': {'columns': {'loaded_at': 'Maximum map_medical_personnel.ADC_UPDT over source rows '
                                                'sharing the non-null MEDSERVICE_GROUP_ID. This includes all '
                                                'contributing group assignments, not only active assignments '
                                                'or the row supplying the selected service name; it is '
                                                'bronze ingestion provenance, not Silver refresh time.',
                                   'member_source': 'personnel_group (MEDSERVICE_GROUP_ID from personnel '
                                                    'assignments) or med_service_code (encounter '
                                                    'MED_SERVICE_CD, named by its bronze description). '
                                                    'INSR_W2 W2.11 (L4164).',
                                   'name': 'Source service name.',
                                   'service_id': 'Millennium MEDSERVICE_GROUP_ID; primary key of this table.',
                                   'service_key': 'Deterministic SHA-256 service key.',
                                   'source_row_id': 'Stable source service identifier.',
                                   'source_service_code': 'Source personnel-group service code.',
                                   'source_table': 'Fully qualified bronze source table.',
                                   'status': 'Source-derived service status.'},
                       'quantity_override': False,
                       'table': 'Thin v1 medical-service dimension from practitioner assignments. Gold QC '
                                'twin of the silver product: 0 columns are repaired or nulled, 0 check(s) '
                                'are advisory. Each rule states its reason in the pipeline notebook, and '
                                'Lakeflow expectation metrics report what every rule matched on each '
                                'update.'}}
