"""Gold spine.practitioner_location_evidence: 2 dataset(s).

- datasets: _qc_reference_practitioner_location_evidence, reference_practitioner_location_evidence
- moved verbatim from: 1_gold_spine_and_reference:4191, 1_gold_spine_and_reference:4227, 1_gold_spine_and_reference:4237

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.practitioner_location_evidence ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PRACTITIONER_LOCATION_EVIDENCE_SELECT = [
    '`practitioner_location_evidence_key` AS `practitioner_location_evidence_key`',
    '`practitioner_id` AS `practitioner_id`',
    '`practitioner_person_id` AS `practitioner_person_id`',
    '`location_code` AS `location_code`',
    '`event_count` AS `event_count`',
    "CASE WHEN CAST(`first_event_datetime_raw` AS DATE) = DATE'1899-12-30' THEN NULL ELSE `first_event_datetime_raw` END AS `first_event_datetime_raw`",
    'CASE WHEN YEAR(CAST(`first_event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `first_event_datetime` END AS `first_event_datetime`',
    "CASE WHEN CAST(`last_event_datetime_raw` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`last_event_datetime_raw` AS TIMESTAMP)) = 1970 THEN NULL ELSE CASE WHEN CAST(`last_event_datetime_raw` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`last_event_datetime_raw` AS DATE)) < 9999 THEN NULL ELSE `last_event_datetime_raw` END END AS `last_event_datetime_raw`",
    'CASE WHEN CAST(`last_event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`last_event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `last_event_datetime` END AS `last_event_datetime`',
    '`location_rank` AS `location_rank`',
    '`top_count_tie_count` AS `top_count_tie_count`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


@dp.materialized_view(
    name=_n("gold_qc._reference_practitioner_location_evidence"),
    refresh_policy="auto",
)
def _gold_qc_reference_practitioner_location_evidence():
    """Quality-controlled twin of journey_reference.practitioner_location_evidence."""
    df = _qc("reference_practitioner_location_evidence", REFERENCE_PRACTITIONER_LOCATION_EVIDENCE_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.practitioner_location_evidence"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_practitioner_location_evidence():
    """Contract-v2 public twin of reference_practitioner_location_evidence; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_practitioner_location_evidence")).selectExpr(
        '`practitioner_location_evidence_key` AS `practitioner_location_evidence_key`',
        '`practitioner_id` AS `practitioner_id`',
        '`practitioner_person_id` AS `practitioner_person_id`',
        '`location_code` AS `location_code`',
        '`event_count` AS `event_count`',
        '`first_event_datetime_raw` AS `first_event_datetime_raw`',
        '`first_event_datetime` AS `first_event_datetime`',
        '`last_event_datetime_raw` AS `last_event_datetime_raw`',
        '`last_event_datetime` AS `last_event_datetime`',
        '`location_rank` AS `location_rank`',
        '`top_count_tie_count` AS `top_count_tie_count`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_practitioner_location_evidence': GoldRules(
        table='reference_practitioner_location_evidence',
        select=REFERENCE_PRACTITIONER_LOCATION_EVIDENCE_SELECT,
        lifecycle={'join_keys': ['practitioner_location_evidence_key'], 'support_columns': ['loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_practitioner_location_evidence': {'columns': {'_source_row_id': 'Observed nurse-unit code.',
                                                              '_source_system': 'Value describing source '
                                                                                'system for the practitioner '
                                                                                'location evidence record. '
                                                                                'It is produced by the '
                                                                                'silver transformation and '
                                                                                'has no direct bronze-column '
                                                                                'lineage entry. Whitespace '
                                                                                'and source sentinel text '
                                                                                'are retained unless the '
                                                                                'pipeline explicitly '
                                                                                'normalizes them; null means '
                                                                                'no value was supplied.',
                                                              '_source_table': 'Value describing source '
                                                                               'table for the practitioner '
                                                                               'location evidence record. It '
                                                                               'is produced by the silver '
                                                                               'transformation and has no '
                                                                               'direct bronze-column lineage '
                                                                               'entry. Whitespace and source '
                                                                               'sentinel text are retained '
                                                                               'unless the pipeline '
                                                                               'explicitly normalizes them; '
                                                                               'null means no value was '
                                                                               'supplied.',
                                                              'event_count': 'Current canonical '
                                                                             'logical-event count at this '
                                                                             'location.',
                                                              'first_event_datetime': 'Earliest supporting '
                                                                                      'event timestamp.',
                                                              'first_event_datetime_raw': 'Earliest '
                                                                                          'supporting event '
                                                                                          'timestamp.',
                                                              'last_event_datetime': 'Latest supporting '
                                                                                     'event timestamp.',
                                                              'last_event_datetime_raw': 'Latest supporting '
                                                                                         'event timestamp.',
                                                              'loaded_at': 'map_medical_personnel_location_evidence.EVIDENCE_ADC_UPDT '
                                                                           'carried unchanged. It is the '
                                                                           'supplied supporting-evidence '
                                                                           'clock, not FIRST_EVENT_DT_TM, '
                                                                           'LAST_EVENT_DT_TM or Silver '
                                                                           'refresh time; Silver performs no '
                                                                           'further timestamp aggregation or '
                                                                           'clamping on this field.',
                                                              'location_code': 'Observed nurse-unit code.',
                                                              'location_rank': 'Deterministic personnel '
                                                                               'location rank.',
                                                              'practitioner_id': 'Native Millennium '
                                                                                 'personnel PERSON_ID as '
                                                                                 'BIGINT; joins to '
                                                                                 'reference_practitioner.practitioner_id.',
                                                              'practitioner_location_evidence_key': 'Deterministic '
                                                                                                    'SHA-256 '
                                                                                                    'key for '
                                                                                                    'practitioner-location '
                                                                                                    'evidence.',
                                                              'practitioner_person_id': 'Source-system '
                                                                                        'identifier for the '
                                                                                        'practitioner person '
                                                                                        'associated with '
                                                                                        'each practitioner '
                                                                                        'location evidence '
                                                                                        'record. It is '
                                                                                        'derived from bronze '
                                                                                        'field `PERSON_ID` '
                                                                                        'in '
                                                                                        '`4_prod.bronze.map_medical_personnel_location_evidence`. '
                                                                                        'Its uniqueness is '
                                                                                        'limited to the '
                                                                                        'originating source '
                                                                                        'namespace; null '
                                                                                        'means no identifier '
                                                                                        'was supplied.',
                                                              'top_count_tie_count': 'Number of locations '
                                                                                     'tied on the maximum '
                                                                                     'event count.'},
                                                  'quantity_override': True,
                                                  'table': 'Internal quality-controlled twin of '
                                                           'reference_practitioner_location_evidence: the '
                                                           'Gold repairs, nulls and expectations are applied '
                                                           'here and the published Gold MV reads this '
                                                           'object. Materialized rather than temporary so '
                                                           'the published MV can refresh incrementally.'},
 'reference_practitioner_location_evidence': {'columns': {'_source_row_id': 'Observed nurse-unit code.',
                                                          '_source_system': 'Value describing source system '
                                                                            'for the practitioner location '
                                                                            'evidence record. It is produced '
                                                                            'by the silver transformation '
                                                                            'and has no direct bronze-column '
                                                                            'lineage entry. Whitespace and '
                                                                            'source sentinel text are '
                                                                            'retained unless the pipeline '
                                                                            'explicitly normalizes them; '
                                                                            'null means no value was '
                                                                            'supplied.',
                                                          '_source_table': 'Value describing source table '
                                                                           'for the practitioner location '
                                                                           'evidence record. It is produced '
                                                                           'by the silver transformation and '
                                                                           'has no direct bronze-column '
                                                                           'lineage entry. Whitespace and '
                                                                           'source sentinel text are '
                                                                           'retained unless the pipeline '
                                                                           'explicitly normalizes them; null '
                                                                           'means no value was supplied.',
                                                          'event_count': 'Current canonical logical-event '
                                                                         'count at this location.',
                                                          'first_event_datetime': 'Earliest supporting event '
                                                                                  'timestamp.',
                                                          'first_event_datetime_raw': 'Earliest supporting '
                                                                                      'event timestamp.',
                                                          'last_event_datetime': 'Latest supporting event '
                                                                                 'timestamp.',
                                                          'last_event_datetime_raw': 'Latest supporting '
                                                                                     'event timestamp.',
                                                          'loaded_at': 'map_medical_personnel_location_evidence.EVIDENCE_ADC_UPDT '
                                                                       'carried unchanged. It is the '
                                                                       'supplied supporting-evidence clock, '
                                                                       'not FIRST_EVENT_DT_TM, '
                                                                       'LAST_EVENT_DT_TM or Silver refresh '
                                                                       'time; Silver performs no further '
                                                                       'timestamp aggregation or clamping on '
                                                                       'this field.',
                                                          'location_code': 'Observed nurse-unit code.',
                                                          'location_rank': 'Deterministic personnel location '
                                                                           'rank.',
                                                          'practitioner_id': 'Native Millennium personnel '
                                                                             'PERSON_ID as BIGINT; joins to '
                                                                             'reference_practitioner.practitioner_id.',
                                                          'practitioner_location_evidence_key': 'Deterministic '
                                                                                                'SHA-256 key '
                                                                                                'for '
                                                                                                'practitioner-location '
                                                                                                'evidence.',
                                                          'practitioner_person_id': 'Source-system '
                                                                                    'identifier for the '
                                                                                    'practitioner person '
                                                                                    'associated with each '
                                                                                    'practitioner location '
                                                                                    'evidence record. It is '
                                                                                    'derived from bronze '
                                                                                    'field `PERSON_ID` in '
                                                                                    '`4_prod.bronze.map_medical_personnel_location_evidence`. '
                                                                                    'Its uniqueness is '
                                                                                    'limited to the '
                                                                                    'originating source '
                                                                                    'namespace; null means '
                                                                                    'no identifier was '
                                                                                    'supplied.',
                                                          'top_count_tie_count': 'Number of locations tied '
                                                                                 'on the maximum event '
                                                                                 'count.'},
                                              'quantity_override': False,
                                              'table': 'One staff location-evidence summary with '
                                                       'sentinel-clamped bounds. Gold QC twin of the silver '
                                                       'product: 4 columns are repaired or nulled, 0 '
                                                       'check(s) are advisory. Each rule states its reason '
                                                       'in the pipeline notebook, and Lakeflow expectation '
                                                       'metrics report what every rule matched on each '
                                                       'update.'}}
