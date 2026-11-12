"""Gold spine.request_thread: 2 dataset(s).

- datasets: _qc_spine_request_thread, spine_request_thread
- moved verbatim from: 1_gold_spine_and_reference:2000, 1_gold_spine_and_reference:2050, 1_gold_spine_and_reference:2055, 1_gold_spine_and_reference:2060, 1_gold_spine_and_reference:2082

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_spine.request_thread ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_REQUEST_THREAD_SELECT = [
    '`request_thread_key` AS `request_thread_key`',
    '`source_object` AS `source_object`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`target_patient_event_key` AS `target_patient_event_key`',
    '`link_type_code` AS `link_type_code`',
    '`subject_key` AS `subject_key`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`request_key` AS `request_key`',
    '`response_key` AS `response_key`',
    "CASE WHEN CAST(`requested_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`requested_datetime` AS DATE)) < 9999 THEN NULL ELSE `requested_datetime` END AS `requested_datetime`",
    'CASE WHEN CAST(`responded_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `responded_datetime` END AS `responded_datetime`',
    '`source_history_row_count` AS `source_history_row_count`',
    '`source_system` AS `source_system`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
SPINE_REQUEST_THREAD_PERSON_SCOPE_RULES = {
    "gold.spine.request_thread.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
SPINE_REQUEST_THREAD_ORPHAN_RULES = {
    "gold.spine.request_thread.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
    "gold.spine.request_thread.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_request_thread"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(SPINE_REQUEST_THREAD_PERSON_SCOPE_RULES)
@dp.expect_all(SPINE_REQUEST_THREAD_ORPHAN_RULES)
def _gold_qc_spine_request_thread():
    """Quality-controlled twin of journey_spine.request_thread."""
    # 343,157 rows point at a encounter_id the spine does not have. The pointer is nulled so
    # it cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    # 479 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_request_thread",
        SPINE_REQUEST_THREAD_SELECT,
        fk_columns=["encounter_id", "person_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_spine.request_thread"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_request_thread():
    """Contract-v2 public twin of spine_request_thread; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_request_thread")).selectExpr(
        '`request_thread_key` AS `request_thread_key`',
        '`source_object` AS `source_object`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`target_patient_event_key` AS `target_patient_event_key`',
        '`link_type_code` AS `link_type_code`',
        '`subject_key` AS `subject_key`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`request_key` AS `request_key`',
        '`response_key` AS `response_key`',
        '`requested_datetime` AS `requested_datetime`',
        '`responded_datetime` AS `responded_datetime`',
        '`source_history_row_count` AS `source_history_row_count`',
        '`source_system` AS `source_system`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'spine_request_thread': GoldRules(
        table='spine_request_thread',
        select=SPINE_REQUEST_THREAD_SELECT,
        person_scope=SPINE_REQUEST_THREAD_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['request_thread_key'],
         'support_columns': ['record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_spine_request_thread': {'columns': {'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                          'link_type_code': 'Controlled directed relationship type.',
                                          'loaded_at': 'Arm-specific contributing load clock: pathology '
                                                       'takes the greatest requested-test ADC_UPDT and the '
                                                       'linked report-series maximum ADC_UPDT across all its '
                                                       'versions; medication takes the greatest '
                                                       'administration ADC_UPDT and order ADC_UPDT; '
                                                       'waiting-list takes ADC_UPDT of the selected whole '
                                                       'representative version. The appointment join adds no '
                                                       'clock, and the union does not aggregate across arms. '
                                                       'Null when that edge has no contributing load '
                                                       'timestamp; not Silver refresh time.',
                                          'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                       'available.',
                                          'record_status': 'Arm-specific edge status: pathology is literal '
                                                           'active. Medication is retracted when the order '
                                                           'SOURCE_PRESENT_IND is false (null defaults to '
                                                           'present), otherwise superseded when '
                                                           'administration CE_VALID_UNTIL_DT_TM is before '
                                                           '2100-01-01, otherwise active. Waiting-list edges '
                                                           "copy the selected representative version's "
                                                           'status: superseded for source absence or '
                                                           'non-current status (null presence defaults true; '
                                                           'null IS_CURRENT defaults false), otherwise '
                                                           'active. This is not a single source '
                                                           'workflow-status code.',
                                          'record_status_effective_from': 'Arm-specific history start: null '
                                                                          'for pathology edges; '
                                                                          'map_medication_order.ORIG_ORDER_DT_TM '
                                                                          'for medication edges; selected '
                                                                          'map_waiting_list representative '
                                                                          'BEG_EFFECTIVE_DT_TM for '
                                                                          'waiting-list edges. These are '
                                                                          'request/version-time proxies, not '
                                                                          'a uniformly observed status '
                                                                          'transition. Missing timestamps '
                                                                          'remain null.',
                                          'record_status_effective_to': 'Pathology edges always have a null '
                                                                        'end. Medication uses order '
                                                                        'SOURCE_ABSENT_DETECTED_TS when '
                                                                        'retracted, without fallback even if '
                                                                        'null; otherwise finite '
                                                                        'administration CE_VALID_UNTIL_DT_TM '
                                                                        'when superseded, else null. '
                                                                        'Waiting-list edges copy their '
                                                                        "representative's end: for "
                                                                        'superseded versions, '
                                                                        'coalesce(SOURCE_ABSENT_DETECTED_TS, '
                                                                        'END_EFFECTIVE_DT_TM, ADC_UPDT), '
                                                                        'with no far-future sentinel filter; '
                                                                        'active versions have a null end.',
                                          'request_key': 'Request-side source identifier retained under the '
                                                         'governed key name.',
                                          'request_thread_key': 'Deterministic request-edge primary key.',
                                          'requested_datetime': 'Source request timestamp.',
                                          'responded_datetime': 'Source report or measurement timestamp.',
                                          'response_key': 'Response-side source identifier retained under '
                                                          'the governed key name.',
                                          'source_history_row_count': 'Number of source rows supporting the '
                                                                      'edge.',
                                          'source_object': 'Request-thread arm: pathology, medication, or '
                                                           'waiting_list.',
                                          'source_patient_event_key': 'Request-side patient event '
                                                                      'identifier.',
                                          'source_row_id': 'Representative stable source row identifier.',
                                          'source_system': 'Source system identifier.',
                                          'source_table': 'Fully qualified configured source table.',
                                          'source_update_timestamp': 'Arm-specific provenance: null for '
                                                                     'pathology edges; greatest of '
                                                                     'map_med_admin.ADC_UPDT, '
                                                                     'map_medication_order.SOURCE_ADC_UPDT '
                                                                     'and map_medication_order.ADC_UPDT for '
                                                                     'medication edges; selected '
                                                                     'map_waiting_list representative '
                                                                     'SOURCE_ADC_UPDT for waiting-list '
                                                                     'edges. This is not uniformly a native '
                                                                     'source modification clock. The '
                                                                     'appointment existence/person join '
                                                                     'contributes no timestamp; no Silver '
                                                                     'refresh timestamp is substituted.',
                                          'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                         'identifier. Not salted, not secret; a stable join '
                                                         'key across feeds where person_id is unresolved.',
                                          'target_patient_event_key': 'Response-side patient event '
                                                                      'identifier.'},
                              'quantity_override': True,
                              'table': 'Internal quality-controlled twin of spine_request_thread: the Gold '
                                       'repairs, nulls and expectations are applied here and the published '
                                       'Gold MV reads this object. Materialized rather than temporary so the '
                                       'published MV can refresh incrementally.'},
 'spine_request_thread': {'columns': {'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                      'link_type_code': 'Controlled directed relationship type.',
                                      'loaded_at': 'Arm-specific contributing load clock: pathology takes '
                                                   'the greatest requested-test ADC_UPDT and the linked '
                                                   'report-series maximum ADC_UPDT across all its versions; '
                                                   'medication takes the greatest administration ADC_UPDT '
                                                   'and order ADC_UPDT; waiting-list takes ADC_UPDT of the '
                                                   'selected whole representative version. The appointment '
                                                   'join adds no clock, and the union does not aggregate '
                                                   'across arms. Null when that edge has no contributing '
                                                   'load timestamp; not Silver refresh time.',
                                      'person_id': 'Resolved Millennium PERSON_ID as BIGINT when available.',
                                      'record_status': 'Arm-specific edge status: pathology is literal '
                                                       'active. Medication is retracted when the order '
                                                       'SOURCE_PRESENT_IND is false (null defaults to '
                                                       'present), otherwise superseded when administration '
                                                       'CE_VALID_UNTIL_DT_TM is before 2100-01-01, otherwise '
                                                       'active. Waiting-list edges copy the selected '
                                                       "representative version's status: superseded for "
                                                       'source absence or non-current status (null presence '
                                                       'defaults true; null IS_CURRENT defaults false), '
                                                       'otherwise active. This is not a single source '
                                                       'workflow-status code.',
                                      'record_status_effective_from': 'Arm-specific history start: null for '
                                                                      'pathology edges; '
                                                                      'map_medication_order.ORIG_ORDER_DT_TM '
                                                                      'for medication edges; selected '
                                                                      'map_waiting_list representative '
                                                                      'BEG_EFFECTIVE_DT_TM for waiting-list '
                                                                      'edges. These are request/version-time '
                                                                      'proxies, not a uniformly observed '
                                                                      'status transition. Missing timestamps '
                                                                      'remain null.',
                                      'record_status_effective_to': 'Pathology edges always have a null end. '
                                                                    'Medication uses order '
                                                                    'SOURCE_ABSENT_DETECTED_TS when '
                                                                    'retracted, without fallback even if '
                                                                    'null; otherwise finite administration '
                                                                    'CE_VALID_UNTIL_DT_TM when superseded, '
                                                                    'else null. Waiting-list edges copy '
                                                                    "their representative's end: for "
                                                                    'superseded versions, '
                                                                    'coalesce(SOURCE_ABSENT_DETECTED_TS, '
                                                                    'END_EFFECTIVE_DT_TM, ADC_UPDT), with no '
                                                                    'far-future sentinel filter; active '
                                                                    'versions have a null end.',
                                      'request_key': 'Request-side source identifier retained under the '
                                                     'governed key name.',
                                      'request_thread_key': 'Deterministic request-edge primary key.',
                                      'requested_datetime': 'Source request timestamp.',
                                      'responded_datetime': 'Source report or measurement timestamp.',
                                      'response_key': 'Response-side source identifier retained under the '
                                                      'governed key name.',
                                      'source_history_row_count': 'Number of source rows supporting the '
                                                                  'edge.',
                                      'source_object': 'Request-thread arm: pathology, medication, or '
                                                       'waiting_list.',
                                      'source_patient_event_key': 'Request-side patient event identifier.',
                                      'source_row_id': 'Representative stable source row identifier.',
                                      'source_system': 'Source system identifier.',
                                      'source_table': 'Fully qualified configured source table.',
                                      'source_update_timestamp': 'Arm-specific provenance: null for '
                                                                 'pathology edges; greatest of '
                                                                 'map_med_admin.ADC_UPDT, '
                                                                 'map_medication_order.SOURCE_ADC_UPDT and '
                                                                 'map_medication_order.ADC_UPDT for '
                                                                 'medication edges; selected '
                                                                 'map_waiting_list representative '
                                                                 'SOURCE_ADC_UPDT for waiting-list edges. '
                                                                 'This is not uniformly a native source '
                                                                 'modification clock. The appointment '
                                                                 'existence/person join contributes no '
                                                                 'timestamp; no Silver refresh timestamp is '
                                                                 'substituted.',
                                      'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                     'identifier. Not salted, not secret; a stable join key '
                                                     'across feeds where person_id is unresolved.',
                                      'target_patient_event_key': 'Response-side patient event identifier.'},
                          'quantity_override': False,
                          'table': 'Directed indexed-event edges for pathology order-to-report, medication '
                                   'order-to-administration, and waiting-list entry-to-appointment routes. '
                                   'Pathology edges are accession-scoped co-membership (requested test and '
                                   'report series share an accession), not result-level attribution. Gold QC '
                                   'twin of the silver product: 4 columns are repaired or nulled, 0 check(s) '
                                   'are advisory. Each rule states its reason in the pipeline notebook, and '
                                   'Lakeflow expectation metrics report what every rule matched on each '
                                   'update. Rows with no person are kept: about 14% of request-thread edges '
                                   'come from unlinked LIMS pathology requests and cannot be attributed to a '
                                   'person. Coverage: The pathology arm of request threads drops from August '
                                   '2017 as pathology reports fall at source while orders rise; '
                                   'complete-through month per release: 6_mgmt.data_quality.release_finding '
                                   '(finding_code F2_complete_through).'}}
