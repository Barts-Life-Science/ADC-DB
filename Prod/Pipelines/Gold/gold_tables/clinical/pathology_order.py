"""Gold clinical.pathology_order: 3 dataset(s).

- datasets: _qc_lifecycle_clinical_pathology_order, _qc_clinical_pathology_order, clinical_pathology_order
- moved verbatim from: 2_gold_clinical_events:4570, 2_gold_clinical_events:4607, 2_gold_clinical_events:4616, 2_gold_clinical_events:4689, 2_gold_clinical_events:4700, 2_gold_clinical_events:4705, 2_gold_clinical_events:4710, 2_gold_clinical_events:4732

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.pathology_order ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_PATHOLOGY_ORDER_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`encounter_link_method` AS `encounter_link_method`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`source_object` AS `source_object`',
    '`wkg_code` AS `wkg_code`',
    "CASE WHEN UPPER(TRIM(CAST(`tlc_code` AS STRING))) = '0' OR UPPER(TRIM(CAST(`tlc_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `tlc_code` END AS `tlc_code`",
    '`order_id` AS `order_id`',
    '`order_mnemonic` AS `order_mnemonic`',
    '`raw_request_text` AS `raw_request_text`',
    '`test_description` AS `test_description`',
    '`test_snomed_code` AS `test_snomed_code`',
    '`test_omop_concept_id` AS `test_omop_concept_id`',
    '`mapping_status` AS `mapping_status`',
    '`request_ordinal` AS `request_ordinal`',
    '`specimen_key` AS `specimen_key`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_pathology_order_metadata; source history stays on the main research table.
CLINICAL_PATHOLOGY_ORDER_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 388,597,322 rows of
    # 388,597,322 that are current and attributable; identity_status = 'resolved' keeps the
    # 314,089,167 rows of 388,597,322 that are current and attributable. Superseded versions
    # and rows whose identity was never resolved are not research data, and a consumer who
    # wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_PATHOLOGY_ORDER_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.pathology_order.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 74,508,155 of 388,597,322 at the profile.
    "gold.clinical.pathology_order.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 388,597,322 at the profile.
    "gold.clinical.pathology_order.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 285,411 of 388,597,322 rows (0.0734%) when profiled on
    # 2026-08-24.
    "gold.clinical.pathology_order.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,203,076 of 388,597,322 rows (0.31%) when profiled on
    # 2026-08-24.
    "gold.clinical.pathology_order.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# LIFECYCLE_ONE_ROW_V1: one lifecycle row per patient_event_key for the twin below (see
# GOLD_LIFECYCLE_ONE_ROW_PER_KEY in gold_journey_shared.py for why it is its own MV).
@dp.materialized_view(
    name=_n("gold_qc._lifecycle_clinical_pathology_order"),
    refresh_policy="auto",
)
def _gold_lifecycle_clinical_pathology_order():
    """One internal lifecycle row per pathology-order key; read by _qc() for the twin below."""
    return _lifecycle_one_row_source("clinical_pathology_order")


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_PATHOLOGY_ORDER_PERSON_SCOPE_RULES = {
    "gold.clinical.pathology_order.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_PATHOLOGY_ORDER_ORPHAN_RULES = {
    "gold.clinical.pathology_order.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
    "gold.clinical.pathology_order.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_pathology_order"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_PATHOLOGY_ORDER_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PATHOLOGY_ORDER_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_PATHOLOGY_ORDER_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_PATHOLOGY_ORDER_ORPHAN_RULES)
def _gold_qc_clinical_pathology_order():
    """Quality-controlled twin of journey_clinical.pathology_order."""
    # 525 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_pathology_order",
        CLINICAL_PATHOLOGY_ORDER_SELECT,
        fk_columns=["person_id", "encounter_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.pathology_order"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_pathology_order():
    """Contract-v2 public twin of clinical_pathology_order; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_pathology_order")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`encounter_link_method` AS `encounter_link_method`',  # GOLD_COLUMN_PARITY_V1
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`source_object` AS `source_object`',
        '`wkg_code` AS `wkg_code`',
        '`tlc_code` AS `tlc_code`',
        '`order_id` AS `order_id`',
        '`order_mnemonic` AS `order_mnemonic`',
        '`raw_request_text` AS `raw_request_text`',
        '`test_description` AS `test_description`',
        '`test_snomed_code` AS `test_snomed_code`',
        '`test_omop_concept_id` AS `test_omop_concept_id`',
        '`mapping_status` AS `mapping_status`',
        '`request_ordinal` AS `request_ordinal`',
        '`specimen_key` AS `specimen_key`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'clinical_pathology_order': GoldRules(
        table='clinical_pathology_order',
        select=CLINICAL_PATHOLOGY_ORDER_SELECT,
        mandatory=CLINICAL_PATHOLOGY_ORDER_MANDATORY_RULES,
        advisory=CLINICAL_PATHOLOGY_ORDER_ADVISORY_RULES,
        person_scope=CLINICAL_PATHOLOGY_ORDER_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth'),
        one_row_per_key=True,
        value_rules={'gold.clinical.pathology_order.test_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                  'column': 'test_snomed_code',
                                                                                  'companions': [],
                                                                                  'match': '`test_snomed_code` '
                                                                                           'IS NOT NULL '
                                                                                           'AND (NOT '
                                                                                           '`test_snomed_code` '
                                                                                           'RLIKE '
                                                                                           "'^[1-9][0-9]{5,17}$' "
                                                                                           'OR '
                                                                                           'substring(`test_snomed_code`, '
                                                                                           'length(`test_snomed_code`) '
                                                                                           '- 2, 2) NOT IN '
                                                                                           "('00', '10') "
                                                                                           'OR '
                                                                                           '(substring(`test_snomed_code`, '
                                                                                           'length(`test_snomed_code`) '
                                                                                           "- 2, 2) = '10' "
                                                                                           'AND '
                                                                                           'length(`test_snomed_code`) '
                                                                                           '< 11))',
                                                                                  'table': 'clinical_pathology_order'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_lifecycle_one_row_source = _rules._lifecycle_one_row_source


finalise()


COMMENTS = {'_qc_clinical_pathology_order': {'columns': {'confidentiality_code': 'Source confidentiality code.',
                                              'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                              'available.',
                                              'encounter_link_method': 'How encounter_id was set '
                                                                       '(PATH_ENCOUNTER_V1). '
                                                                       "source_accession: the row's own "
                                                                       'Millennium source names the '
                                                                       'encounter. accession_single: every '
                                                                       'current Millennium source of the '
                                                                       'alias-resolved accession names this '
                                                                       'one encounter (how TFC LIMS rows '
                                                                       'inherit one once their accession is '
                                                                       'merged with the Millennium order). '
                                                                       'accession_multiple: those sources '
                                                                       'name different encounters, so '
                                                                       'encounter_id is NULL. none: the '
                                                                       'accession has no Millennium source, '
                                                                       'so encounter_id is NULL. Gold: '
                                                                       'encounter_id can still be NULL here '
                                                                       'when the encounter is not in the '
                                                                       'Gold encounter spine.',
                                              'event_datetime': 'Requested or source-validity timestamp.',
                                              'event_datetime_local': 'Local-time equivalent of the order '
                                                                      'event datetime, derived from the '
                                                                      'source request or sample local '
                                                                      'timestamps. Gold: NULL wherever Gold '
                                                                      'nulls event_datetime.',
                                              'event_end_datetime': 'Order event end when supplied.',
                                              'event_end_datetime_local': 'Local-time equivalent of the '
                                                                          'order event end datetime when '
                                                                          'supplied.',
                                              'loaded_at': 'map_pathology_requested_test.ADC_UPDT carried '
                                                           'unchanged for the requested-test occurrence. '
                                                           'Accession alias and identity joins do not '
                                                           'contribute another clock. This is ingestion '
                                                           'provenance, not the request/sample event time or '
                                                           'Silver refresh time; a missing timestamp remains '
                                                           'null.',
                                              'mapping_status': "'mapped' means a rule ran, not that codes "
                                                                'landed — CERNER arm carries zero baked '
                                                                'codes.',
                                              'order_id': 'CERNER ORDER_ID as BIGINT; NULL on the TFC_LIMS '
                                                          'arm.',
                                              'order_mnemonic': 'CERNER order mnemonic.',
                                              'patient_event_key': 'Deterministic pathology-order event key '
                                                                   'minted from '
                                                                   'requested_test_occurrence_id.',
                                              'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                              'raw_request_text': 'Native request wording. Identifiable free '
                                                                  'text; ig_risk 4',
                                              'record_status': 'Literal active for every retained '
                                                               'requested-test occurrence from '
                                                               'map_pathology_requested_test. This '
                                                               'projection does not evaluate source '
                                                               'presence, cancellation or result/report '
                                                               'status; active is not evidence that an order '
                                                               'has never been withdrawn.',
                                              'record_status_effective_from': 'Always NULL as a TIMESTAMP in '
                                                                              'the pathology-order '
                                                                              'projection. No status-start '
                                                                              'clock is supplied, and '
                                                                              'accession request/sample '
                                                                              'times are not substituted.',
                                              'record_status_effective_to': 'Always NULL as a TIMESTAMP in '
                                                                            'the pathology-order projection. '
                                                                            'No status-end clock is '
                                                                            'supplied, and result, report or '
                                                                            'ingestion timestamps are not '
                                                                            'substituted.',
                                              'request_ordinal': 'Source request ordinal within the '
                                                                 'accession.',
                                              'source_code': 'Native order catalogue code.',
                                              'source_coding_system': 'Native order catalogue coding system.',
                                              'source_display': 'Native order description.',
                                              'source_object': 'Source arm normalized from the existing '
                                                               'source_system value.',
                                              'source_update_timestamp': 'Always NULL as a TIMESTAMP in the '
                                                                         'pathology-order projection. The '
                                                                         'contributing ADC_UPDT is published '
                                                                         'separately as loaded_at and is not '
                                                                         'copied into this field.',
                                              'specimen_key': 'Alias-resolved accession-minted specimen key.',
                                              'subject_id_system': 'Identifier system used for subject_key.',
                                              'subject_key': 'Deterministic SHA-256 over the strongest '
                                                             'source identifier. Not salted, not secret; a '
                                                             'stable join key across feeds where person_id '
                                                             'is unresolved.',
                                              'test_description': 'Native requested-test description.',
                                              'test_omop_concept_id': 'Source-provided requested-test OMOP '
                                                                      'concept identifier.',
                                              'test_snomed_code': 'Source-provided requested-test SNOMED '
                                                                  'code.',
                                              'tlc_code': 'TFC test-level code.',
                                              'vip_ind': 'Indicator that the source record carries VIP '
                                                         'handling status for each pathology order record. '
                                                         'It is produced by the silver transformation and '
                                                         'has no direct bronze-column lineage entry. Null '
                                                         'means the source did not state the indicator and '
                                                         'must not be interpreted as false.',
                                              'withheld_identity_ind': 'Withheld identity indicator.',
                                              'wkg_code': 'TFC work-group code.'},
                                  'quantity_override': True,
                                  'table': 'Internal quality-controlled twin of clinical_pathology_order: '
                                           'the Gold repairs, nulls and expectations are applied here and '
                                           'the published Gold MV reads this object. Materialized rather '
                                           'than temporary so the published MV can refresh incrementally.'},
 '_qc_lifecycle_clinical_pathology_order': {'columns': {},
                                            'quantity_override': False,
                                            'table': 'Internal: silver _clinical_pathology_order_metadata '
                                                     'reduced to exactly one row per patient_event_key '
                                                     '(greatest support-column struct per key), so the QC '
                                                     "twin's lifecycle join cannot copy rows. Join keys: "
                                                     'patient_event_key.'},
 'clinical_pathology_order': {'columns': {'confidentiality_code': 'Source confidentiality code.',
                                          'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                          'encounter_link_method': 'How encounter_id was set '
                                                                   '(PATH_ENCOUNTER_V1). source_accession: '
                                                                   "the row's own Millennium source names "
                                                                   'the encounter. accession_single: every '
                                                                   'current Millennium source of the '
                                                                   'alias-resolved accession names this one '
                                                                   'encounter (how TFC LIMS rows inherit one '
                                                                   'once their accession is merged with the '
                                                                   'Millennium order). accession_multiple: '
                                                                   'those sources name different encounters, '
                                                                   'so encounter_id is NULL. none: the '
                                                                   'accession has no Millennium source, so '
                                                                   'encounter_id is NULL. Gold: encounter_id '
                                                                   'can still be NULL here when the '
                                                                   'encounter is not in the Gold encounter '
                                                                   'spine.',
                                          'event_datetime': 'Requested or source-validity timestamp.',
                                          'event_datetime_local': 'Local-time equivalent of the order event '
                                                                  'datetime, derived from the source request '
                                                                  'or sample local timestamps. Gold: NULL '
                                                                  'wherever Gold nulls event_datetime.',
                                          'event_end_datetime': 'Order event end when supplied.',
                                          'event_end_datetime_local': 'Local-time equivalent of the order '
                                                                      'event end datetime when supplied.',
                                          'loaded_at': 'map_pathology_requested_test.ADC_UPDT carried '
                                                       'unchanged for the requested-test occurrence. '
                                                       'Accession alias and identity joins do not contribute '
                                                       'another clock. This is ingestion provenance, not the '
                                                       'request/sample event time or Silver refresh time; a '
                                                       'missing timestamp remains null.',
                                          'mapping_status': "'mapped' means a rule ran, not that codes "
                                                            'landed — CERNER arm carries zero baked codes.',
                                          'order_id': 'CERNER ORDER_ID as BIGINT; NULL on the TFC_LIMS arm.',
                                          'order_mnemonic': 'CERNER order mnemonic.',
                                          'patient_event_key': 'Deterministic pathology-order event key '
                                                               'minted from requested_test_occurrence_id.',
                                          'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                          'raw_request_text': 'Native request wording. Identifiable free '
                                                              'text; ig_risk 4',
                                          'record_status': 'Literal active for every retained requested-test '
                                                           'occurrence from map_pathology_requested_test. '
                                                           'This projection does not evaluate source '
                                                           'presence, cancellation or result/report status; '
                                                           'active is not evidence that an order has never '
                                                           'been withdrawn.',
                                          'record_status_effective_from': 'Always NULL as a TIMESTAMP in the '
                                                                          'pathology-order projection. No '
                                                                          'status-start clock is supplied, '
                                                                          'and accession request/sample '
                                                                          'times are not substituted.',
                                          'record_status_effective_to': 'Always NULL as a TIMESTAMP in the '
                                                                        'pathology-order projection. No '
                                                                        'status-end clock is supplied, and '
                                                                        'result, report or ingestion '
                                                                        'timestamps are not substituted.',
                                          'request_ordinal': 'Source request ordinal within the accession.',
                                          'source_code': 'Native order catalogue code.',
                                          'source_coding_system': 'Native order catalogue coding system.',
                                          'source_display': 'Native order description.',
                                          'source_object': 'Source arm normalized from the existing '
                                                           'source_system value.',
                                          'source_update_timestamp': 'Always NULL as a TIMESTAMP in the '
                                                                     'pathology-order projection. The '
                                                                     'contributing ADC_UPDT is published '
                                                                     'separately as loaded_at and is not '
                                                                     'copied into this field.',
                                          'specimen_key': 'Alias-resolved accession-minted specimen key.',
                                          'subject_id_system': 'Identifier system used for subject_key.',
                                          'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                         'identifier. Not salted, not secret; a stable join '
                                                         'key across feeds where person_id is unresolved.',
                                          'test_description': 'Native requested-test description.',
                                          'test_omop_concept_id': 'Source-provided requested-test OMOP '
                                                                  'concept identifier.',
                                          'test_snomed_code': 'SNOMED CT concept ID of the requested test, '
                                                              'concept IDs only. NLMC codes and Barts panel '
                                                              'ids that TFC put in the same source field are '
                                                              'in test_local_code (system test_code_system).',
                                          'tlc_code': 'TFC test-level code.',
                                          'vip_ind': 'Indicator that the source record carries VIP handling '
                                                     'status for each pathology order record. It is produced '
                                                     'by the silver transformation and has no direct '
                                                     'bronze-column lineage entry. Null means the source did '
                                                     'not state the indicator and must not be interpreted as '
                                                     'false.',
                                          'withheld_identity_ind': 'Withheld identity indicator.',
                                          'wkg_code': 'TFC work-group code.'},
                              'quantity_override': False,
                              'table': 'One requested-test occurrence across TFC_LIMS and CERNER; anchors '
                                       'accession-scoped order-to-report request threads. Gold QC twin of '
                                       'the silver product: 3 columns are repaired or nulled, 1 rule(s) drop '
                                       'rows, 4 check(s) are advisory. Each rule states its reason in the '
                                       'pipeline notebook, and Lakeflow expectation metrics report what '
                                       'every rule matched on each update.'}}
