"""Gold spine.theatre_implant: 2 dataset(s).

- datasets: _qc_reference_theatre_implant, reference_theatre_implant
- moved verbatim from: 1_gold_spine_and_reference:4702, 1_gold_spine_and_reference:4729, 1_gold_spine_and_reference:4735, 1_gold_spine_and_reference:4780, 1_gold_spine_and_reference:4785, 1_gold_spine_and_reference:4789, 1_gold_spine_and_reference:4809

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.theatre_implant ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_THEATRE_IMPLANT_SELECT = [
    '`theatre_implant_key` AS `theatre_implant_key`',
    '`theatre_case_key` AS `theatre_case_key`',
    '`case_link_status` AS `case_link_status`',
    '`item_id` AS `item_id`',
    '`manufacturer` AS `manufacturer`',
    '`model_number` AS `model_number`',
    '`catalog_number` AS `catalog_number`',
    '`serial_number` AS `serial_number`',
    '`lot_number` AS `lot_number`',
    '`batch_number` AS `batch_number`',
    '`implant_site` AS `implant_site`',
    '`implant_size` AS `implant_size`',
    '`quantity` AS `quantity`',
    '`expiry_date` AS `expiry_date`',
    '`free_text_item_desc` AS `free_text_item_desc`',
    '`implanted_by_practitioner_id` AS `implanted_by_practitioner_id`',
    '`mill_implant_event_id` AS `mill_implant_event_id`',
    '`implant_link_method` AS `implant_link_method`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`document_type_desc` AS `document_type_desc`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


REFERENCE_THEATRE_IMPLANT_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`implant_datetime` AS `implant_datetime`',
    '`implant_datetime_basis` AS `implant_datetime_basis`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_THEATRE_IMPLANT_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 55,523 of 205,632
    # rows (27%) when profiled on 2026-08-24.
    # INSR_W3 W3.08 (F16): an expiry date is a planned future date; warn only outside 1990..loaded_at+15y.
    "gold.reference.theatre_implant.expiry_date.expiry_window":
        "NOT COALESCE(CAST(`expiry_date` AS DATE) < DATE'1990-01-01' OR CAST(`expiry_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 15 YEARS, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
REFERENCE_THEATRE_IMPLANT_PERSON_SCOPE_RULES = {
    "gold.reference.theatre_implant.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
REFERENCE_THEATRE_IMPLANT_ORPHAN_RULES = {
    "gold.reference.theatre_implant.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_theatre_implant"),
    refresh_policy="auto",
)
@dp.expect_all(REFERENCE_THEATRE_IMPLANT_ADVISORY_RULES)
@dp.expect_all_or_drop(REFERENCE_THEATRE_IMPLANT_PERSON_SCOPE_RULES)
@dp.expect_all(REFERENCE_THEATRE_IMPLANT_ORPHAN_RULES)
def _gold_qc_reference_theatre_implant():
    """Quality-controlled twin of journey_reference.theatre_implant."""
    # 3,332 rows point at a encounter_id the spine does not have. The pointer is nulled so
    # it cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "reference_theatre_implant",
        REFERENCE_THEATRE_IMPLANT_SELECT,
        fk_columns=["encounter_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_reference.theatre_implant"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_theatre_implant():
    """Contract-v2 public twin of reference_theatre_implant; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_theatre_implant")).selectExpr(
        '`theatre_implant_key` AS `theatre_implant_key`',
        '`theatre_case_key` AS `theatre_case_key`',
        '`case_link_status` AS `case_link_status`',
        '`item_id` AS `item_id`',
        '`manufacturer` AS `manufacturer`',
        '`model_number` AS `model_number`',
        '`catalog_number` AS `catalog_number`',
        '`serial_number` AS `serial_number`',
        '`lot_number` AS `lot_number`',
        '`batch_number` AS `batch_number`',
        '`implant_site` AS `implant_site`',
        '`implant_size` AS `implant_size`',
        '`quantity` AS `quantity`',
        '`expiry_date` AS `expiry_date`',
        '`free_text_item_desc` AS `free_text_item_desc`',
        '`implanted_by_practitioner_id` AS `implanted_by_practitioner_id`',
        '`mill_implant_event_id` AS `mill_implant_event_id`',
        '`implant_link_method` AS `implant_link_method`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`document_type_desc` AS `document_type_desc`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
        '`implant_datetime` AS `implant_datetime`',  # INSR_W3 W3.12
        '`implant_datetime_basis` AS `implant_datetime_basis`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'reference_theatre_implant': GoldRules(
        table='reference_theatre_implant',
        select=REFERENCE_THEATRE_IMPLANT_SELECT,
        advisory=REFERENCE_THEATRE_IMPLANT_ADVISORY_RULES,
        person_scope=REFERENCE_THEATRE_IMPLANT_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['theatre_implant_key'], 'support_columns': ['loaded_at']},
        value_rules={'gold.reference.theatre_implant.implanted_by_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                         'column': 'implanted_by_practitioner_id',
                                                                                         'companions': [],
                                                                                         'match': '`implanted_by_practitioner_id` '
                                                                                                  '= 0',
                                                                                         'table': 'reference_theatre_implant'},
         'gold.reference.theatre_implant.item_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                    'column': 'item_id',
                                                                    'companions': [],
                                                                    'match': "TRIM(`item_id`) = '0'",
                                                                    'table': 'reference_theatre_implant'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_theatre_implant': {'columns': {'_source_row_id': 'S7 procedure implant feeder; retained as a '
                                                                 'source-faithful bronze input or Journey '
                                                                 'standard-block field.',
                                               '_source_system': 'Value describing source system for the '
                                                                 'theatre implant record. It is produced by '
                                                                 'the silver transformation and has no '
                                                                 'direct bronze-column lineage entry. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                               '_source_table': 'Value describing source table for the '
                                                                'theatre implant record. It is produced by '
                                                                'the silver transformation and has no direct '
                                                                'bronze-column lineage entry. Whitespace and '
                                                                'source sentinel text are retained unless '
                                                                'the pipeline explicitly normalizes them; '
                                                                'null means no value was supplied.',
                                               'batch_number': 'S7 procedure implant feeder; retained as a '
                                                               'source-faithful bronze input or Journey '
                                                               'standard-block field.',
                                               'case_link_status': 'S14 theatre feeder and Journey standard '
                                                                   'block; retained as a source-faithful '
                                                                   'bronze input or Journey standard-block '
                                                                   'field.',
                                               'catalog_number': 'S7 procedure implant feeder; retained as a '
                                                                 'source-faithful bronze input or Journey '
                                                                 'standard-block field.',
                                               'document_type_desc': 'S7 procedure implant feeder; retained '
                                                                     'as a source-faithful bronze input or '
                                                                     'Journey standard-block field.',
                                               'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT.',
                                               'expiry_date': 'S7 procedure implant feeder; retained as a '
                                                              'source-faithful bronze input or Journey '
                                                              'standard-block field.',
                                               'free_text_item_desc': 'S7 procedure implant feeder; retained '
                                                                      'as a source-faithful bronze input or '
                                                                      'Journey standard-block field.',
                                               'implant_datetime': 'Time of the theatre case the implant was '
                                                                   'logged against (surgery start, else '
                                                                   'check-in, else scheduled start); the '
                                                                   'implant log has no own date. INSR_W2 '
                                                                   'W2.06 (L5267).',
                                               'implant_datetime_basis': 'case_surgery_start, case_checkin '
                                                                         'or case_scheduled_start. INSR_W2 '
                                                                         'W2.06.',
                                               'implant_link_method': 'S7 crosswalk provenance: '
                                                                      'SERIAL_UNIQUE, SERIAL_AMBIGUOUS or '
                                                                      'NONE.',
                                               'implant_site': 'S7 procedure implant feeder; retained as a '
                                                               'source-faithful bronze input or Journey '
                                                               'standard-block field.',
                                               'implant_size': 'S7 procedure implant feeder; retained as a '
                                                               'source-faithful bronze input or Journey '
                                                               'standard-block field.',
                                               'implanted_by_practitioner_id': 'Native Millennium personnel '
                                                                               'PERSON_ID as BIGINT for the '
                                                                               'implanter.',
                                               'item_id': 'S7 procedure implant feeder; retained as a '
                                                          'source-faithful bronze input or Journey '
                                                          'standard-block field.',
                                               'loaded_at': 'map_theatre_implant_log.ADC_UPDT carried '
                                                            'unchanged for the implant-log row. The '
                                                            'parent-case existence join adds no timestamp; '
                                                            'this is bronze ingestion provenance, not '
                                                            'implant/expiry time or Silver refresh time.',
                                               'lot_number': 'S7 implant traceability input retained because '
                                                             'most SurgiNet rows have no Mill counterpart.',
                                               'manufacturer': 'S7 procedure implant feeder; retained as a '
                                                               'source-faithful bronze input or Journey '
                                                               'standard-block field.',
                                               'mill_implant_event_id': 'S7 identity crosswalk to '
                                                                        'map_implant_details.EVENT_ID when '
                                                                        'person+serial is unique.',
                                               'model_number': 'S7 procedure implant feeder; retained as a '
                                                               'source-faithful bronze input or Journey '
                                                               'standard-block field.',
                                               'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                               'quantity': 'S7 procedure implant feeder; retained as a '
                                                           'source-faithful bronze input or Journey '
                                                           'standard-block field.',
                                               'serial_number': 'S7 implant traceability and '
                                                                'identity-crosswalk input.',
                                               'theatre_case_key': 'Source-preserved theatre case key.',
                                               'theatre_implant_key': 'Deterministic SHA-256 key for the '
                                                                      'theatre implant row.'},
                                   'quantity_override': True,
                                   'table': 'Internal quality-controlled twin of reference_theatre_implant: '
                                            'the Gold repairs, nulls and expectations are applied here and '
                                            'the published Gold MV reads this object. Materialized rather '
                                            'than temporary so the published MV can refresh incrementally.'},
 'reference_theatre_implant': {'columns': {'_source_row_id': 'Native source row identifier of the implant '
                                                             'log entry, as text.',
                                           '_source_system': 'Value describing source system for the theatre '
                                                             'implant record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Whitespace and source sentinel '
                                                             'text are retained unless the pipeline '
                                                             'explicitly normalizes them; null means no '
                                                             'value was supplied.',
                                           '_source_table': 'Value describing source table for the theatre '
                                                            'implant record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Whitespace and source sentinel '
                                                            'text are retained unless the pipeline '
                                                            'explicitly normalizes them; null means no value '
                                                            'was supplied.',
                                           'batch_number': 'Batch or lot number entered on the implant log, '
                                                           'as recorded (free text).',
                                           'case_link_status': 'linked when the implant log row joins a '
                                                               'theatre case; orphan when no case was found.',
                                           'catalog_number': 'Catalogue number entered on the implant log, '
                                                             'as recorded (free text, not normalised).',
                                           'document_type_desc': 'Intra-operative document type (trust site '
                                                                 'and theatre suite) on which the implant '
                                                                 'was logged.',
                                           'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT.',
                                           'expiry_date': 'Device expiry date entered on the implant log; '
                                                          'NULL when not recorded.',
                                           'free_text_item_desc': 'Free-text item description entered on the '
                                                                  'implant log.',
                                           'implant_datetime': 'Time of the theatre case the implant was '
                                                               'logged against (surgery start, else '
                                                               'check-in, else scheduled start); the implant '
                                                               'log has no own date. INSR_W2 W2.06 (L5267).',
                                           'implant_datetime_basis': 'case_surgery_start, case_checkin or '
                                                                     'case_scheduled_start. INSR_W2 W2.06.',
                                           'implant_link_method': 'S7 crosswalk provenance: SERIAL_UNIQUE, '
                                                                  'SERIAL_AMBIGUOUS or NONE.',
                                           'implant_site': 'Body site of the implant as entered on the '
                                                           'implant log (free text).',
                                           'implant_size': 'Implant size as entered on the implant log (free '
                                                           'text).',
                                           'implanted_by_practitioner_id': 'Implanting practitioner: not '
                                                                           'captured at source, so the value '
                                                                           'is 0 on every implant log row; '
                                                                           'use the case surgeon from '
                                                                           'reference_theatre_case.',
                                           'item_id': 'SurgiNet item identifier of the implanted catalogue '
                                                      'item on the intra-operative implant log.',
                                           'loaded_at': 'map_theatre_implant_log.ADC_UPDT carried unchanged '
                                                        'for the implant-log row. The parent-case existence '
                                                        'join adds no timestamp; this is bronze ingestion '
                                                        'provenance, not implant/expiry time or Silver '
                                                        'refresh time.',
                                           'lot_number': 'S7 implant traceability input retained because '
                                                         'most SurgiNet rows have no Mill counterpart.',
                                           'manufacturer': 'Manufacturer entered on the implant log, as '
                                                           'recorded (free text, not normalised); see '
                                                           'reference_device_mapping for the mapped device.',
                                           'mill_implant_event_id': 'S7 identity crosswalk to '
                                                                    'map_implant_details.EVENT_ID when '
                                                                    'person+serial is unique.',
                                           'model_number': 'Model number entered on the implant log, as '
                                                           'recorded (free text).',
                                           'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                           'quantity': 'Number of units of the item implanted, as entered on '
                                                       'the implant log.',
                                           'serial_number': 'S7 implant traceability and identity-crosswalk '
                                                            'input.',
                                           'theatre_case_key': 'Source-preserved theatre case key.',
                                           'theatre_implant_key': 'Deterministic SHA-256 key for the theatre '
                                                                  'implant row.'},
                               'quantity_override': False,
                               'table': 'One SurgiNet theatre implant row; serial, lot and batch fields are '
                                        'IG-sensitive. Gold QC twin of the silver product: 1 columns are '
                                        'repaired or nulled, 1 check(s) are advisory. Each rule states its '
                                        'reason in the pipeline notebook, and Lakeflow expectation metrics '
                                        'report what every rule matched on each update. Coverage: The '
                                        'implanting practitioner is not captured at source (0 of 214,446 '
                                        'implant log rows); use the case surgeon instead; complete-through '
                                        'month per release: 6_mgmt.data_quality.release_finding '
                                        '(finding_code F2_complete_through).'}}
