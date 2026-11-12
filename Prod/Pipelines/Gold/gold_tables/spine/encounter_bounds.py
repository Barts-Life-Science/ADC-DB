"""Gold spine.encounter_bounds: 2 dataset(s).

- datasets: _qc_reference_encounter_bounds, reference_encounter_bounds
- moved verbatim from: 1_gold_spine_and_reference:2664, 1_gold_spine_and_reference:2691, 1_gold_spine_and_reference:2696, 1_gold_spine_and_reference:2702, 1_gold_spine_and_reference:2744, 1_gold_spine_and_reference:2748, 1_gold_spine_and_reference:2765

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.encounter_bounds ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_ENCOUNTER_BOUNDS_SELECT = [
    '`encounter_bounds_key` AS `encounter_bounds_key`',
    '`encounter_id` AS `encounter_id`',
    '`source_encounter_id` AS `source_encounter_id`',
    '`first_clinical_event_datetime` AS `first_clinical_event_datetime`',
    '`last_clinical_event_datetime` AS `last_clinical_event_datetime`',
    '`clinical_event_count` AS `clinical_event_count`',
    '`first_contemporaneous_event_datetime` AS `first_contemporaneous_event_datetime`',
    '`last_contemporaneous_event_datetime` AS `last_contemporaneous_event_datetime`',
    '`contemporaneous_event_count` AS `contemporaneous_event_count`',
    '`first_order_datetime` AS `first_order_datetime`',
    '`last_order_datetime` AS `last_order_datetime`',
    '`order_count` AS `order_count`',
    '`ward_move_count` AS `ward_move_count`',
    '`ward_occupancy_minutes` AS `ward_occupancy_minutes`',
    '`last_ward_in_datetime` AS `last_ward_in_datetime`',
    '`last_ward_out_datetime` AS `last_ward_out_datetime`',
    '`last_ward_still_open_ind` AS `last_ward_still_open_ind`',
    '`event_first_datetime` AS `event_first_datetime`',
    '`event_last_datetime` AS `event_last_datetime`',
    '`in_activity_bounds_ind` AS `in_activity_bounds_ind`',
    '`in_event_bounds_ind` AS `in_event_bounds_ind`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


REFERENCE_ENCOUNTER_BOUNDS_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`bounds_birth_floor_applied_ind` AS `bounds_birth_floor_applied_ind`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_ENCOUNTER_BOUNDS_INSR_W4_ADVISORY_RULES = {  # INSR_W4
    # INSR_W4 W4.05 date_ordering (estate 2026-10-29): The last ward move-in cannot follow its move-out unless the stay is still open (L3749).
    'gold.reference.encounter_bounds.table.insr_f6_last_ward_inversion':
        'NOT COALESCE((`last_ward_in_datetime` > `last_ward_out_datetime` AND NOT coalesce(`last_ward_still_open_ind`, FALSE)), FALSE)',
}


REFERENCE_ENCOUNTER_BOUNDS_MANDATORY_RULES = {
    # The flow nulls encounter_id when it names a parent the spine does not have, and this
    # rule then drops the row, because the row is only the start and end of an encounter, so
    # with no encounter it bounds nothing. That was 413,547 orphaned rows plus 0 that
    # already had no encounter_id, out of 47,674,684.
    "gold.reference.encounter_bounds.encounter_id.fk_containment":
        "`encounter_id` IS NOT NULL",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
REFERENCE_ENCOUNTER_BOUNDS_ORPHAN_RULES = {
    "gold.reference.encounter_bounds.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_encounter_bounds"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_ENCOUNTER_BOUNDS_MANDATORY_RULES)
@dp.expect_all(REFERENCE_ENCOUNTER_BOUNDS_ORPHAN_RULES)
@dp.expect_all(REFERENCE_ENCOUNTER_BOUNDS_INSR_W4_ADVISORY_RULES)  # INSR_W4
def _gold_qc_reference_encounter_bounds():
    """Quality-controlled twin of journey_reference.encounter_bounds."""
    df = _qc(
        "reference_encounter_bounds",
        REFERENCE_ENCOUNTER_BOUNDS_SELECT,
        fk_columns=["encounter_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_reference.encounter_bounds"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_encounter_bounds():
    """Contract-v2 public twin of reference_encounter_bounds; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_encounter_bounds")).selectExpr(
        '`encounter_bounds_key` AS `encounter_bounds_key`',
        '`encounter_id` AS `encounter_id`',
        '`source_encounter_id` AS `source_encounter_id`',
        '`first_clinical_event_datetime` AS `first_clinical_event_datetime`',
        '`last_clinical_event_datetime` AS `last_clinical_event_datetime`',
        '`clinical_event_count` AS `clinical_event_count`',
        '`first_contemporaneous_event_datetime` AS `first_contemporaneous_event_datetime`',
        '`last_contemporaneous_event_datetime` AS `last_contemporaneous_event_datetime`',
        '`contemporaneous_event_count` AS `contemporaneous_event_count`',
        '`first_order_datetime` AS `first_order_datetime`',
        '`last_order_datetime` AS `last_order_datetime`',
        '`order_count` AS `order_count`',
        '`ward_move_count` AS `ward_move_count`',
        '`ward_occupancy_minutes` AS `ward_occupancy_minutes`',
        '`last_ward_in_datetime` AS `last_ward_in_datetime`',
        '`last_ward_out_datetime` AS `last_ward_out_datetime`',
        '`last_ward_still_open_ind` AS `last_ward_still_open_ind`',
        '`event_first_datetime` AS `event_first_datetime`',
        '`event_last_datetime` AS `event_last_datetime`',
        '`in_activity_bounds_ind` AS `in_activity_bounds_ind`',
        '`in_event_bounds_ind` AS `in_event_bounds_ind`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
        '`bounds_birth_floor_applied_ind` AS `bounds_birth_floor_applied_ind`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'reference_encounter_bounds': GoldRules(
        table='reference_encounter_bounds',
        select=REFERENCE_ENCOUNTER_BOUNDS_SELECT,
        mandatory=REFERENCE_ENCOUNTER_BOUNDS_MANDATORY_RULES,
        lifecycle={'join_keys': ['encounter_bounds_key'], 'support_columns': ['loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_encounter_bounds': {'columns': {'_source_row_id': 'Millennium encounter identifier. One row '
                                                                  'per encounter with some recorded clinical '
                                                                  'activity; encounters with none are absent '
                                                                  'from this table.',
                                                '_source_system': 'Value describing source system for the '
                                                                  'encounter bounds record. It is produced '
                                                                  'by the silver transformation and has no '
                                                                  'direct bronze-column lineage entry. '
                                                                  'Whitespace and source sentinel text are '
                                                                  'retained unless the pipeline explicitly '
                                                                  'normalizes them; null means no value was '
                                                                  'supplied.',
                                                '_source_table': 'Value describing source table for the '
                                                                 'encounter bounds record. It is produced by '
                                                                 'the silver transformation and has no '
                                                                 'direct bronze-column lineage entry. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                                'bounds_birth_floor_applied_ind': 'True when a bound before '
                                                                                  'birth or at the epoch was '
                                                                                  'nulled '
                                                                                  '(INSR_W1_BOUNDS_COHERENT_V1). '
                                                                                  '(bronze '
                                                                                  'BOUNDS_BIRTH_FLOOR_APPLIED_IND, '
                                                                                  'INSR W1). INSR_W2 W2.20.',
                                                'clinical_event_count': 'Number of qualifying clinical '
                                                                        'events, zero when the encounter has '
                                                                        'none.',
                                                'contemporaneous_event_count': 'Number of clinical events '
                                                                               'passing the contemporaneity '
                                                                               'filter, zero when the '
                                                                               'encounter has none.',
                                                'encounter_bounds_key': 'Deterministic SHA-256 key for the '
                                                                        'encounter-bounds row.',
                                                'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT; '
                                                                'joins to spine_encounter.encounter_id.',
                                                'event_first_datetime': 'Earliest plausible '
                                                                        'mill_clinical_event.CLINSIG_UPDT_DT_TM '
                                                                        'recorded against the encounter.',
                                                'event_last_datetime': 'Latest plausible '
                                                                       'mill_clinical_event.CLINSIG_UPDT_DT_TM '
                                                                       'recorded against the encounter.',
                                                'first_clinical_event_datetime': 'Earliest plausible '
                                                                                 'mill_clinical_event.EVENT_END_DT_TM '
                                                                                 'for the encounter, over '
                                                                                 'current rows only and '
                                                                                 'excluding cancelled, '
                                                                                 'in-error and not-done '
                                                                                 'results. EVENT_END_DT_TM '
                                                                                 'is clinical time and is '
                                                                                 'freely backdated by '
                                                                                 'retrospective '
                                                                                 'documentation, so this can '
                                                                                 'precede a known arrival by '
                                                                                 'years. It is evidence that '
                                                                                 'the encounter happened, '
                                                                                 'not a usable arrival.',
                                                'first_contemporaneous_event_datetime': 'Earliest clinical '
                                                                                        'event whose '
                                                                                        'clinical time is no '
                                                                                        'more than seven '
                                                                                        'days before its '
                                                                                        'documentation time '
                                                                                        '(EVENT_END_DT_TM at '
                                                                                        'or after '
                                                                                        'VALID_FROM_DT_TM '
                                                                                        'minus seven days), '
                                                                                        'which strips most '
                                                                                        'retrospectively '
                                                                                        'backdated entries. '
                                                                                        'This is the only '
                                                                                        'event-derived '
                                                                                        'timestamp permitted '
                                                                                        'to set a best-guess '
                                                                                        'arrival, and then '
                                                                                        'only at low '
                                                                                        'confidence: '
                                                                                        'measured against '
                                                                                        'known arrivals it '
                                                                                        'still precedes them '
                                                                                        '27.0 per cent of '
                                                                                        'the time, with a '
                                                                                        '1st percentile of '
                                                                                        'minus 48.9 days.',
                                                'first_order_datetime': 'Earliest plausible '
                                                                        'mill_orders.ORIG_ORDER_DT_TM for '
                                                                        'the encounter, excluding order '
                                                                        'templates and cancelled, '
                                                                        'voided-without-results and future '
                                                                        'orders. Orders sit a median 1.4 '
                                                                        'minutes after a known arrival but '
                                                                        'precede it 9.9 per cent of the '
                                                                        'time, because orders can be placed '
                                                                        'at referral.',
                                                'in_activity_bounds_ind': 'Millennium encounter identifier. '
                                                                          'One row per encounter with some '
                                                                          'recorded clinical activity; '
                                                                          'encounters with none are absent '
                                                                          'from this table.',
                                                'in_event_bounds_ind': 'Millennium encounter identifier.',
                                                'last_clinical_event_datetime': 'Latest plausible '
                                                                                'mill_clinical_event.EVENT_END_DT_TM '
                                                                                'for the encounter under the '
                                                                                'same filters.',
                                                'last_contemporaneous_event_datetime': 'Latest clinical '
                                                                                       'event under the same '
                                                                                       'contemporaneity '
                                                                                       'filter.',
                                                'last_order_datetime': 'Latest plausible '
                                                                       'mill_orders.ORIG_ORDER_DT_TM for the '
                                                                       'encounter under the same filters.',
                                                'last_ward_in_datetime': 'Latest plausible nurse-unit '
                                                                         'arrival recorded in '
                                                                         'mill_encntr_loc_hist.',
                                                'last_ward_out_datetime': 'Latest plausible nurse-unit '
                                                                          'departure recorded in '
                                                                          'mill_encntr_loc_hist. Location '
                                                                          'history mirrors the encounter '
                                                                          'DISCH_DT_TM to the minute 99.6 '
                                                                          'per cent of the time, so this '
                                                                          'contributes a departure only for '
                                                                          'the few thousand encounters that '
                                                                          'have neither DEPART_DT_TM nor '
                                                                          'DISCH_DT_TM.',
                                                'last_ward_still_open_ind': 'True when the most recent '
                                                                            'nurse-unit placement has no '
                                                                            'recorded departure after it, '
                                                                            'meaning the patient is still in '
                                                                            'situ. When true, '
                                                                            'LAST_WARD_OUT_DT_TM must not be '
                                                                            'read as a departure.',
                                                'loaded_at': 'Always NULL as a TIMESTAMP in the '
                                                             'encounter-bounds projection. No load clock is '
                                                             'supplied from map_encounter_activity_bounds or '
                                                             'map_encounter_event_bounds, and the pipeline '
                                                             'does not substitute event-boundary timestamps '
                                                             'or the Silver refresh time.',
                                                'order_count': 'Number of qualifying orders, zero when the '
                                                               'encounter has none.',
                                                'source_encounter_id': 'Millennium encounter identifier. One '
                                                                       'row per encounter with some recorded '
                                                                       'clinical activity; encounters with '
                                                                       'none are absent from this table.',
                                                'ward_move_count': 'Number of distinct nurse-unit placements '
                                                                   'recorded in mill_encntr_loc_hist, '
                                                                   'deduplicated on nurse unit and the '
                                                                   'placement arrival and departure pair. '
                                                                   'Zero when the encounter has none.',
                                                'ward_occupancy_minutes': 'Total minutes spent in nurse '
                                                                          'units, summed over placements '
                                                                          'that have both an arrival and a '
                                                                          'later departure. Null when no '
                                                                          'placement is closed. Overlapping '
                                                                          'placements are summed as '
                                                                          'recorded, so read this as '
                                                                          'evidence of inpatient duration '
                                                                          'rather than an exact '
                                                                          'bed-occupancy measure.'},
                                    'quantity_override': True,
                                    'table': 'Internal quality-controlled twin of '
                                             'reference_encounter_bounds: the Gold repairs, nulls and '
                                             'expectations are applied here and the published Gold MV reads '
                                             'this object. Materialized rather than temporary so the '
                                             'published MV can refresh incrementally.'},
 'reference_encounter_bounds': {'columns': {'_source_row_id': 'Millennium encounter identifier. One row per '
                                                              'encounter with some recorded clinical '
                                                              'activity; encounters with none are absent '
                                                              'from this table.',
                                            '_source_system': 'Value describing source system for the '
                                                              'encounter bounds record. It is produced by '
                                                              'the silver transformation and has no direct '
                                                              'bronze-column lineage entry. Whitespace and '
                                                              'source sentinel text are retained unless the '
                                                              'pipeline explicitly normalizes them; null '
                                                              'means no value was supplied.',
                                            '_source_table': 'Value describing source table for the '
                                                             'encounter bounds record. It is produced by the '
                                                             'silver transformation and has no direct '
                                                             'bronze-column lineage entry. Whitespace and '
                                                             'source sentinel text are retained unless the '
                                                             'pipeline explicitly normalizes them; null '
                                                             'means no value was supplied.',
                                            'bounds_birth_floor_applied_ind': 'True when a bound before '
                                                                              'birth or at the epoch was '
                                                                              'nulled '
                                                                              '(INSR_W1_BOUNDS_COHERENT_V1). '
                                                                              '(bronze '
                                                                              'BOUNDS_BIRTH_FLOOR_APPLIED_IND, '
                                                                              'INSR W1). INSR_W2 W2.20.',
                                            'clinical_event_count': 'Number of qualifying clinical events, '
                                                                    'zero when the encounter has none.',
                                            'contemporaneous_event_count': 'Number of clinical events '
                                                                           'passing the contemporaneity '
                                                                           'filter, zero when the encounter '
                                                                           'has none.',
                                            'encounter_bounds_key': 'Deterministic SHA-256 key for the '
                                                                    'encounter-bounds row.',
                                            'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT; joins to '
                                                            'spine_encounter.encounter_id.',
                                            'event_first_datetime': 'Earliest plausible '
                                                                    'mill_clinical_event.CLINSIG_UPDT_DT_TM '
                                                                    'recorded against the encounter.',
                                            'event_last_datetime': 'Latest plausible '
                                                                   'mill_clinical_event.CLINSIG_UPDT_DT_TM '
                                                                   'recorded against the encounter.',
                                            'first_clinical_event_datetime': 'Earliest plausible '
                                                                             'mill_clinical_event.EVENT_END_DT_TM '
                                                                             'for the encounter, over '
                                                                             'current rows only and '
                                                                             'excluding cancelled, in-error '
                                                                             'and not-done results. '
                                                                             'EVENT_END_DT_TM is clinical '
                                                                             'time and is freely backdated '
                                                                             'by retrospective '
                                                                             'documentation, so this can '
                                                                             'precede a known arrival by '
                                                                             'years. It is evidence that the '
                                                                             'encounter happened, not a '
                                                                             'usable arrival.',
                                            'first_contemporaneous_event_datetime': 'Earliest clinical event '
                                                                                    'whose clinical time is '
                                                                                    'no more than seven days '
                                                                                    'before its '
                                                                                    'documentation time '
                                                                                    '(EVENT_END_DT_TM at or '
                                                                                    'after VALID_FROM_DT_TM '
                                                                                    'minus seven days), '
                                                                                    'which strips most '
                                                                                    'retrospectively '
                                                                                    'backdated entries. This '
                                                                                    'is the only '
                                                                                    'event-derived timestamp '
                                                                                    'permitted to set a '
                                                                                    'best-guess arrival, and '
                                                                                    'then only at low '
                                                                                    'confidence: measured '
                                                                                    'against known arrivals '
                                                                                    'it still precedes them '
                                                                                    '27.0 per cent of the '
                                                                                    'time, with a 1st '
                                                                                    'percentile of minus '
                                                                                    '48.9 days.',
                                            'first_order_datetime': 'Earliest plausible '
                                                                    'mill_orders.ORIG_ORDER_DT_TM for the '
                                                                    'encounter, excluding order templates '
                                                                    'and cancelled, voided-without-results '
                                                                    'and future orders. Orders sit a median '
                                                                    '1.4 minutes after a known arrival but '
                                                                    'precede it 9.9 per cent of the time, '
                                                                    'because orders can be placed at '
                                                                    'referral.',
                                            'in_activity_bounds_ind': 'Millennium encounter identifier. One '
                                                                      'row per encounter with some recorded '
                                                                      'clinical activity; encounters with '
                                                                      'none are absent from this table.',
                                            'in_event_bounds_ind': 'Millennium encounter identifier.',
                                            'last_clinical_event_datetime': 'Latest plausible '
                                                                            'mill_clinical_event.EVENT_END_DT_TM '
                                                                            'for the encounter under the '
                                                                            'same filters.',
                                            'last_contemporaneous_event_datetime': 'Latest clinical event '
                                                                                   'under the same '
                                                                                   'contemporaneity filter.',
                                            'last_order_datetime': 'Latest plausible '
                                                                   'mill_orders.ORIG_ORDER_DT_TM for the '
                                                                   'encounter under the same filters.',
                                            'last_ward_in_datetime': 'Latest plausible nurse-unit arrival '
                                                                     'recorded in mill_encntr_loc_hist.',
                                            'last_ward_out_datetime': 'Latest plausible nurse-unit departure '
                                                                      'recorded in mill_encntr_loc_hist. '
                                                                      'Location history mirrors the '
                                                                      'encounter DISCH_DT_TM to the minute '
                                                                      '99.6 per cent of the time, so this '
                                                                      'contributes a departure only for the '
                                                                      'few thousand encounters that have '
                                                                      'neither DEPART_DT_TM nor DISCH_DT_TM.',
                                            'last_ward_still_open_ind': 'True when the most recent '
                                                                        'nurse-unit placement has no '
                                                                        'recorded departure after it, '
                                                                        'meaning the patient is still in '
                                                                        'situ. When true, '
                                                                        'LAST_WARD_OUT_DT_TM must not be '
                                                                        'read as a departure.',
                                            'loaded_at': 'Always NULL as a TIMESTAMP in the encounter-bounds '
                                                         'projection. No load clock is supplied from '
                                                         'map_encounter_activity_bounds or '
                                                         'map_encounter_event_bounds, and the pipeline does '
                                                         'not substitute event-boundary timestamps or the '
                                                         'Silver refresh time.',
                                            'order_count': 'Number of qualifying orders, zero when the '
                                                           'encounter has none.',
                                            'source_encounter_id': 'Millennium encounter identifier. One row '
                                                                   'per encounter with some recorded '
                                                                   'clinical activity; encounters with none '
                                                                   'are absent from this table.',
                                            'ward_move_count': 'Number of distinct nurse-unit placements '
                                                               'recorded in mill_encntr_loc_hist, '
                                                               'deduplicated on nurse unit and the placement '
                                                               'arrival and departure pair. Zero when the '
                                                               'encounter has none.',
                                            'ward_occupancy_minutes': 'Total minutes spent in nurse units, '
                                                                      'summed over placements that have both '
                                                                      'an arrival and a later departure. '
                                                                      'Null when no placement is closed. '
                                                                      'Overlapping placements are summed as '
                                                                      'recorded, so read this as evidence of '
                                                                      'inpatient duration rather than an '
                                                                      'exact bed-occupancy measure.'},
                                'quantity_override': False,
                                'table': 'One full-outer encounter activity and event bounds row. Gold QC '
                                         'twin of the silver product: 1 columns are repaired or nulled, 1 '
                                         'rule(s) drop rows, 0 check(s) are advisory. Each rule states its '
                                         'reason in the pipeline notebook, and Lakeflow expectation metrics '
                                         'report what every rule matched on each update. Coverage: Orders '
                                         'loaded in November 2019 run at about 2.5 times the previous months '
                                         'at source (a bulk order load), so first-order dates of encounters '
                                         'pile up in that month; complete-through month per release: '
                                         '6_mgmt.data_quality.release_finding (finding_code '
                                         'F2_complete_through).'}}
