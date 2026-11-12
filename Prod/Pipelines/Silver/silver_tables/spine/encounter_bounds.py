"""Silver spine.encounter_bounds: 1 dataset(s).

- datasets: reference_encounter_bounds
- moved verbatim from: 1_silver_spine_and_reference:6519, 1_silver_spine_and_reference:6521, 1_silver_spine_and_reference:6551, 1_silver_spine_and_reference:6554, 1_silver_spine_and_reference:6559, 1_silver_spine_and_reference:6595

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / encounter bounds
SRC_ENC_ACTIVITY_BOUNDS = "4_prod.bronze.map_encounter_activity_bounds"


SRC_ENC_EVENT_BOUNDS = "4_prod.bronze.map_encounter_event_bounds"


ENCOUNTER_BOUNDS_LIFECYCLE_FIELDS = [
]


ENCOUNTER_BOUNDS_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_encounter_bounds():
    # Assemble encounter bounds rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    a = read_source(SRC_ENC_ACTIVITY_BOUNDS).alias("a")
    b = read_source(SRC_ENC_EVENT_BOUNDS).alias("b")
    d = a.join(b, a.ENCNTR_ID == b.ENCNTR_ID, "full")
    source_id = F.coalesce(a.ENCNTR_ID, b.ENCNTR_ID)
    # contract v2: rename the bounds hash as a key and publish native encounter_id
    return d.select(
        stable_id("encounter_bounds:mill", source_id).alias("encounter_bounds_key"),
        source_id.cast("bigint").alias("encounter_id"),
        source_id.cast("string").alias("source_encounter_id"),
        a.FIRST_CLINICAL_EVENT_DT_TM.alias("first_clinical_event_datetime"),
        a.LAST_CLINICAL_EVENT_DT_TM.alias("last_clinical_event_datetime"),
        a.CLINICAL_EVENT_COUNT.alias("clinical_event_count"),
        a.FIRST_CONTEMPORANEOUS_EVENT_DT_TM.alias("first_contemporaneous_event_datetime"),
        a.LAST_CONTEMPORANEOUS_EVENT_DT_TM.alias("last_contemporaneous_event_datetime"),
        a.CONTEMPORANEOUS_EVENT_COUNT.alias("contemporaneous_event_count"),
        a.FIRST_ORDER_DT_TM.alias("first_order_datetime"),
        a.LAST_ORDER_DT_TM.alias("last_order_datetime"), a.ORDER_COUNT.alias("order_count"),
        a.WARD_MOVE_COUNT.alias("ward_move_count"),
        a.WARD_OCCUPANCY_MINUTES.alias("ward_occupancy_minutes"),
        a.LAST_WARD_IN_DT_TM.alias("last_ward_in_datetime"),
        a.BOUNDS_BIRTH_FLOOR_APPLIED_IND.alias("bounds_birth_floor_applied_ind"),  # INSR_W2 W2.20
        a.LAST_WARD_OUT_DT_TM.alias("last_ward_out_datetime"),
        a.LAST_WARD_STILL_OPEN_IND.alias("last_ward_still_open_ind"),
        b.FIRST_EVENT_DT_TM.alias("event_first_datetime"),
        b.LAST_EVENT_DT_TM.alias("event_last_datetime"),
        a.ENCNTR_ID.isNotNull().alias("in_activity_bounds_ind"),
        b.ENCNTR_ID.isNotNull().alias("in_event_bounds_ind"),
        F.lit(None).cast("timestamp").alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.concat_ws("|", F.lit(SRC_ENC_ACTIVITY_BOUNDS), F.lit(SRC_ENC_EVENT_BOUNDS))
         .alias("_source_table"), source_id.cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.encounter_bounds"),
    cluster_by=["encounter_id"], refresh_policy="auto",
)
def encounter_bounds():
    # Build the declared dataset: Full-outer encounter activity/event bounds with source-arm
    # presence flags.
    return _lifecycle_source_encounter_bounds().drop(*ENCOUNTER_BOUNDS_LIFECYCLE_FIELDS, *ENCOUNTER_BOUNDS_RETIRED_COLUMNS)


VALUE_POLICIES = {
    'reference_encounter_bounds': ValuePolicy(table='reference_encounter_bounds', enabled=True),
}


WRAP_POLICY = {'reference_encounter_bounds'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_encounter_bounds': {'columns': {'_source_row_id': 'Millennium encounter identifier. One row per '
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
                                'table': 'Full-outer encounter activity/event bounds with source-arm '
                                         'presence flags.'}}
