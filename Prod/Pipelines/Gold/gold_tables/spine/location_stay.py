"""Gold spine.location_stay: 2 dataset(s).

- datasets: _qc_spine_location_stay, spine_location_stay
- moved verbatim from: 1_gold_spine_and_reference:1327, 1_gold_spine_and_reference:1375, 1_gold_spine_and_reference:1441, 1_gold_spine_and_reference:1446, 1_gold_spine_and_reference:1451, 1_gold_spine_and_reference:1474

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_spine.location_stay ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_LOCATION_STAY_SELECT = [
    '`location_stay_key` AS `location_stay_key`',
    '`encounter_id` AS `encounter_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`location_code` AS `location_code`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN CAST(`period_start` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`period_start` AS DATE)) < 9999 OR YEAR(CAST(`period_start` AS DATE)) >= 2100 THEN NULL ELSE `period_start` END AS `period_start`",
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN (CAST(`period_end` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`period_end` AS DATE)) < 9999) OR CAST(`period_end` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`period_end` AS DATE)) < 9999 THEN NULL ELSE `period_end` END AS `period_end`",
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN CAST(`occupancy_start` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`occupancy_start` AS DATE)) < 9999 OR YEAR(CAST(`occupancy_start` AS DATE)) >= 2100 THEN NULL ELSE `occupancy_start` END AS `occupancy_start`",  # GOLD_COLUMN_PARITY_V1
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN (CAST(`occupancy_end` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`occupancy_end` AS DATE)) < 9999) OR CAST(`occupancy_end` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`occupancy_end` AS DATE)) < 9999 THEN NULL ELSE `occupancy_end` END AS `occupancy_end`",  # GOLD_COLUMN_PARITY_V1
    '`occupancy_start_basis` AS `occupancy_start_basis`',  # GOLD_COLUMN_PARITY_V1
    '`occupancy_end_basis` AS `occupancy_end_basis`',  # GOLD_COLUMN_PARITY_V1
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN `occupancy_start` IS NOT NULL AND (CASE WHEN CAST(`occupancy_start` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`occupancy_start` AS DATE)) < 9999 OR YEAR(CAST(`occupancy_start` AS DATE)) >= 2100 THEN NULL ELSE `occupancy_start` END) IS NULL THEN NULL ELSE `occupancy_start_local` END AS `occupancy_start_local`",  # GOLD_COLUMN_PARITY_V1
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN `occupancy_end` IS NOT NULL AND (CASE WHEN (CAST(`occupancy_end` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`occupancy_end` AS DATE)) < 9999) OR CAST(`occupancy_end` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`occupancy_end` AS DATE)) < 9999 THEN NULL ELSE `occupancy_end` END) IS NULL THEN NULL ELSE `occupancy_end_local` END AS `occupancy_end_local`",  # GOLD_COLUMN_PARITY_V1
    "CASE WHEN UPPER(TRIM(CAST(`nurse_unit_code` AS STRING))) = '0' THEN NULL ELSE `nurse_unit_code` END AS `nurse_unit_code`",
    '`nurse_unit_display` AS `nurse_unit_display`',
    "CASE WHEN UPPER(TRIM(CAST(`building_code` AS STRING))) = '0' THEN NULL ELSE `building_code` END AS `building_code`",
    '`building_display` AS `building_display`',
    "CASE WHEN UPPER(TRIM(CAST(`facility_code` AS STRING))) = '0' THEN NULL ELSE `facility_code` END AS `facility_code`",
    '`facility_display` AS `facility_display`',
    "CASE WHEN UPPER(TRIM(CAST(`room_code` AS STRING))) = '0' THEN NULL ELSE `room_code` END AS `room_code`",
    '`room_display` AS `room_display`',
    "CASE WHEN UPPER(TRIM(CAST(`bed_code` AS STRING))) = '0' THEN NULL ELSE `bed_code` END AS `bed_code`",
    '`bed_display` AS `bed_display`',
    "CASE WHEN UPPER(TRIM(CAST(`service_code` AS STRING))) = '0' THEN NULL ELSE `service_code` END AS `service_code`",
    '`service_display` AS `service_display`',
    "CASE WHEN UPPER(TRIM(CAST(`transfer_reason_code` AS STRING))) = '0' THEN NULL ELSE `transfer_reason_code` END AS `transfer_reason_code`",
    '`transfer_reason_display` AS `transfer_reason_display`',
    '`source_history_row_count` AS `source_history_row_count`',
    '`first_history_event_sequence` AS `first_history_event_sequence`',
    '`last_history_event_sequence` AS `last_history_event_sequence`',
    "CASE WHEN UPPER(TRIM(CAST(`confidentiality_code` AS STRING))) = '0' THEN NULL ELSE `confidentiality_code` END AS `confidentiality_code`",
    '`vip_ind` AS `vip_ind`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _spine_location_stay_metadata; source history stays on the main research table.
SPINE_LOCATION_STAY_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1 of 65,583,474
    # rows (1.52e-06%) when profiled on 2026-08-24.
    "gold.spine.location_stay.period_end.future_owner":
        "NOT COALESCE((CAST(`period_end` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1 of 65,583,474
    # rows (1.52e-06%) when profiled on 2026-08-24.
    "gold.spine.location_stay.period_start.future_owner":
        "NOT COALESCE((CAST(`period_start` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1 of 65,583,474
    # rows (1.52e-06%) when profiled on 2026-08-24.
    "gold.spine.location_stay.record_status_effective_to.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
SPINE_LOCATION_STAY_PERSON_SCOPE_RULES = {
    "gold.spine.location_stay.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
SPINE_LOCATION_STAY_ORPHAN_RULES = {
    "gold.spine.location_stay.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
    "gold.spine.location_stay.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_location_stay"),
    refresh_policy="auto",
)
@dp.expect_all(SPINE_LOCATION_STAY_ADVISORY_RULES)
@dp.expect_all_or_drop(SPINE_LOCATION_STAY_PERSON_SCOPE_RULES)
@dp.expect_all(SPINE_LOCATION_STAY_ORPHAN_RULES)
def _gold_qc_spine_location_stay():
    """Quality-controlled twin of journey_spine.location_stay."""
    # 9,473 rows point at a encounter_id the spine does not have. The pointer is nulled so
    # it cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    # 24 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_location_stay",
        SPINE_LOCATION_STAY_SELECT,
        fk_columns=["encounter_id", "person_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_spine.location_stay"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_location_stay():
    """Contract-v2 public twin of spine_location_stay; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_location_stay")).selectExpr(
        '`location_stay_key` AS `location_stay_key`',
        '`encounter_id` AS `encounter_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`location_code` AS `location_code`',
        '`period_start` AS `period_start`',
        '`period_end` AS `period_end`',
        '`occupancy_start` AS `occupancy_start`',  # GOLD_COLUMN_PARITY_V1
        '`occupancy_end` AS `occupancy_end`',  # GOLD_COLUMN_PARITY_V1
        '`occupancy_start_basis` AS `occupancy_start_basis`',  # GOLD_COLUMN_PARITY_V1
        '`occupancy_end_basis` AS `occupancy_end_basis`',  # GOLD_COLUMN_PARITY_V1
        '`occupancy_start_local` AS `occupancy_start_local`',  # GOLD_COLUMN_PARITY_V1
        '`occupancy_end_local` AS `occupancy_end_local`',  # GOLD_COLUMN_PARITY_V1
        '`nurse_unit_code` AS `nurse_unit_code`',
        '`nurse_unit_display` AS `nurse_unit_display`',
        '`building_code` AS `building_code`',
        '`building_display` AS `building_display`',
        '`facility_code` AS `facility_code`',
        '`facility_display` AS `facility_display`',
        '`room_code` AS `room_code`',
        '`room_display` AS `room_display`',
        '`bed_code` AS `bed_code`',
        '`bed_display` AS `bed_display`',
        '`service_code` AS `service_code`',
        '`service_display` AS `service_display`',
        '`transfer_reason_code` AS `transfer_reason_code`',
        '`transfer_reason_display` AS `transfer_reason_display`',
        '`source_history_row_count` AS `source_history_row_count`',
        '`first_history_event_sequence` AS `first_history_event_sequence`',
        '`last_history_event_sequence` AS `last_history_event_sequence`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'spine_location_stay': GoldRules(
        table='spine_location_stay',
        select=SPINE_LOCATION_STAY_SELECT,
        advisory=SPINE_LOCATION_STAY_ADVISORY_RULES,
        person_scope=SPINE_LOCATION_STAY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['location_stay_key'],
         'support_columns': ['record_status', 'record_status_effective_to', 'load_batch_id', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_spine_location_stay': {'columns': {'bed_code': 'Historical bed code.',
                                         'bed_display': 'Historical bed display.',
                                         'building_code': 'Historical building code.',
                                         'building_display': 'Historical building display.',
                                         'confidentiality_code': 'Source confidentiality code.',
                                         'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                         'spine_encounter.encounter_id.',
                                         'facility_code': 'Historical facility code.',
                                         'facility_display': 'Historical facility display.',
                                         'first_history_event_sequence': 'First source history sequence in '
                                                                         'the stop.',
                                         'last_history_event_sequence': 'Last source history sequence in the '
                                                                        'stop.',
                                         'loaded_at': 'Maximum map_patient_journey.ADC_UPDT within the '
                                                      'ENCNTR_ID and stop-discriminator group. The '
                                                      'discriminator uses LOCATION_STOP_SEQUENCE when '
                                                      'present, otherwise ENCNTR_LOC_HIST_ID. This is '
                                                      'contributing ingestion provenance, not the Silver '
                                                      'refresh time; null when all contributing ADC_UPDT '
                                                      'values are null.',
                                         'location_code': 'Native Millennium location code resolved against '
                                                          'reference_location.',
                                         'location_stay_key': 'Deterministic SHA-256 modelled location-stop '
                                                              'key.',
                                         'nurse_unit_code': 'Historical nurse-unit code.',
                                         'nurse_unit_display': 'Historical nurse-unit display.',
                                         'occupancy_end': 'Physical occupancy end of the location stop, '
                                                          'taken from the stop end, and null where that end '
                                                          'is an administrative or fixed-window encounter '
                                                          'close or precedes the occupancy start. Gold: the '
                                                          'period_end sentinel rule applies (2100-12-31 and '
                                                          'post-2100 typo years become NULL).',
                                         'occupancy_end_basis': 'Rule label identifying how occupancy_end '
                                                                'was derived or why it is null.',
                                         'occupancy_end_local': 'Occupancy end converted from UTC to '
                                                                'Europe/London wall-clock time, for use when '
                                                                'deriving local calendar dates and clock '
                                                                'times. Gold: NULL wherever Gold nulls '
                                                                'occupancy_end.',
                                         'occupancy_start': 'Physical occupancy start of the location stop, '
                                                            'taken from the location arrival or else the '
                                                            'stop start clamped to the best encounter '
                                                            'arrival, and null where there is no attendance '
                                                            'evidence or the stop ended before arrival. '
                                                            'Gold: the period_start sentinel rule applies '
                                                            '(2100-12-31 and post-2100 typo years become '
                                                            'NULL).',
                                         'occupancy_start_basis': 'Rule label identifying how '
                                                                  'occupancy_start was derived or why it is '
                                                                  'null.',
                                         'occupancy_start_local': 'Occupancy start converted from UTC to '
                                                                  'Europe/London wall-clock time, for use '
                                                                  'when deriving local calendar dates and '
                                                                  'clock times. Gold: NULL wherever Gold '
                                                                  'nulls occupancy_start.',
                                         'period_end': 'Latest `LOCATION_STOP_END_DT_TM` across the source '
                                                       'history rows grouped into this encounter location '
                                                       'stop; the grouped source timestamp is retained '
                                                       'without sentinel removal.',
                                         'period_start': 'Earliest `LOCATION_STOP_START_DT_TM` across the '
                                                         'source history rows grouped into this encounter '
                                                         'location stop.',
                                         'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                         'record_status': 'Literal active: map_patient_journey carries no '
                                                          'lifecycle flag that retracts or supersedes a '
                                                          'location stop, so every published stop is the '
                                                          'current record of that stop. Whether the patient '
                                                          'has left is period_end / occupancy_end, not this '
                                                          'status.',
                                         'record_status_effective_to': 'Always null: a location stop has no '
                                                                       "record-lifecycle end. The stop's own "
                                                                       'end is period_end.',
                                         'room_code': 'Historical room code.',
                                         'room_display': 'Historical room display.',
                                         'service_code': 'Service code during the stop.',
                                         'service_display': 'Service display during the stop.',
                                         'source_history_row_count': 'Number of history rows grouped into '
                                                                     'the stop.',
                                         'source_row_id': 'Deterministic grouped source-row identity.',
                                         'source_table': 'Fully qualified bronze source table.',
                                         'subject_id_system': 'Identifier system used for subject_key.',
                                         'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                        'identifier. Not salted, not secret; a stable join '
                                                        'key across feeds where person_id is unresolved.',
                                         'transfer_reason_code': 'Source transfer-reason code.',
                                         'transfer_reason_display': 'Source transfer-reason display.',
                                         'vip_ind': 'Source VIP indicator.'},
                             'quantity_override': True,
                             'table': 'Internal quality-controlled twin of spine_location_stay: the Gold '
                                      'repairs, nulls and expectations are applied here and the published '
                                      'Gold MV reads this object. Materialized rather than temporary so the '
                                      'published MV can refresh incrementally.'},
 'spine_location_stay': {'columns': {'bed_code': 'Historical bed code.',
                                     'bed_display': 'Historical bed display.',
                                     'building_code': 'Historical building code.',
                                     'building_display': 'Historical building display.',
                                     'confidentiality_code': 'Source confidentiality code.',
                                     'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                     'spine_encounter.encounter_id.',
                                     'facility_code': 'Historical facility code.',
                                     'facility_display': 'Historical facility display.',
                                     'first_history_event_sequence': 'First source history sequence in the '
                                                                     'stop.',
                                     'last_history_event_sequence': 'Last source history sequence in the '
                                                                    'stop.',
                                     'loaded_at': 'Maximum map_patient_journey.ADC_UPDT within the ENCNTR_ID '
                                                  'and stop-discriminator group. The discriminator uses '
                                                  'LOCATION_STOP_SEQUENCE when present, otherwise '
                                                  'ENCNTR_LOC_HIST_ID. This is contributing ingestion '
                                                  'provenance, not the Silver refresh time; null when all '
                                                  'contributing ADC_UPDT values are null.',
                                     'location_code': 'Native Millennium location code resolved against '
                                                      'reference_location.',
                                     'location_stay_key': 'Deterministic SHA-256 modelled location-stop key.',
                                     'nurse_unit_code': 'Historical nurse-unit code.',
                                     'nurse_unit_display': 'Historical nurse-unit display.',
                                     'occupancy_end': 'Physical occupancy end of the location stop, taken '
                                                      'from the stop end, and null where that end is an '
                                                      'administrative or fixed-window encounter close or '
                                                      'precedes the occupancy start. Gold: the period_end '
                                                      'sentinel rule applies (2100-12-31 and post-2100 typo '
                                                      'years become NULL).',
                                     'occupancy_end_basis': 'Rule label identifying how occupancy_end was '
                                                            'derived or why it is null.',
                                     'occupancy_end_local': 'Occupancy end converted from UTC to '
                                                            'Europe/London wall-clock time, for use when '
                                                            'deriving local calendar dates and clock times. '
                                                            'Gold: NULL wherever Gold nulls occupancy_end.',
                                     'occupancy_start': 'Physical occupancy start of the location stop, '
                                                        'taken from the location arrival or else the stop '
                                                        'start clamped to the best encounter arrival, and '
                                                        'null where there is no attendance evidence or the '
                                                        'stop ended before arrival. Gold: the period_start '
                                                        'sentinel rule applies (2100-12-31 and post-2100 '
                                                        'typo years become NULL).',
                                     'occupancy_start_basis': 'Rule label identifying how occupancy_start '
                                                              'was derived or why it is null.',
                                     'occupancy_start_local': 'Occupancy start converted from UTC to '
                                                              'Europe/London wall-clock time, for use when '
                                                              'deriving local calendar dates and clock '
                                                              'times. Gold: NULL wherever Gold nulls '
                                                              'occupancy_start.',
                                     'period_end': 'Latest `LOCATION_STOP_END_DT_TM` across the source '
                                                   'history rows grouped into this encounter location stop; '
                                                   'the grouped source timestamp is retained without '
                                                   'sentinel removal.',
                                     'period_start': 'Earliest `LOCATION_STOP_START_DT_TM` across the source '
                                                     'history rows grouped into this encounter location '
                                                     'stop.',
                                     'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                     'record_status': 'Literal active: map_patient_journey carries no '
                                                      'lifecycle flag that retracts or supersedes a location '
                                                      'stop, so every published stop is the current record '
                                                      'of that stop. Whether the patient has left is '
                                                      'period_end / occupancy_end, not this status.',
                                     'record_status_effective_to': 'Always null: a location stop has no '
                                                                   "record-lifecycle end. The stop's own end "
                                                                   'is period_end.',
                                     'room_code': 'Historical room code.',
                                     'room_display': 'Historical room display.',
                                     'service_code': 'Service code during the stop.',
                                     'service_display': 'Service display during the stop.',
                                     'source_history_row_count': 'Number of history rows grouped into the '
                                                                 'stop.',
                                     'source_row_id': 'Deterministic grouped source-row identity.',
                                     'source_table': 'Fully qualified bronze source table.',
                                     'subject_id_system': 'Identifier system used for subject_key.',
                                     'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                    'identifier. Not salted, not secret; a stable join key '
                                                    'across feeds where person_id is unresolved.',
                                     'transfer_reason_code': 'Source transfer-reason code.',
                                     'transfer_reason_display': 'Source transfer-reason display.',
                                     'vip_ind': 'Source VIP indicator.'},
                         'quantity_override': False,
                         'table': 'One grouped physical location stop per encounter, retaining raw '
                                  'historical location evidence even when the current dimension cannot '
                                  'resolve it. Gold QC twin of the silver product: 12 columns are repaired '
                                  'or nulled, 3 check(s) are advisory. Each rule states its reason in the '
                                  'pipeline notebook, and Lakeflow expectation metrics report what every '
                                  'rule matched on each update.'}}
