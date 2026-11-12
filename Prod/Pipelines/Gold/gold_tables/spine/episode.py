"""Gold spine.episode: 3 dataset(s).

- datasets: _gold_episode_keys, _qc_spine_episode, spine_episode
- moved verbatim from: 1_gold_spine_and_reference:49, 1_gold_spine_and_reference:653, 1_gold_spine_and_reference:687, 1_gold_spine_and_reference:736, 1_gold_spine_and_reference:741, 1_gold_spine_and_reference:745, 1_gold_spine_and_reference:765

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, _src, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# GOLD_MV_SPLIT_V1: an internal MV (was a temporary view): every domain pipeline reads it for orphan nulling,
# and a temporary view cannot be read from another pipeline.
@dp.materialized_view(name=_n("gold_qc._episode_keys"), refresh_policy="auto")
def _gold_episode_keys():
    """Read distinct non-null Silver episode keys for Gold foreign-key checks."""
    return (
        spark.read.table(_src("spine_episode"))
        .select(F.col("episode_id").alias("_qc_episode_id"))
        .where(F.col("_qc_episode_id").isNotNull())
        .distinct()  # GOLD_MV_SPLIT_V1: dropDuplicates plans First, which blocks incremental refresh
    )


# ==== journey_spine.episode ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_EPISODE_SELECT = [
    '`episode_key` AS `episode_key`',
    '`episode_id` AS `episode_id`',
    '`person_id` AS `person_id`',
    '`subject_key` AS `subject_key`',
    '`episode_display` AS `episode_display`',
    "CASE WHEN UPPER(TRIM(CAST(`episode_type_code` AS STRING))) = '0' THEN NULL ELSE `episode_type_code` END AS `episode_type_code`",
    '`episode_type_display` AS `episode_type_display`',
    "CASE WHEN UPPER(TRIM(CAST(`status_code` AS STRING))) = '0' THEN NULL ELSE `status_code` END AS `status_code`",
    '`status_display` AS `status_display`',
    '`period_start` AS `period_start`',
    'CASE WHEN `period_start` IS NOT NULL AND `period_end` IS NOT NULL AND `period_start` > `period_end` THEN NULL ELSE `period_end` END AS `period_end`',
    '`breach_datetime` AS `breach_datetime`',
    '`pause_days` AS `pause_days`',
    "CASE WHEN UPPER(TRIM(CAST(`close_reason_code` AS STRING))) = '0' THEN NULL ELSE `close_reason_code` END AS `close_reason_code`",
    '`close_reason_display` AS `close_reason_display`',
    "CASE WHEN UPPER(TRIM(CAST(`service_category_code` AS STRING))) = '0' THEN NULL ELSE `service_category_code` END AS `service_category_code`",
    '`service_category_display` AS `service_category_display`',
    "CASE WHEN UPPER(TRIM(CAST(`referring_facility_code` AS STRING))) = '0' THEN NULL ELSE `referring_facility_code` END AS `referring_facility_code`",
    '`referring_facility_display` AS `referring_facility_display`',
    "CASE WHEN UPPER(TRIM(CAST(`contributor_system_code` AS STRING))) = '0' THEN NULL ELSE `contributor_system_code` END AS `contributor_system_code`",
    '`contributor_system_display` AS `contributor_system_display`',
    '`direct_encounter_key` AS `direct_encounter_key`',
    '`direct_encounter_id` AS `direct_encounter_id`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    # INSR_W3 W3.05: Gold: NULL where a pre-1901 date other than the named placeholders.
    "CASE WHEN (CAST(`record_status_effective_to` AS DATE) < DATE'1901-01-01' AND CAST(`record_status_effective_to` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01')) THEN NULL ELSE `record_status_effective_to` END AS `record_status_effective_to`",
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _spine_episode_metadata; source history stays on the main research table.
SPINE_EPISODE_ADVISORY_RULES = {
    # INSR_W4 W4.06 numeric_plausibility (P9_L5193): Negative pause days come from the bronze source (L5193; W2.11 sign fix NO-GO).
    'gold.spine.episode.pause_days.insr_f9_negative_pause':
        'NOT COALESCE((`pause_days` < 0), FALSE)',
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.spine.episode.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1 of 7,799,236
    # rows (1.28e-05%) when profiled on 2026-08-24.
    "gold.spine.episode.record_status_effective_to.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
SPINE_EPISODE_PERSON_SCOPE_RULES = {
    "gold.spine.episode.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
SPINE_EPISODE_ORPHAN_RULES = {
    "gold.spine.episode.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_episode"),
    refresh_policy="auto",
)
@dp.expect_all(SPINE_EPISODE_ADVISORY_RULES)
@dp.expect_all_or_drop(SPINE_EPISODE_PERSON_SCOPE_RULES)
@dp.expect_all(SPINE_EPISODE_ORPHAN_RULES)
def _gold_qc_spine_episode():
    """Quality-controlled twin of journey_spine.episode."""
    # 618 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_episode",
        SPINE_EPISODE_SELECT,
        fk_columns=["person_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_spine.episode"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_episode():
    """Contract-v2 public twin of spine_episode; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_episode")).selectExpr(
        '`episode_key` AS `episode_key`',
        '`episode_id` AS `episode_id`',
        '`person_id` AS `person_id`',
        '`subject_key` AS `subject_key`',
        '`episode_display` AS `episode_display`',
        '`episode_type_code` AS `episode_type_code`',
        '`episode_type_display` AS `episode_type_display`',
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`period_start` AS `period_start`',
        '`period_end` AS `period_end`',
        '`breach_datetime` AS `breach_datetime`',
        '`pause_days` AS `pause_days`',
        '`close_reason_code` AS `close_reason_code`',
        '`close_reason_display` AS `close_reason_display`',
        '`service_category_code` AS `service_category_code`',
        '`service_category_display` AS `service_category_display`',
        '`referring_facility_code` AS `referring_facility_code`',
        '`referring_facility_display` AS `referring_facility_display`',
        '`contributor_system_code` AS `contributor_system_code`',
        '`contributor_system_display` AS `contributor_system_display`',
        '`direct_encounter_key` AS `direct_encounter_key`',
        '`direct_encounter_id` AS `direct_encounter_id`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'spine_episode': GoldRules(
        table='spine_episode',
        select=SPINE_EPISODE_SELECT,
        advisory=SPINE_EPISODE_ADVISORY_RULES,
        person_scope=SPINE_EPISODE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['episode_key'],
         'support_columns': ['record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_gold_episode_keys': {'columns': {}, 'quantity_override': False, 'table': None},
 '_qc_spine_episode': {'columns': {'breach_datetime': 'Sentinel-cleaned episode breach timestamp when '
                                                      'supplied.',
                                   'close_reason_code': 'Source close-reason code.',
                                   'close_reason_display': 'Source close-reason display.',
                                   'contributor_system_code': 'Source contributor-system code.',
                                   'contributor_system_display': 'Source contributor-system display.',
                                   'direct_encounter_id': 'Millennium ENCNTR_ID for the agreement-gated '
                                                          'direct reference; evidence only.',
                                   'direct_encounter_key': 'Deterministic encounter key for the '
                                                           'agreement-gated direct reference; evidence only.',
                                   'episode_display': 'Source episode display name.',
                                   'episode_id': 'Millennium EPISODE_ID; primary key of this table.',
                                   'episode_key': 'Deterministic SHA-256 key minted from Millennium '
                                                  'EPISODE_ID.',
                                   'episode_type_code': 'Source episode type code.',
                                   'episode_type_display': 'Source episode type display.',
                                   'loaded_at': 'map_episode.ADC_UPDT carried unchanged from the '
                                                'contributing bronze episode row. This is ingestion '
                                                'provenance, not the Silver refresh time or the clinical '
                                                'episode start/end; a missing source timestamp remains null.',
                                   'pause_days': 'Source pause-day count.',
                                   'period_end': 'Sentinel-cleaned episode end timestamp when supplied.',
                                   'period_start': 'Sentinel-cleaned episode begin timestamp (bronze DQ '
                                                   'triplet).',
                                   'person_id': 'Millennium PERSON_ID as BIGINT.',
                                   'record_status': 'Derived map_episode row status: superseded when '
                                                    'SOURCE_PRESENT_IND is false or ACTIVE_IND is not 1; '
                                                    'otherwise active. A null SOURCE_PRESENT_IND defaults to '
                                                    'present and a null ACTIVE_IND defaults to 1. Source '
                                                    'absence maps to superseded here, not retracted; this is '
                                                    'not the clinical episode status code.',
                                   'record_status_effective_from': 'map_episode.ACTIVE_STATUS_DT_TM carried '
                                                                   'unchanged as the source active-status '
                                                                   'timestamp. No episode-start or '
                                                                   'ingestion-clock fallback is applied; a '
                                                                   'missing source value remains null.',
                                   'record_status_effective_to': 'For a superseded episode, the first '
                                                                 'available of '
                                                                 'map_episode.END_EFFECTIVE_DT_TM earlier '
                                                                 'than 2100-01-01, ACTIVE_STATUS_DT_TM, and '
                                                                 'ADC_UPDT. Null for an active episode or '
                                                                 'when every fallback is unavailable. This '
                                                                 'is a derived end boundary and may use '
                                                                 'ingestion time; it is not a Boolean active '
                                                                 'indicator.',
                                   'referring_facility_code': 'Source referring-facility code.',
                                   'referring_facility_display': 'Source referring-facility display.',
                                   'service_category_code': 'Source service-category code.',
                                   'service_category_display': 'Source service-category display.',
                                   'source_row_id': 'Native source row identifier.',
                                   'source_table': 'Registered bronze source table.',
                                   'status_code': 'Source episode status code.',
                                   'status_display': 'Source episode status display.',
                                   'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                  'identifier. Not salted, not secret; a stable join key '
                                                  'across feeds where person_id is unresolved.'},
                       'quantity_override': True,
                       'table': 'Internal quality-controlled twin of spine_episode: the Gold repairs, nulls '
                                'and expectations are applied here and the published Gold MV reads this '
                                'object. Materialized rather than temporary so the published MV can refresh '
                                'incrementally.'},
 'spine_episode': {'columns': {'breach_datetime': 'Sentinel-cleaned episode breach timestamp when supplied.',
                               'close_reason_code': 'Source close-reason code.',
                               'close_reason_display': 'Source close-reason display.',
                               'contributor_system_code': 'Source contributor-system code.',
                               'contributor_system_display': 'Source contributor-system display.',
                               'direct_encounter_id': 'Millennium ENCNTR_ID for the agreement-gated direct '
                                                      'reference; evidence only.',
                               'direct_encounter_key': 'Deterministic encounter key for the agreement-gated '
                                                       'direct reference; evidence only.',
                               'episode_display': 'Source episode display name.',
                               'episode_id': 'Millennium EPISODE_ID; primary key of this table.',
                               'episode_key': 'Deterministic SHA-256 key minted from Millennium EPISODE_ID.',
                               'episode_type_code': 'Source episode type code.',
                               'episode_type_display': 'Source episode type display.',
                               'loaded_at': 'map_episode.ADC_UPDT carried unchanged from the contributing '
                                            'bronze episode row. This is ingestion provenance, not the '
                                            'Silver refresh time or the clinical episode start/end; a '
                                            'missing source timestamp remains null.',
                               'pause_days': 'Source pause-day count.',
                               'period_end': 'Sentinel-cleaned episode end timestamp when supplied.',
                               'period_start': 'Sentinel-cleaned episode begin timestamp (bronze DQ '
                                               'triplet).',
                               'person_id': 'Millennium PERSON_ID as BIGINT.',
                               'record_status': 'Derived map_episode row status: superseded when '
                                                'SOURCE_PRESENT_IND is false or ACTIVE_IND is not 1; '
                                                'otherwise active. A null SOURCE_PRESENT_IND defaults to '
                                                'present and a null ACTIVE_IND defaults to 1. Source absence '
                                                'maps to superseded here, not retracted; this is not the '
                                                'clinical episode status code.',
                               'record_status_effective_from': 'map_episode.ACTIVE_STATUS_DT_TM carried '
                                                               'unchanged as the source active-status '
                                                               'timestamp. No episode-start or '
                                                               'ingestion-clock fallback is applied; a '
                                                               'missing source value remains null.',
                               'record_status_effective_to': 'For a superseded episode, the first available '
                                                             'of map_episode.END_EFFECTIVE_DT_TM earlier '
                                                             'than 2100-01-01, ACTIVE_STATUS_DT_TM, and '
                                                             'ADC_UPDT. Null for an active episode or when '
                                                             'every fallback is unavailable. This is a '
                                                             'derived end boundary and may use ingestion '
                                                             'time; it is not a Boolean active indicator.',
                               'referring_facility_code': 'Source referring-facility code.',
                               'referring_facility_display': 'Source referring-facility display.',
                               'service_category_code': 'Source service-category code.',
                               'service_category_display': 'Source service-category display.',
                               'source_row_id': 'Native source row identifier.',
                               'source_table': 'Registered bronze source table.',
                               'status_code': 'Source episode status code.',
                               'status_display': 'Source episode status display.',
                               'subject_key': 'Deterministic SHA-256 over the strongest source identifier. '
                                              'Not salted, not secret; a stable join key across feeds where '
                                              'person_id is unresolved.'},
                   'quantity_override': False,
                   'table': 'One Millennium episode container populated from the governed feed map_episode; '
                            'source-backed only, never heuristic. Gold QC twin of the silver product: 9 '
                            'columns are repaired or nulled, 1 check(s) are advisory. Each rule states its '
                            'reason in the pipeline notebook, and Lakeflow expectation metrics report what '
                            'every rule matched on each update.'}}
