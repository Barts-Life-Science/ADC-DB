"""Gold spine.journey_link: 2 dataset(s).

- datasets: _qc_spine_journey_link, spine_journey_link
- moved verbatim from: 1_gold_spine_and_reference:1073, 1_gold_spine_and_reference:1092, 1_gold_spine_and_reference:1100, 1_gold_spine_and_reference:1150, 1_gold_spine_and_reference:1162

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_spine.journey_link ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_JOURNEY_LINK_SELECT = [
    '`journey_link_key` AS `journey_link_key`',
    '`journey_key` AS `journey_key`',
    '`member_type` AS `member_type`',
    '`encounter_id` AS `encounter_id`',
    '`patient_event_key` AS `patient_event_key`',
    '`role_code` AS `role_code`',
    "CASE WHEN CAST(`member_start_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`member_start_datetime` AS DATE)) < 9999 OR CAST(`member_start_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`member_start_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`member_start_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `member_start_datetime` END AS `member_start_datetime`",
    "CASE WHEN CAST(`member_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`member_end_datetime` AS DATE)) < 9999 THEN NULL ELSE `member_end_datetime` END AS `member_end_datetime`",
    '`construction_rule` AS `construction_rule`',
    '`construction_version` AS `construction_version`',
    '`source_system` AS `source_system`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _spine_journey_link_metadata; source history stays on the main research table.
SPINE_JOURNEY_LINK_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 646,884,263 rows of
    # 646,884,263 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(record_status = 'active')",
}


SPINE_JOURNEY_LINK_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_ordering (P9_L3913): Member end up to 24 h before its start reads as two clocks, not two events (L3913, J24): warn only.
    'gold.spine.journey_link.table.insr_f6_inversion_short':
        'NOT COALESCE((`member_end_datetime` < `member_start_datetime` AND `member_end_datetime` >= `member_start_datetime` - INTERVAL 24 HOURS), FALSE)',
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 55,712 of
    # 646,884,263 rows (0.00861%) when profiled on 2026-08-24.
    "gold.spine.journey_link.member_end_datetime.future_owner":
        "NOT COALESCE((CAST(`member_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 56,267 of
    # 646,884,263 rows (0.0087%) when profiled on 2026-08-24.
    "gold.spine.journey_link.member_start_datetime.future_owner":
        "NOT COALESCE((CAST(`member_start_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 646,884,263 at the profile.
    "gold.spine.journey_link.record_status.default_view_active": "record_status = 'active'",

    # Left as a warning because it fires on 15,386,217 of 646,884,263 rows (2.38%) when
    # profiled on 2026-08-24 -- at that rate the rule's assumption about what
    # member_start_datetime and member_end_datetime mean is the thing in doubt, not the
    # data. The inverted gaps are mostly minutes, which reads as two clocks rather than two
    # events in the wrong order.
    "gold.spine.journey_link.table.ordering_violation_member_start_datetime_member_end_datetime":
        "NOT COALESCE((`member_start_datetime` IS NOT NULL AND `member_end_datetime` IS NOT NULL AND `member_start_datetime` > `member_end_datetime`), FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_journey_link"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(SPINE_JOURNEY_LINK_MANDATORY_RULES)
@dp.expect_all(SPINE_JOURNEY_LINK_ADVISORY_RULES)
def _gold_qc_spine_journey_link():
    """Quality-controlled twin of journey_spine.journey_link."""
    df = _qc("spine_journey_link", SPINE_JOURNEY_LINK_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_spine.journey_link"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_journey_link():
    """Contract-v2 public twin of spine_journey_link; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_journey_link")).selectExpr(
        '`journey_link_key` AS `journey_link_key`',
        '`journey_key` AS `journey_key`',
        '`member_type` AS `member_type`',
        '`encounter_id` AS `encounter_id`',
        '`patient_event_key` AS `patient_event_key`',
        '`role_code` AS `role_code`',
        '`member_start_datetime` AS `member_start_datetime`',
        '`member_end_datetime` AS `member_end_datetime`',
        '`construction_rule` AS `construction_rule`',
        '`construction_version` AS `construction_version`',
        '`source_system` AS `source_system`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'spine_journey_link': GoldRules(
        table='spine_journey_link',
        select=SPINE_JOURNEY_LINK_SELECT,
        mandatory=SPINE_JOURNEY_LINK_MANDATORY_RULES,
        advisory=SPINE_JOURNEY_LINK_ADVISORY_RULES,
        lifecycle={'join_keys': ['journey_link_key'],
         'support_columns': ['record_status', 'load_batch_id', 'loaded_at']},
        value_rules={'gold.spine.journey_link.member_end_datetime.insr_f6_inversion_long': {'action': 'invalidate',
                                                                                'column': 'member_end_datetime',
                                                                                'companions': [],
                                                                                'match': '`member_end_datetime` '
                                                                                         '< '
                                                                                         '`member_start_datetime` '
                                                                                         '- INTERVAL 24 '
                                                                                         'HOURS',
                                                                                'table': 'spine_journey_link'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_spine_journey_link': {'columns': {'construction_rule': 'Governed overlap-construction rule; '
                                                             'episode_membership populated from the governed '
                                                             'episode feeds.',
                                        'construction_version': 'Governed overlap-construction version.',
                                        'encounter_id': 'Millennium ENCNTR_ID for encounter members.',
                                        'journey_key': 'Deterministic SHA-256 journey key.',
                                        'journey_link_key': 'Deterministic SHA-256 journey-membership key.',
                                        'loaded_at': 'Encounter overlap: greatest of constructed journey and '
                                                     'canonical encounter loaded_at. Patient-event overlap: '
                                                     'greatest of journey and internal event-index row '
                                                     'loaded_at, not a maximum over other events. Episode '
                                                     'membership: greatest of grouped anchor, '
                                                     'member-relation and member-encounter clocks across '
                                                     'contributing episode paths. The anchor clock includes '
                                                     'the journey/anchor encounter overlap clock and anchor '
                                                     'episode-encounter relation clock before collapse by '
                                                     'journey and episode; member relations are collapsed by '
                                                     'episode and encounter. Final grouping is by journey '
                                                     'and member encounter. No current Silver refresh time '
                                                     'is substituted.',
                                        'member_end_datetime': 'Linked member end timestamp used by the '
                                                               'constructor.',
                                        'member_start_datetime': 'Linked member start timestamp used by the '
                                                                 'constructor.',
                                        'member_type': 'Kind of linked member.',
                                        'patient_event_key': 'Deterministic SHA-256 patient-event key for '
                                                             'event members.',
                                        'record_status': 'Always active for all retained encounter-overlap, '
                                                         'patient-event-overlap and episode-membership '
                                                         "links, regardless of the linked journey or event's "
                                                         'status. Episode-membership eligibility separately '
                                                         'requires active episode-encounter relations and '
                                                         'excludes SOURCE_EPISODE_ABSENT; that filter does '
                                                         'not create superseded or retracted link labels.',
                                        'role_code': 'Controlled link role.',
                                        'source_row_id': 'Stable constructor source-row identifier.',
                                        'source_system': 'Constructor source-system identifier.',
                                        'source_table': 'Fully qualified configured journey-constructor '
                                                        'source table.'},
                            'quantity_override': True,
                            'table': 'Internal quality-controlled twin of spine_journey_link: the Gold '
                                     'repairs, nulls and expectations are applied here and the published '
                                     'Gold MV reads this object. Materialized rather than temporary so the '
                                     'published MV can refresh incrementally.'},
 'spine_journey_link': {'columns': {'construction_rule': 'Governed overlap-construction rule; '
                                                         'episode_membership populated from the governed '
                                                         'episode feeds.',
                                    'construction_version': 'Governed overlap-construction version.',
                                    'encounter_id': 'Millennium ENCNTR_ID for encounter members.',
                                    'journey_key': 'Deterministic SHA-256 journey key.',
                                    'journey_link_key': 'Deterministic SHA-256 journey-membership key.',
                                    'loaded_at': 'Encounter overlap: greatest of constructed journey and '
                                                 'canonical encounter loaded_at. Patient-event overlap: '
                                                 'greatest of journey and internal event-index row '
                                                 'loaded_at, not a maximum over other events. Episode '
                                                 'membership: greatest of grouped anchor, member-relation '
                                                 'and member-encounter clocks across contributing episode '
                                                 'paths. The anchor clock includes the journey/anchor '
                                                 'encounter overlap clock and anchor episode-encounter '
                                                 'relation clock before collapse by journey and episode; '
                                                 'member relations are collapsed by episode and encounter. '
                                                 'Final grouping is by journey and member encounter. No '
                                                 'current Silver refresh time is substituted.',
                                    'member_end_datetime': 'Linked member end timestamp used by the '
                                                           'constructor.',
                                    'member_start_datetime': 'Linked member start timestamp used by the '
                                                             'constructor.',
                                    'member_type': 'Kind of linked member.',
                                    'patient_event_key': 'Deterministic SHA-256 patient-event key for event '
                                                         'members.',
                                    'record_status': 'Always active for all retained encounter-overlap, '
                                                     'patient-event-overlap and episode-membership links, '
                                                     "regardless of the linked journey or event's status. "
                                                     'Episode-membership eligibility separately requires '
                                                     'active episode-encounter relations and excludes '
                                                     'SOURCE_EPISODE_ABSENT; that filter does not create '
                                                     'superseded or retracted link labels.',
                                    'role_code': 'Controlled link role.',
                                    'source_row_id': 'Stable constructor source-row identifier.',
                                    'source_system': 'Constructor source-system identifier.',
                                    'source_table': 'Fully qualified configured journey-constructor source '
                                                    'table.'},
                        'quantity_override': False,
                        'table': 'One deterministic journey-to-encounter or journey-to-event overlap link. '
                                 'Gold QC twin of the silver product: 2 columns are repaired or nulled, 1 '
                                 'rule(s) drop rows, 4 check(s) are advisory. Each rule states its reason in '
                                 'the pipeline notebook, and Lakeflow expectation metrics report what every '
                                 'rule matched on each update.'}}
