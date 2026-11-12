"""Gold spine.episode_encounter: 2 dataset(s).

- datasets: _qc_spine_episode_encounter, spine_episode_encounter
- moved verbatim from: 1_gold_spine_and_reference:817, 1_gold_spine_and_reference:833, 1_gold_spine_and_reference:864, 1_gold_spine_and_reference:869, 1_gold_spine_and_reference:885

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_spine.episode_encounter ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_EPISODE_ENCOUNTER_SELECT = [
    '`episode_encounter_key` AS `episode_encounter_key`',
    '`episode_encounter_reltn_id` AS `episode_encounter_reltn_id`',
    '`episode_id` AS `episode_id`',
    '`encounter_id` AS `encounter_id`',
    '`relation_status_code` AS `relation_status_code`',
    '`source_duplicate_count` AS `source_duplicate_count`',
    '`valid_from` AS `valid_from`',
    '`valid_to` AS `valid_to`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _spine_episode_encounter_metadata; source history stays on the main research table.
SPINE_EPISODE_ENCOUNTER_MANDATORY_RULES = {
    # The flow nulls encounter_id when it names a parent the spine does not have, and this
    # rule then drops the row, because this table is nothing but the pair of keys, so a link
    # with an end missing links nothing. That was 71,166 orphaned rows plus 0 that already
    # had no encounter_id, out of 26,812,360.
    "gold.spine.episode_encounter.encounter_id.fk_containment":
        "`encounter_id` IS NOT NULL",

    # The flow nulls episode_id when it names a parent the spine does not have, and this
    # rule then drops the row, because this table is nothing but the pair of keys, so a link
    # with an end missing links nothing. That was 20,686 orphaned rows plus 0 that already
    # had no episode_id, out of 26,812,360.
    "gold.spine.episode_encounter.episode_id.fk_containment": "`episode_id` IS NOT NULL",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
SPINE_EPISODE_ENCOUNTER_ORPHAN_RULES = {
    "gold.spine.episode_encounter.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
    "gold.spine.episode_encounter.episode_id.orphan_nulled": "NOT COALESCE(`_qc_episode_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_episode_encounter"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(SPINE_EPISODE_ENCOUNTER_MANDATORY_RULES)
@dp.expect_all(SPINE_EPISODE_ENCOUNTER_ORPHAN_RULES)
def _gold_qc_spine_episode_encounter():
    """Quality-controlled twin of journey_spine.episode_encounter."""
    df = _qc(
        "spine_episode_encounter",
        SPINE_EPISODE_ENCOUNTER_SELECT,
        fk_columns=["encounter_id", "episode_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_spine.episode_encounter"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_episode_encounter():
    """Contract-v2 public twin of spine_episode_encounter; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_episode_encounter")).selectExpr(
        '`episode_encounter_key` AS `episode_encounter_key`',
        '`episode_encounter_reltn_id` AS `episode_encounter_reltn_id`',
        '`episode_id` AS `episode_id`',
        '`encounter_id` AS `encounter_id`',
        '`relation_status_code` AS `relation_status_code`',
        '`source_duplicate_count` AS `source_duplicate_count`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'spine_episode_encounter': GoldRules(
        table='spine_episode_encounter',
        select=SPINE_EPISODE_ENCOUNTER_SELECT,
        mandatory=SPINE_EPISODE_ENCOUNTER_MANDATORY_RULES,
        lifecycle={'join_keys': ['episode_encounter_key'],
         'support_columns': ['record_status', 'load_batch_id', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_spine_episode_encounter': {'columns': {'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                             'spine_encounter.encounter_id.',
                                             'episode_encounter_key': 'Deterministic SHA-256 relationship '
                                                                      'key.',
                                             'episode_encounter_reltn_id': 'Millennium '
                                                                           'EPISODE_ENCNTR_RELTN_ID; primary '
                                                                           'key of this table.',
                                             'episode_id': 'Millennium EPISODE_ID; joins to '
                                                           'spine_episode.episode_id.',
                                             'loaded_at': 'map_episode_encounter.ADC_UPDT carried unchanged '
                                                          'from the contributing bronze relationship row. '
                                                          'This is ingestion provenance, not the Silver '
                                                          'refresh time or relationship-validity time; a '
                                                          'missing source timestamp remains null.',
                                             'record_status': 'Derived map_episode_encounter '
                                                              'relationship-row status: superseded when '
                                                              'SOURCE_PRESENT_IND is false or ACTIVE_IND is '
                                                              'not 1; otherwise active. Null presence and '
                                                              'activity flags default to present and 1 '
                                                              'respectively. Source absence maps to '
                                                              'superseded, not retracted; this does not '
                                                              "describe the encounter's clinical status.",
                                             'relation_status_code': 'Source relation classification '
                                                                     'including the orphan marker for '
                                                                     'episode IDs absent from mill_episode.',
                                             'source_duplicate_count': 'Raw byte-identical duplicate rows '
                                                                       'collapsed into this canonical row by '
                                                                       'bronze.',
                                             'source_row_id': 'Native source row identifier.',
                                             'source_table': 'Registered bronze source table.',
                                             'valid_from': 'Membership effective-from timestamp.',
                                             'valid_to': 'Membership effective-to timestamp when supplied.'},
                                 'quantity_override': True,
                                 'table': 'Internal quality-controlled twin of spine_episode_encounter: the '
                                          'Gold repairs, nulls and expectations are applied here and the '
                                          'published Gold MV reads this object. Materialized rather than '
                                          'temporary so the published MV can refresh incrementally.'},
 'spine_episode_encounter': {'columns': {'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                         'spine_encounter.encounter_id.',
                                         'episode_encounter_key': 'Deterministic SHA-256 relationship key.',
                                         'episode_encounter_reltn_id': 'Millennium EPISODE_ENCNTR_RELTN_ID; '
                                                                       'primary key of this table.',
                                         'episode_id': 'Millennium EPISODE_ID; joins to '
                                                       'spine_episode.episode_id.',
                                         'loaded_at': 'map_episode_encounter.ADC_UPDT carried unchanged from '
                                                      'the contributing bronze relationship row. This is '
                                                      'ingestion provenance, not the Silver refresh time or '
                                                      'relationship-validity time; a missing source '
                                                      'timestamp remains null.',
                                         'record_status': 'Derived map_episode_encounter relationship-row '
                                                          'status: superseded when SOURCE_PRESENT_IND is '
                                                          'false or ACTIVE_IND is not 1; otherwise active. '
                                                          'Null presence and activity flags default to '
                                                          'present and 1 respectively. Source absence maps '
                                                          'to superseded, not retracted; this does not '
                                                          "describe the encounter's clinical status.",
                                         'relation_status_code': 'Source relation classification including '
                                                                 'the orphan marker for episode IDs absent '
                                                                 'from mill_episode.',
                                         'source_duplicate_count': 'Raw byte-identical duplicate rows '
                                                                   'collapsed into this canonical row by '
                                                                   'bronze.',
                                         'source_row_id': 'Native source row identifier.',
                                         'source_table': 'Registered bronze source table.',
                                         'valid_from': 'Membership effective-from timestamp.',
                                         'valid_to': 'Membership effective-to timestamp when supplied.'},
                             'quantity_override': False,
                             'table': 'N-to-M episode-to-encounter membership from the governed feed '
                                      'map_episode_encounter; multi-relation pairs preserved. Relations '
                                      'naming an episode or an encounter the spine does not have are '
                                      'dropped, so both keys resolve. Gold QC twin of the silver product: 2 '
                                      'columns are repaired or nulled, 2 rule(s) drop rows, 0 check(s) are '
                                      'advisory. Each rule states its reason in the pipeline notebook, and '
                                      'Lakeflow expectation metrics report what every rule matched on each '
                                      'update.'}}
