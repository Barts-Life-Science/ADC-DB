"""Gold spine.encounter_identifier: 2 dataset(s).

- datasets: _qc_spine_encounter_identifier, spine_encounter_identifier
- moved verbatim from: 1_gold_spine_and_reference:551, 1_gold_spine_and_reference:569, 1_gold_spine_and_reference:596, 1_gold_spine_and_reference:600, 1_gold_spine_and_reference:616

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_spine.encounter_identifier ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_ENCOUNTER_IDENTIFIER_SELECT = [
    '`encounter_identifier_key` AS `encounter_identifier_key`',
    '`source_identifier_id` AS `source_identifier_id`',
    '`encounter_id` AS `encounter_id`',
    '`identifier_system` AS `identifier_system`',
    '`identifier_type_code` AS `identifier_type_code`',
    '`identifier_value` AS `identifier_value`',
    '`status` AS `status`',
    '`current_ind` AS `current_ind`',
    '`multi_active_ind` AS `multi_active_ind`',
    "CASE WHEN CAST(`valid_from` AS DATE) = DATE'1800-01-01' THEN NULL ELSE `valid_from` END AS `valid_from`",
    '`valid_to` AS `valid_to`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _spine_encounter_identifier_metadata; source history stays on the main research table.
SPINE_ENCOUNTER_IDENTIFIER_MANDATORY_RULES = {
    # The flow nulls encounter_id when it names a parent the spine does not have, and this
    # rule then drops the row, because an identifier for an encounter that is not in the
    # spine identifies nothing a researcher can join to. That was 318,195 orphaned rows plus
    # 0 that already had no encounter_id, out of 142,054,710.
    "gold.spine.encounter_identifier.encounter_id.fk_containment":
        "`encounter_id` IS NOT NULL",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
SPINE_ENCOUNTER_IDENTIFIER_ORPHAN_RULES = {
    "gold.spine.encounter_identifier.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_encounter_identifier"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(SPINE_ENCOUNTER_IDENTIFIER_MANDATORY_RULES)
@dp.expect_all(SPINE_ENCOUNTER_IDENTIFIER_ORPHAN_RULES)
def _gold_qc_spine_encounter_identifier():
    """Quality-controlled twin of journey_spine.encounter_identifier."""
    df = _qc(
        "spine_encounter_identifier",
        SPINE_ENCOUNTER_IDENTIFIER_SELECT,
        fk_columns=["encounter_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_spine.encounter_identifier"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_encounter_identifier():
    """Contract-v2 public twin of spine_encounter_identifier; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_encounter_identifier")).selectExpr(
        '`encounter_identifier_key` AS `encounter_identifier_key`',
        '`source_identifier_id` AS `source_identifier_id`',
        '`encounter_id` AS `encounter_id`',
        '`identifier_system` AS `identifier_system`',
        '`identifier_type_code` AS `identifier_type_code`',
        '`identifier_value` AS `identifier_value`',
        '`status` AS `status`',
        '`current_ind` AS `current_ind`',
        '`multi_active_ind` AS `multi_active_ind`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'spine_encounter_identifier': GoldRules(
        table='spine_encounter_identifier',
        select=SPINE_ENCOUNTER_IDENTIFIER_SELECT,
        mandatory=SPINE_ENCOUNTER_IDENTIFIER_MANDATORY_RULES,
        lifecycle={'join_keys': ['encounter_identifier_key'], 'support_columns': ['load_batch_id', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_spine_encounter_identifier': {'columns': {'current_ind': 'Bronze current-alias indicator.',
                                                'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                                'spine_encounter.encounter_id.',
                                                'encounter_identifier_key': 'Deterministic SHA-256 key '
                                                                            'minted from the source alias '
                                                                            'row.',
                                                'identifier_system': 'Type-scoped identifier system URI '
                                                                     '(urn:cerner:encntr_alias:<normalized '
                                                                     'alias type>) — 23.17M values live '
                                                                     'under multiple alias types',
                                                'identifier_type_code': 'Source alias type verbatim.',
                                                'identifier_value': 'Identifier value; published and '
                                                                    'IG-governed at serve time',
                                                'loaded_at': 'map_encounter_identifier.PIPELINE_UPDT_DT_TM '
                                                             'carried unchanged from the contributing bronze '
                                                             'alias row. This records bronze processing '
                                                             'provenance, not identifier validity or the '
                                                             'Silver refresh time; a missing source '
                                                             'timestamp remains null.',
                                                'multi_active_ind': 'Bronze multiple-active-aliases '
                                                                    'indicator for this encounter and type.',
                                                'source_identifier_id': 'Native map_encounter_identifier '
                                                                        'SOURCE_PK rendered as text.',
                                                'source_row_id': 'Native source row identifier.',
                                                'source_table': 'Registered bronze source table.',
                                                'status': 'Alias lifecycle status.',
                                                'valid_from': 'Alias effective-from timestamp.',
                                                'valid_to': 'Alias effective-to timestamp when supplied.'},
                                    'quantity_override': True,
                                    'table': 'Internal quality-controlled twin of '
                                             'spine_encounter_identifier: the Gold repairs, nulls and '
                                             'expectations are applied here and the published Gold MV reads '
                                             'this object. Materialized rather than temporary so the '
                                             'published MV can refresh incrementally.'},
 'spine_encounter_identifier': {'columns': {'current_ind': 'Bronze current-alias indicator.',
                                            'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                            'spine_encounter.encounter_id.',
                                            'encounter_identifier_key': 'Deterministic SHA-256 key minted '
                                                                        'from the source alias row.',
                                            'identifier_system': 'Type-scoped identifier system URI '
                                                                 '(urn:cerner:encntr_alias:<normalized alias '
                                                                 'type>) — 23.17M values live under multiple '
                                                                 'alias types',
                                            'identifier_type_code': 'Source alias type verbatim.',
                                            'identifier_value': 'Identifier value; published and IG-governed '
                                                                'at serve time',
                                            'loaded_at': 'map_encounter_identifier.PIPELINE_UPDT_DT_TM '
                                                         'carried unchanged from the contributing bronze '
                                                         'alias row. This records bronze processing '
                                                         'provenance, not identifier validity or the Silver '
                                                         'refresh time; a missing source timestamp remains '
                                                         'null.',
                                            'multi_active_ind': 'Bronze multiple-active-aliases indicator '
                                                                'for this encounter and type.',
                                            'source_identifier_id': 'Native map_encounter_identifier '
                                                                    'SOURCE_PK rendered as text.',
                                            'source_row_id': 'Native source row identifier.',
                                            'source_table': 'Registered bronze source table.',
                                            'status': 'Alias lifecycle status.',
                                            'valid_from': 'Alias effective-from timestamp.',
                                            'valid_to': 'Alias effective-to timestamp when supplied.'},
                                'quantity_override': False,
                                'table': 'One source encounter identifier assignment (FIN and sibling alias '
                                         'types) from map_encounter_identifier; reference surface parallel '
                                         'to person_identifier. Gold QC twin of the silver product: 2 '
                                         'columns are repaired or nulled, 1 rule(s) drop rows, 0 check(s) '
                                         'are advisory. Each rule states its reason in the pipeline '
                                         'notebook, and Lakeflow expectation metrics report what every rule '
                                         'matched on each update.'}}
