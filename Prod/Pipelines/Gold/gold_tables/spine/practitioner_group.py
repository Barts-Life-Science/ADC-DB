"""Gold spine.practitioner_group: 2 dataset(s).

- datasets: _qc_reference_practitioner_group, reference_practitioner_group
- moved verbatim from: 1_gold_spine_and_reference:3948, 1_gold_spine_and_reference:3975, 1_gold_spine_and_reference:3982, 1_gold_spine_and_reference:4014, 1_gold_spine_and_reference:4026

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.practitioner_group ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PRACTITIONER_GROUP_SELECT = [
    '`practitioner_group_key` AS `practitioner_group_key`',
    '`practitioner_id` AS `practitioner_id`',
    '`practitioner_person_id` AS `practitioner_person_id`',
    '`in_practitioner_dimension_ind` AS `in_practitioner_dimension_ind`',
    '`source_group_id` AS `source_group_id`',
    '`group_name` AS `group_name`',
    '`group_label` AS `group_label`',
    '`group_type_meaning` AS `group_type_meaning`',
    '`group_type_display` AS `group_type_display`',
    '`primary_ind` AS `primary_ind`',
    '`relation_active_ind` AS `relation_active_ind`',
    '`relation_beg_effective` AS `relation_beg_effective`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN (CAST(`relation_end_effective` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`relation_end_effective` AS DATE)) < 9999) THEN NULL ELSE `relation_end_effective` END AS `relation_end_effective`",
    '`group_active_ind` AS `group_active_ind`',
    '`group_beg_effective` AS `group_beg_effective`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN (CAST(`group_end_effective` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`group_end_effective` AS DATE)) < 9999) THEN NULL ELSE `group_end_effective` END AS `group_end_effective`",
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_PRACTITIONER_GROUP_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 251,212 rows of 251,212 that
    # are current and attributable. Superseded versions and rows whose identity was never
    # resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}


REFERENCE_PRACTITIONER_GROUP_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 251,212 at the profile.
    "gold.reference.practitioner_group.record_status.default_view_active":
        "record_status = 'active'",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_practitioner_group"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_PRACTITIONER_GROUP_MANDATORY_RULES)
@dp.expect_all(REFERENCE_PRACTITIONER_GROUP_ADVISORY_RULES)
def _gold_qc_reference_practitioner_group():
    """Quality-controlled twin of journey_reference.practitioner_group."""
    df = _qc("reference_practitioner_group", REFERENCE_PRACTITIONER_GROUP_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.practitioner_group"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_practitioner_group():
    """Contract-v2 public twin of reference_practitioner_group; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_practitioner_group")).selectExpr(
        '`practitioner_group_key` AS `practitioner_group_key`',
        '`practitioner_id` AS `practitioner_id`',
        '`practitioner_person_id` AS `practitioner_person_id`',
        '`in_practitioner_dimension_ind` AS `in_practitioner_dimension_ind`',
        '`source_group_id` AS `source_group_id`',
        '`group_name` AS `group_name`',
        '`group_label` AS `group_label`',
        '`group_type_meaning` AS `group_type_meaning`',
        '`group_type_display` AS `group_type_display`',
        '`primary_ind` AS `primary_ind`',
        '`relation_active_ind` AS `relation_active_ind`',
        '`relation_beg_effective` AS `relation_beg_effective`',
        '`relation_end_effective` AS `relation_end_effective`',
        '`group_active_ind` AS `group_active_ind`',
        '`group_beg_effective` AS `group_beg_effective`',
        '`group_end_effective` AS `group_end_effective`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_practitioner_group': GoldRules(
        table='reference_practitioner_group',
        select=REFERENCE_PRACTITIONER_GROUP_SELECT,
        mandatory=REFERENCE_PRACTITIONER_GROUP_MANDATORY_RULES,
        advisory=REFERENCE_PRACTITIONER_GROUP_ADVISORY_RULES,
        lifecycle={'join_keys': ['practitioner_group_key'], 'support_columns': ['record_status', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_practitioner_group': {'columns': {'_source_row_id': 'Personnel-group relationship '
                                                                    'identifier.',
                                                  '_source_system': 'Value describing source system for the '
                                                                    'practitioner group record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Whitespace and source sentinel '
                                                                    'text are retained unless the pipeline '
                                                                    'explicitly normalizes them; null means '
                                                                    'no value was supplied.',
                                                  '_source_table': 'Value describing source table for the '
                                                                   'practitioner group record. It is '
                                                                   'produced by the silver transformation '
                                                                   'and has no direct bronze-column lineage '
                                                                   'entry. Whitespace and source sentinel '
                                                                   'text are retained unless the pipeline '
                                                                   'explicitly normalizes them; null means '
                                                                   'no value was supplied.',
                                                  'group_active_ind': 'Source numeric active indicator for '
                                                                      'the personnel group, carried from '
                                                                      '`GROUP_ACTIVE_IND` without boolean '
                                                                      'conversion.',
                                                  'group_beg_effective': 'Group begin-effective timestamp.',
                                                  'group_end_effective': 'Group end-effective timestamp.',
                                                  'group_label': 'Best label from group name, description '
                                                                 'and code metadata.',
                                                  'group_name': 'Raw personnel-group name.',
                                                  'group_type_display': 'Code-value display for the group '
                                                                        'type.',
                                                  'group_type_meaning': 'CDF meaning for the group type '
                                                                        'code.',
                                                  'in_practitioner_dimension_ind': 'Exact Millennium '
                                                                                   'personnel identifier.',
                                                  'loaded_at': 'map_medical_personnel_group.ADC_UPDT carried '
                                                               'unchanged for the membership row. The '
                                                               'personnel-directory existence join supplies '
                                                               'only a person identifier and contributes no '
                                                               'timestamp; this is bronze load provenance, '
                                                               'not relationship validity time or Silver '
                                                               'refresh time.',
                                                  'practitioner_group_key': 'Deterministic SHA-256 key for '
                                                                            'the practitioner-group '
                                                                            'membership.',
                                                  'practitioner_id': 'Native Millennium personnel PERSON_ID '
                                                                     'as BIGINT; joins to '
                                                                     'reference_practitioner.practitioner_id.',
                                                  'practitioner_person_id': 'Source-system identifier for '
                                                                            'the practitioner person '
                                                                            'associated with each '
                                                                            'practitioner group record. It '
                                                                            'is derived from bronze field '
                                                                            '`PERSON_ID` in '
                                                                            '`4_prod.bronze.map_medical_personnel_group`. '
                                                                            'Its uniqueness is limited to '
                                                                            'the originating source '
                                                                            'namespace; null means no '
                                                                            'identifier was supplied.',
                                                  'primary_ind': 'Source relationship primary indicator.',
                                                  'record_status': 'Retracted when '
                                                                   'map_medical_personnel_group.SOURCE_PRESENT_IND '
                                                                   'is false, otherwise active; a missing '
                                                                   'presence flag defaults true. '
                                                                   'Relation/group activity, validity dates '
                                                                   'and personnel-directory membership do '
                                                                   'not determine this label, and no '
                                                                   'superseded status is emitted.',
                                                  'relation_active_ind': 'Source numeric active indicator '
                                                                         'for the personnel-to-group '
                                                                         'relationship, carried from '
                                                                         '`RELATION_ACTIVE_IND` without '
                                                                         'boolean conversion.',
                                                  'relation_beg_effective': 'Relationship begin-effective '
                                                                            'timestamp.',
                                                  'relation_end_effective': 'Relationship end-effective '
                                                                            'timestamp.',
                                                  'source_group_id': 'Native personnel-group identifier from '
                                                                     '`PRSNL_GROUP_ID`, retained as a '
                                                                     'string.'},
                                      'quantity_override': True,
                                      'table': 'Internal quality-controlled twin of '
                                               'reference_practitioner_group: the Gold repairs, nulls and '
                                               'expectations are applied here and the published Gold MV '
                                               'reads this object. Materialized rather than temporary so the '
                                               'published MV can refresh incrementally.'},
 'reference_practitioner_group': {'columns': {'_source_row_id': 'Personnel-group relationship identifier.',
                                              '_source_system': 'Value describing source system for the '
                                                                'practitioner group record. It is produced '
                                                                'by the silver transformation and has no '
                                                                'direct bronze-column lineage entry. '
                                                                'Whitespace and source sentinel text are '
                                                                'retained unless the pipeline explicitly '
                                                                'normalizes them; null means no value was '
                                                                'supplied.',
                                              '_source_table': 'Value describing source table for the '
                                                               'practitioner group record. It is produced by '
                                                               'the silver transformation and has no direct '
                                                               'bronze-column lineage entry. Whitespace and '
                                                               'source sentinel text are retained unless the '
                                                               'pipeline explicitly normalizes them; null '
                                                               'means no value was supplied.',
                                              'group_active_ind': 'Source numeric active indicator for the '
                                                                  'personnel group, carried from '
                                                                  '`GROUP_ACTIVE_IND` without boolean '
                                                                  'conversion.',
                                              'group_beg_effective': 'Group begin-effective timestamp.',
                                              'group_end_effective': 'Group end-effective timestamp.',
                                              'group_label': 'Best label from group name, description and '
                                                             'code metadata.',
                                              'group_name': 'Raw personnel-group name.',
                                              'group_type_display': 'Code-value display for the group type.',
                                              'group_type_meaning': 'CDF meaning for the group type code.',
                                              'in_practitioner_dimension_ind': 'Exact Millennium personnel '
                                                                               'identifier.',
                                              'loaded_at': 'map_medical_personnel_group.ADC_UPDT carried '
                                                           'unchanged for the membership row. The '
                                                           'personnel-directory existence join supplies only '
                                                           'a person identifier and contributes no '
                                                           'timestamp; this is bronze load provenance, not '
                                                           'relationship validity time or Silver refresh '
                                                           'time.',
                                              'practitioner_group_key': 'Deterministic SHA-256 key for the '
                                                                        'practitioner-group membership.',
                                              'practitioner_id': 'Native Millennium personnel PERSON_ID as '
                                                                 'BIGINT; joins to '
                                                                 'reference_practitioner.practitioner_id.',
                                              'practitioner_person_id': 'Source-system identifier for the '
                                                                        'practitioner person associated with '
                                                                        'each practitioner group record. It '
                                                                        'is derived from bronze field '
                                                                        '`PERSON_ID` in '
                                                                        '`4_prod.bronze.map_medical_personnel_group`. '
                                                                        'Its uniqueness is limited to the '
                                                                        'originating source namespace; null '
                                                                        'means no identifier was supplied.',
                                              'primary_ind': 'Source relationship primary indicator.',
                                              'record_status': 'Retracted when '
                                                               'map_medical_personnel_group.SOURCE_PRESENT_IND '
                                                               'is false, otherwise active; a missing '
                                                               'presence flag defaults true. Relation/group '
                                                               'activity, validity dates and '
                                                               'personnel-directory membership do not '
                                                               'determine this label, and no superseded '
                                                               'status is emitted.',
                                              'relation_active_ind': 'Source numeric active indicator for '
                                                                     'the personnel-to-group relationship, '
                                                                     'carried from `RELATION_ACTIVE_IND` '
                                                                     'without boolean conversion.',
                                              'relation_beg_effective': 'Relationship begin-effective '
                                                                        'timestamp.',
                                              'relation_end_effective': 'Relationship end-effective '
                                                                        'timestamp.',
                                              'source_group_id': 'Native personnel-group identifier from '
                                                                 '`PRSNL_GROUP_ID`, retained as a string.'},
                                  'quantity_override': False,
                                  'table': 'One staff-group membership with practitioner-dimension orphan '
                                           'evidence. Gold QC twin of the silver product: 2 columns are '
                                           'repaired or nulled, 1 rule(s) drop rows, 1 check(s) are '
                                           'advisory. Each rule states its reason in the pipeline notebook, '
                                           'and Lakeflow expectation metrics report what every rule matched '
                                           'on each update.'}}
