"""Gold pathways.elective_access_procedure: 2 dataset(s).

- datasets: _qc_reference_elective_access_procedure, reference_elective_access_procedure
- moved verbatim from: 4_gold_care_pathways:4133, 4_gold_care_pathways:4152, 4_gold_care_pathways:4159, 4_gold_care_pathways:4185, 4_gold_care_pathways:4197

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.elective_access_procedure ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_ELECTIVE_ACCESS_PROCEDURE_SELECT = [
    '`elective_access_procedure_key` AS `elective_access_procedure_key`',
    '`elective_access_entry_key` AS `elective_access_entry_key`',
    '`waiting_list_oid` AS `waiting_list_oid`',
    '`procedure_code` AS `procedure_code`',
    '`procedure_desc` AS `procedure_desc`',
    '`procedure_catalog` AS `procedure_catalog`',
    '`procedure_rvid` AS `procedure_rvid`',
    '`procedure_type_seq` AS `procedure_type_seq`',
    '`procedure_seq` AS `procedure_seq`',
    '`active_ind` AS `active_ind`',
    '`parent_present_ind` AS `parent_present_ind`',
    '`source_system_oid` AS `source_system_oid`',
    '`source_system_oid_inherited_ind` AS `source_system_oid_inherited_ind`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_ELECTIVE_ACCESS_PROCEDURE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 2,922,493 rows of 2,922,503
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}


REFERENCE_ELECTIVE_ACCESS_PROCEDURE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 10 of 2,922,503 at the profile.
    "gold.reference.elective_access_procedure.record_status.default_view_active":
        "record_status = 'active'",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_elective_access_procedure"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_ELECTIVE_ACCESS_PROCEDURE_MANDATORY_RULES)
@dp.expect_all(REFERENCE_ELECTIVE_ACCESS_PROCEDURE_ADVISORY_RULES)
def _gold_qc_reference_elective_access_procedure():
    """Quality-controlled twin of journey_reference.elective_access_procedure."""
    df = _qc("reference_elective_access_procedure", REFERENCE_ELECTIVE_ACCESS_PROCEDURE_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.elective_access_procedure"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_elective_access_procedure():
    """Contract-v2 public twin of reference_elective_access_procedure; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_elective_access_procedure")).selectExpr(
        '`elective_access_procedure_key` AS `elective_access_procedure_key`',
        '`elective_access_entry_key` AS `elective_access_entry_key`',
        '`waiting_list_oid` AS `waiting_list_oid`',
        '`procedure_code` AS `procedure_code`',
        '`procedure_desc` AS `procedure_desc`',
        '`procedure_catalog` AS `procedure_catalog`',
        '`procedure_rvid` AS `procedure_rvid`',
        '`procedure_type_seq` AS `procedure_type_seq`',
        '`procedure_seq` AS `procedure_seq`',
        '`active_ind` AS `active_ind`',
        '`parent_present_ind` AS `parent_present_ind`',
        '`source_system_oid` AS `source_system_oid`',
        '`source_system_oid_inherited_ind` AS `source_system_oid_inherited_ind`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_elective_access_procedure': GoldRules(
        table='reference_elective_access_procedure',
        select=REFERENCE_ELECTIVE_ACCESS_PROCEDURE_SELECT,
        mandatory=REFERENCE_ELECTIVE_ACCESS_PROCEDURE_MANDATORY_RULES,
        advisory=REFERENCE_ELECTIVE_ACCESS_PROCEDURE_ADVISORY_RULES,
        lifecycle={'join_keys': ['elective_access_procedure_key'], 'support_columns': ['record_status', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_elective_access_procedure': {'columns': {'active_ind': 'B3 bronze field ACTIVE_IND; source '
                                                                       'value retained unless documented as '
                                                                       'derived.',
                                                         'elective_access_entry_key': 'Deterministic SHA-256 '
                                                                                      'key for the parent '
                                                                                      'elective access '
                                                                                      'entry.',
                                                         'elective_access_procedure_key': 'Deterministic '
                                                                                          'SHA-256 key for '
                                                                                          'this LUNA '
                                                                                          'procedure row.',
                                                         'loaded_at': 'map_elective_access_list_procedure.ADC_UPDT '
                                                                      'carried unchanged for the '
                                                                      'planned-procedure slot. No parent '
                                                                      'waiting-list load clock is joined or '
                                                                      'added; this is bronze load '
                                                                      'provenance, not procedure time or '
                                                                      'Silver refresh time.',
                                                         'parent_present_ind': 'B3 bronze field '
                                                                               'PARENT_PRESENT_IND; source '
                                                                               'value retained unless '
                                                                               'documented as derived.',
                                                         'procedure_catalog': 'B3 bronze field '
                                                                              'PROCEDURE_CATALOG; source '
                                                                              'value retained unless '
                                                                              'documented as derived.',
                                                         'procedure_code': 'B3 bronze field PROCEDURE_CODE; '
                                                                           'source value retained unless '
                                                                           'documented as derived.',
                                                         'procedure_desc': 'B3 bronze field PROCEDURE_DESC; '
                                                                           'source value retained unless '
                                                                           'documented as derived.',
                                                         'procedure_rvid': 'B3 bronze field PROCEDURE_RVID; '
                                                                           'source value retained unless '
                                                                           'documented as derived.',
                                                         'procedure_seq': 'B3 bronze field PROCEDURE_SEQ; '
                                                                          'source value retained unless '
                                                                          'documented as derived.',
                                                         'procedure_type_seq': 'B3 bronze field '
                                                                               'PROCEDURE_TYPE_SEQ; source '
                                                                               'value retained unless '
                                                                               'documented as derived.',
                                                         'record_status': 'Retracted when SOURCE_PRESENT_IND '
                                                                          'is false; otherwise superseded '
                                                                          'when ACTIVE_IND is false; '
                                                                          'otherwise active. Both missing '
                                                                          'flags default true, and '
                                                                          'retraction has priority. '
                                                                          'PARENT_PRESENT_IND is published '
                                                                          'separately and does not determine '
                                                                          'this row status.',
                                                         'source_system_oid': 'LUNA SOURCE_SYSTEM_OID as '
                                                                              'BIGINT.',
                                                         'source_system_oid_inherited_ind': 'Always true on '
                                                                                            'present rows; '
                                                                                            'proves '
                                                                                            'namespace '
                                                                                            'provenance is '
                                                                                            'the parent.',
                                                         'waiting_list_oid': 'LUNA WAITING_LIST_OID as '
                                                                             'BIGINT.'},
                                             'quantity_override': True,
                                             'table': 'Internal quality-controlled twin of '
                                                      'reference_elective_access_procedure: the Gold '
                                                      'repairs, nulls and expectations are applied here and '
                                                      'the published Gold MV reads this object. Materialized '
                                                      'rather than temporary so the published MV can refresh '
                                                      'incrementally.'},
 'reference_elective_access_procedure': {'columns': {'active_ind': 'B3 bronze field ACTIVE_IND; source value '
                                                                   'retained unless documented as derived.',
                                                     'elective_access_entry_key': 'Deterministic SHA-256 key '
                                                                                  'for the parent elective '
                                                                                  'access entry.',
                                                     'elective_access_procedure_key': 'Deterministic SHA-256 '
                                                                                      'key for this LUNA '
                                                                                      'procedure row.',
                                                     'loaded_at': 'map_elective_access_list_procedure.ADC_UPDT '
                                                                  'carried unchanged for the '
                                                                  'planned-procedure slot. No parent '
                                                                  'waiting-list load clock is joined or '
                                                                  'added; this is bronze load provenance, '
                                                                  'not procedure time or Silver refresh '
                                                                  'time.',
                                                     'parent_present_ind': 'B3 bronze field '
                                                                           'PARENT_PRESENT_IND; source value '
                                                                           'retained unless documented as '
                                                                           'derived.',
                                                     'procedure_catalog': 'B3 bronze field '
                                                                          'PROCEDURE_CATALOG; source value '
                                                                          'retained unless documented as '
                                                                          'derived.',
                                                     'procedure_code': 'B3 bronze field PROCEDURE_CODE; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                     'procedure_desc': 'B3 bronze field PROCEDURE_DESC; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                     'procedure_rvid': 'B3 bronze field PROCEDURE_RVID; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                     'procedure_seq': 'B3 bronze field PROCEDURE_SEQ; source '
                                                                      'value retained unless documented as '
                                                                      'derived.',
                                                     'procedure_type_seq': 'B3 bronze field '
                                                                           'PROCEDURE_TYPE_SEQ; source value '
                                                                           'retained unless documented as '
                                                                           'derived.',
                                                     'record_status': 'Retracted when SOURCE_PRESENT_IND is '
                                                                      'false; otherwise superseded when '
                                                                      'ACTIVE_IND is false; otherwise '
                                                                      'active. Both missing flags default '
                                                                      'true, and retraction has priority. '
                                                                      'PARENT_PRESENT_IND is published '
                                                                      'separately and does not determine '
                                                                      'this row status.',
                                                     'source_system_oid': 'LUNA SOURCE_SYSTEM_OID as BIGINT.',
                                                     'source_system_oid_inherited_ind': 'Always true on '
                                                                                        'present rows; '
                                                                                        'proves namespace '
                                                                                        'provenance is the '
                                                                                        'parent.',
                                                     'waiting_list_oid': 'LUNA WAITING_LIST_OID as BIGINT.'},
                                         'quantity_override': False,
                                         'table': 'One LUNA elective-access planned-procedure slot. Gold QC '
                                                  'twin of the silver product: 0 columns are repaired or '
                                                  'nulled, 1 rule(s) drop rows, 1 check(s) are advisory. '
                                                  'Each rule states its reason in the pipeline notebook, and '
                                                  'Lakeflow expectation metrics report what every rule '
                                                  'matched on each update.'}}
