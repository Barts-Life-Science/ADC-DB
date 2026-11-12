"""Gold specialist.registry_entry: 2 dataset(s).

- datasets: _qc_clinical_registry_entry, clinical_registry_entry
- moved verbatim from: 3_gold_specialist_care:2490, 3_gold_specialist_care:2521, 3_gold_specialist_care:2528, 3_gold_specialist_care:2588, 3_gold_specialist_care:2592, 3_gold_specialist_care:2605

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.registry_entry ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_REGISTRY_ENTRY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_object` AS `source_object`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`registry_family` AS `registry_family`',
    '`registry_type` AS `registry_type`',
    '`entry_id` AS `entry_id`',
    '`parent_registry_entry_key` AS `parent_registry_entry_key`',
    '`linkage_status` AS `linkage_status`',
    '`mrn` AS `mrn`',
    '`nhs_number` AS `nhs_number`',
    '`date_of_death` AS `date_of_death`',
    '`source_edit_datetime` AS `source_edit_datetime`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_registry_entry_metadata; source history stays on the main research table.
CLINICAL_REGISTRY_ENTRY_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 236,653 rows of 236,690
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_REGISTRY_ENTRY_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.registry_entry.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 37 of 236,690 at the profile.
    "gold.clinical.registry_entry.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 18 of 236,690 rows (0.0076%) when profiled on 2026-08-24.
    "gold.clinical.registry_entry.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1 of 236,690 rows (0.000422%) when profiled on 2026-08-24.
    "gold.clinical.registry_entry.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_REGISTRY_ENTRY_PERSON_SCOPE_RULES = {
    "gold.clinical.registry_entry.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_entry"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_ENTRY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_REGISTRY_ENTRY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_ENTRY_PERSON_SCOPE_RULES)
def _gold_qc_clinical_registry_entry():
    """Quality-controlled twin of journey_clinical.registry_entry."""
    df = _qc("clinical_registry_entry", CLINICAL_REGISTRY_ENTRY_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.registry_entry"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_registry_entry():
    """Contract-v2 public twin of clinical_registry_entry; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_registry_entry")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_object` AS `source_object`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`registry_family` AS `registry_family`',
        '`registry_type` AS `registry_type`',
        '`entry_id` AS `entry_id`',
        '`parent_registry_entry_key` AS `parent_registry_entry_key`',
        '`linkage_status` AS `linkage_status`',
        '`mrn` AS `mrn`',
        '`nhs_number` AS `nhs_number`',
        '`date_of_death` AS `date_of_death`',
        '`source_edit_datetime` AS `source_edit_datetime`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'clinical_registry_entry': GoldRules(
        table='clinical_registry_entry',
        select=CLINICAL_REGISTRY_ENTRY_SELECT,
        mandatory=CLINICAL_REGISTRY_ENTRY_MANDATORY_RULES,
        advisory=CLINICAL_REGISTRY_ENTRY_ADVISORY_RULES,
        person_scope=CLINICAL_REGISTRY_ENTRY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth')),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_registry_entry': {'columns': {'confidentiality_code': 'Security classification when supplied.',
                                             'date_of_death': 'Family-specific demographic date of death.',
                                             'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                             'available.',
                                             'entry_id': 'Native iWeb ENTRY_ID as BIGINT; primary key within '
                                                         'source_object.',
                                             'event_datetime': 'Family-specific clinical timestamp; child '
                                                               'rows inherit the parent timestamp.',
                                             'event_datetime_local': 'Local-time representation of the '
                                                                     'family-specific clinical event '
                                                                     'timestamp for the registry entry.',
                                             'event_end_datetime': 'Family-specific clinical end timestamp.',
                                             'event_end_datetime_local': 'Local-time representation of the '
                                                                         'family-specific clinical end '
                                                                         'timestamp for the registry entry. '
                                                                         'Gold: NULL wherever Gold nulls '
                                                                         'event_end_datetime.',
                                             'linkage_status': 'Bronze person-link status.',
                                             'loaded_at': 'ADC_UPDT of the contributing iWeb row, carried '
                                                          'unchanged per family. Surgery-procedure and '
                                                          'coronary-lesion parent joins select parent '
                                                          'identity and clinical event time, not a parent '
                                                          'load clock. The family union takes no '
                                                          'cross-family maximum; device rows are excluded '
                                                          'before registry QC/public projections.',
                                             'mrn': 'Source MRN where supplied.',
                                             'nhs_number': 'Source NHS number where supplied.',
                                             'parent_registry_entry_key': 'Deterministic SHA-256 key for the '
                                                                          'parent registry entry.',
                                             'patient_event_key': 'Deterministic event key derived from '
                                                                  'source identifiers; retained as a stable '
                                                                  'join key.',
                                             'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                          'resolved.',
                                             'record_status': "Superseded when the contributing iWeb row's "
                                                              'SOURCE_PRESENT_IND is false, otherwise '
                                                              'active; missing presence defaults true. This '
                                                              'rule applies to all ten configured registry '
                                                              'families and emits no retracted status. '
                                                              'Device rows in the shared internal stage are '
                                                              'filtered out before registry publication.',
                                             'record_status_effective_from': 'Always null as a TIMESTAMP for '
                                                                             'all ten configured iWeb '
                                                                             'registry families. Neither the '
                                                                             "row's clinical event date, a "
                                                                             'joined parent '
                                                                             'procedure/episode event date, '
                                                                             'DATE_LAST_CHANGED nor a '
                                                                             'MediConnect implant date fills '
                                                                             'this history start.',
                                             'record_status_effective_to': 'For nine iWeb families, '
                                                                           'SOURCE_ABSENT_DETECTED_TS only '
                                                                           'when source presence is false, '
                                                                           'otherwise null. '
                                                                           'Coronary-procedure rows always '
                                                                           'have a null history end, '
                                                                           'including source-absent rows, '
                                                                           "because that family's configured "
                                                                           'absence-time expression is null. '
                                                                           'No ADC_UPDT or parent-date '
                                                                           'fallback is applied.',
                                             'registry_family': 'Parameterized registry family.',
                                             'registry_payload': 'Full verbatim source row as key/value '
                                                                 'pairs.',
                                             'registry_type': 'Registry subtype for composite-key MDT rows.',
                                             'source_edit_datetime': 'Source edit timestamp; never treated '
                                                                     'as clinical time.',
                                             'source_object': 'iWeb registry family identifying the source '
                                                              'object; part of the primary key with '
                                                              'entry_id.',
                                             'source_update_timestamp': 'Timestamp of the last change made '
                                                                        'to the record in the source '
                                                                        'registry system.',
                                             'subject_id_system': 'Identifier system used for subject_key.',
                                             'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                            'identifier. Not salted, not secret; a stable '
                                                            'join key across feeds where person_id is '
                                                            'unresolved.',
                                             'vip_ind': 'VIP indicator when supplied.',
                                             'withheld_identity_ind': 'Identity-withholding status is not '
                                                                      'populated by the current source '
                                                                      'projections; null does not mean '
                                                                      'identity was not withheld.'},
                                 'quantity_override': True,
                                 'table': 'Internal quality-controlled twin of clinical_registry_entry: the '
                                          'Gold repairs, nulls and expectations are applied here and the '
                                          'published Gold MV reads this object. Materialized rather than '
                                          'temporary so the published MV can refresh incrementally.'},
 'clinical_registry_entry': {'columns': {'confidentiality_code': 'Security classification when supplied.',
                                         'date_of_death': 'Family-specific demographic date of death.',
                                         'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                         'available.',
                                         'entry_id': 'Native iWeb ENTRY_ID as BIGINT; primary key within '
                                                     'source_object.',
                                         'event_datetime': 'Family-specific clinical timestamp; child rows '
                                                           'inherit the parent timestamp.',
                                         'event_datetime_local': 'Local-time representation of the '
                                                                 'family-specific clinical event timestamp '
                                                                 'for the registry entry.',
                                         'event_end_datetime': 'Family-specific clinical end timestamp.',
                                         'event_end_datetime_local': 'Local-time representation of the '
                                                                     'family-specific clinical end timestamp '
                                                                     'for the registry entry. Gold: NULL '
                                                                     'wherever Gold nulls '
                                                                     'event_end_datetime.',
                                         'linkage_status': 'Bronze person-link status.',
                                         'loaded_at': 'ADC_UPDT of the contributing iWeb row, carried '
                                                      'unchanged per family. Surgery-procedure and '
                                                      'coronary-lesion parent joins select parent identity '
                                                      'and clinical event time, not a parent load clock. The '
                                                      'family union takes no cross-family maximum; device '
                                                      'rows are excluded before registry QC/public '
                                                      'projections.',
                                         'mrn': 'Source MRN where supplied.',
                                         'nhs_number': 'Source NHS number where supplied.',
                                         'parent_registry_entry_key': 'Deterministic SHA-256 key for the '
                                                                      'parent registry entry.',
                                         'patient_event_key': 'Deterministic event key derived from source '
                                                              'identifiers; retained as a stable join key.',
                                         'person_id': 'Native Millennium PERSON_ID as BIGINT when resolved.',
                                         'record_status': "Superseded when the contributing iWeb row's "
                                                          'SOURCE_PRESENT_IND is false, otherwise active; '
                                                          'missing presence defaults true. This rule applies '
                                                          'to all ten configured registry families and emits '
                                                          'no retracted status. Device rows in the shared '
                                                          'internal stage are filtered out before registry '
                                                          'publication.',
                                         'record_status_effective_from': 'Always null as a TIMESTAMP for all '
                                                                         'ten configured iWeb registry '
                                                                         "families. Neither the row's "
                                                                         'clinical event date, a joined '
                                                                         'parent procedure/episode event '
                                                                         'date, DATE_LAST_CHANGED nor a '
                                                                         'MediConnect implant date fills '
                                                                         'this history start.',
                                         'record_status_effective_to': 'For nine iWeb families, '
                                                                       'SOURCE_ABSENT_DETECTED_TS only when '
                                                                       'source presence is false, otherwise '
                                                                       'null. Coronary-procedure rows always '
                                                                       'have a null history end, including '
                                                                       'source-absent rows, because that '
                                                                       "family's configured absence-time "
                                                                       'expression is null. No ADC_UPDT or '
                                                                       'parent-date fallback is applied.',
                                         'registry_family': 'Parameterized registry family.',
                                         'registry_payload': 'Full verbatim source row as key/value pairs.',
                                         'registry_type': 'Registry subtype for composite-key MDT rows.',
                                         'source_edit_datetime': 'Source edit timestamp; never treated as '
                                                                 'clinical time.',
                                         'source_object': 'iWeb registry family identifying the source '
                                                          'object; part of the primary key with entry_id.',
                                         'source_update_timestamp': 'Timestamp of the last change made to '
                                                                    'the record in the source registry '
                                                                    'system.',
                                         'subject_id_system': 'Identifier system used for subject_key.',
                                         'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                        'identifier. Not salted, not secret; a stable join '
                                                        'key across feeds where person_id is unresolved.',
                                         'vip_ind': 'VIP indicator when supplied.',
                                         'withheld_identity_ind': 'Identity-withholding status is not '
                                                                  'populated by the current source '
                                                                  'projections; null does not mean identity '
                                                                  'was not withheld.'},
                             'quantity_override': False,
                             'table': 'One iWeb cardiac-registry entry from one of ten registered families, '
                                      'retaining the full source row as a governed VARIANT payload. Gold QC '
                                      'twin of the silver product: 1 columns are repaired or nulled, 1 '
                                      'rule(s) drop rows, 3 check(s) are advisory. Each rule states its '
                                      'reason in the pipeline notebook, and Lakeflow expectation metrics '
                                      'report what every rule matched on each update.'}}
