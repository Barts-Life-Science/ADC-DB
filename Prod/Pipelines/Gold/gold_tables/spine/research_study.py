"""Gold spine.research_study: 2 dataset(s).

- datasets: _qc_reference_research_study, reference_research_study
- moved verbatim from: 1_gold_spine_and_reference:4274, 1_gold_spine_and_reference:4303, 1_gold_spine_and_reference:4310, 1_gold_spine_and_reference:4345, 1_gold_spine_and_reference:4357

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.research_study ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_RESEARCH_STUDY_SELECT = [
    '`research_study_key` AS `research_study_key`',
    '`source_protocol_id` AS `source_protocol_id`',
    '`study_mnemonic` AS `study_mnemonic`',
    '`study_mnemonic_key` AS `study_mnemonic_key`',
    '`protocol_type_code` AS `protocol_type_code`',
    '`protocol_type_desc` AS `protocol_type_desc`',
    '`protocol_phase_code` AS `protocol_phase_code`',
    '`protocol_phase_desc` AS `protocol_phase_desc`',
    '`protocol_status_code` AS `protocol_status_code`',
    '`protocol_status_desc` AS `protocol_status_desc`',
    '`protocol_purpose_code` AS `protocol_purpose_code`',
    '`protocol_purpose_desc` AS `protocol_purpose_desc`',
    '`parent_protocol_id` AS `parent_protocol_id`',
    '`previous_protocol_id` AS `previous_protocol_id`',
    '`root_protocol_ind` AS `root_protocol_ind`',
    '`beg_effective` AS `beg_effective`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN (CAST(`end_effective` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`end_effective` AS DATE)) < 9999) THEN NULL ELSE `end_effective` END AS `end_effective`",
    '`open_ended_ind` AS `open_ended_ind`',
    '`display_ind` AS `display_ind`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_RESEARCH_STUDY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 1,055 rows of 1,055 that are
    # current and attributable. Superseded versions and rows whose identity was never
    # resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}


REFERENCE_RESEARCH_STUDY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 1,055 at the profile.
    "gold.reference.research_study.record_status.default_view_active":
        "record_status = 'active'",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_research_study"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_RESEARCH_STUDY_MANDATORY_RULES)
@dp.expect_all(REFERENCE_RESEARCH_STUDY_ADVISORY_RULES)
def _gold_qc_reference_research_study():
    """Quality-controlled twin of journey_reference.research_study."""
    df = _qc("reference_research_study", REFERENCE_RESEARCH_STUDY_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.research_study"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_research_study():
    """Contract-v2 public twin of reference_research_study; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_research_study")).selectExpr(
        '`research_study_key` AS `research_study_key`',
        '`source_protocol_id` AS `source_protocol_id`',
        '`study_mnemonic` AS `study_mnemonic`',
        '`study_mnemonic_key` AS `study_mnemonic_key`',
        '`protocol_type_code` AS `protocol_type_code`',
        '`protocol_type_desc` AS `protocol_type_desc`',
        '`protocol_phase_code` AS `protocol_phase_code`',
        '`protocol_phase_desc` AS `protocol_phase_desc`',
        '`protocol_status_code` AS `protocol_status_code`',
        '`protocol_status_desc` AS `protocol_status_desc`',
        '`protocol_purpose_code` AS `protocol_purpose_code`',
        '`protocol_purpose_desc` AS `protocol_purpose_desc`',
        '`parent_protocol_id` AS `parent_protocol_id`',
        '`previous_protocol_id` AS `previous_protocol_id`',
        '`root_protocol_ind` AS `root_protocol_ind`',
        '`beg_effective` AS `beg_effective`',
        '`end_effective` AS `end_effective`',
        '`open_ended_ind` AS `open_ended_ind`',
        '`display_ind` AS `display_ind`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_research_study': GoldRules(
        table='reference_research_study',
        select=REFERENCE_RESEARCH_STUDY_SELECT,
        mandatory=REFERENCE_RESEARCH_STUDY_MANDATORY_RULES,
        advisory=REFERENCE_RESEARCH_STUDY_ADVISORY_RULES,
        lifecycle={'join_keys': ['research_study_key'], 'support_columns': ['record_status', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_research_study': {'columns': {'_source_row_id': 'Source-system identifier for the source row '
                                                                'associated with each research study record. '
                                                                'It is derived from bronze field '
                                                                '`PROT_MASTER_ID` in '
                                                                '`4_prod.bronze.map_research_study`. Its '
                                                                'uniqueness is limited to the originating '
                                                                'source namespace; null means no identifier '
                                                                'was supplied.',
                                              '_source_system': 'Value describing source system for the '
                                                                'research study record. It is produced by '
                                                                'the silver transformation and has no direct '
                                                                'bronze-column lineage entry. Whitespace and '
                                                                'source sentinel text are retained unless '
                                                                'the pipeline explicitly normalizes them; '
                                                                'null means no value was supplied.',
                                              '_source_table': 'Value describing source table for the '
                                                               'research study record. It is produced by the '
                                                               'silver transformation and has no direct '
                                                               'bronze-column lineage entry. Whitespace and '
                                                               'source sentinel text are retained unless the '
                                                               'pipeline explicitly normalizes them; null '
                                                               'means no value was supplied.',
                                              'beg_effective': 'The date and time for which this table row '
                                                               'becomes effective.  Normally, this will be '
                                                               'the date and time the row is added, but '
                                                               'could be a past or future date and time.',
                                              'display_ind': 'This field is an indicator that tells whether '
                                                             'the protocol enrollment information for this '
                                                             'protocol will be displayed in the electronic '
                                                             'medical record.',
                                              'end_effective': 'The date/time after which the row is no '
                                                               'longer valid as active current data.  This '
                                                               'may be valued with the date that the row '
                                                               'became inactive.',
                                              'loaded_at': 'map_research_study.ADC_UPDT carried unchanged '
                                                           'for the protocol/amendment row. No parent or '
                                                           'previous-protocol timestamp is joined; this is '
                                                           'bronze ingestion provenance, not protocol '
                                                           'validity time or Silver refresh time.',
                                              'open_ended_ind': 'The date/time after which the row is no '
                                                                'longer valid as active current data.  This '
                                                                'may be valued with the date that the row '
                                                                'became inactive.',
                                              'parent_protocol_id': 'Source-system identifier for the parent '
                                                                    'protocol associated with each research '
                                                                    'study record. It is derived from bronze '
                                                                    'field `PARENT_PROT_MASTER_ID` in '
                                                                    '`4_prod.bronze.map_research_study`. Its '
                                                                    'uniqueness is limited to the '
                                                                    'originating source namespace; null '
                                                                    'means no identifier was supplied.',
                                              'previous_protocol_id': 'Source-system identifier for the '
                                                                      'previous protocol associated with '
                                                                      'each research study record. It is '
                                                                      'derived from bronze field '
                                                                      '`PREV_PROT_MASTER_ID` in '
                                                                      '`4_prod.bronze.map_research_study`. '
                                                                      'Its uniqueness is limited to the '
                                                                      'originating source namespace; null '
                                                                      'means no identifier was supplied.',
                                              'protocol_phase_code': 'This field contains a code for the '
                                                                     'phase of the protocol.  Examples of '
                                                                     'phases would include, but not be '
                                                                     'limited to, phase 1, phase 1A, phase '
                                                                     '1B, phase 2, phase 3, etc.',
                                              'protocol_phase_desc': 'The description for the code value',
                                              'protocol_purpose_code': 'This field contains a code '
                                                                       'identifying the purpose for the '
                                                                       'protocol/study.  Examples of '
                                                                       'purposes would include, but not be '
                                                                       'limited to, cancer control, '
                                                                       'epidemiology, etc.',
                                              'protocol_purpose_desc': 'The description for the code value',
                                              'protocol_status_code': 'This field contains a code '
                                                                      'identifying the status of the '
                                                                      'protocol/study: approved, open, '
                                                                      'suspended (temporarily closed to '
                                                                      'accrual), closed to accrual, closed '
                                                                      'and terminated.',
                                              'protocol_status_desc': 'The description for the code value',
                                              'protocol_type_code': 'This field contains a code identifying '
                                                                    'the type of protocol; therapeutic, '
                                                                    'non-therapeutic, BCM guidelines etc.',
                                              'protocol_type_desc': 'The description for the code value',
                                              'record_status': 'Always active in this projection. Protocol '
                                                               'status, display flag, begin/end-effective '
                                                               'dates and amendment parent/previous links do '
                                                               'not determine this literal label; it does '
                                                               'not assert that a study is open or '
                                                               'recruiting.',
                                              'research_study_key': 'Deterministic SHA-256 key for the '
                                                                    'Cerner research protocol.',
                                              'root_protocol_ind': 'Indicator of whether root protocol '
                                                                   'applies to the research study record. It '
                                                                   'is derived from bronze field '
                                                                   '`PARENT_PROT_MASTER_ID` in '
                                                                   '`4_prod.bronze.map_research_study`, '
                                                                   'selected as the provenance anchor from 2 '
                                                                   'recorded bronze source fields. Null '
                                                                   'means the source did not state the '
                                                                   'indicator and must not be interpreted as '
                                                                   'false.',
                                              'source_protocol_id': 'Source-system identifier for the source '
                                                                    'protocol associated with each research '
                                                                    'study record. It is derived from bronze '
                                                                    'field `PROT_MASTER_ID` in '
                                                                    '`4_prod.bronze.map_research_study`. Its '
                                                                    'uniqueness is limited to the '
                                                                    'originating source namespace; null '
                                                                    'means no identifier was supplied.',
                                              'study_mnemonic': 'Mnemonic assigned to the protocol',
                                              'study_mnemonic_key': 'Mnemonic of the protocol stripped of '
                                                                    'special characters'},
                                  'quantity_override': True,
                                  'table': 'Internal quality-controlled twin of reference_research_study: '
                                           'the Gold repairs, nulls and expectations are applied here and '
                                           'the published Gold MV reads this object. Materialized rather '
                                           'than temporary so the published MV can refresh incrementally.'},
 'reference_research_study': {'columns': {'_source_row_id': 'Source-system identifier for the source row '
                                                            'associated with each research study record. It '
                                                            'is derived from bronze field `PROT_MASTER_ID` '
                                                            'in `4_prod.bronze.map_research_study`. Its '
                                                            'uniqueness is limited to the originating source '
                                                            'namespace; null means no identifier was '
                                                            'supplied.',
                                          '_source_system': 'Value describing source system for the research '
                                                            'study record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Whitespace and source sentinel '
                                                            'text are retained unless the pipeline '
                                                            'explicitly normalizes them; null means no value '
                                                            'was supplied.',
                                          '_source_table': 'Value describing source table for the research '
                                                           'study record. It is produced by the silver '
                                                           'transformation and has no direct bronze-column '
                                                           'lineage entry. Whitespace and source sentinel '
                                                           'text are retained unless the pipeline explicitly '
                                                           'normalizes them; null means no value was '
                                                           'supplied.',
                                          'beg_effective': 'The date and time for which this table row '
                                                           'becomes effective.  Normally, this will be the '
                                                           'date and time the row is added, but could be a '
                                                           'past or future date and time.',
                                          'display_ind': 'This field is an indicator that tells whether the '
                                                         'protocol enrollment information for this protocol '
                                                         'will be displayed in the electronic medical '
                                                         'record.',
                                          'end_effective': 'The date/time after which the row is no longer '
                                                           'valid as active current data.  This may be '
                                                           'valued with the date that the row became '
                                                           'inactive.',
                                          'loaded_at': 'map_research_study.ADC_UPDT carried unchanged for '
                                                       'the protocol/amendment row. No parent or '
                                                       'previous-protocol timestamp is joined; this is '
                                                       'bronze ingestion provenance, not protocol validity '
                                                       'time or Silver refresh time.',
                                          'open_ended_ind': 'The date/time after which the row is no longer '
                                                            'valid as active current data.  This may be '
                                                            'valued with the date that the row became '
                                                            'inactive.',
                                          'parent_protocol_id': 'Source-system identifier for the parent '
                                                                'protocol associated with each research '
                                                                'study record. It is derived from bronze '
                                                                'field `PARENT_PROT_MASTER_ID` in '
                                                                '`4_prod.bronze.map_research_study`. Its '
                                                                'uniqueness is limited to the originating '
                                                                'source namespace; null means no identifier '
                                                                'was supplied.',
                                          'previous_protocol_id': 'Source-system identifier for the previous '
                                                                  'protocol associated with each research '
                                                                  'study record. It is derived from bronze '
                                                                  'field `PREV_PROT_MASTER_ID` in '
                                                                  '`4_prod.bronze.map_research_study`. Its '
                                                                  'uniqueness is limited to the originating '
                                                                  'source namespace; null means no '
                                                                  'identifier was supplied.',
                                          'protocol_phase_code': 'This field contains a code for the phase '
                                                                 'of the protocol.  Examples of phases would '
                                                                 'include, but not be limited to, phase 1, '
                                                                 'phase 1A, phase 1B, phase 2, phase 3, etc.',
                                          'protocol_phase_desc': 'The description for the code value',
                                          'protocol_purpose_code': 'This field contains a code identifying '
                                                                   'the purpose for the protocol/study.  '
                                                                   'Examples of purposes would include, but '
                                                                   'not be limited to, cancer control, '
                                                                   'epidemiology, etc.',
                                          'protocol_purpose_desc': 'The description for the code value',
                                          'protocol_status_code': 'This field contains a code identifying '
                                                                  'the status of the protocol/study: '
                                                                  'approved, open, suspended (temporarily '
                                                                  'closed to accrual), closed to accrual, '
                                                                  'closed and terminated.',
                                          'protocol_status_desc': 'The description for the code value',
                                          'protocol_type_code': 'This field contains a code identifying the '
                                                                'type of protocol; therapeutic, '
                                                                'non-therapeutic, BCM guidelines etc.',
                                          'protocol_type_desc': 'The description for the code value',
                                          'record_status': 'Always active in this projection. Protocol '
                                                           'status, display flag, begin/end-effective dates '
                                                           'and amendment parent/previous links do not '
                                                           'determine this literal label; it does not assert '
                                                           'that a study is open or recruiting.',
                                          'research_study_key': 'Deterministic SHA-256 key for the Cerner '
                                                                'research protocol.',
                                          'root_protocol_ind': 'Indicator of whether root protocol applies '
                                                               'to the research study record. It is derived '
                                                               'from bronze field `PARENT_PROT_MASTER_ID` in '
                                                               '`4_prod.bronze.map_research_study`, selected '
                                                               'as the provenance anchor from 2 recorded '
                                                               'bronze source fields. Null means the source '
                                                               'did not state the indicator and must not be '
                                                               'interpreted as false.',
                                          'source_protocol_id': 'Source-system identifier for the source '
                                                                'protocol associated with each research '
                                                                'study record. It is derived from bronze '
                                                                'field `PROT_MASTER_ID` in '
                                                                '`4_prod.bronze.map_research_study`. Its '
                                                                'uniqueness is limited to the originating '
                                                                'source namespace; null means no identifier '
                                                                'was supplied.',
                                          'study_mnemonic': 'Mnemonic assigned to the protocol',
                                          'study_mnemonic_key': 'Mnemonic of the protocol stripped of '
                                                                'special characters'},
                              'quantity_override': False,
                              'table': 'One Cerner research protocol amendment with parent and '
                                       'previous-protocol lineage. Gold QC twin of the silver product: 1 '
                                       'columns are repaired or nulled, 1 rule(s) drop rows, 1 check(s) are '
                                       'advisory. Each rule states its reason in the pipeline notebook, and '
                                       'Lakeflow expectation metrics report what every rule matched on each '
                                       'update.'}}
