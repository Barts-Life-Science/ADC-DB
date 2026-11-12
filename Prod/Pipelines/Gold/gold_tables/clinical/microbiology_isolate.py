"""Gold clinical.microbiology_isolate: 2 dataset(s).

- datasets: _qc_clinical_microbiology_isolate, clinical_microbiology_isolate
- moved verbatim from: 2_gold_clinical_events:4362, 2_gold_clinical_events:4409, 2_gold_clinical_events:4417, 2_gold_clinical_events:4482, 2_gold_clinical_events:4487, 2_gold_clinical_events:4491, 2_gold_clinical_events:4505

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.microbiology_isolate ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_MICROBIOLOGY_ISOLATE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`microbiology_isolate_id` AS `microbiology_isolate_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`encounter_link_method` AS `encounter_link_method`',  # GOLD_COLUMN_PARITY_V1
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`pathology_result_id` AS `pathology_result_id`',
    '`report_version_key` AS `report_version_key`',
    '`specimen_type_code` AS `specimen_type_code`',
    '`organism_text` AS `organism_text`',
    '`organism_snomed_code` AS `organism_snomed_code`',
    '`organism_omop_concept_id` AS `organism_omop_concept_id`',
    '`suspected_ind` AS `suspected_ind`',
    '`growth_grade` AS `growth_grade`',
    '`organism_code` AS `organism_code`',
    '`panel_code` AS `panel_code`',
    '`isolate_ordinal` AS `isolate_ordinal`',
    '`isolate_comment` AS `isolate_comment`',
    '`lims_no` AS `lims_no`',
    '`parse_status` AS `parse_status`',
    '`parser_version` AS `parser_version`',
    '`lifecycle_status` AS `lifecycle_status`',
    '`is_current` AS `is_current`',
    '`research_qi_only` AS `research_qi_only`',
    '`specimen_key` AS `specimen_key`',
    '`accession_identifier` AS `accession_identifier`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_microbiology_isolate_metadata; source history stays on the main research table.
CLINICAL_MICROBIOLOGY_ISOLATE_MANDATORY_RULES = {
    # The research surface keeps current, attributable microbiology rows.
    # identity_status = 'resolved' requires a linked person and record_status = 'active' removes retracted/superseded rows.
    # and attributable. Superseded versions and rows whose identity was never resolved are
    # not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_MICROBIOLOGY_ISOLATE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.microbiology_isolate.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 0 at the profile.
    "gold.clinical.microbiology_isolate.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 0 at the profile.
    "gold.clinical.microbiology_isolate.record_status.default_view_active":
        "record_status = 'active'",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_MICROBIOLOGY_ISOLATE_PERSON_SCOPE_RULES = {
    "gold.clinical.microbiology_isolate.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_MICROBIOLOGY_ISOLATE_ORPHAN_RULES = {
    "gold.clinical.microbiology_isolate.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_microbiology_isolate"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_MICROBIOLOGY_ISOLATE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MICROBIOLOGY_ISOLATE_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_MICROBIOLOGY_ISOLATE_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_MICROBIOLOGY_ISOLATE_ORPHAN_RULES)
def _gold_qc_clinical_microbiology_isolate():
    """Quality-controlled twin of journey_clinical.microbiology_isolate."""
    df = _qc("clinical_microbiology_isolate", CLINICAL_MICROBIOLOGY_ISOLATE_SELECT, fk_columns=("encounter_id",))
    return df


@dp.materialized_view(
    name=_n("gold_clinical.microbiology_isolate"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_microbiology_isolate():
    """Contract-v2 public twin of clinical_microbiology_isolate; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_microbiology_isolate")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`microbiology_isolate_id` AS `microbiology_isolate_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`encounter_link_method` AS `encounter_link_method`',  # GOLD_COLUMN_PARITY_V1
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`pathology_result_id` AS `pathology_result_id`',
        '`report_version_key` AS `report_version_key`',
        '`specimen_type_code` AS `specimen_type_code`',
        '`organism_text` AS `organism_text`',
        '`organism_snomed_code` AS `organism_snomed_code`',
        '`organism_omop_concept_id` AS `organism_omop_concept_id`',
        '`suspected_ind` AS `suspected_ind`',
        '`growth_grade` AS `growth_grade`',
        '`organism_code` AS `organism_code`',
        '`panel_code` AS `panel_code`',
        '`isolate_ordinal` AS `isolate_ordinal`',
        '`isolate_comment` AS `isolate_comment`',
        '`lims_no` AS `lims_no`',
        '`parse_status` AS `parse_status`',
        '`parser_version` AS `parser_version`',
        '`lifecycle_status` AS `lifecycle_status`',
        '`is_current` AS `is_current`',
        '`research_qi_only` AS `research_qi_only`',
        '`specimen_key` AS `specimen_key`',
        '`accession_identifier` AS `accession_identifier`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'clinical_microbiology_isolate': GoldRules(
        table='clinical_microbiology_isolate',
        select=CLINICAL_MICROBIOLOGY_ISOLATE_SELECT,
        mandatory=CLINICAL_MICROBIOLOGY_ISOLATE_MANDATORY_RULES,
        advisory=CLINICAL_MICROBIOLOGY_ISOLATE_ADVISORY_RULES,
        person_scope=CLINICAL_MICROBIOLOGY_ISOLATE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        value_rules={'gold.clinical.microbiology_isolate.organism_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                           'column': 'organism_snomed_code',
                                                                                           'companions': [],
                                                                                           'match': '`organism_snomed_code` '
                                                                                                    'IS '
                                                                                                    'NOT '
                                                                                                    'NULL '
                                                                                                    'AND '
                                                                                                    '(NOT '
                                                                                                    '`organism_snomed_code` '
                                                                                                    'RLIKE '
                                                                                                    "'^[1-9][0-9]{5,17}$' "
                                                                                                    'OR '
                                                                                                    'substring(`organism_snomed_code`, '
                                                                                                    'length(`organism_snomed_code`) '
                                                                                                    '- 2, '
                                                                                                    '2) '
                                                                                                    'NOT '
                                                                                                    'IN '
                                                                                                    "('00', "
                                                                                                    "'10') "
                                                                                                    'OR '
                                                                                                    '(substring(`organism_snomed_code`, '
                                                                                                    'length(`organism_snomed_code`) '
                                                                                                    '- 2, '
                                                                                                    '2) = '
                                                                                                    "'10' "
                                                                                                    'AND '
                                                                                                    'length(`organism_snomed_code`) '
                                                                                                    '< '
                                                                                                    '11))',
                                                                                           'table': 'clinical_microbiology_isolate'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_microbiology_isolate': {'columns': {'accession_identifier': 'Alias-resolved accession '
                                                                           'identifier.',
                                                   'confidentiality_code': 'Source confidentiality '
                                                                           'classification attached to the '
                                                                           'record for each microbiology '
                                                                           'isolate record. It is produced '
                                                                           'by the silver transformation and '
                                                                           'has no direct bronze-column '
                                                                           'lineage entry. Source code '
                                                                           'meanings and sentinel values are '
                                                                           'retained unless the pipeline '
                                                                           'explicitly maps them; null means '
                                                                           'no code was supplied.',
                                                   'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                                   'available.',
                                                   'encounter_link_method': 'How encounter_id was set '
                                                                            '(PATH_ENCOUNTER_V1). '
                                                                            "source_accession: the row's own "
                                                                            'Millennium source names the '
                                                                            'encounter. accession_single: '
                                                                            'every current Millennium source '
                                                                            'of the alias-resolved accession '
                                                                            'names this one encounter (how '
                                                                            'TFC LIMS rows inherit one once '
                                                                            'their accession is merged with '
                                                                            'the Millennium order). '
                                                                            'accession_multiple: those '
                                                                            'sources name different '
                                                                            'encounters, so encounter_id is '
                                                                            'NULL. none: the accession has '
                                                                            'no Millennium source, so '
                                                                            'encounter_id is NULL. Gold: '
                                                                            'encounter_id can still be NULL '
                                                                            'here when the encounter is not '
                                                                            'in the Gold encounter spine.',
                                                   'event_datetime': 'Accession sample/report/request '
                                                                     'fallback time.',
                                                   'event_datetime_local': 'Local-time accession event '
                                                                           'timestamp taken from the sample '
                                                                           'time, falling back to report '
                                                                           'time and then request time.',
                                                   'event_end_datetime': 'Event end time.',
                                                   'event_end_datetime_local': 'Local-time equivalent of the '
                                                                               'event end timestamp.',
                                                   'growth_grade': 'Reported growth grade.',
                                                   'is_current': 'Whether the isolate remains current.',
                                                   'isolate_comment': 'Text of the [~ annotation lines '
                                                                      'attached to this isolate, joined with '
                                                                      'single spaces; null when none.',
                                                   'isolate_ordinal': 'Zero-based position of this isolate '
                                                                      'block within the source result value.',
                                                   'lifecycle_status': 'Inherited source lifecycle.',
                                                   'lims_no': 'TFC/LIMS instance number the source row came '
                                                              'from; dictionaries are keyed per instance.',
                                                   'loaded_at': 'map_pathology_microbiology_isolate.ADC_UPDT '
                                                                'carried unchanged through its joins, '
                                                                'usable-code filtering and source/public '
                                                                'projections. Alias resolution and accession '
                                                                'identity add no load clocks; neither joined '
                                                                'report/parent timestamps nor the separately '
                                                                'aggregated accession evidence clock replace '
                                                                'this value. It is identical to '
                                                                'source_update_timestamp, not the clinical '
                                                                'event time or Silver refresh time.',
                                                   'microbiology_isolate_id': 'Pathology '
                                                                              'microbiology_isolate_id; '
                                                                              'primary key of this table.',
                                                   'organism_code': 'WinPath organism code exactly as packed '
                                                                    'in the antibiogram (e.g. ESCOL).',
                                                   'organism_omop_concept_id': 'Standard OMOP organism '
                                                                               'concept.',
                                                   'organism_snomed_code': 'SNOMED organism code.',
                                                   'organism_text': 'Organism as reported.',
                                                   'panel_code': 'WinPath susceptibility panel code '
                                                                 'following the organism (e.g. GU5, 3P); '
                                                                 'null when blank.',
                                                   'parse_status': 'ok, or unterminated_block when this '
                                                                   'isolate block had no terminator before '
                                                                   'the next opener; row-level '
                                                                   'orphan_continuation is recorded in '
                                                                   'validation.',
                                                   'parser_version': 'pathology_antibiogram parser version '
                                                                     'that produced the row.',
                                                   'pathology_result_id': 'Linked pathology-result fact.',
                                                   'patient_event_key': 'Deterministic SHA-256 event '
                                                                        'identity; stable cross-feed join '
                                                                        'key.',
                                                   'person_id': 'Resolved Millennium PERSON_ID as BIGINT '
                                                                'through the shared pathology identity '
                                                                'projection.',
                                                   'record_status': 'Retracted when '
                                                                    'map_pathology_microbiology_isolate.lifecycle_status '
                                                                    'is exactly cancelled or '
                                                                    'entered_in_error; otherwise superseded '
                                                                    'when is_current is false (null defaults '
                                                                    'true), otherwise active. Comparisons '
                                                                    'are case-sensitive without trimming; a '
                                                                    'null lifecycle_status does not select '
                                                                    'retraction. Retraction takes priority '
                                                                    'over supersession; clinical findings '
                                                                    'and joined identity/link fields are '
                                                                    'separate.',
                                                   'record_status_effective_from': 'Always null as a '
                                                                                   'TIMESTAMP in the current '
                                                                                   'microbiology-isolate '
                                                                                   'projection. No '
                                                                                   'source-status start is '
                                                                                   'supplied here; the '
                                                                                   "clinical event's "
                                                                                   'report/sample/request '
                                                                                   'date fallbacks and '
                                                                                   'ADC_UPDT are not used '
                                                                                   'for this field.',
                                                   'record_status_effective_to': 'Always null as a TIMESTAMP '
                                                                                 'in the current '
                                                                                 'microbiology-isolate '
                                                                                 'projection, including when '
                                                                                 'its status or '
                                                                                 'source-current flag '
                                                                                 'changes. No report, '
                                                                                 'clinical-event or '
                                                                                 'ingestion timestamp is '
                                                                                 'substituted as a status '
                                                                                 'end.',
                                                   'report_version_key': 'Deterministic report-version key.',
                                                   'research_qi_only': 'Research/QI release flag.',
                                                   'source_code': 'Organism text or code.',
                                                   'source_coding_system': 'Organism coding namespace.',
                                                   'source_display': 'Approved SNOMED organism code.',
                                                   'source_feed': 'Originating data feed responsible for the '
                                                                  'record for each microbiology isolate '
                                                                  'record. It is produced by the silver '
                                                                  'transformation and has no direct '
                                                                  'bronze-column lineage entry. Whitespace '
                                                                  'and source sentinel text are retained '
                                                                  'unless the pipeline explicitly normalizes '
                                                                  'them; null means no value was supplied.',
                                                   'source_update_timestamp': 'map_pathology_microbiology_isolate.ADC_UPDT '
                                                                              'carried unchanged from the '
                                                                              'contributing row, identical '
                                                                              'to loaded_at. No independent '
                                                                              'native application-update '
                                                                              'clock is projected here, and '
                                                                              'Silver refresh time is not '
                                                                              'substituted.',
                                                   'specimen_key': 'Deterministic specimen key.',
                                                   'specimen_type_code': 'Source specimen type.',
                                                   'subject_id_system': 'Identifier namespace selected for '
                                                                        '`subject_key`: Millennium person '
                                                                        'identifier first, then MRN, then '
                                                                        'NHS number; nosubject denotes the '
                                                                        'per-source-record fallback when '
                                                                        'none is present.',
                                                   'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                  'source identifier. Not salted, not '
                                                                  'secret; a stable join key across feeds '
                                                                  'where person_id is unresolved.',
                                                   'suspected_ind': 'Whether the organism is hedged or '
                                                                    'suspected.',
                                                   'vip_ind': 'Indicator that the source record carries VIP '
                                                              'handling status for each microbiology isolate '
                                                              'record. It is produced by the silver '
                                                              'transformation and has no direct '
                                                              'bronze-column lineage entry. Null means the '
                                                              'source did not state the indicator and must '
                                                              'not be interpreted as false.',
                                                   'withheld_identity_ind': 'Withheld identity indicator.'},
                                       'quantity_override': True,
                                       'table': 'Internal quality-controlled twin of '
                                                'clinical_microbiology_isolate: the Gold repairs, nulls and '
                                                'expectations are applied here and the published Gold MV '
                                                'reads this object. Materialized rather than temporary so '
                                                'the published MV can refresh incrementally.'},
 'clinical_microbiology_isolate': {'columns': {'accession_identifier': 'Alias-resolved accession identifier.',
                                               'confidentiality_code': 'Source confidentiality '
                                                                       'classification attached to the '
                                                                       'record for each microbiology isolate '
                                                                       'record. It is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'encounter_id': 'Millennium ENCNTR_ID when the source or '
                                                               'accession names the encounter (see '
                                                               'encounter_link_method); NULL otherwise. A '
                                                               'spell-containment derived encounter for NULL '
                                                               'rows is published separately in '
                                                               'clinical_microbiology_isolate_encounter_attribution '
                                                               '(J32), never written here.',
                                               'encounter_link_method': 'How encounter_id was set '
                                                                        '(PATH_ENCOUNTER_V1). '
                                                                        "source_accession: the row's own "
                                                                        'Millennium source names the '
                                                                        'encounter. accession_single: every '
                                                                        'current Millennium source of the '
                                                                        'alias-resolved accession names this '
                                                                        'one encounter (how TFC LIMS rows '
                                                                        'inherit one once their accession is '
                                                                        'merged with the Millennium order). '
                                                                        'accession_multiple: those sources '
                                                                        'name different encounters, so '
                                                                        'encounter_id is NULL. none: the '
                                                                        'accession has no Millennium source, '
                                                                        'so encounter_id is NULL. Gold: '
                                                                        'encounter_id can still be NULL here '
                                                                        'when the encounter is not in the '
                                                                        'Gold encounter spine.',
                                               'event_datetime': 'Accession sample/report/request fallback '
                                                                 'time.',
                                               'event_datetime_local': 'Local-time accession event timestamp '
                                                                       'taken from the sample time, falling '
                                                                       'back to report time and then request '
                                                                       'time.',
                                               'event_end_datetime': 'Event end time.',
                                               'event_end_datetime_local': 'Local-time equivalent of the '
                                                                           'event end timestamp.',
                                               'growth_grade': 'Reported growth grade.',
                                               'is_current': 'Whether the isolate remains current.',
                                               'isolate_comment': 'Text of the [~ annotation lines attached '
                                                                  'to this isolate, joined with single '
                                                                  'spaces; null when none.',
                                               'isolate_ordinal': 'Zero-based position of this isolate block '
                                                                  'within the source result value.',
                                               'lifecycle_status': 'Inherited source lifecycle.',
                                               'lims_no': 'TFC/LIMS instance number the source row came '
                                                          'from; dictionaries are keyed per instance.',
                                               'loaded_at': 'map_pathology_microbiology_isolate.ADC_UPDT '
                                                            'carried unchanged through its joins, '
                                                            'usable-code filtering and source/public '
                                                            'projections. Alias resolution and accession '
                                                            'identity add no load clocks; neither joined '
                                                            'report/parent timestamps nor the separately '
                                                            'aggregated accession evidence clock replace '
                                                            'this value. It is identical to '
                                                            'source_update_timestamp, not the clinical event '
                                                            'time or Silver refresh time.',
                                               'microbiology_isolate_id': 'Pathology '
                                                                          'microbiology_isolate_id; primary '
                                                                          'key of this table.',
                                               'organism_code': 'WinPath organism code exactly as packed in '
                                                                'the antibiogram (e.g. ESCOL).',
                                               'organism_omop_concept_id': 'Standard OMOP organism concept.',
                                               'organism_snomed_code': 'SNOMED organism code.',
                                               'organism_text': 'Organism as reported.',
                                               'panel_code': 'WinPath susceptibility panel code following '
                                                             'the organism (e.g. GU5, 3P); null when blank.',
                                               'parse_status': 'ok, or unterminated_block when this isolate '
                                                               'block had no terminator before the next '
                                                               'opener; row-level orphan_continuation is '
                                                               'recorded in validation.',
                                               'parser_version': 'pathology_antibiogram parser version that '
                                                                 'produced the row.',
                                               'pathology_result_id': 'Linked pathology-result fact.',
                                               'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                    'stable cross-feed join key.',
                                               'person_id': 'Resolved Millennium PERSON_ID as BIGINT through '
                                                            'the shared pathology identity projection.',
                                               'record_status': 'Retracted when '
                                                                'map_pathology_microbiology_isolate.lifecycle_status '
                                                                'is exactly cancelled or entered_in_error; '
                                                                'otherwise superseded when is_current is '
                                                                'false (null defaults true), otherwise '
                                                                'active. Comparisons are case-sensitive '
                                                                'without trimming; a null lifecycle_status '
                                                                'does not select retraction. Retraction '
                                                                'takes priority over supersession; clinical '
                                                                'findings and joined identity/link fields '
                                                                'are separate.',
                                               'record_status_effective_from': 'Always null as a TIMESTAMP '
                                                                               'in the current '
                                                                               'microbiology-isolate '
                                                                               'projection. No source-status '
                                                                               'start is supplied here; the '
                                                                               "clinical event's "
                                                                               'report/sample/request date '
                                                                               'fallbacks and ADC_UPDT are '
                                                                               'not used for this field.',
                                               'record_status_effective_to': 'Always null as a TIMESTAMP in '
                                                                             'the current '
                                                                             'microbiology-isolate '
                                                                             'projection, including when its '
                                                                             'status or source-current flag '
                                                                             'changes. No report, '
                                                                             'clinical-event or ingestion '
                                                                             'timestamp is substituted as a '
                                                                             'status end.',
                                               'report_version_key': 'Deterministic report-version key.',
                                               'research_qi_only': 'Research/QI release flag.',
                                               'source_code': 'Organism text or code.',
                                               'source_coding_system': 'Organism coding namespace.',
                                               'source_display': 'Approved SNOMED organism code.',
                                               'source_feed': 'Originating data feed responsible for the '
                                                              'record for each microbiology isolate record. '
                                                              'It is produced by the silver transformation '
                                                              'and has no direct bronze-column lineage '
                                                              'entry. Whitespace and source sentinel text '
                                                              'are retained unless the pipeline explicitly '
                                                              'normalizes them; null means no value was '
                                                              'supplied.',
                                               'source_update_timestamp': 'map_pathology_microbiology_isolate.ADC_UPDT '
                                                                          'carried unchanged from the '
                                                                          'contributing row, identical to '
                                                                          'loaded_at. No independent native '
                                                                          'application-update clock is '
                                                                          'projected here, and Silver '
                                                                          'refresh time is not substituted.',
                                               'specimen_key': 'Deterministic specimen key.',
                                               'specimen_type_code': 'Source specimen type.',
                                               'subject_id_system': 'Identifier namespace selected for '
                                                                    '`subject_key`: Millennium person '
                                                                    'identifier first, then MRN, then NHS '
                                                                    'number; nosubject denotes the '
                                                                    'per-source-record fallback when none is '
                                                                    'present.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'suspected_ind': 'Whether the organism is hedged or '
                                                                'suspected.',
                                               'vip_ind': 'Indicator that the source record carries VIP '
                                                          'handling status for each microbiology isolate '
                                                          'record. It is produced by the silver '
                                                          'transformation and has no direct bronze-column '
                                                          'lineage entry. Null means the source did not '
                                                          'state the indicator and must not be interpreted '
                                                          'as false.',
                                               'withheld_identity_ind': 'Withheld identity indicator.'},
                                   'quantity_override': False,
                                   'table': 'One organism/isolate finding per accession, parsed from the '
                                            'packed WinPath antibiogram in map_pathology (S2.5, parser '
                                            '1.0.0). Person linkage follows the accession identity; '
                                            'unresolved rows stay in silver and drop at gold. Gold keeps '
                                            "identity_status='resolved' AND record_status='active'. Gold QC "
                                            'twin of the silver product: 0 columns are repaired or nulled, 1 '
                                            'rule(s) drop rows, 2 check(s) are advisory. Each rule states '
                                            'its reason in the pipeline notebook, and Lakeflow expectation '
                                            'metrics report what every rule matched on each update.'}}
