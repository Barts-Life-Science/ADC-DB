"""Gold clinical.genomic_test: 2 dataset(s).

- datasets: _qc_clinical_genomic_test, clinical_genomic_test
- moved verbatim from: 2_gold_clinical_events:2116, 2_gold_clinical_events:2176, 2_gold_clinical_events:2183, 2_gold_clinical_events:2263, 2_gold_clinical_events:2268, 2_gold_clinical_events:2272, 2_gold_clinical_events:2290

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.genomic_test ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_GENOMIC_TEST_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`genetic_test_id` AS `genetic_test_id`',
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
    '`assay_code` AS `assay_code`',
    '`assay_name` AS `assay_name`',
    '`method` AS `method`',
    '`analysis_context` AS `analysis_context`',
    '`overall_result_status` AS `overall_result_status`',
    '`panel_code` AS `panel_code`',
    '`panel_version` AS `panel_version`',
    '`panel_version_inferred` AS `panel_version_inferred`',
    '`parser_profile_id` AS `parser_profile_id`',
    '`report_version_key` AS `report_version_key`',
    '`pathology_report_key` AS `pathology_report_key`',
    '`specimen_key` AS `specimen_key`',
    '`accession_identifier` AS `accession_identifier`',
    '`test_snomed_code` AS `test_snomed_code`',
    '`test_loinc_code` AS `test_loinc_code`',
    '`test_omop_concept_id` AS `test_omop_concept_id`',
    '`assay_component` AS `assay_component`',  # GEN4_GOLD_V1
    '`is_primary_component` AS `is_primary_component`',  # GEN4_GOLD_V1
    '`nhs_test_directory_code` AS `nhs_test_directory_code`',  # GEN4_GOLD_V1
    '`test_code_method` AS `test_code_method`',  # GEN4_GOLD_V1
    '`sample_type_site_raw` AS `sample_type_site_raw`',  # GEN4_GOLD_V1
    '`sample_site_snomed_code` AS `sample_site_snomed_code`',  # GEN4_GOLD_V1
    '`block_raw` AS `block_raw`',  # GEN4_GOLD_V1
    '`tumour_content_raw` AS `tumour_content_raw`',  # GEN4_GOLD_V1
    '`tumour_content_pct` AS `tumour_content_pct`',  # GEN4_GOLD_V1
    '`clinical_details_raw` AS `clinical_details_raw`',  # GEN4_GOLD_V1
    '`indication_snomed_code` AS `indication_snomed_code`',  # GEN4_GOLD_V1
    '`interpretation_text` AS `interpretation_text`',  # GEN4_GOLD_V1
    '`predicted_response` AS `predicted_response`',  # GEN4_GOLD_V1
    '`predicted_response_drug_class` AS `predicted_response_drug_class`',  # GEN4_GOLD_V1
    '`panel_genes_not_assessed` AS `panel_genes_not_assessed`',  # GEN4_GOLD_V1
    '`is_current` AS `is_current`',
    '`research_qi_only` AS `research_qi_only`',
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


# contract v2: QC/batch inputs come from internal _clinical_genomic_test_metadata; source history stays on the main research table.
CLINICAL_GENOMIC_TEST_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 40,540 rows of 51,845
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_GENOMIC_TEST_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.genomic_test.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 11,305 of 51,845 at the profile.
    "gold.clinical.genomic_test.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 282 of 51,845 rows (0.544%) when profiled on 2026-08-24.
    "gold.clinical.genomic_test.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_GENOMIC_TEST_PERSON_SCOPE_RULES = {
    "gold.clinical.genomic_test.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_GENOMIC_TEST_ORPHAN_RULES = {
    "gold.clinical.genomic_test.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_genomic_test"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_GENOMIC_TEST_MANDATORY_RULES)
@dp.expect_all(CLINICAL_GENOMIC_TEST_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_GENOMIC_TEST_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_GENOMIC_TEST_ORPHAN_RULES)
def _gold_qc_clinical_genomic_test():
    """Quality-controlled twin of journey_clinical.genomic_test."""
    df = _qc(
        "clinical_genomic_test",
        CLINICAL_GENOMIC_TEST_SELECT, fk_columns=("encounter_id",),
        date_flags=["event_after_death_30d"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.genomic_test"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_genomic_test():
    """Contract-v2 public twin of clinical_genomic_test; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_genomic_test")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`genetic_test_id` AS `genetic_test_id`',
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
        '`assay_code` AS `assay_code`',
        '`assay_name` AS `assay_name`',
        '`method` AS `method`',
        '`analysis_context` AS `analysis_context`',
        '`overall_result_status` AS `overall_result_status`',
        '`panel_code` AS `panel_code`',
        '`panel_version` AS `panel_version`',
        '`panel_version_inferred` AS `panel_version_inferred`',
        '`parser_profile_id` AS `parser_profile_id`',
        '`report_version_key` AS `report_version_key`',
        '`pathology_report_key` AS `pathology_report_key`',
        '`specimen_key` AS `specimen_key`',
        '`accession_identifier` AS `accession_identifier`',
        '`test_snomed_code` AS `test_snomed_code`',
        '`test_loinc_code` AS `test_loinc_code`',
        '`test_omop_concept_id` AS `test_omop_concept_id`',
        '`assay_component` AS `assay_component`',  # GEN4_GOLD_V1
        '`is_primary_component` AS `is_primary_component`',  # GEN4_GOLD_V1
        '`nhs_test_directory_code` AS `nhs_test_directory_code`',  # GEN4_GOLD_V1
        '`test_code_method` AS `test_code_method`',  # GEN4_GOLD_V1
        '`sample_type_site_raw` AS `sample_type_site_raw`',  # GEN4_GOLD_V1
        '`sample_site_snomed_code` AS `sample_site_snomed_code`',  # GEN4_GOLD_V1
        '`block_raw` AS `block_raw`',  # GEN4_GOLD_V1
        '`tumour_content_raw` AS `tumour_content_raw`',  # GEN4_GOLD_V1
        '`tumour_content_pct` AS `tumour_content_pct`',  # GEN4_GOLD_V1
        '`clinical_details_raw` AS `clinical_details_raw`',  # GEN4_GOLD_V1
        '`indication_snomed_code` AS `indication_snomed_code`',  # GEN4_GOLD_V1
        '`interpretation_text` AS `interpretation_text`',  # GEN4_GOLD_V1
        '`predicted_response` AS `predicted_response`',  # GEN4_GOLD_V1
        '`predicted_response_drug_class` AS `predicted_response_drug_class`',  # GEN4_GOLD_V1
        '`panel_genes_not_assessed` AS `panel_genes_not_assessed`',  # GEN4_GOLD_V1
        '`is_current` AS `is_current`',
        '`research_qi_only` AS `research_qi_only`',
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
    'clinical_genomic_test': GoldRules(
        table='clinical_genomic_test',
        select=CLINICAL_GENOMIC_TEST_SELECT,
        mandatory=CLINICAL_GENOMIC_TEST_MANDATORY_RULES,
        advisory=CLINICAL_GENOMIC_TEST_ADVISORY_RULES,
        person_scope=CLINICAL_GENOMIC_TEST_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d',),
        value_rules={'gold.clinical.genomic_test.indication_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                     'column': 'indication_snomed_code',
                                                                                     'companions': [],
                                                                                     'match': '`indication_snomed_code` '
                                                                                              'IS NOT NULL '
                                                                                              'AND (NOT '
                                                                                              '`indication_snomed_code` '
                                                                                              'RLIKE '
                                                                                              "'^[1-9][0-9]{5,17}$' "
                                                                                              'OR '
                                                                                              'substring(`indication_snomed_code`, '
                                                                                              'length(`indication_snomed_code`) '
                                                                                              '- 2, 2) NOT '
                                                                                              "IN ('00', "
                                                                                              "'10') OR "
                                                                                              '(substring(`indication_snomed_code`, '
                                                                                              'length(`indication_snomed_code`) '
                                                                                              '- 2, 2) = '
                                                                                              "'10' AND "
                                                                                              'length(`indication_snomed_code`) '
                                                                                              '< 11))',
                                                                                     'table': 'clinical_genomic_test'},
         'gold.clinical.genomic_test.sample_site_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                      'column': 'sample_site_snomed_code',
                                                                                      'companions': [],
                                                                                      'match': '`sample_site_snomed_code` '
                                                                                               'IS NOT '
                                                                                               'NULL AND '
                                                                                               '(NOT '
                                                                                               '`sample_site_snomed_code` '
                                                                                               'RLIKE '
                                                                                               "'^[1-9][0-9]{5,17}$' "
                                                                                               'OR '
                                                                                               'substring(`sample_site_snomed_code`, '
                                                                                               'length(`sample_site_snomed_code`) '
                                                                                               '- 2, 2) '
                                                                                               'NOT IN '
                                                                                               "('00', "
                                                                                               "'10') OR "
                                                                                               '(substring(`sample_site_snomed_code`, '
                                                                                               'length(`sample_site_snomed_code`) '
                                                                                               '- 2, 2) = '
                                                                                               "'10' AND "
                                                                                               'length(`sample_site_snomed_code`) '
                                                                                               '< 11))',
                                                                                      'table': 'clinical_genomic_test'},
         'gold.clinical.genomic_test.test_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                               'column': 'test_snomed_code',
                                                                               'companions': [],
                                                                               'match': '`test_snomed_code` '
                                                                                        'IS NOT NULL AND '
                                                                                        '(NOT '
                                                                                        '`test_snomed_code` '
                                                                                        'RLIKE '
                                                                                        "'^[1-9][0-9]{5,17}$' "
                                                                                        'OR '
                                                                                        'substring(`test_snomed_code`, '
                                                                                        'length(`test_snomed_code`) '
                                                                                        '- 2, 2) NOT IN '
                                                                                        "('00', '10') OR "
                                                                                        '(substring(`test_snomed_code`, '
                                                                                        'length(`test_snomed_code`) '
                                                                                        "- 2, 2) = '10' "
                                                                                        'AND '
                                                                                        'length(`test_snomed_code`) '
                                                                                        '< 11))',
                                                                               'table': 'clinical_genomic_test'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_genomic_test': {'columns': {'accession_identifier': 'Alias-resolved accession identifier.',
                                           'analysis_context': 'Somatic or germline analysis context.',
                                           'assay_code': 'Source assay or report code.',
                                           'assay_component': 'GEN4 assay within the report: mngs_dna, '
                                                              'mngs_rna, tngs, flt3, npm1, bcr_abl1_qual. A '
                                                              'myeloid NGS report has a DNA and (when run) '
                                                              'an RNA panel row; an FLT3 report also an NPM1 '
                                                              'row.',
                                           'assay_name': 'Resolved assay name.',
                                           'block_raw': 'Histology block identifier as reported (an '
                                                        'identifier: governed by ig tags, published in '
                                                        'full).',
                                           'clinical_details_raw': 'Clinical details/information section of '
                                                                   'the genetics report (free text).',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each genomic '
                                                                   'test record. It is produced by the '
                                                                   'silver transformation and has no direct '
                                                                   'bronze-column lineage entry. Source code '
                                                                   'meanings and sentinel values are '
                                                                   'retained unless the pipeline explicitly '
                                                                   'maps them; null means no code was '
                                                                   'supplied.',
                                           'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                           'encounter_link_method': 'How encounter_id was set '
                                                                    '(PATH_ENCOUNTER_V1). source_accession: '
                                                                    "the row's own Millennium source names "
                                                                    'the encounter. accession_single: every '
                                                                    'current Millennium source of the '
                                                                    'alias-resolved accession names this one '
                                                                    'encounter (how TFC LIMS rows inherit '
                                                                    'one once their accession is merged with '
                                                                    'the Millennium order). '
                                                                    'accession_multiple: those sources name '
                                                                    'different encounters, so encounter_id '
                                                                    'is NULL. none: the accession has no '
                                                                    'Millennium source, so encounter_id is '
                                                                    'NULL. PATH_ENCOUNTER_V2 (genomic only): '
                                                                    'encounter_window_single / '
                                                                    'encounter_window_same_day link a '
                                                                    'Barts-site LIMS accession to the '
                                                                    "patient's encounter containing the "
                                                                    'sample date; encounter_window_multiple '
                                                                    'and none_external_referral leave it '
                                                                    'NULL. Gold: encounter_id can still be '
                                                                    'NULL here when the encounter is not in '
                                                                    'the Gold encounter spine.',
                                           'event_datetime': 'Report issue time with accession fallback.',
                                           'event_datetime_local': 'Local-time report issue time, falling '
                                                                   'back to the accession sample or request '
                                                                   'time when the report issue time is '
                                                                   'unavailable.',
                                           'event_end_datetime': 'Event end time.',
                                           'event_end_datetime_local': 'Event end time expressed in local '
                                                                       'time.',
                                           'genetic_test_id': 'Pathology genetic_test_id; primary key of '
                                                              'this table.',
                                           'indication_snomed_code': 'SNOMED disorder for the stated '
                                                                     'indication, from deterministic term '
                                                                     'rules over clinical_details_raw.',
                                           'interpretation_text': "The laboratory's clinical interpretation "
                                                                  'narrative (free text, up to 4,000 '
                                                                  'characters).',
                                           'is_current': 'Whether the source report version is current.',
                                           'is_primary_component': "True for the report's primary assay, "
                                                                   'which keeps the pre-GEN4 '
                                                                   'genetic_test_id; secondary assays '
                                                                   '(mngs_rna, npm1) have their own ids.',
                                           'loaded_at': 'map_pathology_genetic_test.ADC_UPDT carried '
                                                        'unchanged through its joins, usable-code filtering '
                                                        'and source/public projections. Alias resolution and '
                                                        'accession identity add no load clocks; neither '
                                                        'joined report/parent timestamps nor the separately '
                                                        'aggregated accession evidence clock replace this '
                                                        'value. It is identical to source_update_timestamp, '
                                                        'not the clinical event time or Silver refresh time.',
                                           'method': 'Reported assay method.',
                                           'nhs_test_directory_code': 'NHS National Genomic Test Directory '
                                                                      'code(s) for the assay '
                                                                      "('|'-separated), rule-mapped. © NHS "
                                                                      'England reference codes.',
                                           'overall_result_status': 'Source assay-level result status.',
                                           'panel_code': 'Governed panel code.',
                                           'panel_genes_not_assessed': 'True when the report states genes '
                                                                       'outside its result table were not '
                                                                       'assessed or reported.',
                                           'panel_version': 'Explicit or inferred panel version.',
                                           'panel_version_inferred': 'Whether panel version was inferred.',
                                           'parser_profile_id': 'Deterministic parser profile.',
                                           'pathology_report_key': 'Deterministic pathology-report series '
                                                                   'key.',
                                           'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                'stable cross-feed join key.',
                                           'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                           'predicted_response': 'likely_responsive or unlikely_responsive, '
                                                                 'only when the report itself states it.',
                                           'predicted_response_drug_class': 'Drug class named in that '
                                                                            'statement (e.g. EGFR TKI).',
                                           'record_status': 'Superseded when '
                                                            'map_pathology_genetic_test.is_current is false, '
                                                            'otherwise active; missing is_current defaults '
                                                            'true. No retracted status is emitted. The '
                                                            "assay's overall_result_status is separate, and "
                                                            'joined report/identity fields do not set this '
                                                            'label.',
                                           'record_status_effective_from': 'Always null as a TIMESTAMP in '
                                                                           'the current genomic-test '
                                                                           'projection. No source-status '
                                                                           'start is supplied here; the '
                                                                           "clinical event's "
                                                                           'report/sample/request date '
                                                                           'fallbacks and ADC_UPDT are not '
                                                                           'used for this field.',
                                           'record_status_effective_to': 'Always null as a TIMESTAMP in the '
                                                                         'current genomic-test projection, '
                                                                         'including when its status or '
                                                                         'source-current flag changes. No '
                                                                         'report, clinical-event or '
                                                                         'ingestion timestamp is substituted '
                                                                         'as a status end.',
                                           'report_version_key': 'Deterministic report-version key.',
                                           'research_qi_only': 'Research/QI release flag.',
                                           'sample_site_snomed_code': 'SNOMED body structure for the sample '
                                                                      'site, from deterministic term rules '
                                                                      'over sample_type_site_raw.',
                                           'sample_type_site_raw': 'Sample type/site exactly as written in '
                                                                   'the report.',
                                           'source_code': 'Source assay code.',
                                           'source_coding_system': 'Assay coding namespace.',
                                           'source_display': 'Source assay display.',
                                           'source_feed': 'Originating data feed responsible for the record '
                                                          'for each genomic test record. It is produced by '
                                                          'the silver transformation and has no direct '
                                                          'bronze-column lineage entry. Whitespace and '
                                                          'source sentinel text are retained unless the '
                                                          'pipeline explicitly normalizes them; null means '
                                                          'no value was supplied.',
                                           'source_update_timestamp': 'map_pathology_genetic_test.ADC_UPDT '
                                                                      'carried unchanged from the '
                                                                      'contributing row, identical to '
                                                                      'loaded_at. No independent native '
                                                                      'application-update clock is projected '
                                                                      'here, and Silver refresh time is not '
                                                                      'substituted.',
                                           'specimen_key': 'Deterministic specimen key.',
                                           'subject_id_system': 'Identifier namespace selected for '
                                                                '`subject_key`: Millennium person identifier '
                                                                'first, then MRN, then NHS number; nosubject '
                                                                'denotes the per-source-record fallback when '
                                                                'none is present.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'test_code_method': 'How '
                                                               'test_loinc_code/test_snomed_code/test_omop_concept_id '
                                                               'were set: assay_rule:<rule_id> '
                                                               '(pathology_genetic_assay_concept).',
                                           'test_loinc_code': 'Approved LOINC assay code.',
                                           'test_omop_concept_id': 'Standard OMOP assay concept.',
                                           'test_snomed_code': 'Approved SNOMED assay code.',
                                           'tumour_content_pct': 'Tumour content percent when reported '
                                                                 "without a qualifier; NULL for '<10%' style "
                                                                 'values.',
                                           'tumour_content_raw': "Tumour content as reported (e.g. '25%', "
                                                                 "'<10%').",
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each genomic test record. It is produced '
                                                      'by the silver transformation and has no direct '
                                                      'bronze-column lineage entry. Null means the source '
                                                      'did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Withheld identity indicator.'},
                               'quantity_override': True,
                               'table': 'Internal quality-controlled twin of clinical_genomic_test: the Gold '
                                        'repairs, nulls and expectations are applied here and the published '
                                        'Gold MV reads this object. Materialized rather than temporary so '
                                        'the published MV can refresh incrementally.'},
 'clinical_genomic_test': {'columns': {'accession_identifier': 'Alias-resolved accession identifier.',
                                       'analysis_context': 'Somatic or germline analysis context.',
                                       'assay_code': 'Source assay or report code.',
                                       'assay_component': 'GEN4 assay within the report: mngs_dna, mngs_rna, '
                                                          'tngs, flt3, npm1, bcr_abl1_qual. A myeloid NGS '
                                                          'report has a DNA and (when run) an RNA panel row; '
                                                          'an FLT3 report also an NPM1 row.',
                                       'assay_name': 'Resolved assay name.',
                                       'block_raw': 'Histology block identifier as reported (an identifier: '
                                                    'governed by ig tags, published in full).',
                                       'clinical_details_raw': 'Clinical details/information section of the '
                                                               'genetics report (free text).',
                                       'confidentiality_code': 'Source confidentiality classification '
                                                               'attached to the record for each genomic test '
                                                               'record. It is produced by the silver '
                                                               'transformation and has no direct '
                                                               'bronze-column lineage entry. Source code '
                                                               'meanings and sentinel values are retained '
                                                               'unless the pipeline explicitly maps them; '
                                                               'null means no code was supplied.',
                                       'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                       'encounter_link_method': 'How encounter_id was set '
                                                                '(PATH_ENCOUNTER_V1). source_accession: the '
                                                                "row's own Millennium source names the "
                                                                'encounter. accession_single: every current '
                                                                'Millennium source of the alias-resolved '
                                                                'accession names this one encounter (how TFC '
                                                                'LIMS rows inherit one once their accession '
                                                                'is merged with the Millennium order). '
                                                                'accession_multiple: those sources name '
                                                                'different encounters, so encounter_id is '
                                                                'NULL. none: the accession has no Millennium '
                                                                'source, so encounter_id is NULL. '
                                                                'PATH_ENCOUNTER_V2 (genomic only): '
                                                                'encounter_window_single / '
                                                                'encounter_window_same_day link a Barts-site '
                                                                "LIMS accession to the patient's encounter "
                                                                'containing the sample date; '
                                                                'encounter_window_multiple and '
                                                                'none_external_referral leave it NULL. Gold: '
                                                                'encounter_id can still be NULL here when '
                                                                'the encounter is not in the Gold encounter '
                                                                'spine.',
                                       'event_datetime': 'Report issue time with accession fallback.',
                                       'event_datetime_local': 'Local-time report issue time, falling back '
                                                               'to the accession sample or request time when '
                                                               'the report issue time is unavailable.',
                                       'event_end_datetime': 'Event end time.',
                                       'event_end_datetime_local': 'Event end time expressed in local time.',
                                       'genetic_test_id': 'Pathology genetic_test_id; primary key of this '
                                                          'table.',
                                       'indication_snomed_code': 'SNOMED disorder for the stated indication, '
                                                                 'from deterministic term rules over '
                                                                 'clinical_details_raw.',
                                       'interpretation_text': "The laboratory's clinical interpretation "
                                                              'narrative (free text, up to 4,000 '
                                                              'characters).',
                                       'is_current': 'Whether the source report version is current.',
                                       'is_primary_component': "True for the report's primary assay, which "
                                                               'keeps the pre-GEN4 genetic_test_id; '
                                                               'secondary assays (mngs_rna, npm1) have their '
                                                               'own ids.',
                                       'loaded_at': 'map_pathology_genetic_test.ADC_UPDT carried unchanged '
                                                    'through its joins, usable-code filtering and '
                                                    'source/public projections. Alias resolution and '
                                                    'accession identity add no load clocks; neither joined '
                                                    'report/parent timestamps nor the separately aggregated '
                                                    'accession evidence clock replace this value. It is '
                                                    'identical to source_update_timestamp, not the clinical '
                                                    'event time or Silver refresh time.',
                                       'method': 'Reported assay method.',
                                       'nhs_test_directory_code': 'NHS National Genomic Test Directory '
                                                                  "code(s) for the assay ('|'-separated), "
                                                                  'rule-mapped. © NHS England reference '
                                                                  'codes.',
                                       'overall_result_status': 'Source assay-level result status.',
                                       'panel_code': 'Governed panel code.',
                                       'panel_genes_not_assessed': 'True when the report states genes '
                                                                   'outside its result table were not '
                                                                   'assessed or reported.',
                                       'panel_version': 'Explicit or inferred panel version.',
                                       'panel_version_inferred': 'Whether panel version was inferred.',
                                       'parser_profile_id': 'Deterministic parser profile.',
                                       'pathology_report_key': 'Deterministic pathology-report series key.',
                                       'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                            'cross-feed join key.',
                                       'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                       'predicted_response': 'likely_responsive or unlikely_responsive, only '
                                                             'when the report itself states it.',
                                       'predicted_response_drug_class': 'Drug class named in that statement '
                                                                        '(e.g. EGFR TKI).',
                                       'record_status': 'Superseded when '
                                                        'map_pathology_genetic_test.is_current is false, '
                                                        'otherwise active; missing is_current defaults true. '
                                                        "No retracted status is emitted. The assay's "
                                                        'overall_result_status is separate, and joined '
                                                        'report/identity fields do not set this label.',
                                       'record_status_effective_from': 'Not supplied by the genomic source: '
                                                                       'always NULL. Use the report or '
                                                                       'sample dates for timing.',
                                       'record_status_effective_to': 'Not supplied by the genomic source: '
                                                                     'always NULL, including on superseded '
                                                                     'versions.',
                                       'report_version_key': 'Deterministic report-version key.',
                                       'research_qi_only': 'Research/QI release flag.',
                                       'sample_site_snomed_code': 'SNOMED body structure for the sample '
                                                                  'site, from deterministic term rules over '
                                                                  'sample_type_site_raw.',
                                       'sample_type_site_raw': 'Sample type/site exactly as written in the '
                                                               'report.',
                                       'source_code': 'Source assay code.',
                                       'source_coding_system': 'Assay coding namespace.',
                                       'source_display': 'Source assay display.',
                                       'source_feed': 'Originating data feed responsible for the record for '
                                                      'each genomic test record. It is produced by the '
                                                      'silver transformation and has no direct bronze-column '
                                                      'lineage entry. Whitespace and source sentinel text '
                                                      'are retained unless the pipeline explicitly '
                                                      'normalizes them; null means no value was supplied.',
                                       'source_update_timestamp': 'map_pathology_genetic_test.ADC_UPDT '
                                                                  'carried unchanged from the contributing '
                                                                  'row, identical to loaded_at. No '
                                                                  'independent native application-update '
                                                                  'clock is projected here, and Silver '
                                                                  'refresh time is not substituted.',
                                       'specimen_key': 'Deterministic specimen key.',
                                       'subject_id_system': 'Identifier namespace selected for '
                                                            '`subject_key`: Millennium person identifier '
                                                            'first, then MRN, then NHS number; nosubject '
                                                            'denotes the per-source-record fallback when '
                                                            'none is present.',
                                       'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                      'identifier. Not salted, not secret; a stable join key '
                                                      'across feeds where person_id is unresolved.',
                                       'test_code_method': 'How '
                                                           'test_loinc_code/test_snomed_code/test_omop_concept_id '
                                                           'were set: assay_rule:<rule_id> '
                                                           '(pathology_genetic_assay_concept).',
                                       'test_loinc_code': 'Approved LOINC assay code.',
                                       'test_omop_concept_id': 'Standard OMOP assay concept.',
                                       'test_snomed_code': 'Approved SNOMED assay code.',
                                       'tumour_content_pct': 'Tumour content percent when reported without a '
                                                             "qualifier; NULL for '<10%' style values.",
                                       'tumour_content_raw': "Tumour content as reported (e.g. '25%', "
                                                             "'<10%').",
                                       'vip_ind': 'Indicator that the source record carries VIP handling '
                                                  'status for each genomic test record. It is produced by '
                                                  'the silver transformation and has no direct bronze-column '
                                                  'lineage entry. Null means the source did not state the '
                                                  'indicator and must not be interpreted as false.',
                                       'withheld_identity_ind': 'Withheld identity indicator.'},
                           'quantity_override': False,
                           'table': 'One molecular or cytogenetic assay per report version; every version is '
                                    'published and record_status separates active from superseded '
                                    '(map_pathology_genetic_test.is_current).'}}
