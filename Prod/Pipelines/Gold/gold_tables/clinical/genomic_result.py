"""Gold clinical.genomic_result: 2 dataset(s).

- datasets: _qc_clinical_genomic_result, clinical_genomic_result
- moved verbatim from: 2_gold_clinical_events:1790, 2_gold_clinical_events:1877, 2_gold_clinical_events:1885, 2_gold_clinical_events:1990, 2_gold_clinical_events:1995, 2_gold_clinical_events:1999, 2_gold_clinical_events:2013

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.genomic_result ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_GENOMIC_RESULT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`genetic_result_id` AS `genetic_result_id`',
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
    '`genomic_test_key` AS `genomic_test_key`',
    '`report_version_key` AS `report_version_key`',
    '`hgnc_id` AS `hgnc_id`',
    '`reported_gene_symbol` AS `reported_gene_symbol`',
    '`normalized_gene_symbol` AS `normalized_gene_symbol`',
    '`partner_hgnc_id` AS `partner_hgnc_id`',
    '`partner_gene_symbol` AS `partner_gene_symbol`',
    '`alteration_type` AS `alteration_type`',
    '`detection_status` AS `detection_status`',
    '`hgvs_c_raw` AS `hgvs_c_raw`',
    '`hgvs_c_parsed` AS `hgvs_c_parsed`',
    '`hgvs_p_raw` AS `hgvs_p_raw`',
    '`hgvs_p_parsed` AS `hgvs_p_parsed`',
    '`transcript` AS `transcript`',
    '`hgvs_validation_status` AS `hgvs_validation_status`',
    '`genome_build` AS `genome_build`',
    '`chromosome` AS `chromosome`',
    '`position_start` AS `position_start`',
    '`position_end` AS `position_end`',
    '`vaf_raw` AS `vaf_raw`',
    '`vaf` AS `vaf`',
    '`zygosity` AS `zygosity`',
    '`reported_classification` AS `reported_classification`',
    '`reported_tier` AS `reported_tier`',
    '`copy_number` AS `copy_number`',
    '`ratio_raw` AS `ratio_raw`',
    '`iscn_raw` AS `iscn_raw`',
    '`clinvar_concept_id` AS `clinvar_concept_id`',
    '`omop_genomic_concept_id` AS `omop_genomic_concept_id`',
    '`cosmic_id` AS `cosmic_id`',  # GEN4_GOLD_V1
    '`itd_length_bp` AS `itd_length_bp`',  # GEN4_GOLD_V1
    '`reported_pathogenicity` AS `reported_pathogenicity`',  # GEN4_GOLD_V1
    '`pathogenicity_loinc_code` AS `pathogenicity_loinc_code`',  # SILVER_RELEASE_20261025 item 7
    '`pathogenicity_omop_concept_id` AS `pathogenicity_omop_concept_id`',  # SILVER_RELEASE_20261025 item 7
    '`pathogenicity_display` AS `pathogenicity_display`',  # SILVER_RELEASE_20261025 item 7
    '`pathogenicity_question_loinc_code` AS `pathogenicity_question_loinc_code`',  # SILVER_RELEASE_20261025 item 7
    '`pathogenicity_question_omop_concept_id` AS `pathogenicity_question_omop_concept_id`',  # SILVER_RELEASE_20261025 item 7
    '`tier_system` AS `tier_system`',  # SILVER_RELEASE_20261025 item 7
    '`tier_code` AS `tier_code`',  # SILVER_RELEASE_20261025 item 7
    '`tier_display` AS `tier_display`',  # SILVER_RELEASE_20261025 item 7
    '`resolved_transcript` AS `resolved_transcript`',  # GEN4_GOLD_V1
    '`transcript_source` AS `transcript_source`',  # GEN4_GOLD_V1
    '`variant_key` AS `variant_key`',  # GEN4_GOLD_V1
    '`ref_allele` AS `ref_allele`',  # GEN4_GOLD_V1
    '`alt_allele` AS `alt_allele`',  # GEN4_GOLD_V1
    '`coordinate_method` AS `coordinate_method`',  # GEN4_GOLD_V1
    '`clinvar_variation_id` AS `clinvar_variation_id`',  # GEN4_GOLD_V1
    '`omop_genomic_concept_level` AS `omop_genomic_concept_level`',  # GEN4_GOLD_V1
    '`concept_method` AS `concept_method`',  # GEN4_GOLD_V1
    '`evidence_text` AS `evidence_text`',
    '`evidence_start` AS `evidence_start`',
    '`evidence_end` AS `evidence_end`',
    '`parser_profile_id` AS `parser_profile_id`',
    '`parser_version` AS `parser_version`',
    '`review_status` AS `review_status`',
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


# contract v2: QC/batch inputs come from internal _clinical_genomic_result_metadata; source history stays on the main research table.
CLINICAL_GENOMIC_RESULT_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 0 rows of 0 that are current
    # and attributable; identity_status = 'resolved' keeps the 0 rows of 0 that are current
    # and attributable. Superseded versions and rows whose identity was never resolved are
    # not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_GENOMIC_RESULT_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.genomic_result.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 0 at the profile.
    "gold.clinical.genomic_result.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 0 at the profile.
    "gold.clinical.genomic_result.record_status.default_view_active":
        "record_status = 'active'",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_GENOMIC_RESULT_PERSON_SCOPE_RULES = {
    "gold.clinical.genomic_result.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_GENOMIC_RESULT_ORPHAN_RULES = {
    "gold.clinical.genomic_result.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_genomic_result"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_GENOMIC_RESULT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_GENOMIC_RESULT_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_GENOMIC_RESULT_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_GENOMIC_RESULT_ORPHAN_RULES)
def _gold_qc_clinical_genomic_result():
    """Quality-controlled twin of journey_clinical.genomic_result."""
    df = _qc("clinical_genomic_result", CLINICAL_GENOMIC_RESULT_SELECT, fk_columns=("encounter_id",))
    return df


@dp.materialized_view(
    name=_n("gold_clinical.genomic_result"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_genomic_result():
    """Contract-v2 public twin of clinical_genomic_result; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_genomic_result")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`genetic_result_id` AS `genetic_result_id`',
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
        '`genomic_test_key` AS `genomic_test_key`',
        '`report_version_key` AS `report_version_key`',
        '`hgnc_id` AS `hgnc_id`',
        '`reported_gene_symbol` AS `reported_gene_symbol`',
        '`normalized_gene_symbol` AS `normalized_gene_symbol`',
        '`partner_hgnc_id` AS `partner_hgnc_id`',
        '`partner_gene_symbol` AS `partner_gene_symbol`',
        '`alteration_type` AS `alteration_type`',
        '`detection_status` AS `detection_status`',
        '`hgvs_c_raw` AS `hgvs_c_raw`',
        '`hgvs_c_parsed` AS `hgvs_c_parsed`',
        '`hgvs_p_raw` AS `hgvs_p_raw`',
        '`hgvs_p_parsed` AS `hgvs_p_parsed`',
        '`transcript` AS `transcript`',
        '`hgvs_validation_status` AS `hgvs_validation_status`',
        '`genome_build` AS `genome_build`',
        '`chromosome` AS `chromosome`',
        '`position_start` AS `position_start`',
        '`position_end` AS `position_end`',
        '`vaf_raw` AS `vaf_raw`',
        '`vaf` AS `vaf`',
        '`zygosity` AS `zygosity`',
        '`reported_classification` AS `reported_classification`',
        '`reported_tier` AS `reported_tier`',
        '`copy_number` AS `copy_number`',
        '`ratio_raw` AS `ratio_raw`',
        '`iscn_raw` AS `iscn_raw`',
        '`clinvar_concept_id` AS `clinvar_concept_id`',
        '`omop_genomic_concept_id` AS `omop_genomic_concept_id`',
        '`cosmic_id` AS `cosmic_id`',  # GEN4_GOLD_V1
        '`itd_length_bp` AS `itd_length_bp`',  # GEN4_GOLD_V1
        '`reported_pathogenicity` AS `reported_pathogenicity`',  # GEN4_GOLD_V1
        '`pathogenicity_loinc_code` AS `pathogenicity_loinc_code`',  # SILVER_RELEASE_20261025 item 7
        '`pathogenicity_omop_concept_id` AS `pathogenicity_omop_concept_id`',  # SILVER_RELEASE_20261025 item 7
        '`pathogenicity_display` AS `pathogenicity_display`',  # SILVER_RELEASE_20261025 item 7
        '`pathogenicity_question_loinc_code` AS `pathogenicity_question_loinc_code`',  # SILVER_RELEASE_20261025 item 7
        '`pathogenicity_question_omop_concept_id` AS `pathogenicity_question_omop_concept_id`',  # SILVER_RELEASE_20261025 item 7
        '`tier_system` AS `tier_system`',  # SILVER_RELEASE_20261025 item 7
        '`tier_code` AS `tier_code`',  # SILVER_RELEASE_20261025 item 7
        '`tier_display` AS `tier_display`',  # SILVER_RELEASE_20261025 item 7
        '`resolved_transcript` AS `resolved_transcript`',  # GEN4_GOLD_V1
        '`transcript_source` AS `transcript_source`',  # GEN4_GOLD_V1
        '`variant_key` AS `variant_key`',  # GEN4_GOLD_V1
        '`ref_allele` AS `ref_allele`',  # GEN4_GOLD_V1
        '`alt_allele` AS `alt_allele`',  # GEN4_GOLD_V1
        '`coordinate_method` AS `coordinate_method`',  # GEN4_GOLD_V1
        '`clinvar_variation_id` AS `clinvar_variation_id`',  # GEN4_GOLD_V1
        '`omop_genomic_concept_level` AS `omop_genomic_concept_level`',  # GEN4_GOLD_V1
        '`concept_method` AS `concept_method`',  # GEN4_GOLD_V1
        '`evidence_text` AS `evidence_text`',
        '`evidence_start` AS `evidence_start`',
        '`evidence_end` AS `evidence_end`',
        '`parser_profile_id` AS `parser_profile_id`',
        '`parser_version` AS `parser_version`',
        '`review_status` AS `review_status`',
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
    'clinical_genomic_result': GoldRules(
        table='clinical_genomic_result',
        select=CLINICAL_GENOMIC_RESULT_SELECT,
        mandatory=CLINICAL_GENOMIC_RESULT_MANDATORY_RULES,
        advisory=CLINICAL_GENOMIC_RESULT_ADVISORY_RULES,
        person_scope=CLINICAL_GENOMIC_RESULT_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_genomic_result': {'columns': {'accession_identifier': 'Alias-resolved accession identifier.',
                                             'alt_allele': 'GRCh38 alternate allele (VCF).',
                                             'alteration_type': 'Reported alteration type.',
                                             'chromosome': 'Explicitly reported chromosome.',
                                             'clinvar_concept_id': 'ClinVar concept identifier.',
                                             'clinvar_variation_id': 'ClinVar VariationID matched on '
                                                                     'variant_key (ClinVar GRCh38 release '
                                                                     'loaded in 3_lookup).',
                                             'concept_method': 'Exact match key used for '
                                                               'omop_genomic_concept_id, prefixed by level.',
                                             'confidentiality_code': 'Source confidentiality classification '
                                                                     'attached to the record for each '
                                                                     'genomic result record. It is produced '
                                                                     'by the silver transformation and has '
                                                                     'no direct bronze-column lineage entry. '
                                                                     'Source code meanings and sentinel '
                                                                     'values are retained unless the '
                                                                     'pipeline explicitly maps them; null '
                                                                     'means no code was supplied.',
                                             'coordinate_method': 'How coordinates were derived: '
                                                                  'transcript_model, '
                                                                  'transcript_model_projected (stated older '
                                                                  'version with identical CDS), '
                                                                  '*_position_only (interval only), or why '
                                                                  'none were (ref_mismatch, '
                                                                  'stated_transcript_unavailable, '
                                                                  'version_projection_position_not_coding, '
                                                                  'no_transcript_model, unsupported_hgvs, '
                                                                  'no_parsed_hgvs_c). Positions always '
                                                                  'describe ref_allele.',
                                             'copy_number': 'Reported copy number.',
                                             'cosmic_id': 'COSMIC identifier exactly as reported by the '
                                                          'laboratory; not enriched from COSMIC.',
                                             'detection_status': 'Finding detection status.',
                                             'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
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
                                                                      'so encounter_id is NULL. '
                                                                      'PATH_ENCOUNTER_V2 (genomic only): '
                                                                      'encounter_window_single / '
                                                                      'encounter_window_same_day link a '
                                                                      'Barts-site LIMS accession to the '
                                                                      "patient's encounter containing the "
                                                                      'sample date; '
                                                                      'encounter_window_multiple and '
                                                                      'none_external_referral leave it NULL. '
                                                                      'Gold: encounter_id can still be NULL '
                                                                      'here when the encounter is not in the '
                                                                      'Gold encounter spine.',
                                             'event_datetime': 'Report issue time with accession fallback.',
                                             'event_datetime_local': 'Local-time event timestamp taken from '
                                                                     'the report issue or verification time, '
                                                                     'falling back to the accession sample '
                                                                     'or request time.',
                                             'event_end_datetime': 'Event end time.',
                                             'event_end_datetime_local': 'Event end time expressed in local '
                                                                         'time.',
                                             'evidence_end': 'Evidence end offset.',
                                             'evidence_start': 'Evidence start offset.',
                                             'evidence_text': 'Minimal narrative evidence span.',
                                             'genetic_result_id': 'Pathology genetic_result_id; primary key '
                                                                  'of this table.',
                                             'genome_build': 'Explicit genome build.',
                                             'genomic_test_key': 'Deterministic SHA-256 key of the parent '
                                                                 'genomic test; equals '
                                                                 'clinical_genomic_test.patient_event_key.',
                                             'hgnc_id': 'Primary HGNC identifier.',
                                             'hgvs_c_parsed': 'Parsed coding HGVS.',
                                             'hgvs_c_raw': 'Raw coding HGVS.',
                                             'hgvs_p_parsed': 'Parsed protein HGVS.',
                                             'hgvs_p_raw': 'Raw protein HGVS.',
                                             'hgvs_validation_status': 'HGVS validation state.',
                                             'is_current': 'Whether the source report version is current.',
                                             'iscn_raw': 'Raw ISCN notation.',
                                             'itd_length_bp': 'Reported FLT3/NPM1 insertion length in base '
                                                              'pairs.',
                                             'lifecycle_status': 'Inherited report lifecycle.',
                                             'loaded_at': 'map_pathology_genetic_result.ADC_UPDT carried '
                                                          'unchanged through its joins, usable-code '
                                                          'filtering and source/public projections. Alias '
                                                          'resolution and accession identity add no load '
                                                          'clocks; neither joined report/parent timestamps '
                                                          'nor the separately aggregated accession evidence '
                                                          'clock replace this value. It is identical to '
                                                          'source_update_timestamp, not the clinical event '
                                                          'time or Silver refresh time.',
                                             'normalized_gene_symbol': 'Approved HGNC symbol.',
                                             'omop_genomic_concept_id': 'OMOP Genomic concept identifier.',
                                             'omop_genomic_concept_level': 'Level omop_genomic_concept_id '
                                                                           'was matched at: dna, rna, '
                                                                           'protein, fusion or gene.',
                                             'parser_profile_id': 'Deterministic parser profile identifier.',
                                             'parser_version': 'Parser implementation version.',
                                             'partner_gene_symbol': 'Fusion/rearrangement partner symbol.',
                                             'partner_hgnc_id': 'Partner HGNC identifier.',
                                             'pathogenicity_display': 'LOINC answer display of '
                                                                      'pathogenicity_loinc_code.',
                                             'pathogenicity_loinc_code': 'LOINC answer code for '
                                                                         'reported_pathogenicity, answering '
                                                                         'LOINC 53037-8 (Genetic variation '
                                                                         'clinical significance [Imp]): '
                                                                         'LA6668-3 Pathogenic, LA26332-9 '
                                                                         'Likely pathogenic, LA26333-7 '
                                                                         'Uncertain significance, LA26334-5 '
                                                                         'Likely benign, LA6675-8 Benign. '
                                                                         'Deterministic code table in '
                                                                         'silver; NULL when the laboratory '
                                                                         'reported no pathogenicity class.',
                                             'pathogenicity_omop_concept_id': 'Standard OMOP concept_id '
                                                                              '(vocabulary LOINC, class '
                                                                              'Answer, domain Meas Value) of '
                                                                              'pathogenicity_loinc_code: '
                                                                              '45884094 Pathogenic, 21498358 '
                                                                              'Likely pathogenic, 21498841 '
                                                                              'Uncertain significance, '
                                                                              '21498359 Likely benign, '
                                                                              '45880925 Benign.',
                                             'pathogenicity_question_loinc_code': 'LOINC question the coded '
                                                                                  'pathogenicity answers: '
                                                                                  '53037-8 Genetic variation '
                                                                                  'clinical significance '
                                                                                  '[Imp]. Set exactly when '
                                                                                  'pathogenicity_loinc_code '
                                                                                  'is set.',
                                             'pathogenicity_question_omop_concept_id': 'Standard OMOP '
                                                                                       'concept_id of the '
                                                                                       'question, 3039641 '
                                                                                       '(LOINC 53037-8). Set '
                                                                                       'exactly when '
                                                                                       'pathogenicity_loinc_code '
                                                                                       'is set.',
                                             'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                  'stable cross-feed join key.',
                                             'person_id': 'Resolved Millennium PERSON_ID as BIGINT through '
                                                          'the shared pathology identity projection.',
                                             'position_end': 'Genomic end coordinate.',
                                             'position_start': 'Genomic start coordinate.',
                                             'ratio_raw': 'Raw molecular ratio.',
                                             'record_status': 'Retracted when '
                                                              'map_pathology_genetic_result.lifecycle_status '
                                                              'is exactly cancelled or entered_in_error; '
                                                              'otherwise superseded when is_current is false '
                                                              '(null defaults true), otherwise active. '
                                                              'Comparisons are case-sensitive without '
                                                              'trimming; a null lifecycle_status does not '
                                                              'select retraction. Retraction takes priority '
                                                              'over supersession; clinical findings and '
                                                              'joined identity/link fields are separate.',
                                             'record_status_effective_from': 'Always null as a TIMESTAMP in '
                                                                             'the current genomic-result '
                                                                             'projection. No source-status '
                                                                             'start is supplied here; the '
                                                                             "clinical event's "
                                                                             'report/sample/request date '
                                                                             'fallbacks and ADC_UPDT are not '
                                                                             'used for this field.',
                                             'record_status_effective_to': 'Always null as a TIMESTAMP in '
                                                                           'the current genomic-result '
                                                                           'projection, including when its '
                                                                           'status or source-current flag '
                                                                           'changes. No report, '
                                                                           'clinical-event or ingestion '
                                                                           'timestamp is substituted as a '
                                                                           'status end.',
                                             'ref_allele': 'GRCh38 reference allele (VCF).',
                                             'report_version_key': 'Deterministic report-version key.',
                                             'reported_classification': 'Classification or tier reported by '
                                                                        'the laboratory.',
                                             'reported_gene_symbol': 'Gene symbol as reported.',
                                             'reported_pathogenicity': 'Laboratory pathogenicity class: '
                                                                       'pathogenic, likely_pathogenic, '
                                                                       'uncertain_significance, '
                                                                       'likely_benign, benign.',
                                             'reported_tier': 'Reported clinical/actionability tier.',
                                             'research_qi_only': 'Research/QI release flag.',
                                             'resolved_transcript': 'RefSeq transcript the c. HGVS refers '
                                                                    "to: as reported, else the report's "
                                                                    "panel list, else the gene's MANE Select "
                                                                    '(see transcript_source).',
                                             'review_status': 'Source finding review status.',
                                             'source_code': 'SNV, indel, fusion, CNV, rearrangement, repeat, '
                                                            'karyotype, or other.',
                                             'source_coding_system': 'Gene-symbol or alteration coding '
                                                                     'namespace.',
                                             'source_display': 'SNV, indel, fusion, CNV, rearrangement, '
                                                               'repeat, karyotype, or other.',
                                             'source_feed': 'Originating data feed responsible for the '
                                                            'record for each genomic result record. It is '
                                                            'produced by the silver transformation and has '
                                                            'no direct bronze-column lineage entry. '
                                                            'Whitespace and source sentinel text are '
                                                            'retained unless the pipeline explicitly '
                                                            'normalizes them; null means no value was '
                                                            'supplied.',
                                             'source_update_timestamp': 'map_pathology_genetic_result.ADC_UPDT '
                                                                        'carried unchanged from the '
                                                                        'contributing row, identical to '
                                                                        'loaded_at. No independent native '
                                                                        'application-update clock is '
                                                                        'projected here, and Silver refresh '
                                                                        'time is not substituted.',
                                             'specimen_key': 'Deterministic specimen key.',
                                             'subject_id_system': 'Identifier namespace selected for '
                                                                  '`subject_key`: Millennium person '
                                                                  'identifier first, then MRN, then NHS '
                                                                  'number; nosubject denotes the '
                                                                  'per-source-record fallback when none is '
                                                                  'present.',
                                             'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                            'identifier. Not salted, not secret; a stable '
                                                            'join key across feeds where person_id is '
                                                            'unresolved.',
                                             'tier_code': 'AMP/ASCO/CAP somatic variant tier from '
                                                          'reported_tier: I, II, III or IV; NULL otherwise.',
                                             'tier_display': 'Meaning of tier_code: Tier I strong, Tier II '
                                                             'potential, Tier III unknown clinical '
                                                             'significance; Tier IV benign or likely benign.',
                                             'tier_system': 'Coding system of tier_code: '
                                                            'urn:barts:genomics:amp-asco-cap-2017-somatic-tier, '
                                                            'the AMP/ASCO/CAP 2017 somatic variant tiers (no '
                                                            'standard vocabulary concept exists for them in '
                                                            '3_lookup.omop). Set when reported_tier is I, '
                                                            'II, III or IV.',
                                             'transcript': 'Reported transcript accession.',
                                             'transcript_source': 'reported, report_panel_list or '
                                                                  'mane_select.',
                                             'vaf': 'Parsed variant allele fraction.',
                                             'vaf_raw': 'Raw variant allele frequency.',
                                             'variant_key': 'GRCh38 VCF key chrN:pos:ref:alt '
                                                            '(left-normalised), derived in-estate from '
                                                            'resolved_transcript + hgvs_c_parsed.',
                                             'vip_ind': 'Indicator that the source record carries VIP '
                                                        'handling status for each genomic result record. It '
                                                        'is produced by the silver transformation and has no '
                                                        'direct bronze-column lineage entry. Null means the '
                                                        'source did not state the indicator and must not be '
                                                        'interpreted as false.',
                                             'withheld_identity_ind': 'Withheld identity indicator.',
                                             'zygosity': 'Value describing zygosity for the genomic result '
                                                         'record. It is carried from bronze field `zygosity` '
                                                         'in `4_prod.bronze.map_pathology_genetic_result`. '
                                                         'Whitespace and source sentinel text are retained '
                                                         'unless the pipeline explicitly normalizes them; '
                                                         'null means no value was supplied.'},
                                 'quantity_override': True,
                                 'table': 'Internal quality-controlled twin of clinical_genomic_result: the '
                                          'Gold repairs, nulls and expectations are applied here and the '
                                          'published Gold MV reads this object. Materialized rather than '
                                          'temporary so the published MV can refresh incrementally.'},
 'clinical_genomic_result': {'columns': {'accession_identifier': 'Alias-resolved accession identifier.',
                                         'alt_allele': 'GRCh38 alternate allele (VCF).',
                                         'alteration_type': 'Reported alteration type.',
                                         'chromosome': 'Explicitly reported chromosome.',
                                         'clinvar_concept_id': 'ClinVar concept identifier.',
                                         'clinvar_variation_id': 'ClinVar VariationID matched on variant_key '
                                                                 '(ClinVar GRCh38 release loaded in '
                                                                 '3_lookup).',
                                         'concept_method': 'Exact match key used for '
                                                           'omop_genomic_concept_id, prefixed by level.',
                                         'confidentiality_code': 'Source confidentiality classification '
                                                                 'attached to the record for each genomic '
                                                                 'result record. It is produced by the '
                                                                 'silver transformation and has no direct '
                                                                 'bronze-column lineage entry. Source code '
                                                                 'meanings and sentinel values are retained '
                                                                 'unless the pipeline explicitly maps them; '
                                                                 'null means no code was supplied.',
                                         'coordinate_method': 'How coordinates were derived: '
                                                              'transcript_model, transcript_model_projected '
                                                              '(stated older version with identical CDS), '
                                                              '*_position_only (interval only), or why none '
                                                              'were (ref_mismatch, '
                                                              'stated_transcript_unavailable, '
                                                              'version_projection_position_not_coding, '
                                                              'no_transcript_model, unsupported_hgvs, '
                                                              'no_parsed_hgvs_c). Positions always describe '
                                                              'ref_allele.',
                                         'copy_number': 'Reported copy number.',
                                         'cosmic_id': 'COSMIC identifier exactly as reported by the '
                                                      'laboratory; not enriched from COSMIC.',
                                         'detection_status': 'Finding detection status.',
                                         'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                         'encounter_link_method': 'How encounter_id was set '
                                                                  '(PATH_ENCOUNTER_V1). source_accession: '
                                                                  "the row's own Millennium source names the "
                                                                  'encounter. accession_single: every '
                                                                  'current Millennium source of the '
                                                                  'alias-resolved accession names this one '
                                                                  'encounter (how TFC LIMS rows inherit one '
                                                                  'once their accession is merged with the '
                                                                  'Millennium order). accession_multiple: '
                                                                  'those sources name different encounters, '
                                                                  'so encounter_id is NULL. none: the '
                                                                  'accession has no Millennium source, so '
                                                                  'encounter_id is NULL. PATH_ENCOUNTER_V2 '
                                                                  '(genomic only): encounter_window_single / '
                                                                  'encounter_window_same_day link a '
                                                                  'Barts-site LIMS accession to the '
                                                                  "patient's encounter containing the sample "
                                                                  'date; encounter_window_multiple and '
                                                                  'none_external_referral leave it NULL. '
                                                                  'Gold: encounter_id can still be NULL here '
                                                                  'when the encounter is not in the Gold '
                                                                  'encounter spine.',
                                         'event_datetime': 'Report issue time with accession fallback.',
                                         'event_datetime_local': 'Local-time event timestamp taken from the '
                                                                 'report issue or verification time, falling '
                                                                 'back to the accession sample or request '
                                                                 'time.',
                                         'event_end_datetime': 'Event end time.',
                                         'event_end_datetime_local': 'Event end time expressed in local '
                                                                     'time.',
                                         'evidence_end': 'Evidence end offset.',
                                         'evidence_start': 'Evidence start offset.',
                                         'evidence_text': 'Minimal narrative evidence span.',
                                         'genetic_result_id': 'Pathology genetic_result_id; primary key of '
                                                              'this table.',
                                         'genome_build': 'Explicit genome build.',
                                         'genomic_test_key': 'Deterministic SHA-256 key of the parent '
                                                             'genomic test; equals '
                                                             'clinical_genomic_test.patient_event_key.',
                                         'hgnc_id': 'Primary HGNC identifier.',
                                         'hgvs_c_parsed': 'Parsed coding HGVS.',
                                         'hgvs_c_raw': 'Raw coding HGVS.',
                                         'hgvs_p_parsed': 'Parsed protein HGVS.',
                                         'hgvs_p_raw': 'Raw protein HGVS.',
                                         'hgvs_validation_status': 'HGVS validation state.',
                                         'is_current': 'Whether the source report version is current.',
                                         'iscn_raw': 'Raw ISCN notation.',
                                         'itd_length_bp': 'Reported FLT3/NPM1 insertion length in base '
                                                          'pairs.',
                                         'lifecycle_status': 'Inherited report lifecycle.',
                                         'loaded_at': 'map_pathology_genetic_result.ADC_UPDT carried '
                                                      'unchanged through its joins, usable-code filtering '
                                                      'and source/public projections. Alias resolution and '
                                                      'accession identity add no load clocks; neither joined '
                                                      'report/parent timestamps nor the separately '
                                                      'aggregated accession evidence clock replace this '
                                                      'value. It is identical to source_update_timestamp, '
                                                      'not the clinical event time or Silver refresh time.',
                                         'normalized_gene_symbol': 'Approved HGNC symbol.',
                                         'omop_genomic_concept_id': 'OMOP Genomic concept identifier.',
                                         'omop_genomic_concept_level': 'Level omop_genomic_concept_id was '
                                                                       'matched at: dna, rna, protein, '
                                                                       'fusion or gene.',
                                         'parser_profile_id': 'Deterministic parser profile identifier.',
                                         'parser_version': 'Parser implementation version.',
                                         'partner_gene_symbol': 'Fusion/rearrangement partner symbol.',
                                         'partner_hgnc_id': 'Partner HGNC identifier.',
                                         'pathogenicity_display': 'LOINC answer display of '
                                                                  'pathogenicity_loinc_code.',
                                         'pathogenicity_loinc_code': 'LOINC answer code for '
                                                                     'reported_pathogenicity, answering '
                                                                     'LOINC 53037-8 (Genetic variation '
                                                                     'clinical significance [Imp]): LA6668-3 '
                                                                     'Pathogenic, LA26332-9 Likely '
                                                                     'pathogenic, LA26333-7 Uncertain '
                                                                     'significance, LA26334-5 Likely benign, '
                                                                     'LA6675-8 Benign. Deterministic code '
                                                                     'table in silver; NULL when the '
                                                                     'laboratory reported no pathogenicity '
                                                                     'class.',
                                         'pathogenicity_omop_concept_id': 'Standard OMOP concept_id '
                                                                          '(vocabulary LOINC, class Answer, '
                                                                          'domain Meas Value) of '
                                                                          'pathogenicity_loinc_code: '
                                                                          '45884094 Pathogenic, 21498358 '
                                                                          'Likely pathogenic, 21498841 '
                                                                          'Uncertain significance, 21498359 '
                                                                          'Likely benign, 45880925 Benign.',
                                         'pathogenicity_question_loinc_code': 'LOINC question the coded '
                                                                              'pathogenicity answers: '
                                                                              '53037-8 Genetic variation '
                                                                              'clinical significance [Imp]. '
                                                                              'Set exactly when '
                                                                              'pathogenicity_loinc_code is '
                                                                              'set.',
                                         'pathogenicity_question_omop_concept_id': 'Standard OMOP concept_id '
                                                                                   'of the question, 3039641 '
                                                                                   '(LOINC 53037-8). Set '
                                                                                   'exactly when '
                                                                                   'pathogenicity_loinc_code '
                                                                                   'is set.',
                                         'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                              'cross-feed join key.',
                                         'person_id': 'Resolved Millennium PERSON_ID as BIGINT through the '
                                                      'shared pathology identity projection.',
                                         'position_end': 'Genomic end coordinate.',
                                         'position_start': 'Genomic start coordinate.',
                                         'ratio_raw': 'Raw molecular ratio.',
                                         'record_status': 'Retracted when '
                                                          'map_pathology_genetic_result.lifecycle_status is '
                                                          'exactly cancelled or entered_in_error; otherwise '
                                                          'superseded when is_current is false (null '
                                                          'defaults true), otherwise active. Comparisons are '
                                                          'case-sensitive without trimming; a null '
                                                          'lifecycle_status does not select retraction. '
                                                          'Retraction takes priority over supersession; '
                                                          'clinical findings and joined identity/link fields '
                                                          'are separate.',
                                         'record_status_effective_from': 'Always null as a TIMESTAMP in the '
                                                                         'current genomic-result projection. '
                                                                         'No source-status start is supplied '
                                                                         "here; the clinical event's "
                                                                         'report/sample/request date '
                                                                         'fallbacks and ADC_UPDT are not '
                                                                         'used for this field.',
                                         'record_status_effective_to': 'Always null as a TIMESTAMP in the '
                                                                       'current genomic-result projection, '
                                                                       'including when its status or '
                                                                       'source-current flag changes. No '
                                                                       'report, clinical-event or ingestion '
                                                                       'timestamp is substituted as a status '
                                                                       'end.',
                                         'ref_allele': 'GRCh38 reference allele (VCF).',
                                         'report_version_key': 'Deterministic report-version key.',
                                         'reported_classification': 'Classification or tier reported by the '
                                                                    'laboratory.',
                                         'reported_gene_symbol': 'Gene symbol as reported.',
                                         'reported_pathogenicity': 'Laboratory pathogenicity class: '
                                                                   'pathogenic, likely_pathogenic, '
                                                                   'uncertain_significance, likely_benign, '
                                                                   'benign.',
                                         'reported_tier': 'Reported clinical/actionability tier.',
                                         'research_qi_only': 'Research/QI release flag.',
                                         'resolved_transcript': 'RefSeq transcript the c. HGVS refers to: as '
                                                                "reported, else the report's panel list, "
                                                                "else the gene's MANE Select (see "
                                                                'transcript_source).',
                                         'review_status': 'Source finding review status.',
                                         'source_code': 'SNV, indel, fusion, CNV, rearrangement, repeat, '
                                                        'karyotype, or other.',
                                         'source_coding_system': 'Gene-symbol or alteration coding '
                                                                 'namespace.',
                                         'source_display': 'SNV, indel, fusion, CNV, rearrangement, repeat, '
                                                           'karyotype, or other.',
                                         'source_feed': 'Originating data feed responsible for the record '
                                                        'for each genomic result record. It is produced by '
                                                        'the silver transformation and has no direct '
                                                        'bronze-column lineage entry. Whitespace and source '
                                                        'sentinel text are retained unless the pipeline '
                                                        'explicitly normalizes them; null means no value was '
                                                        'supplied.',
                                         'source_update_timestamp': 'map_pathology_genetic_result.ADC_UPDT '
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
                                                        'identifier. Not salted, not secret; a stable join '
                                                        'key across feeds where person_id is unresolved.',
                                         'tier_code': 'AMP/ASCO/CAP somatic variant tier from reported_tier: '
                                                      'I, II, III or IV; NULL otherwise.',
                                         'tier_display': 'Meaning of tier_code: Tier I strong, Tier II '
                                                         'potential, Tier III unknown clinical significance; '
                                                         'Tier IV benign or likely benign.',
                                         'tier_system': 'Coding system of tier_code: '
                                                        'urn:barts:genomics:amp-asco-cap-2017-somatic-tier, '
                                                        'the AMP/ASCO/CAP 2017 somatic variant tiers (no '
                                                        'standard vocabulary concept exists for them in '
                                                        '3_lookup.omop). Set when reported_tier is I, II, '
                                                        'III or IV.',
                                         'transcript': 'Reported transcript accession.',
                                         'transcript_source': 'reported, report_panel_list or mane_select.',
                                         'vaf': 'Parsed variant allele fraction.',
                                         'vaf_raw': 'Raw variant allele frequency.',
                                         'variant_key': 'GRCh38 VCF key chrN:pos:ref:alt (left-normalised), '
                                                        'derived in-estate from resolved_transcript + '
                                                        'hgvs_c_parsed.',
                                         'vip_ind': 'Indicator that the source record carries VIP handling '
                                                    'status for each genomic result record. It is produced '
                                                    'by the silver transformation and has no direct '
                                                    'bronze-column lineage entry. Null means the source did '
                                                    'not state the indicator and must not be interpreted as '
                                                    'false.',
                                         'withheld_identity_ind': 'Withheld identity indicator.',
                                         'zygosity': 'Value describing zygosity for the genomic result '
                                                     'record. It is carried from bronze field `zygosity` in '
                                                     '`4_prod.bronze.map_pathology_genetic_result`. '
                                                     'Whitespace and source sentinel text are retained '
                                                     'unless the pipeline explicitly normalizes them; null '
                                                     'means no value was supplied.'},
                             'quantity_override': False,
                             'table': 'One reportable molecular or cytogenetic finding per assay result, '
                                      'parsed from genetics reports. Evidence text quotes the report '
                                      'narrative.'}}
