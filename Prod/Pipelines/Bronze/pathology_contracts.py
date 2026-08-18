"""Versioned contracts for the deterministic Bronze pathology expansion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


CONTRACT_VERSION = "1.0.0-dev"


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    comment: str


@dataclass(frozen=True)
class Contract:
    name: str
    comment: str
    keys: tuple[str, ...]
    columns: tuple[Column, ...]


def c(name: str, data_type: str, comment: str) -> Column:
    return Column(name, data_type, comment)


COMMON = (
    c("source_payload_hash", "BIGINT", "Hash of source and derived payload used to guard MERGE updates."),
    c("contract_version", "STRING", "Pathology expansion contract version that produced the row."),
    c("ADC_UPDT", "TIMESTAMP", "Time at which this contract row was last materially changed."),
)


CONTRACTS: tuple[Contract, ...] = (
    Contract(
        "map_pathology_accession",
        "Canonical pathology accession registry; identifiers are assigned once and never derived from LabNo.",
        ("pathology_accession_id",),
        (
            c("pathology_accession_id", "STRING", "Immutable canonical accession UUID."),
            c("primary_source_accession_id", "STRING", "Source accession that originally created this registry row."),
            c("canonical_accession_status", "STRING", "active, merged, ambiguous, unlinked, or retired."),
            c("canonical_person_id", "BIGINT", "Canonical person only when source evidence is unambiguous."),
            c("person_resolution_status", "STRING", "eligible, unresolved, or conflicting."),
            c("normalized_lab_no", "STRING", "Normalized lab number retained as matching evidence, never as identity."),
            c("lab_series", "STRING", "First alphabetic lab-number series code."),
            c("lab_series_desc", "STRING", "Histopathology, cytology, SIHMDS, or other."),
            c("discipline", "STRING", "Cellular pathology, SIHMDS, microbiology, blood science, transfusion, or other."),
            c("request_dt", "TIMESTAMP", "Earliest reliable request time across member sources."),
            c("sample_dt", "TIMESTAMP", "Earliest reliable specimen time across member sources."),
            c("report_dt", "TIMESTAMP", "Latest current report time across member sources."),
            c("clinical_details", "STRING", "Raw WinPath clinical details at accession grain."),
            c("tlcs_requested", "STRING", "Raw WinPath tests-requested string at accession grain."),
            c("conditions", "STRING", "Raw source conditions text."),
            c("reason", "STRING", "Raw source reason text."),
            c("urgent_flag", "STRING", "Source urgency indicator."),
            c("body_site_code", "STRING", "Source body-site code."),
            c("body_site_snomed_code", "STRING", "Approved SNOMED mapping of body site."),
            c("specimen_type_code", "STRING", "Source specimen-type code."),
            c("specimen_type_snomed_code", "STRING", "Approved SNOMED mapping of specimen type."),
            c("source_site_code", "STRING", "Primary requesting or processing site evidence."),
            c("lifecycle_status", "STRING", "Current accession lifecycle status."),
            c("match_rule_version", "STRING", "Approved match-rule version used for current membership."),
            c("research_qi_only", "BOOLEAN", "True until separate direct-care validation and governance approval."),
            c("created_at", "TIMESTAMP", "Time this immutable accession identifier was first assigned."),
            c("retired_at", "TIMESTAMP", "Time this accession was retired into an alias, when applicable."),
        ) + COMMON,
    ),
    Contract(
        "map_pathology_accession_source",
        "One row for every source parent, retaining source-specific identity and matching evidence.",
        ("source_system", "source_parent_key"),
        (
            c("source_accession_id", "STRING", "Stable source-local accession group: raw parent or Cerner order, with parent fallback."),
            c("pathology_accession_id", "STRING", "Current canonical accession registry identifier."),
            c("source_system", "STRING", "TFC_LIMS or CERNER."),
            c("source_parent_key", "STRING", "Existing map_pathology parent reconciliation key."),
            c("LIMSNo", "INT", "WinPath/TFC internal LIMS identifier when available."),
            c("lab_no", "STRING", "Lab number exactly as supplied by the source."),
            c("normalized_lab_no", "STRING", "Uppercase alphanumeric lab number used only as matching evidence."),
            c("wkg_code", "STRING", "Working-code space for raw LIMS accessions."),
            c("source_site_code", "STRING", "Source/request/processing site evidence."),
            c("person_id", "BIGINT", "Person identifier resolved for this source only."),
            c("encounter_id", "BIGINT", "Encounter identifier supplied by this source."),
            c("mrn", "STRING", "Source or canonically resolved MRN."),
            c("nhs_number", "STRING", "Source or canonically resolved NHS number."),
            c("person_match_status", "STRING", "Native, agreed, single-identifier, conflict, ambiguous, or unresolved."),
            c("person_projection_status", "STRING", "Eligibility of this source representation for person-keyed projections."),
            c("request_dt", "TIMESTAMP", "Source request time."),
            c("sample_dt", "TIMESTAMP", "Source specimen/effective time."),
            c("report_dt", "TIMESTAMP", "Source report/verification time."),
            c("order_id", "BIGINT", "Cerner order identifier or NULL for raw LIMS sources."),
            c("order_mnemonic", "STRING", "Cerner order mnemonic when available."),
            c("link_status", "STRING", "source_local, confirmed, probable, ambiguous, or superseded."),
            c("link_rule_id", "STRING", "Approved rule or candidate rule identifier."),
            c("link_confidence", "DOUBLE", "Calibrated match confidence; never used without rule provenance."),
            c("link_evidence_json", "STRING", "Machine-readable matching evidence."),
            c("match_group_key", "STRING", "Internal reconciliation group; not a clinical identifier."),
            c("is_current", "BOOLEAN", "Whether this source membership is current."),
        ) + COMMON,
    ),
    Contract(
        "map_pathology_accession_alias",
        "Stable redirect from a retired accession identifier to its surviving identifier.",
        ("retired_pathology_accession_id",),
        (
            c("retired_pathology_accession_id", "STRING", "Accession identifier retained for historical resolution."),
            c("survivor_pathology_accession_id", "STRING", "Current accession identifier."),
            c("merge_rule_id", "STRING", "Approved rule that caused the entity merge."),
            c("merge_evidence_json", "STRING", "Evidence supporting the merge."),
            c("merged_at", "TIMESTAMP", "Time at which the alias became effective."),
        ) + COMMON,
    ),
    Contract(
        "map_pathology_accession_link_candidate",
        "Ambiguous or unapproved cross-source accession link candidates; candidates never merge entities themselves.",
        ("link_candidate_id",),
        (
            c("link_candidate_id", "STRING", "Stable candidate-pair identifier."),
            c("left_source_accession_id", "STRING", "First source accession in the candidate pair."),
            c("right_source_accession_id", "STRING", "Second source accession in the candidate pair."),
            c("match_group_key", "STRING", "Group reconciled when either candidate changes."),
            c("candidate_rule_id", "STRING", "Rule that generated the candidate."),
            c("candidate_status", "STRING", "proposed, approved, rejected, conflicted, or superseded."),
            c("day_difference", "INT", "Absolute difference between selected source dates."),
            c("identifier_agreement", "BOOLEAN", "Whether unambiguous source persons agree."),
            c("identifier_conflict", "BOOLEAN", "Whether source persons explicitly conflict."),
            c("confidence", "DOUBLE", "Candidate confidence under its rule version."),
            c("evidence_json", "STRING", "Complete candidate evidence."),
            c("rule_version", "STRING", "Candidate-generation rule version."),
        ) + COMMON,
    ),
    Contract(
        "map_pathology_requested_test",
        "One source request/order occurrence with canonical grouping and governed terminology mappings.",
        ("requested_test_occurrence_id",),
        (
            c("requested_test_occurrence_id", "STRING", "Stable source request occurrence identifier."),
            c("canonical_requested_test_id", "STRING", "Canonical group for equivalent raw and Cerner requests."),
            c("pathology_accession_id", "STRING", "Canonical accession identifier."),
            c("source_accession_id", "STRING", "Source accession that supplied the occurrence."),
            c("source_system", "STRING", "TFC_LIMS or CERNER."),
            c("request_ordinal", "INT", "Original token or order position."),
            c("raw_request_text", "STRING", "Unmodified TLC token or Cerner order display."),
            c("wkg_code", "STRING", "Raw working code."),
            c("tlc_code", "STRING", "Resolved WinPath request code."),
            c("order_id", "BIGINT", "Cerner order identifier."),
            c("order_mnemonic", "STRING", "Cerner order mnemonic."),
            c("test_description", "STRING", "Resolved request description."),
            c("test_snomed_code", "STRING", "Approved SNOMED test code."),
            c("test_loinc_code", "STRING", "Approved LOINC test code."),
            c("test_omop_concept_id", "BIGINT", "Approved standard OMOP test concept."),
            c("mapping_status", "STRING", "mapped, proposed, ambiguous, or unmapped."),
            c("mapping_rule_id", "STRING", "Governed mapping rule identifier."),
        ) + COMMON,
    ),
    Contract(
        "map_pathology_report",
        "One pathology report component version with lifecycle and supersession semantics.",
        ("report_version_id",),
        (
            c("report_version_id", "STRING", "Stable identifier for one report component version."),
            c("report_series_id", "STRING", "Stable identifier shared by preliminary/final/amended versions."),
            c("pathology_accession_id", "STRING", "Canonical accession identifier."),
            c("source_accession_id", "STRING", "Source accession that supplied the report."),
            c("source_record_key", "STRING", "Source result record used to assemble this report version."),
            c("report_role", "STRING", "Histology, cytology, morphology, immunophenotyping, molecular, cytogenetics, microbiology, blood_science, technical, administrative, or unknown."),
            c("discipline", "STRING", "Pathology discipline classification."),
            c("report_code", "STRING", "Source report/result code."),
            c("report_section", "STRING", "Source report section."),
            c("report_text", "STRING", "Unmodified assembled source report text."),
            c("report_text_hash", "STRING", "SHA-256 hash of normalized report text."),
            c("lifecycle_status", "STRING", "preliminary, final, amended, corrected, cancelled, entered_in_error, or unknown."),
            c("version_ordinal", "INT", "Source-derived ordering within the report series."),
            c("supersedes_report_version_id", "STRING", "Immediately superseded report version when known."),
            c("valid_from", "TIMESTAMP", "Time this version became valid."),
            c("valid_to", "TIMESTAMP", "Time this version ceased to be current."),
            c("is_current", "BOOLEAN", "Whether this report version is the current non-superseded version."),
            c("issued_dt", "TIMESTAMP", "Report issue or verification time."),
            c("research_qi_only", "BOOLEAN", "True until separate direct-care validation."),
        ) + COMMON,
    ),
    Contract(
        "map_pathology_result_equivalence",
        "Source-result membership of accession-scoped equivalence groups; uncertain results remain separate.",
        ("source_record_key",),
        (
            c("source_record_key", "STRING", "Existing map_pathology source row identifier."),
            c("canonical_result_id", "STRING", "Accession-scoped canonical result group identifier."),
            c("pathology_accession_id", "STRING", "Canonical accession identifier."),
            c("report_version_id", "STRING", "Associated report version when applicable."),
            c("source_table", "STRING", "raw or linked."),
            c("match_group_key", "STRING", "Result-level reconciliation group."),
            c("equivalence_rule_id", "STRING", "Approved exact equivalence rule."),
            c("equivalence_confidence", "DOUBLE", "Confidence under the approved rule."),
            c("representation_role", "STRING", "unique, preferred, alternate, or ambiguous."),
            c("preferred_result_ind", "BOOLEAN", "True only for the chosen current representation."),
            c("person_projection_status", "STRING", "eligible, unresolved, or conflicting."),
            c("lifecycle_status", "STRING", "Result/report lifecycle status."),
            c("is_current", "BOOLEAN", "Whether this result representation is current."),
        ) + COMMON,
    ),
    Contract(
        "map_pathology_genetic_test",
        "One deterministic molecular or cytogenetic assay at accession/report grain.",
        ("genetic_test_id",),
        (
            c("genetic_test_id", "STRING", "Stable assay identifier."),
            c("pathology_accession_id", "STRING", "Canonical accession identifier."),
            c("report_version_id", "STRING", "Report version from which the assay was derived."),
            c("canonical_requested_test_id", "STRING", "Related requested test where available."),
            c("assay_code", "STRING", "Source assay/report code."),
            c("assay_name", "STRING", "Resolved assay name."),
            c("method", "STRING", "NGS, PCR, FISH, karyotype, sequencing, or other reported method."),
            c("panel_code", "STRING", "Governed panel definition code."),
            c("panel_version", "STRING", "Explicit or effective-dated panel version."),
            c("panel_version_inferred", "BOOLEAN", "Whether panel version was inferred from effective dates."),
            c("analysis_context", "STRING", "somatic, germline, mixed, or unknown."),
            c("overall_result_status", "STRING", "detected, not_detected, indeterminate, failed, or unknown."),
            c("test_snomed_code", "STRING", "Approved SNOMED assay code."),
            c("test_loinc_code", "STRING", "Approved LOINC assay code."),
            c("test_omop_concept_id", "BIGINT", "Approved standard OMOP assay concept."),
            c("parser_profile_id", "STRING", "Deterministic parser profile."),
            c("is_current", "BOOLEAN", "Whether the underlying report version is current."),
            c("research_qi_only", "BOOLEAN", "True until separate direct-care validation."),
        ) + COMMON,
    ),
    Contract(
        "map_pathology_gene_tested",
        "One assay-gene denominator row, kept separate from reportable findings.",
        ("gene_tested_id",),
        (
            c("gene_tested_id", "STRING", "Stable assay-gene evidence identifier, including unresolved reported symbols."),
            c("genetic_test_id", "STRING", "Genetic assay identifier."),
            c("hgnc_id", "STRING", "Governed HGNC gene identifier."),
            c("reported_gene_symbol", "STRING", "Gene symbol exactly as reported or configured."),
            c("normalized_gene_symbol", "STRING", "Current approved HGNC symbol."),
            c("alias_match_type", "STRING", "approved_symbol, previous_symbol, alias, ambiguous, or unresolved."),
            c("evidence_type", "STRING", "dedicated_test, report_gene_list, or panel_definition."),
            c("test_scope", "STRING", "Reported exon/region/coverage scope when available."),
            c("panel_version_inferred", "BOOLEAN", "Whether membership came from an inferred effective panel version."),
            c("confidence", "DOUBLE", "Deterministic mapping confidence."),
        ) + COMMON,
    ),
    Contract(
        "map_pathology_genetic_result",
        "One reportable molecular or cytogenetic finding; technical panel lists never create findings.",
        ("genetic_result_id",),
        (
            c("genetic_result_id", "STRING", "Stable finding identifier within a report version."),
            c("genetic_test_id", "STRING", "Parent genetic assay."),
            c("report_version_id", "STRING", "Source report version."),
            c("hgnc_id", "STRING", "Governed HGNC identifier for the primary gene."),
            c("reported_gene_symbol", "STRING", "Primary gene symbol exactly as reported."),
            c("normalized_gene_symbol", "STRING", "Current approved HGNC symbol."),
            c("partner_hgnc_id", "STRING", "Fusion/rearrangement partner HGNC identifier."),
            c("partner_gene_symbol", "STRING", "Fusion/rearrangement partner symbol."),
            c("alteration_type", "STRING", "SNV, indel, fusion, CNV, rearrangement, repeat, karyotype, or other."),
            c("detection_status", "STRING", "detected, not_detected, indeterminate, or unknown."),
            c("hgvs_c_raw", "STRING", "Coding HGVS exactly as reported."),
            c("hgvs_c_parsed", "STRING", "Validated parsed coding HGVS."),
            c("hgvs_p_raw", "STRING", "Protein HGVS exactly as reported."),
            c("hgvs_p_parsed", "STRING", "Validated parsed protein HGVS."),
            c("transcript", "STRING", "Reported transcript accession."),
            c("hgvs_validation_status", "STRING", "valid, partial, invalid, or not_validated."),
            c("genome_build", "STRING", "Genome build only when explicitly reported or validated."),
            c("chromosome", "STRING", "Explicitly reported chromosome."),
            c("position_start", "BIGINT", "Explicitly reported or validated start coordinate."),
            c("position_end", "BIGINT", "Explicitly reported or validated end coordinate."),
            c("vaf_raw", "STRING", "Variant allele frequency exactly as reported."),
            c("vaf", "DOUBLE", "Parsed variant allele fraction on a zero-to-one scale."),
            c("zygosity", "STRING", "Reported zygosity."),
            c("reported_classification", "STRING", "Classification or tier reported by the laboratory."),
            c("reported_tier", "STRING", "Reported clinical/actionability tier."),
            c("copy_number", "DOUBLE", "Reported copy-number value."),
            c("ratio_raw", "STRING", "Reported molecular ratio."),
            c("iscn_raw", "STRING", "ISCN exactly as reported."),
            c("clinvar_concept_id", "BIGINT", "Exact validated ClinVar concept when available."),
            c("omop_genomic_concept_id", "BIGINT", "Mapped standard OMOP Genomic concept."),
            c("snomed_code", "STRING", "Approved SNOMED finding code where suitable."),
            c("evidence_text", "STRING", "Minimal report evidence containing the finding."),
            c("evidence_start", "INT", "Start offset in source report text."),
            c("evidence_end", "INT", "End offset in source report text."),
            c("parser_profile_id", "STRING", "Deterministic parser profile identifier."),
            c("parser_version", "STRING", "Parser implementation version."),
            c("review_status", "STRING", "auto_validated, proposed, approved, rejected, or not_required."),
            c("lifecycle_status", "STRING", "Inherited report lifecycle status."),
            c("is_current", "BOOLEAN", "Whether this finding belongs to the current report version."),
            c("research_qi_only", "BOOLEAN", "True until separate direct-care validation."),
        ) + COMMON,
    ),
    Contract(
        "map_pathology_indication",
        "Deterministic request indication or clinical-context mapping with exact evidence and governance status.",
        ("indication_id",),
        (
            c("indication_id", "STRING", "Stable indication evidence identifier."),
            c("pathology_accession_id", "STRING", "Canonical accession identifier."),
            c("source_accession_id", "STRING", "Source accession containing the evidence."),
            c("relation_type", "STRING", "explicit_indication or clinical_context."),
            c("source_field", "STRING", "ClinicalDetails, Reason, Conditions, order comment/detail, or diagnosis context."),
            c("source_text", "STRING", "Unmodified source field containing the evidence."),
            c("evidence_text", "STRING", "Exact matched evidence span."),
            c("evidence_start", "INT", "Start offset in source text."),
            c("evidence_end", "INT", "End offset in source text."),
            c("snomed_code", "STRING", "Approved SNOMED condition concept."),
            c("snomed_term", "STRING", "Approved SNOMED display term."),
            c("omop_concept_id", "BIGINT", "Mapped standard OMOP condition concept."),
            c("assertion", "STRING", "present, absent, possible, family_history, or unknown."),
            c("temporality", "STRING", "current, historical, future, or unknown."),
            c("experiencer", "STRING", "patient, family, or unknown."),
            c("rule_id", "STRING", "Approved deterministic rule identifier."),
            c("rule_version", "STRING", "Deterministic rule version."),
            c("confidence", "DOUBLE", "Rule-specific confidence."),
            c("mapping_status", "STRING", "approved_exact, approved_rule, proposed, or unmapped."),
            c("ig_release_status", "STRING", "dev_only, approved_research_qi, or blocked."),
            c("is_current", "BOOLEAN", "Whether the evidence remains current."),
            c("research_qi_only", "BOOLEAN", "Always true in v1."),
        ) + COMMON,
    ),
)


LOOKUP_CONTRACTS: tuple[Contract, ...] = (
    Contract(
        "pathology_accession_link_rule",
        "Governed cross-source accession matching rules.",
        ("rule_id", "rule_version"),
        (
            c("rule_id", "STRING", "Stable matching rule identifier."),
            c("rule_version", "STRING", "Rule version."),
            c("status", "STRING", "PROPOSED, APPROVED, REJECTED, or RETIRED."),
            c("max_day_difference", "INT", "Maximum class-specific source-date difference."),
            c("require_identifier_agreement", "BOOLEAN", "Whether a resolved patient must agree."),
            c("require_site_agreement", "BOOLEAN", "Whether site evidence must agree."),
            c("require_discipline_agreement", "BOOLEAN", "Whether discipline evidence must agree."),
            c("auto_merge_ind", "BOOLEAN", "Whether the rule may merge unique candidates."),
            c("reviewed_by", "STRING", "Human reviewer."),
            c("reviewed_at", "TIMESTAMP", "Review time."),
            c("review_notes", "STRING", "Decision evidence."),
            c("valid_from", "TIMESTAMP", "Rule validity start."),
            c("valid_to", "TIMESTAMP", "Rule validity end."),
        ) + COMMON,
    ),
    Contract(
        "pathology_cross_arm_test_map",
        "Governed raw TFC/working-code to Cerner order-mnemonic equivalence map.",
        ("wkg_code", "tfc_code", "order_mnemonic"),
        (
            c("wkg_code", "STRING", "Raw LIMS working code."),
            c("tfc_code", "STRING", "Raw LIMS result code."),
            c("order_mnemonic", "STRING", "Cerner order mnemonic."),
            c("status", "STRING", "PROPOSED, APPROVED, REJECTED, or RETIRED."),
            c("evidence_count", "BIGINT", "Number of high-confidence co-occurrences."),
            c("precision_estimate", "DOUBLE", "Audited precision estimate."),
            c("reviewed_by", "STRING", "Human reviewer."),
            c("reviewed_at", "TIMESTAMP", "Review time."),
            c("valid_from", "TIMESTAMP", "Map validity start."),
            c("valid_to", "TIMESTAMP", "Map validity end."),
        ) + COMMON,
    ),
    Contract(
        "pathology_report_profile",
        "Versioned deterministic report classification and parser profiles.",
        ("profile_id", "profile_version"),
        (
            c("profile_id", "STRING", "Stable report profile identifier."),
            c("profile_version", "STRING", "Profile version."),
            c("status", "STRING", "PROPOSED, APPROVED, REJECTED, or RETIRED."),
            c("source_system", "STRING", "Applicable source system."),
            c("wkg_code_pattern", "STRING", "Anchored working-code regex."),
            c("result_code_pattern", "STRING", "Anchored result-code regex."),
            c("report_section_pattern", "STRING", "Anchored report-section regex."),
            c("text_pattern", "STRING", "Optional deterministic text marker regex."),
            c("report_role", "STRING", "Assigned report role."),
            c("discipline", "STRING", "Assigned pathology discipline."),
            c("parser_type", "STRING", "none, tumour_ngs, myeloid_ngs, single_gene, fish, or cytogenetics."),
            c("assay_name", "STRING", "Governed assay display name."),
            c("method", "STRING", "Governed assay method."),
            c("panel_code", "STRING", "Effective-dated panel definition code when applicable."),
            c("analysis_context", "STRING", "somatic, germline, mixed, or unknown."),
            c("dedicated_gene_symbol", "STRING", "Single target gene for dedicated assays."),
            c("priority", "INT", "Deterministic first-match priority."),
            c("reviewed_by", "STRING", "Human reviewer."),
            c("reviewed_at", "TIMESTAMP", "Review time."),
            c("valid_from", "TIMESTAMP", "Profile validity start."),
            c("valid_to", "TIMESTAMP", "Profile validity end."),
        ) + COMMON,
    ),
    Contract(
        "pathology_hgnc_gene",
        "Versioned HGNC gene reference used for deterministic normalization.",
        ("hgnc_id",),
        (
            c("hgnc_id", "STRING", "HGNC identifier."),
            c("approved_symbol", "STRING", "Current approved gene symbol."),
            c("approved_name", "STRING", "Current approved gene name."),
            c("status", "STRING", "HGNC record status."),
            c("locus_type", "STRING", "HGNC locus type."),
            c("location", "STRING", "HGNC cytogenetic location."),
            c("reference_release", "STRING", "Reference release identifier."),
        ) + COMMON,
    ),
    Contract(
        "pathology_hgnc_alias",
        "HGNC approved, previous, and alias symbols with ambiguity retained.",
        ("alias_symbol", "hgnc_id", "alias_type"),
        (
            c("alias_symbol", "STRING", "Uppercase source symbol or alias."),
            c("hgnc_id", "STRING", "HGNC identifier."),
            c("approved_symbol", "STRING", "Current approved symbol."),
            c("alias_type", "STRING", "approved_symbol, previous_symbol, or alias."),
            c("ambiguous_ind", "BOOLEAN", "Whether the alias maps to multiple genes."),
            c("reference_release", "STRING", "Reference release identifier."),
        ) + COMMON,
    ),
    Contract(
        "pathology_panel_definition",
        "Effective-dated pathology molecular panel definitions.",
        ("panel_code", "panel_version"),
        (
            c("panel_code", "STRING", "Stable panel code."),
            c("panel_version", "STRING", "Panel version."),
            c("panel_name", "STRING", "Panel display name."),
            c("source_assay_code", "STRING", "Source report/request code."),
            c("analysis_context", "STRING", "somatic, germline, mixed, or unknown."),
            c("effective_from", "TIMESTAMP", "Panel validity start."),
            c("effective_to", "TIMESTAMP", "Panel validity end."),
            c("status", "STRING", "PROPOSED, APPROVED, RETIRED, or REJECTED."),
            c("reviewed_by", "STRING", "Human reviewer."),
            c("reviewed_at", "TIMESTAMP", "Review time."),
        ) + COMMON,
    ),
    Contract(
        "pathology_panel_gene",
        "Effective-dated panel-to-HGNC membership and reported coverage scope.",
        ("panel_code", "panel_version", "hgnc_id"),
        (
            c("panel_code", "STRING", "Panel code."),
            c("panel_version", "STRING", "Panel version."),
            c("hgnc_id", "STRING", "HGNC gene identifier."),
            c("gene_symbol", "STRING", "Approved gene symbol."),
            c("test_scope", "STRING", "Exon, region, transcript, or coverage scope."),
            c("status", "STRING", "PROPOSED, APPROVED, RETIRED, or REJECTED."),
        ) + COMMON,
    ),
    Contract(
        "pathology_indication_rule",
        "Governed exact and deterministic request-indication mappings.",
        ("rule_id", "rule_version"),
        (
            c("rule_id", "STRING", "Stable indication rule identifier."),
            c("rule_version", "STRING", "Rule version."),
            c("status", "STRING", "PROPOSED, APPROVED, REJECTED, or RETIRED."),
            c("source_field", "STRING", "Applicable source field or wildcard."),
            c("match_type", "STRING", "exact_normalized or bounded_regex."),
            c("pattern", "STRING", "Deterministic match expression."),
            c("snomed_code", "STRING", "Approved SNOMED concept."),
            c("snomed_term", "STRING", "Approved SNOMED display."),
            c("omop_concept_id", "BIGINT", "Mapped standard OMOP concept."),
            c("default_assertion", "STRING", "Default assertion when pattern matches."),
            c("confidence", "DOUBLE", "Validated rule confidence."),
            c("reviewed_by", "STRING", "Human reviewer."),
            c("reviewed_at", "TIMESTAMP", "Review time."),
            c("valid_from", "TIMESTAMP", "Rule validity start."),
            c("valid_to", "TIMESTAMP", "Rule validity end."),
        ) + COMMON,
    ),
)


AMR_CONTRACTS: tuple[Contract, ...] = (
    Contract(
        "map_pathology_microbiology_isolate",
        "One deterministic organism/isolate finding within a pathology accession.",
        ("microbiology_isolate_id",),
        (
            c("microbiology_isolate_id", "STRING", "Stable accession-scoped isolate identifier."),
            c("pathology_accession_id", "STRING", "Canonical pathology accession."),
            c("report_version_id", "STRING", "Related microbiology report version."),
            c("source_record_key", "STRING", "Source map_pathology result row."),
            c("specimen_type_code", "STRING", "Source specimen type."),
            c("organism_text", "STRING", "Organism exactly as reported."),
            c("organism_snomed_code", "STRING", "Approved SNOMED organism code."),
            c("organism_omop_concept_id", "BIGINT", "Approved standard OMOP organism concept."),
            c("suspected_ind", "BOOLEAN", "Whether the organism identification is hedged or suspected."),
            c("growth_grade", "STRING", "Reported growth grade."),
            c("lifecycle_status", "STRING", "Inherited result/report lifecycle."),
            c("is_current", "BOOLEAN", "Whether the isolate remains current."),
            c("research_qi_only", "BOOLEAN", "True in the research/QI release."),
        ) + COMMON,
    ),
    Contract(
        "map_pathology_antimicrobial_susceptibility",
        "One isolate-antimicrobial susceptibility observation with raw and normalized S/I/R or MIC.",
        ("susceptibility_result_id",),
        (
            c("susceptibility_result_id", "STRING", "Stable source susceptibility identifier."),
            c("microbiology_isolate_id", "STRING", "Linked isolate when uniquely resolvable."),
            c("pathology_accession_id", "STRING", "Canonical pathology accession."),
            c("source_record_key", "STRING", "Source map_pathology result row."),
            c("antimicrobial_text", "STRING", "Antimicrobial exactly as reported/configured."),
            c("antimicrobial_code", "STRING", "Source antimicrobial result/test code."),
            c("antimicrobial_omop_concept_id", "BIGINT", "Approved standard antimicrobial concept."),
            c("interpretation_raw", "STRING", "Raw susceptibility result."),
            c("interpretation", "STRING", "S, I, R, or indeterminate."),
            c("mic_raw", "STRING", "Raw MIC text."),
            c("mic", "DOUBLE", "Parsed MIC numeric value."),
            c("unit_source_value", "STRING", "Raw MIC unit."),
            c("method", "STRING", "Reported susceptibility method."),
            c("link_status", "STRING", "unique_isolate, ambiguous_isolate, or no_isolate."),
            c("lifecycle_status", "STRING", "Inherited result/report lifecycle."),
            c("is_current", "BOOLEAN", "Whether the susceptibility result remains current."),
            c("research_qi_only", "BOOLEAN", "True in the research/QI release."),
        ) + COMMON,
    ),
)


AMR_LOOKUP_CONTRACTS: tuple[Contract, ...] = (
    Contract(
        "pathology_micro_organism_rule",
        "Governed rules identifying organism/isolate result rows.",
        ("rule_id", "rule_version"),
        (
            c("rule_id", "STRING", "Stable organism rule identifier."),
            c("rule_version", "STRING", "Rule version."),
            c("status", "STRING", "PROPOSED, APPROVED, REJECTED, or RETIRED."),
            c("code_pattern", "STRING", "Anchored source-code regex."),
            c("section_pattern", "STRING", "Anchored report-section regex."),
            c("require_result_concept", "BOOLEAN", "Whether a mapped organism concept is required."),
            c("reviewed_by", "STRING", "Human reviewer."),
            c("reviewed_at", "TIMESTAMP", "Review time."),
        ) + COMMON,
    ),
    Contract(
        "pathology_antimicrobial_map",
        "Governed source test-code to antimicrobial concept map.",
        ("code_system", "code"),
        (
            c("code_system", "STRING", "TFC or CERNER_TESTCODE."),
            c("code", "STRING", "Source susceptibility test code."),
            c("antimicrobial_text", "STRING", "Approved antimicrobial display."),
            c("antimicrobial_omop_concept_id", "BIGINT", "Approved standard antimicrobial concept."),
            c("method", "STRING", "Configured susceptibility method."),
            c("status", "STRING", "PROPOSED, APPROVED, REJECTED, or RETIRED."),
            c("reviewed_by", "STRING", "Human reviewer."),
            c("reviewed_at", "TIMESTAMP", "Review time."),
        ) + COMMON,
    ),
)


def _escape_comment(value: str) -> str:
    return value.replace("'", "''")


def create_table_ddl(qualified_schema: str, contract: Contract) -> str:
    columns = ",\n  ".join(
        f"`{column.name}` {column.data_type} COMMENT '{_escape_comment(column.comment)}'"
        for column in contract.columns
    )
    key_comment = ",".join(contract.keys)
    return f"""
CREATE TABLE IF NOT EXISTS {qualified_schema}.{contract.name} (
  {columns}
)
USING DELTA
COMMENT '{_escape_comment(contract.comment)} Primary key: {key_comment}. Contract {CONTRACT_VERSION}.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'='true',
  'delta.deletedFileRetentionDuration'='interval 30 days',
  'pathology.contract.version'='{CONTRACT_VERSION}',
  'pathology.contract.keys'='{key_comment}'
)
""".strip()


def all_create_ddls(
    bronze_schema: str = "8_dev.bronze", lookup_schema: str = "8_dev.lookup"
) -> Iterable[str]:
    for contract in CONTRACTS:
        yield create_table_ddl(bronze_schema, contract)
    for contract in AMR_CONTRACTS:
        yield create_table_ddl(bronze_schema, contract)
    for contract in LOOKUP_CONTRACTS:
        yield create_table_ddl(lookup_schema, contract)
    for contract in AMR_LOOKUP_CONTRACTS:
        yield create_table_ddl(lookup_schema, contract)


def contract(name: str) -> Contract:
    for item in CONTRACTS + AMR_CONTRACTS + LOOKUP_CONTRACTS + AMR_LOOKUP_CONTRACTS:
        if item.name == name:
            return item
    raise KeyError(name)
