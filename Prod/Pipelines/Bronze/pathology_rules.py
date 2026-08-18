"""Deterministic, side-effect-free pathology parsing rules.

These helpers intentionally prefer false negatives over false positive clinical
or genomic assertions. Every returned finding carries the exact source span.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Iterable, Mapping, Sequence


PARSER_VERSION = "1.0.0"


def stable_id(namespace: str, *parts: object) -> str:
    payload = "|".join("∅" if part is None else str(part) for part in parts)
    return hashlib.sha256(f"{namespace}|{payload}".encode("utf-8")).hexdigest()


def normalize_lab_no(value: object) -> str | None:
    text = "" if value is None else str(value).strip().upper()
    normalized = re.sub(r"[^A-Z0-9]", "", text)
    return normalized or None


def lab_series(value: object) -> tuple[str, str]:
    normalized = normalize_lab_no(value) or ""
    match = re.search(r"[A-Z]", normalized)
    code = match.group(0) if match else "UNKNOWN"
    descriptions = {
        "S": "histopathology",
        "N": "cytology",
        "E": "SIHMDS",
        "UNKNOWN": "unknown",
    }
    return code, descriptions.get(code, "other")


def classify_discipline(
    lab_no: object = None,
    wkg_code: object = None,
    section: object = None,
    description: object = None,
) -> str:
    series, _ = lab_series(lab_no)
    if series == "S":
        return "cellular_pathology"
    if series == "N":
        return "cytology"
    if series == "E":
        return "sihmds"
    text = " ".join(str(x or "") for x in (wkg_code, section, description)).lower()
    if re.search(r"\bmicro|bacter|virol|parasit|mycol|culture|suscept|antibi", text):
        return "microbiology"
    if re.search(r"\btransfus|blood bank|group and save|crossmatch", text):
        return "transfusion"
    if re.search(r"\bhaemat|hemat|coag|fbc|blood count", text):
        return "blood_science"
    if re.search(r"\bchem|biochem|electroly|liver|renal|hormone", text):
        return "blood_science"
    return "other"


def split_tlc_requests(value: object) -> tuple[str, ...]:
    """Split request codes without losing order or silently removing tokens."""

    text = "" if value is None else str(value).strip()
    if not text:
        return ()
    # WinPath exports vary by era. Delimiters are structural only when surrounded
    # by whitespace or are the standard comma/semicolon/pipe separators.
    tokens = [part.strip() for part in re.split(r"\s*(?:[,;|]|\r?\n|\t)\s*", text)]
    return tuple(token for token in tokens if token)


def normalize_report_text(value: object) -> str:
    text = "" if value is None else str(value)
    return re.sub(r"\s+", " ", text).strip()


def report_text_hash(value: object) -> str:
    return hashlib.sha256(normalize_report_text(value).encode("utf-8")).hexdigest()


def classify_report_role(
    code: object, section: object, description: object, text: object
) -> str:
    haystack = " ".join(str(x or "") for x in (code, section, description, text[:500] if isinstance(text, str) else text)).lower()
    if re.search(r"immunophenotyp|flow cytometr", haystack):
        return "immunophenotyping"
    if re.search(r"cytogen|karyotyp|\bfish\b|iscn", haystack):
        return "cytogenetics"
    if re.search(r"molecular|\bngs\b|sequenc|mutation|variant|bcr.?abl|pml.?rara", haystack):
        return "molecular"
    if re.search(r"morpholog|bone marrow aspirate|trephine", haystack):
        return "morphology"
    if re.search(r"histolog|histopath|resection|biopsy", haystack):
        return "histology"
    if re.search(r"cytolog|smear", haystack):
        return "cytology"
    if re.search(r"micro|culture|suscept|organism|no growth", haystack):
        return "microbiology"
    if re.search(r"technical|method|target genes|genes covered|limitations", haystack):
        return "technical"
    if re.search(r"administrative|specimen received|test cancelled|referred", haystack):
        return "administrative"
    return "unknown"


def lifecycle_status(
    source_status: object = None,
    authentic_flag: object = None,
    text: object = None,
) -> str:
    status = str(source_status or "").strip().lower()
    body = str(text or "")[:1000].lower()
    authentic = "" if authentic_flag is None else str(authentic_flag).strip().lower()
    if authentic in {"0", "false"} or re.search(
        r"entered in error|report withdrawn|result withdrawn", body
    ):
        return "entered_in_error"
    if re.search(r"\bcancelled|canceled|not processed", status + " " + body):
        return "cancelled"
    if re.search(r"\bcorrected|correction", status + " " + body):
        return "corrected"
    if re.search(r"\bamended|supplementary|addendum", status + " " + body):
        return "amended"
    if re.search(r"\bprelim", status + " " + body):
        return "preliminary"
    if re.search(r"\bfinal|authorised|authorized|verified", status):
        return "final"
    return "unknown"


FINDING_HEADING = re.compile(
    r"(?im)^\s*(?:findings?|results?|variants? identified|molecular findings?)\s*:?[ \t]*$"
)
NON_FINDING_HEADING = re.compile(
    r"(?im)^\s*(?:technical information|method(?:ology)?|genes? (?:tested|covered)|panel content|limitations?|interpretation)\s*:?[ \t]*$"
)
ANY_HEADING = re.compile(r"(?m)^\s*[A-Za-z][A-Za-z /_-]{2,60}:?[ \t]*$")


def finding_sections(text: str) -> tuple[tuple[int, int, str], ...]:
    """Return conservative finding sections and their source offsets."""

    matches = list(FINDING_HEADING.finditer(text))
    sections: list[tuple[int, int, str]] = []
    for match in matches:
        start = match.end()
        end = len(text)
        for heading in ANY_HEADING.finditer(text, start):
            if heading.start() <= start:
                continue
            if NON_FINDING_HEADING.fullmatch(heading.group(0).strip()):
                end = heading.start()
                break
            if FINDING_HEADING.fullmatch(heading.group(0).strip()):
                end = heading.start()
                break
        content = text[start:end]
        if content.strip():
            sections.append((start, end, content))
    return tuple(sections)


HGVS_C_RE = re.compile(r"(?<![A-Za-z0-9_])(?:c\.|g\.|m\.|n\.)[^\s,;()]+", re.I)
HGVS_P_RE = re.compile(r"(?<![A-Za-z0-9_])p\.\(?[A-Za-z*?=0-9_]+\)?", re.I)
TRANSCRIPT_RE = re.compile(r"\b(?:NM|NR|ENST)_?\d+(?:\.\d+)?\b", re.I)
VAF_RE = re.compile(r"\b(?:VAF|variant allele frequency)\s*[:=]?\s*(<?\d+(?:\.\d+)?)\s*%", re.I)
CLASS_RE = re.compile(
    r"\b(pathogenic|likely pathogenic|variant of uncertain significance|VUS|likely benign|benign|tier\s*[1-4IV]+)\b",
    re.I,
)
NEGATIVE_RE = re.compile(
    r"\b(?:no (?:clinically )?(?:significant |reportable )?(?:variant|mutation)s? (?:were )?(?:identified|detected)|negative for|not detected)\b",
    re.I,
)
FUSION_RE = re.compile(r"\b([A-Z0-9]{2,15})\s*(?:-|::|/)\s*([A-Z0-9]{2,15})\b")


def parse_genetic_report_json_worker(
    text: object,
    gene_symbols: Sequence[str],
    _parser_version: str = PARSER_VERSION,
) -> str:
    """Return parser JSON from a worker-self-contained implementation.

    Spark Connect may execute Python UDFs on reused workers that cannot import
    workspace modules.  This function deliberately uses only local imports,
    builtins, arguments, and defaults so cloudpickle can ship it by value.
    """

    import json as _json
    import re as _re

    source = "" if text is None else str(text)
    symbols = tuple(sorted({str(s).upper() for s in gene_symbols if s}, key=len, reverse=True))
    finding_heading = _re.compile(
        r"(?im)^\s*(?:findings?|results?|variants? identified|molecular findings?)\s*:?[ \t]*$"
    )
    non_finding_heading = _re.compile(
        r"(?im)^\s*(?:technical information|method(?:ology)?|genes? (?:tested|covered)|panel content|limitations?|interpretation)\s*:?[ \t]*$"
    )
    any_heading = _re.compile(r"(?m)^\s*[A-Za-z][A-Za-z /_-]{2,60}:?[ \t]*$")
    hgvs_c_re = _re.compile(r"(?<![A-Za-z0-9_])(?:c\.|g\.|m\.|n\.)[^\s,;()]+", _re.I)
    hgvs_p_re = _re.compile(r"(?<![A-Za-z0-9_])p\.\(?[A-Za-z*?=0-9_]+\)?", _re.I)
    transcript_re = _re.compile(r"\b(?:NM|NR|ENST)_?\d+(?:\.\d+)?\b", _re.I)
    vaf_re = _re.compile(
        r"\b(?:VAF|variant allele frequency)\s*[:=]?\s*(<?\d+(?:\.\d+)?)\s*%",
        _re.I,
    )
    class_re = _re.compile(
        r"\b(pathogenic|likely pathogenic|variant of uncertain significance|VUS|likely benign|benign|tier\s*[1-4IV]+)\b",
        _re.I,
    )
    negative_re = _re.compile(
        r"\b(?:no (?:clinically )?(?:significant |reportable )?(?:variant|mutation)s? (?:were )?(?:identified|detected)|negative for|not detected)\b",
        _re.I,
    )
    fusion_re = _re.compile(r"\b([A-Z0-9]{2,15})\s*(?:-|::|/)\s*([A-Z0-9]{2,15})\b")

    def gene_matches(value: str) -> list[tuple[int, int, str]]:
        found: list[tuple[int, int, str]] = []
        for symbol in symbols:
            pattern = _re.compile(
                rf"(?<![A-Z0-9]){_re.escape(symbol)}(?![A-Z0-9])", _re.I
            )
            found.extend((m.start(), m.end(), symbol) for m in pattern.finditer(value))
        return sorted(found)

    genes_tested: set[str] = set()
    for heading in non_finding_heading.finditer(source):
        if not _re.search(
            r"genes? (?:tested|covered)|panel content|technical", heading.group(0), _re.I
        ):
            continue
        start = heading.end()
        next_heading = any_heading.search(source, start)
        end = next_heading.start() if next_heading else min(len(source), start + 5000)
        genes_tested.update(symbol for _, _, symbol in gene_matches(source[start:end]))

    sections: list[tuple[int, str]] = []
    for heading in finding_heading.finditer(source):
        start = heading.end()
        end = len(source)
        for next_heading in any_heading.finditer(source, start):
            if next_heading.start() <= start:
                continue
            candidate = next_heading.group(0).strip()
            if non_finding_heading.fullmatch(candidate) or finding_heading.fullmatch(candidate):
                end = next_heading.start()
                break
        section = source[start:end]
        if section.strip():
            sections.append((start, section))

    findings: list[dict[str, object]] = []
    for section_start, section in sections:
        for sentence_match in _re.finditer(r"[^\n]+", section):
            sentence = sentence_match.group(0)
            if negative_re.search(sentence):
                continue
            genes = gene_matches(sentence)
            fusion = fusion_re.search(sentence)
            hgvs_c = hgvs_c_re.search(sentence)
            hgvs_p = hgvs_p_re.search(sentence)
            transcript = transcript_re.search(sentence)
            vaf = vaf_re.search(sentence)
            classification = class_re.search(sentence)
            if not genes and not fusion and not hgvs_c and not hgvs_p:
                continue
            primary = genes[0][2] if genes else (fusion.group(1).upper() if fusion else None)
            partner = fusion.group(2).upper() if fusion else None
            findings.append(
                {
                    "reported_gene_symbol": primary,
                    "partner_gene_symbol": partner,
                    "alteration_type": "fusion" if fusion else ("sequence_variant" if hgvs_c or hgvs_p else "other"),
                    "detection_status": "detected",
                    "hgvs_c_raw": hgvs_c.group(0) if hgvs_c else None,
                    "hgvs_p_raw": hgvs_p.group(0) if hgvs_p else None,
                    "transcript": transcript.group(0) if transcript else None,
                    "vaf_raw": vaf.group(0) if vaf else None,
                    "vaf": float(vaf.group(1)) / 100.0 if vaf else None,
                    "reported_classification": classification.group(0) if classification else None,
                    "evidence_text": sentence,
                    "evidence_start": section_start + sentence_match.start(),
                    "evidence_end": section_start + sentence_match.end(),
                }
            )

    return _json.dumps(
        {
            "overall_result_status": "detected"
            if findings
            else ("not_detected" if negative_re.search(source) else "unknown"),
            "genes_tested": sorted(genes_tested),
            "findings": findings,
            "parser_version": _parser_version,
        },
        sort_keys=True,
    )


# Force Spark's cloudpickle to carry the implementation instead of expecting
# the executor to import a workspace-only module.
parse_genetic_report_json_worker.__module__ = "__main__"


@dataclass(frozen=True)
class GeneticFinding:
    reported_gene_symbol: str | None
    partner_gene_symbol: str | None
    alteration_type: str
    detection_status: str
    hgvs_c_raw: str | None
    hgvs_p_raw: str | None
    transcript: str | None
    vaf_raw: str | None
    vaf: float | None
    reported_classification: str | None
    evidence_text: str
    evidence_start: int
    evidence_end: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class GeneticParse:
    overall_result_status: str
    genes_tested: tuple[str, ...]
    findings: tuple[GeneticFinding, ...]
    parser_version: str = PARSER_VERSION

    def to_json(self) -> str:
        return json.dumps(
            {
                "overall_result_status": self.overall_result_status,
                "genes_tested": self.genes_tested,
                "findings": [finding.as_dict() for finding in self.findings],
                "parser_version": self.parser_version,
            },
            sort_keys=True,
        )


def _gene_matches(text: str, gene_symbols: Sequence[str]) -> list[tuple[int, int, str]]:
    matches: list[tuple[int, int, str]] = []
    for symbol in sorted({s.upper() for s in gene_symbols if s}, key=len, reverse=True):
        pattern = re.compile(rf"(?<![A-Z0-9]){re.escape(symbol)}(?![A-Z0-9])", re.I)
        matches.extend((m.start(), m.end(), symbol) for m in pattern.finditer(text))
    return sorted(matches)


def _technical_gene_list(text: str, gene_symbols: Sequence[str]) -> tuple[str, ...]:
    genes: set[str] = set()
    for heading in NON_FINDING_HEADING.finditer(text):
        if not re.search(r"genes? (?:tested|covered)|panel content|technical", heading.group(0), re.I):
            continue
        start = heading.end()
        next_heading = ANY_HEADING.search(text, start)
        end = next_heading.start() if next_heading else min(len(text), start + 5000)
        genes.update(symbol for _, _, symbol in _gene_matches(text[start:end], gene_symbols))
    return tuple(sorted(genes))


def parse_genetic_report(text: object, gene_symbols: Sequence[str]) -> GeneticParse:
    """Parse only explicit finding sections; technical gene lists are denominators."""

    payload = json.loads(parse_genetic_report_json_worker(text, gene_symbols))
    return GeneticParse(
        payload["overall_result_status"],
        tuple(payload["genes_tested"]),
        tuple(GeneticFinding(**finding) for finding in payload["findings"]),
        payload["parser_version"],
    )


@dataclass(frozen=True)
class IndicationRule:
    rule_id: str
    rule_version: str
    source_field: str
    match_type: str
    pattern: str
    snomed_code: str
    snomed_term: str
    omop_concept_id: int | None
    default_assertion: str
    confidence: float


@dataclass(frozen=True)
class IndicationMatch:
    rule_id: str
    rule_version: str
    evidence_text: str
    evidence_start: int
    evidence_end: int
    snomed_code: str
    snomed_term: str
    omop_concept_id: int | None
    assertion: str
    confidence: float


def normalize_indication_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().casefold()


def apply_indication_rules(
    source_field: str, source_text: object, rules: Iterable[IndicationRule]
) -> tuple[IndicationMatch, ...]:
    """Apply approved exact or bounded-regex rules with exact source offsets."""

    text = "" if source_text is None else str(source_text)
    normalized = normalize_indication_text(text)
    matches: list[IndicationMatch] = []
    for rule in rules:
        if rule.source_field not in {"*", source_field}:
            continue
        spans: list[tuple[int, int]] = []
        if rule.match_type == "exact_normalized":
            if normalized == normalize_indication_text(rule.pattern):
                spans.append((0, len(text)))
        elif rule.match_type == "bounded_regex":
            pattern = re.compile(rule.pattern, re.I)
            spans.extend((m.start(), m.end()) for m in pattern.finditer(text))
        else:
            continue
        for start, end in spans:
            matches.append(
                IndicationMatch(
                    rule_id=rule.rule_id,
                    rule_version=rule.rule_version,
                    evidence_text=text[start:end],
                    evidence_start=start,
                    evidence_end=end,
                    snomed_code=rule.snomed_code,
                    snomed_term=rule.snomed_term,
                    omop_concept_id=rule.omop_concept_id,
                    assertion=rule.default_assertion,
                    confidence=rule.confidence,
                )
            )
    return tuple(matches)


def normalized_result_value(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def result_equivalence_key(
    pathology_accession_id: object,
    test_key: object,
    value: object,
    unit: object,
    date_bucket: object,
) -> str:
    return stable_id(
        "pathology_result_equivalence",
        pathology_accession_id,
        str(test_key or "").strip().upper(),
        normalized_result_value(value),
        str(unit or "").strip().casefold(),
        date_bucket,
    )


def json_evidence(**values: object) -> str:
    return json.dumps(values, sort_keys=True, default=lambda x: x.isoformat() if isinstance(x, datetime) else str(x))
