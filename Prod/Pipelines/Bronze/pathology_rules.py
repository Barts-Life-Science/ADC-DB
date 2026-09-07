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


PARSER_VERSION = "2.0.0"  # FINDING_HEADING coverage widened, S1 2026-09-03


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
    r"(?im)^\s*(?:"
    r"findings?"
    r"|results?"
    r"|detected variants?"
    r"|detected fusions?"
    r"|variants? identified"
    r"|variants? detected"
    r"|variant\(s\) detected"
    r"|molecular findings?"
    r"|genomic findings?"
    r"|(?:flt3|npm1)\s*:[^\r\n]*(?:mutations?|variants?|deletions?)[^\r\n]*(?:detected|identified)[^\r\n]*"
    r"|(?:no\s+)?bcr[-_ ]?abl1?\s+transcript\s+detected\.?"
    r")\s*:?[ \t]*$"
)
NON_FINDING_HEADING = re.compile(
    r"(?im)^\s*(?:"
    r"---+"
    r"|technical information"
    r"|method(?:ology)?"
    r"|clinical interpretation"
    r"|interpretation"
    r"|genes? (?:tested|covered)"
    r"|panel content"
    r"|regions with insufficient coverage"
    r"|definition of tier i and ii variants"
    r"|fusion genes included in rna assay"
    r"|additional assessed regions of interest"
    r"|limitations?"
    r")\s*:?[ \t]*$"
)
ANY_HEADING = re.compile(r"(?m)^\s*(?:---+|[A-Za-z][A-Za-z /_-]{2,60}:?[ \t]*|(?:FLT3|NPM1)\s*:[^\r\n]{2,80})$")


def finding_sections(text: str) -> tuple[tuple[int, int, str], ...]:
    """Return conservative finding sections and their source offsets."""

    matches = list(FINDING_HEADING.finditer(text))
    sections: list[tuple[int, int, str]] = []
    for match in matches:
        start = match.start()  # include result-bearing headings such as FLT3: ... DETECTED
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
    r"\b(?:"
    r"no[^\r\n]{0,80}(?:variant|mutation|fusion|transcript)s?[^\r\n]{0,30}(?:identified|detected|found)"
    r"|there (?:was|is) no evidence of[^\r\n]{0,80}(?:variant|mutation|fusion)"
    r"|negative for"
    r"|not detected"
    r")\b",
    re.I,
)
FUSION_RE = re.compile(r"\b([A-Z0-9]{2,15})(?:\(\d+\))?\s*(?:-|::|/)\s*([A-Z0-9]{2,15})(?:\(\d+\))?\b")


def parse_genetic_report_json_worker(
    text: object,
    gene_symbols: Sequence[str],
    _parser_version: str = PARSER_VERSION,
    _state: dict[str, object] = {},
) -> str:
    """Return parser JSON from a worker-self-contained implementation.

    The alias index and static regular expressions are initialized once per
    deserialized Python worker function, rather than rebuilding and compiling
    roughly 100,000 alias expressions for every report row.
    """

    import json as _json
    import re as _re

    if _state.get("gene_symbols_owner") is not gene_symbols:
        symbols = frozenset(str(s).upper() for s in gene_symbols if s)
        lengths_by_first: dict[str, tuple[int, ...]] = {}
        mutable_lengths: dict[str, set[int]] = {}
        for symbol in symbols:
            mutable_lengths.setdefault(symbol[0], set()).add(len(symbol))
        for first, lengths in mutable_lengths.items():
            lengths_by_first[first] = tuple(sorted(lengths))

        _state.clear()
        _state.update(
            {
                "gene_symbols_owner": gene_symbols,
                "symbols": symbols,
                "lengths_by_first": lengths_by_first,
                "boundary_char_re": _re.compile(r"[A-Z0-9]", _re.I),
                "finding_heading": _re.compile(
                    r"(?im)^\s*(?:"
                    r"findings?"
                    r"|results?"
                    r"|detected variants?"
                    r"|detected fusions?"
                    r"|variants? identified"
                    r"|variants? detected"
                    r"|variant\(s\) detected"
                    r"|molecular findings?"
                    r"|genomic findings?"
                    r"|(?:flt3|npm1)\s*:[^\r\n]*(?:mutations?|variants?|deletions?)[^\r\n]*(?:detected|identified)[^\r\n]*"
                    r"|(?:no\s+)?bcr[-_ ]?abl1?\s+transcript\s+detected\.?"
                    r")\s*:?[ \t]*$"
                ),
                "non_finding_heading": _re.compile(
                    r"(?im)^\s*(?:"
                    r"---+"
                    r"|technical information"
                    r"|method(?:ology)?"
                    r"|clinical interpretation"
                    r"|interpretation"
                    r"|genes? (?:tested|covered)"
                    r"|panel content"
                    r"|regions with insufficient coverage"
                    r"|definition of tier i and ii variants"
                    r"|fusion genes included in rna assay"
                    r"|additional assessed regions of interest"
                    r"|limitations?"
                    r")\s*:?[ \t]*$"
                ),
                "any_heading": _re.compile(r"(?m)^\s*(?:---+|[A-Za-z][A-Za-z /_-]{2,60}:?[ \t]*|(?:FLT3|NPM1)\s*:[^\r\n]{2,80})$"),
                "hgvs_c_re": _re.compile(
                    r"(?<![A-Za-z0-9_])(?:c\.|g\.|m\.|n\.)[^\s,;()]+", _re.I
                ),
                "hgvs_p_re": _re.compile(
                    r"(?<![A-Za-z0-9_])p\.\(?[A-Za-z*?=0-9_]+\)?", _re.I
                ),
                "transcript_re": _re.compile(
                    r"\b(?:NM|NR|ENST)_?\d+(?:\.\d+)?\b", _re.I
                ),
                "vaf_re": _re.compile(
                    r"\b(?:VAF|variant allele frequency)\s*[:=]?\s*(<?\d+(?:\.\d+)?)\s*%",
                    _re.I,
                ),
                "class_re": _re.compile(
                    r"\b(pathogenic|likely pathogenic|variant of uncertain significance|VUS|likely benign|benign|tier\s*[1-4IV]+)\b",
                    _re.I,
                ),
                "negative_re": _re.compile(
                    r"\b(?:"
                    r"no[^\r\n]{0,80}(?:variant|mutation|fusion|transcript)s?[^\r\n]{0,30}(?:identified|detected|found)"
                    r"|there (?:was|is) no evidence of[^\r\n]{0,80}(?:variant|mutation|fusion)"
                    r"|negative for"
                    r"|not detected"
                    r")\b",
                    _re.I,
                ),
                "fusion_re": _re.compile(
                    r"\b([A-Z0-9]{2,15})(?:\(\d+\))?\s*(?:-|::|/)\s*([A-Z0-9]{2,15})(?:\(\d+\))?\b"
                ),
            }
        )

    source = "" if text is None else str(text)
    symbols = _state["symbols"]
    lengths_by_first = _state["lengths_by_first"]
    boundary_char_re = _state["boundary_char_re"]
    finding_heading = _state["finding_heading"]
    non_finding_heading = _state["non_finding_heading"]
    any_heading = _state["any_heading"]
    hgvs_c_re = _state["hgvs_c_re"]
    hgvs_p_re = _state["hgvs_p_re"]
    transcript_re = _state["transcript_re"]
    vaf_re = _state["vaf_re"]
    class_re = _state["class_re"]
    negative_re = _state["negative_re"]
    fusion_re = _state["fusion_re"]

    def _upper_preserving_offsets(value: str) -> str:
        chars: list[str] = []
        for char in value:
            upper = char.upper()
            chars.append(upper if len(upper) == 1 else char)
        return "".join(chars)

    def gene_matches(value: str) -> list[tuple[int, int, str]]:
        if not value:
            return []
        upper_value = _upper_preserving_offsets(value)
        found: list[tuple[int, int, str]] = []
        last_end_by_symbol: dict[str, int] = {}
        value_len = len(value)
        for start in range(value_len):
            if start and boundary_char_re.fullmatch(value[start - 1]):
                continue
            lengths = lengths_by_first.get(upper_value[start], ())
            for width in lengths:
                end = start + width
                if end > value_len:
                    break
                if end < value_len and boundary_char_re.fullmatch(value[end]):
                    continue
                symbol = upper_value[start:end]
                if symbol not in symbols:
                    continue
                if start < last_end_by_symbol.get(symbol, -1):
                    continue
                found.append((start, end, symbol))
                last_end_by_symbol[symbol] = end
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
        start = heading.start()  # include result-bearing headings such as FLT3: ... DETECTED
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
            context_start = max(0, sentence_match.start() - 160)
            negative_context = section[context_start:sentence_match.end()]
            if negative_re.search(negative_context):
                continue
            # Short HGNC symbols overlap ordinary English words. Within a finding
            # section, only an exact uppercase source token may supply the gene.
            genes = [
                match
                for match in gene_matches(sentence)
                if sentence[match[0]:match[1]] == match[2]
            ]
            fusion = fusion_re.search(sentence)
            if fusion and (
                fusion.group(1).upper() not in symbols
                or fusion.group(2).upper() not in symbols
            ):
                fusion = None
            hgvs_c = hgvs_c_re.search(sentence)
            hgvs_p = hgvs_p_re.search(sentence)
            transcript = transcript_re.search(sentence)
            vaf = vaf_re.search(sentence)
            classification = class_re.search(sentence)
            positive_detection = _re.search(
                r"\b(?:"
                r"(?:mutation|variant|fusion|transcript|insertion|duplication)[^\r\n]{0,50}(?:detected|identified|present)"
                r"|(?:detected|identified)[^\r\n]{0,50}(?:mutation|variant|fusion|transcript|insertion|duplication)"
                r")\b",
                sentence,
                _re.I,
            )
            if not (fusion or hgvs_c or hgvs_p or (genes and positive_detection)):
                continue
            primary = genes[0][2] if genes else (fusion.group(1).upper() if fusion else None)
            partner = fusion.group(2).upper() if fusion else None
            findings.append(
                {
                    "reported_gene_symbol": primary,
                    "partner_gene_symbol": partner,
                    "alteration_type": "fusion"
                    if fusion
                    else ("sequence_variant" if hgvs_c or hgvs_p else "other"),
                    "detection_status": "detected",
                    "hgvs_c_raw": hgvs_c.group(0) if hgvs_c else None,
                    "hgvs_p_raw": hgvs_p.group(0) if hgvs_p else None,
                    "transcript": transcript.group(0) if transcript else None,
                    "vaf_raw": vaf.group(0) if vaf else None,
                    "vaf": float(vaf.group(1)) / 100.0 if vaf else None,
                    "reported_classification": classification.group(0)
                    if classification
                    else None,
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
