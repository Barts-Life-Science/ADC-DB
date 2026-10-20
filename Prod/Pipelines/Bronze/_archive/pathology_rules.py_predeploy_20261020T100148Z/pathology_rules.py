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


PARSER_VERSION = "3.0.0"  # content-anchored result zones, row-level VAF/tier, 2026-09-24


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



def parse_genetic_report_json_worker(
    text: object,
    gene_symbols,
    _parser_version: str = PARSER_VERSION,
    _state: dict = {},
) -> str:
    """Return parser JSON from a worker-self-contained implementation.

    Parsing is anchored on report content rather than on headings that stand
    alone on a line: many reports arrive flattened to one line with cells
    separated by runs of spaces. Result zones run from a result marker
    (Detected Variants:, a GENE/VARIANT/VAF table header, DNA/RNA panel:,
    Detected Fusions:, Additional assessed regions of interest) to the next
    narrative or technical marker. Within a zone every coding/protein HGVS
    anchors one finding; its gene is the nearest preceding exact-uppercase HGNC
    token and VAF/tier/classification are read from the same table row. Reports
    without a result zone (FLT3/NPM1 fragment analysis) are read as gene
    status statements. Interpretation narrative and technical panel lists never
    create findings. The alias index and regular expressions are built once per
    deserialized Python worker function.
    """

    import json as _json
    import re as _re

    if _state.get("gene_symbols_owner") is not gene_symbols:
        # HGNC aliases that collide with report vocabulary (table headers,
        # Roman tiers, diagnoses, sample labels, assay names). Only exact
        # uppercase tokens are considered genes, so this list only needs the
        # uppercase words these report families actually use.
        noise = {
            "ALL", "AML", "AND", "CLL", "CML", "CMML", "COSMIC", "DNA", "ET", "GENE",
            "GENES", "HGVS", "I", "II", "III", "ID", "IV", "ITD", "MDS", "MPN", "NGS",
            "NO", "NONE", "NOT", "PCR", "PMF", "PV", "RES", "RESULT", "RNA", "TKD",
            "VAF", "VARIANT", "VUS",
        }
        symbols = frozenset(
            str(s).upper() for s in gene_symbols if s and str(s).upper() not in noise
        )
        _state.clear()
        _state.update(
            {
                "gene_symbols_owner": gene_symbols,
                "symbols": symbols,
                "token_re": _re.compile(r"(?<![A-Za-z0-9_.])[A-Z][A-Z0-9]{1,14}(?![A-Za-z0-9_])"),
                "zone_start_re": _re.compile(
                    r"(?i)(?:"
                    r"\bdetected\s+variants?\s*:"
                    r"|\bvariants?\s+(?:identified|detected)\s*:"
                    r"|\bdetected\s+fusions?\s*:"
                    r"|\b(?:DNA|RNA)\s+panel\s*:"
                    r"|\badditional\s+assessed\s+regions?\s+of\s+interest"
                    r"|\bresults?\s*:"
                    r"|\bmutation\s+result\b"
                    r"|\bgenes?\s+(?:res(?:ult)?s?|variants?|mutations?|hgvs)\s+"
                    r"(?:vaf|res(?:ult)?|variants?|cosmic|classification|hgvs)\b"
                    r")"
                ),
                "zone_stop_re": _re.compile(
                    r"(?im)(?:"
                    r"\bclinical\s+interpretation"
                    r"|\binterpretation\s*:"
                    r"|\bclinical\s+(?:details|information)\s*:"
                    r"|\bregions?\s+with\s+insufficient\s+coverage"
                    r"|\btechnical\s+information"
                    r"|\bgenes?\s*\(exons?\)\s+included"
                    r"|\bfusion\s+genes?\s+included"
                    r"|\bmethod(?:ology)?\s*:"
                    r"|\bdefinition\s+of\s+tier"
                    r"|\*\s*variant\s+allele\s+frequency"
                    r"|^[ \t]*-{2,}"
                    r"|\s-{2,}\s"
                    r")"
                ),
                "body_stop_re": _re.compile(
                    r"(?im)(?:"
                    r"\bregions?\s+with\s+insufficient\s+coverage"
                    r"|\btechnical\s+information"
                    r"|\bgenes?\s*\(exons?\)\s+included"
                    r"|\bmethod(?:ology)?\s*:"
                    r"|^[ \t]*-{2,}"
                    r"|\s-{2,}\s"
                    r")"
                ),
                "panel_list_re": _re.compile(
                    r"(?i)(?:"
                    r"\bgenes?\s*\(exons?\)\s+included\s+in\s+[A-Za-z]+\s+assay"
                    r"|\bfusion\s+genes?\s+included\s+in\s+[A-Za-z]+\s+assay"
                    r"|\bhotspot\s+regions?\s+within\s+the\s+following\s+genes"
                    r"|\bgenes?\s+(?:tested|covered)\s*:"
                    r"|\bpanel\s+content\s*:"
                    r")"
                ),
                "hgvs_c_re": _re.compile(r"(?<![A-Za-z0-9_])c\.(?=[0-9*(?_+-])(?:\[[^\]\s]{1,80}\]|(?!p\.)[^\s,;])+"),
                "hgvs_p_near_re": _re.compile(r"p\.(?:\([^\s()]{1,60}\)|[A-Za-z*=?][^\s,;()]{0,60})"),
                "hgvs_p_re": _re.compile(
                    r"(?<![A-Za-z0-9_])p\.(?:\([^\s()]{1,60}\)|[A-Za-z*=?][^\s,;()]{0,60})"
                ),
                "hgvs_c_syntax_re": _re.compile(
                    r"c\.(?:[-*]?\d+(?:[+-]\d+)?)(?:_[-*]?\d+(?:[+-]\d+)?)?"
                    r"(?:[ACGT]+>[ACGT]+|del[ACGT]*ins[ACGT]+|del[ACGT]*|dup[ACGT]*|ins(?:[ACGT]+|\[[^\]\s]+\])|inv|[ACGT]*\[\d+\]|=)"
                ),
                "hgvs_p_syntax_re": _re.compile(
                    r"p\.(?:\(?(?:=|\?|0)\)?"
                    r"|\(?[A-Z][a-z]{2}\d+(?:_[A-Z][a-z]{2}\d+)?"
                    r"(?:[A-Z][a-z]{2}|Ter|\*|=|\?|del|dup|ins(?:[A-Z][a-z]{2}|\*)+|delins(?:[A-Z][a-z]{2}|\*)+"
                    r"|(?:[A-Z][a-z]{2})?fs(?:(?:Ter|\*)(?:\d+|\?)?)?|(?:[A-Z][a-z]{2})?ext(?:Ter|\*)?(?:-?\d+|\?)?)\)?)"
                ),
                "transcript_re": _re.compile(r"\b(?:NM_\d+(?:\.\d+)?|LRG_\d+(?:t\d+)?|ENST\d+(?:\.\d+)?)\b"),
                "vaf_re": _re.compile(r"(?<![\w.])([<>]?\s*\d{1,3}(?:\.\d+)?)\s*%"),
                "tier_re": _re.compile(r"(?i)\btier\s*(IV|III|II|I|[1-4])\b"),
                "class_re": _re.compile(
                    r"(?i)\b(likely\s+pathogenic|pathogenic|variant\s+of\s+uncertain\s+significance"
                    r"|uncertain\s+significance|VUS|likely\s+benign|benign)\b"
                ),
                "zygosity_re": _re.compile(r"(?i)\b(heterozygous|homozygous|hemizygous)\b"),
                "fusion_re": _re.compile(
                    r"(?<![A-Za-z0-9_])([A-Z][A-Z0-9]{1,14})(?:\([A-Za-z0-9]{1,6}\))?\s*(?:::|-|–|/)\s*"
                    r"([A-Z][A-Z0-9]{1,14})(?:\([A-Za-z0-9]{1,6}\))?(?![A-Za-z0-9_])"
                ),
                "noise": noise,
                # Tissue block labels ("Block A4:", "Blocks A4 and A10") look like
                # symbols (A10 is an HGNC alias) and separate per-block results.
                "block_label_re": _re.compile(
                    r"(?:(?i:\bblocks?\b)[\s:\u00a0]*[A-Z]\d{1,3}(?:[\s\u00a0]*(?:and|&|,)[\s\u00a0]*[A-Z]\d{1,3})*"
                    r"|(?<![A-Za-z0-9_])[A-Z]\d{1,3}(?=[\s\u00a0]*(?::|c\.[0-9*(?_+-])))[\s:\u00a0]*"
                ),
                "separator_label_re": _re.compile(r"(?:(?i:\bblocks?\b)[\s:\u00a0]*)?\b[A-Z]\d{1,3}\b[\s:\u00a0]*"),
                "row_gene_re": _re.compile(
                    r"(?<![A-Za-z0-9_.])(?!COSM\d)([A-Z][A-Z0-9]{2,9})\*?(?:[\s\u00a0]+(?:NM_\d+(?:\.\d+)?|LRG_\d+(?:t\d+)?))?"
                    r"[\s\u00a0]+(?=c\.[0-9*(?_+-]|p\.\(|(?i:no\s+(?:variants?|mutations?)\s+detected))"
                ),
                "zone_statement_re": _re.compile(
                    r"(?i:(?:mutation|variant)s?\s+(?:detected|identified)\s+in\s+(?:the\s+)?)([A-Z][A-Z0-9]{1,14})\b"
                    r"|\b([A-Z][A-Z0-9]{1,14})\s*:?\s*(?i:(?:mutation|variant)s?\s+(?:detected|identified))"
                ),
                "negated_clause_re": _re.compile(r"(?i)\b(?:no|not|negative|without)\b"),
                "row_negative_re": _re.compile(
                    r"(?i)^[\s:;,.\-\u00a0]*(?:not\s+detected|absent|negative)\b"
                ),
                "negative_re": _re.compile(
                    r"(?i)(?:"
                    r"\bno\s+[^.\r\n]{0,60}?\b(?:variants?|mutations?|fusions?|transcripts?|insertions?)\b"
                    r"[^.\r\n]{0,30}?\b(?:detected|identified|found)\b"
                    r"|\bwild[- ]type\b"
                    r"|\bnot\s+detected\b"
                    r"|\bnegative\s+for\b"
                    r"|\bno\s+(?:abnormality|abnormalities|copy\s+number\s+(?:change|variation|abnormality)s?)\s+(?:was\s+|were\s+)?detected\b"
                    r")"
                ),
                "failed_re": _re.compile(
                    r"(?i)(?:\bfail(?:ed|ure)\b|\binsufficient\s+(?:DNA|quality|material|sample)"
                    r"|\bpoor\s+quality\s+DNA|\bof\s+insufficient\s+(?:quality|quantity)|\blow\s+DNA\s+concentration"
                    r"|\bcould\s+not\s+be\s+(?:analysed|analyzed|tested)\b|\bunable\s+to\s+(?:successfully\s+)?amplify)"
                ),
                "indeterminate_re": _re.compile(
                    r"(?i)\b(?:inconclusive|equivocal|indeterminate|could\s+not\s+be\s+determined)\b"
                ),
                "statement_re": _re.compile(
                    r"(?i)\b(FLT3|NPM1)(?:[ \t]*[-\u2011]?[ \t]*(ITD|TKD))?[ \t]*:?[ \t]*"
                    r"(?:(ITD|TKD)[ \t]+)?"
                    r"(no[ \t]+(?:(?:ITD|TKD)[ \t]+)?mutations?[ \t]+detected"
                    r"|mutations?[ \t]+detected|wild[- ]type|positive|not[ \t]+required"
                    r"|not[ \t]+detected|failed)\b"
                ),
                # Legacy BCR-ABL RT-PCR: "BCR-ABL transcript detected with a size
                # compatible with B2A2 translocation"; "No BCR-ABL1 transcript detected".
                "bcr_abl_re": _re.compile(
                    r"(?i)\b(no[ \t]+)?BCR[ \t]*(?:::|[-/\u2011])[ \t]*ABL1?[ \t]+(?:fusion[ \t]+)?transcripts?"
                    r"\s+(?:(?:was|were|is)\s+)?detected\b"
                ),
                "joint_negative_re": _re.compile(r"(?i)\bFLT3\s+and\s+NPM1\s+not\s+detected\b"),
                "subtype_re": _re.compile(
                    r"(?i)(?:detected|positive|peak\s+corresponding\s+to\s+an?)[^.\r\n]{0,40}?\b(ITD|TKD)\b"
                    r"|\bFLT3[-\s](ITD|TKD)\s+positive"
                ),
                "ratio_re": _re.compile(
                    r"(?i)\((high|low)\s+allelic\s+ratio\)|allelic\s+ratio\s+(?:was\s+)?(?:estimated|calculated)"
                    r"\s+to\s+be\s+([<>]?\s*\d+(?:\.\d+)?)"
                ),
            }
        )

    s = _state
    symbols = s["symbols"]
    raw = "" if text is None else str(text)

    # RTF: strip control words to plain text but keep a map back to raw offsets
    # so evidence_start/evidence_end always address the stored report text.
    if raw.lstrip().startswith("{\\rtf"):
        out_chars: list[str] = []
        offsets: list[int] = []
        i = 0
        depth = 0
        skip_depth = None
        n = len(raw)
        while i < n:
            ch = raw[i]
            if ch == "{":
                depth += 1
                i += 1
                continue
            if ch == "}":
                if skip_depth is not None and depth <= skip_depth:
                    skip_depth = None
                depth -= 1
                i += 1
                continue
            if ch == "\\":
                m = _re.match(r"\\([a-zA-Z]+)(-?\d+)? ?|\\'([0-9a-fA-F]{2})|\\(.)", raw[i:i + 40])
                if not m:
                    i += 1
                    continue
                word, hexcode, symbol = m.group(1), m.group(3), m.group(4)
                if skip_depth is None:
                    if word in ("fonttbl", "colortbl", "stylesheet", "info", "pict", "listtable",
                                "listoverridetable", "rsidtbl", "generator", "xmlnstbl"):
                        skip_depth = depth
                    elif symbol == "*":
                        skip_depth = depth
                    elif word in ("par", "line", "row", "sect", "page"):
                        out_chars.append("\n")
                        offsets.append(i)
                    elif word in ("tab", "cell"):
                        out_chars.append("  ")
                        offsets.extend((i, i))
                    elif hexcode:
                        out_chars.append(bytes([int(hexcode, 16)]).decode("cp1252", "replace"))
                        offsets.append(i)
                    elif symbol in ("\\", "{", "}"):
                        out_chars.append(symbol)
                        offsets.append(i)
                    elif symbol == "~":
                        out_chars.append(" ")
                        offsets.append(i)
                i += m.end()
                continue
            if skip_depth is None and ch not in "\r\n":
                out_chars.append(ch)
                offsets.append(i)
            i += 1
        source = "".join(out_chars)
        offsets.append(n)
    else:
        source = raw
        offsets = None

    def raw_span(start: int, end: int) -> tuple[int, int]:
        if offsets is None:
            return start, end
        return offsets[start], (offsets[end - 1] + 1) if end > start else offsets[start]

    def genes_in(start: int, end: int) -> list[tuple[int, int, str]]:
        text = source[start:end]
        labels = [(m.start(), m.end()) for m in s["block_label_re"].finditer(text)]
        return [
            (start + m.start(), start + m.end(), m.group(0))
            for m in s["token_re"].finditer(text)
            if m.group(0) in symbols and not any(a <= m.start() < b for a, b in labels)
        ]

    body_stop = s["body_stop_re"].search(source)
    body_end = body_stop.start() if body_stop else len(source)

    stops = [m.start() for m in s["zone_stop_re"].finditer(source)]
    starts = [(m.start(), m.end()) for m in s["zone_start_re"].finditer(source)]
    zones: list[tuple[int, int]] = []
    for index, (start, marker_end) in enumerate(starts):
        if zones and start < zones[-1][1] and start - zones[-1][0] < 3:
            continue
        bounds = [p for p in stops if p > marker_end][:1] + [st for st, _ in starts[index + 1:index + 2]]
        end = min(bounds) if bounds else len(source)
        if zones and zones[-1][1] > start:
            zones[-1] = (zones[-1][0], start)
        zones.append((start, end))

    panel_genes: set[str] = set()
    for m in s["panel_list_re"].finditer(source):
        nxt = next((p for p in stops if p > m.end() + 1 and p > m.start() + 40), None)
        end = min(nxt if nxt is not None else len(source), m.end() + 6000)
        # A later "Fusion Genes included" heading is itself a stop, so each list ends there.
        later = s["panel_list_re"].search(source, m.end())
        if later and later.start() < end:
            end = later.start()
        panel_genes.update(g for _, _, g in genes_in(m.end(), end))

    findings: list[dict] = []
    reported: set[str] = set()
    negative_evidence = False

    def tail_fields(tail: str) -> dict:
        vaf = s["vaf_re"].search(tail)
        tier = s["tier_re"].search(tail)
        klass = s["class_re"].search(tail)
        zyg = s["zygosity_re"].search(tail)
        vaf_raw = vaf.group(0).strip() if vaf else None
        vaf_value = None
        if vaf and not vaf.group(1).lstrip().startswith(("<", ">")):
            vaf_value = round(float(vaf.group(1)) / 100.0, 6)
            if vaf_value > 1.0:
                vaf_raw, vaf_value = None, None
        tier_value = None
        if tier:
            tier_value = {"1": "I", "2": "II", "3": "III", "4": "IV"}.get(tier.group(1), tier.group(1).upper())
        classification = None
        if tier and klass:
            classification = tail[min(tier.start(), klass.start()):max(tier.end(), klass.end())]
        elif tier or klass:
            classification = (tier or klass).group(0)
        return {
            "vaf_raw": vaf_raw,
            "vaf": vaf_value,
            "reported_tier": tier_value,
            "reported_classification": _re.sub(r"\s+", " ", classification).strip() if classification else None,
            "zygosity": zyg.group(1).lower() if zyg else None,
        }

    def hgvs_checked(value, syntax_re):
        if value is None:
            return None, None
        cleaned = value.rstrip(".,;:")
        if cleaned.count(")") > cleaned.count("("):
            cleaned = cleaned[: cleaned.rfind(")")] + cleaned[cleaned.rfind(")") + 1:]
        return cleaned, (cleaned if syntax_re.fullmatch(cleaned) else None)

    def add_finding(evidence_start, evidence_end, **fields):
        while evidence_end > evidence_start and source[evidence_end - 1] in " \t\r\n\u00a0":
            evidence_end -= 1
        while evidence_start < evidence_end and source[evidence_start] in " \t\r\n\u00a0":
            evidence_start += 1
        raw_start, raw_end = raw_span(evidence_start, evidence_end)
        finding = {
            "reported_gene_symbol": None,
            "partner_gene_symbol": None,
            "alteration_type": "other",
            "detection_status": "detected",
            "hgvs_c_raw": None,
            "hgvs_c_parsed": None,
            "hgvs_p_raw": None,
            "hgvs_p_parsed": None,
            "hgvs_validation_status": "not_validated",
            "transcript": None,
            "vaf_raw": None,
            "vaf": None,
            "reported_classification": None,
            "reported_tier": None,
            "zygosity": None,
            "ratio_raw": None,
            "evidence_text": source[evidence_start:evidence_end],
            "evidence_start": raw_start,
            "evidence_end": raw_end,
        }
        finding.update(fields)
        findings.append(finding)

    for zone_start, zone_end in zones:
        zone = source[zone_start:zone_end]
        if s["negative_re"].search(zone):
            negative_evidence = True
        genes = genes_in(zone_start, zone_end)

        anchors: list[tuple[int, int, str, object]] = []
        c_matches = [(zone_start + m.start(), zone_start + m.end(), m.group(0)) for m in s["hgvs_c_re"].finditer(zone)]
        p_matches = [(zone_start + m.start(), zone_start + m.end(), m.group(0)) for m in s["hgvs_p_re"].finditer(zone)]
        used_p: set[int] = set()
        for c_start, c_end, c_text in c_matches:
            pair = None
            fused = s["hgvs_p_near_re"].match(source, c_end)  # c.818G>Ap.(Arg273His)
            if fused:
                pair = (fused.start(), fused.end(), fused.group(0))
            else:
                for p in p_matches:
                    if p[0] >= c_end and p[0] - c_end <= 40 and p[0] not in used_p:
                        between = source[c_end:p[0]]
                        if "c." not in between and not any(c_end <= g[0] < p[0] for g in genes):
                            pair = p
                        break
            if pair:
                used_p.add(pair[0])
            anchors.append((c_start, pair[1] if pair else c_end, c_text, pair))
        for p in p_matches:
            if p[0] not in used_p:
                anchors.append((p[0], p[1], None, p))
        anchors.sort()

        # The gene cell is the uppercase token immediately before a variant or a
        # per-gene negative, even when it is a laboratory typo (SRFS2, DNTM3A);
        # such symbols stay unresolved in the HGNC join rather than being guessed.
        row_genes = {g[0]: g for g in genes}
        for m in s["row_gene_re"].finditer(zone):
            if m.group(1) not in s["noise"]:
                row_genes.setdefault(zone_start + m.start(1), (zone_start + m.start(1), zone_start + m.end(1), m.group(1)))
        genes = sorted(row_genes.values())
        reported.update(g[2] for g in genes if g[2] in symbols)
        gene_starts = [g[0] for g in genes]

        rows = []
        for index, (a_start, a_end, c_text, p) in enumerate(anchors):
            later_bounds = [g for g in gene_starts if g > a_end]
            if index + 1 < len(anchors):
                later_bounds.append(anchors[index + 1][0])
            row_end = min(later_bounds) if later_bounds else zone_end
            preceding = [g for g in genes if g[1] <= a_start]
            rows.append([a_start, a_end, c_text, p, row_end, source[a_end:row_end], preceding[-1] if preceding else None])

        # Stacked cells: a table cell may list several variants, with the VAF and
        # classification cells following in the same order ("c.1 p.1 c.2 p.2
        # 33% 22% Tier I Tier I"). Some tables are wholly column-major: k gene
        # cells, then k variant cells, then k VAF cells. Pair them by position.
        fields_by_row: dict[int, dict] = {}
        column_major: set[int] = set()
        index = 0
        previous_end = zone_start
        while index < len(rows):
            group = [index]
            while group[-1] + 1 < len(rows) and not s["separator_label_re"].sub("", source[
                rows[group[-1]][1]:rows[group[-1] + 1][0]
            ]).strip(" \t\r\n\u00a0"):
                group.append(group[-1] + 1)
            last_tail = rows[group[-1]][5]
            if len(group) > 1:
                block_genes = [g for g in genes if previous_end <= g[0] < rows[group[0]][0]]
                stacked = all(
                    not s["separator_label_re"].sub("", source[a[1]:b[0]]).strip(" \t\r\n\u00a0*")
                    for a, b in zip(block_genes, block_genes[1:] + [(rows[group[0]][0],)])
                )
                if stacked and len(block_genes) == len(group) and len({g[2] for g in block_genes}) > 1:
                    for position, row_index in enumerate(group):
                        rows[row_index][6] = block_genes[position]
                        column_major.add(row_index)
                vafs = list(s["vaf_re"].finditer(last_tail))
                tiers = list(s["tier_re"].finditer(last_tail))
                if len(vafs) == len(group) and len(tiers) in (0, len(group)):
                    for position, row_index in enumerate(group):
                        piece_end = vafs[position + 1].start() if position + 1 < len(vafs) else len(last_tail)
                        piece = last_tail[vafs[position].start():piece_end]
                        if tiers:
                            piece = vafs[position].group(0) + " " + tiers[position].group(0)
                        fields_by_row[row_index] = tail_fields(piece)
            for row_index in group:
                fields_by_row.setdefault(row_index, tail_fields(rows[row_index][5]))
            previous_end = rows[group[-1]][4]
            index = group[-1] + 1

        for index, (a_start, a_end, c_text, p, row_end, tail, gene) in enumerate(rows):
            if s["row_negative_re"].search(tail):
                continue
            evidence_start = a_start
            if index in column_major:
                row_end = a_end  # the row's cells are not contiguous; cite the variant cell
            elif gene and not any(gene[1] <= a[0] < a_start for a in anchors):
                evidence_start = gene[0]
            prefix = "" if index in column_major else source[gene[1] if gene else a_start:a_start]
            transcript = s["transcript_re"].search(prefix) or s["transcript_re"].search(tail)
            c_raw, c_parsed = hgvs_checked(c_text, s["hgvs_c_syntax_re"])
            p_raw, p_parsed = hgvs_checked(p[2] if p else None, s["hgvs_p_syntax_re"])
            if (c_raw and not c_parsed) or (p_raw and not p_parsed):
                validation = "invalid"
            else:
                validation = "partial"  # syntactically valid; not checked against a reference
            probe = c_raw or p_raw or ""
            if c_raw and _re.search(r"\d[ACGT]>[ACGT](?![ACGT])", c_raw):
                alteration = "SNV"
            elif _re.search(r"del|ins|dup|fs", probe):
                alteration = "indel"
            elif p_raw and _re.fullmatch(r"p\.\(?[A-Z][a-z]{2}\d+[A-Z][a-z]{2}\)?", p_raw) and not c_raw:
                alteration = "SNV"
            else:
                alteration = "other"
            add_finding(
                evidence_start,
                row_end,
                reported_gene_symbol=gene[2] if gene else None,
                alteration_type=alteration,
                hgvs_c_raw=c_raw,
                hgvs_c_parsed=c_parsed,
                hgvs_p_raw=p_raw,
                hgvs_p_parsed=p_parsed,
                hgvs_validation_status=validation,
                transcript=transcript.group(0) if transcript else None,
                **fields_by_row[index],
            )

        # Positive statements without HGVS ("MUTATION DETECTED IN BRAF CODON 600").
        anchored = {f["reported_gene_symbol"] for f in findings}
        for m in s["zone_statement_re"].finditer(zone):
            gene = m.group(1) or m.group(2)
            if gene not in symbols or gene in anchored:
                continue
            clause_start = max(zone.rfind(".", 0, m.start()), zone.rfind(":", 0, m.start()))
            if s["negated_clause_re"].search(zone[clause_start + 1:m.start()]):
                continue
            row_end = min([g for g in gene_starts if g > zone_start + m.end()] + [zone_end])
            anchored.add(gene)
            reported.add(gene)
            add_finding(zone_start + m.start(), row_end, reported_gene_symbol=gene)

        for m in s["fusion_re"].finditer(zone):
            first, second = m.group(1), m.group(2)
            if first not in symbols or second not in symbols or first == second:
                continue
            clause_start = max(
                zone.rfind(".", 0, m.start()), zone.rfind(":", 0, m.start()), zone.rfind("\n", 0, m.start())
            )
            clause = zone[clause_start + 1:m.start()]
            after = zone[m.end():m.end() + 40]
            if s["negated_clause_re"].search(clause) or _re.match(r"(?i)[^.\r\n]{0,25}\bnot\s+detected", after):
                continue
            line_end = _re.search(r"\s{2,}|[\r\n]|$", zone[m.end():])
            end = zone_start + m.end() + (line_end.start() if line_end else 0)
            reported.update((first, second))
            add_finding(
                zone_start + m.start(),
                end,
                reported_gene_symbol=first,
                partner_gene_symbol=second,
                alteration_type="fusion",
            )

    statements = []
    if not zones:
        joint = s["joint_negative_re"].search(source[:body_end])
        if joint:
            negative_evidence = True
            reported.update(("FLT3", "NPM1"))
        bcr_abl = [m for m in s["bcr_abl_re"].finditer(source[:body_end])]
        for m in bcr_abl:
            reported.update(("BCR", "ABL1"))
            if m.group(1):
                negative_evidence = True
        positive = next((m for m in bcr_abl if not m.group(1)), None)
        if positive:
            stop = source.find(".", positive.end(), body_end)
            add_finding(
                positive.start(),
                stop + 1 if stop >= 0 else body_end,
                reported_gene_symbol="BCR",
                partner_gene_symbol="ABL1",
                alteration_type="fusion",
            )
        heads = list(s["statement_re"].finditer(source[:body_end]))
        for index, m in enumerate(heads):
            gene = m.group(1).upper()
            status = _re.sub(r"\s+", " ", m.group(4).lower())
            end = heads[index + 1].start() if index + 1 < len(heads) else body_end
            statements.append((gene, status, m, end))
        positives: dict[str, list] = {}
        for gene, status, m, end in statements:
            if status in ("not required",):
                continue
            reported.add(gene)
            if status.startswith("no ") or status in ("wild-type", "wild type", "not detected"):
                negative_evidence = True
                continue
            if status == "failed":
                continue
            # "FLT3 mutation detected (FLT3-ITD positive)" is one finding stated twice.
            if gene in positives:
                positives[gene][2] = end
                if not positives[gene][3]:
                    positives[gene][3] = m.group(2) or m.group(3)
                continue
            positives[gene] = [gene, m, end, m.group(2) or m.group(3)]
        for gene, m, end, subtype in positives.values():
            body = source[m.start():end]
            if not subtype:
                sub = s["subtype_re"].search(body)
                if sub:
                    subtype = sub.group(1) or sub.group(2)
            subtype = subtype.upper() if subtype else None
            if gene == "NPM1" or subtype == "ITD" or _re.search(r"(?i)\b(?:insertion|duplication)s?\b", body):
                alteration = "indel"
            elif subtype == "TKD" and _re.search(r"(?i)\bpoint\s+mutation\b", body):
                alteration = "SNV"
            else:
                alteration = "other"
            ratio = s["ratio_re"].search(body)
            ratio_raw = None
            if ratio:
                ratio_raw = (ratio.group(1).lower() + " allelic ratio") if ratio.group(1) else ratio.group(2).replace(" ", "")
            # Evidence is the first sentence after the statement heading, which
            # carries the subtype, size and ratio without the methodology tail.
            sentence = _re.search(r"[\s\S]*?[.](?=\s|$)", source[m.end():end])
            ev_end = m.end() + (sentence.end() if sentence else len(source[m.end():end]))
            add_finding(
                m.start(),
                min(end, ev_end),
                reported_gene_symbol=gene,
                alteration_type=alteration,
                reported_classification=None,
                ratio_raw=ratio_raw,
            )

    body = source[:body_end]
    if not zones and not statements and s["negative_re"].search(body):
        negative_evidence = True
    failed = bool(s["failed_re"].search(body))
    indeterminate = bool(s["indeterminate_re"].search(body))
    if findings:
        overall = "detected"
    elif failed and negative_evidence:
        overall = "indeterminate"  # part of the assay failed; the rest was negative
    elif failed:
        overall = "failed"
    elif indeterminate:
        overall = "indeterminate"
    elif negative_evidence:
        overall = "not_detected"
    else:
        overall = "unknown"

    for finding in findings:
        if finding["reported_gene_symbol"]:
            reported.add(finding["reported_gene_symbol"])

    return _json.dumps(
        {
            "overall_result_status": overall,
            "genes_tested": sorted(reported | panel_genes),
            "genes_reported": sorted(reported),
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
    hgvs_c_parsed: str | None
    hgvs_p_raw: str | None
    hgvs_p_parsed: str | None
    hgvs_validation_status: str
    transcript: str | None
    vaf_raw: str | None
    vaf: float | None
    reported_classification: str | None
    reported_tier: str | None
    zygosity: str | None
    ratio_raw: str | None
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
    genes_reported: tuple[str, ...] = ()

    def to_json(self) -> str:
        return json.dumps(
            {
                "overall_result_status": self.overall_result_status,
                "genes_tested": self.genes_tested,
                "genes_reported": self.genes_reported,
                "findings": [finding.as_dict() for finding in self.findings],
                "parser_version": self.parser_version,
            },
            sort_keys=True,
        )


def parse_genetic_report(text: object, gene_symbols: Sequence[str]) -> GeneticParse:
    """Parse result zones; technical panel lists are panel content, never findings."""

    payload = json.loads(parse_genetic_report_json_worker(text, gene_symbols))
    return GeneticParse(
        payload["overall_result_status"],
        tuple(payload["genes_tested"]),
        tuple(GeneticFinding(**finding) for finding in payload["findings"]),
        payload["parser_version"],
        tuple(payload["genes_reported"]),
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
