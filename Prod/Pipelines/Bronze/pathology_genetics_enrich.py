# src/pathology_genetics_enrich.py
"""GEN4 enrich worker: structure around the v3 genetics parser's findings.

v3 (pathology_rules.parse_genetic_report_json_worker) finds variants. This worker reads the
same report text and v3's JSON and adds what v3 does not model:
  - assay components (a myeloid report holds a DNA and an RNA panel; an FLT3 report also
    reports NPM1), each with its own result status, including not_performed,
    duplicate_request and sent_away;
  - per-finding extras (component, COSMIC id, ITD length, pathogenicity, transcript);
  - regions the lab could not assess, the panel's transcript list, and report-level fields.
It never adds or removes findings: finding_extras[i] describes v3 findings[i].
Self-contained for Spark workers: only stdlib imports, and rtf_to_plain from pathology_rules,
which is pickled by value like this function.
"""

from __future__ import annotations

from pathology_rules import rtf_to_plain

ENRICH_VERSION = "1.0.0"

# Profile → (primary component, optional secondary component). Secondary components exist
# only when the report text contains their section (see _components()).
PROFILE_COMPONENTS = {
    "myeloid_ngs_mngs": ("mngs_dna", "mngs_rna"),
    "flt3": ("flt3", "npm1"),
    "tumour_ngs_tngs": ("tngs", None),
    "bcr_abl_qual": ("bcr_abl1_qual", None),
}


def enrich_genetic_report_json_worker(text, v3_json, profile_id, _version=ENRICH_VERSION, _state={}):
    """Return the GEN4 enrichment JSON for one report (see module docstring)."""
    import bisect as _bisect
    import json as _json
    import re as _re

    if not _state:
        _state.update(
            {
                # Admin outcomes, checked in this order: a duplicate is also "not performed".
                "duplicate_re": _re.compile(r"(?i)\bduplicate\s+(?:request|sample|test)\b|\bdupl?icate\s+sample\b"),
                "not_performed_re": _re.compile(
                    r"(?i)\bnot\s+(?:performed|tested|indicated|required|done)\b|\btest\s+cancelled\b"
                ),
                "sent_away_re": _re.compile(
                    r"(?i)\b(?:sent|referred|forwarded)\s+(?:away\s+)?to\b|\breferral\s+laborator(?:y|ies)\b"
                ),
                "failed_re": _re.compile(
                    r"(?i)\bfail(?:ed|ure)\b|\binsufficient\s+(?:DNA|RNA|quality|material|sample)\b"
                    r"|\bunable\s+to\s+(?:successfully\s+)?amplify\b"
                ),
                "negative_re": _re.compile(
                    r"(?i)\bno\s+[^.\r\n]{0,60}?\b(?:variants?|mutations?|fusions?|transcripts?|insertions?)\b"
                    r"[^.\r\n]{0,30}?\b(?:detected|identified|found)\b|\bwild[- ]type\b|\bnot\s+detected\b"
                ),
                "dna_section_re": _re.compile(r"(?i)\bDNA\s+panel\s*:"),
                "rna_section_re": _re.compile(r"(?i)\bRNA\s+panel\s*:"),
                "section_stop_re": _re.compile(
                    r"(?i)\bclinical\s+interpretation\b|\binterpretation\s*:|\btechnical\s+information\b"
                    r"|\bregions?\s+with\s+insufficient\s+coverage\b|\bmethod(?:ology)?\s*:"
                ),
                "npm1_re": _re.compile(r"\bNPM1\b"),
                "npm1_section_re": _re.compile(r"\bNPM1\s*:?"),
                "block_sep_re": _re.compile(r"(?m)\s-{2,}\s|^[ \t]*-{2,}"),
                "cosmic_re": _re.compile(r"\bCOSM\d+\b"),
                "ins_nt_re": _re.compile(r"ins<(\d{1,4})\s*nt>"),
                "bp_re": _re.compile(
                    r"(?i)\b(\d{1,4})\s*(?:bp|base\s*pairs?)\b(?=[^.]{0,40}\b(?:insertion|duplication|ITD)\b)"
                    r"|\b(?:insertion|duplication|ITD)\b[^.]{0,40}?\b(\d{1,4})\s*(?:bp|base\s*pairs?)\b"
                ),
                "pathogenicity_re": _re.compile(
                    r"(?i)\b(likely\s+pathogenic|pathogenic|variant\s+of\s+uncertain\s+significance"
                    r"|uncertain\s+significance|VUS|likely\s+benign|benign)\b"
                ),
                "gap_head_re": _re.compile(
                    r"(?i)regions?\s+with\s+insufficient\s+coverage|not\s+assessed\s+due\s+to\s+insufficient\s+sequence\s+data"
                ),
                "gap_stop_re": _re.compile(
                    r"(?i)\btechnical\s+information\b|\bgenes?\s*\(exons?\)\s+included\b|\bclinical\s+interpretation\b"
                ),
                "gap_gene_re": _re.compile(
                    r"\b([A-Z][A-Z0-9]{1,14})\s*((?:\(\s*chr[0-9XY]{1,2}:\d+-\d+(?:\s*,\s*chr[0-9XY]{1,2}:\d+-\d+)*\s*\)\s*)+)"
                ),
                "chr_range_re": _re.compile(r"chr[0-9XY]{1,2}:\d+-\d+"),
                "panel_tx_re": _re.compile(r"\b([A-Z][A-Z0-9]{1,14})\*?\s*\([^()]{0,60}?\b(NM_\d+\.\d+)"),
                "site_re": _re.compile(
                    r"(?i)sample\s+type\s*/\s*site\s*:\s*([\s\S]{1,200}?)(?=\s+block\s*:|\s+tumou?r\s+(?:cell\s+)?content|\s{2,}|[\r\n]|$)"
                ),
                "block_re": _re.compile(
                    r"(?i)\bblock\s*:\s*([A-Za-z0-9][A-Za-z0-9 /.-]{0,40}?)(?=\s+tumou?r\s+(?:cell\s+)?content|\s{2,}|[\r\n]|$)"
                ),
                "tumour_re": _re.compile(r"(?i)tumou?r\s+(?:cell\s+)?content\s*:?\s*(([<>~]?)\s*\d{1,3}(?:\.\d+)?\s*%)"),
                "clinical_re": _re.compile(
                    r"(?i)clinical\s+(?:details|information)\s*:\s*([\s\S]{1,600}?)(?=\s+(?:sample\s+type|DNA\s+panel|RNA\s+panel"
                    r"|detected\s+variants|detected\s+fusions|block\s*:|tumou?r\s+content|clinical\s+interpretation"
                    r"|gene\s+(?:variant|result)\s+vaf)\b|$)"
                ),
                "interp_re": _re.compile(
                    r"(?i)clinical\s+interpretation\s*:?\s*([\s\S]{1,4000}?)(?=\s*(?:technical\s+information|regions?\s+with"
                    r"\s+insufficient|genes?\s*\(exons?\)\s+included|method(?:ology)?\s*:|\*\s*variant\s+allele)|$)"
                ),
                "response_re": _re.compile(r"(?i)[^.]*\b(unlikely|likely)\s+to\s+respond\s+to\s+[^.]*\.?"),
                "not_assessed_re": _re.compile(r"(?i)\bnot\s+(?:been\s+)?assessed\s+or\s+reported\b"),
                # Panel-content lists (same headings as v3's panel_list_re) and where each list ends.
                "panel_head_re": _re.compile(
                    r"(?i)\bgenes?\s*\(exons?\)\s+included\s+in\s+[A-Za-z]+\s+assay"
                    r"|\bfusion\s+genes?\s+included\s+in\s+[A-Za-z]+\s+assay"
                    r"|\bhotspot\s+regions?\s+within\s+the\s+following\s+genes"
                    r"|\bgenes?\s+(?:tested|covered)\s*:|\bpanel\s+content\s*:"
                ),
                "panel_stop_re": _re.compile(
                    r"(?i)\bclinical\s+interpretation\b|\bmethod(?:ology)?\s*:|\bregions?\s+with\s+insufficient\s+coverage\b"
                ),
                "gene_token_re": _re.compile(r"(?<![A-Za-z0-9_.])[A-Z][A-Z0-9]{1,14}(?![A-Za-z0-9_])"),
            }
        )
    s = _state

    raw = "" if text is None else str(text)
    v3 = _json.loads(v3_json)
    findings = v3.get("findings") or []
    plain, offsets = rtf_to_plain(raw)

    def plain_pos(raw_index):
        """Map a raw-text offset (v3 evidence_start) to the plain-text offset."""
        if offsets is None:
            return raw_index
        return max(0, _bisect.bisect_left(offsets, raw_index))

    primary, secondary = PROFILE_COMPONENTS.get(profile_id, (profile_id, None))

    # --- sections: [start, end) in plain text for each component ------------------------------
    sections = {primary: (0, len(plain))}
    if secondary == "mngs_rna":
        rna = s["rna_section_re"].search(plain)
        if rna:
            dna = s["dna_section_re"].search(plain)
            stop = s["section_stop_re"].search(plain, rna.end())
            sections = {
                "mngs_dna": (dna.start() if dna and dna.start() < rna.start() else 0, rna.start()),
                "mngs_rna": (rna.start(), stop.start() if stop else len(plain)),
            }
    elif secondary == "npm1" and s["npm1_re"].search(plain):
        head = s["npm1_section_re"].search(plain)
        sep = s["block_sep_re"].search(plain, head.end())
        stop = s["section_stop_re"].search(plain, head.end())
        npm1_end = min([m.start() for m in (sep, stop) if m] + [len(plain)])
        sections = {"flt3": (0, head.start()), "npm1": (head.start(), npm1_end)}
        if head.start() == 0:  # NPM1 reported first: FLT3 is everything after its block
            sections["flt3"] = (npm1_end, len(plain))

    # --- finding → component ----------------------------------------------------------------------
    def component_of(finding):
        if len(sections) == 1:
            return primary
        if secondary == "npm1":
            return "npm1" if (finding.get("reported_gene_symbol") or "").upper() == "NPM1" else "flt3"
        start = plain_pos(int(finding.get("evidence_start") or 0))
        rna_start, rna_end = sections["mngs_rna"]
        if finding.get("alteration_type") == "fusion" or rna_start <= start < rna_end:
            return "mngs_rna"
        return "mngs_dna"

    assigned = [component_of(f) for f in findings]

    # --- status per component -------------------------------------------------------------------
    def admin_status(section_text):
        for name, key in (("duplicate_request", "duplicate_re"), ("not_performed", "not_performed_re"),
                          ("sent_away", "sent_away_re")):
            if s[key].search(section_text):
                return name
        return None

    components = []
    for name, (start, end) in sections.items():
        section_text = plain[start:end]
        if name in assigned:
            status = "detected"
        elif len(sections) == 1:
            status = v3.get("overall_result_status") or "unknown"
            if status == "unknown":
                status = admin_status(section_text) or "unknown"
        else:
            status = admin_status(section_text)
            if status is None:
                if s["failed_re"].search(section_text):
                    status = "failed"
                elif s["negative_re"].search(section_text):
                    status = "not_detected"
                elif name == "mngs_dna" and not s["dna_section_re"].search(plain):
                    # RNA-only report (e.g. eosinophilia fusion screen): no DNA panel section and no DNA evidence.
                    status = "not_performed"
                else:
                    status = "unknown"
        components.append(
            {
                "assay_component": name,
                "is_primary": name == primary,
                "overall_result_status": status,
                "section_start": start,
                "section_end": end,
            }
        )

    # --- per-finding extras ------------------------------------------------------------------------
    panel_transcripts = {}
    for m in s["panel_tx_re"].finditer(plain):
        panel_transcripts.setdefault(m.group(1), m.group(2))

    def pathogenicity(classification):
        m = s["pathogenicity_re"].search(classification or "")
        if not m:
            return None
        word = _re.sub(r"\s+", " ", m.group(1).lower())
        return {"vus": "uncertain_significance", "variant of uncertain significance": "uncertain_significance",
                "uncertain significance": "uncertain_significance"}.get(word, word.replace(" ", "_"))

    def itd_length(finding, component):
        if (finding.get("reported_gene_symbol") or "").upper() not in ("FLT3", "NPM1"):
            return None
        m = s["ins_nt_re"].search(finding.get("hgvs_c_raw") or "")
        if m:
            return int(m.group(1))
        start, end = sections[component]
        sentence_end = plain.find(".", plain_pos(int(finding.get("evidence_start") or 0)))
        window = (finding.get("evidence_text") or "") + " " + plain[start:sentence_end + 1 if sentence_end >= 0 else end]
        m = s["bp_re"].search(window)
        return int(m.group(1) or m.group(2)) if m else None

    finding_extras = []
    for finding, component in zip(findings, assigned):
        cosmic = s["cosmic_re"].search(finding.get("evidence_text") or "")
        gene = (finding.get("reported_gene_symbol") or "").upper()
        if finding.get("transcript"):
            transcript, source = finding["transcript"], "reported"
        elif gene in panel_transcripts:
            transcript, source = panel_transcripts[gene], "report_panel_list"
        else:
            transcript, source = None, None
        finding_extras.append(
            {
                "assay_component": component,
                "cosmic_id": cosmic.group(0) if cosmic else None,
                "itd_length_bp": itd_length(finding, component),
                "reported_pathogenicity": pathogenicity(finding.get("reported_classification")),
                "transcript": transcript,
                "transcript_source": source,
            }
        )

    # --- tested genes per assay component ------------------------------------------------------------
    # Each panel-content list belongs to the assay its heading names (an RNA/fusion list to the RNA panel), so
    # an assay's denominator does not depend on whether it found anything. v3's genes_tested (HGNC-checked)
    # is the gene universe; a gene in no list belongs to the components whose findings report it, a
    # secondary assay's dedicated gene (NPM1) to that assay, and anything else to the primary assay.
    panel_by_component = {name: set() for name in sections}
    for head in s["panel_head_re"].finditer(plain):
        later = [m.start() for m in (s["panel_head_re"].search(plain, head.end()), s["panel_stop_re"].search(plain, head.end())) if m]
        end = min(later + [min(len(plain), head.end() + 6000)])
        is_rna = bool(_re.search(r"(?i)\bRNA\b|fusion", head.group(0)))
        target = "mngs_rna" if is_rna and "mngs_rna" in sections else primary
        panel_by_component[target].update(t.group(0) for t in s["gene_token_re"].finditer(plain[head.end():end]))
    reported_by = {}
    for finding, component in zip(findings, assigned):
        for gene in (finding.get("reported_gene_symbol"), finding.get("partner_gene_symbol")):
            if gene:
                reported_by.setdefault(gene.upper(), set()).add(component)
    dedicated = {"npm1": "NPM1"}
    component_genes = []
    for gene in sorted(set(v3.get("genes_tested") or [])):
        owners = {c for c, listed in panel_by_component.items() if gene in listed} | reported_by.get(gene, set())
        owners |= {c for c, g in dedicated.items() if g == gene and c in sections}
        if not owners:
            owners = {primary}
        for component in sorted(owners):
            component_genes.append({
                "assay_component": component,
                "gene_symbol": gene,
                "test_scope": "reported_result" if component in reported_by.get(gene, set()) else "panel_content",
            })

    # --- regions the lab could not assess -------------------------------------------------------------
    coverage_gaps = []
    head = s["gap_head_re"].search(plain)
    if head:
        stop = s["gap_stop_re"].search(plain, head.end())
        zone = plain[head.end():stop.start() if stop else min(len(plain), head.end() + 4000)]
        for m in s["gap_gene_re"].finditer(zone):
            coverage_gaps.append(
                {"gene_symbol": m.group(1), "regions_raw": ";".join(s["chr_range_re"].findall(m.group(2)))}
            )

    # --- report-level fields --------------------------------------------------------------------------
    def first_group(key):
        m = s[key].search(plain)
        return _re.sub(r"\s+", " ", m.group(1)).strip(" .;,") if m else None

    tumour = s["tumour_re"].search(plain)
    tumour_raw = _re.sub(r"\s+", "", tumour.group(1)) if tumour else None
    tumour_pct = None
    if tumour and not tumour.group(2):
        tumour_pct = float(_re.search(r"\d{1,3}(?:\.\d+)?", tumour.group(1)).group(0))
    response = s["response_re"].search(plain)
    response_text = _re.sub(r"\s+", " ", response.group(0)).strip() if response else None
    drug_class = None
    if response_text and _re.search(r"(?i)\bEGFR\b", response_text) and _re.search(
        r"(?i)tyrosine[\s-]*kinase|\bTKIs?\b", response_text
    ):
        drug_class = "EGFR TKI"
    report_fields = {
        "sample_type_site_raw": first_group("site_re"),
        "block_raw": first_group("block_re"),
        "tumour_content_raw": tumour_raw,
        "tumour_content_pct": tumour_pct,
        "clinical_details_raw": first_group("clinical_re"),
        "interpretation_text": first_group("interp_re"),
        "predicted_response": (
            None if not response else ("unlikely_responsive" if response.group(1).lower() == "unlikely" else "likely_responsive")
        ),
        "predicted_response_drug_class": drug_class,
        "predicted_response_text": response_text,
        "panel_genes_not_assessed": bool(s["not_assessed_re"].search(plain)),
    }

    return _json.dumps(
        {
            "components": components,
            "component_genes": component_genes,
            "finding_extras": finding_extras,
            "coverage_gaps": coverage_gaps,
            "panel_transcripts": panel_transcripts,
            "report_fields": report_fields,
            "enrich_version": _version,
        },
        sort_keys=True,
    )


# Spark's cloudpickle must carry the worker by value (the module is workspace-only).
enrich_genetic_report_json_worker.__module__ = "__main__"
