"""GEN4 variant mapper: HGVS c. on a RefSeq transcript -> GRCh38 VCF key, in-estate and deterministic.

Inputs are the GEN4 lookups pathology_transcript_model (exon structure) and pathology_reference_window
(genome sequence around panel genes). Nothing is sent outside the estate. Every result carries
coordinate_method so consumers can see how (or why not) coordinates were derived.
Coordinates: models use 0-based half-open genomic intervals (UCSC); outputs are 1-based (VCF).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

GENOME_BUILD = "GRCh38"
_COMPLEMENT = str.maketrans("ACGTN", "TGCAN")
_POSITION = re.compile(r"^([*-]?)(\d+)([+-]\d+)?$")
_HGVS = re.compile(r"^c\.(?P<a>[*-]?\d+(?:[+-]\d+)?)(?:_(?P<b>[*-]?\d+(?:[+-]\d+)?))?(?P<edit>.*)$")


@dataclass(frozen=True)
class TranscriptModel:
    """One transcript's exon structure on the genome (0-based half-open)."""
    accession: str
    chromosome: str
    strand: str
    cds_start: int
    cds_end: int
    exons: list
    mane_status: str | None


class MissingReference(Exception):
    """The reference windows do not cover a requested genomic interval."""


def revcomp(sequence: str) -> str:
    """Reverse complement of a DNA string."""
    return sequence.translate(_COMPLEMENT)[::-1]


def window_fetcher(windows: dict, chunk_size: int = 10_000):
    """Return fetch(chrom, start, end) -> uppercase genome sequence for 0-based [start, end)."""

    def fetch(chrom: str, start: int, end: int) -> str:
        if start < 0 or end <= start:
            raise MissingReference(f"{chrom}:{start}-{end}")
        pieces = []
        chunk = (start // chunk_size) * chunk_size
        while chunk < end:
            sequence = windows.get((chrom, chunk))
            if sequence is None:
                raise MissingReference(f"{chrom}:{chunk}")
            pieces.append(sequence)
            chunk += chunk_size
        joined = "".join(pieces)
        offset = (start // chunk_size) * chunk_size
        result = joined[start - offset:end - offset]
        if len(result) != end - start:
            raise MissingReference(f"{chrom}:{start}-{end}")
        return result

    return fetch


# --- transcript coordinates -------------------------------------------------------------------------
def _exons_in_transcript_order(model: TranscriptModel) -> list:
    """Exons ordered 5'->3' along the transcript."""
    exons = sorted(model.exons)
    return exons if model.strand == "+" else exons[::-1]


def _genomic_to_tx(model: TranscriptModel, genomic: int) -> int:
    """Transcript index (0-based, 5'->3') of an exonic genomic base."""
    cumulative = 0
    for start, end in _exons_in_transcript_order(model):
        if start <= genomic < end:
            return cumulative + (genomic - start if model.strand == "+" else end - 1 - genomic)
        cumulative += end - start
    raise ValueError(f"{model.accession}: genomic {genomic} is not exonic")


def _tx_to_genomic(model: TranscriptModel, index: int) -> int:
    """Genomic 0-based position of a transcript index."""
    cumulative = 0
    for start, end in _exons_in_transcript_order(model):
        length = end - start
        if index < cumulative + length:
            within = index - cumulative
            return start + within if model.strand == "+" else end - 1 - within
        cumulative += length
    raise ValueError(f"{model.accession}: transcript index {index} beyond transcript")


def c_position_to_genomic(model: TranscriptModel, token: str) -> int:
    """Genomic 0-based position of an HGVS c. position token (e.g. 1799, -12, *5, 10+1)."""
    match = _POSITION.match(token)
    if not match:
        raise ValueError(f"bad position {token!r}")
    prefix, number, offset = match.group(1), int(match.group(2)), match.group(3)
    first_coding = model.cds_start if model.strand == "+" else model.cds_end - 1
    last_coding = model.cds_end - 1 if model.strand == "+" else model.cds_start
    if prefix == "":
        index = _genomic_to_tx(model, first_coding) + number - 1
    elif prefix == "-":
        index = _genomic_to_tx(model, first_coding) - number
    else:  # '*': 3' UTR
        index = _genomic_to_tx(model, last_coding) + number
    if index < 0:
        raise ValueError(f"{token} lies before the transcript start")
    genomic = _tx_to_genomic(model, index)
    if offset:
        step = int(offset)
        genomic = genomic + step if model.strand == "+" else genomic - step
    return genomic


# --- VCF normalisation ------------------------------------------------------------------------------
def normalise(chrom: str, pos: int, ref: str, alt: str, fetch):
    """Left-align and trim a VCF allele pair (1-based pos); None when ref == alt."""
    if ref == alt:
        return None
    while True:
        changed = False
        if ref and alt and ref[-1] == alt[-1]:
            ref, alt, changed = ref[:-1], alt[:-1], True
        if not ref or not alt:
            base = fetch(chrom, pos - 2, pos - 1)
            ref, alt, pos, changed = base + ref, base + alt, pos - 1, True
        if not changed:
            break
    while len(ref) > 1 and len(alt) > 1 and ref[0] == alt[0]:
        ref, alt, pos = ref[1:], alt[1:], pos + 1
    return pos, ref, alt


def _result(model, method, interval=None, normalised=None):
    """Assemble the mapper output dict.

    Coordinates are published only together with what they describe: the normalised VCF allele
    (position_start = VCF pos, position_end = pos + len(ref) - 1), or, for an insertion of unstated
    sequence, the HGVS interval (position only). Rejections (ref_mismatch etc.) publish no coordinates.
    """
    ref = alt = key = start = end = None
    if normalised:
        pos, ref, alt = normalised
        start, end = pos, pos + len(ref) - 1
        key = f"{model.chromosome}:{pos}:{ref}:{alt}"
    elif interval:
        start, end = interval[0] + 1, interval[1] + 1
    located = start is not None
    if method == "transcript_model" and not located:
        method = "no_change"  # the edit normalised to ref == alt
    return {
        "used_transcript": model.accession if model else None,
        "genome_build": GENOME_BUILD if located else None,
        "chromosome": model.chromosome if located else None,
        "position_start": start,
        "position_end": end,
        "ref_allele": ref,
        "alt_allele": alt,
        "variant_key": key,
        "coordinate_method": method,
    }


def map_hgvs_c(model: TranscriptModel, hgvs_c: str, fetch) -> dict:
    """Map one c. HGVS on one transcript model to GRCh38 (see module docstring)."""
    match = _HGVS.match(hgvs_c or "")
    if not match:
        return _result(model, "unsupported_hgvs")
    try:
        a = c_position_to_genomic(model, match.group("a"))
        b = c_position_to_genomic(model, match.group("b")) if match.group("b") else a
    except ValueError:
        return _result(model, "position_outside_model")
    lo, hi = min(a, b), max(a, b)
    edit = match.group("edit")
    chrom = model.chromosome
    on_genome = (lambda s: s) if model.strand == "+" else revcomp
    try:
        sub = re.fullmatch(r"([ACGT])>([ACGT])", edit)
        if sub:
            ref = fetch(chrom, lo, lo + 1)
            if ref != on_genome(sub.group(1)):
                return _result(model, "ref_mismatch")
            return _result(model, "transcript_model", normalised=(lo + 1, ref, on_genome(sub.group(2))))
        deletion = re.fullmatch(r"del([ACGT]*)", edit)
        if deletion:
            deleted = fetch(chrom, lo, hi + 1)
            if deletion.group(1) and on_genome(deletion.group(1)) != deleted:
                return _result(model, "ref_mismatch")
            anchor = fetch(chrom, lo - 1, lo)
            return _result(model, "transcript_model", normalised=normalise(chrom, lo, anchor + deleted, anchor, fetch))
        duplication = re.fullmatch(r"dup([ACGT]*)", edit)
        if duplication:
            copied = fetch(chrom, lo, hi + 1)
            if duplication.group(1) and on_genome(duplication.group(1)) != copied:
                return _result(model, "ref_mismatch")
            anchor = fetch(chrom, hi, hi + 1)
            return _result(model, "transcript_model", normalised=normalise(chrom, hi + 1, anchor, anchor + copied, fetch))
        if re.fullmatch(r"ins(?:<\d+\s*nt>|\(\d+\)|N+|\d+)", edit):
            return _result(model, "transcript_model_position_only", interval=(lo, hi))
        insertion = re.fullmatch(r"ins([ACGT]+)", edit)
        if insertion:
            anchor = fetch(chrom, lo, lo + 1)
            inserted = on_genome(insertion.group(1))
            return _result(model, "transcript_model", normalised=normalise(chrom, lo + 1, anchor, anchor + inserted, fetch))
        delins = re.fullmatch(r"delins([ACGT]+)", edit)
        if delins:
            ref = fetch(chrom, lo, hi + 1)
            return _result(model, "transcript_model", normalised=normalise(chrom, lo + 1, ref, on_genome(delins.group(1)), fetch))
    except MissingReference:
        return _result(model, "reference_window_missing")
    return _result(model, "unsupported_hgvs")



# --- protein consistency: the stated p. must match the codon the c. change sits in ------------------------------
_BASES = "TCAG"
_AMINO = "FFLLSSSSYY**CC*WLLLLPPPPHHQQRRRRIIIMTTTTNNKKSSRRVVVVAAAADDEEGGGG"
_CODE = {a + b + c: _AMINO[16 * i + 4 * j + k] for i, a in enumerate(_BASES) for j, b in enumerate(_BASES) for k, c in enumerate(_BASES)}
_THREE = {"A": "Ala", "R": "Arg", "N": "Asn", "D": "Asp", "C": "Cys", "Q": "Gln", "E": "Glu", "G": "Gly", "H": "His",
          "I": "Ile", "L": "Leu", "K": "Lys", "M": "Met", "F": "Phe", "P": "Pro", "S": "Ser", "T": "Thr", "W": "Trp",
          "Y": "Tyr", "V": "Val", "*": "Ter"}
_C_SUB = re.compile(r"^c\.(\d+)[ACGT]>[ACGT]$")
_P_SUB = re.compile(r"^p\.\(?([A-Z][a-z]{2})(\d+)(?:[A-Z][a-z]{2}|Ter|\*|=)\)?$")


def protein_consistent(model: TranscriptModel, hgvs_c: str, hgvs_p: str | None, fetch):
    """True/False when a coding substitution's stated p. can be checked against the genome codon; None otherwise.

    A finding attributed to the wrong gene (and so placed on the wrong transcript) can pass the single reference-base
    check by chance; the codon number and reference amino acid together almost never do.
    """
    c_match, p_match = _C_SUB.match(hgvs_c or ""), _P_SUB.match(hgvs_p or "")
    if not c_match or not p_match:
        return None
    codon_number = (int(c_match.group(1)) + 2) // 3
    if codon_number != int(p_match.group(2)):
        return False
    first = 3 * (codon_number - 1) + 1
    bases = []
    for offset in range(3):
        genomic = c_position_to_genomic(model, str(first + offset))
        base = fetch(model.chromosome, genomic, genomic + 1)
        bases.append(base if model.strand == "+" else revcomp(base))
    return _THREE.get(_CODE.get("".join(bases), "?")) == p_match.group(1)

def resolve_model(gene, transcript, models: dict, mane_by_gene: dict, equivalence: dict):
    """Pick the transcript model the c. HGVS can be placed on, and how.

    exact                         - the stated transcript (accession.version) has a model;
    validated_version_projection  - the stated version has no model, but its CDS is identical to a modelled
                                    version (pathology_transcript_equivalence); plain coding positions only;
    mane_select                   - no transcript was stated: the gene's MANE Select (provenance says so);
    stated_transcript_unavailable - a transcript was stated (NM_, LRG_, ENST) and cannot be validated: no
                                    coordinates, because the same c.N can sit elsewhere on another isoform;
    no_transcript_model           - nothing stated and no MANE model for the gene.
    """
    if transcript:
        if transcript in models:
            return models[transcript], "exact"
        projected = equivalence.get(transcript)
        if projected and projected in models:
            return models[projected], "validated_version_projection"
        return None, "stated_transcript_unavailable"
    mane = mane_by_gene.get((gene or "").upper())
    if mane and mane in models:
        return models[mane], "mane_select"
    return None, "no_transcript_model"


def _protein_check(model, hgvs_c, hgvs_p, fetch):
    """protein_consistent, treating a missing reference window as 'cannot check'."""
    try:
        return protein_consistent(model, hgvs_c, hgvs_p, fetch)
    except (MissingReference, ValueError):
        return None


# c.N or c.N_M followed directly by the edit: no UTR prefix (c.-, c.*) and no intronic offset (c.N+k).
_PLAIN_CODING = re.compile(r"^c\.\d+(?:_\d+)?(?![\d+-])")


def map_finding(gene, transcript, hgvs_c, models: dict, mane_by_gene: dict, fetch, equivalence: dict | None = None,
                hgvs_p: str | None = None) -> dict:
    """Map one finding; coordinate_method records projections, MANE use and why nothing was placed."""
    model, how = resolve_model(gene, transcript, models, mane_by_gene, equivalence or {})
    if model is None:
        out = _result(None, how)
    elif _protein_check(model, hgvs_c, hgvs_p, fetch) is False:
        out = _result(model, "protein_mismatch")
    elif how == "validated_version_projection" and not _PLAIN_CODING.match(hgvs_c or ""):
        # CDS identity says nothing about UTR or intron numbering, so only plain coding positions project.
        out = _result(model, "version_projection_position_not_coding")
    else:
        out = map_hgvs_c(model, hgvs_c, fetch)
        if how == "validated_version_projection" and out["coordinate_method"].startswith("transcript_model"):
            out["coordinate_method"] = out["coordinate_method"].replace("transcript_model", "transcript_model_projected", 1)
    out["transcript_resolution"] = how
    return out
