"""WinPath packed-antibiogram grammar (deterministic, side-effect-free).

One LIMS free-text result value can carry several isolate blocks::

    [<*ESCOL //GU5 /AUGR//CIPS//CFXR//MECS//NITR//TMHS//PODs//TEM>]
    [<.d//MCMs//TCMs//AMIs//>]
    [~ as previously isolated

Physical continuations are removed before parsing. Malformed blocks and tokens
are retained with diagnostics so the bronze expansion conserves source content.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field


ANTIBIOGRAM_PARSER_VERSION = "1.0.0"

_WRAP = re.compile(r">\]\s*\[<\.")
_ORPHAN_CONTINUATION = re.compile(r"\[<\.")
_BLOCK_OPEN = "[<*"
_BLOCK_CLOSE = ">]"
_ANNOTATION = re.compile(r"\[~[ \t]*([^\r\n]*)")

MECHANISM_MARKERS = frozenset(
    {"KPC", "NDM", "O48", "VIM", "IMP", "OXA", "ESB", "CCR", "TES"}
)

_SIR = {"s": "S", "r": "R", "i": "I"}
_POSITIVE = frozenset({"+", "p", "y"})
_NEGATIVE = frozenset({"-", "n"})
_MECHANISM_DOMAIN = {
    **{flag: "positive" for flag in _POSITIVE},
    **{flag: "negative" for flag in _NEGATIVE},
}


@dataclass(frozen=True)
class SusceptibilityToken:
    token_ordinal: int
    raw_token: str
    agent_code: str | None
    flag_raw: str | None
    token_class: str
    interpretation: str | None

    def susceptibility_id(self, source_record_key: str, isolate_ordinal: int) -> str:
        return _stable_id(
            "antimicrobial_susceptibility",
            source_record_key,
            isolate_ordinal,
            self.token_ordinal,
        )


@dataclass
class Isolate:
    isolate_ordinal: int
    organism_code: str | None
    panel_code: str | None
    raw_block: str
    tokens: list[SusceptibilityToken] = field(default_factory=list)
    isolate_comment: str | None = None
    parse_status: str = "ok"

    def isolate_id(
        self, pathology_accession_id: str | None, source_record_key: str
    ) -> str:
        return _stable_id(
            "microbiology_isolate",
            pathology_accession_id,
            source_record_key,
            self.isolate_ordinal,
        )


@dataclass
class ParsedAntibiogram:
    parse_status: str
    isolates: list[Isolate]
    parser_version: str = ANTIBIOGRAM_PARSER_VERSION


def _stable_id(namespace: str, *parts: object) -> str:
    payload = "|".join("∅" if part is None else str(part) for part in parts)
    return hashlib.sha256(f"{namespace}|{payload}".encode()).hexdigest()


def unwrap(text: str) -> str:
    """Delete physical wraps so a token split across lines is whole again."""
    return _WRAP.sub("", text)


def _classify(
    agent_code: str | None, flag_raw: str | None
) -> tuple[str, str | None]:
    if agent_code is None or flag_raw is None:
        return "unparsed", None
    flag = flag_raw.lower()
    if agent_code in MECHANISM_MARKERS:
        return "mechanism", _MECHANISM_DOMAIN.get(flag, "indeterminate")
    return "antimicrobial", _SIR.get(flag, "indeterminate")


def _parse_tokens(body: str) -> list[SusceptibilityToken]:
    tokens = []
    for raw in body.split("//"):
        if raw == "":
            continue
        if len(raw) == 4:
            agent = raw[:3].rstrip() or None
            flag = raw[3]
            token_class, interpretation = _classify(agent, flag)
        else:
            agent, flag, token_class, interpretation = None, None, "unparsed", None
        tokens.append(
            SusceptibilityToken(
                len(tokens), raw, agent, flag, token_class, interpretation
            )
        )
    return tokens


def _parse_block(ordinal: int, block: str) -> Isolate:
    # One legacy LIMS 5 row uses ``ORG /1/PANEL/body`` rather than the normal
    # ``ORG //PANEL /body`` header. Recover that bounded shape before looking
    # for the first token separator, which otherwise consumes the first drugs.
    first_slash = block.find("/")
    first_double = block.find("//")
    if first_slash >= 0 and (first_double < 0 or first_slash < first_double):
        parts = block.split("/", 3)
        if len(parts) == 4 and parts[1].strip().isdigit():
            organism = parts[0].strip() or None
            panel_code = parts[2].strip() or None
            return Isolate(
                ordinal, organism, panel_code, block, _parse_tokens(parts[3])
            )
    head, separator, rest = block.partition("//")
    organism = head.strip() or None
    if not separator:
        return Isolate(ordinal, organism, None, block)
    panel, separator, body = rest.partition("/")
    panel_code = panel.strip() or None
    if not separator:
        return Isolate(ordinal, organism, panel_code, block)
    return Isolate(ordinal, organism, panel_code, block, _parse_tokens(body))


def parse_antibiogram(text: str | None) -> ParsedAntibiogram:
    if not text:
        return ParsedAntibiogram("no_block", [])

    diagnostics = set()
    flat = unwrap(text)
    if _ORPHAN_CONTINUATION.search(flat):
        diagnostics.add("orphan_continuation")

    isolates = []
    openers = [match.start() for match in re.finditer(re.escape(_BLOCK_OPEN), flat)]
    for index, start in enumerate(openers):
        limit = openers[index + 1] if index + 1 < len(openers) else len(flat)
        close = flat.find(_BLOCK_CLOSE, start, limit)
        body_end = close if close >= 0 else limit
        isolate = _parse_block(
            len(isolates), flat[start + len(_BLOCK_OPEN) : body_end]
        )
        if close < 0:
            isolate.parse_status = "unterminated_block"
            diagnostics.add("unterminated_block")
        tail = flat[body_end:limit]
        notes = [
            match.group(1).strip()
            for match in _ANNOTATION.finditer(tail)
            if match.group(1).strip()
        ]
        if notes:
            isolate.isolate_comment = " ".join(notes)
        isolates.append(isolate)

    if not isolates and not diagnostics:
        return ParsedAntibiogram("no_block", [])
    return ParsedAntibiogram(";".join(sorted(diagnostics)) or "ok", isolates)


def parse_antibiogram_json(text: str | None) -> str:
    """Return JSON with names matching the Spark UDF schema."""
    parsed = parse_antibiogram(text)
    return json.dumps(
        {
            "parse_status": parsed.parse_status,
            "parser_version": parsed.parser_version,
            "isolates": [asdict(isolate) for isolate in parsed.isolates],
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )
