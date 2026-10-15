"""MAP_LLM_GATE_V1: Claude Opus 5.5 verifies every embedding-sourced mapping before a map commits it.
(MAP_LLM_GATE_OPUS_V1, 2026-10-13: Haiku 5.5 -> Opus 5.5; evidence codes spelled out in the prompt.)

Why. Bare cosine matches put wrong concepts into the maps at every tier (2026-10-11: panel concepts on member
analytes, urine concepts on serum tests, wrong crosswalk entries; ~140M rows). Rule, curated, override and
deterministic mappings bypass the gate; anything an embedding proposed is checked first.

How. Each item is a source term with non-identifying context and a numbered candidate list (the embedding's
proposal plus its next-best guarded candidates). The model returns accept / choose another candidate / reject,
with a reason, as schema-constrained JSON; the answer is validated against the list, so no concept id the
embedding did not propose can come back. Verdicts are cached by (map, key, candidates, model, prompt version) in
VERDICT_TABLE, so reruns never re-ask. If the model cannot be reached the item is HELD: the caller does not commit
it and retries next run. Nothing is ever auto-committed and nothing goes to a human queue: a rejection publishes
as unmapped with provenance.

Callers: map_50_pathology (and its map_split runner) test remap and result arms, device_pipeline GMDN layer.
"""
import concurrent.futures as _cf
import hashlib
import json
import re
import threading
from datetime import datetime, timezone

GATE_SENTINEL = "MAP_LLM_GATE_V1"
MODEL = "claude-opus-5-5"
PROMPT_VERSION = "mlg-2026-10-13.1"
FOUNDRY_RESOURCE = "benja-m6p4lv77-eastus2"   # same Azure AI resource as the pathology embeddings
SECRET_SCOPE, SECRET_KEY = "adc_store", "barts_global_key"
VERDICT_TABLE = "3_lookup.map_gate.llm_verdicts"
MAX_CALLS_PER_RUN = 3000      # budget cap per Python process; overflow is held, not committed
MAX_WORKERS = 8
EFFORT = "high"
MAX_TOKENS = 16000
TOP_K = 8
OUTAGE_TRIP = 5               # consecutive unreachable calls before the rest of the run is held without trying
# Per-map permission to send patient-level text (result values, implant descriptions) to the model. A map set to False
# holds its proposals (never commits them, never rejects them), so turning it on later verifies them. Catalogue-only
# maps (pathology_test) do not need it. Rendered from the deploy config by mlg_apply.
SEND_VALUES = {'pathology_result': True, 'device_gmdn': True}
COST_PER_CALL_USD = 0.04    # Opus 5.5, from the 2026-10-13 backlog run, rounded up for budgeting

# Evidence codes in an item's context, spelled out for the model (MAP_LLM_GATE_OPUS_V1).
CONTEXT_GLOSS = {"dominant unit class": {
    "SCNC": "SCNC = substance concentration (mol/L)",
    "MCNC": "MCNC = mass concentration (g/L, mg/dL)",
    "NCNC": "NCNC = number concentration, a count per volume (10^9/L, /uL), i.e. LOINC [#/volume]",
    "UNITS": "UNITS = catalytic or arbitrary units per volume (U/L, IU/L)",
    "RATE": "RATE = amount per time (per 24 h)",
    "PCT": "PCT = percentage or fraction",
    "PRES": "PRES = pressure (kPa, mmHg)"}}

DECISIONS = ("accept", "choose", "reject")
_SCHEMA = {
    "type": "object",
    "properties": {
        "decision": {"type": "string", "enum": list(DECISIONS)},
        "candidate": {"type": "integer"},
        "reason": {"type": "string"},
    },
    "required": ["decision", "candidate", "reason"],
    "additionalProperties": False,
}

_RUBRICS = {
    "pathology_test": (
        "You verify mappings of hospital laboratory test codes to OMOP standard concepts (LOINC or SNOMED). "
        "Decide whether the PROPOSED concept is the analyte measurement this source test reports. For Cerner keys "
        "the code is the ORDER the result was requested under (it may be a panel or another test) and the "
        "DESCRIPTION is the result itself: judge the description; the order only gives context. For LIMS (TFC) keys "
        "the code is a short test mnemonic. Checks: "
        "(1) analyte: the same substance, cell, antibody class or organism as the description, not a related one "
        "(HbA1c is not HbA, CD25 is not CD27, IgM is not IgG, coproporphyrin I is not III, an APTT under a Factor IX "
        "order is still an APTT). (2) panel vs member: a single analyte must not map to a panel, profile or order "
        "concept (Urea and electrolytes, Liver function panel, Complete blood count, Thyroid function test, "
        "Lymphocyte subsets, Lupus anticoagulant assay) or to the concept of the order it was requested under, unless "
        "the description itself is the panel name. A ratio, index or component result of a screen (DRVVT ratio, dilute "
        "APTT ratio, CD4 %) maps to that specific measurement, not to the screen as a whole. Attribute lines "
        "(Specimen type, Method, Sample type) keep their own attribute concept (for example Specimen type); never move "
        "them to the order's test. By estate convention a comment, report or interpretation line of a test maps to "
        "that test's analyte concept, so accept that for a comment line. (3) specimen: only when the concept names a specimen must it "
        "agree with the source. A specimen named in the description wins over the row evidence. BLOOD evidence covers "
        "serum, plasma, whole blood and red cells (never treat serum vs plasma vs blood as a conflict); FLUID means a "
        "body fluid such as pleural, peritoneal, synovial or drain fluid, not urine, CSF or blood. Point-of-care (POCT) "
        "and blood gas tests are measured in blood: concepts 'in Gas', 'in Pulmonary artery' or 'Expired' are wrong "
        "for them. A concept that names no specimen is compatible with any specimen, but when the evidence or "
        "description shows urine, CSF, fluid or faeces and another listed candidate is the same analyte in that "
        "specimen, choose that candidate. (4) property: reject only a clear conflict, such as a 24-hour or per-time "
        "rate for a concentration, a count for a concentration, a fraction or partial pressure for a concentration, "
        "a [Presence], [Interpretation], [Identifier] or [Type] concept for a source whose results are mostly "
        "numeric, or a ratio concept for a test that is not a ratio. SNOMED Procedure concepts such as '<analyte> "
        "measurement', 'estimation', 'level', 'count' or 'assay' are valid targets: judge them on analyte and any "
        "named specimen only, and never reject a concept just for lacking a property, scale or specimen. "
        "Decision: accept when no check clearly fails. Choose another numbered candidate only when the proposed one "
        "clearly fails a check and that candidate passes all of them. Reject only when the proposed one clearly "
        "fails and no candidate passes."),
    "pathology_result": (
        "You verify mappings of hospital laboratory RESULT VALUES (short coded or free-text answers) to OMOP value "
        "concepts. Decide whether the PROPOSED concept states what this result value means for this test: the same "
        "organism or finding, the same polarity (detected vs not detected, positive vs negative, growth vs no growth), "
        "and not a mere category word. Reject if the value is an instruction, a comment, a workflow note, or "
        "ambiguous. Accept when the meaning clearly matches; otherwise reject."),
    "device_gmdn": (
        "You verify mappings of implanted-device descriptions from surgical records to GMDN device terms. Decide "
        "whether the PROPOSED GMDN term is the same kind of device (device type, body site, material, and active vs "
        "passive). The procedure keywords give context. If the proposed term fails and another numbered candidate "
        "is the same kind of device, choose it; if none is, reject. Accept when no check clearly fails."),
}

_ANSWER_RULES = (
    "\n\nAnswer with JSON only: decision is accept (the proposed candidate is right; candidate = its number), "
    "choose (another listed candidate is right; candidate = that number) or reject (none is right; candidate = 0). "
    "reason: one sentence naming the deciding check.")

_lock = threading.Lock()
_state = {"calls": 0, "consecutive_unreachable": 0, "tripped": False, "client": None,
          "input_tokens": 0, "output_tokens": 0}


def values_allowed(map_name):
    """Whether this map may send patient-level text to the model (catalogue-only maps always may)."""
    return SEND_VALUES.get(map_name, True)


def held_verdict(reason):
    return dict(decision="held", rank=0, reason=reason, model=MODEL, prompt_version=PROMPT_VERSION,
                prompt_hash=None, verified_at=None)


def _spark():
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession()


def _secret():
    """The Azure AI resource key, from the same secret the pathology embed loop uses."""
    try:
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)  # noqa: F821 (notebook global)
    except NameError:
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient().dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)


def _client():
    """Memoised Anthropic client on the Foundry resource."""
    with _lock:
        if _state["client"] is None:
            from anthropic import AnthropicFoundry  # needs `%pip install anthropic` + a Python restart in the caller
            _state["client"] = AnthropicFoundry(api_key=_secret(), resource=FOUNDRY_RESOURCE,
                                                max_retries=4, timeout=300.0)
        return _state["client"]


def reset_run():
    """Start a new budget / outage window (call once per pipeline run)."""
    with _lock:
        _state.update(calls=0, consecutive_unreachable=0, tripped=False, input_tokens=0, output_tokens=0)


def run_stats():
    return {k: _state[k] for k in ("calls", "tripped", "input_tokens", "output_tokens")}


def _canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def _render(map_name, item):
    """System and user text for one item. Candidates are numbered from 1; item['proposed'] is 0-based."""
    lines = [f"Source term ({map_name}):"]
    for k, v in item["context"].items():
        if v not in (None, "", []):
            lines.append(f"- {k}: {CONTEXT_GLOSS.get(k, {}).get(v, v)}")
    lines.append("")
    lines.append(f"PROPOSED: candidate {item['proposed'] + 1}")
    lines.append("Candidates (from the embedding search, best first):")
    for i, c in enumerate(item["candidates"], 1):
        extra = ", ".join(f"{k}={c[k]}" for k in ("vocabulary", "concept_class", "domain") if c.get(k))
        sim = f", similarity={c['similarity']:.3f}" if c.get("similarity") is not None else ""
        lines.append(f"{i}. {c['name']} [{extra}{sim}]")
    return _RUBRICS[map_name] + _ANSWER_RULES, "\n".join(lines)


def cache_key(map_name, item):
    cands = [(c.get("id"), c["name"]) for c in item["candidates"]]
    return hashlib.sha256(_canon([map_name, item["key"], cands, item["proposed"], MODEL, PROMPT_VERSION])
                          .encode()).hexdigest()


def _ask(map_name, item):
    """One verification call. Returns a verdict dict, or None when the model is unreachable (-> held)."""
    try:
        import anthropic
        _client()
    except ImportError as e:  # no SDK in this environment: hold everything, commit nothing
        with _lock:
            _state["tripped"] = True
        print(f"MAP_LLM_GATE_V1: anthropic SDK unavailable ({e}); every proposal is held this run")
        return None
    system, user = _render(map_name, item)
    prompt_hash = hashlib.sha256((system + "\n" + user).encode()).hexdigest()
    base = {"model": MODEL, "prompt_version": PROMPT_VERSION, "prompt_hash": prompt_hash}
    try:
        resp = _client().messages.create(
            model=MODEL, max_tokens=MAX_TOKENS, system=system,
            messages=[{"role": "user", "content": user}],
            output_config={"effort": EFFORT, "format": {"type": "json_schema", "schema": _SCHEMA}})
    except (anthropic.APIConnectionError, anthropic.RateLimitError, anthropic.InternalServerError,
            anthropic.APITimeoutError):
        with _lock:
            _state["consecutive_unreachable"] += 1
            if _state["consecutive_unreachable"] >= OUTAGE_TRIP:
                _state["tripped"] = True
        return None
    except anthropic.APIStatusError as e:
        # A request the endpoint will never accept (4xx other than 429): reject, so it is not retried forever.
        return dict(base, decision="reject", rank=0, reason=f"verifier_error_{e.status_code}")
    with _lock:
        _state["consecutive_unreachable"] = 0
        _state["input_tokens"] += resp.usage.input_tokens
        _state["output_tokens"] += resp.usage.output_tokens
    if resp.stop_reason not in ("end_turn", "stop_sequence"):
        return dict(base, decision="reject", rank=0, reason=f"verifier_stop_{resp.stop_reason}")
    text = next((b.text for b in resp.content if b.type == "text"), "")
    try:
        out = json.loads(text)
        decision, cand, reason = out["decision"], int(out["candidate"]), str(out["reason"])[:500]
    except Exception:
        return dict(base, decision="reject", rank=0, reason="verifier_unparseable")
    n, proposed = len(item["candidates"]), item["proposed"] + 1
    if decision == "choose" and cand == proposed:
        decision = "accept"
    if decision == "accept":
        cand = proposed
    if decision in ("accept", "choose") and not 1 <= cand <= n:
        return dict(base, decision="reject", rank=0, reason="verifier_candidate_out_of_range")
    if decision == "reject":
        cand = 0
    return dict(base, decision=decision, rank=cand, reason=reason)


def _cached(map_name, keys):
    spark = _spark()
    if not keys or not spark.catalog.tableExists(VERDICT_TABLE):
        return {}
    kdf = spark.createDataFrame([(k,) for k in keys], "cache_key string")
    rows = spark.table(VERDICT_TABLE).where(f"map_name = '{map_name}'").join(kdf, "cache_key", "left_semi").collect()
    return {r.cache_key: dict(decision=r.decision, rank=r.chosen_rank, reason=r.reason, model=r.model,
                              prompt_version=r.prompt_version, prompt_hash=r.prompt_hash,
                              verified_at=r.verified_at, cached=True) for r in rows}


def ensure_tables():
    spark = _spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {VERDICT_TABLE.rsplit('.', 1)[0]}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {VERDICT_TABLE} (
        cache_key STRING, map_name STRING, key_json STRING, candidates_json STRING, proposed_rank INT,
        decision STRING, chosen_rank INT, chosen_id STRING, reason STRING, model STRING, prompt_version STRING,
        prompt_hash STRING, verified_at TIMESTAMP) USING DELTA
        COMMENT 'MAP_LLM_GATE_V1: Claude verdicts on embedding-proposed mappings (accept/choose/reject), one row per
        (map, key, candidate set, model, prompt version). Source text is catalogue metadata or screened result values.'""")


def _store(map_name, items, verdicts):
    rows = []
    for item in items:
        v = verdicts.get(item["id"])
        if not v or v.get("cached") or v["decision"] == "held":
            continue
        chosen = item["candidates"][v["rank"] - 1].get("id") if v["rank"] else None
        rows.append((item["cache_key"], map_name, _canon(item["key"]),
                     _canon([(c.get("id"), c["name"]) for c in item["candidates"]]), item["proposed"] + 1,
                     v["decision"], v["rank"], None if chosen is None else str(chosen), v["reason"], v["model"],
                     v["prompt_version"], v["prompt_hash"], v["verified_at"]))
    if not rows:
        return
    spark = _spark()
    ensure_tables()
    df = spark.createDataFrame(rows, spark.table(VERDICT_TABLE).schema)
    from delta.tables import DeltaTable
    (DeltaTable.forName(spark, VERDICT_TABLE).alias("t").merge(df.alias("s"), "t.cache_key = s.cache_key")
     .whenNotMatchedInsertAll().execute())


def verify(map_name, items):
    """Verify items; returns {item id: verdict}. A verdict has decision in accept/choose/reject/held, rank (1-based
    chosen candidate, 0 for reject/held), reason, model, prompt_version, prompt_hash, verified_at.
    item = {id, key (dict), context (dict of non-identifying fields), candidates [{id, name, vocabulary,
    concept_class, domain, similarity}], proposed (0-based index into candidates)}."""
    for item in items:
        item["cache_key"] = cache_key(map_name, item)
    hits = _cached(map_name, sorted({i["cache_key"] for i in items}))
    out, todo = {}, []
    for item in items:
        if item["cache_key"] in hits:
            out[item["id"]] = hits[item["cache_key"]]
        else:
            todo.append(item)
    held = held_verdict("verifier_unavailable_or_budget")

    def run(item):
        with _lock:
            if _state["tripped"] or _state["calls"] >= MAX_CALLS_PER_RUN:
                return item, None
            _state["calls"] += 1
        return item, _ask(map_name, item)

    with _cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for item, v in pool.map(run, todo):
            if v is None:
                out[item["id"]] = dict(held)
            else:
                v["verified_at"] = datetime.now(timezone.utc)
                out[item["id"]] = v
    _store(map_name, items, out)
    return out


# ---- result-text privacy screen ---------------------------------------------------------------------------------
# Result values are patient-level free text. Only short, code-like values are sent; anything that could carry an
# identifier (long digit runs, dates, names of staff/patients, contact details, narrative) is rejected unverified
# and stays unmapped with reason privacy_screen.
_PRIVACY_RE = re.compile(
    r"\d{6,}|\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}|@|\b(dr|mr|mrs|ms|miss|prof|nurse|sister)\b\.?\s|"
    r"\b(tel|phone|phoned|telephoned|bleep|ext|extension|nhs|mrn|hospital number|dob|informed|discussed|spoke|"
    r"called|contacted|ward)\b")
RESULT_MAX_LEN = 80


def result_text_sendable(text):
    return bool(text) and len(text) <= RESULT_MAX_LEN and not _PRIVACY_RE.search(text.lower())
