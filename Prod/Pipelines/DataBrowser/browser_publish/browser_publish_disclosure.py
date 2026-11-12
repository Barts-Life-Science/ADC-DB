# Databricks notebook source
# DataBrowserGold / disclosure — publication-time suppression for column
# profiles and QC counts.
#
# Pure Python with no spark at import time, so browser_publish_profile and
# browser_publish_qc_export can %run it and agent/tests can import it. Every
# figure that leaves the privileged publish step passes through here first.
# Serve-time rounding (standard-v1) is applied again by the app on top.
from __future__ import annotations

import math

THRESHOLD = 10          # no count under 10 is ever published
CENTRAL_MIN_N = 50      # mean, median and IQR need this many non-null values
TAILS_MIN_N = 200       # p5/p95: each 5% tail then holds >= 10 values
SIG_FIGS = 4
PROFILE_LEVELS = ("none", "structural", "summary", "values")
PERCENTILES = ("p5", "p25", "p50", "p75", "p95")


def round_count(n):
    """A publishable count: None under the threshold, otherwise rounded to
    the nearest 10 (nearest 1000 above 100,000)."""
    if n is None or n < THRESHOLD:
        return None
    if n > 100_000:
        return int(1000 * round(n / 1000))
    return int(10 * round(n / 10))


def _scaled(count, scale):
    """Round a sampled count after checking the threshold on the SAMPLE, so
    a large scale can never lift a handful of real rows over it."""
    if count is None or count < THRESHOLD:
        return None
    return round_count(int(round(count * scale)))


def _sig(x):
    """Round a statistic to SIG_FIGS significant figures."""
    if x is None or x == 0 or not math.isfinite(x):
        return x
    return round(x, SIG_FIGS - 1 - int(math.floor(math.log10(abs(x)))))


def column_kind(type_name):
    """numeric | date | complex | other, from a UC data type name."""
    t = (type_name or "").lower()
    if t.startswith(("variant", "struct", "array", "map", "binary")):
        return "complex"
    if t.startswith(("date", "timestamp")):
        return "date"
    if t.startswith(("tinyint", "smallint", "int", "bigint", "float",
                     "double", "decimal", "long", "short", "byte")):
        return "numeric"
    return "other"


def published_type(entry, source_type):
    """The type a published view column has: hash masks become strings and
    month/year masks become timestamps (mirrors build_schema_doc)."""
    if entry.get("action") == "mask":
        return "string" if entry.get("mask") == "hash" else "timestamp"
    return source_type


def publish_level(entry, type_name):
    """Profile level for one published column: the policy's optional
    profile_display, defaulting to summary. String columns never show top
    values (they can carry free text), whatever the policy says."""
    level = entry.get("profile_display") or "summary"
    assert level in PROFILE_LEVELS, f"bad profile_display {level!r}"
    if level == "values" and (type_name or "").lower().startswith("string"):
        return "summary"
    return level


def disclose_profile(raw, level, scale=1.0):
    """The publishable profile for one column.

    raw holds total_rows and non_null (sample counts when sampled) and
    distinct. Numerics add mean and p5..p95, dates add months
    {"YYYY-MM": count}, and 'values' columns add top_values [(value, count)]
    plus top_remainder. scale is 1/sample-fraction. The output uses the
    bundle profile vocabulary plus completeness_band and distinct_band, and
    for sampled tables sample_fraction and distinct_count_sample.
    """
    if level == "none":
        return {}
    total, n = raw["total_rows"], raw["non_null"]
    missing = total - n
    out = {}
    # Completeness is a percentage only when neither side is a small count.
    if n == 0:
        out["null_frac"] = 1.0
    elif n >= THRESHOLD and (missing == 0 or missing >= THRESHOLD):
        out["null_frac"] = round(missing / total, 2)
    else:
        out["completeness_band"] = ("under_10_populated" if n < THRESHOLD
                                    else "under_10_missing")
    if n < THRESHOLD:
        return out
    if scale != 1.0:
        # Every figure below is an estimate from this sample; say so.
        out["sample_fraction"] = round(1.0 / scale, 6)
    distinct = raw.get("distinct")
    if distinct is not None:
        # A sample's distinct count is a floor, not the table's cardinality,
        # so it is published under its own name.
        key = "distinct_count" if scale == 1.0 else "distinct_count_sample"
        if distinct >= THRESHOLD:
            out[key] = round_count(distinct)
        else:
            out["distinct_band"] = "under_10_distinct"
    if level == "structural":
        return out
    if n >= CENTRAL_MIN_N:
        for key in ("mean", "p25", "p50", "p75"):
            if raw.get(key) is not None:
                out[key] = _sig(raw[key])
    if n >= TAILS_MIN_N:
        for key in ("p5", "p95"):
            if raw.get(key) is not None:
                out[key] = _sig(raw[key])
    months = raw.get("months") or {}
    kept = sorted(m for m, c in months.items() if c >= THRESHOLD)
    if kept:
        out["min_month"], out["max_month"] = kept[0], kept[-1]
        out["month_histogram"] = [{"month": m,
                                   "count": _scaled(months[m], scale)}
                                  for m in kept]
    if level == "values" and raw.get("top_values"):
        shown = [(v, c) for v, c in raw["top_values"] if c >= THRESHOLD]
        folded = sum(c for _, c in raw["top_values"] if c < THRESHOLD)
        if shown:
            out["top_values"] = [{"value": v, "count": _scaled(c, scale)}
                                 for v, c in shown]
        if folded >= THRESHOLD:
            out["rare_suppressed_count"] = _scaled(folded, scale)
        remainder = raw.get("top_remainder") or 0
        if remainder >= THRESHOLD:
            out["top_n_remainder_count"] = _scaled(remainder, scale)
    return out


def _count_or_band(n):
    """(count, band): zero stays zero, small non-zero counts become a band."""
    if n is None:
        return None, None
    if n == 0:
        return 0, None
    rounded = round_count(n)
    return (rounded, None) if rounded is not None else (None, "under_10")


def disclose_qc(passed, failed):
    """A publishable expectation result for one rule on one update."""
    p, p_band = _count_or_band(passed)
    f, f_band = _count_or_band(failed)
    return {"passed": p, "passed_band": p_band,
            "failed": f, "failed_band": f_band}


def raw_profiles(columns, agg, month_rows, top_rows):
    """Assemble per-column raw measurements from the profile passes.

    columns: [(name, kind)]. agg: the one-row aggregate, with keys
    __rows, <c>__nn, <c>__distinct and, for numerics, <c>__mean and
    <c>__pct (five percentiles). month_rows: [(column, "YYYY-MM", count)].
    top_rows: [(column, value, count)], at most 25 per column, by count.
    """
    total = agg["__rows"]
    months, tops = {}, {}
    for column, month, count in month_rows:
        months.setdefault(column, {})[month] = count
    for column, value, count in top_rows:
        tops.setdefault(column, []).append((value, count))
    out = {}
    for name, kind in columns:
        raw = {"total_rows": total, "non_null": agg[f"{name}__nn"],
               "distinct": agg.get(f"{name}__distinct")}
        if kind == "numeric" and agg.get(f"{name}__pct"):
            raw["mean"] = agg.get(f"{name}__mean")
            raw.update(zip(PERCENTILES, agg[f"{name}__pct"]))
        if name in months:
            raw["months"] = months[name]
        if name in tops:
            ranked = sorted(tops[name], key=lambda vc: (-vc[1], str(vc[0])))
            raw["top_values"] = ranked
            raw["top_remainder"] = raw["non_null"] - sum(c for _, c in ranked)
        out[name] = raw
    return out

