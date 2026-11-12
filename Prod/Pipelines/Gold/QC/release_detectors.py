"""Driver-side detectors of the gold release report (INSR W4). Pure Python over profile rows; no Spark.

A finding is a dict in the release_finding shape. Two kinds of detector register here:
  * table detectors   f(ctx, fqn, month_rows, column_rows) -> findings, called once per table;
  * release detectors f(ctx, findings_so_far) -> findings, called once after every table (cross-table checks).
run_all loads one table's monthly rows at a time through ctx.load_month, so the driver never holds the whole
release (about 15M monthly rows at full volume). Nothing here blocks publication: severities are info, warn and
gate_candidate (a candidate for a future gate, for people to review, never enforced here).
"""
import json
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date

SEVERITIES = ("info", "warn", "gate_candidate")
# Known gaps from the INSR ledger (DOCUMENT verdicts): findings in these windows are annotated, not suppressed.
KNOWN_GAPS = [
    ("`4_prod`.gold.clinical_condition", "2026-03-01", "2026-04-01", "P6a"),
    ("`4_prod`.gold.clinical_procedure", "2026-03-01", "2026-04-01", "P6b"),
    ("`4_prod`.gold.clinical_medication_order", "2019-07-01", "2021-06-01", "P6c"),
    # P6d: May-Jun 2026 source order-volume outage (raw 0.716). P6g2 routes the reqstart part to W1.12 (FIX-W1;
    # W6.03 reqstart NO-GO); the volume part stays DOCUMENT here.
    ("`4_prod`.gold.clinical_medication_order", "2026-05-01", "2026-06-01", "P6d"),
    ("`4_prod`.pms_pharma.pms_exposure", "2026-05-01", "2026-06-01", "P6d"),
    # Table-wide window, but the gap is HbA1c only (HBA1/HBA12 pending in raw LIMS 2021-10..12): the label says so.
    ("`4_prod`.gold.clinical_pathology_result", "2021-10-01", "2021-12-01", "P7_L1374b (HBA1 only)"),
    ("`4_prod`.gold.clinical_family_history", "2024-09-01", "2099-12-01", "P7_L2686"),
]


@dataclass
class Context:
    """What detectors read: the registry, a per-table monthly-row loader, whole-table rows of this and the
    previous release, statement-check results and the latest dq_rule rows."""
    release_id: str
    boundary: date                                   # release boundary (date part), from the widget
    registry: dict                                   # fqn -> registry entry
    load_month: object = None                        # callable fqn -> list of release_profile_month row dicts
    column: dict = field(default_factory=dict)       # fqn -> column_profile row dicts (this release)
    previous_column: dict = field(default_factory=dict)  # fqn -> column_profile row dicts (previous release)
    checks: dict = field(default_factory=dict)       # check name -> list of result dicts (keys, fk, person, ...)
    dq_rules: list = field(default_factory=list)     # latest dq_rule rows (falsifier)


TABLE_DETECTORS, RELEASE_DETECTORS = [], []


def table_detector(fn):
    """Register f(ctx, fqn, month_rows, column_rows) -> findings."""
    TABLE_DETECTORS.append(fn)
    return fn


def release_detector(fn):
    """Register f(ctx, findings_so_far) -> findings (cross-table and statement-check detectors)."""
    RELEASE_DETECTORS.append(fn)
    return fn


def finding(ctx, fqn, family, code, severity, *, column="*", period=None, arm=None, observed=None,
            expected=None, **detail):
    """One finding dict, with a known-gap annotation when the period falls in a ledger DOCUMENT window."""
    assert severity in SEVERITIES, severity
    for table, start, end, probe in KNOWN_GAPS:
        if fqn == table and period is not None and start <= str(period) <= end:
            detail["known_gap"] = probe
    return {"release_id": ctx.release_id, "table_fqn": fqn, "column_name": column, "family": family,
            "finding_code": code, "severity": severity, "period": period, "arm": arm,
            "observed": None if observed is None else float(observed),
            "expected": None if expected is None else float(expected),
            "detail": json.dumps(detail, default=str, sort_keys=True)}


def series(rows, grain, metric="rows", column="*", arm=None):
    """{period: value} for one grain/metric/column (and arm), sorted by period; NULL periods dropped."""
    out = {}
    for r in rows:
        if (r["grain"] == grain and r["metric"] == metric and r["column_name"] == column
                and (arm is None or r["arm"] == arm) and r["period"] is not None):
            out[r["period"]] = out.get(r["period"], 0.0) + r["value"]
    return dict(sorted(out.items()))


def whole(rows, metric, column="*"):
    """A whole-table metric value from column_profile rows, or None."""
    return next((r["value"] for r in rows if r["metric"] == metric and r["column_name"] == column), None)


def month_index(d):
    """Months since year 0, for month arithmetic on period dates."""
    return d.year * 12 + d.month - 1


def trailing_median(values_by_month, period, n=12):
    """Median of the populated months in the n months before `period` (needs at least 6), else None."""
    i = month_index(period)
    prior = [v for p, v in values_by_month.items() if i - n <= month_index(p) < i and v > 0]
    return statistics.median(prior) if len(prior) >= 6 else None


# The report stage whose CHECKS each statement-check release detector reads. Detectors not listed read only
# persisted profile rows or DQ_RULES, so every detect run can regenerate them.
RELEASE_DETECTOR_STAGE = {"oneoff_temporal": "oneoff", "f10_units": "units", "f13_f7a": "keys", "f12_fk": "keys",
                          "f24_person": "person", "f16_residual": "residual", "sct_not_in_loaded_release": "oneoff",
                          "bbr_advisories": "oneoff"}


def skipped_detectors(stages):
    """Names of the release detectors whose CHECKS stage is not in this run's stages (their findings are kept)."""
    return {name for name, stage in RELEASE_DETECTOR_STAGE.items() if stage not in stages}


def stamp_detector(found, name):
    """Add the writing detector's name to each finding's JSON detail (detect deletes by it)."""
    for f in found:
        detail = json.loads(f["detail"])
        detail["detector"] = name
        f["detail"] = json.dumps(detail, default=str, sort_keys=True)
    return found


def run_all(ctx, stages=None):
    """Every table detector on every registry table (one monthly load per table), then the release detectors.

    With `stages`, a release detector whose CHECKS stage did not run is skipped (its inputs are empty this run).
    """
    skipped = skipped_detectors(stages) if stages is not None else set()
    findings = []
    for fqn in sorted(ctx.registry):
        month_rows = ctx.load_month(fqn) if ctx.load_month else []
        column_rows = ctx.column.get(fqn, [])
        for fn in TABLE_DETECTORS:
            findings += stamp_detector(fn(ctx, fqn, month_rows, column_rows), fn.__name__)
    for fn in RELEASE_DETECTORS:
        if fn.__name__ not in skipped:
            findings += stamp_detector(fn(ctx, list(findings)), fn.__name__)
    return findings


# ---------------------------------------------------------------------------------------------------------------
# W4.02 temporal detectors: F1 volume step (a)-(h), F2 freshness, F20 cadence, F23 null step, one-offs
# ---------------------------------------------------------------------------------------------------------------

COVID_MONTHS = {date(2020, 4, 1)}
# (child, parent) month-count ratios (F1c; L2612 HRG children per spell, L1892 scores per admission, L3527, L2905).
CHILD_PARENT = [
    ("`4_prod`.gold.clinical_hrg_grouping_diagnosis", "`4_prod`.gold.clinical_hrg_grouping"),
    ("`4_prod`.gold.clinical_hrg_grouping_procedure", "`4_prod`.gold.clinical_hrg_grouping"),
    ("`4_prod`.gold.clinical_critical_care_daily_score", "`4_prod`.gold.clinical_critical_care_admission"),
    ("`4_prod`.gold.clinical_tracking_milestone", "`4_prod`.gold.clinical_tracking_attendance"),
    ("`4_prod`.gold.clinical_neonatal_care_day", "`4_prod`.gold.clinical_neonatal_episode"),
]


def month_start(i):
    """The month-start date of a month index."""
    return date(i // 12, i % 12 + 1, 1)


def volume_steps(ctx, fqn, values, *, spike_x=3.0, prefix="F1", arm=None, min_median=0.0):
    """F1 over one monthly series: drop (<0.75x trailing-12 median and same month last year; warn <0.5x,
    gate_candidate <0.25x), spike (>spike_x), level shift (3+ months beyond 1.5x / 0.67x) and empty interior months.
    The last two months before the release boundary are the lag tail (F2) and are not judged. 2020-04 is
    annotated COVID and kept at info."""
    out = []
    cutoff = month_index(ctx.boundary) - 2
    periods = [p for p in values if month_index(p) < cutoff]
    if len(periods) < 7:
        return out
    run_dir, run_start, run_len = 0, None, 0
    for p in periods:
        base = trailing_median(values, p)
        if base is None or base < min_median:
            run_dir, run_len = 0, 0
            continue
        ratio = values[p] / base
        last_year = values.get(date(p.year - 1, p.month, 1))
        yoy = values[p] / last_year if last_year else None
        covid = p in COVID_MONTHS
        if ratio < 0.75 and (yoy is None or yoy < 0.75):
            severity = "gate_candidate" if ratio < 0.25 else "warn" if ratio < 0.5 else "info"
            out.append(finding(ctx, fqn, "F1", f"{prefix}_drop", "info" if covid else severity, period=p, arm=arm,
                               observed=ratio, expected=0.75, yoy=yoy, covid=covid))
        elif ratio > spike_x:
            out.append(finding(ctx, fqn, "F1", f"{prefix}_spike", "info" if covid else "warn", period=p, arm=arm,
                               observed=ratio, expected=spike_x, covid=covid))
        direction = 1 if ratio > 1.5 else -1 if ratio < 0.67 else 0
        if direction and direction == run_dir:
            run_len += 1
        else:
            run_dir, run_start, run_len = direction, p, (1 if direction else 0)
        if run_len == 3:
            out.append(finding(ctx, fqn, "F1", f"{prefix}_level_shift", "warn", period=run_start, arm=arm,
                               observed=ratio, expected=1.5 if direction > 0 else 0.67))
    if statistics.median(values[p] for p in periods) >= 20:
        have = {month_index(p) for p in periods if values[p] > 0}
        for i in range(month_index(periods[0]), month_index(periods[-1]) + 1):
            if i not in have:
                out.append(finding(ctx, fqn, "F1", f"{prefix}_empty_month", "warn", period=month_start(i), arm=arm,
                                   observed=0))
    return out


def coverage_start(values):
    """F1(d): the first month at >= 50% of the median of itself and the next 11 months (at least 6 of them)."""
    ps = list(values)
    for i, p in enumerate(ps):
        ahead = [values[q] for q in ps[i:i + 12]]
        if len(ahead) >= 6 and values[p] >= 0.5 * statistics.median(ahead):
            return p
    return None


@table_detector
def f1_volume(ctx, fqn, month, column):
    """F1 on the table series and every arm, plus (a) load-side gaps, (d) coverage_start, (e) source-mix switches,
    (f) fill-rate steps and (g) the row-count drop since the previous release."""
    e = ctx.registry[fqn]
    out = []
    if e["primary_date"] and not e["snapshot"]:
        spike = 5.0 if e["primary_class"] == "effective" else 3.0
        values = series(month, "month")
        out += volume_steps(ctx, fqn, values, spike_x=spike)
        for a in sorted({r["arm"] for r in month if r["grain"] == "month_arm"}):
            out += volume_steps(ctx, fqn, series(month, "month_arm", arm=a), spike_x=spike, prefix="F1b", arm=a,
                                min_median=100)
        start = coverage_start(values)
        if start:
            out.append(finding(ctx, fqn, "F1", "F1d_coverage_start", "info", period=start))
        out += source_mix(ctx, fqn, month)
        out += fill_steps(ctx, fqn, month, values)
    load = series(month, "loaded_month")
    for p, n in load.items():
        base = trailing_median(load, p)
        if base and n < 0.25 * base and month_index(p) < month_index(ctx.boundary) - 1:
            out.append(finding(ctx, fqn, "F1", "F1a_load_gap", "warn", period=p, observed=n / base, expected=0.25))
    now, before = whole(column, "rows"), whole(ctx.previous_column.get(fqn, []), "rows")
    if now is not None and before:
        if now < 0.85 * before:
            out.append(finding(ctx, fqn, "F1", "F1g_release_drop", "warn", observed=now / before, expected=0.85))
    return out


def source_mix(ctx, fqn, month):
    """F1(e): the dominant arm switches and the new one holds for 2+ months (info)."""
    totals = defaultdict(dict)
    for r in month:
        if r["grain"] == "month_arm" and r["metric"] == "rows" and r["column_name"] == "*" and r["period"]:
            totals[r["period"]][r["arm"]] = r["value"]
    dominant = [(p, max(arms, key=arms.get)) for p, arms in sorted(totals.items())]
    out = []
    for i in range(1, len(dominant) - 1):
        (p0, a0), (p1, a1), (_, a2) = dominant[i - 1], dominant[i], dominant[i + 1]
        if a1 != a0 and a2 == a1:
            out.append(finding(ctx, fqn, "F1", "F1e_source_mix_switch", "info", period=p1, arm=a1, previous=a0))
    return out


def fill_steps(ctx, fqn, month, values):
    """F1(f): a column's non-null rate drops >30pp below its trailing-12 median rate while volume stays within
    0.75-1.33x (first month of each drop run only)."""
    out = []
    for c in ctx.registry[fqn]["columns"]:
        nn = series(month, "month", "nonnull", c["name"])
        rate = {p: nn.get(p, 0.0) / v for p, v in values.items() if v}
        flagged_prev = False
        for p in rate:
            base_rate, base_volume = trailing_median(rate, p), trailing_median(values, p)
            if base_rate is None or not base_volume:
                flagged_prev = False
                continue
            hit = (base_rate - rate[p] > 0.30) and 0.75 <= values[p] / base_volume <= 1.33
            if hit and not flagged_prev:
                out.append(finding(ctx, fqn, "F1", "F1f_fill_step", "warn", column=c["name"], period=p,
                                   observed=rate[p], expected=base_rate))
            flagged_prev = hit
    return out


@release_detector
def f1_cross_table(ctx, findings):
    """F1(c) child/parent ratio steps and F1(h) three or more spine tables spiking in the same month."""
    out = []
    for child, parent in CHILD_PARENT:
        if child not in ctx.registry or parent not in ctx.registry:
            continue
        c, p = series(ctx.load_month(child), "month"), series(ctx.load_month(parent), "month")
        ratio = {m: c.get(m, 0.0) / v for m, v in p.items() if v}
        out += [dict(f, family="F1", finding_code=f["finding_code"].replace("F1", "F1c", 1),
                     detail=json.dumps({"parent": parent}))
                for f in volume_steps(ctx, child, ratio, spike_x=3.0)]
    spikes = defaultdict(set)
    for f in findings:
        if f["finding_code"] == "F1_spike" and ".spine_" in f["table_fqn"]:
            spikes[f["period"]].add(f["table_fqn"])
    for period, tables in sorted(spikes.items()):
        if len(tables) >= 3:
            out.append(finding(ctx, "*", "F1", "F1h_co_occurring_spike", "info", period=period,
                               observed=len(tables), tables=sorted(tables)))
    return out


@table_detector
def f2_freshness(ctx, fqn, month, column):
    """F2: complete_through (latest month at >=0.75x trailing median; exported for W8.05/W7.11), stale feed
    (lag > cadence, not frozen), stalled feed (loads continue while events do not), frozen (info) and per-column
    validity window (column last populated >12 months before the table's last month)."""
    e = ctx.registry[fqn]
    values = series(month, "month")
    if not values or e["snapshot"]:
        return []
    bm = month_index(ctx.boundary)
    out = []
    complete = next((p for p in reversed(list(values)) if month_index(p) <= bm
                     and (trailing_median(values, p) or 0) and values[p] >= 0.75 * trailing_median(values, p)), None)
    if complete:
        out.append(finding(ctx, fqn, "F2", "F2_complete_through", "info", period=complete,
                           observed=bm - month_index(complete)))
    last = max((p for p in values if month_index(p) <= bm), default=None)
    if last is None:
        return out
    lag = bm - month_index(last)
    if e["frozen"]:
        out.append(finding(ctx, fqn, "F2", "F2_frozen", "info", period=last, observed=lag))
    elif lag > e["cadence_months"]:
        loads = series(month, "loaded_month")
        stalled = any(month_index(p) >= bm - 1 for p in loads)
        out.append(finding(ctx, fqn, "F2", "F2_stalled" if stalled else "F2_stale", "warn", period=last,
                           observed=lag, expected=e["cadence_months"]))
    for c in e["columns"]:
        populated = [p for p, v in series(month, "month", "nonnull", c["name"]).items() if v > 0]
        if populated and month_index(last) - month_index(max(populated)) > 12:
            out.append(finding(ctx, fqn, "F2", "F2_column_window", "info", column=c["name"], period=max(populated),
                               observed=month_index(last) - month_index(max(populated))))
    return out


@table_detector
def f20_cadence(ctx, fqn, month, column):
    """F20 on snapshot tables (period = snapshot day): gaps over 8 days, census row-count steps over 20%, daily
    tables with fewer than 30 distinct days in the last 30, and status/start NULL share over 50% in the last census."""
    e = ctx.registry.get(fqn)
    if not e or not e["snapshot"]:
        return []
    days = series(month, "month")
    ps = list(days)
    out = []
    for a, b in zip(ps, ps[1:]):
        if (b - a).days > 8:
            out.append(finding(ctx, fqn, "F20", "F20_gap", "warn", period=b, observed=(b - a).days, expected=8))
        if days[a] and abs(days[b] / days[a] - 1) > 0.20:
            out.append(finding(ctx, fqn, "F20", "F20_census_step", "warn", period=b, observed=days[b] / days[a],
                               expected=1.0))
    if fqn.endswith("clinical_bed_status_observation"):
        recent = [p for p in ps if 0 <= (ctx.boundary - p).days <= 30]
        if len(recent) < 30:
            out.append(finding(ctx, fqn, "F20", "F20_daily_gap", "warn", observed=len(recent), expected=30))
    if ps:
        last = ps[-1]
        for c in e["columns"]:
            if c["name"] in ("status_code", "status_display", "waiting_start_datetime"):
                nn = series(month, "month", "nonnull", c["name"]).get(last, 0.0)
                if days[last] and nn / days[last] < 0.5:
                    out.append(finding(ctx, fqn, "F20", "F20_null_share", "warn", column=c["name"], period=last,
                                       observed=1 - nn / days[last], expected=0.5))
    return out


@table_detector
def f23_null_step(ctx, fqn, month, column):
    """F23: per (column, arm) yearly non-null rate moves more than 30pp year on year (complete years, >=100 rows
    in both years, NULL-by-design arms skipped: rate 0 in both years)."""
    rows_y, nn_y = defaultdict(float), defaultdict(float)
    for r in month:
        if r["grain"] != "month_arm" or r["period"] is None or r["period"].year >= ctx.boundary.year:
            continue
        if r["metric"] == "rows" and r["column_name"] == "*":
            rows_y[(r["arm"], r["period"].year)] += r["value"]
        elif r["metric"] == "nonnull":
            nn_y[(r["column_name"], r["arm"], r["period"].year)] += r["value"]
    out = []
    for (col, arm, year), nn in sorted(nn_y.items()):
        prev_rows, rows_now = rows_y.get((arm, year - 1), 0), rows_y.get((arm, year), 0)
        if prev_rows < 100 or rows_now < 100:
            continue
        rate_now, rate_prev = nn / rows_now, nn_y.get((col, arm, year - 1), 0.0) / prev_rows
        if abs(rate_now - rate_prev) > 0.30:
            out.append(finding(ctx, fqn, "F23", "F23_null_step", "warn", column=col, arm=arm,
                               period=date(year, 1, 1), observed=rate_now, expected=rate_prev))
    return out


@table_detector
def link_share_step(ctx, fqn, month, column):
    """L3301 (generalised): a link-rate pair's monthly share moves >10pp from its trailing median."""
    values = series(month, "month")
    out = []
    for name in sorted({r["metric"] for r in month if r["metric"].startswith("pair:link:")}):
        share = {p: v / values[p] for p, v in series(month, "month", name).items() if values.get(p)}
        for p in share:
            base = trailing_median({k: v + 1e-9 for k, v in share.items()}, p)
            if base is not None and abs(share[p] - base) > 0.10:
                out.append(finding(ctx, fqn, "F12", "L3301_link_share_step", "warn", column=name[5:], period=p,
                                   observed=share[p], expected=base))
    return out


def flat_runs(ctx, fqn, daily, min_days=28):
    """L808: an arm's daily count identical (and >0) for min_days or more consecutive days."""
    out = []
    by_arm = defaultdict(list)
    for r in daily:
        by_arm[r["arm"]].append((r["day"], r["n"]))
    for arm, points in sorted(by_arm.items(), key=lambda kv: str(kv[0])):
        points.sort()
        run = 1
        for (d0, n0), (d1, n1) in zip(points, points[1:]):
            run = run + 1 if n1 == n0 and n1 > 0 and (d1 - d0).days == 1 else 1
            if run == min_days:
                out.append(finding(ctx, fqn, "F1", "L808_flat_series", "warn", arm=arm, period=d1, observed=n1,
                                   expected=min_days))
    return out


@release_detector
def oneoff_temporal(ctx, findings):
    """L4552 turnaround shift, L808 flat daily series, L2905 exam gate loss, L4006 inferred-close drop."""
    out = []
    turnaround = {r["period"]: r["median_min"] for r in ctx.checks.get("L4552", []) if r["median_min"] is not None}
    for p, v in turnaround.items():
        base = trailing_median(turnaround, p)
        if base and v > 3 * base:
            out.append(finding(ctx, "`4_prod`.gold.clinical_pathology_report", "F1", "L4552_turnaround_shift", "warn",
                               period=p, observed=v, expected=3 * base))
    out += flat_runs(ctx, "`4_prod`.gold.clinical_medication_order", ctx.checks.get("L808", []))
    for r in ctx.checks.get("L2905", []):
        out.append(finding(ctx, "`4_prod`.gold.clinical_neonatal_examination", "F1", "L2905_gate_loss", "info",
                           observed=r["silver_rows"] - r["gold_rows"], expected=0))
    enc = "`4_prod`.gold.spine_encounter"
    now = whole(ctx.column.get(enc, []), "nonnull", "inferred_close_datetime")
    before = whole(ctx.previous_column.get(enc, []), "nonnull", "inferred_close_datetime")
    if now is not None and before is not None and now < before:
        out.append(finding(ctx, enc, "F1", "L4006_inferred_close_drop", "warn", column="inferred_close_datetime",
                           observed=now, expected=before))
    return out


# ---------------------------------------------------------------------------------------------------------------
# W4.03 shape/value detectors: F3 discovery, F8 zero share, F9 heavy tail, F10 units, F11 + falsifier, F15, F18, F19
# ---------------------------------------------------------------------------------------------------------------

CUTOVER_MONTHS = {date(2002, 3, 1), date(2002, 4, 1), date(2007, 9, 1), date(2007, 10, 1), date(2007, 11, 1),
                  date(2008, 2, 1), date(2008, 3, 1), date(2008, 4, 1), date(2013, 3, 1), date(2014, 5, 1),
                  date(2017, 9, 1), date(2021, 3, 1)}   # shared estate cutover list (F3d, L2007)
BULK_COLUMNS = ("record_status_effective_from", "source_update_timestamp")


def column_values(rows):
    """{(column, metric): value} from whole-table column_profile rows."""
    return {(r["column_name"], r["metric"]): r["value"] for r in rows}


@table_detector
def f3_day_heap(ctx, fqn, month, column):
    """F3(a) single-day heaping and F3(c) calendar heaping (1 Jan / day 01 / 1 Jul on birth columns)."""
    classes = {c["name"]: c["date_class"] for c in ctx.registry[fqn]["columns"]}
    ratio = {(r["column_name"], r["period"]): r["value"] for r in month
             if r["grain"] == "day_heap" and r["metric"] == "ratio_to_median"}
    out = []
    for r in month:
        if r["grain"] != "day_heap" or r["metric"] != "rows":
            continue
        day, col = r["period"], r["column_name"]
        x = ratio.get((col, day), 0.0)
        calendar = classes.get(col) == "birth" and day.day == 1      # day 01 of any month, 1 Jan and 1 Jul included
        code = "F3c_calendar_heap" if calendar else "F3a_day_heap"
        out.append(finding(ctx, fqn, "F3", code, "warn" if x >= 10 else "info", column=col, period=day,
                           observed=r["value"], expected=x))
    return out


@table_detector
def f3_clusters(ctx, fqn, month, column):
    """F3(b) isolated clusters: per date column, month segments split by gaps over 6 months; every segment except
    the largest is a finding at its first month. F3(d): cutover-month spikes (>3x trailing median)."""
    out = []
    by_col = defaultdict(dict)
    for r in month:
        if r["grain"] == "col_month" and r["metric"] == "rows":
            by_col[r["column_name"]][r["period"]] = r["value"]
    for col, values in sorted(by_col.items()):
        ps = sorted(values)
        segments, current = [], [ps[0]] if ps else []
        for a, b in zip(ps, ps[1:]):
            if month_index(b) - month_index(a) > 6:
                segments.append(current)
                current = []
            current.append(b)
        if current:
            segments.append(current)
        if len(segments) > 1:
            body = max(segments, key=lambda s: sum(values[p] for p in s))
            for s in segments:
                if s is not body:
                    out.append(finding(ctx, fqn, "F3", "F3b_isolated_cluster", "info", column=col, period=s[0],
                                       observed=sum(values[p] for p in s), months=len(s)))
        for p in ps:
            base = trailing_median(values, p)
            if p in CUTOVER_MONTHS and base and values[p] > 3 * base:
                out.append(finding(ctx, fqn, "F3", "F3d_cutover", "info", column=col, period=p,
                                   observed=values[p] / base, expected=3))
    return out


@table_detector
def f8_f9_values(ctx, fqn, month, column):
    """F8: zero share >= 50% of non-null on a numeric column (warn; >= 99% gate_candidate). F9: heavy tail,
    mean > 3 x p95 on >= 100 values."""
    v = column_values(column)
    out = []
    for c in ctx.registry[fqn]["columns"]:
        if c["kind"] != "numeric":
            continue
        nn, zero = v.get((c["name"], "nonnull"), 0.0), v.get((c["name"], "zero"), 0.0)
        if nn >= 100 and zero / nn >= 0.5:
            out.append(finding(ctx, fqn, "F8", "F8_zero_share", "gate_candidate" if zero / nn >= 0.99 else "warn",
                               column=c["name"], observed=zero / nn, expected=0.5))
        mean, p95 = v.get((c["name"], "mean")), v.get((c["name"], "p95"))
        if nn >= 100 and mean is not None and p95 and p95 > 0 and mean > 3 * p95:
            out.append(finding(ctx, fqn, "F9", "F9_heavy_tail", "warn", column=c["name"], observed=mean,
                               expected=3 * p95, p99=v.get((c["name"], "p99")), p999=v.get((c["name"], "p999"))))
    return out


@table_detector
def f11_shape(ctx, fqn, month, column):
    """F11: empty, constant, near-empty (<1% or <10 values) and populated->empty since the previous release
    (gate_candidate). Findings describe the column; none advises dropping it (J19)."""
    v = column_values(column)
    prev = column_values(ctx.previous_column.get(fqn, []))
    rows = v.get(("*", "rows"), 0.0)
    out = []
    for c in ctx.registry[fqn]["columns"]:
        name = c["name"]
        if (name, "nonnull") not in v or rows == 0:
            continue
        nn = v[(name, "nonnull")]
        if nn == 0:
            out.append(finding(ctx, fqn, "F11", "F11_empty", "info", column=name, observed=0))
            if prev.get((name, "nonnull"), 0) > 0:
                out.append(finding(ctx, fqn, "F11", "F11_populated_to_empty", "gate_candidate", column=name,
                                   observed=0, expected=prev[(name, "nonnull")]))
        elif nn < 10 or nn / rows < 0.01:
            out.append(finding(ctx, fqn, "F11", "F11_near_empty", "info", column=name, observed=nn / rows,
                               expected=0.01))
        elif v.get((name, "ndistinct")) == 1 and nn >= 10:
            out.append(finding(ctx, fqn, "F11", "F11_constant", "info", column=name, observed=1))
    return out


@release_detector
def f11_falsifier(ctx, findings):
    """F11 falsifier: a pinned dead_column rule whose column now holds values forces a recompile (gate_candidate).
    Includes spine care_participation.valid_to (81.5M values against a dead_column pass rule; L3892)."""
    out = []
    for rule in ctx.dq_rules:
        if rule["family"] != "dead_column" or rule["action"] != "pass" or not rule["column"]:
            continue
        plane, short = rule["table"].replace("journey_", "", 1).split(".", 1)
        fqn = f"`4_prod`.gold.{plane}_{short}"
        nn = whole(ctx.column.get(fqn, []), "nonnull", rule["column"])
        if nn:
            out.append(finding(ctx, fqn, "F11", "F11_falsified_dead_column", "gate_candidate", column=rule["column"],
                               observed=nn, expected=0, rule_id=rule["rule_id"]))
    return out


@table_detector
def f15_decode(ctx, fqn, month, column):
    """F15: code populated (not '' or '0') with display NULL on more than 5% of populated codes, and the inverse."""
    v = column_values(column)
    out = []
    for (name, metric), value in v.items():
        for prefix, code in (("pair:decode_gap:", "F15_decode_gap"), ("pair:decode_inverse:", "F15_decode_inverse")):
            if name == "*" and metric.startswith(prefix):
                col = metric[len(prefix):]
                base = v.get((col, "nonnull"), 0.0) if code == "F15_decode_gap" else v.get(("*", "rows"), 0.0)
                if base and value / base > 0.05:
                    out.append(finding(ctx, fqn, "F15", code, "warn", column=col, observed=value / base, expected=0.05))
    return out


@table_detector
def f18_types(ctx, fqn, month, column):
    """F18: STRING columns that are >90% numeric, *_id STRING values that fail BIGINT try_cast (gate_candidate),
    and code columns whose distinct count exceeds 10% of their values (free text in a code column)."""
    v = column_values(column)
    out = []
    for c in ctx.registry[fqn]["columns"]:
        name, nn = c["name"], v.get((c["name"], "nonnull"), 0.0)
        if c["kind"] != "string" or nn < 100:
            continue
        if v.get((name, "numeric_share"), 0.0) / nn > 0.9 and not name.endswith(("_id", "_code", "_cd")):
            out.append(finding(ctx, fqn, "F18", "F18_numeric_string", "info", column=name,
                               observed=v[(name, "numeric_share")] / nn, expected=0.9))
        if v.get((name, "id_trycast_fail"), 0.0) > 0:
            out.append(finding(ctx, fqn, "F18", "F18_id_not_bigint", "gate_candidate", column=name,
                               observed=v[(name, "id_trycast_fail")], expected=0))
        if name.endswith(("_code", "_cd")) and nn >= 1000 and v.get((name, "ndistinct"), 0.0) / nn > 0.10:
            out.append(finding(ctx, fqn, "F18", "F18_free_text_code", "info", column=name,
                               observed=v[(name, "ndistinct")] / nn, expected=0.10))
    return out


@table_detector
def f19_aggregates(ctx, fqn, month, column):
    """F19: end equals the load day on >20% of a month (L2786), bulk rewrite (one month >20% of an effective/update
    column, L4447), build stamp (one distinct month on a multi-year table), identical columns (L2659) and
    timestamp-without-local-time (L3636)."""
    out = []
    values = series(month, "month")
    for name in sorted({r["metric"] for r in month if r["metric"].startswith("pair:clock_equal:")}):
        for p, n in series(month, "month", name).items():
            if values.get(p, 0) >= 100 and n / values[p] > 0.20:
                out.append(finding(ctx, fqn, "F19", "F19_clock_equal", "warn", column=name.split(":")[-1], period=p,
                                   observed=n / values[p], expected=0.20))
    for col in BULK_COLUMNS:
        s = {r["period"]: r["value"] for r in month if r["grain"] == "col_month" and r["metric"] == "rows"
             and r["column_name"] == col}
        total = sum(s.values())
        if total >= 1000:
            peak = max(s, key=s.get)
            if s[peak] / total > 0.20:
                out.append(finding(ctx, fqn, "F19", "F19_bulk_rewrite", "warn", column=col, period=peak,
                                   observed=s[peak] / total, expected=0.20))
            if len(s) == 1 and len(values) > 24:
                out.append(finding(ctx, fqn, "F19", "F19_build_stamp", "info", column=col, period=peak))
    v = column_values(column)
    for (name, metric), value in v.items():
        if name == "*" and metric.startswith("pair:identical:"):
            a = metric.split(":", 2)[2].split("|")[0]
            if v.get((a, "nonnull")) and value == v[(a, "nonnull")]:
                out.append(finding(ctx, fqn, "F19", "F19_identical_columns", "info", column=metric[5:], observed=1.0))
        if name == "*" and metric.startswith("pair:local_null:") and value > 0:
            out.append(finding(ctx, fqn, "F19", "F19_local_null", "warn", column=metric.split(":", 2)[2],
                               observed=value, expected=0))
    return out


@release_detector
def f10_units(ctx, findings):
    """F10 per unit group (ctx.checks['F10:<fqn>']): more than one unit, p95/p25 > 1000, bimodal <=5 and >=100
    shares both >5%, unit NULL >5%. At most 200 findings per table, largest groups first."""
    out = []
    for key, rows in sorted(ctx.checks.items()):
        if not key.startswith("F10:"):
            continue
        fqn = key[4:]
        for r in sorted(rows, key=lambda r: -r["n"])[:200]:
            p25, p95 = (r["pct"] or [None, None])
            if r["n_units"] and r["n_units"] > 1:
                out.append(finding(ctx, fqn, "F10", "F10_multi_unit", "info", arm=r["grp"], observed=r["n_units"]))
            if p25 and p25 > 0 and p95 / p25 > 1000:
                out.append(finding(ctx, fqn, "F10", "F10_scale_spread", "warn", arm=r["grp"], observed=p95 / p25,
                                   expected=1000))
            if r["le5"] / r["n"] > 0.05 and r["ge100"] / r["n"] > 0.05:
                out.append(finding(ctx, fqn, "F10", "F10_bimodal", "warn", arm=r["grp"],
                                   observed=r["ge100"] / r["n"], expected=0.05))
            if r["unit_null"] is not None and r["unit_null"] / r["n"] > 0.05:
                out.append(finding(ctx, fqn, "F10", "F10_unit_null", "warn", arm=r["grp"],
                                   observed=r["unit_null"] / r["n"], expected=0.05))
    return out


# ---------------------------------------------------------------------------------------------------------------
# W4.04 detectors: F13 + F7(a), F12, F14, F24 + J8, F19 CC death, BlobIdentity, MSDS guard, L5172
# ---------------------------------------------------------------------------------------------------------------

def previous_check(ctx, fqn, check, metric):
    """A statement check's value in the previous release (stored in release_column_profile, class 'check')."""
    return whole(ctx.previous_column.get(fqn, []), metric, check)


@release_detector
def f13_f7a(ctx, findings):
    """F13: rows - NULL-key rows - distinct keys > 0 is a gate_candidate (any increase vs the previous release is
    noted). F7(a): a hub whose own key is 0 (gate_candidate)."""
    out = []
    for r in ctx.checks.get("F13", []):
        if r["n_distinct"] is not None:
            dup = r["n"] - r["n_null_key"] - r["n_distinct"]
            if dup > 0:
                before = previous_check(ctx, r["fqn"], "F13", "duplicates")
                out.append(finding(ctx, r["fqn"], "F13", "F13_duplicate_keys", "gate_candidate", observed=dup,
                                   expected=0, previous=before))
        if r.get("hub_zero"):
            out.append(finding(ctx, r["fqn"], "F7", "F7a_hub_id_zero", "gate_candidate", observed=r["hub_zero"],
                               expected=0))
    return out


@release_detector
def f12_fk(ctx, findings):
    """F12 per FK pair: zero ids (warn), orphans incl. zero > 2% of non-null (gate_candidate) or rising since the
    previous release (warn), and non-null coverage down more than 10pp since the previous release (warn)."""
    out = []
    for r in ctx.checks.get("F12", []):
        child, fk = r["child"], r["fk"]
        if r["n_zero"]:
            out.append(finding(ctx, child, "F12", "F12_fk_zero", "warn", column=fk, observed=r["n_zero"], expected=0))
        orphans = r["n_zero"] + r["n_orphan"]
        if r["n_nonnull"] and orphans / r["n_nonnull"] > 0.02:
            out.append(finding(ctx, child, "F12", "F12_orphans", "gate_candidate", column=fk,
                               observed=orphans / r["n_nonnull"], expected=0.02, orphans=orphans))
        before = previous_check(ctx, child, f"F12:{fk}", "orphans")
        if before is not None and orphans > before:
            out.append(finding(ctx, child, "F12", "F12_orphans_rising", "warn", column=fk, observed=orphans,
                               expected=before))
        cover_before = previous_check(ctx, child, f"F12:{fk}", "coverage")
        if cover_before is not None and r["n"] and cover_before - r["n_nonnull"] / r["n"] > 0.10:
            out.append(finding(ctx, child, "F12", "F12_coverage_drop", "warn", column=fk,
                               observed=r["n_nonnull"] / r["n"], expected=cover_before))
    for r in ctx.checks.get("F12_gene_coverage", []):
        if r["parents"] >= 20:
            out.append(finding(ctx, "`4_prod`.gold.clinical_genomic_test", "F12", "F12_parent_coverage", "info",
                               arm=r["family"], observed=r["with_child"] / r["parents"]))
    return out


@table_detector
def f14_mapping(ctx, fqn, month, column):
    """F14 per mapping axis from the profile pairs: (a) unmapped share up >5pp since the previous release,
    (b) SNOMED present with no OMOP concept >5%, (c) code without map method/version (warn_row family),
    (d) cosine without target, (e) display distinct count >1.2x code distinct count, (f) granularity ratio."""
    v = column_values(column)
    prev = column_values(ctx.previous_column.get(fqn, []))
    out = []
    axes = sorted({m.split(":", 2)[2] for (c, m) in v if c == "*" and m.startswith("pair:map_unmapped:")})
    for axis in axes:
        base = v.get((f"{axis}_source_code", "nonnull"), 0.0)
        if not base:
            continue
        unmapped = v.get(("*", f"pair:map_unmapped:{axis}"), 0.0) / base
        prev_base = prev.get((f"{axis}_source_code", "nonnull"), 0.0)
        if prev_base:
            before = prev.get(("*", f"pair:map_unmapped:{axis}"), 0.0) / prev_base
            if unmapped - before > 0.05:
                out.append(finding(ctx, fqn, "F14", "F14a_unmapped_rise", "warn", column=axis, observed=unmapped,
                                   expected=before))
        snomed_gap = v.get(("*", f"pair:map_snomed_no_omop:{axis}"), 0.0)
        if snomed_gap / base > 0.05:
            out.append(finding(ctx, fqn, "F14", "F14b_snomed_without_omop", "warn", column=axis,
                               observed=snomed_gap / base, expected=0.05))
        no_prov = v.get(("*", f"pair:map_no_provenance:{axis}"), 0.0)
        if no_prov > 0:
            out.append(finding(ctx, fqn, "F14", "F14c_provenance_missing", "warn", column=axis, observed=no_prov,
                               expected=0))
        cosine = v.get(("*", f"pair:map_cosine_no_target:{axis}"), 0.0)
        if cosine > 0:
            out.append(finding(ctx, fqn, "F14", "F14d_cosine_without_target", "info", column=axis, observed=cosine))
        codes_nd, display_nd = v.get((f"{axis}_source_code", "ndistinct")), v.get((f"{axis}_source_display", "ndistinct"))
        if codes_nd and display_nd and display_nd > 1.2 * codes_nd:
            out.append(finding(ctx, fqn, "F14", "F14e_display_cardinality", "info", column=axis,
                               observed=display_nd / codes_nd, expected=1.2))
        target_nd = v.get((f"{axis}_target_code", "ndistinct"))
        if codes_nd and target_nd:
            out.append(finding(ctx, fqn, "F14", "F14f_granularity", "info", column=axis,
                               observed=target_nd / codes_nd))
    return out


@release_detector
def f24_person(ctx, findings):
    """F24 person plausibility, J8 scope checks, L5172 multiple open intervals, F19 CC/spine death agreement,
    BlobIdentity NULL-person share by feed and the MSDS delivery-agreement guard."""
    spine = "`4_prod`.gold.spine_person"
    out = []
    for r in ctx.checks.get("F24_person", []):
        if r["age_over_116_alive"]:
            out.append(finding(ctx, spine, "F24", "F24_age_over_116", "warn", observed=r["age_over_116_alive"], expected=0))
    for r in ctx.checks.get("F24_implied_age", []):
        if r["over_115"]:
            out.append(finding(ctx, "`4_prod`.gold.clinical_safety_incident_participant", "F24", "F24_implied_age_over_115",
                               "warn", observed=r["over_115"], expected=0))
    for r in ctx.checks.get("F24_after_death", []):
        if r["after_death_30d"]:
            out.append(finding(ctx, "`4_prod`.gold.reference_person_death_evidence", "F24", "F24_activity_after_death",
                               "warn", observed=r["after_death_30d"], expected=0))
    for r in ctx.checks.get("F24_test", []):
        if r["test_flagged"]:
            out.append(finding(ctx, spine, "F24", "F24_test_patients", "warn", observed=r["test_flagged"], expected=0,
                               basis=r["basis"]))
    for r in ctx.checks.get("J8_scope", []):
        for key, code in (("not_in_silver_scope", "J8_not_in_silver_scope"), ("out_of_scope", "J8_out_of_scope")):
            if r[key]:
                out.append(finding(ctx, spine, "F24", code, "gate_candidate", observed=r[key], expected=0))
    for r in ctx.checks.get("J8_fact_person", []):
        if r["not_in_spine"]:
            out.append(finding(ctx, r["fqn"], "F24", "J8_fact_person_not_in_spine", "gate_candidate",
                               column="person_id", observed=r["not_in_spine"], expected=0))
    tables = {"gp_registration": "`4_prod`.gold.reference_person_gp_registration",
              "person_address": "`4_prod`.gold.reference_person_address", "episode": "`4_prod`.gold.spine_episode"}
    for r in ctx.checks.get("L5172_open_intervals", []):
        if r["persons_multi_open"]:
            out.append(finding(ctx, tables[r["tbl"]], "F24", "L5172_multiple_open_intervals", "info",
                               observed=r["persons_multi_open"]))
    for r in ctx.checks.get("F19_cc_death", []):
        for key in ("disagree", "cc_missing_death"):
            if r[key]:
                out.append(finding(ctx, "`4_prod`.gold.clinical_critical_care_admission", "F19", f"F19_cc_death_{key}",
                                   "warn", column="date_of_death", observed=r[key], expected=0))
    for r in ctx.checks.get("BI_text_null_person", []):
        if r["n"]:
            share = r["null_person"] / r["n"]
            consent = "consent" in r["feed"].lower() or "concentric" in r["feed"].lower()
            if share > (0.01 if consent else 0.0):
                out.append(finding(ctx, "`4_prod`.gold.text_document", "F12", "BI_null_person_by_feed",
                                   "warn" if consent else "info", arm=r["feed"], observed=share,
                                   expected=0.01 if consent else 0.0))
    out += msds_guard(ctx)
    labour = {r["period"]: r["n"] for r in ctx.checks.get("F12_labour_baby_ratio", []) if r["kind"] == "labour" and r["period"]}
    baby = {r["period"]: r["n"] for r in ctx.checks.get("F12_labour_baby_ratio", []) if r["kind"] == "baby" and r["period"]}
    ratio = {p: labour[p] / baby[p] for p in sorted(labour) if baby.get(p)}
    for p, x in ratio.items():
        base = trailing_median(ratio, p)
        if base and abs(x / base - 1) > 0.10 and month_index(p) < month_index(ctx.boundary) - 2:
            out.append(finding(ctx, "`4_prod`.gold.clinical_labour_delivery", "F12", "F12_labour_baby_ratio_step", "info",
                               period=p, observed=x, expected=base))
    return out


def msds_guard(ctx):
    """Master W4.04 MSDS regression guard: Millennium vs MSDS delivery agreement from W2.17's `_msds_regression_guard`.

    The person cell puts either `MSDS_guard` (the MV's agreement counts: msds_deliveries, agreed_deliveries) or
    `MSDS_guard_not_built` (the W2.17 gate verdict) into CHECKS. Agreement below 0.95 warns; a missing MV (W2.17
    NO-GO or not yet run) is one info finding. The labour/baby ratio above is a separate F12 proxy, not this guard.
    """
    labour = "`4_prod`.gold.clinical_labour_delivery"
    out = []
    for r in ctx.checks.get("MSDS_guard", []):
        if r["msds_deliveries"]:
            agreement = r["agreed_deliveries"] / r["msds_deliveries"]
            out.append(finding(ctx, labour, "F12", "MSDS_guard_agreement", "warn" if agreement < 0.95 else "info",
                               observed=agreement, expected=0.95))
    for r in ctx.checks.get("MSDS_guard_not_built", []):
        out.append(finding(ctx, labour, "F12", "MSDS_guard_not_built", "info",
                           gate_verdict=f"W2.17 gate verdict: {r['gate_verdict']}"))
    return out


# ---------------------------------------------------------------------------------------------------------------
# W4.07 F16 rule coverage: residual of implemented transforms, and the measured share of the catalogue
# ---------------------------------------------------------------------------------------------------------------

@release_detector
def f16_residual(ctx, findings):
    """F16(c) residual > 0 on a published table is a gate_candidate; F16(g) measured / (measured + unavailable)
    across the latest catalogue is reported once per release."""
    out = []
    for r in ctx.checks.get("F16_residual", []):
        if r["residual"]:
            out.append(finding(ctx, r["fqn"], "F16", "F16c_residual", "gate_candidate", column=r["column"],
                               observed=r["residual"], expected=0, rule_id=r["rule_id"]))
    for r in ctx.checks.get("F16g", []):
        total = r["measured"] + r["unavailable"]
        if total:
            out.append(finding(ctx, "*", "F16", "F16g_coverage", "info", observed=r["measured"] / total,
                               measured=r["measured"], unavailable=r["unavailable"]))
    return out


@release_detector
def sct_not_in_loaded_release(ctx, findings):
    """SCT_TRUD_V1 advisory (design 4): per gold SNOMED column, rows holding a concept-partition SCTID that neither
    the loaded RF2 release nor the OMOP vocabulary knows (ctx.checks['SCT_release:<fqn>'], oneoff stage). These are
    kept on purpose (both releases lag; newer concepts are real), so the finding is info only, never a gate."""
    out = []
    for key, rows in sorted(ctx.checks.items()):
        if not key.startswith("SCT_release:"):
            continue
        for r in rows:
            if r["n"]:
                out.append(finding(ctx, key[len("SCT_release:"):], "SCT", "sct_not_in_loaded_release", "info",
                                   column=r["column_name"], observed=r["n"], codes=r["codes"]))
    return out


# BBR_FOLDIN_V1 advisories (release_profile.bbr_oneoff_sqls): code -> (severity when n > 0, target note). All are
# info/warn findings, never gates. med_category_share is the inverse: it warns when NO non-inpatient order exists.
BBR_ADVISORY_SEVERITY = {
    "condition_std_snomed_no_omop": "warn",       # BBR-1, target 0
    "condition_priority_collision": "info",       # BBR-2, baseline 43 on n100 (40 on n1000)
    "path_concept_multi_dimension": "info",       # BBR-12
    "med_instance_orphan": "info",                # BBR-13, 0 on n1000 raw
    "religion_unmapped_codes": "warn",            # BBR-16, target 0
}


@release_detector
def bbr_advisories(ctx, findings):
    """BBR fold-in advisories from ctx.checks['BBR:<code>:<fqn>'] (oneoff stage): a finding per non-zero count, and
    for med_category_share a warn when discharge/home-medication orders are absent from 2021-09 (BBR-14). A check the
    report recorded as an error (a column the table does not have yet) gives an info finding naming the error."""
    out = []
    for key, rows in sorted(ctx.checks.items()):
        if not key.startswith("BBR:"):
            continue
        _, code, fqn = key.split(":", 2)
        row = rows[0] if rows else {}
        if "error" in row:
            out.append(finding(ctx, fqn, "BBR", f"bbr_{code}_unavailable", "info", error=row["error"][:300]))
        elif code == "med_category_share":
            if row.get("total") and not row.get("n"):
                out.append(finding(ctx, fqn, "BBR", "bbr_med_category_share", "warn", column="category_code",
                                   observed=0, total=row["total"]))
        elif row.get("n"):
            out.append(finding(ctx, fqn, "BBR", f"bbr_{code}", BBR_ADVISORY_SEVERITY.get(code, "info"),
                               observed=row["n"]))
    return out
