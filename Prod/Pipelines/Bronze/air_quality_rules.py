# Databricks notebook source
# Pure rules for the UK air-quality Bronze feed: no Spark, no network.
# Loaded with %run by air_quality_pipeline and imported directly by pytest.
import hashlib
import math
from datetime import date

AIR_QUALITY_RULES_VERSION = "2.0.0"
FIRST_YEAR = 2000

# source -> (R_data base URL, openair file abbreviation). The www. hosts matter:
# the bare aqe/waqn hosts 302-redirect, which is how aqe silently stopped in 2024.
NETWORKS = {
    "aurn": ("https://uk-air.defra.gov.uk/openair/R_data/", "AURN"),
    "aqe": ("https://www.airqualityengland.co.uk/assets/openair/R_data/", "AQE"),
    "saqn": ("https://www.scottishairquality.scot/openair/R_data/", "SCOT"),
    "waqn": ("https://www.airquality.gov.wales/sites/default/files/openair/R_data/", "WAQ"),
    "ni": ("https://www.airqualityni.co.uk/openair/R_data/", "NI"),
    "local": ("https://uk-air.defra.gov.uk/openair/LMAM/R_data/", "LMAM"),
}


def monthly_url(source, year):
    base, abbr = NETWORKS[source]
    return f"{base}summary_monthly_{abbr}_{year}.rds"


def metadata_url(source):
    base, abbr = NETWORKS[source]
    return f"{base}{abbr}_metadata.RData"


def years_to_fetch(today, lookback_years, full_history):
    if lookback_years < 1:
        raise ValueError("lookback_years must be >= 1")
    first = FIRST_YEAR if full_history else max(FIRST_YEAR, today.year - lookback_years + 1)
    return list(range(first, today.year + 1))


def required_years(today):
    """Years whose file must exist; the new year's file gets January as grace."""
    years = {today.year - 1}
    if today.month >= 2:
        years.add(today.year)
    return years


def month_cutoff(today):
    """First day of the current month; that month and later are partial and excluded."""
    return date(today.year, today.month, 1)


def normalise_column(name):
    if name in ("Date", "date"):
        return "date"
    capture = name.endswith(".capture")
    base = name[: -len(".capture")] if capture else name
    if base.endswith(".mean"):
        base = base[: -len(".mean")]
    if base == "NOXasNO2":
        base = "nox"
    base = base.lower().replace(".", "_")
    return base + "_cap" if capture else base


def normalise_frame(frame):
    """Rename raw openair columns; NOXasNO2 wins a nox clash, any other clash fails."""
    chosen = {}
    for raw in frame.columns:
        target = normalise_column(str(raw))
        is_noxasno2 = str(raw).startswith("NOXasNO2")
        if target in chosen:
            previous = chosen[target]
            if is_noxasno2 and not str(previous).startswith("NOXasNO2"):
                chosen[target] = raw
            elif str(previous).startswith("NOXasNO2") and not is_noxasno2:
                continue
            else:
                raise ValueError(f"column collision on {target}: {previous!r} vs {raw!r}")
        else:
            chosen[target] = raw
    return frame[[chosen[t] for t in chosen]].set_axis(list(chosen), axis=1)


POLLUTANTS = (
    "no", "no2", "nox", "o3", "so2", "co", "pm10", "pm2_5",
    "v10", "v2_5", "nv10", "nv2_5", "gr10", "gr2_5",
)
MET_MEASURES = ("ws", "wd", "air_temp", "rh", "pressure", "rain", "solar")
# data_cap_* columns that exist on 3_lookup.geo.uk_air_quality_monthly today.
DATA_CAP_MEASURES = (
    "no", "no2", "nox", "o3", "so2", "co", "pm10", "pm2_5", "v10", "v2_5",
    "nv10", "nv2_5", "ws", "wd", "air_temp", "rh", "pressure", "rain", "solar",
)
KEY_COLUMNS = ("date", "year", "month", "day", "hour", "source", "code", "site", "site_id")
LANDING_COLUMNS = (
    KEY_COLUMNS
    + POLLUTANTS
    + tuple(f"{p}_cap" for p in POLLUTANTS)
    + MET_MEASURES
    + tuple(f"data_cap_{m}" for m in DATA_CAP_MEASURES)
)
_KNOWN = set(POLLUTANTS) | set(MET_MEASURES) | {"date", "code", "site", "uka_code"}


def _clean(value):
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _number(value):
    value = _clean(value)
    return None if value is None else float(value)


def landing_rows(raw, source, year, today):
    """Raw openair monthly frame -> (landing row dicts, unknown measure names)."""
    frame = normalise_frame(raw)
    if "code" not in frame.columns or "date" not in frame.columns:
        raise ValueError(f"{source} {year}: file lacks date/code columns")
    unknown = sorted(
        c for c in frame.columns
        if not c.endswith("_cap") and c not in _KNOWN
    )
    cutoff = month_cutoff(today)
    measures = POLLUTANTS + MET_MEASURES
    rows = []
    for record in frame.to_dict("records"):
        month_start = date.fromisoformat(str(record["date"])[:10])
        if month_start.year != year:
            raise ValueError(f"{source} {year}: row dated {month_start} has the wrong year")
        if month_start >= cutoff:
            continue
        values = {m: _number(record.get(m)) for m in measures}
        if all(v is None for v in values.values()):
            continue
        captures = {m: _number(record.get(f"{m}_cap")) for m in measures}
        code = str(record["code"]).strip()
        row = {
            "date": month_start,
            "year": month_start.year,
            "month": month_start.month,
            "day": 1,
            "hour": None,
            "source": source,
            "code": code,
            "site": _clean(record.get("site")),
            "site_id": f"{source}_{code}",
        }
        row.update(values)
        row.update({f"{p}_cap": captures[p] for p in POLLUTANTS})
        row.update({f"data_cap_{m}": captures[m] for m in DATA_CAP_MEASURES})
        rows.append(row)
    return rows, unknown


SITE_COLUMNS = (
    "site_id", "code", "source", "site", "site_type", "latitude", "longitude",
    "local_authority", "zone", "agglomeration",
)
_SITE_TIEBREAK = ("latitude", "longitude", "site", "site_type", "local_authority", "zone", "agglomeration")


def _text(value):
    value = _clean(value)
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _site_preference(row):
    """Complete coordinate pair first, then a value-based tie-break (NULLs last)."""
    complete = row["latitude"] is not None and row["longitude"] is not None
    return (
        0 if complete else 1,
        tuple((row[c] is None, "" if row[c] is None else row[c]) for c in _SITE_TIEBREAK),
    )


def site_rows(meta, source):
    """openair metadata (one row per site x parameter) -> one row per site_id."""
    candidates = {}
    for record in meta.to_dict("records"):
        code = _text(record.get("site_id"))
        if code is None:
            continue
        candidate = {
            "site_id": f"{source}_{code}",
            "code": code,
            "source": source,
            "site": _text(record.get("site_name")),
            "site_type": _text(record.get("location_type")),
            "latitude": _number(record.get("latitude")),
            "longitude": _number(record.get("longitude")),
            "local_authority": _text(record.get("local_authority")),
            "zone": _text(record.get("zone")),
            "agglomeration": _text(record.get("agglomeration")),
        }
        candidates.setdefault(candidate["site_id"], []).append(candidate)
    return [min(candidates[key], key=_site_preference) for key in sorted(candidates)]


def sha256_hex(payload):
    return hashlib.sha256(payload).hexdigest()


def effective_cutoff(year, cutoff):
    """A year's file contributes rows strictly before this date."""
    return min(cutoff, date(year + 1, 1, 1))


def needs_refresh(prior, sha256, year, cutoff):
    """Re-land when the bytes, the eligible-month window or the rules changed."""
    if prior is None:
        return True
    return (
        prior["sha256"] != sha256
        or prior["applied_cutoff"] != effective_cutoff(year, cutoff)
        or prior["rules_version"] != AIR_QUALITY_RULES_VERSION
    )


def site_count_failure(source, year, new_sites, old_sites):
    """A re-landed network-year must keep at least half the sites it replaces."""
    if new_sites == 0:
        return f"{source} {year}: parsed file has no rows"
    if old_sites > 0 and new_sites < 0.5 * old_sites:
        return f"{source} {year}: {new_sites} sites would replace {old_sites}"
    return None


def delete_budget_failure(label, deletes, target_rows, fraction):
    if target_rows > 0 and deletes > fraction * target_rows:
        return f"{label}: {deletes} deletes exceed {fraction:.0%} of {target_rows} rows"
    return None

