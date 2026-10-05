# Databricks notebook source
# MAGIC %md
# MAGIC # Air-quality Bronze feed
# MAGIC
# MAGIC Weekly task `air_quality_pipeline` of Bronze_Pipeline_Parallel (depends on `map_address`).
# MAGIC Phases: fetch -> sites -> bronze stage -> validate -> publish -> verify -> freshness.
# MAGIC Everything is staged in the control schema and validated before the landing table,
# MAGIC site table or either Bronze table changes. Bronze writes are change-only MERGEs so the
# MAGIC Silver air-quality MVs stay incremental. Until a verified full-history run completes,
# MAGIC every run is a full-history (bootstrap) run.

# COMMAND ----------

# MAGIC %pip install pyreadr==0.5.7 --quiet

# COMMAND ----------

# MAGIC %run ./_bronze_common

# COMMAND ----------

# MAGIC %run ./air_quality_rules

# COMMAND ----------

import tempfile
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone

import pyreadr
from delta.tables import DeltaTable
from pyspark.sql import functions as F

for _name, _default in {
    "landing_schema": "3_lookup.geo",
    "address_source_table": "",
    "lookback_years": "3",
    "full_history": "false",
    "as_of_date": "",
    "allow_production_write": "false",
}.items():
    try:
        dbutils.widgets.text(_name, _default)
    except Exception:
        pass

AIR_QUALITY_SCHEMA_VERSION = "2.0.0"
RUN_ID = bronze_run_id()
STARTED_AT = datetime.now(timezone.utc)
_AS_OF = bronze_value("as_of_date", "")
TODAY = date.fromisoformat(_AS_OF) if _AS_OF else STARTED_AT.date()
assert TODAY <= STARTED_AT.date(), f"as_of_date {TODAY} is in the future"
CUTOFF = month_cutoff(TODAY)

TARGET_SCHEMA = bronze_value("target_schema", "4_prod.bronze")
LANDING_SCHEMA = bronze_value("landing_schema", "3_lookup.geo")
CONTROL_SCHEMA = bronze_control_schema(TARGET_SCHEMA)
ADDRESS_SOURCE = bronze_value("address_source_table", "") or f"{TARGET_SCHEMA}.map_address"
LOOKBACK_YEARS = int(bronze_value("lookback_years", "3"))

LANDING = f"{LANDING_SCHEMA}.uk_air_quality_monthly"
SITES = f"{LANDING_SCHEMA}.uk_air_quality_sites"
SITE_TARGET = f"{TARGET_SCHEMA}.map_air_quality_site_month"
ADDRESS_TARGET = f"{TARGET_SCHEMA}.map_address_air_quality_cell"
STATE = f"{CONTROL_SCHEMA}.air_quality_pipeline_state"
CONTROL = f"{CONTROL_SCHEMA}.air_quality_pipeline_control"
AUDIT = f"{CONTROL_SCHEMA}.air_quality_pipeline_audit"
LANDING_STAGE = f"{CONTROL_SCHEMA}.air_quality_landing_stage"
LANDING_CANDIDATE = f"{CONTROL_SCHEMA}.air_quality_landing_candidate"
SITE_META_STAGE = f"{CONTROL_SCHEMA}.air_quality_site_meta_stage"
SITES_CANDIDATE = f"{CONTROL_SCHEMA}.air_quality_sites_candidate"
SITE_STAGE = f"{CONTROL_SCHEMA}.air_quality_site_month_stage"
ADDRESS_STAGE = f"{CONTROL_SCHEMA}.air_quality_address_cell_stage"

SITE_VALUE_COLUMNS = (
    "measurement_value", "data_capture_pct", "source_network", "source_row_count",
    "site_latitude", "site_longitude", "site_lat_cell", "site_lon_cell",
)
SITE_COLUMNS_ALL = ("site_id", "month_start", "pollutant") + SITE_VALUE_COLUMNS
ADDRESS_VALUE_COLUMNS = ("LATITUDE", "LONGITUDE", "lat_cell", "lon_cell")
ADDRESS_COLUMNS_ALL = ("ADDRESS_ID",) + ADDRESS_VALUE_COLUMNS
NETWORK_LIST = ", ".join(f"'{s}'" for s in NETWORKS)
# The R-era landing kept all-null grid rows; landing_rows drops them, so count measured sites only.
MEASURE_LIST = ", ".join(POLLUTANTS + MET_MEASURES)
TAGS = {
    SITE_TARGET: {c: ("0", "0") for c in SITE_COLUMNS_ALL},
    ADDRESS_TARGET: {
        "ADDRESS_ID": ("0", "0"), "LATITUDE": ("4", "3"), "LONGITUDE": ("4", "3"),
        "lat_cell": ("1", "1"), "lon_cell": ("1", "1"),
    },
}


def _is_dev(name):
    return name.lower().startswith("8_dev.")


# The guard runs before any table is touched, so an unauthorised run writes nothing at all.
if not all(_is_dev(s) for s in (TARGET_SCHEMA, LANDING_SCHEMA, CONTROL_SCHEMA)):
    if not bronze_bool("allow_production_write", False):
        raise RuntimeError(
            "air_quality_pipeline writes outside 8_dev only with allow_production_write=true"
        )

for required in (LANDING, SITES, SITE_TARGET, ADDRESS_TARGET, ADDRESS_SOURCE):
    assert bronze_table_exists(required), f"missing required table {required}"
# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {STATE} (
  source STRING, year INT, url STRING, sha256 STRING, bytes BIGINT, applied_cutoff DATE,
  rules_version STRING, landed_rows BIGINT, landed_sites BIGINT, run_id STRING, landed_at TIMESTAMP
)""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROL} (
  control_key STRING, control_value STRING, run_id STRING, updated_at TIMESTAMP
)""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT} (
  run_id STRING, started_at TIMESTAMP, finished_at TIMESTAMP, status STRING, phase STRING,
  published STRING, error STRING, target_schema STRING, landing_schema STRING, details STRING
)""")

PRIOR_STATE = {(r_["source"], r_["year"]): r_.asDict() for r_ in spark.table(STATE).collect()}
BOOTSTRAPPED = bool(spark.table(CONTROL).where("control_key = 'bootstrap_completed'").take(1))
FULL_HISTORY = bronze_bool("full_history", False) or not BOOTSTRAPPED
YEARS = years_to_fetch(TODAY, LOOKBACK_YEARS, FULL_HISTORY)
REQUIRED = required_years(TODAY)
DELETE_FRACTION = 0.10 if FULL_HISTORY else 0.02

CTX = {
    "phase": "init",
    "published": [],       # writes to landing, sites and bronze targets
    "control_writes": [],  # state and bootstrap rows; these never make a run count as a change
    "details": {
        "rules_version": AIR_QUALITY_RULES_VERSION,
        "schema_version": AIR_QUALITY_SCHEMA_VERSION,
        "as_of": TODAY.isoformat(),
        "cutoff": CUTOFF.isoformat(),
        "bootstrapped_before_run": BOOTSTRAPPED,
        "full_history": FULL_HISTORY,
        "years": [YEARS[0], YEARS[-1]],
    },
}


def _naive(ts):
    return ts.replace(tzinfo=None)


def _audit(status, error=None):
    spark.createDataFrame(
        [(RUN_ID, _naive(STARTED_AT), _naive(datetime.now(timezone.utc)), status, CTX["phase"],
          bronze_json(CTX["published"]), error, TARGET_SCHEMA, LANDING_SCHEMA,
          bronze_json({**CTX["details"], "control_writes": CTX["control_writes"]}))],
        "run_id STRING, started_at TIMESTAMP, finished_at TIMESTAMP, status STRING, phase STRING, "
        "published STRING, error STRING, target_schema STRING, landing_schema STRING, details STRING",
    ).write.mode("append").saveAsTable(AUDIT)


def _one(sql):
    return spark.sql(sql).first().asDict()


# AIR_QUALITY_RETRY_V1: the network hosts have short outages. On 2026-10-04 one 502 from airqualityengland.co.uk
# failed the first-ever prod run (a full-history bootstrap of ~108 files) twice, because three attempts 5-10 s apart
# gave up within 15 s. Transient failures now back off for up to ~8 minutes per file; anything else still fails fast.
_RETRY_DELAYS = (15, 30, 60, 120, 240)
_RETRYABLE_HTTP = {408, 429}


def _download(url):
    """Bytes, or None on 404. 5xx/408/429, network errors and timeouts retry with backoff; other HTTP errors raise."""
    last_error = None
    for attempt in range(len(_RETRY_DELAYS) + 1):
        retry_after = None
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "barts-bronze-air-quality/2"})
            with urllib.request.urlopen(request, timeout=120) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            if exc.code < 500 and exc.code not in _RETRYABLE_HTTP:
                raise
            last_error = exc
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last_error = exc
        if attempt < len(_RETRY_DELAYS):
            wait = _RETRY_DELAYS[attempt]
            if retry_after and str(retry_after).strip().isdigit():
                wait = max(wait, min(int(retry_after), 600))
            print(f"[air_quality] {url}: {last_error}; retry {attempt + 1}/{len(_RETRY_DELAYS)} in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"download failed after {len(_RETRY_DELAYS) + 1} attempts: {url}: {last_error}")


def _read_r(payload, suffix):
    with tempfile.NamedTemporaryFile(suffix=suffix) as handle:
        handle.write(payload)
        handle.flush()
        result = pyreadr.read_r(handle.name)
    frames = [frame for frame in result.values() if frame is not None and len(frame.columns)]
    if len(frames) != 1:
        raise RuntimeError(f"expected one data frame in {suffix} payload, found {len(frames)}")
    return frames[0]


def pending_predicate(pending):
    """The landing rows owned by the re-landed network-years."""
    return " OR ".join(
        f"(source = '{i['source']}' AND date >= DATE'{i['year']}-01-01' AND date < DATE'{i['year'] + 1}-01-01')"
        for i in pending
    )


def _sf(name, data_type, comment):
    return F.col(name).cast(data_type).alias(name, metadata={"comment": comment})


def _changed_predicate(columns):
    return " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in columns)


def _symmetric_difference(left, right, columns):
    cols = ", ".join(f"`{c}`" for c in columns)
    return _one(f"""
        SELECT
          (SELECT count(*) FROM (SELECT {cols} FROM {left} EXCEPT ALL SELECT {cols} FROM {right})) +
          (SELECT count(*) FROM (SELECT {cols} FROM {right} EXCEPT ALL SELECT {cols} FROM {left})) AS n
    """)["n"]


def _present_tags():
    catalog, schema = TARGET_SCHEMA.split(".")
    return {
        (r_["table_name"], r_["column_name"], r_["tag_name"])
        for r_ in spark.sql(f"""
            SELECT table_name, column_name, tag_name FROM system.information_schema.column_tags
            WHERE catalog_name = '{catalog}' AND schema_name = '{schema}'
              AND table_name IN ('map_air_quality_site_month', 'map_address_air_quality_cell')
        """).collect()
    }


def _missing_tags():
    present = _present_tags()
    return [
        (table_name, column_name, risk, severity)
        for table_name, columns in TAGS.items()
        for column_name, (risk, severity) in columns.items()
        if not {(table_name.rsplit(".", 1)[-1], column_name, "ig_risk"),
                (table_name.rsplit(".", 1)[-1], column_name, "ig_severity")} <= present
    ]


def _schema_version(table_name):
    rows = spark.sql(f"SHOW TBLPROPERTIES {table_name} ('air_quality_schema_version')").collect()
    return rows[0]["value"] if rows else None


_audit("RUNNING")
print(bronze_json({"run_id": RUN_ID, "target": TARGET_SCHEMA, "landing": LANDING_SCHEMA, **CTX["details"]}))
# COMMAND ----------

def phase_fetch():
    CTX["phase"] = "fetch"
    prior_sites = {
        (r_["source"], r_["y"]): r_["n"]
        for r_ in spark.sql(f"""
            SELECT source, year(date) AS y, count(DISTINCT site_id) AS n
            FROM {LANDING}
            WHERE source IN ({NETWORK_LIST}) AND coalesce({MEASURE_LIST}) IS NOT NULL
            GROUP BY 1, 2
        """).collect()
    }
    pending, failures, unchanged, absent, unknown_species = [], [], [], [], {}
    for source in NETWORKS:
        for year in YEARS:
            url = monthly_url(source, year)
            payload = _download(url)
            if payload is None:
                if year in REQUIRED:
                    failures.append(f"{source} {year}: required file missing (404) {url}")
                else:
                    absent.append(f"{source}_{year}")
                continue
            digest = sha256_hex(payload)
            if not FULL_HISTORY and not needs_refresh(PRIOR_STATE.get((source, year)), digest, year, CUTOFF):
                unchanged.append(f"{source}_{year}")
                continue
            try:
                rows, unknown = landing_rows(_read_r(payload, ".rds"), source, year, TODAY)
            except Exception as exc:
                failures.append(f"{source} {year}: parse failed: {exc}")
                continue
            if unknown:
                unknown_species[f"{source}_{year}"] = unknown
            new_sites = len({row["site_id"] for row in rows})
            if year < TODAY.year or TODAY.month > 1:
                problem = site_count_failure(source, year, new_sites, prior_sites.get((source, year), 0))
                if problem:
                    failures.append(problem)
            pending.append({
                "source": source, "year": year, "url": url, "sha256": digest, "bytes": len(payload),
                "applied_cutoff": effective_cutoff(year, CUTOFF), "rows": rows, "sites": new_sites,
            })
    CTX["details"].update({
        "files_changed": len(pending), "files_unchanged": len(unchanged),
        "files_absent": absent, "unknown_species": unknown_species,
    })
    assert not failures, "air-quality fetch gates failed: " + "; ".join(failures)

    landing_struct = spark.table(LANDING).schema
    import_ts = STARTED_AT.isoformat()
    landing_tuples = [
        tuple(
            {**row, "import_timestamp": import_ts, "ADC_UPDT": _naive(STARTED_AT)}.get(field.name)
            for field in landing_struct.fields
        )
        for item in pending
        for row in item["rows"]
    ]
    (
        spark.createDataFrame(landing_tuples, schema=landing_struct)
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(LANDING_STAGE)
    )
    # Current-year files change bytes daily (the excluded partial month), so a new hash does not
    # mean new rows. Publish only the network-years whose eligible rows differ from the landing.
    changed_years = set()
    if pending:
        value_cols = ", ".join(
            f"`{f.name}`" for f in landing_struct.fields if f.name not in ("import_timestamp", "ADC_UPDT")
        )
        changed_years = {
            (r_["source"], r_["y"])
            for r_ in spark.sql(f"""
                WITH cur AS (SELECT {value_cols} FROM {LANDING} WHERE {pending_predicate(pending)}),
                     stg AS (SELECT {value_cols} FROM {LANDING_STAGE}),
                     diff AS ((SELECT * FROM cur EXCEPT ALL SELECT * FROM stg)
                              UNION ALL (SELECT * FROM stg EXCEPT ALL SELECT * FROM cur))
                SELECT DISTINCT source, year(date) AS y FROM diff
            """).collect()
        }
    publish = [item for item in pending if (item["source"], item["year"]) in changed_years]
    replace_predicate = pending_predicate(publish) or "false"
    # coalesce: a NULL-source row must stay in the candidate, not vanish through NOT(NULL).
    spark.sql(f"""
        CREATE OR REPLACE TABLE {LANDING_CANDIDATE} AS
        SELECT * FROM {LANDING} WHERE NOT coalesce(({replace_predicate}), false)
        UNION ALL
        SELECT * FROM {LANDING_STAGE} WHERE {replace_predicate}
    """)
    CTX["details"].update({
        "landing_stage_rows": len(landing_tuples),
        "landing_years_published": sorted(f"{i['source']}_{i['year']}" for i in publish),
    })
    return pending, publish, replace_predicate
# COMMAND ----------

def phase_sites():
    CTX["phase"] = "sites"
    meta_rows = []
    for source in NETWORKS:
        payload = _download(metadata_url(source))
        assert payload is not None, f"metadata missing for {source}: {metadata_url(source)}"
        meta_rows.extend(site_rows(_read_r(payload, ".RData"), source))
    (
        spark.createDataFrame(
            [tuple(row[c] for c in SITE_COLUMNS) for row in meta_rows],
            "site_id STRING, code STRING, source STRING, site STRING, site_type STRING, "
            "latitude DOUBLE, longitude DOUBLE, local_authority STRING, zone STRING, agglomeration STRING",
        )
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SITE_META_STAGE)
    )

    new_site_values = {
        "site_id": "m.site_id", "code": "m.code", "source": "m.source", "site": "m.site",
        "site_type": "m.site_type", "latitude": "m.latitude", "longitude": "m.longitude",
        "latitude_rounded": "round(m.latitude, 4)", "longitude_rounded": "round(m.longitude, 4)",
        "local_authority": "m.local_authority", "zone": "m.zone", "agglomeration": "m.agglomeration",
        "import_date": "current_date()", "ADC_UPDT": "current_timestamp()",
    }
    site_fields = spark.table(SITES).schema.fields
    existing_cols = ", ".join(f"`{f.name}`" for f in site_fields)
    new_cols = ", ".join(
        f"CAST({new_site_values.get(f.name, 'NULL')} AS {f.dataType.simpleString()}) AS `{f.name}`"
        for f in site_fields
    )
    spark.sql(f"""
        CREATE OR REPLACE TABLE {SITES_CANDIDATE} AS
        SELECT {existing_cols}, false AS is_new FROM {SITES}
        UNION ALL
        SELECT {new_cols}, true AS is_new
        FROM {SITE_META_STAGE} m
        WHERE m.latitude IS NOT NULL AND m.longitude IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM {SITES} s WHERE s.site_id = m.site_id)
    """)
    new_sites = spark.table(SITES_CANDIDATE).where("is_new").count()

    drift = _one(f"""
        SELECT count(DISTINCT m.site_id) AS n
        FROM {SITE_META_STAGE} m JOIN {SITES} s ON s.site_id = m.site_id
        WHERE abs(s.latitude - m.latitude) > 0.001 OR abs(s.longitude - m.longitude) > 0.001
    """)["n"]
    unresolved = _one(f"""
        WITH observed AS (
          SELECT DISTINCT site_id FROM {LANDING_CANDIDATE}
          WHERE source IN ({NETWORK_LIST}) AND date >= DATE'{YEARS[0]}-01-01'
        )
        SELECT
          count(*) AS observed_sites,
          count_if(s.site_id IS NULL) AS unresolved_sites,
          slice(array_sort(collect_list(CASE WHEN s.site_id IS NULL THEN o.site_id END)), 1, 20) AS sample
        FROM observed o
        LEFT JOIN (SELECT DISTINCT site_id FROM {SITES_CANDIDATE}
                   WHERE latitude IS NOT NULL AND longitude IS NOT NULL) s
          ON s.site_id = o.site_id
    """)
    CTX["details"].update({
        "sites_new": new_sites, "site_coordinate_drift": drift, "unresolved_sites": unresolved,
    })
    return new_sites, unresolved
# COMMAND ----------

def phase_bronze_stage():
    CTX["phase"] = "bronze_stage"
    spark.sql(f"""
    WITH long_measurements AS (
      SELECT site_id, date AS month_start, pollutant, measurement_value, data_capture_pct,
             source AS source_network
      FROM {LANDING_CANDIDATE}
      LATERAL VIEW stack(14,
        'no', no, no_cap, 'no2', no2, no2_cap, 'nox', nox, nox_cap, 'o3', o3, o3_cap,
        'so2', so2, so2_cap, 'co', co, co_cap, 'pm10', pm10, pm10_cap,
        'pm2_5', pm2_5, pm2_5_cap, 'v10', v10, v10_cap, 'v2_5', v2_5, v2_5_cap,
        'nv10', nv10, nv10_cap, 'nv2_5', nv2_5, nv2_5_cap, 'gr10', gr10, gr10_cap,
        'gr2_5', gr2_5, gr2_5_cap
      ) AS pollutant, measurement_value, data_capture_pct
      WHERE measurement_value IS NOT NULL
    ),
    deduped_measurements AS (
      SELECT site_id, month_start, pollutant,
             avg(measurement_value) AS measurement_value,
             max(data_capture_pct) AS data_capture_pct,
             min(source_network) AS source_network,
             count(*) AS source_row_count
      FROM long_measurements
      GROUP BY site_id, month_start, pollutant
    ),
    observed AS (
      SELECT site_id, sum(source_row_count) AS observation_rows
      FROM deduped_measurements GROUP BY site_id
    ),
    ranked_sites AS (
      SELECT s.site_id, s.latitude, s.longitude,
             row_number() OVER (
               PARTITION BY s.site_id
               ORDER BY
                 CASE WHEN s.latitude IS NOT NULL AND s.longitude IS NOT NULL THEN 0 ELSE 1 END ASC,
                 s.local_authority ASC NULLS LAST,
                 s.site_type ASC NULLS LAST
             ) AS dedup_rank
      FROM {SITES_CANDIDATE} s
    ),
    resolved_sites AS (
      SELECT r.site_id, r.latitude, r.longitude
      FROM ranked_sites r JOIN observed o ON o.site_id = r.site_id
      WHERE r.dedup_rank = 1 AND r.latitude IS NOT NULL AND r.longitude IS NOT NULL
    )
    SELECT m.site_id, m.month_start, m.pollutant, m.measurement_value, m.data_capture_pct,
           m.source_network, m.source_row_count,
           s.latitude AS site_latitude, s.longitude AS site_longitude,
           cast(floor(s.latitude / 0.25) AS INT) AS site_lat_cell,
           cast(floor(s.longitude / 0.25) AS INT) AS site_lon_cell
    FROM deduped_measurements m JOIN resolved_sites s ON s.site_id = m.site_id
    """).select(
        _sf("site_id", "string", "Monitoring-site identifier from the UK air-quality reference feed."),
        _sf("month_start", "date", "First day of the monthly observation period."),
        _sf("pollutant", "string", "One of the 14 explicitly approved pollutant measures."),
        _sf("measurement_value", "double", "Monthly average pollutant measurement at the site."),
        _sf("data_capture_pct", "double", "Source-reported monthly data-capture value for this pollutant."),
        _sf("source_network", "string", "Monitoring network or data provider."),
        _sf("source_row_count", "long", "Number of source rows represented by this unique site-month-pollutant observation; duplicate source values are averaged."),
        _sf("site_latitude", "double", "Public monitoring-site latitude used for distance calculation."),
        _sf("site_longitude", "double", "Public monitoring-site longitude used for distance calculation."),
        _sf("site_lat_cell", "int", "0.25-degree latitude cell used to bound the S2 spatial join."),
        _sf("site_lon_cell", "int", "0.25-degree longitude cell used to bound the S2 spatial join."),
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SITE_STAGE)

    spark.sql(f"""
        SELECT ADDRESS_ID, LATITUDE, LONGITUDE,
               cast(floor(LATITUDE / 0.25) AS INT) AS lat_cell,
               cast(floor(LONGITUDE / 0.25) AS INT) AS lon_cell
        FROM {ADDRESS_SOURCE}
        WHERE PARENT_ENTITY_NAME = 'PERSON' AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """).select(
        _sf("ADDRESS_ID", "long", "Source address identifier retained for person-address linkage in Silver."),
        _sf("LATITUDE", "double", "Matched address latitude; precise geospatial field."),
        _sf("LONGITUDE", "double", "Matched address longitude; precise geospatial field."),
        _sf("lat_cell", "int", "0.25-degree latitude cell used to bound air-quality site candidates."),
        _sf("lon_cell", "int", "0.25-degree longitude cell used to bound air-quality site candidates."),
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ADDRESS_STAGE)

    site_key = "t.site_id = s.site_id AND t.month_start = s.month_start AND t.pollutant = s.pollutant"
    site_counts = _one(f"""
        SELECT
          count_if(t.site_id IS NULL) AS inserts,
          count_if(s.site_id IS NULL) AS deletes,
          count_if(s.site_id IS NOT NULL AND t.site_id IS NOT NULL
                   AND ({_changed_predicate(SITE_VALUE_COLUMNS)})) AS updates,
          count_if(s.site_id IS NOT NULL) AS stage_rows,
          count_if(t.site_id IS NOT NULL) AS target_rows,
          count_if(s.month_start >= DATE'{CUTOFF}') AS partial_month_rows,
          (SELECT count(*) FROM (SELECT 1 FROM {SITE_STAGE}
             GROUP BY site_id, month_start, pollutant HAVING count(*) > 1)) AS duplicate_keys,
          (SELECT count(DISTINCT pollutant) FROM {SITE_STAGE}) AS pollutants
        FROM {SITE_STAGE} s FULL OUTER JOIN {SITE_TARGET} t ON {site_key}
    """)
    address_counts = _one(f"""
        SELECT
          count_if(t.ADDRESS_ID IS NULL) AS inserts,
          count_if(s.ADDRESS_ID IS NULL) AS deletes,
          count_if(s.ADDRESS_ID IS NOT NULL AND t.ADDRESS_ID IS NOT NULL
                   AND ({_changed_predicate(ADDRESS_VALUE_COLUMNS)})) AS updates,
          count_if(s.ADDRESS_ID IS NOT NULL) AS stage_rows,
          count_if(t.ADDRESS_ID IS NOT NULL) AS target_rows,
          (SELECT count(*) - count(DISTINCT ADDRESS_ID) FROM {ADDRESS_STAGE}) AS duplicate_ids
        FROM {ADDRESS_STAGE} s FULL OUTER JOIN {ADDRESS_TARGET} t ON t.ADDRESS_ID = s.ADDRESS_ID
    """)
    CTX["details"].update({"site_month": site_counts, "address_cell": address_counts})
    return {"site_month": site_counts, "address_cell": address_counts, "site_key": site_key}
# COMMAND ----------

def phase_validate(unresolved, counts):
    CTX["phase"] = "validate"
    sm, ac = counts["site_month"], counts["address_cell"]
    failures = []
    if unresolved["unresolved_sites"] > 0.10 * unresolved["observed_sites"]:
        failures.append(f"observed sites lacking coordinates: {unresolved}")
    if sm["duplicate_keys"]:
        failures.append(f"site_month duplicate keys={sm['duplicate_keys']}")
    if sm["pollutants"] != 14:
        failures.append(f"site_month pollutants={sm['pollutants']}")
    if sm["partial_month_rows"]:
        failures.append(f"site_month partial_month_rows={sm['partial_month_rows']}")
    if sm["stage_rows"] < 0.95 * sm["target_rows"]:
        failures.append(f"site_month stage {sm['stage_rows']} < 95% of target {sm['target_rows']}")
    if ac["duplicate_ids"]:
        failures.append(f"address_cell duplicate ADDRESS_IDs={ac['duplicate_ids']}")
    for problem in (
        delete_budget_failure(SITE_TARGET, sm["deletes"], sm["target_rows"], DELETE_FRACTION),
        delete_budget_failure(ADDRESS_TARGET, ac["deletes"], ac["target_rows"], 0.02),
    ):
        if problem:
            failures.append(problem)
    CTX["details"]["validation_failures"] = failures
    assert not failures, "air-quality validation failed before publish: " + "; ".join(failures)
# COMMAND ----------

def phase_publish(publish, replace_predicate, new_sites, counts):
    CTX["phase"] = "publish"
    if publish:
        (
            spark.table(LANDING_STAGE).where(replace_predicate)
            .write.format("delta").mode("overwrite")
            .option("replaceWhere", replace_predicate)
            .saveAsTable(LANDING)
        )
        CTX["published"].append(f"landing:{len(publish)} network-years")
    if new_sites:
        spark.sql(f"""
            MERGE INTO {SITES} t
            USING (SELECT * EXCEPT (is_new) FROM {SITES_CANDIDATE} WHERE is_new) s
            ON t.site_id = s.site_id
            WHEN NOT MATCHED THEN INSERT *
        """)
        CTX["published"].append(f"sites:+{new_sites}")
    for label, target, stage, key, value_columns in (
        ("site_month", SITE_TARGET, SITE_STAGE, counts["site_key"], SITE_VALUE_COLUMNS),
        ("address_cell", ADDRESS_TARGET, ADDRESS_STAGE, "t.ADDRESS_ID = s.ADDRESS_ID", ADDRESS_VALUE_COLUMNS),
    ):
        c = counts[label]
        if c["inserts"] + c["updates"] + c["deletes"] == 0:
            continue
        changed = _changed_predicate(value_columns)
        spark.sql(f"""
            MERGE INTO {target} t USING {stage} s ON {key}
            WHEN MATCHED AND ({changed}) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)
        CTX["published"].append(f"{label}:+{c['inserts']}/~{c['updates']}/-{c['deletes']}")
    missing = _missing_tags()
    for table_name, column_name, risk, severity in missing:
        spark.sql(
            f"ALTER TABLE {table_name} ALTER COLUMN {column_name} "
            f"SET TAGS ('ig_risk' = '{risk}', 'ig_severity' = '{severity}')"
        )
    if missing:
        CTX["published"].append(f"tags:{len(missing)} columns")
    for table_name in (SITE_TARGET, ADDRESS_TARGET):
        if _schema_version(table_name) != AIR_QUALITY_SCHEMA_VERSION:
            spark.sql(
                f"ALTER TABLE {table_name} SET TBLPROPERTIES "
                f"('air_quality_schema_version' = '{AIR_QUALITY_SCHEMA_VERSION}')"
            )
            CTX["published"].append(f"property:{table_name.rsplit('.', 1)[-1]}")
# COMMAND ----------

def phase_verify(pending, publish, replace_predicate):
    CTX["phase"] = "verify"
    integrity = {
        "site_month_diff": _symmetric_difference(SITE_TARGET, SITE_STAGE, SITE_COLUMNS_ALL),
        "address_cell_diff": _symmetric_difference(ADDRESS_TARGET, ADDRESS_STAGE, ADDRESS_COLUMNS_ALL),
        "untagged_columns": len(_missing_tags()),
        "schema_versions": [_schema_version(t) for t in (SITE_TARGET, ADDRESS_TARGET)],
    }
    if publish:
        integrity["landing_diff"] = _symmetric_difference(
            f"(SELECT * FROM {LANDING} WHERE {replace_predicate}) AS landing_scope",
            f"(SELECT * FROM {LANDING_STAGE} WHERE {replace_predicate}) AS stage_scope",
            [f.name for f in spark.table(LANDING_STAGE).schema.fields],
        )
    CTX["details"]["integrity"] = integrity
    bad = {
        k: v for k, v in integrity.items()
        if (k == "schema_versions" and v != [AIR_QUALITY_SCHEMA_VERSION] * 2)
        or (k != "schema_versions" and v != 0)
    }
    assert not bad, f"air-quality post-publish integrity failed: {bad}"

    # Only now does the run count: hash state and bootstrap completion advance together.
    if pending:
        update = spark.createDataFrame(
            [(i["source"], i["year"], i["url"], i["sha256"], i["bytes"], i["applied_cutoff"],
              AIR_QUALITY_RULES_VERSION, len(i["rows"]), i["sites"], RUN_ID) for i in pending],
            "source STRING, year INT, url STRING, sha256 STRING, bytes BIGINT, applied_cutoff DATE, "
            "rules_version STRING, landed_rows BIGINT, landed_sites BIGINT, run_id STRING",
        ).withColumn("landed_at", F.current_timestamp())
        (
            DeltaTable.forName(spark, STATE).alias("t")
            .merge(update.alias("s"), "t.source = s.source AND t.year = s.year")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        )
        CTX["control_writes"].append(f"state:{len(pending)}")
    if not BOOTSTRAPPED:
        spark.createDataFrame(
            [("bootstrap_completed", CUTOFF.isoformat(), RUN_ID)],
            "control_key STRING, control_value STRING, run_id STRING",
        ).withColumn("updated_at", F.current_timestamp()).write.mode("append").saveAsTable(CONTROL)
        CTX["control_writes"].append("bootstrap_completed")


# COMMAND ----------

def freshness_alarm():
    """Alarm, not a gate: the data is verified and published; a silent upstream stall must be loud."""
    CTX["phase"] = "freshness"
    floor = _one(f"SELECT add_months(DATE'{CUTOFF}', -3) AS f")["f"]
    latest = {
        r_["source_network"]: r_["latest"]
        for r_ in spark.sql(f"""
            SELECT source_network, max(month_start) AS latest FROM {SITE_TARGET}
            WHERE source_network IN ('aurn', 'aqe', 'local') GROUP BY 1
        """).collect()
    }
    CTX["details"]["freshness"] = {"floor": str(floor), **{k: str(v) for k, v in latest.items()}}
    return [
        f"{n}_latest={latest.get(n)} < {floor}"
        for n in ("aurn", "aqe", "local")
        if latest.get(n) is None or latest[n] < floor
    ]
# COMMAND ----------

stale = []
try:
    pending, publish, replace_predicate = phase_fetch()
    new_sites, unresolved = phase_sites()
    counts = phase_bronze_stage()
    phase_validate(unresolved, counts)
    phase_publish(publish, replace_predicate, new_sites, counts)
    phase_verify(pending, publish, replace_predicate)
    stale = freshness_alarm()
except Exception as exc:
    error = f"{type(exc).__name__}: {exc}"[:2000] + "\n" + traceback.format_exc()[-6000:]
    try:
        _audit("FAILED", error)
    except Exception as audit_exc:
        print(f"could not write FAILED audit row: {audit_exc}")
    raise

status = "STALE_SOURCE" if stale else ("SUCCESS" if CTX["published"] else "NO_CHANGE")
CTX["phase"] = "done"
_audit(status, "; ".join(stale) or None)
assert not stale, f"air-quality source freshness alarm: {stale}"
dbutils.notebook.exit(bronze_json({"status": status, "published": CTX["published"], **CTX["details"]}))

