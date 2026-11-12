"""SQL builders for the gold release report (INSR W4). Pure Python: no Spark import, so pytest can check every
statement offline. Uploaded beside the notebook as a workspace FILE and imported by it.

Grains of the one-scan profile (the explicit form of GROUPING SETS ((m),(m,arm),())):
  gs 0 month, gs 1 month x arm, gs 2 whole table, gs 3 loaded month (row counts only, F1a).
Heavy aggregates are fed `CASE WHEN g.gs = 2`, so they run in the whole-table set only.
"""
from collections import namedtuple

GRAIN = {0: "month", 1: "month_arm", 2: "all", 3: "loaded_month"}
PCTS = (0.25, 0.5, 0.95, 0.99, 0.999)
PCT_NAMES = ("p25", "p50", "p95", "p99", "p999")
# A measure: SQL alias, metric name, column ('*' for table level), the grains it is reported at.
Measure = namedtuple("Measure", "alias metric column grains")
ALL = (2,)
SERIES = (0, 1, 2)


def q(name):
    """Backtick-quote one identifier."""
    return f"`{name}`"


def period_expr(entry):
    """The period of a row: the snapshot day for snapshot tables, else the first day of the primary month."""
    p = entry["primary_date"]
    if not p:
        return "CAST(NULL AS DATE)"
    if entry["snapshot"]:
        return f"CAST({q(p)} AS DATE)"
    return f"CAST(date_trunc('MONTH', CAST({q(p)} AS TIMESTAMP)) AS DATE)"


def grain_sets(entry):
    """Which grouping sets this table needs (no month without a primary date, no arm set without an arm)."""
    sets = [2]
    if entry["primary_date"]:
        sets.insert(0, 0)
        if entry["arm"]:
            sets.insert(1, 1)
    if entry["loaded"]:
        sets.append(3)
    return sets


def column_measures(c, i):
    """(aggregate SQL, Measure) pairs for one column; whole-table aggregates are guarded by g.gs = 2."""
    col, kind, base = q(c["name"]), c["kind"], f"c{i}"
    out = [(f"count(CASE WHEN g.gs <> 3 THEN {col} END)", Measure(f"{base}_nn", "nonnull", c["name"], SERIES))]

    def only_all(expr):
        """Keep an aggregate only on the whole-table grouping set (gs = 2); NULL on the month/arm/load sets."""
        return f"CASE WHEN g.gs = 2 THEN {expr} END"

    if kind == "numeric":
        out += [
            (f"count_if(g.gs = 2 AND {col} = 0)", Measure(f"{base}_zero", "zero", c["name"], ALL)),
            (only_all(f"approx_count_distinct(CASE WHEN g.gs = 2 THEN {col} END)"), Measure(f"{base}_nd", "ndistinct", c["name"], ALL)),
            (only_all(f"min(CASE WHEN g.gs = 2 THEN CAST({col} AS DOUBLE) END)"), Measure(f"{base}_min", "min", c["name"], ALL)),
            (only_all(f"max(CASE WHEN g.gs = 2 THEN CAST({col} AS DOUBLE) END)"), Measure(f"{base}_max", "max", c["name"], ALL)),
            (only_all(f"avg(CASE WHEN g.gs = 2 THEN CAST({col} AS DOUBLE) END)"), Measure(f"{base}_mean", "mean", c["name"], ALL)),
            (only_all(f"percentile_approx(CASE WHEN g.gs = 2 THEN CAST({col} AS DOUBLE) END, array({', '.join(map(str, PCTS))}), 1000)"),
             Measure(f"{base}_pct", "pct", c["name"], ALL)),
        ]
    elif kind == "string":
        out += [
            (f"count_if(g.gs = 2 AND TRIM({col}) = '')", Measure(f"{base}_blank", "blank", c["name"], ALL)),
            (only_all(f"approx_count_distinct(CASE WHEN g.gs = 2 THEN {col} END)"), Measure(f"{base}_nd", "ndistinct", c["name"], ALL)),
            (f"count_if(g.gs = 2 AND try_cast({col} AS DOUBLE) IS NOT NULL)", Measure(f"{base}_num", "numeric_share", c["name"], ALL)),
        ]
        if c["name"].endswith("_id"):
            out.append((f"count_if(g.gs = 2 AND {col} IS NOT NULL AND try_cast({col} AS BIGINT) IS NULL)",
                        Measure(f"{base}_idfail", "id_trycast_fail", c["name"], ALL)))
    elif kind == "date":
        out += [
            (only_all(f"min(CASE WHEN g.gs = 2 THEN unix_date(CAST({col} AS DATE)) END)"), Measure(f"{base}_min", "min", c["name"], ALL)),
            (only_all(f"max(CASE WHEN g.gs = 2 THEN unix_date(CAST({col} AS DATE)) END)"), Measure(f"{base}_max", "max", c["name"], ALL)),
            (only_all(f"approx_count_distinct(CASE WHEN g.gs = 2 THEN CAST({col} AS DATE) END)"), Measure(f"{base}_nd", "ndistinct", c["name"], ALL)),
        ]
    elif kind == "bool":
        out.append((f"count_if(g.gs = 2 AND {col})", Measure(f"{base}_true", "true", c["name"], ALL)))
    return out


def profile_sql(entry, source):
    """(SQL, measures): the one-scan profile of one table. `source` is the table (or sample table) to read."""
    sets = grain_sets(entry)
    arm = f"CAST({q(entry['arm'])} AS STRING)" if entry["arm"] else "CAST(NULL AS STRING)"
    loaded = f"CAST(date_trunc('MONTH', CAST({q(entry['loaded'])} AS TIMESTAMP)) AS DATE)" if entry["loaded"] else "CAST(NULL AS DATE)"
    cols = ", ".join(q(c["name"]) for c in entry["columns"])
    aggs = [("count(*)", Measure("m_rows", "rows", "*", (0, 1, 2, 3)))]
    for i, c in enumerate(entry["columns"]):
        aggs += column_measures(c, i)
    for j, p in enumerate(entry["pairs"]):
        aggs.append((f"count_if(g.gs <> 3 AND ({p['sql']}))", Measure(f"pair{j}", "pair:" + p["name"], "*", SERIES)))
    select = ",\n  ".join(f"{sql} AS {m.alias}" for sql, m in aggs)
    sql = (f"WITH src AS (SELECT {period_expr(entry)} AS _p, {arm} AS _arm, {loaded} AS _lm, {cols} FROM {source})\n"
           f"SELECT g.gs AS gs,\n"
           f"  CASE WHEN g.gs IN (0, 1) THEN _p WHEN g.gs = 3 THEN _lm END AS period,\n"
           f"  CASE WHEN g.gs = 1 THEN coalesce(_arm, '(null)') END AS arm,\n  {select}\n"
           f" FROM src LATERAL VIEW explode(array({', '.join(map(str, sets))})) g AS gs\n"
           f"GROUP BY 1, 2, 3")
    return sql, [m for _, m in aggs]


def long_rows(release_id, entry, rows, measures):
    """Unpivot profile result rows into (release_id, table_fqn, grain, period, arm, metric, column, value)."""
    out = []
    for r in rows:
        gs = r["gs"]
        for m in measures:
            if gs not in m.grains:
                continue
            value = r[m.alias]
            if value is None:
                continue
            if m.metric == "pct":
                out += [(release_id, entry["fqn"], GRAIN[gs], r["period"], r["arm"], name, m.column, float(v))
                        for name, v in zip(PCT_NAMES, value) if v is not None]
            else:
                out.append((release_id, entry["fqn"], GRAIN[gs], r["period"], r["arm"], m.metric, m.column, float(value)))
    return out


PROFILE_MONTH_SCHEMA = ("release_id string, table_fqn string, grain string, period date, arm string, metric string, "
                        "column_name string, value double, stage string")
COLUMN_PROFILE_SCHEMA = ("release_id string, table_fqn string, column_name string, data_type string, "
                         "column_class string, metric string, value double, stage string, profiled_at timestamp")
FINDING_SCHEMA = ("release_id string, table_fqn string, column_name string, family string, finding_code string, "
                  "severity string, period date, arm string, observed double, expected double, detail string")


def ddl(schema):
    """CREATE TABLE IF NOT EXISTS for the three report tables (Delta, CDF, row tracking, DV, commented)."""
    props = ("TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.enableRowTracking = true, "
             "delta.enableDeletionVectors = true)")
    return [
        f"""CREATE TABLE IF NOT EXISTS {schema}.release_profile_month (
  release_id STRING NOT NULL COMMENT 'Release the profile belongs to (the release report release_id widget).',
  table_fqn STRING NOT NULL COMMENT 'Profiled published table, three-part backticked name.',
  grain STRING COMMENT 'month, month_arm, loaded_month (row counts by load month), col_month (per date column), day_heap.',
  period DATE COMMENT 'First day of the month (snapshot tables: the snapshot day; day_heap: the heaped day).',
  arm STRING COMMENT 'Value of the table arm column (source_feed, source_object, stream ...) for grain month_arm.',
  metric STRING COMMENT 'rows, nonnull, pair:<name>, median_day, ratio_to_median.',
  column_name STRING COMMENT 'Column the metric describes; * for table-level metrics.',
  value DOUBLE COMMENT 'Metric value.',
  stage STRING COMMENT 'Report stage that wrote the row (profile, dates); rows are replaced per release, table and stage.')
USING DELTA CLUSTER BY (release_id, table_fqn)
COMMENT 'INSR W4 gold release report: monthly profile series per published table and release (advisory QC input).'
{props}""",
        f"""CREATE TABLE IF NOT EXISTS {schema}.release_column_profile (
  release_id STRING NOT NULL COMMENT 'Release the profile belongs to.',
  table_fqn STRING NOT NULL COMMENT 'Profiled published table.',
  column_name STRING COMMENT 'Column, * for table level, or the check name for statement checks.',
  data_type STRING COMMENT 'information_schema data type of the column (NULL for checks).',
  column_class STRING COMMENT 'Column kind or date class; check for statement checks; run for timings.',
  metric STRING COMMENT 'rows, nonnull, zero, blank, ndistinct, min, max, mean, p25..p999, numeric_share, id_trycast_fail, true, pair:<name>, check metrics, elapsed_s.',
  value DOUBLE COMMENT 'Metric value (dates as days since 1970-01-01).',
  stage STRING COMMENT 'Report stage that wrote the row.',
  profiled_at TIMESTAMP COMMENT 'Start time of the report run that wrote the row; previous_release() orders releases by it.')
USING DELTA CLUSTER BY (release_id, table_fqn)
COMMENT 'INSR W4 gold release report: whole-table column profile per release; the fill rate the browser badge reads (J11).'
{props}""",
        f"""CREATE TABLE IF NOT EXISTS {schema}.release_finding (
  release_id STRING NOT NULL COMMENT 'Release the finding belongs to.',
  table_fqn STRING COMMENT 'Table the finding is about.',
  column_name STRING COMMENT 'Column, or * for table level.',
  family STRING COMMENT 'Rule family F1..F24 or one-off L-reference.',
  finding_code STRING COMMENT 'Detector code, e.g. F1_drop, F2_complete_through, F13_duplicate_keys.',
  severity STRING COMMENT 'info, warn or gate_candidate. Advisory only: nothing blocks publication.',
  period DATE COMMENT 'Month or day the finding refers to, when it has one.',
  arm STRING COMMENT 'Arm value the finding refers to, when it has one.',
  observed DOUBLE COMMENT 'Observed value (ratio, count, share).',
  expected DOUBLE COMMENT 'Baseline or threshold it was compared with.',
  detail STRING COMMENT 'JSON detail, including known_gap annotations from the INSR ledger.')
USING DELTA CLUSTER BY (release_id, table_fqn)
COMMENT 'INSR W4 gold release report: advisory findings per release (info / warn / gate_candidate).'
{props}""",
    ]


# Person-less tables copied whole into the sample (FK parents and small hubs), so F12 parents are complete. Every HUB_KEYS table is here (none has person_id), so the F7(a) hub
# id-0 check sees every id-0 row in the sample: practitioner_group 251,552, practitioner_identifier 521,829,
# research_study 1,084 and pregnancy_reconciliation 6 rows (prod 2026-10-30). tests/test_detectors_keys.py
# asserts every HUB_KEYS table is in WHOLE_COPY.
WHOLE_COPY = ("reference_practitioner", "reference_practitioner_group", "reference_practitioner_identifier",
              "reference_research_study", "reference_pregnancy_reconciliation",
              "reference_organization", "reference_location", "reference_service",
              "care_site", "location", "provider", "reference_concept_map", "reference_value_set",
              "unit_cost_reference", "drug_of_interest", "analyte", "outcome_code")


def sample_table(entry, sample_schema):
    """Name of the sample copy of one published table."""
    return f"{sample_schema}.{entry['schema']}__{entry['table']}"


# Person-less tables with no key column whose size rules out a whole copy (1.9-2.9 billion rows for the order
# detail, fact_relationship and concept provenance; 30-150 million for the others). They are cut by the entity
# stratum on the parent id they hang off, so every row of one sampled parent is kept together.
SAMPLE_ENTITY_COLUMN = {
    "clinical_appointment_booking": "sch_event_id",
    "clinical_medication_admin_ingredient": "event_id",
    "clinical_medication_order_attribute": "order_id",
    "clinical_medication_order_detail": "order_id",
    "clinical_medication_order_ingredient": "order_id",
    "fact_relationship": "fact_id_1",
    "omop_concept_provenance": "cdm_row_id",
    "episode_event": "episode_id",
}


def stage_sample_sql(entry, cohort, sample_schema, entity_n):
    """CTAS of one sample table: person tables by cohort semi-join plus the NULL-person entity stratum (by key);
    small hubs whole; other person-less tables by the entity stratum (key, else SAMPLE_ENTITY_COLUMN); small
    key-less tables whole."""
    src, target = entry["fqn"], sample_table(entry, sample_schema)
    key = entry["key"][0] if entry["key"] else next((c["name"] for c in entry["columns"] if c["name"].endswith("_key")), None)
    if key is None and not entry["person_col"]:
        key = SAMPLE_ENTITY_COLUMN.get(entry["table"])
    if entry["person_col"]:
        stratum = (f" UNION ALL SELECT * FROM {src} WHERE `{entry['person_col']}` IS NULL"
                   f" AND pmod(xxhash64(CAST(`{key}` AS STRING)), {entity_n}) = 0") if key else ""
        body = (f"SELECT s.* FROM {src} s LEFT SEMI JOIN {cohort} c "
                f"ON CAST(s.`{entry['person_col']}` AS BIGINT) = c.person_id{stratum}")
    elif entry["table"] in WHOLE_COPY or key is None:
        body = f"SELECT * FROM {src}"
    else:
        body = f"SELECT * FROM {src} WHERE pmod(xxhash64(CAST(`{key}` AS STRING)), {entity_n}) = 0"
    return f"CREATE OR REPLACE TABLE {target} AS {body}"


def oneoff_sqls(source_of, registry, boundary_ts, mode):
    """[(check name, sql)] for the W4.02 one-offs. `source_of(fqn)` maps a published table to what this run reads.
    L2905 compares silver with gold, so it runs in full mode only (silver is not sampled).
    `registry` is this run's table set: a check whose table is not in it (an `only=` re-run) is left out, so a
    narrowed sample run never reads a sample copy it did not stage."""
    report_fqn = "`4_prod`.gold.clinical_pathology_report"
    orders_fqn = "`4_prod`.gold.clinical_medication_order"
    neonatal_fqn = "`4_prod`.gold.clinical_neonatal_examination"
    report = source_of(report_fqn) if report_fqn in registry else None
    orders = source_of(orders_fqn) if orders_fqn in registry else None
    out = [
        ("L4552", f"""SELECT CAST(date_trunc('MONTH', event_datetime) AS DATE) AS period,
                  percentile_approx((unix_timestamp(issued_datetime) - unix_timestamp(event_datetime)) / 60.0, 0.5, 1000) AS median_min
           FROM {report} WHERE issued_datetime IS NOT NULL AND event_datetime IS NOT NULL GROUP BY 1"""),
        ("L808", f"""SELECT coalesce(source_feed, '(null)') AS arm, CAST(event_datetime AS DATE) AS day, count(*) AS n
           FROM {orders}
           WHERE event_datetime >= TIMESTAMP'{boundary_ts}' - INTERVAL 400 DAYS AND event_datetime < TIMESTAMP'{boundary_ts}'
           GROUP BY 1, 2"""),
    ]
    out = [(name, sql) for name, sql in out if (report if name == "L4552" else orders) is not None]
    if mode == "full" and neonatal_fqn in registry:
        out.append(("L2905", """SELECT (SELECT count(*) FROM `4_prod`.silver.clinical_neonatal_examination) AS silver_rows,
                  (SELECT count(*) FROM `4_prod`.gold.clinical_neonatal_examination) AS gold_rows"""))
    return out + sct_release_sqls(source_of, registry) + bbr_oneoff_sqls(source_of, registry)


def sct_columns(entry):
    """The SNOMED code columns of one registry table: every *_snomed_code, and every *_target_code whose table has
    the matching *_target_system (the columns W4's sct_concept_only rules cover)."""
    names = {c["name"] for c in entry.get("columns") or []}
    out = []
    for name in sorted(names):
        if name.endswith("_snomed_code"):
            out.append((name, None))
        elif name.endswith("_target_code") and name[: -len("_code")] + "_system" in names:
            out.append((name, name[: -len("_code")] + "_system"))
    return out


def sct_release_sqls(source_of, registry):
    """[("SCT_release:<fqn>", sql)] SCT_TRUD_V1 advisory: per SNOMED column, rows whose value is a concept-partition
    SCTID that neither the loaded RF2 release nor the OMOP vocabulary knows (newer concepts; kept by design). One scan
    per table; the known-concept set is semi-joined to the table's own codes first."""
    out = []
    for fqn, entry in sorted(registry.items()):
        columns = sct_columns(entry)
        if not columns:
            continue
        source = source_of(fqn)
        arms = []
        for column, system in columns:
            c = f"`{column}`"
            part = f"substring({c}, length({c}) - 2, 2)"
            scope = f" AND `{system}` = 'http://snomed.info/sct'" if system else ""
            arms.append(f"SELECT '{column}' AS column_name, {c} AS code FROM {source} "
                        f"WHERE {c} RLIKE '^[1-9][0-9]{{5,17}}$' AND {part} IN ('00', '10')"
                        f" AND NOT ({part} = '10' AND length({c}) < 11){scope}")
        out.append((f"SCT_release:{fqn}", f"""
            WITH v AS ({" UNION ALL ".join(arms)}),
            codes AS (SELECT DISTINCT code FROM v),
            known AS (SELECT CAST(CUI AS STRING) AS code FROM `3_lookup`.trud.snomed_sct
                      WHERE CAST(CUI AS STRING) IN (SELECT code FROM codes)
                      UNION SELECT concept_code FROM `3_lookup`.omop.concept
                      WHERE vocabulary_id IN ('SNOMED', 'dm+d') AND concept_code IN (SELECT code FROM codes))
            SELECT column_name, count(*) AS n, count(DISTINCT code) AS codes
            FROM v LEFT ANTI JOIN known USING (code) GROUP BY column_name"""))
    return out


# ---------------------------------------------------------------------------------------------------------------
# W4.03: per-date-column day counts (F3 discovery, F19 bulk rewrite) and unit groups (F10)
# ---------------------------------------------------------------------------------------------------------------
import statistics as _statistics

DAY_HEAP_CLASSES = ("event", "schedule", "effective", "birth", "death", "expiry")
# Load-clock columns the F19 bulk-rewrite detector reads; always day-counted whatever their class.
# release_detectors.BULK_COLUMNS must equal this tuple (asserted in tests/test_detectors_shape.py).
BULK_COLUMNS = ("record_status_effective_from", "source_update_timestamp")


def dates_day_sql(entry, source, classes=DAY_HEAP_CLASSES):
    """One scan: (column_name, day, n) for every date column of the given classes, else None.

    Class `load` is always excluded (no load-day heap warnings, no extra scan), and BULK_COLUMNS are always
    included whatever their class, so the F19 bulk-rewrite input is the same on curated and uncurated tables.
    """
    cols = [c["name"] for c in entry["columns"] if c["kind"] == "date"
            and ((c["date_class"] in classes and c["date_class"] != "load") or c["name"] in BULK_COLUMNS)]
    if not cols:
        return None
    pairs = ", ".join(f"'{c}', CAST({q(c)} AS DATE)" for c in cols)
    return (f"SELECT column_name, day, count(*) AS n FROM (SELECT stack({len(cols)}, {pairs}) AS (column_name, day) "
            f"FROM {source}) WHERE day IS NOT NULL GROUP BY 1, 2")


def day_rows(release_id, entry, days, min_rows=50, heap_x=5.0, heap_share=0.01):
    """Reduce day counts to long profile rows: col_month rows + median_day per (column, month), and day_heap
    rows (rows + ratio_to_median) for days with >= min_rows that exceed heap_x x the month's median day or hold
    more than heap_share of the column's dated rows."""
    by_month, totals = {}, {}
    for d in days:
        key = (d["column_name"], date_month(d["day"]))
        by_month.setdefault(key, []).append(d["n"])
        totals[d["column_name"]] = totals.get(d["column_name"], 0) + d["n"]
    out = []
    medians = {}
    for (column, period), counts in sorted(by_month.items()):
        medians[(column, period)] = _statistics.median(counts)
        out.append((release_id, entry["fqn"], "col_month", period, None, "rows", column, float(sum(counts))))
        out.append((release_id, entry["fqn"], "col_month", period, None, "median_day", column, float(medians[(column, period)])))
    for d in days:
        median = medians[(d["column_name"], date_month(d["day"]))]
        if d["n"] >= min_rows and (d["n"] > heap_x * median or d["n"] > heap_share * totals[d["column_name"]]):
            out.append((release_id, entry["fqn"], "day_heap", d["day"], None, "rows", d["column_name"], float(d["n"])))
            out.append((release_id, entry["fqn"], "day_heap", d["day"], None, "ratio_to_median", d["column_name"],
                        float(d["n"] / median)))
    return out


def date_month(d):
    """First day of d's month."""
    return d.replace(day=1)


# (table, group column, unit column or None, value column): per-group unit and scale checks (F10; L439, L3029,
# L3775, L4529, L2763). Groups with fewer than 100 valued rows are skipped.
UNIT_GROUPS = [
    ("`4_prod`.gold.clinical_pathology_result", "test_source_code", "unit_source_value", "value_number"),
    ("`4_prod`.gold.clinical_vital_sign", "vital_source_code", "unit_source_value", "value_number"),
    ("`4_prod`.gold.clinical_transfusion", "source_code", None, "quantity_value"),
]


def units_sql(group, source):
    """One grouped statement per UNIT_GROUPS row."""
    _, g, u, v = group
    unit_count = f"count(DISTINCT {q(u)})" if u else "CAST(NULL AS BIGINT)"
    unit_null = f"count_if({q(u)} IS NULL)" if u else "CAST(NULL AS BIGINT)"
    return (f"SELECT CAST({q(g)} AS STRING) AS grp, count(*) AS n, {unit_count} AS n_units, {unit_null} AS unit_null, "
            f"percentile_approx(CAST({q(v)} AS DOUBLE), array(0.25, 0.95), 1000) AS pct, "
            f"count_if({q(v)} <= 5) AS le5, count_if({q(v)} >= 100) AS ge100 "
            f"FROM {source} WHERE {q(v)} IS NOT NULL GROUP BY 1 HAVING count(*) >= 100")


# ---------------------------------------------------------------------------------------------------------------
# W4.04: keys (F13, F7a), FK orphans and parent coverage (F12), person / scope / guard checks (F24, J8, BI, MSDS, L5172)
# ---------------------------------------------------------------------------------------------------------------

# Hub tables whose own key must never be 0 (P4f_hub_id0: 5 gold hubs + the OMOP provider; W2.01/W5.04 remove them).
HUB_KEYS = {
    "`4_prod`.gold.reference_practitioner": "practitioner_id",
    "`4_prod`.gold.reference_practitioner_group": "practitioner_id",
    "`4_prod`.gold.reference_practitioner_identifier": "practitioner_id",
    "`4_prod`.gold.reference_research_study": "source_protocol_id",
    "`4_prod`.gold.reference_pregnancy_reconciliation": "pregnancy_id",
    "`4_prod`.omop_gold.provider": "provider_id",
}
# (child, fk, parent, parent key). Estate 2026-10-29 (child rows / matched): cycle 316,572/223,672;
# gene_tested 1,459,928/1,117,752 (342,176 orphans = P9_L2204g); elective access procedure 2,961,535/2,889,619;
# cost_line_item 181,295,498/178,510,401; cc activity 328,215/255,979 (22% parent NULL, P7_L2489);
# community activity 15,523,641/14,191,689 (rest NULL); coronary lesion and surgery follow-up 100%.
FK_PAIRS = [
    ("`4_prod`.gold.clinical_cancer_treatment_cycle", "iqemo_course_id", "`4_prod`.gold.clinical_cancer_treatment", "iqemo_course_id"),
    ("`4_prod`.gold.reference_gene_tested", "genomic_test_key", "`4_prod`.gold.clinical_genomic_test", "patient_event_key"),
    ("`4_prod`.gold.reference_elective_access_procedure", "elective_access_entry_key", "`4_prod`.gold.clinical_elective_access_entry", "patient_event_key"),
    ("`4_prod`.gold.reference_cost_line_item", "costed_activity_key", "`4_prod`.gold.clinical_costed_activity", "patient_event_key"),
    ("`4_prod`.gold.clinical_critical_care_activity", "parent_period_id", "`4_prod`.gold.clinical_critical_care_period", "crit_care_period_id"),
    ("`4_prod`.gold.clinical_community_care_activity", "community_contact_key", "`4_prod`.gold.clinical_community_care_contact", "patient_event_key"),
    ("`4_prod`.gold.clinical_registry_coronary_lesion", "parent_entry_id", "`4_prod`.gold.clinical_registry_coronary_procedure", "entry_id"),
    ("`4_prod`.gold.clinical_registry_surgery_followup", "entry_id", "`4_prod`.gold.clinical_registry_surgery_episode", "entry_id"),
    ("`4_prod`.gold.spine_care_participation", "practitioner_id", "`4_prod`.gold.reference_practitioner", "practitioner_id"),
    ("`4_prod`.gold.clinical_condition", "asserter_practitioner_id", "`4_prod`.gold.reference_practitioner", "practitioner_id"),
    ("`4_prod`.gold.clinical_procedure", "performer_practitioner_id", "`4_prod`.gold.reference_practitioner", "practitioner_id"),
]


def keys_sql(entry, source):
    """F13 count/distinct/null on the declared key plus the F7(a) hub id-0 count; None for keyless non-hubs."""
    key, hub = entry["key"], HUB_KEYS.get(entry["fqn"])
    if not key and not hub:
        return None
    parts = ["count(*) AS n"]
    if key:
        parts += [f"count(DISTINCT {', '.join(q(k) for k in key)}) AS n_distinct",
                  "count_if(" + " OR ".join(f"{q(k)} IS NULL" for k in key) + ") AS n_null_key"]
    else:
        parts += ["CAST(NULL AS BIGINT) AS n_distinct", "CAST(NULL AS BIGINT) AS n_null_key"]
    parts.append(f"count_if(CAST({q(hub)} AS STRING) = '0') AS hub_zero" if hub else "CAST(NULL AS BIGINT) AS hub_zero")
    return f"SELECT {', '.join(parts)} FROM {source}"


def fk_sql(pair, child_source, parent_source):
    """F12 on one FK: rows, non-null, zero (counted as orphan by the detector) and true orphans (key-only)."""
    _, fk, _, pk = pair
    return (f"SELECT count(*) AS n, count(c.fk) AS n_nonnull, count_if(c.fk = '0') AS n_zero, "
            f"count_if(c.fk IS NOT NULL AND c.fk <> '0' AND p.k IS NULL) AS n_orphan "
            f"FROM (SELECT CAST({q(fk)} AS STRING) AS fk FROM {child_source}) c "
            f"LEFT JOIN (SELECT DISTINCT CAST({q(pk)} AS STRING) AS k FROM {parent_source}) p ON c.fk = p.k")


def gene_coverage_sql(test_source, gene_source):
    """F12 parent coverage: genomic tests with at least one gene_tested row, per assay family (L3797)."""
    return (f"SELECT coalesce(t.assay_code, '(null)') AS family, count(*) AS parents, count_if(g.k IS NOT NULL) AS with_child "
            f"FROM {test_source} t LEFT JOIN (SELECT DISTINCT genomic_test_key AS k FROM {gene_source}) g "
            f"ON t.patient_event_key = g.k GROUP BY 1")


TEST_BUCKET_SQL = """CASE WHEN upper(p.NAME_LAST_KEY) IN ('TEST', 'TESTPATIENT', 'TESTING') THEN 'surname_test_exact'
         WHEN upper(p.NAME_LAST_KEY) RLIKE '^ZZ' THEN 'surname_zz_prefix'
         WHEN upper(p.NAME_LAST_KEY) RLIKE '^XX' THEN 'surname_xx_prefix'
         WHEN upper(p.NAME_LAST_KEY) RLIKE '^TEST[0-9]|TESTPATIENT|^DUMMY|^FICTITIOUS|^TRAINING' THEN 'surname_test_variant'
         WHEN upper(p.NAME_LAST_KEY) RLIKE '^TEST' THEN 'surname_test_prefix_other'
         WHEN upper(p.NAME_FIRST_KEY) RLIKE '^(TEST|DUMMY)' THEN 'forename_test_dummy'
         ELSE 'none' END"""   # W0_P1_person_scope.py P1c rule, verbatim (labels only, never names)


def person_sqls(source_of, boundary_ts, scope_available, tables=None):
    """[(check name, sql)] for F24, J8, F19 CC death, BlobIdentity, the MSDS guard and L5172. `source_of(fqn)` maps
    a published table to what this run reads; scope_available says whether silver reference_person_scope (W2.09)
    exists with in_scope and test_patient_ind.

    `tables` (a set of published fqns, None = all) limits the checks to this run's tables: a check is kept only
    when every gold table it reads is in the set, and L5172 keeps one UNION branch per available table. So an
    `only=` re-run neither reads sample copies it never staged nor re-emits findings for tables it does not own."""

    def g(t):
        """What this run reads for gold table `t` (the sample copy in sample mode)."""
        return source_of(f"`4_prod`.gold.{t}")

    def available(*names):
        """True when every named gold table is in this run's tables (always True without a table list)."""
        return tables is None or all(f"`4_prod`.gold.{t}" in tables for t in names)

    # Gold tables each check reads (spine_person is read through `spine`).
    reads = {"F24_person": ("spine_person",), "F24_implied_age": ("clinical_safety_incident_participant",),
             "F24_after_death": ("reference_person_death_evidence",),
             "F19_cc_death": ("clinical_critical_care_admission", "spine_person"),
             "BI_text_null_person": ("text_document",),
             "F12_labour_baby_ratio": ("clinical_labour_delivery", "clinical_baby_delivery"),
             "F24_test": ("spine_person",), "J8_scope": ("spine_person",), "J8_scope_reasons": ("spine_person",)}
    spine = g("spine_person") if available("spine_person") else None
    out = [
        ("F24_person", f"SELECT count(*) AS persons, count_if(birth_datetime < TIMESTAMP'{boundary_ts}' - INTERVAL 116 YEARS "
                       f"AND deceased_datetime IS NULL) AS age_over_116_alive FROM {spine}"),
        ("F24_implied_age", f"SELECT count(*) AS n, count_if(CAST(link_age AS DOUBLE) > 115) AS over_115 "
                            f"FROM {g('clinical_safety_incident_participant')}"),
        ("F24_after_death", f"SELECT count(*) AS n, count_if(last_known_activity_datetime > deceased_datetime + INTERVAL 30 DAYS) "
                            f"AS after_death_30d FROM {g('reference_person_death_evidence')}"),
        ("F19_cc_death", f"""SELECT count(*) AS n,
              count_if(c.date_of_death IS NOT NULL AND s.deceased_datetime IS NOT NULL
                       AND abs(datediff(CAST(c.date_of_death AS DATE), CAST(s.deceased_datetime AS DATE))) > 1) AS disagree,
              count_if(c.date_of_death IS NULL AND s.deceased_datetime BETWEEN c.event_datetime AND c.event_datetime + INTERVAL 30 DAYS) AS cc_missing_death
            FROM {g('clinical_critical_care_admission')} c LEFT JOIN {spine} s ON c.person_id = s.person_id"""),
        ("BI_text_null_person", f"SELECT coalesce(source_feed, '(null)') AS feed, count(*) AS n, count_if(person_id IS NULL) AS null_person "
                                f"FROM {g('text_document')} WHERE is_latest_version GROUP BY 1"),
        # F12 proxy only (not the MSDS guard): Millennium labour vs baby delivery rows per month.
        ("F12_labour_baby_ratio", f"""SELECT 'labour' AS kind, CAST(date_trunc('MONTH', event_datetime) AS DATE) AS period, count(*) AS n
            FROM {g('clinical_labour_delivery')} GROUP BY 1, 2
            UNION ALL SELECT 'baby', CAST(date_trunc('MONTH', event_datetime) AS DATE), count(*) FROM {g('clinical_baby_delivery')} GROUP BY 1, 2"""),
    ]
    # L5172: persons with more than one open interval, one UNION branch per table (label, table, open predicate, grain).
    open_branches = [
        ("gp_registration", "reference_person_gp_registration", "registration_end IS NULL", "person_id"),
        ("person_address", "reference_person_address", "(end_effective IS NULL OR open_ended_ind)", "person_id, address_type_code"),
        ("episode", "spine_episode", "period_end IS NULL", "person_id, episode_type_code"),
    ]
    branches = [f"""SELECT '{label}' AS tbl, count(*) AS persons_multi_open FROM (
              SELECT person_id FROM {g(t)} WHERE {open_pred} AND person_id IS NOT NULL
              GROUP BY {grain} HAVING count(*) > 1)""" for label, t, open_pred, grain in open_branches if available(t)]
    if branches:
        out.append(("L5172_open_intervals", "\n            UNION ALL ".join(branches)))
    if scope_available:
        out += [
            ("F24_test", f"SELECT count_if(s.test_patient_ind) AS test_flagged, 'silver_person_scope' AS basis "
                         f"FROM `4_prod`.silver.reference_person_scope s LEFT SEMI JOIN {spine} g ON s.person_id = g.person_id"),
            ("J8_scope", f"SELECT count(*) AS gold_persons, count_if(s.person_id IS NULL) AS not_in_silver_scope, "
                         f"count_if(NOT s.in_scope) AS out_of_scope FROM {spine} g "
                         f"LEFT JOIN `4_prod`.silver.reference_person_scope s ON g.person_id = s.person_id"),
            ("J8_scope_reasons", "SELECT in_scope, has_encounter, has_clinical_data, count(*) AS persons "
                                 "FROM `4_prod`.silver.reference_person_scope GROUP BY 1, 2, 3"),
        ]
    else:
        out.append(("F24_test", f"""SELECT count(DISTINCT p.PERSON_ID) AS test_flagged, 'raw_name_rule' AS basis
            FROM `4_prod`.raw.mill_person p LEFT SEMI JOIN {spine} g ON CAST(p.PERSON_ID AS BIGINT) = g.person_id
            LEFT JOIN `3_lookup`.mill.mill_code_value cv ON cv.CODE_VALUE = p.PERSON_TYPE_CD
            WHERE upper(cv.DISPLAY) = 'PERSON' AND {TEST_BUCKET_SQL} <> 'none'"""))
    return [(name, sql) for name, sql in out if name not in reads or available(*reads[name])]


def fact_person_sql(fqn, source, spine_source):
    """J8: distinct persons of one fact table, and how many are missing from gold spine_person (key-only)."""
    return (f"SELECT '{fqn}' AS fqn, count(*) AS persons, count_if(s.person_id IS NULL) AS not_in_spine "
            f"FROM (SELECT DISTINCT person_id FROM {source} WHERE person_id IS NOT NULL) f "
            f"LEFT JOIN (SELECT person_id FROM {spine_source}) s ON f.person_id = s.person_id")


# ---------------------------------------------------------------------------------------------------------------
# W4.07 F16(c) residual: latest invalidate/normalize transforms re-evaluated on what this run reads
# ---------------------------------------------------------------------------------------------------------------

def residual_sql(fqn, source, rules):
    """F16(c): one statement counting, per latest invalidate/normalize rule, published rows that the rule's
    transform would still change (residual). `rules` are (rule_id, column, transform-with-{col}) for this table."""
    parts = [f"count_if(NOT (({transform.replace('{col}', q(column))}) <=> {q(column)})) AS r{i}"
             for i, (_, column, transform) in enumerate(rules)]
    return f"SELECT {', '.join(parts)} FROM {source}" if parts else None


# BBR_FOLDIN_V1 (design bbr_review/foldin_design.md 0.4): cross-row and join advisories of the BBR fold-in. Each is
# info or warn only, never a gate (release_detectors.bbr_advisories). Some read columns this release adds to gold
# (order_role, category_code, religion_snomed_code ...), which the 10-29 registry export predates, so the report runs
# them tolerantly (a "BBR:" check that fails analysis is recorded as an error row, not a stage failure).
BBR_ONEOFF_TABLES = {
    "condition_std_snomed_no_omop": "`4_prod`.gold.clinical_condition",
    "condition_priority_collision": "`4_prod`.gold.clinical_condition",
    "path_concept_multi_dimension": "`4_prod`.gold.clinical_pathology_result",
    "med_instance_orphan": "`4_prod`.gold.clinical_medication_order",
    "med_category_share": "`4_prod`.gold.clinical_medication_order",
    "religion_unmapped_codes": "`4_prod`.gold.spine_person",
}


def bbr_oneoff_body(code, src):
    """The SQL of one BBR advisory over `src` (what this run reads for the table). Every query returns one row with
    an `n` (the count the detector reports) and, where useful, a denominator."""
    if code == "condition_std_snomed_no_omop":     # BBR-1: a standard SNOMED concept with no OMOP concept id
        return f"""SELECT count(*) AS n FROM {src} c
            JOIN (SELECT concept_code FROM `3_lookup`.omop.concept WHERE vocabulary_id = 'SNOMED'
                  AND standard_concept = 'S' AND invalid_reason IS NULL) s ON c.condition_snomed_code = s.concept_code
            WHERE c.condition_omop_concept_id IS NULL"""
    if code == "condition_priority_collision":     # BBR-2: one priority held by two diagnoses in a slice
        return f"""SELECT count(*) AS n FROM (SELECT encounter_slice_id, diag_priority FROM {src}
            WHERE diag_priority IS NOT NULL AND diagnosis_id IS NOT NULL GROUP BY 1, 2
            HAVING count(DISTINCT diagnosis_id) > 1)"""
    if code == "path_concept_multi_dimension":     # BBR-12: one test concept, canonical units of several dimensions
        return f"""SELECT count(*) AS n FROM (SELECT test_snomed_code FROM {src}
            WHERE test_snomed_code IS NOT NULL AND canonical_ucum_code IS NOT NULL
              AND coalesce(canonical_conversion, '') <> 'not_convertible_dimension'
            GROUP BY 1 HAVING count(DISTINCT canonical_ucum_code) > 1)"""
    if code == "med_instance_orphan":              # BBR-13: an instance whose parent order is not in gold
        return f"""SELECT count(*) AS n FROM {src} i
            LEFT ANTI JOIN (SELECT DISTINCT order_id FROM {src}) p ON i.template_order_id = p.order_id
            WHERE i.order_role = 'instance'"""
    if code == "med_category_share":               # BBR-14: discharge/home medication present from 2021-09
        return f"""SELECT count_if(category_code <> 'inpatient') AS n, count(*) AS total FROM {src}
            WHERE authored_datetime >= TIMESTAMP'2021-09-01 00:00:00'"""
    if code == "religion_unmapped_codes":          # BBR-16: a religion code the crosswalk has no row for
        return f"""SELECT count(DISTINCT p.religion_code) AS n FROM {src} p
            LEFT ANTI JOIN `3_lookup`.omop.religion_snomed_map m ON p.religion_code = m.source_code
            WHERE p.religion_code IS NOT NULL"""
    raise ValueError(code)


def bbr_oneoff_sqls(source_of, registry):
    """[("BBR:<code>:<fqn>", sql)] for every BBR advisory whose table is in this run."""
    return [(f"BBR:{code}:{fqn}", bbr_oneoff_body(code, source_of(fqn)))
            for code, fqn in sorted(BBR_ONEOFF_TABLES.items()) if fqn in registry]
