# Databricks notebook source
# MAGIC %md
# MAGIC # Gold release report (INSR_20261029, workstream W4)
# MAGIC One profile pass per published table (gold, omop_gold, pms_device, pms_pharma, rbi), persisted per release,
# MAGIC then driver-side detectors write advisory findings (info / warn / gate_candidate). Nothing blocks publication.
# MAGIC
# MAGIC * `mode=sample`: 8_dev only. Every table is first cut to the `cohort` widget (default the chain cohort `persons_n1000`) (+ NULL-person stratum).
# MAGIC * `mode=full`: reads the published tables; writes `out_schema` (6_mgmt.data_quality in prod, INSR_Apply P9b).
# MAGIC * Resumable: each (table, stage) is deleted and rewritten, so a re-run with `only=` repeats just that table.
# MAGIC * `release_boundary` is the only release literal anywhere in W4 (critic 3.1); it never enters a pipeline.

# COMMAND ----------

import json
import time
from datetime import date, datetime, timezone

from pyspark.sql import functions as F

import release_detectors as RD
import release_profile as RP


def get_param(name, default):
    """A run parameter if the runner passed one, else the default (never declares a widget: no reset)."""
    try:
        value = dbutils.widgets.get(name)
        return value if value not in (None, "") else default
    except Exception:
        return default


RELEASE_ID = get_param("release_id", "")
RELEASE_BOUNDARY = get_param("release_boundary", "")          # e.g. 2026-11-03T06:00:00Z, from the release manifest
MODE = get_param("mode", "sample")
OUT = get_param("out_schema", "`8_dev`.insr_w4")
COHORT = get_param("cohort", "`8_dev`.insr_cohort.persons_n1000")   # chain cohort slice (build prep 2026-10-30)
SAMPLE_SCHEMA = get_param("sample_schema", "`8_dev`.insr_w4_sample")
RESTAGE = get_param("restage", "false").lower() == "true"   # re-cut sample tables that already exist
ENTITY_N = int(get_param("entity_n", "1000"))   # NULL-person entity stratum at the cohort's 1/1000
ONLY = [t for t in get_param("only", "").split(",") if t]
STAGES = get_param("stages", "profile,dates,units,keys,person,oneoff,residual,detect").split(",")
REGISTRY_PATH = get_param("registry", "release_registry.json")

assert RELEASE_ID and RELEASE_BOUNDARY, "release_id and release_boundary are required"
assert MODE in ("sample", "full"), MODE
assert OUT.startswith("`8_dev`") or (MODE == "full" and OUT == "`6_mgmt`.data_quality"), OUT
assert SAMPLE_SCHEMA.startswith("`8_dev`"), SAMPLE_SCHEMA   # sample CTAS writes only to 8_dev
RUN_TS = datetime.fromtimestamp(time.time(), timezone.utc)   # profiled_at of every column-profile row (Python clock: no now-relative SQL)
BOUNDARY_DATE = date.fromisoformat(RELEASE_BOUNDARY[:10])
REGISTRY = {e["fqn"]: e for e in json.load(open(REGISTRY_PATH))}
TABLES = [fqn for fqn in sorted(REGISTRY) if not ONLY or REGISTRY[fqn]["table"] in ONLY]
if OUT.startswith("`8_dev`"):   # dev output schema; prod's 6_mgmt.data_quality already exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUT} COMMENT 'INSR W4 release report (dev output)'")
for statement in RP.ddl(OUT):
    spark.sql(statement)
print(RELEASE_ID, MODE, OUT, len(TABLES), "tables")

# COMMAND ----------

def source_of(entry):
    """The table a statement reads: the sample copy in sample mode, else the published table."""
    return RP.sample_table(entry, SAMPLE_SCHEMA) if MODE == "sample" else entry["fqn"]


def replace_rows(table, rows, schema, fqn, stage):
    """Delete this release's rows for (table_fqn, stage) and append `rows` (resumable re-runs).

    release_column_profile rows also get profiled_at = RUN_TS (its last column), which previous_release() reads.
    """
    spark.sql(f"DELETE FROM {OUT}.{table} WHERE release_id = '{RELEASE_ID}' AND table_fqn = '{fqn}' AND stage = '{stage}'")
    if rows:
        tail = (stage, RUN_TS) if table == "release_column_profile" else (stage,)
        spark.createDataFrame([(*r, *tail) for r in rows], schema).write.mode("append").saveAsTable(f"{OUT}.{table}")


# Sample staging: cut each table once. Existing sample tables are reused (the timing-gate invocation, any narrowed
# re-run of one table and the W4.05/W4.06 rule checks would otherwise repeat all 221 CTAS at about $10 a time);
# pass restage=true after a cohort or registry change.
if MODE == "sample":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SAMPLE_SCHEMA} COMMENT 'INSR W4 release-report sample (persons_n1000 chain cohort slice)'")
    staged = 0
    for fqn in TABLES:
        if RESTAGE or not spark.catalog.tableExists(RP.sample_table(REGISTRY[fqn], SAMPLE_SCHEMA)):
            spark.sql(RP.stage_sample_sql(REGISTRY[fqn], COHORT, SAMPLE_SCHEMA, ENTITY_N))
            staged += 1
    print(staged, "sample tables staged;", len(TABLES) - staged, "reused")

# COMMAND ----------

# Stage "profile": one scan per table; month series to release_profile_month, whole-table rows to release_column_profile.
if "profile" in STAGES:
    for fqn in TABLES:
        entry = REGISTRY[fqn]
        sql, measures = RP.profile_sql(entry, source_of(entry))
        started = time.time()
        rows = [r.asDict() for r in spark.sql(sql).collect()]
        elapsed = time.time() - started
        long = RP.long_rows(RELEASE_ID, entry, rows, measures)
        types = {c["name"]: (c["type"], c["date_class"] or c["kind"]) for c in entry["columns"]}
        month_rows = [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in long if r[2] != "all"]
        column_rows = [(r[0], r[1], r[6], *types.get(r[6], (None, "table")), r[5], r[7]) for r in long if r[2] == "all"]
        column_rows.append((RELEASE_ID, fqn, "profile", None, "run", "elapsed_s", float(elapsed)))
        replace_rows("release_profile_month", month_rows, RP.PROFILE_MONTH_SCHEMA, fqn, "profile")
        replace_rows("release_column_profile", column_rows, RP.COLUMN_PROFILE_SCHEMA, fqn, "profile")
        print(fqn, len(rows), "groups", len(long), "metrics", round(elapsed, 1), "s")

# COMMAND ----------

# Stage "dates": one day-count scan per table (date columns of DAY_HEAP_CLASSES), reduced on the driver to
# col_month and day_heap rows. If this costs more than 2x the profile, pass day_heap_classes=birth,event,effective.
HEAP_CLASSES = tuple(get_param("day_heap_classes", ",".join(RP.DAY_HEAP_CLASSES)).split(","))
MIN_HEAP_ROWS = int(get_param("min_heap_rows", "50"))
if "dates" in STAGES:
    for fqn in TABLES:
        entry = REGISTRY[fqn]
        sql = RP.dates_day_sql(entry, source_of(entry), HEAP_CLASSES)
        if sql is None:
            continue
        started = time.time()
        days = [r.asDict() for r in spark.sql(sql).collect()]
        rows = RP.day_rows(RELEASE_ID, entry, days, min_rows=MIN_HEAP_ROWS)
        replace_rows("release_profile_month", rows, RP.PROFILE_MONTH_SCHEMA, fqn, "dates")
        replace_rows("release_column_profile", [(RELEASE_ID, fqn, "dates", None, "run", "elapsed_s",
                                                 float(time.time() - started))],
                     RP.COLUMN_PROFILE_SCHEMA, fqn, "dates_run")

# COMMAND ----------

# Stage "units": F10 unit/scale groups into CHECKS["F10:<fqn>"].
CHECKS = globals().get("CHECKS", {})
if "units" in STAGES:
    for group in RP.UNIT_GROUPS:
        if group[0] in REGISTRY and (not ONLY or REGISTRY[group[0]]["table"] in ONLY):
            CHECKS["F10:" + group[0]] = [r.asDict() for r in spark.sql(RP.units_sql(group, source_of(REGISTRY[group[0]]))).collect()]

# COMMAND ----------

# Latest dq_rule row per rule_id (insr-v* ranks above g*-v*); used by the falsifier and the W4.07 residual cell.
DQ_RULES = [r.asDict() for r in spark.sql("""
    SELECT * FROM (
      SELECT *, row_number() OVER (PARTITION BY rule_id ORDER BY
               CASE WHEN catalog_version LIKE 'insr-v%' THEN 1000 + try_cast(substr(catalog_version, 7) AS INT)
                    ELSE try_cast(regexp_extract(catalog_version, 'g([0-9]+)-v', 1) AS INT) * 10
                         + try_cast(regexp_extract(catalog_version, '-v([0-9]+)', 1) AS INT) END DESC NULLS LAST) AS rn
      FROM `6_mgmt`.data_quality.dq_rule) WHERE rn = 1""").collect()]
print(len(DQ_RULES), "latest dq_rule rows")

# COMMAND ----------

# Stage "keys": F13 + F7(a) per keyed table; F12 per FK pair (sample: only pairs whose child is person-cut) and
# gene_tested parent coverage. Results go to CHECKS and to release_column_profile (class 'check') so the next
# release can compare.
CHECKS = globals().get("CHECKS", {})


# Check rows are buffered and written once per stage (flush_checks): a DELETE + append per table and check cost
# several seconds each, far more than the check statements themselves.
PENDING_CHECKS = {}   # (table_fqn, stage) -> rows


def persist_checks(fqn, check, metrics):
    """Buffer one statement check's numeric metrics as release_column_profile rows (stage 'checks:<check>')."""
    rows = [(RELEASE_ID, fqn, check, None, "check", k, float(v)) for k, v in metrics.items()
            if isinstance(v, (int, float)) and v is not None]
    PENDING_CHECKS[(fqn, "checks:" + check)] = rows


def flush_checks():
    """Replace this release's rows of every buffered (table_fqn, stage) with one DELETE and one append."""
    if not PENDING_CHECKS:
        return
    keys = " OR ".join(f"(table_fqn = '{fqn}' AND stage = '{stage}')" for fqn, stage in sorted(PENDING_CHECKS))
    spark.sql(f"DELETE FROM {OUT}.release_column_profile WHERE release_id = '{RELEASE_ID}' AND ({keys})")
    rows = [(*r, stage, RUN_TS) for (_, stage), batch in sorted(PENDING_CHECKS.items()) for r in batch]
    if rows:
        spark.createDataFrame(rows, RP.COLUMN_PROFILE_SCHEMA).write.mode("append") \
            .saveAsTable(f"{OUT}.release_column_profile")
    print(len(PENDING_CHECKS), "check groups written,", len(rows), "rows")
    PENDING_CHECKS.clear()


if "keys" in STAGES:
    CHECKS["F13"] = []
    for fqn in TABLES:
        sql = RP.keys_sql(REGISTRY[fqn], source_of(REGISTRY[fqn]))
        if sql:
            r = spark.sql(sql).first().asDict()
            CHECKS["F13"].append({"fqn": fqn, **r})
            if r["n_distinct"] is not None:
                persist_checks(fqn, "F13", {"duplicates": r["n"] - r["n_null_key"] - r["n_distinct"]})
    CHECKS["F12"] = []
    for pair in RP.FK_PAIRS:
        child, fk, parent, _ = pair
        if child not in REGISTRY or (ONLY and REGISTRY[child]["table"] not in ONLY):
            continue
        if MODE == "sample" and not REGISTRY[child]["person_col"]:
            print("F12 not measured in sample (person-less child):", child, fk)
            continue
        if MODE == "sample" and not spark.catalog.tableExists(source_of(REGISTRY[parent])):
            print("F12 not measured (parent sample copy not staged in this sample schema):", child, fk)
            continue
        started = time.time()
        r = spark.sql(RP.fk_sql(pair, source_of(REGISTRY[child]), source_of(REGISTRY[parent]))).first().asDict()
        CHECKS["F12"].append({"pair": f"{child}.{fk}", "child": child, "fk": fk, **r})
        persist_checks(child, f"F12:{fk}", {"orphans": r["n_zero"] + r["n_orphan"],
                                            "coverage": (r["n_nonnull"] / r["n"]) if r["n"] else 0.0,
                                            "elapsed_s": time.time() - started})
    genomic, gene = "`4_prod`.gold.clinical_genomic_test", "`4_prod`.gold.reference_gene_tested"
    if genomic in TABLES and gene in REGISTRY:   # the finding belongs to genomic_test: only when this run owns it
        CHECKS["F12_gene_coverage"] = [r.asDict() for r in spark.sql(
            RP.gene_coverage_sql(source_of(REGISTRY[genomic]), source_of(REGISTRY[gene]))).collect()]
    flush_checks()

# COMMAND ----------

# Stage "person": F24, J8, F19 CC death, BlobIdentity, MSDS guard, L5172 (+ the J8 fact-person check per table).
if "person" in STAGES:
    scope_available = (spark.catalog.tableExists("`4_prod`.silver.reference_person_scope")
                       and "in_scope" in spark.table("`4_prod`.silver.reference_person_scope").columns)

    def by_fqn(fqn):
        """What this run reads for a published fqn (the sample copy in sample mode)."""
        return source_of(REGISTRY[fqn])

    # Only checks whose gold tables are all in this run (an only= re-run neither reads unstaged sample copies nor
    # re-emits findings for tables whose findings it does not replace).
    for name, sql in RP.person_sqls(by_fqn, RELEASE_BOUNDARY.replace("T", " ").rstrip("Z"), scope_available,
                                    tables=set(TABLES)):
        CHECKS[name] = [r.asDict() for r in spark.sql(sql).collect()]
    # MSDS regression guard (master W4.04): read W2.17's comparison MV when it exists; otherwise record that it
    # is not built, with the W2.17 gate verdict (run parameter msds_gate_verdict, filled by the runner from the
    # ledger row W2_17_gate; the prod notebook never reads 8_dev). The MV's location and its two count columns
    # (msds_deliveries, agreed_deliveries) are fixed by W2.17 Step 4 (GO branch): silver `_n` publishes
    # journey_clinical._msds_regression_guard as <internal>._clinical_msds_regression_guard (internal = 4_prod.silver).
    # One row per MSDS row, so the sums below are the delivery counts. msds_guard_fqn overrides the default.
    MSDS_GUARD_FQN = get_param("msds_guard_fqn", "`4_prod`.silver._clinical_msds_regression_guard")
    if "`4_prod`.gold.clinical_labour_delivery" not in TABLES:
        pass   # the guard's findings belong to clinical_labour_delivery, which this run does not own
    elif spark.catalog.tableExists(MSDS_GUARD_FQN):
        CHECKS["MSDS_guard"] = [spark.sql(
            f"SELECT sum(msds_deliveries) AS msds_deliveries, sum(agreed_deliveries) AS agreed_deliveries "
            f"FROM {MSDS_GUARD_FQN}").first().asDict()]
    else:
        CHECKS["MSDS_guard_not_built"] = [{"gate_verdict": get_param("msds_gate_verdict", "W2_17_gate not run")}]
    spine_source = source_of(REGISTRY["`4_prod`.gold.spine_person"])
    CHECKS["J8_fact_person"] = []
    if not spark.catalog.tableExists(spine_source):   # e.g. a one-table persons_n50 fallback schema
        print("J8 fact-person not measured: spine_person not staged in", SAMPLE_SCHEMA)
    for fqn in (TABLES if spark.catalog.tableExists(spine_source) else []):
        e = REGISTRY[fqn]
        if e["schema"] == "gold" and e["person_col"] and e["table"] != "spine_person":
            started = time.time()
            r = spark.sql(RP.fact_person_sql(fqn, source_of(e), spine_source)).first().asDict()
            CHECKS["J8_fact_person"].append(r)
            persist_checks(fqn, "J8_fact_person", {"not_in_spine": r["not_in_spine"], "elapsed_s": time.time() - started})
    flush_checks()

# COMMAND ----------

# Stage "residual" (F16c/g): re-evaluate every latest invalidate/normalize transform on what this run reads.
# A rule whose SQL does not analyse against the published columns is "unavailable" (internal input), not a failure.
# F16g (coverage across the whole catalogue) is only recorded on a run over every table: an `only=` run would
# count every other table as unavailable, and its '*' finding is not replaced by an `only=` detect.
CHECKS = globals().get("CHECKS", {})
if "residual" in STAGES:
    CHECKS["F16_residual"], measured, unavailable = [], 0, 0
    by_table = {}
    for rule in DQ_RULES:
        if rule["action"] in ("invalidate", "normalize") and rule["transform"] and rule["column"]:
            plane_short = rule["table"].replace("journey_", "", 1).replace(".", "_")
            by_table.setdefault(f"`4_prod`.gold.{plane_short}", []).append(
                (rule["rule_id"], rule["column"], rule["transform"]))
    for fqn, rules in sorted(by_table.items()):
        if fqn not in REGISTRY or (ONLY and REGISTRY[fqn]["table"] not in ONLY):
            unavailable += len(rules)
            continue
        source = source_of(REGISTRY[fqn])
        ok = []
        for rule in rules:
            try:
                spark.sql(f"SELECT {rule[2].replace('{col}', '`' + rule[1] + '`')} AS x FROM {source}").schema
                ok.append(rule)
            except Exception:
                unavailable += 1
        measured += len(ok)
        sql = RP.residual_sql(fqn, source, ok)
        if sql:
            r = spark.sql(sql).first().asDict()
            CHECKS["F16_residual"] += [{"fqn": fqn, "rule_id": rid, "column": col, "residual": r[f"r{i}"]}
                                       for i, (rid, col, _) in enumerate(ok)]
    CHECKS["F16g"] = [] if ONLY else [{"measured": measured, "unavailable": unavailable}]
    print("residual rules measured", measured, "unavailable", unavailable)

# COMMAND ----------

# Stage "oneoff": small one-off statements (L4552 turnaround, L808 daily series, L2905 gate loss) into CHECKS.
CHECKS = globals().get("CHECKS", {})
if "oneoff" in STAGES:
    def _source_by_fqn(fqn):
        """What this run reads for a published fqn (sample copy in sample mode)."""
        return source_of(REGISTRY[fqn])

    for name, sql in RP.oneoff_sqls(_source_by_fqn, {fqn: REGISTRY[fqn] for fqn in TABLES}, RELEASE_BOUNDARY.replace("T", " ").rstrip("Z"), MODE):
        if name.startswith("BBR:"):
            # BBR_FOLDIN_V1: advisory one-offs that read release-new columns; an analysis error is recorded as an
            # error row (detector bbr_advisories reports it as info) instead of failing the stage.
            try:
                CHECKS[name] = [r.asDict() for r in spark.sql(sql).collect()]
            except Exception as error:  # noqa: BLE001 - recorded, never a gate
                CHECKS[name] = [{"error": f"{type(error).__name__}: {error}"}]
        else:
            CHECKS[name] = [r.asDict() for r in spark.sql(sql).collect()]
        print(name, len(CHECKS[name]), "rows")

# COMMAND ----------

# INSR_R4_RULES_V1 (design R4 "QC evidence", decision 15; Deviation D3): every gold rule id counted per release into
# release_rule_count, from the count SQL R4 generated (Gold/QC/gold_rules.json). Reads silver and the gold lookups,
# never the sample copies, so it runs in mode=full only. A table is recounted when its SQL or its silver source
# changed, or on a GLOBAL release; otherwise its previous counts are carried (release_rule_counts.needs_count).
if "rules" in STAGES:
    import release_rule_counts as RC
    assert MODE == "full", "the rules stage counts silver pre-filter rows: mode=full only"
    with open(get_param("rules_manifest", "gold_rules.json"), encoding="utf-8") as handle:
        RULES = json.load(handle)
    RULES_GLOBAL = get_param("rules_global", "true").lower() == "true"
    for statement in RC.ddl(OUT):
        spark.sql(statement)
    history_by_fqn = {}
    for r in spark.sql(f"SELECT * FROM {OUT}.release_rule_count WHERE release_id <> '{RELEASE_ID}'").collect():
        history_by_fqn.setdefault(r["table_fqn"], []).append(r.asDict())
    rule_rows = []
    for table, entry in sorted(RULES.items()):
        if ONLY and table not in ONLY:
            continue
        cat, sch, tab = [p.strip("`") for p in entry["silver_fqn"].split(".")]
        found = spark.sql(f"SELECT last_altered FROM `{cat}`.information_schema.tables "
                          f"WHERE table_schema = '{sch}' AND table_name = '{tab}'").collect()
        altered = found[0]["last_altered"] if found else None
        previous = RC.latest_counts(history_by_fqn.get(entry["fqn"], []))
        if RC.needs_count(entry, altered, previous, RULES_GLOBAL):
            started = time.time()
            result = spark.sql(entry["sql"]).first().asDict()
            rule_rows += RC.count_rows(RELEASE_ID, table, entry, result, altered, RUN_TS)
            print(table, "counted", result["rows_in"], "rows in,", result["rows_kept"], "kept", round(time.time() - started, 1), "s")
        else:
            rule_rows += RC.carried_rows(RELEASE_ID, previous)
            print(table, "carried from", previous[0]["counted_release_id"])
    fqns = sorted({entry["fqn"] for t, entry in RULES.items() if not ONLY or t in ONLY})
    spark.sql(f"DELETE FROM {OUT}.release_rule_count WHERE release_id = '{RELEASE_ID}' AND table_fqn IN ("
              + ", ".join(f"'{f}'" for f in fqns) + ")")
    if rule_rows:
        spark.createDataFrame(rule_rows, "release_id string, table_fqn string, tier string, rule_id string, action string, "
                              "rows_in bigint, rows_kept bigint, failed bigint, counted_release_id string, signature string, "
                              "source_altered timestamp, counted_at timestamp").write.mode("append").saveAsTable(f"{OUT}.release_rule_count")
    missing = RC.coverage_problems({t: e for t, e in RULES.items() if not ONLY or t in ONLY}, rule_rows)
    print(len(rule_rows), "rule rows;", len(missing), "rules without a count")
    assert not missing, f"rules without a count: {missing[:10]}"

# COMMAND ----------

# Stage "detect" (always last): run every detector (one monthly load per table) and replace this release's findings.
def column_rows_of(release_id):
    """{fqn: [row dicts]} of release_column_profile for one release (about 60 rows per column; fits the driver)."""
    out = {}
    for r in spark.table(f"{OUT}.release_column_profile").where(F.col("release_id") == release_id).collect():
        out.setdefault(r["table_fqn"], []).append(r.asDict())
    return out


def load_month(fqn):
    """This release's monthly rows of one table (called once per table by RD.run_all)."""
    return [r.asDict() for r in spark.table(f"{OUT}.release_profile_month")
            .where((F.col("release_id") == RELEASE_ID) & (F.col("table_fqn") == fqn)).collect()]


def previous_release():
    """The release profiled most recently before this one (by max(profiled_at), not by release_id string order,
    which breaks across naming changes such as INSR_20261029_S1 vs later ids), else None."""
    row = (spark.table(f"{OUT}.release_column_profile")
           .where((F.col("release_id") != RELEASE_ID) & F.col("profiled_at").isNotNull())
           .groupBy("release_id").agg(F.max("profiled_at").alias("last_profiled"))
           .where(F.col("last_profiled") < F.lit(RUN_TS))
           .orderBy(F.col("last_profiled").desc()).first())
    return row["release_id"] if row else None


if "detect" in STAGES:
    prev = previous_release()
    ctx = RD.Context(release_id=RELEASE_ID, boundary=BOUNDARY_DATE,
                     registry={fqn: REGISTRY[fqn] for fqn in TABLES}, load_month=load_month,
                     column=column_rows_of(RELEASE_ID), previous_column=column_rows_of(prev) if prev else {},
                     checks=CHECKS if "CHECKS" in globals() else {},
                     dq_rules=DQ_RULES if "DQ_RULES" in globals() else [])
    findings = RD.run_all(ctx, STAGES)
    # Replace only the findings this run regenerated. Detectors fed by a CHECKS stage that did not run this time
    # (e.g. stages=oneoff,detect, or a resumed run in a new session) keep their earlier findings; each finding's
    # detail carries the detector name that wrote it.
    skipped = RD.skipped_detectors(STAGES)
    keep = ("" if not skipped else " AND coalesce(get_json_object(detail, '$.detector'), '') NOT IN ("
            + ", ".join(f"'{d}'" for d in sorted(skipped)) + ")")
    spark.sql(f"DELETE FROM {OUT}.release_finding WHERE release_id = '{RELEASE_ID}'" + keep
              + ("" if not ONLY else " AND table_fqn IN (" + ", ".join(f"'{t}'" for t in TABLES) + ")"))
    if findings:
        cols = [c.split()[0] for c in RP.FINDING_SCHEMA.split(", ")]
        spark.createDataFrame([tuple(f[c] for c in cols) for f in findings], RP.FINDING_SCHEMA) \
            .write.mode("append").saveAsTable(f"{OUT}.release_finding")
    summary = {s: sum(1 for f in findings if f["severity"] == s) for s in RD.SEVERITIES}
    dbutils.notebook.exit(json.dumps({"release_id": RELEASE_ID, "tables": len(TABLES), "previous": prev, **summary}))

