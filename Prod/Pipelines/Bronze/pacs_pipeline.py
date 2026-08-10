# Databricks notebook source
# MAGIC %md
# MAGIC # PACS Bronze Pipeline (curated)
# MAGIC
# MAGIC Brings the Sectra PACS raw mirror (4_prod.raw.pacs_*) into bronze as a CURATED model -
# MAGIC three consumable tables, not a 1:1 raw mirror. The seven raw tables are combined,
# MAGIC person-resolved and evidence-filtered so downstream reads one table per question:
# MAGIC - `map_pacs_examination`  - one row per PERFORMED exam (~16.36M of 17.78M raw on
# MAGIC   2026-08-08; the v3 evidence union includes series-only imaging; booking/
# MAGIC   cancelled ghosts excluded). Request context, report rollup, series rollup and
# MAGIC   archive rollup are folded onto the exam row.
# MAGIC - `map_pacs_report`       - one row per report (all 14.57M) with request/exam/person
# MAGIC   resolved through the bridge and native report text (RTF or plain; ~2024+ only).
# MAGIC - `map_pacs_patient_link` - PACS patient record -> PERSON_ID crosswalk, published only
# MAGIC   for the ~3.03M patient records referenced by imaging activity (37.7% of raw).
# MAGIC
# MAGIC NOT published as tables (folded in or intentionally excluded; raw retains everything):
# MAGIC - pacs_requests            -> folded onto exam rows (94.0% of requests have exactly 1 exam);
# MAGIC                               the ~131k exam-less requests (nothing performed) are excluded
# MAGIC - pacs_examinationreports  -> resolved into both sides (report pointers + exam rollup);
# MAGIC                               full m:n history (addenda chains) stays in raw only
# MAGIC - pacs_examinationfolders  -> archive/size/access rollup on the exam row
# MAGIC - pacs_series (161M rows)  -> measured series/object counts on the exam row; the
# MAGIC                               object-level rows (frozen 2024-11-02) stay in raw only
# MAGIC
# MAGIC Evidence filter: an exam row is published iff it shows evidence of performed imaging -
# MAGIC status in {100,110,75,83} OR >=1 bridge report link OR ImageCount>0 OR series objects
# MAGIC exist (provenance in PERFORMED_EVIDENCE). ADC_Deleted is dropped everywhere
# MAGIC (2026-08-01 incident: delete-flag pass against empty source slices flagged all 161M
# MAGIC series rows). Loads: content-fingerprint skip (count | max ADC_UPDT | bit_xor
# MAGIC xxhash64 over consumed columns) on raw PACS sources, run-start version pinning of
# MAGIC all external sources, a run lock (state-table sentinel, 12h TTL), staged-snapshot
# MAGIC materialization to <target>_stg, row-hash MERGE, SOURCE_PRESENT_IND soft-deletes,
# MAGIC pre-merge soft-delete tripwire, state in pacs_pipeline_state, run log in
# MAGIC pacs_pipeline_audit.
# MAGIC
# MAGIC Distinct from the legacy 4_prod.pacs_dlt/4_prod.pacs lane (Mill-spine imaging_metadata).
# MAGIC Report text here is net-new (legacy imaging_report text columns are 100% NULL).
# MAGIC
# MAGIC v3 TRANSITION NOTE: the v2.1 layout (7 native-grain tables) is schema-incompatible;
# MAGIC before the first v3 run, DROP the old map_pacs_* tables and delete their
# MAGIC pacs_pipeline_state rows (see the plan's migration step).

# COMMAND ----------

# MAGIC %run ./_bronze_common

# COMMAND ----------

import builtins
from functools import reduce
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F

def _widget_text(name, default):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)

for _n, _d in {
    "target_schema": "8_dev.bronze",          # prod passes 4_prod.bronze via the orchestrator
    "force_full_refresh": "false",
    "allow_production_write": "false",
    "enable_link_group_backfill": "true",
    "max_soft_delete_fraction": "0.05",       # tripwire: max share of a snapshot soft-deleted per run
}.items():
    _widget_text(_n, _d)

TARGET_SCHEMA      = bronze_value("target_schema", "8_dev.bronze")
FORCE_FULL_REFRESH = bronze_bool("force_full_refresh", False)
ALLOW_PROD_WRITE   = bronze_bool("allow_production_write", False)
ENABLE_LINK_GROUP  = bronze_bool("enable_link_group_backfill", True)
MAX_SOFT_DELETE_FRACTION = float(bronze_value("max_soft_delete_fraction", "0.05"))
PACS_RUN_ID        = bronze_run_id()
_STARTED_AT        = bronze_utc_now()

assert TARGET_SCHEMA.startswith("8_dev.") or ALLOW_PROD_WRITE, (
    f"Refusing non-dev target {TARGET_SCHEMA} without allow_production_write=true"
)

RAW = "4_prod.raw"
SRC_EXAMS        = f"{RAW}.pacs_examinations"
SRC_REQUESTS     = f"{RAW}.pacs_requests"
SRC_REPORTS      = f"{RAW}.pacs_reports"
SRC_SERIES       = f"{RAW}.pacs_series"
SRC_PATIENTS     = f"{RAW}.pacs_patients"
SRC_EXAM_REPORTS = f"{RAW}.pacs_examinationreports"
SRC_EXAM_FOLDERS = f"{RAW}.pacs_examinationfolders"
PERSON_ALIAS     = f"{RAW}.mill_person_alias"
MILL_PERSON      = f"{RAW}.mill_person"

TGT_PATIENT_LINK = f"{TARGET_SCHEMA}.map_pacs_patient_link"
TGT_EXAMINATION  = f"{TARGET_SCHEMA}.map_pacs_examination"
TGT_REPORT       = f"{TARGET_SCHEMA}.map_pacs_report"
STATE_TABLE      = f"{bronze_control_schema(TARGET_SCHEMA)}.pacs_pipeline_state"
AUDIT_TABLE      = f"{bronze_control_schema(TARGET_SCHEMA)}.pacs_pipeline_audit"

MRN_ALIAS_TYPE, NHS_ALIAS_TYPE = 10, 18
# Statuses measured as performed imaging (2026-08-08 profile): report-linkage 77.0% /
# 88.0% / 99.9% / 74.0% respectively, zero post-halt booking dates. All other statuses
# are booking/cancelled ghosts (<1-16% report linkage; status 40 holds the post-halt
# pre-booked rows) and are published only when another evidence signal fires.
PERFORMED_STATUS_CDS = (100, 110, 75, 83)
# Bump when transformation logic changes (renames, linkage rules, cleaners, scope
# filters): it is folded into every target's gate fingerprint, so a code change forces
# a re-gate even when sources are unchanged. Mode widgets are folded in the same way.
PIPELINE_LOGIC_VERSION = "2026.08.v3.1"

# Raw PACS sources are gated on content fingerprint (count|max ADC_UPDT), not Delta
# version: the upstream IncrUpdtV2_Upsert job commits delete-flag-only MERGEs that
# bump versions weekly while changing only ADC_Deleted - which bronze drops.
FINGERPRINT_GATED = {
    SRC_EXAMS, SRC_REQUESTS, SRC_REPORTS, SRC_SERIES,
    SRC_PATIENTS, SRC_EXAM_REPORTS, SRC_EXAM_FOLDERS,
}

def qident(name):
    q = builtins.chr(96)
    return q + name.replace(q, q + q) + q

def qname(name):
    return ".".join(qident(p) for p in name.split("."))

_missing = [t for t in (
    SRC_EXAMS, SRC_REQUESTS, SRC_REPORTS, SRC_SERIES, SRC_PATIENTS,
    SRC_EXAM_REPORTS, SRC_EXAM_FOLDERS, PERSON_ALIAS, MILL_PERSON,
) if not bronze_table_exists(t)]
assert not _missing, f"Missing required sources: {_missing}"

print(f"[PACS] target={TARGET_SCHEMA} state={STATE_TABLE} run_id={PACS_RUN_ID}")

# COMMAND ----------

# Frozen source contracts: the columns this pipeline consumes. Missing = abort;
# extra columns are additive and ignored.
EXPECTED_COLUMNS = {
    SRC_EXAMS: {
        "ExaminationId", "ExaminationIdString", "ExaminationRequestId",
        "ExaminationPatientId", "ExaminationStudyUid",
        "ExaminationAccessionNumber", "ExaminationDate", "ExaminationArrivalTime",
        "ExaminationStatus", "ExaminationCode", "ExaminationDescription",
        "ExaminationModality", "ExaminationBodyPart", "ExaminationInstitution",
        "ExaminationComments", "ExaminationDoseInformation", "ExaminationStat",
        "ExaminationReadingPhysician", "ExaminationText1",
        "ExaminationSeriesCount", "ExaminationImageCount", "ADC_UPDT",
    },
    SRC_REQUESTS: {
        "RequestId", "RequestIdString", "RequestPatientId", "RequestReferringUnit",
        "RequestRisHostId", "RequestQuestion", "RequestAnamnesis", "ADC_UPDT",
    },
    SRC_REPORTS: {
        "ReportId", "ReportIdString", "ReportRisId", "ReportDate",
        "ReportModifiedDateUTC", "ReportText", "ReportStatus", "ReportDoctorId",
        "ReportPreliminarySignatureDate", "ReportFinalSignatureDate", "ADC_UPDT",
    },
    SRC_SERIES: {
        "SeriesId", "SeriesExaminationId", "SeriesSeriesInstanceUid",
        "SeriesPixelDataFlag", "ADC_UPDT",
    },
    SRC_PATIENTS: {
        "PatientId", "PatientPersonalId", "PatientIssuerId", "PatientLinkId",
        "PatientBirthdate", "ADC_UPDT",
    },
    SRC_EXAM_REPORTS: {
        "ExaminationReportReportId", "ExaminationReportRequestId",
        "ExaminationReportExaminationId", "ADC_UPDT",
    },
    SRC_EXAM_FOLDERS: {
        "ExaminationFolderId", "ExaminationFolderExaminationId",
        "ExaminationFolderArchiveState", "ExaminationFolderAccessedDate",
        "ExaminationFolderModifyDate", "ExaminationFolderExamSize", "ADC_UPDT",
    },
    PERSON_ALIAS: {
        "PERSON_ID", "ALIAS", "PERSON_ALIAS_TYPE_CD", "ACTIVE_IND",
    },
    MILL_PERSON: {"PERSON_ID", "BIRTH_DT_TM"},
}

for _t, _want in EXPECTED_COLUMNS.items():
    _have = {c.lower() for c in spark.table(_t).columns}
    _miss = sorted(c for c in _want if c.lower() not in _have)  # case-insensitive: raw casing varies
    assert not _miss, f"{_t} missing breaking columns: {_miss}"

# Volume floors (~3% under measured 2026-08-08 counts; a collapse below = broken mirror)
ROW_FLOORS = {
    SRC_EXAMS: 17_200_000, SRC_REQUESTS: 16_100_000, SRC_REPORTS: 14_100_000,
    SRC_SERIES: 156_000_000, SRC_PATIENTS: 7_800_000,
    SRC_EXAM_REPORTS: 16_900_000, SRC_EXAM_FOLDERS: 15_200_000,
}
PK_COLUMNS = {
    SRC_EXAMS: ["ExaminationId"], SRC_REQUESTS: ["RequestId"],
    SRC_REPORTS: ["ReportId"], SRC_SERIES: ["SeriesId"],
    SRC_PATIENTS: ["PatientId"], SRC_EXAM_FOLDERS: ["ExaminationFolderId"],
}
# CTAS-rewritten snapshots re-stamp ADC_UPDT uniformly; a mixed snapshot = partial load.
CTAS_SNAPSHOT_TABLES = [SRC_PATIENTS, SRC_EXAM_REPORTS, SRC_EXAM_FOLDERS]

preflight_report = {}
for _t, _floor in ROW_FLOORS.items():
    _aggs = [F.count(F.lit(1)).alias("rows"), F.max("ADC_UPDT").alias("max_adc")]
    if _t in PK_COLUMNS:
        _k = PK_COLUMNS[_t][0]
        _aggs += [F.countDistinct(_k).alias("distinct_keys"),
                  F.sum(F.when(F.col(_k).isNull(), 1).otherwise(0)).alias("null_keys")]
    if _t in CTAS_SNAPSHOT_TABLES:
        _aggs.append(F.countDistinct("ADC_UPDT").alias("adc_versions"))
    _s = spark.table(_t).agg(*_aggs).collect()[0].asDict()
    assert _s["rows"] >= _floor, f"{_t}: rows {_s['rows']} below floor {_floor}"
    if _t in PK_COLUMNS:
        assert _s["null_keys"] == 0, f"{_t}: NULL primary keys"
        assert _s["distinct_keys"] == _s["rows"], f"{_t}: {PK_COLUMNS[_t][0]} not unique"
    if _t in CTAS_SNAPSHOT_TABLES:
        assert _s["adc_versions"] == 1, f"{_t}: partial CTAS snapshot ({_s['adc_versions']} ADC_UPDT values)"
    preflight_report[_t] = {k: str(v) for k, v in _s.items()}

# Bridge composite grain: (ReportId, ExaminationId) unique treating NULL exam as a value.
_bridge_dups = (
    spark.table(SRC_EXAM_REPORTS)
    .groupBy("ExaminationReportReportId",
             F.coalesce(F.col("ExaminationReportExaminationId").cast("string"), F.lit("NONE")))
    .count().where(F.col("count") > 1).count()
)
assert _bridge_dups == 0, (
    f"pacs_examinationreports: {_bridge_dups} duplicate (ReportId, ExaminationId) groups - "
    "grain assumption broken; STOP, do not dedupe"
)

# Freshness is INFORMATIONAL ONLY: the upstream extract halted ~2026-04-18
# (series 2024-11-02). Do not convert this into an assert.
print("[PACS][PREFLIGHT] source stats:", bronze_json(preflight_report))

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qname(TARGET_SCHEMA)}")
_control_schema = STATE_TABLE.rsplit(".", 1)[0]
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qname(_control_schema)}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {qname(STATE_TABLE)} (
      target_table STRING NOT NULL,
      source_table STRING NOT NULL,
      source_version BIGINT NOT NULL,
      source_fingerprint STRING,
      source_rows BIGINT,
      run_id STRING NOT NULL,
      committed_at TIMESTAMP NOT NULL) USING DELTA
    COMMENT 'PACS bronze pipeline checkpoints: last processed Delta version and content fingerprint per target and source. source_rows on the first source = rows PUBLISHED to the target that run (post-curation), not raw source rows.'""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {qname(AUDIT_TABLE)} (
      run_id STRING NOT NULL,
      event_ts TIMESTAMP NOT NULL,
      target_table STRING,
      event_type STRING NOT NULL,
      details STRING) USING DELTA
    COMMENT 'PACS bronze pipeline run log: one PUBLISHED event per written target plus a RUN_SUCCESS summary. Metrics only - never identifiers or report text.'""")


def audit(target, event_type, details=None):
    spark.createDataFrame(
        [(PACS_RUN_ID, target, event_type, bronze_json(details or {}))],
        "run_id string, target_table string, event_type string, details string",
    ).withColumn("event_ts", F.current_timestamp()) \
     .select("run_id", "event_ts", "target_table", "event_type", "details") \
     .write.mode("append").saveAsTable(AUDIT_TABLE)


# Run mutex: a sentinel row in the state table. Concurrent runs (manual + weekly)
# would otherwise interleave target writes and checkpoint MERGEs out of order. Delta
# MERGE commit conflicts serialize concurrent acquisitions; the read-back assert
# decides the winner. A crashed run leaves the lock until it EXPIRES (TTL below) or
# someone deletes the __RUN_LOCK__ row manually.
LOCK_KEY = "__RUN_LOCK__"
LOCK_TTL_HOURS = 12


def acquire_run_lock():
    lock = spark.createDataFrame(
        [(LOCK_KEY, LOCK_KEY, 0, None, None, PACS_RUN_ID)],
        "target_table string, source_table string, source_version long, "
        "source_fingerprint string, source_rows long, run_id string",
    ).withColumn("committed_at", F.current_timestamp())
    (DeltaTable.forName(spark, STATE_TABLE).alias("t")
     .merge(lock.alias("s"),
            "t.target_table = s.target_table AND t.source_table = s.source_table")
     .whenMatchedUpdate(
         condition=f"t.committed_at < current_timestamp() - INTERVAL {LOCK_TTL_HOURS} HOURS",
         set={"run_id": "s.run_id", "committed_at": "current_timestamp()"})
     .whenNotMatchedInsertAll()
     .execute())
    owner = spark.table(STATE_TABLE).where(F.col("target_table") == LOCK_KEY).collect()[0]
    assert owner["run_id"] == PACS_RUN_ID, (
        f"Another pacs_pipeline run holds the lock (run_id={owner['run_id']}, acquired "
        f"{owner['committed_at']}, expires after {LOCK_TTL_HOURS}h). Wait for it, or "
        f"DELETE the {LOCK_KEY} row from {STATE_TABLE} to break a dead lock.")
    print(f"[PACS] run lock acquired by {PACS_RUN_ID}")


def release_run_lock():
    spark.sql(f"DELETE FROM {qname(STATE_TABLE)} "
              f"WHERE target_table = '{LOCK_KEY}' AND run_id = '{PACS_RUN_ID}'")


acquire_run_lock()

HASH_EXCLUDE = {"ROW_HASH", "ADC_UPDT", "SOURCE_PRESENT_IND"}


def source_version(table):
    return int(spark.sql(f"DESCRIBE HISTORY {qname(table)} LIMIT 1").collect()[0]["version"])


# All EXTERNAL source versions are pinned ONCE at run start, so every target's gates
# describe the same estate snapshot - an upstream load landing mid-run cannot produce
# a mixed-snapshot publication (patient built on v1, reports on v2). Produced deps
# (map_pacs_* read back as sources) are deliberately NOT here: they are rebuilt inside
# this run and must be gated at their post-publish version.
_EXTERNAL_SOURCES = (SRC_EXAMS, SRC_REQUESTS, SRC_REPORTS, SRC_SERIES, SRC_PATIENTS,
                     SRC_EXAM_REPORTS, SRC_EXAM_FOLDERS, PERSON_ALIAS, MILL_PERSON)
_PINNED_VERSIONS = {s: source_version(s) for s in _EXTERNAL_SOURCES}
print(f"[PACS] pinned source versions: {_PINNED_VERSIONS}")

_FINGERPRINT_CACHE = {}


def content_fingerprint(source, version):
    """count | max(ADC_UPDT) | bit_xor(xxhash64(<consumed columns>)) computed FROM the
    pinned version. The per-row hash covers exactly the columns this pipeline consumes
    (EXPECTED_COLUMNS), so: upstream delete-flag-only passes (ADC_Deleted is not
    consumed) leave it stable, while ANY change to consumed content - including
    updates that fail to stamp ADC_UPDT and count-preserving delete/insert pairs -
    changes it. count+max alone were NOT sufficient: the 2026-08-01 incident proves
    the upstream loader can mutate a column without touching ADC_UPDT. Memoized per
    (source, version): the same source appears in several targets' gates."""
    key = (source, version)
    if key not in _FINGERPRINT_CACHE:
        cols = sorted(EXPECTED_COLUMNS[source])
        hash_expr = "bit_xor(xxhash64(" + ", ".join(qident(c) for c in cols) + "))"
        r = (spark.read.option("versionAsOf", version).table(source)
             .agg(F.count(F.lit(1)).alias("n"), F.max("ADC_UPDT").alias("m"),
                  F.expr(hash_expr).alias("h")).collect()[0])
        _FINGERPRINT_CACHE[key] = f"{r['n']}|{r['m']}|{r['h']}"
    return _FINGERPRINT_CACHE[key]


def read_pinned(table, gates):
    """Version-pinned read: every consumer of a gated source sees the exact snapshot
    the fingerprint was computed from (consistent build, no gate->stage race)."""
    if table in gates:
        return spark.read.option("versionAsOf", gates[table]["version"]).table(table)
    return spark.table(table)


def resolve_gates(sources):
    """{source: {"version": int, "fingerprint": str|None}} - computed ONCE per target
    and passed to needs_run, the builder (via read_pinned) and pacs_update_table.
    External sources use the run-start pinned version; produced deps (this run's own
    outputs) resolve live at gate time, i.e. their just-published version."""
    gates = {}
    for s in sources:
        v = _PINNED_VERSIONS[s] if s in _PINNED_VERSIONS else source_version(s)
        fp = content_fingerprint(s, v) if s in FINGERPRINT_GATED else None
        gates[s] = {"version": v, "fingerprint": fp}
    # Non-physical dependency (community-pipeline precedent): transformation-code and
    # mode-widget changes re-gate every target even when sources are unchanged, so a
    # deployed fix can never be silently skipped until someone remembers
    # force_full_refresh. Appended LAST so the first entry stays the raw table
    # (record_checkpoints stores output_rows against the first source).
    gates["__PIPELINE_LOGIC__"] = {
        "version": 0,
        "fingerprint": f"{PIPELINE_LOGIC_VERSION}|link_group={ENABLE_LINK_GROUP}",
    }
    return gates


def stored_state(target):
    if not bronze_table_exists(STATE_TABLE):
        return {}
    return {
        r["source_table"]: r
        for r in spark.table(STATE_TABLE).where(F.col("target_table") == target).collect()
    }


def needs_run(target, gates):
    if FORCE_FULL_REFRESH or not bronze_table_exists(target):
        return True
    state = stored_state(target)
    if set(state.keys()) != set(gates.keys()):
        return True  # dependency set changed
    for s, g in gates.items():
        prev = state[s]
        if g["fingerprint"] is not None:
            if prev["source_fingerprint"] != g["fingerprint"]:
                return True
        elif prev["source_version"] != g["version"]:
            return True
    return False


def with_row_hash(df):
    """Registry-family hash form (sha2 over to_json(struct)): unambiguous - a literal
    '<NULL>' string, SQL NULL, and embedded control characters in free text (ReportText,
    clinical blobs) all serialize distinctly, unlike a concat_ws+sentinel scheme."""
    columns = sorted(c for c in df.columns if c not in HASH_EXCLUDE)
    payload = F.to_json(F.struct(*[F.col(c) for c in columns]),
                        {"ignoreNullFields": "false"})
    return df.withColumn("ROW_HASH", F.sha2(payload, 256))


def verify_unique_key(df, keys):
    return df.groupBy(*keys).count().where(F.col("count") > 1).count()


def assert_unique_non_null(df, keys, label):
    nulls = df.where(reduce(lambda a, b: a | b, [F.col(k).isNull() for k in keys])).count()
    dups = verify_unique_key(df, keys)
    assert nulls == 0 and dups == 0, f"{label}: null keys={nulls} duplicate key groups={dups}"


def ensure_cdf(table):
    props = spark.sql(f"DESCRIBE DETAIL {qname(table)}").collect()[0]["properties"] or {}
    if str(props.get("delta.enableChangeDataFeed", "false")).lower() != "true":
        spark.sql(f"ALTER TABLE {qname(table)} SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")


def record_checkpoints(target, gates, output_rows):
    rows = [
        (target, s, int(g["version"]), g["fingerprint"],
         output_rows if i == 0 else None, PACS_RUN_ID)
        for i, (s, g) in enumerate(gates.items())
    ]
    updates = spark.createDataFrame(
        rows,
        "target_table string, source_table string, source_version long, "
        "source_fingerprint string, source_rows long, run_id string",
    ).withColumn("committed_at", F.current_timestamp())
    (DeltaTable.forName(spark, STATE_TABLE).alias("t")
     .merge(updates.alias("s"),
            "t.target_table = s.target_table AND t.source_table = s.source_table")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    # drop checkpoints for sources no longer declared (endobase behaviour)
    _declared = "', '".join(g.replace("'", "''") for g in gates.keys())
    spark.sql(
        f"DELETE FROM {qname(STATE_TABLE)} WHERE target_table = '{target}' "
        f"AND source_table NOT IN ('{_declared}')"
    )


def pacs_update_table(df, target, keys, gates):
    """Full-snapshot upsert: row hashes, change-gated update + resurrection,
    SOURCE_PRESENT_IND soft-deletes, checkpoint recording.

    The staged snapshot is MATERIALIZED to a transient <target>_stg Delta table
    first: the build graphs join five multi-million-row sources (exam: incl. the
    161M-row series aggregate) and are consumed three times (count, tripwire
    anti-join, MERGE) - serverless forbids .cache(), so without materialization
    the whole graph re-evaluates each time. The _stg table is dropped on success;
    a leftover from a crashed run is harmless (overwritten next run)."""
    staging_table = f"{target}_stg"
    (with_row_hash(df)
     .withColumn("SOURCE_PRESENT_IND", F.lit(True))
     .withColumn("ADC_UPDT", F.current_timestamp())
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(staging_table))
    staged = spark.table(staging_table)
    output_rows = staged.count()
    if not bronze_table_exists(target):
        staged.write.format("delta").option("delta.enableChangeDataFeed", "true").saveAsTable(target)
        metrics = {"operation": "CREATE", "rows": int(output_rows)}
    else:
        ensure_cdf(target)
        # Soft-delete tripwire BEFORE the merge: present target rows whose keys are
        # absent from the staged snapshot are exactly the rows the merge WOULD
        # soft-delete. A mass disappearance (e.g. an empty upstream extract slice)
        # aborts HERE - before any write - so the target is untouched, the checkpoint
        # does not advance, and a retry of the same bad snapshot fails identically.
        # (Keys are assert-verified non-null upstream, so a plain anti-join is sound.)
        _would_delete = (
            spark.table(target).where(F.col("SOURCE_PRESENT_IND"))
            .select(*keys)
            .join(staged.select(*keys), keys, "left_anti")
            .count())
        assert _would_delete <= MAX_SOFT_DELETE_FRACTION * builtins.max(output_rows, 1), (
            f"{target}: staged snapshot would soft-delete {_would_delete} rows "
            f"(> {MAX_SOFT_DELETE_FRACTION:.0%} of {output_rows} staged) - NO write was "
            "performed; investigate the raw mirror; raise max_soft_delete_fraction to override")
        condition = " AND ".join(f"t.{qident(k)} <=> s.{qident(k)}" for k in keys)
        values = {c: f"s.{qident(c)}" for c in staged.columns}
        (DeltaTable.forName(spark, target).alias("t")
         .merge(staged.alias("s"), condition)
         .whenMatchedUpdate(condition="t.ROW_HASH <> s.ROW_HASH OR t.SOURCE_PRESENT_IND = false", set=values)
         .whenNotMatchedInsert(values=values)
         .whenNotMatchedBySourceUpdate(condition="t.SOURCE_PRESENT_IND = true",
             set={"SOURCE_PRESENT_IND": "false", "ADC_UPDT": "current_timestamp()"})
         .execute())
        h = spark.sql(f"DESCRIBE HISTORY {qname(target)} LIMIT 1").collect()[0]
        metrics = {"operation": h["operation"],
                   **{k: v for k, v in (h["operationMetrics"] or {}).items()
                      if k in ("numTargetRowsInserted", "numTargetRowsUpdated",
                               "numTargetRowsDeleted", "numSourceRows")},
                   "softDeletedThisRun": int(_would_delete)}
    spark.sql(f"DROP TABLE IF EXISTS {qname(staging_table)}")
    ensure_cdf(target)
    record_checkpoints(target, gates, output_rows)
    audit(target, "PUBLISHED", metrics)
    return metrics


def apply_comments(table, comments, table_comment):
    """Diff-aware (endobase behaviour): only changed comments are applied, so
    metadata-only Delta versions never invalidate version gating of downstream deps."""
    def esc(v):
        return str(v).replace("\\", "\\\\").replace("'", "''")
    catalog, schema, tbl = table.split(".")
    existing = {
        r["column_name"]: (r["comment"] or "")
        for r in spark.sql(
            f"SELECT column_name, comment FROM {qident(catalog)}.information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{tbl}'"
        ).collect()
    }
    tbl_comment_row = spark.sql(
        f"SELECT comment FROM {qident(catalog)}.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{tbl}'"
    ).collect()
    changed = 0
    if not tbl_comment_row or (tbl_comment_row[0]["comment"] or "") != table_comment:
        spark.sql(f"COMMENT ON TABLE {qname(table)} IS '{esc(table_comment)}'")
        changed += 1
    live_cols = set(spark.table(table).columns)
    for column, comment in comments.items():
        if column in live_cols and existing.get(column, "") != comment:
            spark.sql(f"ALTER TABLE {qname(table)} ALTER COLUMN {qident(column)} COMMENT '{esc(comment)}'")
            changed += 1
    return changed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Person crosswalk
# MAGIC (PatientIssuerId, PatientPersonalId) -> PERSON_ID, typed per issuer:
# MAGIC 2/23 = Barts MRN (type 10) | 4 = NHS number (type 18) | 1 = RNJ-prefixed MRN |
# MAGIC 3/6 = legacy MRN | -1 = mixed (NHS-shape first, else MRN) | 20 = anonymized (no attempt).
# MAGIC Unique-active-alias matches only; ambiguity recorded, never guessed.
# MAGIC Optional PatientLinkId group backfill (deterministic, provenance LINK_GROUP).
# MAGIC The match runs over ALL 8.04M patient records (link groups span issuers), but only
# MAGIC records referenced by ANY RAW EXAM OR REQUEST (~3.03M, 37.7%) are PUBLISHED - the
# MAGIC scope is deliberately the raw superset (incl. ghost-exam-only and exam-less-request
# MAGIC patients) so it is resolvable before any fact table builds and always covers every
# MAGIC publishable fact FK. The other 62.3% carry no imaging rows at all.
# MAGIC Identifiers are transient - only PERSON_ID and statuses are published.

# COMMAND ----------

def norm_identifier(col):
    """Zero-strip numeric identifiers via regexp (NEVER cast: aliases reach 23+ digits,
    bigint overflows). Non-numeric values compared verbatim (upper/trim)."""
    v = F.upper(F.trim(col.cast("string")))
    v = F.when(v == "", F.lit(None)).otherwise(v)
    stripped = F.regexp_replace(v, r"^0+", "")
    return F.when(
        v.rlike(r"^[0-9]+$"),
        F.when(F.length(stripped) > 0, stripped).otherwise(F.lit("0")),
    ).otherwise(v)


def nhs_norm(col):
    """Shared NHS normalizer - used on BOTH the patient side and the type-18 alias
    bridge (one normalizer, no asymmetry). Accepts digits with space/hyphen
    separators ONLY, then requires exactly 10 digits: an alphanumeric value that
    merely CONTAINS 10 digits is NOT NHS-shaped."""
    v = F.trim(col.cast("string"))
    d = F.regexp_replace(v, r"[ -]", "")
    return F.when(v.rlike(r"^[0-9][0-9 -]*$") & d.rlike(r"^[0-9]{10}$"), d)


def nhs_checksum_ok(norm_col):
    weighted = reduce(
        lambda acc, i: acc + F.substring(norm_col, i + 1, 1).cast("int") * (10 - i),
        range(9), F.lit(0),
    )
    check = 11 - (weighted % 11)
    check = F.when(check == 11, F.lit(0)).otherwise(check)
    return (check != 10) & (check == F.substring(norm_col, 10, 1).cast("int"))

# COMMAND ----------

# Inline tests - fail fast on every run.
_t = spark.createDataFrame(
    [("00364525",), ("364525",), ("RNJ00002128705",), ("0000000",), ("N7098914",),
     (" 943 476 5919 ",), ("AB1234567890",)],
    "v string",
).select(
    F.col("v"),
    norm_identifier(F.col("v")).alias("norm"),
    nhs_norm(F.col("v")).alias("nhs"),
).collect()
_by_v = {r["v"]: r for r in _t}
assert _by_v["00364525"]["norm"] == "364525"
assert _by_v["364525"]["norm"] == "364525"          # zero-padding twins converge
assert _by_v["RNJ00002128705"]["norm"] == "RNJ00002128705"  # non-numeric: verbatim
assert _by_v["0000000"]["norm"] == "0"
assert _by_v["N7098914"]["norm"] == "N7098914"
assert _by_v[" 943 476 5919 "]["nhs"] == "9434765919"
assert _by_v["AB1234567890"]["nhs"] is None         # 10 digits inside alnum != NHS-shaped
_c = spark.createDataFrame([("9434765919",), ("9434765918",)], "v string").select(
    F.col("v"), nhs_checksum_ok(F.col("v")).alias("ok")).collect()
assert [r["ok"] for r in _c] == [True, False]
print("[PACS] identifier normalization tests passed")

# COMMAND ----------

def _alias_bridge(alias_type, person_col, count_col, gates):
    """Unique-match lookup: normalized active alias -> PERSON_ID + candidate count."""
    a = (
        read_pinned(PERSON_ALIAS, gates)
        .where((F.col("PERSON_ALIAS_TYPE_CD") == alias_type) & (F.col("ACTIVE_IND") == 1))
        .withColumn("ALIAS_NORM",
                    nhs_norm(F.col("ALIAS")) if alias_type == NHS_ALIAS_TYPE
                    else norm_identifier(F.col("ALIAS")))
        .where(F.col("ALIAS_NORM").isNotNull())
        .groupBy("ALIAS_NORM")
        .agg(F.countDistinct("PERSON_ID").alias(count_col),
             F.max("PERSON_ID").alias("_pid"))
    )
    return a.withColumn(
        person_col,
        F.when(F.col(count_col) == 1, F.col("_pid").cast("long")),
    ).drop("_pid")


def referenced_patient_ids(gates) -> DataFrame:
    """PatientIds referenced by ANY raw exam or request row in the pinned snapshots -
    the publication scope of the crosswalk. Deliberately the raw superset (includes
    ghost-exam-only and exam-less-request patients): computed from RAW, not the
    curated exam table, so it is resolvable before any fact table is built and always
    covers every PACS_PATIENT_ID the fact tables can publish."""
    e = read_pinned(SRC_EXAMS, gates).select(
        F.col("ExaminationPatientId").cast("long").alias("PACS_PATIENT_ID"))
    r = read_pinned(SRC_REQUESTS, gates).select(
        F.col("RequestPatientId").cast("long").alias("PACS_PATIENT_ID"))
    return e.unionByName(r).where(F.col("PACS_PATIENT_ID").isNotNull()).distinct()


def build_patient_link(gates) -> DataFrame:
    p = read_pinned(SRC_PATIENTS, gates).select(
        F.col("PatientId").cast("long").alias("PACS_PATIENT_ID"),
        F.col("PatientLinkId").cast("long").alias("PACS_PATIENT_LINK_ID"),
        F.col("PatientIssuerId").cast("int").alias("PACS_ISSUER_ID"),
        F.trim(F.col("PatientPersonalId").cast("string")).alias("_pid_raw"),
        F.to_date(F.col("PatientBirthdate")).alias("_dob"),
        F.col("ADC_UPDT").alias("SRC_ADC_UPDT"),
    ).dropDuplicates(["PACS_PATIENT_ID"])

    _nhs10 = nhs_norm(F.col("_pid_raw"))
    p = p.withColumn(
        "IDENTIFIER_KIND",
        F.when(F.col("_pid_raw").isNull() | (F.col("_pid_raw") == ""), "NONE")
         .when(F.col("PACS_ISSUER_ID") == 20, "ANON")
         .when(F.col("PACS_ISSUER_ID") == 4, "NHS")
         .when((F.col("PACS_ISSUER_ID") == -1) & _nhs10.isNotNull() & nhs_checksum_ok(_nhs10), "NHS")
         .otherwise("MRN"),
    )
    # Issuer 1 stores RNJ-prefixed MRNs (raw exact match rate: 157 rows; stripped: 75.5%)
    p = p.withColumn(
        "_mrn_norm",
        F.when(F.col("IDENTIFIER_KIND") == "MRN",
               norm_identifier(F.when(F.col("PACS_ISSUER_ID") == 1,
                                      F.regexp_replace(F.col("_pid_raw"), r"^RNJ", ""))
                                .otherwise(F.col("_pid_raw")))),
    ).withColumn(
        "_nhs_norm",
        F.when(F.col("IDENTIFIER_KIND") == "NHS", _nhs10),
    ).withColumn(
        "NHS_CHECKSUM_VALID_IND",
        F.when(F.col("_nhs_norm").isNotNull(), nhs_checksum_ok(F.col("_nhs_norm"))),
    )

    mrn_bridge = _alias_bridge(MRN_ALIAS_TYPE, "_mrn_pid", "ALIAS_CANDIDATES_MRN", gates) \
        .withColumnRenamed("ALIAS_NORM", "_mrn_norm")
    nhs_bridge = _alias_bridge(NHS_ALIAS_TYPE, "_nhs_pid", "ALIAS_CANDIDATES_NHS", gates) \
        .withColumnRenamed("ALIAS_NORM", "_nhs_norm")
    p = (p.join(mrn_bridge, "_mrn_norm", "left")
          .join(nhs_bridge, "_nhs_norm", "left")
          .withColumn("_direct_pid", F.coalesce(F.col("_mrn_pid"), F.col("_nhs_pid")))
          .withColumn("ALIAS_CANDIDATES",
                      F.coalesce(F.col("ALIAS_CANDIDATES_MRN"), F.col("ALIAS_CANDIDATES_NHS"), F.lit(0)).cast("int"))
          .withColumn(
              "PERSON_MATCH_STATUS",
              F.when(F.col("IDENTIFIER_KIND") == "NONE", "NO_IDENTIFIER")
               .when(F.col("IDENTIFIER_KIND") == "ANON", "ANONYMIZED")
               .when(F.col("_direct_pid").isNotNull(), "MATCHED")
               .when(F.col("ALIAS_CANDIDATES") > 1, "AMBIGUOUS")
               .otherwise("NO_ALIAS_MATCH"),
          ))

    if ENABLE_LINK_GROUP:
        # Within a PatientLinkId group, if the directly-matched members agree on exactly
        # one PERSON_ID, unmatched members inherit it (provenance LINK_GROUP).
        # MUST run over the FULL patient set: a referenced record's identifier-bearing
        # group-mate is often itself unreferenced.
        grp = (p.where(F.col("_direct_pid").isNotNull() & F.col("PACS_PATIENT_LINK_ID").isNotNull())
                .groupBy("PACS_PATIENT_LINK_ID")
                .agg(F.countDistinct("_direct_pid").alias("_grp_n"),
                     F.max("_direct_pid").alias("_grp_pid"))
                .where(F.col("_grp_n") == 1)
                .select("PACS_PATIENT_LINK_ID", "_grp_pid"))
        p = p.join(grp, "PACS_PATIENT_LINK_ID", "left")
    else:
        p = p.withColumn("_grp_pid", F.lit(None).cast("long"))

    p = (p.withColumn(
            "PERSON_ID",
            F.coalesce(F.col("_direct_pid"),
                       F.when(F.col("PERSON_MATCH_STATUS").isin("NO_ALIAS_MATCH", "AMBIGUOUS", "NO_IDENTIFIER"),
                              F.col("_grp_pid"))),
         ).withColumn(
            "PERSON_MATCH_METHOD",
            F.when(F.col("_direct_pid").isNotNull(), "DIRECT")
             .when(F.col("PERSON_ID").isNotNull(), "LINK_GROUP")
             .otherwise("NONE"),
         ))

    # DOB concordance (QA only, never gates the match)
    mp = (read_pinned(MILL_PERSON, gates)
          .groupBy(F.col("PERSON_ID").cast("long").alias("PERSON_ID"))
          .agg(F.max(F.to_date(F.col("BIRTH_DT_TM"))).alias("_mill_dob")))
    p = (p.join(mp, "PERSON_ID", "left")
          .withColumn("DOB_MATCH_IND",
                      F.when(F.col("PERSON_ID").isNotNull() & F.col("_dob").isNotNull()
                             & F.col("_mill_dob").isNotNull(),
                             F.col("_dob") == F.col("_mill_dob"))))

    # Publication scope: referenced records only (matching already used the full set).
    p = p.join(referenced_patient_ids(gates), "PACS_PATIENT_ID", "left_semi")

    return p.select(
        "PACS_PATIENT_ID", "PACS_PATIENT_LINK_ID", "PACS_ISSUER_ID",
        "IDENTIFIER_KIND", "PERSON_ID", "PERSON_MATCH_METHOD", "PERSON_MATCH_STATUS",
        "ALIAS_CANDIDATES", "NHS_CHECKSUM_VALID_IND", "DOB_MATCH_IND", "SRC_ADC_UPDT",
    )


PATIENT_LINK_COMMENTS = {
    "PACS_PATIENT_ID": "Sectra PACS PatientId (verified unique). MERGE key.",
    "PACS_PATIENT_LINK_ID": "Sectra person-merge group id: duplicate patient records of one human share it (4.14M groups / 8.04M records).",
    "PACS_ISSUER_ID": "Identifier issuing system: 2=Barts MRN, 4=NHS number, 1=RNJ-prefixed MRN, 3=Newham legacy, 6=mixed legacy, 23=MRN, 20=anonymized, -1=mixed/junk.",
    "IDENTIFIER_KIND": "MRN | NHS | ANON | NONE - the lane the source identifier was matched through (identifier value itself is not published).",
    "PERSON_ID": "Millennium person via unique active alias (type 10 MRN / 18 NHS, zero-strip normalized) or link-group backfill; NULL when unmatched/ambiguous (row retained).",
    "PERSON_MATCH_METHOD": "DIRECT | LINK_GROUP (inherited from PatientLinkId group with exactly one matched PERSON_ID) | NONE.",
    "PERSON_MATCH_STATUS": "Own-identifier outcome: MATCHED | AMBIGUOUS | NO_ALIAS_MATCH | ANONYMIZED | NO_IDENTIFIER. PERSON_ID may still be set via LINK_GROUP when not MATCHED.",
    "ALIAS_CANDIDATES": "Distinct PERSON_IDs carrying the normalized identifier (1 = unique match; >1 = AMBIGUOUS, never guessed).",
    "NHS_CHECKSUM_VALID_IND": "NHS mod-11 checksum result for NHS-lane identifiers (recorded, not a match gate).",
    "DOB_MATCH_IND": "QA: PACS birthdate equals mill_person birth date for the resolved person.",
    "SRC_ADC_UPDT": "Raw pacs_patients load stamp (CTAS snapshot: uniform per load).",
    "SOURCE_PRESENT_IND": "False when the record disappeared from the latest raw snapshot OR fell out of the referenced scope (soft delete); reactivated on reappearance.",
    "ROW_HASH": "SHA-256 over business columns (excl. ROW_HASH/SOURCE_PRESENT_IND/ADC_UPDT).",
    "ADC_UPDT": "Timestamp this row was last inserted or changed by pacs_pipeline.",
}

_patient_gates = resolve_gates([SRC_PATIENTS, SRC_EXAMS, SRC_REQUESTS, PERSON_ALIAS, MILL_PERSON])
_patient_metrics = {"operation": "SKIP"}
if needs_run(TGT_PATIENT_LINK, _patient_gates):
    _pl = build_patient_link(_patient_gates)
    assert_unique_non_null(_pl, ["PACS_PATIENT_ID"], "map_pacs_patient_link")
    _patient_metrics = pacs_update_table(_pl, TGT_PATIENT_LINK,
        keys=["PACS_PATIENT_ID"], gates=_patient_gates)
else:
    print("[PACS] map_pacs_patient_link: sources unchanged, skipping")
apply_comments(TGT_PATIENT_LINK, PATIENT_LINK_COMMENTS,
    "PACS patient record -> Millennium PERSON_ID crosswalk. One row per Sectra patient "
    "record referenced by ANY raw exam or request (~3.03M of 8.04M; deliberately the "
    "raw superset incl. ghost/exam-less references, so every fact FK is covered; a "
    "human may have several records, grouped by PACS_PATIENT_LINK_ID - matching runs "
    "over the full set before the reference filter). Typed per-issuer linkage against "
    "active mill_person_alias; unique matches only; LINK_GROUP backfill "
    "provenance-marked. No identifiers or demographics are published. Soft deletes "
    "via SOURCE_PRESENT_IND.")

# Shared helper for the fact tables: PERSON_ID + status via the published crosswalk,
# read PINNED at the version the caller's gates recorded - so the build consumes
# exactly the crosswalk version its checkpoint describes even if a concurrent run
# (manual + weekly) writes the table mid-build.
def join_person(df, pacs_patient_col, gates):
    link = read_pinned(TGT_PATIENT_LINK, gates).select(
        F.col("PACS_PATIENT_ID").alias(pacs_patient_col),
        "PERSON_ID", "PERSON_MATCH_STATUS",
    )
    assert verify_unique_key(link, [pacs_patient_col]) == 0
    return df.join(link, pacs_patient_col, "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examinations (curated, evidence-filtered)
# MAGIC One row per PERFORMED exam. Folds together: request context (RequestIdString,
# MAGIC referrer, clinical question/anamnesis), report rollup (count + latest via bridge),
# MAGIC series rollup (measured counts from the 161M-row object table, frozen 2024-11-02)
# MAGIC and archive rollup (folder state/size/access).
# MAGIC
# MAGIC ExaminationAccessionNumber is corrupted upstream: 59.4% literal 'VALUE_TOO_LONG',
# MAGIC 12.5% doubled legacy ('128032128032'). MILL_LINK_REF is the cleaned best-effort
# MAGIC join key to Mill radiology events (mill_clinical_event CONTRIBUTOR_SYSTEM_CD
# MAGIC 6141416, accession parsed from SERIES_REF_NBR/REFERENCE_NBR - the join itself is
# MAGIC a silver concern). Staff name strings excluded; clinical text kept.

# COMMAND ----------

def with_mill_link_ref(df, accn_col, idstr_col, text1_col):
    """Adds MILL_LINK_REF + MILL_LINK_REF_METHOD from a corrupted accession column,
    an id-string column, and a last-resort text column (pacs_dlt-proven chain)."""
    a0 = F.trim(F.col(accn_col).cast("string"))
    a1 = F.when((a0 == "") | (a0 == "VALUE_TOO_LONG"), F.lit(None)).otherwise(a0)
    idstr = F.trim(F.col(idstr_col).cast("string"))
    # The extractor placeholder occurs in ExaminationIdString on a tiny edge-case
    # set as well as in the accession field; never publish it as a usable Mill key.
    idstr = F.when((idstr == "") | (idstr == "VALUE_TOO_LONG"), F.lit(None)).otherwise(idstr)
    t1 = F.trim(F.col(text1_col).cast("string"))
    t1 = F.when(t1 == "", F.lit(None)).otherwise(t1)
    df = (df.withColumn("_a1", a1).withColumn("_idstr", idstr).withColumn("_t1", t1)
            .withColumn("_a2", F.when(
                F.col("_idstr").isNotNull() & F.col("_a1").isNotNull()
                & (F.length("_a1") > F.length("_idstr"))
                & (F.expr("right(_a1, length(_idstr))") == F.col("_idstr")),
                F.expr("left(_a1, length(_a1) - length(_idstr))"),
            ).otherwise(F.col("_a1")))
            .withColumn("_a3", F.when(
                F.col("_a2").isNotNull() & (F.length("_a2") % 2 == 0) & (F.length("_a2") > 0)
                & (F.expr("left(_a2, length(_a2) div 2)") == F.expr("right(_a2, length(_a2) div 2)")),
                F.expr("left(_a2, length(_a2) div 2)"),
            ).otherwise(F.col("_a2"))))
    return (df.withColumn("MILL_LINK_REF", F.coalesce(F.col("_a3"), F.col("_idstr"), F.col("_t1")))
              .withColumn("MILL_LINK_REF_METHOD",
                          F.when(F.col("_a3").isNotNull(), "ACCESSION_CLEANED")
                           .when(F.col("_idstr").isNotNull(), "ID_STRING")
                           .when(F.col("_t1").isNotNull(), "EXAM_TEXT1")
                           .otherwise("NONE"))
              .drop("_a1", "_a2", "_a3", "_idstr", "_t1"))

# COMMAND ----------

# Inline tests for the cleaner - fail fast on every run.
_acc_cases = spark.createDataFrame(
    [
        ("VALUE_TOO_LONG", "UKWXH00011622453", None),        # placeholder -> ID_STRING
        ("VALUE_TOO_LONG", "VALUE_TOO_LONG", None),          # no usable fallback
        ("128032128032", "128032", None),                     # doubled == idstr+idstr -> strip suffix
        ("0074987600749876", "00749876", None),               # doubled legacy
        ("ACC123UKWXH00011622453", "UKWXH00011622453", None), # accession+idstring concat
        ("ACC999", None, None),                               # clean value, no idstr
        (None, None, "FALLBACK1"),                            # nothing but text1
        (None, None, None),                                   # nothing at all
    ],
    "accn string, idstr string, text1 string",
)
_acc = {(r["accn"], r["idstr"], r["text1"]): r
        for r in with_mill_link_ref(_acc_cases, "accn", "idstr", "text1").collect()}
assert _acc[("VALUE_TOO_LONG", "UKWXH00011622453", None)]["MILL_LINK_REF"] == "UKWXH00011622453"
assert _acc[("VALUE_TOO_LONG", "UKWXH00011622453", None)]["MILL_LINK_REF_METHOD"] == "ID_STRING"
assert _acc[("VALUE_TOO_LONG", "VALUE_TOO_LONG", None)]["MILL_LINK_REF"] is None
assert _acc[("VALUE_TOO_LONG", "VALUE_TOO_LONG", None)]["MILL_LINK_REF_METHOD"] == "NONE"
assert _acc[("128032128032", "128032", None)]["MILL_LINK_REF"] == "128032"
assert _acc[("0074987600749876", "00749876", None)]["MILL_LINK_REF"] == "00749876"
assert _acc[("ACC123UKWXH00011622453", "UKWXH00011622453", None)]["MILL_LINK_REF"] == "ACC123"
assert _acc[("ACC123UKWXH00011622453", "UKWXH00011622453", None)]["MILL_LINK_REF_METHOD"] == "ACCESSION_CLEANED"
assert _acc[("ACC999", None, None)]["MILL_LINK_REF"] == "ACC999"
assert _acc[(None, None, "FALLBACK1")]["MILL_LINK_REF_METHOD"] == "EXAM_TEXT1"
assert _acc[(None, None, None)]["MILL_LINK_REF"] is None
assert _acc[(None, None, None)]["MILL_LINK_REF_METHOD"] == "NONE"
print("[PACS] accession cleaner tests passed")

# COMMAND ----------

def build_examinations(gates) -> DataFrame:
    e = read_pinned(SRC_EXAMS, gates).select(
        F.col("ExaminationId").cast("long").alias("PACS_EXAMINATION_ID"),
        F.col("ExaminationRequestId").cast("long").alias("PACS_REQUEST_ID"),
        F.col("ExaminationPatientId").cast("long").alias("PACS_PATIENT_ID"),
        F.col("ExaminationIdString").alias("EXAMINATION_ID_STRING"),
        F.col("ExaminationAccessionNumber").alias("_accn_raw"),
        F.col("ExaminationText1").alias("_exam_text1"),
        F.col("ExaminationStudyUid").alias("STUDY_INSTANCE_UID"),
        F.col("ExaminationDate").alias("EXAMINATION_DT_TM"),
        F.col("ExaminationArrivalTime").alias("ARRIVAL_DT_TM"),
        F.col("ExaminationStatus").cast("int").alias("EXAMINATION_STATUS_CD"),
        F.col("ExaminationCode").alias("EXAMINATION_CODE"),
        F.col("ExaminationDescription").alias("EXAMINATION_DESCRIPTION"),
        F.col("ExaminationModality").alias("MODALITY"),
        F.col("ExaminationBodyPart").alias("BODY_PART"),
        F.col("ExaminationInstitution").alias("INSTITUTION"),
        F.col("ExaminationStat").cast("int").alias("STAT_PRIORITY_CD"),
        F.col("ExaminationReadingPhysician").cast("long").alias("READING_PHYSICIAN_ID"),
        F.col("ExaminationComments").alias("EXAMINATION_COMMENTS"),
        F.col("ExaminationDoseInformation").alias("DOSE_INFORMATION"),
        F.col("ExaminationSeriesCount").cast("int").alias("SERIES_COUNT"),
        F.col("ExaminationImageCount").cast("int").alias("IMAGE_COUNT"),
        F.col("ADC_UPDT").alias("SRC_ADC_UPDT"),
    )
    e = with_mill_link_ref(e, "_accn_raw", "EXAMINATION_ID_STRING", "_exam_text1")
    e = e.drop("_accn_raw", "_exam_text1")

    # Request context folded on (94.0% of requests have exactly one exam; multi-exam
    # requests legitimately repeat their question/anamnesis on each exam row).
    req = read_pinned(SRC_REQUESTS, gates).select(
        F.col("RequestId").cast("long").alias("PACS_REQUEST_ID"),
        F.col("RequestIdString").alias("REQUEST_ID_STRING"),
        F.col("RequestReferringUnit").alias("REFERRING_UNIT"),
        F.col("RequestRisHostId").alias("RIS_HOST_ID"),
        F.col("RequestQuestion").alias("CLINICAL_QUESTION"),
        F.col("RequestAnamnesis").alias("CLINICAL_ANAMNESIS"),
    )
    assert verify_unique_key(req, ["PACS_REQUEST_ID"]) == 0
    e = e.join(req, "PACS_REQUEST_ID", "left")

    # Report rollup via the bridge. REPORT_COUNT counts ALL linked ReportIds (incl.
    # ~1.05M ids dangling against the frozen reports mirror - a dangling link still
    # evidences the exam was reported); LATEST_* resolve only against real report rows.
    br = (read_pinned(SRC_EXAM_REPORTS, gates)
          .where(F.col("ExaminationReportExaminationId").isNotNull())
          .select(F.col("ExaminationReportExaminationId").cast("long").alias("PACS_EXAMINATION_ID"),
                  F.col("ExaminationReportReportId").cast("long").alias("_rid")))
    rep = read_pinned(SRC_REPORTS, gates).select(
        F.col("ReportId").cast("long").alias("_rid"),
        F.col("ReportDate").alias("_rdate"),
        F.lit(True).alias("_resolved"))
    # LATEST_* are RESOLVED-ONLY (a real report row must exist; the _resolved marker,
    # not _rdate nullability, decides - reports can carry NULL ReportDate). Dangling
    # bridge ids still count in REPORT_COUNT as reporting evidence.
    rollup = (br.join(rep, "_rid", "left")
              .groupBy("PACS_EXAMINATION_ID")
              .agg(F.countDistinct("_rid").cast("int").alias("REPORT_COUNT"),
                   F.max(F.when(F.col("_resolved"), F.struct(
                       F.coalesce(F.col("_rdate"), F.to_timestamp(F.lit("1000-01-01"))).alias("d"),
                       F.col("_rid").alias("i")))).alias("_latest"),
                   F.max(F.when(F.col("_resolved"), F.col("_rdate"))).alias("LATEST_REPORT_DT_TM")))
    rollup = rollup.select("PACS_EXAMINATION_ID", "REPORT_COUNT",
                           F.col("_latest.i").alias("LATEST_REPORT_ID"),
                           "LATEST_REPORT_DT_TM")
    e = (e.join(rollup, "PACS_EXAMINATION_ID", "left")
          .withColumn("REPORT_COUNT", F.coalesce(F.col("REPORT_COUNT"), F.lit(0)).cast("int")))

    # Series rollup: measured counts from the object-grain table (multiple rows per
    # SeriesInstanceUid). Coverage ends at the 2024-11-02 series freeze - NULL for
    # exams performed after it (native SERIES_COUNT/IMAGE_COUNT may still be set).
    ser = (read_pinned(SRC_SERIES, gates)
           .groupBy(F.col("SeriesExaminationId").cast("long").alias("PACS_EXAMINATION_ID"))
           .agg(F.countDistinct("SeriesSeriesInstanceUid").cast("int").alias("SERIES_COUNT_MEASURED"),
                F.count(F.lit(1)).cast("int").alias("SERIES_OBJECT_COUNT"),
                (F.max("SeriesPixelDataFlag") > 0).alias("SERIES_PIXEL_DATA_IND")))
    e = e.join(ser, "PACS_EXAMINATION_ID", "left")

    # Archive rollup: storage/lifecycle signal for image-retrieval feasibility
    # (state of the most recently modified folder; sizes summed across folders).
    fl = (read_pinned(SRC_EXAM_FOLDERS, gates)
          .groupBy(F.col("ExaminationFolderExaminationId").cast("long").alias("PACS_EXAMINATION_ID"))
          .agg(F.count(F.lit(1)).cast("int").alias("FOLDER_COUNT"),
               F.sum("ExaminationFolderExamSize").cast("long").alias("STORED_SIZE_BYTES"),
               F.max("ExaminationFolderAccessedDate").alias("LAST_ACCESSED_DT_TM"),
               F.max(F.struct(
                   F.coalesce(F.col("ExaminationFolderModifyDate"),
                              F.to_timestamp(F.lit("1000-01-01"))).alias("d"),
                   F.col("ExaminationFolderArchiveState").cast("int").alias("s"))).alias("_arch")))
    fl = fl.select("PACS_EXAMINATION_ID", "FOLDER_COUNT", "STORED_SIZE_BYTES",
                   "LAST_ACCESSED_DT_TM", F.col("_arch.s").alias("ARCHIVE_STATE_CD"))
    e = e.join(fl, "PACS_EXAMINATION_ID", "left")

    # Evidence filter: publish only exams with evidence of performed imaging.
    # Booking/cancelled ghosts (~2.7M rows incl. the post-halt pre-booked bucket,
    # status 40) never happened - they fabricate imaging events downstream.
    _signals = [
        (F.col("EXAMINATION_STATUS_CD").isin(*PERFORMED_STATUS_CDS), "STATUS"),
        (F.col("REPORT_COUNT") > 0, "REPORT"),
        (F.col("IMAGE_COUNT") > 0, "IMAGES"),
        (F.col("SERIES_OBJECT_COUNT") > 0, "SERIES"),
    ]
    e = e.withColumn(
        "PERFORMED_EVIDENCE",
        F.concat_ws(",", *[F.when(cond, F.lit(tag)) for cond, tag in _signals]))
    e = e.where(F.col("PERFORMED_EVIDENCE") != "")

    return join_person(e, "PACS_PATIENT_ID", gates)


EXAMINATION_COMMENTS = {
    "PACS_EXAMINATION_ID": "Sectra ExaminationId (verified unique). MERGE key.",
    "PACS_REQUEST_ID": "Sectra RequestId of the originating request (context folded onto this row; no separate request table).",
    "PACS_PATIENT_ID": "FK to map_pacs_patient_link.",
    "PERSON_ID": "Millennium person via map_pacs_patient_link; NULL when unmatched (row retained).",
    "PERSON_MATCH_STATUS": "Crosswalk status for PACS_PATIENT_ID (MATCHED/AMBIGUOUS/NO_ALIAS_MATCH/ANONYMIZED/NO_IDENTIFIER; NULL = patient row absent).",
    "EXAMINATION_ID_STRING": "Sectra exam id string (UK+site+11 digits on 9.67M rows; legacy numerics otherwise). NOT unique.",
    "REQUEST_ID_STRING": "Sectra request id string - the request-side Mill accession join key (pacs_dlt-proven: RequestIdString = parsed Mill accession).",
    "MILL_LINK_REF": "Cleaned best-effort reference for joining Mill radiology events (BLT_TIE_RAD accession domain); see MILL_LINK_REF_METHOD. Raw ExaminationAccessionNumber is NOT published (59.4% literal VALUE_TOO_LONG, 12.5% doubled legacy).",
    "MILL_LINK_REF_METHOD": "ACCESSION_CLEANED | ID_STRING | EXAM_TEXT1 | NONE - provenance of MILL_LINK_REF.",
    "STUDY_INSTANCE_UID": "DICOM StudyInstanceUID (near-unique; the PACS image-retrieval key).",
    "EXAMINATION_DT_TM": "Examination timestamp. Source carries junk extremes (1753-01-03..2099-01-01) - preserved, not cleaned.",
    "ARRIVAL_DT_TM": "Patient arrival time where recorded.",
    "EXAMINATION_STATUS_CD": "Sectra lifecycle status. Performed set = 100/110/75/83; other codes appear only when another evidence signal fired (see PERFORMED_EVIDENCE).",
    "PERFORMED_EVIDENCE": "Why this row is published: comma-set of STATUS (performed status code), REPORT (>=1 bridge report link), IMAGES (native ImageCount>0), SERIES (series objects exist). Booking/cancelled ghosts with no signal are excluded from bronze (raw retains them).",
    "EXAMINATION_COMMENTS": "Clinical/operational free text, verbatim.",
    "DOSE_INFORMATION": "Radiation dose free text, verbatim.",
    "REFERRING_UNIT": "Referring unit of the originating request (referring physician NAME excluded - staff PII).",
    "RIS_HOST_ID": "Originating RIS feed of the request: BROKER-RISe 8.7M | MDI-WHIPPS-MIGRATION 2.5M | NBSS 1.0M | NEWHAM | BLT | others.",
    "CLINICAL_QUESTION": "Request clinical question blob, VERBATIM (multi-exam requests repeat it on each exam row; sections delimited by '----- <examcode> ------').",
    "CLINICAL_ANAMNESIS": "Request clinical information blob, VERBATIM (same repetition/delimiter convention).",
    "REPORT_COUNT": "Distinct ReportIds linked via the exam-report bridge (incl. ids dangling vs the frozen reports mirror - reporting evidence). >1 = addenda/amendments; only the latest is pointed to here - full m:n history stays in raw.",
    "LATEST_REPORT_ID": "RESOLVED-ONLY FK to map_pacs_report: the linked report row with the latest ReportDate (ties -> highest id). NULL when no linked ReportId resolves to a report row (REPORT_COUNT may still be >0 via dangling links).",
    "LATEST_REPORT_DT_TM": "Latest ReportDate among RESOLVED linked reports.",
    "SERIES_COUNT": "Native Sectra series count (sparsely populated; see SERIES_COUNT_MEASURED).",
    "IMAGE_COUNT": "Native Sectra image count (sparsely populated).",
    "SERIES_COUNT_MEASURED": "Distinct DICOM SeriesInstanceUids measured from the raw series object table. Coverage ends 2024-11-02 (series feed freeze): NULL after that.",
    "SERIES_OBJECT_COUNT": "Series object/order rows measured from the raw series table (same 2024-11-02 coverage limit).",
    "SERIES_PIXEL_DATA_IND": "True when any series object carries pixel data (same coverage limit).",
    "FOLDER_COUNT": "Archive folder rows for this exam (97.2% single).",
    "STORED_SIZE_BYTES": "Total stored study size in bytes across folders - image-retrieval feasibility signal.",
    "LAST_ACCESSED_DT_TM": "Most recent archive-folder access.",
    "ARCHIVE_STATE_CD": "Sectra archive/ILM state of the most recently modified folder (99 dominates; no landed decode table).",
    "SRC_ADC_UPDT": "Raw exam row load timestamp (content frozen ~2026-04-18; upstream feed halt).",
    "SOURCE_PRESENT_IND": "False when the exam disappeared from the raw mirror or lost all performed-evidence (soft delete).",
    "ROW_HASH": "SHA-256 over business columns (excl. ROW_HASH/SOURCE_PRESENT_IND/ADC_UPDT).",
    "ADC_UPDT": "Timestamp this row was last inserted or changed by pacs_pipeline.",
}

_exam_gates = resolve_gates([SRC_EXAMS, SRC_REQUESTS, SRC_EXAM_REPORTS, SRC_REPORTS,
                             SRC_SERIES, SRC_EXAM_FOLDERS, TGT_PATIENT_LINK])
_exam_metrics = {"operation": "SKIP"}
if needs_run(TGT_EXAMINATION, _exam_gates):
    _ex = build_examinations(_exam_gates)
    assert_unique_non_null(_ex, ["PACS_EXAMINATION_ID"], "map_pacs_examination")
    _exam_metrics = pacs_update_table(_ex, TGT_EXAMINATION,
        keys=["PACS_EXAMINATION_ID"], gates=_exam_gates)
else:
    print("[PACS] map_pacs_examination: sources unchanged, skipping")
apply_comments(TGT_EXAMINATION, EXAMINATION_COMMENTS,
    "PACS examinations (Sectra), CURATED: one row per exam with evidence of performed "
    "imaging (~16.36M of 17.78M raw on 2026-08-08; PERFORMED_EVIDENCE records why, raw retains the "
    "excluded booking/cancelled ghosts). Request context, report rollup, series rollup "
    "(measured counts, coverage to 2024-11-02) and archive rollup are folded on - no "
    "separate request/bridge/folder/series bronze tables. Excludes staff name strings "
    "and unreliable ingest metadata (ADC_Deleted - see 2026-08-01 mass-flag incident). "
    "Upstream feed halted ~2026-04-18. Soft deletes via SOURCE_PRESENT_IND.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reports (all kept; request/exam/person resolved through the bridge)
# MAGIC pacs_reports has NO native request/patient FK (100% NULL at source) - the bridge is
# MAGIC the only structural path. Over the 14.57M report rows: 99.1% resolve to a single
# MAGIC request; exam links = 67.8% SINGLE (denormalized here) / 23.1% NONE (request-level
# MAGIC attachment) / 9.1% MULTIPLE (EXAM_LINK_COUNT recorded, full m:n stays in raw).
# MAGIC Person = the single distinct patient across ALL linked requests (covers
# MAGIC multi-request reports whose requests agree).

# COMMAND ----------

def build_reports(gates) -> DataFrame:
    r = read_pinned(SRC_REPORTS, gates).select(
        F.col("ReportId").cast("long").alias("PACS_REPORT_ID"),
        F.col("ReportIdString").alias("REPORT_ID_STRING"),
        F.col("ReportRisId").alias("REPORT_RIS_ID"),
        F.col("ReportDate").alias("REPORT_DT_TM"),
        F.col("ReportModifiedDateUTC").alias("REPORT_MODIFIED_UTC"),
        F.col("ReportText").alias("REPORT_TEXT"),
        F.col("ReportStatus").cast("int").alias("REPORT_STATUS_CD"),
        F.col("ReportDoctorId").cast("long").alias("REPORT_DOCTOR_ID"),
        F.col("ReportPreliminarySignatureDate").alias("PRELIM_SIGNATURE_DT_TM"),
        F.col("ReportFinalSignatureDate").alias("FINAL_SIGNATURE_DT_TM"),
        F.col("ADC_UPDT").alias("SRC_ADC_UPDT"),
    ).withColumn(
        "REPORT_TEXT_FORMAT",
        F.when(F.col("REPORT_TEXT").isNull() | (F.trim(F.col("REPORT_TEXT")) == ""), F.lit(None))
         .when(F.trim(F.col("REPORT_TEXT")).startswith(builtins.chr(123) + builtins.chr(92) + "rtf"), "RTF")
         .otherwise("PLAIN"),
    )

    bridge = read_pinned(SRC_EXAM_REPORTS, gates).select(
        F.col("ExaminationReportReportId").cast("long").alias("PACS_REPORT_ID"),
        F.col("ExaminationReportRequestId").cast("long").alias("_req"),
        F.col("ExaminationReportExaminationId").cast("long").alias("_exam"))

    # Single-request derivation: n==1 -> carry it; else status only (99.1% SINGLE).
    per_req = (bridge.groupBy("PACS_REPORT_ID")
               .agg(F.countDistinct("_req").alias("_n_req"), F.max("_req").alias("_req1")))
    r = (r.join(per_req, "PACS_REPORT_ID", "left")
          .withColumn("PACS_REQUEST_ID", F.when(F.col("_n_req") == 1, F.col("_req1")))
          .withColumn("REQUEST_LINK_STATUS",
                      F.when(F.col("_n_req") == 1, "SINGLE")
                       .when(F.col("_n_req") > 1, "MULTIPLE")
                       .otherwise("NONE"))
          .drop("_n_req", "_req1"))

    # Exam resolution: single distinct non-null exam -> denormalize; else count only.
    per_exam = (bridge.where(F.col("_exam").isNotNull())
                .groupBy("PACS_REPORT_ID")
                .agg(F.countDistinct("_exam").alias("_n_ex"), F.max("_exam").alias("_ex1")))
    r = (r.join(per_exam, "PACS_REPORT_ID", "left")
          .withColumn("EXAM_LINK_COUNT", F.coalesce(F.col("_n_ex"), F.lit(0)).cast("int"))
          .withColumn("PACS_EXAMINATION_ID", F.when(F.col("_n_ex") == 1, F.col("_ex1")))
          .drop("_n_ex", "_ex1"))

    # Person via ALL linked requests: resolves when they agree on one patient
    # (also covers MULTIPLE-request reports; reports have no native patient FK).
    req_pat = read_pinned(SRC_REQUESTS, gates).select(
        F.col("RequestId").cast("long").alias("_req"),
        F.col("RequestPatientId").cast("long").alias("_pat"))
    pat = (bridge.select("PACS_REPORT_ID", "_req").distinct()
           .join(req_pat, "_req", "left")
           .where(F.col("_pat").isNotNull())
           .groupBy("PACS_REPORT_ID")
           .agg(F.countDistinct("_pat").alias("_n_pat"), F.max("_pat").alias("_pat1")))
    r = (r.join(pat, "PACS_REPORT_ID", "left")
          .withColumn("PACS_PATIENT_ID", F.when(F.col("_n_pat") == 1, F.col("_pat1")))
          .drop("_n_pat", "_pat1"))

    # Denormalize the resolved exam's when/what/Mill-link (only meaningful single-exam;
    # read the JUST-PUBLISHED curated exam table at the pinned version).
    ex = (read_pinned(TGT_EXAMINATION, gates)
          .where(F.col("SOURCE_PRESENT_IND"))
          .select(F.col("PACS_EXAMINATION_ID"),
                  F.col("EXAMINATION_DT_TM").alias("EXAM_DT_TM"),
                  F.col("EXAMINATION_CODE").alias("EXAM_CODE"),
                  F.col("MODALITY").alias("EXAM_MODALITY"),
                  F.col("MILL_LINK_REF")))
    assert verify_unique_key(ex, ["PACS_EXAMINATION_ID"]) == 0
    r = r.join(ex, "PACS_EXAMINATION_ID", "left")

    return join_person(r, "PACS_PATIENT_ID", gates)


REPORT_COMMENTS = {
    "PACS_REPORT_ID": "Sectra ReportId (verified unique). MERGE key.",
    "REPORT_RIS_ID": "RIS-side report id: GUID 53.9% | UK-request-style 7.0% (matches ExaminationIdString at 92.6%) | legacy ids ~39%.",
    "REPORT_TEXT": "Native Sectra report body VERBATIM - RTF or plain text (see REPORT_TEXT_FORMAT). Populated only from ~2024 onwards (6.3% of rows; 93.7% NULL - pre-2024 narratives live in Mill RADRPT blobs). NET-NEW where present: the legacy 4_prod.pacs.imaging_report text columns are 100% NULL.",
    "REPORT_TEXT_FORMAT": "RTF (leading '{rtf' control sequence) | PLAIN | NULL (empty). RTF stripping is deliberately NOT done at bronze (striprtf word-fusion trap).",
    "PACS_REQUEST_ID": "Derived: the single distinct RequestId in the exam-report bridge; NULL when REQUEST_LINK_STATUS is MULTIPLE or NONE.",
    "REQUEST_LINK_STATUS": "SINGLE | MULTIPLE | NONE - bridge request multiplicity (reports have no native request/patient FK; source columns are 100% NULL).",
    "PACS_EXAMINATION_ID": "Derived: the single distinct non-null ExaminationId in the bridge; NULL when EXAM_LINK_COUNT is 0 or >1. FK to map_pacs_examination.",
    "EXAM_LINK_COUNT": "Distinct exams this report covers via the bridge. Measured 2026-08-08 over 14.57M reports: 1 = 67.8%, 0 = 23.1% (request-level attachment only), >1 = 9.1% (group report - full m:n stays in raw).",
    "EXAM_DT_TM": "Denormalized from the resolved exam (single-exam reports only).",
    "EXAM_CODE": "Denormalized from the resolved exam.",
    "EXAM_MODALITY": "Denormalized from the resolved exam.",
    "MILL_LINK_REF": "Denormalized from the resolved exam - cleaned Mill radiology-event join reference.",
    "PACS_PATIENT_ID": "Derived: the single distinct RequestPatientId across ALL linked requests; NULL when they disagree or none resolve.",
    "PERSON_ID": "Millennium person via map_pacs_patient_link over the derived patient; NULL when unmatched (row retained).",
    "PERSON_MATCH_STATUS": "Crosswalk status for PACS_PATIENT_ID.",
    "REPORT_DOCTOR_ID": "Numeric reporting-doctor id (name/login strings excluded).",
    "SRC_ADC_UPDT": "Raw row load timestamp (content frozen ~2026-04-18).",
    "SOURCE_PRESENT_IND": "False when the ReportId disappeared from the raw mirror (soft delete).",
    "ROW_HASH": "SHA-256 over business columns (excl. ROW_HASH/SOURCE_PRESENT_IND/ADC_UPDT).",
    "ADC_UPDT": "Timestamp this row was last inserted or changed by pacs_pipeline.",
}

_report_gates = resolve_gates([SRC_REPORTS, SRC_EXAM_REPORTS, SRC_REQUESTS,
                               TGT_EXAMINATION, TGT_PATIENT_LINK])
_report_metrics = {"operation": "SKIP"}
if needs_run(TGT_REPORT, _report_gates):
    _rp = build_reports(_report_gates)
    assert_unique_non_null(_rp, ["PACS_REPORT_ID"], "map_pacs_report")
    _report_metrics = pacs_update_table(_rp, TGT_REPORT,
        keys=["PACS_REPORT_ID"], gates=_report_gates)
else:
    print("[PACS] map_pacs_report: sources unchanged, skipping")
apply_comments(TGT_REPORT, REPORT_COMMENTS,
    "PACS radiology reports (Sectra), one row per ReportId (all kept - every row is a "
    "real authored report), native report text verbatim (RTF or plain; ~2024+ only, "
    "93.7% NULL - net-new where present). Request, exam and person resolved through "
    "the exam-report bridge (single-link denormalized, multiplicity recorded; full m:n "
    "stays in raw). Excludes staff name/login strings, ReportLegacySignature, "
    "ReportMetadata and ReportFeedback. Soft deletes via SOURCE_PRESENT_IND.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation and run summary

# COMMAND ----------

_summary = {"pipeline": "pacs_pipeline", "status": "SUCCESS", "run_id": PACS_RUN_ID,
            "target_schema": TARGET_SCHEMA, "started_at": _STARTED_AT,
            "force_full_refresh": FORCE_FULL_REFRESH,
            "link_group_backfill": ENABLE_LINK_GROUP,
            "metrics": {"patient_link": _patient_metrics,
                        "examination": _exam_metrics,
                        "report": _report_metrics}}

_ALL_TARGETS = {
    TGT_PATIENT_LINK: ("PACS_PATIENT_ID", SRC_PATIENTS),
    TGT_EXAMINATION: ("PACS_EXAMINATION_ID", SRC_EXAMS),
    TGT_REPORT: ("PACS_REPORT_ID", SRC_REPORTS),
}
# Curated-scope floors (~5-8% under expected published counts; a collapse below means
# a scope/join defect even when parity passes).
PUBLISHED_FLOORS = {
    TGT_PATIENT_LINK: 2_800_000,   # expected ~3.03M referenced patient records
    TGT_EXAMINATION: 14_000_000,   # safety floor; 2026-08-08 v3 evidence union produced 16.36M
    TGT_REPORT: 14_100_000,        # expected 14.57M (all reports kept)
}

# 1. Banned-column leak audit: excluded identifier/staff/bookkeeping columns must be absent.
_banned = {
    "patientname", "patientpersonalid", "patientbirthdate", "patientsex",
    "patientdateofdeath", "patientdeceased", "patientalert",
    "examinationphysician", "examinationreportrecipientname",
    "requestreferringphysician", "reportdoctor", "reportdoctorlogin",
    "reportpreliminarysignaturelogin", "reportpreliminarysignaturename",
    "reportfinalsignaturelogin", "reportfinalsignaturename", "reportreviewername",
    "reportsecretaryname", "reportsignlockname", "seriesstatuschangedbylogin",
    "seriesstatuschangedbyname", "reportlegacysignature", "reportmetadata",
    "reportfeedback", "adc_deleted", "adc_mill_person_alias_updt_dt_tm",
    "accession_nbr_raw", "_accn_raw", "_exam_text1", "_pid_raw", "_mrn_norm", "_nhs_norm",
}
for _t in _ALL_TARGETS:
    _leak = _banned & {c.lower() for c in spark.table(_t).columns}
    assert not _leak, f"{_t} leaks excluded columns: {_leak}"

# 2. Key uniqueness + exact present-row parity vs the checkpoint. source_rows on the
#    first source records the rows PUBLISHED from the pinned snapshot (post-curation),
#    so parity holds for curated targets and for SKIPPED targets alike; an upstream
#    load landing mid-run cannot fail it spuriously.
for _t, (_key, _src) in _ALL_TARGETS.items():
    _dups = (spark.table(_t).groupBy(_key).count().where(F.col("count") > 1).count())
    assert _dups == 0, f"{_t}: duplicate {_key}"
    _present = spark.table(_t).where(F.col("SOURCE_PRESENT_IND")).count()
    _ckpt = stored_state(_t).get(_src)
    assert _ckpt is not None and _ckpt["source_rows"] is not None, f"{_t}: no checkpoint for {_src}"
    assert _present == _ckpt["source_rows"], (
        f"{_t}: present rows {_present} != checkpointed published rows {_ckpt['source_rows']}")
    assert _present >= PUBLISHED_FLOORS[_t], (
        f"{_t}: present rows {_present} below curated floor {PUBLISHED_FLOORS[_t]}")
    _absent = spark.table(_t).where(~F.col("SOURCE_PRESENT_IND")).count()
    _summary.setdefault("rows", {})[_t] = {"present": int(_present), "soft_deleted": int(_absent)}

# 3. Person-linkage floors. v3.1 deliberately widened map_pacs_patient_link to every
#    patient referenced by ANY raw exam/request, including 177k raw-reference-only rows
#    that no published fact consumes. Gate the patient crosswalk on distinct patient ids
#    actually referenced by a published exam/report; report the broader superset rate too.
_pl_all_rate = (spark.table(TGT_PATIENT_LINK).where(F.col("SOURCE_PRESENT_IND"))
                .agg(F.avg(F.when(F.col("PERSON_ID").isNotNull(), 1.0).otherwise(0.0))).collect()[0][0])
_fact_patient_ids = (
    spark.table(TGT_EXAMINATION)
    .where(F.col("SOURCE_PRESENT_IND") & F.col("PACS_PATIENT_ID").isNotNull())
    .select("PACS_PATIENT_ID")
    .unionByName(
        spark.table(TGT_REPORT)
        .where(F.col("SOURCE_PRESENT_IND") & F.col("PACS_PATIENT_ID").isNotNull())
        .select("PACS_PATIENT_ID")
    )
    .distinct()
)
_pl_rate = (
    spark.table(TGT_PATIENT_LINK)
    .where(F.col("SOURCE_PRESENT_IND"))
    .join(_fact_patient_ids, "PACS_PATIENT_ID", "inner")
    .agg(F.avg(F.when(F.col("PERSON_ID").isNotNull(), 1.0).otherwise(0.0)))
    .collect()[0][0]
)
_ex_rate = (spark.table(TGT_EXAMINATION).where(F.col("SOURCE_PRESENT_IND"))
            .agg(F.avg(F.when(F.col("PERSON_ID").isNotNull(), 1.0).otherwise(0.0))).collect()[0][0])
_rp_rate = (spark.table(TGT_REPORT).where(F.col("SOURCE_PRESENT_IND"))
            .agg(F.avg(F.when(F.col("PERSON_ID").isNotNull(), 1.0).otherwise(0.0))).collect()[0][0])
_summary["person_rates"] = {k: builtins.round(float(v or 0), 4) for k, v in
    {"patient_fact_referenced": _pl_rate, "patient_all_raw_references": _pl_all_rate,
     "examination": _ex_rate, "report": _rp_rate}.items()}
assert _pl_rate >= 0.85, f"fact-referenced patient-link rate {_pl_rate:.3f} below floor 0.85"
assert _ex_rate >= 0.90, f"examination person rate {_ex_rate:.3f} below floor 0.90"
assert _rp_rate >= 0.80, f"report person rate {_rp_rate:.3f} below floor 0.80"

# 4. Curated-scope accounting (informational): raw exams vs published + evidence mix.
#    The raw row count comes from the SAME pinned fingerprint the build used.
_raw_exam_rows = int(_exam_gates[SRC_EXAMS]["fingerprint"].split("|")[0])
_summary["examination_scope"] = {
    "raw_examination_rows": _raw_exam_rows,
    "published_present": _summary["rows"][TGT_EXAMINATION]["present"],
    "ghost_rows_excluded": _raw_exam_rows - _summary["rows"][TGT_EXAMINATION]["present"],
    "evidence": {r["PERFORMED_EVIDENCE"]: r["n"] for r in
                 spark.table(TGT_EXAMINATION).where(F.col("SOURCE_PRESENT_IND"))
                 .groupBy("PERFORMED_EVIDENCE").agg(F.count(F.lit(1)).alias("n")).collect()},
}
_raw_patient_rows = int(_patient_gates[SRC_PATIENTS]["fingerprint"].split("|")[0])
_summary["patient_link_scope"] = {
    "raw_patient_rows": _raw_patient_rows,
    "published_present": _summary["rows"][TGT_PATIENT_LINK]["present"],
    "unreferenced_excluded": _raw_patient_rows - _summary["rows"][TGT_PATIENT_LINK]["present"],
}

# 5. Informational link/text mixes (report, never assert).
_summary["report_text_format"] = {
    (r["REPORT_TEXT_FORMAT"] or "NULL"): r["n"]
    for r in spark.table(TGT_REPORT).where(F.col("SOURCE_PRESENT_IND"))
        .groupBy("REPORT_TEXT_FORMAT").agg(F.count(F.lit(1)).alias("n")).collect()}
_summary["mill_link_ref_method"] = {
    r["MILL_LINK_REF_METHOD"]: r["n"]
    for r in spark.table(TGT_EXAMINATION).where(F.col("SOURCE_PRESENT_IND"))
        .groupBy("MILL_LINK_REF_METHOD").agg(F.count(F.lit(1)).alias("n")).collect()}
_summary["request_link_status"] = {
    r["REQUEST_LINK_STATUS"]: r["n"]
    for r in spark.table(TGT_REPORT).where(F.col("SOURCE_PRESENT_IND"))
        .groupBy("REQUEST_LINK_STATUS").agg(F.count(F.lit(1)).alias("n")).collect()}
_summary["report_exam_link"] = {
    r["b"]: r["n"]
    for r in spark.table(TGT_REPORT).where(F.col("SOURCE_PRESENT_IND"))
        .groupBy(F.when(F.col("EXAM_LINK_COUNT") == 0, "NONE")
                  .when(F.col("EXAM_LINK_COUNT") == 1, "SINGLE")
                  .otherwise("MULTIPLE").alias("b"))
        .agg(F.count(F.lit(1)).alias("n")).collect()}

# 6. Referential integrity (informational - orphans are expected and PRESERVED, never
#    asserted). Both sides filtered to present rows.
def _orphan_count(child_tbl, child_col, parent_tbl, parent_col):
    c = (spark.table(child_tbl)
         .where(F.col("SOURCE_PRESENT_IND") & F.col(child_col).isNotNull())
         .select(F.col(child_col).alias("k")))
    p = (spark.table(parent_tbl)
         .where(F.col("SOURCE_PRESENT_IND"))   # soft-deleted parents are NOT present
         .select(F.col(parent_col).alias("k")))
    return c.join(p, "k", "left_anti").count()

_summary["referential_orphans"] = {
    "report_to_examination": _orphan_count(TGT_REPORT, "PACS_EXAMINATION_ID", TGT_EXAMINATION, "PACS_EXAMINATION_ID"),
    "examination_to_patient_link": _orphan_count(TGT_EXAMINATION, "PACS_PATIENT_ID", TGT_PATIENT_LINK, "PACS_PATIENT_ID"),
    "examination_latest_report": _orphan_count(TGT_EXAMINATION, "LATEST_REPORT_ID", TGT_REPORT, "PACS_REPORT_ID"),
}
# Expected as of 2026-08-08: report->exam small (single-exam ids absent from the frozen
# exams mirror; bridge->exams resolves 97.31%) | exam->patient_link <= ~11k (referenced
# PatientIds missing from pacs_patients entirely) | exam latest-report EXPECTED 0
# (LATEST_REPORT_ID is resolved-only as of v3.1). Investigate large DRIFT between runs,
# not the absolute numbers.

_summary["completed_at"] = bronze_utc_now()
audit(None, "RUN_SUCCESS", _summary)
release_run_lock()
print(bronze_json(_summary))
dbutils.notebook.exit(bronze_json(_summary))

