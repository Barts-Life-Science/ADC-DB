# Databricks notebook source
# B13 — DICOM pilot tag curation
#
# Owned scope:
#   /Workspace/Shared/PM-DE-Misc/Ben/Python/CC/BronzeCompleteness/dicom_tag_pipeline
#   8_dev.bronze.map_dicom_file_attribute
#   run-scoped 8_dev.bronze.s6_dicom_* scratch objects
#
# Pure Python notebook: no %run and no production writes.
# The source is a deep PILOT slice, not estate-wide DICOM coverage.

# COMMAND ----------

import json
import re
import time
import traceback
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.window import Window

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 412269051825675)
import json

# Prod-idiom target resolution (house pattern: jac_pipeline/endobase_pipeline).
def _widget_text(name, default):
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    try:
        v = dbutils.widgets.get(name)
    except Exception:
        v = default
    return (v or default).strip()

TARGET_SCHEMA = _widget_text("target_schema", "8_dev.bronze")
ALLOW_PROD_WRITE = _widget_text("allow_production_write", "false").lower() == "true"
assert TARGET_SCHEMA.startswith("8_dev.") or ALLOW_PROD_WRITE, (
    f"Refusing to write {TARGET_SCHEMA} without allow_production_write=true")

def _control_schema(target):
    return "6_mgmt.bronze" if target == "4_prod.bronze" else target

CONTROL_SCHEMA = _control_schema(TARGET_SCHEMA)

SOURCE_TAGS = "4_prod.dicom_tags.extracted_dicom_tags"
SOURCE_TIER = "8_dev.pacs.dicom_pii_tier"
SOURCE_EXAM = "4_prod.bronze.map_pacs_examination"
TARGET = f"{TARGET_SCHEMA}.map_dicom_file_attribute"
SHARED_CONTROL = f"{CONTROL_SCHEMA}.s6_source_versions"
PIPELINE = "s6_b13_dicom_tag_pipeline"
PIPELINE_VERSION = "B13_v1_2026-08-13"
EXPECTED_UID_LINK_FLOOR = 0.903

_WIDGET_DEFAULTS = {
    "action": "build",
    "run_id": "",
    "resume_run_id": "",
    "force_rebuild": "false",
    "interrupt_after_phase": "",
    "cleanup_scratch": "false",
    "apply_ig_tags": "true",
    "reset_target_version": "",
}
for _name, _default in _WIDGET_DEFAULTS.items():
    try:
        dbutils.widgets.text(_name, _default)
    except Exception:
        pass

def widget(name, default=""):
    try:
        value = dbutils.widgets.get(name)
        return default if value is None else str(value).strip()
    except Exception:
        return default

def as_bool(value):
    return str(value).strip().lower() in {"1", "true", "yes", "y"}

ACTION = widget("action", "build").lower()
FORCE_REBUILD = as_bool(widget("force_rebuild", "false"))
INTERRUPT_AFTER_PHASE = widget("interrupt_after_phase", "").lower()
CLEANUP_SCRATCH = as_bool(widget("cleanup_scratch", "false"))
APPLY_IG_TAGS = as_bool(widget("apply_ig_tags", "true"))
RESET_TARGET_VERSION = widget("reset_target_version", "")

# Existing-cluster widget state can survive between one-time runs. Benchmark repetitions
# must never reuse a stale run/resume token, so they always mint a fresh isolated token.
if ACTION == "benchmark":
    _requested_run_id = ""
    RUN_ID = f"benchmark_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}"
else:
    _requested_run_id = widget("resume_run_id", "") or widget("run_id", "")
    RUN_ID = _requested_run_id or f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}"
RUN_TOKEN = re.sub(r"[^a-zA-Z0-9]", "", RUN_ID).lower()[:32]
if not RUN_TOKEN:
    raise ValueError("run_id must contain at least one alphanumeric character")

SCRATCH_PIVOT = f"{TARGET_SCHEMA}.s6_dicom_{RUN_TOKEN}_pivot"
SCRATCH_EVIDENCE = f"{TARGET_SCHEMA}.s6_dicom_{RUN_TOKEN}_evidence"

print(json.dumps({
    "pipeline": PIPELINE,
    "pipeline_version": PIPELINE_VERSION,
    "action": ACTION,
    "run_id": RUN_ID,
    "scratch_pivot": SCRATCH_PIVOT,
    "scratch_evidence": SCRATCH_EVIDENCE,
    "force_rebuild": FORCE_REBUILD,
    "interrupt_after_phase": INTERRUPT_AFTER_PHASE,
    "target": TARGET,
}, sort_keys=True))

# COMMAND ----------

# Explicit whitelist manifest.
# Generic sequence keywords such as CodeValue, CodeMeaning, CodingSchemeDesignator,
# ReferencedSOPInstanceUID and ReferencedSOPClassUID are deliberately excluded:
# they are multi-valued sequence content and do not have a stable one-value-per-file meaning.
#
# Identifier-bearing tags are published under the 2026-08-13 doctrine and tagged 4/2.
WHITELIST = [
    ("SpecificCharacterSet", "SPECIFIC_CHARACTER_SET", "encoding"),
    ("AccessionNumber", "TAG_ACCESSION_NBR", "identifier"),
    ("InstitutionName", "INSTITUTION_NAME", "free_text"),
    ("InstitutionAddress", "INSTITUTION_ADDRESS", "free_text"),
    ("ReferringPhysicianName", "REFERRING_PHYSICIAN_NAME", "identifier"),
    ("PerformingPhysicianName", "PERFORMING_PHYSICIAN_NAME", "identifier"),
    ("OperatorsName", "OPERATORS_NAME", "identifier"),
    ("StudyDescription", "STUDY_DESCRIPTION", "free_text"),
    ("SeriesDescription", "SERIES_DESCRIPTION", "free_text"),
    ("ProtocolName", "PROTOCOL_NAME", "free_text"),
    ("Manufacturer", "MANUFACTURER", "equipment"),
    ("ManufacturerModelName", "MANUFACTURER_MODEL_NAME", "equipment"),
    ("StationName", "STATION_NAME", "equipment"),
    ("SoftwareVersions", "SOFTWARE_VERSIONS", "equipment"),
    ("DeviceSerialNumber", "DEVICE_SERIAL_NUMBER", "equipment_identifier"),
    ("PatientName", "PATIENT_NAME", "identifier"),
    ("PatientID", "DICOM_PATIENT_ID", "identifier"),
    ("IssuerOfPatientID", "PATIENT_ID_ISSUER", "identifier"),
    ("PatientBirthDate", "PATIENT_BIRTH_DATE_RAW", "identifier"),
    ("PatientSex", "PATIENT_SEX", "demographic"),
    ("PatientPosition", "PATIENT_POSITION", "acquisition"),
    ("StudyInstanceUID", "STUDY_INSTANCE_UID", "identifier"),
    ("SeriesInstanceUID", "SERIES_INSTANCE_UID", "identifier"),
    ("SOPInstanceUID", "SOP_INSTANCE_UID", "identifier"),
    ("SOPClassUID", "SOP_CLASS_UID", "technical"),
    ("FrameOfReferenceUID", "FRAME_OF_REFERENCE_UID", "identifier"),
    ("StudyID", "STUDY_ID", "identifier"),
    ("Modality", "MODALITY", "acquisition"),
    ("BodyPartExamined", "BODY_PART_EXAMINED", "free_text"),
    ("Laterality", "LATERALITY", "acquisition"),
    ("StudyDate", "STUDY_DATE_RAW", "temporal_raw"),
    ("StudyTime", "STUDY_TIME_RAW", "temporal_raw"),
    ("SeriesDate", "SERIES_DATE_RAW", "temporal_raw"),
    ("SeriesTime", "SERIES_TIME_RAW", "temporal_raw"),
    ("AcquisitionDate", "ACQUISITION_DATE_RAW", "temporal_raw"),
    ("AcquisitionTime", "ACQUISITION_TIME_RAW", "temporal_raw"),
    ("AcquisitionDateTime", "ACQUISITION_DATETIME_RAW", "temporal_raw"),
    ("ContentDate", "CONTENT_DATE_RAW", "temporal_raw"),
    ("ContentTime", "CONTENT_TIME_RAW", "temporal_raw"),
    ("InstanceCreationDate", "INSTANCE_CREATION_DATE_RAW", "temporal_raw"),
    ("InstanceCreationTime", "INSTANCE_CREATION_TIME_RAW", "temporal_raw"),
    ("SeriesNumber", "SERIES_NUMBER", "acquisition"),
    ("AcquisitionNumber", "ACQUISITION_NUMBER", "acquisition"),
    ("InstanceNumber", "INSTANCE_NUMBER", "acquisition"),
    ("ImageType", "IMAGE_TYPE", "acquisition"),
    ("Rows", "IMAGE_ROWS", "pixel"),
    ("Columns", "IMAGE_COLUMNS", "pixel"),
    ("SamplesPerPixel", "SAMPLES_PER_PIXEL", "pixel"),
    ("PhotometricInterpretation", "PHOTOMETRIC_INTERPRETATION", "pixel"),
    ("BitsAllocated", "BITS_ALLOCATED", "pixel"),
    ("BitsStored", "BITS_STORED", "pixel"),
    ("HighBit", "HIGH_BIT", "pixel"),
    ("PixelRepresentation", "PIXEL_REPRESENTATION", "pixel"),
    ("NumberOfFrames", "NUMBER_OF_FRAMES", "pixel"),
    ("PixelSpacing", "PIXEL_SPACING", "geometry"),
    ("ImagerPixelSpacing", "IMAGER_PIXEL_SPACING", "geometry"),
    ("SliceThickness", "SLICE_THICKNESS", "geometry"),
    ("SpacingBetweenSlices", "SPACING_BETWEEN_SLICES", "geometry"),
    ("ImagePositionPatient", "IMAGE_POSITION_PATIENT", "geometry"),
    ("ImageOrientationPatient", "IMAGE_ORIENTATION_PATIENT", "geometry"),
    ("SliceLocation", "SLICE_LOCATION", "geometry"),
    ("KVP", "KVP", "dose"),
    ("ExposureTime", "EXPOSURE_TIME", "dose"),
    ("XRayTubeCurrent", "XRAY_TUBE_CURRENT", "dose"),
    ("Exposure", "EXPOSURE", "dose"),
    ("ExposureInuAs", "EXPOSURE_IN_UAS", "dose"),
    ("CTDIvol", "CTDI_VOL", "dose"),
    ("SpiralPitchFactor", "SPIRAL_PITCH_FACTOR", "ct"),
    ("RevolutionTime", "REVOLUTION_TIME", "ct"),
    ("SingleCollimationWidth", "SINGLE_COLLIMATION_WIDTH", "ct"),
    ("TotalCollimationWidth", "TOTAL_COLLIMATION_WIDTH", "ct"),
    ("TableFeedPerRotation", "TABLE_FEED_PER_ROTATION", "ct"),
    ("MagneticFieldStrength", "MAGNETIC_FIELD_STRENGTH", "mr"),
    ("RepetitionTime", "REPETITION_TIME", "mr"),
    ("EchoTime", "ECHO_TIME", "mr"),
    ("InversionTime", "INVERSION_TIME", "mr"),
    ("FlipAngle", "FLIP_ANGLE", "mr"),
    ("ScanningSequence", "SCANNING_SEQUENCE", "mr"),
    ("SequenceVariant", "SEQUENCE_VARIANT", "mr"),
    ("ScanOptions", "SCAN_OPTIONS", "mr"),
    ("MRAcquisitionType", "MR_ACQUISITION_TYPE", "mr"),
    ("EchoTrainLength", "ECHO_TRAIN_LENGTH", "mr"),
]
WHITELIST_NAMES = [x[0] for x in WHITELIST]
WHITELIST_MAP = {x[0]: x[1] for x in WHITELIST}
assert len(WHITELIST_NAMES) == len(set(WHITELIST_NAMES))
assert len(WHITELIST_MAP.values()) == len(set(WHITELIST_MAP.values()))

print(json.dumps({
    "published_keywords": WHITELIST_NAMES,
    "excluded_classes": [
        "ERROR_ROWS: error_message is non-null; highest precedence",
        "PRIVATE_ROWS: is_private='True' after error-row precedence",
        "NON_WHITELIST_ROWS: public, non-error keywords absent from this explicit manifest",
        "FILE_LEVEL_EXCLUSION: a source file with zero public, non-error whitelisted rows",
    ],
    "duplicate_rule": [
        "top-level tag_path == name first",
        "nonblank value first",
        "latest extraction_timestamp NULLS LAST",
        "shortest tag_path NULLS LAST",
        "tag_path, tag, vr, value, sha256 ascending NULLS LAST",
    ],
}, sort_keys=True))

# COMMAND ----------

# ==== COMMON BLOCK v1 + S2/S6 subset (SYNC-WITH _completeness_common) ====
SENTINEL_FLOOR = "1901-01-01"

def dq_columns(df, date_cols):
    out = df
    for c in date_cols:
        fut = F.col(c) > F.current_timestamp()
        sen = F.col(c) < F.lit(SENTINEL_FLOOR).cast("timestamp")
        out = (out
               .withColumn(f"{c}_FUTURE_IND", F.when(F.col(c).isNull(), F.lit(None)).otherwise(fut))
               .withColumn(f"{c}_SENTINEL_IND", F.when(F.col(c).isNull(), F.lit(None)).otherwise(sen))
               .withColumn(f"{c}_CLEAN",
                           F.when(fut | sen, F.lit(None).cast("timestamp")).otherwise(F.col(c))))
    return out

def table_version(table):
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]["version"])

def dq_all_clinical(df, admin_stamps):
    cols = [field.name for field in df.schema.fields
            if isinstance(field.dataType, (TimestampType, DateType))
            and field.name not in admin_stamps
            and not field.name.endswith("_CLEAN")]
    return dq_columns(df, cols), cols

def replace_with_tombstones(df, target, key_cols):
    fresh = df.withColumn("SOURCE_PRESENT_IND", F.lit(True))
    previous_version = table_version(target) if spark.catalog.tableExists(target) else None
    (fresh.write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true")
          .option("delta.enableChangeDataFeed", "true")
          .saveAsTable(target))
    if previous_version is not None:
        prior = spark.read.option("versionAsOf", previous_version).table(target)
        present_keys = spark.table(target).select(*key_cols).distinct()
        gone = (prior.join(present_keys, key_cols, "left_anti")
                     .withColumn("SOURCE_PRESENT_IND", F.lit(False)))
        if gone.limit(1).count() > 0:
            gone.write.format("delta").mode("append").saveAsTable(target)

def table_fingerprint(table, exclude=("PIPELINE_UPDT_DT_TM",)):
    cols = [c for c in spark.table(table).columns if c not in exclude]
    return (spark.table(table)
            .select(F.sum(F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in cols])))
                          .cast("decimal(38,0)")).alias("fp"))
            .collect()[0]["fp"])

def lookup_counterpart_tags(col_name):
    safe = col_name.replace("'", "''")
    rows = spark.sql(f"""
        SELECT MAX(CASE WHEN tag_name='ig_risk' THEN tag_value END) r,
               MAX(CASE WHEN tag_name='ig_severity' THEN tag_value END) s
        FROM 4_prod.information_schema.column_tags
        WHERE schema_name='bronze' AND upper(column_name)=upper('{safe}')
        GROUP BY table_name
    """).groupBy("r", "s").count().orderBy(F.desc("count"), F.asc("r"), F.asc("s")).collect()
    return (rows[0]["r"], rows[0]["s"]) if rows else None

def ig_tag_table(table, tag_map, default=("0", "0")):
    cols = [r["col_name"] for r in spark.sql(f"DESCRIBE {table}").collect()
            if r["col_name"] and not r["col_name"].startswith("#")]
    defaulted = []
    for c in cols:
        if c in tag_map:
            risk, severity = tag_map[c]
        else:
            found = lookup_counterpart_tags(c)
            risk, severity = found if found else default
            if not found:
                defaulted.append(c)
                print(f"IG-TAG DEFAULTED {table}.{c} -> {default} — REVIEW")
        spark.sql(
            f"ALTER TABLE {table} ALTER COLUMN {c} "
            f"SET TAGS ('ig_risk'='{risk}','ig_severity'='{severity}')"
        )
    return defaulted

def ig_tag_gate(table):
    catalog, schema, tbl = table.split(".")
    columns = {r["column_name"] for r in spark.sql(f"""
        SELECT column_name FROM {catalog}.information_schema.columns
        WHERE table_schema='{schema}' AND table_name='{tbl}'
    """).collect()}
    risk = {r["column_name"] for r in spark.sql(f"""
        SELECT column_name FROM {catalog}.information_schema.column_tags
        WHERE schema_name='{schema}' AND table_name='{tbl}' AND tag_name='ig_risk'
    """).collect()}
    severity = {r["column_name"] for r in spark.sql(f"""
        SELECT column_name FROM {catalog}.information_schema.column_tags
        WHERE schema_name='{schema}' AND table_name='{tbl}' AND tag_name='ig_severity'
    """).collect()}
    missing_risk = sorted(columns - risk)
    missing_severity = sorted(columns - severity)
    assert not missing_risk and not missing_severity, {
        "missing_ig_risk": missing_risk,
        "missing_ig_severity": missing_severity,
    }

# COMMAND ----------

def table_properties(table):
    if not spark.catalog.tableExists(table):
        return {}
    return {str(r["key"]): str(r["value"]) for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}

def sql_quote(value):
    return str(value).replace("\\", "\\\\").replace("'", "''")

def set_properties(table, properties):
    rendered = ", ".join(
        f"'{sql_quote(k)}' = '{sql_quote(v)}'" for k, v in sorted(properties.items())
    )
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({rendered})")

def source_versions():
    return {
        SOURCE_TAGS: table_version(SOURCE_TAGS),
        SOURCE_TIER: table_version(SOURCE_TIER),
        SOURCE_EXAM: table_version(SOURCE_EXAM),
    }

def build_is_due(versions):
    if not spark.catalog.tableExists(TARGET):
        return True
    props = table_properties(TARGET)
    if props.get("s6.pipeline.version") != PIPELINE_VERSION:
        return True
    return any(props.get(f"s6.source.{i}.table") != table or
               props.get(f"s6.source.{i}.version") != str(version)
               for i, (table, version) in enumerate(sorted(versions.items()), start=1))

def pinned_table(table, versions):
    return spark.read.option("versionAsOf", versions[table]).table(table)

def record_shared_versions(versions):
    """Plan-standard shared ledger; call only after every build gate passes."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {SHARED_CONTROL} (
        pipeline STRING, source STRING, version BIGINT, updated_at TIMESTAMP
    ) USING DELTA""")
    for source, version in sorted(versions.items()):
        source_safe = sql_quote(source)
        pipeline_safe = sql_quote(PIPELINE)
        spark.sql(f"""
            MERGE INTO {SHARED_CONTROL} c
            USING (
                SELECT '{pipeline_safe}' pipeline, '{source_safe}' source,
                       CAST({int(version)} AS BIGINT) version
            ) s
            ON c.pipeline=s.pipeline AND c.source=s.source
            WHEN MATCHED THEN UPDATE SET
                c.version=s.version, c.updated_at=current_timestamp()
            WHEN NOT MATCHED THEN INSERT (pipeline, source, version, updated_at)
                VALUES (s.pipeline, s.source, s.version, current_timestamp())
        """)

def scratch_matches(scratch, versions):
    if not spark.catalog.tableExists(scratch):
        return False
    props = table_properties(scratch)
    if props.get("s6.pipeline.version") != PIPELINE_VERSION:
        return False
    return all(props.get(f"s6.source.{i}.table") == table and
               props.get(f"s6.source.{i}.version") == str(version)
               for i, (table, version) in enumerate(sorted(versions.items()), start=1))

def version_properties(versions):
    props = {"s6.pipeline.version": PIPELINE_VERSION, "s6.pipeline.name": PIPELINE}
    for i, (table, version) in enumerate(sorted(versions.items()), start=1):
        props[f"s6.source.{i}.table"] = table
        props[f"s6.source.{i}.version"] = str(version)
    return props

# COMMAND ----------

# Gates are defined before the build and can be run with action=pre_gates.
def run_gates(recompute_fingerprint=True):
    assert spark.catalog.tableExists(TARGET), f"G0 target absent: {TARGET}"
    props = table_properties(TARGET)
    required_metrics = [
        "source_rows", "source_files", "published_files", "excluded_files",
        "pivot_rows", "private_rows", "error_rows", "non_whitelist_rows",
    ]
    metrics = {}
    for key in required_metrics:
        prop = f"s6.metric.{key}"
        assert prop in props, f"G0 missing table property {prop}"
        metrics[key] = int(props[prop])

    present = spark.table(TARGET).where(F.col("SOURCE_PRESENT_IND"))
    shape = present.agg(
        F.count("*").alias("rows"),
        F.countDistinct("DICOM_PATH").alias("files"),
        F.sum(F.col("PIVOT_CONSUMED_TAG_ROW_COUNT")).alias("pivot_rows"),
        F.sum(F.col("PRIVATE_TAG_ROW_COUNT")).alias("private_rows_on_published_files"),
        F.sum(F.col("ERROR_TAG_ROW_COUNT")).alias("error_rows_on_published_files"),
        F.sum(F.col("NON_WHITELIST_TAG_ROW_COUNT")).alias("non_whitelist_rows_on_published_files"),
        F.sum(F.col("DUPLICATE_WHITELIST_TAG_ROW_COUNT")).alias("duplicate_rows"),
        F.sum(F.col("NESTED_FALLBACK_KEYWORD_COUNT")).alias("nested_fallback_keywords"),
    ).collect()[0].asDict()

    # G1a: file-set accounting; units are files only.
    assert metrics["source_files"] == metrics["published_files"] + metrics["excluded_files"], {
        "gate": "G1a", "metrics": metrics,
    }
    assert int(shape["rows"]) == metrics["published_files"]
    assert int(shape["files"]) == metrics["published_files"]

    # G1b: mutually exclusive tag-row accounting with ERROR > PRIVATE > PIVOT > NON_WHITELIST precedence.
    assert metrics["source_rows"] == (
        metrics["pivot_rows"] + metrics["private_rows"] +
        metrics["error_rows"] + metrics["non_whitelist_rows"]
    ), {"gate": "G1b", "metrics": metrics}
    assert int(shape["pivot_rows"] or 0) == metrics["pivot_rows"]

    # G2: one row per file and deterministic duplicate-rule evidence.
    assert int(shape["rows"]) == int(shape["files"])
    assert present.where(F.col("SELECTED_WHITELIST_KEYWORD_COUNT") <= 0).limit(1).count() == 0
    assert present.where(F.col("DUPLICATE_WHITELIST_TAG_ROW_COUNT") < 0).limit(1).count() == 0

    # G3: status accounting and the recon-measured top-level StudyInstanceUID match floor.
    bad_link = present.where(
        ~F.col("EXAM_LINK_STATUS").isin("LINKED", "NO_EXAM", "AMBIGUOUS") |
        (F.col("EXAM_LINK_STATUS") == "LINKED") & F.col("PACS_EXAMINATION_ID").isNull() |
        (F.col("EXAM_LINK_STATUS") != "LINKED") & F.col("PACS_EXAMINATION_ID").isNotNull()
    ).limit(1).count()
    assert bad_link == 0
    uid = (present
           .where((F.col("STUDY_INSTANCE_UID_TAG_PATH") == "StudyInstanceUID") &
                  F.col("STUDY_INSTANCE_UID").isNotNull())
           .groupBy("STUDY_INSTANCE_UID")
           .agg(F.max(F.col("STUDY_EXAM_CANDIDATE_COUNT")).alias("candidate_count"))
           .agg(F.count("*").alias("n"),
                F.sum((F.col("candidate_count") > 0).cast("long")).alias("matched"))
           .collect()[0])
    uid_candidate_rate = float(uid["matched"] or 0) / max(int(uid["n"] or 0), 1)
    assert uid_candidate_rate >= EXPECTED_UID_LINK_FLOOR, {
        "gate": "G3_candidate_presence",
        "denominator": "distinct top-level StudyInstanceUID values",
        "uid_candidate_rate": uid_candidate_rate,
        "uid": uid.asDict(),
    }
    link_counts = {r["EXAM_LINK_STATUS"]: int(r["count"]) for r in
                   present.groupBy("EXAM_LINK_STATUS").count().collect()}
    keyed_files = present.where(
        F.col("STUDY_INSTANCE_UID").isNotNull() | F.col("ACCESSION_NBR").isNotNull()
    ).count()
    unique_link_rate_keyed_files = link_counts.get("LINKED", 0) / max(keyed_files, 1)
    unique_link_rate_all_files = link_counts.get("LINKED", 0) / max(metrics["published_files"], 1)
    disambiguated_files = present.where(
        F.col("EXAM_LINK_METHOD") == "STUDY_UID_ACCESSION_DISAMBIGUATED"
    ).count()
    assert sum(link_counts.values()) == metrics["published_files"]
    assert disambiguated_files > 0, "accession/study intersection did not disambiguate any files"

    # G4: UNTIERED is explicit; tier source is unique by path; >=99% tier accessions resolve to exams.
    assert present.where(
        F.col("BURNED_IN_PII_TIER").isNull() |
        ~F.col("BURNED_IN_PII_TIER").isin("SAFE_SKIP", "GREY", "MUST_OCR", "UNTIERED")
    ).limit(1).count() == 0
    tier = spark.table(SOURCE_TIER)
    tier_shape = tier.agg(
        F.count("*").alias("n"),
        F.countDistinct("dicom_path").alias("files"),
    ).collect()[0]
    assert int(tier_shape["n"]) == int(tier_shape["files"])
    tier_acc = tier.select(F.trim("accession_nbr").alias("ACCESSION_NBR")).where(
        F.col("ACCESSION_NBR").isNotNull() & (F.col("ACCESSION_NBR") != "")
    ).distinct()
    exam_acc = (spark.table(SOURCE_EXAM)
                .where(F.col("SOURCE_PRESENT_IND"))
                .select(F.trim("REQUEST_ID_STRING").alias("ACCESSION_NBR"))
                .where(F.col("ACCESSION_NBR").isNotNull())
                .distinct()
                .withColumn("_exam_match", F.lit(1)))
    tier_join = tier_acc.join(exam_acc, "ACCESSION_NBR", "left").agg(
        F.count("*").alias("n"),
        F.sum(F.coalesce(F.col("_exam_match"), F.lit(0))).alias("matched"),
    ).collect()[0]
    tier_accession_link_rate = float(tier_join["matched"] or 0) / max(int(tier_join["n"] or 0), 1)
    assert tier_accession_link_rate >= 0.99, {
        "gate": "G4", "tier_accession_link_rate": tier_accession_link_rate,
        "tier_join": tier_join.asDict(),
    }

    # Date-quality companions for every derived clinical timestamp.
    temporal_cols = [
        "PATIENT_BIRTH_DT_TM", "STUDY_DT_TM", "SERIES_DT_TM",
        "ACQUISITION_DT_TM", "CONTENT_DT_TM", "INSTANCE_CREATION_DT_TM",
    ]
    cols = set(present.columns)
    missing_dq = sorted(
        f"{c}{suffix}" for c in temporal_cols
        for suffix in ("_FUTURE_IND", "_SENTINEL_IND", "_CLEAN")
        if f"{c}{suffix}" not in cols
    )
    assert not missing_dq, {"missing_dq_columns": missing_dq}

    # G5: every column has both IG tags.
    ig_tag_gate(TARGET)

    fingerprint = str(table_fingerprint(TARGET)) if recompute_fingerprint else props.get("s6.fingerprint")
    if props.get("s6.fingerprint") and recompute_fingerprint:
        assert fingerprint == props["s6.fingerprint"], {
            "stored": props["s6.fingerprint"], "actual": fingerprint,
        }

    result = {
        "G1_file_and_row_accounting": metrics,
        "G2_shape": shape,
        "G3_top_level_uid_candidate_rate": uid_candidate_rate,
        "G3_candidate_rate_denominator": "distinct top-level StudyInstanceUID values",
        "G3_uid_counts": uid.asDict(),
        "G3_file_status_counts": link_counts,
        "G3_unique_link_rate_keyed_files": unique_link_rate_keyed_files,
        "G3_unique_link_rate_all_published_files": unique_link_rate_all_files,
        "G3_disambiguated_files": disambiguated_files,
        "G4_tier_accession_link_rate": tier_accession_link_rate,
        "G4_tier_accession_counts": tier_join.asDict(),
        "fingerprint": fingerprint,
    }
    print(json.dumps(result, default=str, sort_keys=True))
    print("B13 gates PASS")
    return result

# COMMAND ----------

def parse_dicom_date_time(date_col, time_col=None):
    date_digits = F.substring(F.regexp_replace(F.col(date_col), "[^0-9]", ""), 1, 8)
    if time_col:
        time_digits = F.rpad(
            F.substring(F.regexp_replace(F.col(time_col), "[^0-9]", ""), 1, 6),
            6, "0"
        )
    else:
        time_digits = F.lit("000000")
    value = F.concat(date_digits, time_digits)
    return F.try_to_timestamp(value, F.lit("yyyyMMddHHmmss"))

def parse_dicom_datetime(col_name):
    digits = F.substring(F.regexp_replace(F.col(col_name), "[^0-9]", ""), 1, 14)
    padded = F.rpad(digits, 14, "0")
    return F.try_to_timestamp(padded, F.lit("yyyyMMddHHmmss"))

def build_pivot_scratch(versions):
    src = pinned_table(SOURCE_TAGS, versions).select(
        "dicom_path", "sha256", "tag_path", "tag", "name", "vr", "value",
        "is_private", "error_message", "extraction_timestamp",
    )
    is_error = F.col("error_message").isNotNull()
    is_private = F.lower(F.trim(F.col("is_private"))) == F.lit("true")
    is_whitelist = F.col("name").isin(WHITELIST_NAMES)
    base = src.withColumn(
        "_row_class",
        F.when(is_error, F.lit("ERROR"))
         .when(is_private, F.lit("PRIVATE"))
         .when(is_whitelist, F.lit("PIVOT"))
         .otherwise(F.lit("NON_WHITELIST"))
    )

    file_stats = base.groupBy("dicom_path").agg(
        F.count("*").alias("SOURCE_TAG_ROW_COUNT"),
        F.sum((F.col("_row_class") == "PIVOT").cast("long")).alias("PIVOT_CONSUMED_TAG_ROW_COUNT"),
        F.sum((F.col("_row_class") == "PRIVATE").cast("long")).alias("PRIVATE_TAG_ROW_COUNT"),
        F.sum((F.col("_row_class") == "ERROR").cast("long")).alias("ERROR_TAG_ROW_COUNT"),
        F.sum((F.col("_row_class") == "NON_WHITELIST").cast("long")).alias("NON_WHITELIST_TAG_ROW_COUNT"),
        F.min("sha256").alias("_SHA_MIN"),
        F.max("sha256").alias("_SHA_MAX"),
        F.max("extraction_timestamp").alias("SOURCE_MAX_EXTRACTION_TS"),
    )

    eligible = (base.where(F.col("_row_class") == "PIVOT")
                .withColumn("_TOP_LEVEL", F.col("tag_path") == F.col("name"))
                .withColumn("_NONBLANK", F.length(F.trim(F.col("value"))) > 0))
    key_window = Window.partitionBy("dicom_path", "name")
    order_window = key_window.orderBy(
        F.col("_TOP_LEVEL").desc(),
        F.col("_NONBLANK").desc(),
        F.col("extraction_timestamp").desc_nulls_last(),
        F.length(F.col("tag_path")).asc_nulls_last(),
        F.col("tag_path").asc_nulls_last(),
        F.col("tag").asc_nulls_last(),
        F.col("vr").asc_nulls_last(),
        F.col("value").asc_nulls_last(),
        F.col("sha256").asc_nulls_last(),
    )
    winners = (eligible
               .withColumn("_KEYWORD_INSTANCE_COUNT", F.count("*").over(key_window))
               .withColumn("_RN", F.row_number().over(order_window))
               .where(F.col("_RN") == 1))

    agg_exprs = [
        F.count("*").alias("SELECTED_WHITELIST_KEYWORD_COUNT"),
        F.sum((F.col("_KEYWORD_INSTANCE_COUNT") - 1).cast("long"))
         .alias("DUPLICATE_WHITELIST_TAG_ROW_COUNT"),
        F.sum((F.col("_KEYWORD_INSTANCE_COUNT") > 1).cast("long"))
         .alias("DUPLICATE_WHITELIST_KEYWORD_COUNT"),
        F.sum((~F.col("_TOP_LEVEL")).cast("long")).alias("NESTED_FALLBACK_KEYWORD_COUNT"),
    ]
    for source_name, target_col, _classification in WHITELIST:
        agg_exprs.append(
            F.max(F.when(F.col("name") == source_name, F.col("value"))).alias(target_col)
        )
    agg_exprs.extend([
        F.max(F.when(F.col("name") == "StudyInstanceUID", F.col("tag_path")))
         .alias("STUDY_INSTANCE_UID_TAG_PATH"),
        F.max(F.when(F.col("name") == "AccessionNumber", F.col("tag_path")))
         .alias("ACCESSION_NBR_TAG_PATH"),
    ])
    wide = winners.groupBy("dicom_path").agg(*agg_exprs)

    pivot = (file_stats.join(wide, "dicom_path", "left")
             .fillna({
                 "SELECTED_WHITELIST_KEYWORD_COUNT": 0,
                 "DUPLICATE_WHITELIST_TAG_ROW_COUNT": 0,
                 "DUPLICATE_WHITELIST_KEYWORD_COUNT": 0,
                 "NESTED_FALLBACK_KEYWORD_COUNT": 0,
             })
             .withColumn("FILE_SHA256", F.coalesce(F.col("_SHA_MAX"), F.col("_SHA_MIN")))
             .withColumn("FILE_SHA256_CONFLICT_IND",
                         F.col("_SHA_MIN").isNotNull() & F.col("_SHA_MAX").isNotNull() &
                         (F.col("_SHA_MIN") != F.col("_SHA_MAX")))
             .drop("_SHA_MIN", "_SHA_MAX"))

    (pivot.write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(SCRATCH_PIVOT))
    set_properties(SCRATCH_PIVOT, {
        **version_properties(versions),
        "s6.phase": "PIVOT_COMPLETE",
        "s6.run_id": RUN_ID,
        "s6.whitelist.count": str(len(WHITELIST)),
    })
    return aggregate_accounting(SCRATCH_PIVOT)

def aggregate_accounting(pivot_table):
    row = spark.table(pivot_table).agg(
        F.sum("SOURCE_TAG_ROW_COUNT").alias("source_rows"),
        F.count("*").alias("source_files"),
        F.sum((F.col("PIVOT_CONSUMED_TAG_ROW_COUNT") > 0).cast("long")).alias("published_files"),
        F.sum((F.col("PIVOT_CONSUMED_TAG_ROW_COUNT") == 0).cast("long")).alias("excluded_files"),
        F.sum("PIVOT_CONSUMED_TAG_ROW_COUNT").alias("pivot_rows"),
        F.sum("PRIVATE_TAG_ROW_COUNT").alias("private_rows"),
        F.sum("ERROR_TAG_ROW_COUNT").alias("error_rows"),
        F.sum("NON_WHITELIST_TAG_ROW_COUNT").alias("non_whitelist_rows"),
    ).collect()[0]
    metrics = {k: int(row[k] or 0) for k in row.asDict()}
    assert metrics["source_files"] == metrics["published_files"] + metrics["excluded_files"]
    assert metrics["source_rows"] == (
        metrics["pivot_rows"] + metrics["private_rows"] +
        metrics["error_rows"] + metrics["non_whitelist_rows"]
    )
    return metrics

# COMMAND ----------

def build_exam_link_frame(pivot, tier, exam):
    tier_small = tier.select(
        F.col("dicom_path").alias("DICOM_PATH"),
        F.trim(F.col("accession_nbr")).alias("TIER_ACCESSION_NBR"),
        F.col("tier").alias("_TIER"),
        F.col("reason").alias("BURNED_IN_PII_TIER_REASON"),
    )
    tier_dupes = tier_small.groupBy("DICOM_PATH").count().where(F.col("count") != 1).limit(1).count()
    assert tier_dupes == 0, "dicom_pii_tier is not one row per dicom_path"

    keys = (pivot.select(
                F.col("dicom_path").alias("DICOM_PATH"),
                F.trim(F.col("STUDY_INSTANCE_UID")).alias("STUDY_INSTANCE_UID"),
                F.trim(F.col("TAG_ACCESSION_NBR")).alias("TAG_ACCESSION_NBR"),
            )
            .join(tier_small, "DICOM_PATH", "left")
            .withColumn(
                "ACCESSION_CONFLICT_IND",
                F.col("TAG_ACCESSION_NBR").isNotNull() &
                F.col("TIER_ACCESSION_NBR").isNotNull() &
                (F.col("TAG_ACCESSION_NBR") != F.col("TIER_ACCESSION_NBR"))
            )
            .withColumn(
                "ACCESSION_NBR",
                F.coalesce(F.col("TAG_ACCESSION_NBR"), F.col("TIER_ACCESSION_NBR"))
            )
            .withColumn(
                "ACCESSION_SOURCE",
                F.when(F.col("TAG_ACCESSION_NBR").isNotNull(), F.lit("DICOM_TAG"))
                 .when(F.col("TIER_ACCESSION_NBR").isNotNull(), F.lit("PII_TIER"))
                 .otherwise(F.lit("NONE"))
            )
            .withColumn("BURNED_IN_PII_TIER", F.coalesce(F.col("_TIER"), F.lit("UNTIERED")))
            .drop("_TIER"))

    exam_present = exam.where(F.col("SOURCE_PRESENT_IND")).select(
        F.col("PACS_EXAMINATION_ID").cast("long").alias("PACS_EXAMINATION_ID"),
        F.trim(F.col("STUDY_INSTANCE_UID")).alias("EXAM_STUDY_INSTANCE_UID"),
        F.trim(F.col("REQUEST_ID_STRING")).alias("EXAM_ACCESSION_NBR"),
    )
    study_map = (exam_present.where(F.col("EXAM_STUDY_INSTANCE_UID").isNotNull())
                 .groupBy("EXAM_STUDY_INSTANCE_UID")
                 .agg(
                     F.countDistinct("PACS_EXAMINATION_ID").alias("STUDY_EXAM_CANDIDATE_COUNT"),
                     F.min("PACS_EXAMINATION_ID").alias("_STUDY_EXAM_ID"),
                 ))
    accession_map = (exam_present.where(F.col("EXAM_ACCESSION_NBR").isNotNull())
                     .groupBy("EXAM_ACCESSION_NBR")
                     .agg(
                         F.countDistinct("PACS_EXAMINATION_ID").alias("ACCESSION_EXAM_CANDIDATE_COUNT"),
                         F.min("PACS_EXAMINATION_ID").alias("_ACCESSION_EXAM_ID"),
                     ))
    pair_map = (exam_present.where(
                    F.col("EXAM_STUDY_INSTANCE_UID").isNotNull() &
                    F.col("EXAM_ACCESSION_NBR").isNotNull())
                .groupBy("EXAM_STUDY_INSTANCE_UID", "EXAM_ACCESSION_NBR")
                .agg(
                    F.countDistinct("PACS_EXAMINATION_ID").alias("PAIR_EXAM_CANDIDATE_COUNT"),
                    F.min("PACS_EXAMINATION_ID").alias("_PAIR_EXAM_ID"),
                ))

    linked = (keys
              .join(study_map, keys["STUDY_INSTANCE_UID"] == study_map["EXAM_STUDY_INSTANCE_UID"], "left")
              .drop("EXAM_STUDY_INSTANCE_UID")
              .join(accession_map, keys["ACCESSION_NBR"] == accession_map["EXAM_ACCESSION_NBR"], "left")
              .drop("EXAM_ACCESSION_NBR")
              .join(
                  pair_map,
                  (keys["STUDY_INSTANCE_UID"] == pair_map["EXAM_STUDY_INSTANCE_UID"]) &
                  (keys["ACCESSION_NBR"] == pair_map["EXAM_ACCESSION_NBR"]),
                  "left",
              )
              .drop("EXAM_STUDY_INSTANCE_UID", "EXAM_ACCESSION_NBR")
              .fillna({
                  "STUDY_EXAM_CANDIDATE_COUNT": 0,
                  "ACCESSION_EXAM_CANDIDATE_COUNT": 0,
                  "PAIR_EXAM_CANDIDATE_COUNT": 0,
              }))

    pair_unique = F.col("PAIR_EXAM_CANDIDATE_COUNT") == 1
    pair_ambiguous = F.col("PAIR_EXAM_CANDIDATE_COUNT") > 1
    study_unique_only = (
        (F.col("STUDY_EXAM_CANDIDATE_COUNT") == 1) &
        (F.col("ACCESSION_EXAM_CANDIDATE_COUNT") == 0)
    )
    accession_unique_only = (
        (F.col("ACCESSION_EXAM_CANDIDATE_COUNT") == 1) &
        (F.col("STUDY_EXAM_CANDIDATE_COUNT") == 0)
    )
    no_candidates = (
        (F.col("STUDY_EXAM_CANDIDATE_COUNT") == 0) &
        (F.col("ACCESSION_EXAM_CANDIDATE_COUNT") == 0)
    )
    resolved_id = (
        F.when(pair_unique, F.col("_PAIR_EXAM_ID"))
         .when(study_unique_only, F.col("_STUDY_EXAM_ID"))
         .when(accession_unique_only, F.col("_ACCESSION_EXAM_ID"))
    )

    linked = (linked
              .withColumn(
                  "EXAM_LINK_STATUS",
                  F.when(F.col("ACCESSION_CONFLICT_IND"), F.lit("AMBIGUOUS"))
                   .when(pair_unique, F.lit("LINKED"))
                   .when(pair_ambiguous, F.lit("AMBIGUOUS"))
                   .when(study_unique_only | accession_unique_only, F.lit("LINKED"))
                   .when(no_candidates, F.lit("NO_EXAM"))
                   .otherwise(F.lit("AMBIGUOUS"))
              )
              .withColumn(
                  "EXAM_LINK_METHOD",
                  F.when(F.col("ACCESSION_CONFLICT_IND"), F.lit("AMBIGUOUS_ACCESSION_CONFLICT"))
                   .when(pair_unique &
                         ((F.col("STUDY_EXAM_CANDIDATE_COUNT") > 1) |
                          (F.col("ACCESSION_EXAM_CANDIDATE_COUNT") > 1)),
                         F.lit("STUDY_UID_ACCESSION_DISAMBIGUATED"))
                   .when(pair_unique, F.lit("STUDY_UID_ACCESSION_CONSENSUS"))
                   .when(study_unique_only, F.lit("STUDY_UID"))
                   .when(accession_unique_only, F.lit("ACCESSION_FALLBACK"))
                   .when(no_candidates, F.lit("NONE"))
                   .otherwise(F.lit("AMBIGUOUS"))
              )
              .withColumn(
                  "PACS_EXAMINATION_ID",
                  F.when(F.col("EXAM_LINK_STATUS") == "LINKED", resolved_id)
              )
              .drop("_STUDY_EXAM_ID", "_ACCESSION_EXAM_ID", "_PAIR_EXAM_ID"))
    # Return only derived/link columns. The pivot already carries STUDY_INSTANCE_UID
    # and TAG_ACCESSION_NBR; retaining them here would create duplicate column names.
    return linked.select(
        "DICOM_PATH", "TIER_ACCESSION_NBR", "BURNED_IN_PII_TIER_REASON",
        "ACCESSION_CONFLICT_IND", "ACCESSION_NBR", "ACCESSION_SOURCE",
        "BURNED_IN_PII_TIER", "STUDY_EXAM_CANDIDATE_COUNT",
        "ACCESSION_EXAM_CANDIDATE_COUNT", "PAIR_EXAM_CANDIDATE_COUNT",
        "EXAM_LINK_STATUS", "EXAM_LINK_METHOD", "PACS_EXAMINATION_ID",
    )

def enrich_and_publish(versions, accounting):
    pivot = (spark.table(SCRATCH_PIVOT)
             .where(F.col("PIVOT_CONSUMED_TAG_ROW_COUNT") > 0))
    tier = pinned_table(SOURCE_TIER, versions)
    exam = pinned_table(SOURCE_EXAM, versions)

    link = build_exam_link_frame(pivot, tier, exam)
    exam_details = exam.where(F.col("SOURCE_PRESENT_IND")).select(
        F.col("PACS_EXAMINATION_ID").cast("long").alias("PACS_EXAMINATION_ID"),
        F.col("PACS_REQUEST_ID").cast("long").alias("PACS_REQUEST_ID"),
        F.col("PACS_PATIENT_ID").cast("long").alias("PACS_PATIENT_ID"),
        F.col("PERSON_ID").cast("long").alias("PERSON_ID"),
        F.col("PERSON_MATCH_STATUS").alias("PERSON_MATCH_STATUS"),
        F.col("STUDY_INSTANCE_UID").alias("PACS_STUDY_INSTANCE_UID"),
        F.col("REQUEST_ID_STRING").alias("PACS_REQUEST_ID_STRING"),
    )
    assert exam_details.groupBy("PACS_EXAMINATION_ID").count().where(F.col("count") > 1).limit(1).count() == 0

    out = (pivot.withColumnRenamed("dicom_path", "DICOM_PATH")
           .join(link, "DICOM_PATH", "left")
           .join(exam_details, "PACS_EXAMINATION_ID", "left"))

    out = (out
           .withColumn("PATIENT_BIRTH_DT_TM", parse_dicom_date_time("PATIENT_BIRTH_DATE_RAW"))
           .withColumn("STUDY_DT_TM", parse_dicom_date_time("STUDY_DATE_RAW", "STUDY_TIME_RAW"))
           .withColumn("SERIES_DT_TM", parse_dicom_date_time("SERIES_DATE_RAW", "SERIES_TIME_RAW"))
           .withColumn(
               "ACQUISITION_DT_TM",
               F.coalesce(
                   parse_dicom_datetime("ACQUISITION_DATETIME_RAW"),
                   parse_dicom_date_time("ACQUISITION_DATE_RAW", "ACQUISITION_TIME_RAW"),
               )
           )
           .withColumn("CONTENT_DT_TM", parse_dicom_date_time("CONTENT_DATE_RAW", "CONTENT_TIME_RAW"))
           .withColumn(
               "INSTANCE_CREATION_DT_TM",
               parse_dicom_date_time("INSTANCE_CREATION_DATE_RAW", "INSTANCE_CREATION_TIME_RAW")
           ))

    out, flagged = dq_all_clinical(out, admin_stamps={"SOURCE_MAX_EXTRACTION_TS"})
    business_cols = [c for c in out.columns if c not in {"PIPELINE_UPDT_DT_TM", "ROW_HASH"}]
    out = (out
           .withColumn("ROW_HASH", F.sha2(F.to_json(F.struct(*[F.col(c) for c in business_cols])), 256))
           .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp()))

    replace_with_tombstones(out, TARGET, ["DICOM_PATH"])

    table_comment = (
        "PILOT COVERAGE ONLY: one row per DICOM file from the 2026-08-10 deep pilot tag extract "
        "(about 4.12M files but only about 4.8k studies / 3.1k accessions; not estate-wide coverage). "
        "Public, non-error keywords in the explicit B13 whitelist are pivoted. Duplicate instances "
        "resolve deterministically: top-level tag_path first, then nonblank, latest extraction "
        "timestamp NULLS LAST, shortest/lexical path and stable lexical tie-breakers. Private-tag "
        "rows, extraction-error rows and non-whitelisted keyword rows are excluded from the pivot "
        "and reconciled separately; files with zero eligible rows are named file-level exclusions. "
        "PatientName, PatientBirthDate, PatientID, physician names, accessions and UIDs are published "
        "under the 2026-08-13 identifier doctrine and carry ig_risk=4 / ig_severity=2 tags. "
        "Exam linkage uses StudyInstanceUID plus AccessionNumber/REQUEST_ID_STRING fallback and "
        "publishes LINKED, NO_EXAM or AMBIGUOUS. Burned-in PII tier is SAFE_SKIP, GREY, MUST_OCR or "
        "explicit UNTIERED. Static curated pilot: no weekly Bronze_Pipeline step until extraction widens."
    )
    spark.sql(f"COMMENT ON TABLE {TARGET} IS '{sql_quote(table_comment)}'")

    identifier_cols = {
        "DICOM_PATH", "FILE_SHA256", "TAG_ACCESSION_NBR", "TIER_ACCESSION_NBR", "ACCESSION_NBR",
        "PATIENT_NAME", "DICOM_PATIENT_ID", "PATIENT_ID_ISSUER", "PATIENT_BIRTH_DATE_RAW",
        "PATIENT_BIRTH_DT_TM", "PATIENT_BIRTH_DT_TM_FUTURE_IND", "PATIENT_BIRTH_DT_TM_SENTINEL_IND",
        "PATIENT_BIRTH_DT_TM_CLEAN", "REFERRING_PHYSICIAN_NAME", "PERFORMING_PHYSICIAN_NAME",
        "OPERATORS_NAME", "STUDY_INSTANCE_UID", "SERIES_INSTANCE_UID", "SOP_INSTANCE_UID",
        "FRAME_OF_REFERENCE_UID", "STUDY_ID", "PACS_EXAMINATION_ID", "PACS_REQUEST_ID",
        "PACS_PATIENT_ID", "PERSON_ID", "PACS_STUDY_INSTANCE_UID", "PACS_REQUEST_ID_STRING",
    }
    free_text_cols = {
        "INSTITUTION_NAME", "INSTITUTION_ADDRESS", "STUDY_DESCRIPTION",
        "SERIES_DESCRIPTION", "PROTOCOL_NAME", "BODY_PART_EXAMINED",
    }
    tag_map = {c: ("4", "2") for c in identifier_cols | free_text_cols if c in spark.table(TARGET).columns}
    if APPLY_IG_TAGS:
        defaulted = ig_tag_table(TARGET, tag_map)
        print(json.dumps({"ig_defaulted_review": defaulted}, sort_keys=True))
    ig_tag_gate(TARGET)

    fingerprint = str(table_fingerprint(TARGET))
    props = {
        **version_properties(versions),
        "s6.run_id": RUN_ID,
        "s6.build.status": "SUCCESS",
        "s6.fingerprint": fingerprint,
        "s6.whitelist.count": str(len(WHITELIST)),
        "s6.comment.scope": "PILOT_COVERAGE",
        "s6.comment.schedule": "STATIC_NO_WEEKLY_STEP",
        "s6.dq.flagged": json.dumps(flagged, sort_keys=True),
    }
    props.update({f"s6.metric.{k}": str(v) for k, v in accounting.items()})
    set_properties(TARGET, props)
    return fingerprint, flagged

# COMMAND ----------

def write_evidence(record):
    payload = [(str(k), json.dumps(v, default=str, sort_keys=True)) for k, v in sorted(record.items())]
    spark.createDataFrame(payload, "metric STRING, value STRING") \
         .withColumn("recorded_at", F.current_timestamp()) \
         .write.format("delta").mode("append").saveAsTable(SCRATCH_EVIDENCE)
    set_properties(SCRATCH_EVIDENCE, {
        "s6.pipeline.version": PIPELINE_VERSION,
        "s6.run_id": RUN_ID,
        "s6.evidence.kind": "BENCHMARK_AND_CORRECTNESS",
    })

def cluster_evidence():
    values = {}
    for key in [
        "spark.databricks.clusterUsageTags.clusterId",
        "spark.databricks.clusterUsageTags.clusterName",
        "spark.databricks.clusterUsageTags.sparkVersion",
        "spark.databricks.clusterUsageTags.numWorkers",
        "spark.databricks.clusterUsageTags.clusterMaxWorkers",
    ]:
        try:
            values[key] = spark.conf.get(key)
        except Exception:
            values[key] = None
    return values

def cleanup_run_scratch():
    for table in [SCRATCH_PIVOT]:
        if spark.catalog.tableExists(table):
            spark.sql(f"DROP TABLE {table}")
            print(f"dropped {table}")

# COMMAND ----------

if ACTION == "ig_selftest":
    test = f"{TARGET_SCHEMA}.s6_dicom_{RUN_TOKEN}_igtest"
    spark.sql(f"DROP TABLE IF EXISTS {test}")
    spark.sql(f"CREATE TABLE {test} (DICOM_PATH STRING, X INT) USING DELTA")
    ig_tag_table(test, {"DICOM_PATH": ("4", "2"), "X": ("0", "0")})
    ig_tag_gate(test)
    spark.sql(f"DROP TABLE {test}")
    dbutils.notebook.exit(json.dumps({"status": "PASS", "test": "ig_selftest", "run_id": RUN_ID}))

if ACTION == "pre_gates":
    assert not spark.catalog.tableExists(TARGET), (
        f"Expected pre-build target absence, but {TARGET} exists; use action=gates instead"
    )
    dbutils.notebook.exit(json.dumps({
        "status": "EXPECTED_FAIL_CONFIRMED",
        "gate": "G0 target absent before build",
        "run_id": RUN_ID,
    }))

if ACTION == "gates":
    gate_versions = source_versions()
    assert not build_is_due(gate_versions), "target source-version properties do not match live sources"
    result = run_gates(recompute_fingerprint=True)
    record_shared_versions(gate_versions)
    dbutils.notebook.exit(json.dumps({
        "status": "PASS", "run_id": RUN_ID, "gates": result,
        "shared_control": SHARED_CONTROL, "versions": gate_versions,
    }, default=str))

if ACTION == "cleanup":
    cleanup_run_scratch()
    dbutils.notebook.exit(json.dumps({"status": "CLEANED", "run_id": RUN_ID}))

if ACTION not in {"build", "benchmark"}:
    raise ValueError(f"Unsupported action: {ACTION}")

started = time.time()

if RESET_TARGET_VERSION:
    assert spark.catalog.tableExists(TARGET), "reset_target_version requires an existing target"
    spark.sql(f"RESTORE TABLE {TARGET} TO VERSION AS OF {int(RESET_TARGET_VERSION)}")
    print(f"restored {TARGET} content to version {RESET_TARGET_VERSION}")

versions_start = source_versions()
target_version_before = table_version(TARGET) if spark.catalog.tableExists(TARGET) else None

if not FORCE_REBUILD and not build_is_due(versions_start):
    target_version_after = table_version(TARGET)
    assert target_version_after == target_version_before
    evidence = {
        "status": "NO_OP",
        "result": "NO_OP",
        "target": TARGET,
        "target_schema": TARGET_SCHEMA,
        "run_id": RUN_ID,
        "source_versions_start": versions_start,
        "source_versions_end": source_versions(),
        "target_version_before": target_version_before,
        "target_version_after": target_version_after,
        "elapsed_seconds": time.time() - started,
        "cluster": cluster_evidence(),
        "fingerprint_stored": table_properties(TARGET).get("s6.fingerprint"),
    }
    write_evidence(evidence)
    print(json.dumps(evidence, default=str, sort_keys=True))
    dbutils.notebook.exit(json.dumps(evidence, default=str))

if scratch_matches(SCRATCH_PIVOT, versions_start):
    accounting = aggregate_accounting(SCRATCH_PIVOT)
    print(f"RETRY_RESUME: reusing completed pivot {SCRATCH_PIVOT}")
else:
    if spark.catalog.tableExists(SCRATCH_PIVOT):
        spark.sql(f"DROP TABLE {SCRATCH_PIVOT}")
    accounting = build_pivot_scratch(versions_start)
    print(json.dumps({"pivot_accounting": accounting}, sort_keys=True))

if INTERRUPT_AFTER_PHASE == "pivot":
    write_evidence({
        "status": "INTERRUPTED_AFTER_REAL_PIVOT",
        "run_id": RUN_ID,
        "source_versions_start": versions_start,
        "pivot_accounting": accounting,
        "scratch_pivot": SCRATCH_PIVOT,
        "elapsed_seconds": time.time() - started,
        "cluster": cluster_evidence(),
        "resume_instruction": f"rerun with resume_run_id={RUN_ID} and interrupt_after_phase blank",
    })
    raise RuntimeError(
        f"RETRY_RESUME_READY: real 562M-row pivot completed in {SCRATCH_PIVOT}; "
        f"resume with resume_run_id={RUN_ID}"
    )

try:
    fingerprint, flagged = enrich_and_publish(versions_start, accounting)
except Exception as exc:
    write_evidence({
        "status": "FAILED_AFTER_PIVOT",
        "run_id": RUN_ID,
        "source_versions_start": versions_start,
        "scratch_pivot": SCRATCH_PIVOT,
        "elapsed_seconds": time.time() - started,
        "exception_type": type(exc).__name__,
        "exception": str(exc),
        "traceback": traceback.format_exc(),
        "cluster": cluster_evidence(),
    })
    raise
versions_end = source_versions()
assert versions_end == versions_start, {
    "source_drift_invalidates_run": True,
    "start": versions_start,
    "end": versions_end,
}

gates = run_gates(recompute_fingerprint=True)
record_shared_versions(versions_start)
target_version_after = table_version(TARGET)
history = (spark.sql(f"DESCRIBE HISTORY {TARGET}")
           .where(F.col("operation").isin(
               "CREATE OR REPLACE TABLE AS SELECT", "CREATE TABLE AS SELECT",
               "WRITE", "MERGE"))
           .orderBy(F.desc("version")).limit(1).collect()[0])
evidence = {
    "status": "SUCCESS",
    "result": "BUILT",
    "target": TARGET,
    "target_schema": TARGET_SCHEMA,
    "run_id": RUN_ID,
    "source_versions_start": versions_start,
    "source_versions_end": versions_end,
    "target_version_before": target_version_before,
    "target_version_after": target_version_after,
    "target_write_version": int(history["version"]),
    "target_operation": history["operation"],
    "target_operation_metrics": history["operationMetrics"],
    "elapsed_seconds": time.time() - started,
    "cluster": cluster_evidence(),
    "fingerprint": fingerprint,
    "dq_flagged": flagged,
    "accounting": accounting,
    "gates": gates,
}
write_evidence(evidence)
print(json.dumps(evidence, default=str, sort_keys=True))

if CLEANUP_SCRATCH:
    cleanup_run_scratch()

dbutils.notebook.exit(json.dumps(evidence, default=str))

# COMMAND ----------

# BENCHMARK EVIDENCE CELLS — execute as separate one-time runs on General Cluster.
#
# 1. Cold / retry-resume run:
#    action=build, force_rebuild=true, run_id=<stable id>, interrupt_after_phase=pivot
#    The run intentionally fails only after materializing the genuine 562M-row pivot.
# 2. Resume:
#    action=build, resume_run_id=<same stable id>, interrupt_after_phase=""
#    The notebook must print RETRY_RESUME and reuse s6_dicom_<id>_pivot.
# 3. Qualify a target seed version from the successful resume.
# 4. Warm repetitions x3:
#    action=benchmark, force_rebuild=true, reset_target_version=<seed version>,
#    unique run_id per repetition. Capture one cold run separately and exclude it.
# 5. NO_OP repetitions x2:
#    action=build, force_rebuild=false, unique run_id; target version must not change.
# 6. Per repetition retain/report:
#    source versions at start/end, wall-clock, cluster id/name/workers, target Delta
#    operationMetrics, destination fingerprint and all correctness gates.
#    DBUs/node-hours, peak workers and Spark stage shuffle/spill/skew are captured from
#    the Databricks run UI because they are not exposed reliably inside the notebook.
# 7. Reject any repetition with source drift or a fingerprint/gate mismatch.
# 8. Do not claim an absolute production-runtime forecast from this dev cluster.

# COMMAND ----------

# PROMOTION RUNBOOK — HUMAN GATED; THIS NOTEBOOK NEVER EXECUTES THESE PROD WRITES.
#
# Preconditions:
#   - Human approves the static pilot publication and the explicit 4/2 identifier tags.
#   - B13 gates pass on the final dev version; source versions/fingerprint are recorded.
#   - Benchmark pack contains cold, warm median x3, NO_OP x2 and genuine RETRY_RESUME.
#
# Human-operated static promotion:
#   1. Create 4_prod.bronze.map_dicom_file_attribute from the approved dev target
#      using the estate-approved clone/copy method. Do not overwrite an existing prod table.
#   2. Reapply the PILOT COVERAGE table comment and every column's ig_risk/ig_severity tags.
#   3. Run G1a/G1b/G2/G3/G4/G5 against prod and compare the canonical fingerprint.
#   4. Record package B13 as partial_bronze until the extraction sweep widens.
#   5. Do NOT add a weekly Bronze_Pipeline step: this is a one-off static pilot source.
#      Revisit cadence only when the extraction process becomes estate-wide and recurring.
#   6. Keep retries=0 if an eventual scheduled version retains this retry fence.
#
# Rollback:
#   - Because this is a new table, human rollback is DROP of the newly-created prod table
#     after downstream-use confirmation. No existing production object is mutated.


