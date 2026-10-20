# Databricks notebook source
# MAGIC %md
# MAGIC # RBI 1: resource streams
# MAGIC Reads Journey gold, builds one private MV per resource stream, then gives rows without a native
# MAGIC encounter an encounter from a unique observed attendance (rbi_linkage). Logic lives in the
# MAGIC tested helper files beside this notebook; this notebook only wires them into MVs.

# COMMAND ----------

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import functions as F

import rbi_streams as S
from rbi_linkage import apply_attribution, attendance_intervals, attribution_candidates
from rbi_schema import restrict_to_sample, sample_persons, try_bigint

GOLD = spark.conf.get("rbi.gold_schema", "4_prod.gold")
WINDOW = spark.conf.get("rbi.window_start", "2019-04-01")
SAMPLE_MOD = int(spark.conf.get("rbi.sample_mod", "0"))
TARGET_CATALOG = spark.conf.get("rbi.target_catalog")
ENABLE_BEDS = spark.conf.get("rbi.enable_ward_bed_day", "false") == "true"
STAY_SOURCE = spark.conf.get("rbi.location_stay_source", f"{GOLD}.spine_location_stay")
RESULT_EVIDENCE = spark.conf.get("rbi.pathology_result_evidence", "true") == "true"

# Guards: London-day maths needs UTC sessions; prod reads gold only, unsampled.
assert spark.conf.get("spark.sql.session.timeZone") == "UTC", "set spark.sql.session.timeZone=UTC in the pipeline configuration"
assert TARGET_CATALOG in ("8_dev", "4_prod"), TARGET_CATALOG
if TARGET_CATALOG == "4_prod":
    assert GOLD == "4_prod.gold" and SAMPLE_MOD == 0 and STAY_SOURCE == "4_prod.gold.spine_location_stay", "prod must read gold, unsampled"


def gold(name):
    """Read a gold table."""
    return spark.read.table(f"{GOLD}.{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dev person sample (only when rbi.sample_mod > 0). Public in dev so the validation notebook can use it.

# COMMAND ----------

if SAMPLE_MOD > 0:
    @dp.materialized_view(name="rbi_sample_person", comment="Dev-only person cohort: pmod(xxhash64(person_id), rbi.sample_mod) = 0.")
    def rbi_sample_person():
        return sample_persons(gold("spine_person"), SAMPLE_MOD)


def sampled(df):
    """Restrict a person-level source to the dev cohort; no-op in prod. Normalises person_id to BIGINT first."""
    if SAMPLE_MOD == 0:
        return df
    return restrict_to_sample(df.withColumn("person_id", try_bigint("person_id")), spark.read.table("rbi_sample_person"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streams

# COMMAND ----------

STREAMS = []  # names of the stream MVs, unioned below


def stream(name):
    """Register a private stream MV name for the union."""
    STREAMS.append(name)
    return dp.materialized_view(name=name, private=True)


if ENABLE_BEDS:
    @stream("_stream_ward_bed_day")
    def _stream_ward_bed_day():
        stays = spark.read.table(STAY_SOURCE)
        needed = {"occupancy_start", "occupancy_end", "occupancy_start_basis", "occupancy_end_basis"}
        assert needed <= set(stays.columns), f"{STAY_SOURCE} lacks {needed - set(stays.columns)} (gold parity phase A)"
        # Encounters are a lookup by encounter_id, never person-sampled: a stay's person can differ from its
        # encounter's person (merges), and sampling both would silently drop the stay in dev.
        return S.ward_bed_days(sampled(stays), gold("spine_encounter"), WINDOW)


@stream("_stream_critical_care_day")
def _stream_critical_care_day():
    return S.critical_care_days(sampled(gold("clinical_critical_care_period")), WINDOW)


@stream("_stream_neonatal_care_day")
def _stream_neonatal_care_day():
    return S.neonatal_care_days(sampled(gold("clinical_neonatal_care_day")), WINDOW)


@stream("_stream_theatre_case")
def _stream_theatre_case():
    return S.theatre_cases(sampled(gold("reference_theatre_case")), WINDOW)


@stream("_stream_outpatient_appointment")
def _stream_outpatient_appointment():
    return S.outpatient_appointments(sampled(gold("clinical_appointment")), WINDOW)


@stream("_stream_tracking_attendance")
def _stream_tracking_attendance():
    return S.tracking_attendances(sampled(gold("clinical_tracking_attendance")), WINDOW)


if RESULT_EVIDENCE:
    @dp.materialized_view(name="_pathology_result_evidence", private=True)
    def _pathology_result_evidence():
        return S.pathology_result_evidence(sampled(gold("clinical_pathology_result")))


@stream("_stream_pathology_tests")
def _stream_pathology_tests():
    evidence = spark.read.table("_pathology_result_evidence") if RESULT_EVIDENCE else None
    return S.pathology_tests(sampled(gold("clinical_pathology_order")), sampled(gold("clinical_specimen")), evidence, WINDOW)


@stream("_stream_imaging_exam")
def _stream_imaging_exam():
    return S.imaging_exams(sampled(gold("clinical_imaging_exam")), WINDOW)


@stream("_stream_medication_admin")
def _stream_medication_admin():
    return S.medication_admins(sampled(gold("clinical_medication_admin")), WINDOW)


@stream("_stream_high_cost_drug")
def _stream_high_cost_drug():
    return S.high_cost_drug_lines(sampled(gold("clinical_drug_expenditure")), WINDOW)


@stream("_stream_transfusion")
def _stream_transfusion():
    return S.transfusions(sampled(gold("clinical_transfusion")), WINDOW)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attribution: observed attendance intervals, then every stream row gets its encounter method

# COMMAND ----------

@dp.materialized_view(name="_attendance_interval", private=True)
def _attendance_interval():
    return attendance_intervals(sampled(gold("spine_encounter")))


def _attributed(stream_table):
    """Two shallow MVs per stream (candidates, then apply), so a change in one source recomputes only that
    stream and each step can refresh incrementally."""
    suffix = stream_table[len("_stream"):]
    cand_name, attr_name = "_cand" + suffix, "_attr" + suffix

    @dp.materialized_view(name=cand_name, private=True)
    def _cand():
        return attribution_candidates(spark.read.table(stream_table), spark.read.table("_attendance_interval"))

    @dp.materialized_view(name=attr_name, private=True)
    def _attr():
        return apply_attribution(spark.read.table(stream_table), spark.read.table(cand_name))
    return attr_name


ATTRIBUTED = [_attributed(n) for n in STREAMS]


@dp.materialized_view(name="_events_attributed", private=True)
def _events_attributed():
    # A plain union of the per-stream attributed MVs.
    return reduce(lambda a, b: a.unionByName(b), [spark.read.table(n) for n in ATTRIBUTED])
