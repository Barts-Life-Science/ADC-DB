# Databricks notebook source
# neonatal_pipeline - S2/A5. Design decisions: plan 2026-08-12 v2 Task 4.
# True source grains are retained: EntityID for episode/narrative/exam and
# (EntityID, ActivityDate) for NCCMDS. Identifiers resolve at build time and are excluded.
# User decision 2026-08-12: retain the 13 map_mat_birth-overlap fields because the live
# bridge covers only 41.262% of linked episodes; document duplication rather than lose data.
# Free prose and staff-name fields use a governed narrative sidecar with restricted prod grants.
# EpisodeID on NCCMDS is a truncated GUID and is excluded. Placeholder exam rows are retained.
# Refresh is source-version due-check plus deterministic rebuild with tombstone carry-forward.
# Date-quality flags are derived by type; named source system stamps are exempt.

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 961364647615119)
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

SCHEMA = TARGET_SCHEMA
CONTROL = f"{CONTROL_SCHEMA}.s2_source_versions"
FULL = (
    _widget_text("full_rebuild", "false").lower() in ("1", "true", "yes")
    or _widget_text("force_full_refresh", "false").lower() == "true"
)

# COMMAND ----------

# ==== COMMON BLOCK v1 (SYNC-WITH _completeness_common) ====
from pyspark.sql import functions as F

SENTINEL_FLOOR = "1901-01-01"

def dq_columns(df, date_cols):
    """Master plan §2.2 date-quality standard block.
    For each timestamp column C adds:
      C_FUTURE_IND   - value is after now()
      C_SENTINEL_IND - value is before 1901-01-01
      C_CLEAN        - value, or NULL when either flag is set
    Source column is retained untouched (bronze keeps source values; silver chooses).
    """
    out = df
    for c in date_cols:
        fut = F.col(c) > F.current_timestamp()
        sen = F.col(c) < F.lit(SENTINEL_FLOOR).cast("timestamp")
        out = (out
               .withColumn(f"{c}_FUTURE_IND", F.when(F.col(c).isNull(), F.lit(None)).otherwise(fut))
               .withColumn(f"{c}_SENTINEL_IND", F.when(F.col(c).isNull(), F.lit(None)).otherwise(sen))
               .withColumn(f"{c}_CLEAN", F.when(fut | sen, F.lit(None).cast("timestamp")).otherwise(F.col(c))))
    return out

def get_watermark(control_table, source_name, default="1980-01-01"):
    """Per-source watermark (master plan §2.3 rule 5 - one row per source, never GREATEST across sources)."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {control_table} (
        source_name STRING, watermark TIMESTAMP, updated_at TIMESTAMP)""")
    rows = spark.sql(f"""SELECT watermark FROM {control_table}
                         WHERE source_name = '{source_name}'""").collect()
    return rows[0]["watermark"] if rows else spark.sql(
        f"SELECT CAST('{default}' AS TIMESTAMP) w").collect()[0]["w"]

def set_watermark(control_table, source_name, new_wm):
    """new_wm must be the SOURCE MAX(ADC_UPDT) observed this run (source-change clock, never build clock)."""
    if new_wm is None:
        return
    spark.sql(f"""MERGE INTO {control_table} t
        USING (SELECT '{source_name}' source_name, CAST('{new_wm}' AS TIMESTAMP) watermark) s
        ON t.source_name = s.source_name
        WHEN MATCHED AND s.watermark > t.watermark
             THEN UPDATE SET t.watermark = s.watermark, t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (source_name, watermark, updated_at)
             VALUES (s.source_name, s.watermark, current_timestamp())""")
# ==== END COMMON BLOCK v1 ====

# COMMAND ----------

# ==== S2 BLOCK v1 (SYNC-WITH _completeness_common) ====
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType

def table_version(tbl):
    """Current Delta commit version (metadata-only read)."""
    return spark.sql(f"DESCRIBE HISTORY {tbl} LIMIT 1").collect()[0]["version"]

def due_check(control_table, pipeline, sources):
    """Master plan S2.3 rule-4 due-check. Returns (due, current_versions): due=False iff
    EVERY source table's Delta version matches the last recorded successful run.
    Per-source rows (rule 5) - never a combined high-watermark."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {control_table}
        (pipeline STRING, source STRING, version BIGINT, updated_at TIMESTAMP)""")
    cur = {t: table_version(t) for t in sources}
    seen = {r["source"]: r["version"] for r in spark.sql(
        f"SELECT source, version FROM {control_table} WHERE pipeline = '{pipeline}'").collect()}
    return any(seen.get(t) != v for t, v in cur.items()), cur

def record_versions(control_table, pipeline, versions):
    """Call ONLY after a successful publish - a crashed run must re-run in full."""
    for t, v in versions.items():
        spark.sql(f"""MERGE INTO {control_table} c
            USING (SELECT '{pipeline}' pipeline, '{t}' source, CAST({v} AS BIGINT) version) s
            ON c.pipeline = s.pipeline AND c.source = s.source
            WHEN MATCHED THEN UPDATE SET c.version = s.version, c.updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (pipeline, source, version, updated_at)
                 VALUES (s.pipeline, s.source, s.version, current_timestamp())""")

def dq_all_clinical(df, admin_stamps):
    """S2.2 date-quality standard, v2 rule: flag EVERY retained temporal column except the
    product's NAMED admin/system stamps (the declared contract) and derived *_CLEAN columns.
    Returns (df_with_flags, flagged_column_list) - log the list in the session log."""
    cols = [f.name for f in df.schema.fields
            if isinstance(f.dataType, (TimestampType, DateType))
            and f.name not in admin_stamps and not f.name.endswith("_CLEAN")]
    return dq_columns(df, cols), cols

def replace_with_tombstones(df, target, key_cols):
    """Deterministic replace with NO silent hard deletes (S2.2 lifecycle): rows present in
    the prior published version but absent from the fresh build are re-appended with
    SOURCE_PRESENT_IND=false, retaining their previous column values and stamps.
    A key that reappears at source is resurrected as present (its tombstone drops out)."""
    fresh = df.withColumn("SOURCE_PRESENT_IND", F.lit(True))
    v_prev = table_version(target) if spark.catalog.tableExists(target) else None
    (fresh.write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true").saveAsTable(target))
    if v_prev is not None:
        prior = spark.read.option("versionAsOf", v_prev).table(target)
        gone = (prior.join(spark.table(target).select(*key_cols).distinct(),
                           key_cols, "left_anti")
                     .withColumn("SOURCE_PRESENT_IND", F.lit(False)))
        gone.write.format("delta").mode("append").saveAsTable(target)

def table_fingerprint(tbl, exclude=("PIPELINE_UPDT_DT_TM",)):
    """Canonical whole-row fingerprint: order-independent sum of xxhash64 over the JSON of
    every column except volatile stamps. Equal fingerprint == identical published content."""
    cols = [c for c in spark.table(tbl).columns if c not in exclude]
    return (spark.table(tbl)
            .select(F.sum(F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in cols])))
                          .cast("decimal(38,0)")).alias("fp"))
            .collect()[0]["fp"])
# ==== END S2 BLOCK v1 ====

# COMMAND ----------

from pyspark.sql import functions as F

SOURCES = ["4_prod.raw.nnu_episodes", "4_prod.raw.nnu_routineexamination",
           "4_prod.raw.nnu_nccmds", "4_prod.raw.mill_person_alias"]
PIPE = "neonatal"
due, versions = due_check(CONTROL, PIPE, SOURCES)
if not (due or FULL):
    print("NO_OP: no source version advanced; targets untouched")
    dbutils.notebook.exit(json.dumps({"result": "NO_OP", "target": TARGET_SCHEMA, "target_schema": TARGET_SCHEMA}, sort_keys=True))

EXCLUDE_IDENTIFIER = ["NationalIDBaby", "NationalIDBabyAnon", "HospitalIDBaby", "SurnameBaby",
    "ForenameBaby", "NationalIDMother", "HospitalIDMother", "SurnameMother", "ForenameMother",
    "AddressMother", "PostCodeMother", "PhoneMother", "EmailMother", "DischargeAddress",
    "GPCode", "GPName", "GPAddress", "GPPostCode", "GPPhone"]
ALLOW_RETAINED_DUPLICATE = ["Sex", "Apgar1", "Apgar5", "Birthweight", "BirthLength",
    "BirthHeadCircumference", "GestationWeeks", "GestationDays", "GestationWeeksCalculated",
    "GestationDaysCalculated", "FinalNNUOutcome", "PlaceOfBirthName", "Resuscitation"]
EXCLUDE_DECLARED = ([f"preg{i}{s}" for i in range(1, 11) for s in ("gestwks", "gestdays", "outcome")]
    + ["BabyEthnicGroup_Scotland", "BabyEthnicGroup_NI", "BabyEthnicGroup_NZ",
       "MumEthnicGroup_Scotland", "MumEthnicGroup_NI", "MumEthnicGroup_NZ", "MumEthnicGroupOther_NZ",
       "DadEthnicGroup_Scotland", "DadEthnicGroup_NI", "DadEthnicGroup_NZ", "DadEthnicGroupOther_NZ",
       "RowHash"])
NARRATIVE = ["BirthSummary", "EpisodeSummary", "AdmitPrincipalReason_Other", "DrugsDuringStay",
    "DiagnosisDuringStay", "PrincipalDiagnosisAtDischarge", "ActiveProblemsAtDischarge",
    "PrincipleProceduresDuringStay", "RespiratoryDiagnoses", "CardiovascularDiagnoses",
    "GastrointestinalDiagnoses", "NeurologyDiagnoses", "ROPDiagnosis", "HaemDiagnoses",
    "RenalDiagnoses", "SkinDiagnoses", "MetabolicDiagnoses", "MetabolicDiagnoses1",
    "InfectionsDiagnoses", "SocialIssues", "CongenitalAnomalies", "MaternalMedicalNotes",
    "AnomalyScanComments", "FinalSummaryText", "ProblemsMedicalMother", "ProblemsPregnancyMother",
    "DrugsAbusedMother", "DrugsInLabour", "Anticonvulsants", "Inotropes",
    "PulmonaryVasodilatorDrugs", "ResearchStudy", "ResearchStudyDrugs", "OtherRelevantCauses",
    "InfantMainDiseasesOrCond", "InfantOtherDiseasesOrCond", "InfantMainMaternalDiseasesOrCond",
    "InfantOthernMaternalDiseasesOrCond", "CauseOfDeath1A", "CauseOfDeath1B", "CauseOfDeath2",
    "DiedCause", "Consultant", "OtherConsultant", "ObstetricConsultant", "StaffAtResus",
    "AdmissionStaffDes", "DischargeSummaryCompletedBy", "DischargeSummaryCompletedBy_Grade",
    "ConsultationWithParents_StaffDesignation", "PregnancyDetails", "ReasonReceivingUnitChosen"]

src = spark.table("4_prod.raw.nnu_episodes")
explicit_lists = [EXCLUDE_IDENTIFIER, ALLOW_RETAINED_DUPLICATE, EXCLUDE_DECLARED, NARRATIVE]
explicit_union = set().union(*[set(x) for x in explicit_lists])
missing = explicit_union - set(src.columns)
assert not missing, f"manifest names absent from source: {missing}"
assert sum(len(x) for x in explicit_lists) == len(explicit_union), "manifest lists overlap"
ALLOW_REMAINDER = [c for c in src.columns if c not in explicit_union]
covered = explicit_union | set(ALLOW_REMAINDER)
assert covered == set(src.columns) and len(covered) == len(src.columns), "manifest coverage failure"
excluded = set(EXCLUDE_IDENTIFIER) | set(EXCLUDE_DECLARED) | set(NARRATIVE)
ALLOW = [c for c in src.columns if c not in excluded]
assert set(ALLOW_RETAINED_DUPLICATE).issubset(ALLOW), "retained bridge fields must publish"
print(f"MANIFEST allow={len(ALLOW)} remainder={len(ALLOW_REMAINDER)} "
      f"retained_duplicate={len(ALLOW_RETAINED_DUPLICATE)} narrative={len(NARRATIVE)} "
      f"identifier={len(EXCLUDE_IDENTIFIER)} declared={len(EXCLUDE_DECLARED)} total={len(src.columns)}")

alias18 = (spark.table("4_prod.raw.mill_person_alias")
    .filter("PERSON_ALIAS_TYPE_CD = 18 AND ACTIVE_IND = 1")
    .withColumn("ALIAS_N", F.trim(F.col("ALIAS")))
    .groupBy("ALIAS_N").agg(F.min("PERSON_ID").alias("_PID"),
                            F.countDistinct("PERSON_ID").alias("_NP")))

def link_nhs(df, nhs_col, out_id, status_col=None):
    d = (df.withColumn("_NHS_N", F.trim(F.col(nhs_col)))
           .withColumn("_temp", F.col("_NHS_N").startswith("T:"))
           .withColumn("_valid", F.col("_NHS_N").rlike("^[0-9]{10}$"))
           .join(alias18, F.col("_NHS_N") == F.col("ALIAS_N"), "left")
           .withColumn(out_id,
               F.when(F.col("_valid") & (F.col("_NP") == 1), F.col("_PID")).cast("bigint")))
    if status_col:
        d = d.withColumn(status_col,
            F.when(F.col("_temp"), "TEMP_ID")
             .when(~F.coalesce(F.col("_valid"), F.lit(False)), "NO_VALID_NHS")
             .when(F.col(out_id).isNotNull(), "LINKED")
             .when(F.col("_NP") > 1, "AMBIGUOUS_NHS")
             .otherwise("UNMATCHED_NHS"))
    return d.drop("ALIAS_N", "_PID", "_NP", "_NHS_N", "_temp", "_valid")

# --- episode (structured) + narrative sidecar, from one linked frame ---
ep_full = link_nhs(src, "NationalIDBaby", "BABY_PERSON_ID", "PERSON_LINK_STATUS")
ep_full = link_nhs(ep_full, "NationalIDMother", "MOTHER_PERSON_ID")
DERIVED = ["BABY_PERSON_ID", "PERSON_LINK_STATUS", "MOTHER_PERSON_ID"]
ep = ep_full.select(*(ALLOW + DERIVED))
ep, ep_dq = dq_all_clinical(ep, admin_stamps={"LastUpdate", "RecordTimestamp", "ADC_UPDT"})
ep = ep.withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
replace_with_tombstones(ep, f"{SCHEMA}.map_neonatal_episode", ["EntityID"])

exam_src = spark.table("4_prod.raw.nnu_routineexamination")
nar = (ep_full.select("EntityID", "BABY_PERSON_ID", "PERSON_LINK_STATUS", "ADC_UPDT", *NARRATIVE)
    .join(exam_src.select("EntityID", "NameOfExaminer", "GeneralComments"), "EntityID", "left")
    .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp()))
replace_with_tombstones(nar, f"{SCHEMA}.map_neonatal_episode_narrative", ["EntityID"])

# --- examination (findings; named narrative fields live in the sidecar) ---
ep_link = (spark.table(f"{SCHEMA}.map_neonatal_episode")
    .filter(F.col("SOURCE_PRESENT_IND"))
    .select("EntityID", "BABY_PERSON_ID", "PERSON_LINK_STATUS"))
ex = exam_src.drop("NameOfExaminer", "GeneralComments", "RowHash")
ex, ex_dq = dq_all_clinical(ex, admin_stamps={"RecordTimestamp", "ADC_UPDT"})
ex = (ex.join(ep_link, "EntityID", "left")
    .withColumn("PERSON_LINK_STATUS", F.coalesce(F.col("PERSON_LINK_STATUS"), F.lit("NO_EPISODE")))
    .withColumn("EXAM_POPULATED_IND",
        F.col("DateOfExamination").isNotNull() | F.col("Overall").isNotNull())
    .withColumn("EXAM_DATE_DERIVED", F.coalesce(F.col("DateOfExamination"), F.col("RecordTimestamp")))
    .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp()))
replace_with_tombstones(ex, f"{SCHEMA}.map_neonatal_examination", ["EntityID"])

# --- critical care (daily NCCMDS) ---
cc = spark.table("4_prod.raw.nnu_nccmds")
cc = link_nhs(cc, "NHSNumberBaby", "BABY_PERSON_ID", "PERSON_LINK_STATUS")
ep_ids = (spark.table(f"{SCHEMA}.map_neonatal_episode")
    .filter(F.col("SOURCE_PRESENT_IND"))
    .select("EntityID").distinct().withColumn("_HAS_EP", F.lit(True)))
cc = (cc.join(ep_ids, "EntityID", "left")
    .withColumn("EPISODE_LINK_STATUS", F.when(F.col("_HAS_EP"), "LINKED").otherwise("NO_EPISODE"))
    .drop("_HAS_EP", "NHSNumberBaby", "NameBaby", "EpisodeID", "RowHash"))
cc, cc_dq = dq_all_clinical(cc, admin_stamps={"RecordTimestamp", "ADC_UPDT"})
cc = cc.withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
replace_with_tombstones(cc, f"{SCHEMA}.map_neonatal_critical_care", ["EntityID", "ActivityDate"])

spark.sql(f"""COMMENT ON TABLE {SCHEMA}.map_neonatal_episode IS
'Grain: one row per BadgerNet EntityID. Five-way manifest: identifier exclusions, retained duplicate birth fields, governed narrative sidecar fields, declared exclusions, and structured allow remainder. The 13 birth-summary fields are intentionally retained because only 41.262 percent of linked episodes bridge to an NNU-bearing map_mat_birth row; consumers should prefer map_mat_birth when present. BirthTimeBaby and DateTimeOfDeath remain for identity and death-block coherence. Baby and mother identifiers are excluded after unique active alias-18 linkage. SOURCE_PRESENT_IND preserves hard deletes as tombstones. LastUpdate, RecordTimestamp, and ADC_UPDT are declared admin stamps. True source grain differs from the collapsed RDE oracle. Refresh is per-source Delta-version due-check followed by deterministic rebuild. Coordinate before rebuild if Journey later registers this table.'""")
spark.sql(f"""COMMENT ON TABLE {SCHEMA}.map_neonatal_episode_narrative IS
'Grain: one row per episode EntityID. Governed sidecar containing 52 episode narrative fields plus NameOfExaminer and GeneralComments. Free text may embed identifiers; production promotion requires restricted default-deny grants. Join to map_neonatal_episode by EntityID. SOURCE_PRESENT_IND preserves hard deletes as tombstones. Refresh is per-source Delta-version due-check followed by deterministic rebuild. Coordinate before rebuild if Journey later registers this table.'""")
spark.sql(f"""COMMENT ON TABLE {SCHEMA}.map_neonatal_examination IS
'Grain: one source routine-examination row per EntityID, including placeholder rows. EXAM_POPULATED_IND uses DateOfExamination or Overall; four source rows populated only by examiner name remain in the narrative sidecar and do not set this flag. EXAM_DATE_DERIVED follows the RDE coalesce rule. NameOfExaminer, GeneralComments, and RowHash are excluded here. NO_EPISODE identifies six current source orphans. SOURCE_PRESENT_IND preserves hard deletes as tombstones. RecordTimestamp and ADC_UPDT are declared admin stamps. Refresh is per-source Delta-version due-check followed by deterministic rebuild.'""")
spark.sql(f"""COMMENT ON TABLE {SCHEMA}.map_neonatal_critical_care IS
'Grain: daily NCCMDS row keyed by EntityID and ActivityDate. EpisodeID is excluded because it is a truncated GUID and must never be used to join episodes; full EntityID is authoritative. NHSNumberBaby and NameBaby are excluded after unique active alias-18 linkage. EPISODE_LINK_STATUS retains orphan entities. The table preserves daily source grain unlike the period-collapsed RDE oracle. SOURCE_PRESENT_IND preserves hard deletes as tombstones. RecordTimestamp and ADC_UPDT are declared admin stamps. Refresh is per-source Delta-version due-check followed by deterministic rebuild.'""")
record_versions(CONTROL, PIPE, versions)
print(f"BUILD done; dq flagged: episode={ep_dq}, exam={ex_dq}, nccmds={cc_dq}")

# COMMAND ----------

# PROMOTION RUNBOOK (human-gated)
# 1. Copy into /Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/ and integrate retained-column
#    contracts into _bronze_common before the first production write. Fold into
#    completeness_pipeline if S1 promotion created it; otherwise the promoter chooses.
# 2. Set target_schema=4_prod.bronze and set control_table to the production control
#    location returned by the bronze_control_schema() convention. The config guard rejects
#    a production target paired with the dev control table.
# 3. Run one full build from the integrated production notebook, then rerun all A5 gates
#    against production.
# 4. Register after dependencies in bronze_pipeline orchestration. retries=0 is safe:
#    due-check plus deterministic rebuild makes a retry a no-op or a clean rebuild.
# 5. Compare the next weekly runtime with the S2 execution-log baseline.
# 6. Apply restricted default-deny production grants to map_neonatal_episode_narrative;
#    free text and staff names can embed identifiers.
# 7. Interface caveat: these products rebuild on source change. If Journey registers one
#    in silver_source_registry.py, coordinate under interface rule 1 before the next rebuild
#    and likely convert the target to keyed MERGE.

dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET_SCHEMA, "target_schema": TARGET_SCHEMA}, sort_keys=True))


