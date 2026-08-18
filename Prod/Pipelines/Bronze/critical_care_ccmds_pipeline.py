# Databricks notebook source
# critical_care_ccmds_pipeline - S2/A3. Design decisions: plan 2026-08-12 v2 Task 5.
# Policy decision 2026-08-12: allow the operational non-cds_* CCMDS landings.
# Period publishes every revision. Business-key ambiguity is quarantined and never guessed.
# Parent/child keys always combine CC_Period_Local_Id and normalized MRN; local ID alone is unsafe.
# Identifiers resolve at build time and are excluded. CDS_APC_ID is opaque passthrough only.
# Children exact-deduplicate with source-row accounting and retain all orphan history.
# Refresh is source-version due-check plus deterministic rebuild with tombstone carry-forward.
# Date-quality flags are derived by type; named source system stamps are exempt.

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 395793481300414)
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

from pyspark.sql import functions as F, Window

SOURCES = ["4_prod.raw.crit_care_period", "4_prod.raw.crit_care_activity",
           "4_prod.raw.crit_care_opcs", "4_prod.raw.mill_person_alias",
           "4_prod.raw.mill_encntr_alias", "3_lookup.dwh.nhs_data_dct_ref_deprecated"]
PIPE = "ccmds"
due, versions = due_check(CONTROL, PIPE, SOURCES)
if not (due or FULL):
    print("NO_OP: no source version advanced; targets untouched")
    dbutils.notebook.exit(json.dumps({"result": "NO_OP", "target": TARGET_SCHEMA, "target_schema": TARGET_SCHEMA}, sort_keys=True))

def mrn_norm(c):
    return F.regexp_replace(F.upper(F.trim(c)), "^0+", "")

alias10 = (spark.table("4_prod.raw.mill_person_alias")
    .filter("PERSON_ALIAS_TYPE_CD = 10 AND ACTIVE_IND = 1")
    .withColumn("ALIAS_N", mrn_norm(F.col("ALIAS")))
    .groupBy("ALIAS_N").agg(F.min("PERSON_ID").alias("_PID"),
                            F.countDistinct("PERSON_ID").alias("_NP")))
enc1077 = (spark.table("4_prod.raw.mill_encntr_alias")
    .filter("ENCNTR_ALIAS_TYPE_CD = 1077 AND ACTIVE_IND = 1")
    .groupBy("ALIAS").agg(F.min("ENCNTR_ID").alias("_EID"),
                          F.countDistinct("ENCNTR_ID").alias("_NE")))
dct = spark.table("3_lookup.dwh.nhs_data_dct_ref_deprecated")

def decode_dct(df, code_col, element_key, out_col):
    lk = (dct.filter(F.col("NHS_DATA_DICT_ELEMENT_NAME_KEY_TXT") == element_key)
          .select(F.col("NHS_DATA_DICT_NHS_CD_ALIAS").alias(f"_k_{out_col}"),
                  F.col("NHS_DATA_DICT_DESCRIPTION_TXT").alias(out_col)))
    return df.join(lk, F.col(code_col) == F.col(f"_k_{out_col}"), "left").drop(f"_k_{out_col}")

def link_person_mrn(df, mrn_col):
    d = (df.withColumn("MRN_NORM", mrn_norm(F.col(mrn_col)))
           .withColumn("_MRN_VALID", F.col("MRN_NORM").isNotNull() & (F.col("MRN_NORM") != ""))
           .join(alias10, F.col("MRN_NORM") == F.col("ALIAS_N"), "left")
           .withColumn("PERSON_ID",
               F.when(F.col("_MRN_VALID") & (F.col("_NP") == 1), F.col("_PID")).cast("bigint"))
           .withColumn("PERSON_LINK_STATUS",
               F.when(~F.col("_MRN_VALID"), "NO_MRN")
                .when(F.col("PERSON_ID").isNotNull(), "LINKED")
                .when(F.col("_NP") > 1, "AMBIGUOUS_MRN")
                .otherwise("UNMATCHED_MRN")))
    return d.drop("ALIAS_N", "_PID", "_NP", "_MRN_VALID")

# --- period ---
PERIOD_DECODES = [
    ("CC_Adm_Sorc_Cd", "CRITICALCAREADMSRC", "CC_ADM_SORC_DESC"),
    ("CC_Adm_Type_Cd", "CRITICALCAREADMTYPE", "CC_ADM_TYPE_DESC"),
    ("CC_Disch_Dest_Cd", "CRITICALCAREDISCHDESTINATION", "CC_DISCH_DEST_DESC"),
    ("CC_Disch_Locn_Cd", "CRITICALCAREDISCHLOC", "CC_DISCH_LOCN_DESC"),
    ("CC_Disch_Status_Cd", "CRITICALCAREDISCHSTATUS", "CC_DISCH_STATUS_DESC"),
    ("CC_Sorc_Locn_Cd", "CRITICALCARESRCLOC", "CC_SORC_LOCN_DESC"),
    ("CC_Bed_Config_Cd", "CRITICALCAREUNITBEDCONFIG", "CC_BED_CONFIG_DESC"),
    ("CC_Unit_Function_Cd", "CRITICALCAREUNITFUNCTION", "CC_UNIT_FUNCTION_DESC"),
    ("CC_Type", "CRITICALCAREPERIODTYPE", "CC_TYPE_DESC"),
]
p = link_person_mrn(spark.table("4_prod.raw.crit_care_period"), "Mrn")
for c, k, o in PERIOD_DECODES:
    p = decode_dct(p, c, k, o)
for fin_col, out in [("Fin_Nbr", "ENCNTR_ID"), ("Crit_Care_Fin_Nbr", "CC_ENCNTR_ID")]:
    e = (enc1077.withColumnRenamed("ALIAS", f"_a_{out}")
                .withColumnRenamed("_EID", f"_e_{out}").withColumnRenamed("_NE", f"_n_{out}"))
    p = (p.join(e, F.col(fin_col) == F.col(f"_a_{out}"), "left")
          .withColumn(out, F.when(F.col(f"_n_{out}") == 1, F.col(f"_e_{out}")).cast("bigint"))
          .drop(f"_a_{out}", f"_e_{out}", f"_n_{out}"))
wk = Window.partitionBy("CC_Period_Local_Id", "MRN_NORM")
p = (p.withColumn("_N_START", F.size(F.collect_set("CC_Period_Start_Dt_Tm").over(wk)))
      .withColumn("BUSINESS_KEY_STATUS",
          F.when(F.col("MRN_NORM").isNull() | (F.col("MRN_NORM") == ""), "NO_MRN")
           .when(F.col("_N_START") > 1, "AMBIGUOUS_MULTI_PERIOD")
           .otherwise("RESOLVED"))
      .drop("_N_START"))
w = (Window.partitionBy("CC_Period_Local_Id", "MRN_NORM")
     .orderBy(F.col("ADC_UPDT").desc_nulls_last(),
              F.col("Record_Updated_Dt").desc_nulls_last(),
              F.col("Crit_Care_Period_Id").desc()))
p = (p.withColumn("CURRENT_IND",
        (F.col("BUSINESS_KEY_STATUS") == "RESOLVED") & (F.row_number().over(w) == 1))
      .withColumn("PERIOD_BUSINESS_KEY", F.xxhash64("CC_Period_Local_Id", "MRN_NORM")))
p, p_dq = dq_all_clinical(p, admin_stamps={"Extract_Dt", "Record_Updated_Dt", "ADC_UPDT"})
PERIOD_PUBLISH_COLS = [c for c in p.columns
                       if c not in ("Mrn", "Fin_Nbr", "Crit_Care_Fin_Nbr", "MRN_NORM")]
p_publish = p.select(*PERIOD_PUBLISH_COLS).withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
replace_with_tombstones(p_publish, f"{SCHEMA}.map_critical_care_period", ["Crit_Care_Period_Id"])
spark.sql(f"CREATE OR REPLACE VIEW {SCHEMA}.map_critical_care_period_current AS "
          f"SELECT * FROM {SCHEMA}.map_critical_care_period WHERE CURRENT_IND AND SOURCE_PRESENT_IND")

# --- shared child machinery ---
keys = p.select(F.col("CC_Period_Local_Id").alias("_L"), F.col("MRN_NORM").alias("_M"),
                "BUSINESS_KEY_STATUS").distinct()
cur = (p.filter(F.col("CURRENT_IND"))
        .select(F.col("CC_Period_Local_Id").alias("_L"), F.col("MRN_NORM").alias("_M"),
                F.col("Crit_Care_Period_Id").alias("PARENT_PERIOD_SURROGATE_ID")))
cur2 = cur.withColumnRenamed("_L", "_L2").withColumnRenamed("_M", "_M2")

def child_frame(src_tbl, dedup_cols):
    c = spark.table(src_tbl)
    c = (c.groupBy(*dedup_cols)
          .agg(F.count(F.lit(1)).alias("SOURCE_DUPLICATE_COUNT"), F.max("ADC_UPDT").alias("ADC_UPDT")))
    c = link_person_mrn(c, "MRN")
    c = (c.join(keys, (F.col("CC_Period_Local_Id") == F.col("_L")) &
                      (F.col("MRN_NORM") == F.col("_M")), "left")
          .join(cur2, (F.col("CC_Period_Local_Id") == F.col("_L2")) &
                      (F.col("MRN_NORM") == F.col("_M2")), "left")
          .withColumn("PERIOD_LINK_STATUS",
              F.when(F.col("MRN_NORM").isNull() | (F.col("MRN_NORM") == ""), "NO_MRN")
               .when(F.col("BUSINESS_KEY_STATUS") == "AMBIGUOUS_MULTI_PERIOD", "AMBIGUOUS_PARENT")
               .when(F.col("PARENT_PERIOD_SURROGATE_ID").isNotNull(), "LINKED")
               .otherwise("ORPHAN_PERIOD"))
          .withColumn("PERIOD_BUSINESS_KEY", F.xxhash64("CC_Period_Local_Id", "MRN_NORM"))
          .drop("_L", "_M", "_L2", "_M2", "BUSINESS_KEY_STATUS"))
    return c

ACT_DEDUP = ["CC_Period_Local_Id", "MRN", "Activity_Date", "Activity_Code", "Activity_Number",
             "CC_Type", "CDS_APC_ID"]
a = child_frame("4_prod.raw.crit_care_activity", ACT_DEDUP)
a = decode_dct(a, "Activity_Code", "CRITICALCAREACTIVITY", "ACTIVITY_DESC")
a = a.withColumn("ROW_HASH", F.xxhash64("CC_Period_Local_Id", "MRN_NORM", "Activity_Date",
                                        "Activity_Code", "Activity_Number", "CC_Type", "CDS_APC_ID"))
a, a_dq = dq_all_clinical(a, admin_stamps={"ADC_UPDT"})
a = a.drop("MRN", "MRN_NORM").withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
replace_with_tombstones(a, f"{SCHEMA}.map_critical_care_activity", ["ROW_HASH"])

OPCS_DEDUP = ["CC_Period_Local_Id", "MRN", "OPCS_Proc_Dt", "OPCS_Proc_Code", "OPCS_Proc_Num",
              "CC_Type", "CDS_APC_ID"]
o = child_frame("4_prod.raw.crit_care_opcs", OPCS_DEDUP)
o = o.withColumn("_CC_TYPE_DCT", F.lpad(F.trim(F.col("CC_Type")), 2, "0"))
o = decode_dct(o, "_CC_TYPE_DCT", "CRITICALCAREPERIODTYPE", "CC_TYPE_DESC").drop("_CC_TYPE_DCT")
o = o.withColumn("ROW_HASH", F.xxhash64("CC_Period_Local_Id", "MRN_NORM", "OPCS_Proc_Dt",
                                        "OPCS_Proc_Code", "OPCS_Proc_Num", "CC_Type", "CDS_APC_ID"))
o, o_dq = dq_all_clinical(o, admin_stamps={"ADC_UPDT"})
o = o.drop("MRN", "MRN_NORM").withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
replace_with_tombstones(o, f"{SCHEMA}.map_critical_care_procedure", ["ROW_HASH"])

spark.sql(f"""COMMENT ON TABLE {SCHEMA}.map_critical_care_period IS
'Grain: every landed critical-care revision keyed by Crit_Care_Period_Id. Business keys combine CC_Period_Local_Id with normalized MRN because the local ID hash is reused across patients. RESOLVED keys alone receive CURRENT_IND. There are 517 normalized multi-start keys, nine overlapping the 178 NO_MRN keys; exclusive statuses are 508 AMBIGUOUS_MULTI_PERIOD, 178 NO_MRN, and 175726 RESOLVED across 176412 normalized keys. PERIOD_BUSINESS_KEY identifies a source business key and does not imply one real period for ambiguous keys. MRN, FIN, critical-care FIN, and normalized MRN are excluded after unique alias linkage. CDS_APC_ID is opaque passthrough and is never joined to cds_apc. Nine code columns decode by LEFT join to the NHS DCT lookup. SOURCE_PRESENT_IND preserves hard deletes as tombstones. The RDE oracle casts the hex local ID to BIGINT and loses 39.909 percent, so comparisons are coverage-only. Policy approval: 2026-08-12 CCMDS memo. Refresh is per-source Delta-version due-check followed by deterministic rebuild. Coordinate before rebuild if Journey later registers this table.'""")
spark.sql(f"""COMMENT ON TABLE {SCHEMA}.map_critical_care_activity IS
'Grain: one exact-deduplicated activity natural key represented by ROW_HASH; SOURCE_DUPLICATE_COUNT reconciles all source rows. Parent linkage always uses local period ID plus normalized MRN. Ambiguous and no-MRN parents receive no surrogate; historical orphans are retained. Activity_Code decodes by LEFT join to the NHS DCT; integer CC_Type remains raw because semantics are unverified. MRN is excluded after alias linkage. CDS_APC_ID is opaque passthrough. SOURCE_PRESENT_IND preserves hard deletes as tombstones. Activity_Date carries quality flags and future rows are retained. Refresh is per-source Delta-version due-check followed by deterministic rebuild.'""")
spark.sql(f"""COMMENT ON TABLE {SCHEMA}.map_critical_care_procedure IS
'Grain: one exact-deduplicated OPCS natural key represented by ROW_HASH; SOURCE_DUPLICATE_COUNT reconciles all source rows. Parent linkage always uses local period ID plus normalized MRN. Ambiguous and no-MRN parents receive no surrogate; the heavy pre-2023 orphan history is a source property and is retained. String CC_Type is retained raw and zero-padded only for its PERIODTYPE LEFT join to the NHS DCT. MRN is excluded after alias linkage. CDS_APC_ID is opaque passthrough. SOURCE_PRESENT_IND preserves hard deletes as tombstones. OPCS_Proc_Dt carries quality flags. Refresh is per-source Delta-version due-check followed by deterministic rebuild.'""")
record_versions(CONTROL, PIPE, versions)
print(f"BUILD done; dq flagged: period={p_dq}, activity={a_dq}, opcs={o_dq}")

# COMMAND ----------

# PROMOTION RUNBOOK (human-gated)
# 1. Copy into /Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/ and integrate retained-column
#    contracts into _bronze_common before the first production write. Fold into
#    completeness_pipeline if S1 promotion created it; otherwise the promoter chooses.
# 2. Set target_schema=4_prod.bronze and set control_table to the production control
#    location returned by the bronze_control_schema() convention. The config guard rejects
#    a production target paired with the dev control table.
# 3. Run one full build from the integrated production notebook, then rerun all A3 gates
#    against production. Treat RDE comparisons as coverage-only because of the confirmed
#    Period_ID cast defect.
# 4. Register after dependencies in bronze_pipeline orchestration. retries=0 is safe:
#    due-check plus deterministic rebuild makes a retry a no-op or a clean rebuild.
# 5. Compare the next weekly runtime with the S2 execution-log baseline.
# 6. Preserve the approved policy constraints: CDS_APC_ID remains opaque and no cds_apc
#    source or join is introduced. Resolve ambiguous keys only through a reviewed
#    follow-up, never by guessing.
# 7. Interface caveat: these products rebuild on source change. If Journey registers one
#    in silver_source_registry.py, coordinate under interface rule 1 before the next rebuild
#    and likely convert the target to keyed MERGE.

dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET_SCHEMA, "target_schema": TARGET_SCHEMA}, sort_keys=True))


