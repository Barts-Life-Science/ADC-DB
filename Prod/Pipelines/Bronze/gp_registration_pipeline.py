# Databricks notebook source
# PMS_P1_T5_BRONZE_V1 — registered GP practice history per person from Millennium PERSON_ORG_RELTN
# (reltn 4072315 "Registered Practice"), with the practice ODS code from ORGANIZATION_ALIAS pool
# 6031508 ("NHS Client Code"). Grain: one PERSON_ORG_RELTN_ID. Full rebuild each run (~7M rows).
import json
from pyspark.sql import Window, functions as F

for k, v in {"target_schema": "8_dev.pms_bronze", "allow_production_write": "false"}.items():
    dbutils.widgets.text(k, v)
TARGET_SCHEMA = dbutils.widgets.get("target_schema")
assert TARGET_SCHEMA.startswith("8_dev.") or dbutils.widgets.get("allow_production_write") == "true", (
    f"Refusing to write {TARGET_SCHEMA} without allow_production_write=true")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
TARGET = f"{TARGET_SCHEMA}.map_person_gp_registration"
REGISTERED_PRACTICE_CD, ODS_POOL_CD = 4072315, 6031508
FAR_FUTURE = F.lit("2100-01-01").cast("timestamp")

# COMMAND ----------

reltn = spark.table("4_prod.raw.mill_person_org_reltn").where(F.col("PERSON_ORG_RELTN_CD").cast("bigint") == REGISTERED_PRACTICE_CD)
# Raw is append-landed: keep the latest landed version of each relationship row.
w_r = Window.partitionBy("PERSON_ORG_RELTN_ID").orderBy(F.col("ADC_UPDT").desc_nulls_last(), F.col("UPDT_CNT").desc_nulls_last(), F.col("UPDT_DT_TM").desc_nulls_last())
reltn = reltn.withColumn("_rn", F.row_number().over(w_r)).where("_rn = 1").drop("_rn")

alias = (spark.table("4_prod.raw.mill_organization_alias")
         .where((F.col("ALIAS_POOL_CD").cast("bigint") == ODS_POOL_CD) & (F.col("ACTIVE_IND") == 1))
         .select(F.col("ORGANIZATION_ID").cast("bigint").alias("ORGANIZATION_ID"), F.upper(F.trim("ALIAS")).alias("PRACTICE_ODS_CODE"),
                 "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM", "ORGANIZATION_ALIAS_ID"))
# One code per organisation: well-formed ODS code first, then latest effective, then highest alias id.
w_a = Window.partitionBy("ORGANIZATION_ID").orderBy(
    F.col("PRACTICE_ODS_CODE").rlike(r"^[A-Z][0-9]{5}$").desc(), F.col("END_EFFECTIVE_DT_TM").desc_nulls_last(),
    F.col("BEG_EFFECTIVE_DT_TM").desc_nulls_last(), F.col("ORGANIZATION_ALIAS_ID").desc_nulls_last())
alias = alias.withColumn("_rn", F.row_number().over(w_a)).where("_rn = 1").select("ORGANIZATION_ID", "PRACTICE_ODS_CODE")

org = (spark.table("4_prod.raw.mill_organization")
       .withColumn("_rn", F.row_number().over(Window.partitionBy("ORGANIZATION_ID").orderBy(F.col("ADC_UPDT").desc_nulls_last(), F.col("UPDT_CNT").desc_nulls_last())))
       .where("_rn = 1").select(F.col("ORGANIZATION_ID").cast("bigint").alias("ORGANIZATION_ID"), F.col("ORG_NAME").alias("_org_name")))

df = (reltn.withColumn("ORGANIZATION_ID", F.col("ORGANIZATION_ID").cast("bigint"))
      .join(alias, "ORGANIZATION_ID", "left").join(org, "ORGANIZATION_ID", "left")
      .select(
          F.col("PERSON_ORG_RELTN_ID").cast("bigint").alias("PERSON_ORG_RELTN_ID"),
          F.col("PERSON_ID").cast("bigint").alias("PERSON_ID"),
          F.when(F.col("ORGANIZATION_ID") > 0, F.col("ORGANIZATION_ID")).alias("ORGANIZATION_ID"),
          "PRACTICE_ODS_CODE",
          F.coalesce(F.nullif(F.trim("_org_name"), F.lit("")), F.nullif(F.trim("FT_ORG_NAME"), F.lit(""))).alias("PRACTICE_NAME"),
          F.col("FREE_TEXT_IND").cast("int").alias("FREE_TEXT_IND"),
          F.col("PRIORITY_SEQ").cast("int").alias("PRIORITY_SEQ"),
          F.col("ACTIVE_IND").cast("int").alias("ACTIVE_IND"),
          F.col("BEG_EFFECTIVE_DT_TM").alias("BEG_EFFECTIVE_DT_TM"),
          F.when(F.col("END_EFFECTIVE_DT_TM") < FAR_FUTURE, F.col("END_EFFECTIVE_DT_TM")).alias("END_EFFECTIVE_DT_TM"),
          "Trust", "ADC_UPDT"))
# CURRENT_IND: the latest-starting active, open registration per person.
# Open now = active, started, and not yet ended. A registration dated to start in the future (a pending
# practice transfer) is not current: the person stays with the practice they are registered at today.
# A NULL start is treated as started (no evidence it is pending); it ranks after dated starts below.
now = F.current_timestamp()
started = F.col("BEG_EFFECTIVE_DT_TM").isNull() | (F.col("BEG_EFFECTIVE_DT_TM") <= now)
open_now = (F.col("ACTIVE_IND") == 1) & started & (F.col("END_EFFECTIVE_DT_TM").isNull() | (F.col("END_EFFECTIVE_DT_TM") > now))
w_c = Window.partitionBy("PERSON_ID").orderBy(open_now.desc(), F.col("PRACTICE_ODS_CODE").isNotNull().desc(),
                                              F.col("BEG_EFFECTIVE_DT_TM").desc_nulls_last(), F.col("PERSON_ORG_RELTN_ID").desc_nulls_last())
df = df.withColumn("CURRENT_IND", open_now & (F.row_number().over(w_c) == 1)).withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
assert df.where("PERSON_ORG_RELTN_ID IS NULL").limit(1).count() == 0, "null key"

# COMMAND ----------

(df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET))
spark.sql(f"ALTER TABLE {TARGET} SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.enableRowTracking'='true','delta.enableDeletionVectors'='true')")
spark.sql(f"COMMENT ON TABLE {TARGET} IS 'Registered GP practice history. Grain: one Millennium PERSON_ORG_RELTN_ID with PERSON_ORG_RELTN_CD 4072315 (Registered Practice). PRACTICE_ODS_CODE from ORGANIZATION_ALIAS pool 6031508. CURRENT_IND marks the latest active open registration per person. Consumed by silver reference_person_gp_registration.'")
IG = {"PERSON_ORG_RELTN_ID": (0, 0), "PERSON_ID": (0, 1), "ORGANIZATION_ID": (1, 0), "PRACTICE_ODS_CODE": (2, 1),
      "PRACTICE_NAME": (2, 1), "FREE_TEXT_IND": (0, 0), "PRIORITY_SEQ": (0, 0), "ACTIVE_IND": (0, 0),
      "BEG_EFFECTIVE_DT_TM": (1, 0), "END_EFFECTIVE_DT_TM": (1, 0), "Trust": (0, 0), "ADC_UPDT": (0, 0),
      "CURRENT_IND": (0, 0), "PIPELINE_UPDT_DT_TM": (0, 0)}
assert set(IG) == set(df.columns), f"untagged columns: {set(df.columns) ^ set(IG)}"
for c, (risk, sev) in IG.items():
    spark.sql(f"ALTER TABLE {TARGET} ALTER COLUMN `{c}` SET TAGS ('ig_risk'='{risk}','ig_severity'='{sev}')")
n = spark.table(TARGET).count()
dup = spark.sql(f"SELECT count(*) - count(DISTINCT PERSON_ORG_RELTN_ID) d FROM {TARGET}").first().d
assert dup == 0, f"duplicate PERSON_ORG_RELTN_ID {dup}"
dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET, "rows": n}))

