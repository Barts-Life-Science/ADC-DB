# Databricks notebook source
# Weekly, idempotent bed-state observation run inside Bronze_Pipeline_Parallel.
# The source is small (~27k rows), so dev reads the complete snapshot.

# COMMAND ----------

from datetime import datetime, timezone
import json
from delta.tables import DeltaTable
from pyspark.sql import functions as F

for _name,_default in {"target_schema":"8_dev.tdx_bronze","require_today_landing":"true","allow_production_write":"false","observation_date":"","pipeline_run_id":""}.items():
    try: dbutils.widgets.get(_name)
    except Exception: dbutils.widgets.text(_name,_default)
TARGET_SCHEMA=dbutils.widgets.get("target_schema")
REQUIRE_TODAY=dbutils.widgets.get("require_today_landing").lower()=="true"
ALLOW_PRODUCTION_WRITE=dbutils.widgets.get("allow_production_write").lower()=="true"
OBS_DATE=dbutils.widgets.get("observation_date")
RUN_ID=dbutils.widgets.get("pipeline_run_id") or f"tdx-bed-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
assert not TARGET_SCHEMA.lower().startswith("4_prod") or ALLOW_PRODUCTION_WRITE
SRC="4_prod.raw.mill_bed"; LOC=f"{TARGET_SCHEMA}.map_location_unit"; TARGET=f"{TARGET_SCHEMA}.map_bed_status_observation"; RUNS=f"{TARGET_SCHEMA}.map_bed_observation_run"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {'.'.join(f'`{p}`' for p in TARGET_SCHEMA.split('.'))}")
obs_date_expr=F.to_date(F.lit(OBS_DATE)) if OBS_DATE else F.current_date()
obs_date=spark.range(1).select(obs_date_expr.alias("d")).first()["d"]
watermark=spark.table("6_mgmt.incr_updt_v2.watermark").where("watermark_id=101696 AND active_ind=1").select("watermark_timestamp").first()
landing_ok=bool(watermark and watermark["watermark_timestamp"] and watermark["watermark_timestamp"].date()>=obs_date)

def qname(n): return ".".join(f"`{p}`" for p in n.split("."))
def merge_run(status,reason,version=None,beds=None,active=None):
    row=spark.createDataFrame([(obs_date,status,reason,version,beds,active,datetime.now(timezone.utc).replace(tzinfo=None),RUN_ID)],"OBSERVATION_DATE date, STATUS string, REASON string, SOURCE_DELTA_VERSION long, BEDS_OBSERVED long, BEDS_ACTIVE long, OBSERVED_AT timestamp, PIPELINE_RUN_ID string")
    if not spark.catalog.tableExists(RUNS):
      # Enable row tracking for downstream Silver incremental refreshes.
      (
          row.write.format("delta")
          .option("delta.enableRowTracking", "true")
          .mode("overwrite")
          .saveAsTable(RUNS)
      )
    else:
        (DeltaTable.forName(spark,RUNS).alias("t").merge(row.alias("s"),"t.OBSERVATION_DATE=s.OBSERVATION_DATE")
         .whenMatchedUpdate(set={"STATUS":"s.STATUS","REASON":"s.REASON","SOURCE_DELTA_VERSION":"s.SOURCE_DELTA_VERSION","BEDS_OBSERVED":"s.BEDS_OBSERVED","BEDS_ACTIVE":"s.BEDS_ACTIVE","PIPELINE_RUN_ID":"s.PIPELINE_RUN_ID"})
         .whenNotMatchedInsertAll().execute())
    spark.sql(f"COMMENT ON TABLE {qname(RUNS)} IS 'One row per scheduled Bronze TDX bed-observation attempt; skipped executions are explicit and no state is fabricated.'")

if REQUIRE_TODAY and not landing_ok:
    merge_run("skipped","no successful current-date mill_bed landing")
    dbutils.notebook.exit(json.dumps({"status":"skipped","observation_date":str(obs_date)}))
assert spark.catalog.tableExists(LOC), "map_location_unit must be built first"
version=int(spark.sql(f"DESCRIBE HISTORY {SRC} LIMIT 1").first()["version"])
bed=spark.read.option("versionAsOf",version).table(SRC).alias("b")
unit=spark.table(LOC).where("LOCATION_LEVEL='bed' AND SOURCE_PRESENT_IND").select(F.col("LOCATION_CD").alias("_loc"),"PARENT_LOCATION_CD","NURSE_UNIT_CD","NURSE_UNIT_DISPLAY")
lookup=spark.table("3_lookup.mill.mill_code_value").where("CODE_SET=291").select(F.col("CODE_VALUE").cast("long").alias("_status"),F.col("DISPLAY").alias("BED_STATUS_DISPLAY")).dropDuplicates(["_status"])
obs=(bed.join(F.broadcast(unit),F.col("b.LOCATION_CD").cast("long")==F.col("_loc"),"left")
    .join(F.broadcast(lookup),F.col("b.BED_STATUS_CD").cast("long")==F.col("_status"),"left")
    .select(F.col("b.LOCATION_CD").cast("long").alias("LOCATION_CD"),F.lit(obs_date).cast("date").alias("OBSERVATION_DATE"),F.current_timestamp().alias("OBSERVED_AT"),F.lit(version).cast("long").alias("SOURCE_DELTA_VERSION"),
        F.col("b.BED_STATUS_CD").cast("long").alias("BED_STATUS_CD"),"BED_STATUS_DISPLAY",F.col("b.LOC_ROOM_CD").cast("long").alias("LOC_ROOM_CD"),F.col("PARENT_LOCATION_CD").cast("long").alias("ROOM_LOCATION_CD"),
        F.col("NURSE_UNIT_CD").cast("long"),"NURSE_UNIT_DISPLAY",(F.col("b.ACTIVE_IND")==1).alias("BED_ACTIVE_IND"),(F.col("b.DUP_BED_IND")==1).alias("DUP_BED_IND"),
        F.col("b.UPDT_DT_TM").alias("BED_UPDT_DT_TM"),F.col("b.LAST_UTC_TS").alias("SOURCE_LAST_UTC_TS"),F.col("b.ADC_UPDT").alias("SOURCE_ADC_UPDT"),F.lit(SRC).alias("SOURCE_TABLE"),F.col("b.LOCATION_CD").cast("string").alias("SOURCE_ROW_ID"),
        F.lit(RUN_ID).alias("PIPELINE_RUN_ID"),F.lit(True).alias("SOURCE_PRESENT_IND"),F.lit(None).cast("timestamp").alias("SOURCE_ABSENT_DETECTED_TS")))
payload=sorted(c for c in obs.columns if c not in {"OBSERVED_AT","PIPELINE_RUN_ID","SOURCE_ABSENT_DETECTED_TS"})
obs=obs.withColumn("ROW_HASH",F.sha2(F.to_json(F.struct(*[F.col(c) for c in payload])),256)).withColumn("ADC_UPDT",F.current_timestamp())
assert obs.count()==obs.select("LOCATION_CD").distinct().count()
if not spark.catalog.tableExists(TARGET): obs.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","true").option("delta.enableRowTracking","true").saveAsTable(TARGET)
else:
    mutable=[c for c in obs.columns if c not in {"LOCATION_CD","OBSERVATION_DATE","OBSERVED_AT"}]
    (DeltaTable.forName(spark,TARGET).alias("t").merge(obs.alias("s"),"t.LOCATION_CD=s.LOCATION_CD AND t.OBSERVATION_DATE=s.OBSERVATION_DATE")
     .whenMatchedUpdate(condition="NOT (t.ROW_HASH <=> s.ROW_HASH)",set={c:f"s.`{c}`" for c in mutable})
     .whenNotMatchedInsertAll().execute())
spark.sql(f"ALTER TABLE {qname(TARGET)} SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.enableRowTracking'='true','delta.enableDeletionVectors'='true')")
spark.sql(f"COMMENT ON TABLE {qname(TARGET)} IS 'Weekly complete current-state bed observation captured by Bronze_Pipeline_Parallel. No within-week history is claimed.'")
for table in [TARGET,RUNS] if spark.catalog.tableExists(RUNS) else [TARGET]:
    for col in spark.table(table).columns:
        spark.sql(f"ALTER TABLE {qname(table)} ALTER COLUMN `{col}` COMMENT 'TDX bed-observation field {col}.'")
        risk,severity=(("4","2") if col in {"LOCATION_CD","SOURCE_ROW_ID"} else ("1","1"))
        spark.sql(f"ALTER TABLE {qname(table)} ALTER COLUMN `{col}` SET TAGS ('ig_risk'='{risk}','ig_severity'='{severity}')")
beds=obs.count(); active=obs.where("BED_ACTIVE_IND").count(); merge_run("observed",None,version,beds,active)
dbutils.notebook.exit(json.dumps({"status":"observed","observation_date":str(obs_date),"source_delta_version":version,"beds":beds,"active":active},sort_keys=True))
