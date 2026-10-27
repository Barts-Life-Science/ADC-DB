# Databricks notebook source
# MAGIC %md
# MAGIC # RBI 3: published tables
# MAGIC The costed fact (stream rows + unallocated £ rows from the ledger, with spell and journey keys) and its summaries.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

import rbi_costing as K
import rbi_rollups as U
from rbi_schema import restrict_to_sample, try_bigint

GOLD = spark.conf.get("rbi.gold_schema", "4_prod.gold")
SAMPLE_MOD = int(spark.conf.get("rbi.sample_mod", "0"))
WINDOW = spark.conf.get("rbi.window_start", "2019-04-01")  # RBI_REL_20261025: bounds the OP arm of costed_activity_unit


def gold(name):
    """Read a gold table."""
    return spark.read.table(f"{GOLD}.{name}")


def sampled(df):
    """Restrict to the dev cohort when sampling is on."""
    if SAMPLE_MOD == 0:
        return df
    return restrict_to_sample(df.withColumn("person_id", try_bigint("person_id")), spark.read.table("rbi_sample_person"))

# COMMAND ----------

@dp.materialized_view(name="resource_use_event", comment="See governance comments.", cluster_by=["person_id", "event_date_local"])
def resource_use_event():
    ua = U.unallocated_as_rows(K.ledger_unallocated(spark.read.table("cost_allocation_ledger")),
                               spark.read.table("_costed_activity"), spark.read.table("plics_activity_link"))
    return U.resource_use_event(spark.read.table("_events_costed"), ua, gold("spine_encounter"), gold("spine_journey_link"))


@dp.materialized_view(name="spell_resource_summary", comment="See governance comments.")
def spell_resource_summary():
    return U.spell_resource_summary(spark.read.table("resource_use_event"))


@dp.materialized_view(name="journey_resource_summary", comment="See governance comments.")
def journey_resource_summary():
    return U.journey_resource_summary(spark.read.table("resource_use_event"))


@dp.materialized_view(name="person_month_resource", comment="See governance comments.")
def person_month_resource():
    return U.person_month_resource(spark.read.table("resource_use_event"))


@dp.materialized_view(name="service_day_activity", comment="See governance comments.")
def service_day_activity():
    return U.service_day_activity(spark.read.table("resource_use_event"))


@dp.materialized_view(name="costed_activity_unit", comment="See governance comments.")
def costed_activity_unit():
    return U.costed_activity_unit(sampled(gold("clinical_hrg_grouping")), spark.read.table("_fin_encounter"),
                                  spark.read.table("resource_use_event"), WINDOW)


@dp.materialized_view(name="attribution_summary", comment="See governance comments.")
def attribution_summary():
    return U.attribution_summary(spark.read.table("resource_use_event"))
