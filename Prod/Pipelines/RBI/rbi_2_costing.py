# Databricks notebook source
# MAGIC %md
# MAGIC # RBI 2: costing
# MAGIC Duplicate-weighted PLICS lines, activity links, entity-grain unit-cost calibration and backtest,
# MAGIC modelled pricing, the allocation ledger (line-level PLICS and NCC episode totals), then one
# MAGIC authoritative £ per row. The ledger and the activity links are published so validation can
# MAGIC reconcile every £ to its PLICS activity and component.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

import rbi_costing as K
from rbi_linkage import combine_plics_links, fin_to_encounter, plics_fin_links, plics_op_candidates
from rbi_schema import restrict_to_sample, try_bigint

GOLD = spark.conf.get("rbi.gold_schema", "4_prod.gold")
CONFIG = spark.conf.get("rbi.config_schema")
SAMPLE_MOD = int(spark.conf.get("rbi.sample_mod", "0"))
BACKTEST_PAIRS = spark.conf.get("rbi.backtest_pairs", "")          # e.g. "2020/21>2021/22" (Task 3)
MIN_N = int(spark.conf.get("rbi.min_calibration_activities", "100"))


def gold(name):
    """Read a gold table."""
    return spark.read.table(f"{GOLD}.{name}")


def sampled(df):
    """Restrict to the dev cohort when sampling is on."""
    if SAMPLE_MOD == 0:
        return df
    return restrict_to_sample(df.withColumn("person_id", try_bigint("person_id")), spark.read.table("rbi_sample_person"))

# COMMAND ----------

@dp.materialized_view(name="_costed_activity", private=True)
def _costed_activity():
    return sampled(gold("clinical_costed_activity"))


@dp.materialized_view(name="_plics_weighted_line", private=True)
def _plics_weighted_line():
    # Cost lines are selected through their parent activities, so the dev sample stays consistent.
    acts = spark.read.table("_costed_activity")
    lines = gold("reference_cost_line_item").join(acts.select("extract_cd", "activity_record_id"),
                                                  ["extract_cd", "activity_record_id"], "left_semi")
    return K.plics_weighted_lines(lines, acts)


@dp.materialized_view(name="_fin_encounter", private=True)
def _fin_encounter():
    return fin_to_encounter(gold("spine_encounter_identifier"))


# Activity linkage in three shallow stages (each refreshes incrementally).
@dp.materialized_view(name="_plics_fin_link", private=True)
def _plics_fin_link():
    return plics_fin_links(spark.read.table("_costed_activity"), spark.read.table("_fin_encounter"))


@dp.materialized_view(name="_plics_op_candidate", private=True)
def _plics_op_candidate():
    return plics_op_candidates(spark.read.table("_costed_activity"), spark.read.table("_attr_outpatient_appointment"))


@dp.materialized_view(name="plics_activity_link", comment="See governance comments.")
def plics_activity_link():
    return combine_plics_links(spark.read.table("_plics_fin_link"), spark.read.table("_plics_op_candidate"))


@dp.materialized_view(name="_hcd_cover", private=True)
def _hcd_cover():
    return K.hcd_ledger_cover(spark.read.table("_events_attributed"))


@dp.materialized_view(name="_uplift", private=True)
def _uplift():
    return spark.read.table(f"{CONFIG}.cost_uplift_index")

# COMMAND ----------

@dp.materialized_view(name="unit_cost_reference", comment="See governance comments.")
def unit_cost_reference():
    return K.calibrate_unit_costs(spark.read.table("_plics_weighted_line"), spark.read.table("plics_activity_link"),
                                  spark.read.table("_events_attributed"), spark.read.table("_hcd_cover"))


@dp.materialized_view(name="_overhead_ratio", private=True)
def _overhead_ratio():
    return K.overhead_ratios(spark.read.table("_plics_weighted_line"), spark.read.table("plics_activity_link"))


@dp.materialized_view(name="unit_cost_backtest", comment="See governance comments.")
def unit_cost_backtest():
    # Base>test FY pairs come from config (set in Task 3 from the PLICS FY profile); no Spark actions at definition time.
    wl = spark.read.table("_plics_weighted_line")
    pairs = [p.split(">") for p in BACKTEST_PAIRS.split(",") if p.strip()]
    parts = [K.backtest(wl, spark.read.table("plics_activity_link"), spark.read.table("_events_attributed"),
                        spark.read.table("_uplift"), spark.read.table("_hcd_cover"), base.strip(), test.strip(), min_n=MIN_N)
             for base, test in pairs]
    if not parts:
        return spark.createDataFrame([], "stream string, n_entities bigint, mape double, total_actual double, total_modelled double, "
                                         "base_fy string, test_fy string, cost_method_version string")
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out

# COMMAND ----------

# Pricing: resolve each distinct price key once (small), then one lookup join per row.
@dp.materialized_view(name="_price_key", private=True)
def _price_key():
    space = K.price_key_space(spark.read.table("_events_attributed"), spark.read.table("_hcd_cover"))
    return K.price_keys(spark.read.table("unit_cost_reference"), spark.read.table("_uplift"), space, min_n=MIN_N)


@dp.materialized_view(name="_modelled_price", private=True)
def _modelled_price():
    return K.price_from_keys(spark.read.table("_events_attributed"), spark.read.table("_price_key"),
                             spark.read.table("_hcd_cover"))


@dp.materialized_view(name="cost_allocation_ledger", comment="See governance comments.")
def cost_allocation_ledger():
    plics = K.allocate_plics_actuals(spark.read.table("_plics_weighted_line"), spark.read.table("plics_activity_link"),
                                     spark.read.table("_events_attributed"), spark.read.table("_hcd_cover"))
    ncc = K.allocate_episode_totals(spark.read.table("_costed_activity"), spark.read.table("plics_activity_link"),
                                    spark.read.table("_events_attributed"), spark.read.table("_modelled_price"))
    return plics.unionByName(ncc)


@dp.materialized_view(name="_events_costed", private=True)
def _events_costed():
    return K.apply_costs(spark.read.table("_events_attributed"), spark.read.table("_modelled_price"),
                         K.ledger_event_allocations(spark.read.table("cost_allocation_ledger")),
                         spark.read.table("_overhead_ratio"), spark.read.table("_hcd_cover"))
