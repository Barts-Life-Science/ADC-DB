"""Silver clinical.implant_opcs_link: 1 dataset(s).

- datasets: _clinical_implant_opcs_link
- moved verbatim from: 2_silver_clinical_events:820, 2_silver_clinical_events:823, 2_silver_clinical_events:826, 2_silver_clinical_events:872

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from pyspark.sql.window import Window
import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark
from silver_tables._common.journey import _implant_procedure_canonical
from silver_tables.clinical._shared import _opcs_laterality_display


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# ==== IMPLANT_OPCS_LINK_V1: implant rows take the side and site of the operation that placed them ====
IMPLANT_LINK_RULE = "implant_opcs_link_v1"


# Imaging (U) and miscellaneous (X: drugs, oxygen ...) operations never place an implant; their site and side codes
# describe a scan or an infusion, so they are never candidates.
IMPLANT_LINK_EXCLUDED_CHAPTERS = ["U", "X"]


def _implant_opcs_link_query():
    """One row per implant with candidate OPCS operations in its encounter on the implant's London date (else the day
    before or after). The link names the operation when there is exactly one candidate. Side and site are published
    when every candidate that has one agrees and none is itself in conflict; otherwise NULL with a _conflict method."""
    operations = (spark.read.table(_n("journey_clinical._opcs_procedure_block"))
                  .where((F.col("opcs_code_role") == "operation")
                         & ~F.substring("opcs_code", 1, 1).isin(*IMPLANT_LINK_EXCLUDED_CHAPTERS))
                  .select("encounter_id", "block_local_date", F.col("procedure_id").alias("candidate_procedure_id"),
                          "site_concept_id", "body_site_code", "body_site_display", "body_site_method",
                          "laterality_code", "laterality_method"))
    implants = _implant_procedure_canonical().select(
        "patient_event_key", "encounter_id", F.to_date("event_datetime_local").alias("_implant_date"))
    # Equi-join on encounter, keep candidates within a day, and keep only the nearest day per implant.
    pairs = (implants.join(operations, "encounter_id")
             .withColumn("_day_offset", F.abs(F.datediff("block_local_date", "_implant_date")))
             .where(F.col("_day_offset") <= 1))
    pairs = (pairs.withColumn("_nearest", F.min("_day_offset").over(Window.partitionBy("patient_event_key")))
             .where(F.col("_day_offset") == F.col("_nearest")).drop("_nearest"))
    has_site = F.col("body_site_code").isNotNull()
    grouped = pairs.groupBy("patient_event_key").agg(
        F.min("_day_offset").alias("_day_offset"),
        F.countDistinct("candidate_procedure_id").alias("_candidates"),
        F.max("candidate_procedure_id").alias("_any_candidate"),
        F.array_sort(F.collect_set("laterality_code")).alias("_sides"),
        F.max(F.coalesce(F.col("laterality_method").endswith("_conflict"), F.lit(False))).alias("_side_conflict"),
        F.array_sort(F.collect_set(F.when(has_site, F.struct("body_site_code", "body_site_display",
                                                             "site_concept_id")))).alias("_sites"))
    method = F.concat(F.lit(IMPLANT_LINK_RULE),
                      F.when(F.col("_day_offset") == 0, F.lit("_same_day")).otherwise(F.lit("_adjacent_day")))
    one_side = (F.size("_sides") == 1) & ~F.col("_side_conflict")
    one_site = F.size("_sites") == 1
    return grouped.select(
        F.col("patient_event_key").alias("_link_event_key"),
        F.col("_candidates").alias("_link_candidate_count"),
        F.when(F.col("_candidates") == 1, F.col("_any_candidate")).alias("_link_procedure_id"),
        F.when(one_site, F.col("_sites")[0]["body_site_code"]).alias("_link_body_site_code"),
        F.when(one_site, F.col("_sites")[0]["body_site_display"]).alias("_link_body_site_display"),
        F.when(one_site, method).when(F.size("_sites") > 1, F.concat(method, F.lit("_conflict")))
         .alias("_link_body_site_method"),
        F.when(one_side, F.col("_sides")[0]).alias("_link_laterality_code"),
        F.when(one_side, _opcs_laterality_display(F.col("_sides")[0])).alias("_link_laterality_display"),
        F.when(one_side, method)
         .when((F.size("_sides") > 1) | F.col("_side_conflict"), F.concat(method, F.lit("_conflict")))
         .alias("_link_laterality_method"))


@materialized_view(
    name=_n("journey_clinical._implant_opcs_link"),
    private=True,
    refresh_policy="auto",
)
def _implant_opcs_link():
    # Build the declared dataset: each implant's candidate operations, side and site.
    return _implant_opcs_link_query()


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_implant_opcs_link': {'columns': {},
                                 'table': 'Internal implant to OPCS-4 operation link (IMPLANT_OPCS_LINK_V1): '
                                          "same-encounter operations on the implant's London date, else the "
                                          'adjacent day.'}}
