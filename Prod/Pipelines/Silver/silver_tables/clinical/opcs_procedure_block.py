"""Silver clinical.opcs_procedure_block: 1 dataset(s).

- datasets: _clinical_opcs_procedure_block
- moved verbatim from: 2_silver_clinical_events:703, 2_silver_clinical_events:715, 2_silver_clinical_events:742, 2_silver_clinical_events:808

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from pyspark.sql.window import Window
import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source
from silver_tables._common.journey import SRC_PROCEDURE
from silver_tables.clinical._shared import _opcs_laterality_display


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical coders record one operation as an ordered run of OPCS-4 codes in a coding episode (same encounter, same
# PROC_DT_TM): the operation, then its site code(s) (OMOP domain 'Spec Anatomic Site'), method codes (Y chapter)
# and its side (Z94.x). Example: W19.2 nail fixation, Z77.2 shaft of tibia, Z94.3 left. map_procedure holds one row
# per code, so the side and site never reached the operation row. Rows are ordered by PROC_PRIORITY then
# PROCEDURE_ID (pre-2018 rows tie on priority and the ids keep entry order); each non-operation code joins the
# latest operation before it. Qualifiers that come before every operation in their episode stay unattached, and
# a site or side coded after a harvest (Y54-Y69) code describes the donor site, not the operation.
OPCS_BLOCK_RULE = "opcs_qualifier_block_v1"


def _opcs_vocabulary():
    """One row per OPCS-4 code: its OMOP domain, whether it is a harvest (donor-site) method code, and for site codes
    the SNOMED body structure it 'Maps to'.
    A valid concept beats a retired one and a mapped concept beats an unmapped one; then the lowest site id."""
    concept = read_source("3_lookup.omop.concept")
    opcs = concept.where(F.col("vocabulary_id") == "OPCS4").select(
        F.col("concept_code").alias("opcs_code"),
        F.col("concept_id").cast("bigint").alias("_opcs_concept_id"),
        F.col("domain_id").alias("opcs_domain"),
        F.lower(F.col("concept_name")).startswith("harvest of").alias("opcs_is_harvest"),
        F.col("invalid_reason").isNotNull().alias("_retired"))
    maps_to = read_source("3_lookup.omop.concept_relationship").where(
        (F.col("relationship_id") == "Maps to") & F.col("invalid_reason").isNull()).select(
        F.col("concept_id_1").cast("bigint").alias("_opcs_concept_id"),
        F.col("concept_id_2").cast("bigint").alias("site_concept_id"))
    snomed = concept.where(F.col("vocabulary_id") == "SNOMED").select(
        F.col("concept_id").cast("bigint").alias("site_concept_id"),
        F.col("concept_code").alias("site_snomed_code"),
        F.col("concept_name").alias("site_snomed_display"))
    mapped = opcs.join(maps_to, "_opcs_concept_id", "left").join(snomed, "site_concept_id", "left")
    best = mapped.groupBy("opcs_code").agg(F.min(F.struct(
        "_retired", F.col("site_snomed_code").isNull().alias("_unmapped"), "site_concept_id",
        "opcs_domain", "opcs_is_harvest", "site_snomed_code", "site_snomed_display")).alias("_best"))
    return best.select("opcs_code", "_best.opcs_domain", "_best.opcs_is_harvest", "_best.site_concept_id",
                       "_best.site_snomed_code", "_best.site_snomed_display")


def _opcs_procedure_block_query():
    """One row per OPCS-4 map_procedure row: its role, the operation it belongs to, and that operation's site and side."""
    code = F.upper(F.trim(F.col("SOURCE_IDENTIFIER")))
    rows = (read_source(SRC_PROCEDURE)
            .where(F.col("source_vocabulary_desc") == "OPCS4")
            .select(F.col("PROCEDURE_ID").cast("bigint").alias("procedure_id"),
                    F.col("ENCNTR_ID").cast("bigint").alias("encounter_id"),
                    "PROC_DT_TM", F.to_date("PROC_DT_TM_LOCAL").alias("block_local_date"),
                    "PROC_PRIORITY", code.alias("opcs_code"))
            .join(F.broadcast(_opcs_vocabulary()), "opcs_code", "left")
            .withColumn("opcs_code_role",
                        F.when(F.col("opcs_code").startswith("Z94"), F.lit("laterality"))
                        .when(F.col("opcs_domain") == "Spec Anatomic Site", F.lit("site"))
                        .when(F.col("opcs_code").startswith("Y"), F.lit("method"))
                        .otherwise(F.lit("operation"))))
    # Running count of operations in coding order = the block a row belongs to (0 = before any operation).
    coding_order = (Window.partitionBy("encounter_id", "PROC_DT_TM")
                    .orderBy(F.col("PROC_PRIORITY").asc_nulls_last(), F.col("procedure_id").asc())
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    is_operation = F.col("opcs_code_role") == "operation"
    rows = rows.withColumn("_block_no", F.sum(F.when(is_operation, 1).otherwise(0)).over(coding_order))
    block_key = ["encounter_id", "PROC_DT_TM", "_block_no"]
    # A harvest method code (Y54-Y69 "Harvest of ...") carries the donor site's own site and side (Y56.2 harvest of a
    # flap from the back, then Z94.2): qualifiers after the block's first harvest code describe the donor site, so only
    # earlier ones describe the operation. Other Y codes (approach, image guidance) do not split the block.
    block_order = (Window.partitionBy(*block_key)
                   .orderBy(F.col("PROC_PRIORITY").asc_nulls_last(), F.col("procedure_id").asc())
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    is_harvest = (F.col("opcs_code_role") == "method") & F.coalesce(F.col("opcs_is_harvest"), F.lit(False))
    rows = rows.withColumn("_after_method", F.sum(F.when(is_harvest, 1).otherwise(0)).over(block_order) > 0)
    is_site = (F.col("opcs_code_role") == "site") & ~F.col("_after_method")
    is_side = (F.col("opcs_code_role") == "laterality") & ~F.col("_after_method")
    coding_position = F.coalesce(F.col("PROC_PRIORITY").cast("bigint"), F.lit(9223372036854775807))
    blocks = rows.where(F.col("_block_no") > 0).groupBy(*block_key).agg(
        F.max(F.when(is_operation, F.col("procedure_id"))).alias("anchor_procedure_id"),
        F.max(F.when(is_operation, F.col("opcs_code"))).alias("anchor_opcs_code"),
        # The first site in coding order names the body site; all mapped sites feed the indication match.
        F.min(F.when(is_site, F.struct(coding_position.alias("_position"), "procedure_id", "site_concept_id",
                                       "site_snomed_code", "site_snomed_display"))).alias("_first_site"),
        F.countDistinct(F.when(is_site, F.col("opcs_code"))).alias("_site_count"),
        F.array_sort(F.collect_set(F.when(is_site, F.col("site_concept_id")))).alias("site_concept_ids"),
        F.array_sort(F.collect_set(F.when(is_side, F.col("opcs_code")))).alias("_sides"))
    one_side = F.size("_sides") == 1
    blocks = blocks.select(
        *block_key, "anchor_procedure_id", "anchor_opcs_code", "site_concept_ids",
        F.col("_first_site.site_concept_id").alias("site_concept_id"),
        F.col("_first_site.site_snomed_code").alias("body_site_code"),
        F.col("_first_site.site_snomed_display").alias("body_site_display"),
        F.when(F.col("_site_count") == 0, F.lit(None).cast("string"))
         .when(F.col("_first_site.site_snomed_code").isNull(), F.lit(OPCS_BLOCK_RULE + "_unmapped"))
         .when(F.col("_site_count") > 1, F.lit(OPCS_BLOCK_RULE + "_multiple"))
         .otherwise(F.lit(OPCS_BLOCK_RULE)).alias("body_site_method"),
        F.when(one_side, F.col("_sides")[0]).alias("laterality_code"),
        F.when(one_side, _opcs_laterality_display(F.col("_sides")[0])).alias("laterality_display"),
        F.when(one_side, F.lit(OPCS_BLOCK_RULE))
         .when(F.size("_sides") > 1, F.lit(OPCS_BLOCK_RULE + "_conflict")).alias("laterality_method"))
    # The per-row vocabulary columns gave way to the block's values (the first site of the operation).
    rows = rows.drop("opcs_domain", "opcs_is_harvest", "site_concept_id", "site_snomed_code", "site_snomed_display",
                     "_after_method")
    return rows.join(blocks, block_key, "left").select(
        "procedure_id", "encounter_id", "block_local_date", "opcs_code", "opcs_code_role",
        "anchor_procedure_id", "anchor_opcs_code", "site_concept_id", "site_concept_ids",
        "body_site_code", "body_site_display", "body_site_method",
        "laterality_code", "laterality_display", "laterality_method")


@materialized_view(
    name=_n("journey_clinical._opcs_procedure_block"),
    private=True,
    refresh_policy="auto",
)
def _opcs_procedure_block():
    # Build the declared dataset: every OPCS-4 row with the operation it qualifies.
    return _opcs_procedure_block_query()


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_opcs_procedure_block': {'columns': {},
                                    'table': 'Internal OPCS-4 coded-operation runs '
                                             '(OPCS_QUALIFIER_BLOCK_V1): every OPCS map_procedure row with '
                                             "its role and its operation's site and side."}}
