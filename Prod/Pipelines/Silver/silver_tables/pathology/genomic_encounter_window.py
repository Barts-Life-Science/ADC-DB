"""Silver pathology.genomic_encounter_window: 1 dataset(s).

- datasets: _events_genomic_encounter_window
- moved verbatim from: 2_silver_clinical_events:2376, 2_silver_clinical_events:2379

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, spark
from silver_tables._common.journey import SRC_PATHOLOGY_GENETIC_TEST, _alias_resolved_accession


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


EXTERNAL_SITE_RULE = "source_site_code LIKE 'E%' OR source_site_code = 'XQA'"


@materialized_view(
    name=_n("journey_events._genomic_encounter_window"),
    refresh_policy="auto",
)
def _genomic_encounter_window():
    # One row per alias-resolved genomic accession that V1 leaves unlinked, with window_method set and
    # window_encounter_id when exactly one encounter qualifies.
    accessions = _alias_resolved_accession(read_source(SRC_PATHOLOGY_GENETIC_TEST)).select(
        F.col("_resolved_accession_id").alias("pathology_accession_id")
    ).distinct()
    identity = spark.read.table(_n("journey_events._pathology_accession_identity")).select(
        "pathology_accession_id", "canonical_person_id", "sample_dt_local", "request_dt_local", "source_site_code",
        "accession_encounter_id", "accession_encounter_conflict",
    )
    unlinked = (
        accessions.join(identity, "pathology_accession_id", "inner")
        .where(F.col("accession_encounter_id").isNull() & ~F.coalesce(F.col("accession_encounter_conflict"), F.lit(False)))
        .withColumn("_external", F.coalesce(F.expr(EXTERNAL_SITE_RULE), F.lit(False)))
        .withColumn("_sample_date", F.to_date(F.coalesce(F.col("sample_dt_local"), F.col("request_dt_local"))))
        .withColumn("_person_id", F.col("canonical_person_id").cast("bigint"))
    )
    encounters = spark.read.table(_n("journey_spine.encounter")).where(
        F.col("period_start_local").isNotNull() & (F.coalesce(F.col("record_status"), F.lit("active")) == "active")
    ).select(
        F.col("person_id").alias("_enc_person_id"),
        F.col("encounter_id").alias("_enc_id"),
        F.to_date("period_start_local").alias("_enc_start"),
        F.coalesce(F.to_date("period_end_local"), F.date_add(F.to_date("period_start_local"), 1)).alias("_enc_end"),
    )
    containing = (
        unlinked.where(~F.col("_external") & F.col("_person_id").isNotNull() & F.col("_sample_date").isNotNull())
        .join(encounters, F.col("_person_id") == F.col("_enc_person_id"), "inner")
        .where(F.col("_sample_date").between(F.col("_enc_start"), F.col("_enc_end")))
        .groupBy("pathology_accession_id")
        .agg(
            F.min("_enc_id").alias("_lo"), F.max("_enc_id").alias("_hi"),
            F.min(F.when(F.col("_enc_start") == F.col("_sample_date"), F.col("_enc_id"))).alias("_day_lo"),
            F.max(F.when(F.col("_enc_start") == F.col("_sample_date"), F.col("_enc_id"))).alias("_day_hi"),
        )
    )
    return (
        unlinked.select("pathology_accession_id", "_external").join(containing, "pathology_accession_id", "left")
        .select(
            "pathology_accession_id",
            F.when(F.col("_external"), F.lit(None).cast("bigint"))
             .when(F.col("_lo") == F.col("_hi"), F.col("_lo"))
             .when(F.col("_day_lo") == F.col("_day_hi"), F.col("_day_lo"))
             .cast("bigint").alias("window_encounter_id"),
            F.when(F.col("_external"), F.lit("none_external_referral"))
             .when(F.col("_lo") == F.col("_hi"), F.lit("encounter_window_single"))
             .when(F.col("_day_lo") == F.col("_day_hi"), F.lit("encounter_window_same_day"))
             .when(F.col("_lo").isNotNull(), F.lit("encounter_window_multiple"))
             .alias("window_method"),
        )
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_events_genomic_encounter_window': {'columns': {},
                                      'table': 'INTERNAL PATH_ENCOUNTER_V2 evidence: for genomic accessions '
                                               "without a Millennium encounter, the patient's encounter "
                                               'whose period contains the sample date (Barts sites only; '
                                               'external referral sites E*/XQA are marked '
                                               'none_external_referral).'}}
