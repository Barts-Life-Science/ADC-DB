"""Silver clinical.procedure_indication: 1 dataset(s).

- datasets: clinical_procedure_indication
- moved verbatim from: 2_silver_clinical_events:1413, 2_silver_clinical_events:1414, 2_silver_clinical_events:1416, 2_silver_clinical_events:1450, 2_silver_clinical_events:1458, 2_silver_clinical_events:1473, 2_silver_clinical_events:1496, 2_silver_clinical_events:1554

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from pyspark.sql.window import Window
import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, spark, stable_id
from silver_tables._common.journey import SRC_DIAGNOSIS


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# ==== journey_clinical.procedure_indication (PROCEDURE_INDICATION_V1) ====
#
# Millennium never records which diagnosis an operation or implant treated. This table proposes the link by anatomy:
# a same-encounter condition whose SNOMED finding site ('Has finding site' of the condition's standard concept) is
# the operation's coded body site, or within two is-a levels of it in either direction. Example: W19.2 + Z77.2
# (shaft of tibia) matches S82.2 fracture of shaft of tibia exactly and never S82.0 fracture of patella.
# Every candidate is published with its rule and anatomical distance; indication_rank 1 is the closest anatomy, then
# the lowest diagnosis priority. Nothing is curated by hand.
PROCEDURE_INDICATION_RULE = "procedure_site_finding_site_v1"


PROCEDURE_INDICATION_MAX_LEVELS = 2


PROCEDURE_INDICATION_PUBLIC_COLUMNS = [
    "procedure_indication_key",  # SILVER_RELEASE_20261025 item 3
    "procedure_patient_event_key", "procedure_source_object", "procedure_id", "implant_event_id", "implant_sequence",
    "condition_patient_event_key", "diagnosis_id", "problem_id", "person_id", "encounter_id",
    "procedure_site_code", "procedure_site_display", "condition_site_code", "condition_site_display",
    "match_type", "match_levels", "diagnosis_priority", "indication_rank", "rule_id", "rule_version", "loaded_at",
]


def _snomed_names(alias):
    """SNOMED concept id -> code and name, with columns prefixed for one side of the match."""
    return read_source("3_lookup.omop.concept").select(
        F.col("concept_id").cast("bigint").alias(f"{alias}_id"),
        F.col("concept_code").alias(f"{alias}_code"),
        F.col("concept_name").alias(f"{alias}_display"))


def _procedure_site_rows():
    """Operations and singly-linked implants, one row per mapped site of their coded operation."""
    procedures = (spark.read.table(_n("journey_clinical._procedure_primitive"))
                  .where((F.col("opcs_code_role") == "operation")
                         | ((F.col("source_object") == "implant") & F.col("opcs_anchor_procedure_id").isNotNull()))
                  .select("patient_event_key", "source_object", "procedure_id", "implant_event_id", "implant_sequence",
                          "person_id", "encounter_id", "opcs_anchor_procedure_id", "loaded_at"))
    sites = (spark.read.table(_n("journey_clinical._opcs_procedure_block"))
             .where(F.col("opcs_code_role") == "operation")
             .select(F.col("procedure_id").alias("_site_anchor_id"),
                     F.explode("site_concept_ids").alias("procedure_site_id")))
    return (procedures.join(sites, F.col("opcs_anchor_procedure_id") == F.col("_site_anchor_id"))
            .drop("_site_anchor_id", "opcs_anchor_procedure_id"))


def _condition_site_rows():
    """Encounter conditions, one row per SNOMED finding site of their standard concept (a non-standard concept is
    first followed through 'Maps to')."""
    relationship = read_source("3_lookup.omop.concept_relationship").where(F.col("invalid_reason").isNull())
    maps_to = relationship.where(F.col("relationship_id") == "Maps to").select(
        F.col("concept_id_1").cast("bigint").alias("_cond_concept_id"),
        F.col("concept_id_2").cast("bigint").alias("_std_id"))
    finding_site = relationship.where(F.col("relationship_id") == "Has finding site").select(
        F.col("concept_id_1").cast("bigint").alias("_std_id"),
        F.col("concept_id_2").cast("bigint").alias("condition_site_id"))
    conditions = (spark.read.table(_n("journey_clinical.condition"))
                  .where(F.col("encounter_id").isNotNull() & F.col("condition_omop_concept_id").isNotNull())
                  .select(F.col("patient_event_key").alias("condition_patient_event_key"), "diagnosis_id", "problem_id",
                          F.col("encounter_id").alias("_cond_encounter_id"),
                          F.col("condition_omop_concept_id").cast("bigint").alias("_cond_concept_id"),
                          F.col("loaded_at").alias("_cond_loaded_at")))
    standard = (conditions.join(maps_to, "_cond_concept_id", "left")
                .withColumn("_std_id", F.coalesce(F.col("_std_id"), F.col("_cond_concept_id"))))
    return (standard.join(finding_site, "_std_id")
            .drop("_std_id", "_cond_concept_id")
            .dropDuplicates(["condition_patient_event_key", "condition_site_id"]))


def _procedure_indication_query():
    """Anatomy-matched (procedure, condition) candidates in the same encounter, ranked per procedure."""
    procedures = _procedure_site_rows()
    pairs = (procedures.join(_condition_site_rows(), F.col("encounter_id") == F.col("_cond_encounter_id"))
             .drop("_cond_encounter_id"))
    # concept_ancestor narrowed to the procedure sites in play: three equi-joins, never an OR-join.
    proc_sites = F.broadcast(procedures.select(F.col("procedure_site_id").alias("_site")).distinct())
    ancestors = (read_source("3_lookup.omop.concept_ancestor")
                 .where(F.col("min_levels_of_separation").between(1, PROCEDURE_INDICATION_MAX_LEVELS))
                 .select(F.col("ancestor_concept_id").cast("bigint").alias("_anc"),
                         F.col("descendant_concept_id").cast("bigint").alias("_des"),
                         F.col("min_levels_of_separation").cast("int").alias("_levels")))
    under_procedure = ancestors.join(proc_sites, F.col("_anc") == F.col("_site"), "left_semi")
    over_procedure = ancestors.join(proc_sites, F.col("_des") == F.col("_site"), "left_semi")
    exact = (pairs.where(F.col("procedure_site_id") == F.col("condition_site_id"))
             .withColumn("match_type", F.lit("exact")).withColumn("match_levels", F.lit(0)))
    narrower = (pairs.join(under_procedure, (F.col("_anc") == F.col("procedure_site_id"))
                           & (F.col("_des") == F.col("condition_site_id")))
                .withColumn("match_type", F.lit("condition_site_narrower"))
                .withColumn("match_levels", F.col("_levels")).drop("_anc", "_des", "_levels"))
    broader = (pairs.join(over_procedure, (F.col("_anc") == F.col("condition_site_id"))
                          & (F.col("_des") == F.col("procedure_site_id")))
               .withColumn("match_type", F.lit("condition_site_broader"))
               .withColumn("match_levels", F.col("_levels")).drop("_anc", "_des", "_levels"))
    matches = exact.unionByName(narrower).unionByName(broader)
    # Keep the closest site pair per (procedure, condition); deterministic tie-break on the site ids.
    pair_order = Window.partitionBy("patient_event_key", "condition_patient_event_key").orderBy(
        F.col("match_levels").asc(), F.col("match_type").asc(),
        F.col("procedure_site_id").asc(), F.col("condition_site_id").asc())
    matches = (matches.withColumn("_pair_rn", F.row_number().over(pair_order))
               .where(F.col("_pair_rn") == 1).drop("_pair_rn"))
    priority = read_source(SRC_DIAGNOSIS).select(
        F.col("DIAGNOSIS_ID").cast("bigint").alias("diagnosis_id"),
        F.col("DIAG_PRIORITY").cast("int").alias("diagnosis_priority"))
    matches = matches.join(priority, "diagnosis_id", "left")
    # DIAG_PRIORITY 1 is the coded primary; 0 is the unranked bucket (Discharge/Working, e.g. ED) and sorts last.
    ranked_priority = F.when(F.col("diagnosis_priority") > 0, F.col("diagnosis_priority"))
    rank_order = Window.partitionBy("patient_event_key").orderBy(
        F.col("match_levels").asc(), ranked_priority.asc_nulls_last(),
        F.col("condition_patient_event_key").asc())
    return (matches.withColumn("indication_rank", F.row_number().over(rank_order))
            .join(_snomed_names("procedure_site"), "procedure_site_id", "left")
            .join(_snomed_names("condition_site"), "condition_site_id", "left")
            .select(
                # SILVER_RELEASE_20261025 item 3: one row per (procedure, condition) pair by construction (the
                # pair_order row_number above), so this key is unique.
                stable_id("procedure_indication", F.col("patient_event_key"), F.col("condition_patient_event_key"))
                .alias("procedure_indication_key"),
                F.col("patient_event_key").alias("procedure_patient_event_key"),
                F.col("source_object").alias("procedure_source_object"),
                "procedure_id", "implant_event_id", "implant_sequence",
                "condition_patient_event_key", "diagnosis_id", "problem_id", "person_id", "encounter_id",
                "procedure_site_code", "procedure_site_display", "condition_site_code", "condition_site_display",
                "match_type", "match_levels", "diagnosis_priority", "indication_rank",
                F.lit(PROCEDURE_INDICATION_RULE).alias("rule_id"), F.lit("1").alias("rule_version"),
                F.greatest(F.col("loaded_at"), F.col("_cond_loaded_at")).alias("loaded_at")))


@materialized_view(
    name=_n("journey_clinical.procedure_indication"),
    cluster_by=["person_id"],
    # auto, not incremental: a row-based refresh of this join over the OMOP hierarchy took 6-8 min even for a few
    # changed rows in dev, against ~13 s for a full recompute; auto lets the cost model choose (dev proof 2026-10-16).
    refresh_policy="auto",
)
def procedure_indication():
    # Build the declared dataset: anatomy-matched procedure to condition candidates.
    return _procedure_indication_query().select(*PROCEDURE_INDICATION_PUBLIC_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_procedure_indication': {'columns': {'condition_patient_event_key': 'patient_event_key of the '
                                                                              'clinical_condition row '
                                                                              'proposed as the indication.',
                                               'condition_site_code': 'SNOMED finding site of the '
                                                                      "condition's standard concept ('Has "
                                                                      "finding site').",
                                               'condition_site_display': 'SNOMED name of '
                                                                         'condition_site_code.',
                                               'diagnosis_id': 'Millennium DIAGNOSIS_ID of the condition '
                                                               'when it is a diagnosis.',
                                               'diagnosis_priority': 'Millennium DIAG_PRIORITY of the '
                                                                     'diagnosis as recorded: 1 = coded '
                                                                     'primary, 2+ = secondary positions, 0 = '
                                                                     'not ranked (Discharge/Working '
                                                                     'diagnoses such as ED); NULL for '
                                                                     'problems.',
                                               'encounter_id': 'Millennium ENCNTR_ID shared by the procedure '
                                                               'and the condition.',
                                               'implant_event_id': 'Millennium implant EVENT_ID for implant '
                                                                   'rows.',
                                               'implant_sequence': 'Millennium IMPLANT_SEQUENCE for implant '
                                                                   'rows.',
                                               'indication_rank': '1 = best candidate for this procedure: '
                                                                  'fewest match_levels, then lowest positive '
                                                                  'diagnosis_priority (0 and NULL last), '
                                                                  'then condition_patient_event_key.',
                                               'loaded_at': 'Greatest load timestamp of the procedure and '
                                                            'condition rows.',
                                               'match_levels': 'is-a levels between the two sites (0 = '
                                                               'exact; at most 2).',
                                               'match_type': 'exact (same structure), '
                                                             'condition_site_narrower (the condition site is '
                                                             'a descendant of the procedure site) or '
                                                             'condition_site_broader (an ancestor).',
                                               'person_id': 'Millennium PERSON_ID.',
                                               'problem_id': 'Millennium PROBLEM_ID of the condition when it '
                                                             'is a problem.',
                                               'procedure_id': 'Millennium PROCEDURE_ID of the operation row '
                                                               '(NULL for implant rows).',
                                               'procedure_indication_key': 'Deterministic SHA-256 key of the '
                                                                           'candidate: '
                                                                           "stable_id('procedure_indication', "
                                                                           'procedure_patient_event_key, '
                                                                           'condition_patient_event_key). '
                                                                           'Primary key: the table holds one '
                                                                           'row per (procedure, condition) '
                                                                           'pair.',
                                               'procedure_patient_event_key': 'patient_event_key of the '
                                                                              'clinical_procedure row: an '
                                                                              'OPCS-4 operation, or an '
                                                                              'implant linked to exactly one '
                                                                              'operation.',
                                               'procedure_site_code': 'SNOMED body structure of the '
                                                                      'operation (an OPCS-4 site code of the '
                                                                      "coded operation via OMOP 'Maps to').",
                                               'procedure_site_display': 'SNOMED name of '
                                                                         'procedure_site_code.',
                                               'procedure_source_object': 'source_object of the procedure '
                                                                          'row: millennium_procedure or '
                                                                          'implant.',
                                               'rule_id': 'Matching rule: procedure_site_finding_site_v1.',
                                               'rule_version': 'Rule version.'},
                                   'table': 'Candidate indications for coded operations and singly-linked '
                                            'implants (PROCEDURE_INDICATION_V1): same-encounter conditions '
                                            "whose SNOMED finding site matches the operation's coded body "
                                            'site exactly or within two is-a levels. One row per (procedure, '
                                            'condition); indication_rank 1 = closest anatomy, then lowest '
                                            'diagnosis priority. Rule-based, no manual curation.'}}
