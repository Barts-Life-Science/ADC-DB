"""Silver spine.person_gp_registration: 1 dataset(s).

- datasets: reference_person_gp_registration
- moved verbatim from: 1_silver_spine_and_reference:6982, 1_silver_spine_and_reference:6984, 1_silver_spine_and_reference:7021

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from pyspark.sql.window import Window
import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables.spine._shared import SRC_PERSON


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# ==== journey_reference.person_gp_registration ==== PMS_P1_T5_SILVER_V1
SRC_GP_REGISTRATION = "4_prod.bronze.map_person_gp_registration"


def gp_registration_body(s):
    """Shared body: bronze map_person_gp_registration -> silver reference_person_gp_registration."""
    return s.select(
        F.col("PERSON_ID").cast("bigint").alias("person_id"),
        F.col("PERSON_ORG_RELTN_ID").cast("bigint").alias("person_org_reltn_id"),
        F.col("ORGANIZATION_ID").cast("bigint").alias("organization_id"),
        F.col("PRACTICE_ODS_CODE").alias("practice_ods_code"),
        F.col("PRACTICE_NAME").alias("practice_name"),
        F.col("BEG_EFFECTIVE_DT_TM").alias("registration_start"),
        F.col("END_EFFECTIVE_DT_TM").alias("registration_end"),
        (F.col("ACTIVE_IND") == 1).alias("active_ind"),
        F.col("CURRENT_IND").alias("current_ind"),
        (F.col("FREE_TEXT_IND") == 1).alias("free_text_ind"),
        F.col("ADC_UPDT").alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.lit("bronze.map_person_gp_registration").alias("_source_table"),
        F.col("PERSON_ORG_RELTN_ID").cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.person_gp_registration"),
    cluster_by=["person_id"], refresh_policy="auto",
)
def person_gp_registration():
    # Build the declared dataset: registered GP practice history per person.
    # INSR_W2 W2.06 (L5172): starts before 1900 or before birth - 1 day are not registration dates (NULL);
    # registration_end_derived = the next registration's start for the same person (partitioned window).
    df = gp_registration_body(read_source(SRC_GP_REGISTRATION)).withColumn(
        "person_gp_registration_key", stable_id("person_gp_registration:mill", F.col("person_org_reltn_id")))
    birth = read_source(SRC_PERSON).select(F.col("person_id").cast("bigint").alias("_gp_pid"),
                                           F.col("birth_date").alias("_gp_birth"))
    df = df.join(birth, F.col("person_id") == F.col("_gp_pid"), "left")
    implausible = (F.col("registration_start") < F.lit("1900-01-01").cast("timestamp")) | (
        F.col("registration_start") < F.date_sub(F.col("_gp_birth"), 1).cast("timestamp"))
    df = df.withColumn("registration_start", F.when(~F.coalesce(implausible, F.lit(False)), F.col("registration_start")))
    nxt = F.lead("registration_start").over(Window.partitionBy("person_id").orderBy(
        F.col("registration_start").asc_nulls_last(), F.col("person_gp_registration_key").asc()))
    df = (df.withColumn("registration_end_derived", F.coalesce(F.col("registration_end"), nxt))
            .withColumn("registration_end_basis",
                        F.when(F.col("registration_end").isNotNull(), F.lit("source_end"))
                         .when(nxt.isNotNull(), F.lit("next_registration_start"))))
    return df.drop("_gp_pid", "_gp_birth")


VALUE_POLICIES = {
    'reference_person_gp_registration': ValuePolicy(table='reference_person_gp_registration', enabled=True),
}


# W2.06 (L5172): ODS validity of the registered practice code.
VALUE_POLICIES["reference_person_gp_registration"].register("practice_code_ods_valid", ["practice_ods_code"],
         lambda F: F.when(F.col("practice_ods_code").isNull(), F.lit(None).cast("string"))
                    .when(F.upper(F.col("practice_ods_code")).rlike(r"^V81"), F.lit("pseudo_v81"))
                    .when(F.upper(F.col("practice_ods_code")).rlike(r"^5[A-Z0-9]{2}$"), F.lit("legacy_pct"))
                    .when(F.upper(F.col("practice_ods_code")).rlike(r"^[A-Z][0-9]{5}$"), F.lit("valid"))
                    .otherwise(F.lit("malformed")),
         "Practice code class: valid (England/Wales/NI letter + 5 digits), pseudo_v81 (V81 'no registered GP' "
         "pseudo codes), legacy_pct (5xx primary care trust codes), malformed, or NULL. INSR_W2 (L5172).")


WRAP_POLICY = {'reference_person_gp_registration'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_person_gp_registration': {'columns': {'_source_row_id': 'PERSON_ORG_RELTN_ID as string.',
                                                  '_source_system': 'Always millennium.',
                                                  '_source_table': 'Bronze source table.',
                                                  'active_ind': 'Source ACTIVE_IND = 1.',
                                                  'current_ind': 'True on the latest-starting active, open '
                                                                 'registration per person as of the bronze '
                                                                 'build.',
                                                  'free_text_ind': 'Practice was recorded as free text, not '
                                                                   'a coded organisation.',
                                                  'loaded_at': 'map_person_gp_registration.ADC_UPDT carried '
                                                               'unchanged.',
                                                  'organization_id': 'Millennium ORGANIZATION_ID of the '
                                                                     'registered practice; null when '
                                                                     'free-text only.',
                                                  'person_gp_registration_key': 'Deterministic key for the '
                                                                                'Millennium '
                                                                                'registered-practice '
                                                                                'relationship row.',
                                                  'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                                  'person_org_reltn_id': 'Millennium PERSON_ORG_RELTN_ID; '
                                                                         'one registration row.',
                                                  'practice_code_ods_valid': 'Practice code class: valid '
                                                                             '(England/Wales/NI letter + 5 '
                                                                             "digits), pseudo_v81 (V81 'no "
                                                                             "registered GP' pseudo codes), "
                                                                             'legacy_pct (5xx primary care '
                                                                             'trust codes), malformed, or '
                                                                             'NULL. INSR_W2 (L5172).',
                                                  'practice_name': 'Practice name from ORGANIZATION, else '
                                                                   'the free-text name on the relationship.',
                                                  'practice_ods_code': 'NHS ODS code of the practice '
                                                                       '(ORGANIZATION_ALIAS pool 6031508); '
                                                                       'null when the organisation carries '
                                                                       'none.',
                                                  'registration_end': 'End of the registration; null while '
                                                                      'open (2100 sentinel removed in '
                                                                      'bronze).',
                                                  'registration_end_basis': 'source_end or '
                                                                            'next_registration_start; NULL '
                                                                            'when the registration is still '
                                                                            'open. INSR_W2 W2.06.',
                                                  'registration_end_derived': 'registration_end, else the '
                                                                              "start of the same person's "
                                                                              'next registration (an '
                                                                              'open-ended row followed by a '
                                                                              'later registration has '
                                                                              'ended). INSR_W2 W2.06.',
                                                  'registration_start': 'Start of the registration '
                                                                        '(BEG_EFFECTIVE_DT_TM, UTC).'},
                                      'table': 'Registered GP practice history per person (Millennium '
                                               'Registered Practice relationship); practice code/name are '
                                               'quasi-identifiers.'}}
