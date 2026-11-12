"""Silver index.person_scope: 1 dataset(s).

- datasets: reference_person_scope
- moved verbatim from: 1_silver_spine_and_reference:3656, 1_silver_spine_and_reference:3670, 1_silver_spine_and_reference:3701

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _code_value_decoder, _n, read_source, spark
from silver_tables.index._shared import EVENT_FEED_NAMES
from silver_tables.spine._shared import SRC_PATIENT_IDENTIFIER, SRC_PERSON, _person_alias_selection
from silver_tables.spine.person import person


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _person_scope_activity():
    """All feeds' per-person activity, folded to one row per person."""
    frames = [spark.read.table(_n(f"journey_reference._person_activity_{feed}")) for feed in EVENT_FEED_NAMES]
    union = frames[0]
    for frame in frames[1:]:
        union = union.unionByName(frame)
    return union.groupBy("person_id").agg(
        F.sum("n_rows").cast("long").alias("activity_row_count"),
        F.sum("n_clinical_rows").cast("long").alias("clinical_row_count"),
        F.min("first_event_datetime").alias("first_activity_datetime"),
        F.max("last_event_datetime").alias("last_activity_datetime"),
        F.max("last_clinical_event_datetime").alias("last_clinical_event_datetime"))


def _person_scope_sources():
    """Per person: has a Barts MRN alias, has a BHRUT MRN alias, and the decoded Millennium person type."""
    a = read_source(SRC_PATIENT_IDENTIFIER).where(F.col("ALIAS_TYPE") == "MRN")
    pools = a.groupBy(F.col("PERSON_ID").cast("bigint").alias("person_id")).agg(
        F.max((F.col("ALIAS_POOL_CD") == F.lit(6200990)).cast("int")).alias("_barts"),
        F.max((F.col("ALIAS_POOL_CD") == F.lit(1115132483)).cast("int")).alias("_bhrut"))
    return pools


@materialized_view(name=_n("journey_reference.person_scope"), refresh_policy="auto", cluster_by=["person_id"])
def person_scope():
    # Join the spine to the grouped evidence; equi-joins and GROUP BY only.
    spine = spark.read.table(_n("journey_spine.person")).select("person_id")
    contact = spark.read.table(_n("journey_reference._person_contact")).groupBy("person_id").agg(
        F.concat_ws(",", F.sort_array(F.collect_set("contact_source"))).alias("encounter_sources"),
        F.max((F.col("contact_source") == "mill_encounter").cast("int")).alias("_mill"))
    any_encounter = spark.read.table(_n("journey_spine.encounter")).where(F.col("person_id").isNotNull()) \
        .groupBy(F.col("person_id").cast("bigint").alias("person_id")).agg(F.count(F.lit(1)).alias("_any_enc"))
    activity = _person_scope_activity()
    pools = _person_scope_sources()
    # J8: the BHRUT MRN as its own column (RF4 pool), selected by the same governed rule as current_mrn.
    bhrut = _person_alias_selection("MRN", {1115132483: ("bhrut", 1)}).select(
        F.col("_pid").cast("bigint").alias("person_id"), F.col("_value").alias("bhrut_mrn"))
    # W1.12 carries (required in this release, open issue 1): name keys for the test-patient flag and the
    # person update task/time. Absent columns read as typed NULLs so the rest of the graph still builds in dev
    # before W1.12 lands; the W2_09 assertion (test_patient_ind non-null on Person records) then fails, and that
    # failure blocks W2.16, so the release cannot ship the flag empty.
    source_person = read_source(SRC_PERSON)
    cols = {c.lower(): c for c in source_person.columns}

    def carried(name, data_type):
        """A W1.12 map_person column, or a typed NULL while the bronze carry is not there yet."""
        return F.col(cols[name]).cast(data_type) if name in cols else F.lit(None).cast(data_type)
    person = source_person.select(
        F.col(cols["person_id"]).cast("bigint").alias("person_id"),
        F.col(cols["person_type_cd"]).cast("bigint").alias("_ptype"),
        F.upper(carried("name_last_key", "string")).alias("_last"),
        F.upper(carried("name_first_key", "string")).alias("_first"),
        F.expr(f"try_cast(`{cols['source_updt_task']}` AS BIGINT)").alias("source_update_task") if "source_updt_task" in cols
        else F.lit(None).cast("bigint").alias("source_update_task"),
        carried("source_updt_dt_tm", "timestamp").alias("source_update_datetime"))
    ptype = F.broadcast(_code_value_decoder(302, "_pt"))
    df = (spine.join(contact, "person_id", "left").join(any_encounter, "person_id", "left")
               .join(activity, "person_id", "left").join(pools, "person_id", "left")
               .join(bhrut, "person_id", "left").join(person, "person_id", "left")
               .join(ptype, F.col("_ptype") == F.col("_pt_code"), "left"))
    has_encounter = F.col("encounter_sources").isNotNull()
    has_clinical = F.coalesce(F.col("clinical_row_count"), F.lit(0)) > 0
    has_any = (F.coalesce(F.col("activity_row_count"), F.lit(0)) > 0) | F.col("_any_enc").isNotNull()
    # P1c2 buckets (W0.01 rule c): Person-type records only; Freetext / Contributor System records are not patients.
    last, first = F.col("_last"), F.col("_first")
    bucket = (F.when(last.isin("TEST", "TESTPATIENT", "TESTING"), "surname_test_exact")
               .when(last.rlike("^ZZ"), "surname_zz_prefix").when(last.rlike("^XX"), "surname_xx_prefix")
               .when(last.rlike("^TEST[0-9]|TESTPATIENT|^DUMMY|^FICTITIOUS|^TRAINING"), "surname_test_variant")
               .when(last.rlike("^TEST"), "surname_test_prefix_other")
               .when(first.rlike("^(TEST|DUMMY)"), "forename_test_dummy"))
    is_person_record = F.upper(F.col("_pt_display")) == "PERSON"
    has_name_keys = "name_last_key" in cols and "name_first_key" in cols
    test_ind = F.when(is_person_record, bucket.isNotNull()) if has_name_keys else F.lit(None).cast("boolean")
    # INSR_W2 W2.20 (W1.12, L3863): basis of the person record's last update.
    update_basis = (F.when((F.col("source_update_task") == 0)
                           & (F.col("source_update_datetime") >= F.lit("2025-10-01").cast("timestamp"))
                           & (F.col("source_update_datetime") < F.lit("2025-12-01").cast("timestamp")),
                           F.lit("bulk_admin_batch_2025_10_11"))
                     .when(F.col("source_update_task").isNotNull(), F.lit("application_task")))
    return df.select(
        "person_id",
        has_encounter.alias("has_encounter"), "encounter_sources",
        has_clinical.alias("has_clinical_data"),
        (has_encounter | has_clinical).alias("in_scope"),
        has_any.alias("has_any_activity"),
        F.coalesce(F.col("clinical_row_count"), F.lit(0)).cast("long").alias("clinical_row_count"),
        F.to_date("first_activity_datetime").alias("first_activity_date"),
        F.to_date("last_activity_datetime").alias("last_activity_date"),
        "last_clinical_event_datetime",
        F.when((F.col("_barts") == 1) & (F.col("_bhrut") == 1), "both").when(F.col("_barts") == 1, "barts")
         .when(F.col("_bhrut") == 1, "bhrut").otherwise("none").alias("person_source"),
        "bhrut_mrn",
        test_ind.alias("test_patient_ind"), F.when(test_ind, bucket).alias("test_patient_basis"),
        F.col("_pt_display").alias("person_record_type"),
        "source_update_task", "source_update_datetime", update_basis.alias("source_update_basis"),
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_person_scope': {'columns': {'bhrut_mrn': 'Current BHRUT MRN (RF4 MRN pool), selected by the same '
                                                     'governed alias rule as spine_person.current_mrn; NULL '
                                                     'when the person has no BHRUT alias or the pool is '
                                                     'ambiguous. Identifier: tagged by copying '
                                                     "spine_person.current_mrn's estate tags (W7.02 helper).",
                                        'clinical_row_count': 'Clinical-category rows across all silver '
                                                              'event feeds for the person.',
                                        'encounter_sources': 'Sorted, comma-separated contact sources that '
                                                             'witnessed an encounter (see has_encounter).',
                                        'first_activity_date': 'Earliest event date across the event feeds.',
                                        'has_any_activity': 'True when the person has a row of any category '
                                                            '(clinical or administrative) in any silver '
                                                            'event feed, or any Millennium encounter row '
                                                            '(any class).',
                                        'has_clinical_data': 'True when the person has at least one '
                                                             'clinical-category row (fact_category = '
                                                             "'clinical') in any silver event feed, linked "
                                                             'documents included. Administrative rows '
                                                             '(appointments including cancelled and DNA, '
                                                             'waiting list, RTT, costing, HRG, drug '
                                                             'expenditure) do not count (C0 ruling 1).',
                                        'has_encounter': 'True when the person has a qualifying encounter '
                                                         'under the ruled definition (2026-10-29): an '
                                                         'observed Millennium encounter that is not '
                                                         'pre-registration, wait list or results only; an '
                                                         'attended appointment; a performed theatre case; a '
                                                         'community (CSDS) contact; an attended maternity '
                                                         'contact; an ED check-in; a critical-care '
                                                         'admission; a neonatal episode; or a SLAM OP/APC '
                                                         'HRG attendance or admission.',
                                        'in_scope': 'has_encounter OR has_clinical_data: the gold and '
                                                    'omop_gold person scope (C0 ruling 1, 2026-10-29). '
                                                    'Silver keeps every person.',
                                        'last_activity_date': 'Latest event date across the event feeds.',
                                        'last_clinical_event_datetime': 'Latest event_datetime among '
                                                                        'clinical-category feed rows '
                                                                        '(administrative and bulk categories '
                                                                        'excluded); used for death evidence '
                                                                        '(L5145).',
                                        'person_id': 'Native Millennium PERSON_ID; one row per spine_person '
                                                     'person.',
                                        'person_record_type': 'Decoded Millennium PERSON_TYPE_CD (Person, '
                                                              'Freetext, Contributor System): non-Person '
                                                              'records are not patients.',
                                        'person_source': 'barts, bhrut, both or none: which Trust MRN pools '
                                                         'hold an alias for the person (RNJ 5C4 MRN = Barts, '
                                                         'RF4 MRN = BHRUT).',
                                        'source_update_basis': 'How the person record was last updated: '
                                                               'bulk_admin_batch_2025_10_11 (UPDT_TASK 0 in '
                                                               '2025-10/11: 83% of all task-0 updates, '
                                                               'P_W112 L3863) or application_task; NULL '
                                                               'without an update task.',
                                        'source_update_datetime': 'map_person source_updt_dt_tm (raw '
                                                                  "UPDT_DT_TM) of the person record's last "
                                                                  'update (W1.12 carry).',
                                        'source_update_task': 'map_person source_updt_task (raw UPDT_TASK) '
                                                              "of the person record's last update (W1.12 "
                                                              'carry); 0 marks a bulk administrative batch.',
                                        'test_patient_basis': 'The name-key rule that set test_patient_ind '
                                                              '(surname_zz_prefix, surname_test_exact, ...).',
                                        'test_patient_ind': 'True when the Millennium person record matches '
                                                            'a test-patient name pattern (ZZ/XX surname '
                                                            'prefix, TEST/DUMMY/FICTITIOUS/TRAINING names) '
                                                            'and is a Person-type record; false for other '
                                                            'Person-type records. Warning only; nobody is '
                                                            'dropped (J8).'},
                            'table': 'INSR_W2 W2.09: one row per person: encounter / clinical-data evidence '
                                     'and the gold scope flag.'}}
