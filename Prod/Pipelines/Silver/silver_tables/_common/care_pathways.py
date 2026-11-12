"""Silver _common.care_pathways: 0 dataset(s).

- shared helpers (no datasets)
- moved verbatim from: 4_silver_care_pathways:195, 4_silver_care_pathways:298, 4_silver_care_pathways:332, 4_silver_care_pathways:333, 4_silver_care_pathways:334, 4_silver_care_pathways:335, 4_silver_care_pathways:337, 4_silver_care_pathways:340

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import read_source, stable_id, subject_key_with_system
from silver_tables._common.journey import SRC_MAT_PREGNANCY, _mat_pregnancy_dedup


# Spine / person relationship

# ==== Journeys, maternity relationships and temporal membership links ====
SRC_MAT_BIRTH = "4_prod.bronze.map_mat_birth"


# Pregnancy journey canonical
def _pregnancy_journey_canonical():
    # Assemble normalized pregnancy journey rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    base = _pregnancy_constructor_base()
    parent_id = stable_id("journey:pregnancy", F.col("_pregnancy_id"))
    # contract v2: rename journey SHA identities to keys in parent and trimester rows
    parent = base.select(
        parent_id.alias("journey_key"),
        F.lit(None).cast("string").alias("parent_journey_key"),
        F.col("_subject_key").alias("subject_key"),
        F.col("_subject_id_system").alias("subject_id_system"),
        F.col("_person_id").alias("person_id"),
        F.lit("pregnancy").alias("journey_type_code"),
        F.lit("Pregnancy").alias("journey_type_display"),
        F.col("_period_start").alias("period_start"),
        F.col("_period_end").alias("period_end"),
        F.col("_period_start_basis").alias("period_start_basis"),
        F.col("_period_end_basis").alias("period_end_basis"),
        F.col("_status_code").alias("status_code"),
        F.lit("http://snomed.info/sct").alias("defining_coding_system"),
        F.lit("77386006").alias("defining_code"),
        F.lit("Pregnancy").alias("defining_display"),
        F.col("_pregnancy_outcome_code").alias("outcome_code"),
        F.col("_pregnancy_outcome_display").alias("outcome_display"),
        F.col("_pregnancy_id").alias("source_journey_identifier"),
        F.lit("pregnancy").alias("construction_rule"),
        F.lit("pregnancy-v2").alias("construction_version"),  # PREGNANCY_DATES_V2
        F.lit("millennium-maternity").alias("source_system"),
        F.lit(SRC_MAT_PREGNANCY).alias("source_table"),
        F.col("_pregnancy_id").alias("source_row_id"),
        F.date_format("_loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("_source_update_timestamp").alias("source_update_timestamp"),
        F.col("_loaded_at").alias("loaded_at"),
    )

    expanded = base.withColumn("_trimester", F.explode(F.array(F.lit(1), F.lit(2), F.lit(3))))
    raw_start = (
        F.when(F.col("_trimester") == 1, F.col("_period_start"))
        .when(F.col("_trimester") == 2, F.date_add("_period_start", 98).cast("timestamp"))
        .otherwise(F.date_add("_period_start", 196).cast("timestamp"))
    )
    nominal_end = (
        F.when(F.col("_trimester") == 1, F.date_add("_period_start", 97).cast("timestamp"))
        .when(F.col("_trimester") == 2, F.date_add("_period_start", 195).cast("timestamp"))
        .otherwise(F.col("_period_end"))
    )
    child_start = F.when(
        F.col("_period_end").isNull() | (raw_start <= F.col("_period_end")), raw_start
    )
    child_end = F.when(
        child_start.isNotNull(),
        F.when(F.col("_period_end").isNull(), nominal_end)
        .otherwise(F.least(nominal_end, F.col("_period_end"))),
    )
    # PREGNANCY_DATES_V2: trimester boundaries derive from the pregnancy's, so they carry its bases; the fixed
    # 98-day ends of trimesters 1 and 2 are marked trimester_nominal unless the pregnancy end cut them short.
    child_start_basis = F.when(child_start.isNotNull(), F.col("_period_start_basis"))
    child_end_basis = (
        F.when(child_end.isNull(), F.lit(None).cast("string"))
        .when((F.col("_trimester") < 3) & (F.col("_period_end").isNull() | (nominal_end <= F.col("_period_end"))),
              F.lit("trimester_nominal"))
        .otherwise(F.col("_period_end_basis"))
    )
    child = expanded.select(
        stable_id(
            "journey:pregnancy:trimester", F.col("_pregnancy_id"), F.col("_trimester")
        ).alias("journey_key"),
        parent_id.alias("parent_journey_key"),
        F.col("_subject_key").alias("subject_key"),
        F.col("_subject_id_system").alias("subject_id_system"),
        F.col("_person_id").alias("person_id"),
        F.lit("pregnancy_trimester").alias("journey_type_code"),
        F.concat(F.lit("Pregnancy trimester "), F.col("_trimester")).alias("journey_type_display"),
        child_start.alias("period_start"),
        child_end.alias("period_end"),
        child_start_basis.alias("period_start_basis"),
        child_end_basis.alias("period_end_basis"),
        F.col("_status_code").alias("status_code"),
        F.lit("urn:journey:pregnancy-structure").alias("defining_coding_system"),
        F.concat(F.lit("trimester-"), F.col("_trimester")).alias("defining_code"),
        F.concat(F.lit("Pregnancy trimester "), F.col("_trimester")).alias("defining_display"),
        F.lit(None).cast("string").alias("outcome_code"),
        F.lit(None).cast("string").alias("outcome_display"),
        F.col("_pregnancy_id").alias("source_journey_identifier"),
        F.lit("pregnancy-trimester").alias("construction_rule"),
        F.lit("pregnancy-v2").alias("construction_version"),  # PREGNANCY_DATES_V2
        F.lit("millennium-maternity").alias("source_system"),
        F.lit(SRC_MAT_PREGNANCY).alias("source_table"),
        F.concat_ws(":", F.col("_pregnancy_id"), F.lit("trimester"), F.col("_trimester"))
        .alias("source_row_id"),
        F.date_format("_loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("_source_update_timestamp").alias("source_update_timestamp"),
        F.col("_loaded_at").alias("loaded_at"),
    )
    return parent.unionByName(child).select(*JOURNEY_PUBLIC_COLUMNS)


# Spine / journey
JOURNEY_PUBLIC_COLUMNS = [
    "journey_key",
    "parent_journey_key",
    "subject_key",
    "subject_id_system",
    "person_id",
    "journey_type_code",
    "journey_type_display",
    "period_start",
    "period_end",
    "period_start_basis",  # SILVER_RELEASE_20261025 item 5
    "period_end_basis",  # SILVER_RELEASE_20261025 item 5
    "status_code",
    "defining_coding_system",
    "defining_code",
    "defining_display",
    "outcome_code",
    "outcome_display",
    "source_journey_identifier",
    "construction_rule",
    "construction_version",
    "source_system",
    "source_table",
    "source_row_id",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]


# SILVER_RELEASE_20261025 item 5: PREGNANCY_DATES_V2 pregnancy boundaries (Ben, 2026-10-25).
# v1 took the first available of LMP / EDD-280 / first contact / first antenatal appointment as the start and of
# discharge / last birth / EDD as the end, so a late discharge or an early contact date could stretch a pregnancy
# over years. v2 trusts an observed birth first, accepts weaker dates only inside a 308-day (44-week) window, and
# records the rule behind each boundary in period_start_basis / period_end_basis. Nothing is queued for review.
PREGNANCY_MAX_SPAN_DAYS = 308            # 44 weeks: the longest plausible pregnancy


PREGNANCY_NOMINAL_DAYS = 280             # EDD - 280 days = the dated start (40 weeks)


PREGNANCY_MIN_GESTATION_DAYS = 140       # 20 weeks: shortest recorded gestation treated as plausible


PREGNANCY_DATING_TOLERANCE_DAYS = 14     # LMP/EDD dating "contradicts" the birth gestation beyond this


# When the span guard fires, the boundary from the less-trusted rule is NULLed (never both; ties null the start).
PREGNANCY_START_TRUST = {"birth_minus_gestation": 4, "last_menstrual_period": 3,
                         "expected_delivery_date_minus_280": 2, "first_contact": 1,
                         "first_antenatal_appointment": 1}


PREGNANCY_END_TRUST = {"last_birth": 4, "expected_delivery_date": 2, "maternity_service_discharge": 1}


def _birth_gestation_days():
    """Recorded gestation at birth in days (weeks * 7 + remainder days) for a map_mat_birth row."""
    return F.col("GestationWeeks") * 7 + F.coalesce(F.col("GestationDays"), F.lit(0))


def _plausible_birth_gestation():
    """A birth with a timestamp and a gestation bronze did not flag implausible, within 140-308 days."""
    return (F.col("BirthDateTime").isNotNull()
            & ~F.coalesce(F.col("GestationImplausible_IND").cast("boolean"), F.lit(False))
            & _birth_gestation_days().between(PREGNANCY_MIN_GESTATION_DAYS, PREGNANCY_MAX_SPAN_DAYS))


def _trust(basis, table):
    """Trust rank of a boundary basis (0 when there is no boundary)."""
    rank = F.lit(0)
    for name, value in table.items():
        rank = F.when(basis == name, F.lit(value)).otherwise(rank)
    return rank


def _pregnancy_period(lmp, edd, first_contact, first_appointment, discharge, last_birth, gestation_start):
    """PREGNANCY_DATES_V2: (start, start_basis, end, end_basis) for one pregnancy.

    1. Anchored start: LMP, else EDD - 280; replaced by the latest plausible birth minus its gestation when that
       exists and the LMP/EDD date is missing or more than 14 days away from it.
    2. End: the last birth; else discharge, else EDD, each only 0-308 days after the anchored start (accepted
       as-is when there is no anchored start).
    3. Start fallback: first contact, else first antenatal appointment, only on or before the end and within
       308 days of it (accepted as-is when there is no end).
    4. Guard: a span still over 308 days, or negative, NULLs the boundary whose rule is less trusted.
    """
    days = lambda later, earlier: F.datediff(later, earlier)
    edd_start = F.date_sub(edd, PREGNANCY_NOMINAL_DAYS).cast("timestamp")
    dated_start = F.coalesce(lmp, edd_start)
    dated_basis = (F.when(lmp.isNotNull(), F.lit("last_menstrual_period"))
                   .when(edd.isNotNull(), F.lit("expected_delivery_date_minus_280")))
    contradicted = gestation_start.isNotNull() & (
        dated_start.isNull() | (F.abs(days(dated_start, gestation_start)) > PREGNANCY_DATING_TOLERANCE_DAYS))
    anchored_start = F.when(contradicted, gestation_start).otherwise(dated_start)
    anchored_basis = F.when(contradicted, F.lit("birth_minus_gestation")).otherwise(dated_basis)

    def within_window(boundary):
        # The boundary falls 0-308 days after the anchored start (or there is no anchored start to test against).
        span = days(boundary, anchored_start)
        return boundary.isNotNull() & (anchored_start.isNull() | span.between(0, PREGNANCY_MAX_SPAN_DAYS))

    end = (F.when(last_birth.isNotNull(), last_birth)
           .when(within_window(discharge), discharge)
           .when(within_window(edd), edd))
    end_basis = (F.when(last_birth.isNotNull(), F.lit("last_birth"))
                 .when(within_window(discharge), F.lit("maternity_service_discharge"))
                 .when(within_window(edd), F.lit("expected_delivery_date")))

    contact = F.coalesce(first_contact, first_appointment)
    contact_basis = (F.when(first_contact.isNotNull(), F.lit("first_contact"))
                     .when(first_appointment.isNotNull(), F.lit("first_antenatal_appointment")))
    contact_ok = contact.isNotNull() & (end.isNull() | days(end, contact).between(0, PREGNANCY_MAX_SPAN_DAYS))
    start = F.when(anchored_start.isNotNull(), anchored_start).when(contact_ok, contact)
    start_basis = F.when(anchored_start.isNotNull(), anchored_basis).when(contact_ok, contact_basis)

    span = days(end, start)
    implausible = start.isNotNull() & end.isNotNull() & ((span > PREGNANCY_MAX_SPAN_DAYS) | (span < 0))
    null_start = implausible & (_trust(start_basis, PREGNANCY_START_TRUST) <= _trust(end_basis, PREGNANCY_END_TRUST))
    null_end = implausible & ~null_start
    guarded = F.lit("nulled_span_guard")
    return (F.when(~null_start, start), F.when(null_start, guarded).otherwise(start_basis),
            F.when(~null_end, end), F.when(null_end, guarded).otherwise(end_basis))


def _pregnancy_constructor_base():
    # Combine deduplicated pregnancy records with birth dates and outcomes used to construct
    # maternity journeys.
    p = _mat_pregnancy_dedup().alias("p")
    b = (
        read_source(SRC_MAT_BIRTH)
        .groupBy("Pregnancy_ID")
        .agg(
            F.min("BirthDateTime").alias("_first_birth_datetime"),
            F.max("BirthDateTime").alias("_last_birth_datetime"),
            F.max("PregOutcome_CD").cast("string").alias("_pregnancy_outcome_code"),
            F.max("PregOutcome_DESC").alias("_pregnancy_outcome_display"),
            F.max("ADC_UPDT").alias("_birth_loaded_at"),
            F.max("BirthSourceRecordUpdatedDateTime").alias("_birth_source_update"),
            # PREGNANCY_DATES_V2: the latest birth with a plausible recorded gestation, and the start it implies.
            F.max(F.when(_plausible_birth_gestation(), F.struct(
                F.col("BirthDateTime").alias("birth"),
                F.date_sub(F.col("BirthDateTime"), _birth_gestation_days()).cast("timestamp").alias("start"),
            ))).alias("_gestation_birth"),
        )
        .alias("b")
    )
    joined = p.join(b, p.Pregnancy_ID == b.Pregnancy_ID, "left")
    source_row_id = p.Pregnancy_ID.cast("string")
    skey, ssys = subject_key_with_system(
        [
            ("urn:cerner:person_id", p.Person_ID),
            ("urn:barts:mrn", p.MRN),
            ("https://fhir.nhs.uk/Id/nhs-number", p.NHS_Number),
        ],
        SRC_MAT_PREGNANCY,
        source_row_id,
    )
    start, start_basis, end, end_basis = _pregnancy_period(
        p.LastMensPeriodDate, p.ExpectedDeliveryDate, p.PregnancyFirstContactDate, p.FirstAntenatalAPPTDate,
        p.MaternityServiceDischargeDate, F.col("b._last_birth_datetime"), F.col("b._gestation_birth.start"),
    )
    deleted = F.coalesce(p.SOURCE_DELETED_IND.cast("boolean"), F.lit(False))
    completed = p.MaternityServiceDischargeDate.isNotNull() | F.col("b._last_birth_datetime").isNotNull()
    status = (
        F.when(deleted, F.lit("superseded"))
        .when(completed, F.lit("completed"))
        .otherwise(F.lit("active"))
    )
    source_update = F.greatest(
        p.MAT_RECORD_UPDATED_DT,
        p.MSDS_RECORD_UPDATED_DT,
        p.BIRTH_ADC_UPDT,
        F.col("b._birth_source_update"),
    )
    loaded_at = F.greatest(p.ADC_UPDT, F.col("b._birth_loaded_at"))
    # contract v2: publish the resolved pregnancy subject as native Millennium PERSON_ID
    return joined.select(
        p.Pregnancy_ID.cast("string").alias("_pregnancy_id"),
        skey.alias("_subject_key"),
        ssys.alias("_subject_id_system"),
        p.Person_ID.cast("bigint").alias("_person_id"),
        start.alias("_period_start"),
        end.alias("_period_end"),
        start_basis.alias("_period_start_basis"),
        end_basis.alias("_period_end_basis"),
        status.alias("_status_code"),
        F.col("b._pregnancy_outcome_code"),
        F.col("b._pregnancy_outcome_display"),
        source_update.alias("_source_update_timestamp"),
        loaded_at.alias("_loaded_at"),
    )


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_journey():
    # Assemble journey rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    return _pregnancy_journey_canonical()


# Spine / participant

# contract v2: retain the full canonical shape for internal reuse.
JOURNEY_PARTICIPANT_SOURCE_COLUMNS = [
    "journey_participant_key",
    "journey_key",
    "subject_key",
    "subject_id_system",
    "person_id",
    "role_code",
    "role_display",
    "valid_from",
    "valid_to",
    "record_status",
    "construction_rule",
    "construction_version",
    "source_system",
    "source_table",
    "source_row_id",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]


def _mother_journey_participants():
    # Create the mother's participant record for each pregnancy journey, carrying the journey's
    # effective period.
    j = _pregnancy_journey_canonical()
    # contract v2: use the renamed journey key and publish an explicit participant key for mother rows
    return j.select(
        stable_id("journey_participant:mother", F.col("journey_key"), F.col("subject_key"))
        .alias("journey_participant_key"),
        "journey_key", "subject_key", "subject_id_system", "person_id",
        F.lit("mother").alias("role_code"),
        F.lit("Mother / primary subject").alias("role_display"),
        F.col("period_start").alias("valid_from"),
        F.col("period_end").alias("valid_to"),
        F.when(F.col("status_code") == "superseded", F.lit("superseded"))
        .otherwise(F.lit("active")).alias("record_status"),
        F.lit("pregnancy-primary-subject").alias("construction_rule"),
        F.lit("pregnancy-v1").alias("construction_version"),
        "source_system", "source_table", "source_row_id", "load_batch_id", "source_update_timestamp", "loaded_at",
    )


def _baby_journey_participants():
    # Create baby participant records from birth data using the existing person, NHS-number and
    # MRN evidence hierarchy.
    b = read_source(SRC_MAT_BIRTH)
    source_row_id = b.BirthRow_ID.cast("string")
    skey, ssys = subject_key_with_system(
        [
            ("urn:cerner:person_id", b.BabyPerson_ID),
            ("https://fhir.nhs.uk/Id/nhs-number", b.Baby_NHS),
            ("urn:barts:mrn", b.Baby_MRN),
        ],
        SRC_MAT_BIRTH,
        source_row_id,
    )
    deleted = F.coalesce(b.PregnancySource_DELETE_IND.cast("long"), F.lit(0)) != 0
    # contract v2: use explicit journey keys and native baby PERSON_ID for baby participant rows
    return b.select(
        stable_id("journey_participant:baby", b.Pregnancy_ID, b.BirthRow_ID)
        .alias("journey_participant_key"),
        stable_id("journey:pregnancy", b.Pregnancy_ID).alias("journey_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        b.BabyPerson_ID.cast("bigint").alias("person_id"),
        F.lit("baby").alias("role_code"), F.lit("Baby").alias("role_display"),
        b.BirthDateTime.alias("valid_from"), F.lit(None).cast("timestamp").alias("valid_to"),
        F.when(deleted, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        F.lit("pregnancy-birth-participant").alias("construction_rule"),
        F.lit("pregnancy-v1").alias("construction_version"),
        F.lit("millennium-maternity").alias("source_system"),
        F.lit(SRC_MAT_BIRTH).alias("source_table"), source_row_id.alias("source_row_id"),
        F.date_format(b.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.greatest(b.BirthSourceRecordUpdatedDateTime, b.PregnancySourceRecordUpdatedDateTime,
                   b.NNUSourceLastUpdate).alias("source_update_timestamp"),
        b.ADC_UPDT.alias("loaded_at"),
    )


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_journey_participant():
    # Assemble journey participant rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    mother = _mother_journey_participants().select(*JOURNEY_PARTICIPANT_SOURCE_COLUMNS)
    baby = _baby_journey_participants().select(*JOURNEY_PARTICIPANT_SOURCE_COLUMNS)
    return mother.unionByName(baby)


# OGR_MATERNITY_SILVER_V1: pregnancy history (gravida/parity/previous outcomes) and birth grain
# (weight/Apgar/gestation/delivery) from Millennium maternity; no MRN, NHS number or DOB carried.
# Values are typed but kept as recorded: plausibility is decided downstream (OMOP exclusions), not here.
def _as_int(col):
    return F.expr(f"try_cast({col} AS int)")


def _birth_canonical():
    b = read_source(SRC_MAT_BIRTH)
    source_row_id = F.col("BirthRow_ID").cast("string")
    mother_key, mother_sys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("MotherPerson_ID"))], SRC_MAT_BIRTH, source_row_id)
    baby_key, _ = subject_key_with_system(
        [("urn:cerner:person_id", F.col("BabyPerson_ID")),
         ("https://fhir.nhs.uk/Id/nhs-number", F.col("Baby_NHS")),
         ("urn:barts:mrn", F.col("Baby_MRN"))],
        SRC_MAT_BIRTH, source_row_id)
    deleted = F.coalesce(F.col("PregnancySource_DELETE_IND").cast("long"), F.lit(0)) != 0
    key = stable_id("birth:mill", F.col("BirthRow_ID"))

    def coded(name):
        return (F.col(f"{name}_CD").cast("bigint").cast("string"), F.col(f"{name}_DESC"))

    method, outcome, neonatal, preg, pres = (coded(n) for n in
                                             ("DeliveryMethod", "DeliveryOutcome", "NeonatalOutcome", "PregOutcome", "PresDel"))
    return b.select(
        key.alias("birth_key"), key.alias("patient_event_key"),
        source_row_id.alias("birth_row_id"),
        stable_id("journey:pregnancy", F.col("Pregnancy_ID").cast("string")).alias("journey_pregnancy_key"),
        F.col("Pregnancy_ID").cast("string").alias("pregnancy_id"),
        mother_key.alias("subject_key"), mother_sys.alias("subject_id_system"),
        F.col("MotherPerson_ID").cast("bigint").alias("person_id"),
        baby_key.alias("baby_subject_key"),
        F.col("BabyPerson_ID").cast("bigint").alias("baby_person_id"),
        F.when(F.col("BabyPerson_ID").isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
        .alias("baby_identity_status"),
        F.col("BirthDateTime").cast("timestamp").alias("birth_datetime"),
        # MATDQ_SILVER_V1: bronze nulls the year-2100 placeholder and flags it.
        F.coalesce(F.col("BirthDateTimePlaceholder_IND").cast("boolean"), F.lit(False)).alias("birth_datetime_placeholder_ind"),
        F.expr("try_cast(BirthOrder AS int)").alias("birth_order"),
        F.expr("try_cast(FetusNumber AS int)").alias("fetus_number"),
        method[0].alias("delivery_method_code"), method[1].alias("delivery_method_display"),
        outcome[0].alias("delivery_outcome_code"), outcome[1].alias("delivery_outcome_display"),
        neonatal[0].alias("neonatal_outcome_code"), neonatal[1].alias("neonatal_outcome_display"),
        preg[0].alias("pregnancy_outcome_code"), preg[1].alias("pregnancy_outcome_display"),
        pres[0].alias("presentation_code"), pres[1].alias("presentation_display"),
        _as_int("GestationWeeks").alias("gestation_weeks"),
        _as_int("GestationDays").alias("gestation_days"),
        F.col("GestationRaw").cast("string").alias("gestation_source_value"),
        F.coalesce(F.col("GestationImplausible_IND").cast("boolean"), F.lit(False)).alias("gestation_implausible_ind"),
        F.col("BirthWeightGrams").cast("double").alias("birth_weight_grams"),
        F.coalesce(F.col("BirthWeightImplausible_IND").cast("boolean"), F.lit(False)).alias("birth_weight_implausible_ind"),
        F.col("BirthWeightSource").cast("string").alias("birth_weight_source"),
        _as_int("APGAR1Min").alias("apgar_1_minute"),
        _as_int("APGAR5Min").alias("apgar_5_minute"),
        _as_int("APGAR10Min").alias("apgar_10_minute"),
        F.col("BirthSex").cast("string").alias("birth_sex"),
        F.when(deleted, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        F.lit("mat-birth").alias("construction_rule"),
        F.lit("0.2.0").alias("construction_version"),
        F.lit("millennium-maternity").alias("source_system"),
        F.lit(SRC_MAT_BIRTH).alias("source_table"), source_row_id.alias("source_row_id"),
        F.date_format(F.col("ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("BirthSourceRecordUpdatedDateTime").alias("source_update_timestamp"),
        F.col("ADC_UPDT").alias("loaded_at"),
    )
