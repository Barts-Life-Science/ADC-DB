# Databricks notebook source
# omop_model_pipeline_default_target — parameter-free Journey-fed OMOP model.
# Fail closed on unknown sources, publish no direct patient identifiers, and count every rejection.

from datetime import date

import pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

try:
    from pyspark import pipelines as dp
except ImportError:  # classic DLT fallback does not accept refresh_policy
    import dlt

    class _DpShim:
        @staticmethod
        def materialized_view(**kwargs):
            kwargs.pop("refresh_policy", None)
            if kwargs.pop("private", False):
                kwargs["temporary"] = True
            return dlt.table(**kwargs)

        @staticmethod
        def temporary_view(**kwargs):
            return dlt.view(**kwargs)

    dp = _DpShim()


# Parameter-free standalone wiring.
# All patient/clinical inputs are the promoted Journey publications in 4_prod.silver.
# Vocabulary, governed crosswalks, and the append-only ID registry remain authoritative
# reference dependencies; embedding or replacing them would change OMOP semantics.
PIPELINE_VERSION = "0.8.0-ogr-2026-10-01"  # VDB_OMOP_LOCAL_V1  # OGR_METADATA_V1
STUDY_START = "2012-01-01"
# Research-cohort floor: no longer a person gate. Published via omop_person_research_flags
# for the gold layer to apply (HERON-UK: partners may include all persons).
MIN_BIRTH_YEAR = 1910
# HERON-UK observation-period censoring age (no enrolment dates at source).
CENSOR_AGE_YEARS = 120
# HERON-UK IMD convention: observation_concept_id 0, source concept "IMD quintile", unit "quintile".
IMD_SOURCE_CONCEPT_ID = 35812882
IMD_UNIT_CONCEPT_ID = 37524288
# Cerner address types HOME (756) and PREVIOUS (758); business, mailing and temporary addresses
# do not describe where the person lives.
IMD_ADDRESS_TYPES = ("756", "758")
# Pinned on 2026-09-24 to max(to_date(4_prod.silver.spine_person.loaded_at)).
SOURCE_RELEASE_DATE = "2026-09-21"
CARRY_CAUSE_TEXT = True
CARRY_STAFF_IDENTIFIERS = True

SOURCE_TABLES = {
    "src_person": "4_prod.silver.spine_person",
    "src_person_address": "4_prod.silver.reference_person_address",
    "src_death_evidence": "4_prod.silver.reference_person_death_evidence",
    "src_location": "4_prod.silver.reference_location",
    "src_practitioner": "4_prod.silver.reference_practitioner",
    "src_encounter": "4_prod.silver.spine_encounter",
    "src_encounter_bounds": "4_prod.silver.reference_encounter_bounds",
    "src_location_stay": "4_prod.silver.spine_location_stay",
    "src_critical_care_period": "4_prod.silver.clinical_critical_care_period",
    "src_person_relationship": "4_prod.silver.spine_person_relationship",
    "src_patient_event": "4_prod.silver.events_patient_event",
    "src_condition": "4_prod.silver.clinical_condition",
    "src_procedure": "4_prod.silver.clinical_procedure",
    "src_clinical_finding": "4_prod.silver.clinical_clinical_finding",
    "src_clinical_score": "4_prod.silver.clinical_clinical_score",
    "src_family_history": "4_prod.silver.clinical_family_history",
    "src_device": "4_prod.silver.clinical_device",
    "src_community_care_activity": "4_prod.silver.clinical_community_care_activity",
    "src_transfusion": "4_prod.silver.clinical_transfusion",
    "src_pathology_result": "4_prod.silver.clinical_pathology_result",
    "src_pathology_order": "4_prod.silver.clinical_pathology_order",
    "src_vital_sign": "4_prod.silver.clinical_vital_sign",
    "src_specimen": "4_prod.silver.clinical_specimen",
    "src_genomic_test": "4_prod.silver.clinical_genomic_test",
    "src_genomic_result": "4_prod.silver.clinical_genomic_result",
    "src_gene_tested": "4_prod.silver.reference_gene_tested",
    "src_medication_admin": "4_prod.silver.clinical_medication_admin",
    "src_medication_dispense": "4_prod.silver.clinical_medication_dispense",
    "src_drug_expenditure": "4_prod.silver.clinical_drug_expenditure",
    "src_cancer_treatment": "4_prod.silver.clinical_cancer_treatment",
    "src_vocab_concept": "3_lookup.omop.concept",
    "src_vocabulary": "3_lookup.omop.vocabulary",
    "src_vocab_concept_relationship": "3_lookup.omop.concept_relationship",
    "src_vocab_concept_ancestor": "3_lookup.omop.concept_ancestor",
    "src_vocab_drug_strength": "3_lookup.omop.drug_strength",
    "src_id_registry": "4_prod.omop_meta.omop_id_registry",
    "lkp_type_concept": "3_lookup.omop.omop_type_concept_map",
    "lkp_gender": "3_lookup.omop.omop_gender_map",
    "lkp_race": "3_lookup.omop.omop_race_map",
    "lkp_position_specialty": "3_lookup.omop.omop_position_specialty_map",
    "lkp_visit_map": "3_lookup.omop.omop_visit_map",
    "lkp_admit_discharge_map": "3_lookup.omop.omop_admit_discharge_map",
    "lkp_coding_system_priority": "3_lookup.omop.omop_coding_system_priority",
    "lkp_condition_status": "3_lookup.omop.omop_condition_status_map",
    "lkp_route_map": "3_lookup.omop.omop_route_map",
    "lkp_dose_unit_map": "3_lookup.omop.omop_dose_unit_map",
    # OGR_EXT_V1: direct-adapter, maternity, provider and stage sources.
    "src_allergy": "4_prod.silver.clinical_allergy_intolerance",
    "src_medication_supply": "4_prod.silver.clinical_medication_supply",
    "src_journey": "4_prod.silver.spine_journey",
    "src_pregnancy_history": "4_prod.silver.clinical_pregnancy_history",
    "src_birth": "4_prod.silver.clinical_birth",
    "src_vte_form": "4_prod.silver.clinical_form_maternity_vte_risk_assessment",
    "src_care_participation": "4_prod.silver.spine_care_participation",
    "src_condition_stage": "4_prod.silver.clinical_condition_stage",
    "src_vocab_concept_synonym": "3_lookup.omop.concept_synonym",
    "lkp_maternity_map": "3_lookup.omop.omop_maternity_concept_map",
    "lkp_vte_item_map": "3_lookup.omop.omop_maternity_vte_item_map",
    "lkp_stage_map": "3_lookup.omop.omop_stage_group_map",
}

# Unqualified reads bind to datasets in this pipeline and therefore follow the
# pipeline's configured default catalog/schema.
PIPELINE_DATASETS = {
    "src_routed_events": "_om_routed_events",
    "src_om_visit_detail": "visit_detail",
    "src_om_specimen": "specimen",
    "src_om_observation_period": "observation_period",
}

# Dev sources promoted 2026-09-26: lookups to 3_lookup.omop, the ID registry to 4_prod.omop_meta.
# 8_dev is no longer allowlisted, so any residual dev read fails closed.
# OMOP_SOURCES_OFF_8DEV_PATCH_APPLIED v1
ALLOWED_SOURCE_SCHEMAS = {
    "4_prod.silver", "3_lookup.omop", "4_prod.omop_meta",
}


def validate_source_table(name):
    clean = name.replace("`", "")
    parts = clean.split(".")
    if len(parts) != 3 or ".".join(parts[:2]).lower() not in ALLOWED_SOURCE_SCHEMAS:
        raise ValueError(f"source outside OMOP allowlist rejected: {name!r}")
    return name


# R0 Silver contract compatibility. Silver renamed its surrogate *_id columns to
# *_key and dropped identity_status. Key values are unchanged (sample-verified
# 2026-09-24), so registry recipes evt:/ward:/spm:/gt:/ggt: stay byte-identical.
# An alias applies only when the legacy column is absent and the new one present.
SOURCE_COLUMN_COMPAT = {
    "patient_event_row_id": "patient_event_row_key",
    "patient_event_id": "patient_event_key",
    "location_id": "location_key",
    "parent_location_id": "parent_location_key",
    "primary_location_id": "primary_location_key",
    "current_location_id": "current_location_key",
    "location_stay_id": "location_stay_key",
    "specimen_id": "specimen_key",
    "gene_tested_row_id": "gene_tested_row_key",
    "genomic_test_id": "genomic_test_key",
}
# clinical_genomic_result lost fact_row_id; its row key is patient_event_key.
# The gv: registry space held no allocations when this alias was introduced.
SOURCE_COLUMN_COMPAT_BY_KEY = {
    "src_genomic_result": {"fact_row_id": "patient_event_key"},
}
PERSON_SUBJECT_SYSTEM = "urn:cerner:person_id"


def _apply_source_compat(key, frame):
    columns = set(frame.columns)
    aliases = dict(SOURCE_COLUMN_COMPAT)
    aliases.update(SOURCE_COLUMN_COMPAT_BY_KEY.get(key, {}))
    for legacy, current in aliases.items():
        if legacy not in columns and current in columns:
            frame = frame.withColumn(legacy, F.col(current))
            columns.add(legacy)
    if (
        "identity_status" not in columns
        and "subject_id_system" in columns
        and "person_id" in columns
    ):
        # Journey identity: resolved iff the subject is a person-keyed Cerner subject.
        frame = frame.withColumn(
            "identity_status",
            F.when(
                (F.col("subject_id_system") == PERSON_SUBJECT_SYSTEM)
                & F.col("person_id").isNotNull(),
                F.lit("resolved"),
            ).otherwise(F.lit("unresolved")),
        )
    return frame


def read_source(key):
    if key in PIPELINE_DATASETS:
        return spark.read.table(PIPELINE_DATASETS[key])
    try:
        name = SOURCE_TABLES[key]
    except KeyError as error:
        raise KeyError(f"unknown OMOP source key: {key!r}") from error
    validate_source_table(name)
    return _apply_source_compat(key, spark.read.table(name))


# OGR_EXT_V1: extension registry. Adapters register row builders (unioned into a
# published table), transforms (applied to the whole published frame), observation-period
# evidence, exclusion metrics and metadata rows. Every builder reads Silver or lookups only.
TABLE_EXTENSIONS = {}
TABLE_TRANSFORMS = {}
EVIDENCE_BUILDERS = []
METRIC_BUILDERS = []
METADATA_ROWS = []


def _register_extension(table, builder):
    TABLE_EXTENSIONS.setdefault(table, []).append(builder)


def _register_transform(table, fn):
    TABLE_TRANSFORMS.setdefault(table, []).append(fn)


def _finish(table, frame, transforms=True, **kw):
    for builder in TABLE_EXTENSIONS.get(table, []):
        frame = frame.unionByName(builder(**kw))
    if transforms:
        for fn in TABLE_TRANSFORMS.get(table, []):
            frame = fn(frame)
    return frame


def _evidence_extensions():
    empty = spark.createDataFrame([], "person_id bigint, _evidence_start timestamp")
    for builder in EVIDENCE_BUILDERS:
        empty = empty.unionByName(builder().select(
            F.col("person_id").cast("bigint").alias("person_id"),
            F.col("_evidence_start").cast("timestamp").alias("_evidence_start"),
        ))
    return empty


def _extension_condition_occurrences():
    """Extension CONDITION_OCCURRENCE rows in the condition-era input shape."""
    empty = spark.createDataFrame([], "person_id bigint, condition_concept_id int, _start date, _end date")
    for builder in TABLE_EXTENSIONS.get("condition_occurrence", []):
        empty = empty.unionByName(builder().select(
            "person_id", "condition_concept_id",
            F.col("condition_start_date").alias("_start"),
            F.coalesce("condition_end_date", "condition_start_date").alias("_end"),
        ))
    return empty





# COMMAND ----------

# O1 foundations: person.

def _latest_location_rows():
    source = read_source("src_location")
    fields = source.columns
    payload = F.struct(
        F.coalesce(F.col("loaded_at"), F.lit("1900-01-01").cast("timestamp")).alias("_sort_loaded"),
        F.when(F.col("status") == "active", F.lit(1)).otherwise(F.lit(0)).alias("_sort_active"),
        *[F.col(column).alias(column) for column in fields],
    )
    return (
        source.groupBy("source_location_code")
        .agg(F.max(payload).alias("_latest"))
        .select(*[F.col(f"_latest.{column}").alias(column) for column in fields])
    )


def _precision_at_least(level, display_column):
    explicit = F.upper(F.coalesce(display_column, F.lit("")))
    if level == "month":
        return explicit != F.lit("YYYY")
    if level == "day":
        return ~explicit.isin("YYYY", "YYYYMM")
    raise ValueError(f"unsupported precision level: {level}")


def _nhs_ethnic_category():
    # HERON-UK ethnicity: race_source_concept_id is the NHS Ethnic Category concept for the
    # recorded letter; race_concept_id is its Maps-to Race target, taken only when unambiguous.
    letters = read_source("lkp_race").select(
        F.col("ethnicity_code").alias("_ethnicity_code"),
        F.upper(F.trim(F.col("nhs_letter"))).alias("_nhs_letter"),
    )
    category = (
        read_source("src_vocab_concept")
        .where(
            (F.col("vocabulary_id") == "NHS Ethnic Category")
            & F.col("invalid_reason").isNull()
        )
        .groupBy(F.col("concept_code").alias("_nhs_letter"))
        .agg(F.min("concept_id").cast("int").alias("_race_source_concept_id"))
    )
    standard_race = (
        read_source("src_vocab_concept")
        .where(
            (F.col("domain_id") == "Race")
            & (F.col("standard_concept") == "S")
            & F.col("invalid_reason").isNull()
        )
        .select(F.col("concept_id").alias("concept_id_2"))
    )
    maps_to = (
        read_source("src_vocab_concept_relationship")
        .where((F.col("relationship_id") == "Maps to") & F.col("invalid_reason").isNull())
        .join(standard_race, "concept_id_2")
        .groupBy(F.col("concept_id_1").cast("int").alias("_race_source_concept_id"))
        .agg(
            F.countDistinct("concept_id_2").alias("_target_count"),
            F.min("concept_id_2").cast("int").alias("_target"),
        )
        .select(
            "_race_source_concept_id",
            F.when(F.col("_target_count") == 1, F.col("_target")).alias("_race_concept_id"),
        )
    )
    return (
        letters.join(category, "_nhs_letter")
        .join(maps_to, "_race_source_concept_id", "left")
        .select("_ethnicity_code", "_race_source_concept_id", "_race_concept_id")
    )


def _person_base():
    # Every Silver person publishes (HERON-UK: partners may include all persons). The former
    # O1 cohort gates are carried as _research_exclusion_reason for the gold layer; only a
    # person that cannot be keyed is excluded here.
    person_source = read_source("src_person").alias("p")
    gender = read_source("lkp_gender").select(
        F.col("gender_code").alias("_gender_code"),
        F.col("gender_concept_id").alias("_gender_concept_id"),
    )
    joined = (
        person_source
        .join(gender, F.col("p.gender_code") == F.col("_gender_code"), "left")
        .join(_nhs_ethnic_category(), F.col("p.ethnicity_code") == F.col("_ethnicity_code"), "left")
        .join(_person_lsoa_location(), F.expr("try_cast(p.person_id AS bigint)") == F.col("_pl_person_id"), "left")  # OGR_LSOA_V1
        .join(_person_gp_provider(), F.expr("try_cast(p.person_id AS bigint)") == F.col("_gp_person_id"), "left")  # OGR_PROVIDER_V1
    )
    gender_concept = F.coalesce(F.col("_gender_concept_id"), F.lit(0)).cast("int")
    person_key = F.expr("try_cast(p.person_id AS bigint)")
    return joined.select(
        person_key.alias("person_id"),
        gender_concept.alias("gender_concept_id"),
        F.year("p.birth_date").cast("int").alias("year_of_birth"),
        F.when(
            _precision_at_least("month", F.col("p.birth_precision_display")),
            F.month("p.birth_date"),
        ).cast("int").alias("month_of_birth"),
        F.when(
            _precision_at_least("day", F.col("p.birth_precision_display")),
            F.dayofmonth("p.birth_date"),
        ).cast("int").alias("day_of_birth"),
        F.col("p.birth_datetime").cast("timestamp").alias("birth_datetime"),
        F.coalesce(F.col("_race_concept_id"), F.lit(0)).cast("int").alias("race_concept_id"),
        F.lit(0).cast("int").alias("ethnicity_concept_id"),
        F.col("_pl_location_id").alias("location_id"),  # OGR_LSOA_V1
        F.col("_gp_provider_id").cast("bigint").alias("provider_id"),  # OGR_PROVIDER_V1
        F.lit(None).cast("bigint").alias("care_site_id"),
        F.col("p.person_id").cast("string").alias("person_source_value"),
        F.col("p.gender_code").cast("string").alias("gender_source_value"),
        F.lit(0).cast("int").alias("gender_source_concept_id"),
        F.col("p.ethnicity_code").cast("string").alias("race_source_value"),
        F.coalesce(F.col("_race_source_concept_id"), F.lit(0)).cast("int").alias("race_source_concept_id"),
        F.col("p.ethnicity_code").cast("string").alias("ethnicity_source_value"),
        F.lit(0).cast("int").alias("ethnicity_source_concept_id"),
        F.col("p.record_status").cast("string").alias("_record_status"),
        F.col("p.birth_date").cast("date").alias("_birth_date"),
        F.when(F.col("p.record_status") != "active", F.lit("record_status_superseded"))
        .when(F.col("p.birth_date").isNull(), F.lit("missing_birth_date"))
        .when(F.year("p.birth_date") < F.lit(MIN_BIRTH_YEAR), F.lit("birth_year_below_floor"))
        .when(~gender_concept.isin(8507, 8532), F.lit("invalid_gender"))
        .alias("_research_exclusion_reason"),
        F.when(person_key.isNull(), F.lit("person_id_not_castable")).alias("_exclusion_reason"),
    )


# === GENERATED: explicit schema for person (contract_codegen.py) — do not edit ===
person_schema = T.StructType([
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Millennium PERSON_ID carried as a natural key.'}),
    T.StructField('gender_concept_id', T.IntegerType(), False, metadata={'comment': 'Standard gender concept via governed gender map.'}),
    T.StructField('year_of_birth', T.IntegerType(), True, metadata={'comment': 'Year from Journey birth date; NULL when Silver has no birth date (all persons publish; see omop_person_research_flags).'}),
    T.StructField('month_of_birth', T.IntegerType(), True, metadata={'comment': 'Month from Journey birth date where source precision permits.'}),
    T.StructField('day_of_birth', T.IntegerType(), True, metadata={'comment': 'Day from Journey birth date where source precision permits; no day-15 imputation.'}),
    T.StructField('birth_datetime', T.TimestampType(), True, metadata={'comment': 'Full birth datetime retained by approved D4 ruling.'}),
    T.StructField('race_concept_id', T.IntegerType(), False, metadata={'comment': 'Standard Race concept via the NHS Ethnic Category Maps-to relationship (HERON-UK); 0 where the source concept is missing or its mapping is absent or ambiguous.'}),
    T.StructField('ethnicity_concept_id', T.IntegerType(), False, metadata={'comment': 'Recorded convention is 0 because the NHS source does not carry the Hispanic axis.'}),
    T.StructField('location_id', T.LongType(), True, metadata={'comment': 'LSOA LOCATION of the current address (natural key); NULL when the address has no E01/W01 LSOA.'}),
    T.StructField('provider_id', T.LongType(), True, metadata={'comment': 'Current registered GP (PERSON_PRSNL_RELTN code 1115, active and in effect at refresh; latest start, then lowest id) resolved to a published provider; NULL otherwise.'}),
    T.StructField('care_site_id', T.LongType(), True, metadata={'comment': 'NULL in O1.'}),
    T.StructField('person_source_value', T.StringType(), True, metadata={'comment': 'Millennium PERSON_ID as string; never MRN or NHS number.'}),
    T.StructField('gender_source_value', T.StringType(), True, metadata={'comment': 'Cerner gender code.'}),
    T.StructField('gender_source_concept_id', T.IntegerType(), True, metadata={'comment': '0 because there is no source vocabulary concept.'}),
    T.StructField('race_source_value', T.StringType(), True, metadata={'comment': 'Cerner ethnicity code used by the race crosswalk.'}),
    T.StructField('race_source_concept_id', T.IntegerType(), True, metadata={'comment': 'NHS Ethnic Category concept for the governed Cerner code to NHS letter crosswalk (HERON-UK); 0 where the code has no NHS letter.'}),
    T.StructField('ethnicity_source_value', T.StringType(), True, metadata={'comment': 'Cerner ethnicity code retained as source value.'}),
    T.StructField('ethnicity_source_concept_id', T.IntegerType(), True, metadata={'comment': '0 because there is no source vocabulary concept.'}),
])
# === END GENERATED: explicit schema for person ===

@dp.materialized_view(
    name="person",
    schema=person_schema,
    comment="OMOP PERSON from Journey demographics; every keyed Silver person publishes (HERON-UK), research gates are in omop_person_research_flags.",
)
def person():
    return (
        _person_base()
        .where(F.col("_exclusion_reason").isNull())
        .drop("_exclusion_reason", "_research_exclusion_reason", "_record_status", "_birth_date")
    )


omop_person_research_flags_schema = T.StructType([
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key; one row per published person.'}),
    T.StructField('record_status', T.StringType(), True, metadata={'comment': 'Silver spine_person record_status; superseded rows are persons combined into another PERSON_ID.'}),
    T.StructField('research_exclusion_reason', T.StringType(), True, metadata={'comment': 'First failing former O1 gate (record_status_superseded, missing_birth_date, birth_year_below_floor, invalid_gender); NULL when the person passes every gate. Applied in gold, not here.'}),
])


@dp.materialized_view(
    name="omop_person_research_flags",
    schema=omop_person_research_flags_schema,
    comment="Per-person research-cohort flags for the gold OMOP layer; the silver OMOP publishes every person.",
)
def omop_person_research_flags():
    return (
        _person_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(
            "person_id",
            F.col("_record_status").alias("record_status"),
            F.col("_research_exclusion_reason").alias("research_exclusion_reason"),
        )
    )


# COMMAND ----------

# O1 foundations: death.

def _cause_norm(column):
    return F.lower(F.trim(F.regexp_replace(column.cast("string"), r"[\s]+", " ")))


def _death_cause_map():
    """Unique exact name/synonym match of death-cause text to a standard SNOMED Condition. OGR_DEATH_CAUSE_V1"""
    concepts = read_source("src_vocab_concept").where(
        (F.col("vocabulary_id") == "SNOMED") & (F.col("domain_id") == "Condition")
        & (F.col("standard_concept") == "S") & F.col("invalid_reason").isNull()
    ).select("concept_id", "concept_name")
    names = concepts.select(_cause_norm(F.col("concept_name")).alias("_dc_text"), "concept_id",
                            F.lit("exact_name").alias("_dc_method"), F.lit(1).alias("_dc_rank"))
    synonyms = read_source("src_vocab_concept_synonym").join(concepts.select("concept_id"), "concept_id").select(
        _cause_norm(F.col("concept_synonym_name")).alias("_dc_text"), "concept_id",
        F.lit("exact_synonym").alias("_dc_method"), F.lit(2).alias("_dc_rank"))
    both = names.unionByName(synonyms)
    best_rank = both.groupBy("_dc_text").agg(F.min("_dc_rank").alias("_dc_best"))
    winners = both.join(best_rank, "_dc_text").where(F.col("_dc_rank") == F.col("_dc_best"))
    unique = winners.groupBy("_dc_text").agg(
        F.countDistinct("concept_id").alias("_dc_n"), F.min("concept_id").alias("_dc_concept_id"),
        F.min("_dc_method").alias("_dc_method"))
    return unique.where(F.col("_dc_n") == 1).select("_dc_text", "_dc_concept_id", "_dc_method")


def _death_base():
    people = read_source("src_person").select(
        F.col("person_id").alias("_person_source_id"),
        "deceased_ind",
        F.col("deceased_datetime").alias("_person_deceased_datetime"),
    )
    evidence = read_source("src_death_evidence").select(
        F.col("person_id").alias("_evidence_person_id"),
        F.col("deceased_datetime").alias("_evidence_deceased_datetime"),
        F.col("calculated_death_date").alias("_calculated_death_date"),
        F.col("cause_of_death").alias("_cause_of_death"),
    )
    candidates = (
        people.join(
            evidence,
            F.col("_person_source_id") == F.col("_evidence_person_id"),
            "full",
        )
        .where(F.col("_evidence_person_id").isNotNull() | F.coalesce(F.col("deceased_ind"), F.lit(False)))
        .withColumn(
            "_origin",
            F.when(F.col("_evidence_person_id").isNull(), F.lit("person_only"))
            .when(F.col("_person_source_id").isNull(), F.lit("evidence"))
            .otherwise(F.lit("both")),
        )
        .withColumn("person_id", F.expr("try_cast(coalesce(_evidence_person_id, _person_source_id) AS bigint)"))
    )
    published_people = (
        _person_base()
        .where(F.col("_exclusion_reason").isNull())
        .select("person_id")
        .withColumn("_person_published", F.lit(True))
    )
    joined = candidates.join(published_people, "person_id", "left").join(  # OGR_DEATH_CAUSE_V1
        _death_cause_map(), _cause_norm(F.col("_cause_of_death")) == F.col("_dc_text"), "left")
    raw_date_value = F.coalesce(
        F.col("_evidence_deceased_datetime"),
        F.col("_calculated_death_date"),
        F.col("_person_deceased_datetime"),
    )
    raw_datetime_value = F.coalesce(
        F.col("_evidence_deceased_datetime"),
        F.col("_person_deceased_datetime"),
    )
    release_date = F.to_date(F.lit(SOURCE_RELEASE_DATE))
    future_date = F.to_date(raw_date_value) > release_date
    return joined.select(
        "person_id",
        F.when(future_date, release_date).otherwise(F.to_date(raw_date_value)).alias("death_date"),
        F.when(
            F.to_date(raw_datetime_value) > release_date,
            F.to_timestamp(F.lit(SOURCE_RELEASE_DATE)),
        ).otherwise(raw_datetime_value.cast("timestamp")).alias("death_datetime"),
        F.lit(32817).cast("int").alias("death_type_concept_id"),
        F.coalesce(F.col("_dc_concept_id"), F.lit(0)).cast("int").alias("cause_concept_id"),  # OGR_DEATH_CAUSE_V1
        (
            F.substring(F.col("_cause_of_death"), 1, 50)
            if CARRY_CAUSE_TEXT else F.lit(None).cast("string")
        ).alias("cause_source_value"),
        F.coalesce(F.col("_dc_concept_id"), F.lit(0)).cast("int").alias("cause_source_concept_id"),
        F.col("_dc_method").alias("_cause_map_method"),
        F.col("_origin"),
        future_date.alias("_future_death_clamped"),
        F.when(raw_date_value.isNull(), F.lit("missing_death_date"))
        .when(~F.coalesce(F.col("_person_published"), F.lit(False)), F.lit("person_not_published"))
        .alias("_exclusion_reason"),
    )


# === GENERATED: explicit schema for death (contract_codegen.py) — do not edit ===
death_schema = T.StructType([
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key and DEATH primary key.'}),
    T.StructField('death_date', T.DateType(), False, metadata={'comment': 'Best available death date from evidence then person.'}),
    T.StructField('death_datetime', T.TimestampType(), True, metadata={'comment': 'Best available death timestamp consistent with death_date.'}),
    T.StructField('death_type_concept_id', T.IntegerType(), True, metadata={'comment': 'Verified EHR type concept 32817.'}),
    T.StructField('cause_concept_id', T.IntegerType(), True, metadata={'comment': 'Standard SNOMED Condition matched uniquely by exact concept name, then synonym, on normalised cause text; 0 when unmatched or ambiguous.'}),
    T.StructField('cause_source_value', T.StringType(), True, metadata={'comment': 'Death cause text truncated to 50 characters and governed by omop.carry_cause_text.'}),
    T.StructField('cause_source_concept_id', T.IntegerType(), True, metadata={'comment': 'Same concept as cause_concept_id (the text is the source); 0 when unmatched.'}),
])
# === END GENERATED: explicit schema for death ===

@dp.materialized_view(
    name="death",
    schema=death_schema,
    comment="OMOP DEATH reconciled from person and person-keyed death evidence.",
)
def death():
    return (
        _death_base()
        .where(F.col("_exclusion_reason").isNull())
        .drop("_origin", "_future_death_clamped", "_exclusion_reason", "_cause_map_method")
    )


# COMMAND ----------

# O1 foundations: location, care site, and provider.

def _lsoa_expr(column):
    """Natural-key LOCATION id for an LSOA code; NULL for anything but E01/W01 + 6 digits. OGR_LSOA_V1"""
    code = F.upper(F.trim(column))
    rank = F.when(code.startswith("E01"), F.lit(1)).when(code.startswith("W01"), F.lit(2))
    digits = F.when(code.rlike(r"^(E01|W01)[0-9]{6}$"), F.substring(code, 4, 6).cast("bigint"))
    return (F.lit(8000000000000000).cast("bigint") + rank.cast("bigint") * F.lit(1000000000) + digits)


def _lsoa_location_base():
    addresses = read_source("src_person_address").where(F.col("lsoa").isNotNull())
    ranked = addresses.groupBy("lsoa", "local_authority_code").agg(F.count(F.lit(1)).alias("_n"))
    modal = Window.partitionBy("lsoa").orderBy(
        F.col("_n").desc(), F.col("local_authority_code").asc_nulls_last())
    la = ranked.withColumn("_r", F.row_number().over(modal)).where(F.col("_r") == 1).select(
        "lsoa", F.col("local_authority_code").alias("_la"))
    return la.select(
        _lsoa_expr(F.col("lsoa")).alias("location_id"),
        F.lit(None).cast("string").alias("address_1"),
        F.lit(None).cast("string").alias("address_2"),
        F.lit(None).cast("string").alias("city"),
        F.lit(None).cast("string").alias("state"),
        F.lit(None).cast("string").alias("zip"),
        F.col("_la").cast("string").alias("county"),
        F.upper(F.trim(F.col("lsoa"))).cast("string").alias("location_source_value"),
        F.lit(42035286).cast("int").alias("country_concept_id"),
        F.lit("GB").cast("string").alias("country_source_value"),
        F.lit(None).cast("double").alias("latitude"),
        F.lit(None).cast("double").alias("longitude"),
        F.when(_lsoa_expr(F.col("lsoa")).isNull(), F.lit("lsoa_unparseable")).alias("_exclusion_reason"),
    )


def _person_lsoa_location():
    address = read_source("src_person_address").select(
        F.col("address_id").alias("_pl_address_id"), _lsoa_expr(F.col("lsoa")).alias("_pl_location_id"),
    )
    person = read_source("src_person").select(
        F.expr("try_cast(person_id AS bigint)").alias("_pl_person_id"),
        F.col("current_address_id").alias("_pl_current_address_id"),
    )
    return person.join(address, F.col("_pl_current_address_id") == F.col("_pl_address_id"), "inner").select(
        "_pl_person_id", "_pl_location_id",
    ).where(F.col("_pl_location_id").isNotNull()).dropDuplicates(["_pl_person_id"])


def _location_base():
    source = _latest_location_rows().where(F.col("location_level").isin("facility", "building"))
    natural_key = F.expr("try_cast(source_location_code AS bigint)")
    postcode_outward = F.upper(
        F.regexp_extract(F.coalesce(F.col("address_postcode_masked"), F.lit("")), r"^([A-Z]{1,2}[0-9][0-9A-Z]?)", 1)
    )
    return source.select(
        natural_key.alias("location_id"),
        F.lit(None).cast("string").alias("address_1"),
        F.lit(None).cast("string").alias("address_2"),
        F.col("address_city").cast("string").alias("city"),
        F.lit(None).cast("string").alias("state"),
        F.when(F.length(postcode_outward) > 0, postcode_outward).alias("zip"),
        F.lit(None).cast("string").alias("county"),
        F.col("source_location_code").cast("string").alias("location_source_value"),
        F.lit(42035286).cast("int").alias("country_concept_id"),  # OGR_LSOA_V1
        F.lit("GB").cast("string").alias("country_source_value"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.when(natural_key.isNull(), F.lit("unparseable_natural_key")).alias("_exclusion_reason"),
    )


# === GENERATED: explicit schema for location (contract_codegen.py) — do not edit ===
location_schema = T.StructType([
    T.StructField('location_id', T.LongType(), False, metadata={'comment': 'Cerner source_location_code natural key for facilities; 8e15 + rank*1e9 + LSOA digits for LSOA rows (E01 rank 1, W01 rank 2).'}),
    T.StructField('address_1', T.StringType(), True, metadata={'comment': 'Always NULL; street address is outside the approved surface.'}),
    T.StructField('address_2', T.StringType(), True, metadata={'comment': 'Always NULL; street address is outside the approved surface.'}),
    T.StructField('city', T.StringType(), True, metadata={'comment': 'Facility or building city supplied by Journey.'}),
    T.StructField('state', T.StringType(), True, metadata={'comment': 'NULL in O1.'}),
    T.StructField('zip', T.StringType(), True, metadata={'comment': 'Outward postcode only, derived from the Journey location postcode; PID scan rejects full postcode shapes.'}),
    T.StructField('county', T.StringType(), True, metadata={'comment': 'Local authority code (modal across address rows) for LSOA rows; NULL for facilities.'}),
    T.StructField('location_source_value', T.StringType(), True, metadata={'comment': 'Cerner source_location_code as string.'}),
    T.StructField('country_concept_id', T.IntegerType(), True, metadata={'comment': '42035286 United Kingdom.'}),
    T.StructField('country_source_value', T.StringType(), True, metadata={'comment': 'GB source code.'}),
    T.StructField('latitude', T.DoubleType(), True, metadata={'comment': 'Facility or building latitude where supplied.'}),
    T.StructField('longitude', T.DoubleType(), True, metadata={'comment': 'Facility or building longitude where supplied.'}),
])
# === END GENERATED: explicit schema for location ===

@dp.materialized_view(
    name="location",
    # Lakeflow CURRENT rejects this dataset's explicit StructType metadata
    # during the Delta materialization write, although the inferred data types
    # are identical.  Keep location_schema above as the documented contract,
    # but let Lakeflow infer this one physical schema.
    comment="OMOP LOCATION: Journey facility/building locations plus one PID-free row per LSOA (natural key 8e15 + rank*1e9 + digits); no street addresses or postcodes.",
)
def location():
    facility = _location_base().where(F.col("_exclusion_reason").isNull()).drop("_exclusion_reason")
    lsoa = _lsoa_location_base().where(F.col("_exclusion_reason").isNull()).drop("_exclusion_reason")
    # OGR_LSOA_V1: facility natural keys must stay below the LSOA range.
    clash = facility.where(F.col("location_id") >= F.lit(8000000000000000)).limit(1).count()
    if clash:
        raise ValueError("facility location_id collides with the LSOA natural-key range")
    return facility.unionByName(lsoa)


def _care_site_base():
    locations = _latest_location_rows()
    nurse = locations.where(F.col("location_level") == "nurse_unit").alias("n")
    parent = locations.alias("p")
    grandparent = locations.alias("g")
    joined = (
        nurse.join(parent, F.col("n.parent_location_id") == F.col("p.location_id"), "left")
        .join(grandparent, F.col("p.parent_location_id") == F.col("g.location_id"), "left")
    )
    natural_key = F.expr("try_cast(n.source_location_code AS bigint)")
    ancestor_source_code = (
        F.when(F.col("p.location_level") == "facility", F.col("p.source_location_code"))
        .when(
            (F.col("p.location_level") == "building") & (F.col("g.location_level") == "facility"),
            F.col("g.source_location_code"),
        )
    )
    return joined.select(
        natural_key.alias("care_site_id"),
        F.col("n.name").cast("string").alias("care_site_name"),
        F.lit(0).cast("int").alias("place_of_service_concept_id"),
        F.expr("try_cast(NULL AS bigint)").alias("_placeholder_location_id"),
        F.col("n.source_location_code").cast("string").alias("care_site_source_value"),
        F.col("n.physical_type_code").cast("string").alias("place_of_service_source_value"),
        ancestor_source_code.alias("_ancestor_source_code"),
        F.when(natural_key.isNull(), F.lit("unparseable_natural_key")).alias("_exclusion_reason"),
    ).select(
        "care_site_id", "care_site_name", "place_of_service_concept_id",
        F.expr("try_cast(_ancestor_source_code AS bigint)").alias("location_id"),
        "care_site_source_value", "place_of_service_source_value", "_exclusion_reason",
    )


# === GENERATED: explicit schema for care_site (contract_codegen.py) — do not edit ===
care_site_schema = T.StructType([
    T.StructField('care_site_id', T.LongType(), False, metadata={'comment': 'Cerner source_location_code carried as a natural key.'}),
    T.StructField('care_site_name', T.StringType(), True, metadata={'comment': 'Journey nurse-unit display name.'}),
    T.StructField('place_of_service_concept_id', T.IntegerType(), True, metadata={'comment': 'Dominant published visit concept at the care site mapped 9201→8717, 9202→8756, 9203→8870; 0 otherwise.'}),
    T.StructField('location_id', T.LongType(), True, metadata={'comment': 'Nearest published facility or building ancestor.'}),
    T.StructField('care_site_source_value', T.StringType(), True, metadata={'comment': 'Cerner source_location_code as string.'}),
    T.StructField('place_of_service_source_value', T.StringType(), True, metadata={'comment': 'Journey physical_type_code.'}),
])
# === END GENERATED: explicit schema for care_site ===

VISIT_TO_PLACE_OF_SERVICE = {9201: 8717, 9202: 8756, 9203: 8870}  # OGR_CARE_SITE_POS_V1


def _care_site_pos():
    """Dominant published visit concept per care site mapped to a Visit-domain place of service."""
    visits = _visit_base().where(F.col("_exclusion_reason").isNull() & F.col("care_site_id").isNotNull())
    counts = visits.groupBy("care_site_id", "visit_concept_id").agg(F.count(F.lit(1)).alias("_n"))
    order = Window.partitionBy("care_site_id").orderBy(F.col("_n").desc(), F.col("visit_concept_id").asc())
    dominant = counts.withColumn("_r", F.row_number().over(order)).where(F.col("_r") == 1)
    mapping = F.create_map(*[F.lit(x) for kv in VISIT_TO_PLACE_OF_SERVICE.items() for x in kv])
    return dominant.select(
        F.col("care_site_id").alias("_pos_care_site_id"),
        mapping[F.col("visit_concept_id")].cast("int").alias("_pos_concept_id"),
    )


@dp.materialized_view(
    name="care_site",
    schema=care_site_schema,
    comment="OMOP CARE_SITE for Journey nurse units linked to their facility ancestor.",
)
def care_site():
    base = _care_site_base().where(F.col("_exclusion_reason").isNull()).drop("_exclusion_reason")
    columns = base.columns
    return (
        base.join(_care_site_pos(), F.col("care_site_id") == F.col("_pos_care_site_id"), "left")
        .withColumn("place_of_service_concept_id",
                    F.coalesce(F.col("_pos_concept_id"), F.lit(0)).cast("int"))
        .select(*columns)
    )  # OGR_CARE_SITE_POS_V1


def _provider_base():
    practitioner = read_source("src_practitioner").alias("p")
    specialty = read_source("lkp_position_specialty").select(
        F.col("position_code").alias("_position_code"),
        F.col("specialty_concept_id").alias("_specialty_concept_id"),
    )
    nurse = (
        _latest_location_rows()
        .where(F.col("location_level") == "nurse_unit")
        .select(
            F.col("location_id").alias("_nurse_location_id"),
            F.col("source_location_code").alias("_nurse_source_code"),
        )
    )
    joined = (
        practitioner
        .join(specialty, F.col("p.position_code") == F.col("_position_code"), "left")
        .join(nurse, F.col("p.primary_location_id") == F.col("_nurse_location_id"), "left")
    )
    natural_key = F.expr("try_cast(p.source_practitioner_id AS bigint)")
    return joined.select(
        natural_key.alias("provider_id"),
        (F.col("p.name") if CARRY_STAFF_IDENTIFIERS else F.lit(None).cast("string")).alias("provider_name"),
        (
            F.coalesce(F.col("p.npi"), F.col("p.doctor_number"))
            if CARRY_STAFF_IDENTIFIERS else F.lit(None).cast("string")
        ).alias("npi"),
        F.lit(None).cast("string").alias("dea"),
        F.coalesce(F.col("_specialty_concept_id"), F.lit(0)).cast("int").alias("specialty_concept_id"),
        F.expr("try_cast(_nurse_source_code AS bigint)").alias("care_site_id"),
        F.lit(None).cast("int").alias("year_of_birth"),
        F.lit(0).cast("int").alias("gender_concept_id"),
        F.col("p.source_practitioner_id").cast("string").alias("provider_source_value"),
        F.concat(F.lit("position:"), F.col("p.position_code").cast("string"))
        .alias("specialty_source_value"),
        F.lit(0).cast("int").alias("specialty_source_concept_id"),
        F.lit(None).cast("string").alias("gender_source_value"),
        F.lit(0).cast("int").alias("gender_source_concept_id"),
        F.when(natural_key.isNull(), F.lit("unparseable_natural_key")).alias("_exclusion_reason"),
    )


# === GENERATED: explicit schema for provider (contract_codegen.py) — do not edit ===
provider_schema = T.StructType([
    T.StructField('provider_id', T.LongType(), False, metadata={'comment': 'Journey source_practitioner_id carried as a natural key.'}),
    T.StructField('provider_name', T.StringType(), True, metadata={'comment': 'Staff name governed by omop.carry_staff_identifiers.'}),
    T.StructField('npi', T.StringType(), True, metadata={'comment': 'Staff NPI or doctor number governed by omop.carry_staff_identifiers.'}),
    T.StructField('dea', T.StringType(), True, metadata={'comment': 'NULL in O1.'}),
    T.StructField('specialty_concept_id', T.IntegerType(), True, metadata={'comment': 'Governed best-effort standard Provider concept; 0 when unmapped.'}),
    T.StructField('care_site_id', T.LongType(), True, metadata={'comment': 'Primary nurse-unit location where it resolves to a published care site.'}),
    T.StructField('year_of_birth', T.IntegerType(), True, metadata={'comment': 'NULL in O1.'}),
    T.StructField('gender_concept_id', T.IntegerType(), True, metadata={'comment': '0 in O1.'}),
    T.StructField('provider_source_value', T.StringType(), True, metadata={'comment': 'Journey source_practitioner_id as string.'}),
    T.StructField('specialty_source_value', T.StringType(), True, metadata={'comment': 'Journey position_code prefixed with position: so numeric source codes cannot mimic NHS numbers.'}),
    T.StructField('specialty_source_concept_id', T.IntegerType(), True, metadata={'comment': '0 because there is no source vocabulary concept.'}),
    T.StructField('gender_source_value', T.StringType(), True, metadata={'comment': 'NULL in O1.'}),
    T.StructField('gender_source_concept_id', T.IntegerType(), True, metadata={'comment': '0 in O1.'}),
])
# === END GENERATED: explicit schema for provider ===

@dp.materialized_view(
    name="provider",
    schema=provider_schema,
    comment="OMOP PROVIDER from Journey practitioner with governed best-effort specialty.",
)
def provider():
    return _provider_base().where(F.col("_exclusion_reason").isNull()).drop("_exclusion_reason")


# COMMAND ----------

# O2 encounter model: visits, visit detail, observation periods, and relationship inputs.

VISIT_SCOPE_LEVELS = (
    "spell", "emergency_visit", "outpatient_attendance",
    "recurring_contact", "results_only", "other",
)


def _visit_base():
    encounter = read_source("src_encounter").alias("e")
    bounds = read_source("src_encounter_bounds").select(
        F.col("encounter_id").alias("_bounds_encounter_id"),
        F.expr("try_cast(source_encounter_id AS bigint)").alias("_natural_visit_id"),
    )
    visit_map = read_source("lkp_visit_map").select(
        F.col("type_code").alias("_vm_type_code"),
        F.col("visit_concept_id").alias("_vm_concept_id"),
    )
    admit = (
        read_source("lkp_admit_discharge_map")
        .where(F.col("axis") == "admitted_from")
        .select(
            F.col("source_code").alias("_adm_code"),
            F.col("concept_id").alias("_adm_concept_id"),
        )
    )
    discharge = (
        read_source("lkp_admit_discharge_map")
        .where(F.col("axis") == "discharged_to")
        .select(
            F.col("source_code").alias("_dis_code"),
            F.col("concept_id").alias("_dis_concept_id"),
        )
    )
    nurse = (
        _latest_location_rows()
        .where(F.col("location_level") == "nurse_unit")
        .select(
            F.col("location_id").alias("_nurse_location_id"),
            F.expr("try_cast(source_location_code AS bigint)").alias("_nurse_care_site_id"),
        )
    )
    published_people = (
        _person_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(F.col("person_id").alias("_published_person_id"))
    )
    joined = (
        encounter
        .join(bounds, F.col("e.encounter_id") == F.col("_bounds_encounter_id"), "left")
        .join(visit_map, F.col("e.type_code") == F.col("_vm_type_code"), "left")
        .join(admit, F.col("e.admission_source_code") == F.col("_adm_code"), "left")
        .join(discharge, F.col("e.discharge_destination_code") == F.col("_dis_code"), "left")
        .join(nurse, F.col("e.current_location_id") == F.col("_nurse_location_id"), "left")
        .join(
            published_people,
            F.expr("try_cast(e.person_id AS bigint)") == F.col("_published_person_id"),
            "left",
        )
        .join(_visit_provider(), F.col("e.encounter_id").cast("bigint") == F.col("_vpv_encounter_id"), "left")  # OGR_PROVIDER_V1
    )
    # Europe/London wall clock, derived in bronze (map_encounter *_BEST_LOCAL).
    start = F.col("e.period_start_local")
    end_raw = F.coalesce(F.col("e.period_end_local"), start)
    end = F.when(end_raw < start, start).otherwise(end_raw)
    placeholder_code = F.lit("0")
    return joined.select(
        F.col("_natural_visit_id").alias("visit_occurrence_id"),
        F.col("_published_person_id").alias("person_id"),
        F.coalesce(F.col("_vm_concept_id"), F.lit(0)).cast("int").alias("visit_concept_id"),
        F.to_date(start).alias("visit_start_date"),
        start.cast("timestamp").alias("visit_start_datetime"),
        F.to_date(end).alias("visit_end_date"),
        end.cast("timestamp").alias("visit_end_datetime"),
        F.lit(32817).cast("int").alias("visit_type_concept_id"),
        F.col("_vpv_provider_id").cast("bigint").alias("provider_id"),  # OGR_PROVIDER_V1
        F.col("_nurse_care_site_id").alias("care_site_id"),
        F.col("e.type_display").cast("string").alias("visit_source_value"),
        F.lit(0).cast("int").alias("visit_source_concept_id"),
        F.coalesce(F.col("_adm_concept_id"), F.lit(0)).cast("int").alias("admitted_from_concept_id"),
        F.when(
            F.col("e.admission_source_code") != placeholder_code,
            F.col("e.admission_source_display"),
        ).alias("admitted_from_source_value"),
        F.coalesce(F.col("_dis_concept_id"), F.lit(0)).cast("int").alias("discharged_to_concept_id"),
        F.when(
            F.col("e.discharge_destination_code") != placeholder_code,
            F.col("e.discharge_destination_display"),
        ).alias("discharged_to_source_value"),
        F.col("e.encounter_id").alias("_encounter_id"),
        F.when(F.col("e.record_status") != "active", F.lit("record_status_superseded"))
        .when(
            ~F.col("e.encounter_level").isin(*VISIT_SCOPE_LEVELS),
            F.lit("encounter_level_out_of_scope"),
        )
        .when(start.isNull(), F.lit("missing_start_datetime"))
        .when(F.to_date(start) < F.to_date(F.lit(STUDY_START)), F.lit("before_study_window"))
        .when(F.col("_natural_visit_id").isNull(), F.lit("no_source_encounter_key"))
        .when(F.expr("try_cast(e.person_id AS bigint)").isNull(), F.lit("person_unresolved"))
        .when(F.col("_published_person_id").isNull(), F.lit("person_not_published"))
        .alias("_exclusion_reason"),
    )


# === GENERATED: explicit schema for visit_occurrence (contract_codegen.py) — do not edit ===
visit_occurrence_schema = T.StructType([
    T.StructField('visit_occurrence_id', T.LongType(), False, metadata={'comment': 'Raw Cerner ENCNTR_ID via Journey encounter_bounds; legacy-continuous natural key.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('visit_concept_id', T.IntegerType(), False, metadata={'comment': 'Standard Visit concept via governed omop_visit_map; 0 when the source type is unmapped.'}),
    T.StructField('visit_start_date', T.DateType(), False, metadata={'comment': 'Encounter period_start_local (Europe/London) date.'}),
    T.StructField('visit_start_datetime', T.TimestampType(), True, metadata={'comment': 'Encounter period_start_local: Europe/London wall-clock time.'}),
    T.StructField('visit_end_date', T.DateType(), False, metadata={'comment': 'Encounter period_end_local (Europe/London) date; equals start when the end is unknown or precedes start.'}),
    T.StructField('visit_end_datetime', T.TimestampType(), True, metadata={'comment': 'Encounter period_end_local (Europe/London wall-clock time) under the same convention.'}),
    T.StructField('visit_type_concept_id', T.IntegerType(), False, metadata={'comment': 'Verified EHR type concept 32817.'}),
    T.StructField('provider_id', T.LongType(), True, metadata={'comment': 'Encounter participant resolved to a published provider by role priority attending > admitting > consulting > mental-health attending > locum (latest validity start, then lowest provider id); NULL when none.'}),
    T.StructField('care_site_id', T.LongType(), True, metadata={'comment': 'Nurse-unit care site resolved from encounter current_location_id.'}),
    T.StructField('visit_source_value', T.StringType(), True, metadata={'comment': 'Journey encounter type_display.'}),
    T.StructField('visit_source_concept_id', T.IntegerType(), True, metadata={'comment': '0; no source vocabulary concept.'}),
    T.StructField('admitted_from_concept_id', T.IntegerType(), True, metadata={'comment': 'Standard concept via governed omop_admit_discharge_map; 0 when unmapped or not applicable.'}),
    T.StructField('admitted_from_source_value', T.StringType(), True, metadata={'comment': 'Admission source display; NULL when the source code is the 0 placeholder.'}),
    T.StructField('discharged_to_concept_id', T.IntegerType(), True, metadata={'comment': 'Standard concept via governed omop_admit_discharge_map; 0 when unmapped or not applicable.'}),
    T.StructField('discharged_to_source_value', T.StringType(), True, metadata={'comment': 'Discharge destination display; NULL when the source code is the 0 placeholder.'}),
    T.StructField('preceding_visit_occurrence_id', T.LongType(), True, metadata={'comment': 'Previous published visit for the person by start datetime; deterministic explicit ordering.'}),
])
# === END GENERATED: explicit schema for visit_occurrence ===

@dp.materialized_view(
    name="visit_occurrence",
    schema=visit_occurrence_schema,
    comment="OMOP VISIT_OCCURRENCE from Journey encounters with natural ENCNTR_ID keys.",
)
def visit_occurrence():
    published = _visit_base().where(F.col("_exclusion_reason").isNull())
    order = Window.partitionBy("person_id").orderBy(
        F.col("visit_start_datetime").asc_nulls_last(),
        F.col("visit_occurrence_id").asc_nulls_last(),
    )
    return (
        published
        .withColumn("preceding_visit_occurrence_id", F.lag("visit_occurrence_id").over(order))
        .select(
            "visit_occurrence_id", "person_id", "visit_concept_id",
            "visit_start_date", "visit_start_datetime", "visit_end_date", "visit_end_datetime",
            "visit_type_concept_id", "provider_id", "care_site_id",
            "visit_source_value", "visit_source_concept_id",
            "admitted_from_concept_id", "admitted_from_source_value",
            "discharged_to_concept_id", "discharged_to_source_value",
            "preceding_visit_occurrence_id",
        )
    )


def _visit_detail_base():
    registry = (
        read_source("src_id_registry")
        .where(F.col("id_space") == "visit_detail")
        .select(
            F.col("source_key").alias("_registry_key"),
            F.col("allocated_id").alias("_allocated_id"),
        )
    )
    published_visits = (
        _visit_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(
            F.col("_encounter_id").alias("_encounter_id"),
            F.col("visit_occurrence_id").alias("_visit_id"),
            F.col("person_id").alias("_visit_person_id"),
            F.col("admitted_from_concept_id").alias("_v_adm_concept"),  # OGR_VD_LINKS_V1
            F.col("admitted_from_source_value").alias("_v_adm_source"),
            F.col("discharged_to_concept_id").alias("_v_dis_concept"),
            F.col("discharged_to_source_value").alias("_v_dis_source"),
        )
    )
    published_care_sites = (
        _care_site_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(F.col("care_site_id").alias("_published_care_site_id"))
        .distinct()
    )
    # location_stay record_status='active' means still open; consume all lifecycle states.
    ward = read_source("src_location_stay").select(
        F.concat(F.lit("ward:"), F.col("location_stay_id")).alias("_registry_source_key"),
        F.col("encounter_id").alias("_encounter_id"),
        # Occupancy, not the stop span: outpatient/day-case stops open at the booking transaction.
        F.col("occupancy_start_local").alias("_start"),
        F.col("occupancy_end_local").alias("_end_raw"),
        F.col("occupancy_start_basis").alias("_start_basis"),
        F.lit(9201).cast("int").alias("visit_detail_concept_id"),
        F.expr("try_cast(nurse_unit_code AS bigint)").alias("_care_site_candidate"),
        F.col("nurse_unit_display").cast("string").alias("visit_detail_source_value"),
        F.lit("ward_stay").alias("_lane"),
        F.lit(False).alias("_retracted"),
    )
    critical_care = read_source("src_critical_care_period").select(
        F.concat(F.lit("cc:"), F.col("period_business_key")).alias("_registry_source_key"),
        F.coalesce(F.col("encounter_id"), F.col("cc_encounter_id")).alias("_encounter_id"),
        F.col("event_datetime_local").alias("_start"),
        F.col("event_end_datetime_local").alias("_end_raw"),
        F.lit(None).cast("string").alias("_start_basis"),
        F.lit(32037).cast("int").alias("visit_detail_concept_id"),
        F.lit(None).cast("bigint").alias("_care_site_candidate"),
        F.col("unit_function").cast("string").alias("visit_detail_source_value"),
        F.lit("critical_care").alias("_lane"),
        (F.col("record_status") != "active").alias("_retracted"),
    )
    lanes = ward.unionByName(critical_care)
    joined = (
        lanes
        .join(registry, F.col("_registry_source_key") == F.col("_registry_key"), "left")
        .join(published_visits, "_encounter_id", "left")
        .join(
            published_care_sites,
            F.col("_care_site_candidate") == F.col("_published_care_site_id"),
            "left",
        )
    )
    # OGR_PROVIDER_V1: attending whose validity (Europe/London) contains the detail start.
    attending = (
        lanes.where(F.col("_registry_source_key").isNotNull())
        .select(F.col("_registry_source_key").alias("_ap_key"),
                F.col("_encounter_id").cast("bigint").alias("_ap_encounter"), F.col("_start").alias("_ap_start"))
        .join(_participation_providers().where(F.col("_vp_role") == "attending"),
              (F.col("_ap_encounter") == F.col("_vp_encounter_id"))
              & (F.col("_vp_valid_from_local") <= F.col("_ap_start"))
              & (F.col("_ap_start") < F.coalesce(F.col("_vp_valid_to_local"), F.lit(OPEN_END).cast("timestamp"))))
        .withColumn("_ap_rank", F.row_number().over(Window.partitionBy("_ap_key").orderBy(
            F.col("_vp_valid_from_local").desc_nulls_last(), F.col("_vp_provider_id").asc_nulls_last())))
        .where(F.col("_ap_rank") == 1)
        .select("_ap_key", F.col("_vp_provider_id").alias("_ap_provider_id"))
    )
    joined = joined.join(attending, F.col("_registry_source_key") == F.col("_ap_key"), "left")
    start = F.col("_start")
    end_raw = F.coalesce(F.col("_end_raw"), start)
    end = F.when(end_raw < start, start).otherwise(end_raw)
    return joined.select(
        F.col("_allocated_id").alias("visit_detail_id"),
        F.col("_visit_person_id").alias("person_id"),
        "visit_detail_concept_id",
        F.to_date(start).alias("visit_detail_start_date"),
        start.cast("timestamp").alias("visit_detail_start_datetime"),
        F.to_date(end).alias("visit_detail_end_date"),
        end.cast("timestamp").alias("visit_detail_end_datetime"),
        F.lit(32817).cast("int").alias("visit_detail_type_concept_id"),
        F.col("_ap_provider_id").cast("bigint").alias("provider_id"),  # OGR_PROVIDER_V1
        F.col("_published_care_site_id").alias("care_site_id"),
        "visit_detail_source_value",
        F.lit(0).cast("int").alias("visit_detail_source_concept_id"),
        F.lit(0).cast("int").alias("admitted_from_concept_id"),
        F.lit(None).cast("string").alias("admitted_from_source_value"),
        F.lit(0).cast("int").alias("discharged_to_concept_id"),
        F.lit(None).cast("string").alias("discharged_to_source_value"),
        F.lit(None).cast("bigint").alias("parent_visit_detail_id"),
        F.col("_visit_id").alias("visit_occurrence_id"),
        F.col("_lane"),
        "_v_adm_concept", "_v_adm_source", "_v_dis_concept", "_v_dis_source",  # OGR_VD_LINKS_V1
        F.when(F.col("_retracted"), F.lit("record_status_retracted"))
        .when(F.col("_start_basis") == "PRE_ATTENDANCE", F.lit("pre_attendance_stop"))
        .when(F.col("_start_basis") == "NO_ATTENDANCE_EVIDENCE", F.lit("no_attendance_evidence"))
        .when(F.col("_start").isNull(), F.lit("missing_start_datetime"))
        .when(F.col("_encounter_id").isNull(), F.lit("no_encounter_link"))
        .when(F.col("_visit_id").isNull(), F.lit("visit_not_published"))
        .when(F.col("_allocated_id").isNull(), F.lit("id_not_allocated"))
        .alias("_exclusion_reason"),
    )


# === GENERATED: explicit schema for visit_detail (contract_codegen.py) — do not edit ===
visit_detail_schema = T.StructType([
    T.StructField('visit_detail_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for ward:<location_stay_id> or cc:<period_business_key>; never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key inherited from the parent visit.'}),
    T.StructField('visit_detail_concept_id', T.IntegerType(), False, metadata={'comment': '9201 ward stay or 32037 critical care.'}),
    T.StructField('visit_detail_start_date', T.DateType(), False, metadata={'comment': 'Ward lane: location_stay occupancy_start_local (Europe/London) date (stops ending before the patient arrived are excluded); critical-care lane: CCMDS period start date (source local time).'}),
    T.StructField('visit_detail_start_datetime', T.TimestampType(), True, metadata={'comment': 'Ward lane: location_stay occupancy_start_local (Europe/London wall-clock time); critical-care lane: CCMDS period start.'}),
    T.StructField('visit_detail_end_date', T.DateType(), False, metadata={'comment': 'Ward lane: location_stay occupancy_end_local (null when the stop end was the encounter administrative close), Europe/London date; critical-care lane: CCMDS period end. Open or inverted intervals fall back to start.'}),
    T.StructField('visit_detail_end_datetime', T.TimestampType(), True, metadata={'comment': 'Stay or period end in local wall-clock time under the same convention.'}),
    T.StructField('visit_detail_type_concept_id', T.IntegerType(), False, metadata={'comment': 'Verified EHR type concept 32817.'}),
    T.StructField('provider_id', T.LongType(), True, metadata={'comment': 'Attending participant whose validity (converted to Europe/London) contains the detail start, resolved to a published provider; latest start, then lowest id; NULL otherwise.'}),
    T.StructField('care_site_id', T.LongType(), True, metadata={'comment': 'Historical nurse-unit code when it resolves to a published care site; NULL otherwise.'}),
    T.StructField('visit_detail_source_value', T.StringType(), True, metadata={'comment': 'Ward lane nurse_unit_display; critical-care lane unit_function.'}),
    T.StructField('visit_detail_source_concept_id', T.IntegerType(), True, metadata={'comment': '0; no source vocabulary concept.'}),
    T.StructField('admitted_from_concept_id', T.IntegerType(), True, metadata={'comment': 'Parent visit admitted_from_concept_id on the first detail of the visit (start order); 0 on later details, whose origin is preceding_visit_detail_id.'}),
    T.StructField('admitted_from_source_value', T.StringType(), True, metadata={'comment': 'Parent visit admission source display on the first detail; NULL otherwise.'}),
    T.StructField('discharged_to_concept_id', T.IntegerType(), True, metadata={'comment': 'Parent visit discharged_to_concept_id on the last detail of the visit (end order); 0 otherwise.'}),
    T.StructField('discharged_to_source_value', T.StringType(), True, metadata={'comment': 'Parent visit discharge destination display on the last detail; NULL otherwise.'}),
    T.StructField('preceding_visit_detail_id', T.LongType(), True, metadata={'comment': 'Previous detail within the same visit and lane by start; deterministic explicit ordering.'}),
    T.StructField('parent_visit_detail_id', T.LongType(), True, metadata={'comment': 'Critical-care detail: the ward stay in the same visit whose interval contains the critical-care start (latest ward start, then lowest id); NULL for ward stays and unmatched periods.'}),
    T.StructField('visit_occurrence_id', T.LongType(), False, metadata={'comment': 'Published parent visit natural ENCNTR_ID key.'}),
])
# === END GENERATED: explicit schema for visit_detail ===

@dp.materialized_view(
    name="visit_detail",
    schema=visit_detail_schema,
    comment="OMOP VISIT_DETAIL from ward stays and critical-care periods with registry IDs.",
)
def visit_detail():
    # OGR_VD_LINKS_V1: parent (critical care inside a ward stay) and visit-edge admission/discharge.
    published = _visit_detail_base().where(F.col("_exclusion_reason").isNull())
    order = Window.partitionBy("visit_occurrence_id", "_lane").orderBy(
        F.col("visit_detail_start_datetime").asc_nulls_last(),
        F.col("visit_detail_id").asc_nulls_last(),
    )
    in_visit = Window.partitionBy("visit_occurrence_id")
    first = in_visit.orderBy(F.col("visit_detail_start_datetime").asc_nulls_last(),
                             F.col("visit_detail_id").asc_nulls_last())
    last = in_visit.orderBy(F.col("visit_detail_end_datetime").desc_nulls_last(),
                            F.col("visit_detail_start_datetime").desc_nulls_last(),
                            F.col("visit_detail_id").desc_nulls_last())
    wards = published.where(F.col("_lane") == "ward_stay").select(
        F.col("visit_detail_id").alias("_w_id"),
        F.col("visit_occurrence_id").alias("_w_visit"),
        F.col("visit_detail_start_datetime").alias("_w_start"),
        F.col("visit_detail_end_datetime").alias("_w_end"),
    )
    cc = published.where(F.col("_lane") == "critical_care").select(
        F.col("visit_detail_id").alias("_c_id"),
        F.col("visit_occurrence_id").alias("_c_visit"),
        F.col("visit_detail_start_datetime").alias("_c_start"),
    )
    parent = (
        cc.join(wards, (F.col("_c_visit") == F.col("_w_visit"))
                & (F.col("_w_start") <= F.col("_c_start")) & (F.col("_c_start") <= F.col("_w_end")), "inner")
        .groupBy("_c_id")
        .agg(F.max(F.struct(F.col("_w_start"), (-F.col("_w_id")).alias("_neg_id"))).alias("_best"))
        .select("_c_id", (-F.col("_best._neg_id")).alias("_vd_parent"))
    )
    linked = (
        published
        .withColumn("preceding_visit_detail_id", F.lag("visit_detail_id").over(order))
        .withColumn("_first_in_visit", F.row_number().over(first) == 1)
        .withColumn("_last_in_visit", F.row_number().over(last) == 1)
        .join(parent, F.col("visit_detail_id") == F.col("_c_id"), "left")
        .withColumn("parent_visit_detail_id", F.col("_vd_parent").cast("bigint"))
        .withColumn("admitted_from_concept_id",
                    F.when(F.col("_first_in_visit"), F.coalesce(F.col("_v_adm_concept"), F.lit(0)))
                    .otherwise(F.lit(0)).cast("int"))
        .withColumn("admitted_from_source_value",
                    F.when(F.col("_first_in_visit"), F.col("_v_adm_source")).cast("string"))
        .withColumn("discharged_to_concept_id",
                    F.when(F.col("_last_in_visit"), F.coalesce(F.col("_v_dis_concept"), F.lit(0)))
                    .otherwise(F.lit(0)).cast("int"))
        .withColumn("discharged_to_source_value",
                    F.when(F.col("_last_in_visit"), F.col("_v_dis_source")).cast("string"))
    )
    return linked.select(
        "visit_detail_id", "person_id", "visit_detail_concept_id",
        "visit_detail_start_date", "visit_detail_start_datetime",
        "visit_detail_end_date", "visit_detail_end_datetime",
        "visit_detail_type_concept_id", "provider_id", "care_site_id",
        "visit_detail_source_value", "visit_detail_source_concept_id",
        "admitted_from_concept_id", "admitted_from_source_value",
        "discharged_to_concept_id", "discharged_to_source_value",
        "preceding_visit_detail_id", "parent_visit_detail_id", "visit_occurrence_id",
    )


def _observation_period_base():
    # HERON-UK convention for sources without enrolment dates: one period per person from the
    # first published record to the earliest of death, the censoring age and the extraction
    # (source release) date. Evidence dated before birth - 1 day (maternal records, epoch
    # truncations) is dropped rather than opening the period early.
    visit_evidence = (
        _visit_base()
        .where(F.col("_exclusion_reason").isNull())
        .select("person_id", F.col("visit_start_datetime").alias("_evidence_start"))
    )
    routed_evidence = (
        read_source("src_routed_events")
        .where(F.col("_exclusion_reason").isNull())
        .select(
            F.col("person_id").cast("bigint").alias("person_id"),
            F.col("event_datetime").alias("_evidence_start"),
        )
    )
    visit_detail_evidence = read_source("src_om_visit_detail").select(
        "person_id", F.col("visit_detail_start_datetime").alias("_evidence_start"),
    )
    specimen_evidence = read_source("src_om_specimen").select(
        "person_id", F.col("specimen_datetime").alias("_evidence_start"),
    )
    births = (
        _person_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(
            F.col("person_id").alias("_birth_person_id"),
            F.col("_birth_date"),
            F.coalesce(F.col("birth_datetime"), F.col("_birth_date").cast("timestamp")).alias("_birth_datetime"),
        )
    )
    evidence = (
        visit_evidence.unionByName(routed_evidence)
        .unionByName(visit_detail_evidence)
        .unionByName(specimen_evidence)
        .unionByName(_evidence_extensions())  # OGR_EXT_V1
        .where(F.col("person_id").isNotNull() & F.col("_evidence_start").isNotNull())
        .join(births, F.col("person_id") == F.col("_birth_person_id"), "left")
        .where(
            F.col("_birth_datetime").isNull()
            | (F.col("_evidence_start") >= F.col("_birth_datetime") - F.expr("INTERVAL 1 DAY"))
        )
    )
    per_person = evidence.groupBy("person_id").agg(
        F.min("_evidence_start").alias("_min_start"),
    )
    people = (
        _person_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(F.col("person_id").alias("_published_person_id"), "_birth_date")
    )
    deaths = (
        _death_base()
        .where(F.col("_exclusion_reason").isNull())
        .select("person_id", F.col("death_date").alias("_death_date"))
    )
    release_date = F.to_date(F.lit(SOURCE_RELEASE_DATE))
    joined = (
        per_person
        .join(people, per_person.person_id == F.col("_published_person_id"), "left")
        .join(deaths, "person_id", "left")
    )
    censor_date = F.add_months(F.col("_birth_date"), CENSOR_AGE_YEARS * 12)
    start = F.to_date("_min_start")
    end = F.least(release_date, F.col("_death_date"), censor_date)
    return joined.select(
        F.col("person_id").alias("observation_period_id"),
        "person_id",
        start.alias("observation_period_start_date"),
        end.alias("observation_period_end_date"),
        F.lit(32817).cast("int").alias("period_type_concept_id"),
        F.when(F.col("_published_person_id").isNull(), F.lit("person_not_published"))
        .when(F.col("_death_date") < start, F.lit("first_record_after_death"))
        .when(censor_date < start, F.lit("first_record_after_censor_age"))
        .when(release_date < start, F.lit("first_record_after_extraction"))
        .alias("_exclusion_reason"),
    )


# === GENERATED: explicit schema for observation_period (contract_codegen.py) — do not edit ===
observation_period_schema = T.StructType([
    T.StructField('observation_period_id', T.LongType(), False, metadata={'comment': 'Equal to person_id because O2 publishes one period per person.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('observation_period_start_date', T.DateType(), False, metadata={'comment': 'Date of the first published record (visit, visit_detail, routed event or specimen) on or after birth - 1 day; no study-window clamp (HERON-UK).'}),
    T.StructField('observation_period_end_date', T.DateType(), False, metadata={'comment': 'Earliest of death date, the 120th birthday and the source-release (extraction) date (HERON-UK); independent of the last record.'}),
    T.StructField('period_type_concept_id', T.IntegerType(), False, metadata={'comment': 'Verified EHR type concept 32817.'}),
])
# === END GENERATED: explicit schema for observation_period ===

@dp.materialized_view(
    name="observation_period",
    schema=observation_period_schema,
    comment="OMOP OBSERVATION_PERIOD under the HERON-UK no-enrolment convention: first record to earliest of death, age 120 and extraction.",
)
def observation_period():
    return (
        _observation_period_base()
        .where(F.col("_exclusion_reason").isNull())
        .drop("_exclusion_reason")
    )


def _fact_relationship_base():
    pairs = (
        read_source("src_person_relationship")
        .where(
            (F.col("record_status") == "active")
            & (F.col("relationship_type_code") == "mother_to_child")
        )
        .select(
            F.expr("try_cast(source_person_id AS bigint)").alias("_mother_id"),
            F.expr("try_cast(target_person_id AS bigint)").alias("_child_id"),
        )
        .distinct()
    )
    published = _person_base().where(F.col("_exclusion_reason").isNull()).select("person_id")
    mothers = published.select(F.col("person_id").alias("_mother_published"))
    children = published.select(F.col("person_id").alias("_child_published"))
    joined = (
        pairs
        .join(mothers, F.col("_mother_id") == F.col("_mother_published"), "left")
        .join(children, F.col("_child_id") == F.col("_child_published"), "left")
    )
    return joined.select(
        "_mother_id", "_child_id",
        F.when(F.col("_mother_id").isNull(), F.lit("mother_unresolved"))
        .when(F.col("_child_id").isNull(), F.lit("child_unresolved"))
        .when(F.col("_mother_published").isNull(), F.lit("mother_not_published"))
        .when(F.col("_child_published").isNull(), F.lit("child_not_published"))
        .alias("_exclusion_reason"),
    )


# COMMAND ----------

# O3/O4/O5 event router: governed concepts, source precedence, and routed staging.

ROUTE_LANES = (
    "condition", "procedure", "observation", "device",
    "registry", "family_history", "community_activity", "measurement", "drug",
)
ROUTED_EVENT_TYPES = (
    "allergy_intolerance", "cancer_treatment", "clinical_finding",
    "clinical_score", "community_care_activity", "condition",
    "condition_stage", "device", "drug_expenditure", "elective_access_entry",
    "endoscopy_finding", "family_history", "medication_admin",
    "medication_dispense", "pathology_order", "pathology_result", "procedure",
    "referral", "registry_entry", "rtt_activity", "rtt_pathway", "transfusion",
    "vital_sign", "waiting_list_entry",
)
DEFERRED_ROUTER_EVENT_TYPES = ("medication_supply",)
# R0 adapter deferral: an (event_type, lane) pair routes only when admitted here.
# Admitted = the pairs published by the last green prod build (2026-09-03), minus
# allergy_intolerance (absence assertions need a dedicated adapter). Any new Silver
# family or lane defers as deferred_adapter / deferred_adapter_role until reviewed.
ADAPTER_ALLOWLIST_VERSION = "r0-2026-09-24"
ADMITTED_ADAPTERS = {
    "cancer_treatment": ("drug", "procedure"),
    "clinical_finding": (
        "condition", "device", "drug", "measurement", "observation", "procedure",
    ),
    "clinical_score": ("observation",),
    "community_care_activity": ("community_activity",),
    "condition": ("condition",),
    "condition_stage": ("condition",),
    "device": ("device",),
    "drug_expenditure": ("drug",),
    "elective_access_entry": ("procedure",),
    "endoscopy_finding": ("observation",),
    "family_history": ("family_history",),
    "medication_admin": ("drug",),
    "medication_dispense": ("drug",),
    "pathology_order": ("measurement",),
    "pathology_result": ("measurement",),
    "procedure": ("device", "procedure"),
    "referral": ("observation",),
    "registry_entry": ("registry",),
    "rtt_activity": ("observation",),
    "rtt_pathway": ("observation",),
    "transfusion": ("device", "observation", "procedure"),
    "vital_sign": ("measurement",),
    "waiting_list_entry": ("observation",),
}
# Measurement value-role rows are admitted only for these event types.
ADMITTED_VALUE_ROLE_EVENT_TYPES = ("pathology_result",)


def _adapter_admitted(event_type_col, lane_col):
    admitted = F.lit(False)
    for event_type, lanes in sorted(ADMITTED_ADAPTERS.items()):
        admitted = admitted | ((event_type_col == event_type) & lane_col.isin(*lanes))
    return admitted

DOMAIN_TABLES = {
    "CONDITION": "condition_occurrence",
    "PROCEDURE": "procedure_occurrence",
    "OBSERVATION": "observation",
    "DEVICE": "device_exposure",
    "MEASUREMENT": "measurement",
    "DRUG": "drug_exposure",
}
MEASUREMENT_TABLE = "measurement"
DRUG_TABLE = "drug_exposure"
ERA_GAP_DAYS = 30
DIRECT_CONCEPT_ID_MAP_SOURCES = (
    "bronze.map_implant_details",
    "lookup.bloodtrack_blood_group_map:applied",
    "lookup.bloodtrack_product_group_map:applied",
    "lookup.mediconnect_device_type_map:applied",
)
FAMILY_HISTORY_CONCEPT = 4167217
# Verified standard Meas Value Operator concepts and their symbol forms.
OPERATOR_SYMBOL_CONCEPTS = {
    "=": 4172703, "<": 4171756, "<=": 4171754, ">=": 4171755, ">": 4172704,
}
OPERATOR_CONCEPT_IDS = tuple(sorted(set(OPERATOR_SYMBOL_CONCEPTS.values())))

# Exactly one staging row is emitted for an event rejected for one of these reasons.
EVENT_GRAIN_REASONS = (
    "deferred_adapter",
    "record_status_not_active", "drug_in_error", "drug_not_administered",  # OGR_ADMIN_STATUS_V1
    "identity_unresolved", "missing_event_datetime",
    "before_study_window", "coding_system_not_governed", "concept_unresolved",
    "standard_unmapped", "resolved_domain_not_published",
    "person_unresolved", "person_not_published",
)
# These are advisory or allocation failures at event-standard row grain.
ROW_GRAIN_REASONS = (
    "id_not_allocated", "fan_row_outside_o3",
    "measurement_standard_suppressed", "drug_standard_suppressed",
    "deferred_adapter_role",
)


def _vocab_concepts():
    # Source concepts remain available even when invalid: a valid Maps-to target can
    # recover them. Validity is enforced on standard targets, not source rows.
    return read_source("src_vocab_concept").select(
        F.col("concept_id").alias("_c_id"),
        F.col("vocabulary_id").alias("_c_vocab"),
        F.col("concept_code").alias("_c_code"),
        F.col("standard_concept").alias("_c_std"),
        F.upper(F.col("domain_id")).alias("_c_domain"),
        F.col("invalid_reason").alias("_c_invalid"),
    )


def _source_concepts_by_code():
    concepts = _vocab_concepts()
    return (
        concepts.groupBy("_c_vocab", "_c_code")
        .agg(
            F.min(
                F.struct(
                    F.when(F.col("_c_invalid").isNull(), F.lit(0))
                    .otherwise(F.lit(1)).alias("invalid_rank"),
                    F.when(F.col("_c_std") == "S", F.lit(0))
                    .otherwise(F.lit(1)).alias("standard_rank"),
                    F.col("_c_id").alias("concept_id"),
                )
            ).alias("_best")
        )
        .select(
            "_c_vocab", "_c_code",
            F.col("_best.concept_id").alias("_source_concept_id"),
        )
    )


def _source_concept_info():
    return _vocab_concepts().select(
        F.col("_c_id").alias("_si_id"),
        F.col("_c_std").alias("_source_standard_flag"),
        F.col("_c_domain").alias("_source_domain"),
        F.col("_c_invalid").alias("_source_invalid_reason"),
    )


def _maps_to():
    valid_targets = _vocab_concepts().where(
        F.col("_c_invalid").isNull() & (F.col("_c_std") == "S")
    ).select(
        F.col("_c_id").alias("_target_id"),
        F.col("_c_domain").alias("_target_domain"),
    )
    return (
        read_source("src_vocab_concept_relationship")
        .where((F.col("relationship_id") == "Maps to") & F.col("invalid_reason").isNull())
        .join(valid_targets, F.col("concept_id_2") == F.col("_target_id"), "inner")
        .select(
            F.col("concept_id_1").alias("_map_source_id"),
            F.col("_target_id").alias("_standard_concept_id"),
            F.col("_target_domain").alias("_standard_domain"),
        )
        .distinct()
    )


def _normalise_mapping_rows(rows):
    return (
        rows.withColumn("_trim_code", F.trim("mapped_code"))
        .withColumn("_upper_code", F.upper("_trim_code"))
        .withColumn(
            "_normalized_code",
            F.when(
                F.col("_cs_vocab").isin("ICD10", "OPCS4")
                & (F.instr(F.col("_upper_code"), ".") == 0)
                & (F.length(F.col("_upper_code")) > 3),
                F.concat(
                    F.substring(F.col("_upper_code"), 1, 3),
                    F.lit("."),
                    F.expr("substring(_upper_code, 4)"),
                ),
            ).otherwise(F.col("_upper_code")),
        )
    )


def _router_rows():
    priority = read_source("lkp_coding_system_priority").select(
        F.col("coding_system").alias("_cs"),
        F.col("vocabulary_id").alias("_cs_vocab"),
        F.col("priority").alias("_priority"),
    )
    rows = (
        read_source("src_patient_event")
        .where(F.col("target_domain").isin(*ROUTE_LANES))
        .where(~F.col("event_type").isin(*DEFERRED_ROUTER_EVENT_TYPES))
        .withColumn("_fact_kind", F.substring_index("fact_table", ".", -1))
        .where(F.col("_fact_kind") != "indication")
        .where(F.col("mapped_code").isNotNull())
        .select(
            "patient_event_row_id", "patient_event_id", "person_id",
            "identity_status", "encounter_id",
            # TZ_OMOP_LOCAL_V1: Europe/London wall clock (bronze *_LOCAL via silver); every routed domain reads these.
            F.col("event_datetime_local").alias("event_datetime"),
            F.col("event_end_datetime_local").alias("event_end_datetime"), "record_status", "source_object",
            "source_code", "source_display", "event_type", "fact_table",
            "_fact_kind", "map_source", "mapped_coding_system", "mapped_code",
            "target_domain",
        )
        .withColumn(
            "_adapter_admitted",
            _adapter_admitted(F.col("event_type"), F.col("target_domain")),
        )
        .join(priority, F.col("mapped_coding_system") == F.col("_cs"), "left")
    )
    return _normalise_mapping_rows(rows)


def _mapping_source_candidates(rows):
    source_by_code = _source_concepts_by_code()
    direct = (
        rows.where(
            (F.col("mapped_coding_system") == "urn:omop:concept_id")
            | (
                (F.col("mapped_coding_system") == "http://snomed.info/sct")
                & F.col("map_source").isin(*DIRECT_CONCEPT_ID_MAP_SOURCES)
            )
        )
        .select(
            "patient_event_row_id", "patient_event_id", "_priority",
            "_normalized_code",
            F.expr("try_cast(_trim_code AS int)").alias("_source_concept_id"),
            F.lit(0).alias("_resolution_rank"),
            F.lit("direct_concept_id").alias("_resolution_method"),
        )
        .where(F.col("_source_concept_id").isNotNull())
    )
    exact = (
        rows.where(F.col("_cs_vocab").isNotNull())
        .join(
            source_by_code,
            (F.col("_cs_vocab") == F.col("_c_vocab"))
            & (F.col("_trim_code") == F.col("_c_code")),
            "inner",
        )
        .select(
            "patient_event_row_id", "patient_event_id", "_priority",
            "_normalized_code", "_source_concept_id",
            F.lit(1).alias("_resolution_rank"),
            F.lit("vocabulary_exact").alias("_resolution_method"),
        )
    )
    normalized = (
        rows.where(
            F.col("_cs_vocab").isNotNull()
            & (F.col("_normalized_code") != F.col("_trim_code"))
        )
        .join(
            source_by_code,
            (F.col("_cs_vocab") == F.col("_c_vocab"))
            & (F.col("_normalized_code") == F.col("_c_code")),
            "inner",
        )
        .select(
            "patient_event_row_id", "patient_event_id", "_priority",
            "_normalized_code", "_source_concept_id",
            F.lit(2).alias("_resolution_rank"),
            F.lit("vocabulary_normalized").alias("_resolution_method"),
        )
    )
    return direct.unionByName(exact).unionByName(normalized).distinct()


def _router_resolved_sources(rows=None, candidates=None):
    rows = rows if rows is not None else _router_rows().where(F.col("_adapter_admitted"))
    candidates = candidates if candidates is not None else _mapping_source_candidates(rows)
    preferred = candidates.groupBy("patient_event_row_id").agg(
        F.min(
            F.struct(
                F.col("_resolution_rank").alias("rank"),
                F.col("_source_concept_id").alias("concept_id"),
                F.col("_resolution_method").alias("method"),
            )
        ).alias("_preferred")
    ).select(
        "patient_event_row_id",
        F.col("_preferred.concept_id").alias("_source_concept_id"),
        F.col("_preferred.method").alias("_resolution_method"),
    )
    return (
        rows.join(preferred, "patient_event_row_id", "left")
        .join(
            _source_concept_info(),
            F.col("_source_concept_id") == F.col("_si_id"),
            "left",
        )
        .drop("_si_id")
    )


def _router_fanned(resolved=None):
    resolved = resolved if resolved is not None else _router_resolved_sources()
    resolvable = resolved.where(F.col("_source_concept_id").isNotNull())
    already_standard = resolvable.where(
        F.col("_source_invalid_reason").isNull()
        & (F.col("_source_standard_flag") == "S")
    ).select(
        "*",
        F.col("_source_concept_id").alias("_standard_concept_id"),
        F.col("_source_domain").alias("_standard_domain"),
    )
    mapped_standard = (
        resolvable.where(
            F.col("_source_invalid_reason").isNotNull()
            | F.col("_source_standard_flag").isNull()
            | (F.col("_source_standard_flag") != "S")
        )
        .join(_maps_to(), F.col("_source_concept_id") == F.col("_map_source_id"), "inner")
        .drop("_map_source_id")
    )
    fanned = already_standard.unionByName(mapped_standard)
    domain_map = F.create_map(*[
        F.lit(item) for pair in DOMAIN_TABLES.items() for item in pair
    ])
    return fanned.withColumn(
        "_final_table",
        F.when(F.col("target_domain") == "family_history", F.lit("observation"))
        .otherwise(domain_map[F.col("_standard_domain")]),
    )


def _router_standard_rows(fanned=None):
    """Retain every distinct event-standard pair; priority breaks provenance ties only.

    R-O4-2: rows landing in measurement additionally reduce to one winner standard
    per event (governed priority, then normalized code); the losers remain in
    staging as suppressed row-grain diagnostics, never published.

    R-O5-2: drug rows retain every standard reached by the minimum-priority coding
    system arm for the event. Combination products can therefore retain several
    standards while lower-priority arms remain visible as suppressed diagnostics."""
    fanned = fanned if fanned is not None else _router_fanned()
    # Keep this as one linear windowed plan. Rejoining independent aggregates to
    # the multi-billion-row router lineage made Spark serialize the same plan
    # repeatedly and exhausted driver heap during O5's first update.
    preferred_key = F.struct(
        F.coalesce(F.col("_priority"), F.lit(999)).alias("priority"),
        F.coalesce(F.col("_normalized_code"), F.lit("")).alias("code"),
        F.col("patient_event_row_id").alias("row_id"),
    )
    preferred_window = Window.partitionBy(
        "patient_event_id", "_standard_concept_id"
    )
    selected = (
        fanned.withColumn(
            "_preferred",
            F.min(preferred_key).over(preferred_window),
        )
        .where(preferred_key == F.col("_preferred"))
        .drop("_preferred")
    )
    selected = selected.withColumn(
        "_final_table_key", F.coalesce(F.col("_final_table"), F.lit("__outside_o3__"))
    )
    winner_window = Window.partitionBy("patient_event_id", "_final_table_key")
    winner_key = F.struct(
        F.coalesce(F.col("_priority"), F.lit(999)).alias("priority"),
        F.coalesce(F.col("_normalized_code"), F.lit("")).alias("code"),
        F.col("_standard_concept_id").alias("standard"),
    )
    selected = (
        selected.withColumn("_winner", F.min(winner_key).over(winner_window))
        .withColumn(
            "_winner_priority",
            F.min(F.coalesce(F.col("_priority"), F.lit(999))).over(winner_window),
        )
        .withColumn(
            "_standard_suppressed",
            F.when(
                F.col("_final_table") == F.lit(MEASUREMENT_TABLE),
                F.coalesce(
                    F.col("_standard_concept_id") != F.col("_winner.standard"),
                    F.lit(False),
                ),
            ).when(
                F.col("_final_table") == F.lit(DRUG_TABLE),
                F.coalesce(F.col("_priority"), F.lit(999))
                != F.col("_winner_priority"),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "_suppressed_reason",
            F.when(
                F.col("_standard_suppressed")
                & (F.col("_final_table") == F.lit(DRUG_TABLE)),
                F.lit("drug_standard_suppressed"),
            ).when(
                F.col("_standard_suppressed"),
                F.lit("measurement_standard_suppressed"),
            ),
        )
        .drop("_winner", "_winner_priority")
    )
    return (
        selected.withColumn(
            "_mapping_count",
            F.size(
                F.collect_set(
                    F.when(
                        ~F.col("_standard_suppressed"),
                        F.col("_standard_concept_id"),
                    )
                ).over(winner_window)
            ).cast("int"),
        )
        .drop("_final_table_key")
    )


def _vocab_source_for_standard(candidates):
    vocab_candidates = (
        candidates.where(F.col("_resolution_method").isin(
            "vocabulary_exact", "vocabulary_normalized"
        ))
        .join(
            _source_concept_info(),
            F.col("_source_concept_id") == F.col("_si_id"),
            "left",
        )
    )
    self_bound = vocab_candidates.where(
        F.col("_source_invalid_reason").isNull()
        & (F.col("_source_standard_flag") == "S")
    ).select(
        "patient_event_id", "_priority", "_normalized_code", "_source_concept_id",
        F.col("_source_concept_id").alias("_bound_standard"),
    )
    map_bound = (
        vocab_candidates.where(
            F.col("_source_invalid_reason").isNotNull()
            | F.col("_source_standard_flag").isNull()
            | (F.col("_source_standard_flag") != "S")
        )
        .join(_maps_to(), F.col("_source_concept_id") == F.col("_map_source_id"), "inner")
        .select(
            "patient_event_id", "_priority", "_normalized_code", "_source_concept_id",
            F.col("_standard_concept_id").alias("_bound_standard"),
        )
    )
    return (
        self_bound.unionByName(map_bound)
        .groupBy("patient_event_id", "_bound_standard")
        .agg(
            F.min(
                F.struct(
                    F.coalesce(F.col("_priority"), F.lit(999)).alias("priority"),
                    F.coalesce(F.col("_normalized_code"), F.lit("")).alias("code"),
                    F.col("_source_concept_id").alias("concept_id"),
                )
            ).alias("_preferred")
        )
        .select(
            "patient_event_id",
            F.col("_bound_standard").alias("_standard_for_source"),
            F.col("_preferred.concept_id").alias("_source_vocab_concept_id"),
        )
    )


def _value_role_concepts():
    priority = read_source("lkp_coding_system_priority").select(
        F.col("coding_system").alias("_cs"),
        F.col("vocabulary_id").alias("_cs_vocab"),
        F.col("priority").alias("_priority"),
    )
    rows = _normalise_mapping_rows(
        read_source("src_patient_event")
        .where(F.col("target_domain").isin("meas value", "measurement_value"))
        .where(F.col("event_type").isin(*ADMITTED_VALUE_ROLE_EVENT_TYPES))
        .where(F.col("mapped_code").isNotNull())
        .select(
            "patient_event_row_id", "patient_event_id", "map_source",
            "mapped_coding_system", "mapped_code",
        )
        .join(priority, F.col("mapped_coding_system") == F.col("_cs"), "inner")
    )
    candidates = _mapping_source_candidates(rows)
    resolved = _router_resolved_sources(rows, candidates).where(
        F.col("_source_concept_id").isNotNull()
    )
    self_standard = resolved.where(
        F.col("_source_invalid_reason").isNull()
        & (F.col("_source_standard_flag") == "S")
    ).select(
        "patient_event_id", F.col("_source_concept_id").alias("_value_standard")
    )
    mapped_standard = (
        resolved.where(
            F.col("_source_invalid_reason").isNotNull()
            | F.col("_source_standard_flag").isNull()
            | (F.col("_source_standard_flag") != "S")
        )
        .join(_maps_to(), F.col("_source_concept_id") == F.col("_map_source_id"), "inner")
        .select(
            "patient_event_id", F.col("_standard_concept_id").alias("_value_standard")
        )
    )
    return (
        self_standard.unionByName(mapped_standard)
        .groupBy("patient_event_id")
        .agg(F.min("_value_standard").cast("int").alias("_value_concept"))
    )


def _ucum_units():
    return (
        _vocab_concepts()
        .where(
            (F.col("_c_vocab") == "UCUM")
            & (F.col("_c_std") == "S")
            & F.col("_c_invalid").isNull()
        )
        .select(
            F.col("_c_code").alias("_ucum_code"),
            F.col("_c_id").alias("_ucum_concept"),
        )
    )


# OGR_PROVIDER_V1: provider attribution from spine_care_participation (metadata provider_policy).
VISIT_PROVIDER_RANK = {"attending": 1, "admitting": 2, "consulting": 3, "mental_health_attending": 4,
                       "locum_attending": 5, "locum_admitting": 5}
OPEN_END = "9999-12-31 00:00:00"


def _participation_providers():
    """Participations whose practitioner is a published provider; validity also in Europe/London wall-clock."""
    return (
        read_source("src_care_participation")
        .join(_published_provider_keys(),
              F.col("practitioner_id").cast("string") == F.col("_pract_key").cast("string"), "inner")
        .select(
            F.col("encounter_id").cast("bigint").alias("_vp_encounter_id"),
            F.expr("try_cast(person_id AS bigint)").alias("_vp_person_id"),
            F.col("role").alias("_vp_role"),
            F.col("valid_from").cast("timestamp").alias("_vp_valid_from"),
            F.col("valid_to").cast("timestamp").alias("_vp_valid_to"),
            F.from_utc_timestamp(F.col("valid_from"), "Europe/London").alias("_vp_valid_from_local"),
            F.from_utc_timestamp(F.col("valid_to"), "Europe/London").alias("_vp_valid_to_local"),
            F.col("_resolved_provider_id").alias("_vp_provider_id"),
        )
    )


def _visit_provider():
    rank = F.lit(None).cast("int")
    for role, n in sorted(VISIT_PROVIDER_RANK.items()):
        rank = F.when(F.col("_vp_role") == role, F.lit(n)).otherwise(rank)
    order = Window.partitionBy("_vp_encounter_id").orderBy(
        F.col("_vp_rank_role").asc(), F.col("_vp_valid_from").desc_nulls_last(),
        F.col("_vp_provider_id").asc_nulls_last())
    return (
        _participation_providers()
        .where(F.col("_vp_encounter_id").isNotNull() & F.col("_vp_role").isin(*VISIT_PROVIDER_RANK))
        .withColumn("_vp_rank_role", rank)
        .withColumn("_vp_rank", F.row_number().over(order))
        .where(F.col("_vp_rank") == 1)
        .select(F.col("_vp_encounter_id").alias("_vpv_encounter_id"),
                F.col("_vp_provider_id").alias("_vpv_provider_id"))
    )


def _person_gp_provider():
    now = F.current_timestamp()
    order = Window.partitionBy("_vp_person_id").orderBy(
        F.col("_vp_valid_from").desc_nulls_last(), F.col("_vp_provider_id").asc_nulls_last())
    return (
        _participation_providers()
        .where((F.col("_vp_role") == "registered_gp") & F.col("_vp_person_id").isNotNull()
               & (F.coalesce(F.col("_vp_valid_from"), F.lit("1900-01-01").cast("timestamp")) <= now)
               & (F.coalesce(F.col("_vp_valid_to"), F.lit(OPEN_END).cast("timestamp")) > now))
        .withColumn("_gp_rank", F.row_number().over(order))
        .where(F.col("_gp_rank") == 1)
        .select(F.col("_vp_person_id").alias("_gp_person_id"), F.col("_vp_provider_id").alias("_gp_provider_id"))
    )


def _published_visit_keys():
    return (
        _visit_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(
            F.col("_encounter_id").alias("_pv_encounter_id"),
            F.col("visit_occurrence_id").alias("_pv_visit_id"),
        )
    )


def _published_provider_keys():
    published = (
        _provider_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(F.col("provider_id").alias("_prov_id"))
    )
    practitioners = read_source("src_practitioner").select(
        F.col("practitioner_id").alias("_pract_key"),
        F.expr("try_cast(source_practitioner_id AS bigint)").alias("_prov_candidate"),
    )
    return practitioners.join(
        published, F.col("_prov_candidate") == F.col("_prov_id"), "inner"
    ).select("_pract_key", F.col("_prov_id").alias("_resolved_provider_id"))


def _admin_status():
    """Per-event administration outcome flags from active medication_admin rows. OGR_ADMIN_STATUS_V1"""
    admin = read_source("src_medication_admin").where(F.col("record_status") == "active")
    code = F.expr("try_cast(administration_status_code AS int)")
    event_display = F.lower(F.trim(F.coalesce(F.col("source_event_type_display"), F.lit(""))))
    return admin.groupBy("patient_event_id").agg(
        F.max(F.when(code == 31, 1).otherwise(0)).alias("_adm_in_error"),
        F.max(
            F.when((code == 36) | event_display.isin("not given", "not done"), 1).otherwise(0)
        ).alias("_adm_not_given"),
    )


def _routed_events_frame():
    all_rows = _router_rows()
    # Only admitted (event_type, lane) rows resolve concepts; the rest defer.
    rows = all_rows.where(F.col("_adapter_admitted"))
    candidates = _mapping_source_candidates(rows)
    resolved = _router_resolved_sources(rows, candidates)
    standards = _router_standard_rows(_router_fanned(resolved))

    representative_key = all_rows.groupBy("patient_event_id").agg(
        F.min(
            F.struct(
                F.when(F.col("_adapter_admitted"), F.lit(0)).otherwise(F.lit(1))
                .alias("deferred"),
                F.coalesce(F.col("_priority"), F.lit(999)).alias("priority"),
                F.coalesce(F.col("_normalized_code"), F.lit("")).alias("code"),
                F.col("patient_event_row_id").alias("row_id"),
            )
        ).alias("_representative")
    ).select(
        "patient_event_id",
        F.col("_representative.row_id").alias("_representative_row_id"),
    )
    representative = (
        all_rows.alias("r")
        .join(
            representative_key.alias("k"),
            (F.col("r.patient_event_id") == F.col("k.patient_event_id"))
            & (F.col("r.patient_event_row_id") == F.col("k._representative_row_id")),
            "inner",
        )
        .select(*[F.col(f"r.{column}").alias(column) for column in all_rows.columns])
    )
    admission = all_rows.groupBy("patient_event_id").agg(
        F.max(F.when(F.col("_adapter_admitted"), F.lit(1)).otherwise(F.lit(0)))
        .alias("_has_admitted")
    )
    governed = rows.groupBy("patient_event_id").agg(
        F.max(F.when(F.col("_priority").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .alias("_has_governed")
    )
    source_resolution = resolved.groupBy("patient_event_id").agg(
        F.max(F.when(F.col("_source_concept_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .alias("_has_source")
    )
    standard_resolution = standards.groupBy("patient_event_id").agg(
        F.count(F.lit(1)).alias("_standard_rows"),
        F.count_if(
            F.col("_final_table").isNotNull() & ~F.col("_standard_suppressed")
        ).alias("_o3_rows"),
    )
    published_people = (
        _person_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(F.col("person_id").alias("_published_person_id"))
    )
    type_map = read_source("lkp_type_concept").select(
        F.col("source_feed").alias("_tc_feed"),
        F.col("type_concept_id").alias("_tc_concept"),
    )
    events = (
        representative
        .join(admission, "patient_event_id", "left")
        .join(governed, "patient_event_id", "left")
        .join(source_resolution, "patient_event_id", "left")
        .join(standard_resolution, "patient_event_id", "left")
        .join(_admin_status(), "patient_event_id", "left")  # OGR_ADMIN_STATUS_V1
        .withColumn("_person_candidate", F.expr("try_cast(person_id AS bigint)"))
        .join(
            published_people,
            F.col("_person_candidate") == F.col("_published_person_id"),
            "left",
        )
        .join(
            _published_visit_keys(),
            F.col("encounter_id") == F.col("_pv_encounter_id"),
            "left",
        )
        .join(type_map, F.col("source_object") == F.col("_tc_feed"), "left")
        .select(
            "patient_event_id", "event_type", "event_datetime", "event_end_datetime",
            "source_code", "source_display", "mapped_code", "fact_table", "_fact_kind",
            "mapped_coding_system",
            F.col("target_domain").alias("lane"),
            F.col("_published_person_id").alias("person_id"),
            F.col("_pv_visit_id").alias("visit_occurrence_id"),
            F.coalesce(F.col("_tc_concept"), F.lit(32817)).cast("int")
            .alias("type_concept_id"),
            F.when(
                F.coalesce(F.col("_has_admitted"), F.lit(0)) == 0,
                F.lit("deferred_adapter"),
            )
            .when(
                F.coalesce(F.col("record_status"), F.lit("")) != "active",
                F.lit("record_status_not_active"),
            )
            .when(F.coalesce(F.col("_adm_in_error"), F.lit(0)) == 1, F.lit("drug_in_error"))  # OGR_ADMIN_STATUS_V1
            .when(F.coalesce(F.col("_adm_not_given"), F.lit(0)) == 1, F.lit("drug_not_administered"))
            .when(
                F.coalesce(F.col("identity_status"), F.lit("")) != "resolved",
                F.lit("identity_unresolved"),
            )
            .when(F.col("event_datetime").isNull(), F.lit("missing_event_datetime"))
            .when(
                F.to_date("event_datetime") < F.to_date(F.lit(STUDY_START)),
                F.lit("before_study_window"),
            )
            .when(F.coalesce(F.col("_has_governed"), F.lit(0)) == 0,
                  F.lit("coding_system_not_governed"))
            .when(F.coalesce(F.col("_has_source"), F.lit(0)) == 0,
                  F.lit("concept_unresolved"))
            .when(F.coalesce(F.col("_standard_rows"), F.lit(0)) == 0,
                  F.lit("standard_unmapped"))
            .when(F.coalesce(F.col("_o3_rows"), F.lit(0)) == 0,
                  F.lit("resolved_domain_not_published"))
            .when(F.col("_person_candidate").isNull(), F.lit("person_unresolved"))
            .when(F.col("_published_person_id").isNull(), F.lit("person_not_published"))
            .alias("_exclusion_reason"),
        )
    )
    null_int = F.lit(None).cast("int")
    excluded_events = events.where(F.col("_exclusion_reason").isNotNull()).select(
        "patient_event_id", "event_type", "lane", "fact_table",
        F.col("_fact_kind").alias("fact_kind"),
        "event_datetime", "event_end_datetime", "source_code", "source_display",
        "mapped_code", "mapped_coding_system", "person_id", "visit_occurrence_id",
        "type_concept_id",
        null_int.alias("standard_concept_id"),
        null_int.alias("source_concept_id"),
        F.lit(None).cast("string").alias("final_table"),
        null_int.alias("mapping_count"),
        F.lit(None).cast("bigint").alias("allocated_id"),
        "_exclusion_reason",
    )
    event_gate = events.where(F.col("_exclusion_reason").isNull()).select(
        "patient_event_id", "event_datetime", "event_end_datetime", "person_id",
        "visit_occurrence_id", "type_concept_id",
    )
    source_binding = _vocab_source_for_standard(candidates)
    registry = (
        read_source("src_id_registry")
        .where(F.col("id_space") == "clinical_event")
        .select(
            F.col("source_key").alias("_registry_key"),
            F.col("allocated_id").alias("_registry_id"),
        )
    )
    published_rows = (
        standards.alias("s")
        .join(event_gate.alias("e"), "patient_event_id", "inner")
        .join(
            source_binding.alias("b"),
            (F.col("s.patient_event_id") == F.col("b.patient_event_id"))
            & (F.col("s._standard_concept_id") == F.col("b._standard_for_source")),
            "left",
        )
        .withColumn(
            "_registry_source_key",
            F.concat_ws(
                ":", F.lit("evt"), F.col("s.patient_event_id"),
                F.col("s._standard_concept_id").cast("string"),
            ),
        )
        .join(registry, F.col("_registry_source_key") == F.col("_registry_key"), "left")
        .select(
            F.col("s.patient_event_id").alias("patient_event_id"),
            F.col("s.event_type").alias("event_type"),
            F.col("s.target_domain").alias("lane"),
            F.col("s.fact_table").alias("fact_table"),
            F.col("s._fact_kind").alias("fact_kind"),
            F.col("e.event_datetime").alias("event_datetime"),
            F.col("e.event_end_datetime").alias("event_end_datetime"),
            F.col("s.source_code").alias("source_code"),
            F.col("s.source_display").alias("source_display"),
            F.col("s.mapped_code").alias("mapped_code"),
            F.col("s.mapped_coding_system").alias("mapped_coding_system"),
            F.col("e.person_id").alias("person_id"),
            F.col("e.visit_occurrence_id").alias("visit_occurrence_id"),
            F.col("e.type_concept_id").alias("type_concept_id"),
            F.col("s._standard_concept_id").cast("int").alias("standard_concept_id"),
            F.coalesce(F.col("b._source_vocab_concept_id"), F.lit(0)).cast("int")
            .alias("source_concept_id"),
            F.col("s._final_table").alias("final_table"),
            F.col("s._mapping_count").cast("int").alias("mapping_count"),
            F.col("_registry_id").alias("allocated_id"),
            F.when(F.col("s._standard_suppressed"), F.col("s._suppressed_reason"))
            .when(F.col("s._final_table").isNull(), F.lit("fan_row_outside_o3"))
            .when(F.col("_registry_id").isNull(), F.lit("id_not_allocated"))
            .alias("_exclusion_reason"),
        )
    )
    # Published events keep one diagnostic row per unadmitted lane they also carry.
    deferred_roles = (
        all_rows.where(~F.col("_adapter_admitted"))
        .groupBy("patient_event_id", "target_domain")
        .agg(
            F.min(
                F.struct(
                    F.col("patient_event_row_id").alias("row_id"),
                    F.col("event_type").alias("event_type"),
                    F.col("fact_table").alias("fact_table"),
                    F.col("_fact_kind").alias("fact_kind"),
                    F.col("source_code").alias("source_code"),
                    F.col("source_display").alias("source_display"),
                    F.col("mapped_code").alias("mapped_code"),
                    F.col("mapped_coding_system").alias("mapped_coding_system"),
                )
            ).alias("_d")
        )
        .join(event_gate, "patient_event_id", "inner")
        .select(
            "patient_event_id",
            F.col("_d.event_type").alias("event_type"),
            F.col("target_domain").alias("lane"),
            F.col("_d.fact_table").alias("fact_table"),
            F.col("_d.fact_kind").alias("fact_kind"),
            "event_datetime", "event_end_datetime",
            F.col("_d.source_code").alias("source_code"),
            F.col("_d.source_display").alias("source_display"),
            F.col("_d.mapped_code").alias("mapped_code"),
            F.col("_d.mapped_coding_system").alias("mapped_coding_system"),
            "person_id", "visit_occurrence_id", "type_concept_id",
            null_int.alias("standard_concept_id"),
            null_int.alias("source_concept_id"),
            F.lit(None).cast("string").alias("final_table"),
            null_int.alias("mapping_count"),
            F.lit(None).cast("bigint").alias("allocated_id"),
            F.lit("deferred_adapter_role").alias("_exclusion_reason"),
        )
    )
    return excluded_events.unionByName(published_rows).unionByName(deferred_roles)


@dp.materialized_view(
    name="_om_routed_events",
    comment="Internal router staging: one row per excluded event plus one row per distinct event-standard concept.",
)
def _om_routed_events():
    return _routed_events_frame()


def _staged_rows(final_table):
    return (
        read_source("src_routed_events")
        .where(F.col("_exclusion_reason").isNull())
        .where(F.col("final_table") == final_table)
    )


def _clamp_end(start_col, end_col):
    end_raw = F.coalesce(end_col, start_col)
    return F.when(end_raw < start_col, start_col).otherwise(end_raw)


def _safe_source_code(value, namespace):
    text_value = value.cast("string")
    identifier_shaped = (
        text_value.rlike(r"^[0-9]{10}$")
        | F.upper(text_value).rlike(r"^[A-Z]{1,2}[0-9][0-9A-Z]?[ ]?[0-9][A-Z]{2}$")
    )
    return F.when(
        identifier_shaped,
        F.concat(F.lit(f"{namespace}:"), text_value),
    ).otherwise(text_value)


# COMMAND ----------

# O3 routed clinical domains.

# === GENERATED: explicit schema for condition_occurrence (contract_codegen.py) — do not edit ===
condition_occurrence_schema = T.StructType([
    T.StructField('condition_occurrence_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for evt:<patient_event_id>:<standard_concept_id>; event-grain and mapping-release-stable; pobs:<journey_pregnancy_key>:<item> for pregnancy history rows or bir:<birth_key>:<item> for birth rows (clinical_event space); never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('condition_concept_id', T.IntegerType(), False, metadata={'comment': 'Resolved standard Condition concept; never 0 under the mapped-only policy.'}),
    T.StructField('condition_start_date', T.DateType(), False, metadata={'comment': 'clinical_condition event_datetime_local (Europe/London) date.'}),
    T.StructField('condition_start_datetime', T.TimestampType(), True, metadata={'comment': 'clinical_condition event_datetime_local: Europe/London wall-clock time.'}),
    T.StructField('condition_end_date', T.DateType(), True, metadata={'comment': 'Abatement or event end where supplied; clamped to not precede start; Europe/London wall-clock time (clinical_condition *_local).'}),
    T.StructField('condition_end_datetime', T.TimestampType(), True, metadata={'comment': 'Abatement or event end under the same convention; Europe/London wall-clock time (clinical_condition *_local).'}),
    T.StructField('condition_type_concept_id', T.IntegerType(), False, metadata={'comment': 'Governed per-feed type concept via omop_type_concept_map; 32817 default.'}),
    T.StructField('condition_status_concept_id', T.IntegerType(), True, metadata={'comment': 'Governed Condition Status concept from category_display; 0 when unmapped.'}),
    T.StructField('stop_reason', T.StringType(), True, metadata={'comment': 'NULL in O3; no source assertion.'}),
    T.StructField('provider_id', T.LongType(), True, metadata={'comment': 'Asserting practitioner resolved to a published provider; NULL otherwise.'}),
    T.StructField('visit_occurrence_id', T.LongType(), True, metadata={'comment': "Published parent visit where the event's encounter resolves; NULL otherwise."}),
    T.StructField('visit_detail_id', T.LongType(), True, metadata={'comment': 'Published VISIT_DETAIL of the same visit whose interval contains the event datetime (critical care before ward stay, latest start, lowest id); NULL when none.'}),
    T.StructField('condition_source_value', T.StringType(), True, metadata={'comment': 'Source code, falling back to the selected mapped code; identifier-shaped 10-digit values are prefixed condition: for D4 safety.'}),
    T.StructField('condition_source_concept_id', T.IntegerType(), True, metadata={'comment': 'Best vocabulary-arm source concept for the event; 0 when only the urn arm exists.'}),
    T.StructField('condition_status_source_value', T.StringType(), True, metadata={'comment': 'Source category display driving the status concept.'}),
])
# === END GENERATED: explicit schema for condition_occurrence ===

@dp.materialized_view(
    name="condition_occurrence",
    schema=condition_occurrence_schema,
    comment="OMOP CONDITION_OCCURRENCE routed from Journey patient events; mapped-only.",
)
def condition_occurrence():
    return _finish("condition_occurrence", _condition_occurrence_core())  # OGR_EXT_V1


def _condition_occurrence_core():
    rows = _staged_rows("condition_occurrence")
    enrich = read_source("src_condition").select(
        "patient_event_id",
        F.col("category_display").alias("_category_display"),
        F.col("abatement_datetime").alias("_abatement"),
        F.col("asserter_practitioner_id").alias("_asserter"),
        F.col("event_datetime_local").alias("_start_local"),
        F.col("event_end_datetime_local").alias("_end_local"),
        F.col("abatement_datetime_local").alias("_abatement_local"),
    )
    status = read_source("lkp_condition_status").select(
        F.col("category_display").alias("_status_key"),
        F.col("condition_status_concept_id").alias("_status_concept"),
    )
    joined = (
        rows.join(enrich, "patient_event_id", "left")
        .join(status, F.col("_category_display") == F.col("_status_key"), "left")
        .join(
            _published_provider_keys(),
            F.col("_asserter") == F.col("_pract_key"),
            "left",
        )
    )
    start = F.coalesce(F.col("_start_local"), F.col("event_datetime"))
    end = _clamp_end(start, F.when(F.col("_start_local").isNotNull(),
                                   F.coalesce(F.col("_abatement_local"), F.col("_end_local")))
                     .otherwise(F.coalesce(F.col("_abatement"), F.col("event_end_datetime"))))
    return joined.select(
        F.col("allocated_id").alias("condition_occurrence_id"),
        "person_id",
        F.col("standard_concept_id").alias("condition_concept_id"),
        F.to_date(start).alias("condition_start_date"),
        start.cast("timestamp").alias("condition_start_datetime"),
        F.to_date(end).alias("condition_end_date"),
        end.cast("timestamp").alias("condition_end_datetime"),
        F.col("type_concept_id").alias("condition_type_concept_id"),
        F.coalesce(F.col("_status_concept"), F.lit(0)).cast("int")
        .alias("condition_status_concept_id"),
        F.lit(None).cast("string").alias("stop_reason"),
        F.col("_resolved_provider_id").alias("provider_id"),
        "visit_occurrence_id",
        F.lit(None).cast("bigint").alias("visit_detail_id"),
        _safe_source_code(
            F.coalesce(F.col("source_code"), F.col("mapped_code")), "condition"
        ).alias("condition_source_value"),
        F.col("source_concept_id").alias("condition_source_concept_id"),
        F.col("_category_display").cast("string").alias("condition_status_source_value"),
    )


# === GENERATED: explicit schema for procedure_occurrence (contract_codegen.py) — do not edit ===
procedure_occurrence_schema = T.StructType([
    T.StructField('procedure_occurrence_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for evt:<patient_event_id>:<standard_concept_id>; event-grain and mapping-release-stable; pobs:<journey_pregnancy_key>:<item> for pregnancy history rows or bir:<birth_key>:<item> for birth rows (clinical_event space); never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('procedure_concept_id', T.IntegerType(), False, metadata={'comment': 'Resolved standard Procedure concept; never 0 under the mapped-only policy.'}),
    T.StructField('procedure_date', T.DateType(), False, metadata={'comment': 'Event datetime date. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('procedure_datetime', T.TimestampType(), True, metadata={'comment': 'Event datetime. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('procedure_end_date', T.DateType(), True, metadata={'comment': 'Event end (silver performed_end is the same source column) where supplied; clamped to not precede start. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('procedure_end_datetime', T.TimestampType(), True, metadata={'comment': 'Event end under the same convention. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('procedure_type_concept_id', T.IntegerType(), False, metadata={'comment': 'Governed per-feed type concept via omop_type_concept_map; 32817 default.'}),
    T.StructField('modifier_concept_id', T.IntegerType(), True, metadata={'comment': '0 in O3; no modifier lane.'}),
    T.StructField('quantity', T.IntegerType(), True, metadata={'comment': 'Source quantity where meaningful; NULL when the event produces more than one published standard concept in procedure_occurrence.'}),
    T.StructField('provider_id', T.LongType(), True, metadata={'comment': 'Performing practitioner resolved to a published provider; NULL otherwise.'}),
    T.StructField('visit_occurrence_id', T.LongType(), True, metadata={'comment': "Published parent visit where the event's encounter resolves; NULL otherwise."}),
    T.StructField('visit_detail_id', T.LongType(), True, metadata={'comment': 'Published VISIT_DETAIL of the same visit whose interval contains the event datetime (critical care before ward stay, latest start, lowest id); NULL when none.'}),
    T.StructField('procedure_source_value', T.StringType(), True, metadata={'comment': 'Source code, falling back to the selected mapped code; identifier-shaped 10-digit values are prefixed procedure: for D4 safety.'}),
    T.StructField('procedure_source_concept_id', T.IntegerType(), True, metadata={'comment': 'Best vocabulary-arm source concept for the event; 0 when only the urn arm exists.'}),
    T.StructField('modifier_source_value', T.StringType(), True, metadata={'comment': 'NULL in O3.'}),
])
# === END GENERATED: explicit schema for procedure_occurrence ===

@dp.materialized_view(
    name="procedure_occurrence",
    schema=procedure_occurrence_schema,
    comment="OMOP PROCEDURE_OCCURRENCE routed from Journey patient events; mapped-only.",
)
def procedure_occurrence():
    return _finish("procedure_occurrence", _procedure_occurrence_core())  # OGR_EXT_V1


def _procedure_occurrence_core():
    rows = _staged_rows("procedure_occurrence")
    enrich = read_source("src_procedure").select(
        "patient_event_id",
        F.col("quantity").alias("_quantity"),
        F.col("performer_practitioner_id").alias("_performer"),
    )
    joined = (
        rows.join(enrich, "patient_event_id", "left")
        .join(
            _published_provider_keys(),
            F.col("_performer") == F.col("_pract_key"),
            "left",
        )
    )
    start = F.col("event_datetime")
    # silver performed_end is the same source column as event_end_datetime in every procedure lane.
    end = _clamp_end(start, F.col("event_end_datetime"))
    return joined.select(
        F.col("allocated_id").alias("procedure_occurrence_id"),
        "person_id",
        F.col("standard_concept_id").alias("procedure_concept_id"),
        F.to_date(start).alias("procedure_date"),
        start.cast("timestamp").alias("procedure_datetime"),
        F.to_date(end).alias("procedure_end_date"),
        end.cast("timestamp").alias("procedure_end_datetime"),
        F.col("type_concept_id").alias("procedure_type_concept_id"),
        F.lit(0).cast("int").alias("modifier_concept_id"),
        F.when(F.col("mapping_count") <= 1, F.col("_quantity")).cast("int")
        .alias("quantity"),
        F.col("_resolved_provider_id").alias("provider_id"),
        "visit_occurrence_id",
        F.lit(None).cast("bigint").alias("visit_detail_id"),
        _safe_source_code(
            F.coalesce(F.col("source_code"), F.col("mapped_code")), "procedure"
        ).alias("procedure_source_value"),
        F.col("source_concept_id").alias("procedure_source_concept_id"),
        F.lit(None).cast("string").alias("modifier_source_value"),
    )


def _imd_candidates():
    # Registry key recipe imd:<address_id>. _om_id_registry allocates the same filter set as a
    # superset; the pipeline gates below only remove rows, so every published row is allocated.
    return (
        read_source("src_person_address")
        .where(
            (F.col("parent_entity") == "PERSON")
            & (F.col("active_ind") == 1)
            & F.col("address_type_code").isin(*IMD_ADDRESS_TYPES)
            & F.trim(F.col("imd_quintile")).isin("1", "2", "3", "4", "5")
            & F.col("address_id").isNotNull()
            & F.col("person_id").isNotNull()
        )
        .select(
            F.col("person_id").cast("bigint").alias("person_id"),
            F.col("address_id").cast("bigint").alias("_address_id"),
            F.trim(F.col("imd_quintile")).alias("_imd_quintile"),
            F.to_date("beg_effective").alias("_beg_effective_date"),
        )
    )


def _imd_base():
    # HERON-UK IMD convention: observation_concept_id 0, observation_source_concept_id 35812882,
    # value_as_number = quintile, unit 37524288. The date is the address effective-from date when
    # usable, else observation_period_start_date — the latter only for a person's sole IMD record.
    candidates = _imd_candidates()
    people = (
        _person_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(F.col("person_id").alias("_published_person_id"), "_birth_date")
    )
    periods = read_source("src_om_observation_period").select(
        F.col("person_id").alias("_period_person_id"),
        F.col("observation_period_start_date").alias("_period_start"),
    )
    registry = (
        read_source("src_id_registry")
        .where(F.col("id_space") == "clinical_event")
        .select(
            F.col("source_key").alias("_registry_key"),
            F.col("allocated_id").alias("_registry_id"),
        )
    )
    release_date = F.to_date(F.lit(SOURCE_RELEASE_DATE))
    dated = (
        (F.col("_beg_effective_date") > F.to_date(F.lit("1900-01-01")))
        & (F.col("_beg_effective_date") <= release_date)
        & (F.col("_birth_date").isNull() | (F.col("_beg_effective_date") >= F.col("_birth_date")))
    )
    per_person = Window.partitionBy("person_id")
    observation_date = F.when(dated, F.col("_beg_effective_date")).otherwise(F.col("_period_start"))
    joined = (
        candidates
        .join(people, F.col("person_id") == F.col("_published_person_id"), "left")
        .join(periods, F.col("person_id") == F.col("_period_person_id"), "left")
        .withColumn("_dated", F.coalesce(dated, F.lit(False)))
        .withColumn("_person_imd_rows", F.count(F.lit(1)).over(per_person))
        .withColumn("observation_date", observation_date)
        .withColumn(
            "_gate_reason",
            F.when(F.col("_published_person_id").isNull(), F.lit("person_not_published"))
            .when(F.col("_period_start").isNull(), F.lit("no_observation_period"))
            .when(~F.col("_dated") & (F.col("_person_imd_rows") > 1), F.lit("undated_with_multiple_imd_records")),
        )
    )
    # Rows that pass the gates win the duplicate tiebreak; lowest address_id among them publishes.
    duplicate_order = Window.partitionBy("person_id", "observation_date", "_imd_quintile").orderBy(
        F.col("_gate_reason").isNull().desc(), F.col("_address_id").asc_nulls_last()
    )
    ranked = (
        joined
        .withColumn("_duplicate_rank", F.row_number().over(duplicate_order))
        .withColumn("_registry_lookup", F.concat(F.lit("imd:"), F.col("_address_id").cast("string")))
        .join(registry, F.col("_registry_lookup") == F.col("_registry_key"), "left")
    )
    return ranked.select(
        F.col("_registry_id").alias("observation_id"),
        "person_id",
        F.lit(0).cast("int").alias("observation_concept_id"),
        F.col("observation_date").cast("date").alias("observation_date"),
        F.col("observation_date").cast("timestamp").alias("observation_datetime"),
        F.lit(32817).cast("int").alias("observation_type_concept_id"),
        F.col("_imd_quintile").cast("double").alias("value_as_number"),
        F.lit(None).cast("string").alias("value_as_string"),
        F.lit(0).cast("int").alias("value_as_concept_id"),
        F.lit(0).cast("int").alias("qualifier_concept_id"),
        F.lit(IMD_UNIT_CONCEPT_ID).cast("int").alias("unit_concept_id"),
        F.lit(None).cast("bigint").alias("provider_id"),
        F.lit(None).cast("bigint").alias("visit_occurrence_id"),
        F.lit(None).cast("bigint").alias("visit_detail_id"),
        F.lit("imd_quintile").alias("observation_source_value"),
        F.lit(IMD_SOURCE_CONCEPT_ID).cast("int").alias("observation_source_concept_id"),
        F.lit(None).cast("string").alias("unit_source_value"),
        F.lit(None).cast("string").alias("qualifier_source_value"),
        F.col("_imd_quintile").cast("string").alias("value_source_value"),
        F.lit(None).cast("bigint").alias("observation_event_id"),
        F.lit(0).cast("int").alias("obs_event_field_concept_id"),
        F.when(F.col("_gate_reason").isNotNull(), F.col("_gate_reason"))
        .when(F.col("_duplicate_rank") > 1, F.lit("duplicate_imd_record"))
        .when(F.col("_registry_id").isNull(), F.lit("id_not_allocated"))
        .alias("_exclusion_reason"),
    )


# === GENERATED: explicit schema for observation (contract_codegen.py) — do not edit ===
observation_schema = T.StructType([
    T.StructField('observation_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for evt:<patient_event_id>:<standard_concept_id>, imd:<address_id> for IMD rows, or alg:<patient_event_id> for allergy rows; pobs:<journey_pregnancy_key>:<item> for pregnancy history rows or bir:<birth_key>:<item> for birth rows (clinical_event space); never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('observation_concept_id', T.IntegerType(), False, metadata={'comment': 'Resolved standard concept; 4167217 for family-history-lane rows; 43530807 (allergy) or 37396387 (no known allergy) on allergy rows; 0 only on HERON-UK IMD rows.'}),
    T.StructField('observation_date', T.DateType(), False, metadata={'comment': 'Event datetime date. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('observation_datetime', T.TimestampType(), True, metadata={'comment': 'Event datetime. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('observation_type_concept_id', T.IntegerType(), False, metadata={'comment': 'Governed per-feed type concept via omop_type_concept_map; registry lanes carry 32879.'}),
    T.StructField('value_as_number', T.DoubleType(), True, metadata={'comment': 'Score or community numeric value, or IMD quintile 1-5; NULL when the event produces more than one published standard concept in observation.'}),
    T.StructField('value_as_string', T.StringType(), True, metadata={'comment': 'NULL in O3 per rider R-O3-6; source value_text stays outside the D4 surface.'}),
    T.StructField('value_as_concept_id', T.IntegerType(), True, metadata={'comment': 'Value-role mapping concept or resolved coded value; family-history rows carry the family condition concept; allergy rows carry the substance concept; 0 otherwise.'}),
    T.StructField('qualifier_concept_id', T.IntegerType(), True, metadata={'comment': '0 in O3.'}),
    T.StructField('unit_concept_id', T.IntegerType(), True, metadata={'comment': 'Silver-carried UCUM unit concept where castable; 0 otherwise.'}),
    T.StructField('provider_id', T.LongType(), True, metadata={'comment': 'Performing practitioner resolved to a published provider; NULL otherwise.'}),
    T.StructField('visit_occurrence_id', T.LongType(), True, metadata={'comment': "Published parent visit where the event's encounter resolves; NULL otherwise."}),
    T.StructField('visit_detail_id', T.LongType(), True, metadata={'comment': 'Published VISIT_DETAIL of the same visit whose interval contains the event datetime (critical care before ward stay, latest start, lowest id); NULL when none.'}),
    T.StructField('observation_source_value', T.StringType(), True, metadata={'comment': 'Source code, falling back to the selected mapped code; identifier-shaped 10-digit values are prefixed observation: for D4 safety.'}),
    T.StructField('observation_source_concept_id', T.IntegerType(), True, metadata={'comment': 'Best vocabulary-arm source concept for the event; 0 when only the urn arm exists.'}),
    T.StructField('unit_source_value', T.StringType(), True, metadata={'comment': 'Silver unit source value where carried.'}),
    T.StructField('qualifier_source_value', T.StringType(), True, metadata={'comment': 'Family-history relationship display, or allergy severity display; NULL elsewhere.'}),
    T.StructField('value_source_value', T.StringType(), True, metadata={'comment': 'IMD quintile as text on IMD rows; NULL elsewhere per rider R-O3-6.'}),
    T.StructField('observation_event_id', T.LongType(), True, metadata={'comment': 'NULL in O3; event-modifier mechanics arrive with oncology staging in O4.'}),
    T.StructField('obs_event_field_concept_id', T.IntegerType(), True, metadata={'comment': '0 in O3.'}),
])
# === END GENERATED: explicit schema for observation ===

@dp.materialized_view(
    name="observation",
    schema=observation_schema,
    comment="OMOP OBSERVATION routed from Journey patient events including registry, family-history, and community lanes (mapped-only), plus HERON-UK IMD quintile rows.",
)
def observation():
    return _finish("observation", _observation_core())  # OGR_EXT_V1


def _observation_core():
    rows = _staged_rows("observation")
    finding = read_source("src_clinical_finding").select(
        "patient_event_id",
        F.col("performer_practitioner_id").alias("_cf_performer"),
    )
    score = read_source("src_clinical_score").select(
        "patient_event_id",
        F.col("value_number").cast("double").alias("_score_number"),
        F.col("unit_source_value").alias("_score_unit_source"),
        F.expr("try_cast(unit_concept_id AS int)").alias("_score_unit_concept"),
        F.col("performer_practitioner_id").alias("_cs_performer"),
    )
    community = read_source("src_community_care_activity").select(
        "patient_event_id",
        F.col("observation_value_numeric").cast("double").alias("_community_number"),
        F.col("unit_source_value").alias("_community_unit_source"),
        F.expr("try_cast(unit_concept_id AS int)").alias("_community_unit_concept"),
    )
    family = read_source("src_family_history").select(
        "patient_event_id",
        F.col("relationship_display").alias("_relationship_display"),
    )
    joined = (
        rows.join(finding, "patient_event_id", "left")
        .join(score, "patient_event_id", "left")
        .join(community, "patient_event_id", "left")
        .join(family, "patient_event_id", "left")
        .join(_value_role_concepts(), "patient_event_id", "left")
        .join(
            _published_provider_keys(),
            F.coalesce(F.col("_cf_performer"), F.col("_cs_performer"))
            == F.col("_pract_key"),
            "left",
        )
    )
    is_family = F.col("lane") == "family_history"
    value_number = F.when(
        F.col("mapping_count") <= 1,
        F.coalesce(F.col("_score_number"), F.col("_community_number")),
    )
    routed = joined.select(
        F.col("allocated_id").alias("observation_id"),
        "person_id",
        F.when(is_family, F.lit(FAMILY_HISTORY_CONCEPT))
        .otherwise(F.col("standard_concept_id")).cast("int")
        .alias("observation_concept_id"),
        F.to_date("event_datetime").alias("observation_date"),
        F.col("event_datetime").cast("timestamp").alias("observation_datetime"),
        F.col("type_concept_id").alias("observation_type_concept_id"),
        value_number.alias("value_as_number"),
        F.lit(None).cast("string").alias("value_as_string"),
        F.when(is_family, F.col("standard_concept_id"))
        .otherwise(F.coalesce(F.col("_value_concept"), F.lit(0))).cast("int")
        .alias("value_as_concept_id"),
        F.lit(0).cast("int").alias("qualifier_concept_id"),
        F.coalesce(F.col("_score_unit_concept"), F.col("_community_unit_concept"), F.lit(0))
        .cast("int").alias("unit_concept_id"),
        F.col("_resolved_provider_id").alias("provider_id"),
        "visit_occurrence_id",
        F.lit(None).cast("bigint").alias("visit_detail_id"),
        _safe_source_code(
            F.coalesce(F.col("source_code"), F.col("mapped_code")), "observation"
        ).alias("observation_source_value"),
        F.col("source_concept_id").alias("observation_source_concept_id"),
        F.coalesce(F.col("_score_unit_source"), F.col("_community_unit_source"))
        .cast("string").alias("unit_source_value"),
        F.when(is_family, F.col("_relationship_display")).cast("string")
        .alias("qualifier_source_value"),
        F.lit(None).cast("string").alias("value_source_value"),
        F.lit(None).cast("bigint").alias("observation_event_id"),
        F.lit(0).cast("int").alias("obs_event_field_concept_id"),
    )
    imd = _imd_base().where(F.col("_exclusion_reason").isNull()).drop("_exclusion_reason")
    return routed.unionByName(imd)


# === GENERATED: explicit schema for device_exposure (contract_codegen.py) — do not edit ===
device_exposure_schema = T.StructType([
    T.StructField('device_exposure_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for evt:<patient_event_id>:<standard_concept_id>; event-grain and mapping-release-stable; never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('device_concept_id', T.IntegerType(), False, metadata={'comment': 'Resolved standard Device concept; never 0 under the mapped-only policy.'}),
    T.StructField('device_exposure_start_date', T.DateType(), False, metadata={'comment': 'Event datetime date. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('device_exposure_start_datetime', T.TimestampType(), True, metadata={'comment': 'Event datetime. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('device_exposure_end_date', T.DateType(), True, metadata={'comment': 'Event end where supplied; clamped to not precede start. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('device_exposure_end_datetime', T.TimestampType(), True, metadata={'comment': 'Event end under the same convention. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('device_type_concept_id', T.IntegerType(), False, metadata={'comment': 'Governed per-feed type concept via omop_type_concept_map; 32817 default.'}),
    T.StructField('unique_device_id', T.StringType(), True, metadata={'comment': 'Namespace-prefixed device identifier: sn: serials (MediConnect/implants), udi: UDI-DI (implants), unit: blood-unit barcode (transfusions); prefixes stop numeric values mimicking NHS numbers.'}),
    T.StructField('production_id', T.StringType(), True, metadata={'comment': 'batch:-prefixed implant batch number where carried; NULL elsewhere.'}),
    T.StructField('quantity', T.IntegerType(), True, metadata={'comment': 'Implant quantity or integral transfusion quantity; NULL when the event produces more than one published standard concept in device_exposure or the source value is fractional.'}),
    T.StructField('provider_id', T.LongType(), True, metadata={'comment': 'Implant (procedure-kind) rows: performing practitioner resolved to a published provider; NULL for device and transfusion rows (no practitioner in silver) and when unresolved.'}),
    T.StructField('visit_occurrence_id', T.LongType(), True, metadata={'comment': "Published parent visit where the event's encounter resolves; NULL otherwise."}),
    T.StructField('visit_detail_id', T.LongType(), True, metadata={'comment': 'Published VISIT_DETAIL of the same visit whose interval contains the event datetime (critical care before ward stay, latest start, lowest id); NULL when none.'}),
    T.StructField('device_source_value', T.StringType(), True, metadata={'comment': 'Source code or model code; identifier-shaped 10-digit values are prefixed device: for D4 safety.'}),
    T.StructField('device_source_concept_id', T.IntegerType(), True, metadata={'comment': 'Best vocabulary-arm source concept for the event; 0 when only the urn arm exists.'}),
    T.StructField('unit_concept_id', T.IntegerType(), True, metadata={'comment': '0 in O3.'}),
    T.StructField('unit_source_value', T.StringType(), True, metadata={'comment': 'NULL in O3.'}),
    T.StructField('unit_source_concept_id', T.IntegerType(), True, metadata={'comment': '0 in O3.'}),
])
# === END GENERATED: explicit schema for device_exposure ===

@dp.materialized_view(
    name="device_exposure",
    schema=device_exposure_schema,
    comment="OMOP DEVICE_EXPOSURE for governed MediConnect, implant, and transfusion device concepts.",
)
def device_exposure():
    return _finish("device_exposure", _device_exposure_core())  # OGR_EXT_V1


def _device_exposure_core():
    rows = _staged_rows("device_exposure")
    device = read_source("src_device").select(
        "patient_event_id",
        F.col("serial_number").alias("_dev_serial"),
        F.col("model_code").alias("_model_code"),
    )
    implants = read_source("src_procedure").select(
        "patient_event_id",
        F.col("udi_di").alias("_udi_di"),
        F.col("serial_number").alias("_imp_serial"),
        F.col("batch_number").alias("_batch"),
        F.col("quantity").alias("_imp_quantity"),
        F.col("performer_practitioner_id").alias("_imp_performer"),  # OGR_DEVICE_PROVIDER_V1
    )
    transfusion = read_source("src_transfusion").select(
        "patient_event_id",
        F.col("unit_number").alias("_unit_number"),
        F.col("quantity_value").cast("double").alias("_tx_quantity"),
    )
    joined = (
        rows.join(device, "patient_event_id", "left")
        .join(implants, "patient_event_id", "left")
        .join(transfusion, "patient_event_id", "left")
        .join(_published_provider_keys(), F.col("_imp_performer") == F.col("_pract_key"), "left")  # OGR_DEVICE_PROVIDER_V1
    )
    start = F.col("event_datetime")
    end = _clamp_end(start, F.col("event_end_datetime"))
    unique_device = F.when(
        (F.col("fact_kind") == "device") & F.col("_dev_serial").isNotNull(),
        F.concat(F.lit("sn:"), F.col("_dev_serial")),
    ).when(
        (F.col("fact_kind") == "procedure") & F.col("_udi_di").isNotNull(),
        F.concat(F.lit("udi:"), F.col("_udi_di")),
    ).when(
        (F.col("fact_kind") == "procedure") & F.col("_imp_serial").isNotNull(),
        F.concat(F.lit("sn:"), F.col("_imp_serial")),
    ).when(
        (F.col("fact_kind") == "transfusion") & F.col("_unit_number").isNotNull(),
        F.concat(F.lit("unit:"), F.col("_unit_number")),
    )
    quantity = F.when(
        F.col("mapping_count") <= 1,
        F.when(F.col("fact_kind") == "procedure", F.col("_imp_quantity"))
        .when(
            (F.col("fact_kind") == "transfusion")
            & (F.col("_tx_quantity") == F.floor(F.col("_tx_quantity"))),
            F.col("_tx_quantity"),
        ),
    ).cast("int")
    return joined.select(
        F.col("allocated_id").alias("device_exposure_id"),
        "person_id",
        F.col("standard_concept_id").alias("device_concept_id"),
        F.to_date(start).alias("device_exposure_start_date"),
        start.cast("timestamp").alias("device_exposure_start_datetime"),
        F.to_date(end).alias("device_exposure_end_date"),
        end.cast("timestamp").alias("device_exposure_end_datetime"),
        F.col("type_concept_id").alias("device_type_concept_id"),
        unique_device.cast("string").alias("unique_device_id"),
        F.when(
            (F.col("fact_kind") == "procedure") & F.col("_batch").isNotNull(),
            F.concat(F.lit("batch:"), F.col("_batch")),
        ).cast("string").alias("production_id"),
        quantity.alias("quantity"),
        F.when(F.col("fact_kind") == "procedure", F.col("_resolved_provider_id")).cast("bigint").alias("provider_id"),  # OGR_DEVICE_PROVIDER_V1
        "visit_occurrence_id",
        F.lit(None).cast("bigint").alias("visit_detail_id"),
        _safe_source_code(
            F.coalesce(F.col("source_code"), F.col("_model_code"), F.col("mapped_code")),
            "device",
        ).alias("device_source_value"),
        F.col("source_concept_id").alias("device_source_concept_id"),
        F.lit(0).cast("int").alias("unit_concept_id"),
        F.lit(None).cast("string").alias("unit_source_value"),
        F.lit(0).cast("int").alias("unit_source_concept_id"),
    )


# COMMAND ----------

# O4 measurement domain.

# === GENERATED: explicit schema for measurement (contract_codegen.py) — do not edit ===
measurement_schema = T.StructType([
    T.StructField('measurement_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for evt:<patient_event_id>:<standard_concept_id>; event-grain and mapping-release-stable; stg:<patient_event_id> for oncology stage-group rows; pobs:<journey_pregnancy_key>:<item> for pregnancy history rows or bir:<birth_key>:<item> for birth rows (clinical_event space); never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('measurement_concept_id', T.IntegerType(), False, metadata={'comment': 'Resolved standard Measurement concept; never 0 under the mapped-only policy; single winner per event per R-O4-2; Cancer Modifier AJCC/UICC stage concept on stage-group rows.'}),
    T.StructField('measurement_date', T.DateType(), False, metadata={'comment': 'Event datetime date. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('measurement_datetime', T.TimestampType(), True, metadata={'comment': 'Event datetime. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('measurement_time', T.StringType(), True, metadata={'comment': 'NULL in O4 per rider R-O4-4; derivable from measurement_datetime.'}),
    T.StructField('measurement_type_concept_id', T.IntegerType(), False, metadata={'comment': 'Governed per-feed type concept via omop_type_concept_map; pathology feeds carry 32856.'}),
    T.StructField('operator_concept_id', T.IntegerType(), True, metadata={'comment': 'Standard Meas Value Operator concept resolved from the pathology operator field; 0 when absent or unresolvable.'}),
    T.StructField('value_as_number', T.DoubleType(), True, metadata={'comment': 'Pathology, vital-sign, or score numeric value; single-standard rule R-O4-2 keeps it populated.'}),
    T.StructField('value_as_concept_id', T.IntegerType(), True, metadata={'comment': 'Silver-resolved pathology value concept, else the vocabulary-resolved measurement_value role concept; 0 otherwise.'}),
    T.StructField('unit_concept_id', T.IntegerType(), True, metadata={'comment': 'Castable silver unit concept, else standard UCUM concept resolved from ucum_code; 0 otherwise.'}),
    T.StructField('range_low', T.DoubleType(), True, metadata={'comment': 'Pathology reference range low where carried.'}),
    T.StructField('range_high', T.DoubleType(), True, metadata={'comment': 'Pathology reference range high where carried.'}),
    T.StructField('provider_id', T.LongType(), True, metadata={'comment': 'Vital-sign performer resolved to a published provider; NULL otherwise.'}),
    T.StructField('visit_occurrence_id', T.LongType(), True, metadata={'comment': "Published parent visit where the event's encounter resolves; pathology linkage is sparse at source (19.4%) — tracked ask."}),
    T.StructField('visit_detail_id', T.LongType(), True, metadata={'comment': 'Published VISIT_DETAIL of the same visit whose interval contains the event datetime (critical care before ward stay, latest start, lowest id); NULL when none.'}),
    T.StructField('measurement_source_value', T.StringType(), True, metadata={'comment': 'Source code, falling back to the selected mapped code; identifier-shaped 10-digit values are prefixed measurement: for D4 safety.'}),
    T.StructField('measurement_source_concept_id', T.IntegerType(), True, metadata={'comment': 'Best vocabulary-arm source concept bound to the published standard; 0 when only the urn arm exists.'}),
    T.StructField('unit_source_value', T.StringType(), True, metadata={'comment': 'Silver unit source value where carried.'}),
    T.StructField('unit_source_concept_id', T.IntegerType(), True, metadata={'comment': '0 in O4; no source unit vocabulary concept.'}),
    T.StructField('value_source_value', T.StringType(), True, metadata={'comment': 'NULL in O4 per rider R-O4-4; pathology value_text stays outside the D4 surface.'}),
    T.StructField('measurement_event_id', T.LongType(), True, metadata={'comment': 'Stage-group rows: the published condition_occurrence of the same patient event (lowest id when it published several standards); NULL otherwise.'}),
    T.StructField('meas_event_field_concept_id', T.IntegerType(), True, metadata={'comment': '1147127 (condition_occurrence.condition_occurrence_id) when measurement_event_id is set; 0 otherwise.'}),
])
# === END GENERATED: explicit schema for measurement ===

@dp.materialized_view(
    name="measurement",
    schema=measurement_schema,
    cluster_by=["person_id", "measurement_datetime"],
    comment="OMOP MEASUREMENT routed from the Journey measurement lane plus cross-arrivals; mapped-only; one standard per event (R-O4-2).",
)
def measurement():
    return _finish("measurement", _measurement_core())  # OGR_EXT_V1


def _measurement_core():
    rows = _staged_rows("measurement")
    pathology = read_source("src_pathology_result").select(
        "patient_event_id",
        F.col("value_number").cast("double").alias("_path_number"),
        F.expr("try_cast(value_concept_id AS int)").alias("_path_value_concept"),
        F.col("operator_concept_id").alias("_operator_raw"),
        F.expr("try_cast(unit_concept_id AS int)").alias("_path_unit_concept"),
        F.col("ucum_code").alias("_path_ucum"),
        F.col("unit_source_value").alias("_path_unit_source"),
        F.col("reference_range_low").cast("double").alias("_range_low"),
        F.col("reference_range_high").cast("double").alias("_range_high"),
    )
    vital = read_source("src_vital_sign").select(
        "patient_event_id",
        F.col("value_number").cast("double").alias("_vital_number"),
        F.expr("try_cast(unit_concept_id AS int)").alias("_vital_unit_concept"),
        F.col("unit_source_value").alias("_vital_unit_source"),
        F.col("performer_practitioner_id").alias("_vital_performer"),
    )
    score = read_source("src_clinical_score").select(
        "patient_event_id",
        F.col("value_number").cast("double").alias("_meas_score_number"),
        F.expr("try_cast(unit_concept_id AS int)").alias("_meas_score_unit"),
        F.col("unit_source_value").alias("_meas_score_unit_source"),
    )
    joined = (
        rows.join(pathology, "patient_event_id", "left")
        .join(vital, "patient_event_id", "left")
        .join(score, "patient_event_id", "left")
        .join(_ucum_units(), F.col("_path_ucum") == F.col("_ucum_code"), "left")
        .join(_value_role_concepts(), "patient_event_id", "left")
        .join(
            _published_provider_keys(),
            F.col("_vital_performer") == F.col("_pract_key"),
            "left",
        )
    )
    operator_symbol_map = F.create_map(*[
        F.lit(item)
        for pair in OPERATOR_SYMBOL_CONCEPTS.items()
        for item in pair
    ])
    operator_int = F.expr("try_cast(_operator_raw AS int)")
    operator = (
        F.when(operator_int.isin(*OPERATOR_CONCEPT_IDS), operator_int)
        .otherwise(
            F.coalesce(
                operator_symbol_map[F.trim(F.col("_operator_raw"))], F.lit(0)
            )
        )
    )
    value_number = F.when(
        F.col("mapping_count") <= 1,
        F.coalesce(
            F.col("_path_number"), F.col("_vital_number"), F.col("_meas_score_number")
        ),
    )
    return joined.select(
        F.col("allocated_id").alias("measurement_id"),
        "person_id",
        F.col("standard_concept_id").alias("measurement_concept_id"),
        F.to_date("event_datetime").alias("measurement_date"),
        F.col("event_datetime").cast("timestamp").alias("measurement_datetime"),
        F.lit(None).cast("string").alias("measurement_time"),
        F.col("type_concept_id").alias("measurement_type_concept_id"),
        F.when(F.col("_operator_raw").isNotNull(), operator)
        .otherwise(F.lit(0)).cast("int").alias("operator_concept_id"),
        value_number.alias("value_as_number"),
        F.coalesce(
            F.col("_path_value_concept"), F.col("_value_concept"), F.lit(0)
        ).cast("int").alias("value_as_concept_id"),
        F.coalesce(
            F.col("_path_unit_concept"), F.col("_ucum_concept"),
            F.col("_vital_unit_concept"), F.col("_meas_score_unit"), F.lit(0)
        ).cast("int").alias("unit_concept_id"),
        F.col("_range_low").alias("range_low"),
        F.col("_range_high").alias("range_high"),
        F.col("_resolved_provider_id").alias("provider_id"),
        "visit_occurrence_id",
        F.lit(None).cast("bigint").alias("visit_detail_id"),
        _safe_source_code(
            F.coalesce(F.col("source_code"), F.col("mapped_code")), "measurement"
        ).alias("measurement_source_value"),
        F.col("source_concept_id").alias("measurement_source_concept_id"),
        F.coalesce(
            F.col("_path_unit_source"), F.col("_vital_unit_source"),
            F.col("_meas_score_unit_source")
        ).cast("string").alias("unit_source_value"),
        F.lit(0).cast("int").alias("unit_source_concept_id"),
        F.lit(None).cast("string").alias("value_source_value"),
        F.lit(None).cast("bigint").alias("measurement_event_id"),
        F.lit(0).cast("int").alias("meas_event_field_concept_id"),
    )


# COMMAND ----------

# O5 drug exposure and ingredient derivation.

def _drug_route_concepts():
    return read_source("lkp_route_map").select(
        F.col("route_text").alias("_route_text"),
        F.col("route_concept_id").cast("int").alias("_route_concept"),
    )


def _drug_unit_concepts():
    return read_source("lkp_dose_unit_map").select(
        F.col("unit_text").alias("_unit_text"),
        F.col("unit_concept_id").cast("int").alias("_unit_concept"),
        F.col("unit_is_amount").cast("boolean").alias("_unit_is_amount"),
    )


def _normalized_text(column):
    # Route and unit text can contain CR/LF. Use [\s] rather than dot so Photon
    # and Spark-JVM apply the same normalization.
    return F.lower(F.trim(F.regexp_replace(column.cast("string"), r"[\s]+", " ")))


def _drug_exposure_frame(keep_internals=False, transforms=None):
    """Published drug-exposure shape plus registered extensions (# OGR_EXT_V1).
    Transforms (visit-detail attach) are skipped for keep_internals and era inputs."""
    return _finish("drug_exposure", _drug_exposure_frame_core(keep_internals),
                   transforms=(not keep_internals) if transforms is None else transforms,
                   keep_internals=keep_internals)


def _drug_exposure_frame_core(keep_internals=False):
    """Build the published drug-exposure shape from the routed drug frame."""
    rows = _staged_rows("drug_exposure")
    admin = read_source("src_medication_admin").select(
        "patient_event_id",
        F.col("dose_value").cast("double").alias("_adm_dose"),
        F.col("dose_unit").alias("_adm_dose_unit"),
        F.coalesce(F.col("route_display"), F.col("route_code")).alias("_adm_route"),
        F.col("performer_practitioner_id").alias("_adm_performer"),
    )
    dispense = read_source("src_medication_dispense").select(
        "patient_event_id",
        F.col("quantity").cast("double").alias("_dsp_quantity"),
        F.col("quantity_unit").alias("_dsp_unit"),
    )
    expenditure = read_source("src_drug_expenditure").select(
        "patient_event_id",
        F.col("quantity").cast("double").alias("_exp_quantity"),
        F.col("unit_of_measure").alias("_exp_unit"),
        F.coalesce(F.col("route_of_administration"), F.col("dispensing_route"))
        .alias("_exp_route"),
    )
    sact = read_source("src_cancer_treatment").select(
        "patient_event_id",
        F.col("dose_value").cast("double").alias("_sact_dose"),
        F.col("dose_unit_code").alias("_sact_unit"),
        F.col("route_code").alias("_sact_route"),
        F.col("end_date").cast("timestamp").alias("_sact_end"),
    )
    joined = (
        rows.join(admin, "patient_event_id", "left")
        .join(dispense, "patient_event_id", "left")
        .join(expenditure, "patient_event_id", "left")
        .join(sact, "patient_event_id", "left")
        .withColumn(
            "_route_source",
            F.coalesce(F.col("_adm_route"), F.col("_exp_route"), F.col("_sact_route")),
        )
        .withColumn(
            "_unit_source",
            F.coalesce(
                F.col("_adm_dose_unit"), F.col("_dsp_unit"),
                F.col("_exp_unit"), F.col("_sact_unit"),
            ),
        )
        .join(
            _drug_route_concepts(),
            _normalized_text(F.col("_route_source")) == F.col("_route_text"),
            "left",
        )
        .join(
            _published_provider_keys(),
            F.col("_adm_performer") == F.col("_pract_key"),
            "left",
        )
    )
    start_ts = F.col("event_datetime").cast("timestamp")
    verbatim_end = F.coalesce(F.col("_sact_end"), F.col("event_end_datetime"))
    end_ts = _clamp_end(start_ts, verbatim_end)
    quantity = F.when(
        F.col("mapping_count") <= 1,
        F.when(F.col("event_type") == "medication_admin", F.col("_adm_dose"))
        .when(F.col("event_type") == "medication_dispense", F.col("_dsp_quantity"))
        .when(F.col("event_type") == "drug_expenditure", F.col("_exp_quantity"))
        .when(F.col("event_type") == "cancer_treatment", F.col("_sact_dose")),
    )
    return joined.select(
        F.col("allocated_id").alias("drug_exposure_id"),
        "person_id",
        F.col("standard_concept_id").alias("drug_concept_id"),
        F.to_date(start_ts).alias("drug_exposure_start_date"),
        start_ts.alias("drug_exposure_start_datetime"),
        F.to_date(end_ts).alias("drug_exposure_end_date"),
        end_ts.alias("drug_exposure_end_datetime"),
        F.to_date(verbatim_end).alias("verbatim_end_date"),
        F.col("type_concept_id").alias("drug_type_concept_id"),
        F.lit(None).cast("string").alias("stop_reason"),
        F.lit(None).cast("int").alias("refills"),
        quantity.cast("double").alias("quantity"),
        F.lit(None).cast("int").alias("days_supply"),
        F.lit(None).cast("string").alias("sig"),
        F.coalesce(F.col("_route_concept"), F.lit(0)).cast("int")
        .alias("route_concept_id"),
        F.lit(None).cast("string").alias("lot_number"),
        F.col("_resolved_provider_id").alias("provider_id"),
        "visit_occurrence_id",
        F.lit(None).cast("bigint").alias("visit_detail_id"),
        _safe_source_code(
            F.coalesce(F.col("source_code"), F.col("mapped_code")), "drug"
        ).alias("drug_source_value"),
        F.col("source_concept_id").alias("drug_source_concept_id"),
        F.col("_route_source").cast("string").alias("route_source_value"),
        F.col("_unit_source").cast("string").alias("dose_unit_source_value"),
        *([F.col("event_type").alias("_event_type")] if keep_internals else []),
    )


# === GENERATED: explicit schema for drug_exposure (contract_codegen.py) — do not edit ===
drug_exposure_schema = T.StructType([
    T.StructField('drug_exposure_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for evt:<patient_event_id>:<standard_concept_id>; event-grain and mapping-release-stable; or sup:<patient_event_id>:<standard_concept_id> for homecare supply rows; never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('drug_concept_id', T.IntegerType(), False, metadata={'comment': 'Resolved standard Drug concept; never 0 under the mapped-only policy; all standards from the winning arm are retained per R-O5-2.'}),
    T.StructField('drug_exposure_start_date', T.DateType(), False, metadata={'comment': 'Event datetime date. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('drug_exposure_start_datetime', T.TimestampType(), True, metadata={'comment': 'Event datetime. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('drug_exposure_end_date', T.DateType(), False, metadata={'comment': 'Source stop datetime where stated, else the start date per R-O5-7; clamped never to precede the start. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('drug_exposure_end_datetime', T.TimestampType(), True, metadata={'comment': 'Same convention as drug_exposure_end_date. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('verbatim_end_date', T.DateType(), True, metadata={'comment': 'Populated only where the source states an explicit stop; NULL where the end was coalesced from the start per R-O5-7.'}),
    T.StructField('drug_type_concept_id', T.IntegerType(), False, metadata={'comment': 'Governed per-feed type concept via omop_type_concept_map per R-O5-4; administrations 32818, pharmacy issues 32825, expenditure and SACT 32817; 32825 on homecare supply rows.'}),
    T.StructField('stop_reason', T.StringType(), True, metadata={'comment': 'NULL in O5; no routed drug feed carries a governed stop reason.'}),
    T.StructField('refills', T.IntegerType(), True, metadata={'comment': 'NULL in O5 per R-O5-6; no routed drug feed carries a refill count and it is never inferred.'}),
    T.StructField('quantity', T.DoubleType(), True, metadata={'comment': 'Source-stated quantity per R-O5-6: dispensed quantity, expenditure quantity, SACT delivered dose, or administered dose value; never inferred from strength.'}),
    T.StructField('days_supply', T.IntegerType(), True, metadata={'comment': 'NULL in O5; no routed feed carries a positively established days-supply value.'}),
    T.StructField('sig', T.StringType(), True, metadata={'comment': 'NULL in O5; directions are free text outside the D4 surface.'}),
    T.StructField('route_concept_id', T.IntegerType(), True, metadata={'comment': 'Standard Route concept from the governed omop_route_map per R-O5-5; 0 when normalized route text is unmapped.'}),
    T.StructField('lot_number', T.StringType(), True, metadata={'comment': 'NULL in O5; batch identifiers are not promoted to the CDM surface.'}),
    T.StructField('provider_id', T.LongType(), True, metadata={'comment': 'Administering performer resolved to a published provider; NULL otherwise.'}),
    T.StructField('visit_occurrence_id', T.LongType(), True, metadata={'comment': "Published parent visit where the event's encounter resolves."}),
    T.StructField('visit_detail_id', T.LongType(), True, metadata={'comment': 'Published VISIT_DETAIL of the same visit whose interval contains the event datetime (critical care before ward stay, latest start, lowest id); NULL when none.'}),
    T.StructField('drug_source_value', T.StringType(), True, metadata={'comment': 'Source code, falling back to the selected mapped code; identifier-shaped 10-digit values are prefixed drug: for D4 safety.'}),
    T.StructField('drug_source_concept_id', T.IntegerType(), True, metadata={'comment': 'Best vocabulary-arm source concept bound to the published standard; 0 when only the urn arm exists.'}),
    T.StructField('route_source_value', T.StringType(), True, metadata={'comment': 'Source route text preserved verbatim, mapped or not, per R-O5-5.'}),
    T.StructField('dose_unit_source_value', T.StringType(), True, metadata={'comment': 'Source dose unit text preserved verbatim, mapped or not, per R-O5-5.'}),
])
# === END GENERATED: explicit schema for drug_exposure ===

@dp.materialized_view(
    name="drug_exposure",
    schema=drug_exposure_schema,
    cluster_by=["person_id", "drug_exposure_start_datetime"],
    comment="OMOP DRUG_EXPOSURE routed from the Journey drug lane; mapped-only; every standard from the winning coding-system arm (R-O5-2).",
)
def drug_exposure():
    return _drug_exposure_frame()


def _drug_ingredients():
    """Resolve each standard Drug concept to its standard Ingredient ancestors."""
    ingredients = read_source("src_vocab_concept").where(
        (F.col("standard_concept") == "S")
        & (F.upper(F.col("domain_id")) == "DRUG")
        & (F.col("concept_class_id") == "Ingredient")
        & F.col("invalid_reason").isNull()
    ).select(F.col("concept_id").cast("int").alias("_ingredient_id"))
    return (
        read_source("src_vocab_concept_ancestor")
        .select(
            F.col("descendant_concept_id").cast("int").alias("_drug_concept"),
            F.col("ancestor_concept_id").cast("int").alias("_ancestor_id"),
        )
        .join(ingredients, F.col("_ancestor_id") == F.col("_ingredient_id"), "inner")
        .select(
            "_drug_concept",
            F.col("_ancestor_id").alias("_ingredient_concept"),
        )
        .distinct()
    )


def _eras(frame, group_columns, start_column, end_column):
    """Collapse intervals separated by no more than ERA_GAP_DAYS."""
    order = Window.partitionBy(*group_columns).orderBy(
        F.col(start_column).asc_nulls_last(), F.col(end_column).asc_nulls_last()
    )
    running = order.rowsBetween(Window.unboundedPreceding, -1)
    with_gap = (
        frame.withColumn("_prior_end", F.max(F.col(end_column)).over(running))
        .withColumn(
            "_new_era",
            F.when(F.col("_prior_end").isNull(), F.lit(1))
            .when(
                F.datediff(F.col(start_column), F.col("_prior_end"))
                > F.lit(ERA_GAP_DAYS),
                F.lit(1),
            )
            .otherwise(F.lit(0)),
        )
        .withColumn(
            "_era_index",
            F.sum("_new_era").over(
                order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )
    sub_order = Window.partitionBy(*group_columns, "_era_index").orderBy(
        F.col(start_column).asc_nulls_last(), F.col(end_column).asc_nulls_last()
    )
    merged = (
        with_gap.withColumn(
            "_sub_prior_end",
            F.max(F.col(end_column)).over(
                sub_order.rowsBetween(Window.unboundedPreceding, -1)
            ),
        )
        .withColumn(
            "_new_block",
            F.when(F.col("_sub_prior_end").isNull(), F.lit(1))
            .when(F.col(start_column) > F.col("_sub_prior_end"), F.lit(1))
            .otherwise(F.lit(0)),
        )
        .withColumn(
            "_block_index",
            F.sum("_new_block").over(
                sub_order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .groupBy(*group_columns, "_era_index", "_block_index")
        .agg(
            F.min(F.col(start_column)).alias("_block_start"),
            F.max(F.col(end_column)).alias("_block_end"),
            F.count(F.lit(1)).cast("int").alias("_block_facts"),
        )
    )
    return merged.groupBy(*group_columns, "_era_index").agg(
        F.min("_block_start").alias("_era_start"),
        F.max("_block_end").alias("_era_end"),
        F.sum("_block_facts").cast("int").alias("_fact_count"),
        F.sum(F.datediff(F.col("_block_end"), F.col("_block_start")) + F.lit(1))
        .cast("int").alias("_covered_days"),
    )


def _drug_era_candidates():
    return (
        _drug_exposure_frame(transforms=False)  # OGR_EXT_V1
        .select(
            F.col("drug_exposure_id").alias("_exposure_id"),
            "person_id",
            F.col("drug_concept_id").cast("int").alias("_drug_concept"),
            F.col("drug_exposure_start_date").alias("_start"),
            F.col("drug_exposure_end_date").alias("_end"),
        )
        .join(_drug_ingredients(), "_drug_concept", "left")
        .withColumn(
            "_era_reason",
            F.when(
                F.col("_ingredient_concept").isNull(),
                F.lit("ingredient_unresolved"),
            )
            .when(F.col("_start").isNull(), F.lit("era_start_missing"))
            .otherwise(F.lit(None).cast("string")),
        )
    )


# COMMAND ----------

# O5 drug, dose, and condition eras.

def _drug_era_frame():
    eligible = (
        _drug_era_candidates()
        .where(F.col("_era_reason").isNull())
        .select(
            "person_id",
            F.col("_ingredient_concept").alias("drug_concept_id"),
            "_start", "_end",
        )
    )
    return _eras(eligible, ["person_id", "drug_concept_id"], "_start", "_end")


def _single_ingredient_products():
    strength = (
        read_source("src_vocab_drug_strength")
        .select(
            F.col("drug_concept_id").cast("int").alias("_drug_concept"),
            F.col("ingredient_concept_id").cast("int").alias("_ingredient_concept"),
        )
        .where(F.col("_ingredient_concept").isNotNull())
        .distinct()
    )
    counts = strength.groupBy("_drug_concept").agg(
        F.countDistinct("_ingredient_concept").alias("_ingredient_n")
    )
    return (
        strength.join(counts, "_drug_concept", "inner")
        .where(F.col("_ingredient_n") == 1)
        .select("_drug_concept", "_ingredient_concept")
        .distinct()
    )


def _dose_era_candidates():
    exposures = _drug_exposure_frame(keep_internals=True).select(
        "person_id",
        F.col("drug_exposure_id").alias("_exposure_id"),
        F.col("drug_concept_id").cast("int").alias("_drug_concept"),
        F.col("_event_type"),
        F.col("quantity").cast("double").alias("_quantity"),
        F.col("dose_unit_source_value").alias("_unit_source"),
        F.col("drug_exposure_start_date").alias("_start"),
        F.col("drug_exposure_end_date").alias("_end"),
    )
    with_unit = exposures.join(
        _drug_unit_concepts(),
        _normalized_text(F.col("_unit_source")) == F.col("_unit_text"),
        "left",
    )
    with_ingredient = with_unit.join(
        _single_ingredient_products(), "_drug_concept", "left"
    )
    any_ingredient = (
        _drug_ingredients().select("_drug_concept").distinct()
        .withColumn("_has_any_ingredient", F.lit(True))
    )
    scored = with_ingredient.join(any_ingredient, "_drug_concept", "left")
    reason = (
        F.when(
            F.col("_event_type") != F.lit("medication_admin"),
            F.lit("dose_not_administration_feed"),
        )
        .when(F.col("_quantity").isNull(), F.lit("dose_missing"))
        .when(F.col("_quantity") <= 0, F.lit("dose_not_positive"))
        .when(F.col("_unit_source").isNull(), F.lit("dose_unit_absent"))
        .when(F.col("_unit_concept").isNull(), F.lit("dose_unit_unmapped"))
        .when(
            ~F.coalesce(F.col("_unit_is_amount"), F.lit(False)),
            F.lit("dose_unit_not_an_amount"),
        )
        .when(F.col("_has_any_ingredient").isNull(), F.lit("ingredient_unresolved"))
        .when(F.col("_ingredient_concept").isNull(), F.lit("dose_multi_ingredient"))
        .when(F.col("_start").isNull(), F.lit("era_start_missing"))
        .otherwise(F.lit(None).cast("string"))
    )
    return scored.withColumn("_dose_reason", reason)


def _dose_era_frame():
    eligible = _dose_era_candidates().where(F.col("_dose_reason").isNull()).select(
        "person_id", "_exposure_id",
        F.col("_ingredient_concept").alias("drug_concept_id"),
        F.col("_unit_concept").cast("int").alias("unit_concept_id"),
        F.col("_quantity").alias("_dose_amount"),
        "_start", "_end",
    )
    eras = _eras(
        eligible,
        ["person_id", "drug_concept_id", "unit_concept_id"],
        "_start", "_end",
    )
    totals = (
        eligible.alias("e")
        .join(
            eras.alias("r"),
            (F.col("e.person_id") == F.col("r.person_id"))
            & (F.col("e.drug_concept_id") == F.col("r.drug_concept_id"))
            & (F.col("e.unit_concept_id") == F.col("r.unit_concept_id"))
            & (F.col("e._start") >= F.col("r._era_start"))
            & (F.col("e._start") <= F.col("r._era_end")),
            "inner",
        )
        .groupBy(
            F.col("r.person_id").alias("person_id"),
            F.col("r.drug_concept_id").alias("drug_concept_id"),
            F.col("r.unit_concept_id").alias("unit_concept_id"),
            F.col("r._era_start").alias("_era_start"),
            F.col("r._era_end").alias("_era_end"),
            F.col("r._fact_count").alias("_fact_count"),
            F.col("r._covered_days").alias("_covered_days"),
        )
        # Floating-point SUM is partition-order dependent. Accumulate at fixed
        # decimal scale so a full refresh cannot silently rebind an era ID to a
        # slightly different daily dose.
        .agg(
            F.sum(F.col("e._dose_amount").cast("decimal(38,12)"))
            .alias("_total_amount")
        )
    )
    return totals.withColumn(
        "dose_value",
        F.col("_total_amount")
        / F.greatest(
            F.col("_covered_days").cast("decimal(38,12)"),
            F.lit(1).cast("decimal(38,12)"),
        ),
    ).drop("_total_amount")


def _condition_era_frame():
    rows = _staged_rows("condition_occurrence")
    enrich = read_source("src_condition").select(
        "patient_event_id",
        F.col("abatement_datetime").alias("_abatement"),
        F.col("event_datetime_local").alias("_start_local"),
        F.col("event_end_datetime_local").alias("_end_local"),
        F.col("abatement_datetime_local").alias("_abatement_local"),
    )
    joined = rows.join(enrich, "patient_event_id", "left")
    start = F.coalesce(F.col("_start_local"), F.col("event_datetime"))
    end = _clamp_end(start, F.when(F.col("_start_local").isNotNull(),
                                   F.coalesce(F.col("_abatement_local"), F.col("_end_local")))
                     .otherwise(F.coalesce(F.col("_abatement"), F.col("event_end_datetime"))))
    occurrences = joined.select(
        "person_id",
        F.col("standard_concept_id").cast("int").alias("condition_concept_id"),
        F.to_date(start).alias("_start"),
        F.to_date(end).alias("_end"),
    )
    occurrences = occurrences.unionByName(_extension_condition_occurrences())  # OGR_EXT_V1
    return _eras(
        occurrences, ["person_id", "condition_concept_id"], "_start", "_end"
    )


def _era_keys_frame():
    drug = _drug_era_frame().select(
        F.lit("drug_era").alias("era_kind"),
        F.concat_ws(
            ":", F.lit("dre"), F.col("person_id").cast("string"),
            F.col("drug_concept_id").cast("string"), F.col("_era_start").cast("string"),
        ).alias("source_key"),
        "person_id",
        F.col("drug_concept_id").cast("int").alias("concept_id"),
        F.lit(None).cast("int").alias("unit_concept_id"),
        F.lit(None).cast("double").alias("dose_value"),
        F.col("_era_start").alias("era_start_date"),
        F.col("_era_end").alias("era_end_date"),
        F.col("_fact_count").alias("fact_count"),
        F.col("_covered_days").alias("covered_days"),
    )
    dose = _dose_era_frame().select(
        F.lit("dose_era").alias("era_kind"),
        F.concat_ws(
            ":", F.lit("doe"), F.col("person_id").cast("string"),
            F.col("drug_concept_id").cast("string"),
            F.col("unit_concept_id").cast("string"),
            F.col("_era_start").cast("string"),
        ).alias("source_key"),
        "person_id",
        F.col("drug_concept_id").cast("int").alias("concept_id"),
        F.col("unit_concept_id").cast("int").alias("unit_concept_id"),
        F.col("dose_value").cast("double").alias("dose_value"),
        F.col("_era_start").alias("era_start_date"),
        F.col("_era_end").alias("era_end_date"),
        F.col("_fact_count").alias("fact_count"),
        F.col("_covered_days").alias("covered_days"),
    )
    condition = _condition_era_frame().select(
        F.lit("condition_era").alias("era_kind"),
        F.concat_ws(
            ":", F.lit("cre"), F.col("person_id").cast("string"),
            F.col("condition_concept_id").cast("string"),
            F.col("_era_start").cast("string"),
        ).alias("source_key"),
        "person_id",
        F.col("condition_concept_id").cast("int").alias("concept_id"),
        F.lit(None).cast("int").alias("unit_concept_id"),
        F.lit(None).cast("double").alias("dose_value"),
        F.col("_era_start").alias("era_start_date"),
        F.col("_era_end").alias("era_end_date"),
        F.col("_fact_count").alias("fact_count"),
        F.col("_covered_days").alias("covered_days"),
    )
    return drug.unionByName(dose).unionByName(condition)


@dp.temporary_view(
    name="_om_era_keys_current",
    comment="In-pipeline era-key calculation shared by staging, era outputs, and metrics.",
)
def _om_era_keys_current():
    return _era_keys_frame()


@dp.materialized_view(
    name="_om_era_keys",
    comment="Internal era staging: drug, dose, and condition eras with deterministic registry keys.",
)
def _om_era_keys():
    return spark.read.table("_om_era_keys_current")


def _era_registry(space):
    return (
        read_source("src_id_registry")
        .where(F.col("id_space") == space)
        .select(
            F.col("source_key").alias("_reg_key"),
            F.col("allocated_id").alias("_reg_id"),
        )
    )


def _era_base(kind, space):
    return (
        # Use the in-pipeline view, not the catalog materialization being
        # replaced in this update. Reading the latter made downstream flows
        # request a retired __materialization_mat_* table immediately after
        # _om_era_keys completed.
        spark.read.table("_om_era_keys_current")
        .where(F.col("era_kind") == kind)
        .join(_era_registry(space), F.col("source_key") == F.col("_reg_key"), "left")
        .withColumn(
            "_exclusion_reason",
            F.when(F.col("_reg_id").isNull(), F.lit("id_not_allocated")),
        )
    )


# === GENERATED: explicit schema for drug_era (contract_codegen.py) — do not edit ===
drug_era_schema = T.StructType([
    T.StructField('drug_era_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for dre:<person_id>:<ingredient_concept_id>:<era_start_date>; never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('drug_concept_id', T.IntegerType(), False, metadata={'comment': 'Standard RxNorm ingredient concept resolved through concept_ancestor; never a clinical drug.'}),
    T.StructField('drug_era_start_date', T.DateType(), False, metadata={'comment': 'Earliest contributing exposure start in the era.'}),
    T.StructField('drug_era_end_date', T.DateType(), False, metadata={'comment': 'Latest contributing exposure end in the era.'}),
    T.StructField('drug_exposure_count', T.IntegerType(), True, metadata={'comment': 'Count of published DRUG_EXPOSURE rows contributing to the era.'}),
    T.StructField('gap_days', T.IntegerType(), True, metadata={'comment': 'Days inside the era interval not covered by a contributing exposure.'}),
])
# === END GENERATED: explicit schema for drug_era ===

@dp.materialized_view(
    name="drug_era",
    schema=drug_era_schema,
    cluster_by=["person_id", "drug_era_start_date"],
    comment="OMOP DRUG_ERA by ingredient with a 30-day persistence window (R-O5-9).",
)
def drug_era():
    base = _era_base("drug_era", "drug_era").where(
        F.col("_exclusion_reason").isNull()
    )
    return base.select(
        F.col("_reg_id").alias("drug_era_id"),
        "person_id",
        F.col("concept_id").alias("drug_concept_id"),
        F.col("era_start_date").alias("drug_era_start_date"),
        F.col("era_end_date").alias("drug_era_end_date"),
        F.col("fact_count").alias("drug_exposure_count"),
        F.greatest(
            F.datediff(F.col("era_end_date"), F.col("era_start_date"))
            + F.lit(1) - F.col("covered_days"),
            F.lit(0),
        ).cast("int").alias("gap_days"),
    )


# === GENERATED: explicit schema for dose_era (contract_codegen.py) — do not edit ===
dose_era_schema = T.StructType([
    T.StructField('dose_era_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for doe:<person_id>:<ingredient_concept_id>:<unit_concept_id>:<era_start_date>; never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('drug_concept_id', T.IntegerType(), False, metadata={'comment': 'Standard RxNorm ingredient concept resolved through concept_ancestor.'}),
    T.StructField('unit_concept_id', T.IntegerType(), False, metadata={'comment': 'Standard amount-unit concept from the governed omop_dose_unit_map.'}),
    T.StructField('dose_value', T.DoubleType(), False, metadata={'comment': 'Daily ingredient dose: total administered amount divided by covered days; administration feed, single-ingredient products, and amount units only.'}),
    T.StructField('dose_era_start_date', T.DateType(), False, metadata={'comment': 'Earliest contributing exposure start in the era.'}),
    T.StructField('dose_era_end_date', T.DateType(), False, metadata={'comment': 'Latest contributing exposure end in the era.'}),
])
# === END GENERATED: explicit schema for dose_era ===

@dp.materialized_view(
    name="dose_era",
    schema=dose_era_schema,
    cluster_by=["person_id", "dose_era_start_date"],
    comment="OMOP DOSE_ERA daily ingredient dose with a 30-day persistence window (R-O5-9).",
)
def dose_era():
    base = _era_base("dose_era", "dose_era").where(
        F.col("_exclusion_reason").isNull()
    )
    return base.select(
        F.col("_reg_id").alias("dose_era_id"),
        "person_id",
        F.col("concept_id").alias("drug_concept_id"),
        F.col("unit_concept_id").cast("int").alias("unit_concept_id"),
        F.col("dose_value").cast("double").alias("dose_value"),
        F.col("era_start_date").alias("dose_era_start_date"),
        F.col("era_end_date").alias("dose_era_end_date"),
    )


# === GENERATED: explicit schema for condition_era (contract_codegen.py) — do not edit ===
condition_era_schema = T.StructType([
    T.StructField('condition_era_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for cre:<person_id>:<condition_concept_id>:<era_start_date>; never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('condition_concept_id', T.IntegerType(), False, metadata={'comment': 'Standard Condition concept carried unchanged from CONDITION_OCCURRENCE.'}),
    T.StructField('condition_era_start_date', T.DateType(), False, metadata={'comment': 'Earliest contributing occurrence start in the era.'}),
    T.StructField('condition_era_end_date', T.DateType(), False, metadata={'comment': 'Latest contributing occurrence end in the era.'}),
    T.StructField('condition_occurrence_count', T.IntegerType(), True, metadata={'comment': 'Count of published CONDITION_OCCURRENCE rows contributing to the era.'}),
])
# === END GENERATED: explicit schema for condition_era ===

@dp.materialized_view(
    name="condition_era",
    schema=condition_era_schema,
    cluster_by=["person_id", "condition_era_start_date"],
    comment="OMOP CONDITION_ERA with a 30-day persistence window (R-O5-9).",
)
def condition_era():
    base = _era_base("condition_era", "condition_era").where(
        F.col("_exclusion_reason").isNull()
    )
    return base.select(
        F.col("_reg_id").alias("condition_era_id"),
        "person_id",
        F.col("concept_id").alias("condition_concept_id"),
        F.col("era_start_date").alias("condition_era_start_date"),
        F.col("era_end_date").alias("condition_era_end_date"),
        F.col("fact_count").alias("condition_occurrence_count"),
    )


# COMMAND ----------

# O4 specimen and cross-domain relationships.

def _specimen_base():
    source = read_source("src_specimen").select(
        F.col("patient_event_id").alias("_source_event"),
        "record_status", "identity_status",
        F.expr("try_cast(person_id AS bigint)").alias("_person_candidate"),
        F.col("event_datetime_local").alias("_specimen_dt"),
        F.col("specimen_type_code").alias("_type_code"),
        F.col("body_site_code").alias("_site_code"),
        F.expr("try_cast(specimen_type_omop_concept_id AS int)").alias("_type_concept_candidate"),
    )
    # STM_SPECIMEN_TYPE_V1/omop-specimen: silver's specimen-type concept is kept only when it is a valid standard Specimen concept.
    specimen_concepts = read_source("src_vocab_concept").where(
        (F.col("standard_concept") == "S")
        & (F.col("domain_id") == "Specimen")
        & F.col("invalid_reason").isNull()
    ).select(F.col("concept_id").cast("int").alias("_specimen_concept"))
    registry = (
        read_source("src_id_registry")
        .where(F.col("id_space") == "specimen")
        .select(
            F.col("source_key").alias("_registry_key"),
            F.col("allocated_id").alias("_allocated_id"),
        )
    )
    published_people = (
        _person_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(F.col("person_id").alias("_published_person_id"))
    )
    joined = (
        source
        .withColumn(
            "_registry_source_key",
            F.concat(F.lit("spm:"), F.col("_source_event")),
        )
        .join(registry, F.col("_registry_source_key") == F.col("_registry_key"), "left")
        .join(F.broadcast(specimen_concepts), F.col("_type_concept_candidate") == F.col("_specimen_concept"), "left")
        .join(
            published_people,
            F.col("_person_candidate") == F.col("_published_person_id"),
            "left",
        )
    )
    return joined.select(
        F.col("_allocated_id").alias("specimen_id"),
        F.col("_published_person_id").alias("person_id"),
        F.coalesce(F.col("_specimen_concept"), F.lit(0)).cast("int").alias("specimen_concept_id"),
        F.lit(32856).cast("int").alias("specimen_type_concept_id"),
        F.to_date("_specimen_dt").alias("specimen_date"),
        F.col("_specimen_dt").cast("timestamp").alias("specimen_datetime"),
        F.lit(None).cast("double").alias("quantity"),
        F.lit(0).cast("int").alias("unit_concept_id"),
        F.lit(0).cast("int").alias("anatomic_site_concept_id"),
        F.lit(0).cast("int").alias("disease_status_concept_id"),
        F.col("_source_event").cast("string").alias("specimen_source_id"),
        _safe_source_code(F.col("_type_code"), "specimen").alias("specimen_source_value"),
        F.lit(None).cast("string").alias("unit_source_value"),
        _safe_source_code(F.col("_site_code"), "specimen").alias("anatomic_site_source_value"),
        F.lit(None).cast("string").alias("disease_status_source_value"),
        F.col("_source_event"),
        F.when(F.coalesce(F.col("record_status"), F.lit("")) != "active",
               F.lit("record_status_not_active"))
        .when(F.coalesce(F.col("identity_status"), F.lit("")) != "resolved",
              F.lit("identity_unresolved"))
        .when(F.col("_specimen_dt").isNull(), F.lit("missing_specimen_datetime"))
        .when(F.to_date("_specimen_dt") < F.to_date(F.lit(STUDY_START)),
              F.lit("before_study_window"))
        .when(F.col("_person_candidate").isNull(), F.lit("person_unresolved"))
        .when(F.col("_published_person_id").isNull(), F.lit("person_not_published"))
        .when(F.col("_allocated_id").isNull(), F.lit("id_not_allocated"))
        .alias("_exclusion_reason"),
    )


# === GENERATED: explicit schema for specimen (contract_codegen.py) — do not edit ===
specimen_schema = T.StructType([
    T.StructField('specimen_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for spm:<patient_event_id>; never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('specimen_concept_id', T.IntegerType(), False, metadata={'comment': 'Standard Specimen-domain concept from clinical_specimen.specimen_type_omop_concept_id (3_lookup.omop.specimen_type_snomed_map); 0 when unmapped, non-standard or outside domain Specimen (R-O4-5 revised 2026-09-28).'}),
    T.StructField('specimen_type_concept_id', T.IntegerType(), False, metadata={'comment': '32856 Lab for all rows.'}),
    T.StructField('specimen_date', T.DateType(), False, metadata={'comment': 'clinical_specimen event_datetime_local date (sample, else request, else report time). Europe/London wall-clock time (silver *_local).'}),
    T.StructField('specimen_datetime', T.TimestampType(), True, metadata={'comment': 'clinical_specimen event_datetime_local (sample, else request, else report time). Europe/London wall-clock time (silver *_local).'}),
    T.StructField('quantity', T.DoubleType(), True, metadata={'comment': 'NULL in O4; Journey carries no specimen quantity.'}),
    T.StructField('unit_concept_id', T.IntegerType(), True, metadata={'comment': '0 in O4.'}),
    T.StructField('anatomic_site_concept_id', T.IntegerType(), True, metadata={'comment': '0 in O4 per rider R-O4-5; body-site SNOMED is absent at source.'}),
    T.StructField('disease_status_concept_id', T.IntegerType(), True, metadata={'comment': '0 in O4; no source assertion.'}),
    T.StructField('specimen_source_id', T.StringType(), True, metadata={'comment': 'Journey patient_event_id surrogate for the specimen; never a lab number or accession.'}),
    T.StructField('specimen_source_value', T.StringType(), True, metadata={'comment': 'Local specimen type code; identifier-shaped 10-digit values are prefixed specimen: for D4 safety.'}),
    T.StructField('unit_source_value', T.StringType(), True, metadata={'comment': 'NULL in O4.'}),
    T.StructField('anatomic_site_source_value', T.StringType(), True, metadata={'comment': 'Local body-site code where carried.'}),
    T.StructField('disease_status_source_value', T.StringType(), True, metadata={'comment': 'NULL in O4.'}),
])
# === END GENERATED: explicit schema for specimen ===

@dp.materialized_view(
    name="specimen",
    schema=specimen_schema,
    comment="OMOP SPECIMEN projected structurally from Journey clinical_specimen; concept from the governed specimen-type map (R-O4-5 revised); registry IDs.",
)
def specimen():
    return (
        _specimen_base()
        .where(F.col("_exclusion_reason").isNull())
        .drop("_source_event", "_exclusion_reason")
    )
def _measurement_specimen_pairs():
    measurements = (
        _staged_rows("measurement")
        .where(F.col("fact_kind").isin("pathology_result", "pathology_order"))
        .select("patient_event_id", F.col("allocated_id").alias("_meas_id"))
    )
    links = (
        read_source("src_pathology_result")
        .select("patient_event_id", F.col("specimen_id").alias("_specimen_event"))
        .unionByName(
            read_source("src_pathology_order")
            .select("patient_event_id", F.col("specimen_id").alias("_specimen_event"))
        )
        .where(F.col("_specimen_event").isNotNull())
        .distinct()
    )
    published_specimens = (
        _specimen_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(
            F.col("_source_event").alias("_specimen_event"),
            F.col("specimen_id").alias("_spec_id"),
        )
    )
    return (
        measurements.join(links, "patient_event_id", "inner")
        .join(published_specimens, "_specimen_event", "left")
        .select(
            "_meas_id", "_spec_id",
            F.when(F.col("_spec_id").isNull(), F.lit("specimen_not_published"))
            .alias("_exclusion_reason"),
        )
        .distinct()
    )
# === GENERATED: explicit schema for fact_relationship (contract_codegen.py) — do not edit ===
fact_relationship_schema = T.StructType([
    T.StructField('domain_concept_id_1', T.IntegerType(), False, metadata={'comment': 'Domain of fact 1: 56 Person, 21 Measurement, or 36 Specimen.'}),
    T.StructField('fact_id_1', T.LongType(), False, metadata={'comment': 'Published person_id.'}),
    T.StructField('domain_concept_id_2', T.IntegerType(), False, metadata={'comment': 'Domain of fact 2: 56 Person, 21 Measurement, or 36 Specimen.'}),
    T.StructField('fact_id_2', T.LongType(), False, metadata={'comment': 'Published person_id.'}),
    T.StructField('relationship_concept_id', T.IntegerType(), False, metadata={'comment': '4248584 Mother / 4285883 Child, or 32668 Measurement-to-Specimen / 32669 Specimen-to-Measurement.'}),
])
# === END GENERATED: explicit schema for fact_relationship ===

@dp.materialized_view(
    name="fact_relationship",
    schema=fact_relationship_schema,
    comment="OMOP FACT_RELATIONSHIP: mother-child person pairs and measurement-specimen pairs, both directions.",
)
def fact_relationship():
    pairs = _fact_relationship_base().where(F.col("_exclusion_reason").isNull())
    mother_rows = pairs.select(
        F.lit(56).cast("int").alias("domain_concept_id_1"),
        F.col("_mother_id").cast("bigint").alias("fact_id_1"),
        F.lit(56).cast("int").alias("domain_concept_id_2"),
        F.col("_child_id").cast("bigint").alias("fact_id_2"),
        F.lit(4248584).cast("int").alias("relationship_concept_id"),
    )
    child_rows = pairs.select(
        F.lit(56).cast("int").alias("domain_concept_id_1"),
        F.col("_child_id").cast("bigint").alias("fact_id_1"),
        F.lit(56).cast("int").alias("domain_concept_id_2"),
        F.col("_mother_id").cast("bigint").alias("fact_id_2"),
        F.lit(4285883).cast("int").alias("relationship_concept_id"),
    )
    specimen_pairs = _measurement_specimen_pairs().where(F.col("_exclusion_reason").isNull())
    meas_to_spec = specimen_pairs.select(
        F.lit(21).cast("int").alias("domain_concept_id_1"),
        F.col("_meas_id").cast("bigint").alias("fact_id_1"),
        F.lit(36).cast("int").alias("domain_concept_id_2"),
        F.col("_spec_id").cast("bigint").alias("fact_id_2"),
        F.lit(32668).cast("int").alias("relationship_concept_id"),
    )
    spec_to_meas = specimen_pairs.select(
        F.lit(36).cast("int").alias("domain_concept_id_1"),
        F.col("_spec_id").cast("bigint").alias("fact_id_1"),
        F.lit(21).cast("int").alias("domain_concept_id_2"),
        F.col("_meas_id").cast("bigint").alias("fact_id_2"),
        F.lit(32669).cast("int").alias("relationship_concept_id"),
    )
    return (
        mother_rows.unionByName(child_rows)
        .unionByName(meas_to_spec)
        .unionByName(spec_to_meas)
    )


# COMMAND ----------

# O4 genomic extension.

def _genomic_registry(space):
    return (
        read_source("src_id_registry")
        .where(F.col("id_space") == space)
        .select(
            F.col("source_key").alias("_registry_key"),
            F.col("allocated_id").alias("_allocated_id"),
        )
    )


def _genomic_test_base():
    # The silver test table's unique key is patient_event_id (no genomic_test_id
    # column exists); child tables' genomic_test_id references this value.
    source = read_source("src_genomic_test").select(
        F.col("patient_event_id").alias("_source_test_id"),
        "record_status", "identity_status", "is_current",
        F.expr("try_cast(person_id AS bigint)").alias("_person_candidate"),
        F.col("event_datetime_local").alias("event_datetime"),
        "assay_code", "assay_name", "method", "analysis_context",
        "overall_result_status", "panel_code", "panel_version",
        "panel_version_inferred", "test_snomed_code", "test_loinc_code",
        F.col("specimen_id").alias("_specimen_event"),
        "research_qi_only",
    )
    published_people = (
        _person_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(F.col("person_id").alias("_published_person_id"))
    )
    published_specimens = (
        _specimen_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(
            F.col("_source_event").alias("_specimen_event"),
            F.col("specimen_id").alias("_published_specimen_id"),
        )
    )
    joined = (
        source
        .withColumn(
            "_registry_source_key",
            F.concat(F.lit("gt:"), F.col("_source_test_id")),
        )
        .join(_genomic_registry("genomic_test"),
              F.col("_registry_source_key") == F.col("_registry_key"), "left")
        .join(published_people,
              F.col("_person_candidate") == F.col("_published_person_id"), "left")
        .join(published_specimens, "_specimen_event", "left")
    )
    return joined.select(
        F.col("_allocated_id").alias("genomic_test_id"),
        F.col("_published_person_id").alias("person_id"),
        F.to_date("event_datetime").alias("test_date"),
        F.col("event_datetime").cast("timestamp").alias("test_datetime"),
        F.col("assay_code").cast("string").alias("assay_code"),
        F.col("assay_name").cast("string").alias("assay_name"),
        F.col("method").cast("string").alias("method"),
        F.col("analysis_context").cast("string").alias("analysis_context"),
        F.col("overall_result_status").cast("string").alias("overall_result_status"),
        F.col("panel_code").cast("string").alias("panel_code"),
        F.col("panel_version").cast("string").alias("panel_version"),
        F.col("panel_version_inferred").cast("boolean").alias("panel_version_inferred"),
        F.col("test_snomed_code").cast("string").alias("test_snomed_code"),
        F.col("test_loinc_code").cast("string").alias("test_loinc_code"),
        F.col("_published_specimen_id").alias("specimen_id"),
        F.col("research_qi_only").cast("boolean").alias("research_qi_only"),
        F.col("_source_test_id").cast("string").alias("test_source_value"),
        F.when(F.coalesce(F.col("record_status"), F.lit("")) != "active",
               F.lit("record_status_not_active"))
        .when(~F.coalesce(F.col("is_current"), F.lit(False)), F.lit("not_current"))
        .when(F.coalesce(F.col("identity_status"), F.lit("")) != "resolved",
              F.lit("identity_unresolved"))
        .when(F.col("event_datetime").isNull(), F.lit("missing_event_datetime"))
        .when(F.to_date("event_datetime") < F.to_date(F.lit(STUDY_START)),
              F.lit("before_study_window"))
        .when(F.col("_person_candidate").isNull(), F.lit("person_unresolved"))
        .when(F.col("_published_person_id").isNull(), F.lit("person_not_published"))
        .when(F.col("_allocated_id").isNull(), F.lit("id_not_allocated"))
        .alias("_exclusion_reason"),
    )


# === GENERATED: explicit schema for genomic_test (contract_codegen.py) — do not edit ===
genomic_test_schema = T.StructType([
    T.StructField('genomic_test_id', T.LongType(), False, metadata={'comment': "Persistent registry allocation for gt:<patient_event_id> (the silver test table's unique key, which child tables reference as genomic_test_id); never re-minted."}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('test_date', T.DateType(), False, metadata={'comment': 'Event datetime date. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('test_datetime', T.TimestampType(), True, metadata={'comment': 'Event datetime. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('assay_code', T.StringType(), True, metadata={'comment': 'Source assay code.'}),
    T.StructField('assay_name', T.StringType(), True, metadata={'comment': 'Source assay display name.'}),
    T.StructField('method', T.StringType(), True, metadata={'comment': 'Assay method label.'}),
    T.StructField('analysis_context', T.StringType(), True, metadata={'comment': 'Analysis context label.'}),
    T.StructField('overall_result_status', T.StringType(), True, metadata={'comment': 'Overall report status.'}),
    T.StructField('panel_code', T.StringType(), True, metadata={'comment': 'Panel code where carried.'}),
    T.StructField('panel_version', T.StringType(), True, metadata={'comment': 'Panel version where carried.'}),
    T.StructField('panel_version_inferred', T.BooleanType(), True, metadata={'comment': 'True when the panel version was inferred rather than reported.'}),
    T.StructField('test_snomed_code', T.StringType(), True, metadata={'comment': 'Source SNOMED test code where carried.'}),
    T.StructField('test_loinc_code', T.StringType(), True, metadata={'comment': 'Source LOINC test code where carried.'}),
    T.StructField('specimen_id', T.LongType(), True, metadata={'comment': 'Published SPECIMEN foreign key where the source specimen resolves; NULL otherwise.'}),
    T.StructField('research_qi_only', T.BooleanType(), True, metadata={'comment': 'Source research/QI-only governance flag, retained verbatim.'}),
    T.StructField('test_source_value', T.StringType(), True, metadata={'comment': 'Journey patient_event_id surrogate for the test; the value child tables reference as genomic_test_id.'}),
])
# === END GENERATED: explicit schema for genomic_test ===

@dp.materialized_view(
    name="genomic_test",
    schema=genomic_test_schema,
    comment="Journey-projected genomic assay reports (D8, R-O4-8); registry IDs; person-gated.",
)
def genomic_test():
    return (
        _genomic_test_base()
        .where(F.col("_exclusion_reason").isNull())
        .drop("_exclusion_reason")
    )


def _gene_tested_base():
    source = read_source("src_gene_tested").select(
        F.col("gene_tested_row_id").alias("_source_row_id"),
        F.col("genomic_test_id").alias("_source_test_id"),
        "hgnc_id", "reported_gene_symbol", "normalized_gene_symbol",
        "alias_match_type", "evidence_type", "test_scope",
        "panel_version_inferred", "confidence",
    )
    published_tests = (
        _genomic_test_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(
            F.col("test_source_value").alias("_source_test_id"),
            F.col("genomic_test_id").alias("_published_test_id"),
        )
    )
    joined = (
        source
        .withColumn(
            "_registry_source_key",
            F.concat(F.lit("ggt:"), F.col("_source_row_id")),
        )
        .join(_genomic_registry("gene_tested"),
              F.col("_registry_source_key") == F.col("_registry_key"), "left")
        .join(published_tests, "_source_test_id", "left")
    )
    return joined.select(
        F.col("_allocated_id").alias("gene_tested_id"),
        F.col("_published_test_id").alias("genomic_test_id"),
        F.col("hgnc_id").cast("string").alias("hgnc_id"),
        F.col("reported_gene_symbol").cast("string").alias("reported_gene_symbol"),
        F.col("normalized_gene_symbol").cast("string").alias("normalized_gene_symbol"),
        F.col("alias_match_type").cast("string").alias("alias_match_type"),
        F.col("evidence_type").cast("string").alias("evidence_type"),
        F.col("test_scope").cast("string").alias("test_scope"),
        F.col("panel_version_inferred").cast("boolean").alias("panel_version_inferred"),
        F.col("confidence").cast("double").alias("confidence"),
        F.when(F.col("_published_test_id").isNull(), F.lit("parent_test_not_published"))
        .when(F.col("_allocated_id").isNull(), F.lit("id_not_allocated"))
        .alias("_exclusion_reason"),
    )


# === GENERATED: explicit schema for genomic_gene_tested (contract_codegen.py) — do not edit ===
genomic_gene_tested_schema = T.StructType([
    T.StructField('gene_tested_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for ggt:<gene_tested_row_id>; never re-minted.'}),
    T.StructField('genomic_test_id', T.LongType(), False, metadata={'comment': 'Published GENOMIC_TEST foreign key.'}),
    T.StructField('hgnc_id', T.StringType(), True, metadata={'comment': 'HGNC gene identifier.'}),
    T.StructField('reported_gene_symbol', T.StringType(), True, metadata={'comment': 'Gene symbol as reported.'}),
    T.StructField('normalized_gene_symbol', T.StringType(), True, metadata={'comment': 'Normalized gene symbol.'}),
    T.StructField('alias_match_type', T.StringType(), True, metadata={'comment': 'How the symbol matched the HGNC alias set.'}),
    T.StructField('evidence_type', T.StringType(), True, metadata={'comment': "Evidence category for the gene's inclusion."}),
    T.StructField('test_scope', T.StringType(), True, metadata={'comment': 'Panel scope label.'}),
    T.StructField('panel_version_inferred', T.BooleanType(), True, metadata={'comment': 'True when the panel version was inferred.'}),
    T.StructField('confidence', T.DoubleType(), True, metadata={'comment': 'Upstream match confidence.'}),
])
# === END GENERATED: explicit schema for genomic_gene_tested ===

@dp.materialized_view(
    name="genomic_gene_tested",
    schema=genomic_gene_tested_schema,
    comment="Journey-projected genomic panel coverage (D8, R-O4-8); registry IDs.",
)
def genomic_gene_tested():
    return (
        _gene_tested_base()
        .where(F.col("_exclusion_reason").isNull())
        .drop("_exclusion_reason")
    )


def _genomic_variant_base():
    source = read_source("src_genomic_result").select(
        F.col("fact_row_id").alias("_source_row_id"),
        F.col("genomic_test_id").alias("_source_test_id"),
        "record_status", "identity_status", "is_current",
        F.expr("try_cast(person_id AS bigint)").alias("_person_candidate"),
        F.col("event_datetime_local").alias("event_datetime"),
        "hgnc_id", "normalized_gene_symbol", "partner_hgnc_id", "partner_gene_symbol",
        "alteration_type", "detection_status",
        F.col("hgvs_c_parsed").alias("_hgvs_c"),
        F.col("hgvs_p_parsed").alias("_hgvs_p"),
        "transcript", "hgvs_validation_status", "genome_build", "chromosome",
        "position_start", "position_end", "vaf", "zygosity",
        "reported_classification", "reported_tier", "copy_number",
        F.expr("try_cast(omop_genomic_concept_id AS int)").alias("_omop_genomic"),
        "snomed_code",
        F.col("specimen_id").alias("_specimen_event"),
    )
    published_people = (
        _person_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(F.col("person_id").alias("_published_person_id"))
    )
    published_tests = (
        _genomic_test_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(
            F.col("test_source_value").alias("_source_test_id"),
            F.col("genomic_test_id").alias("_published_test_id"),
        )
    )
    published_specimens = (
        _specimen_base()
        .where(F.col("_exclusion_reason").isNull())
        .select(
            F.col("_source_event").alias("_specimen_event"),
            F.col("specimen_id").alias("_published_specimen_id"),
        )
    )
    joined = (
        source
        .withColumn(
            "_registry_source_key",
            F.concat(F.lit("gv:"), F.col("_source_row_id")),
        )
        .join(_genomic_registry("genomic_variant"),
              F.col("_registry_source_key") == F.col("_registry_key"), "left")
        .join(published_people,
              F.col("_person_candidate") == F.col("_published_person_id"), "left")
        .join(published_tests, "_source_test_id", "left")
        .join(published_specimens, "_specimen_event", "left")
    )
    return joined.select(
        F.col("_allocated_id").alias("genomic_variant_id"),
        F.col("_published_person_id").alias("person_id"),
        F.col("_published_test_id").alias("genomic_test_id"),
        F.to_date("event_datetime").alias("variant_date"),
        F.col("event_datetime").cast("timestamp").alias("variant_datetime"),
        F.col("hgnc_id").cast("string").alias("hgnc_id"),
        F.col("normalized_gene_symbol").cast("string").alias("normalized_gene_symbol"),
        F.col("partner_hgnc_id").cast("string").alias("partner_hgnc_id"),
        F.col("partner_gene_symbol").cast("string").alias("partner_gene_symbol"),
        F.col("alteration_type").cast("string").alias("alteration_type"),
        F.col("detection_status").cast("string").alias("detection_status"),
        F.col("_hgvs_c").cast("string").alias("hgvs_c"),
        F.col("_hgvs_p").cast("string").alias("hgvs_p"),
        F.col("transcript").cast("string").alias("transcript"),
        F.col("hgvs_validation_status").cast("string").alias("hgvs_validation_status"),
        F.col("genome_build").cast("string").alias("genome_build"),
        F.col("chromosome").cast("string").alias("chromosome"),
        F.col("position_start").cast("bigint").alias("position_start"),
        F.col("position_end").cast("bigint").alias("position_end"),
        F.col("vaf").cast("double").alias("vaf"),
        F.col("zygosity").cast("string").alias("zygosity"),
        F.col("reported_classification").cast("string").alias("reported_classification"),
        F.col("reported_tier").cast("string").alias("reported_tier"),
        F.col("copy_number").cast("double").alias("copy_number"),
        F.coalesce(F.col("_omop_genomic"), F.lit(0)).cast("int")
        .alias("omop_genomic_concept_id"),
        F.col("snomed_code").cast("string").alias("snomed_code"),
        F.col("_published_specimen_id").alias("specimen_id"),
        F.col("is_current").cast("boolean").alias("is_current"),
        F.when(F.coalesce(F.col("record_status"), F.lit("")) != "active",
               F.lit("record_status_not_active"))
        .when(~F.coalesce(F.col("is_current"), F.lit(False)), F.lit("not_current"))
        .when(F.coalesce(F.col("identity_status"), F.lit("")) != "resolved",
              F.lit("identity_unresolved"))
        .when(F.col("event_datetime").isNull(), F.lit("missing_event_datetime"))
        .when(F.to_date("event_datetime") < F.to_date(F.lit(STUDY_START)),
              F.lit("before_study_window"))
        .when(F.col("_person_candidate").isNull(), F.lit("person_unresolved"))
        .when(F.col("_published_person_id").isNull(), F.lit("person_not_published"))
        .when(F.col("_allocated_id").isNull(), F.lit("id_not_allocated"))
        .alias("_exclusion_reason"),
    )


# === GENERATED: explicit schema for genomic_variant (contract_codegen.py) — do not edit ===
genomic_variant_schema = T.StructType([
    T.StructField('genomic_variant_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for gv:<fact_row_id>; never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published PERSON foreign key.'}),
    T.StructField('genomic_test_id', T.LongType(), True, metadata={'comment': 'Published GENOMIC_TEST foreign key where the parent test resolves; NULL otherwise.'}),
    T.StructField('variant_date', T.DateType(), False, metadata={'comment': 'Event datetime date. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('variant_datetime', T.TimestampType(), True, metadata={'comment': 'Event datetime. Europe/London wall-clock time (silver *_local).'}),
    T.StructField('hgnc_id', T.StringType(), True, metadata={'comment': 'HGNC gene identifier.'}),
    T.StructField('normalized_gene_symbol', T.StringType(), True, metadata={'comment': 'Normalized gene symbol.'}),
    T.StructField('partner_hgnc_id', T.StringType(), True, metadata={'comment': 'Fusion partner HGNC id where applicable.'}),
    T.StructField('partner_gene_symbol', T.StringType(), True, metadata={'comment': 'Fusion partner gene symbol where applicable.'}),
    T.StructField('alteration_type', T.StringType(), True, metadata={'comment': 'Alteration category.'}),
    T.StructField('detection_status', T.StringType(), True, metadata={'comment': 'Detected / not-detected style status.'}),
    T.StructField('hgvs_c', T.StringType(), True, metadata={'comment': 'Parsed coding-level HGVS notation.'}),
    T.StructField('hgvs_p', T.StringType(), True, metadata={'comment': 'Parsed protein-level HGVS notation.'}),
    T.StructField('transcript', T.StringType(), True, metadata={'comment': 'Reference transcript.'}),
    T.StructField('hgvs_validation_status', T.StringType(), True, metadata={'comment': 'Upstream HGVS validation status.'}),
    T.StructField('genome_build', T.StringType(), True, metadata={'comment': 'Genome build label.'}),
    T.StructField('chromosome', T.StringType(), True, metadata={'comment': 'Chromosome label.'}),
    T.StructField('position_start', T.LongType(), True, metadata={'comment': 'Genomic start position.'}),
    T.StructField('position_end', T.LongType(), True, metadata={'comment': 'Genomic end position.'}),
    T.StructField('vaf', T.DoubleType(), True, metadata={'comment': 'Variant allele fraction.'}),
    T.StructField('zygosity', T.StringType(), True, metadata={'comment': 'Zygosity label.'}),
    T.StructField('reported_classification', T.StringType(), True, metadata={'comment': 'Reported clinical classification.'}),
    T.StructField('reported_tier', T.StringType(), True, metadata={'comment': 'Reported tier.'}),
    T.StructField('copy_number', T.DoubleType(), True, metadata={'comment': 'Copy number where applicable.'}),
    T.StructField('omop_genomic_concept_id', T.IntegerType(), True, metadata={'comment': 'Castable OMOP Genomic concept where the parser resolves one; 0 otherwise.'}),
    T.StructField('snomed_code', T.StringType(), True, metadata={'comment': 'Source SNOMED code where carried.'}),
    T.StructField('specimen_id', T.LongType(), True, metadata={'comment': 'Published SPECIMEN foreign key where the source specimen resolves; NULL otherwise.'}),
    T.StructField('is_current', T.BooleanType(), True, metadata={'comment': 'Upstream currency flag; only current rows publish.'}),
])
# === END GENERATED: explicit schema for genomic_variant ===

@dp.materialized_view(
    name="genomic_variant",
    schema=genomic_variant_schema,
    comment="Journey-projected genomic variants (D8, R-O4-8); schema-first, empty-by-design until upstream lands.",
)
def genomic_variant():
    return (
        _genomic_variant_base()
        .where(F.col("_exclusion_reason").isNull())
        .drop("_exclusion_reason")
    )


# COMMAND ----------

# Router and lane profiles.

def _unrouted_base():
    rows = (
        read_source("src_patient_event")
        .where(F.col("event_type").isin(*ROUTED_EVENT_TYPES))
        .withColumn("_fact_kind", F.substring_index("fact_table", ".", -1))
        .where(F.col("_fact_kind") != "indication")
        .where(F.col("mapped_code").isNull())
    )
    representative = rows.groupBy("patient_event_id").agg(
        F.min(
            F.struct(
                F.coalesce(F.col("event_type"), F.lit("")).alias("event_type"),
                F.coalesce(F.col("record_status"), F.lit("")).alias("record_status"),
                F.coalesce(F.col("identity_status"), F.lit("")).alias("identity_status"),
            )
        ).alias("_representative")
    )
    return representative.select(
        F.col("_representative.event_type").alias("_lane"),
        F.when(F.col("_representative.record_status") != "active",
               F.lit("record_status_not_active"))
        .when(F.col("_representative.identity_status") != "resolved",
              F.lit("identity_unresolved"))
        .otherwise(F.lit("unmapped"))
        .alias("_exclusion_reason"),
    )


def _indication_base():
    return (
        read_source("src_patient_event")
        .withColumn("_fact_kind", F.substring_index("fact_table", ".", -1))
        .where(F.col("_fact_kind") == "indication")
        .select("patient_event_id")
        .distinct()
        .select(
            F.lit("indication").alias("_lane"),
            F.lit("indication_excluded_d3").alias("_exclusion_reason"),
        )
    )


# === GENERATED: explicit schema for o3_router_profile (contract_codegen.py) — do not edit ===
o3_router_profile_schema = T.StructType([
    T.StructField('lane', T.StringType(), False, metadata={'comment': 'Journey target_domain lane.'}),
    T.StructField('measure', T.StringType(), False, metadata={'comment': 'Accounting measure or CDM table name.'}),
    T.StructField('events', T.LongType(), False, metadata={'comment': 'Distinct events counted for this measure.'}),
    T.StructField('row_count', T.LongType(), True, metadata={'comment': 'Published rows where the measure is a CDM table or a fan advisory; NULL for event-only measures.'}),
    T.StructField('pipeline_version', T.StringType(), False, metadata={'comment': 'OMOP pipeline version that produced the profile.'}),
])
# === END GENERATED: explicit schema for o3_router_profile ===

@dp.materialized_view(
    name="o3_router_profile",
    schema=o3_router_profile_schema,
    comment="Global event algebra plus per-lane routed table rows and fan-outside advisory counts (O3+O4 lanes).",
)
def o3_router_profile():
    staged = read_source("src_routed_events")
    null_rows = F.lit(None).cast("bigint")

    def _global_measure(frame, measure):
        return frame.agg(F.countDistinct("patient_event_id").alias("events")).select(
            F.lit("__all__").alias("lane"),
            F.lit(measure).alias("measure"),
            F.col("events").cast("bigint"),
            null_rows.alias("row_count"),
            F.lit(PIPELINE_VERSION).alias("pipeline_version"),
        )

    def _lane_measure(frame, measure):
        return frame.groupBy("lane").agg(
            F.countDistinct("patient_event_id").alias("events"),
            F.count(F.lit(1)).alias("row_count"),
        ).select(
            "lane", F.lit(measure).alias("measure"),
            F.col("events").cast("bigint"), F.col("row_count").cast("bigint"),
            F.lit(PIPELINE_VERSION).alias("pipeline_version"),
        )

    frames = [
        _global_measure(staged, "__total__"),
        _global_measure(staged.where(F.col("_exclusion_reason").isNull()), "__published__"),
        _global_measure(
            staged.where(F.col("_exclusion_reason").isin(*EVENT_GRAIN_REASONS)),
            "__excluded__",
        ),
    ]
    for table_name in (
        "condition_occurrence", "procedure_occurrence", "observation",
        "device_exposure", "measurement", "drug_exposure",
    ):
        frames.append(_lane_measure(
            staged.where(
                F.col("_exclusion_reason").isNull()
                & (F.col("final_table") == table_name)
            ),
            table_name,
        ))
    frames.append(_lane_measure(
        staged.where(F.col("_exclusion_reason") == "fan_row_outside_o3"),
        "__fan_outside_o3__",
    ))
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


# COMMAND ----------

# CDM provenance and controlled metadata.

# === GENERATED: explicit schema for cdm_source (contract_codegen.py) — do not edit ===
cdm_source_schema = T.StructType([
    T.StructField('cdm_source_name', T.StringType(), False, metadata={'comment': 'Barts Health Journey-fed OMOP source name.'}),
    T.StructField('cdm_source_abbreviation', T.StringType(), False, metadata={'comment': 'Stable source abbreviation.'}),
    T.StructField('cdm_holder', T.StringType(), False, metadata={'comment': 'Barts Health NHS Trust.'}),
    T.StructField('source_description', T.StringType(), True, metadata={'comment': 'Controlled description of the Journey-fed model.'}),
    T.StructField('source_documentation_reference', T.StringType(), True, metadata={'comment': 'Repository documentation reference.'}),
    T.StructField('cdm_etl_reference', T.StringType(), True, metadata={'comment': 'Repository ETL reference.'}),
    T.StructField('source_release_date', T.DateType(), False, metadata={'comment': 'Configured Journey source release date.'}),
    T.StructField('cdm_release_date', T.DateType(), False, metadata={'comment': 'Configured OMOP release date.'}),
    T.StructField('cdm_version', T.StringType(), True, metadata={'comment': 'Resolved OMOP CDM version label.'}),
    T.StructField('cdm_version_concept_id', T.IntegerType(), False, metadata={'comment': 'Resolved OMOP CDM version concept, expected 705800.'}),
    T.StructField('vocabulary_version', T.StringType(), False, metadata={'comment': 'Pinned OMOP vocabulary version.'}),
])
# === END GENERATED: explicit schema for cdm_source ===

@dp.materialized_view(
    name="cdm_source",
    schema=cdm_source_schema,
    comment="One dynamic OMOP CDM source row with pinned vocabulary and release date.",
)
def cdm_source():
    concept = (
        read_source("src_vocab_concept")
        .where(F.col("concept_id") == F.lit(705800))
        .agg(F.max(F.struct("concept_id", "concept_name")).alias("version"))
        .select(
            F.col("version.concept_id").cast("int").alias("cdm_version_concept_id"),
            F.regexp_extract(F.col("version.concept_name"), r"Version ([0-9.]+)", 1).alias("cdm_version"),
        )
    )
    vocabulary = (
        read_source("src_vocabulary")
        .where(F.col("vocabulary_id") == "None")
        .select(F.col("vocabulary_version").cast("string").alias("vocabulary_version"))
    )
    static_schema = T.StructType([
        T.StructField("cdm_source_name", T.StringType(), False),
        T.StructField("cdm_source_abbreviation", T.StringType(), False),
        T.StructField("cdm_holder", T.StringType(), False),
        T.StructField("source_description", T.StringType(), True),
        T.StructField("source_documentation_reference", T.StringType(), True),
        T.StructField("cdm_etl_reference", T.StringType(), True),
    ])
    static = spark.createDataFrame([(
        "Barts Health Journey-fed OMOP",
        "BH-J-OMOP",
        "Barts Health NHS Trust",
        "OMOP CDM v5.4 rebuilt from governed Journey silver products.",
        "target-common-data-model/omop/docs/o1-conventions.md",
        "target-common-data-model/omop",
    )], static_schema)
    return static.crossJoin(concept).crossJoin(vocabulary).select(
        "cdm_source_name", "cdm_source_abbreviation", "cdm_holder", "source_description",
        "source_documentation_reference", "cdm_etl_reference",
        F.to_date(F.lit(SOURCE_RELEASE_DATE)).alias("source_release_date"),
        F.to_date(F.lit(SOURCE_RELEASE_DATE)).alias("cdm_release_date"),
        "cdm_version", "cdm_version_concept_id", "vocabulary_version",
    )


# === GENERATED: explicit schema for metadata (contract_codegen.py) — do not edit ===
metadata_schema = T.StructType([
    T.StructField('metadata_id', T.LongType(), False, metadata={'comment': 'Stable small-table metadata key.'}),
    T.StructField('metadata_concept_id', T.IntegerType(), False, metadata={'comment': '0 where no governed metadata concept is available.'}),
    T.StructField('metadata_type_concept_id', T.IntegerType(), False, metadata={'comment': '0 where no governed metadata type concept is available.'}),
    T.StructField('name', T.StringType(), False, metadata={'comment': 'Controlled metadata key.'}),
    T.StructField('value_as_string', T.StringType(), True, metadata={'comment': 'Controlled metadata value; contains no source free text.'}),
    T.StructField('value_as_concept_id', T.IntegerType(), True, metadata={'comment': 'Optional metadata concept value.'}),
    T.StructField('value_as_number', T.DoubleType(), True, metadata={'comment': 'Optional numeric metadata value.'}),
    T.StructField('metadata_date', T.DateType(), True, metadata={'comment': 'Optional metadata date.'}),
    T.StructField('metadata_datetime', T.TimestampType(), True, metadata={'comment': 'Optional metadata timestamp.'}),
])
# === END GENERATED: explicit schema for metadata ===

@dp.materialized_view(
    name="metadata",
    schema=metadata_schema,
    comment="Controlled O1 policy, release, and pipeline metadata.",
)
def metadata():
    schema = T.StructType([
        T.StructField("metadata_id", T.LongType(), False),
        T.StructField("metadata_concept_id", T.IntegerType(), False),
        T.StructField("metadata_type_concept_id", T.IntegerType(), False),
        T.StructField("name", T.StringType(), False),
        T.StructField("value_as_string", T.StringType(), True),
        T.StructField("value_as_concept_id", T.IntegerType(), True),
        T.StructField("value_as_number", T.DoubleType(), True),
        T.StructField("metadata_date", T.DateType(), True),
        T.StructField("metadata_datetime", T.TimestampType(), True),
    ])
    rows = [
        (1, 0, 0, "pid_policy", "direct-identifier-free pseudonymised; owner-approved 2026-08-21", None, None, None, None),
        (2, 0, 0, "carry_cause_text", str(CARRY_CAUSE_TEXT).lower(), None, None, None, None),
        (3, 0, 0, "carry_staff_identifiers", str(CARRY_STAFF_IDENTIFIERS).lower(), None, None, None, None),
        (4, 0, 0, "study_start_date", STUDY_START, None, None, date.fromisoformat(STUDY_START), None),
        (5, 0, 0, "journey_source_release", SOURCE_RELEASE_DATE, None, None, date.fromisoformat(SOURCE_RELEASE_DATE), None),
        (6, 0, 0, "vocabulary_version", "v5.0 27-FEB-26", None, None, None, None),
        (7, 0, 0, "pipeline_version", PIPELINE_VERSION, None, None, None, None),
        (8, 0, 0, "person_gate", "HERON-UK: every keyed Silver person publishes (superseded, missing birth date, birth year < 1910 and non-binary gender included; year_of_birth NULL when unknown); the former O1 gates are per-person flags in omop_person_research_flags, applied by gold", None, None, None, None),
        (9, 0, 0, "ethnicity_axis", "HERON-UK: race_source_concept_id = NHS Ethnic Category concept for the NHS letter; race_concept_id = its unambiguous Maps-to Race target, else 0; ethnicity_concept_id = 0", None, None, None, None),
        (10, 0, 0, "visit_scope", "spell, emergency_visit, outpatient_attendance, recurring_contact, results_only, other; waiting_list_placeholder and preadmission excluded (O2 R1)", None, None, None, None),
        (11, 0, 0, "visit_id_policy", "natural ENCNTR_ID via 4_prod.silver.reference_encounter_bounds.source_encounter_id; keyless encounters excluded with accounting (O2 R2)", None, None, None, None),
        (12, 0, 0, "visit_detail_lanes", "ward_stay=9201 from location_stay occupancy bounds (all lifecycle states; pre-attendance and no-attendance stops excluded); critical_care=32037; parent = containing ward stay (OGR)", None, None, None, None),  # OGR_METADATA_V1
        (13, 0, 0, "observation_period_interpretation", f"HERON-UK no-enrolment convention: one period per person; start = first published record (visit, visit_detail, routed event, specimen, direct-adapter rows) on or after birth - 1 day; end = earliest of death, age {CENSOR_AGE_YEARS} and source release date", None, None, None, None),  # OGR_METADATA_V1
        (14, 0, 0, "mother_child_linkage", "fact_relationship person-person; domain 56; concepts 4248584/4285883; both directions", None, None, None, None),
        (15, 0, 0, "event_router_scope", "nine governed O3-O5 lanes; qualified fact_table reduced to fact kind; every indication event excluded at event grain; additional observed event families flow event-only", None, None, None, None),
        (16, 0, 0, "mapping_ladder", "all distinct event-standard pairs retained; coding-system priority chooses provenance only for duplicate pairs; invalid or absent source concepts may traverse to valid standard Maps-to targets", None, None, None, None),
        (17, 0, 0, "clinical_event_id_policy", "registry id_space clinical_event; key evt:<patient_event_id>:<standard_concept_id>; event-grain and mapping-release-stable; superset allocation; never re-minted", None, None, None, None),
        (18, 0, 0, "condition_status_policy", "category_display via governed omop_condition_status_map; 0 when unmapped (O3 R-4)", None, None, None, None),
        (19, 0, 0, "family_history_shape", "observation_concept 4167217; value_as_concept is the family condition concept; relationship retained in qualifier_source_value (O3 R-5)", None, None, None, None),
        (20, 0, 0, "observation_value_policy", "value_as_string NULL; value_source_value NULL except IMD rows (quintile text); vocabulary-resolved value_as_concept_id and numeric values retained; quantitative values NULL when an event has multiple standards in the same CDM table", None, None, None, None),
        (21, 0, 0, "measurement_lane_scope", "measurement lane routed: pathology results and orders, vital signs, measurement-domain clinical findings; measurement-domain cross-arrivals from the O3 lanes publish (O4 R-1)", None, None, None, None),
        (22, 0, 0, "measurement_winner_policy", "one standard per event in measurement: ladder winner by governed priority then normalized code; suppressed standards retained as row-grain diagnostics; O3 lanes stay lossless (O4 R-2)", None, None, None, None),
        (23, 0, 0, "measurement_value_policy", "value_as_number from pathology/vitals/scores; value_as_concept from silver value concept then measurement_value role; units from silver concept then UCUM code; operator from verified operator concepts; value_source_value and measurement_time NULL; value_text, value_datetime, interpretation excluded with recorded dispositions (O4 R-3, R-4)", None, None, None, None),
        (24, 0, 0, "specimen_policy", "structural projection; specimen_concept = standard Specimen-domain concept from 3_lookup.omop.specimen_type_snomed_map via clinical_specimen, else 0 (R-O4-5 revised 2026-09-28); type concept 32856; registry id_space specimen key spm:<patient_event_id>; lab numbers and accessions never published (O4 R-5)", None, None, None, None),
        (25, 0, 0, "measurement_specimen_relationship", "fact_relationship domains 21/36 concepts 32668/32669 both directions; endpoints must be published rows (O4 R-6)", None, None, None, None),
        (26, 0, 0, "genomic_extension_shape", "D8: journey-projected extension tables genomic_test, genomic_gene_tested, genomic_variant (schema-first, empty-by-design until upstream lands); evidence_text never crosses (O4 R-8)", None, None, None, None),
        (27, 0, 0, "oncology_modifier_policy", "Cancer Modifier stage groups published with measurement_event_id = same-event condition_occurrence and meas_event_field_concept_id 1147127 (see stage_policy); other modifier axes (T/N/M, grade) not yet sourced", None, None, None, None),  # OGR_METADATA_V1
        (28, 0, 0, "drug_lane_scope", "Drug lane routes medication_admin, medication_dispense, drug_expenditure and cancer_treatment events whose target_domain is drug; medication_supply publishes through the direct supply adapter (see supply_policy), not the router.", None, None, None, None),  # OGR_METADATA_V1
        (29, 0, 0, "drug_single_standard_policy", "R-O5-2: every standard from the minimum-priority coding-system arm is retained; lower-priority arms remain in staging under drug_standard_suppressed.", None, None, None, None),
        (30, 0, 0, "drug_quantity_policy", "R-O5-6/R-O5-6b: quantity is source-stated per routed feed and is NULL when an event publishes multiple standards; days_supply and refills are NULL; homecare supply quantity = quantity_delivered for single-standard events.", None, None, None, None),  # OGR_METADATA_V1
        (31, 0, 0, "era_derivation_policy", "R-O5-8/R-O5-9: eras derive from fact frames with a 30-day persistence window and registry-minted IDs; gap_days is the era span minus the union of exposure intervals; new keys require a registry allocation update.", None, None, None, None),
        (32, 0, 0, "era_eligibility_policy", "R-O5-11: dose_era is a daily ingredient dose from medication administrations only, single-ingredient products only, and amount units only; every ineligible exposure has a named accounting reason.", None, None, None, None),
        (33, 0, 0, "adapter_admission_policy", f"R0 allowlist {ADAPTER_ALLOWLIST_VERSION}: an event_type routes only into its admitted lanes (the pairs published by the 2026-09-03 build; allergy_intolerance publishes through the direct allergy adapter); unadmitted events are excluded as deferred_adapter and unadmitted lanes of published events as deferred_adapter_role; measurement value roles admitted only for {', '.join(ADMITTED_VALUE_ROLE_EVENT_TYPES)}.", None, None, None, None),  # OGR_METADATA_V1
        (34, 0, 0, "imd_policy", f"HERON-UK: observation_concept_id 0, observation_source_concept_id {IMD_SOURCE_CONCEPT_ID}, unit_concept_id {IMD_UNIT_CONCEPT_ID}, value_as_number = IMD quintile; one row per active HOME/PREVIOUS address with a quintile (types {', '.join(IMD_ADDRESS_TYPES)}); date = address effective-from date, else observation_period_start_date for a person's sole IMD record; same-day same-quintile duplicates collapse; persons without an observation period excluded; registry key imd:<address_id>; street address and postcode never published; LSOA published as a LOCATION natural key (lsoa_location_policy)", None, None, None, None),  # OGR_METADATA_V1
        (35, 0, 0, "heron_uk_conventions", "HERON-UK data conventions (ethnicity, observation_period, socioeconomic_status, who_to_keep) applied in silver; research-cohort exclusions belong to the gold OMOP layer", None, None, None, None),
        (36, 0, 0, "censor_age_years", str(CENSOR_AGE_YEARS), None, float(CENSOR_AGE_YEARS), None, None),
    ] + [tuple(row) for row in METADATA_ROWS]  # OGR_EXT_V1
    return spark.createDataFrame(rows, schema)


# COMMAND ----------

# Complete exclusion and era-eligibility accounting.

def _metric(frame, model, lane_column=None):
    lane = F.col(lane_column) if lane_column else F.lit(model)
    counted = (
        frame.where(F.col("_exclusion_reason").isNotNull())
        .groupBy(lane.alias("lane"), F.col("_exclusion_reason").alias("reason"))
        .agg(F.count(F.lit(1)).cast("bigint").alias("excluded_rows"))
        .select(
            F.lit(model).alias("captured_model"), "lane", "reason", "excluded_rows",
            F.lit(PIPELINE_VERSION).alias("pipeline_version"),
        )
    )
    zero_schema = T.StructType([
        T.StructField("captured_model", T.StringType(), False),
        T.StructField("lane", T.StringType(), False),
        T.StructField("reason", T.StringType(), False),
        T.StructField("excluded_rows", T.LongType(), False),
        T.StructField("pipeline_version", T.StringType(), False),
    ])
    zero = spark.createDataFrame([(model, model, "__none__", 0, PIPELINE_VERSION)], zero_schema)
    return counted.unionByName(zero)


def _era_eligibility_metrics():
    drug = (
        _drug_era_candidates()
        .groupBy(
            F.coalesce(F.col("_era_reason"), F.lit("__eligible__")).alias("reason")
        )
        .agg(F.countDistinct("_exposure_id").cast("bigint").alias("excluded_rows"))
        .select(
            F.lit("drug_era").alias("captured_model"),
            F.lit("drug").alias("lane"),
            "reason", "excluded_rows",
            F.lit(PIPELINE_VERSION).alias("pipeline_version"),
        )
    )
    dose = (
        _dose_era_candidates()
        .groupBy(
            F.coalesce(F.col("_dose_reason"), F.lit("__eligible__")).alias("reason")
        )
        .agg(F.countDistinct("_exposure_id").cast("bigint").alias("excluded_rows"))
        .select(
            F.lit("dose_era").alias("captured_model"),
            F.lit("drug").alias("lane"),
            "reason", "excluded_rows",
            F.lit(PIPELINE_VERSION).alias("pipeline_version"),
        )
    )
    return drug.unionByName(dose)


# OGR_ADAPTERS_V1: shared gate for direct-adapter rows (allergy, supply, stage, maternity, VTE).
# Adapters read Silver, lookups and the registry only, never a CDM MV, so they may feed
# observation-period evidence without a cycle.
PROVENANCE_BUILDERS = []


def _clinical_event_registry():
    return read_source("src_id_registry").where(F.col("id_space") == "clinical_event").select(
        F.col("source_key").alias("_ad_key"), F.col("allocated_id").alias("_ad_id"))


def _adapter_gate(frame, key, at):
    """Adds _ad_id (registry id), _ad_at, _ad_visit (published visit) and _gate_reason.
    The frame must carry person_id and encounter_id; registry and visit keys are unique, so no fan-out."""
    people = _person_base().where(F.col("_exclusion_reason").isNull()).select(
        F.col("person_id").alias("_ad_person"))
    return (
        frame.withColumn("_ad_lookup", key)
        .withColumn("_ad_at", at.cast("timestamp"))
        .join(_clinical_event_registry(), F.col("_ad_lookup") == F.col("_ad_key"), "left")
        .join(people, F.col("person_id") == F.col("_ad_person"), "left")
        .join(_published_visit_keys(), F.col("encounter_id") == F.col("_pv_encounter_id"), "left")
        .withColumn("_ad_visit", F.col("_pv_visit_id"))
        .withColumn(
            "_gate_reason",
            F.when(F.col("_ad_at").isNull(), F.lit("missing_event_datetime"))
            .when(F.to_date("_ad_at") < F.to_date(F.lit(STUDY_START)), F.lit("before_study_window"))
            .when(F.col("person_id").isNull(), F.lit("person_unresolved"))
            .when(F.col("_ad_person").isNull(), F.lit("person_not_published"))
            .when(F.col("_ad_id").isNull(), F.lit("id_not_allocated")),
        )
    )


def _provenance_rows(frame, table, row_id, field, concept, source_concept, coding_system,
                     method, confidence, rule, model, version):
    """One omop_concept_provenance row per published concept field (Task 2.11 schema)."""
    return frame.select(
        (F.lit(table) if isinstance(table, str) else table).alias("cdm_table"),  # str, or a column for multi-table lanes
        row_id.cast("bigint").alias("cdm_row_id"),
        (F.lit(field) if isinstance(field, str) else field).alias("concept_field"),  # a CDM field name, or a column
        concept.cast("int").alias("concept_id"),
        source_concept.cast("int").alias("source_concept_id"),
        coding_system.cast("string").alias("coding_system"),
        method.cast("string").alias("map_method"),
        confidence.cast("double").alias("map_confidence"),
        rule.cast("string").alias("map_rule_id"),
        model.cast("string").alias("scoring_model"),
        version.cast("string").alias("map_version"),
        F.lit(PIPELINE_VERSION).alias("pipeline_version"),
    )


# OGR_ALLERGY_V1: allergy direct adapter (observation 43530807 / 37396387, value = substance).
ALLERGY_PUBLISHED_STATUSES = ("Active", "Resolved")


def _allergy_substance_map():
    """SNOMED substance code -> unique standard concept (itself when standard, else Maps-to)."""
    concept = read_source("src_vocab_concept")
    snomed = concept.where(F.col("vocabulary_id") == "SNOMED").select(
        F.col("concept_code").alias("_as_code"), F.col("concept_id").alias("_as_source"),
        F.col("standard_concept").alias("_as_std"), F.col("invalid_reason").alias("_as_invalid"))
    standard = concept.where((F.col("standard_concept") == "S") & F.col("invalid_reason").isNull()).select(
        F.col("concept_id").alias("_std_id"))
    maps_to = read_source("src_vocab_concept_relationship").where(
        (F.col("relationship_id") == "Maps to") & F.col("invalid_reason").isNull()
    ).select(F.col("concept_id_1").alias("_mt_from"), F.col("concept_id_2").alias("_mt_to"))
    itself = snomed.where((F.col("_as_std") == "S") & F.col("_as_invalid").isNull()).select(
        "_as_code", "_as_source", F.col("_as_source").alias("_as_target"),
        F.lit("snomed_standard").alias("_as_method"), F.lit(1).alias("_as_rank"))
    mapped = (snomed.join(maps_to, F.col("_as_source") == F.col("_mt_from"))
              .join(standard, F.col("_mt_to") == F.col("_std_id"))
              .select("_as_code", "_as_source", F.col("_mt_to").alias("_as_target"),
                      F.lit("snomed_maps_to").alias("_as_method"), F.lit(2).alias("_as_rank")))
    both = itself.unionByName(mapped)
    best = both.groupBy("_as_code").agg(F.min("_as_rank").alias("_as_best"))
    winners = both.join(best, "_as_code").where(F.col("_as_rank") == F.col("_as_best"))
    return (winners.groupBy("_as_code")
            .agg(F.countDistinct("_as_target").alias("_as_n"), F.min("_as_target").alias("_as_target"),
                 F.min("_as_source").alias("_as_source"), F.min("_as_method").alias("_as_method"))
            .where(F.col("_as_n") == 1).drop("_as_n"))


def _allergy_base():
    standard = read_source("src_vocab_concept").where(
        (F.col("standard_concept") == "S") & F.col("invalid_reason").isNull()).select(
        F.col("concept_id").alias("_silver_std"))
    rows = (
        read_source("src_allergy").where(F.col("record_status") == "active")
        .join(standard, F.col("substance_omop_concept_id") == F.col("_silver_std"), "left")
        .join(_allergy_substance_map(), F.trim(F.col("substance_snomed_code")) == F.col("_as_code"), "left")
    )
    absent = F.coalesce(F.col("absence_assertion_ind"), F.lit(False))
    substance = F.coalesce(F.col("_silver_std"), F.col("_as_target"))
    gated = _adapter_gate(rows, F.concat(F.lit("alg:"), F.col("patient_event_id")), F.col("event_datetime"))
    return gated.select(
        F.col("_ad_id").alias("observation_id"),
        "person_id",
        F.when(absent, F.lit(37396387)).otherwise(F.lit(43530807)).cast("int").alias("observation_concept_id"),
        F.to_date("_ad_at").alias("observation_date"),
        F.col("_ad_at").alias("observation_datetime"),
        F.lit(32817).cast("int").alias("observation_type_concept_id"),
        F.lit(None).cast("double").alias("value_as_number"),
        F.lit(None).cast("string").alias("value_as_string"),
        F.coalesce(substance, F.lit(0)).cast("int").alias("value_as_concept_id"),
        F.lit(0).cast("int").alias("qualifier_concept_id"),
        F.lit(0).cast("int").alias("unit_concept_id"),
        F.lit(None).cast("bigint").alias("provider_id"),
        F.col("_ad_visit").cast("bigint").alias("visit_occurrence_id"),
        F.lit(None).cast("bigint").alias("visit_detail_id"),
        _safe_source_code(F.coalesce(F.col("substance_snomed_code"), F.col("source_code")), "allergy")
        .alias("observation_source_value"),
        F.coalesce(F.col("_as_source"), F.lit(0)).cast("int").alias("observation_source_concept_id"),
        F.lit(None).cast("string").alias("unit_source_value"),
        F.col("severity_display").cast("string").alias("qualifier_source_value"),
        F.lit(None).cast("string").alias("value_source_value"),
        F.lit(None).cast("bigint").alias("observation_event_id"),
        F.lit(0).cast("int").alias("obs_event_field_concept_id"),
        F.when(F.col("_silver_std").isNotNull(), F.col("substance_map_method"))
        .when(F.col("_as_target").isNotNull(), F.col("_as_method")).alias("_map_method"),
        F.when(F.col("_silver_std").isNotNull(), F.col("substance_map_confidence"))
        .when(F.col("_as_target").isNotNull(), F.lit(1.0)).alias("_map_confidence"),
        F.when(F.col("_silver_std").isNotNull(), F.col("substance_map_rule_id")).alias("_map_rule_id"),
        F.when(F.col("_silver_std").isNotNull(), F.col("substance_map_scoring_model")).alias("_map_model"),
        F.when(F.col("_silver_std").isNotNull(), F.col("substance_map_version")).alias("_map_version"),
        F.col("_ad_at").alias("_evidence_start"),
        F.when(~F.col("reaction_status_display").isin(*ALLERGY_PUBLISHED_STATUSES), F.lit("allergy_not_confirmed"))
        .when(~absent & substance.isNull(), F.lit("allergy_substance_unmapped"))
        .otherwise(F.col("_gate_reason")).alias("_exclusion_reason"),
    )


def _allergy_published():
    return _allergy_base().where(F.col("_exclusion_reason").isNull())


def _allergy_observation_rows(**_):
    return _allergy_published().select(*[f.name for f in observation_schema.fields])


def _allergy_metrics():
    return _metric(_allergy_base().withColumn("_lane", F.lit("allergy")), "observation", "_lane")


def _allergy_evidence():
    """Reads Silver only (observation-period evidence)."""
    return _allergy_published().select("person_id", "_evidence_start")


def _allergy_provenance():
    published = _allergy_published()
    substance = _provenance_rows(
        published.where(F.col("value_as_concept_id") != 0), "observation", F.col("observation_id"),
        "value_as_concept_id", F.col("value_as_concept_id"),
        F.col("observation_source_concept_id"), F.lit("SNOMED"), F.col("_map_method"),
        F.col("_map_confidence"), F.col("_map_rule_id"), F.col("_map_model"), F.col("_map_version"))
    # The allergy/no-known-allergy concept is fixed by the absence assertion, not mapped.
    fixed = _provenance_rows(
        published, "observation", F.col("observation_id"), "observation_concept_id", F.col("observation_concept_id"),
        F.lit(0), F.lit("Millennium"), F.lit("fixed_allergy_concept"), F.lit(1.0),
        F.lit("allergy_absence_assertion:v1"), F.lit(None), F.lit(PIPELINE_VERSION))
    return substance.unionByName(fixed)


_register_extension("observation", _allergy_observation_rows)
METRIC_BUILDERS.append(_allergy_metrics)
EVIDENCE_BUILDERS.append(_allergy_evidence)
PROVENANCE_BUILDERS.append(_allergy_provenance)


SUPPLY_BAN_ALIASES = {  # UK BAN/rINN VTM name -> US ingredient name(s); governed rule uk_ban_alias:v1
    "desferrioxamine": "deferoxamine", "aciclovir": "acyclovir", "valaciclovir": "valacyclovir",
    "co-careldopa": "carbidopa + levodopa", "co-trimoxazole": "sulfamethoxazole + trimethoprim",
    "calcium folinate": "leucovorin", "amikacin liposomal": "amikacin",
}


def _supply_standard_map(supply, concept, relationship, synonym):
    """Supply VTM -> standard RxNorm/RxNorm Extension Ingredients. OGR_SUPPLY_V1
    1: dm+d Maps-to; else per ' + ' component (after the BAN alias rewrite) 2: exact lower-case
    Ingredient name, 3: unique Ingredient synonym. Returns patient_event_id, _sup_standard
    (NULL = unmapped component), _sup_method."""
    ingredients = concept.where(
        (F.col("concept_class_id") == "Ingredient") & (F.col("standard_concept") == "S")
        & F.col("invalid_reason").isNull() & F.col("vocabulary_id").isin("RxNorm", "RxNorm Extension")
    )
    names = (ingredients.groupBy(F.lower(F.trim(F.col("concept_name"))).alias("_ing_name"))
             .agg(F.countDistinct("concept_id").alias("_ing_n"), F.min("concept_id").alias("_ing_id"))
             .where(F.col("_ing_n") == 1).drop("_ing_n"))
    synonyms = (synonym.join(ingredients.select("concept_id"), "concept_id")
                .groupBy(F.lower(F.trim(F.col("concept_synonym_name"))).alias("_syn_name"))
                .agg(F.countDistinct("concept_id").alias("_syn_n"), F.min("concept_id").alias("_syn_id"))
                .where(F.col("_syn_n") == 1).drop("_syn_n"))
    aliases = F.create_map(*[F.lit(x) for kv in SUPPLY_BAN_ALIASES.items() for x in kv])
    rows = supply.select(
        "patient_event_id",
        F.expr("try_cast(dmd_vtm_concept_id AS int)").alias("_vtm"),
        F.lower(F.trim(F.col("dmd_vtm_name"))).alias("_vtm_name"),
    )
    maps = relationship.where(
        (F.col("relationship_id") == "Maps to") & F.col("invalid_reason").isNull()
    ).join(ingredients.select(F.col("concept_id").alias("_to")), F.col("concept_id_2") == F.col("_to"))
    via_maps = rows.join(maps, F.col("_vtm") == F.col("concept_id_1")).select(
        "patient_event_id", F.col("_to").alias("_sup_standard"), F.lit("dmd_maps_to").alias("_sup_method"))
    parts = (
        rows.join(via_maps.select("patient_event_id").distinct(), "patient_event_id", "left_anti")
        .withColumn("_aliased", F.coalesce(aliases[F.col("_vtm_name")], F.col("_vtm_name")))
        .select("patient_event_id", (aliases[F.col("_vtm_name")].isNotNull()).alias("_via_alias"),
                F.explode(F.split(F.col("_aliased"), r" [+] ")).alias("_part"))
        .withColumn("_part", F.trim(F.col("_part")))
    )
    via_name = (
        parts.join(names, F.col("_part") == F.col("_ing_name"), "left")
        .join(synonyms, F.col("_part") == F.col("_syn_name"), "left")
        .select(
            "patient_event_id",
            F.coalesce(F.col("_ing_id"), F.col("_syn_id")).alias("_sup_standard"),
            F.when(F.col("_via_alias") & F.coalesce(F.col("_ing_id"), F.col("_syn_id")).isNotNull(),
                   F.lit("uk_ban_alias"))
            .when(F.col("_ing_id").isNotNull(), F.lit("exact_ingredient_name"))
            .when(F.col("_syn_id").isNotNull(), F.lit("ingredient_synonym")).alias("_sup_method"),
        )
    )
    return via_maps.unionByName(via_name).distinct()


# OGR_SUPPLY_V1: medication-supply (homecare) direct adapter into DRUG_EXPOSURE.
def _supply_base():
    supply = read_source("src_medication_supply").where(F.col("record_status") == "active")
    eligible = supply.where((F.col("item_status_display") == "COMPLETE") & F.col("dmd_vtm_name").isNotNull())
    standards = _supply_standard_map(
        eligible, read_source("src_vocab_concept"), read_source("src_vocab_concept_relationship"),
        read_source("src_vocab_concept_synonym"))
    per_event = Window.partitionBy("patient_event_id")
    expanded = (
        supply.join(standards, "patient_event_id", "left")
        .withColumn("encounter_id", F.col("encounter_id").cast("bigint"))
        .withColumn("_mapped_n", F.count("_sup_standard").over(per_event))
        .withColumn("_standard_n", F.size(F.collect_set("_sup_standard").over(per_event)))
    )
    gated = _adapter_gate(
        expanded,
        F.concat_ws(":", F.lit("sup"), F.col("patient_event_id"), F.col("_sup_standard").cast("string")),
        F.col("request_released_date"),
    )
    start = F.col("_ad_at")
    return gated.select(
        F.col("_ad_id").alias("drug_exposure_id"),
        "person_id",
        F.col("_sup_standard").cast("int").alias("drug_concept_id"),
        F.to_date(start).alias("drug_exposure_start_date"),
        start.alias("drug_exposure_start_datetime"),
        F.to_date(start).alias("drug_exposure_end_date"),
        start.alias("drug_exposure_end_datetime"),
        F.lit(None).cast("date").alias("verbatim_end_date"),
        F.lit(32825).cast("int").alias("drug_type_concept_id"),
        F.lit(None).cast("string").alias("stop_reason"),
        F.lit(None).cast("int").alias("refills"),
        F.when(F.col("_standard_n") == 1, F.col("quantity_delivered")).cast("double").alias("quantity"),
        F.lit(None).cast("int").alias("days_supply"),
        F.lit(None).cast("string").alias("sig"),
        F.lit(0).cast("int").alias("route_concept_id"),
        F.lit(None).cast("string").alias("lot_number"),
        F.lit(None).cast("bigint").alias("provider_id"),
        F.col("_ad_visit").cast("bigint").alias("visit_occurrence_id"),
        F.lit(None).cast("bigint").alias("visit_detail_id"),
        F.col("dmd_vtm_code").cast("string").alias("drug_source_value"),
        F.coalesce(F.expr("try_cast(dmd_vtm_concept_id AS int)"), F.lit(0)).alias("drug_source_concept_id"),
        F.lit(None).cast("string").alias("route_source_value"),
        F.col("order_unit").cast("string").alias("dose_unit_source_value"),
        F.col("_sup_method").alias("_map_method"),
        F.col("_ad_at").alias("_evidence_start"),
        F.when(F.col("item_status_display") != "COMPLETE", F.lit("supply_not_complete"))
        .when(F.col("drug_mapping_method") == "NON_DRUG", F.lit("supply_non_drug_item"))  # set by bronze from Task 3.5
        .when(F.col("dmd_vtm_name").isNull(), F.lit("supply_vtm_absent"))
        .when(F.col("_mapped_n") == 0, F.lit("supply_vtm_unmapped"))
        .when(F.col("_sup_standard").isNull(), F.lit("supply_component_unmapped"))
        .otherwise(F.col("_gate_reason")).alias("_exclusion_reason"),
    )


def _supply_published():
    return _supply_base().where(F.col("_exclusion_reason").isNull())


def _supply_drug_rows(keep_internals=False, **_):
    columns = [F.col(f.name) for f in drug_exposure_schema.fields]
    if keep_internals:
        columns.append(F.lit("medication_supply").alias("_event_type"))
    return _supply_published().select(*columns)


def _supply_metrics():
    return _metric(_supply_base().withColumn("_lane", F.lit("medication_supply")), "drug_exposure", "_lane")


def _supply_evidence():
    """Reads Silver only (observation-period evidence)."""
    return _supply_published().select("person_id", "_evidence_start")


def _supply_provenance():
    rows = _supply_published()
    return _provenance_rows(
        rows, "drug_exposure", F.col("drug_exposure_id"), "drug_concept_id", F.col("drug_concept_id"),
        F.col("drug_source_concept_id"), F.lit("dm+d"), F.col("_map_method"),
        F.lit(1.0), F.lit(None), F.lit(None), F.lit(None))


_register_extension("drug_exposure", _supply_drug_rows)
METRIC_BUILDERS.append(_supply_metrics)
EVIDENCE_BUILDERS.append(_supply_evidence)
PROVENANCE_BUILDERS.append(_supply_provenance)


# OGR_STAGE_V1: oncology stage group -> Cancer Modifier MEASUREMENT linked to its condition.
def _stage_norm(column):
    return F.regexp_replace(F.upper(F.trim(F.regexp_replace(column.cast("string"), r"[\s]+", " "))), r"^STAGE ", "")


def _stage_base():
    stage_map = read_source("lkp_stage_map").select(
        F.col("stage_display_norm").alias("_sg_key"), F.col("concept_id").alias("_sg_concept"),
        F.col("map_method").alias("_sg_method"), F.col("map_rule_id").alias("_sg_rule"),
        F.col("map_version").alias("_sg_version"))
    parent = (
        read_source("src_routed_events")
        .where(F.col("_exclusion_reason").isNull() & (F.col("final_table") == "condition_occurrence"))
        .groupBy("patient_event_id")
        .agg(F.min("allocated_id").alias("_sg_parent"))
    )
    rows = (
        read_source("src_condition_stage").where(F.col("record_status") == "active")
        .withColumn("encounter_id", F.col("encounter_id").cast("bigint"))
        .withColumn("_sg_display", F.trim(F.col("stage_group_source_display")))
        .join(stage_map, _stage_norm(F.col("stage_group_source_display")) == F.col("_sg_key"), "left")
        .join(parent, "patient_event_id", "left")
    )
    gated = _adapter_gate(rows, F.concat(F.lit("stg:"), F.col("patient_event_id")), F.col("event_datetime"))
    return gated.select(
        F.col("_ad_id").alias("measurement_id"),
        "person_id",
        F.col("_sg_concept").cast("int").alias("measurement_concept_id"),
        F.to_date("_ad_at").alias("measurement_date"),
        F.col("_ad_at").alias("measurement_datetime"),
        F.lit(None).cast("string").alias("measurement_time"),
        F.lit(32817).cast("int").alias("measurement_type_concept_id"),
        F.lit(0).cast("int").alias("operator_concept_id"),
        F.lit(None).cast("double").alias("value_as_number"),
        F.lit(0).cast("int").alias("value_as_concept_id"),
        F.lit(0).cast("int").alias("unit_concept_id"),
        F.lit(None).cast("double").alias("range_low"),
        F.lit(None).cast("double").alias("range_high"),
        F.lit(None).cast("bigint").alias("provider_id"),
        F.col("_ad_visit").cast("bigint").alias("visit_occurrence_id"),
        F.lit(None).cast("bigint").alias("visit_detail_id"),
        F.substring(F.col("_sg_display"), 1, 50).alias("measurement_source_value"),
        F.lit(0).cast("int").alias("measurement_source_concept_id"),
        F.lit(None).cast("string").alias("unit_source_value"),
        F.lit(0).cast("int").alias("unit_source_concept_id"),
        F.lit(None).cast("string").alias("value_source_value"),
        F.col("_sg_parent").cast("bigint").alias("measurement_event_id"),
        F.when(F.col("_sg_parent").isNotNull(), F.lit(1147127)).otherwise(F.lit(0)).cast("int")
        .alias("meas_event_field_concept_id"),
        "_sg_method", "_sg_rule", "_sg_version",
        F.col("_ad_at").alias("_evidence_start"),
        F.when(F.col("_sg_display").isNull() | (F.col("_sg_display") == ""), F.lit("stage_group_absent"))
        .when(F.col("_sg_concept").isNull(), F.lit("stage_group_unmapped"))
        .otherwise(F.col("_gate_reason")).alias("_exclusion_reason"),
    )


def _stage_published():
    return _stage_base().where(F.col("_exclusion_reason").isNull())


def _stage_measurement_rows(**_):
    return _stage_published().select(*[f.name for f in measurement_schema.fields])


def _stage_metrics():
    return _metric(_stage_base().withColumn("_lane", F.lit("condition_stage")), "measurement", "_lane")


def _stage_evidence():
    """Reads Silver, lookups and the router staging MV (never a CDM MV)."""
    return _stage_published().select("person_id", "_evidence_start")


def _stage_provenance():
    return _provenance_rows(
        _stage_published(), "measurement", F.col("measurement_id"), "measurement_concept_id",
        F.col("measurement_concept_id"), F.lit(0), F.lit("Cancer Modifier"), F.col("_sg_method"),
        F.lit(1.0), F.col("_sg_rule"), F.lit(None), F.col("_sg_version"))


_register_extension("measurement", _stage_measurement_rows)
METRIC_BUILDERS.append(_stage_metrics)
EVIDENCE_BUILDERS.append(_stage_evidence)
PROVENANCE_BUILDERS.append(_stage_provenance)


# OGR_MATERNITY_V1: pregnancy episode, maternity facts (mother) and infant birth measurements (baby).
MATERNITY_TABLE_BY_DOMAIN = {"Procedure": "procedure_occurrence", "Condition": "condition_occurrence",
                             "Observation": "observation", "Measurement": "measurement"}
EPISODE_EVENT_FIELD = {"procedure_occurrence": 1147082, "condition_occurrence": 1147127,
                       "measurement": 1147138, "observation": 1147165}
CDM_CORE_FIELDS = {  # table: (id, concept, date, datetime, type, source_value, source_concept)
    "procedure_occurrence": ("procedure_occurrence_id", "procedure_concept_id", "procedure_date",
                             "procedure_datetime", "procedure_type_concept_id", "procedure_source_value",
                             "procedure_source_concept_id"),
    "condition_occurrence": ("condition_occurrence_id", "condition_concept_id", "condition_start_date",
                             "condition_start_datetime", "condition_type_concept_id", "condition_source_value",
                             "condition_source_concept_id"),
    "observation": ("observation_id", "observation_concept_id", "observation_date", "observation_datetime",
                    "observation_type_concept_id", "observation_source_value", "observation_source_concept_id"),
    "measurement": ("measurement_id", "measurement_concept_id", "measurement_date", "measurement_datetime",
                    "measurement_type_concept_id", "measurement_source_value", "measurement_source_concept_id"),
}


def _cdm_concept_field(table):
    """The concept column _cdm_rows writes _concept into, for a table-name column (provenance field)."""
    return F.create_map(*[F.lit(x) for t, f in CDM_CORE_FIELDS.items() for x in (t, f[1])])[table]


def _cdm_rows(table, frame):
    """Project an adapter frame (_ad_id, person_id, _concept, _ad_at, _source_value, _value_number,
    _value_concept, _unit, _unit_source, _value_source, _ad_visit) onto <table>_schema.
    Unset *_concept_id fields are 0; every other unset field is a typed NULL."""
    rid, concept, d, dt, typ, sv, sc = CDM_CORE_FIELDS[table]
    given = {
        rid: F.col("_ad_id"), "person_id": F.col("person_id"), concept: F.col("_concept"),
        d: F.to_date("_ad_at"), dt: F.col("_ad_at"), typ: F.lit(32817), sv: F.col("_source_value"),
        sc: F.lit(0), "visit_occurrence_id": F.col("_ad_visit"), "value_as_number": F.col("_value_number"),
        "value_as_concept_id": F.coalesce(F.col("_value_concept"), F.lit(0)),
        "unit_concept_id": F.coalesce(F.col("_unit"), F.lit(0)), "unit_source_value": F.col("_unit_source"),
        "value_source_value": F.col("_value_source"),
    }
    schema = globals()[f"{table}_schema"]
    return frame.select(*[
        given[f.name].cast(f.dataType).alias(f.name) if f.name in given
        else (F.lit(0) if f.name.endswith("_concept_id") else F.lit(None)).cast(f.dataType).alias(f.name)
        for f in schema.fields
    ])


def _maternity_map():
    return read_source("lkp_maternity_map").select(
        F.col("source_field").alias("_mm_field"), F.col("source_code").alias("_mm_code"),
        F.col("target_concept_id").cast("int").alias("_mm_concept"),
        F.col("map_method").alias("_mm_method"), F.col("map_rule_id").alias("_mm_rule"),
        F.col("map_version").alias("_mm_version"))


def _maternity_facts():
    """Unpivoted maternity facts before the adapter gate; one row per (source row, item) with a value."""
    journeys = read_source("src_journey").select(
        F.col("journey_key").alias("_mj_key"), F.col("period_start").cast("timestamp").alias("_mj_start"))
    hist = (read_source("src_pregnancy_history").where(F.col("record_status") == "active")
            .join(journeys, F.col("journey_pregnancy_key") == F.col("_mj_key"), "left"))
    hist_at = F.coalesce(F.col("first_antenatal_appointment_date").cast("timestamp"), F.col("_mj_start"))
    null_s = F.lit(None).cast("string")
    null_d = F.lit(None).cast("double")

    def fact(frame, person, key, at, concept, number, unit, unit_src, src_value, field, code, reason, on_baby):
        return frame.select(
            person.cast("bigint").alias("person_id"), F.lit(None).cast("bigint").alias("encounter_id"),
            key.alias("_key"), at.cast("timestamp").alias("_at"),
            F.col("journey_pregnancy_key").alias("_journey_key"),
            F.lit(concept).cast("int").alias("_fixed_concept"), number.cast("double").alias("_value_number"),
            F.lit(unit).cast("int").alias("_unit"), F.lit(unit_src).cast("string").alias("_unit_source"),
            src_value.cast("string").alias("_source_value"), F.lit(field).cast("string").alias("_map_field"),
            code.cast("string").alias("_map_code"), reason.alias("_pre_reason"),
            F.lit(on_baby).alias("_on_baby"),
        )

    pobs = F.concat(F.lit("pobs:"), F.col("journey_pregnancy_key"), F.lit(":"))


    def outside(col, low, high, reason):
        return F.when(~F.col(col).between(low, high), F.lit(reason))

    frames = [
        fact(hist.where(F.col("gravida").isNotNull()), F.col("person_id"), F.concat(pobs, F.lit("gravida")), hist_at,
             4060186, F.col("gravida"), None, None, F.lit("Gravida_NBR"), None, null_s,
             outside("gravida", 1, 30, "gravida_implausible"), False),
        fact(hist.where(F.col("parity").isNotNull()), F.col("person_id"), F.concat(pobs, F.lit("parity")), hist_at,
             4264419, F.col("parity"), None, None, F.lit("Parity"), None, null_s,
             outside("parity", 0, 20, "parity_implausible"), False),
    ]
    births = read_source("src_birth").where(F.col("record_status") == "active")
    bir = F.concat(F.lit("bir:"), F.col("birth_key"), F.lit(":"))
    at = F.col("birth_datetime")
    baby = F.col("baby_person_id")
    no_baby = F.when(baby.isNull(), F.lit("baby_person_unresolved"))
    for field, col, suffix, on_baby in (("DeliveryMethod", "delivery_method_code", "method", False),
                                        ("DeliveryOutcome", "delivery_outcome_code", "outcome", False),
                                        ("PregOutcome", "pregnancy_outcome_code", "preg_outcome", False),
                                        ("NeonatalOutcome", "neonatal_outcome_code", "neonatal", True)):
        frames.append(fact(births.where(F.col(col).isNotNull()), baby if on_baby else F.col("person_id"),
                           F.concat(bir, F.lit(suffix)), at, None, null_d, None, None,
                           F.col(col), field, F.col(col), no_baby if on_baby else null_s, on_baby))
    for suffix, col, concept, number, unit, unit_src, reason in (
            ("weight", "birth_weight_grams", 4264825, F.col("birth_weight_grams"), 8504, "g",
             F.when(F.col("birth_weight_implausible_ind"), F.lit("birth_weight_implausible"))),
            ("apgar1", "apgar_1_minute", 3016704, F.col("apgar_1_minute"), None, None,
             outside("apgar_1_minute", 0, 10, "apgar_out_of_range")),
            ("apgar5", "apgar_5_minute", 3004221, F.col("apgar_5_minute"), None, None,
             outside("apgar_5_minute", 0, 10, "apgar_out_of_range")),
            # gestation in weeks needs both parts: a missing day count is never defaulted to 0
            ("gestation", "gestation_weeks", 4260747,
             F.round(F.col("gestation_weeks") + F.col("gestation_days") / 7.0, 2), 8511, "wk",
             F.coalesce(F.when(F.col("gestation_days").isNull(), F.lit("gestation_days_missing")),
                        outside("gestation_weeks", 20, 45, "gestation_implausible"),
                        outside("gestation_days", 0, 6, "gestation_implausible")))):
        frames.append(fact(births.where(F.col(col).isNotNull()), baby, F.concat(bir, F.lit(suffix)), at, concept,
                           number, unit, unit_src, F.lit(col), None, null_s,
                           F.coalesce(no_baby, reason), True))
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    mapped = out.join(_maternity_map(), (F.col("_map_field") == F.col("_mm_field"))
                      & (F.col("_map_code") == F.col("_mm_code")), "left")
    return mapped.withColumn("_concept", F.coalesce(F.col("_fixed_concept"), F.col("_mm_concept"))).withColumn(
        "_pre_reason",
        F.coalesce(F.col("_pre_reason"),
                   F.when(F.col("_map_field").isNotNull() & F.col("_mm_concept").isNull(),
                          F.lit("maternity_code_unmapped"))))


def _maternity_base():
    domains = read_source("src_vocab_concept").select(
        F.col("concept_id").cast("int").alias("_md_concept"), F.col("domain_id").alias("_md_domain"))
    table = F.create_map(*[F.lit(x) for kv in MATERNITY_TABLE_BY_DOMAIN.items() for x in kv])
    gated = _adapter_gate(
        _maternity_facts().join(domains, F.col("_concept") == F.col("_md_concept"), "left"),
        F.col("_key"), F.col("_at"))
    return (
        gated.withColumn("_table", table[F.col("_md_domain")])
        .withColumn("_value_concept", F.lit(None).cast("int"))
        .withColumn("_value_source", F.lit(None).cast("string"))
        .withColumn("_lane", F.lit("maternity"))
        .withColumn("_evidence_start", F.col("_ad_at"))
        .withColumn("_exclusion_reason", F.coalesce(
            F.col("_pre_reason"),
            F.when(F.col("_concept").isNotNull() & F.col("_table").isNull(), F.lit("maternity_domain_unsupported")),
            F.col("_gate_reason")))
    )


def _maternity_published():
    return _maternity_base().where(F.col("_exclusion_reason").isNull())


def _maternity_rows_for(table):
    return _cdm_rows(table, _maternity_published().where(F.col("_table") == table))


def _maternity_rows_procedure_occurrence(**_):
    return _maternity_rows_for("procedure_occurrence")


def _maternity_rows_condition_occurrence(**_):
    return _maternity_rows_for("condition_occurrence")


def _maternity_rows_observation(**_):
    return _maternity_rows_for("observation")


def _maternity_rows_measurement(**_):
    return _maternity_rows_for("measurement")


def _maternity_metrics():
    return _metric(_maternity_base(), "maternity", "_lane")


def _maternity_evidence():
    return _maternity_published().select("person_id", "_evidence_start")


def _maternity_provenance():
    rows = _maternity_published()
    return _provenance_rows(
        rows, F.col("_table"), F.col("_ad_id"), _cdm_concept_field(F.col("_table")), F.col("_concept"), F.lit(0),
        F.lit("Millennium maternity"),
        F.coalesce(F.col("_mm_method"), F.lit("fixed_field_concept")), F.lit(None),
        F.coalesce(F.col("_mm_rule"), F.lit("mat_birth_field:v1")), F.lit(None),
        F.coalesce(F.col("_mm_version"), F.lit(PIPELINE_VERSION)))


def _episode_registry():
    return read_source("src_id_registry").where(F.col("id_space") == "episode").select(
        F.col("source_key").alias("_ep_key"), F.col("allocated_id").alias("_ep_id"))


def _episode_base():
    people = _person_base().where(F.col("_exclusion_reason").isNull()).select(F.col("person_id").alias("_ep_person"))
    journeys = read_source("src_journey").where(
        (F.col("journey_type_code") == "pregnancy") & F.col("parent_journey_key").isNull()
        & (F.coalesce(F.col("status_code"), F.lit("")) != "superseded"))
    start = F.col("period_start").cast("timestamp")
    end = F.when(F.col("status_code") == "active", F.lit(None).cast("timestamp")).otherwise(
        F.col("period_end").cast("timestamp"))
    person = F.expr("try_cast(person_id AS bigint)")
    number = F.row_number().over(Window.partitionBy(person).orderBy(start.asc_nulls_last(), F.col("journey_key").asc()))
    j = (journeys.withColumn("_ep_person_id", person).withColumn("_ep_start", start).withColumn("_ep_end", end)
         .withColumn("_ep_number", number)
         .join(_episode_registry(), F.concat(F.lit("preg:"), F.col("journey_key")) == F.col("_ep_key"), "left")
         .join(people, F.col("_ep_person_id") == F.col("_ep_person"), "left"))
    return j.select(
        F.col("_ep_id").cast("bigint").alias("episode_id"),
        F.col("_ep_person_id").alias("person_id"),
        F.lit(32533).cast("int").alias("episode_concept_id"),
        F.to_date("_ep_start").alias("episode_start_date"),
        F.col("_ep_start").alias("episode_start_datetime"),
        F.to_date("_ep_end").alias("episode_end_date"),
        F.col("_ep_end").alias("episode_end_datetime"),
        F.lit(None).cast("bigint").alias("episode_parent_id"),
        F.col("_ep_number").cast("int").alias("episode_number"),
        F.lit(4299535).cast("int").alias("episode_object_concept_id"),
        F.lit(32817).cast("int").alias("episode_type_concept_id"),
        F.lit("pregnancy").alias("episode_source_value"),
        F.lit(0).cast("int").alias("episode_source_concept_id"),
        F.col("journey_key").alias("_journey_key"),
        F.when(F.col("_ep_start").isNull(), F.lit("missing_episode_start"))
        .when(F.to_date("_ep_start") < F.to_date(F.lit(STUDY_START)), F.lit("before_study_window"))
        .when(F.col("_ep_person_id").isNull(), F.lit("person_unresolved"))
        .when(F.col("_ep_person").isNull(), F.lit("person_not_published"))
        .when(F.col("_ep_id").isNull(), F.lit("id_not_allocated")).alias("_exclusion_reason"),
    )


def _episode_metrics():
    return _metric(_episode_base(), "episode")


# === GENERATED: explicit schema for episode (OGR) — do not edit ===
episode_schema = T.StructType([
    T.StructField('episode_id', T.LongType(), False, metadata={'comment': 'Persistent registry allocation for preg:<journey_key> (id space episode); never re-minted.'}),
    T.StructField('person_id', T.LongType(), False, metadata={'comment': 'Published mother person_id.'}),
    T.StructField('episode_concept_id', T.IntegerType(), False, metadata={'comment': '32533 Disease Episode for pregnancy episodes.'}),
    T.StructField('episode_start_date', T.DateType(), False, metadata={'comment': 'spine_journey pregnancy period_start date.'}),
    T.StructField('episode_start_datetime', T.TimestampType(), True, metadata={'comment': 'spine_journey pregnancy period_start.'}),
    T.StructField('episode_end_date', T.DateType(), True, metadata={'comment': 'period_end date; NULL while the pregnancy is active.'}),
    T.StructField('episode_end_datetime', T.TimestampType(), True, metadata={'comment': 'period_end; NULL while the pregnancy is active.'}),
    T.StructField('episode_parent_id', T.LongType(), True, metadata={'comment': 'NULL: pregnancy episodes have no parent episode.'}),
    T.StructField('episode_number', T.IntegerType(), True, metadata={'comment': 'Pregnancy ordinal for the person by start (1-based; ties by journey key).'}),
    T.StructField('episode_object_concept_id', T.IntegerType(), False, metadata={'comment': '4299535 Pregnancy.'}),
    T.StructField('episode_type_concept_id', T.IntegerType(), False, metadata={'comment': '32817 EHR.'}),
    T.StructField('episode_source_value', T.StringType(), True, metadata={'comment': 'pregnancy.'}),
    T.StructField('episode_source_concept_id', T.IntegerType(), True, metadata={'comment': '0: no source concept.'}),
])
# === END GENERATED: explicit schema for episode ===

@dp.materialized_view(
    name="episode",
    schema=episode_schema,
    comment="OMOP EPISODE: one pregnancy episode per non-superseded spine_journey pregnancy (32533 / object 4299535).",
)
def episode():
    return _episode_base().where(F.col("_exclusion_reason").isNull()).select(
        *[f.name for f in episode_schema.fields])


# === GENERATED: explicit schema for episode_event (OGR) — do not edit ===
episode_event_schema = T.StructType([
    T.StructField('episode_id', T.LongType(), False, metadata={'comment': 'Published pregnancy episode.'}),
    T.StructField('event_id', T.LongType(), False, metadata={'comment': 'Primary key of the linked published row (table given by episode_event_field_concept_id).'}),
    T.StructField('episode_event_field_concept_id', T.IntegerType(), False, metadata={'comment': '1147082 procedure_occurrence, 1147127 condition_occurrence, 1147138 measurement or 1147165 observation id field.'}),
])
# === END GENERATED: explicit schema for episode_event ===

@dp.materialized_view(
    name="episode_event",
    schema=episode_event_schema,
    comment="OMOP EPISODE_EVENT: links each published mother-side maternity fact (delivery, outcome, gravida/parity) to its pregnancy episode.",
)
def episode_event():
    episodes = _episode_base().where(F.col("_exclusion_reason").isNull()).select(
        F.col("episode_id"), F.col("person_id").alias("_ee_person"), F.col("_journey_key").alias("_ee_journey"))
    field = F.create_map(*[F.lit(x) for kv in EPISODE_EVENT_FIELD.items() for x in kv])
    facts = _maternity_published().where(F.col("_journey_key").isNotNull() & ~F.col("_on_baby"))
    return facts.join(
        episodes, (F.col("_journey_key") == F.col("_ee_journey")) & (F.col("person_id") == F.col("_ee_person")), "inner"
    ).select(
        F.col("episode_id").cast("bigint").alias("episode_id"),
        F.col("_ad_id").cast("bigint").alias("event_id"),
        field[F.col("_table")].cast("int").alias("episode_event_field_concept_id"),
    )


_register_extension("procedure_occurrence", _maternity_rows_procedure_occurrence)
_register_extension("condition_occurrence", _maternity_rows_condition_occurrence)
_register_extension("observation", _maternity_rows_observation)
_register_extension("measurement", _maternity_rows_measurement)
METRIC_BUILDERS.append(_maternity_metrics)
METRIC_BUILDERS.append(_episode_metrics)
EVIDENCE_BUILDERS.append(_maternity_evidence)
PROVENANCE_BUILDERS.append(_maternity_provenance)


# OGR_VTE_V1: maternity VTE risk assessment form -> procedure 40483208, guarded item facts routed by
# concept domain (observation / measurement with value and unit), and the total score.
VTE_ITEMS = ["age_35_parity_3", "ob_vte_obesity", "obstetric_vte_risk_assessment_type", "previous_vte", "smoker", "current_systemic_infection", "dehydration_reduced_immobility_art_ivf", "family_history_of_vte", "gross_varicose_veins", "hyperemesis", "medical_comorbidities", "ohss_overian_hyperstimulation_syndrome", "pre_eclampsia", "surg_procedure_in_this_preg_or_6_weeks", "vte_known_thrombophilia", "patient_at_risk_of_vte", "height_length_measured", "weight_measured", "maternity_vte_transient_risk_score", "caesarean_section_in_labour", "elective_caesarean_section", "mid_cavity_or_rotational_forceps", "pph_1_litre_or_more_and_or_transfusion", "preterm_birth_this_pregnancy", "prolonged_labour_over_24_hours", "still_birth_this_pregnancy", "multiple_pregnancy", "maternity_vte_action_plan", "body_mass_index_measured", "bmi", "multiple_pregnancy_twins_or_more", "maternity_vte_permanent_risk_score", "contraindication_to_lmwh_or_heparin", "heparin_type", "maternity_vte_intermediate_trans_risk", "medical_comorbidities_type", "maternity_vte_intermediate_perm_risk", "antiphospholipid_antibodies", "factorv_leiden_heterozygous", "prothrombin_gene_mutation_heterozygous"]


def _vte_item_map():
    return read_source("lkp_vte_item_map").select(
        F.col("item_code").alias("_vq_item"), F.col("question_concept_id").cast("int").alias("_vq_concept"),
        F.col("target_table").alias("_vq_table"), F.col("value_kind").alias("_vq_kind"),
        F.col("unit_concept_id").cast("int").alias("_vq_unit"), F.col("unit_source_value").alias("_vq_unit_source"),
        F.col("map_method").alias("_vq_method"), F.col("map_confidence").alias("_vq_confidence"),
        F.col("map_rule_id").alias("_vq_rule"), F.col("map_version").alias("_vq_version"))


def _vte_base():
    forms = read_source("src_vte_form").where(F.col("record_status") == "active")
    pek = F.col("patient_event_key")
    base_cols = ["person_id", "encounter_id", "event_datetime", "patient_event_key"]
    null_i, null_d, null_s = F.lit(None).cast("int"), F.lit(None).cast("double"), F.lit(None).cast("string")
    form = forms.select(
        *base_cols, F.concat(F.lit("vte:"), pek, F.lit(":form")).alias("_key"),
        F.lit("form").alias("_item"), F.lit(40483208).alias("_fixed_concept"), null_d.alias("_value_number"),
        null_i.alias("_value_concept"), null_s.alias("_value_source"),
        F.lit("procedure_occurrence").alias("_fixed_table"))
    total_value = F.coalesce(F.expr("try_cast(obstetric_vte_risk_total_v2 AS double)"),
                             F.expr("try_cast(maternity_vte_total_risk_score AS double)"))
    total = forms.where(total_value.isNotNull()).select(
        *base_cols, F.concat(F.lit("vte:"), pek, F.lit(":total")).alias("_key"),
        F.lit("total").alias("_item"), null_i.alias("_fixed_concept"), total_value.alias("_value_number"),
        null_i.alias("_value_concept"), null_s.alias("_value_source"), null_s.alias("_fixed_table"))
    items = forms.select(*base_cols, F.explode(F.array(
        F.struct(F.lit("age_35_parity_3").alias("item"), F.col("age_35_parity_3").cast("string").alias("raw"), F.col("age_35_parity_3_text").cast("string").alias("text"), F.expr("try_cast(age_35_parity_3_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("ob_vte_obesity").alias("item"), F.col("ob_vte_obesity").cast("string").alias("raw"), F.col("ob_vte_obesity_text").cast("string").alias("text"), F.expr("try_cast(ob_vte_obesity_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("obstetric_vte_risk_assessment_type").alias("item"), F.col("obstetric_vte_risk_assessment_type").cast("string").alias("raw"), F.col("obstetric_vte_risk_assessment_type_text").cast("string").alias("text"), F.expr("try_cast(obstetric_vte_risk_assessment_type_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("previous_vte").alias("item"), F.col("previous_vte").cast("string").alias("raw"), F.col("previous_vte_text").cast("string").alias("text"), F.expr("try_cast(previous_vte_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("smoker").alias("item"), F.col("smoker").cast("string").alias("raw"), F.col("smoker_text").cast("string").alias("text"), F.expr("try_cast(smoker_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("current_systemic_infection").alias("item"), F.col("current_systemic_infection").cast("string").alias("raw"), F.col("current_systemic_infection_text").cast("string").alias("text"), F.expr("try_cast(current_systemic_infection_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("dehydration_reduced_immobility_art_ivf").alias("item"), F.col("dehydration_reduced_immobility_art_ivf").cast("string").alias("raw"), F.col("dehydration_reduced_immobility_art_ivf_text").cast("string").alias("text"), F.expr("try_cast(dehydration_reduced_immobility_art_ivf_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("family_history_of_vte").alias("item"), F.col("family_history_of_vte").cast("string").alias("raw"), F.col("family_history_of_vte_text").cast("string").alias("text"), F.expr("try_cast(family_history_of_vte_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("gross_varicose_veins").alias("item"), F.col("gross_varicose_veins").cast("string").alias("raw"), F.col("gross_varicose_veins_text").cast("string").alias("text"), F.expr("try_cast(gross_varicose_veins_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("hyperemesis").alias("item"), F.col("hyperemesis").cast("string").alias("raw"), F.col("hyperemesis_text").cast("string").alias("text"), F.expr("try_cast(hyperemesis_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("medical_comorbidities").alias("item"), F.col("medical_comorbidities").cast("string").alias("raw"), F.col("medical_comorbidities_text").cast("string").alias("text"), F.expr("try_cast(medical_comorbidities_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("ohss_overian_hyperstimulation_syndrome").alias("item"), F.col("ohss_overian_hyperstimulation_syndrome").cast("string").alias("raw"), F.col("ohss_overian_hyperstimulation_syndrome_text").cast("string").alias("text"), F.expr("try_cast(ohss_overian_hyperstimulation_syndrome_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("pre_eclampsia").alias("item"), F.col("pre_eclampsia").cast("string").alias("raw"), F.col("pre_eclampsia_text").cast("string").alias("text"), F.expr("try_cast(pre_eclampsia_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("surg_procedure_in_this_preg_or_6_weeks").alias("item"), F.col("surg_procedure_in_this_preg_or_6_weeks").cast("string").alias("raw"), F.col("surg_procedure_in_this_preg_or_6_weeks_text").cast("string").alias("text"), F.expr("try_cast(surg_procedure_in_this_preg_or_6_weeks_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("vte_known_thrombophilia").alias("item"), F.col("vte_known_thrombophilia").cast("string").alias("raw"), F.col("vte_known_thrombophilia_text").cast("string").alias("text"), F.expr("try_cast(vte_known_thrombophilia_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("patient_at_risk_of_vte").alias("item"), F.col("patient_at_risk_of_vte").cast("string").alias("raw"), F.col("patient_at_risk_of_vte_text").cast("string").alias("text"), F.expr("try_cast(patient_at_risk_of_vte_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("height_length_measured").alias("item"), F.col("height_length_measured").cast("string").alias("raw"), F.col("height_length_measured_text").cast("string").alias("text"), F.lit(None).cast("int").alias("concept"), F.expr("try_cast(height_length_measured AS double)").alias("number")),
        F.struct(F.lit("weight_measured").alias("item"), F.col("weight_measured").cast("string").alias("raw"), F.col("weight_measured_text").cast("string").alias("text"), F.lit(None).cast("int").alias("concept"), F.expr("try_cast(weight_measured AS double)").alias("number")),
        F.struct(F.lit("maternity_vte_transient_risk_score").alias("item"), F.col("maternity_vte_transient_risk_score").cast("string").alias("raw"), F.col("maternity_vte_transient_risk_score_text").cast("string").alias("text"), F.lit(None).cast("int").alias("concept"), F.expr("try_cast(maternity_vte_transient_risk_score AS double)").alias("number")),
        F.struct(F.lit("caesarean_section_in_labour").alias("item"), F.col("caesarean_section_in_labour").cast("string").alias("raw"), F.col("caesarean_section_in_labour_text").cast("string").alias("text"), F.expr("try_cast(caesarean_section_in_labour_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("elective_caesarean_section").alias("item"), F.col("elective_caesarean_section").cast("string").alias("raw"), F.col("elective_caesarean_section_text").cast("string").alias("text"), F.expr("try_cast(elective_caesarean_section_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("mid_cavity_or_rotational_forceps").alias("item"), F.col("mid_cavity_or_rotational_forceps").cast("string").alias("raw"), F.col("mid_cavity_or_rotational_forceps_text").cast("string").alias("text"), F.expr("try_cast(mid_cavity_or_rotational_forceps_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("pph_1_litre_or_more_and_or_transfusion").alias("item"), F.col("pph_1_litre_or_more_and_or_transfusion").cast("string").alias("raw"), F.col("pph_1_litre_or_more_and_or_transfusion_text").cast("string").alias("text"), F.expr("try_cast(pph_1_litre_or_more_and_or_transfusion_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("preterm_birth_this_pregnancy").alias("item"), F.col("preterm_birth_this_pregnancy").cast("string").alias("raw"), F.col("preterm_birth_this_pregnancy_text").cast("string").alias("text"), F.expr("try_cast(preterm_birth_this_pregnancy_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("prolonged_labour_over_24_hours").alias("item"), F.col("prolonged_labour_over_24_hours").cast("string").alias("raw"), F.col("prolonged_labour_over_24_hours_text").cast("string").alias("text"), F.expr("try_cast(prolonged_labour_over_24_hours_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("still_birth_this_pregnancy").alias("item"), F.col("still_birth_this_pregnancy").cast("string").alias("raw"), F.col("still_birth_this_pregnancy_text").cast("string").alias("text"), F.expr("try_cast(still_birth_this_pregnancy_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("multiple_pregnancy").alias("item"), F.col("multiple_pregnancy").cast("string").alias("raw"), F.col("multiple_pregnancy_text").cast("string").alias("text"), F.expr("try_cast(multiple_pregnancy_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("maternity_vte_action_plan").alias("item"), F.col("maternity_vte_action_plan").cast("string").alias("raw"), F.col("maternity_vte_action_plan_text").cast("string").alias("text"), F.expr("try_cast(maternity_vte_action_plan_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("body_mass_index_measured").alias("item"), F.col("body_mass_index_measured").cast("string").alias("raw"), F.col("body_mass_index_measured_text").cast("string").alias("text"), F.lit(None).cast("int").alias("concept"), F.expr("try_cast(body_mass_index_measured AS double)").alias("number")),
        F.struct(F.lit("bmi").alias("item"), F.col("bmi").cast("string").alias("raw"), F.col("bmi_text").cast("string").alias("text"), F.lit(None).cast("int").alias("concept"), F.expr("try_cast(bmi AS double)").alias("number")),
        F.struct(F.lit("multiple_pregnancy_twins_or_more").alias("item"), F.col("multiple_pregnancy_twins_or_more").cast("string").alias("raw"), F.col("multiple_pregnancy_twins_or_more_text").cast("string").alias("text"), F.expr("try_cast(multiple_pregnancy_twins_or_more_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("maternity_vte_permanent_risk_score").alias("item"), F.col("maternity_vte_permanent_risk_score").cast("string").alias("raw"), F.col("maternity_vte_permanent_risk_score_text").cast("string").alias("text"), F.lit(None).cast("int").alias("concept"), F.expr("try_cast(maternity_vte_permanent_risk_score AS double)").alias("number")),
        F.struct(F.lit("contraindication_to_lmwh_or_heparin").alias("item"), F.col("contraindication_to_lmwh_or_heparin").cast("string").alias("raw"), F.col("contraindication_to_lmwh_or_heparin_text").cast("string").alias("text"), F.expr("try_cast(contraindication_to_lmwh_or_heparin_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("heparin_type").alias("item"), F.col("heparin_type").cast("string").alias("raw"), F.col("heparin_type_text").cast("string").alias("text"), F.expr("try_cast(heparin_type_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("maternity_vte_intermediate_trans_risk").alias("item"), F.col("maternity_vte_intermediate_trans_risk").cast("string").alias("raw"), F.col("maternity_vte_intermediate_trans_risk_text").cast("string").alias("text"), F.expr("try_cast(maternity_vte_intermediate_trans_risk_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("medical_comorbidities_type").alias("item"), F.col("medical_comorbidities_type").cast("string").alias("raw"), F.col("medical_comorbidities_type_text").cast("string").alias("text"), F.expr("try_cast(medical_comorbidities_type_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("maternity_vte_intermediate_perm_risk").alias("item"), F.col("maternity_vte_intermediate_perm_risk").cast("string").alias("raw"), F.col("maternity_vte_intermediate_perm_risk_text").cast("string").alias("text"), F.expr("try_cast(maternity_vte_intermediate_perm_risk_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("antiphospholipid_antibodies").alias("item"), F.col("antiphospholipid_antibodies").cast("string").alias("raw"), F.col("antiphospholipid_antibodies_text").cast("string").alias("text"), F.expr("try_cast(antiphospholipid_antibodies_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("factorv_leiden_heterozygous").alias("item"), F.col("factorv_leiden_heterozygous").cast("string").alias("raw"), F.col("factorv_leiden_heterozygous_text").cast("string").alias("text"), F.expr("try_cast(factorv_leiden_heterozygous_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number")),
        F.struct(F.lit("prothrombin_gene_mutation_heterozygous").alias("item"), F.col("prothrombin_gene_mutation_heterozygous").cast("string").alias("raw"), F.col("prothrombin_gene_mutation_heterozygous_text").cast("string").alias("text"), F.expr("try_cast(prothrombin_gene_mutation_heterozygous_concept_id AS int)").alias("concept"), F.lit(None).cast("double").alias("number"))
    )).alias("_i")).where(F.col("_i.raw").isNotNull() | F.col("_i.text").isNotNull()).select(
        *base_cols, F.concat(F.lit("vte:"), pek, F.lit(":"), F.col("_i.item")).alias("_key"),
        F.col("_i.item").alias("_item"), null_i.alias("_fixed_concept"),
        F.col("_i.number").alias("_value_number"), F.col("_i.concept").alias("_value_concept"),
        F.substring(F.coalesce(F.col("_i.text"), F.col("_i.raw")), 1, 50).alias("_value_source"),
        null_s.alias("_fixed_table"))
    facts = (form.unionByName(total).unionByName(items)
             .join(_vte_item_map(), F.col("_item") == F.col("_vq_item"), "left")
             .withColumn("_concept", F.coalesce(F.col("_fixed_concept"), F.col("_vq_concept")))
             .withColumn("_table", F.coalesce(F.col("_fixed_table"), F.col("_vq_table"))))
    numeric = F.col("_vq_kind") == "numeric"
    gated = _adapter_gate(facts, F.col("_key"), F.col("event_datetime"))
    return (
        gated.withColumn("_source_value", F.col("_item"))
        # numeric items carry the canonical unit and no value concept; coded items carry the answer concept
        .withColumn("_unit", F.when(numeric, F.col("_vq_unit")))
        .withColumn("_unit_source", F.when(numeric, F.col("_vq_unit_source")))
        .withColumn("_value_concept", F.when(~F.coalesce(numeric, F.lit(False)), F.col("_value_concept")))
        .withColumn("_lane", F.lit("maternity_vte"))
        .withColumn("_evidence_start", F.col("_ad_at"))
        .withColumn("_exclusion_reason", F.coalesce(
            F.when(F.col("_concept").isNull(), F.lit("vte_question_unmapped")),
            F.when(numeric & F.col("_value_number").isNull(), F.lit("vte_value_not_numeric")),
            F.col("_gate_reason")))
    )


def _vte_published():
    return _vte_base().where(F.col("_exclusion_reason").isNull())


def _vte_rows_procedure_occurrence(**_):
    return _cdm_rows("procedure_occurrence", _vte_published().where(F.col("_table") == "procedure_occurrence"))


def _vte_rows_observation(**_):
    return _cdm_rows("observation", _vte_published().where(F.col("_table") == "observation"))


def _vte_rows_measurement(**_):
    return _cdm_rows("measurement", _vte_published().where(F.col("_table") == "measurement"))


def _vte_metrics():
    return _metric(_vte_base(), "maternity_vte", "_lane")


def _vte_evidence():
    return _vte_published().select("person_id", "_evidence_start")


def _vte_provenance():
    rows = _vte_published()
    return _provenance_rows(
        rows, F.col("_table"), F.col("_ad_id"), _cdm_concept_field(F.col("_table")), F.col("_concept"), F.lit(0), F.lit("PowerForm"),
        F.coalesce(F.col("_vq_method"), F.lit("fixed_form_concept")), F.col("_vq_confidence"),
        F.coalesce(F.col("_vq_rule"), F.lit("vte_form:v1")), F.lit(None),
        F.coalesce(F.col("_vq_version"), F.lit(PIPELINE_VERSION)))


_register_extension("procedure_occurrence", _vte_rows_procedure_occurrence)
_register_extension("observation", _vte_rows_observation)
_register_extension("measurement", _vte_rows_measurement)
METRIC_BUILDERS.append(_vte_metrics)
EVIDENCE_BUILDERS.append(_vte_evidence)
PROVENANCE_BUILDERS.append(_vte_provenance)


# OGR_VD_ATTACH_V1: fact-to-stay interval matching (published visit_detail of the same visit).
VISIT_DETAIL_ATTACH_TABLES = {
    "condition_occurrence": ("condition_occurrence_id", "condition_start_datetime"),
    "procedure_occurrence": ("procedure_occurrence_id", "procedure_datetime"),
    "observation": ("observation_id", "observation_datetime"),
    "device_exposure": ("device_exposure_id", "device_exposure_start_datetime"),
    "measurement": ("measurement_id", "measurement_datetime"),
    "drug_exposure": ("drug_exposure_id", "drug_exposure_start_datetime"),
}


def _attach_visit_detail(table):
    id_column, datetime_column = VISIT_DETAIL_ATTACH_TABLES[table]

    def attach(frame):
        details = read_source("src_om_visit_detail").select(
            F.col("visit_detail_id").alias("_vd_id"),
            F.col("visit_occurrence_id").alias("_vd_visit"),
            F.col("visit_detail_start_datetime").alias("_vd_start"),
            F.coalesce(F.col("visit_detail_end_datetime"), F.col("visit_detail_start_datetime")).alias("_vd_end"),
            F.when(F.col("visit_detail_concept_id") == 32037, F.lit(0)).otherwise(F.lit(1)).alias("_vd_pref"),
        )
        facts = frame.where(F.col("visit_occurrence_id").isNotNull() & F.col(datetime_column).isNotNull()).select(
            F.col(id_column).alias("_f_id"),
            F.col("visit_occurrence_id").alias("_f_visit"),
            F.col(datetime_column).alias("_f_at"),
        )
        best = (
            facts.join(details, (F.col("_f_visit") == F.col("_vd_visit"))
                       & (F.col("_vd_start") <= F.col("_f_at")) & (F.col("_f_at") <= F.col("_vd_end")), "inner")
            .groupBy("_f_id")
            .agg(F.min(F.struct(
                F.col("_vd_pref"),
                (-F.unix_timestamp(F.col("_vd_start"))).alias("_neg_start"),
                F.col("_vd_id"),
            )).alias("_best"))
            .select("_f_id", F.col("_best._vd_id").alias("_attached_vd"))
        )
        columns = frame.columns
        return (
            frame.join(best, F.col(id_column) == F.col("_f_id"), "left")
            .withColumn("visit_detail_id", F.coalesce(F.col("_attached_vd"), F.col("visit_detail_id")).cast("bigint"))
            .select(*columns)
        )

    return attach


for _table in VISIT_DETAIL_ATTACH_TABLES:
    _register_transform(_table, _attach_visit_detail(_table))


# OGR_METADATA_V1: policy rows for the OGR components (ids 37+).
METADATA_ROWS.extend([
    (37, 0, 0, "unit_domain_policy", "unit_concept_id is published only when it is a standard, valid Unit-domain concept (IMD 37524288 exempt); others are demoted to 0 with unit_source_value kept; counts in omop_exclusion_metrics lane unit_guard", None, None, None, None),
    (38, 0, 0, "lsoa_location_policy", "one LOCATION per LSOA from the current HOME address (natural key 8e15 + prefix_rank*1e9 + digits; E01=1, W01=2); county = modal local authority; country_concept_id 42035286; person.location_id = current-address LSOA; street address and postcode never published", None, None, None, None),
    (39, 0, 0, "person_care_site_policy", "person.care_site_id is NULL by decision: silver holds no primary care-site assertion; the registered GP is a practitioner and is published as person.provider_id", None, None, None, None),
    (40, 0, 0, "death_cause_policy", "cause_concept_id = unique exact normalised-name match of cause text to a standard SNOMED Condition, then synonym; 0 when unmatched or ambiguous; cause text itself not published beyond 50 characters", None, None, None, None),
    (41, 0, 0, "place_of_service_policy", "care_site.place_of_service_concept_id from the dominant published visit concept at the site: 9201->8717, 9202->8756, 9203->8870; 0 otherwise", None, None, None, None),
    (42, 0, 0, "visit_detail_attach_policy", f"fact rows in {', '.join(VISIT_DETAIL_ATTACH_TABLES)} take the published visit_detail of the same visit whose interval contains the event datetime (critical care first, latest start, lowest id); visit_detail parent = containing ward stay; admitted_from on the first detail, discharged_to on the last", None, None, None, None),
    (43, 0, 0, "allergy_policy", "direct adapter: observation 43530807 (37396387 for absence assertions), value_as_concept_id = substance (silver concept, else SNOMED standard/Maps-to); Proposed excluded; positive rows without a substance concept excluded; key alg:<patient_event_id>", None, None, None, None),
    (44, 0, 0, "supply_policy", "homecare supply: COMPLETE items only; standards by dm+d Maps-to, else per ' + ' component exact ingredient name (after the uk_ban_alias:v1 rewrite), else unique ingredient synonym; start=end=release date; type 32825; quantity only for single-standard events; key sup:<patient_event_id>:<standard>", None, None, None, None),
    (45, 0, 0, "provenance_policy", "omop_concept_provenance carries method, confidence (silver, else S3-axis JSON; never invented), rule id, scoring model and version per published concept field", None, None, None, None),
    (46, 0, 0, "stage_policy", "stage group -> Cancer Modifier AJCC/UICC concept via 3_lookup.omop.omop_stage_group_map (rule ajcc_uicc_code:v1); measurement_event_id = same-event condition_occurrence (1147127); unmapped displays excluded", None, None, None, None),
    (47, 0, 0, "provider_policy", "visit provider = attending > admitting > consulting > mental-health attending > locum from spine_care_participation; visit_detail = attending valid at detail start; person = current registered GP (code set 331 PCP); device = implant performer", None, None, None, None),
    (48, 0, 0, "pregnancy_episode_policy", "episode 32533 (Disease Episode, object 4299535 Pregnancy) per non-superseded spine_journey pregnancy; episode_event links delivery procedures, outcomes and pregnancy measurements; gravida 4060186 (excluded outside 1-30) and parity 4264419 (excluded outside 0-20) as measurements at booking; key preg:<journey_key>", None, None, None, None),
    (49, 0, 0, "infant_birth_policy", "clinical_birth rows: birth weight, Apgar 1/5 and gestation at birth on the baby person when published, delivery method, delivery outcome and pregnancy outcome on the mother, neonatal outcome on the baby; unmapped Cerner codes excluded; implausible weights (bronze flag), Apgar outside 0-10, gestation weeks outside 20-45 / days outside 0-6 or days not recorded excluded with named reasons (silver keeps values as recorded); baby MRN/NHS never carried", None, None, None, None),
    (50, 0, 0, "maternity_vte_policy", "maternity VTE risk assessment form -> procedure 40483208, total score observation 43054879, and items via 3_lookup.omop.omop_maternity_vte_item_map: question concept guarded (standard Obs/Meas, not a hub >5 labels, not a panel/tool, <=2 items per concept, label agreement), routed by domain, numeric items keep value + canonical unit; unguarded items excluded vte_question_unmapped; telephone/completer/system items never published", None, None, None, None),
    (51, 0, 0, "drug_administration_status_policy", "medication administrations with In Error (31) or Not Done (36) result status are excluded as drug_in_error / drug_not_administered before concept resolution", None, None, None, None),
])


# OGR_UNIT_GUARD_V1: unit_concept_id must be a valid standard Unit-domain concept.
def _unit_concepts():
    return (
        read_source("src_vocab_concept")
        .where((F.col("domain_id") == "Unit") & (F.col("standard_concept") == "S")
               & F.col("invalid_reason").isNull())
        .select(F.col("concept_id").cast("int").alias("_unit_ok"))
    )


def _unit_exempt(frame):
    """HERON-UK IMD rows keep their convention unit, which the pinned vocabulary does not carry."""
    if "observation_source_concept_id" in frame.columns:
        return F.coalesce(F.col("observation_source_concept_id") == F.lit(IMD_SOURCE_CONCEPT_ID), F.lit(False))
    return F.lit(False)


def _guard_unit(frame):
    ok = F.broadcast(_unit_concepts())
    columns = frame.columns
    return (
        frame.join(ok, F.col("unit_concept_id") == F.col("_unit_ok"), "left")
        .withColumn(
            "unit_concept_id",
            F.when(F.col("_unit_ok").isNotNull() | F.col("unit_concept_id").isNull() | _unit_exempt(frame),
                   F.col("unit_concept_id"))
            .otherwise(F.lit(0)).cast("int"),
        )
        .select(*columns)
    )


def _unit_guard_metrics():
    ok = F.broadcast(_unit_concepts())
    frames = []
    for table, core in (("measurement", _measurement_core), ("observation", _observation_core),
                        ("device_exposure", _device_exposure_core)):
        rows = core()
        frames.append(
            rows.join(ok, F.col("unit_concept_id") == F.col("_unit_ok"), "left")
            .where((F.coalesce(F.col("unit_concept_id"), F.lit(0)) != 0) & F.col("_unit_ok").isNull()
                   & ~_unit_exempt(rows))
            .agg(F.count(F.lit(1)).cast("bigint").alias("excluded_rows"))
            .select(F.lit("unit_guard").alias("captured_model"), F.lit(table).alias("lane"),
                    F.lit("unit_concept_demoted_not_unit_domain").alias("reason"), "excluded_rows",
                    F.lit(PIPELINE_VERSION).alias("pipeline_version"))
        )
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


_register_transform("measurement", _guard_unit)
_register_transform("observation", _guard_unit)
_register_transform("device_exposure", _guard_unit)
METRIC_BUILDERS.append(_unit_guard_metrics)


# OGR_PROVENANCE_V1: one row per published concept field with its mapping provenance.
PROVENANCE_ROUTED_FIELDS = {
    "condition_occurrence": "condition_concept_id",
    "procedure_occurrence": "procedure_concept_id",
    "observation": "observation_concept_id",
    "device_exposure": "device_concept_id",
    "measurement": "measurement_concept_id",
    "drug_exposure": "drug_concept_id",
}


def _routed_provenance():
    routed = read_source("src_routed_events").where(
        F.col("_exclusion_reason").isNull() & F.col("final_table").isin(*PROVENANCE_ROUTED_FIELDS))
    axis = F.when(F.col("map_source").startswith("s3_axis:"), F.expr("substring(map_source, 9)"))
    events = (
        read_source("src_patient_event")
        .groupBy("patient_event_id", "mapped_code", "mapped_coding_system")
        .agg(F.min(F.struct(
            F.coalesce(F.col("map_method"), F.lit("")).alias("method"),
            F.coalesce(F.col("map_rule_id"), F.lit("")).alias("rule"),
            F.col("map_confidence").alias("confidence"),
            F.col("map_version").alias("version"),
            axis.alias("axis"),
        )).alias("_p"))
    )
    joined = routed.alias("r").join(
        events.alias("e"),
        (F.col("r.patient_event_id") == F.col("e.patient_event_id"))
        & F.col("r.mapped_code").eqNullSafe(F.col("e.mapped_code"))
        & F.col("r.mapped_coding_system").eqNullSafe(F.col("e.mapped_coding_system")),
        "left",
    )
    field = F.create_map(*[F.lit(x) for kv in PROVENANCE_ROUTED_FIELDS.items() for x in kv])
    # observation() publishes family-history rows as 4167217 in observation_concept_id with the routed
    # concept in value_as_concept_id, so their provenance describes value_as_concept_id.
    family = (F.col("r.final_table") == "observation") & (F.col("r.lane") == "family_history")
    return joined.select(
        F.col("r.final_table").alias("cdm_table"),
        F.col("r.allocated_id").cast("bigint").alias("cdm_row_id"),
        F.when(family, F.lit("value_as_concept_id")).otherwise(field[F.col("r.final_table")]).alias("concept_field"),
        F.col("r.standard_concept_id").cast("int").alias("concept_id"),
        F.col("r.source_concept_id").cast("int").alias("source_concept_id"),
        F.col("r.mapped_coding_system").cast("string").alias("coding_system"),
        F.nullif(F.col("_p.method"), F.lit("")).alias("map_method"),
        F.coalesce(F.col("_p.confidence"),
                   F.get_json_object(F.col("_p.axis"), "$.confidence").cast("double")).alias("map_confidence"),
        F.coalesce(F.nullif(F.col("_p.rule"), F.lit("")),
                   F.get_json_object(F.col("_p.axis"), "$.rule")).alias("map_rule_id"),
        F.get_json_object(F.col("_p.axis"), "$.scoring_model").alias("scoring_model"),
        F.col("_p.version").cast("string").alias("map_version"),
        F.lit(PIPELINE_VERSION).alias("pipeline_version"),
    )


def _death_provenance():
    rows = _death_base().where(F.col("_exclusion_reason").isNull() & F.col("_cause_map_method").isNotNull())
    return _provenance_rows(
        rows, "death", F.col("person_id"), "cause_concept_id", F.col("cause_concept_id"),
        F.col("cause_source_concept_id"), F.lit("SNOMED"), F.col("_cause_map_method"),
        F.lit(1.0), F.lit(None), F.lit(None), F.lit(None))


def _imd_provenance():
    # HERON-UK IMD rows: concept 0 by convention, identified by the fixed source concept.
    rows = _imd_base().where(F.col("_exclusion_reason").isNull())
    return _provenance_rows(
        rows, "observation", F.col("observation_id"), "observation_concept_id", F.col("observation_concept_id"),
        F.col("observation_source_concept_id"), F.lit("HERON-UK"), F.lit("fixed_source_concept"), F.lit(1.0),
        F.lit("heron_uk_imd_quintile"), F.lit(None), F.lit(PIPELINE_VERSION))


# === GENERATED: explicit schema for omop_concept_provenance (OGR) — do not edit ===
omop_concept_provenance_schema = T.StructType([
    T.StructField('cdm_table', T.StringType(), False, metadata={'comment': 'Published CDM table holding the row.'}),
    T.StructField('cdm_row_id', T.LongType(), False, metadata={'comment': 'Primary key of the row in cdm_table (person_id for death).'}),
    T.StructField('concept_field', T.StringType(), False, metadata={'comment': 'Concept column the provenance describes.'}),
    T.StructField('concept_id', T.IntegerType(), True, metadata={'comment': 'Published standard concept in concept_field.'}),
    T.StructField('source_concept_id', T.IntegerType(), True, metadata={'comment': 'Published source concept for the row; 0 when none.'}),
    T.StructField('coding_system', T.StringType(), True, metadata={'comment': 'Coding system of the mapped source code.'}),
    T.StructField('map_method', T.StringType(), True, metadata={'comment': 'Mapping method (source_native, rule, snomed_maps_to, exact_ingredient_name, ...).'}),
    T.StructField('map_confidence', T.DoubleType(), True, metadata={'comment': 'Silver map_confidence, else the S3-axis confidence; NULL when the upstream mapping states none.'}),
    T.StructField('map_rule_id', T.StringType(), True, metadata={'comment': 'Rule id of the mapping (silver map_rule_id, else the S3-axis rule).'}),
    T.StructField('scoring_model', T.StringType(), True, metadata={'comment': 'Embedding/scoring model for similarity-scored mappings; NULL for rule and exact mappings.'}),
    T.StructField('map_version', T.StringType(), True, metadata={'comment': 'Mapping release/version string from silver.'}),
    T.StructField('pipeline_version', T.StringType(), False, metadata={'comment': 'OMOP pipeline version that published the row.'}),
])
# === END GENERATED: explicit schema for omop_concept_provenance ===

@dp.materialized_view(
    name="omop_concept_provenance",
    schema=omop_concept_provenance_schema,
    comment="Mapping provenance sidecar: one row per published concept field (routed, adapter and death cause) with method, confidence, rule, scoring model and version.",
)
def omop_concept_provenance():
    frame = _routed_provenance().unionByName(_death_provenance()).unionByName(_imd_provenance())
    for builder in PROVENANCE_BUILDERS:
        frame = frame.unionByName(builder())
    return frame


# === GENERATED: explicit schema for omop_exclusion_metrics (contract_codegen.py) — do not edit ===
omop_exclusion_metrics_schema = T.StructType([
    T.StructField('captured_model', T.StringType(), False, metadata={'comment': 'OMOP model whose base lane was counted.'}),
    T.StructField('lane', T.StringType(), False, metadata={'comment': 'Input origin or publication lane.'}),
    T.StructField('reason', T.StringType(), False, metadata={'comment': 'Stable reason code.'}),
    T.StructField('excluded_rows', T.LongType(), False, metadata={'comment': 'Rows rejected from the public model.'}),
    T.StructField('pipeline_version', T.StringType(), False, metadata={'comment': 'OMOP pipeline version that produced the metric.'}),
])
# === END GENERATED: explicit schema for omop_exclusion_metrics ===

@dp.materialized_view(
    name="omop_exclusion_metrics",
    schema=omop_exclusion_metrics_schema,
    comment="Reason-coded pre-publication exclusion and era-eligibility counts for O1-O5 models.",
)
def omop_exclusion_metrics():
    frames = [
        _metric(_person_base(), "person"),
        _metric(_death_base(), "death", "_origin"),
        _metric(_location_base(), "location"),
        _metric(_care_site_base(), "care_site"),
        _metric(_provider_base(), "provider"),
        _metric(_visit_base(), "visit_occurrence"),
        _metric(_visit_detail_base(), "visit_detail", "_lane"),
        _metric(_observation_period_base(), "observation_period"),
        _metric(_imd_base(), "observation_imd"),
        _metric(_fact_relationship_base(), "fact_relationship"),
        _metric(
            read_source("src_routed_events").where(
                F.col("_exclusion_reason").isNull()
                | F.col("_exclusion_reason").isin(*EVENT_GRAIN_REASONS)
            ),
            "event_router", "lane",
        ),
        _metric(
            read_source("src_routed_events")
            .where(F.col("_exclusion_reason").isin(*ROW_GRAIN_REASONS))
            .withColumn("_row_lane", F.coalesce(F.col("final_table"), F.col("lane"))),
            "event_router_rows", "_row_lane",
        ),
        _metric(_unrouted_base(), "event_router_unmapped", "_lane"),
        _metric(_indication_base(), "event_router_indication", "_lane"),
        _metric(_specimen_base(), "specimen"),
        _metric(
            _measurement_specimen_pairs(), "fact_relationship_measurement_specimen"
        ),
        _metric(_genomic_test_base(), "genomic_test"),
        _metric(_gene_tested_base(), "genomic_gene_tested"),
        _metric(_genomic_variant_base(), "genomic_variant"),
        _metric(_era_base("drug_era", "drug_era"), "drug_era"),
        _metric(_era_base("dose_era", "dose_era"), "dose_era"),
        _metric(_era_base("condition_era", "condition_era"), "condition_era"),
        _era_eligibility_metrics(),
        *[builder() for builder in METRIC_BUILDERS],  # OGR_EXT_V1
    ]
    static_schema = T.StructType([
        T.StructField("captured_model", T.StringType(), False),
        T.StructField("lane", T.StringType(), False),
        T.StructField("reason", T.StringType(), False),
        T.StructField("excluded_rows", T.LongType(), False),
        T.StructField("pipeline_version", T.StringType(), False),
    ])
    frames.append(spark.createDataFrame([
        ("cdm_source", "cdm_source", "__none__", 0, PIPELINE_VERSION),
        ("metadata", "metadata", "__none__", 0, PIPELINE_VERSION),
        ("omop_person_research_flags", "omop_person_research_flags", "__none__", 0, PIPELINE_VERSION),
        ("o3_router_profile", "o3_router_profile", "__none__", 0, PIPELINE_VERSION),
        ("drug_exposure", "drug_exposure", "__none__", 0, PIPELINE_VERSION),
    ], static_schema))
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result
