# Databricks notebook source
# MAGIC %md
# MAGIC # OMOP Gold
# MAGIC Quality-controlled OMOP CDM over `omop_silver`. It succeeds the legacy `OMOP Clean`
# MAGIC pipeline and adds the data-quality rules first built for gold journey.
# MAGIC
# MAGIC Silver OMOP shapes and publishes every keyed person and every best-effort mapping.
# MAGIC Gold decides what a research user sees and counts everything it withholds:
# MAGIC
# MAGIC 1. Person gate: persons flagged in `omop_person_research_flags`, or born after the
# MAGIC    release date, are dropped together with all their facts.
# MAGIC 2. Mapping gate: facts whose standard concept fails the gold-journey S3D admission
# MAGIC    policy ("exclude only if likely wrong") are dropped.
# MAGIC 3. Release date: facts that start after `cdm_source.source_release_date` are dropped.
# MAGIC 4. Integrity: rows whose parent was dropped are dropped; nullable pointers to dropped
# MAGIC    rows are nulled; nullable end dates before their start are nulled.
# MAGIC
# MAGIC Every drop is decided once, in an `_og_audit_<table>` dataset, with one reason per
# MAGIC row. The public table is the silver table minus those rows, so for every table
# MAGIC silver = gold + audit drops. Advisory findings (event before birth, more than 30 days
# MAGIC after death, sex-implausible concept) keep the row and are listed in `omop_gold_dq_finding`.
# MAGIC
# MAGIC Design: `~/work/omop/omop_gold/docs/2026-10-16-omop-gold-design.md` (approved 2026-10-16).

# COMMAND ----------

import json

import pyspark.sql.functions as F
from pyspark import pipelines as dp

PIPELINE_VERSION = "1.0.0-og-2026-10-16"

# ==== Configuration and fail-closed source reads ====

# The pipeline reads only the OMOP silver schema of its environment and the governed
# vocabulary lookups. Any other read raises, so a dev pipeline can never publish prod
# data from a dev source, or the reverse.
ALLOWED_SOURCE_SCHEMAS = {
    "prod": ["4_prod.omop_silver", "3_lookup.omop"],
    "dev": ["8_dev.omop_gold_src", "3_lookup.omop"],
}
ENV = spark.conf.get("omop_gold.env")
SOURCE_SCHEMA = spark.conf.get("omop_gold.source_schema")
LOOKUP_SCHEMA = "3_lookup.omop"
if ENV not in ALLOWED_SOURCE_SCHEMAS or SOURCE_SCHEMA not in ALLOWED_SOURCE_SCHEMAS[ENV]:
    raise ValueError(f"omop_gold source {SOURCE_SCHEMA!r} is not allowed in environment {ENV!r}")


def _validated(schema, table):
    """Return the three-part name after checking the schema is allowlisted for this environment."""
    if schema not in ALLOWED_SOURCE_SCHEMAS[ENV]:
        raise ValueError(f"source outside the omop_gold allowlist rejected: {schema}.{table}")
    return f"{schema}.{table}"


def read_source(table):
    """Read one omop_silver table (or its dev sample)."""
    return spark.read.table(_validated(SOURCE_SCHEMA, table))


def read_lookup(table):
    """Read one governed vocabulary lookup from 3_lookup.omop."""
    return spark.read.table(_validated(LOOKUP_SCHEMA, table))


def source_exists(table):
    """True when an optional silver table (for example pregnancy or infant) is published.

    Resolving the schema is analysis only. `spark.catalog.tableExists` is not allowed inside
    a Lakeflow pipeline (Py4J whitelist), so a failed resolution is the not-found signal.
    """
    try:
        spark.read.table(_validated(SOURCE_SCHEMA, table)).schema
        return True
    except Exception as error:  # AnalysisException class differs between Spark Connect and classic
        if "TABLE_OR_VIEW_NOT_FOUND" in str(error) or "not found" in str(error).lower():
            return False
        raise


# OMOP CDM v5.4 column descriptions (from legacy OMOP Clean) used where a silver column has
# no comment. Silver location lost its comments when OMOP_AREA_OBS_V1 rebuilt it (Lakeflow
# applies MV comments on first build only).
FALLBACK_COMMENTS = {
    "location": {
        "location_id": "A unique identifier for each geographic location.",
        "address_1": "The first line of the address.",
        "address_2": "The second line of the address.",
        "city": "The city field is the text name of the city.",
        "state": "The state field contains the state name; outside the US, provinces or other administrative regions.",
        "zip": "The zip or postal code; for UK locations, the postcode district or LSOA-level code as published by silver.",
        "county": "The county, if available; may hold other regional information.",
        "location_source_value": "The verbatim value for the location as it appears in the source data.",
        "country_concept_id": "A foreign key to the Concept table for the country of the location.",
        "country_source_value": "The source code for the country as it appears in the source data.",
        "latitude": "The latitude of the location; between -90 and 90.",
        "longitude": "The longitude of the location; between -180 and 180.",
    },
}


def silver_schema(table):
    """The silver table's schema reused as the gold schema; missing column comments come from FALLBACK_COMMENTS."""
    schema = read_source(table).schema
    fallback = FALLBACK_COMMENTS.get(table, {})
    for field in schema.fields:
        if not field.metadata.get("comment") and field.name in fallback:
            field.metadata = {**field.metadata, "comment": fallback[field.name]}
    return schema


# COMMAND ----------

# ==== Mapping admission gate (verbatim from gold journey) ====

# The policy JSON is the gold pipeline's `journey.s3b_policies` configuration, copied at
# deploy time so OMOP gold and gold journey admit exactly the same mappings. An empty
# policy set would withhold every similarity-scored mapping, so it is refused.
GOLD_V2_POLICIES = json.loads(spark.conf.get("journey.s3b_policies", "{}"))
if not GOLD_V2_POLICIES:
    raise ValueError("journey.s3b_policies is empty: copy it from the gold pipeline configuration")

# Copied verbatim from Prod/Pipelines/Gold/gold_journey_shared.py (2026-10-16); the
# test suite compares the syntax trees, so edit both or neither.
GOVERNED_TRANSLATION_RULES = (
    "omop:maps_to:", "trud:icd10:closest", "trud:icdsctmap:single",
    "trud:opcs4:closest", "trud:opcssctmap:single", "trud.nicip.",
    "omop:concept_name:exact", "ukcore:ethnic_category:",
    "form:smoking_status_instrument:", "powerform:S5:validated", "ucum:unit:",
    "score:component:validated", "omop:concept_synonym:exact", "nicip:",
    "bronze.map_medication_order:", "bronze.map_med_admin:LOOKUP_STANDARD_MAP",
    "bronze.map_med_admin:EXACT_UNIQUE_STANDARD_NAME", "bronze.map_med_admin:RXNORM_CODE",
    "bronze.map_med_admin:SNOMED_CODE", "bronze.map_condition:NATIVE",
    "bronze.map_procedure:NATIVE", "omop:snomed_concept_crosswalk", "source:",
    "omop:concept_synonym:exact:lower", "concept_synonym:exact:lower",
    "rxnorm:mmsl_gn:snomedct_us:", "ecds:chief_complaint:description_exact",
    "ecds:chief_complaint:pool_seed_exact", "ecds:chief_complaint:rfv_exact", "ecds:chief_complaint:closest",
    "nicip:preferred_term:exact:", "nicip:synonym:exact:", "nicip:preferred_term:closest",
    "dmd:id:native", "dmd:name:exact",
    "bronze.map_implant_details:LAYER1_UDI_GUDID", "bronze.map_implant_details:LAYER1B_CATALOGUE_GUDID",
    "bronze.map_implant_details:LAYER2_EXACT_ANCHOR", "bronze.map_implant_details:LAYER3_BRAND_RULE",
    "bronze.map_implant_details:LAYER4_CLEANED_ANCHOR", "bronze.map_implant_details:LAYER6_OPCS_STANDARD",
)
GOVERNED_ROLLUP_RULES = ("trud:icdsctmap:rollup", "trud:opcssctmap:rollup")

# GOLD_COMPACT_GATE_PATCH_APPLIED v1
def _s3_axis_gate_and_policy(item):
    """Decide whether a mapped target meets the configured policy and return its provenance.

    Governed rule prefixes and policies are array literals tested by higher-order functions, so
    the expression is one size whatever their number. Unrolled per-prefix OR and per-policy CASE
    chains made Enzyme planning of the QC twins exhaust the driver (2026-09-25/26 gold failures).
    """
    method = item["map_method"]
    rule = F.coalesce(item["map_rule_id"], F.lit(""))
    native = method == "source_native"
    levels = F.coalesce(item["map_rollup_levels"], F.lit(99))
    translation = (
        (method.isin("rule", "exact_synonym")
         & F.exists(F.array(*[F.lit(p) for p in GOVERNED_TRANSLATION_RULES]), lambda p: rule.startswith(p)))
        | ((method == "ancestor_rollup")
           & F.exists(F.array(*[F.lit(p) for p in GOVERNED_ROLLUP_RULES]), lambda p: rule.startswith(p))
           & (levels <= F.lit(4)))
    ) & ~rule.contains("EXACT_string_similarity")
    policy_id = F.when(native, F.lit("source_native")).when(translation, F.lit("governed_translation"))
    banded = F.lit(False)
    if GOLD_V2_POLICIES:
        policies = F.array(*[
            F.struct(
                F.lit(current_id).alias("id"),
                F.lit(policy["method"]).alias("method"),
                F.lit(f":{policy['lane']}:").alias("lane"),
                F.lit(policy["scoring_model"]).alias("scoring_model"),
                F.lit(policy["hierarchy_version"]).alias("hierarchy_version"),
                F.lit(float(policy["cutoff"])).alias("cutoff"),
                F.lit(int(policy.get("rollup_config", {}).get("max_levels", 3))).alias("max_levels"),
            )
            for current_id, policy in sorted(GOLD_V2_POLICIES.items())
        ])
        matched = F.filter(policies, lambda p: (
            (method == p["method"]) &
            rule.contains(p["lane"]) &
            (F.coalesce(item["map_scoring_model"], F.lit("")) == p["scoring_model"]) &
            F.coalesce(item["map_version"], F.lit("")).contains(p["hierarchy_version"]) &
            (F.coalesce(item["map_cosine"], F.lit(0.0)) >= p["cutoff"]) &
            ((method != "ancestor_rollup") | (levels <= p["max_levels"]))
        ))
        # The last matching policy in id order wins, as the unrolled CASE chain did.
        banded = F.size(matched) > 0
        policy_id = F.when(banded, F.element_at(matched, -1)["id"]).otherwise(policy_id)
    rejected = F.coalesce(item["map_status"], F.lit("")).isin("judge_rejected", "vetoed_polarity")
    gate = F.coalesce((native | translation | banded) & ~rejected, F.lit(False))
    policy_id = F.when(rejected, F.concat(F.lit("withheld:"), item["map_status"])).otherwise(policy_id)
    return gate, F.coalesce(policy_id, F.lit("withheld:no_matching_policy"))


# The S3 axis travels as JSON in silver provenance (`map_axis_json`, added by
# OMOP_PROVENANCE_AXIS_V1). Fields absent from an axis parse as NULL, exactly as an empty
# axis column does in gold journey.
AXIS_SCHEMA = "struct<method:string,cosine:double,scoring_model:string,status:string,rule:string,rollup_levels:int>"
# Only routed facts carry an S3 axis; these are the tables the gate can withhold from.
GATED_TABLES = [
    "condition_occurrence", "procedure_occurrence", "drug_exposure",
    "device_exposure", "measurement", "observation",
]


def _with_gate(provenance):
    """Add `_og_gate_pass` and `policy_id` to provenance rows; rows without an axis get NULL for both.

    The axis is parsed once into a column: an inline from_json would be copied into every
    field reference of the policy expression (the gold-journey driver OOM of 2026-09-25/26).
    """
    if "map_axis_json" not in provenance.columns:
        # Silver patch not applied yet: nothing can be gated, every row counts as ungated.
        return provenance.withColumn("_og_gate_pass", F.lit(None).cast("boolean")).withColumn(
            "policy_id", F.lit(None).cast("string"))
    parsed = provenance.withColumn("_og_axis", F.from_json(F.col("map_axis_json"), AXIS_SCHEMA))
    item = {
        "map_method": F.col("_og_axis.method"),
        "map_rule_id": F.col("_og_axis.rule"),
        "map_rollup_levels": F.col("_og_axis.rollup_levels"),
        "map_scoring_model": F.col("_og_axis.scoring_model"),
        "map_version": F.col("map_version"),
        "map_cosine": F.col("_og_axis.cosine"),
        "map_status": F.col("_og_axis.status"),
    }
    gate, policy_id = _s3_axis_gate_and_policy(item)
    has_axis = F.col("map_axis_json").isNotNull()
    return (
        parsed
        .withColumn("_og_gate_pass", F.when(has_axis, gate))
        .withColumn("policy_id", F.when(has_axis, policy_id))
        .drop("_og_axis")
    )


@dp.materialized_view(
    name="_og_gate_fail",
    comment="Internal: one row per routed fact whose standard concept fails the gold mapping-admission policy, with the withholding policy id.",
)
def _og_gate_fail():
    """Facts to withhold under the S3D policy. A fact fails when any of its gated concept fields fails."""
    provenance = read_source("omop_concept_provenance").where(F.col("cdm_table").isin(*GATED_TABLES))
    return (
        _with_gate(provenance)
        .where(F.col("_og_gate_pass") == F.lit(False))
        .groupBy("cdm_table", "cdm_row_id")
        .agg(F.min("policy_id").alias("policy_id"))
    )


# COMMAND ----------

# ==== Shared views: release date and the person gate ====

# Materialized, not temporary: a temporary-view input stops Enzyme from proving a NO_OP
# (every audit planned ROW_BASED on a quiet dev update, 2026-10-16).
@dp.materialized_view(name="_og_release", comment="Internal: the silver release date, the pinned future-date boundary.")
def _og_release():
    """One row holding `cdm_source.source_release_date`; replaces the legacy run-date checks."""
    return read_source("cdm_source").agg(F.max("source_release_date").alias("_og_release_date"))


# INCR_FIX_20261018: the release-date gate as an equi-join on the dates after the release, not a cross
# join to _og_release (a cross join stops every audit from refreshing incrementally). A new release date
# then re-touches only rows dated between the old and the new release date.
CALENDAR_LAST_DATE = "2300-12-31"  # last row of 3_lookup.omop.calendar_day; later dates are always "after"


@dp.materialized_view(name="_og_future_day", comment="Internal: every calendar date after the silver release date.")
def _og_future_day():
    """One row per calendar date later than `_og_release_date` (empty when silver has no release date)."""
    release = spark.read.table("_og_release").withColumn("_og_release_key", F.lit(True))
    return (
        read_lookup("calendar_day")
        .withColumn("_og_release_key", F.lit(True))
        .join(release, "_og_release_key")
        .where(F.col("calendar_date") > F.col("_og_release_date"))
        .select(F.col("calendar_date").alias("_og_future_date"))
    )


def _after_release(frame, date_column, flag):
    """Add boolean `flag`: the DATE column is after the release date (NULL when the date is NULL)."""
    future = spark.read.table("_og_future_day").withColumn("_og_future_hit", F.lit(True))
    after = F.col("_og_future_hit").isNotNull() | (F.col(date_column) > F.lit(CALENDAR_LAST_DATE).cast("date"))
    return (
        frame.join(future, F.col(date_column) == F.col("_og_future_date"), "left")
        .withColumn(flag, F.when(F.col(date_column).isNotNull(), after))
        .drop("_og_future_date", "_og_future_hit")
    )


@dp.materialized_view(
    name="_og_person_gate",
    comment="Internal: every silver person with the gold person-gate reason, birth, valid death date and recorded sex.",
)
def _og_person_gate():
    """Person-level inputs for every audit.

    `_og_person_reason` is the first failing person rule (research flag, else birth after
    the release date). `_og_death_date` is NULL where death precedes birth, mirroring the
    gold death table. `_og_sex` is Male/Female only, for the sex-plausibility finding.
    """
    flags = read_source("omop_person_research_flags").select("person_id", "research_exclusion_reason")
    deaths = read_source("death").groupBy("person_id").agg(F.min("death_date").alias("_og_raw_death_date"))
    persons = (
        read_source("person")
        .select("person_id", "birth_datetime", "gender_concept_id")
        .join(flags, "person_id", "left")
        .join(deaths, "person_id", "left")
        .withColumn("_og_birth_date", F.to_date("birth_datetime"))
    )
    persons = _after_release(persons, "_og_birth_date", "_og_birth_after_release")  # INCR_FIX_20261018
    birth_date = F.col("_og_birth_date")
    return persons.select(
        "person_id",
        F.lit(True).alias("_og_person_known"),
        F.when(F.col("research_exclusion_reason").isNotNull(),
               F.concat(F.lit("research_excluded:"), F.col("research_exclusion_reason")))
        .when(F.col("_og_birth_after_release"), F.lit("birth_after_release_date"))
        .alias("_og_person_reason"),
        F.col("birth_datetime").alias("_og_birth_datetime"),
        F.when(birth_date.isNull() | (F.col("_og_raw_death_date") >= birth_date), F.col("_og_raw_death_date"))
        .alias("_og_death_date"),
        F.when(F.col("gender_concept_id") == 8507, F.lit("Male"))
        .when(F.col("gender_concept_id") == 8532, F.lit("Female"))
        .alias("_og_sex"),
    )


# COMMAND ----------

# ==== Rules ====

# Mandatory rules drop a row (`dp.expect_all_or_drop`) and are structural: keys, required
# concepts and dates. Silver already guarantees them, so their drop count is expected to be
# zero; a non-zero count in the event log is a silver regression. Advisory rules
# (`dp.expect_all`) only report. Nothing compares against the run date: future dates are an audit
# rule against the pinned release date. '2012-01-01' is the silver study window, restated
# as an advisory regression guard.
TABLE_RULES = {
    "person": {
        "mandatory": {
            "valid_person_id": "person_id IS NOT NULL",
            "valid_gender_concept": "gender_concept_id IS NOT NULL",
            "valid_year_of_birth": "year_of_birth IS NOT NULL",
            "valid_race_concept": "race_concept_id IS NOT NULL",
            "valid_ethnicity_concept": "ethnicity_concept_id IS NOT NULL",
        },
        "advisory": {
            "binary_gender": "gender_concept_id IN (8507, 8532)",
            "birth_year_min": "year_of_birth >= 1910",
            "valid_birth_month": "month_of_birth IS NULL OR month_of_birth BETWEEN 1 AND 12",
            "valid_birth_day": "day_of_birth IS NULL OR day_of_birth BETWEEN 1 AND 31",
        },
    },
    "death": {
        "mandatory": {
            "valid_person": "person_id IS NOT NULL",
            "valid_death_date": "death_date IS NOT NULL",
            "valid_type_concept": "death_type_concept_id IS NOT NULL",
        },
        "advisory": {
            "valid_cause_concept": "cause_concept_id IS NULL OR cause_concept_id >= 0",
            "death_date_matches_datetime": "death_datetime IS NULL OR CAST(death_datetime AS DATE) = death_date",
        },
    },
    "observation_period": {
        "mandatory": {
            "valid_observation_period_id": "observation_period_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_start_date": "observation_period_start_date IS NOT NULL",
            "valid_end_date": "observation_period_end_date IS NOT NULL",
            "valid_type_concept": "period_type_concept_id IS NOT NULL",
        },
        "advisory": {
            "valid_dates": "observation_period_end_date >= observation_period_start_date",
            "within_study_window": "observation_period_start_date >= '2012-01-01'",
        },
    },
    "visit_occurrence": {
        "mandatory": {
            "valid_visit_id": "visit_occurrence_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_visit_concept": "visit_concept_id IS NOT NULL",
            "valid_start_date": "visit_start_date IS NOT NULL",
            "valid_end_date": "visit_end_date IS NOT NULL",
            "valid_type_concept": "visit_type_concept_id IS NOT NULL",
        },
        "advisory": {
            "valid_dates": "visit_start_date <= visit_end_date",
            "within_study_window": "visit_start_date >= '2012-01-01'",
            "valid_visit_concept_value": "visit_concept_id > 0",
        },
    },
    "visit_detail": {
        "mandatory": {
            "valid_visit_detail_id": "visit_detail_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_visit_detail_concept": "visit_detail_concept_id IS NOT NULL",
            "valid_start_date": "visit_detail_start_date IS NOT NULL",
            "valid_end_date": "visit_detail_end_date IS NOT NULL",
            "valid_type_concept": "visit_detail_type_concept_id IS NOT NULL",
            "valid_visit_occurrence": "visit_occurrence_id IS NOT NULL",
        },
        "advisory": {
            "valid_dates": "visit_detail_start_date <= visit_detail_end_date",
            "within_study_window": "visit_detail_start_date >= '2012-01-01'",
            "valid_visit_detail_concept_value": "visit_detail_concept_id > 0",
        },
    },
    "condition_occurrence": {
        "mandatory": {
            "valid_condition_id": "condition_occurrence_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_concept": "condition_concept_id IS NOT NULL",
            "valid_start_date": "condition_start_date IS NOT NULL",
            "valid_type_concept": "condition_type_concept_id IS NOT NULL",
        },
        "advisory": {
            "valid_concept_value": "condition_concept_id > 0",
            "within_study_window": "condition_start_date >= '2012-01-01'",
            "valid_status_concept": "condition_status_concept_id IS NULL OR condition_status_concept_id >= 0",
        },
    },
    "procedure_occurrence": {
        "mandatory": {
            "valid_procedure_id": "procedure_occurrence_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_concept": "procedure_concept_id IS NOT NULL",
            "valid_date": "procedure_date IS NOT NULL",
            "valid_type_concept": "procedure_type_concept_id IS NOT NULL",
        },
        "advisory": {
            "valid_concept_value": "procedure_concept_id > 0",
            "within_study_window": "procedure_date >= '2012-01-01'",
            "valid_quantity": "quantity IS NULL OR quantity > 0",
        },
    },
    "drug_exposure": {
        "mandatory": {
            "valid_drug_id": "drug_exposure_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_concept": "drug_concept_id IS NOT NULL",
            "valid_start_date": "drug_exposure_start_date IS NOT NULL",
            "valid_end_date": "drug_exposure_end_date IS NOT NULL",
            "valid_type_concept": "drug_type_concept_id IS NOT NULL",
        },
        "advisory": {
            "valid_concept_value": "drug_concept_id > 0",
            "valid_dates": "drug_exposure_end_date >= drug_exposure_start_date",
            "within_study_window": "drug_exposure_start_date >= '2012-01-01'",
            "valid_quantity": "quantity IS NULL OR quantity > 0",
            "valid_days_supply": "days_supply IS NULL OR days_supply > 0",
        },
    },
    "device_exposure": {
        "mandatory": {
            "valid_device_id": "device_exposure_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_concept": "device_concept_id IS NOT NULL",
            "valid_start_date": "device_exposure_start_date IS NOT NULL",
            "valid_type_concept": "device_type_concept_id IS NOT NULL",
        },
        "advisory": {
            "valid_concept_value": "device_concept_id > 0",
            "within_study_window": "device_exposure_start_date >= '2012-01-01'",
            "valid_quantity": "quantity IS NULL OR quantity > 0",
        },
    },
    "measurement": {
        "mandatory": {
            "valid_measurement_id": "measurement_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_concept": "measurement_concept_id IS NOT NULL",
            "valid_date": "measurement_date IS NOT NULL",
            "valid_type_concept": "measurement_type_concept_id IS NOT NULL",
        },
        "advisory": {
            "valid_concept_value": "measurement_concept_id > 0",
            "within_study_window": "measurement_date >= '2012-01-01'",
            "single_value_kind": "value_as_number IS NULL OR value_as_concept_id IS NULL",
            "valid_range": "range_high IS NULL OR range_low IS NULL OR range_high >= range_low",
        },
    },
    "observation": {
        "mandatory": {
            "valid_observation_id": "observation_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_concept": "observation_concept_id IS NOT NULL",
            "valid_date": "observation_date IS NOT NULL",
            "valid_type_concept": "observation_type_concept_id IS NOT NULL",
        },
        "advisory": {
            "within_study_window": "observation_date >= '2012-01-01'",
            "valid_qualifier": "qualifier_concept_id IS NULL OR qualifier_concept_id >= 0",
        },
    },
    "specimen": {
        "mandatory": {
            "valid_specimen_id": "specimen_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_concept": "specimen_concept_id IS NOT NULL",
            "valid_date": "specimen_date IS NOT NULL",
            "valid_type_concept": "specimen_type_concept_id IS NOT NULL",
        },
        "advisory": {
            "within_study_window": "specimen_date >= '2012-01-01'",
            "valid_quantity": "quantity IS NULL OR quantity > 0",
        },
    },
    "drug_era": {
        "mandatory": {
            "valid_drug_era_id": "drug_era_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_drug": "drug_concept_id IS NOT NULL",
            "valid_start_date": "drug_era_start_date IS NOT NULL",
            "valid_end_date": "drug_era_end_date IS NOT NULL",
        },
        "advisory": {
            "valid_dates": "drug_era_end_date >= drug_era_start_date",
            "valid_exposure_count": "drug_exposure_count IS NULL OR drug_exposure_count > 0",
            "valid_gap_days": "gap_days IS NULL OR gap_days >= 0",
        },
    },
    "dose_era": {
        "mandatory": {
            "valid_dose_era_id": "dose_era_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_drug_concept": "drug_concept_id IS NOT NULL",
            "valid_unit_concept": "unit_concept_id IS NOT NULL",
            "valid_dose_value": "dose_value IS NOT NULL",
            "valid_start_date": "dose_era_start_date IS NOT NULL",
            "valid_end_date": "dose_era_end_date IS NOT NULL",
        },
        "advisory": {
            "valid_dates": "dose_era_end_date >= dose_era_start_date",
            "positive_dose": "dose_value > 0",
        },
    },
    "condition_era": {
        "mandatory": {
            "valid_condition_era_id": "condition_era_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_condition": "condition_concept_id IS NOT NULL",
            "valid_start_date": "condition_era_start_date IS NOT NULL",
            "valid_end_date": "condition_era_end_date IS NOT NULL",
        },
        "advisory": {
            "valid_dates": "condition_era_end_date >= condition_era_start_date",
            "valid_count": "condition_occurrence_count IS NULL OR condition_occurrence_count > 0",
        },
    },
    "episode": {
        "mandatory": {
            "valid_episode_id": "episode_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_concept": "episode_concept_id IS NOT NULL",
            "valid_start_date": "episode_start_date IS NOT NULL",
            "valid_type_concept": "episode_type_concept_id IS NOT NULL",
        },
        "advisory": {
            "valid_dates": "episode_end_date IS NULL OR episode_end_date >= episode_start_date",
        },
    },
    "genomic_test": {
        "mandatory": {
            "valid_genomic_test_id": "genomic_test_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_test_date": "test_date IS NOT NULL",
        },
        "advisory": {
            "within_study_window": "test_date >= '2012-01-01'",
        },
    },
    "genomic_variant": {
        "mandatory": {
            "valid_genomic_variant_id": "genomic_variant_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
            "valid_genomic_test": "genomic_test_id IS NOT NULL",
        },
        "advisory": {
            "valid_vaf": "vaf IS NULL OR vaf BETWEEN 0 AND 1",
            "valid_positions": "position_start IS NULL OR position_end IS NULL OR position_end >= position_start",
        },
    },
    "pregnancy": {
        "mandatory": {
            "valid_pregnancy_id": "pregnancy_id IS NOT NULL",
            "valid_person": "person_id IS NOT NULL",
        },
        "advisory": {
            "valid_dates": "pregnancy_start_date IS NULL OR pregnancy_end_date IS NULL OR pregnancy_end_date >= pregnancy_start_date",
            "valid_gestational_length": "gestational_length_in_day IS NULL OR gestational_length_in_day BETWEEN 0 AND 320",
            "liveborn_not_exceeds_fetuses": "pregnancy_number_liveborn IS NULL OR pregnancy_number_fetuses IS NULL OR pregnancy_number_liveborn <= pregnancy_number_fetuses",
        },
    },
    "genomic_gene_tested": {
        "mandatory": {
            "valid_gene_tested_id": "gene_tested_id IS NOT NULL",
            "valid_genomic_test": "genomic_test_id IS NOT NULL",
        },
        "advisory": {
            "valid_confidence": "confidence IS NULL OR confidence BETWEEN 0 AND 1",
        },
    },
    "episode_event": {
        "mandatory": {
            "valid_episode": "episode_id IS NOT NULL",
            "valid_event": "event_id IS NOT NULL",
            "valid_field_concept": "episode_event_field_concept_id IS NOT NULL",
        },
        "advisory": {},
    },
    "fact_relationship": {
        "mandatory": {
            "valid_fact_1": "domain_concept_id_1 IS NOT NULL AND fact_id_1 IS NOT NULL",
            "valid_fact_2": "domain_concept_id_2 IS NOT NULL AND fact_id_2 IS NOT NULL",
            "valid_relationship": "relationship_concept_id IS NOT NULL",
        },
        "advisory": {},
    },
    "location": {
        "mandatory": {"valid_location_id": "location_id IS NOT NULL"},
        "advisory": {
            "valid_latitude": "latitude IS NULL OR latitude BETWEEN -90 AND 90",
            "valid_longitude": "longitude IS NULL OR longitude BETWEEN -180 AND 180",
        },
    },
    "care_site": {
        "mandatory": {"valid_care_site_id": "care_site_id IS NOT NULL"},
        "advisory": {"valid_location_id": "location_id IS NULL OR location_id >= 0"},
    },
    "provider": {
        "mandatory": {"valid_provider_id": "provider_id IS NOT NULL"},
        "advisory": {"valid_specialty_concept": "specialty_concept_id IS NULL OR specialty_concept_id >= 0"},
    },
    "cdm_source": {
        "mandatory": {
            "valid_source_name": "cdm_source_name IS NOT NULL",
            "valid_release_dates": "source_release_date IS NOT NULL AND cdm_release_date IS NOT NULL",
            "valid_cdm_version_concept": "cdm_version_concept_id > 0",
        },
        "advisory": {"valid_vocabulary_version": "vocabulary_version IS NOT NULL"},
    },
    "metadata": {
        "mandatory": {"valid_metadata_id": "metadata_id IS NOT NULL", "valid_name": "name IS NOT NULL"},
        "advisory": {},
    },
}


def _expectations(table):
    """Wrap a dataset function in its table's mandatory and advisory expectations."""
    rules = TABLE_RULES[table]

    def decorate(function):
        """Apply advisory first, so the mandatory drop rules are evaluated outermost."""
        if rules["advisory"]:
            function = dp.expect_all(rules["advisory"])(function)
        return dp.expect_all_or_drop(rules["mandatory"])(function)

    return decorate


# COMMAND ----------

# ==== Person-keyed tables: one audit and one public table each ====

# One spec per person-keyed table:
#   id          primary key column
#   start_date  the date the release-date rule and the findings compare
#   start_datetime  finer start used for "before birth" when present
#   end_pairs   nullable (end_date, end_datetime) pairs nulled when the end precedes the start
#   concept     concept checked against the sex-plausibility list (DQ4 G2)
#   gated       the mapping gate applies (routed facts only)
#   parents     column -> table: drop the row when that parent row was dropped (NOT NULL pointers)
#   fk_null     column -> table: null the pointer when that parent row was dropped (nullable pointers)
#   optional    build only when silver publishes the table
#   findings    record before-birth / after-death findings
FACT_TABLES = {
    "observation_period": {
        "id": "observation_period_id", "start_date": "observation_period_start_date", "start_datetime": None,
        "end_pairs": [], "concept": None, "gated": False, "parents": {}, "fk_null": {},
        "optional": False, "findings": False,
    },
    "visit_occurrence": {
        "id": "visit_occurrence_id", "start_date": "visit_start_date", "start_datetime": "visit_start_datetime",
        "end_pairs": [], "concept": None, "gated": False, "parents": {},
        "fk_null": {"preceding_visit_occurrence_id": "visit_occurrence"},
        "optional": False, "findings": True,
    },
    "visit_detail": {
        "id": "visit_detail_id", "start_date": "visit_detail_start_date", "start_datetime": "visit_detail_start_datetime",
        "end_pairs": [], "concept": None, "gated": False,
        "parents": {"visit_occurrence_id": "visit_occurrence"},
        "fk_null": {"preceding_visit_detail_id": "visit_detail", "parent_visit_detail_id": "visit_detail"},
        "optional": False, "findings": True,
    },
    "condition_occurrence": {
        "id": "condition_occurrence_id", "start_date": "condition_start_date", "start_datetime": "condition_start_datetime",
        "end_pairs": [["condition_end_date", "condition_end_datetime"]], "concept": "condition_concept_id",
        "gated": True, "parents": {},
        "fk_null": {"visit_occurrence_id": "visit_occurrence", "visit_detail_id": "visit_detail"},
        "optional": False, "findings": True,
    },
    "procedure_occurrence": {
        "id": "procedure_occurrence_id", "start_date": "procedure_date", "start_datetime": "procedure_datetime",
        "end_pairs": [["procedure_end_date", "procedure_end_datetime"]], "concept": "procedure_concept_id",
        "gated": True, "parents": {},
        "fk_null": {"visit_occurrence_id": "visit_occurrence", "visit_detail_id": "visit_detail"},
        "optional": False, "findings": True,
    },
    "drug_exposure": {
        "id": "drug_exposure_id", "start_date": "drug_exposure_start_date", "start_datetime": "drug_exposure_start_datetime",
        "end_pairs": [], "concept": None, "gated": True, "parents": {},
        "fk_null": {"visit_occurrence_id": "visit_occurrence", "visit_detail_id": "visit_detail"},
        "optional": False, "findings": True,
    },
    "device_exposure": {
        "id": "device_exposure_id", "start_date": "device_exposure_start_date", "start_datetime": "device_exposure_start_datetime",
        "end_pairs": [["device_exposure_end_date", "device_exposure_end_datetime"]], "concept": None,
        "gated": True, "parents": {},
        "fk_null": {"visit_occurrence_id": "visit_occurrence", "visit_detail_id": "visit_detail"},
        "optional": False, "findings": True,
    },
    "measurement": {
        "id": "measurement_id", "start_date": "measurement_date", "start_datetime": "measurement_datetime",
        "end_pairs": [], "concept": None, "gated": True, "parents": {},
        "fk_null": {"visit_occurrence_id": "visit_occurrence", "visit_detail_id": "visit_detail"},
        "optional": False, "findings": True,
    },
    "observation": {
        "id": "observation_id", "start_date": "observation_date", "start_datetime": "observation_datetime",
        "end_pairs": [], "concept": None, "gated": True, "parents": {},
        "fk_null": {"visit_occurrence_id": "visit_occurrence", "visit_detail_id": "visit_detail"},
        "optional": False, "findings": True,
    },
    "specimen": {
        "id": "specimen_id", "start_date": "specimen_date", "start_datetime": "specimen_datetime",
        "end_pairs": [], "concept": None, "gated": False, "parents": {}, "fk_null": {},
        "optional": False, "findings": True,
    },
    "drug_era": {
        "id": "drug_era_id", "start_date": "drug_era_start_date", "start_datetime": None,
        "end_pairs": [], "concept": None, "gated": False, "parents": {}, "fk_null": {},
        "optional": False, "findings": False,
    },
    "dose_era": {
        "id": "dose_era_id", "start_date": "dose_era_start_date", "start_datetime": None,
        "end_pairs": [], "concept": None, "gated": False, "parents": {}, "fk_null": {},
        "optional": False, "findings": False,
    },
    "condition_era": {
        "id": "condition_era_id", "start_date": "condition_era_start_date", "start_datetime": None,
        "end_pairs": [], "concept": None, "gated": False, "parents": {}, "fk_null": {},
        "optional": False, "findings": False,
    },
    "episode": {
        "id": "episode_id", "start_date": "episode_start_date", "start_datetime": "episode_start_datetime",
        "end_pairs": [["episode_end_date", "episode_end_datetime"]], "concept": None, "gated": False,
        "parents": {}, "fk_null": {"episode_parent_id": "episode"},
        "optional": False, "findings": True,
    },
    "genomic_test": {
        "id": "genomic_test_id", "start_date": "test_date", "start_datetime": "test_datetime",
        "end_pairs": [], "concept": None, "gated": False, "parents": {},
        "fk_null": {"specimen_id": "specimen"},
        "optional": False, "findings": True,
    },
    "genomic_variant": {
        "id": "genomic_variant_id", "start_date": "variant_date", "start_datetime": "variant_datetime",
        "end_pairs": [], "concept": None, "gated": False,
        "parents": {"genomic_test_id": "genomic_test"}, "fk_null": {"specimen_id": "specimen"},
        "optional": False, "findings": True,
    },
    "pregnancy": {
        "id": "pregnancy_id", "start_date": "pregnancy_start_date", "start_datetime": None,
        "end_pairs": [], "concept": None, "gated": False, "parents": {}, "fk_null": {},
        "optional": True, "findings": False,
    },
}

AUDIT_COMMENT = (
    "Internal: every {table} row gold drops (drop_reason, one per row, first failing rule) "
    "or flags (findings, row kept). The public table is silver minus the dropped rows."
)


def _dropped_ids(table):
    """Ids of rows dropped from a published gold table, read from its audit."""
    return (
        spark.read.table(f"_og_audit_{table}")
        .where(F.col("drop_reason").isNotNull())
        .select("cdm_row_id")
    )


def _findings_array(rows, spec):
    """Advisory findings of a fact row: before birth, after death (+30 days), sex-implausible concept."""
    flags = []
    if spec["findings"]:
        start = F.coalesce(F.col("_og_start_datetime"), F.col("_og_start_date").cast("timestamp"))
        flags.append(F.when(start < F.col("_og_birth_datetime"), F.lit("event_before_birth")))
        flags.append(F.when(F.col("_og_start_date") > F.date_add(F.col("_og_death_date"), 30),
                            F.lit("event_after_death_30d")))
    if spec["concept"]:
        flags.append(F.when(F.col("_og_sex_rule").isNotNull() & F.col("_og_sex").isNotNull()
                            & (F.col("_og_sex_rule") != F.col("_og_sex")),
                            F.lit("dq4_g2_sex_plausibility_v1")))
    if not flags:
        return F.array().cast("array<string>")
    return F.filter(F.array(*flags), lambda flag: flag.isNotNull())


def _audit_frame(table, spec):
    """Rows of one person-keyed table that gold drops or flags.

    The drop reason is the FIRST failing rule in this order: person not in silver, person
    gate, mapping gate, start after the release date, dropped parent. Kept rows appear
    only when they carry an advisory finding.
    """
    silver = read_source(table)
    columns = [
        F.col(spec["id"]).alias("cdm_row_id"),
        "person_id",
        F.col(spec["start_date"]).alias("_og_start_date"),
        (F.col(spec["start_datetime"]) if spec["start_datetime"] else F.lit(None).cast("timestamp"))
        .alias("_og_start_datetime"),
    ]
    if spec["concept"]:
        columns.append(F.col(spec["concept"]).alias("_og_concept"))
    columns += [F.col(column).alias(f"_og_parent_{column}") for column in spec["parents"]]
    rows = _after_release(  # INCR_FIX_20261018
        silver.select(*columns).join(spark.read.table("_og_person_gate"), "person_id", "left"),
        "_og_start_date", "_og_start_after_release",
    )

    policy = F.lit(None).cast("string")
    if spec["gated"]:
        withheld = (
            spark.read.table("_og_gate_fail")
            .where(F.col("cdm_table") == table)
            .select(F.col("cdm_row_id"), F.col("policy_id").alias("_og_policy_id"))
        )
        rows = rows.join(withheld, "cdm_row_id", "left")
        policy = F.col("_og_policy_id")

    parent_dropped = F.lit(False)
    for column, parent in spec["parents"].items():
        marker = f"_og_parent_dropped_{column}"
        dropped = _dropped_ids(parent).select(F.col("cdm_row_id").alias(f"_og_parent_{column}"),
                                              F.lit(True).alias(marker))
        rows = rows.join(dropped, f"_og_parent_{column}", "left")
        parent_dropped = parent_dropped | F.coalesce(F.col(marker), F.lit(False))

    if spec["concept"]:
        sex_rules = read_lookup("concept_sex_plausibility").select(
            F.col("concept_id").alias("_og_concept"), F.col("plausible_gender").alias("_og_sex_rule")
        ).dropDuplicates(["_og_concept"])
        rows = rows.join(F.broadcast(sex_rules), "_og_concept", "left")

    reason = (
        F.when(F.col("_og_person_known").isNull(), F.lit("person_not_published"))
        .when(F.col("_og_person_reason").isNotNull(), F.col("_og_person_reason"))
        .when(policy.isNotNull(), F.lit("mapping_gate_withheld"))
        .when(F.col("_og_start_after_release"), F.lit("after_release_date"))
        .when(parent_dropped, F.lit("parent_dropped"))
    )
    audited = rows.select(
        F.lit(table).alias("cdm_table"),
        "cdm_row_id",
        "person_id",
        reason.alias("drop_reason"),
        F.when(reason == "mapping_gate_withheld", policy).alias("policy_id"),
        _findings_array(rows, spec).alias("findings"),
    )
    return audited.where(F.col("drop_reason").isNotNull() | (F.size("findings") > 0))


def _null_dropped_pointers(frame, pointers):
    """Null nullable pointer columns whose target row was dropped (gold-journey FK nulling)."""
    for column, parent in pointers.items():
        marker = f"_og_dropped_{column}"
        dropped = _dropped_ids(parent).select(F.col("cdm_row_id").alias(column), F.lit(True).alias(marker))
        frame = (
            frame.join(dropped, column, "left")
            .withColumn(column, F.when(F.col(marker).isNull(), F.col(column)))
            .drop(marker)
        )
    return frame


def _null_ends_before_start(frame, spec):
    """Null each nullable end date (and its datetime) that precedes the start date."""
    for end_date, end_datetime in spec["end_pairs"]:
        before = F.col(end_date) < F.col(spec["start_date"])
        frame = (
            frame.withColumn("_og_end_before_start", F.coalesce(before, F.lit(False)))
            .withColumn(end_date, F.when(~F.col("_og_end_before_start"), F.col(end_date)))
            .withColumn(end_datetime, F.when(~F.col("_og_end_before_start"), F.col(end_datetime)))
            .drop("_og_end_before_start")
        )
    return frame


def _public_frame(table, spec):
    """The published gold table: silver minus audit drops, with pointer and end-date repairs."""
    silver = read_source(table)
    dropped = _dropped_ids(table).withColumnRenamed("cdm_row_id", spec["id"])
    kept = silver.join(dropped, spec["id"], "left_anti")
    kept = _null_dropped_pointers(kept, spec["fk_null"])
    kept = _null_ends_before_start(kept, spec)
    return kept.select(*silver.columns)


def _register_fact_table(table, spec):
    """Declare `_og_audit_<table>` and the public `<table>` materialized views."""

    @dp.materialized_view(name=f"_og_audit_{table}", comment=AUDIT_COMMENT.format(table=table))
    def audit():
        """Dropped and flagged rows of this table."""
        return _audit_frame(table, spec)

    @dp.materialized_view(
        name=table,
        comment=f"OMOP CDM {table}, quality-controlled from omop_silver (omop_gold {PIPELINE_VERSION}).",
        schema=silver_schema(table),
        table_properties={"quality": "gold"},
    )
    @_expectations(table)
    def public():
        """Silver rows that pass every gold rule."""
        return _public_frame(table, spec)


for _table, _spec in FACT_TABLES.items():
    if not _spec["optional"] or source_exists(_table):
        _register_fact_table(_table, _spec)


# COMMAND ----------

# ==== Person and death ====

@dp.materialized_view(name="_og_audit_person", comment=AUDIT_COMMENT.format(table="person"))
def _og_audit_person():
    """Persons dropped by the person gate (research flag or birth after the release date)."""
    return (
        spark.read.table("_og_person_gate")
        .where(F.col("_og_person_reason").isNotNull())
        .select(
            F.lit("person").alias("cdm_table"),
            F.col("person_id").alias("cdm_row_id"),
            "person_id",
            F.col("_og_person_reason").alias("drop_reason"),
            F.lit(None).cast("string").alias("policy_id"),
            F.array().cast("array<string>").alias("findings"),
        )
    )


@dp.materialized_view(
    name="person",
    comment=f"OMOP CDM person: the research cohort of omop_silver (omop_gold {PIPELINE_VERSION}).",
    schema=silver_schema("person"),
    table_properties={"quality": "gold"},
)
@_expectations("person")
def person():
    """Silver persons minus those the person gate drops."""
    silver = read_source("person")
    return silver.join(_dropped_ids("person").withColumnRenamed("cdm_row_id", "person_id"), "person_id", "left_anti")


@dp.materialized_view(name="_og_audit_death", comment=AUDIT_COMMENT.format(table="death"))
def _og_audit_death():
    """Deaths dropped: person gate, death before birth (the birth is better attested), after the release date."""
    deaths = (
        read_source("death")
        .select("person_id", "death_date")
        .join(spark.read.table("_og_person_gate"), "person_id", "left")
    )
    deaths = _after_release(deaths, "death_date", "_og_death_after_release")  # INCR_FIX_20261018
    reason = (
        F.when(F.col("_og_person_known").isNull(), F.lit("person_not_published"))
        .when(F.col("_og_person_reason").isNotNull(), F.col("_og_person_reason"))
        .when(F.col("death_date") < F.to_date("_og_birth_datetime"), F.lit("death_before_birth"))
        .when(F.col("_og_death_after_release"), F.lit("after_release_date"))
    )
    return deaths.select(
        F.lit("death").alias("cdm_table"),
        F.col("person_id").alias("cdm_row_id"),
        "person_id",
        reason.alias("drop_reason"),
        F.lit(None).cast("string").alias("policy_id"),
        F.array().cast("array<string>").alias("findings"),
    ).where(F.col("drop_reason").isNotNull())


@dp.materialized_view(
    name="death",
    comment=f"OMOP CDM death, quality-controlled from omop_silver (omop_gold {PIPELINE_VERSION}).",
    schema=silver_schema("death"),
    table_properties={"quality": "gold"},
)
@_expectations("death")
def death():
    """Silver deaths minus audit drops."""
    silver = read_source("death")
    return silver.join(_dropped_ids("death").withColumnRenamed("cdm_row_id", "person_id"), "person_id", "left_anti")


# COMMAND ----------

# ==== Child tables without a person: parent integrity only ====

def _register_child_table(table, key, parent_column, parent_table, extra_parents=None):
    """Declare audit and public views for a table whose rows live and die with a parent row.

    `key` is the column used as the audit row id. `extra_parents` maps further pointer
    columns to parent tables; a row is dropped when any of its parents was dropped.
    """
    parents = {parent_column: parent_table, **(extra_parents or {})}

    @dp.materialized_view(name=f"_og_audit_{table}", comment=AUDIT_COMMENT.format(table=table))
    def audit():
        """Rows whose parent was dropped."""
        rows = read_source(table).select(F.col(key).alias("cdm_row_id"), *parents)
        dropped_any = F.lit(False)
        for column, parent in parents.items():
            marker = f"_og_dropped_{column}"
            rows = rows.join(_dropped_ids(parent).select(F.col("cdm_row_id").alias(column), F.lit(True).alias(marker)),
                             column, "left")
            dropped_any = dropped_any | F.coalesce(F.col(marker), F.lit(False))
        return rows.where(dropped_any).select(
            F.lit(table).alias("cdm_table"),
            "cdm_row_id",
            F.lit(None).cast("bigint").alias("person_id"),
            F.lit("parent_dropped").alias("drop_reason"),
            F.lit(None).cast("string").alias("policy_id"),
            F.array().cast("array<string>").alias("findings"),
        )

    @dp.materialized_view(
        name=table,
        comment=f"OMOP CDM {table}, quality-controlled from omop_silver (omop_gold {PIPELINE_VERSION}).",
        schema=silver_schema(table),
        table_properties={"quality": "gold"},
    )
    @_expectations(table)
    def public():
        """Silver rows whose parents all survived (anti-joined per parent, so a non-unique key is safe)."""
        kept = read_source(table)
        for column, parent in parents.items():
            kept = kept.join(_dropped_ids(parent).withColumnRenamed("cdm_row_id", column), column, "left_anti")
        return kept


_register_child_table("genomic_gene_tested", "gene_tested_id", "genomic_test_id", "genomic_test")
# episode_event endpoints other than the episode are not checked in v1 (the table is empty
# at 2026-10-16; the field-concept to table map is not yet needed).
_register_child_table("episode_event", "event_id", "episode_id", "episode")

# The infant table keys on the baby (infant_id = the baby's person_id) and its pregnancy.
if source_exists("infant") and source_exists("pregnancy"):
    TABLE_RULES["infant"] = {
        "mandatory": {"valid_infant_id": "infant_id IS NOT NULL", "valid_pregnancy": "pregnancy_id IS NOT NULL"},
        "advisory": {
            "valid_birth_weight": "birth_weight IS NULL OR birth_weight BETWEEN 200 AND 10000",
            "valid_apgar_range": "birth_apgar IS NULL OR birth_apgar BETWEEN 0 AND 10",
        },
    }
    _register_child_table("infant", "infant_id", "pregnancy_id", "pregnancy", {"infant_id": "person"})


# COMMAND ----------

# ==== fact_relationship: both endpoints must survive ====

# OMOP domain concept ids of the tables fact_relationship can point at.
FACT_DOMAIN_TABLES = {
    8: "visit_occurrence",
    10: "procedure_occurrence",
    13: "drug_exposure",
    17: "device_exposure",
    19: "condition_occurrence",
    21: "measurement",
    27: "observation",
    36: "specimen",
    56: "person",
}


@dp.temporary_view(name="_og_dropped_fact_endpoints", comment="Dropped rows of every table fact_relationship can reference.")
def _og_dropped_fact_endpoints():
    """(domain_concept_id, fact_id) of every dropped row in a fact_relationship target table."""
    frames = [
        _dropped_ids(table).select(F.lit(domain).alias("_og_domain"), F.col("cdm_row_id").alias("_og_fact_id"))
        for domain, table in FACT_DOMAIN_TABLES.items()
    ]
    union = frames[0]
    for frame in frames[1:]:
        union = union.unionByName(frame)
    return union


FACT_RELATIONSHIP_KEYS = ["domain_concept_id_1", "fact_id_1", "domain_concept_id_2", "fact_id_2", "relationship_concept_id"]


@dp.materialized_view(name="_og_audit_fact_relationship", comment=AUDIT_COMMENT.format(table="fact_relationship"))
def _og_audit_fact_relationship():
    """Relationships with a dropped endpoint (either side). Carries the full key: the table has no id."""
    endpoints = spark.read.table("_og_dropped_fact_endpoints")
    side_1 = endpoints.select(F.col("_og_domain").alias("domain_concept_id_1"),
                              F.col("_og_fact_id").alias("fact_id_1"), F.lit(True).alias("_og_drop_1"))
    side_2 = endpoints.select(F.col("_og_domain").alias("domain_concept_id_2"),
                              F.col("_og_fact_id").alias("fact_id_2"), F.lit(True).alias("_og_drop_2"))
    rows = (
        read_source("fact_relationship")
        .join(side_1, ["domain_concept_id_1", "fact_id_1"], "left")
        .join(side_2, ["domain_concept_id_2", "fact_id_2"], "left")
    )
    return rows.where(F.col("_og_drop_1") | F.col("_og_drop_2")).select(
        F.lit("fact_relationship").alias("cdm_table"),
        F.lit(None).cast("bigint").alias("cdm_row_id"),
        F.lit(None).cast("bigint").alias("person_id"),
        F.lit("endpoint_dropped").alias("drop_reason"),
        F.lit(None).cast("string").alias("policy_id"),
        F.array().cast("array<string>").alias("findings"),
        *FACT_RELATIONSHIP_KEYS,
    )


@dp.materialized_view(
    name="fact_relationship",
    comment=f"OMOP CDM fact_relationship: pairs whose both endpoints are published (omop_gold {PIPELINE_VERSION}).",
    schema=silver_schema("fact_relationship"),
    table_properties={"quality": "gold"},
)
@_expectations("fact_relationship")
def fact_relationship():
    """Silver relationships minus those with a dropped endpoint."""
    dropped = spark.read.table("_og_audit_fact_relationship").select(*FACT_RELATIONSHIP_KEYS)
    return read_source("fact_relationship").join(dropped, FACT_RELATIONSHIP_KEYS, "left_anti")


# COMMAND ----------

# ==== Reference tables passed through with structural checks ====

def _register_pass_through(table):
    """Declare a public table copied from silver under its mandatory and advisory rules."""

    @dp.materialized_view(
        name=table,
        comment=f"OMOP CDM {table}, copied from omop_silver (omop_gold {PIPELINE_VERSION}).",
        schema=silver_schema(table),
        table_properties={"quality": "gold"},
    )
    @_expectations(table)
    def public():
        """The silver table unchanged."""
        return read_source(table)


for _table in ["location", "care_site", "provider", "cdm_source"]:
    _register_pass_through(_table)

# Gold metadata ids start at 1000 so they never collide with silver's.
GOLD_METADATA_ROWS = [
    (1000, "omop_gold_pipeline_version", PIPELINE_VERSION),
    (1001, "omop_gold_source_schema", SOURCE_SCHEMA),
    (1002, "omop_gold_mapping_policy_ids", ",".join(sorted(GOLD_V2_POLICIES))),
    (1003, "omop_gold_rules", "person gate (omop_person_research_flags + birth after release); "
                              "S3D mapping gate; start after source_release_date; parent integrity; "
                              "FK nulling; end-before-start nulling"),
]


@dp.materialized_view(
    name="metadata",
    comment=f"OMOP CDM metadata: silver rows plus gold pipeline and policy rows (omop_gold {PIPELINE_VERSION}).",
    schema=silver_schema("metadata"),
    table_properties={"quality": "gold"},
)
@_expectations("metadata")
def metadata():
    """Silver metadata plus one row per gold release fact, dated at the source release."""
    silver = read_source("metadata")
    gold_rows = spark.createDataFrame(GOLD_METADATA_ROWS, "metadata_id long, name string, value_as_string string")
    gold = gold_rows.crossJoin(F.broadcast(spark.read.table("_og_release"))).select(
        F.col("metadata_id"),
        F.lit(0).cast("int").alias("metadata_concept_id"),
        F.lit(0).cast("int").alias("metadata_type_concept_id"),
        "name",
        "value_as_string",
        F.lit(None).cast("int").alias("value_as_concept_id"),
        F.lit(None).cast("double").alias("value_as_number"),
        F.col("_og_release_date").alias("metadata_date"),
        F.col("_og_release_date").cast("timestamp").alias("metadata_datetime"),
    )
    return silver.unionByName(gold.select(*silver.columns))


# COMMAND ----------

# ==== Sidecars: drop metrics, findings and mapping provenance ====

def _audit_tables():
    """Every audit dataset declared above, in a stable order."""
    tables = ["person", "death", *[t for t, s in FACT_TABLES.items() if not s["optional"] or source_exists(t)],
              "genomic_gene_tested", "episode_event", "fact_relationship"]
    if "infant" in TABLE_RULES:
        tables.append("infant")
    return tables


def _all_audits():
    """Union of every audit with the common columns."""
    common = ["cdm_table", "cdm_row_id", "person_id", "drop_reason", "policy_id", "findings"]
    frames = [spark.read.table(f"_og_audit_{table}").select(*common) for table in _audit_tables()]
    union = frames[0]
    for frame in frames[1:]:
        union = union.unionByName(frame)
    return union


@dp.materialized_view(
    name="omop_gold_dq_finding",
    comment="Advisory findings on published gold rows (row kept): event_before_birth, event_after_death_30d, dq4_g2_sex_plausibility_v1.",
)
def omop_gold_dq_finding():
    """One row per (published row, finding)."""
    return (
        _all_audits()
        .where(F.col("drop_reason").isNull())
        .select("cdm_table", "cdm_row_id", "person_id", F.explode("findings").alias("rule_id"))
        .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    )


@dp.materialized_view(
    name="omop_concept_provenance",
    comment="Mapping provenance of published gold rows, with the gold admission policy that admitted each gated mapping.",
)
def omop_concept_provenance():
    """Silver provenance minus rows of dropped facts; `policy_id` names the admitting S3D policy (NULL when ungated)."""
    dropped = _all_audits().where(F.col("drop_reason").isNotNull() & F.col("cdm_row_id").isNotNull()).select(
        "cdm_table", "cdm_row_id")
    provenance = read_source("omop_concept_provenance")
    kept = provenance.join(dropped, ["cdm_table", "cdm_row_id"], "left_anti")
    return _with_gate(kept).drop("_og_gate_pass")


@dp.materialized_view(
    name="omop_gold_drop_metrics",
    comment="Reason-coded gold drop counts per table (silver rows = gold rows + dropped) and published mappings per admission policy.",
)
def omop_gold_drop_metrics():
    """Dropped rows by (table, reason, policy), plus published provenance rows by admitting policy."""
    dropped = (
        _all_audits()
        .where(F.col("drop_reason").isNotNull())
        .groupBy("cdm_table", "drop_reason", "policy_id")
        .agg(F.count(F.lit(1)).alias("rows"))
        .select("cdm_table", F.lit("dropped").alias("measure"), F.col("drop_reason").alias("reason"), "policy_id", "rows")
    )
    published = (
        spark.read.table("omop_concept_provenance")
        .groupBy("cdm_table", "policy_id")
        .agg(F.count(F.lit(1)).alias("rows"))
        .select(
            "cdm_table",
            F.lit("published_mapping").alias("measure"),
            F.when(F.col("policy_id").isNull(), F.lit("ungated_no_axis")).otherwise(F.lit("admitted")).alias("reason"),
            "policy_id",
            "rows",
        )
    )
    return dropped.unionByName(published).withColumn("pipeline_version", F.lit(PIPELINE_VERSION))

