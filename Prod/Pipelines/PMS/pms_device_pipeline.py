# Databricks notebook source
# MAGIC %md
# MAGIC # Device post-market surveillance dataset
# MAGIC One row per implanted device with device identity, procedure context, patient basics, baseline,
# MAGIC outcomes and follow-up. Reads `4_prod.gold` only (TRE rule). Every derived value carries its rule id.

# COMMAND ----------

from datetime import datetime, timedelta, timezone

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

GOLD = spark.conf.get("pms.gold_schema", "4_prod.gold")
RULES_VERSION = "pms-2026.09.1"


def gold(table):
    return spark.read.table(f"{GOLD}.{table}")


def gold_exists(table):
    # spark.catalog.tableExists is not whitelisted inside a Lakeflow flow (Py4JSecurityException); ask
    # information_schema instead.
    cat, sch = GOLD.split(".")
    return spark.sql(f"SELECT 1 FROM `{cat}`.information_schema.tables WHERE table_schema = '{sch}' "
                     f"AND table_name = '{table}'").limit(1).count() > 0


def col_or_null(df, name, dtype="string"):
    # Phase 1 columns (GUDID, Datix device_*, theatre_case) may not be published yet; keep the schema stable.
    return (F.col(name) if name in df.columns else F.lit(None).cast(dtype)).alias(name)


def days_between(later, earlier):
    return F.datediff(F.to_date(later), F.to_date(earlier))


# PMS_DEVICE_RUN_DATE_V1: the data-as-of cutoff is the last complete UTC day before this update runs; there is no
# pinned release date. Nothing later is counted as follow-up or outcome, so a mid-day refresh never mixes partial
# days into censoring.
DATA_AS_OF = F.to_date(F.lit((datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()))

# INCR_FIX_20261018: DATA_AS_OF is a literal only inside the one-row _pms_as_of MV. Filters anti-join the
# days after it (_pms_unobserved_day), so a new data-as-of date touches only rows dated in between, and
# pms_followup reads the date from _pms_as_of. A literal in a model's query changes its fingerprint every
# day and forces a full recompute.
CALENDAR_TABLE = spark.conf.get("pms.calendar_table", "3_lookup.omop.calendar_day")
CALENDAR_LAST_DATE = "2300-12-31"  # last calendar row; later dates are always after data_as_of


@dp.materialized_view(name="_pms_as_of", private=True)
def _pms_as_of():
    """One row: this update's data-as-of date (the last complete UTC day before the run)."""
    return (spark.read.table(CALENDAR_TABLE).where(F.col("calendar_date") == DATA_AS_OF)
            .select(F.col("calendar_date").alias("_pms_as_of"), F.lit(True).alias("_pms_as_of_key")))


@dp.materialized_view(name="_pms_unobserved_day", private=True)
def _pms_unobserved_day():
    """Every calendar date after data_as_of."""
    as_of = spark.read.table("_pms_as_of")
    return (spark.read.table(CALENDAR_TABLE).withColumn("_pms_as_of_key", F.lit(True))
            .join(as_of, "_pms_as_of_key")
            .where(F.col("calendar_date") > F.col("_pms_as_of"))
            .select(F.col("calendar_date").alias("_pms_unobserved_date")))


def on_or_before_as_of(df, day, keep_null=False):
    """Keep rows whose DATE expression `day` is on or before data_as_of (NULL days kept only when keep_null)."""
    unobserved = spark.read.table("_pms_unobserved_day").withColumn("_pms_unobserved", F.lit(True))
    joined = df.withColumn("_pms_day", day).join(unobserved, F.col("_pms_day") == F.col("_pms_unobserved_date"), "left")
    late = F.col("_pms_unobserved").isNotNull() | (F.col("_pms_day") > F.lit(CALENDAR_LAST_DATE).cast("date"))
    keep = (F.col("_pms_day").isNotNull() & ~late) | (F.lit(keep_null) & F.col("_pms_day").isNull())
    return joined.where(keep).drop("_pms_day", "_pms_unobserved_date", "_pms_unobserved")


def date_only(ts):
    # Midnight = the source recorded a date without a time (2.5M of 51M gold clinical_condition rows).
    return F.date_format(ts, "HH:mm:ss") == "00:00:00"


def timing(ts, index_ts):
    # Compare timestamps, not dates. A same-day date-only event cannot be ordered against the implant, so it is
    # flagged instead of being called pre-op or post-op. Implant performed_start is never midnight (256,405/256,405).
    same_day_date_only = (F.to_date(ts) == F.to_date(index_ts)) & date_only(ts)
    return (F.when(ts.isNull() | index_ts.isNull(), F.lit(None))
             .when(same_day_date_only, F.lit("same_day_date_only"))
             .when(ts > index_ts, F.lit("after"))
             .otherwise(F.lit("before")))


def norm_text(c):
    # Upper-case alphanumerics only; used to compare manufacturer names across Datix and device identity.
    return F.regexp_replace(F.upper(c), "[^A-Z0-9]", "")


# COMMAND ----------

# OPCS Z94 laterality supplementary codes -> SNOMED side.
Z94_SIDE = {"Z94.1": ("51440002", "Bilateral"), "Z94.2": ("24028007", "Right"), "Z94.3": ("7771000", "Left")}
Z94_MAP = F.create_map(*[F.lit(x) for k, (c, _) in Z94_SIDE.items() for x in (k, c)])
Z94_NAME = F.create_map(*[F.lit(x) for c, n in Z94_SIDE.values() for x in (c, n)])


# Explicit projection: gold clinical_procedure also carries laterality_code/laterality_display, which for implants
# are the checklist flag (Task 2). They are never read here, so the Z94 join cannot produce a duplicate column.
IMPLANT_COLS = ("patient_event_key", "person_id", "encounter_id", "implant_event_id", "implant_sequence",
                "performed_start", "implant_description", "device_display", "manufacturer", "serial_number",
                "batch_number", "udi_di", "udi_standard", "quantity", "device_snomed_code", "device_snomed_display",
                "procedure_source_code", "procedure_source_display", "performer_practitioner_id")


def _implants():
    return gold("clinical_procedure").where("source_object = 'implant'").select(*IMPLANT_COLS)


def _opcs():
    # Millennium-coded OPCS rows (dotted codes). Shared by laterality, site family and revision rules.
    return (gold("clinical_procedure").where("source_object = 'millennium_procedure'")
            .select("patient_event_key", "person_id", "encounter_id", "performed_start",
                    F.col("procedure_source_code").alias("opcs"), "procedure_source_display"))


def _z94_side(events, key):
    # Z94.x recorded in the event's encounter within a day of it. Several sides -> conflict, not a guess.
    # events: (key, encounter_id, performed_start). Used for implants (Task 9) and revisions (Task 11).
    ev = events.select(F.col(key).alias("_k"), F.col("encounter_id").alias("_enc"),
                       F.to_date("performed_start").alias("_d"))
    z = (_opcs().where("opcs LIKE 'Z94%'")
         .select(F.col("encounter_id").alias("z_enc"), F.to_date("performed_start").alias("z_d"), F.col("opcs").alias("z")))
    j = ev.join(z, (ev._enc == z.z_enc) & (F.abs(F.datediff(ev._d, z.z_d)) <= 1))
    g = j.groupBy("_k").agg(
        F.array_distinct(F.collect_list(Z94_MAP[F.col("z")])).alias("sides"),
        F.max((F.col("z") == "Z94.4").cast("int")).alias("unilateral_unspecified"))
    n = F.size(F.col("sides"))
    return g.select(
        F.col("_k").alias(key),
        F.when(n == 1, F.col("sides")[0]).alias("laterality_snomed_code"),
        F.when(n == 1, Z94_NAME[F.col("sides")[0]]).alias("laterality_display"),
        F.when(n == 1, F.lit("opcs_z94_encounter"))
         .when(n > 1, F.lit("opcs_z94_conflict"))
         .when(F.col("unilateral_unspecified") == 1, F.lit("opcs_z94_unilateral_unspecified"))
         .otherwise(F.lit("opcs_z94_other")).alias("laterality_method"))


def _laterality():
    return _z94_side(_implants(), "patient_event_key")


# Anatomical site family from dotted OPCS codes. It links a later revision to the device family it revises
# (Task 11). Rule id = "site_family:<family>". Codes outside these families give no family, never a guess.
OPCS_SITE_FAMILY = {
    "hip": r"^W3[7-9]|^W9[3-5]|^W4[6-8]",
    "knee": r"^W4[0-2]|^O18",
    "shoulder": r"^W49|^W5[01]|^O0[6-8]",
    "cardiac_rhythm": r"^K59|^K6[01]|^K7[23]",
    "heart_valve": r"^K2[5-9]|^K3[0-8]",
    "coronary_stent": r"^K49|^K7[56]",
    "hernia_mesh": r"^T2[0-7]",
    "breast": r"^B30",
    "lens": r"^C7[1-5]",
}


def site_family(code):
    out = F.lit(None).cast("string")
    for fam, rx in reversed(list(OPCS_SITE_FAMILY.items())):
        out = F.when(code.rlike(rx), F.lit(fam)).otherwise(out)
    return out


def _site_families():
    # Families of the OPCS procedures coded in the implant encounter within a day of the implant.
    imp = _implants().select("patient_event_key", F.col("encounter_id").alias("_enc"), F.to_date("performed_start").alias("_d"))
    o = (_opcs().select(F.col("encounter_id").alias("o_enc"), F.to_date("performed_start").alias("o_d"),
                        site_family(F.col("opcs")).alias("fam"))
         .where("fam IS NOT NULL"))
    j = imp.join(o, (imp._enc == o.o_enc) & (F.abs(F.datediff(imp._d, o.o_d)) <= 1))
    return j.groupBy("patient_event_key").agg(F.array_sort(F.collect_set("fam")).alias("site_families"))


# Implant attributes (gold clinical_procedure_implant_attribute). 877,630 of 3.34M (key, attribute) pairs carry
# more than one distinct value, so the latest wins and every other value is published in attribute_conflicts.
ATTRS = {"SIZE": "implant_size", "EXPIRY_DATE_RAW": "implant_expiry_raw",
         "STERILISATION_DATE_RAW": "implant_sterilisation_raw"}


def cerner_date(c):
    # Raw format 1:YYYYMMDD00000000:0.000000:0:0. Rule attr_date_floor_1990: 13 expiry values keyed as 1919-1934
    # (century slips) parse to impossible dates; they stay in *_raw and the parsed date is NULL.
    d = f"try_to_date(regexp_extract({c}, '^1:([0-9]{{8}})', 1), 'yyyyMMdd')"
    return F.expr(f"CASE WHEN year({d}) >= 1990 THEN {d} END")


def _attributes():
    a = (gold("clinical_procedure_implant_attribute")
         .where(F.col("attribute_name").isin(list(ATTRS)))
         .where(~F.upper(F.trim("attribute_value")).isin("", "N/A", "NA") & F.col("attribute_value").isNotNull()))
    w = Window.partitionBy("patient_event_key", "attribute_name").orderBy(
        F.col("valid_from").desc_nulls_last(), F.col("clinical_event_id").desc_nulls_last(), F.col("attribute_value").asc())
    r = a.withColumn("_rn", F.row_number().over(w)).withColumn("_latest", F.first("attribute_value").over(w))
    latest = r.where("_rn = 1").groupBy("patient_event_key").pivot("attribute_name", list(ATTRS)).agg(F.first("attribute_value"))
    conflicts = (r.where("attribute_value <> _latest")
                 .select("patient_event_key", F.concat_ws("=", "attribute_name", "attribute_value").alias("c"))
                 .groupBy("patient_event_key").agg(F.array_sort(F.collect_set("c")).alias("attribute_conflicts")))
    out = latest.join(conflicts, "patient_event_key", "left")
    for src, dst in ATTRS.items():
        out = out.withColumnRenamed(src, dst)
    return (out.withColumn("implant_expiry_date", cerner_date("implant_expiry_raw"))
               .withColumn("implant_sterilisation_date", cerner_date("implant_sterilisation_raw")))


@dp.materialized_view(
    name="pms_device_exposure",
    comment="One implanted device (gold clinical_procedure implant row) with device identity, UDI capture evidence, laterality and index-date quality.",
    cluster_by=["person_id"],
)
def pms_device_exposure():
    i = _implants()
    dm = gold("reference_device_mapping")
    dmc = [col_or_null(dm, c) for c in (
        "gmdn_code", "gmdn_name", "gmdn_implantable", "gmdn_code_status", "gudid_primary_di", "gudid_brand_name",
        "gudid_model_number", "gudid_company_name", "gudid_catalogue_number", "gudid_device_status",
        "source_catalogue_number", "source_manufacturer", "mapped_device_name", "mapping_status",
        "mapping_confidence", "confidence_tier", "mapping_rule_id", "mapping_layer", "mapping_ambiguous_ind")]
    d = dm.select(F.col("source_event_id").alias("_dm_event_id"), *dmc)
    year = F.year("performed_start")
    out = (i.join(d, i.implant_event_id.cast("string") == F.col("_dm_event_id"), "left")
            .join(_laterality(), "patient_event_key", "left")
            .join(_site_families(), "patient_event_key", "left")
            .join(_attributes(), "patient_event_key", "left"))
    return out.select(
        F.col("patient_event_key").alias("exposure_id"),
        "person_id", "encounter_id",
        F.col("implant_event_id").cast("string").alias("implant_event_id"), "implant_sequence",
        F.col("performed_start").alias("index_datetime"),
        F.when(F.col("performed_start").isNull(), F.lit("missing"))
         .when(year <= 1970, F.lit("epoch_truncated")).otherwise(F.lit("recorded")).alias("index_date_status"),
        F.when(year > 1970, F.to_date("performed_start")).alias("index_date"),
        "implant_description", "device_display", "manufacturer", "serial_number", "batch_number",
        "udi_di", "udi_standard", "quantity", "device_snomed_code", "device_snomed_display",
        F.when(F.col("udi_standard") == "GS1", F.lit("gs1_scan"))
         .when(F.col("udi_standard") == "HIBCC", F.lit("hibcc_scan"))
         .when(F.col("udi_di").isNotNull(), F.lit("udi_other"))
         .otherwise(F.lit("manual_or_none")).alias("udi_capture_evidence"),
        *[F.col(c) for c in d.columns if c != "_dm_event_id"],
        F.col("procedure_source_code").alias("implant_procedure_code"),
        F.col("procedure_source_display").alias("implant_procedure_display"),
        F.col("performer_practitioner_id").alias("implanter_practitioner_id"),
        "laterality_snomed_code", "laterality_display",
        F.coalesce("laterality_method", F.lit("none")).alias("laterality_method"),
        F.coalesce("site_families", F.array().cast("array<string>")).alias("site_families"),
        "implant_size", "implant_expiry_raw", "implant_expiry_date", "implant_sterilisation_raw",
        "implant_sterilisation_date",
        F.when(year > 1970, F.to_date("performed_start") > F.col("implant_expiry_date")).alias("implant_expired_at_index_ind"),
        F.coalesce("attribute_conflicts", F.array().cast("array<string>")).alias("attribute_conflicts"),
        F.lit(RULES_VERSION).alias("rules_version"),
    )


# COMMAND ----------

CASE_COLS = [("theatre_case_key", "string"), ("surgical_case_number", "string"), ("case_status", "string"),
             ("surgery_start", "timestamp"), ("surgery_stop", "timestamp"), ("surgery_duration_minutes", "double"),
             ("scheduled_duration_minutes", "double"), ("add_on_ind", "bigint"), ("asa_class_display", "string"),
             ("wound_class_display", "string"), ("anaesthesia_type_display", "string"), ("case_level_display", "string"),
             ("patient_type_display", "string"), ("surgical_specialty_id", "bigint"), ("surgeon_practitioner_id", "bigint"),
             ("anaesthetist_practitioner_id", "bigint"), ("institution_display", "string"),
             ("operating_location_display", "string"), ("implant_expected_ind", "boolean"),
             ("implant_expected_source", "string"), ("person_id", "bigint"), ("encounter_id", "bigint")]


def _cases():
    # gold_reference.theatre_case arrives with Phase 1; before that there are no case rows to link by time.
    if gold_exists("reference_theatre_case"):
        c = gold("reference_theatre_case")
        return c.select(*[col_or_null(c, n, t) for n, t in CASE_COLS])
    return spark.createDataFrame([], ", ".join(f"{n} {t}" for n, t in CASE_COLS))


@dp.materialized_view(
    name="pms_procedure_context",
    comment="Theatre case linked to each exposure (method + rank), case covariates, and operator x device-type case ordinal.",
    cluster_by=["exposure_id"],
)
def pms_procedure_context():
    e = spark.read.table("pms_device_exposure").select(
        "exposure_id", "person_id", "encounter_id", "implant_event_id", "index_datetime", "index_date",
        "gmdn_code", "implanter_practitioner_id")
    c = _cases().select(*[F.col(n).alias("c_" + n) for n, _ in CASE_COLS])
    ti = gold("reference_theatre_implant").where("mill_implant_event_id IS NOT NULL").select(
        F.col("mill_implant_event_id").alias("x_event_id"), F.col("theatre_case_key").alias("x_case_key"),
        F.col("implant_link_method").alias("x_method"))
    # Rank 1: SurgiNet implant-log crosswalk (person + serial unique). Rank 2: same encounter, inside the case window.
    # Rank 3: same person, same day. Best rank wins; ties at that rank are flagged, the nearest start is kept.
    r1 = (e.join(ti, e.implant_event_id == ti.x_event_id)
           .select("exposure_id", F.col("x_case_key").alias("theatre_case_key"), F.lit(1).alias("link_rank"),
                   F.concat(F.lit("implant_log_crosswalk:"), F.col("x_method")).alias("link_method")))
    win_end = F.coalesce(F.col("c_surgery_stop"), F.col("c_surgery_start") + F.expr("INTERVAL 12 HOURS")) + F.expr("INTERVAL 1 HOUR")
    r2 = (e.join(c, (e.encounter_id == c.c_encounter_id)
                    & (e.index_datetime >= c.c_surgery_start - F.expr("INTERVAL 1 HOUR")) & (e.index_datetime <= win_end))
           .select("exposure_id", F.col("c_theatre_case_key").alias("theatre_case_key"), F.lit(2).alias("link_rank"),
                   F.lit("encounter_time_window").alias("link_method")))
    r3 = (e.join(c, (e.person_id == c.c_person_id) & (e.index_date == F.to_date(c.c_surgery_start)))
           .select("exposure_id", F.col("c_theatre_case_key").alias("theatre_case_key"), F.lit(3).alias("link_rank"),
                   F.lit("person_same_day").alias("link_method")))
    cand = r1.unionByName(r2).unionByName(r3).join(e.select("exposure_id", "index_datetime"), "exposure_id") \
             .join(c.select("c_theatre_case_key", "c_surgery_start"), F.col("theatre_case_key") == F.col("c_theatre_case_key"), "left")
    best = cand.withColumn("best_rank", F.min("link_rank").over(Window.partitionBy("exposure_id"))).where("link_rank = best_rank")
    w = Window.partitionBy("exposure_id").orderBy(
        F.abs(F.col("index_datetime").cast("long") - F.col("c_surgery_start").cast("long")).asc_nulls_last(),
        F.col("theatre_case_key").asc_nulls_last())
    pick = (best.withColumn("n_at_rank", F.size(F.collect_set("theatre_case_key").over(Window.partitionBy("exposure_id"))))
                .withColumn("rn", F.row_number().over(w)).where("rn = 1")
                .select("exposure_id", "theatre_case_key", "link_rank", "link_method",
                        (F.col("n_at_rank") > 1).alias("link_ambiguous_ind")))
    out = (e.join(pick, "exposure_id", "left")
            .join(c, F.col("theatre_case_key") == F.col("c_theatre_case_key"), "left"))
    operator = F.coalesce(F.col("c_surgeon_practitioner_id"), F.col("implanter_practitioner_id"))
    case_ref = F.coalesce(F.col("theatre_case_key"), F.concat(F.lit("enc:"), F.col("encounter_id").cast("string")))
    out = out.withColumn("_operator", operator).withColumn("_case_ref", case_ref) \
             .withColumn("_case_first", F.min("index_datetime").over(Window.partitionBy("_case_ref")))
    ow = Window.partitionBy("_operator", "gmdn_code").orderBy(F.col("_case_first").asc_nulls_last(), "_case_ref")
    return out.select(
        "exposure_id", "theatre_case_key",
        F.coalesce("link_method", F.lit("none")).alias("case_link_method"), F.col("link_rank").alias("case_link_rank"),
        F.coalesce("link_ambiguous_ind", F.lit(False)).alias("case_link_ambiguous_ind"),
        *[F.col("c_" + n).alias(n) for n, _ in CASE_COLS if n not in ("theatre_case_key", "person_id", "encounter_id")],
        F.col("_operator").alias("operator_practitioner_id"),
        F.when(F.col("_operator").isNotNull() & F.col("gmdn_code").isNotNull() & F.col("index_datetime").isNotNull(),
               F.dense_rank().over(ow)).alias("operator_device_type_case_ordinal"),
        F.lit(RULES_VERSION).alias("rules_version"),
    )


# COMMAND ----------

@dp.materialized_view(
    name="pms_patient",
    comment="One person with at least one device exposure: sex, birth date, age at first index, ethnicity, deprivation at first index, death. NHS number is IG-tagged; de-identify at serve time.",
    cluster_by=["person_id"],
)
def pms_patient():
    e = spark.read.table("pms_device_exposure").groupBy("person_id").agg(
        F.min("index_date").alias("first_index_date"), F.count("*").alias("exposure_count"))
    p = gold("spine_person").select("person_id", "gender_display", "birth_date", "ethnicity_display",
                                    "ethnicity_snomed_code", "deceased_ind", "deceased_datetime", "nhs_number")
    a = gold("reference_person_address").select("person_id", "lsoa", "imd_decile", "imd_quintile",
                                                "beg_effective", "end_effective")
    # Address in force on the first index date; else the latest address on record, and say which.
    j = e.join(a, "person_id", "left")
    in_force = (F.col("beg_effective") <= F.col("first_index_date")) & \
               (F.col("end_effective").isNull() | (F.col("end_effective") >= F.col("first_index_date")))
    aw = Window.partitionBy("person_id").orderBy(in_force.desc_nulls_last(), F.col("beg_effective").desc_nulls_last(),
                                                 F.col("lsoa").asc_nulls_last())
    addr = (j.withColumn("rn", F.row_number().over(aw)).where("rn = 1")
             .select("person_id", "lsoa", "imd_decile", "imd_quintile",
                     F.when(in_force, F.lit("in_force_at_index"))
                      .when(F.col("lsoa").isNotNull(), F.lit("latest_on_record"))
                      .otherwise(F.lit("none")).alias("imd_method")))
    return (e.join(p, "person_id", "left").join(addr, "person_id", "left").select(
        "person_id", "exposure_count", "first_index_date",
        F.col("gender_display").alias("sex"), "birth_date",
        # Rule age_plausible_0_110: placeholder birth dates (1910-01-01) give ages over 110; publish NULL age and
        # keep birth_date as recorded.
        F.expr("CASE WHEN floor(months_between(first_index_date, birth_date) / 12) BETWEEN 0 AND 110 "
               "THEN cast(floor(months_between(first_index_date, birth_date) / 12) AS int) END").alias("age_at_first_index"),
        "ethnicity_display", "ethnicity_snomed_code", "lsoa", "imd_decile", "imd_quintile", "imd_method",
        "deceased_ind", "deceased_datetime", "nhs_number", F.lit(RULES_VERSION).alias("rules_version")))


# COMMAND ----------

ICD10 = "http://hl7.org/fhir/sid/icd-10"
# rule id -> ICD-10 prefix regex on dotted codes. Anchored; no bare '.' (engine divergence).
COMORBIDITY_RULES = {
    "diabetes": r"^E1[0-4]", "obesity": r"^E66", "chronic_kidney_disease": r"^N18", "heart_failure": r"^I50",
    "ischaemic_heart_disease": r"^I2[0-5]", "cerebrovascular_disease": r"^I6[0-9]|^G4[56]",
    "peripheral_vascular_disease": r"^I7[0-3]", "copd": r"^J4[34]", "rheumatoid_arthritis": r"^M0[56]",
    "osteoporosis": r"^M8[01]", "malignancy": r"^C[0-8][0-9]|^C9[0-7]", "liver_disease": r"^K7[0-7]",
    "dementia": r"^F0[0-3]|^G30", "immunodeficiency": r"^D8[0-4]|^B2[0-4]",
    "smoking": r"^F17|^Z72\.0", "anticoagulation_long_term": r"^Z92\.1",
}
VITALS = {"bmi": ("60621009", 10.0, 80.0), "weight_kg": ("27113001", 20.0, 350.0), "height": ("50373000", 0.9, 250.0)}
# Pre-op labs: gold clinical_pathology_result, preferred result only, latest in the 90 days before index.
# name -> (test SNOMED codes, accepted UCUM units). The unit filter matters: HbA also maps to 313995005, NULL unit.
LABS = {
    "haemoglobin": (["313995005"], ["g/L"]), "albumin": (["104485008"], ["g/L"]),
    "creatinine": (["313936008", "113075003"], ["umol/L"]), "egfr": (["791801000000102"], ["mL/min"]),
    "hba1c": (["43396009"], ["mmol/mol"]), "crp": (["55235003"], ["mg/L"]), "wbc": (["767002"], ["10*9/L"]),
}


def _latest_before(df, t_col, days, part="exposure_id", tiebreak=None):
    # Strictly before the implant time; same-day date-only rows cannot be ordered and are excluded.
    x = df.where((timing(F.col(t_col), F.col("idx_t")) == "before")
                 & (F.col(t_col) >= F.col("idx_t") - F.expr(f"INTERVAL {days} DAYS")))
    order = [F.col(t_col).desc()] + ([tiebreak] if tiebreak is not None else [])
    return x.withColumn("rn", F.row_number().over(Window.partitionBy(part).orderBy(*order))).where("rn = 1")


@dp.materialized_view(
    name="pms_baseline",
    comment="Per exposure: comorbidity flags (ICD-10 rules, 5 years before the implant time), indication diagnoses in the index encounter, latest BMI/weight/height (365 d), smoking status (5 y) and pre-op labs (90 d). Same-day date-only evidence is excluded and listed.",
    cluster_by=["exposure_id"],
)
def pms_baseline():
    # idx_t: implant timestamp, NULL when the index date is epoch-truncated or missing.
    e = spark.read.table("pms_device_exposure").select(
        "exposure_id", "person_id", "encounter_id", "index_date",
        F.when(F.col("index_date").isNotNull(), F.col("index_datetime")).alias("idx_t"))
    cond = gold("clinical_condition").where(F.col("condition_source_system") == ICD10).select(
        "person_id", F.col("encounter_id").alias("c_enc"), F.col("event_datetime").alias("c_t"),
        F.col("condition_source_code").alias("code"))
    cj = e.join(cond, "person_id").withColumn("_timing", timing(F.col("c_t"), F.col("idx_t")))
    prior = cj.where((F.col("_timing") == "before") & (F.col("c_t") >= F.col("idx_t") - F.expr("INTERVAL 1826 DAYS")))
    flags = prior.groupBy("exposure_id").agg(
        *[F.max(F.col("code").rlike(rx).cast("int")).alias(f"cm_{k}") for k, rx in COMORBIDITY_RULES.items()])
    sd = (cj.where(F.col("_timing") == "same_day_date_only").groupBy("exposure_id")
            .agg(F.sort_array(F.collect_set("code")).alias("same_day_date_only_icd10_codes")))
    ind = (e.join(cond, (e.person_id == cond.person_id) & (e.encounter_id == cond.c_enc))
            .groupBy("exposure_id").agg(F.sort_array(F.collect_set("code")).alias("index_encounter_icd10_codes")))
    v = gold("clinical_vital_sign").where(F.col("vital_snomed_code").isin([c for c, _, _ in VITALS.values()])).select(
        "person_id", "vital_snomed_code", "value_number", F.col("event_datetime").alias("v_t"))
    vj = e.join(v, "person_id")
    parts = [flags, sd, ind]
    for name, (code, lo, hi) in VITALS.items():
        x = vj.where((F.col("vital_snomed_code") == code) & F.col("value_number").between(lo, hi))
        if name == "height":  # metres or centimetres at source -> cm
            x = x.withColumn("value_number", F.when(F.col("value_number") < 3, F.col("value_number") * 100)
                                               .otherwise(F.col("value_number"))).where(F.col("value_number").between(90, 250))
        x = _latest_before(x, "v_t", 365, tiebreak=F.col("value_number").desc())
        parts.append(x.select("exposure_id", F.col("value_number").alias(name if name != "height" else "height_cm"),
                              F.to_date("v_t").alias(f"{name}_date")))
    smk = gold("clinical_form_smoking_status").select(
        "person_id", F.col("event_datetime").alias("s_t"), "smoking_status_snomed_code", "smoking_status_snomed_display",
        "smoking_status_source_display", "smoking_status_map_method")
    s = _latest_before(e.join(smk, "person_id"), "s_t", 1826,
                       tiebreak=F.col("smoking_status_snomed_code").asc_nulls_last())
    parts.append(s.select("exposure_id", "smoking_status_snomed_code", "smoking_status_snomed_display",
                          "smoking_status_source_display", "smoking_status_map_method",
                          F.to_date("s_t").alias("smoking_status_date")))
    lab = gold("clinical_pathology_result").where("preferred_result_ind AND value_number IS NOT NULL").select(
        "person_id", "test_snomed_code", "ucum_code", "value_number", F.col("event_datetime").alias("l_t"))
    lj = e.join(lab, "person_id")
    for name, (codes, units) in LABS.items():
        x = lj.where(F.col("test_snomed_code").isin(codes) & F.col("ucum_code").isin(units))
        x = _latest_before(x, "l_t", 90, tiebreak=F.col("value_number").desc())
        parts.append(x.select("exposure_id", F.col("value_number").alias(f"preop_{name}"),
                              F.to_date("l_t").alias(f"preop_{name}_date")))
    out = e.select("exposure_id")
    for p in parts:
        out = out.join(p, "exposure_id", "left")
    cm = [F.coalesce(F.col(f"cm_{k}"), F.lit(0)).cast("boolean").alias(f"cm_{k}") for k in COMORBIDITY_RULES]
    return out.select("exposure_id", *cm, "index_encounter_icd10_codes",
                      F.coalesce("same_day_date_only_icd10_codes", F.array().cast("array<string>")).alias("same_day_date_only_icd10_codes"),
                      "bmi", "bmi_date", "weight_kg", "weight_kg_date", "height_cm", "height_date",
                      "smoking_status_snomed_code", "smoking_status_snomed_display", "smoking_status_source_display",
                      "smoking_status_map_method", "smoking_status_date",
                      *[c for n in LABS for c in (f"preop_{n}", f"preop_{n}_date")],
                      F.lit(RULES_VERSION).alias("rules_version"))


# COMMAND ----------

ICD10_OUTCOMES = {
    ("infection", "icd10_device_infection"): r"^T84\.[5-7]|^T82\.[67]|^T83\.[56]|^T85\.7|^T81\.4",
    ("mechanical_complication", "icd10_device_mechanical"): r"^T84\.[0-4]|^T82\.[0-5]|^T83\.[0-4]|^T85\.[0-6]",
    ("other_device_complication", "icd10_device_other"): r"^T8[2-5]\.[89]",
    ("periprosthetic_fracture", "icd10_m96_6"): r"^M96\.6",
}
REVISION_RX = r"(?i)\brevision\b|\bremoval of\b|\bre-?implant|\bconversion\b|\breplacement of prosthe"
BILATERAL = "51440002"
# Contacts that did not happen: excluded from readmission, ED and last-contact. Blank status (2.32M) is kept
# and its share is reported by V10.
NOT_COMPLETED = ["Preadmit", "Cancelled", "Cancelled Pending Arrival", "Rejected Pending Arrival", "Pending Arrival"]
# attribution values that let an event count toward first_*/n_*/horizon flags (with timing = 'after' and
# candidate_count <= 1). Everything else is published as evidence but not counted.
COUNTED_ATTRIBUTION = ["person", "site_side", "site_side_unknown", "same_gmdn_side", "serial_person",
                       "serial_manufacturer"]
OUT_COLS = ["exposure_id", "outcome_type", "rule_id", "event_datetime", "source_table", "source_key", "detail",
            "in_index_encounter_ind", "timing", "attribution", "candidate_count"]


def _norm_serial(c):
    return F.regexp_replace(F.upper(F.trim(c)), r"[^A-Z0-9]", "")


def _outcome(df, otype, rule, ts, table, key, detail, in_idx, attribution=None, candidate_count=None):
    # df carries idx_t (implant timestamp, NULL when undated). attribution defaults to 'person': a patient-level
    # outcome (infection, readmission, ...) applies to every dated exposure of the person, with no candidate count.
    return df.select("exposure_id", F.lit(otype).alias("outcome_type"), F.lit(rule).alias("rule_id"),
                     F.col(ts).cast("timestamp").alias("event_datetime"), F.lit(f"{GOLD}.{table}").alias("source_table"),
                     F.col(key).cast("string").alias("source_key"), F.col(detail).cast("string").alias("detail"),
                     in_idx.alias("in_index_encounter_ind"), timing(F.col(ts).cast("timestamp"), F.col("idx_t")).alias("timing"),
                     (attribution if attribution is not None else F.lit("person")).alias("attribution"),
                     (candidate_count if candidate_count is not None else F.lit(None)).cast("int").alias("candidate_count"))


def _exposures():
    return spark.read.table("pms_device_exposure").select(
        "exposure_id", "person_id", F.col("encounter_id").alias("idx_enc"), "index_date",
        F.when(F.col("index_date").isNotNull(), F.col("index_datetime")).alias("idx_t"),
        "gmdn_code", "serial_number", "site_families", F.col("laterality_snomed_code").alias("idx_side"),
        norm_text(F.coalesce("gudid_company_name", "source_manufacturer", "manufacturer")).alias("ex_mfr"))


def _side_ok(a, b):
    # Compatible unless both sides are known, neither is bilateral, and they differ.
    return a.isNull() | b.isNull() | (a == F.lit(BILATERAL)) | (b == F.lit(BILATERAL)) | (a == b)


def _latest_prior_instance(j, part, enc="idx_enc"):
    # Attribute to the latest prior operation only: every component implanted on that index date, not every
    # earlier implant. candidate_count = distinct index encounters on that date (>1 = ambiguous, not counted).
    w = Window.partitionBy(part)
    k = j.withColumn("_latest", F.max("index_date").over(w)).where(F.col("index_date") == F.col("_latest"))
    return k.withColumn("candidate_count", F.size(F.collect_set(enc).over(w)))


def _revisions():
    # OPCS revision/removal procedures with their site family and Z94 side.
    rv = _opcs().where(F.col("procedure_source_display").rlike(REVISION_RX))
    side = _z94_side(rv, "patient_event_key").select("patient_event_key", F.col("laterality_snomed_code").alias("rv_side"))
    return rv.join(side, "patient_event_key", "left").select(
        F.col("patient_event_key").alias("rv_key"), "person_id", F.col("encounter_id").alias("o_enc"),
        F.col("performed_start").alias("t"), F.col("opcs").alias("code"),
        F.col("procedure_source_display").alias("disp"), site_family(F.col("opcs")).alias("fam"), "rv_side")


def _revision_links():
    # Candidate exposures: same person, implanted before the revision, same site family, compatible side.
    e = _exposures().where("idx_t IS NOT NULL")
    rv = _revisions().where("fam IS NOT NULL")
    j = (e.join(rv, "person_id").where((F.col("t") > F.col("idx_t")) & F.array_contains("site_families", F.col("fam"))
                                       & _side_ok(F.col("idx_side"), F.col("rv_side"))))
    return (_latest_prior_instance(j, "rv_key")
            .withColumn("attribution", F.when(F.col("idx_side").isNotNull() & F.col("rv_side").isNotNull(), F.lit("site_side"))
                                         .otherwise(F.lit("site_side_unknown"))))


@dp.materialized_view(
    name="pms_outcome_event",
    comment="Long-format outcome events and device evidence per exposure: rule id, timing against the implant time, attribution and candidate count. counted_ind marks the rows that feed pms_dataset. Surveillance rules, not adjudicated outcomes.",
    cluster_by=["exposure_id"],
)
def pms_outcome_event():
    e = _exposures()
    e_dated = e.where("idx_t IS NOT NULL")
    kept = F.col("timing").isin("after", "same_day_date_only")  # 'before' = baseline, never an outcome
    same_enc = lambda enc: F.coalesce(F.col(enc) == F.col("idx_enc"), F.lit(False))
    parts = []

    cond = gold("clinical_condition").where(F.col("condition_source_system") == ICD10).select(
        "person_id", F.col("encounter_id").alias("o_enc"), "event_datetime", F.col("condition_source_code").alias("code"))
    cj = e_dated.join(cond, "person_id")
    for (otype, rule), rx in ICD10_OUTCOMES.items():
        parts.append(_outcome(cj.where(F.col("code").rlike(rx)), otype, rule, "event_datetime", "clinical_condition",
                              "code", "code", same_enc("o_enc")))

    mi = gold("clinical_microbiology_isolate").where("organism_text IS NOT NULL OR organism_snomed_code IS NOT NULL").select(
        "person_id", F.col("encounter_id").alias("o_enc"), "event_datetime",
        F.coalesce("organism_snomed_code", "organism_text").alias("org"),
        F.concat_ws(" | ", "organism_text", "specimen_type_code",
                    F.when(F.col("suspected_ind"), F.lit("suspected"))).alias("det"))
    mj = e_dated.join(mi, "person_id").where(F.col("event_datetime") <= F.col("idx_t") + F.expr("INTERVAL 365 DAYS"))
    parts.append(_outcome(mj, "microbiology_isolate", "micro_isolate_365d", "event_datetime",
                          "clinical_microbiology_isolate", "org", "det", same_enc("o_enc")))

    rl = _revision_links().withColumn("det", F.concat_ws(" | ", "fam", "code", "disp"))
    parts.append(_outcome(rl, "revision_procedure", "opcs_revision_site_side", "t", "clinical_procedure", "rv_key",
                          "det", same_enc("o_enc"), F.col("attribution"), F.col("candidate_count")))

    later = e_dated.select(F.col("person_id").alias("l_person"), F.col("gmdn_code").alias("l_gmdn"),
                           F.col("idx_t").alias("l_t"), F.col("exposure_id").alias("l_id"),
                           F.col("idx_enc").alias("o_enc"), F.col("idx_side").alias("l_side"))
    lj = e_dated.join(later, (e_dated.person_id == later.l_person) & (e_dated.gmdn_code == later.l_gmdn)
                      & (later.l_t > e_dated.idx_t + F.expr("INTERVAL 1 DAY")) & _side_ok(F.col("idx_side"), F.col("l_side")))
    lj = _latest_prior_instance(lj, "l_id")
    parts.append(_outcome(lj, "repeat_same_gmdn_implant", "repeat_gmdn_implant", "l_t", "clinical_procedure", "l_id",
                          "l_gmdn", same_enc("o_enc"), F.lit("same_gmdn_side"), F.col("candidate_count")))

    inc = gold("clinical_safety_incident").select(
        F.col("incident_id").cast("string").alias("iid"), F.col("person_id").alias("i_person"),
        F.col("encounter_id").alias("o_enc"), F.col("incident_dt_tm_clean").alias("t"),
        F.col("inc_severity_display").alias("sev"))
    ij = e_dated.join(inc, e_dated.person_id == inc.i_person)
    parts.append(_outcome(ij, "safety_incident_person", "datix_person", "t", "clinical_safety_incident", "iid", "sev",
                          same_enc("o_enc")))

    fac = gold("clinical_safety_incident_factor")
    fac = fac.where(F.col("factor_type") == "device").select(
        F.col("inc_id").cast("string").alias("iid"), _norm_serial(col_or_null(fac, "device_serial_number")).alias("ns"),
        norm_text(col_or_null(fac, "device_manufacturer")).alias("i_mfr"))
    sj = (e.where(F.length(_norm_serial(F.col("serial_number"))) >= 5)
           .withColumn("ns", _norm_serial(F.col("serial_number")))
           .join(fac.where(F.length("ns") >= 5), "ns")
           .join(inc.select("iid", "t", "sev", "o_enc", "i_person"), "iid", "left"))
    mfr_known = (F.length("ex_mfr") >= 3) & (F.length("i_mfr") >= 3)
    mfr_conflict = mfr_known & (F.expr("instr(ex_mfr, i_mfr)") == 0) & (F.expr("instr(i_mfr, ex_mfr)") == 0)
    sj = sj.withColumn("attribution",
        F.when(F.col("i_person").isNotNull() & (F.col("i_person") != F.col("person_id")), F.lit("rejected_person_conflict"))
         .when(mfr_conflict, F.lit("rejected_manufacturer_conflict"))
         .when(F.col("i_person") == F.col("person_id"), F.lit("serial_person"))
         .when(mfr_known, F.lit("serial_manufacturer"))
         .otherwise(F.lit("serial_only")))
    acc = ~F.col("attribution").startswith("rejected")
    sj = sj.withColumn("candidate_count", F.size(F.collect_set(F.when(acc, F.col("person_id"))).over(Window.partitionBy("iid"))))
    sj = sj.withColumn("det", F.concat_ws(" | ", "sev", F.concat(F.lit("mfr="), F.coalesce("i_mfr", F.lit("")))))
    parts.append(_outcome(sj, "safety_incident_device_serial", "datix_device_serial", "t",
                          "clinical_safety_incident_factor", "iid", "det", same_enc("o_enc"),
                          F.col("attribution"), F.col("candidate_count")))

    enc = (gold("spine_encounter").where(~F.coalesce(F.col("status_display"), F.lit("")).isin(NOT_COMPLETED))
           # class_display is NULL on 41M of 48.5M rows (Emergency only 2005-2010); type_class_display (Cerner
           # encounter type class) is populated through today, so it fills the gaps.
           .select("person_id", F.col("encounter_id").alias("o_enc"),
                   F.coalesce("class_display", "type_class_display").alias("class_display"), "period_start"))
    idx_end = gold("spine_encounter").select(F.col("encounter_id").alias("idx_enc"),
                                             F.coalesce("discharge_datetime", "period_end").alias("idx_end"))
    ej = e_dated.join(idx_end, "idx_enc", "left").join(enc, "person_id").where(F.col("o_enc") != F.col("idx_enc"))
    parts.append(_outcome(ej.where((F.col("class_display") == "Inpatient")
                                   & (F.col("period_start") > F.coalesce("idx_end", "idx_t"))),
                          "readmission", "inpatient_after_index", "period_start", "spine_encounter", "o_enc",
                          "class_display", F.lit(False)))
    parts.append(_outcome(ej.where(F.col("class_display") == "Emergency"),
                          "ed_attendance", "ed_after_index", "period_start", "spine_encounter", "o_enc",
                          "class_display", F.lit(False)))

    cc = gold("clinical_critical_care_period").select("person_id", F.col("encounter_id").alias("o_enc"),
                                                      F.col("event_datetime").alias("t"), "period_end_datetime")
    parts.append(_outcome(e_dated.join(cc, "person_id"), "icu_admission", "critical_care_period", "t",
                          "clinical_critical_care_period", "o_enc", "period_end_datetime", same_enc("o_enc")))

    d = gold("spine_person").where("deceased_ind AND deceased_datetime IS NOT NULL").select("person_id", "deceased_datetime")
    dj = e_dated.join(d, "person_id").withColumn("pid", F.col("person_id"))
    parts.append(_outcome(dj, "death", "spine_death", "deceased_datetime", "spine_person", "pid", "deceased_datetime",
                          F.lit(False)))

    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    serial = F.col("outcome_type") == "safety_incident_device_serial"
    out = on_or_before_as_of(out.where(serial | kept), F.to_date("event_datetime"), keep_null=True)  # INCR_FIX_20261018
    ix = spark.read.table("pms_device_exposure").select("exposure_id", "index_date")
    counted = ((F.col("timing") == "after") & F.col("attribution").isin(COUNTED_ATTRIBUTION)
               & (F.coalesce(F.col("candidate_count"), F.lit(1)) <= 1))
    return (out.join(ix, "exposure_id")
               .withColumn("days_from_index", F.datediff(F.to_date("event_datetime"), F.col("index_date")))
               .withColumn("counted_ind", F.coalesce(counted, F.lit(False)))
               .drop("index_date").dropDuplicates(["exposure_id", "outcome_type", "source_key", "event_datetime"])
               .withColumn("rules_version", F.lit(RULES_VERSION)))


@dp.materialized_view(
    name="pms_patient_evidence",
    comment="Patient-level device evidence that no exposure could claim: OPCS revisions after a person's first dated implant with no site family or no compatible prior exposure. Kept so unattributed revisions are never lost.",
    cluster_by=["person_id"],
)
def pms_patient_evidence():
    first = _exposures().where("idx_t IS NOT NULL").groupBy("person_id").agg(F.min("idx_t").alias("first_idx_t"))
    claimed = _revision_links().select("rv_key").distinct()
    rv = _revisions().join(first, "person_id").where(F.col("t") > F.col("first_idx_t")).join(claimed, "rv_key", "left_anti")
    return on_or_before_as_of(rv, F.to_date("t")).select(  # INCR_FIX_20261018
        "person_id", F.lit("revision_procedure").alias("evidence_type"), F.lit("opcs_revision_site_side").alias("rule_id"),
        F.col("t").alias("event_datetime"), F.col("rv_key").alias("source_key"), F.col("code").alias("procedure_code"),
        F.col("disp").alias("procedure_display"), F.col("fam").alias("site_family"), F.col("rv_side").alias("laterality_snomed_code"),
        F.when(F.col("fam").isNull(), F.lit("no_site_family")).otherwise(F.lit("no_matching_prior_exposure")).alias("unattributed_reason"),
        F.lit(RULES_VERSION).alias("rules_version"))

# COMMAND ----------

@dp.materialized_view(name="_pms_last_contact", private=True)
def _pms_last_contact():
    """Per cohort person: the last completed Barts contact on or before data_as_of (INCR_FIX_20261018)."""
    # Completed contacts only (same NOT_COMPLETED list as the outcome rules), capped at the data-as-of date.
    contact = F.to_date(F.coalesce("discharge_datetime", "period_end", "period_start"))
    cohort = spark.read.table("pms_device_exposure").select("person_id").distinct()
    encounters = (gold("spine_encounter").where(~F.coalesce(F.col("status_display"), F.lit("")).isin(NOT_COMPLETED))
                  .join(cohort, "person_id", "left_semi")
                  .select("person_id", contact.alias("_contact")))
    return (on_or_before_as_of(encounters, F.col("_contact"))
            .groupBy("person_id").agg(F.max("_contact").alias("last_contact_date")))


@dp.materialized_view(
    name="pms_followup",
    comment="Per exposure: last completed Barts contact, death, follow-up end and reason, all capped at data_as_of. Barts-only follow-up; care elsewhere is invisible.",
    cluster_by=["exposure_id"],
)
def pms_followup():
    # INCR_FIX_20261018: the data-as-of date comes from _pms_as_of (an equi-join on a constant key), not a
    # literal; the last-contact aggregation over spine_encounter is its own incremental model.
    DATA_AS_OF = F.col("_pms_as_of")  # noqa: N806 - shadows the module literal on purpose
    as_of = spark.read.table("_pms_as_of")
    e = (spark.read.table("pms_device_exposure").select("exposure_id", "person_id", "index_date")
         .withColumn("_pms_as_of_key", F.lit(True)).join(as_of, "_pms_as_of_key"))
    last = spark.read.table("_pms_last_contact")
    d = gold("spine_person").select("person_id", "deceased_ind", "deceased_datetime")
    j = (e.join(last, "person_id", "left").join(d, "person_id", "left")
         .withColumn("death_date", F.when(F.col("deceased_ind") & (F.to_date("deceased_datetime") <= DATA_AS_OF),
                                          F.to_date("deceased_datetime"))))
    end = F.coalesce(F.col("death_date"), F.least(F.greatest("last_contact_date", "index_date"), DATA_AS_OF))
    return j.select(
        "exposure_id", "last_contact_date", "death_date", end.alias("followup_end_date"),
        DATA_AS_OF.alias("data_as_of"),
        # Rule death_before_index: 68 implants are dated after the recorded death (contradictory record); the
        # death date is kept as recorded but gives no follow-up.
        F.when(F.col("death_date") < F.col("index_date"), F.lit("death_before_index"))
         .when(F.col("death_date").isNotNull(), F.lit("death"))
         .when(F.col("last_contact_date").isNotNull(), F.lit("last_contact"))
         .when(F.col("index_date") > DATA_AS_OF, F.lit("data_as_of"))
         .otherwise(F.lit("index_only")).alias("censor_reason"),
        # Implants after data_as_of (the current partial day) have no follow-up yet: NULL, not negative.
        F.when((F.col("index_date") <= DATA_AS_OF) & ~F.coalesce(F.col("death_date") < F.col("index_date"), F.lit(False)),
               days_between(end, F.col("index_date"))).alias("followup_days"),
        F.lit(RULES_VERSION).alias("rules_version"))


# COMMAND ----------

# iWeb cardiac registries (gold clinical_registry_*): device detail and the registry's own device-failure field.
# rule id "iweb_device_failure": registry-recorded failure of the device used in that procedure (procedural,
# not a post-index outcome), so it is an exposure attribute in pms_dataset, not an outcome row.
REGISTRY_FAILURE = ["Probably iatrogenic", "Probably intrinsic"]
REG_COLS = ["registry_key", "registry", "person_id", "procedure_datetime", "site_family", "device_text",
            "stents_used", "drug_eluting_stents_used", "drugs_eluted", "valve_implant_name", "valve_model_number",
            "valve_serial_number", "valve_size", "valve_implant_type", "valve_explant_type", "registry_device_failure_raw"]


def _reg(df, registry, key, ts, family, **cols):
    base = {c: F.lit(None).cast("string") for c in REG_COLS}
    base.update(registry_key=F.concat(F.lit(registry + ":"), F.col(key).cast("string")), registry=F.lit(registry),
                person_id=F.col("person_id"), procedure_datetime=(F.col(ts) if isinstance(ts, str) else ts).cast("timestamp"),
                site_family=F.lit(family).cast("string"))
    base.update(cols)
    return df.select(*[base[c].cast("string").alias(c) if c not in ("person_id", "procedure_datetime") else base[c].alias(c)
                       for c in REG_COLS])


@dp.materialized_view(
    name="pms_registry_device_procedure",
    comment="iWeb registry procedures (coronary PCI, non-coronary, surgical valves) with device detail and registry device-failure field, linked to exposures of the same person on the same day.",
    cluster_by=["person_id"],
)
def pms_registry_device_procedure():
    cor = gold("clinical_registry_coronary_procedure")
    non = gold("clinical_registry_noncoronary_procedure")
    ep = gold("clinical_registry_surgery_episode").select(F.col("entry_id").alias("ep_id"), "date_and_time_of_operation", "date_of_operation")
    sp = gold("clinical_registry_surgery_procedure").join(ep, F.col("parent_entry_id") == F.col("ep_id"), "left")
    r = (_reg(cor, "iweb_coronary", "entry_id", "pre_dateandtimeofoperation_clean", "coronary_stent",
              device_text=F.col("post_m_details_of_all_devices_implanted"), stents_used=F.col("post_number_stents_used"),
              drug_eluting_stents_used=F.col("post_number_of_drug_eluting_stents_used"),
              drugs_eluted=F.col("post_drugs_eluted_by_stentstxt"), registry_device_failure_raw=F.col("post_device_failure"))
         .unionByName(_reg(non, "iweb_noncoronary", "entry_id", "dateofprocedure", None,
                           device_text=F.col("m_details_of_all_devices_implanted"),
                           registry_device_failure_raw=F.col("device_failure")))
         .unionByName(_reg(sp, "iweb_surgery_valve", "entry_id", F.coalesce("date_and_time_of_operation", "date_of_operation"),
                           "heart_valve", valve_implant_name=F.col("valve_implant_name"),
                           valve_model_number=F.col("valve_or_ring_model_number"),
                           valve_serial_number=F.col("valve_or_ring_serial_number"), valve_size=F.col("valve_or_ring_size"),
                           valve_implant_type=F.col("valve_implant_type"), valve_explant_type=F.col("valve_explant_type"))
                      .where("valve_implant_name IS NOT NULL OR valve_model_number IS NOT NULL OR valve_explant_type IS NOT NULL")))
    r = r.withColumn("registry_device_failure",
                     F.when(F.col("registry_device_failure_raw").isin(REGISTRY_FAILURE), F.col("registry_device_failure_raw")))
    # Link: same person, same day; the site family must match when both sides have one.
    e = spark.read.table("pms_device_exposure").where("index_date IS NOT NULL").select(
        F.col("person_id").alias("e_person"), "exposure_id", "index_date", "site_families")
    fam_ok = F.col("site_family").isNull() | (F.size("site_families") == 0) | F.array_contains("site_families", F.col("site_family"))
    lk = (r.select("registry_key", "person_id", "procedure_datetime", "site_family")
           .join(e, (F.col("person_id") == F.col("e_person")) & (F.to_date("procedure_datetime") == F.col("index_date")))
           .where(fam_ok).groupBy("registry_key")
           .agg(F.array_sort(F.collect_set(F.col("exposure_id").cast("string"))).alias("linked_exposure_ids")))
    return (r.join(lk, "registry_key", "left")
             .withColumn("linked_exposure_ids", F.coalesce("linked_exposure_ids", F.array().cast("array<string>")))
             .withColumn("link_method", F.when(F.size("linked_exposure_ids") > 0, F.lit("person_same_day_site")).otherwise(F.lit("none")))
             .withColumn("rules_version", F.lit(RULES_VERSION)))


# COMMAND ----------

OUTCOME_TYPES = ["infection", "mechanical_complication", "other_device_complication", "periprosthetic_fracture",
                 "microbiology_isolate", "revision_procedure", "repeat_same_gmdn_implant", "safety_incident_person",
                 "safety_incident_device_serial", "readmission", "ed_attendance", "icu_admission", "death"]
HORIZONS = [30, 90, 365]
PATIENT_COLS = ["sex", "birth_date", "age_at_first_index", "ethnicity_display", "ethnicity_snomed_code", "lsoa",
                "imd_decile", "imd_quintile", "imd_method", "nhs_number"]
# Datix person linkage is ~6% of patient incidents today (26k of 430k PATIE1/PATIE2, 2026-09-29): the live
# datix_pipeline resolves persons from a 10k-row sample. Below this rate an absent incident means nothing, so the
# safety_incident_person columns are NULL, not false. The pharma PMS T1 fix raises the rate (see Prerequisites).
DATIX_LINK_MIN_RATE = 0.8


def _datix_link_rate():
    return (gold("clinical_safety_incident").where(F.upper(F.trim("inc_type")).isin("PATIE1", "PATIE2"))
            .agg((F.count("person_id") / F.count("*")).alias("datix_person_link_rate")))


@dp.materialized_view(
    name="pms_dataset",
    comment="Analysis table: one row per implanted device with identity, procedure context, patient, baseline, follow-up, first-event days and 30/90/365-day outcome flags from counted outcome rows only (NULL when follow-up is shorter than the horizon and no event occurred).",
    cluster_by=["gmdn_code", "person_id"],
)
def pms_dataset():
    ex = spark.read.table("pms_device_exposure").drop("rules_version")
    pc = spark.read.table("pms_procedure_context").drop("rules_version")
    pt = spark.read.table("pms_patient").select("person_id", *PATIENT_COLS)
    bl = spark.read.table("pms_baseline").drop("rules_version")
    fu = spark.read.table("pms_followup").drop("rules_version")
    ev = spark.read.table("pms_outcome_event")
    oe = ev.where("counted_ind")  # post-index, accepted attribution, unambiguous
    first = oe.groupBy("exposure_id").pivot("outcome_type", OUTCOME_TYPES).agg(F.min("days_from_index"))
    first = first.select("exposure_id", *[F.col(t).alias(f"first_{t}_days") for t in OUTCOME_TYPES])
    ever = oe.groupBy("exposure_id").pivot("outcome_type", OUTCOME_TYPES).agg(F.count(F.lit(1)))  # count("*") is invalid in pivot
    ever = ever.select("exposure_id", *[F.col(t).alias(f"n_{t}") for t in OUTCOME_TYPES])
    # Pre-implant serial reports and same-day date-only events: evidence, reported beside the outcomes, never in them.
    pre = (ev.where((F.col("outcome_type") == "safety_incident_device_serial") & (F.col("timing") == "before")
                    & F.col("attribution").isin(COUNTED_ATTRIBUTION))
             .groupBy("exposure_id").agg(F.count("*").alias("n_pre_index_device_serial_incidents"),
                                         F.min("days_from_index").alias("first_pre_index_device_serial_days")))
    sd = ev.where(F.col("timing") == "same_day_date_only").groupBy("exposure_id").agg(
        F.count("*").alias("n_same_day_date_only_events"))
    pe = spark.read.table("pms_patient_evidence").select(F.col("person_id").alias("pe_person"), F.col("event_datetime").alias("pe_t"))
    upe = (ex.select("exposure_id", "person_id", "index_datetime", "index_date").where("index_date IS NOT NULL")
             .join(pe, (F.col("person_id") == F.col("pe_person")) & (F.col("pe_t") > F.col("index_datetime")))
             .groupBy("exposure_id").agg(F.count("*").alias("n_patient_revisions_unattributed_after_index")))
    reg = (spark.read.table("pms_registry_device_procedure")
           .select(F.explode("linked_exposure_ids").alias("exposure_id"), "registry_key", "registry_device_failure")
           .groupBy("exposure_id").agg(F.array_sort(F.collect_set("registry_key")).alias("registry_keys"),
                                       F.array_sort(F.collect_set("registry_device_failure")).alias("registry_device_failure")))
    j = (ex.join(pc, "exposure_id", "left").join(pt, "person_id", "left").join(bl, "exposure_id", "left")
           .join(fu, "exposure_id", "left").join(first, "exposure_id", "left").join(ever, "exposure_id", "left")
           .join(pre, "exposure_id", "left").join(sd, "exposure_id", "left").join(upe, "exposure_id", "left")
           .join(reg, "exposure_id", "left")
           # INCR_FIX_20261018: equi-join on a constant key, not crossJoin, so pms_dataset can refresh incrementally.
           .withColumn("_pms_rate_key", F.lit(True))
           .join(_datix_link_rate().withColumn("_pms_rate_key", F.lit(True)), "_pms_rate_key")
           .drop("_pms_rate_key"))
    datix_ok = F.col("datix_person_link_rate") >= DATIX_LINK_MIN_RATE
    gate = lambda t, c: F.when(datix_ok, c) if t == "safety_incident_person" else c
    flags = []
    for t in OUTCOME_TYPES:
        for h in HORIZONS:
            fd = F.col(f"first_{t}_days")
            flags.append(gate(t, F.when(fd.isNotNull() & (fd >= 0) & (fd <= h), F.lit(True))
                                  .when(F.col("followup_days") >= h, F.lit(False))).alias(f"{t}_{h}d"))
    counts = [gate(t, F.coalesce(F.col(f"n_{t}"), F.lit(0))).alias(f"n_{t}") for t in OUTCOME_TYPES]
    firsts = [gate(t, F.col(f"first_{t}_days")).alias(f"first_{t}_days") for t in OUTCOME_TYPES]
    extra = [F.coalesce(F.col(c), F.lit(0)).alias(c) for c in
             ("n_pre_index_device_serial_incidents", "n_same_day_date_only_events",
              "n_patient_revisions_unattributed_after_index")]
    skip = ({f"n_{t}" for t in OUTCOME_TYPES} | {f"first_{t}_days" for t in OUTCOME_TYPES}
            | {"n_pre_index_device_serial_incidents", "n_same_day_date_only_events",
               "n_patient_revisions_unattributed_after_index"})
    return j.select(*[c for c in j.columns if c not in skip], *firsts, *counts, *extra, *flags,
                    F.lit(RULES_VERSION).alias("rules_version"))

