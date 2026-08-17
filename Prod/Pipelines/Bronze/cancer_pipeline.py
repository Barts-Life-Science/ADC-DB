# Databricks notebook source
# MAGIC %run ./_bronze_common

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DecimalType,
    DoubleType, BooleanType, DateType, TimestampType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

for _w, _d in {
    "target_schema": "8_dev.bronze",
    "allow_production_write": "false",
    "enable_drug_term_mapping": "true",
    "enable_opcs_mapping": "true",
}.items():
    try:
        dbutils.widgets.text(_w, _d)
    except Exception:
        pass

TARGET_SCHEMA = bronze_value("target_schema", "8_dev.bronze")
ALLOW_PROD_WRITE = bronze_bool("allow_production_write", False)
ENABLE_DRUG_MAP = bronze_bool("enable_drug_term_mapping", True)
ENABLE_OPCS_MAP = bronze_bool("enable_opcs_mapping", True)
FORCE_FULL_REFRESH = bronze_bool("force_full_refresh", False)
CANCER_RUN_ID = bronze_run_id()
_STARTED_AT = bronze_utc_now()

assert TARGET_SCHEMA.startswith("8_dev.") or ALLOW_PROD_WRITE, (
    f"Refusing non-dev target {TARGET_SCHEMA} without allow_production_write=true"
)

RAW = "4_prod.raw"
SRC_ARIA_PTKEY = f"{RAW}.aria_pt_inst_key"
SRC_ARIA_RX = f"{RAW}.aria_rx"
SRC_ARIA_AGT = f"{RAW}.aria_agt_rx"
SRC_IQ_COURSE = f"{RAW}.iqemo_chemotherapy_course"
SRC_IQ_REGIMEN = f"{RAW}.iqemo_regimen"
SRC_IQ_PATIENT = f"{RAW}.iqemo_patient"
PERSON_ALIAS = f"{RAW}.mill_person_alias"
OMOP_CONCEPT = "3_lookup.omop.concept"
OMOP_SYNONYM = "3_lookup.omop.concept_synonym"
OMOP_REL = "3_lookup.omop.concept_relationship"

T_TREATMENT = f"{TARGET_SCHEMA}.map_cancer_treatment"
T_TERM_MAP = f"{bronze_lookup_schema(TARGET_SCHEMA)}.cancer_treatment_term_map"
T_STATE = f"{bronze_control_schema(TARGET_SCHEMA)}.cancer_pipeline_state"
T_AUDIT = f"{bronze_control_schema(TARGET_SCHEMA)}.cancer_pipeline_audit"

ARIA_MRN_KEY_CD, ARIA_NHS_KEY_CD = 2, 24
MRN_ALIAS_TYPE, NHS_ALIAS_TYPE = 10, 18
IQEMO_MRN_POOLS = [683996, 1115132483, 6200990, 6173940]
NULL_TOKEN = "#NULL#"

_missing = [
    t for t in [
        SRC_ARIA_PTKEY, SRC_ARIA_RX, SRC_ARIA_AGT, SRC_IQ_COURSE,
        SRC_IQ_REGIMEN, SRC_IQ_PATIENT, PERSON_ALIAS,
        OMOP_CONCEPT, OMOP_SYNONYM, OMOP_REL,
    ]
    if not bronze_table_exists(t)
]
assert not _missing, f"Missing sources: {_missing}"

# COMMAND ----------

def _assert_columns(table, cols):
    have = {c.lower() for c in spark.table(table).columns}
    miss = [c for c in cols if c.lower() not in have]
    assert not miss, f"{table} missing columns: {miss}"

_assert_columns(SRC_ARIA_PTKEY, ["pt_id", "pt_key_cd", "pt_key_value", "ADC_UPDT"])
_assert_columns(SRC_ARIA_RX, ["pt_id", "pt_visit_id", "rx_id", "ADC_UPDT"])
_assert_columns(SRC_ARIA_AGT, [
    "pt_id", "pt_visit_id", "rx_id", "item_no", "agt_name", "tp_name",
    "dosage_form", "dose_level", "admn_dosage_unit", "admn_route",
    "rx_dose", "rx_total", "admn_start_date", "ADC_UPDT",
])
_assert_columns(SRC_IQ_COURSE, [
    "ChemotherapyCourseID", "PatientID", "RegimenID", "StartDate", "EndDate",
    "FinalTreatmentDate", "CourseFinished", "PlannedCycles", "CycleCancelledFrom",
    "ADC_UPDT",
])
_assert_columns(SRC_IQ_REGIMEN, [
    "RegimenID", "Name", "SactName", "DefaultCycles", "ChemoRadiation",
    "OPCSProcurementCode", "OPCSDeliveryCode", "Indication", "ADC_UPDT",
])
_assert_columns(SRC_IQ_PATIENT, ["PatientID", "PrimaryIdentifier", "NHSNumber", "ADC_UPDT"])
_assert_columns(PERSON_ALIAS, [
    "PERSON_ID", "ALIAS", "PERSON_ALIAS_TYPE_CD", "ALIAS_POOL_CD", "ACTIVE_IND",
])


def _dup_groups(table, keys):
    return (
        spark.table(table)
        .groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
    )

assert _dup_groups(SRC_ARIA_RX, ["pt_id", "pt_visit_id", "rx_id"]) == 0, \
    "aria_rx grain (pt_id, pt_visit_id, rx_id) violated — the ARIA arm's left " \
    "join to the header would fan out agent rows"
assert _dup_groups(SRC_ARIA_AGT, ["pt_id", "pt_visit_id", "rx_id", "item_no"]) == 0, \
    "aria_agt_rx grain (pt_id, pt_visit_id, rx_id, item_no) violated"
assert _dup_groups(SRC_IQ_COURSE, ["ChemotherapyCourseID"]) == 0, \
    "iqemo_chemotherapy_course grain violated"
assert _dup_groups(SRC_IQ_REGIMEN, ["RegimenID"]) == 0, "iqemo_regimen grain violated"
assert _dup_groups(SRC_IQ_PATIENT, ["PatientID"]) == 0, "iqemo_patient grain violated"

# pt_key_cd semantics re-check (2=MRN, 24=NHS): NHS lane must be >90% 10-digit
_nhs_shape = (
    spark.table(SRC_ARIA_PTKEY)
    .filter(F.col("pt_key_cd") == ARIA_NHS_KEY_CD)
    .select(
        F.avg(
            F.when(
                F.regexp_replace(F.col("pt_key_value"), " ", "").rlike("^[0-9]{10}$"), 1.0
            ).otherwise(0.0)
        ).alias("frac")
    )
    .first()["frac"]
)
assert _nhs_shape is not None and _nhs_shape > 0.9, (
    f"pt_key_cd={ARIA_NHS_KEY_CD} no longer looks like NHS numbers ({_nhs_shape}); "
    "confirm key-code semantics before proceeding"
)

# Freshness report — informational only, stale data is NOT rejected
_freshness = {
    t: str(spark.table(t).select(F.max("ADC_UPDT")).first()[0])
    for t in [SRC_ARIA_AGT, SRC_IQ_COURSE, SRC_IQ_REGIMEN]
}
print("[PREFLIGHT] source freshness:", bronze_json(_freshness))

# COMMAND ----------

STATE_SCHEMA = StructType([
    StructField("target_table", StringType()),
    StructField("fingerprint", StringType()),
    StructField("run_id", StringType()),
    StructField("updated_at", TimestampType()),
])
AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("target_table", StringType()),
    StructField("event_type", StringType()),
    StructField("details", StringType()),
])

for _t, _s in [(T_STATE, STATE_SCHEMA), (T_AUDIT, AUDIT_SCHEMA)]:
    DeltaTable.createIfNotExists(spark).tableName(_t).addColumns(_s).execute()


def audit(target, event, details=None):
    spark.createDataFrame(
        [(CANCER_RUN_ID, target, event, bronze_json(details or {}))],
        ["run_id", "target_table", "event_type", "details"],
    ).withColumn("event_ts", F.current_timestamp()) \
     .select("run_id", "event_ts", "target_table", "event_type", "details") \
     .write.mode("append").saveAsTable(T_AUDIT)


def source_fingerprint(tables):
    return bronze_json({
        t: DeltaTable.forName(spark, t).history(1).collect()[0]["version"]
        for t in sorted(tables)
    })


def stored_fingerprint(target):
    rows = (
        spark.table(T_STATE).filter(F.col("target_table") == target)
        .orderBy(F.col("updated_at").desc()).limit(1).collect()
    )
    return rows[0]["fingerprint"] if rows else None


def save_fingerprint(target, fp):
    spark.createDataFrame(
        [(target, fp, CANCER_RUN_ID)], ["target_table", "fingerprint", "run_id"]
    ).withColumn("updated_at", F.current_timestamp()) \
     .select("target_table", "fingerprint", "run_id", "updated_at") \
     .write.mode("append").saveAsTable(T_STATE)


def _hash_over(cols):
    return F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit(NULL_TOKEN)) for c in cols],
        ),
        256,
    )


def align_to_schema(df, schema):
    return df.select(
        *[
            (
                F.col(f.name).cast(f.dataType)
                if f.name in df.columns
                else F.lit(None).cast(f.dataType)
            ).alias(f.name, metadata=f.metadata)
            for f in schema.fields
        ]
    )


def assert_target_schema_compatible(target, schema):
    if not bronze_table_exists(target):
        return
    actual = {
        field.name.lower(): field.dataType.simpleString()
        for field in spark.table(target).schema.fields
    }
    expected = {
        field.name.lower(): field.dataType.simpleString()
        for field in schema.fields
    }
    missing = sorted(name for name in expected if name not in actual)
    mismatched = {
        name: {"expected": expected[name], "actual": actual[name]}
        for name in expected
        if name in actual and actual[name] != expected[name]
    }
    assert not missing and not mismatched, (
        f"{target}: existing schema is incompatible with this pipeline; "
        f"missing={missing}, mismatched={mismatched}. Preserve/archive the "
        "legacy table and recreate the target before Run All. The pipeline "
        "will not mutate an incompatible shared table automatically."
    )


def publish(build_fn, target, schema, comment, key_cols, sources,
            key_name="ROW_KEY", protect_condition=None, enabled=True):
    if not enabled:
        print(f"[SKIP] {target}: disabled by widget")
        audit(target, "SKIPPED_DISABLED")
        return {"target": target, "status": "SKIPPED_DISABLED"}

    assert_target_schema_compatible(target, schema)
    fp = source_fingerprint(sources)
    if (not FORCE_FULL_REFRESH and bronze_table_exists(target)
            and stored_fingerprint(target) == fp):
        print(f"[SKIP] {target}: sources unchanged")
        audit(target, "SKIPPED_UNCHANGED", {"fingerprint": fp})
        return {"target": target, "status": "SKIPPED_UNCHANGED"}

    harness = [key_name, "SOURCE_PRESENT_IND", "ROW_HASH", "ADC_UPDT"]
    payload = [f.name for f in schema.fields if f.name not in harness]

    df = (
        build_fn()
        .withColumn(key_name, _hash_over(key_cols))
        .withColumn("ROW_HASH", _hash_over(payload))
        .withColumn("SOURCE_PRESENT_IND", F.lit(1))
        .withColumn("ADC_UPDT", F.current_timestamp())
    )
    df = align_to_schema(df, schema)

    dup = df.groupBy(key_name).count().filter(F.col("count") > 1).limit(1).count()
    assert dup == 0, (
        f"{target}: {key_name} not unique over {key_cols} — grain assumption "
        "broken; STOP, do not dedupe"
    )

    (
        DeltaTable.createIfNotExists(spark)
        .tableName(target)
        .addColumns(schema)
        .property("delta.enableChangeDataFeed", "true")
        .property("delta.enableRowTracking", "true")
        .comment(comment)
        .execute()
    )
    if FORCE_FULL_REFRESH:
        spark.sql(f"TRUNCATE TABLE {target}")

    protect = f" AND NOT ({protect_condition})" if protect_condition else ""
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(df.alias("s"), f"t.{key_name} = s.{key_name}")
        .whenMatchedUpdateAll(
            condition=f"(t.ROW_HASH <> s.ROW_HASH OR t.SOURCE_PRESENT_IND = 0){protect}"
        )
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceUpdate(
            condition=f"t.SOURCE_PRESENT_IND = 1{protect}",
            set={"SOURCE_PRESENT_IND": "0", "ADC_UPDT": "current_timestamp()"},
        )
        .execute()
    )
    save_fingerprint(target, fp)
    audit(target, "PUBLISHED", {"fingerprint": fp})
    return {"target": target, "status": "PUBLISHED"}

# COMMAND ----------

SALT_WORDS = (
    "hydrochloride|sulphate|sulfate|phosphate|acetate|succinate|sodium|disodium"
)

def normalise_drug_text(col):
    text = F.lower(col.cast("string"))
    text = F.regexp_replace(text, r"[^\w\s+/]", " ")
    text = F.regexp_replace(text, r"\b(" + SALT_WORDS + r")\b", " ")
    text = F.regexp_replace(text, r"\s+", " ")
    return F.trim(text)


def split_drugs(col):
    arr = F.split(normalise_drug_text(col), r"[+/]")
    arr = F.transform(arr, lambda d: F.trim(d))
    return F.array_distinct(F.filter(arr, lambda d: d != ""))


def explode_drugs(df: DataFrame, raw_col: str) -> DataFrame:
    return df.withColumn("drug", F.explode_outer(split_drugs(F.col(raw_col))))

# COMMAND ----------

def _active_aliases():
    return (
        spark.table(PERSON_ALIAS)
        .filter(F.col("ACTIVE_IND") == 1)
        .select(
            F.col("PERSON_ID").cast("long").alias("alias_person_id"),
            F.col("ALIAS").alias("alias_value"),
            F.col("PERSON_ALIAS_TYPE_CD").cast("int").alias("alias_type"),
            F.col("ALIAS_POOL_CD").cast("long").alias("alias_pool"),
        )
    )


def _resolve_link(ids_df):
    """ids_df: source_patient_id, id_kind ('MRN'|'NHS'), id_value, pool_restricted.
    Returns one row per source patient: PERSON_ID, link_status, NHS_Number, MRN."""
    al = _active_aliases()
    matched = ids_df.join(
        al,
        (F.col("id_value") == F.col("alias_value"))
        & (
            (
                (F.col("id_kind") == "MRN")
                & (F.col("alias_type") == MRN_ALIAS_TYPE)
                & ((~F.col("pool_restricted")) | F.col("alias_pool").isin(IQEMO_MRN_POOLS))
            )
            | ((F.col("id_kind") == "NHS") & (F.col("alias_type") == NHS_ALIAS_TYPE))
        ),
        "left",
    )
    per_patient = (
        matched
        .groupBy("source_patient_id")
        .agg(
            F.min(F.when(F.col("id_kind") == "MRN", F.col("id_value"))).alias("MRN"),
            F.min(F.when(F.col("id_kind") == "NHS", F.col("id_value"))).alias("NHS_Number"),
            F.countDistinct(
                F.when(F.col("id_kind") == "MRN", F.col("alias_person_id"))
            ).alias("mrn_n"),
            F.countDistinct(
                F.when(F.col("id_kind") == "NHS", F.col("alias_person_id"))
            ).alias("nhs_n"),
            F.min(
                F.when(F.col("id_kind") == "MRN", F.col("alias_person_id"))
            ).alias("mrn_p"),
            F.min(
                F.when(F.col("id_kind") == "NHS", F.col("alias_person_id"))
            ).alias("nhs_p"),
        )
        .withColumn(
            "link_status",
            F.when((F.col("mrn_n") > 1) | (F.col("nhs_n") > 1), "AMBIGUOUS")
            .when(
                (F.col("mrn_n") == 1) & (F.col("nhs_n") == 1)
                & (F.col("mrn_p") == F.col("nhs_p")),
                "LINKED_BOTH",
            )
            .when((F.col("mrn_n") == 1) & (F.col("nhs_n") == 1), "CONFLICT")
            .when(F.col("mrn_n") == 1, "LINKED_MRN")
            .when(F.col("nhs_n") == 1, "LINKED_NHS")
            .otherwise("UNLINKED"),
        )
        .withColumn(
            "PERSON_ID",
            F.when(
                F.col("link_status").isin("LINKED_BOTH", "LINKED_MRN"), F.col("mrn_p")
            ).when(F.col("link_status") == "LINKED_NHS", F.col("nhs_p")),
        )
    )
    return per_patient.select(
        "source_patient_id", "PERSON_ID", "link_status", "NHS_Number", "MRN"
    )


def aria_patient_link() -> DataFrame:
    return _resolve_link(
        spark.table(SRC_ARIA_PTKEY)
        .filter(F.col("pt_key_cd").isin(ARIA_MRN_KEY_CD, ARIA_NHS_KEY_CD))
        .select(
            F.col("pt_id").cast("string").alias("source_patient_id"),
            F.when(F.col("pt_key_cd") == ARIA_MRN_KEY_CD, "MRN")
            .otherwise("NHS").alias("id_kind"),
            F.regexp_replace(F.col("pt_key_value"), " ", "").alias("id_value"),
            F.lit(False).alias("pool_restricted"),
        )
    )


def iqemo_patient_link() -> DataFrame:
    iq = spark.table(SRC_IQ_PATIENT)
    ids = (
        iq.select(
            F.col("PatientID").cast("string").alias("source_patient_id"),
            F.lit("MRN").alias("id_kind"),
            F.trim(F.col("PrimaryIdentifier")).alias("id_value"),
            F.lit(True).alias("pool_restricted"),
        )
        .unionByName(
            iq.select(
                F.col("PatientID").cast("string").alias("source_patient_id"),
                F.lit("NHS").alias("id_kind"),
                F.regexp_replace(F.col("NHSNumber"), " ", "").alias("id_value"),
                F.lit(False).alias("pool_restricted"),
            )
        )
    )
    return _resolve_link(ids)

# COMMAND ----------

TERM_MAP_COMMENT = (
    "Distinct normalised systemic-therapy drug tokens from ARIA agt_name and "
    "iQemo SactName (lowercased, salts stripped, combinations split) with "
    "deterministic OMOP mapping: exact name/synonym match against RxNorm/"
    "RxNorm Extension/dm+d, resolved to standard ingredient-level concepts via "
    "'Maps to'; ambiguous -> NULL. Rows with mapping_status='OVERRIDE' are "
    "curated by hand and never updated by the pipeline."
)

SCHEMA_TERM_MAP = StructType([
    StructField("ROW_KEY", StringType(), True, metadata={"comment": "sha2(drug_token). MERGE key."}),
    StructField("drug_token", StringType(), True, metadata={"comment": "Normalised drug token (join key from map_cancer_treatment.drug)."}),
    StructField("drug_concept_id", LongType(), True, metadata={"comment": "OMOP standard drug concept (ingredient-level preferred); NULL when unmapped/ambiguous."}),
    StructField("drug_concept_code", StringType(), True, metadata={"comment": "Concept code in its source vocabulary."}),
    StructField("drug_concept_name", StringType(), True, metadata={"comment": "Concept name."}),
    StructField("mapping_status", StringType(), True, metadata={"comment": "EXACT_NAME / EXACT_SYNONYM / OVERRIDE / AMBIGUOUS / UNMAPPED."}),
    StructField("sources", StringType(), True, metadata={"comment": "Comma-joined source systems contributing this token (ARIA, IQEMO)."}),
    StructField("row_count", LongType(), True, metadata={"comment": "Source rows carrying this token (informational)."}),
    StructField("SOURCE_PRESENT_IND", IntegerType(), True, metadata={"comment": "1 = token present in latest recompute; 0 = soft-deleted. OVERRIDE rows always 1."}),
    StructField("ROW_HASH", StringType(), True, metadata={"comment": "sha2 over payload."}),
    StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": "Pipeline processing timestamp."}),
])

# COMMAND ----------

DRUG_VOCABS = ["RxNorm", "RxNorm Extension", "dm+d"]

def build_term_map() -> DataFrame:
    aria_tokens = (
        explode_drugs(spark.table(SRC_ARIA_AGT).select("agt_name"), "agt_name")
        .filter(F.col("drug").isNotNull())
        .groupBy(F.col("drug").alias("drug_token"))
        .agg(F.count("*").alias("row_count"))
        .withColumn("src", F.lit("ARIA"))
    )
    iq_tokens = (
        explode_drugs(spark.table(SRC_IQ_REGIMEN).select("SactName"), "SactName")
        .filter(F.col("drug").isNotNull())
        .groupBy(F.col("drug").alias("drug_token"))
        .agg(F.count("*").alias("row_count"))
        .withColumn("src", F.lit("IQEMO"))
    )
    tokens = (
        aria_tokens.unionByName(iq_tokens)
        .groupBy("drug_token")
        .agg(
            F.sum("row_count").alias("row_count"),
            F.concat_ws(",", F.sort_array(F.collect_set("src"))).alias("sources"),
        )
    )

    concept = spark.table(OMOP_CONCEPT)
    drug_concepts = concept.filter(
        (F.col("domain_id") == "Drug")
        & F.col("vocabulary_id").isin(DRUG_VOCABS)
        & F.col("invalid_reason").isNull()
    )
    names = drug_concepts.select(
        "concept_id", F.lower(F.trim(F.col("concept_name"))).alias("name"),
        F.lit(1).alias("tier"),
    )
    syns = (
        spark.table(OMOP_SYNONYM)
        .join(drug_concepts.select("concept_id"), "concept_id")
        .select(
            "concept_id",
            F.lower(F.trim(F.col("concept_synonym_name"))).alias("name"),
            F.lit(2).alias("tier"),
        )
    )
    pool = names.unionByName(syns).distinct()

    std = concept.filter(
        (F.col("standard_concept") == "S") & F.col("invalid_reason").isNull()
    )
    maps_to = spark.table(OMOP_REL).filter(
        F.col("relationship_id") == "Maps to"
    ).select("concept_id_1", "concept_id_2")

    resolved = (
        pool.alias("np")
        .join(
            concept.select("concept_id", "standard_concept").alias("c"),
            F.col("np.concept_id") == F.col("c.concept_id"),
        )
        .join(maps_to.alias("mt"), F.col("np.concept_id") == F.col("mt.concept_id_1"), "left")
        .withColumn(
            "standard_id",
            F.when(F.col("c.standard_concept") == "S", F.col("np.concept_id"))
            .otherwise(F.col("mt.concept_id_2")),
        )
        .filter(F.col("standard_id").isNotNull())
        .join(std.alias("s"), F.col("standard_id") == F.col("s.concept_id"))
        .select(
            F.col("np.name").alias("name"),
            F.col("np.tier").alias("tier"),
            F.col("s.concept_id").alias("drug_concept_id"),
            F.col("s.concept_code").alias("drug_concept_code"),
            F.col("s.concept_name").alias("drug_concept_name"),
            F.when(F.col("s.concept_class_id") == "Ingredient", 0).otherwise(10).alias("class_pref"),
            F.when(F.col("s.vocabulary_id") == "RxNorm", 0)
            .when(F.col("s.vocabulary_id") == "RxNorm Extension", 1)
            .otherwise(2).alias("vocab_pref"),
        )
        .distinct()
        .withColumn("pref", F.col("tier") * 100 + F.col("class_pref") + F.col("vocab_pref"))
    )

    cand = tokens.join(resolved, tokens["drug_token"] == resolved["name"], "left")
    w = Window.partitionBy("drug_token")
    return (
        cand
        .withColumn("min_pref", F.min("pref").over(w))
        .filter((F.col("pref").isNull()) | (F.col("pref") == F.col("min_pref")))
        .withColumn("n_at_pref", F.size(F.collect_set("drug_concept_id").over(w)))
        .withColumn(
            "mapping_status",
            F.when(F.col("drug_concept_id").isNull(), "UNMAPPED")
            .when(F.col("n_at_pref") > 1, "AMBIGUOUS")
            .when(F.col("tier") == 1, "EXACT_NAME")
            .otherwise("EXACT_SYNONYM"),
        )
        .withColumn(
            "drug_concept_id",
            F.when(
                F.col("mapping_status").isin("EXACT_NAME", "EXACT_SYNONYM"),
                F.col("drug_concept_id").cast("long"),
            ),
        )
        .withColumn(
            "drug_concept_code",
            F.when(F.col("drug_concept_id").isNotNull(), F.col("drug_concept_code")),
        )
        .withColumn(
            "drug_concept_name",
            F.when(F.col("drug_concept_id").isNotNull(), F.col("drug_concept_name")),
        )
        .select(
            "drug_token", "drug_concept_id", "drug_concept_code",
            "drug_concept_name", "mapping_status", "sources", "row_count",
        )
        .dropDuplicates(["drug_token"])
    )
    # the final dropDuplicates collapses identical rows produced by the window
    # logic for AMBIGUOUS tokens (concept columns already NULLed) — it is not
    # best-record selection

# COMMAND ----------

TERM_MAP_OUT_COLS = [
    ("drug_concept_id", "long"), ("drug_concept_code", "string"),
    ("drug_concept_name", "string"), ("drug_mapping_status", "string"),
]

def _with_term_map(df: DataFrame) -> DataFrame:
    if not ENABLE_DRUG_MAP:
        # keep downstream column contract intact when mapping is disabled
        for c, t in TERM_MAP_OUT_COLS:
            df = df.withColumn(c, F.lit(None).cast(t))
        return df
    tm = (
        spark.table(T_TERM_MAP)
        .filter(F.col("SOURCE_PRESENT_IND") == 1)
        .select(
            F.col("drug_token").alias("_tm_token"),
            "drug_concept_id", "drug_concept_code", "drug_concept_name",
            F.col("mapping_status").alias("drug_mapping_status"),
        )
    )
    return df.join(tm, df["drug"] == F.col("_tm_token"), "left").drop("_tm_token")


def build_aria_arm() -> DataFrame:
    agt = spark.table(SRC_ARIA_AGT).alias("arx")
    rx = spark.table(SRC_ARIA_RX).alias("rx")
    link = aria_patient_link().alias("lnk")

    base = (
        agt
        .join(
            rx,
            (F.col("arx.pt_id") == F.col("rx.pt_id"))
            & (F.col("arx.pt_visit_id") == F.col("rx.pt_visit_id"))
            & (F.col("arx.rx_id") == F.col("rx.rx_id")),
            "left",
        )
        .join(
            link,
            F.col("arx.pt_id").cast("string") == F.col("lnk.source_patient_id"),
            "left",
        )
        .select(
            F.col("lnk.PERSON_ID").alias("PERSON_ID"),
            F.coalesce(F.col("lnk.link_status"), F.lit("UNLINKED")).alias("link_status"),
            F.col("lnk.NHS_Number").alias("NHS_Number"),
            F.col("lnk.MRN").alias("MRN"),
            F.to_date(F.col("arx.admn_start_date")).alias("start_date"),
            F.col("arx.agt_name").alias("AriaAgentName"),
            F.col("arx.tp_name").alias("TreatmentPlan"),
            F.col("arx.dosage_form").alias("DosageForm"),
            F.col("arx.dose_level").alias("DoseLevel"),
            F.col("arx.admn_dosage_unit").alias("AdmnDosageUnit"),
            F.col("arx.admn_route").alias("AdmnRoute"),
            F.col("arx.rx_dose").alias("RxDose"),
            F.col("arx.rx_total").alias("RxTotal"),
            F.col("arx.pt_id").alias("aria_pt_id"),
            F.col("arx.pt_visit_id").alias("aria_pt_visit_id"),
            F.col("arx.rx_id").alias("aria_rx_id"),
            F.col("arx.item_no").alias("aria_item_no"),
            F.col("arx.ADC_UPDT").alias("SRC_ADC_UPDT"),
        )
    )
    return _with_term_map(explode_drugs(base, "AriaAgentName"))

# COMMAND ----------

def _opcs_dot_strip(col):
    return F.upper(F.regexp_replace(F.trim(col.cast("string")), r"[^A-Za-z0-9]", ""))


def _opcs_map():
    opcs = (
        spark.table(OMOP_CONCEPT)
        .filter((F.col("vocabulary_id") == "OPCS4") & F.col("invalid_reason").isNull())
        .select(
            F.col("concept_id").alias("opcs_concept_id"),
            _opcs_dot_strip(F.col("concept_code")).alias("opcs_code_stripped"),
        )
    )
    snomed = (
        spark.table(OMOP_REL)
        .filter(F.col("relationship_id") == "Maps to")
        .join(
            spark.table(OMOP_CONCEPT)
            .filter((F.col("standard_concept") == "S") & F.col("invalid_reason").isNull())
            .select(F.col("concept_id").alias("std_id")),
            F.col("concept_id_2") == F.col("std_id"),
        )
        .groupBy(F.col("concept_id_1"))
        .agg(F.min("std_id").alias("snomed_concept_id"))
    )
    return (
        opcs.join(snomed, opcs["opcs_concept_id"] == snomed["concept_id_1"], "left")
        .groupBy("opcs_code_stripped")
        .agg(
            F.min("opcs_concept_id").alias("opcs_concept_id"),
            F.min("snomed_concept_id").alias("snomed_concept_id"),
            F.countDistinct("opcs_concept_id").alias("n_opcs"),
        )
        .filter(F.col("n_opcs") == 1)  # ambiguous stripped codes -> unmapped
        .drop("n_opcs")
    )


def _with_opcs(df, raw_col, prefix, opcs_map_df):
    stripped = _opcs_dot_strip(F.col(raw_col))
    joined = df.join(
        opcs_map_df.withColumnRenamed("opcs_code_stripped", f"_{prefix}_code"),
        stripped == F.col(f"_{prefix}_code"),
        "left",
    )
    return (
        joined
        .withColumn(f"{prefix}_opcs4_concept_id", F.col("opcs_concept_id").cast("long"))
        .withColumn(f"{prefix}_snomed_concept_id", F.col("snomed_concept_id").cast("long"))
        .withColumn(
            f"{prefix}_map_status",
            F.when(F.col(raw_col).isNull() | (F.trim(F.col(raw_col)) == ""), "NO_CODE")
            .when(F.col("opcs_concept_id").isNull(), "UNMAPPED")
            .when(F.col("snomed_concept_id").isNull(), "OPCS_ONLY")
            .otherwise("MAPPED"),
        )
        .drop("opcs_concept_id", "snomed_concept_id", f"_{prefix}_code")
    )


def build_iqemo_arm() -> DataFrame:
    course = spark.table(SRC_IQ_COURSE).alias("cc")
    regimen = spark.table(SRC_IQ_REGIMEN).alias("rg")
    link = iqemo_patient_link().alias("lnk")

    base = (
        course
        .join(regimen, F.col("cc.RegimenID") == F.col("rg.RegimenID"), "left")
        .join(
            link,
            F.col("cc.PatientID").cast("string") == F.col("lnk.source_patient_id"),
            "left",
        )
        .select(
            F.col("lnk.PERSON_ID").alias("PERSON_ID"),
            F.coalesce(F.col("lnk.link_status"), F.lit("UNLINKED")).alias("link_status"),
            F.col("lnk.NHS_Number").alias("NHS_Number"),
            F.col("lnk.MRN").alias("MRN"),
            F.to_date(F.col("cc.StartDate")).alias("start_date"),
            F.col("rg.SactName").alias("IqemoSactName"),
            F.col("rg.Name").alias("RegimenName"),
            F.col("rg.DefaultCycles").alias("DefaultCycles"),
            F.col("rg.ChemoRadiation").alias("ChemoRadiation"),
            F.col("rg.OPCSProcurementCode").alias("OPCSProcurementCode"),
            F.col("rg.OPCSDeliveryCode").alias("OPCSDeliveryCode"),
            F.col("rg.Indication").alias("Indication"),
            F.to_date(F.col("cc.EndDate")).alias("EndDate"),
            F.to_date(F.col("cc.FinalTreatmentDate")).alias("FinalTreatmentDate"),
            F.col("cc.CourseFinished").alias("CourseFinished"),
            F.col("cc.PlannedCycles").alias("PlannedCycles"),
            F.col("cc.CycleCancelledFrom").alias("CycleCancelledFrom"),
            F.col("cc.ChemotherapyCourseID").alias("iqemo_chemotherapy_course_id"),
            F.col("cc.RegimenID").alias("iqemo_regimen_id"),
            F.col("cc.PatientID").alias("iqemo_patient_id"),
            F.col("cc.ADC_UPDT").alias("SRC_ADC_UPDT"),
        )
    )
    if ENABLE_OPCS_MAP:
        om = _opcs_map()
        base = _with_opcs(base, "OPCSProcurementCode", "procurement", om)
        base = _with_opcs(base, "OPCSDeliveryCode", "delivery", om)
    else:
        # keep downstream column contract intact when OPCS mapping is disabled
        for p in ("procurement", "delivery"):
            base = (
                base
                .withColumn(f"{p}_opcs4_concept_id", F.lit(None).cast("long"))
                .withColumn(f"{p}_snomed_concept_id", F.lit(None).cast("long"))
                .withColumn(f"{p}_map_status", F.lit(None).cast("string"))
            )
    return _with_term_map(explode_drugs(base, "IqemoSactName"))

# COMMAND ----------

ARIA_KEYS = ["aria_pt_id", "aria_pt_visit_id", "aria_rx_id", "aria_item_no", "drug"]
IQEMO_KEYS = ["iqemo_chemotherapy_course_id", "drug"]

def consolidate(aria_arm: DataFrame, iqemo_arm: DataFrame) -> DataFrame:
    ar = aria_arm.select([F.col(c).alias("ar_" + c) for c in aria_arm.columns])
    iq = iqemo_arm.select([F.col(c).alias("iq_" + c) for c in iqemo_arm.columns])

    pairs = (
        ar.join(iq, F.col("ar_PERSON_ID") == F.col("iq_PERSON_ID"), "inner")
        .withColumn(
            "start_date_diff_days",
            F.abs(F.datediff(F.col("ar_start_date"), F.col("iq_start_date"))),
        )
        .withColumn(
            "drug_similarity",
            1
            - F.levenshtein(F.col("ar_drug"), F.col("iq_drug"))
            / F.greatest(F.length(F.col("iq_drug")), F.lit(1)),
        )
        .filter(
            (F.col("drug_similarity") >= 0.8) & (F.col("start_date_diff_days") <= 3)
        )
    )

    w_ar = Window.partitionBy(*["ar_" + k for k in ARIA_KEYS]).orderBy(
        F.col("drug_similarity").desc(),
        F.col("start_date_diff_days").asc(),
        F.col("iq_iqemo_chemotherapy_course_id").asc_nulls_last(),
    )
    w_iq = Window.partitionBy(*["iq_" + k for k in IQEMO_KEYS]).orderBy(
        F.col("drug_similarity").desc(),
        F.col("start_date_diff_days").asc(),
        F.col("ar_aria_pt_id").asc_nulls_last(),
        F.col("ar_aria_rx_id").asc_nulls_last(),
        F.col("ar_aria_item_no").asc_nulls_last(),
    )
    matched_pairs = (
        pairs
        .withColumn("rn_ar", F.row_number().over(w_ar))
        .filter(F.col("rn_ar") == 1)
        .withColumn("rn_iq", F.row_number().over(w_iq))
        .filter(F.col("rn_iq") == 1)
    )

    matched = matched_pairs.select(
        F.coalesce(F.col("ar_PERSON_ID"), F.col("iq_PERSON_ID")).alias("PERSON_ID"),
        F.col("ar_link_status").alias("link_status"),
        F.coalesce(F.col("ar_NHS_Number"), F.col("iq_NHS_Number")).alias("NHS_Number"),
        F.coalesce(F.col("ar_MRN"), F.col("iq_MRN")).alias("MRN"),
        F.coalesce(F.col("ar_start_date"), F.col("iq_start_date")).alias("start_date"),
        F.col("ar_drug").alias("drug"),
        F.col("ar_AriaAgentName").alias("AriaAgentName"),
        F.col("iq_IqemoSactName").alias("IqemoSactName"),
        F.coalesce(F.col("ar_drug_concept_id"), F.col("iq_drug_concept_id")).alias("drug_concept_id"),
        F.coalesce(F.col("ar_drug_concept_code"), F.col("iq_drug_concept_code")).alias("drug_concept_code"),
        F.coalesce(F.col("ar_drug_concept_name"), F.col("iq_drug_concept_name")).alias("drug_concept_name"),
        F.coalesce(F.col("ar_drug_mapping_status"), F.col("iq_drug_mapping_status")).alias("drug_mapping_status"),
        F.col("ar_TreatmentPlan").alias("TreatmentPlan"),
        F.col("ar_DosageForm").alias("DosageForm"),
        F.col("ar_DoseLevel").alias("DoseLevel"),
        F.col("ar_AdmnDosageUnit").alias("AdmnDosageUnit"),
        F.col("ar_AdmnRoute").alias("AdmnRoute"),
        F.col("ar_RxDose").alias("RxDose"),
        F.col("ar_RxTotal").alias("RxTotal"),
        F.col("ar_aria_pt_id").alias("aria_pt_id"),
        F.col("ar_aria_pt_visit_id").alias("aria_pt_visit_id"),
        F.col("ar_aria_rx_id").alias("aria_rx_id"),
        F.col("ar_aria_item_no").alias("aria_item_no"),
        F.col("iq_RegimenName").alias("RegimenName"),
        F.col("iq_DefaultCycles").alias("DefaultCycles"),
        F.col("iq_ChemoRadiation").alias("ChemoRadiation"),
        F.col("iq_OPCSProcurementCode").alias("OPCSProcurementCode"),
        F.col("iq_procurement_opcs4_concept_id").alias("procurement_opcs4_concept_id"),
        F.col("iq_procurement_snomed_concept_id").alias("procurement_snomed_concept_id"),
        F.col("iq_procurement_map_status").alias("procurement_map_status"),
        F.col("iq_OPCSDeliveryCode").alias("OPCSDeliveryCode"),
        F.col("iq_delivery_opcs4_concept_id").alias("delivery_opcs4_concept_id"),
        F.col("iq_delivery_snomed_concept_id").alias("delivery_snomed_concept_id"),
        F.col("iq_delivery_map_status").alias("delivery_map_status"),
        F.col("iq_Indication").alias("Indication"),
        F.col("iq_EndDate").alias("EndDate"),
        F.col("iq_FinalTreatmentDate").alias("FinalTreatmentDate"),
        F.col("iq_CourseFinished").alias("CourseFinished"),
        F.col("iq_PlannedCycles").alias("PlannedCycles"),
        F.col("iq_CycleCancelledFrom").alias("CycleCancelledFrom"),
        F.col("iq_iqemo_chemotherapy_course_id").alias("iqemo_chemotherapy_course_id"),
        F.col("iq_iqemo_regimen_id").alias("iqemo_regimen_id"),
        F.col("iq_iqemo_patient_id").alias("iqemo_patient_id"),
        F.greatest(F.col("ar_SRC_ADC_UPDT"), F.col("iq_SRC_ADC_UPDT")).alias("SRC_ADC_UPDT"),
        F.col("drug_similarity"),
        F.col("start_date_diff_days"),
        F.lit("matched").alias("record_type"),
    )

    matched_aria_keys = (
        matched_pairs.select(*[F.col("ar_" + k).alias(k) for k in ARIA_KEYS]).distinct()
    )
    matched_iqemo_keys = (
        matched_pairs.select(*[F.col("iq_" + k).alias(k) for k in IQEMO_KEYS]).distinct()
    )
    aria_only = (
        aria_arm.join(matched_aria_keys, ARIA_KEYS, "left_anti")
        .withColumn("record_type", F.lit("aria_only"))
    )
    iqemo_only = (
        iqemo_arm.join(matched_iqemo_keys, IQEMO_KEYS, "left_anti")
        .withColumn("record_type", F.lit("iqemo_only"))
    )

    return (
        matched
        .unionByName(aria_only, allowMissingColumns=True)
        .unionByName(iqemo_only, allowMissingColumns=True)
    )

# COMMAND ----------

def _f(name, dtype, comment):
    return StructField(name, dtype, True, metadata={"comment": comment})

CANCER_TREATMENT_COMMENT = (
    "SACT/chemotherapy treatment detail consolidated from ARIA and iQemo, all "
    "tumour sites (systemic therapy only — not a cancer registry; no "
    "radiotherapy sources are landed). One row per source treatment record "
    "(ARIA prescription agent line / iQemo chemotherapy course) per "
    "constituent drug. record_type = matched (ranked 1:1 fuzzy ARIA-iQemo "
    "pair, similarity>=0.8 within 3 days, audit columns kept), aria_only, "
    "iqemo_only. Bronze: shaped and coded (OMOP drug concepts via "
    "cancer_treatment_term_map; OPCS->SNOMED), no DQ filtering; unlinked, "
    "placebo and orphan rows kept. Soft deletes via SOURCE_PRESENT_IND."
)

SCHEMA_CANCER_TREATMENT = StructType([
    _f("TREATMENT_KEY", StringType(), "Deterministic sha2 over record_type-scoped native source keys + drug. MERGE key."),
    _f("PERSON_ID", LongType(), "Millennium person via typed active-alias linkage; NULL when unlinked, ambiguous or conflicting (row retained)."),
    _f("link_status", StringType(), "LINKED_BOTH / LINKED_MRN / LINKED_NHS / CONFLICT / AMBIGUOUS / UNLINKED."),
    _f("NHS_Number", StringType(), "NHS number from the source identifier rows (kept even when unlinked)."),
    _f("MRN", StringType(), "Medical record number from the source identifier rows (kept even when unlinked)."),
    _f("start_date", DateType(), "ARIA agent administration start / iQemo course start; ARIA-first on matched rows."),
    _f("drug", StringType(), "Normalised drug token (lowercased, salts stripped, combinations split one row per constituent). NULL when source text unparseable."),
    _f("AriaAgentName", StringType(), "Raw ARIA agt_name, verbatim."),
    _f("IqemoSactName", StringType(), "Raw iQemo regimen SactName, verbatim."),
    _f("drug_concept_id", LongType(), "OMOP standard drug concept (ingredient-level preferred) via cancer_treatment_term_map; NULL when unmapped/ambiguous."),
    _f("drug_concept_code", StringType(), "Concept code of drug_concept_id."),
    _f("drug_concept_name", StringType(), "Concept name of drug_concept_id."),
    _f("drug_mapping_status", StringType(), "Term-map status: EXACT_NAME / EXACT_SYNONYM / OVERRIDE / AMBIGUOUS / UNMAPPED."),
    _f("TreatmentPlan", StringType(), "ARIA treatment plan name (tp_name)."),
    _f("DosageForm", IntegerType(), "ARIA dosage form code (unmapped; no reference table landed)."),
    _f("DoseLevel", IntegerType(), "ARIA dose level code (unmapped)."),
    _f("AdmnDosageUnit", IntegerType(), "ARIA administration dosage unit code (unmapped)."),
    _f("AdmnRoute", IntegerType(), "ARIA administration route code (unmapped)."),
    _f("RxDose", DecimalType(11, 4), "ARIA prescribed dose (source decimal precision)."),
    _f("RxTotal", DecimalType(18, 4), "ARIA total ordered amount (source decimal precision)."),
    _f("aria_pt_id", StringType(), "ARIA native patient id (traceability / re-linkage)."),
    _f("aria_pt_visit_id", IntegerType(), "ARIA native visit id."),
    _f("aria_rx_id", IntegerType(), "ARIA native prescription id."),
    _f("aria_item_no", IntegerType(), "ARIA native agent item number."),
    _f("RegimenName", StringType(), "iQemo regimen name (regimen.Name)."),
    _f("DefaultCycles", IntegerType(), "iQemo default cycles for the regimen."),
    _f("ChemoRadiation", BooleanType(), "iQemo flag: regimen given with radiotherapy."),
    _f("OPCSProcurementCode", StringType(), "Raw OPCS procurement code, verbatim."),
    _f("procurement_opcs4_concept_id", LongType(), "OPCS4 OMOP concept for the procurement code."),
    _f("procurement_snomed_concept_id", LongType(), "Standard SNOMED concept via 'Maps to'."),
    _f("procurement_map_status", StringType(), "MAPPED / OPCS_ONLY / UNMAPPED / NO_CODE."),
    _f("OPCSDeliveryCode", StringType(), "Raw OPCS delivery code, verbatim."),
    _f("delivery_opcs4_concept_id", LongType(), "OPCS4 OMOP concept for the delivery code."),
    _f("delivery_snomed_concept_id", LongType(), "Standard SNOMED concept via 'Maps to'."),
    _f("delivery_map_status", StringType(), "MAPPED / OPCS_ONLY / UNMAPPED / NO_CODE."),
    _f("Indication", StringType(), "iQemo free-text indication for the regimen."),
    _f("EndDate", DateType(), "iQemo course end date."),
    _f("FinalTreatmentDate", DateType(), "iQemo final day of treatment (differs from EndDate)."),
    _f("CourseFinished", BooleanType(), "iQemo course finished flag."),
    _f("PlannedCycles", IntegerType(), "iQemo planned cycle count (SACT reporting)."),
    _f("CycleCancelledFrom", IntegerType(), "iQemo cycle number the course was cancelled from."),
    _f("iqemo_chemotherapy_course_id", IntegerType(), "iQemo native course id (traceability)."),
    _f("iqemo_regimen_id", StringType(), "iQemo native regimen id."),
    _f("iqemo_patient_id", IntegerType(), "iQemo native patient id."),
    _f("SRC_ADC_UPDT", TimestampType(), "Max contributing source-row load timestamp (separate from processing time)."),
    _f("drug_similarity", DoubleType(), "Levenshtein-based similarity of the matched drug names (matched rows only)."),
    _f("start_date_diff_days", IntegerType(), "Absolute day gap of the matched start dates (matched rows only)."),
    _f("record_type", StringType(), "matched / aria_only / iqemo_only."),
    _f("SOURCE_PRESENT_IND", IntegerType(), "1 = present in latest recompute; 0 = soft-deleted."),
    _f("ROW_HASH", StringType(), "sha2 over payload; update-only-when-changed MERGE."),
    _f("ADC_UPDT", TimestampType(), "Pipeline processing timestamp."),
])

TREATMENT_KEY_COLS = [
    "record_type", "aria_pt_id", "aria_pt_visit_id", "aria_rx_id",
    "aria_item_no", "iqemo_chemotherapy_course_id", "drug",
]

# COMMAND ----------

assert_target_schema_compatible(T_TERM_MAP, SCHEMA_TERM_MAP)
assert_target_schema_compatible(T_TREATMENT, SCHEMA_CANCER_TREATMENT)

results = []

results.append(publish(
    build_term_map, T_TERM_MAP, SCHEMA_TERM_MAP, TERM_MAP_COMMENT,
    ["drug_token"],
    sources=[SRC_ARIA_AGT, SRC_IQ_REGIMEN, OMOP_CONCEPT, OMOP_SYNONYM, OMOP_REL],
    protect_condition="t.mapping_status = 'OVERRIDE'",
    enabled=ENABLE_DRUG_MAP,
))

_treatment_sources = [
    SRC_ARIA_PTKEY, SRC_ARIA_RX, SRC_ARIA_AGT,
    SRC_IQ_COURSE, SRC_IQ_REGIMEN, SRC_IQ_PATIENT,
    PERSON_ALIAS, OMOP_CONCEPT, OMOP_REL,
] + ([T_TERM_MAP] if ENABLE_DRUG_MAP else [])

results.append(publish(
    lambda: consolidate(build_aria_arm(), build_iqemo_arm()),
    T_TREATMENT, SCHEMA_CANCER_TREATMENT, CANCER_TREATMENT_COMMENT,
    TREATMENT_KEY_COLS,
    sources=_treatment_sources,
    key_name="TREATMENT_KEY",
))

record_counts = {}
if bronze_table_exists(T_TREATMENT):
    record_counts = {
        r["record_type"]: r["n"]
        for r in spark.table(T_TREATMENT)
        .filter(F.col("SOURCE_PRESENT_IND") == 1)
        .groupBy("record_type").agg(F.count("*").alias("n")).collect()
    }

summary = {
    "pipeline": "cancer_pipeline",
    "status": "SUCCESS",
    "run_id": CANCER_RUN_ID,
    "started_at": _STARTED_AT,
    "finished_at": bronze_utc_now(),
    "target_schema": TARGET_SCHEMA,
    "results": results,
    "record_counts": record_counts,
    "handoff": [
        "Copy this notebook to /Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/cancer_pipeline.",
        "Add run_cancer_pipeline=true to _BRONZE_WIDGET_DEFAULTS in _bronze_common.",
        "Add the step to the restored normal master orchestrator after Map and before device/registry; do not add it to the temporary Resume After Map Failure orchestrator.",
        "Pass target_schema=4_prod.bronze and allow_production_write=true in production.",
        "Update any workflow/job that invokes domain notebooks directly.",
        "Retire cancer_consolidation and its dev table only after consumers migrate.",
        "Maintain curated drug mappings with mapping_status=OVERRIDE rows in cancer_treatment_term_map.",
    ],
}
print(bronze_json(summary))
dbutils.notebook.exit(bronze_json(summary))
