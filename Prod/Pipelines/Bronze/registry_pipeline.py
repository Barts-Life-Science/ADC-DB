# Databricks notebook source
# MAGIC %md
# MAGIC # iWeb and MediConnect Registry Bronze Pipeline
# MAGIC
# MAGIC Single executable notebook for source preflight, metadata inventory, all registry builds,
# MAGIC curated mapping application, validation, idempotency checks and audit persistence.
# MAGIC Reusable implementation helpers live in `_registry_common`.

# COMMAND ----------

# MAGIC %run ./_registry_common

# COMMAND ----------

_registry_metadata = refresh_source_metadata()

# COMMAND ----------

# MAGIC %md
# MAGIC ## MediConnect CIED devices

# COMMAND ----------

ensure_target_schema()

DEVICE_MAP_TABLE = f"{bronze_lookup_schema(TARGET_SCHEMA)}.mediconnect_device_type_map"
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {qname(DEVICE_MAP_TABLE)} (
      DEVICE_TYPE INT COMMENT 'MediConnect numeric device category (canonical source category)',
      SOURCE_LABEL STRING COMMENT 'Most common observed TYPE label for this code',
      DEVICE_ROLE STRING COMMENT 'GENERATOR | LEAD | MONITOR | OTHER',
      SNOMED_CONCEPT_ID BIGINT COMMENT 'Candidate standard SNOMED Device concept',
      SNOMED_CONCEPT_NAME STRING,
      MAPPING_METHOD STRING COMMENT 'CURATED',
      MAPPING_STATUS STRING COMMENT 'APPROVED | PROPOSED | REJECTED',
      MAPPING_VERSION STRING,
      CURATED_BY STRING,
      CURATED_AT TIMESTAMP,
      NOTES STRING
    ) USING DELTA
    COMMENT 'Curated MediConnect device category to SNOMED CT map. Only APPROVED concepts are applied.'
    """
)

# Candidate concepts were selected from 3_lookup.omop.concept at runtime design review; all are
# standard, current, Device-domain SNOMED concepts. They remain PROPOSED pending clinical review.
DEVICE_TYPE_SEED = [
    (0, "", "OTHER", None, None, "CURATED", "PROPOSED", "1.0.0", None, None, "Blank/unknown source category"),
    (1, "PM", "GENERATOR", 4041473, "Pacemaker pulse generator", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (2, "CRT-P", "GENERATOR", 45767329, "Cardiac resynchronization therapy implantable pacemaker", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (3, "ICD", "GENERATOR", 4217646, "Implantable defibrillator", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (4, "CRT-D", "GENERATOR", 37166983, "Cardiac resynchronization therapy defibrillator pulse generator", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (5, "LV", "LEAD", 37168901, "Cardiac ventricular pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, "Laterality retained in source TYPE/LOCATION1"),
    (6, "RV", "LEAD", 37168901, "Cardiac ventricular pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, "Laterality retained in source TYPE/LOCATION1"),
    (7, "ICM", "MONITOR", 1448963, "Implantable electrocardiographic monitor and loop recorder", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (8, "RA/Atrial", "LEAD", 37168903, "Atrial pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (9, "Other leads", "LEAD", 4236068, "Cardiac pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (10, "Lead 1", "LEAD", 4236068, "Cardiac pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (11, "Lead 2", "LEAD", 4236068, "Cardiac pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (12, "Lead 3/CS", "LEAD", 4236068, "Cardiac pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, None),
]
seed = spark.createDataFrame(DEVICE_TYPE_SEED, spark.table(DEVICE_MAP_TABLE).schema)
(
    DeltaTable.forName(spark, DEVICE_MAP_TABLE)
    .alias("target")
    .merge(seed.alias("source"), "target.DEVICE_TYPE <=> source.DEVICE_TYPE")
    .whenNotMatchedInsertAll()
    .execute()
)

invalid_candidates = (
    spark.table(DEVICE_MAP_TABLE)
    .where(F.col("SNOMED_CONCEPT_ID").isNotNull())
    .join(
        spark.table(OMOP_CONCEPT).select(
            F.col("concept_id").cast("long").alias("SNOMED_CONCEPT_ID"),
            "domain_id", "vocabulary_id", "standard_concept", "invalid_reason",
        ),
        "SNOMED_CONCEPT_ID",
        "left",
    )
    .where(
        F.col("domain_id").isNull()
        | (F.col("domain_id") != "Device")
        | (F.col("vocabulary_id") != "SNOMED")
        | (F.col("standard_concept") != "S")
        | F.col("invalid_reason").isNotNull()
    )
    .count()
)
assert invalid_candidates == 0, f"Invalid MediConnect device map concepts: {invalid_candidates}"

# COMMAND ----------

source = spark.table(f"{RAW}.mediconnect_mc_devices")
natural_columns = [
    "PATIENTID", "DEVICE_TYPE", "TYPE", "MANUFACTURER", "MODEL_NAME",
    "SERIAL_NO", "IMPLANTED_DATE", "LOCATION1", "COMMENT", "STATUS",
]
natural_payload = F.struct(
    *[
        F.coalesce(F.col(name).cast("string"), F.lit("<NULL>")).alias(name)
        for name in natural_columns
    ]
)

devices = (
    source.withColumn("MC_DEVICE_ID", F.sha2(F.to_json(natural_payload), 256))
    .dropDuplicates(["MC_DEVICE_ID"])
    .withColumn("IMPLANTED_DATE_QUALITY", date_quality("IMPLANTED_DATE", sentinel_year=1901))
    .withColumn("IMPLANTED_DATE_CLEAN", clean_date("IMPLANTED_DATE", sentinel_year=1901))
    .withColumn(
        "MODEL_CODE",
        blank_to_null(F.regexp_extract("MODEL_NAME", r"^\s*([A-Za-z0-9][A-Za-z0-9/_.-]*)", 1)),
    )
    .withColumn(
        "MANUFACTURER_CLEAN",
        F.initcap(F.trim(F.regexp_replace(F.col("MANUFACTURER"), r"\s*\([^)]*\)\s*$", ""))),
    )
    .withColumn(
        "MANUFACTURER_PARENT",
        blank_to_null(F.regexp_extract("MANUFACTURER", r"\(([^)]+)\)", 1)),
    )
    .withColumn(
        "LEAD_CHAMBER",
        blank_to_null(F.regexp_extract("LOCATION1", r"(?i)Chamber:\s*([^\r\n]+)", 1)),
    )
    .withColumn(
        "LEAD_LOCATION",
        blank_to_null(F.regexp_extract("LOCATION1", r"(?i)Location:\s*([^\r\n]+)", 1)),
    )
    .withColumn(
        "LEAD_ACCESS",
        blank_to_null(F.regexp_extract("LOCATION1", r"(?i)Access:\s*([^\r\n]+)", 1)),
    )
    .withColumn(
        "LEAD_CONNECT",
        blank_to_null(F.regexp_extract("LOCATION1", r"(?i)Connect:\s*([^\r\n]+)", 1)),
    )
    .withColumn(
        "POCKET_SITE",
        F.when(
            ~F.coalesce(F.col("LOCATION1"), F.lit("")).rlike(r"(?i)Chamber:"),
            blank_to_null(F.col("LOCATION1")),
        ),
    )
    .withColumn(
        "IMPLANTING_CLINICIAN_1",
        blank_to_null(F.regexp_extract("COMMENT", r"(?i)Imp\s*MD\s*1:\s*([^/\r\n]+)", 1)),
    )
    .withColumn(
        "IMPLANTING_CLINICIAN_2",
        blank_to_null(F.regexp_extract("COMMENT", r"(?i)Imp\s*MD\s*2:\s*([^/\r\n]+)", 1)),
    )
    .withColumn("EXPLANTED_IND", F.when(F.col("STATUS").isNull(), F.lit(None).cast("boolean")).otherwise(F.col("STATUS") == 1))
    .drop("NO_OF_CHAMBERS", "MODEL_NO")
)

device_map = spark.table(DEVICE_MAP_TABLE).select(
    "DEVICE_TYPE",
    "DEVICE_ROLE",
    F.col("MAPPING_STATUS").alias("DEVICE_MAPPING_STATUS"),
    F.col("MAPPING_VERSION").alias("DEVICE_MAPPING_VERSION"),
    F.when(F.col("MAPPING_STATUS") == "APPROVED", F.col("SNOMED_CONCEPT_ID"))
    .cast("long").alias("DEVICE_SNOMED_CONCEPT_ID"),
    F.when(F.col("MAPPING_STATUS") == "APPROVED", F.col("SNOMED_CONCEPT_NAME"))
    .alias("DEVICE_SNOMED_CONCEPT_NAME"),
)
devices = devices.join(device_map, "DEVICE_TYPE", "left")
devices = resolve_persons(devices, mrn_col="PATIENTID")
devices = with_source_metadata(devices, "mediconnect_mc_devices", "MC_DEVICE_ID")

registry_update_table(
    devices,
    f"{TARGET_SCHEMA}.mediconnect_device",
    ["MC_DEVICE_ID"],
    "MediConnect CIED/lead registry (legacy Heart Hospital NICOR migration; implants end "
    "2020-01-01 in the current frozen extract). One row per distinct source device/lead record. "
    "STATUS=1 is exposed as an inferred explant flag pending source-owner confirmation.",
    {
        "MC_DEVICE_ID": "Stable SHA-256 key over the retained source natural fields; the source has no primary key.",
        "PATIENTID": "Source MediConnect identifier, deterministically resolved as a Millennium MRN candidate.",
        "EXPLANTED_IND": "INFERRED from STATUS=1; semantics require confirmation against the source lookup.",
        "IMPLANTED_DATE_QUALITY": "VALID | SENTINEL (year <=1901) | FUTURE | MISSING; raw date retained.",
        "DEVICE_SNOMED_CONCEPT_ID": "Applied only when the category mapping row is APPROVED; no model-level UDI claim.",
        "PERSON_ID": "Millennium PERSON_ID via a unique active MRN alias; NULL for ambiguous or unmatched rows.",
        "ADC_UPDT": "Source landing timestamp. Excluded from ROW_HASH because the full snapshot refreshes it daily.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cardiac surgery

# COMMAND ----------

MODULES = [
    ("PRE1", "reg_cs2010g_pre1"),
    ("PRE2", "reg_cs2010g_pre2"),
    ("POST1", "reg_cs2010g_post1"),
    ("POST2", "reg_cs2010g_post2"),
]

# Link once from the complete pre1 identifier pair, then remove direct identifiers. Other modules
# contribute clinical content only; repeated identifiers/demographics are not merged back in.
base_raw = spark.table(f"{IWEB}.reg_cs2010g_pre1")
base_linked = resolve_persons(base_raw, mrn_col="MRN", nhs_col="NHSNumber")
base, dropped_pii = drop_pii(base_linked)
if "ADC_UPDT" in base.columns:
    base = base.withColumnRenamed("ADC_UPDT", "PRE1_ADC_UPDT")

merged = base
kept = set(merged.columns)
for prefix, table_name in MODULES[1:]:
    module, dropped = drop_pii(spark.table(f"{IWEB}.{table_name}"))
    dropped_pii.extend(dropped)
    module = module.drop(
        *[
            name
            for name in ("MRN", "NHSNumber", "DateOfOperation", "DateOfDeath")
            if name in module.columns
        ]
    )
    if "ADC_UPDT" in module.columns:
        module = module.withColumnRenamed("ADC_UPDT", f"{prefix}_ADC_UPDT")
    renamed = {}
    for name in module.columns:
        renamed[name] = f"{prefix}_{name}" if name != "EntryId" and name in kept else name
    module = module.select(*[F.col(f"`{name}`").alias(renamed[name]) for name in module.columns])
    kept.update(renamed.values())
    merged = merged.join(module, "EntryId", "full_outer")

last_changed_columns = [
    name for name in merged.columns if _normalised_column_name(name).endswith("datelastchanged")
]
adc_columns = [name for name in merged.columns if name.endswith("_ADC_UPDT")]
if not last_changed_columns or not adc_columns:
    raise AssertionError(
        f"Expected module timestamps are missing: last_changed={last_changed_columns}, adc={adc_columns}"
    )

episode = (
    merged.withColumn(
        "EPISODE_LAST_CHANGED",
        F.greatest(*[F.col(f"`{name}`") for name in last_changed_columns]),
    )
    .withColumn("ADC_UPDT", F.greatest(*[F.col(f"`{name}`") for name in adc_columns]))
)
episode = with_source_metadata(
    episode,
    "reg_cs2010g_pre1+reg_cs2010g_pre2+reg_cs2010g_post1+reg_cs2010g_post2",
    "EntryId",
)
episode = to_snake(episode)
print("[PII_DROP] cardiac_surgery_episode " + json.dumps(sorted(set(dropped_pii))))

registry_update_table(
    episode,
    f"{TARGET_SCHEMA}.iweb_cardiac_surgery_episode",
    ["ENTRY_ID"],
    "iWeb CS2010G adult cardiac surgery registry with pre1/pre2/post1/post2 modules merged "
    "1:1 on EntryId after a hard set-equivalence gate. One row per surgical episode; source "
    "labels and module-specific fields are retained, while direct patient identifiers are removed.",
    {
        "ENTRY_ID": "Stable iWeb episode key and parent of iweb_cardiac_surgery_procedure.",
        "PERSON_ID": "Resolved independently through NHS number and MRN; NULL on conflict or ambiguity.",
        "MRN": "Native source hospital number retained for trace-back; NHS number is dropped post-linkage.",
        "EPISODE_LAST_CHANGED": "Greatest DateLastChanged timestamp across the four source modules.",
        "ADC_UPDT": "Greatest landing timestamp across the four source modules; excluded from ROW_HASH.",
    },
)

# COMMAND ----------

procedure_raw = spark.table(f"{IWEB}.reg_cs2010g_subprocedure")
procedure = resolve_persons(procedure_raw, mrn_col="MRN", nhs_col="NHSNumber")
procedure, procedure_drops = drop_pii(procedure)
procedure = with_source_metadata(procedure, "reg_cs2010g_subprocedure", "EntryId")
procedure = to_snake(procedure)
print("[PII_DROP] cardiac_surgery_procedure " + json.dumps(procedure_drops))

registry_update_table(
    procedure,
    f"{TARGET_SCHEMA}.iweb_cardiac_surgery_procedure",
    ["ENTRY_ID"],
    "One row per graft, valve or aortic-implant component of a CS2010G surgery. "
    "PARENT_ENTRY_ID joins iweb_cardiac_surgery_episode and is hard-gated to zero orphans. "
    "Known source defect: IMPLANT_PROSTHESIS_NAMETXT contains some drug-picklist bleed-through.",
    {
        "ENTRY_ID": "Stable iWeb child-record key.",
        "PARENT_ENTRY_ID": "Foreign key to iweb_cardiac_surgery_episode.ENTRY_ID.",
    },
)

# COMMAND ----------

followup_raw = spark.table(f"{IWEB}.reg_cs2010g_followup")
tie_columns = sorted(followup_raw.columns)
tie_hash = F.sha2(
    F.to_json(
        F.struct(
            *[
                F.coalesce(F.col(f"`{name}`").cast("string"), F.lit("<NULL>")).alias(name)
                for name in tie_columns
            ]
        )
    ),
    256,
)
followup_window = Window.partitionBy("EntryId").orderBy(
    F.col("DateLastChanged").desc_nulls_last(),
    F.col("ADC_UPDT").desc_nulls_last(),
    tie_hash.desc(),
)
followup = (
    followup_raw.withColumn("_ROW_NUMBER", F.row_number().over(followup_window))
    .where(F.col("_ROW_NUMBER") == 1)
    .drop("_ROW_NUMBER")
)
followup = resolve_persons(followup, mrn_col="MRN", nhs_col="NHSNumber")
followup, followup_drops = drop_pii(followup)
followup = with_source_metadata(followup, "reg_cs2010g_followup", "EntryId")
followup = to_snake(followup)
print("[PII_DROP] cardiac_surgery_followup " + json.dumps(followup_drops))

registry_update_table(
    followup,
    f"{TARGET_SCHEMA}.iweb_cardiac_surgery_followup",
    ["ENTRY_ID"],
    "CS2010G follow-up module. The source is small and dormant but semantically distinct. "
    "Duplicate EntryId rows are reduced deterministically to the latest DateLastChanged/ADC_UPDT row.",
    {
        "ENTRY_ID": "Stable iWeb follow-up key after deterministic latest-row deduplication.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Procedures and pathways

# COMMAND ----------

def build_single_registry(
    source_table: str,
    target_table: str,
    table_comment: str,
    extra_drop=(),
    col_comments=None,
    transform=None,
):
    source = spark.table(f"{IWEB}.{source_table}")
    frame = resolve_persons(source, mrn_col="MRN", nhs_col="NHSNumber")
    if transform is not None:
        frame = transform(frame)
    frame, dropped = drop_pii(frame, extra=extra_drop)
    frame = with_source_metadata(frame, source_table, "EntryId")
    frame = to_snake(frame)
    print(f"[PII_DROP] {target_table} " + json.dumps(dropped))
    return registry_update_table(
        frame,
        f"{TARGET_SCHEMA}.{target_table}",
        ["ENTRY_ID"],
        table_comment,
        col_comments or {},
    )


build_single_registry(
    "reg_coronary_subprocedure",
    "iweb_coronary_lesion",
    "Per-lesion/per-vessel PCI and angiography detail from the iWeb coronary registry. "
    "The parent coronary procedure registry is not landed, so PARENT_ENTRY_ID is retained for "
    "future linkage but currently has no bronze parent and there is no reliable procedure date.",
    col_comments={
        "PARENT_ENTRY_ID": "Foreign key to the currently un-landed iWeb coronary procedure registry.",
        "DATE_LAST_CHANGED": "Source edit timestamp; it must not be interpreted as the procedure date.",
    },
)

build_single_registry(
    "reg_noncoronary",
    "iweb_noncoronary_procedure",
    "iWeb non-coronary/structural cath-lab registry covering interventions such as TAVI, TMVI, "
    "ASD/VSD closure and septal ablation. Local N-Label values are retained here and exploded "
    "separately in iweb_multiselect_value.",
)


def _acs_dates(frame: DataFrame) -> DataFrame:
    if "Admissiondate" not in frame.columns:
        raise AssertionError("reg_dghminap.Admissiondate is missing")
    return (
        frame.withColumn("ADMISSION_DATE_QUALITY", date_quality("Admissiondate", sentinel_year=2000))
        .withColumn("ADMISSION_DATE_CLEAN", clean_date("Admissiondate", sentinel_year=2000))
    )


build_single_registry(
    "reg_dghminap",
    "iweb_acs_transfer",
    "MINAP-style ACS transfer pathway: symptom, transfer and reperfusion timestamps, GRACE/Killip, "
    "drugs, laboratory results and Takotsubo fields. Diagnoses remain source labels. "
    "ADMISSION_DATE_QUALITY identifies the known 2000-01-01 placeholder range.",
    col_comments={
        "ADMISSION_DATE_QUALITY": "VALID | SENTINEL (year <=2000) | FUTURE | MISSING.",
        "ADMISSION_DATE_CLEAN": "Admission date with sentinel/future values nulled; raw ADMISSIONDATE retained.",
    },
    transform=_acs_dates,
)

build_single_registry(
    "reg_eracs",
    "iweb_eracs_episode",
    "Enhanced Recovery After Cardiac Surgery pathway registry, live since 2025: pathway "
    "timestamps, pacing, TEG/platelet mapping, transfusion, mobilisation and discharge milestones.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cardiac MDT and mortality review

# COMMAND ----------

def load_mdt(source_table: str, registry_type: str) -> DataFrame:
    frame = spark.table(f"{IWEB}.{source_table}")
    frame = resolve_persons(frame, mrn_col="MRN", nhs_col="NHSNumber")
    frame, dropped = drop_pii(frame)
    frame = (
        frame.withColumn("REGISTRY_TYPE", F.lit(registry_type))
        .withColumn("SOURCE_TABLE", F.lit(source_table))
        .withColumn("SOURCE_RECORD_KEY", F.col("EntryId").cast("string"))
    )
    print(f"[PII_DROP] {source_table} " + json.dumps(dropped))
    return to_snake(frame)


coronary_mdt = load_mdt("reg_mdt", "CORONARY_REVASC")
ct_aortic_mdt = load_mdt("reg_ctmdt", "CT_AORTIC")
mdt = coronary_mdt.unionByName(ct_aortic_mdt, allowMissingColumns=True)

registry_update_table(
    mdt,
    f"{TARGET_SCHEMA}.iweb_cardiac_mdt",
    ["REGISTRY_TYPE", "ENTRY_ID"],
    "Harmonised iWeb cardiac MDT record. REGISTRY_TYPE=CORONARY_REVASC identifies the coronary "
    "revascularisation workflow; REGISTRY_TYPE=CT_AORTIC identifies CT/aortic surveillance. "
    "Workflow-specific columns are null for the other registry.",
    {
        "REGISTRY_TYPE": "Source workflow discriminator and part of the composite key.",
        "ENTRY_ID": "Source EntryId, unique only within REGISTRY_TYPE.",
    },
)

# COMMAND ----------

mortality = spark.table(f"{IWEB}.reg_mort")
mortality = resolve_persons(mortality, mrn_col="MRN", nhs_col="NHSNumber")
mortality, mortality_drops = drop_pii(mortality)
if not any("kin" in name.lower() for name in mortality_drops):
    raise AssertionError("Mortality next-of-kin columns were not detected by the PII policy")
if "DateofDeath" not in mortality.columns:
    raise AssertionError("reg_mort.DateofDeath is missing")
mortality = (
    mortality.withColumn("DATE_OF_DEATH_QUALITY", date_quality("DateofDeath"))
    .withColumn("DATE_OF_DEATH_CLEAN", clean_date("DateofDeath"))
)
mortality = with_source_metadata(mortality, "reg_mort", "EntryId")
mortality = to_snake(mortality)
print("[PII_DROP] mortality_review " + json.dumps(mortality_drops))

registry_update_table(
    mortality,
    f"{TARGET_SCHEMA}.iweb_cardiac_mortality_review",
    ["ENTRY_ID"],
    "Cardiac mortality/M&M review registry: source-text cause-of-death chain (not asserted to be "
    "ICD-coded), NCEPOD classification, coronial/medical-examiner fields and structured care-problem "
    "flags. Patient names, contact details and next-of-kin content are removed.",
    {
        "DATE_OF_DEATH_QUALITY": "VALID | FUTURE | MISSING; raw DATEOF_DEATH is retained.",
        "DATE_OF_DEATH_CLEAN": "Date of death with future values nulled.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Local picklist and multiselect values

# COMMAND ----------

inventory = spark.table(f"{bronze_control_schema(TARGET_SCHEMA)}.iweb_value_inventory")
multiselect_fields = (
    inventory.where(F.col("RAW_VALUE").rlike(r"^\s*\d+\s*-\s*"))
    .where(F.col("SOURCE_TABLE").startswith(f"{IWEB}."))
    .select("SOURCE_TABLE", "FIELD_NAME")
    .distinct()
    .collect()
)
if not multiselect_fields:
    raise AssertionError("No N-Label iWeb fields were found in the value inventory")

fields_by_table = defaultdict(list)
for row in multiselect_fields:
    fields_by_table[row.SOURCE_TABLE].append(row.FIELD_NAME)

value_frames = []
link_frames = []


def linkage_for_source(short_name: str) -> DataFrame:
    linkage_columns = [
        "PERSON_ID", "LINKAGE_STATUS", "LINKAGE_METHOD", "NHS_NUMBER_VALID_IND",
        "LINKAGE_HISTORICAL_FALLBACK_IND", "LINKAGE_FALLBACK_CONFLICT_IND",
    ]
    if short_name in {
        "reg_cs2010g_pre1", "reg_cs2010g_pre2", "reg_cs2010g_post1", "reg_cs2010g_post2"
    }:
        target = "iweb_cardiac_surgery_episode"
        frame = spark.table(f"{TARGET_SCHEMA}.{target}").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_cs2010g_subprocedure":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_cardiac_surgery_procedure").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_cs2010g_followup":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_cardiac_surgery_followup").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_coronary_subprocedure":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_coronary_lesion").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_dghminap":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_acs_transfer").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_noncoronary":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_noncoronary_procedure").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_eracs":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_eracs_episode").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_mdt":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_cardiac_mdt").where(
            "IS_CURRENT_IN_SOURCE AND REGISTRY_TYPE = 'CORONARY_REVASC'"
        )
    elif short_name == "reg_ctmdt":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_cardiac_mdt").where(
            "IS_CURRENT_IN_SOURCE AND REGISTRY_TYPE = 'CT_AORTIC'"
        )
    elif short_name == "reg_mort":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_cardiac_mortality_review").where("IS_CURRENT_IN_SOURCE")
    else:
        raise AssertionError(f"No parent linkage table configured for {short_name}")
    available = set(frame.columns)
    missing = [name for name in linkage_columns if name not in available]
    if missing:
        raise AssertionError(f"Parent linkage columns missing for {short_name}: {missing}")
    return frame.select(
        F.lit(short_name).alias("SOURCE_TABLE"),
        F.col("ENTRY_ID").cast("long").alias("ENTRY_ID"),
        *linkage_columns,
    )


for source_name, fields in sorted(fields_by_table.items()):
    source = spark.table(source_name)
    short_name = source_name.split(".")[-1]
    if short_name == "reg_cs2010g_followup":
        source_order = Window.partitionBy("EntryId").orderBy(
            F.col("DateLastChanged").desc_nulls_last(),
            F.col("ADC_UPDT").desc_nulls_last(),
        )
        source = (
            source.withColumn("_SOURCE_RN", F.row_number().over(source_order))
            .where(F.col("_SOURCE_RN") == 1)
            .drop("_SOURCE_RN")
        )
    value_frames.append(explode_multiselect(source, short_name, "EntryId", sorted(fields)))
    link_frames.append(linkage_for_source(short_name))

values = reduce(lambda left, right: left.unionByName(right), value_frames)
links = reduce(lambda left, right: left.unionByName(right), link_frames)
values = values.join(links, ["SOURCE_TABLE", "ENTRY_ID"], "left")
values = values.withColumn("SOURCE_RECORD_KEY", F.col("ENTRY_ID").cast("string"))

# COMMAND ----------

VALUE_MAP_TABLE = f"{bronze_lookup_schema(TARGET_SCHEMA)}.iweb_value_to_concept"
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {qname(VALUE_MAP_TABLE)} (
      SOURCE_TABLE STRING,
      FIELD_NAME STRING,
      CODE STRING,
      LABEL STRING,
      TARGET_VOCABULARY STRING COMMENT 'SNOMED | OPCS4 | ICD10',
      TARGET_CODE STRING,
      TARGET_CONCEPT_ID BIGINT,
      TARGET_CONCEPT_NAME STRING,
      MAPPING_METHOD STRING COMMENT 'CURATED | TEXT_CANDIDATE',
      MAPPING_STATUS STRING COMMENT 'APPROVED | PROPOSED | REJECTED',
      MAPPING_VERSION STRING,
      CURATED_BY STRING,
      CURATED_AT TIMESTAMP
    ) USING DELTA
    COMMENT 'Versioned curated map from iWeb local picklist values to standard concepts. Only APPROVED rows are applied.'
    """
)

PRIORITY_FIELDS = [
    ("reg_noncoronary", "Interventionstxt"),
    ("reg_noncoronary", "Investigationstxt"),
    ("reg_cs2010g_post1", "CABGcodingtxt"),
    ("reg_cs2010g_post1", "ValvesReplacementCodingtxt"),
    ("reg_cs2010g_pre1", "PreviousCardiacSurgerytxt"),
    ("reg_eracs", "ProcedurePerformedtxt"),
    ("reg_dghminap", "DischargeDiagnosis"),
]
priority = spark.createDataFrame(PRIORITY_FIELDS, "SOURCE_TABLE string, FIELD_NAME string")
seed = (
    values.select("SOURCE_TABLE", "FIELD_NAME", "CODE", "LABEL")
    .distinct()
    .join(priority, ["SOURCE_TABLE", "FIELD_NAME"], "left_semi")
    .withColumn("TARGET_VOCABULARY", F.lit(None).cast("string"))
    .withColumn("TARGET_CODE", F.lit(None).cast("string"))
    .withColumn("TARGET_CONCEPT_ID", F.lit(None).cast("long"))
    .withColumn("TARGET_CONCEPT_NAME", F.lit(None).cast("string"))
    .withColumn("MAPPING_METHOD", F.lit("CURATED"))
    .withColumn("MAPPING_STATUS", F.lit("PROPOSED"))
    .withColumn("MAPPING_VERSION", F.lit("1.0.0"))
    .withColumn("CURATED_BY", F.lit(None).cast("string"))
    .withColumn("CURATED_AT", F.lit(None).cast("timestamp"))
    .select(*spark.table(VALUE_MAP_TABLE).columns)
)
map_condition = (
    "target.SOURCE_TABLE <=> source.SOURCE_TABLE AND "
    "target.FIELD_NAME <=> source.FIELD_NAME AND "
    "target.CODE <=> source.CODE AND target.LABEL <=> source.LABEL"
)
(
    DeltaTable.forName(spark, VALUE_MAP_TABLE)
    .alias("target")
    .merge(seed.alias("source"), map_condition)
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

approved = (
    spark.table(VALUE_MAP_TABLE)
    .where(F.col("MAPPING_STATUS") == "APPROVED")
    .select(
        "SOURCE_TABLE", "FIELD_NAME", "CODE", "LABEL",
        "TARGET_VOCABULARY", "TARGET_CODE", "TARGET_CONCEPT_ID", "TARGET_CONCEPT_NAME",
        "MAPPING_METHOD", "MAPPING_STATUS", "MAPPING_VERSION", "CURATED_BY", "CURATED_AT",
    )
)
approved_duplicates = (
    approved.groupBy("SOURCE_TABLE", "FIELD_NAME", "CODE", "LABEL")
    .count()
    .where(F.col("count") > 1)
    .count()
)
assert approved_duplicates == 0, f"Duplicate APPROVED iWeb mappings: {approved_duplicates}"

value_alias = values.alias("value")
map_alias = approved.alias("mapping")
join_condition = (
    (F.col("value.SOURCE_TABLE") == F.col("mapping.SOURCE_TABLE"))
    & (F.col("value.FIELD_NAME") == F.col("mapping.FIELD_NAME"))
    & F.col("value.CODE").eqNullSafe(F.col("mapping.CODE"))
    & F.col("value.LABEL").eqNullSafe(F.col("mapping.LABEL"))
)
mapped = value_alias.join(map_alias, join_condition, "left").select(
    *[F.col(f"value.`{name}`").alias(name) for name in values.columns],
    *[
        F.col(f"mapping.`{name}`").alias(name)
        for name in (
            "TARGET_VOCABULARY", "TARGET_CODE", "TARGET_CONCEPT_ID", "TARGET_CONCEPT_NAME",
            "MAPPING_METHOD", "MAPPING_STATUS", "MAPPING_VERSION", "CURATED_BY", "CURATED_AT",
        )
    ],
)

registry_update_table(
    mapped,
    f"{TARGET_SCHEMA}.iweb_multiselect_value",
    ["SOURCE_TABLE", "ENTRY_ID", "FIELD_NAME", "POSITION"],
    "Long-form exploded values for every empirically identified iWeb N-Label picklist field. "
    "Splitting uses a numeric-code lookahead so commas inside labels survive. Only APPROVED mappings "
    "are applied; unmapped source values remain visible for curation.",
    {
        "CODE": "Local iWeb picklist code parsed from the N - Label source value; not a standard code.",
        "LABEL": "Source label retained verbatim after removal of the local numeric prefix.",
        "TARGET_CONCEPT_ID": "Standard concept applied only from an APPROVED curated mapping row.",
        "POSITION": "Zero-based position in the source field after deterministic explosion.",
    },
)

coverage = (
    mapped.groupBy("SOURCE_TABLE", "FIELD_NAME")
    .agg(
        F.count("*").alias("VALUES_TOTAL"),
        F.sum(F.when(F.col("TARGET_CONCEPT_ID").isNotNull(), 1).otherwise(0)).alias("VALUES_MAPPED"),
        F.countDistinct(F.when(F.col("TARGET_CONCEPT_ID").isNull(), F.col("LABEL"))).alias("DISTINCT_UNMAPPED"),
    )
    .orderBy(F.desc("VALUES_TOTAL"))
)
coverage.show(200, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Release validation

# COMMAND ----------

RESULTS = []


def check(name, passed, table_name="", metric_value=None, detail="", severity="ERROR"):
    record = {
        "check_name": str(name),
        "table_name": str(table_name or ""),
        "passed": bool(passed),
        "severity": str(severity),
        "metric_value": float(metric_value) if metric_value is not None else None,
        "detail": str(detail)[:1000],
    }
    RESULTS.append(record)
    print(("PASS " if passed else "FAIL ") + json.dumps(record, sort_keys=True))
    return bool(passed)


TABLE_KEYS = {
    "mediconnect_device": ["MC_DEVICE_ID"],
    "iweb_cardiac_surgery_episode": ["ENTRY_ID"],
    "iweb_cardiac_surgery_procedure": ["ENTRY_ID"],
    "iweb_cardiac_surgery_followup": ["ENTRY_ID"],
    "iweb_coronary_lesion": ["ENTRY_ID"],
    "iweb_acs_transfer": ["ENTRY_ID"],
    "iweb_noncoronary_procedure": ["ENTRY_ID"],
    "iweb_eracs_episode": ["ENTRY_ID"],
    "iweb_cardiac_mdt": ["REGISTRY_TYPE", "ENTRY_ID"],
    "iweb_cardiac_mortality_review": ["ENTRY_ID"],
    "iweb_multiselect_value": ["SOURCE_TABLE", "ENTRY_ID", "FIELD_NAME", "POSITION"],
}
CLINICAL_TABLES = list(TABLE_KEYS)
SUPPORT_TABLES = {
    "iweb_source_manifest": f"{bronze_control_schema(TARGET_SCHEMA)}.iweb_source_manifest",
    "iweb_value_inventory": f"{bronze_control_schema(TARGET_SCHEMA)}.iweb_value_inventory",
    "mediconnect_device_type_map": f"{bronze_lookup_schema(TARGET_SCHEMA)}.mediconnect_device_type_map",
    "iweb_value_to_concept": f"{bronze_lookup_schema(TARGET_SCHEMA)}.iweb_value_to_concept",
}

for table_name in CLINICAL_TABLES:
    check(
        f"table_exists:{table_name}",
        table_exists(f"{TARGET_SCHEMA}.{table_name}"),
        table_name,
    )
for table_name, full_name in SUPPORT_TABLES.items():
    check(
        f"table_exists:{table_name}",
        table_exists(full_name),
        table_name,
    )

if (
    not all(table_exists(f"{TARGET_SCHEMA}.{name}") for name in CLINICAL_TABLES)
    or not all(table_exists(name) for name in SUPPORT_TABLES.values())
):
    raise AssertionError("Required output tables are missing; validation cannot continue")


def current_table(table_name: str) -> DataFrame:
    return spark.table(f"{TARGET_SCHEMA}.{table_name}").where(F.col("IS_CURRENT_IN_SOURCE") == True)


# Source-to-current-target row-count reconciliation.
natural_columns = [
    "PATIENTID", "DEVICE_TYPE", "TYPE", "MANUFACTURER", "MODEL_NAME",
    "SERIAL_NO", "IMPLANTED_DATE", "LOCATION1", "COMMENT", "STATUS",
]
expected_counts = {
    "mediconnect_device": spark.table(f"{RAW}.mediconnect_mc_devices").select(*natural_columns).dropDuplicates().count(),
    "iweb_cardiac_surgery_episode": spark.table(f"{IWEB}.reg_cs2010g_pre1").count(),
    "iweb_cardiac_surgery_procedure": spark.table(f"{IWEB}.reg_cs2010g_subprocedure").count(),
    "iweb_cardiac_surgery_followup": spark.table(f"{IWEB}.reg_cs2010g_followup").select("EntryId").distinct().count(),
    "iweb_coronary_lesion": spark.table(f"{IWEB}.reg_coronary_subprocedure").count(),
    "iweb_acs_transfer": spark.table(f"{IWEB}.reg_dghminap").count(),
    "iweb_noncoronary_procedure": spark.table(f"{IWEB}.reg_noncoronary").count(),
    "iweb_eracs_episode": spark.table(f"{IWEB}.reg_eracs").count(),
    "iweb_cardiac_mdt": spark.table(f"{IWEB}.reg_mdt").count() + spark.table(f"{IWEB}.reg_ctmdt").count(),
    "iweb_cardiac_mortality_review": spark.table(f"{IWEB}.reg_mort").count(),
}
for table_name, expected in expected_counts.items():
    actual = current_table(table_name).count()
    check(
        f"rowcount:{table_name}",
        actual == expected,
        table_name,
        actual,
        f"source_expected={expected}, target_current={actual}",
    )


# Recompute long-form source keys for exact multiselect reconciliation.
inventory = spark.table(f"{bronze_control_schema(TARGET_SCHEMA)}.iweb_value_inventory")
ms_rows = (
    inventory.where(F.col("RAW_VALUE").rlike(r"^\s*\d+\s*-\s*"))
    .where(F.col("SOURCE_TABLE").startswith(f"{IWEB}."))
    .select("SOURCE_TABLE", "FIELD_NAME")
    .distinct()
    .collect()
)
ms_by_table = defaultdict(list)
for row in ms_rows:
    ms_by_table[row.SOURCE_TABLE].append(row.FIELD_NAME)
ms_frames = []
for source_name, fields in sorted(ms_by_table.items()):
    source = spark.table(source_name)
    if source_name.endswith(".reg_cs2010g_followup"):
        latest = Window.partitionBy("EntryId").orderBy(
            F.col("DateLastChanged").desc_nulls_last(), F.col("ADC_UPDT").desc_nulls_last()
        )
        source = source.withColumn("_RN", F.row_number().over(latest)).where(F.col("_RN") == 1).drop("_RN")
    ms_frames.append(
        explode_multiselect(source, source_name.split(".")[-1], "EntryId", sorted(fields)).select(
            "SOURCE_TABLE", "ENTRY_ID", "FIELD_NAME", "POSITION"
        )
    )
expected_ms = reduce(lambda left, right: left.unionByName(right), ms_frames)
expected_ms_count = expected_ms.count()
actual_ms = current_table("iweb_multiselect_value").select(
    "SOURCE_TABLE", "ENTRY_ID", "FIELD_NAME", "POSITION"
)
actual_ms_count = actual_ms.count()
missing_ms = expected_ms.join(actual_ms, TABLE_KEYS["iweb_multiselect_value"], "left_anti").count()
extra_ms = actual_ms.join(expected_ms, TABLE_KEYS["iweb_multiselect_value"], "left_anti").count()
check(
    "rowcount:iweb_multiselect_value",
    expected_ms_count == actual_ms_count and missing_ms == 0 and extra_ms == 0,
    "iweb_multiselect_value",
    actual_ms_count,
    f"source_expected={expected_ms_count}, target_current={actual_ms_count}, missing={missing_ms}, extra={extra_ms}",
)


# Key uniqueness/null checks across current and retained historical-presence rows.
for table_name, keys in TABLE_KEYS.items():
    frame = spark.table(f"{TARGET_SCHEMA}.{table_name}")
    null_condition = reduce(lambda left, right: left | right, [F.col(key).isNull() for key in keys])
    row = frame.agg(
        F.count("*").alias("rows"),
        F.countDistinct(F.struct(*[F.col(key) for key in keys])).alias("distinct_keys"),
        F.sum(F.when(null_condition, 1).otherwise(0)).alias("null_keys"),
    ).first()
    passed = row.rows == row.distinct_keys and int(row.null_keys or 0) == 0
    check(
        f"key_unique:{table_name}",
        passed,
        table_name,
        int(row.rows),
        f"rows={row.rows}, distinct={row.distinct_keys}, null_keys={int(row.null_keys or 0)}, keys={keys}",
    )


# Aggregate linkage quality only; no source identifiers are printed or persisted.
# ERACS linkage matures materially more slowly than its registry landing. Historical
# evidence on 2026-08-05 showed 64-79% linkage through days 8-90 and 99.65%
# after 90 days. Keep the 95% standard on the mature cohort and report all newer
# or undated rows separately so expected alias lag cannot make the release gate
# oscillate as individual records cross a short boundary.
ERACS_LINKAGE_MATURITY_DAYS = 90

for table_name in CLINICAL_TABLES:
    frame = current_table(table_name)
    all_counts = {
        row.LINKAGE_STATUS: int(row["count"])
        for row in frame.groupBy("LINKAGE_STATUS").count().collect()
    }
    all_total = sum(all_counts.values())
    all_conflicts = all_counts.get("CONFLICT", 0)
    all_linked = sum(
        value
        for key, value in all_counts.items()
        if key and key.startswith("MATCHED")
    )
    all_linkage_denominator = all_total - all_conflicts
    all_rate = (
        all_linked / all_linkage_denominator
        if all_linkage_denominator
        else 1.0
    )

    quality_frame = frame
    quality_cohort = "ALL_CURRENT_ROWS"
    recent_counts = None
    if table_name == "iweb_eracs_episode":
        mature_window = (
            F.col("DATE_OF_ENTRY").isNotNull()
            & (
                F.col("DATE_OF_ENTRY")
                <= F.current_timestamp()
                - F.expr(f"INTERVAL {ERACS_LINKAGE_MATURITY_DAYS} DAYS")
            )
            & (F.col("DATE_OF_ENTRY") <= F.current_timestamp())
        )
        quality_frame = frame.where(mature_window)
        recent_frame = frame.where(~mature_window)
        recent_counts = {
            row.LINKAGE_STATUS: int(row["count"])
            for row in recent_frame.groupBy("LINKAGE_STATUS").count().collect()
        }
        quality_cohort = (
            f"DATE_OF_ENTRY_AT_LEAST_{ERACS_LINKAGE_MATURITY_DAYS}_DAYS_OLD"
        )

    counts = {
        row.LINKAGE_STATUS: int(row["count"])
        for row in quality_frame.groupBy("LINKAGE_STATUS").count().collect()
    }
    total = sum(counts.values())
    linked = sum(value for key, value in counts.items() if key and key.startswith("MATCHED"))
    quality_conflicts = counts.get("CONFLICT", 0)
    linkage_denominator = total - quality_conflicts
    threshold = {
        "mediconnect_device": 0.99,
        "iweb_eracs_episode": 0.95,
    }.get(table_name, 0.97)
    rate = linked / linkage_denominator if linkage_denominator else 1.0
    conflict_rate = all_conflicts / all_total if all_total else 1.0
    check(
        f"linkage_rate:{table_name}",
        rate >= threshold,
        table_name,
        rate,
        json.dumps(
            {
                "cohort": quality_cohort,
                "counts": counts,
                "all_current_counts": all_counts,
                "all_current_rate_excluding_conflicts": all_rate,
                "linkage_denominator_excluding_conflicts": linkage_denominator,
                "conflicts_excluded_from_linkage_rate": quality_conflicts,
                "maturity_days": (
                    ERACS_LINKAGE_MATURITY_DAYS
                    if table_name == "iweb_eracs_episode"
                    else None
                ),
            },
            sort_keys=True,
        ),
    )
    check(
        f"linkage_conflicts:{table_name}",
        conflict_rate < 0.005,
        table_name,
        conflict_rate,
        f"conflicts={all_conflicts}, total={all_total}",
    )
    if table_name == "iweb_eracs_episode":
        recent_total = sum((recent_counts or {}).values())
        recent_conflicts = (recent_counts or {}).get("CONFLICT", 0)
        recent_denominator = recent_total - recent_conflicts
        recent_linked = sum(
            value
            for key, value in (recent_counts or {}).items()
            if key and key.startswith("MATCHED")
        )
        recent_rate = (
            recent_linked / recent_denominator
            if recent_denominator
            else 1.0
        )
        check(
            "linkage_grace_cohort:iweb_eracs_episode",
            True,
            table_name,
            recent_rate,
            json.dumps(
                {
                    "maturity_days": ERACS_LINKAGE_MATURITY_DAYS,
                    "cohort": "DATE_OF_ENTRY_NEWER_THAN_MATURITY_WINDOW_OR_UNKNOWN",
                    "counts": recent_counts or {},
                    "rows": recent_total,
                    "linked": recent_linked,
                    "conflicts_excluded_from_rate": recent_conflicts,
                },
                sort_keys=True,
            ),
            severity="INFO",
        )
# Published schemas must not contain direct patient identifiers/contact fields.
bad_columns = []
for table_name in CLINICAL_TABLES:
    for column in spark.table(f"{TARGET_SCHEMA}.{table_name}").columns:
        if is_pii_column(column):
            bad_columns.append(f"{table_name}.{column}")
check("pii_absent", not bad_columns, metric_value=len(bad_columns), detail=bad_columns)


# Parent/child integrity for the landed surgery spine.
orphans = (
    current_table("iweb_cardiac_surgery_procedure").alias("procedure")
    .join(
        current_table("iweb_cardiac_surgery_episode").select(F.col("ENTRY_ID").alias("PARENT_ENTRY_ID")),
        "PARENT_ENTRY_ID",
        "left_anti",
    )
    .count()
)
check("orphans:surgery_procedure", orphans == 0, "iweb_cardiac_surgery_procedure", orphans)


# Known date defects are explicit and cleaned deterministically.
device_quality = current_table("mediconnect_device")
device_total = device_quality.count()
device_valid = device_quality.where(F.col("IMPLANTED_DATE_QUALITY") == "VALID").count()
device_bad_clean = device_quality.where(
    (F.col("IMPLANTED_DATE_QUALITY") != "VALID") & F.col("IMPLANTED_DATE_CLEAN").isNotNull()
).count()
device_valid_rate = device_valid / device_total if device_total else 0.0
check("date_valid_rate:mediconnect", device_valid_rate >= 0.98, "mediconnect_device", device_valid_rate)
check("date_clean_consistency:mediconnect", device_bad_clean == 0, "mediconnect_device", device_bad_clean)

mortality = current_table("iweb_cardiac_mortality_review")
mort_total = mortality.count()
mort_future = mortality.where(F.col("DATE_OF_DEATH_QUALITY") == "FUTURE").count()
mort_bad_clean = mortality.where(
    (F.col("DATE_OF_DEATH_QUALITY") == "FUTURE") & F.col("DATE_OF_DEATH_CLEAN").isNotNull()
).count()
check(
    "date_future_rate:mortality",
    (mort_future / mort_total if mort_total else 1.0) < 0.01,
    "iweb_cardiac_mortality_review",
    mort_future / mort_total if mort_total else 1.0,
    f"future_rows={mort_future}",
)
check("date_clean_consistency:mortality", mort_bad_clean == 0, "iweb_cardiac_mortality_review", mort_bad_clean)

acs = current_table("iweb_acs_transfer")
acs_sentinel = acs.where(F.col("ADMISSION_DATE_QUALITY") == "SENTINEL").count()
acs_future = acs.where(F.col("ADMISSION_DATE_QUALITY") == "FUTURE").count()
check("date_sentinel_report:acs", True, "iweb_acs_transfer", acs_sentinel, severity="INFO")
check(
    "date_future_rate:acs",
    acs_future / acs.count() < 0.01,
    "iweb_acs_transfer",
    acs_future,
)


# Mapping validity. Zero approved iWeb mappings is valid; any approved row must resolve cleanly.
concept = spark.table(OMOP_CONCEPT).select(
    F.col("concept_id").cast("long").alias("CONCEPT_ID"),
    F.col("vocabulary_id").alias("CONCEPT_VOCABULARY"),
    "domain_id", "standard_concept", "invalid_reason",
)
iweb_maps = spark.table(f"{bronze_lookup_schema(TARGET_SCHEMA)}.iweb_value_to_concept").where(
    F.col("MAPPING_STATUS") == "APPROVED"
)
approved_null_concepts = iweb_maps.where(F.col("TARGET_CONCEPT_ID").isNull()).count()
iweb_invalid = (
    iweb_maps.where(F.col("TARGET_CONCEPT_ID").isNotNull())
    .join(concept, F.col("TARGET_CONCEPT_ID") == F.col("CONCEPT_ID"), "left")
    .where(
        F.col("CONCEPT_ID").isNull()
        | (F.col("standard_concept") != "S")
        | F.col("invalid_reason").isNotNull()
        | F.col("TARGET_VOCABULARY").isNull()
        | (F.upper(F.col("TARGET_VOCABULARY")) != F.upper(F.col("CONCEPT_VOCABULARY")))
    )
    .count()
)
check(
    "mapping_concepts_valid:iweb",
    approved_null_concepts == 0 and iweb_invalid == 0,
    "iweb_value_to_concept",
    approved_null_concepts + iweb_invalid,
    f"approved_null_concepts={approved_null_concepts}, invalid_concepts={iweb_invalid}",
)

device_maps = spark.table(f"{bronze_lookup_schema(TARGET_SCHEMA)}.mediconnect_device_type_map")
device_approved_null = device_maps.where(
    (F.col("MAPPING_STATUS") == "APPROVED") & F.col("SNOMED_CONCEPT_ID").isNull()
).count()
device_invalid = (
    device_maps.where(F.col("SNOMED_CONCEPT_ID").isNotNull())
    .join(concept, F.col("SNOMED_CONCEPT_ID") == F.col("CONCEPT_ID"), "left")
    .where(
        F.col("CONCEPT_ID").isNull()
        | (F.col("CONCEPT_VOCABULARY") != "SNOMED")
        | (F.col("domain_id") != "Device")
        | (F.col("standard_concept") != "S")
        | F.col("invalid_reason").isNotNull()
    )
    .count()
)
check(
    "mapping_concepts_valid:mediconnect",
    device_approved_null == 0 and device_invalid == 0,
    "mediconnect_device_type_map",
    device_approved_null + device_invalid,
)

mapping_coverage = current_table("iweb_multiselect_value").agg(
    F.count("*").alias("total"),
    F.sum(F.when(F.col("TARGET_CONCEPT_ID").isNotNull(), 1).otherwise(0)).alias("mapped"),
).first()
coverage_rate = int(mapping_coverage.mapped or 0) / int(mapping_coverage.total or 1)
check(
    "mapping_coverage:iweb",
    True,
    "iweb_multiselect_value",
    coverage_rate,
    f"mapped={int(mapping_coverage.mapped or 0)}, total={int(mapping_coverage.total or 0)}",
    severity="INFO",
)


# Additivity spot-check against existing Cerner implant capture.
implant_table = "4_prod.bronze.map_implant_details"
if table_exists(implant_table):
    implant_columns = set(spark.table(implant_table).columns)
    serial_candidates = [
        name for name in (
            "EFFECTIVE_SERIAL_NUMBER", "GS1_SERIAL_NUMBER", "SERIAL_NUMBER",
            "SOURCE_SERIAL_NUMBER", "IMPLANT_SERIAL_NUMBER",
        ) if name in implant_columns
    ]
    if serial_candidates:
        cerner_frames = [
            spark.table(implant_table).select(
                F.upper(F.regexp_replace(F.col(name).cast("string"), r"[^A-Za-z0-9]", "")).alias("SERIAL")
            )
            for name in serial_candidates
        ]
        cerner_serials = (
            reduce(lambda left, right: left.unionByName(right), cerner_frames)
            .where(F.col("SERIAL") != "")
            .distinct()
        )
        mc_serials = (
            current_table("mediconnect_device")
            .select(F.upper(F.regexp_replace(F.col("SERIAL_NO").cast("string"), r"[^A-Za-z0-9]", "")).alias("SERIAL"))
            .where(F.col("SERIAL") != "")
            .distinct()
        )
        mc_serial_count = mc_serials.count()
        overlap = mc_serials.join(cerner_serials, "SERIAL", "inner").count()
        overlap_rate = overlap / mc_serial_count if mc_serial_count else 0.0
        check(
            "additivity:mediconnect_serial_overlap",
            overlap_rate < 0.05,
            "mediconnect_device",
            overlap_rate,
            f"overlap={overlap}, mediconnect_serials={mc_serial_count}, columns={serial_candidates}",
        )
    else:
        check("additivity:mediconnect_serial_overlap", False, "mediconnect_device", detail="No serial column in map_implant_details")
else:
    check("additivity:mediconnect_serial_overlap", False, "mediconnect_device", detail="map_implant_details missing")


# On the explicit second run, every snapshot merge must be a no-op.
if EXPECT_IDEMPOTENT:
    for table_name in CLINICAL_TABLES:
        metrics = _latest_operation_metrics(f"{TARGET_SCHEMA}.{table_name}")
        inserted = int(metrics.get("numTargetRowsInserted", 0) or 0)
        updated = int(metrics.get("numTargetRowsUpdated", 0) or 0)
        deleted = int(metrics.get("numTargetRowsDeleted", 0) or 0)
        check(
            f"idempotent:{table_name}",
            metrics.get("operation") == "MERGE" and inserted == 0 and updated == 0 and deleted == 0,
            table_name,
            inserted + updated + deleted,
            json.dumps(metrics, sort_keys=True, default=str),
        )
else:
    check("idempotency_requested", True, detail="expect_idempotent=false", severity="INFO")


# Persist results before raising so failed releases remain auditable.
result_schema = T.StructType([
    T.StructField("RUN_ID", T.StringType(), False),
    T.StructField("TARGET_SCHEMA", T.StringType(), False),
    T.StructField("CHECK_NAME", T.StringType(), False),
    T.StructField("TABLE_NAME", T.StringType(), True),
    T.StructField("PASSED", T.BooleanType(), False),
    T.StructField("SEVERITY", T.StringType(), False),
    T.StructField("METRIC_VALUE", T.DoubleType(), True),
    T.StructField("DETAIL", T.StringType(), True),
])
result_rows = [
    (
        REGISTRY_RUN_ID, TARGET_SCHEMA, row["check_name"], row["table_name"], row["passed"],
        row["severity"], row["metric_value"], row["detail"],
    )
    for row in RESULTS
]
results_df = spark.createDataFrame(result_rows, result_schema).withColumn("RUN_AT", F.current_timestamp())
validation_table = f"{bronze_control_schema(TARGET_SCHEMA)}.registry_validation_results"
writer = results_df.write.format("delta").mode("append")
if not table_exists(validation_table):
    writer = writer.option("delta.enableChangeDataFeed", "true")
writer.saveAsTable(validation_table)
apply_comments(
    validation_table,
    {
        "DETAIL": "Aggregate validation detail only; no source identifiers are logged.",
        "SEVERITY": "ERROR gates release; INFO is persisted reporting only.",
    },
    "Persisted registry bronze validation results, one row per check and pipeline run.",
)

failures = [
    row for row in RESULTS if row["severity"] == "ERROR" and not row["passed"]
]
if failures:
    raise AssertionError(
        "REGISTRY VALIDATION FAILED: " + json.dumps(
            [{"check": row["check_name"], "table": row["table_name"], "detail": row["detail"]} for row in failures],
            sort_keys=True,
        )
    )
print(f"[VALIDATION] {len(RESULTS)} checks passed or reported; run_id={REGISTRY_RUN_ID}")

# COMMAND ----------

_registry_summary = {
    "status": "SUCCESS",
    "pipeline": "registry_pipeline",
    "run_id": REGISTRY_RUN_ID,
    "target_schema": TARGET_SCHEMA,
    "expect_idempotent": EXPECT_IDEMPOTENT,
    "clinical_tables": list(TABLE_KEYS),
    "validation_checks": len(RESULTS),
}
print(json.dumps(_registry_summary, sort_keys=True))
dbutils.notebook.exit(json.dumps(_registry_summary, sort_keys=True))

