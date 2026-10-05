# Databricks notebook source
# _om_id_registry — append-only persistent BIGINT allocation for OMOP models without natural keys.
# Governed admin step run before pipeline updates that may introduce new source rows.
# Spaces: visit_detail, clinical_event, specimen, and genomic extensions. Allocations are never re-minted.
import json

import pyspark.sql.functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "4_prod")
dbutils.widgets.text("meta_schema", "omop_meta")
dbutils.widgets.text("id_space", "visit_detail")
dbutils.widgets.text("src_location_stay", "4_prod.silver.spine_location_stay")
dbutils.widgets.text("src_critical_care_period", "4_prod.silver.clinical_critical_care_period")
dbutils.widgets.text("src_patient_event", "4_prod.silver.events_patient_event")
dbutils.widgets.text("src_concept", "3_lookup.omop.concept")
dbutils.widgets.text("src_concept_relationship", "3_lookup.omop.concept_relationship")
dbutils.widgets.text("lkp_coding_system_priority", "3_lookup.omop.omop_coding_system_priority")
dbutils.widgets.text("src_specimen", "4_prod.silver.clinical_specimen")
dbutils.widgets.text("src_genomic_test", "4_prod.silver.clinical_genomic_test")
dbutils.widgets.text("src_gene_tested", "4_prod.silver.reference_gene_tested")
dbutils.widgets.text("src_genomic_result", "4_prod.silver.clinical_genomic_result")
dbutils.widgets.text("src_era_keys", "4_prod.omop_silver._om_era_keys")
dbutils.widgets.text("src_person_address", "4_prod.silver.reference_person_address")
dbutils.widgets.text("study_start", "2012-01-01")
dbutils.widgets.text("pipeline_version", "0.6.0-r0")
CATALOG = dbutils.widgets.get("catalog").strip()
META_SCHEMA = dbutils.widgets.get("meta_schema").strip()
ID_SPACE = dbutils.widgets.get("id_space").strip()
SRC_LOCATION_STAY = dbutils.widgets.get("src_location_stay").strip()
SRC_CC_PERIOD = dbutils.widgets.get("src_critical_care_period").strip()
SRC_PATIENT_EVENT = dbutils.widgets.get("src_patient_event").strip()
SRC_CONCEPT = dbutils.widgets.get("src_concept").strip()
SRC_CONCEPT_REL = dbutils.widgets.get("src_concept_relationship").strip()
LKP_PRIORITY = dbutils.widgets.get("lkp_coding_system_priority").strip()
SRC_SPECIMEN = dbutils.widgets.get("src_specimen").strip()
SRC_GENOMIC_TEST = dbutils.widgets.get("src_genomic_test").strip()
SRC_GENE_TESTED = dbutils.widgets.get("src_gene_tested").strip()
SRC_GENOMIC_RESULT = dbutils.widgets.get("src_genomic_result").strip()
SRC_ERA_KEYS = dbutils.widgets.get("src_era_keys").strip()
SRC_PERSON_ADDRESS = dbutils.widgets.get("src_person_address").strip()
# OGR_NO_DEV_V1: direct-adapter sources (allergy, supply, maternity, VTE).
for _w, _d in (("src_allergy", "4_prod.silver.clinical_allergy_intolerance"),
               ("src_medication_supply", "4_prod.silver.clinical_medication_supply"),
               ("src_pregnancy", "4_prod.silver.spine_journey"),
               ("src_pregnancy_history", "4_prod.silver.clinical_pregnancy_history"),
               ("src_birth", "4_prod.silver.clinical_birth"),
               ("src_vte_form", "4_prod.silver.clinical_form_maternity_vte_risk_assessment")):
    dbutils.widgets.text(_w, _d)
SRC_ALLERGY = dbutils.widgets.get("src_allergy").strip()
SRC_MEDICATION_SUPPLY = dbutils.widgets.get("src_medication_supply").strip()
SRC_PREGNANCY = dbutils.widgets.get("src_pregnancy").strip()
SRC_PREGNANCY_HISTORY = dbutils.widgets.get("src_pregnancy_history").strip()
SRC_BIRTH = dbutils.widgets.get("src_birth").strip()
SRC_VTE_FORM = dbutils.widgets.get("src_vte_form").strip()
STUDY_START = dbutils.widgets.get("study_start").strip()
PIPELINE_VERSION = dbutils.widgets.get("pipeline_version").strip()

# OGR_NO_DEV_V1: production allocator; development schemas are not valid sources.
ALLOWED_SOURCE_SCHEMAS = {
    "4_prod.silver", "3_lookup.omop",
    # R0 release: era keys are read from the published production staging MV.
    "4_prod.omop_silver",
}
for name in (
    SRC_LOCATION_STAY,
    SRC_CC_PERIOD,
    SRC_PATIENT_EVENT,
    SRC_CONCEPT,
    SRC_CONCEPT_REL,
    LKP_PRIORITY,
    SRC_SPECIMEN,
    SRC_GENOMIC_TEST,
    SRC_GENE_TESTED,
    SRC_GENOMIC_RESULT,
    SRC_ERA_KEYS,
    SRC_PERSON_ADDRESS,
    SRC_ALLERGY,  # OGR_NO_DEV_V1
    SRC_MEDICATION_SUPPLY,
    SRC_PREGNANCY,
    SRC_PREGNANCY_HISTORY,
    SRC_BIRTH,
    SRC_VTE_FORM,
):
    parts = name.replace("`", "").split(".")
    if len(parts) != 3 or ".".join(parts[:2]).lower() not in ALLOWED_SOURCE_SCHEMAS:
        raise ValueError(f"registry source outside allowlist: {name!r}")

# The production registry moved to 4_prod.omop_meta on 2026-09-26 (deep clone of 8_dev.omop_meta,
# which is now frozen). Allocating into 8_dev.omop_meta would fork the id sequence.
# OMOP_REGISTRY_PROD_TARGET_PATCH_APPLIED v1
if (CATALOG, META_SCHEMA) not in {("4_prod", "omop_meta")}:  # OGR_NO_DEV_V1
    raise ValueError(f"registry write target not approved: {CATALOG}.{META_SCHEMA}")
if CATALOG == "4_prod" and any(n.replace("`", "").startswith("8_dev.") for n in (
        SRC_LOCATION_STAY, SRC_CC_PERIOD, SRC_PATIENT_EVENT, SRC_CONCEPT, SRC_CONCEPT_REL, LKP_PRIORITY,
        SRC_SPECIMEN, SRC_GENOMIC_TEST, SRC_GENE_TESTED, SRC_GENOMIC_RESULT, SRC_ERA_KEYS)):
    raise ValueError("the production registry must not allocate from 8_dev sources")
if ID_SPACE not in {
    "visit_detail", "clinical_event", "specimen",
    "genomic_test", "gene_tested", "genomic_variant",
    "drug_era", "dose_era", "condition_era",
    "episode",  # OGR_NO_DEV_V1
}:
    raise ValueError(f"unknown id_space: {ID_SPACE!r}")

TARGET = f"`{CATALOG}`.`{META_SCHEMA}`.omop_id_registry"
if CATALOG == "4_prod" and not spark.catalog.tableExists(TARGET.replace("`", "")):
    raise RuntimeError(f"{TARGET} is missing: never re-create the production registry (ids would be re-minted)")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
  id_space STRING NOT NULL,
  source_key STRING NOT NULL,
  allocated_id BIGINT NOT NULL,
  allocated_at TIMESTAMP NOT NULL,
  pipeline_version STRING NOT NULL
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true,
               comment = 'Append-only OMOP ID registry; allocations are never re-minted or deleted.')
""")


# R0 Silver contract compatibility; must match omop_model_pipeline SOURCE_COLUMN_COMPAT.
# Silver renamed surrogate *_id columns to *_key (values unchanged) and dropped
# identity_status, so every key recipe below stays byte-identical.
SOURCE_COLUMN_COMPAT = {
    "patient_event_row_id": "patient_event_row_key",
    "patient_event_id": "patient_event_key",
    "location_stay_id": "location_stay_key",
    "gene_tested_row_id": "gene_tested_row_key",
    "genomic_test_id": "genomic_test_key",
}
PERSON_SUBJECT_SYSTEM = "urn:cerner:person_id"

# R0 adapter allowlist; must match omop_model_pipeline ADMITTED_ADAPTERS so the
# registry never allocates keys for deferred (event_type, lane) pairs.
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


def _source(name, extra_aliases=None):
    frame = spark.table(name)
    columns = set(frame.columns)
    aliases = dict(SOURCE_COLUMN_COMPAT)
    aliases.update(extra_aliases or {})
    for legacy, current in aliases.items():
        if legacy not in columns and current in columns:
            frame = frame.withColumn(legacy, F.col(current))
            columns.add(legacy)
    if (
        "identity_status" not in columns
        and "subject_id_system" in columns
        and "person_id" in columns
    ):
        frame = frame.withColumn(
            "identity_status",
            F.when(
                (F.col("subject_id_system") == PERSON_SUBJECT_SYSTEM)
                & F.col("person_id").isNotNull(),
                F.lit("resolved"),
            ).otherwise(F.lit("unresolved")),
        )
    return frame


def _adapter_admitted(event_type_col, lane_col):
    admitted = F.lit(False)
    for event_type, lanes in sorted(ADMITTED_ADAPTERS.items()):
        admitted = admitted | ((event_type_col == event_type) & lane_col.isin(*lanes))
    return admitted


def _visit_detail_keys():
    ward = (
        _source(SRC_LOCATION_STAY)
        .where(F.col("location_stay_id").isNotNull())
        .select(F.concat(F.lit("ward:"), F.col("location_stay_id")).alias("source_key"))
    )
    cc = (
        spark.table(SRC_CC_PERIOD)
        .where(F.col("period_business_key").isNotNull())
        .select(F.concat(F.lit("cc:"), F.col("period_business_key")).alias("source_key"))
    )
    return ward.unionByName(cc).distinct()


def _clinical_event_keys():
    lanes = (
        "condition", "procedure", "observation", "device",
        "registry", "family_history", "community_activity", "measurement", "drug",
    )
    o3_domains = (
        "CONDITION", "PROCEDURE", "OBSERVATION", "DEVICE", "MEASUREMENT", "DRUG",
    )
    direct_id_sources = (
        "bronze.map_implant_details",
        "lookup.bloodtrack_blood_group_map:applied",
        "lookup.bloodtrack_product_group_map:applied",
        "lookup.mediconnect_device_type_map:applied",
    )
    priority = (
        spark.table(LKP_PRIORITY)
        .where(F.col("priority").isNotNull())
        .select(
            F.col("coding_system").alias("_cs"),
            F.col("vocabulary_id").alias("_vocab"),
        )
    )
    concepts = spark.table(SRC_CONCEPT).select(
        F.col("concept_id").alias("_c_id"),
        F.col("vocabulary_id").alias("_c_vocab"),
        F.col("concept_code").alias("_c_code"),
        F.col("standard_concept").alias("_c_std"),
        F.upper(F.col("domain_id")).alias("_c_domain"),
        F.col("invalid_reason").alias("_c_invalid"),
    )
    source_by_code = (
        concepts.groupBy("_c_vocab", "_c_code")
        .agg(
            F.min(
                F.struct(
                    F.when(F.col("_c_invalid").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("invalid_rank"),
                    F.when(F.col("_c_std") == "S", F.lit(0)).otherwise(F.lit(1)).alias("standard_rank"),
                    F.col("_c_id").alias("concept_id"),
                )
            ).alias("_best")
        )
        .select(
            "_c_vocab", "_c_code",
            F.col("_best.concept_id").alias("_source_concept"),
        )
    )
    source_info = concepts.select(
        F.col("_c_id").alias("_si_id"),
        F.col("_c_std").alias("_si_std"),
        F.col("_c_domain").alias("_si_domain"),
        F.col("_c_invalid").alias("_si_invalid"),
    )
    valid_targets = concepts.where(
        F.col("_c_invalid").isNull() & (F.col("_c_std") == "S")
    ).select(
        F.col("_c_id").alias("_target_id"),
        F.col("_c_domain").alias("_target_domain"),
    )
    maps_to = (
        spark.table(SRC_CONCEPT_REL)
        .where((F.col("relationship_id") == "Maps to") & F.col("invalid_reason").isNull())
        .join(valid_targets, F.col("concept_id_2") == F.col("_target_id"), "inner")
        .select(
            F.col("concept_id_1").alias("_map_source_id"),
            F.col("_target_id").alias("_standard"),
            F.col("_target_domain").alias("_domain"),
        )
        .distinct()
    )
    rows = (
        _source(SRC_PATIENT_EVENT)
        .where(F.col("target_domain").isin(*lanes))
        .where(F.col("event_type") != "medication_supply")
        .where(_adapter_admitted(F.col("event_type"), F.col("target_domain")))
        .withColumn("_fact_kind", F.substring_index("fact_table", ".", -1))
        .where(F.col("_fact_kind") != "indication")
        .where(F.col("mapped_code").isNotNull() & (F.trim("mapped_code") != ""))
        .where(F.col("record_status") == "active")
        .where(F.col("identity_status") == "resolved")
        .where(F.col("event_datetime").isNotNull())
        .where(F.to_date("event_datetime") >= F.to_date(F.lit(STUDY_START)))
        .join(priority, F.col("mapped_coding_system") == F.col("_cs"), "inner")
        .withColumn("_trim_code", F.trim("mapped_code"))
        .withColumn(
            "_normalized_code",
            F.when(
                F.col("_vocab").isin("ICD10", "OPCS4")
                & (F.instr(F.col("_trim_code"), ".") == 0)
                & (F.length(F.col("_trim_code")) > 3),
                F.concat(
                    F.substring(F.col("_trim_code"), 1, 3),
                    F.lit("."),
                    F.expr("substring(_trim_code, 4)"),
                ),
            ).otherwise(F.upper(F.col("_trim_code"))),
        )
        .select(
            "patient_event_id", "target_domain", "map_source",
            "mapped_coding_system", "_vocab", "_trim_code", "_normalized_code",
        )
    )
    direct = rows.where(
        (F.col("mapped_coding_system") == "urn:omop:concept_id")
        | (
            (F.col("mapped_coding_system") == "http://snomed.info/sct")
            & F.col("map_source").isin(*direct_id_sources)
        )
    ).select(
        "patient_event_id", "target_domain",
        F.expr("try_cast(_trim_code AS int)").alias("_source_concept"),
    ).where(F.col("_source_concept").isNotNull())
    exact = (
        rows.where(F.col("_vocab").isNotNull())
        .join(
            source_by_code,
            (F.col("_vocab") == F.col("_c_vocab"))
            & (F.col("_trim_code") == F.col("_c_code")),
            "inner",
        )
        .select("patient_event_id", "target_domain", "_source_concept")
    )
    normalized = (
        rows.where(
            F.col("_vocab").isNotNull()
            & (F.col("_normalized_code") != F.col("_trim_code"))
        )
        .join(
            source_by_code,
            (F.col("_vocab") == F.col("_c_vocab"))
            & (F.col("_normalized_code") == F.col("_c_code")),
            "inner",
        )
        .select("patient_event_id", "target_domain", "_source_concept")
    )
    resolved = direct.unionByName(exact).unionByName(normalized).distinct()
    resolved_info = resolved.join(
        source_info, F.col("_source_concept") == F.col("_si_id"), "left"
    )
    already_standard = resolved_info.where(
        F.col("_si_invalid").isNull() & (F.col("_si_std") == "S")
    ).select(
        "patient_event_id", "target_domain",
        F.col("_source_concept").alias("_standard"),
        F.col("_si_domain").alias("_domain"),
    )
    mapped_standard = (
        resolved_info.where(
            F.col("_si_invalid").isNotNull()
            | F.col("_si_std").isNull()
            | (F.col("_si_std") != "S")
        )
        .join(maps_to, F.col("_source_concept") == F.col("_map_source_id"), "inner")
        .select("patient_event_id", "target_domain", "_standard", "_domain")
    )
    placed = already_standard.unionByName(mapped_standard).withColumn(
        "_final_domain",
        F.when(F.col("target_domain") == "family_history", F.lit("OBSERVATION"))
        .otherwise(F.col("_domain")),
    ).where(F.col("_final_domain").isin(*o3_domains))
    events = placed.select(
        F.concat_ws(
            ":", F.lit("evt"), F.col("patient_event_id"),
            F.col("_standard").cast("string"),
        ).alias("source_key")
    ).distinct()
    return events.unionByName(_imd_keys()).unionByName(_allergy_keys()).unionByName(_supply_keys()).unionByName(_stage_keys()).unionByName(_maternity_keys()).unionByName(_vte_keys())  # OGR_NO_DEV_V1


def _episode_keys():
    return (
        _source(SRC_PREGNANCY)
        .where((F.col("journey_type_code") == "pregnancy") & F.col("parent_journey_key").isNull()
               & F.col("journey_key").isNotNull())
        .select(F.concat(F.lit("preg:"), F.col("journey_key")).alias("source_key"))
        .distinct()
    )


def _keyed(frame, prefix, stem, suffixes):
    # One key per (row, suffix) whose value column is non-NULL; suffixes = [(suffix, column), ...].
    parts = [frame.where(F.col(c).isNotNull()).select(
        F.concat(F.lit(prefix), F.col(stem), F.lit(":" + s)).alias("source_key")) for s, c in suffixes]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def _maternity_keys():
    hist = _source(SRC_PREGNANCY_HISTORY).where(
        (F.col("record_status") == "active") & F.col("journey_pregnancy_key").isNotNull())
    births = _source(SRC_BIRTH).where((F.col("record_status") == "active") & F.col("birth_key").isNotNull())
    return _keyed(hist, "pobs:", "journey_pregnancy_key", [("gravida", "gravida"), ("parity", "parity")]).unionByName(
        _keyed(births, "bir:", "birth_key", [
            ("method", "delivery_method_code"), ("outcome", "delivery_outcome_code"),
            ("preg_outcome", "pregnancy_outcome_code"), ("neonatal", "neonatal_outcome_code"),
            ("weight", "birth_weight_grams"), ("apgar1", "apgar_1_minute"), ("apgar5", "apgar_5_minute"),
            ("gestation", "gestation_weeks")])).distinct()


VTE_ITEMS = ['age_35_parity_3', 'ob_vte_obesity', 'obstetric_vte_risk_assessment_type', 'previous_vte', 'smoker', 'current_systemic_infection', 'dehydration_reduced_immobility_art_ivf', 'family_history_of_vte', 'gross_varicose_veins', 'hyperemesis', 'medical_comorbidities', 'ohss_overian_hyperstimulation_syndrome', 'pre_eclampsia', 'surg_procedure_in_this_preg_or_6_weeks', 'vte_known_thrombophilia', 'patient_at_risk_of_vte', 'height_length_measured', 'weight_measured', 'maternity_vte_transient_risk_score', 'caesarean_section_in_labour', 'elective_caesarean_section', 'mid_cavity_or_rotational_forceps', 'pph_1_litre_or_more_and_or_transfusion', 'preterm_birth_this_pregnancy', 'prolonged_labour_over_24_hours', 'still_birth_this_pregnancy', 'multiple_pregnancy', 'maternity_vte_action_plan', 'body_mass_index_measured', 'bmi', 'multiple_pregnancy_twins_or_more', 'maternity_vte_permanent_risk_score', 'contraindication_to_lmwh_or_heparin', 'heparin_type', 'maternity_vte_intermediate_trans_risk', 'medical_comorbidities_type', 'maternity_vte_intermediate_perm_risk', 'antiphospholipid_antibodies', 'factorv_leiden_heterozygous', 'prothrombin_gene_mutation_heterozygous']
VTE_TEXT = ['age_35_parity_3', 'antiphospholipid_antibodies', 'bmi', 'body_mass_index_measured', 'caesarean_section_in_labour', 'contraindication_to_lmwh_or_heparin', 'current_systemic_infection', 'dehydration_reduced_immobility_art_ivf', 'elective_caesarean_section', 'factorv_leiden_heterozygous', 'family_history_of_vte', 'gross_varicose_veins', 'height_length_measured', 'heparin_type', 'hyperemesis', 'maternity_vte_action_plan', 'maternity_vte_intermediate_perm_risk', 'maternity_vte_intermediate_trans_risk', 'maternity_vte_permanent_risk_score', 'maternity_vte_transient_risk_score', 'medical_comorbidities', 'medical_comorbidities_type', 'mid_cavity_or_rotational_forceps', 'multiple_pregnancy', 'multiple_pregnancy_twins_or_more', 'ob_vte_obesity', 'obstetric_vte_risk_assessment_type', 'ohss_overian_hyperstimulation_syndrome', 'patient_at_risk_of_vte', 'pph_1_litre_or_more_and_or_transfusion', 'pre_eclampsia', 'preterm_birth_this_pregnancy', 'previous_vte', 'prolonged_labour_over_24_hours', 'prothrombin_gene_mutation_heterozygous', 'smoker', 'still_birth_this_pregnancy', 'surg_procedure_in_this_preg_or_6_weeks', 'vte_known_thrombophilia', 'weight_measured']


def _vte_keys():
    forms = _source(SRC_VTE_FORM).where((F.col("record_status") == "active") & F.col("patient_event_key").isNotNull())
    stem = F.concat(F.lit("vte:"), F.col("patient_event_key"), F.lit(":"))
    fixed = forms.select(F.explode(F.array(F.concat(stem, F.lit("form")), F.concat(stem, F.lit("total")))).alias("source_key"))
    items = forms.select(stem.alias("_stem"), F.explode(F.array(*[
        F.when(F.col(i).isNotNull() | (F.col(i + "_text").isNotNull() if i in VTE_TEXT else F.lit(False)), F.lit(i))
        for i in VTE_ITEMS])).alias("_item")).where(F.col("_item").isNotNull())
    return fixed.unionByName(items.select(F.concat(F.col("_stem"), F.col("_item")).alias("source_key"))).distinct()


def _stage_keys():
    # Superset: every active stage event (display/mapping/person gates only remove rows).
    return (
        _source(SRC_PATIENT_EVENT)
        .where((F.col("event_type") == "condition_stage") & (F.col("record_status") == "active")
               & F.col("patient_event_id").isNotNull())
        .select(F.concat(F.lit("stg:"), F.col("patient_event_id")).alias("source_key"))
        .distinct()
    )


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


def _supply_keys():
    # Same mapping text as the pipeline adapter; status and person gates only remove rows (superset).
    supply = _source(SRC_MEDICATION_SUPPLY).where(
        (F.col("record_status") == "active") & (F.col("item_status_display") == "COMPLETE")
        & F.col("dmd_vtm_name").isNotNull() & F.col("patient_event_id").isNotNull())
    standards = _supply_standard_map(
        supply, spark.table(SRC_CONCEPT), spark.table(SRC_CONCEPT_REL),
        spark.table(SRC_CONCEPT.rsplit(".", 1)[0] + ".concept_synonym"))
    return (
        standards.where(F.col("_sup_standard").isNotNull())
        .select(F.concat_ws(":", F.lit("sup"), F.col("patient_event_id"),
                            F.col("_sup_standard").cast("string")).alias("source_key"))
        .distinct()
    )


def _allergy_keys():
    # Superset of the pipeline's allergy adapter (status, mapping and person gates only remove rows).
    return (
        _source(SRC_ALLERGY)
        .where((F.col("record_status") == "active") & F.col("patient_event_id").isNotNull())
        .select(F.concat(F.lit("alg:"), F.col("patient_event_id")).alias("source_key"))
        .distinct()
    )


def _imd_keys():
    # HERON-UK IMD observation rows share the clinical_event space. Same address filters as the
    # pipeline's _imd_candidates(); the pipeline's person/period/date/duplicate gates only remove
    # rows, so this is a superset.
    return (
        _source(SRC_PERSON_ADDRESS)
        .where(
            (F.col("parent_entity") == "PERSON")
            & (F.col("active_ind") == 1)
            & F.col("address_type_code").isin("756", "758")
            & F.trim(F.col("imd_quintile")).isin("1", "2", "3", "4", "5")
            & F.col("address_id").isNotNull()
            & F.col("person_id").isNotNull()
        )
        .select(F.concat(F.lit("imd:"), F.col("address_id").cast("bigint").cast("string")).alias("source_key"))
        .distinct()
    )


def _specimen_keys():
    return (
        _source(SRC_SPECIMEN)
        .where(F.col("patient_event_id").isNotNull())
        .select(F.concat(F.lit("spm:"), F.col("patient_event_id")).alias("source_key"))
        .distinct()
    )


def _genomic_test_keys():
    # clinical_genomic_test has NO genomic_test_id column; patient_event_id is its
    # unique key and is what child tables reference as genomic_test_id.
    return (
        _source(SRC_GENOMIC_TEST)
        .where(F.col("patient_event_id").isNotNull())
        .select(F.concat(F.lit("gt:"), F.col("patient_event_id")).alias("source_key"))
        .distinct()
    )


def _gene_tested_keys():
    return (
        _source(SRC_GENE_TESTED)
        .where(F.col("gene_tested_row_id").isNotNull())
        .select(F.concat(F.lit("ggt:"), F.col("gene_tested_row_id")).alias("source_key"))
        .distinct()
    )


def _genomic_variant_keys():
    return (
        _source(SRC_GENOMIC_RESULT, {"fact_row_id": "patient_event_key"})
        .where(F.col("fact_row_id").isNotNull())
        .select(F.concat(F.lit("gv:"), F.col("fact_row_id")).alias("source_key"))
        .distinct()
    )


def _era_keys(kind):
    # The pipeline owns the 30-day era derivation and publishes deterministic
    # keys. The registry only allocates ids; it must not duplicate era logic.
    return (
        spark.table(SRC_ERA_KEYS)
        .where((F.col("era_kind") == kind) & F.col("source_key").isNotNull())
        .select("source_key")
        .distinct()
    )


def _drug_era_keys():
    return _era_keys("drug_era")


def _dose_era_keys():
    return _era_keys("dose_era")


def _condition_era_keys():
    return _era_keys("condition_era")


if ID_SPACE in {"drug_era", "dose_era", "condition_era"} and not spark.catalog.tableExists(
    SRC_ERA_KEYS.replace("`", "")
):
    result = {
        "id_space": ID_SPACE,
        "new_allocations": 0,
        "total": 0,
        "note": "era keys not published yet; run the pipeline first",
    }
    print(json.dumps(result))
    dbutils.notebook.exit(json.dumps(result))


candidate = {
    "visit_detail": _visit_detail_keys,
    "clinical_event": _clinical_event_keys,
    "specimen": _specimen_keys,
    "genomic_test": _genomic_test_keys,
    "gene_tested": _gene_tested_keys,
    "genomic_variant": _genomic_variant_keys,
    "drug_era": _drug_era_keys,
    "dose_era": _dose_era_keys,
    "condition_era": _condition_era_keys,
    "episode": _episode_keys,  # OGR_NO_DEV_V1
}[ID_SPACE]()

existing = spark.table(TARGET).where(F.col("id_space") == ID_SPACE)
new_keys = candidate.join(existing.select("source_key"), "source_key", "left_anti")

base = existing.agg(F.coalesce(F.max("allocated_id"), F.lit(0)).alias("m")).first().m
new_count = new_keys.count()
if new_count:
    # Deterministic numbering inside hash buckets avoids a single global sort.
    buckets = 1024 if ID_SPACE == "visit_detail" else 4096
    bucketed = new_keys.select(
        "source_key",
        F.pmod(F.xxhash64(F.col("source_key")), F.lit(buckets)).cast("int").alias("_bucket"),
    )
    bucket_counts = bucketed.groupBy("_bucket").agg(F.count(F.lit(1)).alias("_n"))
    offset_order = Window.orderBy(F.col("_bucket").asc())
    offsets = bucket_counts.select(
        "_bucket", (F.sum("_n").over(offset_order) - F.col("_n")).alias("_offset")
    )
    within_bucket = Window.partitionBy("_bucket").orderBy(F.col("source_key").asc())
    allocation = (
        bucketed.join(offsets, "_bucket")
        .select(
            F.lit(ID_SPACE).alias("id_space"),
            "source_key",
            (F.row_number().over(within_bucket) + F.col("_offset") + F.lit(base))
            .cast("bigint").alias("allocated_id"),
            F.current_timestamp().alias("allocated_at"),
            F.lit(PIPELINE_VERSION).alias("pipeline_version"),
        )
    )

    from delta.tables import DeltaTable

    target_table = DeltaTable.forName(spark, TARGET.replace("`", ""))
    (
        target_table.alias("t")
        .merge(
            allocation.alias("s"),
            "t.id_space = s.id_space AND t.source_key = s.source_key",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

post = spark.table(TARGET).where(F.col("id_space") == ID_SPACE)
total = post.count()
distinct_keys = post.select("source_key").distinct().count()
distinct_ids = post.select("allocated_id").distinct().count()
if total != distinct_keys or total != distinct_ids:
    raise RuntimeError(
        f"registry integrity violated: rows={total} keys={distinct_keys} ids={distinct_ids}"
    )

result = {
    "id_space": ID_SPACE,
    "new_allocations": new_count,
    "total": total,
    "max_base": base,
    "adapter_allowlist_version": ADAPTER_ALLOWLIST_VERSION,
}
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))

