"""Silver _common.spine_and_reference: 0 dataset(s).

- shared helpers (no datasets)
- moved verbatim from: 1_silver_spine_and_reference:187, 1_silver_spine_and_reference:1888, 1_silver_spine_and_reference:1917, 1_silver_spine_and_reference:198, 1_silver_spine_and_reference:2018, 1_silver_spine_and_reference:203, 1_silver_spine_and_reference:2037, 1_silver_spine_and_reference:2039

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from pyspark.sql.window import Window
import pyspark.sql.functions as F
from silver_core.journey_core import (
    SNOMED_URI,
    _n,
    canonical_coding_system,
    read_source,
    spark,
    stable_id,
    subject_key_with_system,
)
from silver_tables._common.journey import _transfusion_canonical


from silver_core.table_spec import ACTIVE

materialized_view = ACTIVE.materialized_view
dp = ACTIVE


# Naming the source a row came from

# ==== Naming the source a row came from ====
SOURCE_SYSTEM_DISPLAY = {
    "millennium": "Millennium",
    "millennium-pm": "Millennium (patient management)",
    "millennium-scheduling": "Millennium (scheduling)",
    "surginet": "Millennium SurgiNet",
    "luna": "LUNA",
    "jac": "JAC",
    "laboratory": "WinPath",
    "sectra-pacs": "Sectra PACS",
}


def _source_system_display(token_col):
    # Translate a source-system token to its configured display name, with a readable fallback.
    mapping = F.create_map(*[F.lit(x) for kv in SOURCE_SYSTEM_DISPLAY.items() for x in kv])
    return F.coalesce(mapping[token_col], F.initcap(token_col))


def _source_object_display(table_col):
    # Turn a source table's leaf name into a display label by removing fixture and source
    # prefixes.
    tail = F.element_at(F.split(table_col, r"\."), -1)
    unfixtured = F.regexp_replace(tail, r"_s\d+$", "")
    unprefixed = F.regexp_replace(unfixtured, r"^(map_|mill_|ancil_)", "")
    return F.regexp_replace(unprefixed, "_", " ")


# Events /  event index
def _event_index_projection(
    df,
    event_type,
    fact_table,
    fact_category,
    source_system=None,
    source_object=None,
    source_row_key=None,
):
    # Project a typed fact into the common patient-event index, including its source and storage
    # references.
    sys_col = _source_system_display(F.col("_source_system")) if source_system is None else source_system
    obj_col = _source_object_display(F.col("_source_table")) if source_object is None else source_object
    key_col = F.col("_source_row_id").cast("string") if source_row_key is None else source_row_key
    # contract v2: carry the v2 event key and derive each typed-fact row key from that same immutable key
    return df.select(
        "patient_event_key", "subject_key", "subject_id_system", "person_id", "identity_status",
        "encounter_id", "event_datetime", "event_end_datetime", "event_datetime_local", "event_end_datetime_local",
        # TZ_SILVER_LOCAL_V1: Europe/London companions (bronze *_LOCAL for Millennium; local-clock sources unchanged)
        F.lit(event_type).alias("event_type"),
        F.lit(fact_category).alias("fact_category"),
        sys_col.alias("source_system"), obj_col.alias("source_object"),
        key_col.alias("source_row_key"),
        "source_coding_system", "source_code", "source_display",
        "record_status", "confidentiality_code", "vip_ind", "withheld_identity_ind",
        F.lit(fact_table).alias("fact_table"), F.col("patient_event_key").alias("fact_row_key"),
        "load_batch_id", "loaded_at",
    )


def _s3b_public_event_index_projection(df, event_type, fact_table, axis, source_object):
    """Project component and registry rows that have no canonical event-index row."""
    columns = set(df.columns)

    def optional(name, data_type):
        # Return a typed source column when present, or a typed null for a missing optional
        # field.
        return F.col(name).cast(data_type) if name in columns else F.lit(None).cast(data_type)

    source_object_col = optional("source_object", "string")
    source_system_col = optional("source_system", "string")
    source_row_key = (
        optional("source_patient_event_key", "string")
        if "source_patient_event_key" in columns
        else F.col("patient_event_key").cast("string")
    )
    source_coding_system = (
        optional("source_coding_system", "string")
        if "source_coding_system" in columns
        else optional(f"{axis}_source_system", "string")
    )
    source_code = (
        optional("source_code", "string")
        if "source_code" in columns
        else optional(f"{axis}_source_code", "string")
    )
    source_display = (
        optional("source_display", "string")
        if "source_display" in columns
        else optional(f"{axis}_source_display", "string")
    )
    return df.select(
        F.col("patient_event_key").cast("string").alias("patient_event_key"),
        optional("subject_key", "string").alias("subject_key"),
        optional("subject_id_system", "string").alias("subject_id_system"),
        optional("person_id", "bigint").alias("person_id"),
        F.when(optional("person_id", "bigint").isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        optional("encounter_id", "string").alias("encounter_id"),
        optional("event_datetime", "timestamp").alias("event_datetime"),
        optional("event_end_datetime", "timestamp").alias("event_end_datetime"),
        optional("event_datetime_local", "timestamp").alias("event_datetime_local"),
        optional("event_end_datetime_local", "timestamp").alias("event_end_datetime_local"),
        F.lit(event_type).alias("event_type"), F.lit("clinical").alias("fact_category"),
        F.coalesce(source_system_col, F.lit("S3b Silver component")).alias("source_system"),
        F.coalesce(source_object_col, F.lit(source_object)).alias("source_object"),
        source_row_key.alias("source_row_key"),
        source_coding_system.alias("source_coding_system"), source_code.alias("source_code"),
        source_display.alias("source_display"), optional("record_status", "string").alias("record_status"),
        optional("confidentiality_code", "string").alias("confidentiality_code"),
        optional("vip_ind", "boolean").alias("vip_ind"),
        optional("withheld_identity_ind", "boolean").alias("withheld_identity_ind"),
        F.lit(fact_table).alias("fact_table"),
        F.col("patient_event_key").cast("string").alias("fact_row_key"),
        optional("load_batch_id", "string").alias("load_batch_id"),
        optional("loaded_at", "timestamp").alias("loaded_at"),
    )


# Iweb target system
def _iweb_target_system(vocabulary):
    # Map an iWeb vocabulary label to its coding-system URI, preserving unknown vocabularies in
    # an iWeb namespace.
    return (
        F.when(vocabulary == "SNOMED", F.lit("http://snomed.info/sct"))
        .when(vocabulary == "OPCS4", F.lit("http://fhir.hl7.org.uk/CodeSystem/OPCS-4"))
        .when(vocabulary == "ICD10", F.lit("http://hl7.org/fhir/sid/icd-10"))
        .when(vocabulary == "LOINC", F.lit("http://loinc.org"))
        .otherwise(F.concat(F.lit("urn:iweb:"),
                            F.lower(F.coalesce(vocabulary, F.lit("unmapped")))))
    )


# Events /  event map
SRC_CODED_EVENT_FINDING_MAPPING = "4_prod.bronze.map_coded_events_omop_bridge"


SRC_IWEB_MULTISELECT = "4_prod.bronze.iweb_multiselect_value"


S3_EVENT_AXIS_SOURCES = [
    ("journey_clinical.presenting_complaint", "complaint"),
    ("journey_clinical.condition", "condition"),
    ("journey_clinical.family_history", "condition"),
    ("journey_clinical.allergy_intolerance", "substance"),
    ("journey_clinical.cancer_treatment", "drug"),
    ("journey_clinical.clinical_finding", "finding"),
    ("journey_clinical.clinical_finding", "value"),
    ("journey_clinical.clinical_score", "score"),
    ("journey_clinical.condition_stage", "stage_diagnosis"),
    ("journey_clinical.condition_stage", "stage_group"),
    ("journey_clinical.device", "device"),
    ("journey_clinical.endoscopy_finding", "finding"),
    ("journey_clinical.imaging_exam", "exam"),
    ("journey_clinical.medication_admin", "medication"),
    ("journey_clinical.medication_dispense", "medication"),
    ("journey_clinical.medication_order", "medication"),
    ("journey_clinical.pathology_result", "test"),
    ("journey_clinical.pathology_result", "result"),
    ("journey_clinical.procedure", "procedure"),
    ("journey_clinical.procedure", "device"),
    ("journey_clinical.specimen", "specimen_type"),
    ("journey_clinical.vital_sign", "vital"),
    ("journey_clinical.vital_sign", "unit"),
    ("journey_clinical.vital_sign", "method"),
    ("journey_clinical.vital_sign", "body_site"),
    ("journey_clinical.vital_sign", "interpretation"),
    ("journey_clinical.clinical_score_component", "component"),
    ("journey_clinical.form_response", "question"),
    ("journey_clinical.form_response", "answer"),
    ("journey_clinical.form_response", "unit"),
    ("journey_clinical.form_smoking_status", "smoking_status"),
    ("journey_clinical.form_smoking_status", "cessation_advice"),
    ("journey_clinical.form_smoking_status", "cessation_referral"),
    ("journey_clinical.form_smoking_status", "nicotine_treatment"),
    ("journey_clinical.form_smoking_status", "dependence_level"),
]


def _s3_axis_event_map(axis_sources=None):
    """Carry the selected flat SNOMED targets into the one-to-many event child.

    `axis_sources` limits the (relation, axis) pairs; a feed passes its own (EIR_20261021_V1).
    """
    frames = []
    for relation, axis in (S3_EVENT_AXIS_SOURCES if axis_sources is None else axis_sources):
        source = spark.read.table(_n(relation))
        code = F.col(f"{axis}_snomed_code")
        method = F.col(f"{axis}_map_method")
        rule = F.col(f"{axis}_map_rule_id")
        version = F.col(f"{axis}_map_version")
        confidence = F.col(f"{axis}_map_confidence")
        candidates = F.col(f"{axis}_map_candidate_count")
        cosine = F.col(f"{axis}_map_cosine")
        scoring_model = F.col(f"{axis}_map_scoring_model")
        status = F.col(f"{axis}_map_status")
        competing = F.col(f"{axis}_map_competing_count")
        candidate_set = F.col(f"{axis}_map_candidate_set_id")
        component_index = F.col(f"{axis}_map_component_index")
        component_count = F.col(f"{axis}_map_component_count")
        rollup_levels = F.col(f"{axis}_map_rollup_levels")
        provenance = F.concat(
            F.lit("s3_axis:"),
            F.to_json(F.struct(method.alias("method"), confidence.alias("confidence"),
                               cosine.alias("cosine"), scoring_model.alias("scoring_model"),
                               status.alias("status"), rule.alias("rule"),
                               candidates.alias("candidate_count"), competing.alias("competing_count"),
                               candidate_set.alias("candidate_set_id"), component_index.alias("component_index"),
                               component_count.alias("component_count"), rollup_levels.alias("rollup_levels"))),
        )
        frames.append(source.where(code.isNotNull()).select(
            stable_id("patient_event_map:s3_axis", F.col("patient_event_key"), F.lit(axis), code, rule).alias("patient_event_map_id"),
            "patient_event_key", F.lit(SNOMED_URI).alias("mapped_coding_system"),
            code.cast("string").alias("mapped_code"), F.col(f"{axis}_snomed_display").alias("mapped_display"),
            F.lit(axis).alias("target_domain"), provenance.alias("map_source"),
            version.cast("string").alias("map_version"), F.lit(None).cast("timestamp").alias("valid_from"),
            F.lit(None).cast("timestamp").alias("valid_to"), F.col("loaded_at"),
        ))
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out.dropDuplicates(["patient_event_map_id"])


# contract v2: carry patient_event_key through every internal coding-map projection
def _map_projection(df, code_col, display_col, system, target_domain, map_source):
    # Create patient-event mapping rows for non-null mapped codes in a fixed target domain.
    filtered = df.where(F.col(code_col).isNotNull())
    return filtered.select(
        stable_id(
            "patient_event_map",
            F.col("patient_event_key"), F.lit(system), F.col(code_col),
            F.lit(target_domain), F.lit(map_source), F.lit(None),
        ).alias("patient_event_map_id"),
        "patient_event_key",
        F.lit(system).alias("mapped_coding_system"),
        F.col(code_col).cast("string").alias("mapped_code"),
        F.col(display_col).alias("mapped_display"),
        F.lit(target_domain).alias("target_domain"),
        F.lit(map_source).alias("map_source"),
        F.lit(None).cast("string").alias("map_version"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        "loaded_at",
    )


def _transfusion_blood_group_event_map():
    """Publish unit mapping plus a distinct patient mapping at event-map grain."""
    transfusion = _transfusion_canonical()
    unit = _map_projection(
        transfusion, "unit_group_concept_id", "_unit_group_concept_name",
        "http://snomed.info/sct", "observation",
        "lookup.bloodtrack_blood_group_map:applied",
    )
    distinct_patient = transfusion.where(
        F.col("patient_group_concept_id").isNotNull()
        & ~F.col("patient_group_concept_id").eqNullSafe(F.col("unit_group_concept_id"))
    )
    patient = _map_projection(
        distinct_patient, "patient_group_concept_id", "_patient_group_concept_name",
        "http://snomed.info/sct", "observation",
        "lookup.bloodtrack_blood_group_map:applied",
    )
    return unit.unionByName(patient)


def _dynamic_map_projection(df, code_col, display_col, system, domain_col, map_source):
    # Create patient-event mapping rows using each row's mapped target domain, defaulting to
    # observation.
    filtered = df.where(F.col(code_col).isNotNull())
    domain = F.lower(F.coalesce(F.col(domain_col), F.lit("observation")))
    return filtered.select(
        stable_id(
            "patient_event_map", F.col("patient_event_key"), F.lit(system), F.col(code_col),
            domain, F.lit(map_source), F.lit(None),
        ).alias("patient_event_map_id"),
        "patient_event_key", F.lit(system).alias("mapped_coding_system"),
        F.col(code_col).cast("string").alias("mapped_code"),
        F.col(display_col).alias("mapped_display"), domain.alias("target_domain"),
        F.lit(map_source).alias("map_source"), F.lit(None).cast("string").alias("map_version"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"), "loaded_at",
    )


def _luna_code_event_map(fact_df, code_col, source_system_urn):
    # Join LUNA facts to source-system-specific terminology mappings and retain the mapping rule
    # evidence.
    m = (
        read_source(SRC_LUNA_CODE_MAPPING)
        .where(F.col("source_coding_system") == F.lit(source_system_urn))
        .select(
            F.col("source_code").alias("_m_source_code"),
            F.col("target_coding_system").alias("_m_target_system"),
            F.col("target_code").alias("_m_target_code"),
            F.col("target_display").alias("_m_target_display"),
            F.col("target_domain").alias("_m_target_domain"),
            F.col("mapping_rule_id").alias("_m_rule"),
        )
    )
    facts = fact_df.where(F.col(code_col).isNotNull()).select(
        "patient_event_key", F.col(code_col).alias("_f_code"),
        "loaded_at",
    )
    joined = facts.join(m, facts["_f_code"] == m["_m_source_code"], "inner")
    map_source = F.concat(F.lit("lookup.luna_code_map:"), F.col("_m_rule"))
    return joined.select(
        stable_id(
            "patient_event_map", F.col("patient_event_key"), F.col("_m_target_system"),
            F.col("_m_target_code"), F.col("_m_target_domain"), map_source,
            F.lit(None),
        ).alias("patient_event_map_id"),
        "patient_event_key",
        F.col("_m_target_system").alias("mapped_coding_system"),
        F.col("_m_target_code").cast("string").alias("mapped_code"),
        F.col("_m_target_display").alias("mapped_display"),
        F.col("_m_target_domain").alias("target_domain"),
        map_source.alias("map_source"),
        F.lit(None).cast("string").alias("map_version"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        "loaded_at",
    )


def _seed_code_event_map(fact_df, code_col, source_system_urn, rule_id):
    # Join facts to seed mappings restricted to the requested source coding system and mapping
    # rule.
    m = (
        read_source(SRC_SEED_CODE_MAPPING)
        .where(
            (F.col("source_coding_system") == F.lit(source_system_urn))
            & (F.col("mapping_rule_id") == F.lit(rule_id))
        )
        .select(
            F.col("source_code").alias("_m_source_code"),
            F.col("target_coding_system").alias("_m_target_system"),
            F.col("target_code").alias("_m_target_code"),
            F.col("target_display").alias("_m_target_display"),
            F.col("target_domain").alias("_m_target_domain"),
            F.col("mapping_rule_id").alias("_m_rule"),
        )
    )
    facts = fact_df.where(F.col(code_col).isNotNull()).select(
        "patient_event_key", F.col(code_col).alias("_f_code"),
        "loaded_at",
    )
    joined = facts.join(m, facts["_f_code"] == m["_m_source_code"], "inner")
    map_source = F.concat(F.lit("lookup.seed_code_map:"), F.col("_m_rule"))
    return joined.select(
        stable_id(
            "patient_event_map", F.col("patient_event_key"), F.col("_m_target_system"),
            F.col("_m_target_code"), F.col("_m_target_domain"), map_source,
            F.lit(None),
        ).alias("patient_event_map_id"),
        "patient_event_key",
        F.col("_m_target_system").alias("mapped_coding_system"),
        F.col("_m_target_code").cast("string").alias("mapped_code"),
        F.col("_m_target_display").alias("mapped_display"),
        F.col("_m_target_domain").alias("target_domain"),
        map_source.alias("map_source"),
        F.lit(None).cast("string").alias("map_version"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        "loaded_at",
    )


def _coded_bridge_event_map():
    # Project the coded-event finding bridge into patient-event mapping rows with its target
    # domain.
    b = read_source(SRC_CODED_EVENT_FINDING_MAPPING).alias("b")
    event_id = stable_id("coded_event:mill", b.SOURCE_ROW_KEY)
    domain = F.lower(F.coalesce(b.OMOP_CONCEPT_DOMAIN, F.lit("observation")))
    return b.where(b.OMOP_CONCEPT_ID.isNotNull()).select(
        stable_id(
            "patient_event_map:coded_bridge", event_id, b.OMOP_CONCEPT_ID,
            b.MAPPING_RULE_ID, b.MAPPING_RANK, b.SOURCE_FIELD, F.lit(None),
        ).alias("patient_event_map_id"),
        event_id.alias("patient_event_key"),
        F.lit("urn:omop:concept_id").alias("mapped_coding_system"),
        b.OMOP_CONCEPT_ID.cast("string").alias("mapped_code"),
        b.OMOP_CONCEPT_NAME.alias("mapped_display"), domain.alias("target_domain"),
        F.lit("bronze.map_coded_events_omop_bridge").alias("map_source"),
        F.lit(None).cast("string").alias("map_version"),
        b.OMOP_VALID_START_DATE.cast("timestamp").alias("valid_from"),
        b.OMOP_VALID_END_DATE.cast("timestamp").alias("valid_to"),
        F.coalesce(b.PIPELINE_UPDT_DT_TM, b.MAPPING_ADC_UPDT).alias("loaded_at"),
    )


def _iweb_multiselect_event_map():
    # Expand mapped iWeb multiselect answers into patient-event mapping rows while retaining
    # vocabulary and rule evidence.
    v = read_source(SRC_IWEB_VALUE_MAP).where(
        F.col("TARGET_CODE").isNotNull() | F.col("TARGET_CONCEPT_ID").isNotNull()
    ).select(
        F.col("SOURCE_TABLE").alias("_v_table"),
        F.col("FIELD_NAME").alias("_v_field"),
        F.col("CODE").alias("_v_code"),
        F.col("TARGET_VOCABULARY").alias("_v_vocabulary"),
        F.col("TARGET_CODE").alias("_v_target_code"),
        F.col("TARGET_CONCEPT_ID").alias("_v_target_concept_id"),
        F.col("TARGET_CONCEPT_NAME").alias("_v_target_name"),
        F.col("MAPPING_STATUS").alias("_v_mapping_status"),
    )
    m = read_source(SRC_IWEB_MULTISELECT).join(
        v,
        (F.col("SOURCE_TABLE") == F.col("_v_table"))
        & (F.col("FIELD_NAME") == F.col("_v_field"))
        & (F.col("CODE") == F.col("_v_code")),
        "inner",
    )
    family = (
        F.when(F.col("SOURCE_TABLE") == "reg_coronary_subprocedure", F.lit("coronary_lesion"))
        .when(F.col("SOURCE_TABLE").isin("reg_cs2010g_pre1", "reg_cs2010g_pre2",
                                         "reg_cs2010g_post1", "reg_cs2010g_post2"),
              F.lit("surgery_episode"))
        .when(F.col("SOURCE_TABLE") == "reg_cs2010g_subprocedure", F.lit("surgery_procedure"))
        .when(F.col("SOURCE_TABLE") == "reg_cs2010g_followup", F.lit("surgery_followup"))
        .when(F.col("SOURCE_TABLE") == "reg_dghminap", F.lit("acs_transfer"))
        .when(F.col("SOURCE_TABLE") == "reg_eracs", F.lit("eracs_episode"))
        .when(F.col("SOURCE_TABLE") == "reg_mort", F.lit("mortality_review"))
        .when(F.col("SOURCE_TABLE") == "reg_noncoronary", F.lit("noncoronary_procedure"))
        .when(F.col("SOURCE_TABLE").isin("reg_mdt", "reg_ctmdt"), F.lit("cardiac_mdt"))
    )
    registry_type = (
        F.when(F.col("SOURCE_TABLE") == "reg_mdt", F.lit("CORONARY_REVASC"))
        .when(F.col("SOURCE_TABLE") == "reg_ctmdt", F.lit("CT_AORTIC"))
        .otherwise(F.lit("~"))
    )
    m = m.withColumn("_family", family).withColumn("_registry_type", registry_type) \
        .where(F.col("_family").isNotNull())
    target_system = _iweb_target_system(F.col("_v_vocabulary"))
    target_code = F.coalesce(F.col("_v_target_code"),
                             F.col("_v_target_concept_id").cast("string"))
    parent_event_id = stable_id("registry_entry:iweb", F.col("_family"),
                                F.col("_registry_type"), F.col("ENTRY_ID"))
    map_source = F.concat(F.lit("lookup.iweb_value_to_concept:"),
                          F.coalesce(F.col("_v_mapping_status"), F.lit("UNKNOWN")))
    return m.select(
        stable_id("patient_event_map", parent_event_id, target_system, target_code,
                  F.lit("registry"), map_source, F.lit(None))
        .alias("patient_event_map_id"),
        parent_event_id.alias("patient_event_key"), target_system.alias("mapped_coding_system"),
        target_code.alias("mapped_code"), F.col("_v_target_name").alias("mapped_display"),
        F.lit("registry").alias("target_domain"), map_source.alias("map_source"),
        F.lit(None).cast("string").alias("map_version"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.col("ADC_UPDT").alias("loaded_at"),
    ).dropDuplicates(["patient_event_map_id"])


# (EIR_20261021_V1) Feed clustering is declared on each feed MV via cluster_by., exactly as for
# the v1 UNION-backed index (the join-backed MV keeps the same repair pathway).
PATIENT_EVENT_LIFECYCLE_FIELDS = [
    'identity_status',
    'load_batch_id',
]


PATIENT_EVENT_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_patient_event(events, maps):
    # Assemble patient event rows with lifecycle and source evidence from one feed's event rows
    # and mapping rows (EIR_20261021_V1: previously the whole event index and event map).
    e = events.alias("e")
    m = maps.alias("m")
    # contract v2: publish patient-event and row hashes under key names and join the event index on patient_event_key
    return (
        e.join(m, F.col("e.patient_event_key") == F.col("m.patient_event_key"), "left")
        .select(
            F.coalesce(
                F.col("m.patient_event_map_id"),
                stable_id("patient_event:unmapped", F.col("e.patient_event_key")),
            ).alias("patient_event_row_key"),
            F.col("e.patient_event_key").alias("patient_event_key"),
            F.col("e.subject_key").alias("subject_key"),
            F.col("e.subject_id_system").alias("subject_id_system"),
            F.col("e.person_id").alias("person_id"),
            F.col("e.identity_status").alias("identity_status"),
            F.col("e.encounter_id").alias("encounter_id"),
            F.col("e.event_datetime").alias("event_datetime"),
            F.col("e.event_end_datetime").alias("event_end_datetime"),
            F.col("e.event_datetime_local").alias("event_datetime_local"),
            F.col("e.event_end_datetime_local").alias("event_end_datetime_local"),
            F.col("e.event_type").alias("event_type"),
            F.col("e.fact_category").alias("fact_category"),
            F.col("e.fact_table").alias("fact_table"),
            F.col("e.fact_row_key").alias("fact_row_key"),
            F.col("e.source_system").alias("source_system"),
            F.col("e.source_object").alias("source_object"),
            F.col("e.source_row_key").alias("source_row_key"),
            canonical_coding_system(F.col("e.source_coding_system")).alias("source_coding_system"),
            F.col("e.source_code").alias("source_code"),
            F.col("e.source_display").alias("source_display"),
            F.col("e.record_status").alias("record_status"),
            F.col("e.confidentiality_code").alias("confidentiality_code"),
            F.col("e.vip_ind").alias("vip_ind"),
            F.col("e.withheld_identity_ind").alias("withheld_identity_ind"),
            canonical_coding_system(F.col("m.mapped_coding_system")).alias("mapped_coding_system"),
            F.col("m.mapped_code").alias("mapped_code"),
            F.col("m.mapped_display").alias("mapped_display"),
            F.col("m.target_domain").alias("target_domain"),
            F.col("m.map_source").alias("map_source"),
            F.col("m.map_version").alias("map_version"),
            F.coalesce(
                F.col("m.map_method"),
                F.when(F.col("m.mapped_code").isNull(), F.lit(None).cast("string"))
                 .when(canonical_coding_system(F.col("m.mapped_coding_system")) ==
                       canonical_coding_system(F.col("e.source_coding_system")), F.lit("source_native"))
                 .otherwise(F.lit("rule")),
            ).alias("map_method"),
            F.col("m.map_confidence").alias("map_confidence"),
            F.coalesce(F.col("m.map_rule_id"), F.col("m.map_source")).alias("map_rule_id"),
            F.coalesce(
                F.col("m.map_candidate_count"),
                F.when(F.col("m.mapped_code").isNotNull(),
                       F.count(F.col("m.mapped_code")).over(Window.partitionBy(
                           F.col("e.patient_event_key"), F.col("m.mapped_coding_system"), F.col("m.map_source"))))
                 .otherwise(F.lit(0)),
            ).cast("int").alias("map_candidate_count"),
            F.col("e.load_batch_id").alias("load_batch_id"),
            F.col("e.loaded_at").alias("loaded_at"),
            # EIR_20261021_V1: the mapping row's own clock (null on unmapped rows), so
            # concept_registry can keep timestamping mapped codes by their mapping.
            F.col("m.loaded_at").alias("map_loaded_at"),
        )
    )


def _with_s3_map_columns(out):
    """Derive map_method/confidence/rule_id/candidate_count from the s3_axis JSON provenance.

    Moved verbatim out of the retired _event_map MV so each event feed applies it (EIR_20261021_V1).
    """
    s3_json = F.regexp_replace(F.col("map_source"), r"^s3_axis:", "")
    is_s3 = F.col("map_source").startswith("s3_axis:")
    return (out.withColumn("map_method", F.when(is_s3, F.get_json_object(s3_json, "$.method")))
               .withColumn("map_confidence", F.when(is_s3, F.expr("try_cast(get_json_object(regexp_replace(map_source, '^s3_axis:', ''), '$.confidence') as double)")))
               .withColumn("map_rule_id", F.when(is_s3, F.get_json_object(s3_json, "$.rule")))
               .withColumn("map_candidate_count", F.when(is_s3, F.expr("try_cast(get_json_object(regexp_replace(map_source, '^s3_axis:', ''), '$.candidate_count') as int)"))))


def _feed_supplement(spec):
    """S3 split-component (and registry) event rows for one feed, deduplicated on the event key.

    Same rows as the old _s3b_event_index_supplement, restricted to this feed's fact table.
    """
    frames = []
    for relation, event_type, fact_table, axis, source_object in spec["supplement"]:
        source = spark.read.table(_n(relation))
        if "source_patient_event_key" not in source.columns:
            continue
        components = source.where(
            ~F.col("patient_event_key").eqNullSafe(F.col("source_patient_event_key"))
        )
        frames.append(_s3b_public_event_index_projection(
            components, event_type, fact_table, axis, source_object
        ))
    if spec["registry_findings"]:
        registry_findings = spark.read.table(_n("journey_clinical.clinical_finding")).where(
            F.col("source_object").startswith("registry:")
        )
        frames.append(_s3b_public_event_index_projection(
            registry_findings, "clinical_finding", "journey_clinical.clinical_finding",
            "value", "registry_observation"
        ))
    if not frames:
        return None
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out.dropDuplicates(["patient_event_key"])


def _feed_events(spec):
    """Event-grain rows for one feed: its canonical lanes plus its S3 supplement rows."""
    lanes = spec["lanes"]()
    out = lanes[0]
    for lane in lanes[1:]:
        out = out.unionByName(lane)
    supplement = _feed_supplement(spec)
    return out if supplement is None else out.unionByName(supplement)


def _feed_s3_axis_maps(spec):
    """Flat S3 SNOMED targets of this feed's relations, exactly as the old _s3_axis_event_map."""
    if not spec["s3_axes"]:
        return None
    return _s3_axis_event_map(spec["s3_axes"])


def _feed_maps(spec):
    """Event x mapping rows for one feed, with the derived S3 map_* columns of the old _event_map."""
    frames = spec["maps"]()
    s3 = _feed_s3_axis_maps(spec)
    if s3 is not None:
        frames = frames + [s3]
    if not frames:
        return None
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return _with_s3_map_columns(out)


def _empty_feed_maps():
    """A typed, row-free mapping frame for feeds that have no mapping arms.

    Built from a real Delta table outside the pipeline with a false predicate (not
    createDataFrame), so the feed stays incrementally refreshable and gains no dependency edge.
    """
    return _with_s3_map_columns(
        spark.read.table(SRC_CODED_EVENT_FINDING_MAPPING).where(F.lit(False)).select(
            F.lit(None).cast("string").alias("patient_event_map_id"),
            F.lit(None).cast("string").alias("patient_event_key"),
            F.lit(None).cast("string").alias("mapped_coding_system"),
            F.lit(None).cast("string").alias("mapped_code"),
            F.lit(None).cast("string").alias("mapped_display"),
            F.lit(None).cast("string").alias("target_domain"),
            F.lit(None).cast("string").alias("map_source"),
            F.lit(None).cast("string").alias("map_version"),
            F.lit(None).cast("timestamp").alias("valid_from"),
            F.lit(None).cast("timestamp").alias("valid_to"),
            F.lit(None).cast("timestamp").alias("loaded_at"),
        )
    )


def _declare_event_feed(feed, spec):
    """Declare one internal feed MV: this fact table's events_patient_event rows."""
    @materialized_view(
        name=_n(f"journey_events._feed_{feed}"),
        cluster_by=["person_id", "event_datetime"],
        refresh_policy="auto",
    )
    def event_feed():
        # Join the feed's events to its mappings and project the patient-event columns.
        maps = _feed_maps(spec)
        return _lifecycle_source_patient_event(
            _feed_events(spec), maps if maps is not None else _empty_feed_maps()
        ).drop(*PATIENT_EVENT_LIFECYCLE_FIELDS, *PATIENT_EVENT_RETIRED_COLUMNS)
    return event_feed


# Reference / concept map

# ==== Terminology and mapping plane ====
SRC_LUNA_CODE_MAPPING = "3_lookup.omop.luna_code_map"


SRC_SEED_CODE_MAPPING = "3_lookup.omop.seed_code_map"


SRC_IWEB_VALUE_MAP = "3_lookup.omop.iweb_value_to_concept"


# Research enrollment canonical
def _research_enrollment_canonical():
    # Assemble normalized research enrollment rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    s = read_source(SRC_RESEARCH_SUBJECT)
    event_id = stable_id("research_enrollment:cerner", s.PT_PROT_REG_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_RESEARCH_SUBJECT, s.PT_PROT_REG_ID
    )
    # contract v2: publish the native Cerner registration id and native person/encounter ids while retaining SHA event and study keys
    return s.select(
        event_id.alias("patient_event_key"),
        s.PT_PROT_REG_ID.cast("bigint").alias("pt_prot_reg_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"), F.lit("resolved").alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.ON_STUDY_DT_TM_CLEAN.alias("event_datetime"),
        s.OFF_STUDY_DT_TM_CLEAN.alias("event_end_datetime"),
        s.ON_STUDY_DT_TM_CLEAN_LOCAL.alias("event_datetime_local"),
        s.OFF_STUDY_DT_TM_CLEAN_LOCAL.alias("event_end_datetime_local"),
        F.lit("urn:cerner:research-status").alias("source_coding_system"),
        s.STATUS_ENUM.cast("long").cast("string").alias("source_code"),
        s.STATUS_DESC.alias("source_display"),
        stable_id("research_study:cerner", s.PROT_MASTER_ID).alias("research_study_key"),
        s.REG_ID.cast("string").alias("registration_id"),
        s.PROT_ACCESSION_NBR.alias("protocol_accession_number"),
        s.PROT_ARM_ID.cast("string").alias("protocol_arm_id"),
        s.STATUS_ENUM.cast("long").alias("status_code"), s.STATUS_DESC.alias("status_desc"),
        s.OFF_STUDY_DT_TM_CLEAN.alias("off_study_datetime"),
        s.TX_START_DT_TM_CLEAN.alias("treatment_start_datetime"),
        s.TX_COMPLETION_DT_TM_CLEAN.alias("treatment_completion_datetime"),
        s.REMOVAL_REASON_DESC_CV.alias("removal_reason_desc"),
        s.REMOVAL_REASON_FT.alias("removal_reason_text"),
        s.REASON_OFF_TX_DESC_CV.alias("off_treatment_reason_desc"),
        s.REASON_OFF_TX_FT.alias("off_treatment_reason_text"),
        s.EPISODE_ID.cast("string").alias("source_episode_id"),
        s.ENROLLING_ORGANIZATION_ID.cast("string").alias("enrolling_organization_id"),
        F.lit("active").alias("record_status"),
        s.ON_STUDY_DT_TM_CLEAN.alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("research_subject").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.PIPELINE_UPDT_DT_TM.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("cerner-research").alias("_source_system"),
        F.lit(SRC_RESEARCH_SUBJECT).alias("_source_table"),
        s.PT_PROT_REG_ID.cast("string").alias("_source_row_id"),
    )


# Clinical / research enrollment
SRC_RESEARCH_SUBJECT = "4_prod.bronze.map_research_subject"


# contract v2: retain the full canonical shape for internal reuse.
RESEARCH_ENROLLMENT_SOURCE_COLUMNS = [
    "patient_event_key",
    "pt_prot_reg_id",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "event_datetime_local",
    "event_end_datetime_local",
    "source_coding_system",
    "source_code",
    "source_display",
    "research_study_key",
    "registration_id",
    "protocol_accession_number",
    "protocol_arm_id",
    "status_code",
    "status_desc",
    "off_study_datetime",
    "treatment_start_datetime",
    "treatment_completion_datetime",
    "removal_reason_desc",
    "removal_reason_text",
    "off_treatment_reason_desc",
    "off_treatment_reason_text",
    "source_episode_id",
    "enrolling_organization_id",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "fact_category",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
RESEARCH_ENROLLMENT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'pt_prot_reg_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'event_datetime_local',
    'event_end_datetime_local',
    'source_coding_system',
    'source_code',
    'source_display',
    'research_study_key',
    'registration_id',
    'protocol_accession_number',
    'protocol_arm_id',
    'status_code',
    'status_desc',
    'off_study_datetime',
    'treatment_start_datetime',
    'treatment_completion_datetime',
    'removal_reason_desc',
    'removal_reason_text',
    'off_treatment_reason_desc',
    'off_treatment_reason_text',
    'source_episode_id',
    'enrolling_organization_id',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_research_enrollment():
    # Assemble research enrollment rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _research_enrollment_canonical().select(*RESEARCH_ENROLLMENT_SOURCE_COLUMNS)
