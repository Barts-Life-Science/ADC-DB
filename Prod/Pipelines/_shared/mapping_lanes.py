"""R1 lane registry and the three per-lane operations every caller shares.

  project(df, table, model)  bronze (or silver-derived) rows -> rows + _r1_bridge, _r1_in_*, _r1_in_scope
  key_frame(projected, lane) rows -> rows + _r1_key_<lane> (NULL outside the arm's scope)
  resolve(inputs, lane)      distinct key inputs -> one row per (map_key, component): the 24 axis fields,
                             extras_json, component_identity (the verbatim S3 chain, mapping_chain)

A lane is one (model, axis) (deviation D3). Key inputs use the projected names (_r1_bridge, _r1_in_*), never the
silver column names, so a key is the same JSON whether bronze or silver computed it (D4).

The chain and the generated projections are imported inside the functions that need them: silver imports this
module for the registry (lanes_of_model, axis, extras) and must not load the mapping chain.
"""
import re
from dataclasses import dataclass

import pyspark.sql.functions as F

import mapping_contract as M


@dataclass(frozen=True)
class Lane:
    """One (model, axis) lane: where its rows come from, what its key hashes, which maps it reads."""
    lane_id: str
    model: str
    axis: str
    start: str                     # "bridge" (JSON flatten) or "direct" (_s3_direct_axis via prepare__<model>)
    key_inputs: tuple
    tables: tuple                  # bronze tables (attach targets)
    silver_sources: tuple = ()     # D4 silver-derived arms (published silver/tmp names)
    input_maps: tuple = ()         # 3_lookup tables whose content hash triggers a full re-resolve of the lane
    extras: tuple = ()             # non-contract columns the chain adds for this lane (carried in extras_json)
    allow_split: bool = False


CONCEPT = ("3_lookup.omop.concept",)
# SCT_TRUD_V1: the RF2 description table, an input of the lanes whose output the concept guard can change (native
# SNOMED and ICD/OPCS arms). Other lanes' SNOMED values come from governed maps whose targets are all concepts.
RF2 = ("3_lookup.trud.snomed_sct",)
PROC_TABLES = ("map_procedure", "map_implant_details", "map_theatre_case_procedure", "map_endobase_exam",
               "map_critical_care_procedure")
FINDING_TABLES = ("map_coded_events", "map_nomen_events", "map_date_events", "map_text_events")
B = "_r1_bridge"
LANES = {lane.lane_id: lane for lane in (
    Lane("condition", "condition", "condition", "bridge",
         (B, "_r1_in__s3_meta_type", "_r1_in__s3_meta_similarity", "_r1_in__s3_meta_candidates"),
         ("map_diagnosis", "map_problem", "map_maternity_diagnosis"),
         input_maps=("3_lookup.omop.icd10_snomed_map", "3_lookup.omop.s3_vocab_maps_to") + CONCEPT + RF2,
         extras=("condition_legacy_snomed_code", "condition_omop_map_rule_id", "condition_omop_map_version"),
         allow_split=True),
    Lane("fhx_condition", "family_history", "condition", "bridge",
         (B, "_r1_in__s3_meta_type", "_r1_in__s3_meta_similarity", "_r1_in__s3_meta_candidates"),
         ("map_family_history",), input_maps=("3_lookup.omop.icd10_snomed_map",) + CONCEPT + RF2,
         extras=("condition_legacy_snomed_code",), allow_split=True),
    Lane("procedure", "procedure", "procedure", "bridge",
         (B, "_r1_in_source_object", "_r1_in__s3_meta_type", "_r1_in__s3_meta_similarity", "_r1_in__s3_meta_candidates"),
         PROC_TABLES, input_maps=("3_lookup.omop.opcs4_snomed_map", "3_lookup.omop.endobase_exam_type_snomed_map",
                                  "3_lookup.omop.procedure_text_snomed_map") + CONCEPT + RF2,
         extras=("procedure_legacy_snomed_code",), allow_split=True),
    Lane("procedure_device", "procedure", "device", "direct",
         ("_r1_in_device_code", "_r1_in_device_display", "_r1_in__device_snomed_code", "_r1_in__device_mapping_layer",
          "_r1_in__device_mapping_confidence", "_r1_in_implant_description"),
         PROC_TABLES, input_maps=("3_lookup.omop.tier_lane_rescore",) + CONCEPT),
    Lane("path_test", "pathology_result", "test", "bridge", (B,), ("map_pathology",),
         input_maps=("3_lookup.omop.pathology_test_snomed_additions", "3_lookup.omop.tier_lane_rescore") + CONCEPT),
    Lane("path_result", "pathology_result", "result", "direct",
         ("_r1_in_source_code", "_r1_in_value_text", "_r1_in_value_concept_display", "_r1_in__result_snomed_code",
          "_r1_in__result_snomed_display", "_r1_in__result_omop_code", "_r1_in__result_confidence_tier"),
         ("map_pathology",), input_maps=("3_lookup.omop.tier_lane_rescore",) + CONCEPT),
    Lane("vital", "vital_sign", "vital", "bridge", (B,), ("map_numeric_events",),
         ("_clinical_registry_observations", "_clinical_promoted_vital_sign_primitive"),
         input_maps=("3_lookup.omop.s3_vocab_maps_to",) + CONCEPT,
         extras=("vital_omop_map_rule_id", "vital_omop_map_version")),
    Lane("vital_unit", "vital_sign", "unit", "direct", ("_r1_in_unit_source_value",), ("map_numeric_events",),
         ("_clinical_registry_observations", "_clinical_promoted_vital_sign_primitive"),
         input_maps=("3_lookup.omop.vital_unit_ucum_map",)),
    Lane("vital_method", "vital_sign", "method", "direct", ("_r1_in_method_display",), ("map_numeric_events",),
         ("_clinical_registry_observations", "_clinical_promoted_vital_sign_primitive"),
         input_maps=("3_lookup.omop.measurement_context_snomed_map",)),
    Lane("vital_body_site", "vital_sign", "body_site", "direct", ("_r1_in_body_site_display",), ("map_numeric_events",),
         ("_clinical_registry_observations", "_clinical_promoted_vital_sign_primitive"),
         input_maps=("3_lookup.omop.measurement_context_snomed_map",)),
    Lane("vital_interpretation", "vital_sign", "interpretation", "direct", ("_r1_in_interpretation_display",),
         ("map_numeric_events",), ("_clinical_registry_observations", "_clinical_promoted_vital_sign_primitive"),
         input_maps=("3_lookup.omop.measurement_context_snomed_map",)),
    Lane("score", "clinical_score", "score", "bridge", (B,), ("map_numeric_events",),
         ("_clinical_promoted_clinical_score_primitive",),
         input_maps=("3_lookup.omop.insr_lane_score_event",) + CONCEPT),
    Lane("med_admin", "medication_admin", "medication", "bridge",
         (B, "_r1_in__s3c_multum", "_r1_in__s3_med_snomed", "_r1_in__s3_med_snomed_display", "_r1_in__s3_med_omop",
          "_r1_in__s3_med_bronze_method", "_r1_in__s3_med_confidence", "_r1_in__s3_med_candidates"),
         ("map_med_admin",), input_maps=("3_lookup.omop.rxnorm_multum_snomed_map", "3_lookup.omop.rxnorm_class_residue",
                                         "3_lookup.omop.dmd_name_map", "3_lookup.omop.med_text_snomed_map") + CONCEPT,
         extras=("medication_legacy_snomed_code",), allow_split=True),
    Lane("med_order", "medication_order", "medication", "bridge",
         (B, "_r1_in__s3c_multum", "_r1_in__s3_med_snomed", "_r1_in__s3_med_snomed_display", "_r1_in__s3_med_omop",
          "_r1_in__s3_med_bronze_method", "_r1_in__s3_med_confidence", "_r1_in__s3_med_candidates"),
         ("map_medication_order",), input_maps=("3_lookup.omop.rxnorm_multum_snomed_map",
                                                "3_lookup.omop.rxnorm_class_residue", "3_lookup.omop.dmd_name_map",
                                                "3_lookup.omop.med_text_snomed_map") + CONCEPT,
         extras=("medication_legacy_snomed_code",), allow_split=True),
    Lane("med_dispense", "medication_dispense", "medication", "bridge", (B, "_r1_in__dmd_code"), ("map_pharmacy_issue",),
         input_maps=("3_lookup.omop.dmd_id_map", "3_lookup.omop.dmd_name_map", "3_lookup.omop.med_text_snomed_map")
         + CONCEPT, extras=("medication_legacy_snomed_code",), allow_split=True),
    Lane("finding", "clinical_finding", "finding", "bridge", (B,), FINDING_TABLES, ("_clinical_registry_observations",),
         input_maps=("3_lookup.omop.finding_event_code_snomed_map",) + CONCEPT),
    Lane("finding_value", "clinical_finding", "value", "direct",
         ("_r1_in_value_display", "_r1_in_value_code", "_r1_in_value_text"), FINDING_TABLES,
         ("_clinical_registry_observations",), input_maps=("3_lookup.omop.value_text_snomed_map",) + CONCEPT),
    Lane("exam", "imaging_exam", "exam", "bridge",
         (B, "_r1_in_source_code", "_r1_in_source_display", "_r1_in_source_coding_system"),
         ("map_radiology_event", "map_pacs_examination"),
         input_maps=("3_lookup.trud.nicip_snomed_map", "3_lookup.omop.imaging_exam_name_nicip_map",
                     "3_lookup.omop.imaging_local_code_map", "3_lookup.omop.concept_relationship") + CONCEPT,
         extras=("exam_legacy_snomed_code", "body_site_snomed_code", "body_site_snomed_display",
                 "body_site_snomed_method")),
    Lane("allergy_substance", "allergy_intolerance", "substance", "bridge", (B, "_r1_in_source_display"),
         ("map_allergy",), input_maps=("3_lookup.omop.allergy_substance_snomed_map", "3_lookup.omop.s3_vocab_maps_to")
         + CONCEPT, extras=("substance_omop_map_rule_id", "substance_omop_map_version")),
    Lane("cancer_drug", "cancer_treatment", "drug", "bridge", (B,), ("map_cancer_treatment",), input_maps=CONCEPT),
    Lane("stage_diagnosis", "condition_stage", "stage_diagnosis", "bridge", (B,),
         ("map_aria_diagnosis_staging", "map_cancer_stage_mill"),
         input_maps=("3_lookup.omop.icd10_snomed_map", "3_lookup.omop.s3_vocab_maps_to") + CONCEPT + RF2,
         extras=("stage_diagnosis_legacy_snomed_code",)),
    Lane("stage_group", "condition_stage", "stage_group", "direct", ("_r1_in_stage_of_disease",),
         ("map_aria_diagnosis_staging", "map_cancer_stage_mill"),
         input_maps=("3_lookup.omop.stage_group_snomed_map",) + CONCEPT),
    Lane("device", "device", "device", "bridge", (B,), ("mediconnect_device",),
         input_maps=("3_lookup.omop.tier_lane_rescore",) + CONCEPT),
    Lane("complaint", "presenting_complaint", "complaint", "direct",
         ("_r1_in_source_coding_system", "_r1_in_source_code", "_r1_in_source_display", "_r1_in_source_object",
          "_r1_in_complaint_text_normalised"), ("map_encounter", "map_nomen_events", "map_text_events"),
         input_maps=("3_lookup.omop.ecds_chief_complaint_map", "3_lookup.omop.complaint_text_snomed_map"),
         allow_split=True),
    Lane("specimen_type", "specimen", "specimen_type", "bridge", (B, "_r1_in__specimen_type_lookup_key"), (),
         ("_events_pathology_accession_identity",), input_maps=("3_lookup.omop.specimen_type_snomed_map",)),
)}
# Bronze SNOMED provenance applies to these lanes only (silver_journey_shared.py:772-812 handles these models).
PROVENANCE = {"condition": ("condition", "bronze.map_condition"),
              "fhx_condition": ("condition", "bronze.map_family_history"),
              "procedure": ("procedure", "bronze.map_procedure")}
MED_INIT = {"med_admin": "medication_admin", "med_order": "medication_order"}


# Native arms (plan Task 2 Step 5b; design rev 3 R1: "the resolver also writes native keys, with method native"):
# usage-only arms for the coded feed columns of the models no lane maps (appointments, encounters, referrals, ...).
# They have no chain and put no columns on bronze; their keys and counts feed reference_concept_registry only.
# mapping_native.py is GENERATED by tools/inventory.py --write from the staged feed projections (plus NATIVE_MANUAL);
# each entry: {"lane_id": "native_<feed>", "model", "table", "coding_system", "code", "display"} where the last
# three are Spark SQL expressions over the bronze row (a literal system stays a literal).
def native_arms():
    """The native arms (a tuple of dicts), imported lazily so silver never needs them."""
    import mapping_native
    return mapping_native.NATIVE_ARMS


def lanes_of_table(table):
    """Lane ids attached to one bronze table (bare name)."""
    return [lane_id for lane_id, lane in LANES.items() if table in lane.tables]


def lanes_of_model(model):
    """Lane ids of one silver model, in registry order."""
    return [lane_id for lane_id, lane in LANES.items() if lane.model == model]


def r1_columns(table):
    """The bronze columns R1 adds to one table, lane by lane."""
    return [c for lane_id in lanes_of_table(table) for c in M.lane_columns(lane_id)]


def arm_function(table, model):
    """The projection of one (table, model) arm: a hand port when one exists, else the generated function."""
    import mapping_inputs as GEN
    import mapping_inputs_manual as MAN
    name = f"arm__{table}__{model}"
    return MAN.MANUAL.get(name) or getattr(GEN, name)


def project(df, table, model):
    """Rows plus _r1_bridge, _r1_in_* and _r1_in_scope for every lane of `model` on `table`."""
    import mapping_inputs as GEN
    import mapping_inputs_manual as MAN
    out = arm_function(table, model)(df)
    if model in ("condition", "family_history", "procedure"):
        out = MAN.meta_extras(out, table)
    for lane_id, med_model in MED_INIT.items():
        if med_model == model:
            out = getattr(GEN, f"med_extras__{model}")(out)
    return out


def key_frame(projected, lane_id):
    """projected rows + _r1_key_<lane>: the lane's map key inside the arm's scope, NULL outside it."""
    lane = LANES[lane_id]
    M.assert_key_inputs(projected.columns, lane_id, lane.key_inputs)
    return projected.withColumn(f"_r1_key_{lane_id}",
                                F.when(F.col("_r1_in_scope"), M.map_key_expr(lane_id, lane.key_inputs)))


def distinct_inputs(keyed, lane_id):
    """One row per map key of a lane: the key plus its key inputs (the resolver's input frame)."""
    lane = LANES[lane_id]
    key = f"_r1_key_{lane_id}"
    return (keyed.where(F.col(key).isNotNull())
            .select(F.col(key).alias("map_key"), *lane.key_inputs).dropDuplicates(["map_key"]))


def _prechain(inputs, lane):
    """The per-row work _cross_qc_prepare did before the chain, applied to one lane's distinct inputs."""
    import mapping_chain as C
    import mapping_inputs as GEN
    from mapping_inputs import MODEL_EXTRA_NAMES as EXTRA_NAMES   # generated alongside the arms
    df = inputs
    for c in [c for c in df.columns if c.startswith("_r1_in_")]:
        df = df.withColumnRenamed(c, c[len("_r1_in_"):])
    for name in EXTRA_NAMES.get(lane.model, ()):          # other axes' inputs of the same model, as NULL
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast("string"))
    if "source_coding_system" in df.columns:
        df = df.withColumn("source_coding_system", C.canonical_coding_system(F.col("source_coding_system")))
    if lane.start == "bridge":
        df = C._s3_flatten_codeable_json(df, "_r1_bridge", lane.axis, False, lane.model)
    if lane.model in ("pathology_result", "clinical_finding", "condition_stage", "vital_sign", "procedure", "device"):
        df = getattr(GEN, f"prepare__{lane.model}")(df, lane.model)
    if lane.lane_id in PROVENANCE:
        axis, source = PROVENANCE[lane.lane_id]
        df = GEN.provenance_tail(df, lane.model, axis, source)
    if lane.lane_id in MED_INIT:
        df = getattr(GEN, f"prepare_med__{lane.model}")(df, lane.model)
    if lane.model in ("medication_admin", "medication_order", "medication_dispense"):
        df = GEN.prepare_med_common(df, lane.model)
    return (df.withColumn("_s3d_key", F.col("map_key"))
              .withColumn("_s3d_split_axis", F.lit(None).cast("string"))
              .withColumn("_s3d_split_identity", F.lit(None).cast("string"))
              .withColumn("_s3d_split_quantities", F.lit(False)))


def resolve(inputs, lane_id):
    """Distinct key inputs of one lane -> one row per (map_key, component) with the axis contract and extras.

    The chain is _s3_mapping_steps(model) restricted to the lane's axis, applied in order; with _s3d_key present the
    lookups run in domain mode and record splits in _s3d_split_* exactly as the domain stages did. The concept guard
    (SCT_TRUD_V1, below) is the last mapping step: every SNOMED code that leaves the resolver is a concept ID or NULL.
    The status guard (BBR_FOLDIN_V1, below) runs after it: 'mapped' always means a code landed.
    COMPLAINT_PICKLIST_TEXT_V1 (below) runs after the chain on the complaint lane only: it fills free-text ED keys
    the chain left unmapped whose text is an ECDS picklist term."""
    import mapping_chain as C
    lane = LANES[lane_id]
    df = _prechain(inputs, lane)
    for step in C._s3_mapping_steps(lane.model):
        if step.axis == lane.axis:
            df = step.apply(df)
    if lane_id == COMPLAINT_PICKLIST_LANE:
        df = complaint_picklist_text_step(df, lane.axis)
    extras = [c for c in lane.extras if c in df.columns]
    resolved = df.select(
        F.col("map_key"),
        F.coalesce(F.col(f"{lane.axis}_map_component_index"), F.lit(1)).cast("int").alias("component_index"),
        *[F.col(f"{lane.axis}_{n}").cast(t).alias(n) for n, t in M.AXIS_FIELDS],
        (F.to_json(F.struct(*[F.col(c) for c in extras])) if extras else F.lit(None).cast("string")).alias("extras_json"),
        F.col("_s3d_split_identity").alias("component_identity"))
    return status_guard(concept_guard(resolved, lane_id), lane_id)


# ==== COMPLAINT_PICKLIST_TEXT_V1: free-text ED complaints that are an ECDS picklist term (Ben, 2026-11-09) ====
# The ED free-text arm (map_text_events, source_object ed_presenting_complaint_text) keeps source_code NULL
# (INSR_W2 W2.04), and the chain's ECDS picklist step keys on source_code. So a typed complaint that is exactly a
# picklist term ("Chest pain") lost the mapping the picklist gives the coded arm (INSR_20261029 P3.R1.axis_fill:
# complaint lane -0.58%, waived in r1_steps.INSR_KNOWN_FALLS).
#
# The rule: after the chain, a free-text ED key that the chain left with no method and no code, and whose text
# equals one picklist term after picklist_text_key on both sides, takes that term's picklist row, the same row the
# chain's coded lookup applies to the coded arm. Exact match after normalisation only, no fuzzy matching.
# Provenance: map_rule_id is the picklist row's rule plus COMPLAINT_PICKLIST_RULE_SUFFIX, so it differs from the
# coded-key rule, and the gold gate still admits it (the gate matches translation rules by prefix and banded
# embedding policies by ':<lane>:' substring). map_status_reason names the rule when the picklist row has no reason.
# The rule never touches a key the chain mapped, rolled up, kept or ruled out (any map_method or any code).
COMPLAINT_PICKLIST_LANE = "complaint"
COMPLAINT_PICKLIST_RULE_VERSION = "COMPLAINT_PICKLIST_TEXT_V1"
COMPLAINT_PICKLIST_MAP = "3_lookup.omop.ecds_chief_complaint_map"            # = mapping_chain.SRC_S3C_ECDS_COMPLAINT_MAP
COMPLAINT_PICKLIST_SYSTEM = "urn:cerner:nomenclature:ed-presenting-complaint"
COMPLAINT_FREE_TEXT_OBJECT = "ed_presenting_complaint_text"                 # mapping_inputs_manual, ED free-text arm
COMPLAINT_PICKLIST_RULE_SUFFIX = ":free_text_exact_normalised"
COMPLAINT_PICKLIST_REASON = "free text equals an ECDS picklist term after normalisation (COMPLAINT_PICKLIST_TEXT_V1)"
# The statuses the chain's lookup reads (mapping_chain._s3_lookup_axis), in its preference order.
PICKLIST_LOOKUP_STATUSES = ("PROPOSED", "VETOED", "UNMAPPED", "FALLBACK")
# Java regex (Spark): a run of characters that are not letters or digits. Python twin: [\W_]+ (see picklist_text_key).
PICKLIST_TEXT_SEPARATORS = r"[^\p{L}\p{N}]+"

# Lanes whose resolution rules changed since their last full resolve. The version joins that lane's stored trigger
# (mapping_resolver.trigger_hashes), so the next run re-resolves that lane, and only that lane, in full once; its
# changed keys then reach bronze through the normal backfill. Lanes absent here keep their trigger unchanged.
LANE_RULE_VERSIONS = {COMPLAINT_PICKLIST_LANE: COMPLAINT_PICKLIST_RULE_VERSION}


def picklist_text_key(text):
    """The normalised text a free-text complaint and a picklist term are compared on (pure-Python twin of
    picklist_text_key_col): lower case; every run of characters that are not letters or digits (spaces, tabs,
    line breaks, punctuation, symbols, underscores) becomes one space; trimmed. Empty becomes None (never matches).
    Nothing else is folded: no accents, spelling, abbreviations, word order or plurals."""
    if text is None:
        return None
    key = re.sub(r"[\W_]+", " ", text.lower()).strip()
    return key or None


def picklist_text_key_col(column):
    """picklist_text_key as a Spark column (NULL for NULL or an empty result)."""
    key = F.trim(F.regexp_replace(F.lower(column), PICKLIST_TEXT_SEPARATORS, " "))
    return F.when(F.length(key) > 0, key)


def picklist_fill_eligible(source_object, map_method, snomed_code, omop_concept_id, target_code, component_count):
    """True when the rule may fill one resolved key (pure twin of the eligibility column in
    complaint_picklist_text_step): the ED free-text arm, and the chain left it with no method, no code and one
    component."""
    return (source_object == COMPLAINT_FREE_TEXT_OBJECT and map_method is None and snomed_code is None
            and omop_concept_id is None and target_code is None and (component_count or 1) == 1)


def picklist_rows_by_text(rows):
    """{normalised text: picklist row} from picklist rows (dicts with source_code, status, target_code,
    mapping_status, component_index, component_count) that are each already the best row of their term (pure twin
    of _complaint_picklist_by_text). A normalised text shared by two terms is dropped (ambiguous, no guess); a term
    whose best row is not a PROPOSED single-component row with a target and a status is never used."""
    terms = {}
    for row in rows:
        terms.setdefault(picklist_text_key(row["source_code"]), set()).add(row["source_code"])
    out = {}
    for row in rows:
        text = picklist_text_key(row["source_code"])
        usable = (row["status"] == "PROPOSED" and row["target_code"] is not None
                  and row["mapping_status"] is not None
                  and (row.get("component_index") or 1) == 1 and (row.get("component_count") or 1) == 1)
        if text is not None and len(terms[text]) == 1 and usable:
            out[text] = row
    return out


def _complaint_picklist_by_text():
    """One row per normalised picklist text, columns prefixed _pk_: the best row of the one picklist term with that
    text, ranked exactly as mapping_chain._s3_lookup_axis ranks a term's rows, kept only when that row is PROPOSED,
    single-component, and lands a target with a status. Texts that two terms share are dropped."""
    import mapping_chain as C
    from pyspark.sql import Window
    lookup = (C.read_source(COMPLAINT_PICKLIST_MAP)
              .where((F.col("source_coding_system") == COMPLAINT_PICKLIST_SYSTEM)
                     & F.col("status").isin(*PICKLIST_LOOKUP_STATUSES)))
    # The chain's ranking of a term's rows (mapping_chain._s3_lookup_axis), restricted to component 1.
    rank = Window.partitionBy("source_code").orderBy(
        F.when(F.col("status") == "PROPOSED", 0).when(F.col("status") == "VETOED", 1)
         .when(F.col("status") == "UNMAPPED", 2).otherwise(3),
        F.when(F.col("mapping_method") == "rule", 0).when(F.col("mapping_method") == "ancestor_rollup", 1).otherwise(2),
        F.col("mapping_cosine").desc_nulls_last(), F.col("legacy_confidence").desc_nulls_last(),
        F.col("mapping_rule_id").asc_nulls_last())
    best = (lookup.where(F.coalesce(F.col("component_index"), F.lit(1)) == 1)
            .withColumn("_rn", F.row_number().over(rank)).where("_rn = 1").drop("_rn")
            .withColumn("_pk_text", picklist_text_key_col(F.col("source_code")))
            .where(F.col("_pk_text").isNotNull()))
    # A normalised text two terms share is ambiguous: no term is chosen for it.
    unambiguous = (best.groupBy("_pk_text").agg(F.countDistinct("source_code").alias("_terms"))
                   .where("_terms = 1").select("_pk_text"))
    usable = ((F.col("status") == "PROPOSED") & F.col("target_code").isNotNull() & F.col("mapping_status").isNotNull()
              & (F.coalesce(F.col("component_count"), F.lit(1)) == 1))
    fields = ("target_coding_system", "target_code", "target_display", "omop_concept_id", "omop_standard",
              "mapping_method", "mapping_version", "mapping_rule_id", "candidate_count", "mapping_cosine",
              "scoring_model", "mapping_status", "status_reason", "competing_count", "candidate_set_id",
              "rollup_levels", "hierarchy_version")
    return (best.where(usable).join(unambiguous, "_pk_text")
            .select("_pk_text", *[F.col(f).alias(f"_pk_{f}") for f in fields]))


def complaint_picklist_text_step(df, axis):
    """Resolved complaint-lane rows (the chain's <axis>_* columns, one row per key and component) -> the same rows,
    with each eligible free-text key that matches a picklist term filled from that term's picklist row.

    The fields written are those the chain's lookup writes for the coded arm (mapping_chain._s3_lookup_axis: target,
    SNOMED, OMOP, method, version, rule, counts, cosine, scoring model, status, reason, component, rollup levels);
    map_confidence is left as the chain set it, as the coded lookup leaves it. Only the rule id (suffix) and, where
    the picklist row has none, the status reason differ from the coded arm."""
    col = lambda name: F.col(f"{axis}_{name}")    # noqa: E731  (one axis column)
    eligible = ((F.col("source_object") == COMPLAINT_FREE_TEXT_OBJECT) & col("map_method").isNull()
                & col("snomed_code").isNull() & col("omop_concept_id").isNull() & col("target_code").isNull()
                & (F.coalesce(col("map_component_count"), F.lit(1)) == 1))
    picklist = _complaint_picklist_by_text()
    df = df.withColumn("_pk_key", F.when(eligible, picklist_text_key_col(col("source_display"))))
    df = df.join(F.broadcast(picklist), F.col("_pk_key") == F.col("_pk_text"), "left")
    hit = F.coalesce(F.col("_pk_text").isNotNull(), F.lit(False))
    is_snomed = F.col("_pk_target_coding_system") == F.lit(SNOMED_SYSTEM)
    standard = F.col("_pk_omop_standard") == "S"
    filled = {
        "target_system": F.col("_pk_target_coding_system"),
        "target_code": F.col("_pk_target_code"),
        "target_display": F.col("_pk_target_display"),
        "snomed_code": F.when(is_snomed, F.col("_pk_target_code")),
        "snomed_display": F.when(is_snomed, F.col("_pk_target_display")),
        "omop_concept_id": F.when(standard, F.col("_pk_omop_concept_id")),
        "omop_source_concept_id": F.when(~standard, F.col("_pk_omop_concept_id")),
        "map_method": F.col("_pk_mapping_method"),
        "map_version": F.concat_ws("|", F.col("_pk_mapping_version"), F.col("_pk_hierarchy_version")),
        "map_rule_id": F.concat(F.coalesce(F.col("_pk_mapping_rule_id"), F.lit("")),
                                F.lit(COMPLAINT_PICKLIST_RULE_SUFFIX)),
        "map_candidate_count": F.col("_pk_candidate_count"),
        "map_cosine": F.col("_pk_mapping_cosine"),
        "map_scoring_model": F.col("_pk_scoring_model"),
        "map_status": F.col("_pk_mapping_status"),
        "map_status_reason": F.coalesce(F.col("_pk_status_reason"), F.lit(COMPLAINT_PICKLIST_REASON)),
        "map_competing_count": F.col("_pk_competing_count"),
        "map_candidate_set_id": F.col("_pk_candidate_set_id"),
        "map_component_index": F.lit(1),
        "map_component_count": F.lit(1),
        "map_rollup_levels": F.col("_pk_rollup_levels"),
    }
    types = {f.name: f.dataType for f in df.schema.fields}
    for name, value in filled.items():
        df = df.withColumn(f"{axis}_{name}", F.when(hit, value.cast(types[f"{axis}_{name}"])).otherwise(col(name)))
    return df.drop(*[c for c in df.columns if c.startswith("_pk_")])


# ==== SCT_TRUD_V1: the concept guard (design snomed_trud/design.md 2.2 B) ====
# Ben's ruling 2026-11-03: every SNOMED code column holds a concept ID or NULL. In an SCTID the partition identifier
# is the two digits before the check digit: 00/10 = concept, 01/11 = description (10/11 are extension SCTIDs, which
# carry a 7-digit namespace and so have at least 11 digits). The guard runs at key grain, as resolve()'s last step.
RF2_SCT = "3_lookup.trud.snomed_sct"            # RF2 descriptions: TUI = description ID, CUI = its concept
OMOP_CONCEPT = "3_lookup.omop.concept"          # concept_code of SNOMED / dm+d rows = known concepts
SNOMED_SYSTEM = "http://snomed.info/sct"
SCTID_SHAPE = "^[1-9][0-9]{5,17}$"
NATIVE_PASS_THROUGH_RULE = "source:canonical_snomed:source_code"   # the chain passed a native source code through
RF2_DESCRIPTION_SUFFIX = ":rf2_description"
GUARD_REASONS = {
    "not_sctid": "snomed_not_an_sctid",                        # NULLed: not an SCTID at all
    "description_unresolved": "snomed_description_unresolved", # NULLed: a description ID RF2 does not know
    "source_not_concept": "snomed_source_not_a_known_concept", # NULLed: a raw native pass-through no release knows
    "advisory": "sctid_not_in_loaded_release",                 # kept: a governed concept newer than our releases
}


def sctid_kind(code):
    """'concept', 'description' or 'not_sctid' for one code string (None for None). The pure-Python form of
    sctid_kind_sql below; the tests and the dev fixture notebook check both give the same answer."""
    if code is None:
        return None
    if not re.fullmatch(SCTID_SHAPE[1:-1], code):
        return "not_sctid"
    partition = code[-3:-1]
    if partition in ("10", "11") and len(code) < 11:
        return "not_sctid"
    return {"00": "concept", "10": "concept", "01": "description", "11": "description"}.get(partition, "not_sctid")


def sctid_kind_sql(column):
    """sctid_kind as Spark SQL text over a string column (W4's gold QC rule renders the same predicate)."""
    part = f"substring({column}, length({column}) - 2, 2)"
    return (f"CASE WHEN {column} IS NULL THEN NULL "
            f"WHEN NOT ({column} RLIKE '{SCTID_SHAPE}') THEN 'not_sctid' "
            f"WHEN {part} IN ('10', '11') AND length({column}) < 11 THEN 'not_sctid' "
            f"WHEN {part} IN ('00', '10') THEN 'concept' "
            f"WHEN {part} IN ('01', '11') THEN 'description' "
            f"ELSE 'not_sctid' END")


def guard_decision(kind, rf2_concept, known, rule):
    """(action, reason) for one SNOMED code, the design's guard table as plain Python: action is 'keep', 'replace'
    (description -> its RF2 concept) or 'null'. _guard_columns builds the same table as Spark columns."""
    if kind is None:
        return "keep", None
    if kind == "not_sctid":
        return "null", GUARD_REASONS["not_sctid"]
    if kind == "description":
        return ("replace", None) if rf2_concept else ("null", GUARD_REASONS["description_unresolved"])
    if known:
        return "keep", None
    if rule == NATIVE_PASS_THROUGH_RULE:
        return "null", GUARD_REASONS["source_not_concept"]
    return "keep", GUARD_REASONS["advisory"]


def guarded_rule(rule):
    """map_rule_id after an RF2 description -> concept replacement: a native pass-through becomes
    source:canonical_snomed:rf2_description; any other rule gains the :rf2_description suffix."""
    if rule and rule.endswith(":source_code"):
        return rule[: -len(":source_code")] + RF2_DESCRIPTION_SUFFIX
    return (rule or "") + RF2_DESCRIPTION_SUFFIX


def _guard_reference(codes):
    """One row per distinct code: _g_kind, _g_rf2_concept (its concept when it is a description RF2 knows),
    _g_rf2_term (that concept's RF2 preferred term) and _g_known (a concept RF2 or the OMOP vocabulary knows).

    Every lookup is semi-joined to the lane's own codes first, so the 3.3M-row RF2 table is read once per lane and
    only the lane's few distinct codes travel through the joins."""
    import mapping_chain as C
    from pyspark.sql import Window
    rf2 = C.read_source(RF2_SCT)
    kinds = codes.withColumn("_g_kind", F.expr(sctid_kind_sql("_g_code")))
    descriptions = (rf2.select(F.col("TUI").cast("string").alias("_g_code"),
                               F.col("CUI").cast("string").alias("_g_rf2_concept"))
                    .join(codes, "_g_code", "left_semi"))
    # The preferred term (TYP 1) of each concept a description resolves to; current descriptions first.
    term_rank = Window.partitionBy("_g_rf2_concept").orderBy(F.col("TUISTAT").asc_nulls_last(), F.col("TUI").asc())
    terms = (rf2.where(F.col("TYP") == 1)
             .select(F.col("CUI").cast("string").alias("_g_rf2_concept"), "TUISTAT", "TUI",
                     F.col("TERM").alias("_g_rf2_term"))
             .join(descriptions.select("_g_rf2_concept").distinct(), "_g_rf2_concept", "left_semi")
             .withColumn("_g_rn", F.row_number().over(term_rank)).where("_g_rn = 1")
             .select("_g_rf2_concept", "_g_rf2_term"))
    known = (rf2.select(F.col("CUI").cast("string").alias("_g_code"))
             .unionByName(C.read_source(OMOP_CONCEPT).where(F.col("vocabulary_id").isin("SNOMED", "dm+d"))
                          .select(F.col("concept_code").cast("string").alias("_g_code")))
             .join(codes, "_g_code", "left_semi").distinct().withColumn("_g_known", F.lit(True)))
    return (kinds.join(descriptions, "_g_code", "left").join(terms, "_g_rf2_concept", "left")
            .join(known, "_g_code", "left")
            .withColumn("_g_known", F.coalesce(F.col("_g_known"), F.lit(False))))


def _guard_columns(prefix, rule):
    """(action, reason) Spark columns of guard_decision over one joined reference (columns <prefix>kind, ...)."""
    kind, concept, known = F.col(f"{prefix}kind"), F.col(f"{prefix}rf2_concept"), F.col(f"{prefix}known")
    pass_through = F.coalesce(rule == F.lit(NATIVE_PASS_THROUGH_RULE), F.lit(False))
    action = (F.when(kind.isNull(), F.lit("keep"))
              .when(kind == "not_sctid", F.lit("null"))
              .when(kind == "description", F.when(concept.isNotNull(), F.lit("replace")).otherwise(F.lit("null")))
              .when(known, F.lit("keep"))
              .when(pass_through, F.lit("null"))
              .otherwise(F.lit("keep")))
    reason = (F.when(kind.isNull(), F.lit(None).cast("string"))
              .when(kind == "not_sctid", F.lit(GUARD_REASONS["not_sctid"]))
              .when(kind == "description",
                    F.when(concept.isNull(), F.lit(GUARD_REASONS["description_unresolved"])))
              .when(known, F.lit(None).cast("string"))
              .when(pass_through, F.lit(GUARD_REASONS["source_not_concept"]))
              .otherwise(F.lit(GUARD_REASONS["advisory"])))
    return action, reason


def concept_guard(df, lane_id):
    """Resolved rows (the contract fields) -> the same rows with every SNOMED code a concept ID or NULL.

    snomed_code, and target_code when target_system is SNOMED, are each classified (sctid_kind) and looked up:
      * a description RF2 knows becomes its concept, the display its preferred term, the rule gains :rf2_description;
      * a non-SCTID, an unknown description, or a raw native pass-through no release knows is NULLed with its
        companions (snomed_display, and the SNOMED target's code, display and system); the row becomes
        map_method NULL / map_status no_candidate / a map_status_reason naming why (never a description ID);
      * a governed concept missing from the loaded releases is kept with the advisory reason (newer concepts exist).
    map_status_reason is outside the resolution hash, so an advisory alone never re-hashes a key."""
    snomed_target = F.coalesce(F.col("target_system") == F.lit(SNOMED_SYSTEM), F.lit(False))
    codes = (df.select(F.col("snomed_code").alias("_g_code"))
             .unionByName(df.where(snomed_target).select(F.col("target_code").alias("_g_code")))
             .where(F.col("_g_code").isNotNull()).distinct())
    ref = _guard_reference(codes)
    s_ref = ref.select(*[F.col(c).alias("_gs" + c[2:]) for c in ref.columns])     # _g_x -> _gs_x (snomed_code)
    t_ref = ref.select(*[F.col(c).alias("_gt" + c[2:]) for c in ref.columns])     # _g_x -> _gt_x (target_code)
    out = (df.join(s_ref, F.col("snomed_code") == F.col("_gs_code"), "left")
             .join(t_ref, snomed_target & (F.col("target_code") == F.col("_gt_code")), "left"))
    rule = F.col("map_rule_id")
    s_action, s_reason = _guard_columns("_gs_", rule)
    t_action, t_reason = _guard_columns("_gt_", rule)
    out = (out.withColumn("_g_s_action", s_action).withColumn("_g_s_reason", s_reason)
              .withColumn("_g_t_action", F.when(snomed_target, t_action).otherwise(F.lit("keep")))
              .withColumn("_g_t_reason", F.when(snomed_target, t_reason)))
    nulled = (F.col("_g_s_action") == "null") | (F.col("_g_t_action") == "null")
    replaced_s = F.col("_g_s_action") == "replace"
    replaced_t = F.col("_g_t_action") == "replace"
    replaced = replaced_s | replaced_t
    null_reason = F.when(F.col("_g_s_action") == "null", F.col("_g_s_reason")).otherwise(F.col("_g_t_reason"))
    advisory = F.coalesce(F.col("_g_s_reason"), F.col("_g_t_reason"))
    rf2_rule = F.when(F.coalesce(rule, F.lit("")).endswith(":source_code"),
                      F.concat(F.expr("substring(map_rule_id, 1, length(map_rule_id) - length(':source_code'))"),
                               F.lit(RF2_DESCRIPTION_SUFFIX)))
    rf2_rule = rf2_rule.otherwise(F.concat(F.coalesce(rule, F.lit("")), F.lit(RF2_DESCRIPTION_SUFFIX)))
    snomed_nulled = nulled
    target_nulled = nulled & snomed_target
    fields = {
        "snomed_code": F.when(snomed_nulled, F.lit(None)).when(replaced_s, F.col("_gs_rf2_concept"))
                        .otherwise(F.col("snomed_code")),
        "snomed_display": F.when(snomed_nulled, F.lit(None)).when(replaced_s, F.col("_gs_rf2_term"))
                           .otherwise(F.col("snomed_display")),
        "target_system": F.when(target_nulled, F.lit(None)).otherwise(F.col("target_system")),
        "target_code": F.when(target_nulled, F.lit(None)).when(replaced_t, F.col("_gt_rf2_concept"))
                        .otherwise(F.col("target_code")),
        "target_display": F.when(target_nulled, F.lit(None)).when(replaced_t, F.col("_gt_rf2_term"))
                           .otherwise(F.col("target_display")),
        "map_method": F.when(nulled, F.lit(None)).otherwise(F.col("map_method")),
        "map_status": F.when(nulled, F.lit("no_candidate")).otherwise(F.col("map_status")),
        "map_status_reason": F.when(nulled, null_reason)
                              .otherwise(F.coalesce(F.col("map_status_reason"), advisory)),
        "map_rule_id": F.when(~nulled & replaced, rf2_rule).otherwise(rule),
    }
    types = dict(M.AXIS_FIELDS)
    return out.select(*[fields[c].cast(types[c]).alias(c) if c in fields else F.col(c) for c in df.columns])



# ==== BBR_FOLDIN_V1: the status guard (design bbr_review/foldin_design.md BBR-10) ====
# A rule can run and land no code (a tier rescore that cleared a vetoed target, a lookup row with no target): such an
# axis row said map_status 'mapped' with every code NULL (n100 gold: 518,827 pathology test rows). After the guard
# 'mapped' always means a code landed. The method and rule stay, so the provenance of what ran survives; the status
# vocabulary needs no new value, and map_status_reason is outside HASH_FIELDS.
STATUS_GUARD_REASON = "rule_applied_no_code"


def status_after_guard(status, snomed_code, omop_concept_id, target_code):
    """(map_status, reason or None) of one axis row after the guard; the pure twin of status_guard."""
    if status == "mapped" and snomed_code is None and omop_concept_id is None and target_code is None:
        return "no_candidate", STATUS_GUARD_REASON
    return status, None


def status_guard(df, lane_id):
    """Resolved rows -> the same rows with map_status 'mapped' only where snomed_code, omop_concept_id or
    target_code is set; the others become no_candidate / rule_applied_no_code. Applies to every lane (the same
    semantics as triage L2806 for orders); lane_id is kept for symmetry with concept_guard and the step log."""
    no_code = (F.col("map_status") == "mapped") & F.col("snomed_code").isNull() \
        & F.col("omop_concept_id").isNull() & F.col("target_code").isNull()
    return (df.withColumn("map_status_reason", F.when(no_code, F.lit(STATUS_GUARD_REASON))
                          .otherwise(F.col("map_status_reason")))
              .withColumn("map_status", F.when(no_code, F.lit("no_candidate")).otherwise(F.col("map_status"))))

# Row keys of the bronze targets (MERGE key of the post-landing attach); verified unique by tools/inventory.py --write.
# Copied from release/inventory.json["row_keys"] (Task 2 Step 5 corrected map_coded_events and map_maternity_diagnosis).
ROW_KEYS = {
    "map_diagnosis": ["DIAGNOSIS_ID"],
    "map_problem": ["PROBLEM_ID"],
    "map_procedure": ["PROCEDURE_ID"],
    "map_family_history": ["FHX_ACTIVITY_ID"],
    "map_pathology": ["source_record_key"],
    "map_numeric_events": ["EVENT_ID"],
    "map_med_admin": ["EVENT_ID"],
    "map_medication_order": ["ORDER_ID"],
    "map_endobase_exam": ["ENDOBASE_EXAM_ID"],
    "map_aria_diagnosis_staging": ["ARIA_PT_ID", "ARIA_DX_ID"],
    "map_critical_care_procedure": ["ROW_HASH"],
    "mediconnect_device": ["MC_DEVICE_ID"],
    "map_encounter": ["ENCNTR_ID"],
    "map_nomen_events": ["EVENT_ID", "SEQUENCE_NBR"],
    "map_implant_details": ["EVENT_ID"],
    "map_theatre_case_procedure": ["SURG_CASE_PROC_ID"],
    "map_maternity_diagnosis": ["PREGNANCYID", "DIAGSCHEME", "DIAG", "DIAGDATE", "COMPLICATINGDIAGIND", "LOCALFETALID", "FETALORDER", "PERSON_ID"],
    "map_pharmacy_issue": ["PHARMACY_ISSUE_ID"],
    "map_coded_events": ["EVENT_ID", "SEQUENCE_NBR"],
    "map_date_events": ["EVENT_ID"],
    "map_text_events": ["EVENT_ID"],
    "map_radiology_event": ["EVENT_ID"],
    "map_pacs_examination": ["PACS_EXAMINATION_ID"],
    "map_allergy": ["ALLERGY_INSTANCE_ID"],
    "map_cancer_treatment": ["TREATMENT_KEY"],
    "map_cancer_stage_mill": ["STAGE_KEY"],
}
