"""Shared Silver Journey helpers and source projections.

Imported by the subject notebooks. This module declares no datasets.
Identity rules, mapping provenance and source projections are kept in one place
because both the patient event index and typed clinical facts use them.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ==== Imports ====
import pyspark.sql.functions as F

from pyspark.sql import Column

from pyspark.sql.window import Window

def _make_strip_rtf_text():
    """Send a pure parser closure to workers, without importing this Spark module."""
    try:
        from striprtf.striprtf import rtf_to_text as _rtf_to_text
        parser_version = "striprtf-0.0.32"
    except ImportError:
        # Preserve the original value if the optional parser is unavailable.
        def _rtf_to_text(value, errors="ignore"):
            # Preserve the original value when the optional RTF parser is unavailable.
            return value
        parser_version = "rtf-preserved-no-parser"

    def _strip_rtf_text_value(value):
        """Parse RTF with the same pinned library as Blob v4; never erase on failure."""
        if value is None:
            return None
        try:
            parsed = _rtf_to_text(value, errors="ignore")
            return parsed if parsed and parsed.strip() else value
        except Exception:
            return value

    return F.udf(_strip_rtf_text_value, "string"), parser_version

_strip_rtf_text, _RTF_PARSER_VERSION = _make_strip_rtf_text()



# ==== Declarative pipelines API ====

# The declarative pipelines API lives in pyspark.pipelines on the serverless CURRENT
# channel. The fallback keeps the notebook runnable on a runtime that still only has
# the dlt module, at the cost of the refresh_policy hint.
try:
    from pyspark import pipelines as dp
    _NEW_API = True
except ImportError:  # fallback: classic dlt module (no refresh_policy kwarg)
    import dlt
    class _DpShim:
        @staticmethod
        def materialized_view(**kw):
            # Adapt the materialized-view declaration to classic DLT by removing unsupported
            # options.
            kw.pop("refresh_policy", None)
            return dlt.table(**kw)
    dp = _DpShim()
    _NEW_API = False

def materialized_view(column_comments=None, **options):
    """Declare a published table, attaching per-column comments to its schema.

    Unity Catalog reads the comments off the schema Lakeflow records for the flow,
    the same way the RDE and OMOP pipelines attach StructField metadata. They only
    reach the catalog when the table is first built: an existing table keeps the
    comments it already has, so changing one here also needs a catalog reconcile.

    Private stages pass no comments and go straight through.
    """
    # S3 retires the registered public CodeableConcept columns.  Adjust the
    # decorator metadata at declaration time; internal JSON bridges remain private.
    table_leaf = str(options.get("name", "")).split(".")[-1]
    axis_specs = globals().get("S3_TABLE_AXIS_SPECS", {}).get(table_leaf, {})
    if column_comments and axis_specs:
        column_comments = dict(column_comments)
        for old_name, axes in axis_specs.items():
            column_comments.pop(old_name, None)
            for axis, source_only, subject in axes:
                column_comments.update(axis_comments(axis, subject, source_only))
    declare = dp.materialized_view(**options)
    if not column_comments:
        return declare

    def decorate(build):
        # Wrap the dataset builder so column descriptions are attached before Lakeflow records
        # its output schema.
        def with_column_comments():
            # Run the dataset builder and attach the supplied descriptions to its output
            # columns.
            df = build()
            for column in df.columns:
                if column in column_comments:
                    df = df.withMetadata(column, {"comment": column_comments[column]})
            return df

        with_column_comments.__name__ = build.__name__
        return declare(with_column_comments)

    return decorate




# ==== Dataset names ====

# Two schemas, both supplied by the pipeline specification (see _s2_pipeline_admin):
#   journey.public_schema   — the research contract researchers browse
#   journey.internal_schema — primitives, grouped aggregates and QC stages that only
#                             exist to keep the graph incremental (Enzyme cannot plan a
#                             VARIANT coming out of a join, so joins run in the internal
#                             schema on JSON strings and the public MV is a single-source
#                             parse_json wrapper)
# Missing configuration is fatal on purpose: a notebook that silently publishes to the
# pipeline default schema is how implementation tables ended up beside research tables.
PUBLIC_SCHEMA = spark.conf.get("journey.public_schema")

INTERNAL_SCHEMA = spark.conf.get("journey.internal_schema")

def _n(name):
    """Flatten a logical plane name into a two-part published name.

    journey_clinical.condition      -> <public>.clinical_condition
    journey_clinical._qc_condition  -> <internal>._clinical_qc_condition
    journey_clinical.__x_primitive  -> <internal>.__clinical_x_primitive
    A leading underscore on the table part marks it internal; that convention is already
    what the production notebook uses, so no product is renamed here.
    """
    logical_schema, table = name.split(".", 1)
    if not logical_schema.startswith("journey_"):
        raise ValueError(f"unexpected flow schema in {name!r}")
    plane = logical_schema[len("journey_"):]
    if table.startswith("_"):
        return INTERNAL_SCHEMA + "._" + plane + table
    return PUBLIC_SCHEMA + "." + plane + "_" + table




# ==== Reading sources ====
# Dev proof for S2.5: the pipeline configuration may repoint a named bronze source to a
# dev twin. Default is an empty map, which preserves the previous behaviour.
import json as _json

SOURCE_OVERRIDES = _json.loads(spark.conf.get("journey.source_overrides", "{}"))

def read_source(name):
    # Read a source table, honoring the configured development source override when present.
    return spark.read.table(SOURCE_OVERRIDES.get(name, name))




# ==== Deterministic identity helpers ====

# Identity helpers. These decide what every key in the product hashes to, so a change
# here is a full rebuild of everything downstream, not an edit.
NULL_TOKEN = "~"   # outside the base64 alphabet => can never collide with an encoded value

def _enc(col_or_lit):
    """Encode one part of a key: NULL becomes '~', anything else base64(trim(string)).
    base64 output contains neither ':' nor '~', so parts joined with ':' can only be
    read back one way and two different inputs can never produce the same key."""
    c = col_or_lit if isinstance(col_or_lit, Column) else F.lit(col_or_lit)
    s = F.trim(c.cast("string"))
    return F.when(c.isNull(), F.lit(NULL_TOKEN)).otherwise(F.base64(s.cast("binary")))

def _present(col):
    """True only when identifier evidence is non-null and non-blank."""
    return col.isNotNull() & (F.trim(col.cast("string")) != "")

def _usable_code(col):
    """Gate B coded test: non-null, non-blank, and not the zero sentinel."""
    v = F.trim(col.cast("string"))
    return col.isNotNull() & (v != "") & (v != "0")

def _code_or_display(code_col, display_col, excluded_displays=()):
    """Preserve clinically meaningful display-only rows without admitting placeholders."""
    display = F.trim(display_col.cast("string"))
    display_ok = display_col.isNotNull() & (display != "") & (display != "0")
    if excluded_displays:
        display_ok = display_ok & ~F.upper(display).isin(*excluded_displays)
    return F.when(_usable_code(code_col), code_col.cast("string")).when(display_ok, display)

def stable_id(namespace, *cols):
    """patient_event_id / encounter_id minting: sha2 over encoded namespace + natural-key parts.
    namespace = fact kind + source feed. NEVER the target table name (promotion-safe)."""
    return F.sha2(F.concat_ws(":", _enc(namespace), *[_enc(c) for c in cols]), 256)

def subject_key_with_system(candidates, source_table_lit, source_row_id_col):
    """Build a deterministic subject key from the strongest available source identifier.

    Identifier-less rows receive a deterministic per-source-row key and therefore remain
    non-joinable across feeds. No secret or environment-specific state is involved.
    """
    key = F.sha2(
        F.concat_ws(
            ":",
            _enc("subject"),
            _enc("nosubject"),
            _enc(source_table_lit),
            _enc(source_row_id_col),
        ),
        256,
    )
    system = F.lit("nosubject")
    for sys_lit, col in reversed(candidates):
        value = F.trim(col.cast("string"))
        present = col.isNotNull() & (value != "")
        key = F.when(
            present,
            F.sha2(F.concat_ws(":", _enc("subject"), _enc(sys_lit), _enc(col)), 256),
        ).otherwise(key)
        system = F.when(present, F.lit(sys_lit)).otherwise(system)
    return key, system




# ==== CodeableConcept ====

def coding_obj(system_col, code_col, display_col, is_source, map_source=None, map_version=None):
    # Construct a coding struct containing the source or target code and its mapping provenance.
    map_source_col = map_source.cast("string") if isinstance(map_source, Column) else F.lit(map_source).cast("string")
    map_version_col = map_version.cast("string") if isinstance(map_version, Column) else F.lit(map_version).cast("string")
    return F.struct(
        system_col.cast("string").alias("coding_system"),
        code_col.cast("string").alias("coding_code"),
        display_col.cast("string").alias("coding_display"),
        F.lit(is_source).alias("is_source"),
        map_source_col.alias("map_source"),
        map_version_col.alias("map_version"),
    )

def codeable_concept_json(*objs):
    """Deterministic JSON CodeableConcept for aggregate/join boundaries."""
    arr = F.filter(F.array(*objs), lambda o: o["is_source"] | o["coding_code"].isNotNull())
    return F.to_json(arr)

def codeable_concept(*objs):
    """VARIANT CodeableConcept. The source object is ALWAYS kept — 465 live rows have no source
    code, and a display-only source concept is valid; exactly-one-is_source is an invariant.
    Mapped objects are dropped when their code is null. parse_json(to_json(...)) — deterministic."""
    return F.parse_json(codeable_concept_json(*objs))

# ==== S3 flat coded-axis contract ====

# ==== S3b doctrine helpers (source of record: CC/s3b/_s3b_helpers) ====

from pyspark.sql import functions as F, Window

SNOMED_URI = "http://snomed.info/sct"

S3B_SHARED_QUANTITIES = {
    "medication_admin": ("dose_value", "dose_unit", "volume_value", "volume_unit", "rate_value", "rate_unit"),
    "medication_order": ("dose_value", "dose_unit", "volume_value", "volume_unit", "duration_value", "duration_unit"),
    "medication_dispense": ("quantity_value", "quantity_unit", "cost_value", "cost_unit"),
    "clinical_condition": (),
    "clinical_procedure": (),
}

S3B_INFERIOR_RULE_PATTERNS = (
    "EXACT_string_similarity", "bronze.map_condition:EXACT", "bronze.map_condition:EXACT_OMOP_ASSISTED",
    "bronze.map_condition:OMOP_DERIVED", "bronze.map_procedure:EXACT",
    "bronze.map_procedure:EXACT_OMOP_ASSISTED", "bronze.map_procedure:OMOP_DERIVED",
    "bronze.map_family_history:EXACT",
    "embedding:bronze_med_admin_embedding", "bronze.map_med_admin:LOOKUP_VECTOR_SIMILARITY",
    "embedding:bronze_medication_order_embedding", "embedding:imaging_local_exam", "rollup:imaging_local_exam",
    "embedding:presenting_complaint_picklist", "rollup:presenting_complaint_picklist",
)

def s3c_strength_key(value):
    # Normalize medicine-strength text for matching, removing packaging and infusion wording
    # handled by the existing rules.
    x = F.lower(F.trim(F.regexp_replace(value, r"\s+", " ")))
    x = F.regexp_replace(x, r"\b(?:intravenous\s+(?:infusion\s+)?solution|infusion)\b", " ")
    x = F.regexp_replace(x, r"\s+\d+(?:,\d{3})*(?:\.\d+)?\s*(?:ml\(s\)|mls|ml|l|litre|bag\(s\)|unit\(s\))\s*$", "")
    return F.trim(F.regexp_replace(x, r"\s+", " "))

OMOP_URI = "urn:omop:concept_id"

SRC_S3_NICIP_MAP = "3_lookup.trud.nicip_snomed_map"

SRC_S3_ICD10_MAP = "3_lookup.omop.icd10_snomed_map"

SRC_S3_OPCS4_MAP = "3_lookup.omop.opcs4_snomed_map"

SRC_S3_STAGE_MAP = "3_lookup.omop.stage_group_snomed_map"

SRC_S3_VALUE_TEXT_MAP = "3_lookup.omop.value_text_snomed_map"

SRC_S3_ALLERGY_MAP = "3_lookup.omop.allergy_substance_snomed_map"

SRC_S3_ENDOBASE_EXAM_MAP = "3_lookup.omop.endobase_exam_type_snomed_map"

SRC_S3_PATHOLOGY_TEST_ADDITIONS = "3_lookup.omop.pathology_test_snomed_additions"

SRC_S3B_MED_TEXT_MAP = "3_lookup.omop.med_text_snomed_map"

SRC_S3B_IMAGING_LOCAL_MAP_V2 = "3_lookup.omop.imaging_local_code_map"

SRC_S3B_COMPLAINT_TEXT_MAP = "3_lookup.omop.complaint_text_snomed_map"

SRC_S3B_PROCEDURE_TEXT_MAP = "3_lookup.omop.procedure_text_snomed_map"

SRC_S3B_VITAL_UNIT_MAP = "3_lookup.omop.vital_unit_ucum_map"

SRC_S3B_MEASUREMENT_CONTEXT_MAP = "3_lookup.omop.measurement_context_snomed_map"

SRC_S3C_IMAGING_NAME_MAP = "3_lookup.omop.imaging_exam_name_nicip_map"

SRC_S3C_RXNORM_MULTUM_MAP = "3_lookup.omop.rxnorm_multum_snomed_map"

SRC_S3C_ECDS_COMPLAINT_MAP = "3_lookup.omop.ecds_chief_complaint_map"

SRC_S3C_DMD_NAME_MAP = "3_lookup.omop.dmd_name_map"

SRC_S3C_RXNORM_CLASS_MAP = "3_lookup.omop.rxnorm_class_residue"

SRC_S3C_DMD_ID_MAP = "3_lookup.omop.dmd_id_map"

S3B_BRONZE_MED_EMBED_MODEL = "bronze_med_admin_vector:unversioned"

S3B_HIERARCHY_VERSION = spark.conf.get("journey.s3b_hierarchy_version", "")

def canonical_coding_system(value):
    """Return a stable URI for the measured source vocabulary spellings."""
    raw = value.cast("string")
    lowered = F.lower(F.trim(raw))
    slug = F.regexp_replace(lowered, r"[^a-z0-9]+", "-")
    slug = F.regexp_replace(slug, r"(^-+|-+$)", "")
    return (F.when(lowered.isin("snomed", "snomed ct", SNOMED_URI), F.lit(SNOMED_URI))
             .when(lowered.isin("icd-10", "icd10", "http://hl7.org/fhir/sid/icd-10"),
                   F.lit("http://hl7.org/fhir/sid/icd-10"))
             .when(lowered.isin("opcs4", "opcs-4", "http://fhir.hl7.org.uk/codesystem/opcs-4"),
                   F.lit("http://fhir.hl7.org.uk/CodeSystem/OPCS-4"))
             .when(lowered.isin("loinc", "http://loinc.org"), F.lit("http://loinc.org"))
             .when(lowered.rlike(r"^(urn:|https?://)"), raw)
             .when(raw.isNotNull(), F.concat(F.lit("urn:cerner:nomenclature:"), slug)))

def axis_columns(axis, source_only=False):
    # List the scalar source, mapped-code and provenance fields belonging to one terminology
    # axis.
    cols = [f"{axis}_source_system", f"{axis}_source_code", f"{axis}_source_display"]
    if not source_only:
        cols += [f"{axis}_snomed_code", f"{axis}_snomed_display", f"{axis}_omop_concept_id",
                 f"{axis}_map_method", f"{axis}_map_version", f"{axis}_map_confidence",
                 f"{axis}_map_rule_id", f"{axis}_map_candidate_count",
                 f"{axis}_target_system", f"{axis}_target_code", f"{axis}_target_display",
                 f"{axis}_omop_source_concept_id", f"{axis}_map_cosine", f"{axis}_map_scoring_model",
                 f"{axis}_map_status", f"{axis}_map_status_reason", f"{axis}_map_competing_count",
                 f"{axis}_map_candidate_set_id", f"{axis}_map_component_index",
                 f"{axis}_map_component_count", f"{axis}_map_rollup_levels"]
    return cols

def axis_comments(axis, subject, source_only=False):
    # Describe every source, mapped-code and provenance column belonging to one terminology
    # axis.
    comments = {
        f"{axis}_source_system": f"Canonical coding-system URI for the source {subject}.",
        f"{axis}_source_code": f"Source code for the {subject}, retained without de-identification.",
        f"{axis}_source_display": f"Source display for the {subject}.",
    }
    if not source_only:
        comments.update({
            f"{axis}_snomed_code": f"SNOMED CT code selected for the {subject}; null when no unambiguous mapping is available.",
            f"{axis}_snomed_display": f"SNOMED CT preferred display selected for the {subject}.",
            f"{axis}_omop_concept_id": f"OMOP concept identifier selected with the {subject} mapping.",
            f"{axis}_map_method": "Mapping method: source_native, rule, exact_synonym or embedding_proposed.",
            f"{axis}_map_version": "Version of the source, terminology release or model used by the mapping.",
            f"{axis}_map_confidence": "Mapping confidence in [0,1] where the lane measured one.",
            f"{axis}_map_rule_id": "Stable rule or lane identifier that produced the mapping.",
            f"{axis}_map_candidate_count": "Number of candidate targets observed; a mapped parent is unambiguous when this is 1.",
            f"{axis}_target_system": "Coding-system URI of the selected target.",
            f"{axis}_target_code": "Code of the selected target in its own vocabulary.",
            f"{axis}_target_display": "Display of the selected target in its own vocabulary.",
            f"{axis}_omop_source_concept_id": "Non-standard OMOP source concept identifier, kept separate from a standard target.",
            f"{axis}_map_cosine": "Measured cosine between the normalised source and selected target; null when scoring did not run.",
            f"{axis}_map_scoring_model": "Embedding model that produced map_cosine.",
            f"{axis}_map_status": "Explicit mapping outcome from the S3b status vocabulary.",
            f"{axis}_map_status_reason": "Reason for the mapping outcome or abstention.",
            f"{axis}_map_competing_count": "Candidates within 0.05 cosine of the selected candidate.",
            f"{axis}_map_candidate_set_id": "Stable identifier of the retained candidate set.",
            f"{axis}_map_component_index": "One-based source-derived component index.",
            f"{axis}_map_component_count": "Number of source-derived components.",
            f"{axis}_map_rollup_levels": "Maximum hierarchy distance to a selected common ancestor.",
        })
    return comments

_S3_CODING_SCHEMA = "array<struct<coding_system:string,coding_code:string,coding_display:string,is_source:boolean,map_source:string,map_version:string>>"

def _s3_flatten_codeable_json(df, json_name, axis, source_only=False, model_name=None):
    """Flatten one existing CodeableConcept JSON bridge without crossing a VARIANT join boundary."""
    arr = F.from_json(F.col(json_name), _S3_CODING_SCHEMA)
    source = F.element_at(F.filter(arr, lambda x: x["is_source"]), 1)
    snomed = F.element_at(F.filter(arr, lambda x: canonical_coding_system(x["coding_system"]) == F.lit(SNOMED_URI)), 1)
    omop = F.element_at(F.filter(arr, lambda x: canonical_coding_system(x["coding_system"]) == F.lit(OMOP_URI)), 1)
    source_system = canonical_coding_system(source["coding_system"])
    source_native = source_system == F.lit(SNOMED_URI)
    mapped = source_native | snomed["coding_code"].isNotNull() | omop["coding_code"].isNotNull()
    df = (df.withColumn(f"{axis}_source_system", source_system)
            .withColumn(f"{axis}_source_code", source["coding_code"])
            .withColumn(f"{axis}_source_display", source["coding_display"]))
    if source_only:
        return df
    has_source = source["coding_code"].isNotNull() | source["coding_display"].isNotNull()
    method_source = F.coalesce(snomed["map_source"], omop["map_source"])
    method_lower = F.lower(F.coalesce(method_source, F.lit("")))
    # Bronze embedding tiers keep stable lane names so Gold can consult the
    # measured calibration table. Other governed mappings are deterministic.
    embedding_lane = (
        F.when((F.lit(model_name) == "pathology_result") & (F.lit(axis) == "test") & method_lower.rlike("auto[_ -]?high"),
               F.lit("bronze_pathology_test_auto_high"))
         .when((F.lit(model_name) == "pathology_result") & (F.lit(axis) == "test") & method_lower.rlike("auto[_ -]?low"),
               F.lit("bronze_pathology_test_auto_low"))
         .when((F.lit(model_name) == "pathology_result") & (F.lit(axis) == "result") & method_lower.rlike("auto|embed|vector"),
               F.lit("bronze_pathology_result_auto"))
         .when((F.lit(model_name) == "procedure") & (F.lit(axis) == "device") & method_lower.rlike("layer5|gmdn|embed|vector"),
               F.lit("bronze_implant_layer5"))
         .when((F.lit(model_name) == "device") & (F.lit(axis) == "device"),
               F.lit("bronze_mediconnect_device"))
    )
    embedding = embedding_lane.isNotNull()
    deterministic_method = (F.when(method_lower.rlike("exact|synonym"), F.lit("exact_synonym"))
                              .otherwise(F.lit("rule")))
    return (df.withColumn(f"{axis}_snomed_code", F.when(source_native, source["coding_code"]).otherwise(snomed["coding_code"]))
              .withColumn(f"{axis}_snomed_display", F.when(source_native, source["coding_display"]).otherwise(snomed["coding_display"]))
              .withColumn(f"{axis}_omop_concept_id", F.expr(f"try_cast(element_at(filter(from_json(`{json_name}`, '{_S3_CODING_SCHEMA}'), x -> lower(x.coding_system) = '{OMOP_URI}'), 1).coding_code as bigint)"))
              .withColumn(f"{axis}_map_method", F.when(source_native, F.lit("source_native"))
                          .when(mapped & embedding, F.lit("embedding_proposed"))
                          .when(mapped, deterministic_method))
              .withColumn(f"{axis}_map_version", F.coalesce(snomed["map_version"], omop["map_version"]))
              .withColumn(f"{axis}_map_confidence", F.when(source_native, F.lit(1.0)).cast("double"))
              .withColumn(f"{axis}_map_rule_id", F.when(source_native, F.lit("source:canonical_snomed"))
                          .when(mapped & embedding, F.concat(F.lit("embedding:"), embedding_lane, F.lit(":tier")))
                          .when(mapped, F.concat(F.lit("source:"), F.coalesce(method_source, F.lit("governed_mapping")))))
              .withColumn(f"{axis}_map_candidate_count", F.when(mapped, F.lit(1)).otherwise(F.lit(0)).cast("int"))
              .withColumn(f"{axis}_target_system", F.when(source_native | snomed["coding_code"].isNotNull(), F.lit(SNOMED_URI))
                          .when(omop["coding_code"].isNotNull(), F.lit(OMOP_URI)))
              .withColumn(f"{axis}_target_code", F.when(source_native, source["coding_code"])
                          .otherwise(F.coalesce(snomed["coding_code"], omop["coding_code"])))
              .withColumn(f"{axis}_target_display", F.when(source_native, source["coding_display"])
                          .otherwise(F.coalesce(snomed["coding_display"], omop["coding_display"])))
              .withColumn(f"{axis}_omop_source_concept_id", F.lit(None).cast("bigint"))
              .withColumn(f"{axis}_map_cosine", F.lit(None).cast("double"))
              .withColumn(f"{axis}_map_scoring_model", F.lit(None).cast("string"))
              .withColumn(f"{axis}_map_status", F.when(mapped, F.lit("mapped")).otherwise(F.lit("no_candidate")))
              .withColumn(f"{axis}_map_status_reason", F.lit(None).cast("string"))
              .withColumn(f"{axis}_map_competing_count", F.when(mapped, F.lit(1)).otherwise(F.lit(0)).cast("int"))
              .withColumn(f"{axis}_map_candidate_set_id", F.lit(None).cast("string"))
              .withColumn(f"{axis}_map_component_index", F.when(has_source, F.lit(1)).cast("int"))
              .withColumn(f"{axis}_map_component_count", F.when(has_source, F.lit(1)).cast("int"))
              .withColumn(f"{axis}_map_rollup_levels", F.lit(None).cast("int")))

def _s3_direct_axis(df, axis, source_system, source_code, source_display,
                    snomed_code=None, snomed_display=None, omop_concept_id=None,
                    method=None, version=None, confidence=None, rule=None, candidates=None,
                    component_index=None, component_count=None, rollup_levels=None):
    # Build scalar coding and mapping-provenance expressions from already available source and
    # target values.
    col = lambda value, dtype: (value.cast(dtype) if value is not None else F.lit(None).cast(dtype))
    has_source = col(source_code, "string").isNotNull() | col(source_display, "string").isNotNull()
    has_target = (snomed_code.isNotNull() if snomed_code is not None else F.lit(False)) | \
                 (omop_concept_id.isNotNull() if omop_concept_id is not None else F.lit(False))
    return (df.withColumn(f"{axis}_source_system", canonical_coding_system(source_system))
              .withColumn(f"{axis}_source_code", col(source_code, "string"))
              .withColumn(f"{axis}_source_display", col(source_display, "string"))
              .withColumn(f"{axis}_snomed_code", col(snomed_code, "string"))
              .withColumn(f"{axis}_snomed_display", col(snomed_display, "string"))
              .withColumn(f"{axis}_omop_concept_id", col(omop_concept_id, "bigint"))
              .withColumn(f"{axis}_map_method", col(method, "string"))
              .withColumn(f"{axis}_map_version", col(version, "string"))
              .withColumn(f"{axis}_map_confidence", col(confidence, "double"))
              .withColumn(f"{axis}_map_rule_id", col(rule, "string"))
              .withColumn(f"{axis}_map_candidate_count", col(candidates, "int"))
              .withColumn(f"{axis}_target_system", F.when(col(snomed_code, "string").isNotNull(), F.lit(SNOMED_URI))
                          .when(col(omop_concept_id, "bigint").isNotNull(), F.lit(OMOP_URI)))
              .withColumn(f"{axis}_target_code", F.coalesce(col(snomed_code, "string"), col(omop_concept_id, "string")))
              .withColumn(f"{axis}_target_display", col(snomed_display, "string"))
              .withColumn(f"{axis}_omop_source_concept_id", F.lit(None).cast("bigint"))
              .withColumn(f"{axis}_map_cosine", F.lit(None).cast("double"))
              .withColumn(f"{axis}_map_scoring_model", F.lit(None).cast("string"))
              .withColumn(f"{axis}_map_status", F.when(has_target, F.lit("mapped"))
                          .when(has_source, F.lit("no_candidate")))
              .withColumn(f"{axis}_map_status_reason", F.lit(None).cast("string"))
              .withColumn(f"{axis}_map_competing_count", F.lit(None).cast("int"))
              .withColumn(f"{axis}_map_candidate_set_id", F.lit(None).cast("string"))
              .withColumn(f"{axis}_map_component_index", F.coalesce(col(component_index, "int"), F.when(has_source, F.lit(1))))
              .withColumn(f"{axis}_map_component_count", F.coalesce(col(component_count, "int"), F.when(has_source, F.lit(1))))
              .withColumn(f"{axis}_map_rollup_levels", col(rollup_levels, "int")))

def _s3_lookup_axis(df, axis, lookup_src, source_system, key_col, allow_split=False,
                    model_name=None, row_key="patient_event_key", replace_inferior=False):
    """Apply a v2 lookup, including explicit outcomes and source-derived component expansion."""
    model_name = model_name or "unknown"
    lookup = (read_source(lookup_src)
              .where((F.col("source_coding_system") == source_system) &
                     F.col("status").isin("PROPOSED", "FALLBACK", "UNMAPPED", "VETOED")))
    rank = Window.partitionBy("source_code", "component_index").orderBy(
        F.when(F.col("status") == "PROPOSED", 0).when(F.col("status") == "VETOED", 1)
         .when(F.col("status") == "UNMAPPED", 2).otherwise(3),
        F.when(F.col("mapping_method") == "rule", 0).when(F.col("mapping_method") == "ancestor_rollup", 1).otherwise(2),
        F.col("mapping_cosine").desc_nulls_last(), F.col("legacy_confidence").desc_nulls_last(),
        F.col("mapping_rule_id").asc_nulls_last(),
    )
    lookup = lookup.withColumn("_s3_rn", F.row_number().over(rank)).where("_s3_rn = 1").drop("_s3_rn")
    heads = lookup.where("component_index = 1").select(
        F.col("source_code").alias("_s3_key"), F.col("component_count").alias("_s3_head_count"),
        F.col("status").alias("_s3_head_status"),
    )
    source_row_key = "source_" + row_key
    if allow_split and source_row_key not in df.columns:
        df = df.withColumn(source_row_key, F.col(row_key))
    df = df.join(heads, key_col == F.col("_s3_key"), "left")
    existing_rule = F.coalesce(F.col(f"{axis}_map_rule_id"), F.lit(""))
    inferior = F.lit(False)
    for pattern in S3B_INFERIOR_RULE_PATTERNS:
        inferior = inferior | existing_rule.contains(pattern)
    eligible = F.col("_s3_key").isNotNull() & (
        F.col(f"{axis}_map_method").isNull() | (F.lit(replace_inferior) & inferior)
    )
    df = df.withColumn("_s3_attempt", eligible)
    if allow_split:
        components = lookup.select(F.col("source_code").alias("_s3_ckey"), F.col("component_index").alias("_s3_cidx"))
        df = df.join(components, (F.col("_s3_key") == F.col("_s3_ckey")) & F.col("_s3_attempt") &
                     (F.col("_s3_head_count") > 1), "left")
        df = df.withColumn("_s3_cidx", F.coalesce(F.col("_s3_cidx"), F.lit(1))).drop("_s3_ckey")
    else:
        df = df.withColumn("_s3_cidx", F.lit(1))
    fields = (
        "target_coding_system", "target_code", "target_display", "omop_concept_id", "omop_standard",
        "mapping_method", "mapping_version", "legacy_confidence", "mapping_rule_id", "candidate_count",
        "mapping_cosine", "scoring_model", "mapping_status", "status_reason", "competing_count",
        "candidate_set_id", "component_index", "component_count", "component_identity", "rollup_levels",
        "hierarchy_version",
    )
    records = lookup.select(F.col("source_code").alias("_s3_rkey"), F.col("component_index").alias("_s3_ridx"),
                            *[F.col(field).alias("_s3_" + field) for field in fields])
    df = df.join(records, (F.col("_s3_key") == F.col("_s3_rkey")) &
                 (F.col("_s3_cidx") == F.col("_s3_ridx")) & F.col("_s3_attempt"), "left")
    df = (df.withColumn("_s3_use_target", F.col("_s3_attempt") & F.col("_s3_target_code").isNotNull())
          .withColumn("_s3_use_outcome", F.col("_s3_attempt") & F.col("_s3_mapping_status").isNotNull() &
              (F.col(f"{axis}_map_method").isNull() | F.col("_s3_target_code").isNotNull() |
               F.col("_s3_mapping_status").startswith("unmappable"))))
    use_target, use_outcome = F.col("_s3_use_target"), F.col("_s3_use_outcome")
    if replace_inferior:
        legacy_column = (F.col(f"{axis}_legacy_snomed_code") if f"{axis}_legacy_snomed_code" in df.columns
                         else F.lit(None).cast("string"))
        df = (df.withColumn(f"{axis}_legacy_snomed_code",
                            F.when(use_target & inferior, F.col(f"{axis}_snomed_code")).otherwise(legacy_column))
              .withColumn(f"{axis}_map_confidence",
                          F.when(use_target & inferior, F.col(f"{axis}_map_confidence"))
                           .otherwise(F.coalesce(F.col("_s3_legacy_confidence"), F.col(f"{axis}_map_confidence")))))
    is_snomed = F.col("_s3_target_coding_system") == F.lit(SNOMED_URI)
    targets = {
        "target_system": F.col("_s3_target_coding_system"), "target_code": F.col("_s3_target_code"),
        "target_display": F.col("_s3_target_display"),
        "snomed_code": F.when(is_snomed, F.col("_s3_target_code")),
        "snomed_display": F.when(is_snomed, F.col("_s3_target_display")),
        "omop_concept_id": F.when(F.col("_s3_omop_standard") == "S", F.col("_s3_omop_concept_id")),
        "omop_source_concept_id": F.when(F.col("_s3_omop_standard") != "S", F.col("_s3_omop_concept_id")),
    }
    for target, expression in targets.items():
        df = df.withColumn(f"{axis}_{target}", F.when(use_target, expression).otherwise(F.col(f"{axis}_{target}")))
    outcomes = {
        "map_method": "mapping_method", "map_version": "mapping_version", "map_rule_id": "mapping_rule_id",
        "map_candidate_count": "candidate_count", "map_cosine": "mapping_cosine",
        "map_scoring_model": "scoring_model", "map_status": "mapping_status",
        "map_status_reason": "status_reason", "map_competing_count": "competing_count",
        "map_candidate_set_id": "candidate_set_id", "map_component_index": "component_index",
        "map_component_count": "component_count", "map_rollup_levels": "rollup_levels",
    }
    for target, source in outcomes.items():
        df = df.withColumn(f"{axis}_{target}", F.when(use_outcome, F.col("_s3_" + source)).otherwise(F.col(f"{axis}_{target}")))
    df = (df.withColumn(
            f"{axis}_map_component_index",
            F.when(use_outcome, F.coalesce(F.col("_s3_component_index"), F.lit(1)))
             .otherwise(F.col(f"{axis}_map_component_index")),
        )
        .withColumn(
            f"{axis}_map_component_count",
            F.when(use_outcome, F.greatest(F.coalesce(F.col("_s3_component_count"), F.lit(1)), F.lit(1)))
             .otherwise(F.col(f"{axis}_map_component_count")),
        ))
    governed_version = F.concat_ws("|", F.col("_s3_mapping_version"), F.col("_s3_hierarchy_version"))
    df = df.withColumn(
        f"{axis}_map_version",
        F.when(use_outcome, governed_version).otherwise(F.col(f"{axis}_map_version")),
    )
    if allow_split:
        df = df.withColumn(
            row_key,
            F.when(use_outcome & (F.col("_s3_component_index") > 1),
                   stable_id("s3b_component", F.col(source_row_key), F.lit(axis), F.col("_s3_component_identity")))
             .otherwise(F.col(row_key)),
        )
        for quantity in S3B_SHARED_QUANTITIES.get(model_name, ()):
            if quantity in df.columns:
                df = df.withColumn(quantity, F.when(
                    use_outcome & (F.col("_s3_component_index") > 1),
                    F.lit(None).cast(df.schema[quantity].dataType),
                ).otherwise(F.col(quantity)))
    return df.drop(*[column for column in df.columns if column.startswith("_s3_")])

def _s3_snomed_from_omop(df, axis):
    """Recover SNOMED code/display when a bronze row carried only its OMOP concept id."""
    xw=(spark.read.table(_n("journey_reference._snomed_concept_crosswalk"))
        .select(F.col("concept_id").alias("_s3_xw_id"),
                F.col("concept_code").alias("_s3_xw_code"),
                F.col("concept_name").alias("_s3_xw_display")))
    df=df.join(xw,F.col(f"{axis}_omop_concept_id")==F.col("_s3_xw_id"),"left")
    use=F.col(f"{axis}_snomed_code").isNull() & F.col("_s3_xw_code").isNotNull()
    return (df.withColumn(f"{axis}_snomed_code",F.when(use,F.col("_s3_xw_code")).otherwise(F.col(f"{axis}_snomed_code")))
              .withColumn(f"{axis}_snomed_display",F.when(use,F.col("_s3_xw_display")).otherwise(F.col(f"{axis}_snomed_display")))
              .withColumn(f"{axis}_target_system",F.when(use,F.lit(SNOMED_URI)).otherwise(F.col(f"{axis}_target_system")))
              .withColumn(f"{axis}_target_code",F.when(use,F.col("_s3_xw_code")).otherwise(F.col(f"{axis}_target_code")))
              .withColumn(f"{axis}_target_display",F.when(use,F.col("_s3_xw_display")).otherwise(F.col(f"{axis}_target_display")))
              .withColumn(f"{axis}_map_method",F.when(use,F.coalesce(F.col(f"{axis}_map_method"),F.lit("rule"))).otherwise(F.col(f"{axis}_map_method")))
              .withColumn(f"{axis}_map_rule_id",F.when(use,F.coalesce(F.col(f"{axis}_map_rule_id"),F.lit("omop:snomed_concept_crosswalk"))).otherwise(F.col(f"{axis}_map_rule_id")))
              .withColumn(f"{axis}_map_candidate_count",F.when(use,F.lit(1)).otherwise(F.col(f"{axis}_map_candidate_count")))
              .withColumn(f"{axis}_map_status",F.when(use,F.lit("mapped")).otherwise(F.col(f"{axis}_map_status")))
              .withColumn(f"{axis}_map_competing_count",F.when(use,F.lit(1)).otherwise(F.col(f"{axis}_map_competing_count")))
              .withColumn(f"{axis}_map_component_index",F.when(use,F.lit(1)).otherwise(F.col(f"{axis}_map_component_index")))
              .withColumn(f"{axis}_map_component_count",F.when(use,F.lit(1)).otherwise(F.col(f"{axis}_map_component_count")))
              .drop("_s3_xw_id","_s3_xw_code","_s3_xw_display"))

def _s3_enforce_omop_standard_target(df, axis):
    """A12: a non-standard OMOP id is source provenance, never a target."""
    concept = (read_source("3_lookup.omop.concept")
               .select(F.col("concept_id").cast("bigint").alias("_s3_omop_id"),
                       F.col("standard_concept").alias("_s3_omop_standard")))
    df = df.join(concept, F.col(f"{axis}_omop_concept_id") == F.col("_s3_omop_id"), "left")
    nonstandard = F.col(f"{axis}_omop_concept_id").isNotNull() & (F.coalesce(F.col("_s3_omop_standard"), F.lit("")) != "S")
    only_nonstandard = nonstandard & F.col(f"{axis}_snomed_code").isNull()
    return (df.withColumn(f"{axis}_omop_source_concept_id",
                          F.when(nonstandard, F.col(f"{axis}_omop_concept_id"))
                           .otherwise(F.col(f"{axis}_omop_source_concept_id")))
              .withColumn(f"{axis}_omop_concept_id", F.when(nonstandard, F.lit(None).cast("bigint"))
                          .otherwise(F.col(f"{axis}_omop_concept_id")))
              .withColumn(f"{axis}_target_system", F.when(only_nonstandard, F.lit(None).cast("string"))
                          .otherwise(F.col(f"{axis}_target_system")))
              .withColumn(f"{axis}_target_code", F.when(only_nonstandard, F.lit(None).cast("string"))
                          .otherwise(F.col(f"{axis}_target_code")))
              .withColumn(f"{axis}_target_display", F.when(only_nonstandard, F.lit(None).cast("string"))
                          .otherwise(F.col(f"{axis}_target_display")))
              .withColumn(f"{axis}_map_status", F.when(only_nonstandard, F.lit("no_candidate"))
                          .otherwise(F.col(f"{axis}_map_status")))
              .withColumn(f"{axis}_map_status_reason",
                          F.when(only_nonstandard, F.lit("non-standard OMOP source concept retained separately"))
                           .otherwise(F.col(f"{axis}_map_status_reason")))
              .drop("_s3_omop_id", "_s3_omop_standard"))

def _s3_apply_tier_rescore(df, axis, lane, source_display):
    """Attach measured cosine to a legacy tier lane; polarity vetoes also clear its target."""
    target = F.coalesce(F.col(f"{axis}_snomed_code"), F.col(f"{axis}_omop_concept_id").cast("string"))
    scored = (read_source("8_dev.lookup.tier_lane_rescore").where(F.col("lane") == lane)
              .select(F.lower(F.trim(F.regexp_replace(F.col("source_display"), r"\s+", " "))).alias("_s3_tier_source"),
                      F.col("target_code").alias("_s3_tier_target"),
                      F.col("mapping_cosine").alias("_s3_tier_cosine"),
                      F.col("mapping_status").alias("_s3_tier_status"),
                      F.col("mapping_rule_id").alias("_s3_tier_rule"),
                      F.col("scoring_model").alias("_s3_tier_model"),
                      F.col("polarity_verdict").alias("_s3_tier_polarity"),
                      F.col("rescored_at").alias("_s3_tier_rescored_at"))
              .withColumn("_s3_tier_rn", F.row_number().over(Window.partitionBy(
                  "_s3_tier_source", "_s3_tier_target").orderBy(
                      F.desc("_s3_tier_cosine"), F.desc("_s3_tier_rescored_at"))))
              .where("_s3_tier_rn=1").drop("_s3_tier_rn"))
    source = F.lower(F.trim(F.regexp_replace(source_display.cast("string"), r"\s+", " ")))
    df = df.join(scored, (source == F.col("_s3_tier_source")) & (target == F.col("_s3_tier_target")), "left")
    matched = F.col("_s3_tier_rule").isNotNull()
    vetoed = F.col("_s3_tier_status") == "vetoed_polarity"
    df = (df.withColumn(f"{axis}_map_cosine", F.when(matched, F.col("_s3_tier_cosine")).otherwise(F.col(f"{axis}_map_cosine")))
          .withColumn(f"{axis}_map_scoring_model", F.when(matched, F.col("_s3_tier_model")).otherwise(F.col(f"{axis}_map_scoring_model")))
          .withColumn(f"{axis}_map_status", F.when(matched, F.col("_s3_tier_status")).otherwise(F.col(f"{axis}_map_status")))
          .withColumn(f"{axis}_map_rule_id", F.when(matched, F.col("_s3_tier_rule")).otherwise(F.col(f"{axis}_map_rule_id")))
          .withColumn(f"{axis}_map_version", F.when(matched,
                      F.concat_ws("|", F.col("_s3_tier_model"), F.lit(S3B_HIERARCHY_VERSION)))
                      .otherwise(F.col(f"{axis}_map_version"))))
    for suffix in ("snomed_code", "snomed_display", "omop_concept_id", "target_system", "target_code", "target_display"):
        df = df.withColumn(f"{axis}_{suffix}", F.when(vetoed, F.lit(None).cast(df.schema[f"{axis}_{suffix}"].dataType))
                           .otherwise(F.col(f"{axis}_{suffix}")))
    return df.drop(*[c for c in df.columns if c.startswith("_s3_tier_")])

def _s3_apply_bronze_snomed_provenance(df, model_name):
    """Restore the measured SNOMED method, similarity and ambiguity beside flat axes."""
    if model_name == "family_history":
        meta = (read_source(SRC_FAMILY_HISTORY_PRODUCT)
                .select(F.col("FHX_ACTIVITY_ID").cast("bigint").alias("_s3_meta_id"),
                        F.col("SNOMED_TYPE").alias("_s3_meta_type"),
                        F.col("SNOMED_SIMILARITY").cast("double").alias("_s3_meta_similarity"),
                        F.col("SNOMED_MATCH_NUMBER").cast("int").alias("_s3_meta_candidates")))
        df = df.join(meta, F.col("fhx_activity_id") == F.col("_s3_meta_id"), "left")
        axis, source = "condition", "bronze.map_family_history"
    elif model_name == "condition":
        diagnosis = (read_source(SRC_DIAGNOSIS)
                     .select(F.col("DIAGNOSIS_ID").cast("bigint").alias("_s3_diag_id"),
                             F.col("SNOMED_TYPE").alias("_s3_diag_type"),
                             F.col("SNOMED_SIMILARITY").cast("double").alias("_s3_diag_similarity"),
                             F.col("SNOMED_MATCH_NUMBER").cast("int").alias("_s3_diag_candidates")))
        problem = (read_source(SRC_PROBLEM)
                   .select(F.col("PROBLEM_ID").cast("bigint").alias("_s3_problem_id"),
                           F.col("SNOMED_TYPE").alias("_s3_problem_type"),
                           F.col("SNOMED_SIMILARITY").cast("double").alias("_s3_problem_similarity"),
                           F.col("SNOMED_MATCH_NUMBER").cast("int").alias("_s3_problem_candidates")))
        df = (df.join(diagnosis, F.col("diagnosis_id") == F.col("_s3_diag_id"), "left")
                .join(problem, F.col("problem_id") == F.col("_s3_problem_id"), "left")
                .withColumn("_s3_meta_type", F.when(F.col("source_object") == "diagnosis", F.col("_s3_diag_type"))
                            .when(F.col("source_object") == "problem", F.col("_s3_problem_type")))
                .withColumn("_s3_meta_similarity", F.when(F.col("source_object") == "diagnosis", F.col("_s3_diag_similarity"))
                            .when(F.col("source_object") == "problem", F.col("_s3_problem_similarity")))
                .withColumn("_s3_meta_candidates", F.when(F.col("source_object") == "diagnosis", F.col("_s3_diag_candidates"))
                            .when(F.col("source_object") == "problem", F.col("_s3_problem_candidates"))))
        axis, source = "condition", "bronze.map_condition"
    elif model_name == "procedure":
        meta = (read_source(SRC_PROCEDURE)
                .select(F.col("PROCEDURE_ID").cast("bigint").alias("_s3_meta_id"),
                        F.col("SNOMED_TYPE").alias("_s3_meta_type"),
                        F.col("SNOMED_SIMILARITY").cast("double").alias("_s3_meta_similarity"),
                        F.col("SNOMED_MATCH_NUMBER").cast("int").alias("_s3_meta_candidates")))
        df = df.join(meta, F.col("procedure_id") == F.col("_s3_meta_id"), "left")
        axis, source = "procedure", "bronze.map_procedure"
    else:
        return df
    mapped = F.col(f"{axis}_snomed_code").isNotNull()
    native = F.col(f"{axis}_map_method") == "source_native"
    exact = F.lower(F.coalesce(F.col("_s3_meta_type"), F.lit(""))).contains("exact")
    return (df.withColumn(f"{axis}_map_method",
                         F.when(native, F.lit("source_native"))
                          .when(mapped & exact, F.lit("exact_synonym"))
                          .when(mapped & F.col("_s3_meta_type").isNotNull(), F.lit("rule"))
                          .otherwise(F.col(f"{axis}_map_method")))
              .withColumn(f"{axis}_map_confidence",
                          F.when(native, F.lit(1.0)).when(mapped, F.coalesce(F.col("_s3_meta_similarity"), F.col(f"{axis}_map_confidence")))
                           .otherwise(F.col(f"{axis}_map_confidence")))
              .withColumn(f"{axis}_map_candidate_count",
                          F.when(native, F.lit(1)).when(mapped, F.coalesce(F.col("_s3_meta_candidates"), F.col(f"{axis}_map_candidate_count"), F.lit(1)))
                           .otherwise(F.col(f"{axis}_map_candidate_count")))
              .withColumn(f"{axis}_map_rule_id",
                          F.when(native, F.col(f"{axis}_map_rule_id"))
                           .when(mapped & exact, F.lit(source + ":EXACT_string_similarity"))
                           .when(mapped & F.col("_s3_meta_type").isNotNull(), F.concat(F.lit(source + ":"), F.col("_s3_meta_type")))
                           .otherwise(F.col(f"{axis}_map_rule_id")))
              .drop(*[name for name in df.columns if name.startswith("_s3_diag_") or name.startswith("_s3_problem_")])
              .drop("_s3_meta_id", "_s3_meta_type", "_s3_meta_similarity", "_s3_meta_candidates"))

S3_PUBLIC_CODED_AXES = {
    "presenting_complaint": {},
    "family_history": {"condition_code": ("condition", False)},
    "condition": {"condition_code": ("condition", False)},
    "procedure": {"procedure_code": ("procedure", False), "device_code": ("device", False)},
    "specimen": {"specimen_type": ("specimen_type", False)},
    "pathology_report": {"report_code": ("report", True)},
    "pathology_result": {"result_code": ("test", False)},
    "vital_sign": {"vital_code": ("vital", False)},
    "clinical_score": {"score_code": ("score", False)},
    "medication_admin": {"medication_code": ("medication", False)},
    "medication_order": {"medication_code": ("medication", False)},
    "medication_dispense": {"medication_code": ("medication", False)},
    "clinical_finding": {"finding_code": ("finding", False)},
    "imaging_exam": {"exam_code": ("exam", False)},
    "allergy_intolerance": {"substance_code": ("substance", False)},
    "cancer_treatment": {"drug_code": ("drug", False)},
    "condition_stage": {"stage_code": ("stage_diagnosis", False)},
    "device": {"device_code": ("device", False)},
    "document": {"document_type": ("document_type", True)},
}

S3_TABLE_AXIS_SPECS = {
    "clinical_presenting_complaint": {},
    "clinical_family_history": {"condition_code": [("condition", False, "family-history condition")]},
    "clinical_condition": {"condition_code": [("condition", False, "condition")]},
    "clinical_procedure": {"procedure_code": [("procedure", False, "procedure")], "device_code": [("device", False, "implanted device")]},
    "clinical_specimen": {"specimen_type": [("specimen_type", False, "specimen type")]},
    "clinical_pathology_report": {"report_code": [("report", True, "pathology report type")]},
    "clinical_pathology_result": {"result_code": [("test", False, "pathology test"), ("result", False, "pathology result value")]},
    "clinical_vital_sign": {"vital_code": [("vital", False, "vital sign")]},
    "clinical_clinical_score": {"score_code": [("score", False, "clinical score")]},
    "clinical_medication_admin": {"medication_code": [("medication", False, "administered medication")]},
    "clinical_medication_order": {"medication_code": [("medication", False, "ordered medication")]},
    "clinical_medication_dispense": {"medication_code": [("medication", False, "dispensed medication")]},
    "clinical_clinical_finding": {"finding_code": [("finding", False, "clinical finding"), ("value", False, "coded result value")]},
    "clinical_imaging_exam": {"exam_code": [("exam", False, "imaging examination")]},
    "artifact_asset": {"artifact_type": [("artifact_type", True, "artifact type")]},
    "text_document": {"document_type": [("document_type", True, "document type")]},
    "clinical_allergy_intolerance": {"substance_code": [("substance", False, "allergy substance")]},
    "clinical_cancer_treatment": {"drug_code": [("drug", False, "cancer treatment drug")]},
    "clinical_condition_stage": {"stage_code": [("stage_diagnosis", False, "staging diagnosis"), ("stage_group", False, "stage group")]},
    "clinical_endoscopy_finding": {"finding_code": [("finding", False, "endoscopy finding")]},
    "clinical_device": {"device_code": [("device", False, "device")]},
}




# ==== Cross-table quality flags ====

# Cross-table quality flags for products that publish a VARIANT column. The private
# stage does the joining with the VARIANT serialized to a JSON string, and the public
# product is a single-source parse_json wrapper over it: a VARIANT column coming out
# of a join-shaped flow crashes incremental planning and forces a full recompute on
# every update.
SILVER_CROSS_QC = {
    "presenting_complaint": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "condition": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "allergy_intolerance": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "appointment": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "cancer_treatment": {"fk": (), "dates": ("event_after_death_30d", "event_before_birth")},
    "condition_stage": {"fk": (), "dates": ("event_after_death_30d", "event_before_birth")},
    "device": {"fk": (), "dates": ("event_after_death_30d", "event_before_birth")},
    "family_history": {"fk": (), "dates": ("event_after_death_30d",)},
    "form": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "imaging_exam": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "medication_admin": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "medication_dispense": {"fk": (), "dates": ("event_before_birth",)},
    "pathology_report": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "procedure": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "registry_entry": {"fk": (), "dates": ("event_after_death_30d", "event_before_birth")},
    "rtt_pathway": {"fk": ("person_id",), "dates": ("event_after_death_30d", "event_before_birth")},
    "clinical_finding": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "clinical_score": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "medication_order": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "pathology_result": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "specimen": {"fk": ("person_id",), "dates": ("event_after_death_30d", "event_before_birth")},
    "vital_sign": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
    "document": {"fk": ("encounter_id", "person_id"), "dates": ("event_after_death_30d", "event_before_birth")},
}

# Ben's 2026-09-05 ruling: source history is public; quality and batch metadata is internal.
LIFECYCLE_FIELD_NAMES = [
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'episode_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'is_illogical',
    'load_batch_id',
]

LIFECYCLE_COLUMN_COMMENTS = {
    "identity_status": "Subject identity resolution state recorded by the silver canonical query.",
    "record_status": "active, superseded or retracted, normalised from the source's own status fields; superseded rows are earlier versions the source kept.",
    "record_status_effective_from": "Timestamp from which the normalised record status applies, when the source supplies one.",
    "record_status_effective_to": "Timestamp until which the normalised record status applies; null means no source end is known.",
    "person_id_resolved": "True when person_id is null or resolves to the governed person spine; false records unresolved linkage without dropping the row.",
    "encounter_id_resolved": "True when encounter_id is null or resolves to the governed encounter spine; false records unresolved linkage without dropping the row.",
    "episode_id_resolved": "True when episode_id is null or resolves to the governed episode spine; false records unresolved linkage without dropping the row.",
    "event_before_birth": "True when the event timestamp precedes the resolved person's birth timestamp; silver records the doubt and does not filter the row.",
    "event_after_death_30d": "True when the event timestamp is more than 30 days after the resolved person's death timestamp; silver records the doubt and does not filter the row.",
    "is_illogical": "Source RTT chronology flag indicating an illogical activity sequence; retained for the gold drop rule.",
    "load_batch_id": "Deterministic batch token derived from the source row's bronze load timestamp.",
    "source_update_timestamp": "Native source-system update timestamp carried through the canonical silver query.",
    "loaded_at": "Latest bronze load timestamp contributing to the public row; never the lifecycle pipeline's wall-clock time.",
    "person_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "person_identifier_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "source_identifier_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "episode_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "episode_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "episode_encounter_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "episode_encounter_reltn_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "encounter_identifier_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "location_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "location_code": "Primary-key component carried unchanged from the public parent; combine it with the companion's other key columns when joining.",
    "practitioner_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "practitioner_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "service_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "service_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "organization_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "organization_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "patient_event_key": "Deterministic SHA-256 event key carried unchanged from the public parent; joins one lifecycle row to that event row.",
    "fhx_activity_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "patient_event_row_key": "Deterministic row-level key carried unchanged from events_patient_event; joins one lifecycle row to one event-index row.",
    "encounter_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "encounter_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "location_stay_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "care_participation_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "concept_map_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "value_set_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "source_object": "Source-arm discriminator carried unchanged from the public parent; use it with arm-specific native identifiers when joining.",
    "diagnosis_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "problem_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "endobase_exam_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "implant_event_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "implant_sequence": "Primary-key component carried unchanged from the public parent; combine it with the companion's other key columns when joining.",
    "procedure_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "surg_case_proc_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "genetic_test_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "genetic_result_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "gene_tested_row_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "indication_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "microbiology_isolate_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "susceptibility_result_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "request_thread_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "dcp_forms_activity_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "doc_response_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "event_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "order_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "pharmacy_issue_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "sequence_nbr": "Primary-key component carried unchanged from the public parent; combine it with the companion's other key columns when joining.",
    "pacs_examination_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "dicom_path": "Primary-key component carried unchanged from the public parent; combine it with the companion's other key columns when joining.",
    "artifact_link_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "update_count": "Primary-key component carried unchanged from the public parent; combine it with the companion's other key columns when joining.",
    "valid_from_datetime": "Primary-key component carried unchanged from the public parent; combine it with the companion's other key columns when joining.",
    "pacs_report_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "report_version_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "person_relationship_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "birth_row_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "journey_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "journey_participant_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "journey_link_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "sch_event_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "source_system_oid": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "referral_oid": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "pathway_oid": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "period_oid": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "rtt_activity_oid": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "pm_wait_list_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "row_source": "Physical bronze row source, CURRENT or HIST, carried unchanged from the public parent; a key component because the research table is at physical version grain and patient_event_key is shared across an entry's versions.",
    "source_version_id": "Native source version identifier carried unchanged from the public parent; CURRENT rows carry -1. A key component alongside row_source.",
    "waiting_list_snapshot_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "snapshot_date": "Primary-key component carried unchanged from the public parent; combine it with the companion's other key columns when joining.",
    "allergy_instance_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "transfusion_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "transfusion_event_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "bloodtrack_transaction_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "treatment_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "cancer_treatment_cycle_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "chemotherapy_course_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "treatment_cycle_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "aria_pt_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "aria_dx_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "endobase_exam_term_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "entry_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "mc_device_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "community_care_contact_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "community_care_activity_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "cds_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "extract_cd": "Primary-key component carried unchanged from the public parent; combine it with the companion's other key columns when joining.",
    "activity_record_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "cost_line_item_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "homecare_request_item_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "waiting_list_oid": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "elective_access_procedure_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "ptl_unique_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "pregnancy_reconciliation_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "period_business_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "admission_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "source_unit": "Primary-key component carried unchanged from the public parent; combine it with the companion's other key columns when joining.",
    "source_site": "Primary-key component carried unchanged from the public parent; combine it with the companion's other key columns when joining.",
    "source_daily_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "score_type": "Primary-key component carried unchanged from the public parent; combine it with the companion's other key columns when joining.",
    "entity_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "activity_date": "Primary-key component carried unchanged from the public parent; combine it with the companion's other key columns when joining.",
    "labour_delivery_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "care_contact_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "pregnancy_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "research_study_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "pt_prot_reg_id": "Native source identifier carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "person_address_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "person_death_evidence_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "device_mapping_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "encounter_bounds_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "practitioner_identifier_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "practitioner_group_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "practitioner_location_evidence_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "theatre_attendance_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "theatre_case_milestone_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "theatre_implant_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "encounter_attribute_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
    "person_attribute_key": "Deterministic v2 key carried unchanged from the public parent; joins one lifecycle row to that research row.",
}

def _cross_qc_primitive(df, model_name, variant_json_columns):
    # Replace configured VARIANT fields with JSON strings and select the internal lifecycle-
    # stage columns.
    for public_name, json_name in variant_json_columns.items():
        if public_name in df.columns:
            df = df.withColumn(json_name, F.to_json(F.col(public_name))).drop(public_name)
        elif json_name not in df.columns:
            raise RuntimeError(
                f"{model_name}: missing VARIANT column {public_name!r} and JSON bridge {json_name!r}"
            )

    spec = SILVER_CROSS_QC[model_name]
    fk_columns = spec["fk"]
    date_flags = spec["dates"]
    if "person_id" in fk_columns or date_flags:
        person = (
            spark.read.table(_n("journey_spine.person"))
            .select(
                F.col("person_id").alias("_qc_person_id"),
                F.col("birth_datetime").alias("_qc_birth_datetime"),
                F.col("deceased_datetime").alias("_qc_deceased_datetime"),
            )
            .where(F.col("_qc_person_id").isNotNull())
            .dropDuplicates(["_qc_person_id"])
        )
        df = df.join(person, df.person_id == person._qc_person_id, "left")
        if "person_id" in fk_columns:
            df = df.withColumn(
                "person_id_resolved",
                F.col("person_id").isNull() | F.col("_qc_person_id").isNotNull(),
            )
        if "event_before_birth" in date_flags:
            df = df.withColumn(
                "event_before_birth",
                F.coalesce(
                    F.col("event_datetime") < F.col("_qc_birth_datetime"),
                    F.lit(False),
                ),
            )
        if "event_after_death_30d" in date_flags:
            df = df.withColumn(
                "event_after_death_30d",
                F.coalesce(
                    F.col("event_datetime")
                    > F.col("_qc_deceased_datetime") + F.expr("INTERVAL 30 DAYS"),
                    F.lit(False),
                ),
            )
        df = df.drop("_qc_person_id", "_qc_birth_datetime", "_qc_deceased_datetime")
    if "encounter_id" in fk_columns:
        encounter = (
            spark.read.table(_n("journey_spine.encounter"))
            .select(F.col("encounter_id").alias("_qc_encounter_id"))
            .where(F.col("_qc_encounter_id").isNotNull())
            .dropDuplicates(["_qc_encounter_id"])
        )
        df = (
            df.join(encounter, df.encounter_id == encounter._qc_encounter_id, "left")
            .withColumn(
                "encounter_id_resolved",
                F.col("encounter_id").isNull() | F.col("_qc_encounter_id").isNotNull(),
            )
            .drop("_qc_encounter_id")
        )
    return df

def _cross_qc_public(df, model_name, variant_json_columns, public_columns, lifecycle_columns):
    # contract v2: lifecycle columns are validated here but never appended to the public parent.
    # Read the internal stage and reconstruct the public schema, rejecting lifecycle fields in
    # the public column list.
    leaked = [column for column in public_columns if column in LIFECYCLE_FIELD_NAMES]
    if leaked:
        raise RuntimeError(f"{model_name}: lifecycle columns remain in the public list: {leaked}")
    missing = [column for column in lifecycle_columns if column not in df.columns]
    if missing:
        raise RuntimeError(f"{model_name}: lifecycle source columns are missing: {missing}")
    if "source_coding_system" in df.columns:
        df = df.withColumn("source_coding_system", canonical_coding_system(F.col("source_coding_system")))
    retired = S3_PUBLIC_CODED_AXES.get(model_name, {})
    for public_name, json_name in variant_json_columns.items():
        if public_name in retired:
            axis, source_only = retired[public_name]
            df = _s3_flatten_codeable_json(df, json_name, axis, source_only, model_name)
        else:
            df = df.withColumn(public_name, F.expr(f"parse_json(`{json_name}`)"))
    if model_name == "pathology_result":
        present = F.col("_result_snomed_code").isNotNull() | F.col("_result_omop_code").isNotNull()
        result_embedding = present & F.lower(F.col("_result_confidence_tier")).rlike("auto")
        df = _s3_direct_axis(
            df, "result", F.concat(F.lit("urn:barts:pathology:result-value:"), F.coalesce(F.col("source_code"), F.lit("unknown"))),
            F.lower(F.trim(F.coalesce(F.col("value_text"), F.col("value_concept_display")))),
            F.coalesce(F.col("value_text"), F.col("value_concept_display")),
            snomed_code=F.col("_result_snomed_code"), snomed_display=F.col("_result_snomed_display"),
            omop_concept_id=F.expr("try_cast(_result_omop_code as bigint)"),
            method=F.when(result_embedding, F.lit("embedding_proposed")).when(present, F.lit("rule")),
            rule=F.when(result_embedding, F.lit("embedding:bronze_pathology_result_auto:tier"))
                  .when(present, F.lit("bronze.map_pathology:result_concept")),
            candidates=F.when(present, F.lit(1)).otherwise(F.lit(0)))
    elif model_name == "clinical_finding":
        df = _s3_direct_axis(
            df, "value", F.lit("urn:cerner:nomenclature:answer-text"),
            F.lower(F.trim(F.coalesce(F.col("value_display"), F.col("value_code"), F.col("value_text")))),
            F.coalesce(F.col("value_display"), F.col("value_text")))
    elif model_name == "condition_stage":
        df = _s3_direct_axis(
            df, "stage_group", F.lit("urn:barts:aria:stage-of-disease"),
            F.lower(F.trim(F.col("stage_of_disease"))), F.col("stage_of_disease"))
    elif model_name == "vital_sign":
        for _axis, _value in (
            ("unit", F.col("unit_source_value")),
            ("method", F.col("method_display")),
            ("body_site", F.col("body_site_display")),
            ("interpretation", F.col("interpretation_display")),
        ):
            df = _s3_direct_axis(
                df, _axis, F.lit(f"urn:barts:vital:{_axis}"),
                F.lower(F.trim(F.regexp_replace(_value, r"\s+", " "))), _value,
            )
    elif model_name == "procedure":
        present = F.col("_device_snomed_code").isNotNull()
        device_embedding = present & (F.col("_device_mapping_layer") == "LAYER5_GMDN_EMBEDDING")
        df = _s3_direct_axis(
            # Task 1.6 proved SNOMED_DEVICE_CONCEPT_ID is an OMOP concept_id in this
            # source despite its name. Preserve that identifier in the OMOP slot and
            # let the governed SNOMED crosswalk recover concept_code/display below.
            df, "device", F.lit("urn:barts:implant:device"), F.col("device_code"), F.col("device_display"),
            omop_concept_id=F.expr("try_cast(_device_snomed_code as bigint)"),
            method=F.when(device_embedding, F.lit("embedding_proposed")).when(present, F.lit("rule")),
            confidence=F.when(present, F.col("_device_mapping_confidence")),
            rule=F.when(device_embedding, F.lit("embedding:bronze_implant_layer5:tier"))
                  .when(present, F.concat(F.lit("bronze.map_implant_details:"),
                                         F.coalesce(F.col("_device_mapping_layer"), F.lit("mapped")))),
            candidates=F.when(present, F.lit(1)).otherwise(F.lit(0)))
    elif model_name == "device":
        # The MediConnect fields named *_SNOMED_CONCEPT_ID are likewise OMOP
        # concept_ids. The JSON bridge historically labelled them as SNOMED codes.
        # Correct the slot before the shared OMOP-to-SNOMED crosswalk runs.
        df = (df.withColumn("device_omop_concept_id", F.expr("try_cast(device_snomed_code as bigint)"))
                .withColumn("device_snomed_code", F.lit(None).cast("string"))
                .withColumn("device_snomed_display", F.lit(None).cast("string")))
    # Freeze bronze provenance before authoritative lookups decide whether a row is replaceable.
    df = _s3_apply_bronze_snomed_provenance(df, model_name)
    # Deterministic S3 lookups fill only an otherwise unmapped axis. Each lookup
    # is reduced to one explicitly ranked row before the left join.
    if model_name == "family_history":
        df = _s3_lookup_axis(
            df, "condition", SRC_S3_ICD10_MAP, "http://hl7.org/fhir/sid/icd-10",
            F.when(F.col("condition_source_system") == "http://hl7.org/fhir/sid/icd-10",
                   F.regexp_replace(F.upper(F.col("condition_source_code")), r"[^A-Z0-9]", "")),
            allow_split=True, model_name=model_name, replace_inferior=True)
    elif model_name == "imaging_exam":
        df = _s3_lookup_axis(df, "exam", SRC_S3_NICIP_MAP, "urn:nhs:nicip", F.upper(F.trim(F.col("source_code"))), model_name=model_name)
        norm_display = F.lower(F.trim(F.regexp_replace(F.col("source_display"), r"\s+", " ")))
        df = _s3_lookup_axis(df, "exam", SRC_S3C_IMAGING_NAME_MAP, "urn:cerner:radiology-exam",
                             F.when(F.col("source_coding_system") == "urn:cerner:radiology-exam", norm_display), model_name=model_name, replace_inferior=True)
        df = _s3_lookup_axis(df, "exam", SRC_S3C_IMAGING_NAME_MAP, "urn:sectra:examination-code",
                             F.when(F.col("source_coding_system") == "urn:sectra:examination-code", norm_display), model_name=model_name, replace_inferior=True)
        df = _s3_lookup_axis(df, "exam", SRC_S3B_IMAGING_LOCAL_MAP_V2, "urn:barts:imaging-local-exam",
                             F.concat_ws("|", F.col("exam_source_system"), F.col("exam_source_code")), model_name=model_name)
    elif model_name == "allergy_intolerance":
        df = _s3_lookup_axis(df, "substance", SRC_S3_ALLERGY_MAP, "urn:cerner:allergy-substance-text",
                             F.lower(F.trim(F.regexp_replace(F.col("source_display"), r"\s+", " "))), model_name=model_name)
    elif model_name == "clinical_finding":
        df = _s3_lookup_axis(df, "value", SRC_S3_VALUE_TEXT_MAP, "urn:cerner:nomenclature:answer-text",
                             F.lower(F.trim(F.regexp_replace(F.coalesce(F.col("value_display"), F.col("value_text")), r"\s+", " "))), model_name=model_name)
    elif model_name == "condition_stage":
        df = _s3_lookup_axis(df, "stage_group", SRC_S3_STAGE_MAP, "urn:barts:aria:stage-of-disease",
                             F.lower(F.trim(F.regexp_replace(F.col("stage_of_disease"), r"\s+", " "))), model_name=model_name)
        df = _s3_lookup_axis(
            df, "stage_diagnosis", SRC_S3_ICD10_MAP, "http://hl7.org/fhir/sid/icd-10",
            F.when(F.col("stage_diagnosis_source_system") == "http://hl7.org/fhir/sid/icd-10",
                   F.regexp_replace(F.upper(F.col("stage_diagnosis_source_code")), r"[^A-Z0-9]", "")),
            model_name=model_name, replace_inferior=True)
    elif model_name == "condition":
        df = _s3_lookup_axis(
            df, "condition", SRC_S3_ICD10_MAP, "http://hl7.org/fhir/sid/icd-10",
            F.when(F.col("condition_source_system") == "http://hl7.org/fhir/sid/icd-10",
                   F.regexp_replace(F.upper(F.col("condition_source_code")), r"[^A-Z0-9]", "")),
            allow_split=True, model_name=model_name, replace_inferior=True)
    elif model_name == "procedure":
        df = _s3_lookup_axis(
            df, "procedure", SRC_S3_OPCS4_MAP, "http://fhir.hl7.org.uk/CodeSystem/OPCS-4",
            F.when(F.col("procedure_source_system") == "http://fhir.hl7.org.uk/CodeSystem/OPCS-4",
                   F.regexp_replace(F.upper(F.col("procedure_source_code")), r"[^A-Z0-9]", "")),
            allow_split=True, model_name=model_name, replace_inferior=True)
        df = _s3_lookup_axis(
            df, "procedure", SRC_S3_ENDOBASE_EXAM_MAP, "urn:barts:endobase:exam-type",
            F.when(F.col("source_object") == "endobase_exam",
                   F.lower(F.trim(F.regexp_replace(F.col("procedure_source_code"), r"\s+", " ")))), model_name=model_name)
        df = _s3_lookup_axis(
            df, "procedure", SRC_S3B_PROCEDURE_TEXT_MAP, "urn:barts:procedure-text",
            F.sha2(F.concat_ws("|",
                F.when(F.col("source_object") == "endobase_exam", F.lit("urn:barts:endobase-exam-type"))
                 .when(F.col("source_object").contains("implant"), F.lit("urn:barts:implant-primary-procedure"))
                 .otherwise(F.lit("urn:barts:surginet-procedure")),
                F.lower(F.trim(F.regexp_replace(F.col("procedure_source_display"), r"\s+", " ")))), 256),
            allow_split=True, model_name=model_name)
    elif model_name == "pathology_result":
        df = _s3_lookup_axis(
            df, "test", SRC_S3_PATHOLOGY_TEST_ADDITIONS, "urn:barts:pathology:test:cerner_testcode",
            F.when(F.lower(F.col("test_source_system")).contains("cerner-testcode"), F.col("test_source_code")), model_name=model_name)
        df = _s3_lookup_axis(
            df, "test", SRC_S3_PATHOLOGY_TEST_ADDITIONS, "urn:barts:pathology:test:tfc",
            F.when(F.lower(F.col("test_source_system")).contains("tfc"), F.col("test_source_code")), model_name=model_name)
    elif model_name == "presenting_complaint":
        df = _s3_direct_axis(df, "complaint", F.col("source_coding_system"), F.col("source_code"), F.col("source_display"))
        df = _s3_lookup_axis(
            df, "complaint", SRC_S3C_ECDS_COMPLAINT_MAP,
            "urn:cerner:nomenclature:ed-presenting-complaint",
            F.when(F.col("source_object").isin("ed_presenting_complaint", "ed_presenting_complaint_text"),
                   F.col("source_code")), model_name=model_name)
        df = _s3_lookup_axis(
            df, "complaint", SRC_S3B_COMPLAINT_TEXT_MAP, "urn:barts:complaint-text",
            F.when(F.col("source_object").isin("registration_reason_for_visit", "ed_presenting_complaint_text"),
                   F.col("complaint_text_normalised")), allow_split=True, model_name=model_name)
    elif model_name == "vital_sign":
        df = _s3_lookup_axis(
            df, "unit", SRC_S3B_VITAL_UNIT_MAP, "urn:barts:vital:unit",
            F.lower(F.trim(F.regexp_replace(F.col("unit_source_value"), r"\s+", " "))),
            model_name=model_name,
        )
        for _axis, _value in (
            ("method", F.col("method_display")),
            ("body_site", F.col("body_site_display")),
            ("interpretation", F.col("interpretation_display")),
        ):
            df = _s3_lookup_axis(
                df, _axis, SRC_S3B_MEASUREMENT_CONTEXT_MAP, f"urn:barts:vital:{_axis}",
                F.lower(F.trim(F.regexp_replace(_value, r"\s+", " "))),
                model_name=model_name,
            )
    if model_name == "medication_order":
        b=(read_source("4_prod.bronze.map_medication_order").select(
            F.col("ORDER_ID").cast("bigint").alias("_s3_order_id"),
            F.col("MULTUM_CODE").cast("string").alias("_s3c_multum"),
            F.col("SNOMED_CODE").cast("string").alias("_s3_med_snomed"),
            F.coalesce("OMOP_STANDARD_CONCEPT_NAME", "OMOP_CONCEPT_NAME").alias("_s3_med_snomed_display"),
            F.col("OMOP_STANDARD_CONCEPT_ID").cast("bigint").alias("_s3_med_omop"),
            F.coalesce("OMOP_STANDARD_MAPPING_METHOD","OMOP_MAPPING_METHOD").alias("_s3_med_bronze_method"),
            F.col("OMOP_MAPPING_CONFIDENCE").cast("double").alias("_s3_med_confidence"),
            F.col("OMOP_STANDARD_CANDIDATE_COUNT").cast("int").alias("_s3_med_candidates")))
        df=df.join(b,F.col("order_id")==F.col("_s3_order_id"),"left")
        mapped=F.col("_s3_med_snomed").isNotNull()|F.col("_s3_med_omop").isNotNull()
        order_embedding = F.upper(F.coalesce(F.col("_s3_med_bronze_method"), F.lit(""))).contains("VECTOR")
        df=(df.withColumn("medication_snomed_code",F.col("_s3_med_snomed"))
              .withColumn("medication_snomed_display",F.col("_s3_med_snomed_display"))
              .withColumn("medication_omop_concept_id",F.col("_s3_med_omop"))
              .withColumn("medication_map_method",F.when(mapped & order_embedding,F.lit("embedding_proposed"))
                          .when(mapped,F.lit("rule")))
              .withColumn("medication_map_confidence",F.col("_s3_med_confidence"))
              .withColumn("medication_map_rule_id",F.when(mapped & order_embedding,
                          F.lit("embedding:bronze_medication_order_embedding:tier"))
                          .when(mapped,F.concat(F.lit("bronze.map_medication_order:"),
                                              F.coalesce(F.col("_s3_med_bronze_method"),F.lit("mapped")))))
              .withColumn("medication_map_candidate_count",F.when(mapped,F.coalesce(F.col("_s3_med_candidates"),F.lit(1))))
              .drop(*[c for c in df.columns if c.startswith("_s3_med_")]))
    elif model_name == "medication_admin":
        b=(read_source("4_prod.bronze.map_med_admin").select(
            F.col("EVENT_ID").cast("bigint").alias("_s3_admin_id"),
            F.col("MULTUM").cast("string").alias("_s3c_multum"),
            F.coalesce("SNOMED_VALIDATED_CODE","SNOMED_CODE","LOOKUP_SNOMED_CODE").cast("string").alias("_s3_med_snomed"),
            F.coalesce("SNOMED_VALIDATED_STR","SNOMED_STR","LOOKUP_SNOMED_FROM_OMOP").alias("_s3_med_snomed_display"),
            F.coalesce("OMOP_STANDARD_CONCEPT_ID","OMOP_CONCEPT_ID").cast("bigint").alias("_s3_med_omop"),
            F.col("OMOP_MAPPING_METHOD").alias("_s3_med_bronze_method"),
            F.col("OMOP_MAPPING_CONFIDENCE").cast("double").alias("_s3_med_confidence"),
            F.col("SNOMED_CANDIDATE_COUNT").cast("int").alias("_s3_med_candidates")))
        df=df.join(b,F.col("event_id")==F.col("_s3_admin_id"),"left")
        mapped=F.col("_s3_med_snomed").isNotNull()|F.col("_s3_med_omop").isNotNull()
        method=(F.when(F.col("_s3_med_bronze_method")=="LOOKUP_VECTOR_SIMILARITY",F.lit("embedding_proposed"))
                .when(F.col("_s3_med_bronze_method")=="EXACT_UNIQUE_STANDARD_NAME",F.lit("exact_synonym"))
                .when(mapped,F.lit("rule")))
        rule=F.when(F.col("_s3_med_bronze_method")=="LOOKUP_VECTOR_SIMILARITY",F.lit("embedding:bronze_med_admin_embedding:tier")) \
              .otherwise(F.concat(F.lit("bronze.map_med_admin:"),F.coalesce(F.col("_s3_med_bronze_method"),F.lit("mapped"))))
        df=(df.withColumn("medication_snomed_code",F.col("_s3_med_snomed"))
              .withColumn("medication_snomed_display",F.col("_s3_med_snomed_display"))
              .withColumn("medication_omop_concept_id",F.col("_s3_med_omop"))
              .withColumn("medication_map_method",method)
              .withColumn("medication_map_confidence",F.col("_s3_med_confidence"))
              .withColumn("medication_map_rule_id",F.when(mapped,rule))
              .withColumn("medication_map_candidate_count",F.when(mapped,F.coalesce(F.col("_s3_med_candidates"),F.lit(1))))
              .drop(*[c for c in df.columns if c.startswith("_s3_med_") or c=="_s3_admin_id"]))
    if model_name in ("medication_admin", "medication_order", "medication_dispense"):
        med_target = F.col("medication_snomed_code").isNotNull() | F.col("medication_omop_concept_id").isNotNull()
        bronze_embedding = F.col("medication_map_method") == "embedding_proposed"
        med_source = F.col("medication_source_code").isNotNull() | F.col("medication_source_display").isNotNull()
        df = (df.withColumn("medication_target_system", F.when(F.col("medication_snomed_code").isNotNull(), F.lit(SNOMED_URI))
                            .when(F.col("medication_omop_concept_id").isNotNull(), F.lit(OMOP_URI)))
                .withColumn("medication_target_code", F.coalesce("medication_snomed_code", F.col("medication_omop_concept_id").cast("string")))
                .withColumn("medication_target_display", F.col("medication_snomed_display"))
                .withColumn("medication_map_cosine", F.when(bronze_embedding, F.col("medication_map_confidence")))
                .withColumn("medication_map_scoring_model", F.when(bronze_embedding, F.lit(S3B_BRONZE_MED_EMBED_MODEL)))
                .withColumn("medication_map_version", F.when(bronze_embedding,
                            F.concat_ws("|", F.lit(S3B_BRONZE_MED_EMBED_MODEL), F.lit(S3B_HIERARCHY_VERSION)))
                            .otherwise(F.col("medication_map_version")))
                .withColumn("medication_map_status", F.when(med_target, F.lit("mapped")).otherwise(F.lit("no_candidate")))
                .withColumn("medication_map_competing_count", F.col("medication_map_candidate_count"))
                .withColumn("medication_map_component_index", F.when(med_source, F.lit(1)).cast("int"))
                .withColumn("medication_map_component_count", F.when(med_source, F.lit(1)).cast("int")))
        if model_name in ("medication_admin", "medication_order"):
            df = _s3_lookup_axis(df, "medication", SRC_S3C_RXNORM_MULTUM_MAP, "urn:cerner:multum",
                                 F.col("_s3c_multum"), allow_split=False, model_name=model_name, replace_inferior=True)
            df = _s3_lookup_axis(df, "medication", SRC_S3C_RXNORM_CLASS_MAP, "urn:cerner:multum:class",
                                 F.col("_s3c_multum"), allow_split=True, model_name=model_name, replace_inferior=True)
        if model_name == "medication_dispense":
            df = _s3_lookup_axis(df, "medication", SRC_S3C_DMD_ID_MAP, "urn:nhs:dmd",
                                 F.col("_dmd_code"), model_name=model_name, replace_inferior=True)
        df = _s3_lookup_axis(df, "medication", SRC_S3C_DMD_NAME_MAP, "urn:cerner:medication-text:strength",
                             s3c_strength_key(F.col("medication_source_display")), allow_split=True, model_name=model_name)
        if "_s3c_multum" in df.columns:
            df = df.drop("_s3c_multum")
        df = _s3_lookup_axis(
            df, "medication", SRC_S3B_MED_TEXT_MAP, "urn:cerner:medication-text",
            F.lower(F.trim(F.regexp_replace(F.col("medication_source_display"), r"\s+", " "))),
            allow_split=True, model_name=model_name)
    if model_name == "pathology_result":
        df = _s3_apply_tier_rescore(
            df, "test", "bronze_pathology_test_auto_high",
            F.when(F.col("test_map_rule_id").contains("bronze_pathology_test_auto_high"), F.col("test_source_display")))
        df = _s3_apply_tier_rescore(
            df, "test", "bronze_pathology_test_auto_low",
            F.when(F.col("test_map_rule_id").contains("bronze_pathology_test_auto_low"), F.col("test_source_display")))
        df = _s3_apply_tier_rescore(df, "result", "bronze_pathology_result_auto", F.col("result_source_display"))
    elif model_name == "procedure":
        df = _s3_apply_tier_rescore(df, "device", "bronze_implant_layer5", F.col("implant_description"))
    elif model_name == "device":
        df = _s3_apply_tier_rescore(df, "device", "bronze_mediconnect_device", F.col("device_source_display"))
        unscorable = ((F.col("device_map_method") == "embedding_proposed") &
                      (F.length(F.trim(F.coalesce(F.col("device_source_display"), F.lit("")))) == 0) &
                      F.col("device_target_code").isNotNull())
        df = (df.withColumn("device_map_method", F.when(unscorable, F.lit("rule")).otherwise(F.col("device_map_method")))
              .withColumn("device_map_rule_id", F.when(unscorable, F.lit("lookup.mediconnect_device_type_map:approved_without_text_score"))
                          .otherwise(F.col("device_map_rule_id")))
              .withColumn("device_map_status_reason", F.when(unscorable, F.lit("blank source display; governed source-code mapping retained"))
                          .otherwise(F.col("device_map_status_reason"))))
    for _old_name, (_axis, _source_only) in retired.items():
        if not _source_only:
            df = _s3_snomed_from_omop(df, _axis)
            df = _s3_enforce_omop_standard_target(df, _axis)
    if model_name in ("pathology_result", "clinical_finding", "condition_stage"):
        _extra_axis = {"pathology_result":"result", "clinical_finding":"value", "condition_stage":"stage_group"}[model_name]
        df = _s3_snomed_from_omop(df, _extra_axis)
        df = _s3_enforce_omop_standard_target(df, _extra_axis)
    return df.select(*public_columns)




SRC_EPISODE_ENCOUNTER = "4_prod.bronze.map_episode_encounter"

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_episode_encounter():
    # Assemble episode encounter rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    s = read_source(SRC_EPISODE_ENCOUNTER)
    superseded = (~F.coalesce(s.SOURCE_PRESENT_IND.cast("boolean"), F.lit(True))) | (
        F.coalesce(s.ACTIVE_IND, F.lit(1)) != 1
    )
    # contract v2: retain the relationship SHA as a key and publish native relationship, episode, and encounter ids
    return s.select(
        stable_id("episode_encounter:mill", s.EPISODE_ENCNTR_RELTN_ID).alias("episode_encounter_key"),
        s.EPISODE_ENCNTR_RELTN_ID.cast("bigint").alias("episode_encounter_reltn_id"),
        s.EPISODE_ID.cast("bigint").alias("episode_id"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.EPISODE_LINK_STATUS.alias("relation_status_code"),
        s.SOURCE_DUPLICATE_COUNT.cast("long").alias("source_duplicate_count"),
        s.BEG_EFFECTIVE_DT_TM.alias("valid_from"),
        F.when(s.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"),
               s.END_EFFECTIVE_DT_TM).alias("valid_to"),
        F.when(superseded, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        F.lit(SRC_EPISODE_ENCOUNTER).alias("source_table"),
        s.EPISODE_ENCNTR_RELTN_ID.cast("string").alias("source_row_id"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("loaded_at"),
    )


SRC_ENCOUNTER      = "4_prod.bronze.map_encounter"

SRC_DIAGNOSIS = "4_prod.bronze.map_diagnosis"

SRC_PROBLEM = "4_prod.bronze.map_problem"

SRC_IMPLANT_DETAILS = "4_prod.bronze.map_implant_details"

SRC_FORM_ACTIVITY = "4_prod.bronze.mill_form_activity"

SRC_NUMERIC_EVENTS = "4_prod.bronze.map_numeric_events"

SRC_MED_ADMIN = "4_prod.bronze.map_med_admin"

SRC_MEDICATION_ORDER = "4_prod.bronze.map_medication_order"

SRC_MEDICATION_ORDER_ACTION = "4_prod.bronze.map_medication_order_action"

SRC_CODED_EVENTS = "4_prod.bronze.map_coded_events"

SRC_NOMEN_EVENTS = "4_prod.bronze.map_nomen_events"

SRC_DATE_EVENTS = "4_prod.bronze.map_date_events"

SRC_TEXT_EVENTS = "4_prod.bronze.map_text_events"

SRC_MILL_BLOB_TEXT = "4_prod.bronze.mill_blob_text"

SRC_APPOINTMENT = "4_prod.bronze.map_appointment"

SRC_APPOINTMENT_RESOURCE = "4_prod.bronze.map_appointment_resource"

SRC_THEATRE_CASE = "4_prod.bronze.map_theatre_case"

SRC_THEATRE_CASE_PROCEDURE = "4_prod.bronze.map_theatre_case_procedure"




def _family_history_canonical_pregate():
    # Normalize family history source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_FAMILY_HISTORY_PRODUCT)
    event_id = stable_id("family_history:mill", s.FHX_ACTIVITY_ID)
    raw_source_code = F.coalesce(s.SOURCE_IDENTIFIER, s.FOUND_CUI).cast("string")
    source_display = s.CONDITION_DESC
    source_code = _code_or_display(raw_source_code, source_display)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID), ("urn:barts:mrn", s.MRN)],
        SRC_FAMILY_HISTORY_PRODUCT,
        s.FHX_ACTIVITY_ID,
    )
    ended = F.coalesce(
        s.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"),
        F.lit(False),
    )
    # contract v2: publish FHX_ACTIVITY_ID, use native person and encounter ids, and keep only patient_event_key
    return s.select(
        event_id.alias("patient_event_key"),
        s.FHX_ACTIVITY_ID.cast("bigint").alias("fhx_activity_id"),
        skey.alias("subject_key"),
        ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .when(s.MRN.isNotNull(), F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        s.ORIGINATING_ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.BEG_EFFECTIVE_DT_TM.alias("event_datetime"),
        F.when(ended, s.END_EFFECTIVE_DT_TM).alias("event_end_datetime"),
        s.SOURCE_VOCABULARY_DESC.alias("source_coding_system"),
        source_code.alias("source_code"),
        source_display.alias("source_display"),
        codeable_concept(
            coding_obj(s.SOURCE_VOCABULARY_DESC, source_code,
                       F.coalesce(s.CONDITION_DESC_CODED, source_display), True),
            coding_obj(F.lit("http://snomed.info/sct"), s.SNOMED_CODE, s.SNOMED_TERM, False,
                       "bronze.map_family_history", None),
            coding_obj(F.lit("http://hl7.org/fhir/sid/icd-10"), s.ICD10_CODE, s.ICD10_TERM, False,
                       "bronze.map_family_history", None),
            coding_obj(F.lit("urn:omop:concept_id"), s.OMOP_CONCEPT_ID, s.OMOP_CONCEPT_NAME, False,
                       "bronze.map_family_history", None),
        ).alias("condition_code"),
        s.RELATION_CD.cast("string").alias("relationship_code"),
        s.RELATION_DESC.alias("relationship_display"),
        s.RELATION_TYPE_CD.cast("string").alias("relationship_type_code"),
        s.RELATION_TYPE_DESC.alias("relationship_type_display"),
        s.ONSET_AGE.cast("decimal(18,4)").alias("onset_age"),
        s.ONSET_AGE_UNIT.alias("onset_age_unit"),
        s.SEVERITY_CD.cast("string").alias("severity_code"),
        s.SEVERITY.alias("severity_display"),
        s.LIFE_CYCLE_STATUS.alias("source_lifecycle_status"),
        F.when(ended, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        s.FHX_ACTIVE_STATUS_DT_TM.alias("record_status_effective_from"),
        F.when(ended, s.END_EFFECTIVE_DT_TM).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit(None).cast("bigint").alias("asserter_practitioner_id"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.lit(SRC_FAMILY_HISTORY_PRODUCT).alias("_source_table"),
        s.FHX_ACTIVITY_ID.cast("string").alias("_source_row_id"),
        s.SNOMED_CODE.cast("string").alias("_snomed_code"),
        s.SNOMED_TERM.alias("_snomed_display"),
        s.ICD10_CODE.cast("string").alias("_icd10_code"),
        s.ICD10_TERM.alias("_icd10_display"),
        s.OMOP_CONCEPT_ID.cast("string").alias("_omop_code"),
        s.OMOP_CONCEPT_NAME.alias("_omop_display"),
    )

def _family_history_canonical():
    # Keep the normalized family history rows that pass the existing source-code admission rule.
    return _family_history_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_FAMILY_HISTORY_PRODUCT = "4_prod.bronze.map_family_history"




def _presenting_complaint_canonical():
    # Assemble normalized presenting complaint rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    enc = read_source(SRC_ENCOUNTER)
    rkey, rsys = subject_key_with_system(
        [("urn:cerner:person_id", enc.PERSON_ID)], SRC_ENCOUNTER, enc.ENCNTR_ID)
    rfv = enc.where(F.length(F.trim("REASON_FOR_VISIT")) > 0).select(
        stable_id("presenting_complaint:mill_encounter_rfv", enc.ENCNTR_ID).alias("patient_event_key"),
        stable_id("encounter:mill", enc.ENCNTR_ID).alias("encounter_key"), enc.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        rkey.alias("subject_key"), rsys.alias("subject_id_system"), enc.PERSON_ID.cast("bigint").alias("person_id"),
        F.lit("registration_reason_for_visit").alias("source_object"), enc.ENCNTR_ID.cast("bigint").alias("source_event_id"),
        F.lit(1).cast("int").alias("statement_sequence"), F.coalesce(enc.ARRIVAL_DT_TM_BEST, enc.REG_DT_TM).alias("event_datetime"),
        enc.encntr_type_class_desc.alias("encounter_class"), F.lit("urn:cerner:encounter:reason-for-visit").alias("source_coding_system"),
        F.lower(F.trim(F.regexp_replace(enc.REASON_FOR_VISIT, r"\s+", " "))).alias("source_code"),
        enc.REASON_FOR_VISIT.alias("source_display"), F.lit(None).cast("string").alias("complaint_group_display"),
        F.lit("active").alias("record_status"), enc.REG_DT_TM.alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"), F.date_format(enc.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        enc.UPDT_DT_TM.alias("source_update_timestamp"), enc.ADC_UPDT.alias("loaded_at"),
        F.lit(SRC_ENCOUNTER).alias("_source_table"), enc.ENCNTR_ID.cast("string").alias("_source_row_id"))

    nom = read_source(SRC_NOMEN_EVENTS).where("EVENT_CD=3844529 AND SOURCE_STRING IS NOT NULL")
    grp = (read_source(SRC_NOMEN_EVENTS).where("EVENT_CD=467136611")
           .groupBy("ENCNTR_ID").agg(F.first("SOURCE_STRING", ignorenulls=True).alias("complaint_group_display")))
    nkey, nsys = subject_key_with_system(
        [("urn:cerner:person_id", nom.PERSON_ID)], SRC_NOMEN_EVENTS, nom.EVENT_ID)
    pick = nom.join(grp, "ENCNTR_ID", "left").select(
        stable_id("presenting_complaint:mill_nomen", nom.EVENT_ID, nom.SEQUENCE_NBR).alias("patient_event_key"),
        stable_id("encounter:mill", nom.ENCNTR_ID).alias("encounter_key"), nom.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        nkey.alias("subject_key"), nsys.alias("subject_id_system"), nom.PERSON_ID.cast("bigint").alias("person_id"),
        F.lit("ed_presenting_complaint").alias("source_object"), nom.EVENT_ID.cast("bigint").alias("source_event_id"),
        F.row_number().over(Window.partitionBy(nom.ENCNTR_ID).orderBy(
            nom.CLINICAL_EVENT_DT_TM.asc_nulls_last(), nom.EVENT_ID, nom.SEQUENCE_NBR)).cast("int").alias("statement_sequence"),
        nom.CLINICAL_EVENT_DT_TM.alias("event_datetime"), F.lit("Emergency").alias("encounter_class"),
        F.lit("urn:cerner:nomenclature:ed-presenting-complaint").alias("source_coding_system"),
        F.lower(F.trim(F.regexp_replace(nom.SOURCE_STRING, r"\s+", " "))).alias("source_code"), nom.SOURCE_STRING.alias("source_display"),
        F.col("complaint_group_display"),
        F.when(F.coalesce(nom.SOURCE_DELETED_IND, F.lit(False)), "retracted").otherwise("active").alias("record_status"),
        nom.CE_VALID_FROM_DT_TM.alias("record_status_effective_from"),
        F.when(F.coalesce(nom.SOURCE_DELETED_IND, F.lit(False)), nom.ADC_UPDT).alias("record_status_effective_to"),
        F.date_format(nom.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"), nom.CE_UPDT_DT_TM.alias("source_update_timestamp"),
        nom.ADC_UPDT.alias("loaded_at"), F.lit(SRC_NOMEN_EVENTS).alias("_source_table"),
        F.concat_ws(":", nom.EVENT_ID, nom.SEQUENCE_NBR).alias("_source_row_id"))

    txt = read_source(SRC_TEXT_EVENTS).where("EVENT_CD=3844529")
    ttext = txt.TEXT_RESULT
    tkey, tsys = subject_key_with_system(
        [("urn:cerner:person_id", txt.PERSON_ID)], SRC_TEXT_EVENTS, txt.EVENT_ID)
    text = txt.where(F.length(F.trim(ttext)).between(1, 120)).select(
        stable_id("presenting_complaint:mill_text", txt.EVENT_ID).alias("patient_event_key"),
        stable_id("encounter:mill", txt.ENCNTR_ID).alias("encounter_key"), txt.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        tkey.alias("subject_key"), tsys.alias("subject_id_system"), txt.PERSON_ID.cast("bigint").alias("person_id"),
        F.lit("ed_presenting_complaint_text").alias("source_object"), txt.EVENT_ID.cast("bigint").alias("source_event_id"),
        F.lit(1).cast("int").alias("statement_sequence"), txt.RESULT_DT_TM.alias("event_datetime"),
        F.lit("Emergency").alias("encounter_class"), F.lit("urn:cerner:clinical-event:presenting-complaint-text").alias("source_coding_system"),
        F.lower(F.trim(F.regexp_replace(ttext, r"\s+", " "))).alias("source_code"), ttext.alias("source_display"),
        F.lit(None).cast("string").alias("complaint_group_display"),
        F.when(F.coalesce(txt.SOURCE_DELETED_IND, F.lit(False)), "retracted").otherwise("active").alias("record_status"),
        txt.RESULT_DT_TM.alias("record_status_effective_from"),
        F.when(F.coalesce(txt.SOURCE_DELETED_IND, F.lit(False)), txt.ADC_UPDT).alias("record_status_effective_to"),
        F.date_format(txt.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"), txt.ADC_UPDT.alias("source_update_timestamp"),
        txt.ADC_UPDT.alias("loaded_at"), F.lit(SRC_TEXT_EVENTS).alias("_source_table"), txt.EVENT_ID.cast("string").alias("_source_row_id"))
    return (rfv.unionByName(pick).unionByName(text)
            .withColumn("source_patient_event_key", F.col("patient_event_key"))
            .withColumn("complaint_text_normalised", F.col("source_code"))
            .withColumn("event_end_datetime", F.lit(None).cast("timestamp"))
            .withColumn("_source_system", F.lit("millennium"))
            .withColumn("identity_status", F.when(F.col("person_id").isNotNull(), "resolved").otherwise("unresolved"))
            .withColumn("confidentiality_code", F.lit(None).cast("string"))
            .withColumn("vip_ind", F.lit(None).cast("boolean"))
            .withColumn("withheld_identity_ind", F.lit(None).cast("boolean")))




def _condition_diagnosis_canonical_pregate():
    # Normalize condition diagnosis source rows before the downstream admission filter so
    # excluded evidence remains countable.
    s = read_source(SRC_DIAGNOSIS)
    event_id = stable_id("condition:mill:diagnosis", s.DIAGNOSIS_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_DIAGNOSIS, s.DIAGNOSIS_ID
    )
    raw_source_code = F.coalesce(
        s.SOURCE_IDENTIFIER, s.CONCEPT_CKI_IDENTIFIER, s.NOMENCLATURE_ID.cast("string")
    )
    source_display = F.coalesce(s.SOURCE_STRING, s.DIAGNOSIS_DISPLAY, s.DIAGNOSIS_TEXT)
    source_code = _code_or_display(raw_source_code, source_display)
    ended = F.coalesce(
        s.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"), F.lit(False)
    )
    return s.select(
        event_id.alias("patient_event_key"),
        # contract v2: publish diagnosis, problem, and maternity arm ids with native relationship types
        F.lit("diagnosis").alias("source_object"),
        s.DIAGNOSIS_ID.cast("bigint").alias("diagnosis_id"),
        F.lit(None).cast("bigint").alias("problem_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        F.coalesce(s.DIAG_DT_TM, s.ASSERTED_DT_TM, s.BEG_EFFECTIVE_DT_TM).alias("event_datetime"),
        F.when(ended, s.END_EFFECTIVE_DT_TM).alias("event_end_datetime"),
        F.coalesce(s.source_vocabulary_desc, s.CONCEPT_CKI_SOURCE,
                   F.lit("urn:cerner:nomenclature")).alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept_json(
            coding_obj(F.coalesce(s.source_vocabulary_desc, s.CONCEPT_CKI_SOURCE,
                                  F.lit("urn:cerner:nomenclature")),
                       source_code, source_display, True),
            coding_obj(F.lit("http://snomed.info/sct"), s.SNOMED_CODE, s.SNOMED_TERM, False,
                       "bronze.map_diagnosis", None),
            coding_obj(F.lit("http://hl7.org/fhir/sid/icd-10"), s.ICD10_CODE, s.ICD10_TERM, False,
                       "bronze.map_diagnosis", None),
            coding_obj(F.lit("urn:omop:concept_id"), s.OMOP_CONCEPT_ID, s.OMOP_CONCEPT_NAME, False,
                       "bronze.map_diagnosis", None),
        ).alias("_condition_code_json"),
        s.DIAG_TYPE_CD.cast("string").alias("category_code"),
        s.diag_type_desc.alias("category_display"),
        s.ACTIVE_STATUS_CD.cast("string").alias("clinical_status_code"),
        F.lit(None).cast("string").alias("clinical_status_display"),
        s.CONFIRMATION_STATUS_CD.cast("string").alias("verification_status_code"),
        s.confirmation_status_desc.alias("verification_status_display"),
        F.coalesce(s.earliest_diagnosis_date, s.DIAG_DT_TM).alias("onset_datetime"),
        F.when(ended, s.END_EFFECTIVE_DT_TM).alias("abatement_datetime"),
        F.lit(None).cast("string").alias("body_site_code"),
        F.lit(None).cast("string").alias("body_site_display"),
        s.SEVERITY_CD.cast("string").alias("severity_code"),
        s.SEVERITY_FTDESC.alias("severity_display"),
        s.LATERALITY_CD.cast("string").alias("laterality_code"),
        F.lit(None).cast("string").alias("laterality_display"),
        s.ASSERTED_DT_TM.alias("asserted_datetime"),
        s.DIAG_PRSNL_ID.cast("bigint").alias("asserter_practitioner_id"),
        F.lit(None).cast("bigint").alias("recorder_practitioner_id"),
        F.when(ended, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        s.ACTIVE_STATUS_DT_TM.alias("record_status_effective_from"),
        F.when(ended, s.END_EFFECTIVE_DT_TM).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("diagnosis").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.UPDT_DT_TM.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.lit(SRC_DIAGNOSIS).alias("_source_table"),
        s.DIAGNOSIS_ID.cast("string").alias("_source_row_id"),
        s.SNOMED_CODE.cast("string").alias("_snomed_code"), s.SNOMED_TERM.alias("_snomed_display"),
        s.ICD10_CODE.cast("string").alias("_icd10_code"), s.ICD10_TERM.alias("_icd10_display"),
        s.OMOP_CONCEPT_ID.cast("string").alias("_omop_code"), s.OMOP_CONCEPT_NAME.alias("_omop_display"),
    )

def _condition_diagnosis_canonical():
    # Keep the normalized condition diagnosis rows that pass the existing source-code admission
    # rule.
    return _condition_diagnosis_canonical_pregate().where(_usable_code(F.col("source_code")))

def _condition_problem_canonical_pregate():
    # Normalize condition problem source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_PROBLEM)
    event_id = stable_id("condition:mill:problem", s.PROBLEM_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_PROBLEM, s.PROBLEM_ID
    )
    raw_source_code = F.coalesce(
        s.SOURCE_IDENTIFIER, s.CONCEPT_CKI_IDENTIFIER, s.NOMENCLATURE_ID.cast("string")
    )
    source_display = F.coalesce(s.SOURCE_STRING, s.PROBLEM_DISPLAY, s.PROBLEM_FTDESC)
    source_code = _code_or_display(raw_source_code, source_display)
    ended = F.coalesce(
        s.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"), F.lit(False)
    )
    encounter_key = F.coalesce(s.ENCNTR_ID, s.ORIGINATING_ENCNTR_ID)
    return s.select(
        event_id.alias("patient_event_key"),
        # contract v2: publish the problem arm discriminator and PROBLEM_ID with native relationship ids
        F.lit("problem").alias("source_object"),
        F.lit(None).cast("bigint").alias("diagnosis_id"),
        s.PROBLEM_ID.cast("bigint").alias("problem_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        encounter_key.cast("bigint").alias("encounter_id"),
        F.coalesce(s.ASSERTED_DT_TM, s.ONSET_DT_TM, s.BEG_EFFECTIVE_DT_TM,
                   s.earliest_problem_date).alias("event_datetime"),
        F.when(ended, s.END_EFFECTIVE_DT_TM).alias("event_end_datetime"),
        F.coalesce(s.source_vocabulary_desc, s.CONCEPT_CKI_SOURCE,
                   F.lit("urn:cerner:nomenclature")).alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept_json(
            coding_obj(F.coalesce(s.source_vocabulary_desc, s.CONCEPT_CKI_SOURCE,
                                  F.lit("urn:cerner:nomenclature")),
                       source_code, source_display, True),
            coding_obj(F.lit("http://snomed.info/sct"), s.SNOMED_CODE, s.SNOMED_TERM, False,
                       "bronze.map_problem", None),
            coding_obj(F.lit("http://hl7.org/fhir/sid/icd-10"), s.ICD10_CODE, s.ICD10_TERM, False,
                       "bronze.map_problem", None),
            coding_obj(F.lit("urn:omop:concept_id"), s.OMOP_CONCEPT_ID, s.OMOP_CONCEPT_NAME, False,
                       "bronze.map_problem", None),
        ).alias("_condition_code_json"),
        s.CLASSIFICATION_CD.cast("string").alias("category_code"),
        s.classification_desc.alias("category_display"),
        s.LIFE_CYCLE_STATUS_CD.cast("string").alias("clinical_status_code"),
        s.life_cycle_status_desc.alias("clinical_status_display"),
        s.CONFIRMATION_STATUS_CD.cast("string").alias("verification_status_code"),
        s.confirmation_status_desc.alias("verification_status_display"),
        s.ONSET_DT_TM.alias("onset_datetime"),
        F.when(ended, s.END_EFFECTIVE_DT_TM).alias("abatement_datetime"),
        F.lit(None).cast("string").alias("body_site_code"),
        F.lit(None).cast("string").alias("body_site_display"),
        s.SEVERITY_CD.cast("string").alias("severity_code"), s.severity_desc.alias("severity_display"),
        s.LATERALITY_CD.cast("string").alias("laterality_code"), s.laterality_desc.alias("laterality_display"),
        s.ASSERTED_DT_TM.alias("asserted_datetime"),
        F.lit(None).cast("bigint").alias("asserter_practitioner_id"),
        s.ACTIVE_STATUS_PRSNL_ID.cast("bigint").alias("recorder_practitioner_id"),
        F.when(ended, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        s.ACTIVE_STATUS_DT_TM.alias("record_status_effective_from"),
        F.when(ended, s.END_EFFECTIVE_DT_TM).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("problem").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.UPDT_DT_TM.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_PROBLEM).alias("_source_table"),
        s.PROBLEM_ID.cast("string").alias("_source_row_id"),
        s.SNOMED_CODE.cast("string").alias("_snomed_code"), s.SNOMED_TERM.alias("_snomed_display"),
        s.ICD10_CODE.cast("string").alias("_icd10_code"), s.ICD10_TERM.alias("_icd10_display"),
        s.OMOP_CONCEPT_ID.cast("string").alias("_omop_code"), s.OMOP_CONCEPT_NAME.alias("_omop_display"),
    )

def _condition_problem_canonical():
    # Keep the normalized condition problem rows that pass the existing source-code admission
    # rule.
    return _condition_problem_canonical_pregate().where(_usable_code(F.col("source_code")))

# contract v2: use the canonical patient-event key for waiting-list representative grouping and deterministic tie-breaking
def _waiting_list_index_representative():
    # Incrementalizable collapse: MAX over an orderable struct (is_current, version id,
    # version time, fact_row_id tiebreak), payload as JSON — the _mill_blob_document pattern.
    # Choose one deterministic current or latest version per waiting-list entry for the patient-
    # event index.
    base = _waiting_list_entry_canonical()
    payload_schema = base.schema
    ranked = base.select(
        F.col("patient_event_key").alias("_entry"),
        F.struct(
            F.coalesce(F.col("is_current").cast("int"), F.lit(0)).alias("o_current"),
            F.coalesce(F.col("source_version_id"), F.lit(-2)).alias("o_version"),
            F.coalesce(
                F.col("version_datetime"), F.lit("1900-01-01").cast("timestamp")
            ).alias("o_time"),
            F.col("patient_event_key").alias("o_tiebreak"),
            F.to_json(F.struct(*[F.col(c) for c in base.columns])).alias("payload"),
        ).alias("ranked"),
    )
    winner = ranked.groupBy("_entry").agg(F.max("ranked").alias("w"))
    return winner.select(
        F.from_json(F.col("w.payload"), payload_schema).alias("r")
    ).select("r.*")

def _endobase_procedure_canonical_pregate():
    # Normalize endobase procedure source rows before the downstream admission filter so
    # excluded evidence remains countable.
    s = read_source(SRC_ENDOBASE_EXAM)
    event_id = stable_id("procedure:endobase_exam", s.ENDOBASE_EXAM_ID)
    source_display = s.EXAM_TYPE_DESC
    source_code = _code_or_display(s.ENDOBASE_EXAM_TYPE_ID.cast("string"), source_display)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_ENDOBASE_EXAM, s.ENDOBASE_EXAM_ID
    )
    performed = F.coalesce(s.PERFORMED_TS_CLEAN, s.EXAM_TS_CLEAN,
                           s.TRUE_START_TS_CLEAN, s.START_TS_CLEAN)
    performed_end = F.coalesce(s.TRUE_END_TS_CLEAN, s.END_TS_CLEAN)
    inactive = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    status = F.when(performed.isNotNull(), F.lit("completed")).otherwise(F.lit("preparation"))
    return s.select(
        event_id.alias("patient_event_key"),
        # contract v2: publish the endobase arm, ENDOBASE_EXAM_ID, and native relationship ids
        F.lit("endobase_exam").alias("source_object"),
        F.lit(None).cast("bigint").alias("procedure_id"),
        s.ENDOBASE_EXAM_ID.cast("bigint").alias("endobase_exam_id"),
        F.lit(None).cast("bigint").alias("implant_event_id"),
        F.lit(None).cast("bigint").alias("implant_sequence"),
        F.lit(None).cast("bigint").alias("surg_case_proc_id"),
        F.lit(None).cast("string").alias("source_row_hash"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        s.MILL_ENCNTR_ID.cast("bigint").alias("encounter_id"),
        performed.alias("event_datetime"), performed_end.alias("event_end_datetime"),
        F.lit("urn:barts:endobase:exam-type").alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:barts:endobase:exam-type"), source_code,
                       source_display, True)
        ).alias("_procedure_code_json"),
        status.alias("status_code"), status.alias("status_display"),
        performed.alias("performed_start"), performed_end.alias("performed_end"),
        F.lit(None).cast("string").alias("body_site_code"),
        F.lit(None).cast("string").alias("body_site_display"),
        F.lit(None).cast("string").alias("laterality_code"),
        F.lit(None).cast("string").alias("laterality_display"),
        F.lit(None).cast("bigint").alias("performer_practitioner_id"),
        s.DEPARTMENT_ID.cast("string").alias("procedure_location_code"),
        F.lit(None).cast("string").alias("procedure_location_display"),
        F.lit(None).cast("string").alias("procedure_note"),
        F.lit(None).cast("string").alias("implant_description"),
        F.lit(None).cast("string").alias("device_code"),
        F.lit(None).cast("string").alias("device_display"),
        F.lit(None).cast("string").alias("manufacturer"),
        F.lit(None).cast("string").alias("serial_number"),
        F.lit(None).cast("string").alias("batch_number"),
        F.lit(None).cast("string").alias("udi_di"),
        F.lit(None).cast("string").alias("udi_standard"),
        F.lit(None).cast("decimal(38,6)").alias("quantity"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        performed.alias("record_status_effective_from"),
        F.when(inactive, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("endobase_exam").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.PIPELINE_UPDT_DT_TM.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("endobase").alias("_source_system"), F.lit(SRC_ENDOBASE_EXAM).alias("_source_table"),
        s.ENDOBASE_EXAM_ID.cast("string").alias("_source_row_id"),
        F.lit(None).cast("string").alias("_snomed_code"),
        F.lit(None).cast("string").alias("_snomed_display"),
        F.lit(None).cast("string").alias("_opcs4_code"),
        F.lit(None).cast("string").alias("_opcs4_display"),
        F.lit(None).cast("string").alias("_omop_code"),
        F.lit(None).cast("string").alias("_omop_display"),
        F.lit(None).cast("string").alias("_device_snomed_code"),
        F.lit(None).cast("string").alias("_device_snomed_display"),
        F.lit(None).cast("string").alias("_device_mapping_layer"),
        F.lit(None).cast("double").alias("_device_mapping_confidence"),
    )

def _endobase_procedure_canonical():
    # Keep the normalized endobase procedure rows that pass the existing source-code admission
    # rule.
    return _endobase_procedure_canonical_pregate().where(_usable_code(F.col("source_code")))

def _cc_procedure_canonical_pregate():
    # Normalize cc procedure source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_CC_PROCEDURE)
    event_id = stable_id("procedure:nccmds", s.ROW_HASH)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_CC_PROCEDURE, s.ROW_HASH
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    note = F.concat_ws(
        " | ",
        F.concat(F.lit("period_link_status="), F.coalesce(s.PERIOD_LINK_STATUS, F.lit("UNKNOWN"))),
        F.concat(F.lit("period_business_key="), s.PERIOD_BUSINESS_KEY.cast("string")),
        F.concat(F.lit("cds_apc_id="), s.CDS_APC_ID),
    )
    return s.select(
        event_id.alias("patient_event_key"),
        # contract v2: publish the nccmds arm with ROW_HASH evidence and nullable typed native columns
        F.lit("nccmds").alias("source_object"),
        F.lit(None).cast("bigint").alias("procedure_id"),
        F.lit(None).cast("bigint").alias("endobase_exam_id"),
        F.lit(None).cast("bigint").alias("implant_event_id"),
        F.lit(None).cast("bigint").alias("implant_sequence"),
        F.lit(None).cast("bigint").alias("surg_case_proc_id"),
        s.ROW_HASH.cast("string").alias("source_row_hash"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.OPCS_Proc_Dt_CLEAN.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("http://fhir.hl7.org.uk/CodeSystem/OPCS-4").alias("source_coding_system"),
        s.OPCS_Proc_Code.cast("string").alias("source_code"),
        F.lit(None).cast("string").alias("source_display"),
        codeable_concept_json(
            coding_obj(
                F.lit("http://fhir.hl7.org.uk/CodeSystem/OPCS-4"),
                s.OPCS_Proc_Code.cast("string"), F.lit(None).cast("string"), True,
            )
        ).alias("_procedure_code_json"),
        F.lit("completed").alias("status_code"),
        s.CC_TYPE_DESC.alias("status_display"),
        s.OPCS_Proc_Dt_CLEAN.alias("performed_start"),
        F.lit(None).cast("timestamp").alias("performed_end"),
        F.lit(None).cast("string").alias("body_site_code"),
        F.lit(None).cast("string").alias("body_site_display"),
        F.lit(None).cast("string").alias("laterality_code"),
        F.lit(None).cast("string").alias("laterality_display"),
        F.lit(None).cast("bigint").alias("performer_practitioner_id"),
        F.lit(None).cast("string").alias("procedure_location_code"),
        F.lit(None).cast("string").alias("procedure_location_display"),
        note.alias("procedure_note"),
        F.lit(None).cast("string").alias("implant_description"),
        F.lit(None).cast("string").alias("device_code"),
        F.lit(None).cast("string").alias("device_display"),
        F.lit(None).cast("string").alias("manufacturer"),
        F.lit(None).cast("string").alias("serial_number"),
        F.lit(None).cast("string").alias("batch_number"),
        F.lit(None).cast("string").alias("udi_di"),
        F.lit(None).cast("string").alias("udi_standard"),
        F.lit(None).cast("decimal(38,6)").alias("quantity"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
         .alias("record_status"),
        s.OPCS_Proc_Dt_CLEAN.alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("critical_care_procedure").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.PIPELINE_UPDT_DT_TM.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("cds-nccmds").alias("_source_system"),
        F.lit(SRC_CC_PROCEDURE).alias("_source_table"),
        s.ROW_HASH.cast("string").alias("_source_row_id"),
        F.lit(None).cast("string").alias("_snomed_code"),
        F.lit(None).cast("string").alias("_snomed_display"),
        s.OPCS_Proc_Code.cast("string").alias("_opcs4_code"),
        F.lit(None).cast("string").alias("_opcs4_display"),
        F.lit(None).cast("string").alias("_omop_code"),
        F.lit(None).cast("string").alias("_omop_display"),
        F.lit(None).cast("string").alias("_device_snomed_code"),
        F.lit(None).cast("string").alias("_device_snomed_display"),
        F.lit(None).cast("string").alias("_device_mapping_layer"),
        F.lit(None).cast("double").alias("_device_mapping_confidence"),
        F.lit(None).cast("string").alias("_theatre_case_id"),
    )

def _cc_procedure_canonical():
    # Keep the normalized cc procedure rows that pass the existing source-code admission rule.
    return _cc_procedure_canonical_pregate().where(_usable_code(F.col("source_code")))

def _maternity_diagnosis_canonical_pregate():
    # Normalize maternity diagnosis source rows before the downstream admission filter so
    # excluded evidence remains countable.
    d = _maternity_join(SRC_MATERNITY_DIAGNOSIS)
    person = F.coalesce(F.col("_spine_person_id"), F.col("s.PERSON_ID"))
    event_id = stable_id(
        "condition:msds", F.col("s.PREGNANCYID"), F.col("s.DIAGSCHEME"),
        F.col("s.DIAG"), F.col("s.DIAGDATE"),
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", person)], SRC_MATERNITY_DIAGNOSIS,
        F.concat_ws("|", F.col("s.PREGNANCYID"), F.col("s.DIAGSCHEME"),
                    F.col("s.DIAG"), F.col("s.DIAGDATE")),
    )
    return d.select(
        event_id.alias("patient_event_key"),
        # contract v2: publish the maternity arm with nullable native diagnosis fields and BIGINT relationship columns
        F.lit("maternity").alias("source_object"),
        F.lit(None).cast("bigint").alias("diagnosis_id"),
        F.lit(None).cast("bigint").alias("problem_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"), person.cast("bigint").alias("person_id"),
        _maternity_identity_status(F.col("s.PERSON_ID"), F.col("_spine_person_id"))
         .alias("identity_status"), F.lit(None).cast("bigint").alias("encounter_id"),
        F.col("s.DIAGDATE_CLEAN").alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("http://snomed.info/sct").alias("source_coding_system"),
        F.col("s.DIAG").alias("source_code"), F.col("s.DIAG_SNOMED_DESC").alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("http://snomed.info/sct"), F.col("s.DIAG"),
                       F.col("s.DIAG_SNOMED_DESC"), True)
        ).alias("_condition_code_json"),
        F.col("s.DIAGSCHEME").alias("category_code"), F.lit("maternity diagnosis").alias("category_display"),
        F.lit("active").alias("clinical_status_code"), F.lit("active").alias("clinical_status_display"),
        F.lit("confirmed").alias("verification_status_code"),
        F.lit("confirmed").alias("verification_status_display"),
        F.col("s.DIAGDATE_CLEAN").alias("onset_datetime"),
        F.lit(None).cast("timestamp").alias("abatement_datetime"),
        F.lit(None).cast("string").alias("body_site_code"),
        F.lit(None).cast("string").alias("body_site_display"),
        F.lit(None).cast("string").alias("severity_code"),
        F.lit(None).cast("string").alias("severity_display"),
        F.lit(None).cast("string").alias("laterality_code"),
        F.lit(None).cast("string").alias("laterality_display"),
        F.col("s.DIAGDATE_CLEAN").alias("asserted_datetime"),
        F.lit(None).cast("bigint").alias("asserter_practitioner_id"),
        F.lit(None).cast("bigint").alias("recorder_practitioner_id"),
        F.lit("active").alias("record_status"),
        F.col("s.DIAGDATE_CLEAN").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("maternity_diagnosis").alias("source_feed"),
        F.date_format(F.col("s.ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("s.RECORD_UPDATED_DT").alias("source_update_timestamp"),
        F.col("s.ADC_UPDT").alias("loaded_at"), F.lit("msds").alias("_source_system"),
        F.lit(SRC_MATERNITY_DIAGNOSIS).alias("_source_table"),
        F.col("s.ROW_HASH").cast("string").alias("_source_row_id"),
        F.col("s.DIAG").alias("_snomed_code"),
        F.col("s.DIAG_SNOMED_DESC").alias("_snomed_display"),
        F.lit(None).cast("string").alias("_icd10_code"),
        F.lit(None).cast("string").alias("_icd10_display"),
        F.lit(None).cast("string").alias("_omop_code"),
        F.lit(None).cast("string").alias("_omop_display"),
    )

def _maternity_diagnosis_canonical():
    # Keep the normalized maternity diagnosis rows that pass the existing source-code admission
    # rule.
    return _maternity_diagnosis_canonical_pregate().where(_usable_code(F.col("source_code")))

def _mill_radiology_exam_canonical_pregate(integration=False):
    # Normalize mill radiology exam source rows before the downstream admission filter so
    # excluded evidence remains countable.
    s = read_source(SRC_RADIOLOGY_EVENT)
    event_id = stable_id("imaging_exam:mill", s.EVENT_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_RADIOLOGY_EVENT, s.EVENT_ID
    )
    source_code = _code_or_display(s.EXAM_TYPE_CODE, s.EVENT_TITLE_TEXT)
    status = (
        F.when(F.coalesce(s.IN_ERROR_IND, F.lit(False)), F.lit("entered-in-error"))
        .otherwise(F.coalesce(s.RESULT_STATUS_DESC, F.lit("available")))
    )
    # contract v2: publish native radiology identifiers and a typed nullable mixed-arm organization key
    return s.select(
        event_id.alias("patient_event_key"),
        s.EVENT_ID.cast("bigint").alias("event_id"),
        F.lit(None).cast("bigint").alias("pacs_examination_id"),
        F.lit("millennium").alias("source_object"),
        F.lit(None).cast("string").alias("organization_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.PERFORMED_DT_TM_CLEAN.alias("event_datetime"),
        s.EVENT_END_DT_TM_CLEAN.alias("event_end_datetime"),
        F.lit("urn:cerner:radiology-exam").alias("source_coding_system"),
        source_code.alias("source_code"), s.EVENT_TITLE_TEXT.alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:cerner:radiology-exam"), source_code,
                       s.EVENT_TITLE_TEXT, True)
        ).alias("exam_code"),
        status.alias("status_code"), s.REFERENCE_NBR.alias("accession_identifier"),
        F.lit(None).cast("string").alias("study_instance_uid"),
        s.NHSI_MODALITY_CATEGORY.alias("modality_code"),
        F.lit(None).cast("string").alias("body_site_code"),
        F.lit(None).cast("string").alias("report_patient_event_key"),
        F.lit(None).cast("bigint").alias("requester_practitioner_id"),
        F.lit(None).cast("bigint").alias("performer_practitioner_id"),
        F.when(F.coalesce(s.IN_ERROR_IND, F.lit(False)), F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        s.VALID_FROM_DT_TM_CLEAN.alias("record_status_effective_from"),
        F.when(F.coalesce(s.IN_ERROR_IND, F.lit(False)), s.UPDT_DT_TM)
         .otherwise(s.VALID_UNTIL_DT_TM_CLEAN).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("radiology_event").alias("source_feed"),
        F.date_format(s.PIPELINE_UPDT_DT_TM, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.UPDT_DT_TM.alias("source_update_timestamp"),
        s.PIPELINE_UPDT_DT_TM.alias("loaded_at"),
        *(_mill_radiology_integration_columns(s) if integration else []),
        F.lit("millennium").alias("_source_system"),
        F.lit(SRC_RADIOLOGY_EVENT).alias("_source_table"),
        s.EVENT_ID.cast("string").alias("_source_row_id"),
    )

def _mill_radiology_exam_canonical(integration=False):
    # Keep the normalized mill radiology exam rows that pass the existing source-code admission
    # rule.
    return _mill_radiology_exam_canonical_pregate(integration).where(
        _usable_code(F.col("source_code"))
    )


SRC_CC_PROCEDURE = "4_prod.bronze.map_critical_care_procedure"

SRC_MATERNITY_DIAGNOSIS = "4_prod.bronze.map_maternity_diagnosis"

SRC_RADIOLOGY_EVENT = "4_prod.bronze.map_radiology_event"




SRC_PHARMACY_ISSUE = "4_prod.bronze.map_pharmacy_issue"

SRC_PATHOLOGY_REPORT_VERSIONS = "4_prod.bronze.map_pathology_report"

# contract v2: publish ENDOBASE_EXAM_ID and native encounter identity for the endoscopy document arm
def _endobase_document_canonical():
    # One clinically meaningful report per exam. Template and free-text terms are
    # interleaved in source display order; DGVS tab names are not landed, so section
    # titles deliberately expose the raw section/subsection ids instead of inventing
    # a decode. CREATED_TS is authoring provenance and is only a clinical-time fallback
    # when the parent exam is absent.
    # Assemble normalized endobase document rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    t = (
        read_source(SRC_ENDOBASE_EXAM_TERM)
        .where(F.coalesce(F.col("SOURCE_PRESENT_IND"), F.lit(True)))
        .where(_present(F.col("TERM_TEXT")))
    )
    ordered_term = F.struct(
        F.coalesce(F.col("SECTION_TAB_ID").cast("long"), F.lit(2147483647)).alias("section_sort"),
        F.coalesce(F.col("SUBSECTION_TAB_ID").cast("long"), F.lit(2147483647)).alias("subsection_sort"),
        F.coalesce(F.col("DISPLAY_ORDER").cast("long"), F.lit(2147483647)).alias("display_sort"),
        F.col("ENDOBASE_EXAM_TERM_ID").cast("long").alias("term_sort"),
        F.col("SECTION_TAB_ID").cast("string").alias("section_tab_id"),
        F.col("SUBSECTION_TAB_ID").cast("string").alias("subsection_tab_id"),
        F.col("TERM_TEXT").alias("term_text"),
        
    )
    assembled = (
        t.groupBy("ENDOBASE_EXAM_ID")
        .agg(
            F.sort_array(F.collect_list(ordered_term)).alias("_ordered_terms"),
            F.min("CREATED_TS").alias("_first_authored_ts"),
            F.max("ADC_UPDT").alias("_term_loaded_at"),
            F.count(F.lit(1)).cast("long").alias("_term_count"),
            F.countDistinct("PERSON_ID").alias("_term_person_count"),
            F.max("PERSON_ID").alias("_term_person_id"),
        )
        .withColumn(
            "_document_text",
            F.expr("concat_ws('\\n', transform(_ordered_terms, x -> x.term_text))"),
        )
        .withColumn("_anon_document_text", F.lit(None).cast("string"))
        .withColumn(
            "_sections_json",
            F.expr("""
              to_json(transform(
                _ordered_terms,
                (x, i) -> named_struct(
                  'sequence', i + 1,
                  'section_title', concat(
                    'section_tab_id=', coalesce(x.section_tab_id, 'null'),
                    ';subsection_tab_id=', coalesce(x.subsection_tab_id, 'null')
                  ),
                  'section_text', x.term_text
                )
              ))
            """),
        )
        .alias("a")
    )
    x = read_source(SRC_ENDOBASE_EXAM).select(
        F.col("ENDOBASE_EXAM_ID").alias("_x_exam_id"),
        F.col("PERSON_ID").alias("_x_person_id"),
        F.col("MILL_ENCNTR_ID").alias("_x_encntr_id"),
        F.col("MILL_ORDER_ID").alias("_x_order_id"),
        F.col("PERFORMED_TS_CLEAN").alias("_x_performed_ts"),
        F.col("EXAM_TS_CLEAN").alias("_x_exam_ts"),
        F.col("TRUE_START_TS_CLEAN").alias("_x_true_start_ts"),
        F.col("START_TS_CLEAN").alias("_x_start_ts"),
        F.col("TRUE_END_TS_CLEAN").alias("_x_true_end_ts"),
        F.col("END_TS_CLEAN").alias("_x_end_ts"),
        F.col("SOURCE_CREATE_TS").alias("_x_source_create_ts"),
        F.col("EXAM_TYPE_DESC").alias("_x_exam_type_desc"),
        F.col("SIGNER_ID").alias("_x_signer_id"),
        F.col("EXAMINER_ID").alias("_x_examiner_id"),
        F.col("SOURCE_PRESENT_IND").alias("_x_source_present_ind"),
        F.col("PIPELINE_UPDT_DT_TM").alias("_x_pipeline_updt"),
        F.col("ADC_UPDT").alias("_x_loaded_at"),
    ).alias("x")
    s = assembled.join(x, F.col("a.ENDOBASE_EXAM_ID") == F.col("x._x_exam_id"), "left")
    e = read_source(SRC_ENCOUNTER).select(
        F.col("ENCNTR_ID").alias("_e_encntr_id"),
        F.col("PERSON_ID").alias("_e_person_id"),
        F.col("ADC_UPDT").alias("_e_loaded_at"),
    ).alias("e")
    s = s.join(e, F.col("x._x_encntr_id") == F.col("e._e_encntr_id"), "left")
    term_person = F.when(F.col("a._term_person_count") == 1, F.col("a._term_person_id"))
    resolved_person = F.coalesce(F.col("x._x_person_id"), F.col("e._e_person_id"), term_person)
    natural = F.col("a.ENDOBASE_EXAM_ID").cast("string")
    event_id = stable_id("document:endobase_exam", F.col("a.ENDOBASE_EXAM_ID"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", resolved_person)], SRC_ENDOBASE_EXAM, natural
    )
    event_time = F.coalesce(
        F.col("x._x_performed_ts"), F.col("x._x_exam_ts"),
        F.col("x._x_true_start_ts"), F.col("x._x_start_ts"),
        F.when(F.col("x._x_exam_id").isNull(), F.col("a._first_authored_ts")),
    )
    event_end = F.coalesce(F.col("x._x_true_end_ts"), F.col("x._x_end_ts"))
    loaded_at = F.greatest(
        F.col("a._term_loaded_at"), F.col("x._x_loaded_at"), F.col("e._e_loaded_at")
    )
    inactive = F.col("x._x_exam_id").isNotNull() & ~F.coalesce(
        F.col("x._x_source_present_ind"), F.lit(True)
    )
    author_source_id = F.coalesce(
        F.col("x._x_signer_id"), F.col("x._x_examiner_id")
    ).cast("string")
    source_code = F.lit("ENDOSCOPY_REPORT")
    source_display = F.lit("Endoscopy report")
    return s.select(
        event_id.alias("patient_event_id"), event_id.alias("fact_row_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        resolved_person.cast("string").alias("person_id"),
        F.when(resolved_person.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.col("x._x_encntr_id").cast("bigint").alias("encounter_id"),
        event_time.alias("event_datetime"), event_end.alias("event_end_datetime"),
        F.lit("urn:endobase:document-type").alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:endobase:document-type"), source_code,
                       source_display, True)
        ).alias("_document_type_json"),
        F.coalesce(F.col("x._x_exam_type_desc"), source_display).alias("title"),
        F.lit(None).cast("string").alias("author_practitioner_id"),
        F.when(author_source_id.isNotNull(), F.lit("report_author")).alias("author_role"),
        F.lit(None).cast("string").alias("service_id"),
        F.col("a.ENDOBASE_EXAM_ID").cast("bigint").alias("endobase_exam_id"),
        F.when(F.col("x._x_exam_id").isNull(), F.lit("orphan_exam"))
         .otherwise(F.lit("assembled")).alias("status_code"),
        F.concat_ws(
            ":", natural, F.col("a._term_count").cast("string"),
            F.date_format(F.col("a._term_loaded_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        ).alias("version_id"),
        F.col("a._document_text").alias("document_text"),
        F.col("a._sections_json").alias("_sections_json"),
        F.lit("endobase-exam-assembly-v1").alias("parser_version"),
        F.lit(None).cast("string").alias("decompressor_version"),
        F.lit(None).cast("string").alias("post_processor_version"),
        F.lit("text/plain").alias("content_type"), F.lit("UTF-8").alias("encoding"),
        F.sha2(F.col("a._document_text"), 256).alias("text_sha256"),
        F.length(F.col("a._document_text")).cast("long").alias("text_length"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        F.coalesce(F.col("x._x_source_create_ts"), F.col("a._first_authored_ts"))
         .alias("record_status_effective_from"),
        F.when(inactive, loaded_at).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("null").alias("_sensitivity_labels_json"),
        F.lit(None).cast("string").alias("document_class"),
        F.lit(None).cast("string").alias("contributor_system"),
        F.lit(None).cast("string").alias("succession_status"),
        F.lit(None).cast("string").alias("source_parent_event_id"),
        F.lit(None).cast("string").alias("parent_relation"),
        F.lit(None).cast("string").alias("source_parent_display"),
        F.lit(None).cast("string").alias("source_parent_title"),
        F.lit(None).cast("string").alias("source_parent_tag"),
        F.lit(None).cast("string").alias("source_tag"),
        F.lit(None).cast("string").alias("source_record_status"),
        F.col("a._anon_document_text").alias("document_text_anonymised"),
        F.lit(False).alias("text_is_anonymised"),
        F.lit("urn:barts:endobase").alias("_label_system"),
        F.lit("endoscopy_report").alias("_label_key"),
        F.lit("endobase_exam").alias("source_feed"),
        F.date_format(loaded_at, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.greatest(F.col("a._term_loaded_at"), F.col("x._x_pipeline_updt"))
         .alias("source_update_timestamp"),
        loaded_at.alias("loaded_at"),
        F.lit("endobase").alias("_source_system"),
        F.lit(SRC_ENDOBASE_EXAM).alias("_source_table"), natural.alias("_source_row_id"),
        F.lit(None).cast("string").alias("_raw_content_sha256"),
        F.when(F.col("x._x_person_id").isNotNull() | term_person.isNotNull(), F.lit("direct"))
         .when(F.col("e._e_person_id").isNotNull(), F.lit("encounter_join"))
         .otherwise(F.lit("none")).alias("_linkage_route"),
        F.when(author_source_id.isNotNull(), F.lit("urn:barts:endobase:staff-id"))
         .alias("_author_id_system"),
        author_source_id.alias("_author_source_id"),
        F.lit("reassembled").alias("_assembly_status"),
        F.col("a._term_count").cast("long").alias("_chunk_count"),
    )

def _neonatal_narrative_document_canonical():
    # Assemble normalized neonatal narrative document rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    s = read_source(SRC_NEO_NARRATIVE)
    narrative_source = s.select(
        "EntityID", "BABY_PERSON_ID", "ADC_UPDT", "SOURCE_PRESENT_IND",
        "DischargeSummaryCompletedBy", "DischargeSummaryCompletedBy_Grade",
        *_NEO_NARRATIVE_FIELDS,
    )
    stacked = None
    for narrative_field in _NEO_NARRATIVE_FIELDS:
        field_rows = narrative_source.select(
            "EntityID", "BABY_PERSON_ID", "ADC_UPDT", "SOURCE_PRESENT_IND",
            "DischargeSummaryCompletedBy", "DischargeSummaryCompletedBy_Grade",
            F.lit(narrative_field).alias("narrative_field"),
            F.col(narrative_field).alias("narrative_text"),
            F.lit(None).cast("string").alias("anon_narrative_text"),
        ).where(
            F.col("narrative_text").isNotNull()
            & (F.trim(F.col("narrative_text")) != "")
        )
        stacked = field_rows if stacked is None else stacked.unionByName(field_rows)
    ep = read_source(SRC_NEO_EPISODE).select(
        F.col("EntityID").alias("_ep_entity_id"),
        F.col("DischTime_CLEAN").alias("_ep_disch"),
        F.col("AdmitTime_CLEAN").alias("_ep_admit"),
    )
    d = stacked.join(ep, stacked.EntityID == ep._ep_entity_id, "left")
    event_id = stable_id("document:neonatal_narrative", d.EntityID, d.narrative_field)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", d.BABY_PERSON_ID)],
        SRC_NEO_NARRATIVE,
        F.concat_ws("|", d.EntityID, d.narrative_field),
    )
    retracted = ~F.coalesce(d.SOURCE_PRESENT_IND, F.lit(True))
    event_time = F.coalesce(d._ep_disch, d._ep_admit)
    author_source_id = F.when(_present(d.DischargeSummaryCompletedBy),
                              F.trim(d.DischargeSummaryCompletedBy))
    author_grade = F.when(_present(d.DischargeSummaryCompletedBy_Grade),
                          F.trim(d.DischargeSummaryCompletedBy_Grade))
    return d.select(
        event_id.alias("patient_event_id"), event_id.alias("fact_row_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        d.BABY_PERSON_ID.cast("string").alias("person_id"),
        F.when(d.BABY_PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("string").alias("encounter_id"), event_time.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:badgernet:narrative-field").alias("source_coding_system"),
        d.narrative_field.alias("source_code"), d.narrative_field.alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:badgernet:narrative-field"),
                       d.narrative_field, d.narrative_field, True)
        ).alias("_document_type_json"),
        F.concat_ws(" ", F.lit("Neonatal"), d.narrative_field).alias("title"),
        F.lit(None).cast("string").alias("author_practitioner_id"),
        F.when(author_source_id.isNotNull(),
               F.coalesce(author_grade, F.lit("badgernet_care_team")))
         .alias("author_role"),
        F.lit(None).cast("string").alias("service_id"), F.lit("final").alias("status_code"),
        F.concat_ws(":", d.EntityID, d.narrative_field).alias("version_id"),
        d.narrative_text.alias("document_text"), F.lit("[]").alias("_sections_json"),
        F.lit("badgernet-narrative-v1").alias("parser_version"),
        F.lit(None).cast("string").alias("decompressor_version"),
        F.lit(None).cast("string").alias("post_processor_version"),
        F.lit("text/plain").alias("content_type"), F.lit("UTF-8").alias("encoding"),
        F.sha2(d.narrative_text, 256).alias("text_sha256"),
        F.length(d.narrative_text).cast("long").alias("text_length"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
         .alias("record_status"),
        event_time.alias("record_status_effective_from"),
        F.when(retracted, d.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit('["IG"]').alias("_sensitivity_labels_json"),
        F.lit(None).cast("string").alias("document_class"),
        F.lit(None).cast("string").alias("contributor_system"),
        F.lit(None).cast("string").alias("succession_status"),
        F.lit(None).cast("string").alias("source_parent_event_id"),
        F.lit(None).cast("string").alias("parent_relation"),
        F.lit(None).cast("string").alias("source_parent_display"),
        F.lit(None).cast("string").alias("source_parent_title"),
        F.lit(None).cast("string").alias("source_parent_tag"),
        F.lit(None).cast("string").alias("source_tag"),
        F.lit(None).cast("string").alias("source_record_status"),
        F.lit(None).cast("string").alias("document_text_anonymised"),
        F.lit(False).alias("text_is_anonymised"),
        F.lit("urn:badgernet:narrative-field").alias("_label_system"),
        F.lower(F.trim(d.narrative_field)).alias("_label_key"),
        F.lit("neonatal_episode_narrative").alias("source_feed"),
        F.date_format(d.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        d.ADC_UPDT.alias("source_update_timestamp"), d.ADC_UPDT.alias("loaded_at"),
        F.lit("badgernet").alias("_source_system"),
        F.lit(SRC_NEO_NARRATIVE).alias("_source_table"),
        F.concat_ws("|", d.EntityID, d.narrative_field).alias("_source_row_id"),
        F.lit(None).cast("string").alias("_raw_content_sha256"),
        F.when(author_source_id.isNotNull(), F.lit("urn:badgernet:staff-name"))
         .alias("_author_id_system"),
        author_source_id.alias("_author_source_id"),
        F.when(d.BABY_PERSON_ID.isNotNull(), F.lit("direct"))
         .otherwise(F.lit("none")).alias("_linkage_route"),
    )


SRC_NEO_NARRATIVE = "4_prod.bronze.map_neonatal_episode_narrative"

_NEO_NARRATIVE_FIELDS = [
    "FinalSummaryText", "BirthSummary", "EpisodeSummary",
    "DiagnosisDuringStay", "DrugsDuringStay", "MaternalMedicalNotes",
]




def _encounter_canonical():
    # Assemble normalized encounter rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_ENCOUNTER)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)],
        SRC_ENCOUNTER,
        s.ENCNTR_ID,
    )
    type_class = F.lower(F.coalesce(s.encntr_type_class_desc, s.encntr_class_desc, F.lit("")))
    ended = F.coalesce(
        s.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"),
        F.lit(False),
    )
    # contract v2: retain the encounter SHA as encounter_key and publish native encounter, person, and organization ids
    return s.select(
        stable_id("encounter:mill", s.ENCNTR_ID).alias("encounter_key"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        skey.alias("subject_key"),
        ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.lit(None).cast("string").alias("parent_encounter_id"),
        F.lit("source_parent_unavailable").alias("parentage_status"),
        F.when(type_class.contains("inpatient"), F.lit("spell"))
         .when(type_class.contains("emergency"), F.lit("emergency_visit"))
         .when(type_class.contains("outpatient"), F.lit("outpatient_attendance"))
         .when(type_class.contains("recurring"), F.lit("recurring_contact"))
         .when(type_class.contains("preadmit"), F.lit("preadmission"))
         .when(type_class.contains("wait list"), F.lit("waiting_list_placeholder"))
         .when(type_class.contains("results only"), F.lit("results_only"))
         .otherwise(F.lit("other")).alias("encounter_level"),
        s.ENCNTR_CLASS_CD.cast("string").alias("class_code"),
        s.encntr_class_desc.alias("class_display"),
        s.ENCNTR_TYPE_CD.cast("string").alias("type_code"),
        s.encntr_type_desc.alias("type_display"),
        s.ENCNTR_TYPE_CLASS_CD.cast("string").alias("type_class_code"),
        s.encntr_type_class_desc.alias("type_class_display"),
        s.ENCNTR_STATUS_CD.cast("string").alias("status_code"),
        s.encntr_status_desc.alias("status_display"),
        s.ARRIVAL_DT_TM_BEST.alias("period_start"),
        s.DEPARTURE_DT_TM_BEST.alias("period_end"),
        s.ARRIVAL_METHOD.alias("arrival_method"),
        s.ARRIVAL_CONFIDENCE.alias("arrival_confidence"),
        s.DEPARTURE_METHOD.alias("departure_method"),
        s.DEPARTURE_CONFIDENCE.alias("departure_confidence"),
        s.LENGTH_OF_STAY_MINUTES.cast("long").alias("length_of_stay_minutes"),
        s.SCHEDULED_ARRIVAL_DT_TM.alias("scheduled_start"),
        s.SCHEDULED_DEPARTURE_DT_TM.alias("scheduled_end"),
        s.REG_DT_TM.alias("registration_datetime"),
        s.INPATIENT_ADMIT_DT_TM.alias("inpatient_admit_datetime"),
        s.DISCH_DT_TM.alias("discharge_datetime"),
        s.ENCOUNTER_COMPLETE_DT_TM_EFFECTIVE.alias("workflow_complete_datetime"),
        s.ARRIVE_DT_TM.alias("raw_arrival_datetime"),
        s.DEPART_DT_TM.alias("raw_departure_datetime"),
        s.ADMIT_SRC_CD.cast("string").alias("admission_source_code"),
        s.admit_src_desc.alias("admission_source_display"),
        s.DISCH_TO_LOCTN_CD.cast("string").alias("discharge_destination_code"),
        s.disch_loctn_desc.alias("discharge_destination_display"),
        s.MED_SERVICE_CD.cast("string").alias("responsible_service_code"),
        s.med_service_desc.alias("responsible_service_display"),
        s.SPECIALTY_UNIT_CD.cast("string").alias("specialty_code"),
        s.specialty_unit_desc.alias("specialty_display"),
        F.when(s.LOC_NURSE_UNIT_CD.isNotNull(),
               stable_id("location:mill:nurse_unit", s.LOC_NURSE_UNIT_CD)).alias("current_location_key"),
        s.ORGANIZATION_ID.cast("bigint").alias("organization_id"),
        F.when(s.SERVICE_PROVIDER_ORG_ID.isNotNull(),
               stable_id("organization:mill", s.SERVICE_PROVIDER_ORG_ID))
         .alias("service_provider_organization_key"),
        s.REASON_FOR_VISIT.alias("reason_for_visit"),
        s.ATTENDANCE_EVIDENCE.alias("attendance_evidence"),
        s.ATTENDANCE_WITNESS_COUNT.cast("int").alias("attendance_witness_count"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.when(ended, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        s.ACTIVE_STATUS_DT_TM.alias("record_status_effective_from"),
        F.when(ended, s.END_EFFECTIVE_DT_TM).alias("record_status_effective_to"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.UPDT_DT_TM.alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        s.ENCNTR_ID.alias("_source_encounter_id"),
        s.REG_PRSNL_ID.alias("_registration_practitioner_id"),
        s.DISCH_PRSNL_ID.alias("_discharge_practitioner_id"),
        s.CREATE_PRSNL_ID.alias("_creator_practitioner_id"),
        s.REG_DT_TM.alias("_registration_role_start"),
        s.DISCH_DT_TM.alias("_discharge_role_start"),
        s.CREATE_DT_TM.alias("_creator_role_start"),
    )




# ==== Encounter spine, location stays, relationships and care participation ====

# contract v2: retain the full canonical shape for internal reuse.
ENCOUNTER_SOURCE_COLUMNS = [
    "encounter_key",
    "encounter_id",
    "subject_key",
    "subject_id_system",
    "person_id",
    "parent_encounter_id",
    "parentage_status",
    "encounter_level",
    "class_code",
    "class_display",
    "type_code",
    "type_display",
    "type_class_code",
    "type_class_display",
    "status_code",
    "status_display",
    "period_start",
    "period_end",
    "arrival_method",
    "arrival_confidence",
    "departure_method",
    "departure_confidence",
    "length_of_stay_minutes",
    "scheduled_start",
    "scheduled_end",
    "registration_datetime",
    "inpatient_admit_datetime",
    "discharge_datetime",
    "workflow_complete_datetime",
    "raw_arrival_datetime",
    "raw_departure_datetime",
    "admission_source_code",
    "admission_source_display",
    "discharge_destination_code",
    "discharge_destination_display",
    "responsible_service_code",
    "responsible_service_display",
    "specialty_code",
    "specialty_display",
    "current_location_key",
    "organization_id",
    "service_provider_organization_key",
    "reason_for_visit",
    "attendance_evidence",
    "attendance_witness_count",
    "confidentiality_code",
    "vip_ind",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_encounter():
    # Assemble encounter rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    return _encounter_canonical().select(*ENCOUNTER_SOURCE_COLUMNS)


SRC_MEDICONNECT_TYPE_MAP = "3_lookup.omop.mediconnect_device_type_map"


# contract v2: retain the full canonical shape for internal reuse.
CONDITION_SOURCE_COLUMNS = [
    "patient_event_key",
    "source_object",
    "diagnosis_id",
    "problem_id",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "condition_code",
    "category_code",
    "category_display",
    "clinical_status_code",
    "clinical_status_display",
    "verification_status_code",
    "verification_status_display",
    "onset_datetime",
    "abatement_datetime",
    "body_site_code",
    "body_site_display",
    "severity_code",
    "severity_display",
    "laterality_code",
    "laterality_display",
    "asserted_datetime",
    "asserter_practitioner_id",
    "recorder_practitioner_id",
    "revision_history_count",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

CONDITION_PRIMITIVE_COLUMNS = [
    {
        "condition_code": "_condition_code_json",
        "revision_history": "_revision_history_json",
        "revision_history_count": "_revision_history_count",
    }.get(name, name)
    for name in CONDITION_SOURCE_COLUMNS
]




# contract v2: retain the full canonical shape for internal reuse.
PROCEDURE_SOURCE_COLUMNS = [
    "patient_event_key",
    "source_object",
    "procedure_id",
    "endobase_exam_id",
    "implant_event_id",
    "implant_sequence",
    "surg_case_proc_id",
    "source_row_hash",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "procedure_code",
    "status_code",
    "status_display",
    "performed_start",
    "performed_end",
    "body_site_code",
    "body_site_display",
    "laterality_code",
    "laterality_display",
    "performer_practitioner_id",
    "procedure_location_code",
    "procedure_location_display",
    "procedure_note",
    "implant_description",
    "device_code",
    "device_display",
    "manufacturer",
    "serial_number",
    "batch_number",
    "udi_di",
    "udi_standard",
    "quantity",
    "implant_attribute_count",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
    # Internal implant mapping evidence needed by the S3 device axis. These
    # cross the primitive/QC boundary and are removed by PROCEDURE_PUBLIC_COLUMNS.
    "_device_snomed_code",
    "_device_snomed_display",
    "_device_mapping_layer",
    "_device_mapping_confidence",
]

def _procedure_canonical_pregate():
    # Normalize procedure source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_PROCEDURE)
    event_id = stable_id("procedure:mill:procedure", s.PROCEDURE_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_PROCEDURE, s.PROCEDURE_ID
    )
    raw_source_code = F.coalesce(
        s.SOURCE_IDENTIFIER, s.CONCEPT_CKI_IDENTIFIER, s.NOMENCLATURE_ID.cast("string")
    )
    source_display = F.coalesce(s.SOURCE_STRING, s.PROCEDURE_DISPLAY, s.PROC_FTDESC)
    source_code = _code_or_display(raw_source_code, source_display)
    inactive = F.lower(F.coalesce(s.active_status_desc, F.lit(""))).contains("inactive")
    return s.select(
        event_id.alias("patient_event_key"),
        # contract v2: publish the Millennium procedure arm, PROCEDURE_ID, and native relationship ids
        F.lit("millennium_procedure").alias("source_object"),
        s.PROCEDURE_ID.cast("bigint").alias("procedure_id"),
        F.lit(None).cast("bigint").alias("endobase_exam_id"),
        F.lit(None).cast("bigint").alias("implant_event_id"),
        F.lit(None).cast("bigint").alias("implant_sequence"),
        F.lit(None).cast("bigint").alias("surg_case_proc_id"),
        F.lit(None).cast("string").alias("source_row_hash"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        F.coalesce(s.PROCEDURE_DT_TM_EFFECTIVE, s.PROC_START_DT_TM, s.PROC_DT_TM).alias("event_datetime"),
        s.PROC_END_DT_TM.alias("event_end_datetime"),
        F.coalesce(s.source_vocabulary_desc, s.CONCEPT_CKI_SOURCE,
                   F.lit("urn:cerner:nomenclature")).alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept_json(
            coding_obj(F.coalesce(s.source_vocabulary_desc, s.CONCEPT_CKI_SOURCE,
                                  F.lit("urn:cerner:nomenclature")), source_code, source_display, True),
            coding_obj(F.lit("http://snomed.info/sct"), s.SNOMED_CODE, s.SNOMED_TERM, False,
                       "bronze.map_procedure", None),
            coding_obj(F.lit("http://fhir.hl7.org.uk/CodeSystem/OPCS-4"), s.OPCS4_CODE, s.OPCS4_TERM, False,
                       "bronze.map_procedure", None),
            coding_obj(F.lit("urn:omop:concept_id"), s.OMOP_CONCEPT_ID, s.OMOP_CONCEPT_NAME, False,
                       "bronze.map_procedure", None),
        ).alias("_procedure_code_json"),
        F.when(inactive, F.lit("stopped")).otherwise(F.lit("completed")).alias("status_code"),
        s.active_status_desc.alias("status_display"),
        F.coalesce(s.PROC_START_DT_TM, s.PROCEDURE_DT_TM_EFFECTIVE).alias("performed_start"),
        s.PROC_END_DT_TM.alias("performed_end"),
        F.lit(None).cast("string").alias("body_site_code"),
        F.lit(None).cast("string").alias("body_site_display"),
        s.LATERALITY_CD.cast("string").alias("laterality_code"), s.laterality_desc.alias("laterality_display"),
        F.lit(None).cast("bigint").alias("performer_practitioner_id"),
        s.PROC_LOC_CD.cast("string").alias("procedure_location_code"),
        s.proc_location_desc.alias("procedure_location_display"),
        s.PROCEDURE_NOTE.alias("procedure_note"),
        F.lit(None).cast("string").alias("implant_description"),
        F.lit(None).cast("string").alias("device_code"), F.lit(None).cast("string").alias("device_display"),
        F.lit(None).cast("string").alias("manufacturer"), F.lit(None).cast("string").alias("serial_number"),
        F.lit(None).cast("string").alias("batch_number"), F.lit(None).cast("string").alias("udi_di"),
        F.lit(None).cast("string").alias("udi_standard"), F.lit(None).cast("decimal(38,6)").alias("quantity"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        s.ACTIVE_STATUS_DT_TM.alias("record_status_effective_from"),
        F.when(inactive, s.ACTIVE_STATUS_DT_TM).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("procedure").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.UPDT_DT_TM.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_PROCEDURE).alias("_source_table"),
        s.PROCEDURE_ID.cast("string").alias("_source_row_id"),
        s.SNOMED_CODE.cast("string").alias("_snomed_code"), s.SNOMED_TERM.alias("_snomed_display"),
        s.OPCS4_CODE.cast("string").alias("_opcs4_code"), s.OPCS4_TERM.alias("_opcs4_display"),
        s.OMOP_CONCEPT_ID.cast("string").alias("_omop_code"), s.OMOP_CONCEPT_NAME.alias("_omop_display"),
        F.lit(None).cast("string").alias("_device_snomed_code"),
        F.lit(None).cast("string").alias("_device_snomed_display"),
        F.lit(None).cast("string").alias("_device_mapping_layer"),
        F.lit(None).cast("double").alias("_device_mapping_confidence"),
    )

def _procedure_canonical():
    # Keep the normalized procedure rows that pass the existing source-code admission rule.
    return _procedure_canonical_pregate().where(_usable_code(F.col("source_code")))

def _implant_procedure_canonical_pregate():
    # Normalize implant procedure source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_IMPLANT_DETAILS)
    source_row_id = F.concat_ws(":", s.EVENT_ID.cast("string"), s.IMPLANT_SEQUENCE.cast("string"))
    event_id = stable_id("procedure:mill:implant", s.EVENT_ID, s.IMPLANT_SEQUENCE)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_IMPLANT_DETAILS, source_row_id
    )
    loaded = F.coalesce(s.SOURCE_MAX_ADC_UPDT, s.ADC_UPDT, s.BASE_EVENT_ADC_UPDT)
    return s.select(
        event_id.alias("patient_event_key"),
        # contract v2: publish the implant arm with its EVENT_ID and IMPLANT_SEQUENCE composite and native relationships
        F.lit("implant").alias("source_object"),
        F.lit(None).cast("bigint").alias("procedure_id"),
        F.lit(None).cast("bigint").alias("endobase_exam_id"),
        s.EVENT_ID.cast("bigint").alias("implant_event_id"),
        s.IMPLANT_SEQUENCE.cast("bigint").alias("implant_sequence"),
        F.lit(None).cast("bigint").alias("surg_case_proc_id"),
        F.lit(None).cast("string").alias("source_row_hash"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        F.coalesce(s.IMPLANT_DT_TM, s.EVENT_START_DT_TM, s.CLINSIG_UPDT_DT_TM).alias("event_datetime"),
        s.EVENT_END_DT_TM.alias("event_end_datetime"),
        F.lit("urn:barts:implant:primary-procedure").alias("source_coding_system"),
        s.PRIMARY_PROCEDURE.alias("source_code"), s.PRIMARY_PROCEDURE.alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:barts:implant:primary-procedure"),
                       s.PRIMARY_PROCEDURE, s.PRIMARY_PROCEDURE, True)
        ).alias("_procedure_code_json"),
        F.lit("completed").alias("status_code"), F.lit(None).cast("string").alias("status_display"),
        F.coalesce(s.IMPLANT_DT_TM, s.EVENT_START_DT_TM).alias("performed_start"),
        s.EVENT_END_DT_TM.alias("performed_end"),
        F.lit(None).cast("string").alias("body_site_code"),
        F.lit(None).cast("string").alias("body_site_display"),
        F.lit(None).cast("string").alias("laterality_code"), s.SIDE_OF_PROCEDURE.alias("laterality_display"),
        s.PERFORMED_PRSNL_ID.cast("bigint").alias("performer_practitioner_id"),
        F.lit(None).cast("string").alias("procedure_location_code"),
        F.lit(None).cast("string").alias("procedure_location_display"),
        F.lit(None).cast("string").alias("procedure_note"),
        s.IMPLANT_DESCRIPTION.alias("implant_description"),
        F.coalesce(s.SNOMED_DEVICE_CONCEPT_ID.cast("string"), s.GMDN_CODE.cast("string"))
         .alias("device_code"),
        F.coalesce(s.SNOMED_DEVICE_CONCEPT_NAME, s.GMDN_NAME, s.DEVICE_TYPE).alias("device_display"),
        s.MANUFACTURER.alias("manufacturer"), F.coalesce(s.GS1_SERIAL_NUMBER, s.SERIAL_NUMBER).alias("serial_number"),
        s.GS1_BATCH_NUMBER.alias("batch_number"), s.UDI_DI.alias("udi_di"), s.UDI_STANDARD.alias("udi_standard"),
        s.QUANTITY_NUMERIC.cast("decimal(38,6)").alias("quantity"),
        F.lit("active").alias("record_status"),
        s.VALID_FROM_DT_TM.alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("implant_details").alias("source_feed"),
        F.date_format(loaded, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.CLINSIG_UPDT_DT_TM.alias("source_update_timestamp"), loaded.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_IMPLANT_DETAILS).alias("_source_table"),
        source_row_id.alias("_source_row_id"),
        F.lit(None).cast("string").alias("_snomed_code"), F.lit(None).cast("string").alias("_snomed_display"),
        F.lit(None).cast("string").alias("_opcs4_code"), F.lit(None).cast("string").alias("_opcs4_display"),
        F.lit(None).cast("string").alias("_omop_code"), F.lit(None).cast("string").alias("_omop_display"),
        s.SNOMED_DEVICE_CONCEPT_ID.cast("string").alias("_device_snomed_code"),
        s.SNOMED_DEVICE_CONCEPT_NAME.alias("_device_snomed_display"),
        s.MAPPING_LAYER.alias("_device_mapping_layer"),
        s.MAPPING_CONFIDENCE.cast("double").alias("_device_mapping_confidence"),
        s.BASE_CLINICAL_EVENT_ID.alias("_base_clinical_event_id"),
        s.IMPLANT_FORM_EVENT_ID.alias("_implant_form_event_id"),
    )

def _implant_procedure_canonical():
    # Keep the normalized implant procedure rows that pass the existing source-code admission
    # rule.
    return _implant_procedure_canonical_pregate().where(_usable_code(F.col("source_code")))

def _theatre_procedure_canonical_pregate():
    # Normalize theatre procedure source rows before the downstream admission filter so excluded
    # evidence remains countable.
    p = read_source(SRC_THEATRE_CASE_PROCEDURE).alias("p")
    c = read_source(SRC_THEATRE_CASE).alias("c")
    s = p.join(c, p.SURG_CASE_ID == c.SURG_CASE_ID, "left")
    source_row_id = p.SURG_CASE_PROC_ID.cast("string")
    event_id = stable_id("procedure:surginet_case", p.SURG_CASE_PROC_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", p.PERSON_ID)], SRC_THEATRE_CASE_PROCEDURE, source_row_id
    )
    raw_source_code = F.coalesce(p.SURG_PROC_CD.cast("string"), p.PROC_TEXT)
    source_display = F.coalesce(p.SURG_PROC_DESCRIPTION, p.PROC_TEXT)
    source_code = _code_or_display(
        raw_source_code,
        source_display,
        excluded_displays=(".", "N/A", "SEE ADDITIONAL COMMENTS"),
    )
    case_status = F.upper(F.coalesce(c.CASE_STATUS, F.lit("SCHEDULED_ONLY")))
    status = (
        F.when(case_status == "CANCELLED", F.lit("stopped"))
        .when(case_status == "PERFORMED", F.lit("completed"))
        .otherwise(F.lit("preparation"))
    )
    inactive = (
        (F.coalesce(p.SOURCE_PRESENT_IND.cast("boolean"), F.lit(True)) == F.lit(False))
        | (F.coalesce(p.ACTIVE_IND.cast("long"), F.lit(1)) == 0)
    )
    performed_start = F.coalesce(
        p.PROC_START_DT_TM, c.SURG_START_DT_TM, c.FIRST_PERFORMED_MILESTONE_DT_TM,
        c.SCHED_START_DT_TM,
    )
    loaded_at = F.greatest(p.ADC_UPDT, c.ADC_UPDT)
    source_update = F.greatest(p.SOURCE_ADC_UPDT, c.SOURCE_ADC_UPDT)
    return s.select(
        event_id.alias("patient_event_key"),
        # contract v2: publish the theatre arm, SURG_CASE_PROC_ID, and native relationship ids
        F.lit("theatre").alias("source_object"),
        F.lit(None).cast("bigint").alias("procedure_id"),
        F.lit(None).cast("bigint").alias("endobase_exam_id"),
        F.lit(None).cast("bigint").alias("implant_event_id"),
        F.lit(None).cast("bigint").alias("implant_sequence"),
        p.SURG_CASE_PROC_ID.cast("bigint").alias("surg_case_proc_id"),
        F.lit(None).cast("string").alias("source_row_hash"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        p.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(p.PERSON_ID.isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
        .alias("identity_status"),
        p.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        performed_start.alias("event_datetime"), p.PROC_END_DT_TM.alias("event_end_datetime"),
        F.lit("urn:cerner:surginet:procedure").alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:cerner:surginet:procedure"), source_code,
                       source_display, True)
        ).alias("_procedure_code_json"),
        status.alias("status_code"), c.CASE_STATUS.alias("status_display"),
        performed_start.alias("performed_start"), p.PROC_END_DT_TM.alias("performed_end"),
        F.lit(None).cast("string").alias("body_site_code"),
        F.lit(None).cast("string").alias("body_site_display"),
        F.lit(None).cast("string").alias("laterality_code"),
        F.lit(None).cast("string").alias("laterality_display"),
        p.PRIMARY_SURGEON_ID.cast("bigint").alias("performer_practitioner_id"),
        c.SURG_OP_LOC_CD.cast("string").alias("procedure_location_code"),
        c.SURG_OP_LOCATION_DESCRIPTION.alias("procedure_location_display"),
        F.concat_ws(
            " | ", p.PROC_TEXT,
            F.when(p.MODIFIER_DESCRIPTIONS.isNotNull(), F.to_json(p.MODIFIER_DESCRIPTIONS)),
        ).alias("procedure_note"),
        F.lit(None).cast("string").alias("implant_description"),
        F.lit(None).cast("string").alias("device_code"),
        F.lit(None).cast("string").alias("device_display"),
        F.lit(None).cast("string").alias("manufacturer"),
        F.lit(None).cast("string").alias("serial_number"),
        F.lit(None).cast("string").alias("batch_number"),
        F.lit(None).cast("string").alias("udi_di"),
        F.lit(None).cast("string").alias("udi_standard"),
        F.lit(None).cast("decimal(38,6)").alias("quantity"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        F.coalesce(p.PROC_START_DT_TM, c.SCHED_START_DT_TM).alias("record_status_effective_from"),
        F.when(inactive, F.coalesce(p.SOURCE_ABSENT_DETECTED_TS, p.ADC_UPDT))
        .alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("theatre_case_procedure").alias("source_feed"),
        F.date_format(loaded_at, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"), loaded_at.alias("loaded_at"),
        F.lit("surginet").alias("_source_system"),
        F.lit(SRC_THEATRE_CASE_PROCEDURE).alias("_source_table"),
        source_row_id.alias("_source_row_id"),
        F.lit(None).cast("string").alias("_snomed_code"),
        F.lit(None).cast("string").alias("_snomed_display"),
        F.lit(None).cast("string").alias("_opcs4_code"),
        F.lit(None).cast("string").alias("_opcs4_display"),
        F.lit(None).cast("string").alias("_omop_code"),
        F.lit(None).cast("string").alias("_omop_display"),
        F.lit(None).cast("string").alias("_device_snomed_code"),
        F.lit(None).cast("string").alias("_device_snomed_display"),
        F.lit(None).cast("string").alias("_device_mapping_layer"),
        F.lit(None).cast("double").alias("_device_mapping_confidence"),
        p.SURG_CASE_ID.cast("string").alias("_theatre_case_id"),
    )

def _theatre_procedure_canonical():
    # Keep the normalized theatre procedure rows that pass the existing source-code admission
    # rule.
    return _theatre_procedure_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_PROCEDURE = "4_prod.bronze.map_procedure"

PROCEDURE_PRIMITIVE_COLUMNS = [
    {
        "procedure_code": "_procedure_code_json",
        "implant_attribute_history": "_implant_attribute_json",
        "implant_attribute_count": "_implant_attribute_count",
    }.get(name, name)
    for name in PROCEDURE_SOURCE_COLUMNS
]




def _alias_resolved_accession(df, id_col="pathology_accession_id"):
    """Resolve retired accessions to survivors before specimen:accession minting."""
    a = read_source(SRC_PATHOLOGY_ACCESSION_ALIAS).select(
        F.col("retired_pathology_accession_id").alias("_alias_retired"),
        F.col("survivor_pathology_accession_id").alias("_alias_survivor"),
    )
    return (
        df.join(a, df[id_col] == a["_alias_retired"], "left")
        .withColumn(
            "_resolved_accession_id",
            F.coalesce(F.col("_alias_survivor"), F.col(id_col)),
        )
        .drop("_alias_retired", "_alias_survivor")
    )


SRC_PATHOLOGY_ACCESSION_ALIAS = "4_prod.bronze.map_pathology_accession_alias"


SRC_PATHOLOGY_ACCESSION = "4_prod.bronze.map_pathology_accession"




def _pathology_specimen_canonical():
    # Assemble normalized pathology specimen rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    i = spark.read.table(_n("journey_events._pathology_accession_identity"))
    event_id = stable_id("specimen:accession", F.col("pathology_accession_id"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("canonical_person_id")),
         ("urn:barts:mrn", F.col("evidence_mrn")),
         ("https://fhir.nhs.uk/Id/nhs-number", F.col("evidence_nhs"))],
        SRC_PATHOLOGY_ACCESSION, F.col("pathology_accession_id"),
    )
    # contract v2: publish patient_event_key, BIGINT identity columns, and the ruled resolved accession key
    return i.select(
        event_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("canonical_person_id").cast("bigint").alias("person_id"),
        F.when(F.col("person_resolution_status") == "eligible", F.lit("resolved"))
         .when(F.col("person_resolution_status") == "conflicting", F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        _clamped_ts(F.coalesce("sample_dt", "request_dt", "report_dt")).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:barts:pathology:specimen-type").alias("source_coding_system"),
        F.col("specimen_type_code").cast("string").alias("source_code"),
        F.col("specimen_type_code").cast("string").alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:barts:pathology:specimen-type"),
                       F.col("specimen_type_code"), F.col("specimen_type_code"), True),
            coding_obj(F.lit("http://snomed.info/sct"), F.col("specimen_type_snomed_code"),
                       F.col("specimen_type_code"), False,
                       "bronze.map_pathology_accession", None),
        ).alias("specimen_type"),
        F.col("pathology_accession_id").alias("accession_identifier"),
        F.col("primary_source_accession_id").alias("resolved_accession_key"), F.col("normalized_lab_no"),
        F.col("canonical_accession_status"), F.col("person_resolution_status"),
        F.col("lab_series"), F.col("discipline"), F.col("urgent_flag"),
        F.col("research_qi_only"), F.col("clinical_details"), F.col("tlcs_requested"),
        F.col("conditions"), F.col("reason"), F.col("body_site_code"),
        F.col("body_site_snomed_code"), F.col("specimen_type_code"),
        F.col("specimen_type_snomed_code"),
        _clamped_ts(F.col("sample_dt")).alias("sample_datetime"),
        _clamped_ts(F.col("request_dt")).alias("request_datetime"),
        _clamped_ts(F.col("report_dt")).alias("report_datetime"),
        F.coalesce(F.col("source_row_count"), F.lit(0).cast("long"))
         .alias("source_history_row_count"),
        F.lit("active").alias("record_status"),
        F.col("created_at").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("pathology_accession").alias("source_feed"),
        F.date_format("ADC_UPDT", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("created_at").alias("source_update_timestamp"),
        F.col("ADC_UPDT").alias("loaded_at"),
        F.lit("laboratory").alias("_source_system"),
        F.lit(SRC_PATHOLOGY_ACCESSION).alias("_source_table"),
        F.col("pathology_accession_id").alias("_source_row_id"),
    )




def _pathology_report_series_canonical():
    # The report feed already carries the canonical accession. Resolve
    # aliases on that governed key directly; source_record_key identifies the
    # source result used to assemble a version and is not an accession key.
    # Assemble normalized pathology report series rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    r = _alias_resolved_accession(
        read_source(SRC_PATHOLOGY_REPORT_VERSIONS)
    ).alias("r")
    text_present = r.report_text.isNotNull() & (F.trim(r.report_text) != "")
    link = spark.read.table(_n("journey_events._pathology_parent_link")).select(
        F.col("source_parent_key").alias("_link_parent_key"),
        F.col("pathology_accession_id").alias("_link_accession_id"),
        F.col("source_encounter_id").alias("_link_encounter_id"),
    ).alias("l")
    i = spark.read.table(_n("journey_events._pathology_accession_identity")).select(
        F.col("pathology_accession_id").alias("_identity_accession_id"),
        "canonical_person_id", "person_resolution_status", "evidence_mrn", "evidence_nhs",
    ).alias("i")
    enriched = (
        r.join(link, r.source_record_key == F.col("l._link_parent_key"), "left")
        .join(
            i,
            F.col("r._resolved_accession_id") == F.col("i._identity_accession_id"),
            "left",
        )
    )
    latest = (
        enriched.groupBy(r.report_series_id.alias("report_series_id"))
        .agg(
            F.max(F.struct(
                r.version_ordinal, r.report_version_id, r.source_record_key,
                r.report_role, r.discipline, r.report_code, r.report_section,
                r.lifecycle_status, r.supersedes_report_version_id, r.issued_dt,
                r.report_text_hash, r.research_qi_only, r.valid_from,
                F.col("r._resolved_accession_id").alias("pathology_accession_id"),
                F.col("l._link_encounter_id").alias("source_encounter_id"),
                F.col("i.canonical_person_id").alias("canonical_person_id"),
                F.col("i.person_resolution_status").alias("person_resolution_status"),
                F.col("i.evidence_mrn").alias("evidence_mrn"),
                F.col("i.evidence_nhs").alias("evidence_nhs"),
            )).alias("v"),
            F.count(F.lit(1)).cast("long").alias("version_count"),
            F.max(F.when(r.is_current & text_present, r.report_version_id))
             .alias("current_text_version_id"),
            F.max(F.when(r.is_current, F.lit(True)).otherwise(F.lit(False)))
             .alias("is_current_present"),
            F.max("ADC_UPDT").alias("loaded_at"),
        )
    )
    event_id = stable_id("pathology_report:series", F.col("report_series_id"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("v.canonical_person_id")),
         ("urn:barts:mrn", F.col("v.evidence_mrn")),
         ("https://fhir.nhs.uk/Id/nhs-number", F.col("v.evidence_nhs"))],
        SRC_PATHOLOGY_REPORT_VERSIONS, F.col("report_series_id"),
    )
    retracted = F.col("v.lifecycle_status").isin("cancelled", "entered_in_error")
    # contract v2: publish report and related hashes under key names with BIGINT person and encounter ids
    return latest.select(
        event_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("v.canonical_person_id").cast("bigint").alias("person_id"),
        F.when(F.col("v.person_resolution_status") == "eligible", F.lit("resolved"))
         .when(F.col("v.person_resolution_status") == "conflicting", F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.col("v.source_encounter_id").cast("bigint").alias("encounter_id"),
        _clamped_ts(F.col("v.issued_dt")).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:barts:pathology:report-code").alias("source_coding_system"),
        F.col("v.report_code").cast("string").alias("source_code"),
        F.coalesce(F.col("v.report_role"), F.col("v.report_code")).cast("string")
         .alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:barts:pathology:report-code"),
                       F.col("v.report_code"), F.col("v.report_role"), True),
        ).alias("report_code"),
        F.col("v.report_role").alias("report_role"),
        F.col("v.discipline").alias("discipline"),
        F.col("v.report_section").alias("report_section"),
        F.col("v.lifecycle_status").alias("lifecycle_status"),
        F.col("v.version_ordinal").cast("long").alias("version_ordinal"),
        F.col("version_count"),
        F.col("v.report_version_id").alias("report_version_key"),
        F.col("v.supersedes_report_version_id").alias("supersedes_report_version_key"),
        F.col("is_current_present"),
        F.when(F.col("current_text_version_id").isNotNull(),
               stable_id("document:pathology_report", F.col("current_text_version_id")))
         .alias("document_key"),
        _clamped_ts(F.col("v.issued_dt")).alias("issued_datetime"),
        F.when(F.col("v.pathology_accession_id").isNotNull(),
               stable_id("specimen:accession", F.col("v.pathology_accession_id")))
         .alias("specimen_key"),
        F.col("v.pathology_accession_id").alias("accession_identifier"),
        F.col("v.report_text_hash").alias("report_text_hash"),
        F.col("v.research_qi_only").alias("research_qi_only"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        F.col("v.valid_from").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("pathology_report").alias("source_feed"),
        F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("v.valid_from").alias("source_update_timestamp"), F.col("loaded_at"),
        F.lit("laboratory").alias("_source_system"),
        F.lit(SRC_PATHOLOGY_REPORT_VERSIONS).alias("_source_table"),
        F.col("report_series_id").alias("_source_row_id"),
    )




# contract v2: retain the full canonical shape for internal reuse.
RESULT_SOURCE_COLUMNS = [
    "patient_event_key",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "result_code",
    "pathology_report_key",
    "specimen_key",
    "equivalence_group_key",
    "representation_role",
    "preferred_result_ind",
    "person_projection_status",
    "value_number",
    "value_text",
    "value_datetime",
    "value_concept_id",
    "value_concept_display",
    "operator_concept_id",
    "unit_source_value",
    "ucum_code",
    "unit_concept_id",
    "reference_range_low",
    "reference_range_high",
    "interpretation_code",
    "polarity",
    "finding_axis",
    "result_status",
    "body_site_code",
    "clinician_code",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
    "_result_snomed_code",
    "_result_snomed_display",
    "_result_omop_code",
    "_result_confidence_tier",
]

def _pathology_result_canonical_pregate(source_df=None, include_parent_links=False):
    # Normalize pathology result source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_PATHOLOGY) if source_df is None else source_df
    source_row_id = F.coalesce(
        s.source_record_key,
        F.concat_ws(":", s.source_table, s.source_event_id.cast("string"), s.source_sequence_start.cast("string")),
    )
    event_id = stable_id("pathology_result:pathology", source_row_id)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID), ("urn:barts:mrn", s.MRN),
         ("https://fhir.nhs.uk/Id/nhs-number", s.NHS_Number)],
        SRC_PATHOLOGY, source_row_id,
    )
    ended = s.valid_until_dt_tm.isNotNull() & (
        s.valid_until_dt_tm < F.lit("2100-01-01").cast("timestamp")
    )
    # contract v2: publish result and related hashes under key names with BIGINT person and encounter ids
    return s.select(
        event_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(_present(s.PERSON_ID), F.lit("resolved"))
         .when(_present(s.MRN) | _present(s.NHS_Number), F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        _clamped_ts(s.measurement_datetime).alias("event_datetime"),
        _clamped_ts(s.event_end_dt_tm).alias("event_end_datetime"),
        F.coalesce(s.code_system, F.lit("urn:barts:pathology:test")).alias("source_coding_system"),
        F.coalesce(s.code, s.EVENT_CD.cast("string")).alias("source_code"),
        F.coalesce(s.description, s.EVENT_CD_DISPLAY).alias("source_display"),
        codeable_concept_json(
            coding_obj(F.coalesce(s.code_system, F.lit("urn:barts:pathology:test")),
                       F.coalesce(s.code, s.EVENT_CD.cast("string")),
                       F.coalesce(s.description, s.EVENT_CD_DISPLAY), True),
            coding_obj(F.lit("http://snomed.info/sct"), s.test_snomed_code, s.description, False,
                       F.concat(F.lit("bronze.map_pathology:"),
                                F.coalesce(s.test_confidence_tier, F.lit("unknown"))), None),
            coding_obj(F.lit("http://loinc.org"), s.test_loinc_code, s.description, False,
                       "bronze.map_pathology", None),
            coding_obj(F.lit("urn:omop:concept_id"), s.test_omop_concept_id,
                       s.measurement_concept_name, False,
                       F.concat(F.lit("bronze.map_pathology:"),
                                F.coalesce(s.test_confidence_tier, F.lit("unknown"))), None),
        ).alias("_result_code_json"),
        *(
            [
                F.when(
                    F.col("_link_series_id").isNotNull(),
                    stable_id("pathology_report:series", F.col("_link_series_id")),
                ).alias("pathology_report_key"),
                F.when(
                    F.col("_link_accession_id").isNotNull(),
                    stable_id("specimen:accession", F.col("_link_accession_id")),
                ).alias("specimen_key"),
                F.col("_eq_group").alias("equivalence_group_key"),
                F.col("_eq_role").alias("representation_role"),
                F.col("_eq_preferred").alias("preferred_result_ind"),
                F.col("_eq_person_projection").alias("person_projection_status"),
            ]
            if include_parent_links else []
        ),
        s.value_as_number.cast("decimal(38,10)").alias("value_number"),
        s.value_source_value.alias("value_text"), s.value_as_datetime.alias("value_datetime"),
        s.value_as_concept_id.cast("string").alias("value_concept_id"),
        s.result_concept_name.alias("value_concept_display"),
        s.operator_concept_id.cast("string").alias("operator_concept_id"),
        s.unit_source_value, s.ucum_code, s.unit_concept_id.cast("string").alias("unit_concept_id"),
        s.range_low.cast("decimal(38,10)").alias("reference_range_low"),
        s.range_high.cast("decimal(38,10)").alias("reference_range_high"),
        s.normalcy.alias("interpretation_code"), s.result_growth_grade.alias("polarity"),
        s.master_result_type.alias("finding_axis"), s.result_status,
        s.body_site_code, s.clinician_code,
        F.when(ended, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        s.valid_from_dt_tm.alias("record_status_effective_from"),
        F.when(ended, s.valid_until_dt_tm).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"), F.lit("pathology").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.source_adc_updt.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("laboratory").alias("_source_system"), F.lit(SRC_PATHOLOGY).alias("_source_table"),
        source_row_id.alias("_source_row_id"), s.source_parent_key.alias("_source_parent_key"),
        s.test_snomed_code.cast("string").alias("_test_snomed_code"), s.description.alias("_test_snomed_display"),
        s.test_loinc_code.cast("string").alias("_test_loinc_code"), s.description.alias("_test_loinc_display"),
        s.test_omop_concept_id.cast("string").alias("_test_omop_code"),
        s.measurement_concept_name.alias("_test_omop_display"),
        s.result_snomed_code.cast("string").alias("_result_snomed_code"),
        s.result_concept_name.alias("_result_snomed_display"),
        s.result_loinc_code.cast("string").alias("_result_loinc_code"),
        s.result_concept_name.alias("_result_loinc_display"),
        s.result_omop_concept_id.cast("string").alias("_result_omop_code"),
        s.result_concept_name.alias("_result_omop_display"),
        s.result_confidence_tier.alias("_result_confidence_tier"),
    )

def _pathology_result_canonical(source_df=None, include_parent_links=False):
    # Assemble normalized pathology result rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    return _pathology_result_canonical_pregate(
        source_df, include_parent_links
    ).where(_usable_code(F.col("source_code")))


SRC_PATHOLOGY = "4_prod.bronze.map_pathology"

RESULT_PRIMITIVE_COLUMNS = [
    "_result_code_json" if c == "result_code" else c for c in RESULT_SOURCE_COLUMNS
]




SRC_PATHOLOGY_REQUESTED_TEST = "4_prod.bronze.map_pathology_requested_test"

def _pathology_requested_test_canonical_pregate():
    # Normalize pathology requested test source rows before the downstream admission filter so
    # excluded evidence remains countable.
    t = _alias_resolved_accession(read_source(SRC_PATHOLOGY_REQUESTED_TEST))
    i = spark.read.table(_n("journey_events._pathology_accession_identity")).select(
        F.col("pathology_accession_id").alias("_resolved_accession_id"),
        "canonical_person_id", "person_resolution_status", "evidence_mrn", "evidence_nhs",
        F.col("request_dt").alias("_acc_request_dt"),
        F.col("sample_dt").alias("_acc_sample_dt"),
    )
    j = t.join(i, "_resolved_accession_id", "left")
    event_id = stable_id(
        "pathology_order:requested_test", F.col("requested_test_occurrence_id")
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("canonical_person_id")),
         ("urn:barts:mrn", F.col("evidence_mrn")),
         ("https://fhir.nhs.uk/Id/nhs-number", F.col("evidence_nhs"))],
        SRC_PATHOLOGY_REQUESTED_TEST, F.col("requested_test_occurrence_id"),
    )
    source_code = F.coalesce(F.col("wkg_code"), F.col("order_mnemonic"))
    system_uri = (
        F.when(F.col("source_system") == "TFC_LIMS", F.lit("urn:barts:pathology:wkg-tlc"))
        .otherwise(F.lit("urn:cerner:order-mnemonic"))
    )
    # contract v2: publish source_object, CERNER order_id when present, BIGINT identity columns, and key-named hashes
    return j.select(
        event_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("canonical_person_id").cast("bigint").alias("person_id"),
        F.when(F.col("person_resolution_status") == "eligible", F.lit("resolved"))
         .when(F.col("person_resolution_status") == "conflicting", F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        _clamped_ts(F.coalesce("_acc_request_dt", "_acc_sample_dt")).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        system_uri.alias("source_coding_system"),
        source_code.cast("string").alias("source_code"),
        F.coalesce(F.col("test_description"), source_code).cast("string").alias("source_display"),
        F.lower(F.col("source_system")).alias("source_object"),
        F.col("wkg_code"), F.col("tlc_code"),
        F.col("order_id").cast("bigint").alias("order_id"), F.col("order_mnemonic"),
        F.col("raw_request_text"), F.col("test_description"),
        F.col("test_snomed_code").cast("string").alias("test_snomed_code"),
        F.col("test_omop_concept_id").cast("string").alias("test_omop_concept_id"),
        F.col("mapping_status"), F.col("request_ordinal").cast("long").alias("request_ordinal"),
        stable_id("specimen:accession", F.col("_resolved_accession_id")).alias("specimen_key"),
        F.lit("active").alias("record_status"),
        F.lit(None).cast("timestamp").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.date_format("ADC_UPDT", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.lit(None).cast("timestamp").alias("source_update_timestamp"),
        F.col("ADC_UPDT").alias("loaded_at"),
        F.lit("laboratory").alias("_source_system"),
        F.lit(SRC_PATHOLOGY_REQUESTED_TEST).alias("_source_table"),
        F.col("requested_test_occurrence_id").alias("_source_row_id"),
        F.col("test_snomed_code").cast("string").alias("_test_snomed_code"),
        F.coalesce(F.col("test_description"), source_code).alias("_test_snomed_display"),
        F.col("test_omop_concept_id").cast("string").alias("_test_omop_code"),
        F.coalesce(F.col("test_description"), source_code).alias("_test_omop_display"),
    )

def _pathology_requested_test_canonical():
    # Keep the normalized pathology requested test rows that pass the existing source-code
    # admission rule.
    return _pathology_requested_test_canonical_pregate().where(
        _usable_code(F.col("source_code"))
    )




SRC_PATHOLOGY_GENETIC_TEST = "4_prod.bronze.map_pathology_genetic_test"

def _genomic_identity_stage():
    # Read accession-level patient-resolution evidence and rename its fields for joining genomic
    # facts.
    return spark.read.table(_n("journey_events._pathology_accession_identity")).select(
        F.col("pathology_accession_id").alias("_identity_accession_id"),
        "canonical_person_id", "person_resolution_status", "evidence_mrn", "evidence_nhs",
        F.col("request_dt").alias("_acc_request_dt"),
        F.col("sample_dt").alias("_acc_sample_dt"),
        F.col("report_dt").alias("_acc_report_dt"),
    )

def _identity_mapped_columns():
    # contract v2: type the shared pathology identity projection as BIGINT person_id
    # Translate accession identity evidence into the canonical person identifier and resolution
    # status.
    return [
        F.col("canonical_person_id").cast("bigint").alias("person_id"),
        F.when(F.col("person_resolution_status") == "eligible", F.lit("resolved"))
         .when(F.col("person_resolution_status") == "conflicting", F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
    ]

def _genomic_test_canonical_pregate():
    # Normalize genomic test source rows before the downstream admission filter so excluded
    # evidence remains countable.
    t = _alias_resolved_accession(read_source(SRC_PATHOLOGY_GENETIC_TEST)).alias("t")
    i = _genomic_identity_stage().alias("i")
    r = read_source(SRC_PATHOLOGY_REPORT_VERSIONS).select(
        F.col("report_version_id").alias("_rv_id"),
        F.col("report_series_id").alias("_rv_series_id"),
        F.col("issued_dt").alias("_rv_issued_dt"),
    ).alias("r")
    j = (
        t.join(i, F.col("t._resolved_accession_id") == F.col("i._identity_accession_id"), "left")
        .join(r, F.col("t.report_version_id") == F.col("r._rv_id"), "left")
    )
    event_id = stable_id("genomic_test:assay", F.col("t.genetic_test_id"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("canonical_person_id")),
         ("urn:barts:mrn", F.col("evidence_mrn")),
         ("https://fhir.nhs.uk/Id/nhs-number", F.col("evidence_nhs"))],
        SRC_PATHOLOGY_GENETIC_TEST, F.col("t.genetic_test_id"),
    )
    superseded = ~F.coalesce(F.col("t.is_current"), F.lit(True))
    # contract v2: publish genetic_test_id and key-named report/specimen relationships
    return j.select(
        event_id.alias("patient_event_key"),
        F.col("t.genetic_test_id").cast("string").alias("genetic_test_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        *_identity_mapped_columns(),
        F.lit(None).cast("bigint").alias("encounter_id"),
        _clamped_ts(F.coalesce(F.col("r._rv_issued_dt"), F.col("_acc_sample_dt"),
                               F.col("_acc_request_dt"))).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:barts:pathology:assay-code").alias("source_coding_system"),
        F.col("t.assay_code").cast("string").alias("source_code"),
        F.coalesce(F.col("t.assay_name"), F.col("t.assay_code")).alias("source_display"),
        F.col("t.assay_code"), F.col("t.assay_name"), F.col("t.method"),
        F.col("t.analysis_context"), F.col("t.overall_result_status"),
        F.col("t.panel_code"), F.col("t.panel_version"), F.col("t.panel_version_inferred"),
        F.col("t.parser_profile_id"), F.col("t.report_version_id").alias("report_version_key"),
        F.when(F.col("r._rv_series_id").isNotNull(),
               stable_id("pathology_report:series", F.col("r._rv_series_id")))
         .alias("pathology_report_key"),
        F.when(F.col("t._resolved_accession_id").isNotNull(),
               stable_id("specimen:accession", F.col("t._resolved_accession_id")))
         .alias("specimen_key"),
        F.col("t._resolved_accession_id").alias("accession_identifier"),
        F.col("t.test_snomed_code").cast("string").alias("test_snomed_code"),
        F.col("t.test_loinc_code").cast("string").alias("test_loinc_code"),
        F.col("t.test_omop_concept_id").cast("string").alias("test_omop_concept_id"),
        F.col("t.is_current"), F.col("t.research_qi_only"),
        F.when(superseded, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        F.lit(None).cast("timestamp").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("pathology_genetic_test").alias("source_feed"),
        F.date_format(F.col("t.ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("t.ADC_UPDT").alias("source_update_timestamp"),
        F.col("t.ADC_UPDT").alias("loaded_at"),
        F.lit("laboratory").alias("_source_system"),
        F.lit(SRC_PATHOLOGY_GENETIC_TEST).alias("_source_table"),
        F.col("t.genetic_test_id").alias("_source_row_id"),
        F.col("t.test_snomed_code").cast("string").alias("_gt_snomed_code"),
        F.coalesce(F.col("t.assay_name"), F.col("t.assay_code")).alias("_gt_snomed_display"),
        F.col("t.test_loinc_code").cast("string").alias("_gt_loinc_code"),
        F.coalesce(F.col("t.assay_name"), F.col("t.assay_code")).alias("_gt_loinc_display"),
        F.col("t.test_omop_concept_id").cast("string").alias("_gt_omop_code"),
        F.coalesce(F.col("t.assay_name"), F.col("t.assay_code")).alias("_gt_omop_display"),
    )

def _genomic_test_canonical():
    # Keep the normalized genomic test rows that pass the existing source-code admission rule.
    return _genomic_test_canonical_pregate().where(_usable_code(F.col("source_code")))




def _genomic_result_canonical_pregate():
    # Normalize genomic result source rows before the downstream admission filter so excluded
    # evidence remains countable.
    g = read_source(SRC_PATHOLOGY_GENETIC_RESULT).alias("g")
    parent = _alias_resolved_accession(
        read_source(SRC_PATHOLOGY_GENETIC_TEST)
    ).select(
        F.col("genetic_test_id").alias("_gt_id"),
        F.col("_resolved_accession_id"),
    ).alias("p")
    i = _genomic_identity_stage().alias("i")
    r = read_source(SRC_PATHOLOGY_REPORT_VERSIONS).select(
        F.col("report_version_id").alias("_rv_id"),
        F.col("issued_dt").alias("_rv_issued_dt"),
    ).alias("r")
    j = (
        g.join(parent, F.col("g.genetic_test_id") == F.col("p._gt_id"), "left")
        .join(i, F.col("p._resolved_accession_id") == F.col("i._identity_accession_id"), "left")
        .join(r, F.col("g.report_version_id") == F.col("r._rv_id"), "left")
    )
    event_id = stable_id("genomic_result:finding", F.col("g.genetic_result_id"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("canonical_person_id")),
         ("urn:barts:mrn", F.col("evidence_mrn")),
         ("https://fhir.nhs.uk/Id/nhs-number", F.col("evidence_nhs"))],
        SRC_PATHOLOGY_GENETIC_RESULT, F.col("g.genetic_result_id"),
    )
    retracted = F.col("g.lifecycle_status").isin("cancelled", "entered_in_error")
    superseded = ~F.coalesce(F.col("g.is_current"), F.lit(True))
    # contract v2: publish genetic_result_id and key-named report/specimen relationships with BIGINT encounter type
    return j.select(
        event_id.alias("patient_event_key"),
        F.col("g.genetic_result_id").cast("string").alias("genetic_result_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        *_identity_mapped_columns(),
        F.lit(None).cast("bigint").alias("encounter_id"),
        _clamped_ts(F.coalesce(F.col("r._rv_issued_dt"), F.col("_acc_sample_dt"),
                               F.col("_acc_request_dt"))).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.when(F.coalesce(F.col("g.normalized_gene_symbol"),
                          F.col("g.reported_gene_symbol")).isNotNull(),
               F.lit("urn:barts:pathology:gene-symbol"))
         .otherwise(F.lit("urn:barts:pathology:alteration-type"))
         .alias("source_coding_system"),
        F.coalesce(F.col("g.normalized_gene_symbol"), F.col("g.reported_gene_symbol"),
                   F.col("g.alteration_type"), F.col("g.detection_status"))
         .cast("string").alias("source_code"),
        F.coalesce(F.col("g.reported_gene_symbol"), F.col("g.normalized_gene_symbol"),
                   F.col("g.alteration_type"), F.col("g.detection_status"))
         .alias("source_display"),
        F.when(F.col("g.genetic_test_id").isNotNull(),
               stable_id("genomic_test:assay", F.col("g.genetic_test_id")))
         .alias("genomic_test_key"),
        F.col("g.report_version_id").alias("report_version_key"),
        F.col("g.hgnc_id"), F.col("g.reported_gene_symbol"), F.col("g.normalized_gene_symbol"),
        F.col("g.partner_hgnc_id"), F.col("g.partner_gene_symbol"),
        F.col("g.alteration_type"), F.col("g.detection_status"),
        F.col("g.hgvs_c_raw"), F.col("g.hgvs_c_parsed"),
        F.col("g.hgvs_p_raw"), F.col("g.hgvs_p_parsed"),
        F.col("g.transcript"), F.col("g.hgvs_validation_status"),
        F.col("g.genome_build"), F.col("g.chromosome"),
        F.col("g.position_start"), F.col("g.position_end"),
        F.col("g.vaf_raw"), F.col("g.vaf"), F.col("g.zygosity"),
        F.col("g.reported_classification"), F.col("g.reported_tier"),
        F.col("g.copy_number"), F.col("g.ratio_raw"), F.col("g.iscn_raw"),
        F.col("g.clinvar_concept_id").cast("string").alias("clinvar_concept_id"),
        F.col("g.omop_genomic_concept_id").cast("string").alias("omop_genomic_concept_id"),
        F.col("g.snomed_code").cast("string").alias("snomed_code"),
        F.col("g.evidence_text"), F.col("g.evidence_start"), F.col("g.evidence_end"),
        F.col("g.parser_profile_id"), F.col("g.parser_version"),
        F.col("g.review_status"), F.col("g.lifecycle_status"),
        F.col("g.is_current"), F.col("g.research_qi_only"),
        F.when(F.col("p._resolved_accession_id").isNotNull(),
               stable_id("specimen:accession", F.col("p._resolved_accession_id")))
         .alias("specimen_key"),
        F.col("p._resolved_accession_id").alias("accession_identifier"),
        F.when(retracted, F.lit("retracted"))
         .when(superseded, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        F.lit(None).cast("timestamp").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("pathology_genetic_result").alias("source_feed"),
        F.date_format(F.col("g.ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("g.ADC_UPDT").alias("source_update_timestamp"),
        F.col("g.ADC_UPDT").alias("loaded_at"),
        F.lit("laboratory").alias("_source_system"),
        F.lit(SRC_PATHOLOGY_GENETIC_RESULT).alias("_source_table"),
        F.col("g.genetic_result_id").alias("_source_row_id"),
        F.col("g.snomed_code").cast("string").alias("_gr_snomed_code"),
        F.coalesce(F.col("g.reported_classification"), F.col("g.alteration_type"))
         .alias("_gr_snomed_display"),
        F.col("g.clinvar_concept_id").cast("string").alias("_gr_clinvar_code"),
        F.col("g.reported_classification").alias("_gr_clinvar_display"),
        F.col("g.omop_genomic_concept_id").cast("string").alias("_gr_omop_code"),
        F.col("g.reported_classification").alias("_gr_omop_display"),
    )

def _genomic_result_canonical():
    # Keep the normalized genomic result rows that pass the existing source-code admission rule.
    return _genomic_result_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_PATHOLOGY_GENETIC_RESULT = "4_prod.bronze.map_pathology_genetic_result"




def _indication_canonical_pregate():
    # Normalize indication source rows before the downstream admission filter so excluded
    # evidence remains countable.
    x = _alias_resolved_accession(read_source(SRC_PATHOLOGY_INDICATION)).alias("x")
    i = _genomic_identity_stage().alias("i")
    j = x.join(i, F.col("x._resolved_accession_id") == F.col("i._identity_accession_id"), "left")
    event_id = stable_id("indication:evidence", F.col("x.indication_id"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("canonical_person_id")),
         ("urn:barts:mrn", F.col("evidence_mrn")),
         ("https://fhir.nhs.uk/Id/nhs-number", F.col("evidence_nhs"))],
        SRC_PATHOLOGY_INDICATION, F.col("x.indication_id"),
    )
    # contract v2: publish indication_id and specimen_key with BIGINT encounter type
    return j.select(
        event_id.alias("patient_event_key"),
        F.col("x.indication_id").cast("string").alias("indication_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        *_identity_mapped_columns(),
        F.lit(None).cast("bigint").alias("encounter_id"),
        _clamped_ts(F.coalesce(F.col("_acc_report_dt"), F.col("_acc_sample_dt"),
                               F.col("_acc_request_dt"))).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:barts:pathology:indication-text").alias("source_coding_system"),
        F.col("x.source_text").cast("string").alias("source_code"),
        F.col("x.source_text").alias("source_display"),
        F.col("x.relation_type"), F.col("x.source_field"), F.col("x.source_text"),
        F.col("x.evidence_text"), F.col("x.evidence_start"), F.col("x.evidence_end"),
        F.col("x.snomed_code").cast("string").alias("snomed_code"),
        F.col("x.snomed_term"),
        F.col("x.omop_concept_id").cast("string").alias("omop_concept_id"),
        F.col("x.assertion"), F.col("x.temporality"), F.col("x.experiencer"),
        F.col("x.rule_id"), F.col("x.rule_version"), F.col("x.confidence"),
        F.col("x.mapping_status"), F.col("x.ig_release_status"),
        F.col("x.is_current"), F.col("x.research_qi_only"),
        F.when(F.col("x._resolved_accession_id").isNotNull(),
               stable_id("specimen:accession", F.col("x._resolved_accession_id")))
         .alias("specimen_key"),
        F.col("x._resolved_accession_id").alias("accession_identifier"),
        F.lit("active").alias("record_status"),
        F.lit(None).cast("timestamp").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("pathology_indication").alias("source_feed"),
        F.date_format(F.col("x.ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("x.ADC_UPDT").alias("source_update_timestamp"),
        F.col("x.ADC_UPDT").alias("loaded_at"),
        F.lit("laboratory").alias("_source_system"),
        F.lit(SRC_PATHOLOGY_INDICATION).alias("_source_table"),
        F.col("x.indication_id").alias("_source_row_id"),
        F.col("x.snomed_code").cast("string").alias("_ind_snomed_code"),
        F.coalesce(F.col("x.snomed_term"), F.col("x.source_text")).alias("_ind_snomed_display"),
        F.col("x.omop_concept_id").cast("string").alias("_ind_omop_code"),
        F.coalesce(F.col("x.snomed_term"), F.col("x.source_text")).alias("_ind_omop_display"),
    )

def _indication_canonical():
    # Keep the normalized indication rows that pass the existing source-code admission rule.
    return _indication_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_PATHOLOGY_INDICATION = "4_prod.bronze.map_pathology_indication"




def _micro_isolate_canonical_pregate():
    # Normalize micro isolate source rows before the downstream admission filter so excluded
    # evidence remains countable.
    m = _alias_resolved_accession(read_source(SRC_PATHOLOGY_MICRO_ISOLATE)).alias("m")
    i = _genomic_identity_stage().alias("i")
    j = m.join(i, F.col("m._resolved_accession_id") == F.col("i._identity_accession_id"), "left")
    event_id = stable_id("microbiology_isolate:isolate", F.col("m.microbiology_isolate_id"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("canonical_person_id")),
         ("urn:barts:mrn", F.col("evidence_mrn")),
         ("https://fhir.nhs.uk/Id/nhs-number", F.col("evidence_nhs"))],
        SRC_PATHOLOGY_MICRO_ISOLATE, F.col("m.microbiology_isolate_id"),
    )
    retracted = F.col("m.lifecycle_status").isin("cancelled", "entered_in_error")
    superseded = ~F.coalesce(F.col("m.is_current"), F.lit(True))
    # contract v2: publish microbiology_isolate_id and key-named report/specimen relationships
    return j.select(
        event_id.alias("patient_event_key"),
        F.col("m.microbiology_isolate_id").cast("string").alias("microbiology_isolate_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        *_identity_mapped_columns(),
        F.lit(None).cast("bigint").alias("encounter_id"),
        _clamped_ts(F.coalesce(F.col("_acc_sample_dt"), F.col("_acc_report_dt"),
                               F.col("_acc_request_dt"))).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.concat(F.lit("urn:barts:winpath:lims"), F.col("m.lims_no"),
                 F.lit(":organism-code")).alias("source_coding_system"),
        F.col("m.organism_code").alias("source_code"),
        F.col("m.organism_text").alias("source_display"),
        F.when(F.col("m.source_record_key").isNotNull(),
               stable_id("pathology_result:pathology", F.col("m.source_record_key")))
         .alias("pathology_result_id"),
        F.col("m.report_version_id").alias("report_version_key"), F.col("m.specimen_type_code"), F.col("m.organism_text"),
        F.col("m.organism_snomed_code").cast("string").alias("organism_snomed_code"),
        F.col("m.organism_omop_concept_id").cast("string").alias("organism_omop_concept_id"),
        F.col("m.suspected_ind"), F.col("m.growth_grade"),
        F.col("m.organism_code"), F.col("m.panel_code"),
        F.col("m.isolate_ordinal").cast("int").alias("isolate_ordinal"),
        F.col("m.isolate_comment"), F.col("m.lims_no").cast("int").alias("lims_no"),
        F.col("m.parse_status"), F.col("m.parser_version"),
        F.col("m.lifecycle_status"), F.col("m.is_current"), F.col("m.research_qi_only"),
        F.when(F.col("m._resolved_accession_id").isNotNull(),
               stable_id("specimen:accession", F.col("m._resolved_accession_id")))
         .alias("specimen_key"),
        F.col("m._resolved_accession_id").alias("accession_identifier"),
        F.when(retracted, F.lit("retracted"))
         .when(superseded, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        F.lit(None).cast("timestamp").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("pathology_microbiology_isolate").alias("source_feed"),
        F.date_format(F.col("m.ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("m.ADC_UPDT").alias("source_update_timestamp"),
        F.col("m.ADC_UPDT").alias("loaded_at"),
        F.lit("laboratory").alias("_source_system"),
        F.lit(SRC_PATHOLOGY_MICRO_ISOLATE).alias("_source_table"),
        F.col("m.microbiology_isolate_id").alias("_source_row_id"),
        F.col("m.organism_snomed_code").cast("string").alias("_iso_snomed_code"),
        F.col("m.organism_text").alias("_iso_snomed_display"),
        F.col("m.organism_omop_concept_id").cast("string").alias("_iso_omop_code"),
        F.col("m.organism_text").alias("_iso_omop_display"),
    )

def _micro_isolate_canonical():
    # Keep the normalized micro isolate rows that pass the existing source-code admission rule.
    return _micro_isolate_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_PATHOLOGY_MICRO_ISOLATE = "4_prod.bronze.map_pathology_microbiology_isolate"




def _susceptibility_canonical_pregate():
    # Normalize susceptibility source rows before the downstream admission filter so excluded
    # evidence remains countable.
    a = _alias_resolved_accession(
        read_source(SRC_PATHOLOGY_ANTIMICROBIAL_SUSCEPTIBILITY)
    ).alias("a")
    g = _genomic_identity_stage().alias("g")
    i = (read_source(SRC_PATHOLOGY_MICRO_ISOLATE)
         .select("microbiology_isolate_id", "lims_no").alias("i"))
    j = (a.join(g, F.col("a._resolved_accession_id") == F.col("g._identity_accession_id"), "left")
         .join(i, F.col("a.microbiology_isolate_id") == F.col("i.microbiology_isolate_id"), "left"))
    event_id = stable_id(
        "susceptibility_result:observation", F.col("a.susceptibility_result_id")
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("canonical_person_id")),
         ("urn:barts:mrn", F.col("evidence_mrn")),
         ("https://fhir.nhs.uk/Id/nhs-number", F.col("evidence_nhs"))],
        SRC_PATHOLOGY_ANTIMICROBIAL_SUSCEPTIBILITY,
        F.col("a.susceptibility_result_id"),
    )
    retracted = F.col("a.lifecycle_status").isin("cancelled", "entered_in_error")
    superseded = ~F.coalesce(F.col("a.is_current"), F.lit(True))
    # contract v2: publish susceptibility_result_id and specimen_key with BIGINT encounter type
    return j.select(
        event_id.alias("patient_event_key"),
        F.col("a.susceptibility_result_id").cast("string").alias("susceptibility_result_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        *_identity_mapped_columns(),
        F.lit(None).cast("bigint").alias("encounter_id"),
        _clamped_ts(F.coalesce(F.col("_acc_sample_dt"), F.col("_acc_report_dt"),
                               F.col("_acc_request_dt"))).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.concat(F.lit("urn:barts:winpath:lims"), F.col("i.lims_no"),
                 F.lit(":agent-code")).alias("source_coding_system"),
        F.col("a.antimicrobial_code").cast("string").alias("source_code"),
        F.col("a.antimicrobial_text").alias("source_display"),
        F.col("a.microbiology_isolate_id").cast("string").alias("microbiology_isolate_id"),
        stable_id("microbiology_isolate:isolate", F.col("a.microbiology_isolate_id"))
         .alias("isolate_event_key"),
        F.when(F.col("a.source_record_key").isNotNull(),
               stable_id("pathology_result:pathology", F.col("a.source_record_key")))
         .alias("pathology_result_id"),
        F.col("a.link_status"), F.col("a.antimicrobial_text"),
        F.col("a.antimicrobial_code"),
        F.col("a.antimicrobial_omop_concept_id").cast("string")
         .alias("antimicrobial_omop_concept_id"),
        F.col("a.interpretation_raw"), F.col("a.interpretation"),
        F.col("a.mic_raw"), F.col("a.mic"), F.col("a.unit_source_value"), F.col("a.method"),
        F.col("a.token_class"), F.col("a.token_ordinal").cast("int").alias("token_ordinal"),
        F.col("a.parser_version"),
        F.col("a.lifecycle_status"), F.col("a.is_current"), F.col("a.research_qi_only"),
        F.when(F.col("a._resolved_accession_id").isNotNull(),
               stable_id("specimen:accession", F.col("a._resolved_accession_id")))
         .alias("specimen_key"),
        F.col("a._resolved_accession_id").alias("accession_identifier"),
        F.when(retracted, F.lit("retracted"))
         .when(superseded, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        F.lit(None).cast("timestamp").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("pathology_antimicrobial_susceptibility").alias("source_feed"),
        F.date_format(F.col("a.ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("a.ADC_UPDT").alias("source_update_timestamp"),
        F.col("a.ADC_UPDT").alias("loaded_at"),
        F.lit("laboratory").alias("_source_system"),
        F.lit(SRC_PATHOLOGY_ANTIMICROBIAL_SUSCEPTIBILITY).alias("_source_table"),
        F.col("a.susceptibility_result_id").alias("_source_row_id"),
        F.col("a.antimicrobial_code").cast("string").alias("_sus_code_code"),
        F.col("a.antimicrobial_text").alias("_sus_code_display"),
        F.col("a.antimicrobial_omop_concept_id").cast("string").alias("_sus_omop_code"),
        F.col("a.antimicrobial_text").alias("_sus_omop_display"),
    )

def _susceptibility_canonical():
    # Keep the normalized susceptibility rows that pass the existing source-code admission rule.
    return _susceptibility_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_PATHOLOGY_ANTIMICROBIAL_SUSCEPTIBILITY = "4_prod.bronze.map_pathology_antimicrobial_susceptibility"




def _form_canonical_pregate():
    # Normalize form source rows before the downstream admission filter so excluded evidence
    # remains countable.
    grouped = spark.read.table(_n("journey_clinical._form_grouped"))
    event_id = stable_id("form:mill:powerform", F.col("DCP_FORMS_ACTIVITY_ID"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("PERSON_ID"))],
        SRC_FORM_ACTIVITY, F.col("DCP_FORMS_ACTIVITY_ID"),
    )
    retracted = F.coalesce(F.col("source_present_int") == 0, F.lit(False))
    superseded = F.coalesce(F.col("assessment_active_int") == 0, F.lit(False)) | (
        F.col("active_response_row_count") == 0
    )
    loaded_at = F.greatest(
        "activity_loaded_at", "item_loaded_at", "assessment_loaded_at", "vte_loaded_at"
    )
    # contract v2: publish the native PowerForm id and native relationship identifiers while retaining the event SHA as patient_event_key
    return grouped.select(
        event_id.alias("patient_event_key"),
        F.col("DCP_FORMS_ACTIVITY_ID").cast("bigint").alias("dcp_forms_activity_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("PERSON_ID").cast("bigint").alias("person_id"),
        F.when(_present(F.col("PERSON_ID")), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.col("ENCNTR_ID").cast("bigint").alias("encounter_id"),
        F.col("authored_datetime").alias("event_datetime"),
        F.col("completed_datetime").alias("event_end_datetime"),
        F.lit("urn:cerner:form_ref_id").alias("source_coding_system"),
        F.col("FORM_REF_ID").cast("string").alias("source_code"),
        F.col("FORM_DESC_TXT").alias("source_display"),
        F.col("FORM_REF_ID").cast("string").alias("form_type_code"),
        F.col("FORM_DESC_TXT").alias("form_type_display"),
        F.col("FORM_STATUS_CD").cast("string").alias("form_status_code"),
        F.col("STATUS").alias("form_status_display"),
        F.col("authored_datetime"), F.col("completed_datetime"),
        F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("performed_practitioner_id"),
        F.col("ORGANIZATION_ID").cast("bigint").alias("organization_id"),
        F.col("responses"),
        F.col("response_row_count"), F.col("active_response_row_count"),
        F.col("empty_response_row_count"), F.coalesce("invalid_response_row_count", F.lit(0)).alias("invalid_response_row_count"),
        F.coalesce("matched_response_row_count", F.lit(0)).alias("matched_response_row_count"),
        F.coalesce("unmatched_response_row_count", F.lit(0)).alias("unmatched_response_row_count"),
        (F.coalesce("context_conflict_int", F.lit(0)) != 0).alias("context_conflict_ind"),
        (F.coalesce("context_quarantined_int", F.lit(0)) != 0).alias("context_quarantined_ind"),
        F.when(retracted, F.lit("retracted"))
         .when(superseded, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        F.col("authored_datetime").alias("record_status_effective_from"),
        F.when(retracted | superseded, F.col("completed_datetime")).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("powerform").alias("source_feed"),
        F.date_format(loaded_at, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("source_update_timestamp"), loaded_at.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.lit(SRC_FORM_ACTIVITY).alias("_source_table"),
        F.col("DCP_FORMS_ACTIVITY_ID").cast("string").alias("_source_row_id"),
    )

def _form_canonical():
    # Keep the normalized form rows that pass the existing source-code admission rule.
    return _form_canonical_pregate().where(_usable_code(F.col("source_code")))




def _numeric_route_expr(s):
    # Classify numeric events as scores, vital signs or residual findings using the existing
    # label rules.
    label = F.lower(F.coalesce(
        s.EVENT_LABEL, s.EVENT_CD_DISPLAY, s.OMOP_MANUAL_CONCEPT_NAME, F.lit("")
    ))
    score = label.rlike("(news|ews|score|scale|glasgow|gcs|braden|waterlow|morse|frailty|risk)")
    vital = label.rlike(
        "^(systolic blood pressure|diastolic blood pressure|mean arterial pressure.*|"
        "respiratory rate|heart rate.*|peripheral pulse rate|pulse rate|spo2|"
        "oxygen saturation.*|temperature .*|weight|height|body mass index|bmi|"
        "capillary refill time actual)$"
    )
    return F.when(score, F.lit("clinical_score")).when(vital, F.lit("vital_sign")).otherwise(F.lit("excluded"))

def _registry_vital_canonical():
    # Assemble normalized registry vital rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = spark.read.table(_n("journey_clinical._registry_observations")).where("route='measurement'")
    return s.select(
        "patient_event_key", F.lit(None).cast("bigint").alias("event_id"),
        F.lit(None).cast("string").alias("doc_response_key"), F.lit(None).cast("bigint").alias("source_dcp_forms_activity_id"),
        F.concat(F.lit("registry:"), F.col("registry_product")).alias("source_object"),
        "subject_key", "subject_id_system", "person_id",
        F.when(F.col("person_id").isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved")).alias("identity_status"),
        "encounter_id", "event_datetime", "event_end_datetime",
        F.lit("urn:barts:registry-field").alias("source_coding_system"), F.col("registry_field_id").alias("source_code"),
        F.col("question_display").alias("source_display"),
        codeable_concept(coding_obj(F.lit("urn:barts:registry-field"), F.col("registry_field_id"), F.col("question_display"), True)).alias("vital_code"),
        "value_number", F.col("answer_text").alias("value_text"), F.col("unit").alias("unit_source_value"),
        F.lit(None).cast("string").alias("unit_concept_id"), F.lit(None).cast("decimal(38,10)").alias("reference_range_low"),
        F.lit(None).cast("decimal(38,10)").alias("reference_range_high"), F.lit(None).cast("string").alias("method_code"),
        F.lit(None).cast("string").alias("method_display"), F.lit(None).cast("string").alias("body_site_code"),
        F.lit(None).cast("string").alias("body_site_display"), F.lit(None).cast("string").alias("interpretation_code"),
        F.lit(None).cast("string").alias("interpretation_display"), F.lit(None).cast("string").alias("result_status_code"),
        F.lit(None).cast("string").alias("result_status_display"), F.lit(None).cast("bigint").alias("performer_practitioner_id"),
        F.lit(None).cast("string").alias("source_form_key"), F.lit("registry_field_route:s3b").alias("promotion_rule_id"),
        "record_status", "record_status_effective_from", "record_status_effective_to",
        F.lit(None).cast("string").alias("confidentiality_code"), F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"), F.lit("iweb_registry").alias("source_feed"),
        F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"), "source_update_timestamp", "loaded_at",
        F.lit("iweb").alias("_source_system"), F.concat(F.lit("registry:"), F.col("registry_product")).alias("_source_table"),
        F.concat_ws(":", "registry_entry_key", "registry_field").alias("_source_row_id"),
        F.lit(None).cast("string").alias("_omop_code"), F.lit(None).cast("string").alias("_omop_display"),
        "registry_field_id", "event_datetime_status",
    )

# contract v2: retain the full canonical shape for internal reuse.
VITAL_SOURCE_COLUMNS = [
    "patient_event_key",
    "event_id",
    "doc_response_key",
    "source_dcp_forms_activity_id",
    "source_object",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "vital_code",
    "value_number",
    "value_text",
    "unit_source_value",
    "unit_concept_id",
    "reference_range_low",
    "reference_range_high",
    "method_code",
    "method_display",
    "body_site_code",
    "body_site_display",
    "interpretation_code",
    "interpretation_display",
    "result_status_code",
    "result_status_display",
    "performer_practitioner_id",
    "source_form_key",
    "promotion_rule_id",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

VITAL_STAGE_COLUMNS = VITAL_SOURCE_COLUMNS + [
    "_source_system", "_source_table", "_source_row_id", "_omop_code", "_omop_display",
]

def _numeric_vital_canonical():
    # Assemble normalized numeric vital rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_NUMERIC_EVENTS)
    s = s.where(_numeric_route_expr(s) == "vital_sign")
    event_id = stable_id("numeric_event:mill", s.EVENT_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_NUMERIC_EVENTS, s.EVENT_ID
    )
    deleted = F.coalesce(s.SOURCE_DELETED_IND, F.lit(False))
    ended = s.CLINICAL_EVENT_VALID_UNTIL_DT_TM.isNotNull() & (
        s.CLINICAL_EVENT_VALID_UNTIL_DT_TM < F.lit("2100-01-01").cast("timestamp")
    )
    source_update = F.greatest(
        s.STRING_RESULT_UPDT_DT_TM, s.CLINICAL_EVENT_UPDT_DT_TM, s.ADC_UPDT
    )
    # contract v2: publish native numeric-event identifiers and native foreign keys for the vital numeric arm
    return s.select(
        event_id.alias("patient_event_key"),
        s.EVENT_ID.cast("bigint").alias("event_id"),
        F.lit(None).cast("string").alias("doc_response_key"),
        F.lit(None).cast("bigint").alias("source_dcp_forms_activity_id"),
        F.lit("numeric_event").alias("source_object"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(_present(s.PERSON_ID), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        F.coalesce(s.PERFORMED_DT_TM, s.EVENT_START_DT_TM).alias("event_datetime"),
        s.EVENT_END_DT_TM.alias("event_end_datetime"),
        F.lit("urn:cerner:event_cd").alias("source_coding_system"),
        s.EVENT_CD.cast("string").alias("source_code"),
        F.coalesce(s.EVENT_LABEL, s.EVENT_CD_DISPLAY).alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:cerner:event_cd"), s.EVENT_CD,
                       F.coalesce(s.EVENT_LABEL, s.EVENT_CD_DISPLAY), True),
            coding_obj(F.lit("urn:omop:concept_id"), s.OMOP_MANUAL_CONCEPT_ID,
                       s.OMOP_MANUAL_CONCEPT_NAME, False, "bronze.map_numeric_events", None),
        ).alias("vital_code"),
        s.NUMERIC_RESULT.cast("decimal(38,10)").alias("value_number"),
        s.RESULT_TEXT_EFFECTIVE.alias("value_text"),
        F.coalesce(s.UNIT_OF_MEASURE_DISPLAY, s.RESULT_UNITS_DISPLAY).alias("unit_source_value"),
        s.OMOP_MANUAL_UNITS.cast("string").alias("unit_concept_id"),
        s.NORMAL_LOW.cast("decimal(38,10)").alias("reference_range_low"),
        s.NORMAL_HIGH.cast("decimal(38,10)").alias("reference_range_high"),
        s.ENTRY_MODE_CD.cast("string").alias("method_code"), s.ENTRY_MODE_DISPLAY.alias("method_display"),
        F.lit(None).cast("string").alias("body_site_code"),
        F.lit(None).cast("string").alias("body_site_display"),
        s.NORMALCY_CD.cast("string").alias("interpretation_code"),
        s.NORMALCY_DISPLAY.alias("interpretation_display"),
        s.RESULT_STATUS_CD.cast("string").alias("result_status_code"),
        s.RESULT_STATUS_DISPLAY.alias("result_status_display"),
        s.PERFORMED_PRSNL_ID.cast("bigint").alias("performer_practitioner_id"),
        F.lit(None).cast("string").alias("source_form_key"),
        F.lit(None).cast("string").alias("promotion_rule_id"),
        F.when(deleted, F.lit("retracted")).when(ended, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        s.CLINICAL_EVENT_VALID_FROM_DT_TM.alias("record_status_effective_from"),
        F.when(deleted | ended, s.CLINICAL_EVENT_VALID_UNTIL_DT_TM).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("numeric_event").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_NUMERIC_EVENTS).alias("_source_table"),
        s.EVENT_ID.cast("string").alias("_source_row_id"),
        s.OMOP_MANUAL_CONCEPT_ID.cast("string").alias("_omop_code"),
        s.OMOP_MANUAL_CONCEPT_NAME.alias("_omop_display"),
    )


PROMOTED_VITAL_STAGE_COLUMNS = [c for c in VITAL_STAGE_COLUMNS if c != "vital_code"]




def _promoted_vital_from_stage():
    # Read promoted vital-sign rows and restore their source coding struct for the common vital-
    # sign projection.
    s = spark.read.table(_n("journey_clinical._promoted_vital_sign_primitive"))
    return s.withColumn(
        "vital_code",
        codeable_concept(
            coding_obj(
                F.col("source_coding_system"), F.col("source_code"),
                F.col("source_display"), True,
            )
        ),
    ).select(*VITAL_STAGE_COLUMNS)

def _vital_sign_canonical():
    # Keep the normalized vital sign rows that pass the existing source-code admission rule.
    return _vital_sign_canonical_pregate().where(_usable_code(F.col("source_code")))




def _vital_sign_canonical_pregate():
    # Normalize vital sign source rows before the downstream admission filter so excluded
    # evidence remains countable.
    numeric = spark.read.table(_n("journey_clinical._numeric_vital_sign"))
    promoted = _promoted_vital_from_stage()
    return numeric.unionByName(promoted).unionByName(_registry_vital_canonical(), allowMissingColumns=True)




# contract v2: retain the full canonical shape for internal reuse.
SCORE_SOURCE_COLUMNS = [
    "patient_event_key",
    "event_id",
    "doc_response_key",
    "source_dcp_forms_activity_id",
    "source_object",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "score_code",
    "score_name",
    "value_number",
    "value_text",
    "unit_source_value",
    "unit_concept_id",
    "component_count",
    "interpretation_code",
    "interpretation_display",
    "result_status_code",
    "result_status_display",
    "performer_practitioner_id",
    "source_form_key",
    "promotion_rule_id",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

SCORE_STAGE_COLUMNS = SCORE_SOURCE_COLUMNS + [
    "_source_system", "_source_table", "_source_row_id", "_omop_code", "_omop_display",
    "_components_json",
]

def _numeric_score_canonical():
    # Assemble normalized numeric score rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_NUMERIC_EVENTS)
    s = s.where(_numeric_route_expr(s) == "clinical_score")
    event_id = stable_id("numeric_event:mill", s.EVENT_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_NUMERIC_EVENTS, s.EVENT_ID
    )
    deleted = F.coalesce(s.SOURCE_DELETED_IND, F.lit(False))
    ended = s.CLINICAL_EVENT_VALID_UNTIL_DT_TM.isNotNull() & (
        s.CLINICAL_EVENT_VALID_UNTIL_DT_TM < F.lit("2100-01-01").cast("timestamp")
    )
    component = F.struct(
        F.lit(1).cast("long").alias("sequence"), s.EVENT_ID.cast("string").alias("component_id"),
        F.lit("urn:cerner:event_cd").alias("coding_system"),
        s.EVENT_CD.cast("string").alias("coding_code"),
        F.coalesce(s.EVENT_LABEL, s.EVENT_CD_DISPLAY).alias("coding_display"),
        s.NUMERIC_RESULT.cast("decimal(38,10)").alias("value_number"),
        s.RESULT_TEXT_EFFECTIVE.alias("value_text"),
        F.coalesce(s.UNIT_OF_MEASURE_DISPLAY, s.RESULT_UNITS_DISPLAY).alias("unit"),
    )
    source_update = F.greatest(
        s.STRING_RESULT_UPDT_DT_TM, s.CLINICAL_EVENT_UPDT_DT_TM, s.ADC_UPDT
    )
    # contract v2: publish native numeric-event identifiers and native foreign keys for the score numeric arm
    return s.select(
        event_id.alias("patient_event_key"),
        s.EVENT_ID.cast("bigint").alias("event_id"),
        F.lit(None).cast("string").alias("doc_response_key"),
        F.lit(None).cast("bigint").alias("source_dcp_forms_activity_id"),
        F.lit("numeric_event").alias("source_object"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(_present(s.PERSON_ID), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        F.coalesce(s.PERFORMED_DT_TM, s.EVENT_START_DT_TM).alias("event_datetime"),
        s.EVENT_END_DT_TM.alias("event_end_datetime"),
        F.lit("urn:cerner:event_cd").alias("source_coding_system"),
        s.EVENT_CD.cast("string").alias("source_code"),
        F.coalesce(s.EVENT_LABEL, s.EVENT_CD_DISPLAY).alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:cerner:event_cd"), s.EVENT_CD,
                       F.coalesce(s.EVENT_LABEL, s.EVENT_CD_DISPLAY), True),
            coding_obj(F.lit("urn:omop:concept_id"), s.OMOP_MANUAL_CONCEPT_ID,
                       s.OMOP_MANUAL_CONCEPT_NAME, False, "bronze.map_numeric_events", None),
        ).alias("score_code"),
        F.coalesce(s.OMOP_MANUAL_CONCEPT_NAME, s.EVENT_LABEL, s.EVENT_CD_DISPLAY).alias("score_name"),
        s.NUMERIC_RESULT.cast("decimal(38,10)").alias("value_number"),
        s.RESULT_TEXT_EFFECTIVE.alias("value_text"),
        F.coalesce(s.UNIT_OF_MEASURE_DISPLAY, s.RESULT_UNITS_DISPLAY).alias("unit_source_value"),
        s.OMOP_MANUAL_UNITS.cast("string").alias("unit_concept_id"),
        F.to_json(F.array(component)).alias("_components_json"),
        F.lit(1).cast("long").alias("component_count"),
        s.NORMALCY_CD.cast("string").alias("interpretation_code"),
        s.NORMALCY_DISPLAY.alias("interpretation_display"),
        s.RESULT_STATUS_CD.cast("string").alias("result_status_code"),
        s.RESULT_STATUS_DISPLAY.alias("result_status_display"),
        s.PERFORMED_PRSNL_ID.cast("bigint").alias("performer_practitioner_id"),
        F.lit(None).cast("string").alias("source_form_key"),
        F.lit(None).cast("string").alias("promotion_rule_id"),
        F.when(deleted, F.lit("retracted")).when(ended, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        s.CLINICAL_EVENT_VALID_FROM_DT_TM.alias("record_status_effective_from"),
        F.when(deleted | ended, s.CLINICAL_EVENT_VALID_UNTIL_DT_TM).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"), F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("numeric_event").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_NUMERIC_EVENTS).alias("_source_table"),
        s.EVENT_ID.cast("string").alias("_source_row_id"),
        s.OMOP_MANUAL_CONCEPT_ID.cast("string").alias("_omop_code"),
        s.OMOP_MANUAL_CONCEPT_NAME.alias("_omop_display"),
    )


PROMOTED_SCORE_STAGE_COLUMNS = [
    c for c in SCORE_STAGE_COLUMNS if c != "score_code"
]




def _promoted_score_from_stage():
    # Read promoted score rows and restore their source coding struct for the common score
    # projection.
    s = spark.read.table(_n("journey_clinical._promoted_clinical_score_primitive"))
    return (
        s.withColumn(
            "score_code",
            codeable_concept(
                coding_obj(
                    F.col("source_coding_system"), F.col("source_code"),
                    F.col("source_display"), True,
                )
            ),
        )
        .select(*SCORE_STAGE_COLUMNS)
    )

def _clinical_score_canonical():
    # Keep the normalized clinical score rows that pass the existing source-code admission rule.
    return _clinical_score_canonical_pregate().where(_usable_code(F.col("source_code")))




def _clinical_score_canonical_pregate():
    # Normalize clinical score source rows before the downstream admission filter so excluded
    # evidence remains countable.
    numeric = spark.read.table(_n("journey_clinical._numeric_clinical_score"))
    promoted = _promoted_score_from_stage()
    return numeric.unionByName(promoted)




def _medication_admin_canonical_pregate():
    # Normalize medication admin source rows before the downstream admission filter so excluded
    # evidence remains countable.
    p = spark.read.table(_n("journey_clinical._medication_admin_primitive"))
    return (
        p.withColumn("medication_code", F.parse_json(F.col("_medication_code_json")))
         .withColumn("status_history", F.parse_json(F.col("_status_history_json")))
         .withColumn("ingredients", F.parse_json(F.col("_ingredients_json")))
         .drop("_medication_code_json", "_status_history_json", "_ingredients_json")
    )

def _medication_admin_canonical():
    # Keep the normalized medication admin rows that pass the existing source-code admission
    # rule.
    return _medication_admin_canonical_pregate().where(_usable_code(F.col("source_code")))




def _medication_order_canonical_pregate():
    # Normalize medication order source rows before the downstream admission filter so excluded
    # evidence remains countable.
    p = spark.read.table(_n("journey_clinical._medication_order_primitive"))
    return (
        p.withColumn("medication_code", F.parse_json(F.col("_medication_code_json")))
         .withColumn("status_history", F.parse_json(F.col("_status_history_json")))
         .withColumn("ingredients", F.parse_json(F.col("_ingredients_json")))
         .withColumn("order_details", F.parse_json(F.col("_order_details_json")))
         .drop(
             "_medication_code_json", "_status_history_json",
             "_ingredients_json", "_order_details_json",
         )
    )

def _medication_order_canonical():
    # Keep the normalized medication order rows that pass the existing source-code admission
    # rule.
    return _medication_order_canonical_pregate().where(_usable_code(F.col("source_code")))




def _medication_dispense_canonical_pregate():
    # Normalize medication dispense source rows before the downstream admission filter so
    # excluded evidence remains countable.
    s = read_source(SRC_PHARMACY_ISSUE).where(
        F.upper(F.trim(F.coalesce(F.col("ISSUE_CATEGORY"), F.lit("")))) == F.lit("ISSUE")
    )
    event_id = stable_id("pharmacy_issue:jac", s.PHARMACY_ISSUE_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID), ("urn:jac:lnkpid", s.LNKPID)],
        SRC_PHARMACY_ISSUE,
        s.PHARMACY_ISSUE_ID,
    )
    retracted = F.coalesce(~s.SOURCE_PRESENT_IND, F.lit(False))
    source_code = F.coalesce(s.JAC_DRUG_ID, s.BNF_CODE_RAW)
    source_display = F.coalesce(s.DRUG_FULL, s.DRUG_NAME)
    # contract v2: publish the JAC pharmacy issue id and native nullable relationship identifiers
    return s.select(
        event_id.alias("patient_event_key"),
        s.PHARMACY_ISSUE_ID.cast("string").alias("pharmacy_issue_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
        .when(_present(s.LNKPID), F.lit("provisional"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.ISSUE_DTTM.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:jac:drug-id").alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:jac:drug-id"), source_code, source_display, True),
            coding_obj(F.lit("http://snomed.info/sct"), s.DMD_VTM_CODE, s.DMD_VTM_NAME,
                       False, "bronze.map_pharmacy_issue", None),
        ).alias("medication_code"),
        F.when(retracted, F.lit("stopped")).otherwise(F.lit("completed"))
        .alias("status_code"),
        s.ISSUE_TYPE.alias("issue_type"), s.ISSUE_CATEGORY.alias("issue_category"),
        s.TOTAL_UNITS.cast("decimal(38,18)").alias("quantity"),
        s.DRUG_DOSEUNIT.alias("quantity_unit"),
        s.ISSUED_CONTAINERS.cast("decimal(38,18)").alias("issued_containers"),
        s.UNITS_PER_CONTAINER.cast("decimal(38,18)").alias("units_per_container"),
        s.DRUG_FORM.alias("drug_form"), s.DRUG_STRENGTH.alias("drug_strength"),
        s.CARE_SITE_CD.cast("string").alias("location_code"),
        s.ISSUE_VALUE_GBP.cast("decimal(38,18)").alias("issue_value_gbp"),
        s.DAILYISSUES_KEY.alias("source_transaction_identifier"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
        .alias("record_status"),
        s.SOURCE_RECORD_UPDATED_DT.alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("pharmacy_issue").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.SOURCE_RECORD_UPDATED_DT.alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("jac").alias("_source_system"),
        F.lit(SRC_PHARMACY_ISSUE).alias("_source_table"),
        s.PHARMACY_ISSUE_ID.alias("_source_row_id"),
        s.DMD_VTM_CODE.alias("_dmd_code"), s.DMD_VTM_NAME.alias("_dmd_display"),
    )

def _medication_dispense_canonical():
    # Keep the normalized medication dispense rows that pass the existing source-code admission
    # rule.
    return _medication_dispense_canonical_pregate().where(_usable_code(F.col("source_code")))




def _closed_timestamp(value):
    # Treat only non-null timestamps before the open-ended 2100 sentinel as closed dates.
    return value.isNotNull() & (value < F.lit("2100-01-01").cast("timestamp"))

def _coded_finding_canonical():
    # Assemble normalized coded finding rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_CODED_EVENTS).alias("s")
    natural = F.coalesce(
        s.SOURCE_ROW_KEY,
        F.concat_ws(":", s.EVENT_ID.cast("string"), s.SEQUENCE_NBR.cast("string")),
        s.ROW_HASH.cast("string"),
    )
    event_id = stable_id("coded_event:mill", natural)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_CODED_EVENTS, natural
    )
    event_time = F.coalesce(s.PERFORMED_DT_TM, s.EVENT_END_DT_TM, s.EVENT_START_DT_TM)
    event_display = F.coalesce(s.EVENT_LABEL, s.EVENT_CD_LABEL, s.EVENT_TITLE_TEXT, s.EVENT_TAG)
    value_code = F.when(s.NOMENCLATURE_ID > 0, s.NOMENCLATURE_ID.cast("string")) \
        .otherwise(s.RESULT_CD.cast("string"))
    value_display = F.coalesce(s.RESULT_LABEL, s.DESCRIPTOR, s.RESULT_VAL)
    ended = _closed_timestamp(F.least(s.CR_VALID_UNTIL_DT_TM, s.CE_VALID_UNTIL_DT_TM))
    status = _generic_record_status(F.lit(False), ended)
    source_update = F.greatest(s.SOURCE_ADC_UPDT, s.CR_UPDT_DT_TM, s.CE_UPDT_DT_TM, s.ADC_UPDT)
    # contract v2: publish native identifiers and explicit source arms across all clinical-finding lanes
    return s.select(
        event_id.alias("patient_event_key"),
        s.EVENT_ID.cast("bigint").alias("event_id"),
        s.SEQUENCE_NBR.cast("bigint").alias("sequence_nbr"),
        F.lit("coded_event").alias("source_object"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(_present(s.PERSON_ID), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        event_time.alias("event_datetime"), s.EVENT_END_DT_TM.alias("event_end_datetime"),
        F.lit("urn:cerner:code_value").alias("source_coding_system"),
        s.EVENT_CD.cast("string").alias("source_code"), event_display.alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:cerner:code_value"), s.EVENT_CD, event_display, True)
        ).alias("finding_code"),
        F.lit("coded").alias("finding_kind"),
        F.coalesce(s.DESCRIPTOR, s.RESULT_VAL).alias("value_text"),
        F.lit(None).cast("timestamp").alias("value_datetime"),
        value_code.alias("value_code"), value_display.alias("value_display"),
        s.NORMALCY_CD.cast("string").alias("normalcy_code"),
        s.NORMALCY_DISPLAY.alias("normalcy_display"),
        s.RESULT_STATUS_CD.cast("string").alias("result_status_code"),
        s.RESULT_STATUS_LABEL.alias("result_status_display"),
        s.ORDER_ID.cast("string").alias("order_id"),
        s.PARENT_EVENT_ID.cast("string").alias("parent_event_id"),
        s.PERFORMED_PRSNL_ID.cast("bigint").alias("performer_practitioner_id"),
        s.VERIFIED_PRSNL_ID.cast("bigint").alias("verifier_practitioner_id"),
        s.ORGANIZATION_ID.cast("bigint").alias("organization_id"),
        status.alias("record_status"),
        F.coalesce(s.CR_VALID_FROM_DT_TM, s.CE_VALID_FROM_DT_TM, event_time)
         .alias("record_status_effective_from"),
        F.when(status == "superseded", F.least(s.CR_VALID_UNTIL_DT_TM, s.CE_VALID_UNTIL_DT_TM))
         .alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("coded_event").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.lit(SRC_CODED_EVENTS).alias("_source_table"), natural.alias("_source_row_id"),
        s.SOURCE_ROW_KEY.alias("_mapping_source_row_key"),
        s.OMOP_MANUAL_CONCEPT.cast("string").alias("_omop_code"),
        s.OMOP_MANUAL_CONCEPT_NAME.alias("_omop_display"),
        s.OMOP_MANUAL_CONCEPT_DOMAIN.alias("_omop_domain"),
        _generic_payload(s).alias("_payload"),
        s.SOURCE_ROW_KEY.isNotNull().alias("_typed_route"),
    )

def _nomen_finding_canonical():
    # Assemble normalized nomen finding rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_NOMEN_EVENTS).alias("s")
    natural = F.coalesce(
        s.SOURCE_ROW_KEY,
        F.concat_ws(":", s.EVENT_ID.cast("string"), s.SEQUENCE_NBR.cast("string")),
        s.ROW_HASH.cast("string"),
    )
    event_id = stable_id("nomen_event:mill", natural)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_NOMEN_EVENTS, natural
    )
    event_time = F.coalesce(s.CLINICAL_EVENT_DT_TM, s.EVENT_END_DT_TM,
                            s.PERFORMED_DT_TM, s.EVENT_START_DT_TM)
    event_display = F.coalesce(s.EVENT_NAME, s.EVENT_CD_DISPLAY, s.EVENT_TAG)
    value_code = F.when(_present(s.SOURCE_IDENTIFIER), F.trim(s.SOURCE_IDENTIFIER)) \
        .otherwise(F.coalesce(s.NOMENCLATURE_CODE, s.NOMENCLATURE_ID.cast("string")))
    value_display = F.coalesce(s.SOURCE_STRING, s.SNOMED_TERM,
                               s.RESOLVED_OMOP_CONCEPT_NAME, s.EVENT_NAME)
    ended_at = F.least(s.CR_VALID_UNTIL_DT_TM, s.CE_VALID_UNTIL_DT_TM)
    ended = _closed_timestamp(ended_at)
    status = _generic_record_status(s.SOURCE_DELETED_IND, ended, s.AUTHENTIC_FLAG)
    source_update = F.greatest(s.SOURCE_CHANGE_TS, s.NOMENCLATURE_ADC_UPDT, s.ADC_UPDT)
    organization = F.coalesce(s.CE_ORGANIZATION_ID, s.CR_ORGANIZATION_ID)
    return s.select(
        event_id.alias("patient_event_key"),
        s.EVENT_ID.cast("bigint").alias("event_id"),
        s.SEQUENCE_NBR.cast("bigint").alias("sequence_nbr"),
        F.lit("nomen_event").alias("source_object"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(_present(s.PERSON_ID), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        event_time.alias("event_datetime"), s.EVENT_END_DT_TM.alias("event_end_datetime"),
        F.lit("urn:cerner:code_value").alias("source_coding_system"),
        s.EVENT_CD.cast("string").alias("source_code"), event_display.alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:cerner:code_value"), s.EVENT_CD, event_display, True)
        ).alias("finding_code"),
        F.lit("nomenclature").alias("finding_kind"),
        s.SOURCE_STRING.alias("value_text"), F.lit(None).cast("timestamp").alias("value_datetime"),
        value_code.alias("value_code"), value_display.alias("value_display"),
        s.NORMALCY_CD.cast("string").alias("normalcy_code"),
        s.NORMALCY_DISPLAY.alias("normalcy_display"),
        s.RESULT_STATUS_CD.cast("string").alias("result_status_code"),
        s.RESULT_STATUS_DISPLAY.alias("result_status_display"),
        s.ORDER_ID.cast("string").alias("order_id"),
        s.PARENT_EVENT_ID.cast("string").alias("parent_event_id"),
        s.PERFORMED_PRSNL_ID.cast("bigint").alias("performer_practitioner_id"),
        s.VERIFIED_PRSNL_ID.cast("bigint").alias("verifier_practitioner_id"),
        organization.cast("bigint").alias("organization_id"),
        status.alias("record_status"),
        F.coalesce(s.CR_VALID_FROM_DT_TM, s.CE_VALID_FROM_DT_TM, event_time)
         .alias("record_status_effective_from"),
        F.when(status == "retracted", source_update)
         .when(status == "superseded", ended_at).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("nomen_event").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.lit(SRC_NOMEN_EVENTS).alias("_source_table"), natural.alias("_source_row_id"),
        s.RESOLVED_OMOP_CONCEPT_ID.cast("string").alias("_omop_code"),
        s.RESOLVED_OMOP_CONCEPT_NAME.alias("_omop_display"),
        s.RESOLVED_OMOP_CONCEPT_DOMAIN.alias("_omop_domain"),
        s.SNOMED_CODE.cast("string").alias("_snomed_code"),
        s.SNOMED_TERM.alias("_snomed_display"),
        _generic_payload(s).alias("_payload"),
        s.SOURCE_ROW_KEY.isNotNull().alias("_typed_route"),
    )

def _date_finding_canonical():
    # Assemble normalized date finding rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_DATE_EVENTS).alias("s")
    natural = F.coalesce(s.EVENT_ID.cast("string"), s.ROW_HASH.cast("string"))
    event_id = stable_id("date_event:mill", natural)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_DATE_EVENTS, natural
    )
    event_time = F.coalesce(s.RESULT_DT_TM, s.PERFORMED_DT_TM,
                            s.EVENT_END_DT_TM, s.EVENT_START_DT_TM)
    event_display = F.coalesce(s.EVENT_LABEL, s.EVENT_CD_DISPLAY, s.EVENT_TITLE_TEXT, s.EVENT_TAG)
    ended_at = F.least(s.DATE_RESULT_VALID_UNTIL_DT_TM, s.CLINICAL_EVENT_VALID_UNTIL_DT_TM)
    ended = _closed_timestamp(ended_at)
    status = _generic_record_status(s.SOURCE_DELETED_IND, ended, s.AUTHENTIC_FLAG)
    source_update = F.greatest(s.DATE_RESULT_EFFECTIVE_UPDT_DT_TM,
                               s.CLINICAL_EVENT_ADC_UPDT, s.LOOKUP_ADC_UPDT, s.ADC_UPDT)
    return s.select(
        event_id.alias("patient_event_key"),
        s.EVENT_ID.cast("bigint").alias("event_id"),
        F.lit(None).cast("bigint").alias("sequence_nbr"),
        F.lit("date_event").alias("source_object"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(_present(s.PERSON_ID), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        event_time.alias("event_datetime"), s.EVENT_END_DT_TM.alias("event_end_datetime"),
        F.lit("urn:cerner:code_value").alias("source_coding_system"),
        s.EVENT_CD.cast("string").alias("source_code"), event_display.alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:cerner:code_value"), s.EVENT_CD, event_display, True)
        ).alias("finding_code"),
        F.lit("date").alias("finding_kind"), F.lit(None).cast("string").alias("value_text"),
        s.RESULT_DT_TM.alias("value_datetime"), F.lit(None).cast("string").alias("value_code"),
        F.lit(None).cast("string").alias("value_display"),
        s.NORMALCY_CD.cast("string").alias("normalcy_code"),
        s.NORMALCY_DISPLAY.alias("normalcy_display"),
        s.RESULT_STATUS_CD.cast("string").alias("result_status_code"),
        F.lit(None).cast("string").alias("result_status_display"),
        s.ORDER_ID.cast("string").alias("order_id"),
        s.PARENT_EVENT_ID.cast("string").alias("parent_event_id"),
        s.PERFORMED_PRSNL_ID.cast("bigint").alias("performer_practitioner_id"),
        s.VERIFIED_PRSNL_ID.cast("bigint").alias("verifier_practitioner_id"),
        s.ORGANIZATION_ID.cast("bigint").alias("organization_id"),
        status.alias("record_status"),
        F.coalesce(s.DATE_RESULT_VALID_FROM_DT_TM, s.CLINICAL_EVENT_VALID_FROM_DT_TM, event_time)
         .alias("record_status_effective_from"),
        F.when(status == "retracted", source_update)
         .when(status == "superseded", ended_at).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("date_event").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.lit(SRC_DATE_EVENTS).alias("_source_table"), natural.alias("_source_row_id"),
        _generic_payload(s).alias("_payload"),
        (s.EVENT_ID.isNotNull() & (s.RESULT_DT_TM.isNotNull() | s.EVENT_CD.isNotNull()))
         .alias("_typed_route"),
    )

def _text_finding_canonical():
    # Assemble normalized text finding rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_TEXT_EVENTS).alias("s")
    natural = F.coalesce(s.EVENT_ID.cast("string"), s.ROW_HASH.cast("string"))
    event_id = stable_id("text_event:mill", natural)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_TEXT_EVENTS, natural
    )
    event_time = F.coalesce(s.RESULT_DT_TM, s.PERFORMED_DT_TM,
                            s.EVENT_END_DT_TM, s.EVENT_START_DT_TM)
    event_display = F.coalesce(s.EVENT_LABEL, s.EVENT_CD_DISPLAY, s.EVENT_TITLE_TEXT, s.EVENT_TAG)
    ended_at = F.least(s.STRING_RESULT_VALID_UNTIL_DT_TM, s.CLINICAL_EVENT_VALID_UNTIL_DT_TM)
    ended = _closed_timestamp(ended_at)
    status = _generic_record_status(s.SOURCE_DELETED_IND, ended, s.AUTHENTIC_FLAG)
    source_update = F.greatest(s.STRING_RESULT_EFFECTIVE_UPDT_DT_TM,
                               s.CLINICAL_EVENT_ADC_UPDT, s.LONG_TEXT_ADC_UPDT,
                               s.LOOKUP_ADC_UPDT, s.ADC_UPDT)
    document_candidate = _text_document_candidate(s)
    route = F.when(s.EVENT_ID.isNull(), F.lit("annex_catch_all")) \
        .when(document_candidate, F.lit("document_candidate")) \
        .otherwise(F.lit("clinical_finding"))
    return s.select(
        event_id.alias("patient_event_key"),
        s.EVENT_ID.cast("bigint").alias("event_id"),
        F.lit(None).cast("bigint").alias("sequence_nbr"),
        F.lit("text_event").alias("source_object"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(_present(s.PERSON_ID), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        event_time.alias("event_datetime"), s.EVENT_END_DT_TM.alias("event_end_datetime"),
        F.lit("urn:cerner:code_value").alias("source_coding_system"),
        s.EVENT_CD.cast("string").alias("source_code"), event_display.alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:cerner:code_value"), s.EVENT_CD, event_display, True)
        ).alias("finding_code"),
        F.lit("text").alias("finding_kind"), s.TEXT_RESULT.alias("value_text"),
        F.lit(None).cast("timestamp").alias("value_datetime"),
        F.lit(None).cast("string").alias("value_code"),
        F.lit(None).cast("string").alias("value_display"),
        s.NORMALCY_CD.cast("string").alias("normalcy_code"),
        s.NORMALCY_DISPLAY.alias("normalcy_display"),
        s.RESULT_STATUS_CD.cast("string").alias("result_status_code"),
        s.RESULT_STATUS_DISPLAY.alias("result_status_display"),
        s.ORDER_ID.cast("string").alias("order_id"),
        s.PARENT_EVENT_ID.cast("string").alias("parent_event_id"),
        s.PARENT_EVENT_CD.cast("string").alias("_parent_event_cd"),
        s.PERFORMED_PRSNL_ID.cast("bigint").alias("performer_practitioner_id"),
        s.VERIFIED_PRSNL_ID.cast("bigint").alias("verifier_practitioner_id"),
        s.ORGANIZATION_ID.cast("bigint").alias("organization_id"),
        status.alias("record_status"),
        F.coalesce(s.STRING_RESULT_VALID_FROM_DT_TM, s.CLINICAL_EVENT_VALID_FROM_DT_TM, event_time)
         .alias("record_status_effective_from"),
        F.when(status == "retracted", source_update)
         .when(status == "superseded", ended_at).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("text_event").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.lit(SRC_TEXT_EVENTS).alias("_source_table"), natural.alias("_source_row_id"),
        s.OMOP_MANUAL_CONCEPT_ID.cast("string").alias("_omop_code"),
        s.OMOP_MANUAL_CONCEPT_NAME.alias("_omop_display"),
        s.OMOP_MANUAL_CONCEPT_DOMAIN.alias("_omop_domain"),
        s.OMOP_MANUAL_VALUE_CONCEPT_ID.cast("string").alias("_omop_value_code"),
        s.OMOP_MANUAL_VALUE_CONCEPT_NAME.alias("_omop_value_display"),
        s.OMOP_MANUAL_VALUE_CONCEPT_DOMAIN.alias("_omop_value_domain"),
        F.lit(None).cast("string").alias("_anon_document_text"),
        F.lit(None).cast("string").alias("_anon_document_status"),
        _generic_payload(s).alias("_payload"), route.alias("_route"),
    )

def _coded_finding_typed():
    # Admit coded finding rows routed to clinical findings only when their source code is
    # usable.
    return _coded_finding_canonical().where(
        F.col("_typed_route") & _usable_code(F.col("source_code"))
    )

def _nomen_finding_typed():
    # Admit nomen finding rows routed to clinical findings only when their source code is
    # usable.
    return _nomen_finding_canonical().where(
        F.col("_typed_route") & _usable_code(F.col("source_code"))
    )

def _date_finding_typed():
    # Admit date finding rows routed to clinical findings only when their source code is usable.
    return _date_finding_canonical().where(
        F.col("_typed_route") & _usable_code(F.col("source_code"))
    )

def _text_finding_typed():
    # Admit text finding rows routed to clinical findings only when their source code is usable.
    return _text_finding_canonical().where(
        (F.col("_route") == "clinical_finding") & _usable_code(F.col("source_code"))
    )




def _generic_record_status(deleted, ended, authentic=None):
    # Derive active, superseded or retracted status from deletion, end-date and optional
    # authenticity evidence.
    status = F.when(F.coalesce(deleted.cast("boolean"), F.lit(False)), F.lit("retracted"))
    superseded = ended
    if authentic is not None:
        superseded = superseded | (F.coalesce(authentic.cast("long"), F.lit(1)) == 0)
    return status.when(superseded, F.lit("superseded")).otherwise(F.lit("active"))

def _generic_payload(source):
    # Preserve all source columns in a single structured VARIANT payload.
    return F.parse_json(F.to_json(F.struct(*[source[c] for c in source.columns])))

TEXT_EVENT_DOCUMENT_CANDIDATE_PREDICATE = (
    "EVENT_ID IS NOT NULL "
    "AND coalesce(TEXT_RESULT_LENGTH, length(TEXT_RESULT), 0) > 100 "
    "AND lower(coalesce(nullif(trim(EVENT_LABEL), ''), nullif(trim(EVENT_CD_DISPLAY), ''), "
    "nullif(trim(EVENT_TITLE_TEXT), ''), '')) NOT IN ("
    "'25 vit d comment','25-hydroxy vitamin d3 serum','ana comment','ana pattern',"
    "'cardiolipin antibody comment','cardiolipin antibody screen','egfr comment',"
    "'estimated gfr','fbc comments','gastric parietal cell antibody','haemoglobin',"
    "'haemoglobinopathy screen conclusion','haemostasis comments','hav igm qualitative',"
    "'hb core antibody qualitative (anti hbc)','hb surface antigen qualitative (hbsag)',"
    "'hba1c comments','hba1c diabetic control ranges:','hcv igg qualitative',"
    "'hiv 1 and 2 antibody qualitative','kleihauer','lithium comments',"
    "'lupus anticoagulant screen','malaria rdt antigen result','oestradiol ref. range',"
    "'pcr comment','tacrolimus comments','tb gamma interferon assay',"
    "'tissue transglutaminase antibody comment','troponin comments',"
    "'urine albumin comments','urine protein comments')"
)

def _text_document_candidate(source):
    # Apply the shared predicate that decides whether a text event belongs in the document feed.
    return F.expr(TEXT_EVENT_DOCUMENT_CANDIDATE_PREDICATE)




SRC_PACS_EXAMINATION = "4_prod.bronze.map_pacs_examination"

def _imaging_exam_canonical_pregate(integration=False):
    # Normalize imaging exam source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_PACS_EXAMINATION)
    event_id = stable_id("imaging_exam:pacs", s.PACS_EXAMINATION_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID),
         ("urn:sectra:pacs-patient-id", s.PACS_PATIENT_ID)],
        SRC_PACS_EXAMINATION,
        s.PACS_EXAMINATION_ID,
    )
    retracted = F.coalesce(~s.SOURCE_PRESENT_IND, F.lit(False))
    source_display = s.EXAMINATION_DESCRIPTION
    source_code = _code_or_display(s.EXAMINATION_CODE, source_display)
    # contract v2: publish native PACS examination identity and typed nullable foreign keys for the PACS arm
    return s.select(
        event_id.alias("patient_event_key"),
        F.lit(None).cast("bigint").alias("event_id"),
        s.PACS_EXAMINATION_ID.cast("bigint").alias("pacs_examination_id"),
        F.lit("pacs").alias("source_object"),
        F.lit(None).cast("string").alias("organization_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
        .when(s.PACS_PATIENT_ID.isNotNull(), F.lit("provisional"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        F.coalesce(s.EXAMINATION_DT_TM, s.ARRIVAL_DT_TM).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:sectra:examination-code").alias("source_coding_system"),
        source_code.alias("source_code"),
        source_display.alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:sectra:examination-code"), source_code,
                       source_display, True)
        ).alias("exam_code"),
        F.when(retracted, F.lit("cancelled")).otherwise(F.lit("available"))
        .alias("status_code"),
        s.MILL_LINK_REF.alias("accession_identifier"),
        s.STUDY_INSTANCE_UID.alias("study_instance_uid"), s.MODALITY.alias("modality_code"),
        s.BODY_PART.alias("body_site_code"),
        F.when(s.LATEST_REPORT_ID.isNotNull(),
               stable_id("document:pacs_report", s.LATEST_REPORT_ID))
        .alias("report_patient_event_key"),
        F.lit(None).cast("bigint").alias("requester_practitioner_id"),
        F.lit(None).cast("bigint").alias("performer_practitioner_id"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
        .alias("record_status"),
        s.SRC_ADC_UPDT.alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("pacs_examination").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.SRC_ADC_UPDT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        *(_pacs_exam_integration_columns(s) if integration else []),
        F.lit("sectra-pacs").alias("_source_system"),
        F.lit(SRC_PACS_EXAMINATION).alias("_source_table"),
        s.PACS_EXAMINATION_ID.cast("string").alias("_source_row_id"),
    )

def _imaging_exam_canonical(integration=False):
    # Keep the normalized imaging exam rows that pass the existing source-code admission rule.
    return _imaging_exam_canonical_pregate(integration).where(_usable_code(F.col("source_code")))




def _pacs_study_artifact_canonical():
    """One traceable DICOM-study asset per governed PACS examination.

    The study UID is a source locator, not a promise that Silver can retrieve the
    underlying pixels. storage_uri remains null until a governed serving manifest exists.
    """
    s = read_source(SRC_PACS_EXAMINATION)
    artifact_id = stable_id("artifact:pacs_study", s.PACS_EXAMINATION_ID)
    imaging_event_id = stable_id("imaging_exam:pacs", s.PACS_EXAMINATION_ID)
    skey, ssys = subject_key_with_system(
        [
            ("urn:cerner:person_id", s.PERSON_ID),
            ("urn:sectra:pacs-patient-id", s.PACS_PATIENT_ID),
        ],
        SRC_PACS_EXAMINATION,
        s.PACS_EXAMINATION_ID,
    )
    retracted = F.coalesce(~s.SOURCE_PRESENT_IND, F.lit(False))
    event_time = F.coalesce(s.EXAMINATION_DT_TM, s.ARRIVAL_DT_TM)
    source_code = _code_or_display(s.EXAMINATION_CODE, s.EXAMINATION_DESCRIPTION)
    return (
        s.where(s.STUDY_INSTANCE_UID.isNotNull())
        # contract v2: publish PACS study native identity and explicit artifact SHA keys
        .select(
            artifact_id.alias("patient_event_key"),
            s.PACS_EXAMINATION_ID.cast("bigint").alias("pacs_examination_id"),
            F.lit(None).cast("string").alias("dicom_path"),
            F.lit("pacs_study").alias("source_object"),
            F.lit(None).cast("string").alias("organization_key"),
            artifact_id.alias("artifact_key"),
            F.lit(None).cast("string").alias("parent_artifact_key"),
            skey.alias("subject_key"),
            ssys.alias("subject_id_system"),
            s.PERSON_ID.cast("bigint").alias("person_id"),
            F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
            .when(s.PACS_PATIENT_ID.isNotNull(), F.lit("provisional"))
            .otherwise(F.lit("unresolved")).alias("identity_status"),
            F.lit(None).cast("bigint").alias("encounter_id"),
            event_time.alias("event_datetime"),
            F.lit(None).cast("timestamp").alias("event_end_datetime"),
            F.lit("urn:sectra:examination-code").alias("source_coding_system"),
            source_code.alias("source_code"),
            s.EXAMINATION_DESCRIPTION.alias("source_display"),
            codeable_concept(
                coding_obj(
                    F.lit("urn:sectra:examination-code"),
                    source_code,
                    s.EXAMINATION_DESCRIPTION,
                    True,
                )
            ).alias("artifact_type"),
            F.lit("image").alias("artifact_class"),
            F.lit("study").alias("artifact_level"),
            event_time.alias("acquisition_datetime"),
            s.MODALITY.alias("modality_code"),
            s.BODY_PART.alias("body_site_code"),
            F.lit("urn:dicom:study-instance-uid").alias("locator_system"),
            s.STUDY_INSTANCE_UID.alias("locator_value"),
            F.lit(None).cast("string").alias("storage_uri"),
            F.when(retracted, F.lit("withdrawn"))
            .when(s.SERIES_PIXEL_DATA_IND == F.lit(False), F.lit("unavailable"))
            .otherwise(F.lit("metadata_only")).alias("availability_status"),
            s.STUDY_INSTANCE_UID.alias("study_instance_uid"),
            F.lit(None).cast("string").alias("series_instance_uid"),
            F.lit(None).cast("string").alias("sop_instance_uid"),
            F.coalesce(s.SERIES_COUNT_MEASURED, s.SERIES_COUNT).cast("long")
            .alias("series_count"),
            F.coalesce(s.SERIES_OBJECT_COUNT, s.IMAGE_COUNT).cast("long")
            .alias("object_count"),
            s.FOLDER_COUNT.cast("long").alias("folder_count"),
            s.SERIES_PIXEL_DATA_IND.cast("boolean").alias("payload_present_ind"),
            F.lit(None).cast("string").alias("file_name"),
            F.lit("application/dicom").alias("content_type"),
            s.STORED_SIZE_BYTES.cast("long").alias("byte_size"),
            F.lit(None).cast("string").alias("sha256"),
            s.PACS_EXAMINATION_ID.cast("string").alias("source_artifact_id"),
            F.lit(None).cast("string").alias("ingest_run_id"),
            s.LAST_ACCESSED_DT_TM.alias("last_accessed_datetime"),
            s.ARCHIVE_STATE_CD.cast("string").alias("archive_status_code"),
            F.lit(None).cast("string").alias("burned_in_pii_tier"),
            s.EXAMINATION_STATUS_CD.cast("string").alias("status_code"),
            F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
            .alias("record_status"),
            s.SRC_ADC_UPDT.alias("record_status_effective_from"),
            F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
            F.lit(None).cast("string").alias("confidentiality_code"),
            F.lit(None).cast("boolean").alias("vip_ind"),
            F.lit(None).cast("boolean").alias("withheld_identity_ind"),
            F.lit("null").alias("_sensitivity_labels_json"),
            F.lit("pacs_examination").alias("source_feed"),
            F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
            s.SRC_ADC_UPDT.alias("source_update_timestamp"),
            s.ADC_UPDT.alias("loaded_at"),
            F.lit("sectra-pacs").alias("_source_system"),
            F.lit(SRC_PACS_EXAMINATION).alias("_source_table"),
            s.PACS_EXAMINATION_ID.cast("string").alias("_source_row_id"),
            imaging_event_id.alias("_imaging_patient_event_id"),
            F.when(
                s.LATEST_REPORT_ID.isNotNull(),
                stable_id("document:pacs_report", s.LATEST_REPORT_ID),
            ).alias("_report_patient_event_id"),
        )
    )

def _dicom_file_artifact_canonical():
    """One file-level asset per governed DICOM volume path in the curated pilot."""
    s = read_source(SRC_DICOM_FILE_ATTRIBUTE)
    artifact_id = stable_id("artifact:dicom_file", s.DICOM_PATH)
    parent_artifact_id = F.when(
        s.PACS_EXAMINATION_ID.isNotNull(),
        stable_id("artifact:pacs_study", s.PACS_EXAMINATION_ID),
    )
    imaging_event_id = F.when(
        s.PACS_EXAMINATION_ID.isNotNull(),
        stable_id("imaging_exam:pacs", s.PACS_EXAMINATION_ID),
    )
    skey, ssys = subject_key_with_system(
        [
            ("urn:cerner:person_id", s.PERSON_ID),
            ("urn:sectra:pacs-patient-id", s.PACS_PATIENT_ID),
            ("urn:dicom:patient-id", s.DICOM_PATIENT_ID),
        ],
        SRC_DICOM_FILE_ATTRIBUTE,
        s.DICOM_PATH,
    )
    retracted = F.coalesce(~s.SOURCE_PRESENT_IND, F.lit(False))
    event_time = F.coalesce(
        s.ACQUISITION_DT_TM_CLEAN,
        s.CONTENT_DT_TM_CLEAN,
        s.SERIES_DT_TM_CLEAN,
        s.STUDY_DT_TM_CLEAN,
        s.INSTANCE_CREATION_DT_TM_CLEAN,
    )
    source_code = _code_or_display(s.SOP_CLASS_UID, s.MODALITY)
    source_display = F.coalesce(
        s.IMAGE_TYPE, s.SERIES_DESCRIPTION, s.STUDY_DESCRIPTION, s.SOP_CLASS_UID,
    )
    return (
        s.where(s.DICOM_PATH.isNotNull())
        # contract v2: publish DICOM path native identity and explicit parent artifact SHA key
        .select(
            artifact_id.alias("patient_event_key"),
            F.lit(None).cast("bigint").alias("pacs_examination_id"),
            s.DICOM_PATH.cast("string").alias("dicom_path"),
            F.lit("dicom_file").alias("source_object"),
            F.lit(None).cast("string").alias("organization_key"),
            artifact_id.alias("artifact_key"),
            parent_artifact_id.alias("parent_artifact_key"),
            skey.alias("subject_key"),
            ssys.alias("subject_id_system"),
            s.PERSON_ID.cast("bigint").alias("person_id"),
            F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
            .when(s.PACS_PATIENT_ID.isNotNull() | s.DICOM_PATIENT_ID.isNotNull(),
                  F.lit("provisional"))
            .otherwise(F.lit("unresolved")).alias("identity_status"),
            F.lit(None).cast("bigint").alias("encounter_id"),
            event_time.alias("event_datetime"),
            F.lit(None).cast("timestamp").alias("event_end_datetime"),
            F.lit("urn:dicom:sop-class-uid").alias("source_coding_system"),
            source_code.alias("source_code"),
            source_display.alias("source_display"),
            codeable_concept(
                coding_obj(
                    F.lit("urn:dicom:sop-class-uid"),
                    source_code,
                    source_display,
                    True,
                )
            ).alias("artifact_type"),
            F.lit("image").alias("artifact_class"),
            F.lit("file").alias("artifact_level"),
            event_time.alias("acquisition_datetime"),
            s.MODALITY.alias("modality_code"),
            s.BODY_PART_EXAMINED.alias("body_site_code"),
            F.lit("urn:barts:unity-catalog-volume-path").alias("locator_system"),
            s.DICOM_PATH.alias("locator_value"),
            s.DICOM_PATH.alias("storage_uri"),
            F.when(retracted, F.lit("withdrawn"))
            .otherwise(F.lit("available")).alias("availability_status"),
            s.STUDY_INSTANCE_UID.alias("study_instance_uid"),
            s.SERIES_INSTANCE_UID.alias("series_instance_uid"),
            s.SOP_INSTANCE_UID.alias("sop_instance_uid"),
            F.lit(None).cast("long").alias("series_count"),
            F.lit(1).cast("long").alias("object_count"),
            F.lit(None).cast("long").alias("folder_count"),
            (~retracted).cast("boolean").alias("payload_present_ind"),
            F.regexp_extract(s.DICOM_PATH, r"[^/]+$", 0).alias("file_name"),
            F.lit("application/dicom").alias("content_type"),
            F.lit(None).cast("long").alias("byte_size"),
            s.FILE_SHA256.alias("sha256"),
            s.DICOM_PATH.alias("source_artifact_id"),
            F.lit(None).cast("string").alias("ingest_run_id"),
            F.lit(None).cast("timestamp").alias("last_accessed_datetime"),
            F.lit(None).cast("string").alias("archive_status_code"),
            s.BURNED_IN_PII_TIER.alias("burned_in_pii_tier"),
            s.EXAM_LINK_STATUS.alias("status_code"),
            F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
            .alias("record_status"),
            s.SOURCE_MAX_EXTRACTION_TS.alias("record_status_effective_from"),
            F.when(retracted, s.PIPELINE_UPDT_DT_TM).alias("record_status_effective_to"),
            F.lit(None).cast("string").alias("confidentiality_code"),
            F.lit(None).cast("boolean").alias("vip_ind"),
            F.lit(None).cast("boolean").alias("withheld_identity_ind"),
            F.lit("null").alias("_sensitivity_labels_json"),
            F.lit("dicom_file_attribute").alias("source_feed"),
            F.date_format(s.PIPELINE_UPDT_DT_TM, "yyyyMMddHHmmss").alias("load_batch_id"),
            s.SOURCE_MAX_EXTRACTION_TS.alias("source_update_timestamp"),
            s.PIPELINE_UPDT_DT_TM.alias("loaded_at"),
            F.lit("sectra-dicom-volume").alias("_source_system"),
            F.lit(SRC_DICOM_FILE_ATTRIBUTE).alias("_source_table"),
            s.DICOM_PATH.alias("_source_row_id"),
            imaging_event_id.alias("_imaging_patient_event_id"),
            F.lit(None).cast("string").alias("_report_patient_event_id"),
        )
    )

def _artifact_asset_canonical():
    # Assemble normalized artifact asset rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    return _pacs_study_artifact_canonical().unionByName(
        _dicom_file_artifact_canonical()
    )


SRC_DICOM_FILE_ATTRIBUTE = "4_prod.bronze.map_dicom_file_attribute"




# ==== Text documents ====

# contract v2: publish pathology report_version_id and native encounter identity
def _pathology_report_document_base(rows):
    # Join accession aliases, encounter links and patient-resolution evidence onto pathology
    # report versions.
    rows = _alias_resolved_accession(rows).alias("r")
    link = spark.read.table(_n("journey_events._pathology_parent_link")).select(
        F.col("source_parent_key").alias("_link_parent_key"),
        F.col("source_encounter_id"),
    ).alias("l")
    i = spark.read.table(_n("journey_events._pathology_accession_identity")).select(
        F.col("pathology_accession_id").alias("_identity_accession_id"),
        "canonical_person_id", "person_resolution_status", "evidence_mrn", "evidence_nhs",
    ).alias("i")
    d = (
        rows.join(link, F.col("r.source_record_key") == F.col("l._link_parent_key"), "left")
        .join(
            i,
            F.col("r._resolved_accession_id") == F.col("i._identity_accession_id"),
            "left",
        )
        .join(
            _pathology_document_alias_map("MRN", "mrn"),
            F.trim(F.col("i.evidence_mrn")) == F.col("_mrn_value"),
            "left",
        )
        .join(
            _pathology_document_alias_map("NHS", "nhs"),
            F.trim(F.col("i.evidence_nhs")) == F.col("_nhs_value"),
            "left",
        )
    )
    mrn_unique = F.coalesce(F.col("_mrn_count"), F.lit(0)) == 1
    nhs_unique = F.coalesce(F.col("_nhs_count"), F.lit(0)) == 1
    alias_conflict = (
        mrn_unique & nhs_unique
        & (F.col("_mrn_person") != F.col("_nhs_person"))
    )
    alias_person = (
        F.when(mrn_unique & nhs_unique
               & (F.col("_mrn_person") == F.col("_nhs_person")), F.col("_nhs_person"))
        .when(nhs_unique & ~mrn_unique, F.col("_nhs_person"))
        .when(mrn_unique & ~nhs_unique, F.col("_mrn_person"))
    )
    resolved_person = F.coalesce(F.col("canonical_person_id").cast("string"), alias_person)
    linkage_route = (
        F.when(F.col("canonical_person_id").isNotNull(), F.lit("direct"))
        .when(alias_conflict, F.lit("none"))
        .when(alias_person.isNotNull() & nhs_unique, F.lit("alias_nhs"))
        .when(alias_person.isNotNull(), F.lit("alias_mrn"))
        .otherwise(F.lit("none"))
    )
    event_id = stable_id("document:pathology_report", F.col("report_version_id"))
    thread_id = stable_id(
        "document_thread:pathology_report", F.col("report_series_id")
    )
    supersedes_id = F.when(
        _present(F.col("supersedes_report_version_id")),
        stable_id(
            "document:pathology_report", F.col("supersedes_report_version_id")
        ),
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", resolved_person),
         ("urn:barts:mrn", F.col("evidence_mrn")),
         ("https://fhir.nhs.uk/Id/nhs-number", F.col("evidence_nhs"))],
        SRC_PATHOLOGY_REPORT_VERSIONS, F.col("report_version_id"),
    )
    retracted = F.col("lifecycle_status").isin("cancelled", "entered_in_error")
    return d.select(
        event_id.alias("patient_event_id"), event_id.alias("fact_row_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        resolved_person.alias("person_id"),
        F.when(resolved_person.isNotNull(), F.lit("resolved"))
         .when(F.col("person_resolution_status") == "conflicting", F.lit("provisional"))
         .when(alias_conflict, F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.col("source_encounter_id").cast("bigint").alias("encounter_id"),
        _clamped_ts(F.col("issued_dt")).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:barts:pathology:report-code").alias("source_coding_system"),
        F.col("report_code").cast("string").alias("source_code"),
        F.coalesce(F.col("report_role"), F.col("report_code")).cast("string")
         .alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:barts:pathology:report-code"),
                       F.col("report_code"), F.col("report_role"), True),
        ).alias("_document_type_json"),
        F.col("report_code").cast("string").alias("title"),
        F.lit(None).cast("string").alias("author_practitioner_id"),
        F.lit(None).cast("string").alias("author_role"),
        F.lit(None).cast("string").alias("service_id"),
        F.col("lifecycle_status").alias("status_code"),
        F.col("report_version_id").alias("version_id"),
        F.col("report_version_id").cast("string").alias("report_version_id"),
        F.col("report_text").alias("document_text"),
        F.lit("[]").alias("_sections_json"),
        F.lit(None).cast("string").alias("parser_version"),
        F.lit(None).cast("string").alias("decompressor_version"),
        F.lit(None).cast("string").alias("post_processor_version"),
        F.lit("text/plain").alias("content_type"),
        F.lit(None).cast("string").alias("encoding"),
        F.sha2(F.col("report_text"), 256).alias("text_sha256"),
        F.length(F.col("report_text")).cast("long").alias("text_length"),
        F.when(retracted, F.lit("retracted"))
         .when(F.coalesce(F.col("is_current"), F.lit(False)), F.lit("active"))
         .otherwise(F.lit("superseded")).alias("record_status"),
        F.col("valid_from").alias("record_status_effective_from"),
        F.col("valid_to").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("null").alias("_sensitivity_labels_json"),
        F.lit(None).cast("string").alias("document_class"),
        F.lit(None).cast("string").alias("contributor_system"),
        F.lit(None).cast("string").alias("succession_status"),
        F.lit(None).cast("string").alias("source_parent_event_id"),
        F.lit(None).cast("string").alias("parent_relation"),
        F.lit(None).cast("string").alias("source_parent_display"),
        F.lit(None).cast("string").alias("source_parent_title"),
        F.lit(None).cast("string").alias("source_parent_tag"),
        F.lit(None).cast("string").alias("source_tag"),
        F.lit(None).cast("string").alias("source_record_status"),
        F.lit(None).cast("string").alias("document_text_anonymised"),
        F.lit(False).alias("text_is_anonymised"),
        F.lit("urn:barts:pathology:role").alias("_label_system"),
        F.lower(F.concat_ws(":",
            F.coalesce(F.expr("nullif(trim(discipline), '')"), F.lit("~")),
            F.coalesce(F.expr("nullif(trim(report_role), '')"), F.lit("~")),
        )).alias("_label_key"),
        F.lit("pathology_report").alias("source_feed"),
        F.date_format("ADC_UPDT", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("valid_from").alias("source_update_timestamp"),
        F.col("ADC_UPDT").alias("loaded_at"),
        F.lit("laboratory").alias("_source_system"),
        F.lit(SRC_PATHOLOGY_REPORT_VERSIONS).alias("_source_table"),
        F.col("report_version_id").alias("_source_row_id"),
        F.lit(None).cast("string").alias("_raw_content_sha256"),
        linkage_route.alias("_linkage_route"),
        thread_id.alias("_document_thread_id"),
        supersedes_id.alias("_supersedes_document_id"),
        F.col("version_ordinal").cast("long").alias("_version_ordinal"),
    )

def _pathology_report_document_canonical():
    # Assemble normalized pathology report document rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    return _pathology_report_document_base(
        _pathology_report_document_text_rows().where(F.col("is_current"))
    )

def _pathology_report_document_history_canonical():
    # Assemble normalized pathology report document history rows for downstream dataset
    # builders, preserving the existing source and identity rules.
    return _pathology_report_document_base(
        _pathology_report_document_text_rows().where(
            ~F.coalesce(F.col("is_current"), F.lit(False))
        )
    )

# contract v2: retain the full canonical shape for internal reuse.
DOCUMENT_SOURCE_COLUMNS = [
    "patient_event_key",
    "event_id",
    "update_count",
    "valid_from_datetime",
    "pacs_report_id",
    "endobase_exam_id",
    "report_version_id",
    "organization_key",
    "source_object",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "document_type",
    "title",
    "author_practitioner_id",
    "author_role",
    "service_id",
    "status_code",
    "version_id",
    "document_thread_key",
    "supersedes_document_key",
    "version_ordinal",
    "is_latest_version",
    "document_text",
    "sections",
    "parser_version",
    "decompressor_version",
    "post_processor_version",
    "content_type",
    "encoding",
    "language",
    "text_sha256",
    "raw_content_sha256",
    "text_length",
    "content_class",
    "date_quality",
    "text_is_truncated",
    "linkage_route",
    "source_class",
    "assembly_status",
    "chunk_count",
    "corpus_frequency",
    "is_boilerplate",
    "source_link_event_id",
    "source_link_system",
    "author_id_system",
    "author_source_id",
    "verified_practitioner_id",
    "verified_datetime",
    "source_organization_key",
    "source_organization_display",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "document_class",
    "contributor_system",
    "succession_status",
    "source_parent_event_id",
    "parent_relation",
    "source_parent_display",
    "source_parent_title",
    "source_parent_tag",
    "source_tag",
    "source_record_status",
    "document_text_anonymised",
    "text_is_anonymised",
    "prsb_document_type",
    "prsb_subtype",
    "prsb_standard",
    "prsb_setting",
    "prsb_map_method",
    "prsb_map_score",
    "prsb_map_version",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

DOCUMENT_THREADED_COLUMNS = [
    {
        "document_type": "_document_type_json",
        "sections": "_sections_json",
    }.get(name, name)
    for name in DOCUMENT_SOURCE_COLUMNS
] + ["_sensitivity_labels_json"]

# contract v2: publish native person and encounter identifiers for order-comment documents
def _order_comment_document_canonical():
    # The >100-character floor is deliberate: the source contains about 98M
    # near-label rows. TEXT_AVAILABLE_IND=false is honest missingness from the
    # mill_long_text feed freeze (2025-09-23), so those rows never mint documents.
    # Assemble normalized order comment document rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    c = read_source(SRC_ORDER_COMMENT).where(
        F.expr(ORDER_COMMENT_DOCUMENT_PREDICATE)
    ).alias("c")
    o = read_source(SRC_ORDERS).select(
        F.col("ORDER_ID").alias("_o_order_id"),
        F.col("PERSON_ID").alias("_o_person_id"),
        F.col("ENCNTR_ID").alias("_o_encntr_id"),
        F.col("CATALOG_DISPLAY").alias("_o_catalog_display"),
        F.col("ORDERED_AS_MNEMONIC").alias("_o_ordered_as"),
        F.col("HNA_ORDER_MNEMONIC").alias("_o_hna_mnemonic"),
        F.col("ORIG_ORDER_DT_TM_CLEAN").alias("_o_orig_dt"),
        F.col("CURRENT_START_DT_TM_CLEAN").alias("_o_start_dt"),
        F.col("SOURCE_ADC_UPDT").alias("_o_loaded_at"),
    ).alias("o")
    s = c.join(o, c.ORDER_ID == F.col("o._o_order_id"), "left")
    natural = F.concat_ws(
        ":", c.ORDER_ID.cast("string"), c.ACTION_SEQUENCE.cast("string"),
        c.COMMENT_TYPE_CD.cast("string"),
    )
    event_id = stable_id(
        "document:order_comment", c.ORDER_ID, c.ACTION_SEQUENCE, c.COMMENT_TYPE_CD
    )
    resolved_person = F.col("o._o_person_id")
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", resolved_person)], SRC_ORDER_COMMENT, natural
    )
    event_time = F.coalesce(
        c.COMMENT_DT_TM_CLEAN, c.COMMENT_UPDT_DT_TM_CLEAN,
        F.col("o._o_orig_dt"), F.col("o._o_start_dt"),
        c.COMMENT_DT_TM, c.COMMENT_UPDT_DT_TM,
    )
    loaded_at = F.greatest(
        c.SOURCE_COMMENT_ADC_UPDT, c.SOURCE_TEXT_ADC_UPDT,
        c.PIPELINE_UPDT_DT_TM, F.col("o._o_loaded_at"),
    )
    source_code = c.COMMENT_TYPE_CD.cast("string")
    source_display = F.coalesce(c.COMMENT_TYPE_DESC, F.lit("Order comment"))
    order_display = F.coalesce(
        F.col("o._o_catalog_display"), F.col("o._o_ordered_as"),
        F.col("o._o_hna_mnemonic"),
    )
    inactive = F.coalesce(c.TEXT_ACTIVE_IND.cast("long"), F.lit(1)) == 0
    return s.select(
        event_id.alias("patient_event_id"), event_id.alias("fact_row_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        resolved_person.cast("bigint").alias("person_id"),
        F.when(resolved_person.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.col("o._o_encntr_id").cast("bigint").alias("encounter_id"),
        event_time.alias("event_datetime"), F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:cerner:order-comment-type").alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:cerner:order-comment-type"), source_code,
                       source_display, True)
        ).alias("_document_type_json"),
        F.concat_ws(" — ", source_display, order_display).alias("title"),
        F.lit(None).cast("string").alias("author_practitioner_id"),
        F.lit(None).cast("string").alias("author_role"),
        F.lit(None).cast("string").alias("service_id"),
        F.when(inactive, F.lit("inactive")).otherwise(F.lit("active")).alias("status_code"),
        F.concat_ws(
            ":", c.COMMENT_UPDT_CNT.cast("string"),
            F.date_format(c.TEXT_UPDT_DT_TM, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        ).alias("version_id"),
        c.COMMENT_TEXT.alias("document_text"), F.lit("[]").alias("_sections_json"),
        F.lit("order-comment-native-v1").alias("parser_version"),
        F.lit(None).cast("string").alias("decompressor_version"),
        F.lit(None).cast("string").alias("post_processor_version"),
        F.lit("text/plain").alias("content_type"), F.lit("UTF-8").alias("encoding"),
        F.sha2(c.COMMENT_TEXT, 256).alias("text_sha256"),
        F.length(c.COMMENT_TEXT).cast("long").alias("text_length"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        event_time.alias("record_status_effective_from"),
        F.when(inactive, c.TEXT_UPDT_DT_TM_CLEAN).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("null").alias("_sensitivity_labels_json"),
        F.lit("order_comment").alias("document_class"),
        F.lit("Millennium Orders").alias("contributor_system"),
        F.lit(None).cast("string").alias("succession_status"),
        c.ORDER_ID.cast("string").alias("source_parent_event_id"),
        F.lit("order").alias("parent_relation"),
        order_display.alias("source_parent_display"),
        F.lit(None).cast("string").alias("source_parent_title"),
        F.lit(None).cast("string").alias("source_parent_tag"),
        F.lit(None).cast("string").alias("source_tag"),
        F.lit(None).cast("string").alias("source_record_status"),
        F.lit(None).cast("string").alias("document_text_anonymised"),
        F.lit(False).alias("text_is_anonymised"),
        F.lit("urn:cerner:order-comment-type").alias("_label_system"),
        c.COMMENT_TYPE_CD.cast("string").alias("_label_key"),
        F.lit("order_comment").alias("source_feed"),
        F.date_format(loaded_at, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.coalesce(c.TEXT_UPDT_DT_TM_CLEAN, c.COMMENT_UPDT_DT_TM_CLEAN)
         .alias("source_update_timestamp"),
        loaded_at.alias("loaded_at"), F.lit("millennium").alias("_source_system"),
        F.lit(SRC_ORDER_COMMENT).alias("_source_table"), natural.alias("_source_row_id"),
        F.lit(None).cast("string").alias("_raw_content_sha256"),
        F.when(resolved_person.isNotNull(), F.lit("direct"))
         .otherwise(F.lit("none")).alias("_linkage_route"),
    )

# contract v2: publish native person typing and a typed nullable encounter for elective-access documents
def _elective_access_comment_document_canonical():
    # Assemble normalized elective access comment document rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    s = read_source(SRC_EAL_COMMENT).where(
        F.expr(ELECTIVE_ACCESS_COMMENT_PREDICATE)
    ).alias("s")
    natural = F.concat_ws(
        ":", s.SOURCE_SYSTEM_OID.cast("string"), s.WAITING_LIST_OID.cast("string")
    )
    event_id = stable_id(
        "document:elective_access_comment", s.SOURCE_SYSTEM_OID, s.WAITING_LIST_OID
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_EAL_COMMENT, natural
    )
    event_time = F.coalesce(
        s.MODIFIED_DT_TM, s.CREATED_DT_TM, s.WAITING_LIST_STATUS_CHANGE_DT_TM_CLEAN,
        s.DECIDED_TO_ADMIT_DT_TM_CLEAN, s.TCI_DT_TM_CLEAN,
    )
    author_source_id = F.coalesce(s.MODIFIED_BY_PRID, s.CREATED_BY_PRID).cast("string")
    source_code = F.lit("ELECTIVE_ACCESS_COMMENT")
    source_display = F.lit("Elective access scheduling comment")
    missing = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    closed = ~F.coalesce(s.ACTIVE_IND, F.lit(True))
    return s.select(
        event_id.alias("patient_event_id"), event_id.alias("fact_row_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        event_time.alias("event_datetime"), F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:luna:eal-comment").alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:luna:eal-comment"), source_code, source_display, True)
        ).alias("_document_type_json"),
        F.concat_ws(" — ", source_display, s.WAITING_LIST_NAME).alias("title"),
        F.lit(None).cast("string").alias("author_practitioner_id"),
        F.when(author_source_id.isNotNull(), F.lit("list_updater")).alias("author_role"),
        F.lit(None).cast("string").alias("service_id"),
        F.when(missing, F.lit("absent"))
         .when(closed, F.lit("closed")).otherwise(F.lit("active")).alias("status_code"),
        s.ROW_HASH.cast("string").alias("version_id"),
        s.COMMENTS.alias("document_text"), F.lit("[]").alias("_sections_json"),
        F.lit("luna-eal-native-v1").alias("parser_version"),
        F.lit(None).cast("string").alias("decompressor_version"),
        F.lit(None).cast("string").alias("post_processor_version"),
        F.lit("text/plain").alias("content_type"), F.lit("UTF-8").alias("encoding"),
        F.sha2(s.COMMENTS, 256).alias("text_sha256"),
        F.length(s.COMMENTS).cast("long").alias("text_length"),
        F.when(missing, F.lit("retracted"))
         .when(closed, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        s.CREATED_DT_TM.alias("record_status_effective_from"),
        F.when(missing | closed, event_time).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("null").alias("_sensitivity_labels_json"),
        F.lit("waiting_list_comment").alias("document_class"),
        F.lit("LUNA elective access").alias("contributor_system"),
        F.lit(None).cast("string").alias("succession_status"),
        s.WAITING_LIST_OID.cast("string").alias("source_parent_event_id"),
        F.lit("elective_access_entry").alias("parent_relation"),
        s.WAITING_LIST_NAME.alias("source_parent_display"),
        F.lit(None).cast("string").alias("source_parent_title"),
        F.lit(None).cast("string").alias("source_parent_tag"),
        F.lit(None).cast("string").alias("source_tag"),
        F.lit(None).cast("string").alias("source_record_status"),
        F.lit(None).cast("string").alias("document_text_anonymised"),
        F.lit(False).alias("text_is_anonymised"),
        F.lit("urn:luna:eal-comment").alias("_label_system"),
        F.lit("elective_access_comment").alias("_label_key"),
        F.lit("elective_access_comment").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.MODIFIED_DT_TM.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("luna").alias("_source_system"), F.lit(SRC_EAL_COMMENT).alias("_source_table"),
        natural.alias("_source_row_id"), F.lit(None).cast("string").alias("_raw_content_sha256"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("direct"))
         .otherwise(F.lit("none")).alias("_linkage_route"),
        F.when(author_source_id.isNotNull(), F.lit("urn:luna:prid")).alias("_author_id_system"),
        author_source_id.alias("_author_source_id"),
    )

def _document_canonical():
    # Assemble normalized document rows for downstream dataset builders, preserving the existing
    # source and identity rules.
    union = (
        _document_lane(_text_document_canonical()).select(*DOCUMENT_LANE_COLUMNS)
        .unionByName(_document_lane(_mill_blob_document_canonical()).select(*DOCUMENT_LANE_COLUMNS))
        .unionByName(_document_lane(_pacs_report_document_canonical()).select(*DOCUMENT_LANE_COLUMNS))
        .unionByName(_document_lane(_endobase_document_canonical()).select(*DOCUMENT_LANE_COLUMNS))
        .unionByName(_document_lane(_order_comment_document_canonical()).select(*DOCUMENT_LANE_COLUMNS))
        .unionByName(_document_lane(_elective_access_comment_document_canonical()).select(*DOCUMENT_LANE_COLUMNS))
        .unionByName(_document_lane(_neonatal_narrative_document_canonical()).select(*DOCUMENT_LANE_COLUMNS))
        .unionByName(_document_lane(_pathology_report_document_canonical()).select(*DOCUMENT_LANE_COLUMNS))
    )
    return _document_prsb_enrich(union)




SRC_ORDER_COMMENT = "4_prod.bronze.map_order_comment"

SRC_ORDERS = "4_prod.bronze.map_orders"

SRC_DOC_TYPE_PRSB_MAP = "3_lookup.omop.doc_type_prsb_map"

SRC_PACS_REPORT = "4_prod.bronze.map_pacs_report"

SRC_DOCUMENT_PATIENT_IDENTIFIER = "4_prod.bronze.map_patient_identifier"

SRC_DOCUMENT_LINK_CONTEXT = "8_dev.bronze.document_link_context_s37"

SRC_EAL_COMMENT = "4_prod.bronze.map_elective_access_list"

SRC_PACS_TEXT_BRIDGE = "4_prod.bronze.map_pacs_report_text_bridge"

def _pathology_document_alias_map(alias_type, prefix):
    # Group current nonblank patient aliases, retaining both the candidate person and the
    # ambiguity count.
    aliases = (
        read_source(SRC_DOCUMENT_PATIENT_IDENTIFIER)
        .where((F.col("ALIAS_TYPE") == alias_type)
               & F.coalesce(F.col("CURRENT_IND"), F.lit(False))
               & _present(F.col("ALIAS_VALUE")))
        .groupBy(F.trim(F.col("ALIAS_VALUE")).alias(f"_{prefix}_value"))
        .agg(
            F.countDistinct(F.col("PERSON_ID").cast("string")).alias(f"_{prefix}_count"),
            F.max(F.col("PERSON_ID").cast("string")).alias(f"_{prefix}_person"),
        )
    )
    return aliases

def _pathology_report_document_text_rows():
    # Select pathology report versions with nonblank report text.
    r = read_source(SRC_PATHOLOGY_REPORT_VERSIONS)
    return r.where(r.report_text.isNotNull() & (F.trim(r.report_text) != ""))

DOCUMENT_PRIMITIVE_COLUMNS = [
    name for name in DOCUMENT_THREADED_COLUMNS if name != "is_latest_version"
]

# PRSB typing columns are joined on by _document_prsb_enrich, never emitted by lanes.
DOCUMENT_PRSB_COLUMNS = [
    "prsb_document_type", "prsb_subtype", "prsb_standard", "prsb_setting",
    "prsb_map_method", "prsb_map_score", "prsb_map_version",
]

DOCUMENT_HYGIENE_COLUMNS = [
    "raw_content_sha256", "content_class", "date_quality", "text_is_truncated",
    "language", "linkage_route", "source_class", "assembly_status", "chunk_count",
]

# contract v2: align document organization evidence and verifier typing with the v2 parent contracts
DOCUMENT_EVIDENCE_COLUMNS = [
    "corpus_frequency", "is_boilerplate", "source_link_event_id",
    "source_link_system", "author_id_system", "author_source_id",
    "verified_practitioner_id", "verified_datetime", "source_organization_key",
    "organization_key", "source_organization_display",
]

DOCUMENT_THREAD_COLUMNS = [
    "document_thread_key", "supersedes_document_key", "version_ordinal",
]

DOCUMENT_EVIDENCE_PRIVATE_TYPES = {
    "_linkage_route": "string",
    "_assembly_status": "string",
    "_chunk_count": "long",
    "_source_link_event_id": "string",
    "_source_link_system": "string",
    "_author_id_system": "string",
    "_author_source_id": "string",
    "_verified_practitioner_id": "long",
    "_verified_datetime": "timestamp",
    "_source_organization_id": "string",
    "_source_organization_native_id": "string",
    "_source_organization_display": "string",
}

DOCUMENT_THREAD_PRIVATE_TYPES = {
    "_document_thread_id": "string",
    "_supersedes_document_id": "string",
    "_version_ordinal": "long",
}

DOCUMENT_LANE_COLUMNS = [
    c for c in DOCUMENT_PRIMITIVE_COLUMNS
    if c not in DOCUMENT_PRSB_COLUMNS
    and c not in DOCUMENT_HYGIENE_COLUMNS
    and c not in DOCUMENT_EVIDENCE_COLUMNS
    and c not in DOCUMENT_THREAD_COLUMNS
] + [
    "_source_system", "_source_table", "_source_row_id", "_label_system", "_label_key",
    "_raw_content_sha256",
] + list(DOCUMENT_EVIDENCE_PRIVATE_TYPES) + list(DOCUMENT_THREAD_PRIVATE_TYPES)

DOCUMENT_VERSION_COLUMNS = [
    c for c in DOCUMENT_PRIMITIVE_COLUMNS if c not in DOCUMENT_THREAD_COLUMNS
] + list(DOCUMENT_THREAD_PRIVATE_TYPES)

_DOCUMENT_CONTROL_CHARS_RE = r"[\x00\x01-\x08\x0B\x0C\x0E-\x1F]"

_DOCUMENT_PUNCT_ONLY_RE = r"^[\p{P}\p{S}\s]+$"

_DOCUMENT_SHORT_CODE_RE = r"^[A-Za-z0-9][A-Za-z0-9 _/-]{0,11}$"

_DOCUMENT_PLACEHOLDERS = [
    "empty", "none", "nil", "null", "n/a", "na", "not available", "no result",
    "no data", "unknown", "not recorded", "deleted",
]

_DOCUMENT_TRUNCATION_CAPS = [1_000_000, 65_535, 32_767, 32_000]

ORDER_COMMENT_DOCUMENT_PREDICATE = (
    "TEXT_AVAILABLE_IND = true "
    "AND length(nullif(trim(COMMENT_TEXT), '')) > 100"
)

ELECTIVE_ACCESS_COMMENT_PREDICATE = (
    "length(nullif(trim(COMMENTS), '')) > 100"
)

def _document_lane(df):
    """Give every lane the same typed private enrichment surface before union."""
    # contract v2: normalise document key names, add source-arm native ids, and type native person and author columns before the lane union
    out = df
    if "patient_event_id" in out.columns:
        out = out.withColumnRenamed("patient_event_id", "patient_event_key")
    if "fact_row_id" in out.columns:
        out = out.drop("fact_row_id")
    source_object = F.when(F.col("source_feed") == "mill_blob_text", F.lit("mill_blob")).otherwise(
        F.col("source_feed")
    )
    out = (
        out.withColumn("source_object", source_object)
        .withColumn("person_id", F.col("person_id").cast("bigint"))
        .withColumn("author_practitioner_id", F.col("author_practitioner_id").cast("bigint"))
    )
    for name, data_type in {
        "event_id": "bigint",
        "update_count": "bigint",
        "valid_from_datetime": "timestamp",
        "pacs_report_id": "bigint",
        "endobase_exam_id": "bigint",
        "report_version_id": "string",
    }.items():
        if name not in out.columns:
            out = out.withColumn(name, F.lit(None).cast(data_type))
    private_types = {**DOCUMENT_EVIDENCE_PRIVATE_TYPES, **DOCUMENT_THREAD_PRIVATE_TYPES}
    for name, data_type in private_types.items():
        if name not in out.columns:
            out = out.withColumn(name, F.lit(None).cast(data_type))
    return out

def _hygiene_text(text, strip_controls):
    # Remove the configured control characters only when the caller enables cleaning for that
    # feed.
    return F.when(
        strip_controls,
        F.regexp_replace(text, _DOCUMENT_CONTROL_CHARS_RE, ""),
    ).otherwise(text)

def _document_hygiene(df):
    # Clean Cerner blob control characters and recalculate the document text-derived fields
    # consistently.
    controlled_feed = F.col("source_feed") == F.lit("mill_blob_text")
    out = (
        df.withColumn(
            "document_text",
            _hygiene_text(F.col("document_text"), controlled_feed),
        )
        .withColumn(
            "document_text_anonymised",
            _hygiene_text(F.col("document_text_anonymised"), controlled_feed),
        )
    )
    text = F.col("document_text")
    trimmed = F.trim(F.coalesce(text, F.lit("")))
    lowered = F.lower(trimmed)
    text_length = F.length(text).cast("long")
    content_class = (
        F.when(text.isNull() | (trimmed == ""), F.lit("empty"))
        .when(trimmed.rlike(_DOCUMENT_PUNCT_ONLY_RE), F.lit("punctuation_only"))
        .when(lowered.isin(*_DOCUMENT_PLACEHOLDERS), F.lit("placeholder"))
        .when((F.length(trimmed) <= 12) & trimmed.rlike(_DOCUMENT_SHORT_CODE_RE),
              F.lit("short_code"))
        .otherwise(F.lit("narrative"))
    )
    date_reference = F.coalesce(F.col("loaded_at"), F.col("source_update_timestamp"))
    date_quality = (
        F.when(F.col("event_datetime").isNull(), F.lit("null"))
        .when(F.col("event_datetime") < F.lit("1975-01-01").cast("timestamp"),
              F.lit("epoch_sentinel"))
        .when(
            date_reference.isNotNull()
            & (F.col("event_datetime") > date_reference + F.expr("INTERVAL 1 DAY")),
            F.lit("future"),
        )
        .otherwise(F.lit("ok"))
    )
    post_processor_version = F.when(
        controlled_feed,
        F.when(F.col("post_processor_version").isNull(), F.lit("silver-control-strip-v1"))
        .when(F.col("post_processor_version").contains("silver-control-strip-v1"),
              F.col("post_processor_version"))
        .otherwise(F.concat(F.col("post_processor_version"),
                            F.lit("+silver-control-strip-v1"))),
    ).otherwise(F.col("post_processor_version"))
    source_class = F.lit("clinical")
    assembly_status = F.coalesce(F.col("_assembly_status"), F.lit("single"))
    chunk_count = F.coalesce(F.col("_chunk_count"), F.lit(1).cast("long"))
    tombstone = lowered == F.lit("deleted")
    return (
        out.withColumn("post_processor_version", post_processor_version)
        .withColumn("encoding", F.coalesce(F.col("encoding"), F.lit("UTF-8")))
        .withColumn("language", F.lit("en"))
        .withColumn("text_sha256", F.sha2(F.coalesce(text, F.lit("")), 256))
        .withColumn("raw_content_sha256", F.col("_raw_content_sha256"))
        .withColumn("text_length", text_length)
        .withColumn("content_class", content_class)
        .withColumn("date_quality", date_quality)
        .withColumn("text_is_truncated", F.coalesce(text_length.isin(*_DOCUMENT_TRUNCATION_CAPS),
                                                    F.lit(False)))
        .withColumn("linkage_route", F.coalesce(F.col("_linkage_route"), F.lit("none")))
        .withColumn("source_class", source_class)
        .withColumn("assembly_status", assembly_status)
        .withColumn("chunk_count", chunk_count)
        .withColumn("record_status", F.when(tombstone, F.lit("retracted"))
                    .otherwise(F.col("record_status")))
        .withColumn(
            "record_status_effective_to",
            F.when(tombstone, F.coalesce(F.col("record_status_effective_to"),
                                         F.col("source_update_timestamp"), F.col("loaded_at")))
            .otherwise(F.col("record_status_effective_to")),
        )
    )

def _document_prsb_enrich(df):
    """LEFT-join the governed PRSB doc-type lookup and finalise document typing.
    Runs at the canonical layer: _document_type_json stays a JSON string across the
    join (VARIANT never crosses a join boundary) and is recomputed here from the
    scalar source columns so every lane gets the mapped coding appended uniformly."""
    df = _document_hygiene(df)
    m = read_source(SRC_DOC_TYPE_PRSB_MAP).select(
        F.col("label_system").alias("_m_label_system"),
        F.col("label_key").alias("_m_label_key"),
        F.col("canonical_name").alias("prsb_document_type"),
        F.col("subtype").alias("prsb_subtype"),
        F.col("prsb_standard"),
        F.col("prsb_setting"),
        F.col("method").alias("prsb_map_method"),
        F.col("score").cast("double").alias("prsb_map_score"),
        F.col("map_version").alias("prsb_map_version"),
    )
    j = df.join(
        m,
        (df["_label_system"] == m["_m_label_system"])
        & (df["_label_key"] == m["_m_label_key"]),
        "left",
    )
    prsb_display = F.initcap(F.regexp_replace(F.col("prsb_document_type"), "_", " "))
    mapped = F.struct(
        F.lit("urn:barts:prsb-doc-type").cast("string").alias("coding_system"),
        F.col("prsb_document_type").cast("string").alias("coding_code"),
        prsb_display.cast("string").alias("coding_display"),
        F.lit(False).alias("is_source"),
        F.when(F.col("prsb_document_type").isNotNull(),
               F.lit("doc_type_prsb_map")).cast("string").alias("map_source"),
        F.col("prsb_map_version").cast("string").alias("map_version"),
    )
    source = coding_obj(F.col("source_coding_system"), F.col("source_code"),
                        F.col("source_display"), True)
    typed = j.withColumn("_document_type_json", codeable_concept_json(source, mapped))
    return _document_evidence_enrich(typed)

def _document_evidence_enrich(df):
    """Attach organization evidence; corpus frequency is calculated after all lanes union."""
    # contract v2: join organization evidence on native organization_id and publish retained SHA keys explicitly
    organization = spark.read.table(_n("journey_reference.organization")).select(
        F.col("organization_id").cast("string").alias("_mill_organization_native_id"),
        F.col("organization_key").alias("_mill_organization_key"),
        F.col("name").alias("_mill_organization_display"),
    )
    enriched = df.join(
        organization,
        F.col("_source_organization_native_id")
        == F.col("_mill_organization_native_id"),
        "left",
    )
    return (
        enriched
        .withColumn("corpus_frequency", F.lit(None).cast("long"))
        .withColumn("is_boilerplate", F.lit(False))
        .withColumn("source_link_event_id", F.col("_source_link_event_id"))
        .withColumn("source_link_system", F.col("_source_link_system"))
        .withColumn("author_id_system", F.col("_author_id_system"))
        .withColumn("author_source_id", F.col("_author_source_id"))
        .withColumn("verified_practitioner_id", F.col("_verified_practitioner_id").cast("bigint"))
        .withColumn("verified_datetime", F.col("_verified_datetime"))
        .withColumn(
            "source_organization_key",
            F.coalesce(F.col("_source_organization_id"), F.col("_mill_organization_key")),
        )
        .withColumn("organization_key", F.col("source_organization_key"))
        .withColumn(
            "source_organization_display",
            F.coalesce(
                F.col("_source_organization_display"),
                F.col("_mill_organization_display"),
            ),
        )
    )

# contract v2: consume the renamed clinical-finding event key and native identifiers in the text-event document lane
def _text_document_canonical():
    # Assemble normalized text document rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = _text_finding_canonical().where(F.col("_route") == "document_candidate").alias("s")
    return s.select(
        "patient_event_key", "subject_key", "subject_id_system", "person_id",
        "identity_status", "encounter_id", "event_datetime", "event_end_datetime",
        "source_coding_system", "source_code", "source_display",
        codeable_concept_json(
            coding_obj(F.col("source_coding_system"), F.col("source_code"),
                       F.col("source_display"), True)
        ).alias("_document_type_json"),
        F.col("source_display").alias("title"),
        F.col("performer_practitioner_id").alias("author_practitioner_id"),
        F.lit("performer").alias("author_role"),
        F.lit(None).cast("string").alias("service_id"),
        F.col("result_status_code").alias("status_code"),
        F.col("_source_row_id").alias("version_id"),
        F.col("value_text").alias("document_text"),
        F.lit("[]").alias("_sections_json"),
        F.lit(None).cast("string").alias("parser_version"),
        F.lit(None).cast("string").alias("decompressor_version"),
        F.lit(None).cast("string").alias("post_processor_version"),
        F.lit("text/plain").alias("content_type"),
        F.lit(None).cast("string").alias("encoding"),
        F.sha2(F.coalesce(F.col("value_text"), F.col("patient_event_key")), 256)
         .alias("text_sha256"),
        F.length(F.col("value_text")).cast("long").alias("text_length"),
        "record_status", "record_status_effective_from", "record_status_effective_to",
        "confidentiality_code", "vip_ind", "withheld_identity_ind",
        F.lit("null").alias("_sensitivity_labels_json"),
        F.lit(None).cast("string").alias("document_class"),
        F.lit(None).cast("string").alias("contributor_system"),
        F.lit(None).cast("string").alias("succession_status"),
        F.col("parent_event_id").alias("source_parent_event_id"),
        F.lit(None).cast("string").alias("parent_relation"),
        F.lit(None).cast("string").alias("source_parent_display"),
        F.lit(None).cast("string").alias("source_parent_title"),
        F.lit(None).cast("string").alias("source_parent_tag"),
        F.lit(None).cast("string").alias("source_tag"),
        F.lit(None).cast("string").alias("source_record_status"),
        F.lit(None).cast("string").alias("document_text_anonymised"),
        F.lit(False).alias("text_is_anonymised"),
        F.lit("urn:cerner:code_value:pair").alias("_label_system"),
        F.concat_ws(":",
            F.coalesce(F.col("_parent_event_cd"), F.col("source_code")),
            F.col("source_code")).alias("_label_key"),
        F.lit("text_event").alias("source_feed"), "load_batch_id", "source_update_timestamp", "loaded_at", "_source_system", "_source_table",
        "_source_row_id", F.lit(None).cast("string").alias("_raw_content_sha256"),
        F.lit("direct").alias("_linkage_route"),
    )

# contract v2: publish Millennium document EVENT_ID and native encounter, author, and verifier identifiers
def _mill_blob_document_canonical():
    # Assemble normalized mill blob document rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    raw = read_source(SRC_MILL_BLOB_TEXT).where(F.col("STATUS") == F.lit("Decoded"))
    source_hash = F.sha2(F.to_json(F.struct(*[raw[c] for c in raw.columns])), 256)
    # Source occasionally contains more than one physical extraction row for the same governed
    # document-version key. Collapse those rows with an incrementalizable aggregate rather than a
    # row_number window: latest source/update timestamp wins, then the full-row hash breaks ties.
    # The selected value is the complete source struct, so no column is silently re-aggregated.
    order_key = F.concat_ws(
        "|",
        F.coalesce(F.date_format("UPDT_DT_TM", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), F.lit("~")),
        F.coalesce(F.date_format("ADC_UPDT", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), F.lit("~")),
        source_hash,
    )
    source_json = F.to_json(
        F.struct(*[raw[c] for c in raw.columns]), {"ignoreNullFields": "false"}
    )
    # MAX over an orderable struct is deterministic and incrementally composable. The winning
    # source row is round-tripped through its exact Spark schema after selection.
    selected_row = F.max(
        F.struct(order_key.alias("order_key"), source_json.alias("source_json"))
    )
    b = (
        raw.groupBy("EVENT_ID", "UPDT_CNT", "VALID_FROM_DT_TM")
        .agg(selected_row.alias("_selected"))
        .select(F.from_json(F.col("_selected.source_json"), raw.schema).alias("_source"))
        .select("_source.*")
        .alias("b")
    )
    e = read_source(SRC_ENCOUNTER).select(
        F.col("ENCNTR_ID").alias("_context_encounter_id"),
        F.col("PERSON_ID").alias("_context_person_id"),
    ).alias("e")
    s = b.join(e, b.ENCNTR_ID == e._context_encounter_id, "left")
    natural = F.concat_ws(
        ":", b.EVENT_ID.cast("string"), b.UPDT_CNT.cast("string"),
        F.date_format(b.VALID_FROM_DT_TM, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
    )
    event_id = stable_id(
        "document:mill_blob", b.EVENT_ID, b.UPDT_CNT, b.VALID_FROM_DT_TM
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", e._context_person_id)], SRC_MILL_BLOB_TEXT, natural
    )
    text_value = b.BLOB_TEXT
    anon_fallback = F.lit(False)
    ended = _closed_timestamp(b.VALID_UNTIL_DT_TM)
    # code set 8: 29/30/31 all decode to 'In Error' — surfaced as retracted, never dropped
    # (the legacy RDE extract silently filters these rows out).
    in_error = b.EVENT_RESULT_STATUS_CD.isin(29, 30, 31)
    record_status = (
        F.when(in_error, F.lit("retracted"))
        .when(ended, F.lit("superseded"))
        .otherwise(F.lit("active"))
    )
    version_time = F.coalesce(b.VALID_FROM_DT_TM, b.UPDT_DT_TM, b.ADC_UPDT)
    # Clinical time preferred; version/lifecycle time retained on record_status_effective_from.
    event_time = F.coalesce(b.CLINSIG_DT_TM, version_time)
    # Label columns are bronze-enriched (Blob 5:Labels); fail OPEN when enrichment
    # hasn't landed for a row — the document still publishes with content-type typing.
    has_event = b.EVENT_CD.isNotNull()
    label_display = F.coalesce(b.EVENT_CD_DISPLAY, b.EVENT_CD_DESC)
    performed_id = F.when(_usable_code(b.PERFORMED_PRSNL_ID),
                          b.PERFORMED_PRSNL_ID.cast("string"))
    updater_id = F.when(_usable_code(b.UPDT_ID), b.UPDT_ID.cast("string"))
    author_source_id = F.coalesce(performed_id, updater_id)
    verifier_source_id = F.when(_usable_code(b.VERIFIED_PRSNL_ID),
                                b.VERIFIED_PRSNL_ID.cast("string"))
    organization_native_id = F.when(_usable_code(b.ORGANIZATION_ID),
                                    b.ORGANIZATION_ID.cast("string"))
    blob_thread_key = F.when(
        _present(b.SERIES_REF_NBR), F.trim(b.SERIES_REF_NBR.cast("string"))
    ).otherwise(b.EVENT_ID.cast("string"))
    return s.select(
        event_id.alias("patient_event_id"), event_id.alias("fact_row_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        e._context_person_id.cast("string").alias("person_id"),
        F.when(e._context_person_id.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        b.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        event_time.alias("event_datetime"),
        F.when(ended, b.VALID_UNTIL_DT_TM).alias("event_end_datetime"),
        F.when(has_event, F.lit("urn:cerner:code_value"))
         .otherwise(F.lit("urn:cerner:blob-content-type")).alias("source_coding_system"),
        F.coalesce(b.EVENT_CD.cast("string"), b.CONTENT_TYPE).alias("source_code"),
        F.coalesce(label_display, b.CONTENT_TYPE).alias("source_display"),
        codeable_concept_json(
            coding_obj(
                F.when(has_event, F.lit("urn:cerner:code_value"))
                 .otherwise(F.lit("urn:cerner:blob-content-type")),
                F.coalesce(b.EVENT_CD.cast("string"), b.CONTENT_TYPE),
                F.coalesce(label_display, b.CONTENT_TYPE), True)
        ).alias("_document_type_json"),
        F.coalesce(
            b.EVENT_TITLE_TEXT,
            F.concat_ws(" ", F.lit("Millennium document"), b.EVENT_ID.cast("string")),
        ).alias("title"),
        author_source_id.cast("bigint").alias("author_practitioner_id"),
        F.when(performed_id.isNotNull(), F.lit("performer"))
         .when(updater_id.isNotNull(), F.lit("updater")).alias("author_role"),
        F.lit(None).cast("string").alias("service_id"), b.STATUS.alias("status_code"),
        natural.alias("version_id"),
        b.EVENT_ID.cast("bigint").alias("event_id"),
        b.UPDT_CNT.cast("bigint").alias("update_count"),
        b.VALID_FROM_DT_TM.alias("valid_from_datetime"),
        text_value.alias("document_text"),
        F.lit("[]").alias("_sections_json"),
        b.parser_version.cast("string").alias("parser_version"),
        b.decompressor_version.cast("string").alias("decompressor_version"),
        b.post_processor_version.cast("string").alias("post_processor_version"),
        b.CONTENT_TYPE.alias("content_type"), b.ENCODING.alias("encoding"),
        F.coalesce(b.raw_sha256, F.sha2(text_value, 256), F.sha2(natural, 256))
         .alias("text_sha256"),
        F.coalesce(b.TEXT_LENGTH, F.length(text_value).cast("long")).alias("text_length"),
        record_status.alias("record_status"),
        version_time.alias("record_status_effective_from"),
        F.when(ended, b.VALID_UNTIL_DT_TM).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("null").alias("_sensitivity_labels_json"),
        b.EVENT_CLASS_DISPLAY.alias("document_class"),
        b.CONTRIBUTOR_SYSTEM_DISPLAY.alias("contributor_system"),
        b.SUCCESSION_TYPE_DISPLAY.alias("succession_status"),
        b.PARENT_EVENT_ID.cast("string").alias("source_parent_event_id"),
        b.EVENT_RELTN_DISPLAY.alias("parent_relation"),
        b.PARENT_EVENT_CD_DESC.alias("source_parent_display"),
        b.PARENT_EVENT_TITLE_TEXT.alias("source_parent_title"),
        b.PARENT_EVENT_TAG.alias("source_parent_tag"),
        b.EVENT_TAG.alias("source_tag"),
        b.EVENT_RECORD_STATUS_DISPLAY.alias("source_record_status"),
        F.lit(None).cast("string").alias("document_text_anonymised"),
        anon_fallback.alias("text_is_anonymised"),
        F.when(has_event, F.lit("urn:cerner:code_value:pair")).alias("_label_system"),
        F.when(has_event,
               F.concat_ws(":", F.coalesce(b.PARENT_EVENT_CD, b.EVENT_CD).cast("string"),
                           b.EVENT_CD.cast("string"))).alias("_label_key"),
        F.lit("mill_blob_text").alias("source_feed"),
        F.date_format(b.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        b.UPDT_DT_TM.alias("source_update_timestamp"),
        b.ADC_UPDT.alias("loaded_at"), F.lit("millennium").alias("_source_system"),
        F.lit(SRC_MILL_BLOB_TEXT).alias("_source_table"), natural.alias("_source_row_id"),
        b.raw_sha256.alias("_raw_content_sha256"),
        F.when(author_source_id.isNotNull(), F.lit("urn:cerner:prsnl_id"))
         .alias("_author_id_system"),
        author_source_id.alias("_author_source_id"),
        verifier_source_id.cast("bigint").alias("_verified_practitioner_id"),
        F.when(verifier_source_id.isNotNull(), b.VERIFIED_DT_TM)
         .alias("_verified_datetime"),
        organization_native_id.alias("_source_organization_native_id"),
        F.when(e._context_person_id.isNotNull(), F.lit("encounter_join"))
         .when(b.ENCNTR_ID.isNotNull(), F.lit("direct"))
         .otherwise(F.lit("none")).alias("_linkage_route"),
        stable_id("document_thread:mill_blob", blob_thread_key)
         .alias("_document_thread_id"),
        F.lit(None).cast("string").alias("_supersedes_document_id"),
        b.UPDT_CNT.cast("long").alias("_version_ordinal"),
    )

# contract v2: publish PACS_REPORT_ID and native person and encounter identifiers for the PACS report arm
def _pacs_report_document_canonical():
    # Assemble normalized pacs report document rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    r = read_source(SRC_PACS_REPORT).alias("r")
    # PACS integration v1: bridge v4 keeps source-absent rows as tombstones with their old text.
    b = read_source(SRC_PACS_TEXT_BRIDGE).where(
        F.coalesce(F.col("SOURCE_PRESENT_IND"), F.lit(True))
    ).select(
        F.col("REPORT_ID").alias("_bridge_report_id"),
        F.col("EVENT_ID").alias("_bridge_event_id"),
        F.col("BRIDGED_TEXT").alias("_bridged_text"),
        F.col("BRIDGED_TEXT_FORMAT").alias("_bridged_format"),
        F.col("BRIDGED_TEXT_PARSER_VERSION").alias("_bridged_parser"),
    ).alias("b")
    r = r.join(b, r.PACS_REPORT_ID == F.col("_bridge_report_id"), "left")
    examination = (
        read_source(SRC_PACS_EXAMINATION)
        .groupBy("PACS_EXAMINATION_ID")
        .agg(F.max(F.when(_present(F.col("INSTITUTION")), F.trim("INSTITUTION")))
             .alias("_exam_institution"))
        .select(
            F.col("PACS_EXAMINATION_ID").alias("_exam_id"),
            "_exam_institution",
        )
    )
    r = r.join(examination, r.PACS_EXAMINATION_ID == F.col("_exam_id"), "left")
    event_context = (
        read_source(SRC_DOCUMENT_LINK_CONTEXT)
        .where(F.col("source_kind") == "clinical_event")
        .select(
            F.col("source_id").alias("_event_context_id"),
            F.col("person_id").alias("_event_context_person_id"),
            F.col("encntr_id").alias("_event_context_encntr_id"),
        )
    )
    r = r.join(
        event_context,
        F.col("_bridge_event_id") == F.col("_event_context_id"),
        "left",
    )
    context_compatible = (
        r.PERSON_ID.isNull() | F.col("_event_context_person_id").isNull()
        | (r.PERSON_ID.cast("string") == F.col("_event_context_person_id").cast("string"))
    )
    resolved_person = F.coalesce(r.PERSON_ID, F.col("_event_context_person_id"))
    resolved_encounter = F.when(context_compatible, F.col("_event_context_encntr_id"))
    event_id = stable_id("document:pacs_report", r.PACS_REPORT_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", resolved_person),
         ("urn:sectra:pacs-patient-id", r.PACS_PATIENT_ID)],
        SRC_PACS_REPORT,
        r.PACS_REPORT_ID,
    )
    retracted = F.coalesce(~r.SOURCE_PRESENT_IND, F.lit(False))
    event_time = F.coalesce(r.REPORT_DT_TM, r.REPORT_MODIFIED_UTC, r.ADC_UPDT)
    source_code = F.coalesce(r.EXAM_CODE, r.REPORT_TEXT_FORMAT, F.lit("IMAGING_REPORT"))
    source_display = F.concat_ws(" ", r.EXAM_MODALITY, r.EXAM_CODE, F.lit("imaging report"))
    native_text = F.when(
        r.REPORT_TEXT.isNotNull() & (F.trim(r.REPORT_TEXT) != ""), r.REPORT_TEXT
    )
    source_document_text = F.coalesce(native_text, F.col("_bridged_text"))
    source_document_text_anonymised = F.lit(None).cast("string")
    text_format = F.when(native_text.isNotNull(), r.REPORT_TEXT_FORMAT).otherwise(
        F.col("_bridged_format")
    )
    is_rtf = F.upper(F.trim(F.coalesce(text_format, F.lit("")))).isin(
        "RTF", "APPLICATION/RTF", "TEXT/RTF"
    )
    document_text = F.when(
        is_rtf & source_document_text.isNotNull(), _strip_rtf_text(source_document_text)
    ).otherwise(source_document_text)
    document_text_anonymised = source_document_text_anonymised
    parser_version = (
        F.when(is_rtf & source_document_text.isNotNull(), F.lit(_RTF_PARSER_VERSION))
        .when(native_text.isNotNull(), F.lit("pacs-native-v1"))
        .when(
            F.col("_bridged_text").isNotNull(),
            F.coalesce(F.col("_bridged_parser"), F.lit("pacs-bridge-v1")),
        )
        .otherwise(F.lit("pacs-native-v1"))
    )
    report_doctor_id = F.when(_usable_code(r.REPORT_DOCTOR_ID),
                              r.REPORT_DOCTOR_ID.cast("string"))
    linked_event_id = F.when(_present(F.col("_bridge_event_id")),
                             F.trim(F.col("_bridge_event_id")))
    institution = F.when(_present(F.col("_exam_institution")),
                         F.trim(F.col("_exam_institution")))
    return r.select(
        event_id.alias("patient_event_id"), event_id.alias("fact_row_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        resolved_person.cast("bigint").alias("person_id"),
        F.when(resolved_person.isNotNull(), F.lit("resolved"))
        .when(r.PACS_PATIENT_ID.isNotNull(), F.lit("provisional"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        resolved_encounter.cast("bigint").alias("encounter_id"),
        event_time.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:sectra:imaging-report").alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:sectra:imaging-report"), source_code,
                       source_display, True)
        ).alias("_document_type_json"),
        F.concat_ws(" ", F.lit("Imaging report"), r.EXAM_CODE,
                    r.PACS_REPORT_ID.cast("string")).alias("title"),
        F.lit(None).cast("string").alias("author_practitioner_id"),
        F.when(report_doctor_id.isNotNull(), F.lit("reporting_doctor"))
         .alias("author_role"),
        F.lit(None).cast("string").alias("service_id"),
        F.when(document_text.isNull() | (F.trim(document_text) == ""),
               F.lit("no_text_at_source"))
        .otherwise(r.REPORT_STATUS_CD.cast("string")).alias("status_code"),
        F.concat_ws(":", r.PACS_REPORT_ID.cast("string"),
                    F.date_format(r.REPORT_MODIFIED_UTC, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
        .alias("version_id"),
        r.PACS_REPORT_ID.cast("bigint").alias("pacs_report_id"),
        document_text.alias("document_text"), F.lit("[]").alias("_sections_json"),
        parser_version.alias("parser_version"),
        F.lit(None).cast("string").alias("decompressor_version"),
        F.lit(None).cast("string").alias("post_processor_version"),
        F.when(is_rtf, F.lit("application/rtf"))
        .otherwise(F.lit("text/plain")).alias("content_type"),
        F.lit("UTF-8").alias("encoding"),
        F.sha2(F.coalesce(document_text, r.PACS_REPORT_ID.cast("string")), 256)
        .alias("text_sha256"),
        F.length(document_text).cast("long").alias("text_length"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
        .alias("record_status"),
        r.SRC_ADC_UPDT.alias("record_status_effective_from"),
        F.when(retracted, r.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("null").alias("_sensitivity_labels_json"),
        F.lit(None).cast("string").alias("document_class"),
        F.lit(None).cast("string").alias("contributor_system"),
        F.when(r.FINAL_SIGNATURE_DT_TM.isNotNull(), F.lit("final"))
         .when(r.PRELIM_SIGNATURE_DT_TM.isNotNull(), F.lit("preliminary"))
         .alias("succession_status"),
        F.lit(None).cast("string").alias("source_parent_event_id"),
        F.lit(None).cast("string").alias("parent_relation"),
        F.lit(None).cast("string").alias("source_parent_display"),
        F.lit(None).cast("string").alias("source_parent_title"),
        F.lit(None).cast("string").alias("source_parent_tag"),
        F.lit(None).cast("string").alias("source_tag"),
        F.lit(None).cast("string").alias("source_record_status"),
        document_text_anonymised.alias("document_text_anonymised"),
        F.lit(False).alias("text_is_anonymised"),
        F.lit("urn:sectra:imaging-modality").alias("_label_system"),
        F.coalesce(F.upper(F.trim(r.EXAM_MODALITY)), F.lit("IMAGING")).alias("_label_key"),
        F.lit("pacs_report").alias("source_feed"),
        F.date_format(r.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        r.REPORT_MODIFIED_UTC.alias("source_update_timestamp"),
        r.ADC_UPDT.alias("loaded_at"), F.lit("sectra-pacs").alias("_source_system"),
        F.lit(SRC_PACS_REPORT).alias("_source_table"),
        r.PACS_REPORT_ID.cast("string").alias("_source_row_id"),
        F.lit(None).cast("string").alias("_raw_content_sha256"),
        linked_event_id.alias("_source_link_event_id"),
        F.when(linked_event_id.isNotNull(), F.lit("urn:cerner:event_id"))
         .alias("_source_link_system"),
        F.when(report_doctor_id.isNotNull(), F.lit("urn:sectra:doctor_id"))
         .alias("_author_id_system"),
        report_doctor_id.alias("_author_source_id"),
        F.when(institution.isNotNull(), stable_id("organization:pacs", institution))
         .alias("_source_organization_id"),
        institution.alias("_source_organization_display"),
        F.when(resolved_encounter.isNotNull()
               | (r.PERSON_ID.isNull() & F.col("_event_context_person_id").isNotNull()),
               F.lit("bridge_event"))
         .when(r.PERSON_ID.isNotNull(), F.lit("direct"))
         .otherwise(F.lit("none")).alias("_linkage_route"),
        stable_id("document_thread:pacs_report", r.PACS_REPORT_ID)
         .alias("_document_thread_id"),
        F.lit(None).cast("string").alias("_supersedes_document_id"),
        F.lit(1).cast("long").alias("_version_ordinal"),
    )


SRC_MAT_PREGNANCY = "4_prod.bronze.map_mat_pregnancy"




# ==== Scheduling activity and SurgiNet case procedures ====

# contract v2: retain the full canonical shape for internal reuse.
APPOINTMENT_SOURCE_COLUMNS = [
    "patient_event_key",
    "sch_event_id",
    "recurrence_parent_sch_event_id",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "appointment_type_code",
    "appointment_type_display",
    "status_code",
    "status_display",
    "status_meaning",
    "referral_identifier",
    "requested_datetime",
    "original_requested_start",
    "original_requested_end",
    "first_booked_datetime",
    "requested_practitioner_id",
    "allocated_practitioner_id",
    "location_code",
    "organization_id",
    "recurrence_parent_key",
    "recurrence_type_flag",
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

def _appointment_canonical():
    # Assemble normalized appointment rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    a = read_source(SRC_APPOINTMENT).alias("a")
    s = spark.read.table(_n("journey_clinical._appointment_schedule_grouped")).alias("s")
    r = spark.read.table(_n("journey_clinical._appointment_resource_grouped")).alias("r")
    joined = a.join(s, "SCH_EVENT_ID", "left").join(r, "SCH_EVENT_ID", "left")
    event_id = stable_id("appointment:mill_scheduling", a.SCH_EVENT_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", a.PERSON_ID)], SRC_APPOINTMENT, a.SCH_EVENT_ID
    )
    source_present = F.coalesce(a.SOURCE_PRESENT_IND.cast("boolean"), F.lit(True))
    inactive = (
        (~source_present)
        | (F.coalesce(a.ACTIVE_IND.cast("long"), F.lit(1)) == 0)
        | ((a.END_EFFECTIVE_DT_TM.isNotNull())
           & (a.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp")))
    )
    loaded_at = F.greatest(a.ADC_UPDT, F.col("s._schedule_loaded_at"),
                           F.col("r._resource_loaded_at"))
    source_update = F.greatest(a.SOURCE_ADC_UPDT, F.col("s._schedule_source_update"),
                               F.col("r._resource_source_update"))
    location_code = F.coalesce(F.col("r._resource_location_cd"),
                               F.col("s._schedule_location_cd"))
    # contract v2: publish SCH_EVENT_ID, native foreign keys, and explicit recurrence SHA/native identities
    return joined.select(
        event_id.alias("patient_event_key"),
        a.SCH_EVENT_ID.cast("bigint").alias("sch_event_id"),
        a.RECUR_PARENT_ID.cast("bigint").alias("recurrence_parent_sch_event_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        a.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(a.PERSON_ID.isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
        .alias("identity_status"),
        a.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        F.coalesce(F.col("r._slot_start"), a.ORIG_REQ_START_DT_TM,
                   a.FIRST_BKD_ASI_DT_TM, a.REFER_DT_TM).alias("event_datetime"),
        F.col("r._slot_end").alias("event_end_datetime"),
        F.lit("urn:cerner:scheduling:appointment-type").alias("source_coding_system"),
        a.APPT_TYPE_CD.cast("string").alias("source_code"),
        F.coalesce(a.APPT_TYPE_DESCRIPTION, a.APPT_SYNONYM_DESCRIPTION).alias("source_display"),
        a.APPT_TYPE_CD.cast("string").alias("appointment_type_code"),
        F.coalesce(a.APPT_TYPE_DESCRIPTION, a.APPT_SYNONYM_DESCRIPTION)
        .alias("appointment_type_display"),
        a.SCH_STATE_CD.cast("string").alias("status_code"),
        a.SCH_STATE_DESCRIPTION.alias("status_display"),
        a.SOURCE_SCHEDULE_MEANING.alias("status_meaning"),
        a.REFERRAL_IDENT.alias("referral_identifier"), a.REFER_DT_TM.alias("requested_datetime"),
        a.ORIG_REQ_START_DT_TM.alias("original_requested_start"),
        a.ORIG_REQ_END_DT_TM.alias("original_requested_end"),
        a.FIRST_BKD_ASI_DT_TM.alias("first_booked_datetime"),
        F.coalesce(F.col("s._booking_json"), F.lit("[]"))
        .alias("_booking_iterations_json"),
        F.coalesce(F.col("r._resource_json"), F.lit("[]"))
        .alias("_resource_history_json"),
        a.REQUESTED_PERSONNEL_ID.cast("bigint").alias("requested_practitioner_id"),
        F.col("r._allocated_personnel_id").cast("bigint").alias("allocated_practitioner_id"),
        location_code.cast("string").alias("location_code"),
        a.ORGANIZATION_ID.cast("bigint").alias("organization_id"),
        F.when(a.RECUR_PARENT_ID.isNotNull(),
               stable_id("appointment:mill_scheduling", a.RECUR_PARENT_ID))
        .alias("recurrence_parent_key"),
        a.RECUR_TYPE_FLAG.cast("long").alias("recurrence_type_flag"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        a.BEG_EFFECTIVE_DT_TM.alias("record_status_effective_from"),
        F.when(inactive, F.coalesce(a.SOURCE_ABSENT_DETECTED_TS, a.END_EFFECTIVE_DT_TM,
                                    a.ADC_UPDT)).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("appointment").alias("source_feed"),
        F.date_format(loaded_at, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"), loaded_at.alias("loaded_at"),
        F.lit("millennium-scheduling").alias("_source_system"),
        F.lit(SRC_APPOINTMENT).alias("_source_table"),
        a.SCH_EVENT_ID.cast("string").alias("_source_row_id"),
    )


APPOINTMENT_PRIMITIVE_COLUMNS = [
    {
        "booking_iterations": "_booking_iterations_json",
        "resource_history": "_resource_history_json",
    }.get(name, name)
    for name in APPOINTMENT_SOURCE_COLUMNS
]




def _referral_canonical():
    # Assemble normalized referral rows for downstream dataset builders, preserving the existing
    # source and identity rules.
    s = read_source(SRC_REFERRAL).alias("s")
    event_id = stable_id("referral:luna", s.SOURCE_SYSTEM_OID, s.REFERRAL_OID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_REFERRAL,
        F.concat_ws(":", s.SOURCE_SYSTEM_OID.cast("string"), s.REFERRAL_OID.cast("string")),
    )
    source_present = F.coalesce(s.SOURCE_PRESENT_IND.cast("boolean"), F.lit(True))
    inactive = (~source_present) | (
        F.coalesce(s.ACTIVE_IND.cast("boolean"), F.lit(True)) == F.lit(False)
    )
    # contract v2: publish the LUNA composite native key and native-typed person and encounter identifiers
    return s.select(
        event_id.alias("patient_event_key"),
        s.SOURCE_SYSTEM_OID.cast("bigint").alias("source_system_oid"),
        s.REFERRAL_OID.cast("bigint").alias("referral_oid"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
        .alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.REFERRAL_RECEIVED_DATETIME.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:barts:luna:treatment-function").alias("source_coding_system"),
        s.TREATMENT_FUNCTION_CD.alias("source_code"),
        s.TREATMENT_FUNCTION_DESC.alias("source_display"),
        s.UBRN.alias("ubrn"),
        s.WAITING_LIST_OID.cast("string").alias("waiting_list_oid"),
        s.PATHWAY_OID.cast("string").alias("pathway_oid"),
        s.REFERRAL_PRIORITY_CD.alias("referral_priority_code"),
        s.REFERRAL_PRIORITY_DESC.alias("referral_priority_display"),
        s.REFERRAL_SOURCE_CD.alias("referral_source_code"),
        s.REFERRAL_SOURCE_DESC.alias("referral_source_display"),
        s.REFERRAL_STATUS_CD.alias("status_code"),
        s.REFERRAL_STATUS_DESC.alias("status_display"),
        s.REFERRAL_STATUS_CHANGE_REASON_CD.alias("status_change_reason_code"),
        s.REFERRAL_STATUS_CHANGE_REASON_DESC.alias("status_change_reason_display"),
        s.REFERRAL_STATUS_CHANGE_DATETIME.alias("status_change_datetime"),
        s.ENCOUNTER_TYPE_CD.alias("encounter_type_code"),
        s.ENCOUNTER_TYPE_DESC.alias("encounter_type_display"),
        s.SUSPECTED_CANCER_SITE_CD.alias("suspected_cancer_site_code"),
        s.SUSPECTED_CANCER_SITE_DESC.alias("suspected_cancer_site_display"),
        s.TREATMENT_FUNCTION_CD.alias("treatment_function_code"),
        s.TREATMENT_FUNCTION_DESC.alias("treatment_function_display"),
        s.SERVICE_TYPE_REQUESTED_CD.alias("service_type_requested_code"),
        s.SERVICE_TYPE_REQUESTED_DESC.alias("service_type_requested_display"),
        s.SITE_CD.alias("site_code"), s.SITE_DESC.alias("site_display"),
        s.REFERRING_FACILITY_CD.alias("referring_facility_code"),
        s.REFERRING_FACILITY_DESC.alias("referring_facility_display"),
        s.REFERRED_BY_ORG_ID.cast("long").alias("referred_by_org_id"),
        s.BOOKING_TYPE_CD.alias("booking_type_code"),
        s.BOOKING_TYPE_DESC.alias("booking_type_display"),
        s.ADMIN_CATEGORY_CD.alias("admin_category_code"),
        s.ADMIN_CATEGORY_DESC.alias("admin_category_display"),
        s.BUSINESS_UNIT.alias("business_unit"), s.DIVISION.alias("division"),
        s.ORIGINAL_REFERRAL_RECEIVED_DATETIME.alias("original_received_datetime"),
        s.ERS_UBRN_RECEIVED.alias("ers_ubrn_received"),
        s.ERS_PATHWAY_START.alias("ers_pathway_start"),
        s.ERS_SERVICE_NAME.alias("ers_service_name"), s.ERS_SPECIALTY.alias("ers_specialty"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        s.CREATED_DATETIME.alias("record_status_effective_from"),
        F.when(inactive, F.coalesce(s.SOURCE_ABSENT_DETECTED_TS, s.MODIFIED_DATETIME, s.ADC_UPDT))
        .alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("referral").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.SOURCE_ADC_UPDT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("luna").alias("_source_system"), F.lit(SRC_REFERRAL).alias("_source_table"),
        F.concat_ws(":", s.SOURCE_SYSTEM_OID.cast("string"), s.REFERRAL_OID.cast("string"))
        .alias("_source_row_id"),
    )


SRC_REFERRAL = "4_prod.bronze.map_referral"




def _rtt_pathway_canonical():
    # Assemble normalized rtt pathway rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_RTT_PATHWAY).alias("s")
    # PERIOD_OID is never NULL in bronze (0 = clockless-pathway sentinel); coalesce is
    # future-NULL defence only.
    period_key = F.coalesce(s.PERIOD_OID, F.lit(0))
    event_id = stable_id("rtt_period:luna", s.SOURCE_SYSTEM_OID, s.PATHWAY_OID, period_key)
    row_ref = F.concat_ws(
        ":", s.SOURCE_SYSTEM_OID.cast("string"), s.PATHWAY_OID.cast("string"),
        period_key.cast("string"),
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_RTT_PATHWAY, row_ref
    )
    source_present = F.coalesce(s.SOURCE_PRESENT_IND.cast("boolean"), F.lit(True))
    inactive = (
        (~source_present)
        | (F.coalesce(s.PERIOD_ACTIVE_IND, F.lit(True)) == F.lit(False))
        | (F.coalesce(s.CORE_ACTIVE_IND, F.lit(True)) == F.lit(False))
    )
    # contract v2: publish the LUNA source-system key component and native-typed person and encounter identifiers
    return s.select(
        event_id.alias("patient_event_key"),
        s.SOURCE_SYSTEM_OID.cast("bigint").alias("source_system_oid"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
        .alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.START_DATETIME.alias("event_datetime"), s.STOP_DATETIME.alias("event_end_datetime"),
        F.lit("urn:barts:luna:rtt-status").alias("source_coding_system"),
        s.CURRENT_RTT_STATUS_CD.alias("source_code"),
        s.CURRENT_RTT_STATUS_DESC.alias("source_display"),
        s.PATHWAY_OID.cast("long").alias("pathway_oid"),
        s.PERIOD_OID.cast("long").alias("period_oid"),
        s.IS_LATEST_PERIOD.alias("is_latest_period"),
        s.START_RTT_STATUS_CD.alias("start_status_code"),
        s.START_RTT_STATUS_DESC.alias("start_status_display"),
        s.STOP_RTT_STATUS_CD.alias("stop_status_code"),
        s.STOP_RTT_STATUS_DESC.alias("stop_status_display"),
        s.CURRENT_RTT_STATUS_CD.alias("current_status_code"),
        s.CURRENT_RTT_STATUS_DESC.alias("current_status_display"),
        s.SEQ_NO_ASC.cast("long").alias("sequence_asc"),
        s.SEQ_NO_DESC.cast("long").alias("sequence_desc"),
        s.CLOCK_DISCREPANT.alias("clock_discrepant"),
        s.CORE_CLOCK_START_DT_TM.alias("core_clock_start"),
        s.CORE_CLOCK_STOP_DT_TM.alias("core_clock_stop"),
        s.PATHWAY_START_DATE.alias("pathway_start_date"),
        s.PATHWAY_TYPE_CD.alias("pathway_type_code"),
        s.PATHWAY_TYPE_DESC.alias("pathway_type_display"),
        s.BREACH_DATE.alias("breach_date"),
        s.DAYS_WAITED.cast("long").alias("days_waited"),
        s.DAYS_WAITED_ACTIVE.cast("long").alias("days_waited_active"),
        s.OP_APPT_DNA_COUNT.cast("long").alias("op_appt_dna_count"),
        s.TREATMENT_FUNCTION_CD.alias("treatment_function_code"),
        s.TREATMENT_FUNCTION_DESC.alias("treatment_function_display"),
        s.SITE_CD.alias("site_code"), s.SITE_DESC.alias("site_display"),
        s.REFERRING_FACILITY_CD.alias("referring_facility_code"),
        s.REFERRING_FACILITY_DESC.alias("referring_facility_display"),
        F.coalesce(F.to_json(s.ENCOUNTER_TYPES), F.lit("[]")).alias("_encounter_types_json"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        s.PERIOD_CREATED_DATETIME.alias("record_status_effective_from"),
        F.when(
            inactive,
            F.coalesce(s.SOURCE_ABSENT_DETECTED_TS, s.PERIOD_MODIFIED_DATETIME, s.ADC_UPDT),
        ).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("rtt_pathway").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.SOURCE_ADC_UPDT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("luna").alias("_source_system"), F.lit(SRC_RTT_PATHWAY).alias("_source_table"),
        row_ref.alias("_source_row_id"),
    )


SRC_RTT_PATHWAY = "4_prod.bronze.map_rtt_pathway"




def _rtt_activity_canonical():
    # Assemble normalized rtt activity rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_RTT_ACTIVITY).alias("s")
    event_id = stable_id("rtt_activity_event:luna", s.SOURCE_SYSTEM_OID, s.RTT_ACTIVITY_OID)
    row_ref = F.concat_ws(
        ":", s.SOURCE_SYSTEM_OID.cast("string"), s.RTT_ACTIVITY_OID.cast("string")
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_RTT_ACTIVITY, row_ref
    )
    source_present = F.coalesce(s.SOURCE_PRESENT_IND.cast("boolean"), F.lit(True))
    inactive = (~source_present) | (F.coalesce(s.ACTIVE_IND, F.lit(True)) == F.lit(False))
    # contract v2: publish the LUNA source-system component, native-typed person id, and explicit referral key
    return s.select(
        event_id.alias("patient_event_key"),
        s.SOURCE_SYSTEM_OID.cast("bigint").alias("source_system_oid"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
        .alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.RTT_ACTIVITY_DATETIME.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:barts:luna:rtt-status").alias("source_coding_system"),
        s.RTT_STATUS_CD.alias("source_code"), s.RTT_STATUS_DESC.alias("source_display"),
        s.RTT_ACTIVITY_OID.cast("long").alias("rtt_activity_oid"),
        s.PATHWAY_OID.cast("long").alias("pathway_oid"),
        F.when(
            s.REFERRAL_OID.isNotNull(),
            stable_id("referral:luna", s.SOURCE_SYSTEM_OID, s.REFERRAL_OID),
        ).alias("referral_key"),
        s.APPOINTMENT_OID.cast("long").alias("appointment_oid"),
        s.RTT_ACTIVITY_CD.alias("activity_code"), s.RTT_ACTIVITY_DESC.alias("activity_display"),
        s.RTT_ACTIVITY_TYPE_CD.alias("activity_type_code"),
        s.RTT_ACTIVITY_TYPE_DESC.alias("activity_type_display"),
        s.RTT_STATUS_CD.alias("status_code"), s.RTT_STATUS_DESC.alias("status_display"),
        s.RTT_STATUS_SEQUENCE_ASC.cast("long").alias("status_sequence_asc"),
        s.RTT_STATUS_SEQUENCE_DESC.cast("long").alias("status_sequence_desc"),
        s.RTT_ACTIVITY_SEQUENCE_ASC.cast("long").alias("activity_sequence_asc"),
        s.RTT_ACTIVITY_SEQUENCE_DESC.cast("long").alias("activity_sequence_desc"),
        s.IS_ILLOGICAL.alias("is_illogical"),
        s.RTT_ACTIVITY_DATETIME_QUALITY.alias("activity_datetime_quality"),
        s.TREATMENT_FUNCTION_CD.alias("treatment_function_code"),
        s.TREATMENT_FUNCTION_DESC.alias("treatment_function_display"),
        s.SITE_CD.alias("site_code"), s.SITE_DESC.alias("site_display"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        s.CREATED_DATETIME.alias("record_status_effective_from"),
        F.when(inactive, F.coalesce(s.SOURCE_ABSENT_DETECTED_TS, s.MODIFIED_DATETIME, s.ADC_UPDT))
        .alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("rtt_activity").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.SOURCE_ADC_UPDT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("luna").alias("_source_system"),
        F.lit(SRC_RTT_ACTIVITY).alias("_source_table"), row_ref.alias("_source_row_id"),
    )


SRC_RTT_ACTIVITY = "4_prod.bronze.map_rtt_activity"




SRC_WAITING_LIST = "4_prod.bronze.map_waiting_list"

def _waiting_list_entry_canonical():
    # Assemble normalized waiting list entry rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    s = read_source(SRC_WAITING_LIST).alias("s")
    entry_id = stable_id("waiting_list:mill_pm", s.PM_WAIT_LIST_ID)
    # SOURCE_VERSION_ID is never NULL in bronze (CURRENT rows always carry -1); coalesce is
    # future-NULL defence only.
    version_row_id = F.concat_ws(
        ":", s.PM_WAIT_LIST_ID.cast("string"), s.ROW_SOURCE,
        F.coalesce(s.SOURCE_VERSION_ID.cast("string"), F.lit("~")),
    )
    fact_row = stable_id(
        "waiting_list:mill_pm", s.PM_WAIT_LIST_ID, s.ROW_SOURCE,
        F.coalesce(s.SOURCE_VERSION_ID, F.lit(-2)),
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_WAITING_LIST, version_row_id
    )
    source_present = F.coalesce(s.SOURCE_PRESENT_IND.cast("boolean"), F.lit(True))
    superseded = (~source_present) | (~F.coalesce(s.IS_CURRENT, F.lit(False)))
    # contract v2: publish native person, encounter, and location identifiers and drop the duplicate version-row hash
    return s.select(
        entry_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
        .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.WAITING_START_DT_TM.alias("event_datetime"),
        s.WAITING_END_DT_TM.alias("event_end_datetime"),
        F.lit("urn:cerner:mill:planned-procedure").alias("source_coding_system"),
        s.PLANNED_PROCEDURE_CD.cast("string").alias("source_code"),
        s.PLANNED_PROCEDURE_DESC.alias("source_display"),
        s.PM_WAIT_LIST_ID.cast("long").alias("pm_wait_list_id"),
        s.ROW_SOURCE.alias("row_source"),
        s.SOURCE_VERSION_ID.cast("long").alias("source_version_id"),
        s.HIST_ACTION.alias("hist_action"), s.VERSION_DT_TM.alias("version_datetime"),
        s.IS_CURRENT.alias("is_current"), s.UPDT_CNT.cast("long").alias("updt_cnt"),
        s.SCH_EVENT_ID.cast("long").alias("sch_event_id"),
        s.STATUS_CD.cast("string").alias("status_code"), s.STATUS_DESC.alias("status_display"),
        s.SUB_STATUS_DESC.alias("sub_status_display"),
        s.ACTIVE_STATUS_DESC.alias("active_status_display"),
        s.URGENCY_DESC.alias("urgency_display"), s.STAND_BY_DESC.alias("stand_by_display"),
        s.ADMIT_CATEGORY_DESC.alias("admit_category_display"),
        s.ADMIT_BOOKING_DESC.alias("admit_booking_display"),
        s.ADMIT_TYPE_DESC.alias("admit_type_display"),
        s.ADMIT_OFFER_OUTCOME_DESC.alias("admit_offer_outcome_display"),
        s.MANAGEMENT_DESC.alias("management_display"),
        s.ATTENDANCE_DESC.alias("attendance_display"),
        s.REASON_FOR_CHANGE_DESC.alias("reason_for_change_display"),
        s.REASON_FOR_REMOVAL_DESC.alias("reason_for_removal_display"),
        s.ANESTHETIC_DESC.alias("anesthetic_display"),
        s.PLANNED_PROCEDURE_CD.cast("string").alias("planned_procedure_code"),
        s.PLANNED_PROCEDURE_DESC.alias("planned_procedure_display"),
        s.REFERRAL_SOURCE_DESC.alias("referral_source_display"),
        s.REFERRAL_TYPE_DESC.alias("referral_type_display"),
        s.SERVICE_TYPE_REQUESTED_DESC.alias("service_type_requested_display"),
        s.FROM_ED_IND.cast("long").alias("from_ed_ind"),
        s.SUSPENDED_DAYS.alias("suspended_days"),
        s.RECOMMEND_DT_TM.alias("recommend_datetime"),
        s.REFERRAL_DT_TM.alias("referral_datetime"),
        s.ORIG_REQUEST_RECEIVED_DT_TM.alias("original_request_received_datetime"),
        s.ADMIT_DECISION_DT_TM.alias("admit_decision_datetime"),
        s.ADMIT_GUARANTEED_DT_TM.alias("admit_guaranteed_datetime"),
        s.PROVISIONAL_ADMIT_DT_TM.alias("provisional_admit_datetime"),
        s.PREV_PROV_ADMIT_DT_TM.alias("previous_provisional_admit_datetime"),
        s.ADJ_WAITING_START_DT_TM.alias("adjusted_waiting_start_datetime"),
        s.SCHEDULE_DT_TM.alias("scheduled_datetime"),
        s.REQUESTED_DT_TM.alias("requested_datetime"),
        s.REMOVAL_DT_TM.alias("removal_datetime"),
        s.LAST_DNA_DT_TM.alias("last_dna_datetime"),
        s.STATUS_DT_TM.alias("status_datetime"),
        s.STATUS_END_DT_TM.alias("status_end_datetime"),
        s.LOC_NURSE_UNIT_CD.cast("string").alias("location_code"),
        s.LOC_NURSE_UNIT_DESC.alias("location_display"),
        s.LOC_FACILITY_DESC.alias("facility_display"),
        F.when(superseded, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        s.BEG_EFFECTIVE_DT_TM.alias("record_status_effective_from"),
        F.when(
            superseded,
            F.coalesce(s.SOURCE_ABSENT_DETECTED_TS, s.END_EFFECTIVE_DT_TM, s.ADC_UPDT),
        ).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("waiting_list").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.SOURCE_ADC_UPDT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium-pm").alias("_source_system"),
        F.lit(SRC_WAITING_LIST).alias("_source_table"), version_row_id.alias("_source_row_id"),
    )




def _allergy_canonical_pregate():
    # Normalize allergy source rows before the downstream admission filter so excluded evidence
    # remains countable.
    s = read_source(SRC_ALLERGY)
    event_id = stable_id("allergy:mill", s.ALLERGY_INSTANCE_ID)
    raw_source_code = F.coalesce(
        s.SUBSTANCE_SNOMED_CODE,
        s.SUBSTANCE_SOURCE_IDENTIFIER,
        s.SUBSTANCE_NOM_ID.cast("long").cast("string"),
    )
    source_display = F.coalesce(
        s.SUBSTANCE_FTDESC, s.SUBSTANCE_SHORT_STRING, s.SUBSTANCE_SOURCE_STRING
    )
    source_code = _code_or_display(raw_source_code, source_display)
    source_system = F.coalesce(s.REC_SRC_VOCAB_DESC, F.lit("urn:cerner:nomenclature"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_ALLERGY, s.ALLERGY_INSTANCE_ID
    )
    cancelled = F.upper(F.coalesce(s.REACTION_STATUS_DESC, F.lit(""))) == F.lit("CANCELLED")
    ended = F.coalesce(
        s.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"), F.lit(False)
    ) | (F.coalesce(s.ACTIVE_IND.cast("long"), F.lit(1)) == 0)
    record_status = (
        F.when(cancelled, F.lit("retracted"))
        .when(ended, F.lit("superseded"))
        .otherwise(F.lit("active"))
    )
    # contract v2: publish ALLERGY_INSTANCE_ID and native relationship identifiers while retaining the event SHA as patient_event_key
    return s.select(
        event_id.alias("patient_event_key"),
        s.ALLERGY_INSTANCE_ID.cast("bigint").alias("allergy_instance_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        F.coalesce(s.ONSET_DT_TM_CLEAN, s.CREATED_DT_TM_CLEAN).alias("event_datetime"),
        F.when(cancelled, s.CANCEL_DT_TM_CLEAN)
        .when(ended, s.END_EFFECTIVE_DT_TM).alias("event_end_datetime"),
        source_system.alias("source_coding_system"), source_code.alias("source_code"),
        source_display.alias("source_display"),
        codeable_concept(
            coding_obj(source_system, source_code, source_display, True),
            coding_obj(F.lit("http://snomed.info/sct"), s.SUBSTANCE_SNOMED_CODE,
                       source_display, False, "bronze.map_allergy", None),
        ).alias("substance_code"),
        s.SUBSTANCE_TYPE_CD.cast("string").alias("substance_type_code"),
        s.SUBSTANCE_TYPE_DESC.alias("substance_type_display"),
        s.REACTION_CLASS_CD.cast("string").alias("reaction_class_code"),
        s.REACTION_CLASS_DESC.alias("reaction_class_display"),
        s.REACTION_STATUS_CD.cast("string").alias("reaction_status_code"),
        s.REACTION_STATUS_DESC.alias("reaction_status_display"),
        s.SEVERITY_CD.cast("string").alias("severity_code"),
        s.SEVERITY_DESC.alias("severity_display"),
        s.ABSENCE_ASSERTION_IND.cast("boolean").alias("absence_assertion_ind"),
        s.ONSET_PRECISION_DESC.alias("onset_precision_display"),
        F.coalesce(s.SOURCE_OF_INFO_DESC, s.SOURCE_OF_INFO_FT).alias("source_of_info_display"),
        s.VERIFIED_STATUS_FLAG.cast("string").alias("verified_status_flag"),
        s.REVIEWED_DT_TM_CLEAN.alias("reviewed_datetime"),
        s.CANCEL_REASON_DESC.alias("cancel_reason_display"),
        record_status.alias("record_status"),
        F.coalesce(s.REACTION_STATUS_DT_TM_CLEAN, s.ACTIVE_STATUS_DT_TM,
                   s.BEG_EFFECTIVE_DT_TM).alias("record_status_effective_from"),
        F.when(cancelled, s.CANCEL_DT_TM_CLEAN)
        .when(ended, s.END_EFFECTIVE_DT_TM).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.PIPELINE_UPDT_DT_TM.alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.lit(SRC_ALLERGY).alias("_source_table"),
        s.ALLERGY_INSTANCE_ID.cast("string").alias("_source_row_id"),
        s.SUBSTANCE_SNOMED_CODE.cast("string").alias("_snomed_code"),
        source_display.alias("_snomed_display"),
    )

def _allergy_canonical():
    # Keep the normalized allergy rows that pass the existing source-code admission rule.
    return _allergy_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_ALLERGY = "4_prod.bronze.map_allergy"




def _transfusion_canonical_pregate():
    # Normalize transfusion source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_BLOODTRACK_TRANSFUSION)
    event_id = stable_id("transfusion:bloodtrack", s.TRANSFUSION_KEY)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)],
        SRC_BLOODTRACK_TRANSFUSION,
        s.TRANSFUSION_KEY,
    )
    inactive = ~F.coalesce(s.IS_CURRENT_IN_SOURCE, F.lit(True))
    source_code = _code_or_display(s.PRODUCT_CODE, s.BLOOD_PRODUCT_GROUP)
    # contract v2: publish native BloodTrack transfusion identity and native-typed person and encounter identifiers
    return s.select(
        event_id.alias("patient_event_key"),
        s.TRANSFUSION_KEY.cast("string").alias("transfusion_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        F.coalesce(s.BEGIN_TS, s.END_TS).alias("event_datetime"),
        s.END_TS.alias("event_end_datetime"),
        F.lit("urn:isbt:product-code").alias("source_coding_system"),
        source_code.alias("source_code"), s.BLOOD_PRODUCT_GROUP.alias("source_display"),
        s.BEGIN_TS.alias("begin_datetime"), s.END_TS.alias("end_datetime"),
        s.TRANSFUSION_STATUS.alias("transfusion_status"),
        s.ELAPSED_MINUTES.cast("decimal(38,6)").alias("elapsed_minutes"),
        s.UNIT_NUMBER.alias("unit_number"), s.BLOOD_PRODUCT_GROUP.alias("blood_product_group"),
        s.BLOOD_UNIT_GROUP.alias("blood_unit_group"),
        s.PATIENT_BLOOD_GROUP.alias("patient_blood_group"),
        s.END_QUANTITY_VALUE.cast("decimal(38,6)").alias("quantity_value"),
        s.END_QUANTITY_RAW.alias("quantity_raw"),
        s.BEGIN_LOCATION_NAME.alias("begin_location"),
        s.END_LOCATION_NAME.alias("end_location"),
        s.UNIT_IS_IRRADIATED.alias("unit_is_irradiated"),
        s.UNIT_IS_CMV_NEG.alias("unit_is_cmv_neg"),
        s.REQUIRES_IRRADIATED.alias("requires_irradiated"),
        s.REQUIRES_CMV_NEG.alias("requires_cmv_neg"),
        s.AMBIGUITY_IND.alias("ambiguity_ind"),
        s.PRODUCT_CONCEPT_ID.cast("string").alias("product_concept_id"),
        s.PRODUCT_CONCEPT_NAME.alias("product_concept_name"),
        s.UNIT_GROUP_CONCEPT_ID.cast("string").alias("unit_group_concept_id"),
        s.PATIENT_GROUP_CONCEPT_ID.cast("string").alias("patient_group_concept_id"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        F.coalesce(s.BEGIN_TS, s.END_TS).alias("record_status_effective_from"),
        F.when(inactive, s.PIPELINE_LOADED_AT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.date_format(s.PIPELINE_LOADED_AT, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.lit(None).cast("timestamp").alias("source_update_timestamp"),
        s.PIPELINE_LOADED_AT.alias("loaded_at"),
        F.lit("bloodtrack").alias("_source_system"),
        F.lit(SRC_BLOODTRACK_TRANSFUSION).alias("_source_table"),
        s.TRANSFUSION_KEY.cast("string").alias("_source_row_id"),
        s.PRODUCT_PROC_CONCEPT_ID.cast("string").alias("_product_proc_concept_id"),
        s.PRODUCT_PROC_CONCEPT_NAME.alias("_product_proc_concept_name"),
        s.UNIT_GROUP_CONCEPT_NAME.alias("_unit_group_concept_name"),
        s.PATIENT_GROUP_CONCEPT_NAME.alias("_patient_group_concept_name"),
    )

def _transfusion_canonical():
    # Keep the normalized transfusion rows that pass the existing source-code admission rule.
    return _transfusion_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_BLOODTRACK_TRANSFUSION = "4_prod.bronze.map_bloodtrack_transfusion"




def _cancer_treatment_canonical_pregate():
    # Normalize cancer treatment source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_CANCER_TREATMENT)
    event_id = stable_id("cancer_treatment:sact", s.TREATMENT_KEY)
    source_display = F.coalesce(s.AriaAgentName, s.IqemoSactName)
    source_code = _code_or_display(s.drug, source_display)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID), ("urn:barts:mrn", s.MRN),
         ("https://fhir.nhs.uk/Id/nhs-number", s.NHS_Number)],
        SRC_CANCER_TREATMENT,
        s.TREATMENT_KEY,
    )
    inactive = ~F.coalesce(s.SOURCE_PRESENT_IND.cast("boolean"), F.lit(True))
    normalized_record_type = (
        F.when(s.record_type.rlike("(?i)matched"), F.lit("matched"))
        .when(s.record_type.rlike("(?i)aria"), F.lit("aria_only"))
        .otherwise(F.lit("iqemo_only"))
    )
    event_time = s.start_date.cast("timestamp")
    end_time = F.coalesce(s.EndDate, s.FinalTreatmentDate).cast("timestamp")
    # contract v2: retain the SACT native treatment key, publish native person/encounter types, and keep only the SHA event key
    return s.select(
        event_id.alias("patient_event_key"),
        s.TREATMENT_KEY.cast("string").alias("treatment_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
        .when(_present(s.MRN) | _present(s.NHS_Number), F.lit("provisional"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        event_time.alias("event_datetime"), end_time.alias("event_end_datetime"),
        F.lit("urn:barts:sact:drug-token").alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:barts:sact:drug-token"), source_code, source_display, True),
            coding_obj(F.lit("urn:omop:concept_id"), s.drug_concept_id,
                       s.drug_concept_name, False,
                       "lookup.cancer_treatment_term_map:applied", None),
        ).alias("drug_code"),
        s.TreatmentPlan.alias("treatment_plan"), s.RegimenName.alias("regimen_name"),
        s.Indication.alias("indication"), normalized_record_type.alias("record_type"),
        s.RxDose.cast("decimal(38,6)").alias("dose_value"),
        s.RxTotal.cast("decimal(38,6)").alias("dose_total"),
        s.AdmnDosageUnit.cast("string").alias("dose_unit_code"),
        s.AdmnRoute.cast("string").alias("route_code"),
        event_time.alias("start_date"), s.EndDate.cast("timestamp").alias("end_date"),
        s.FinalTreatmentDate.cast("timestamp").alias("final_treatment_date"),
        s.CourseFinished.cast("boolean").alias("course_finished"),
        s.PlannedCycles.cast("long").alias("planned_cycles"),
        s.DefaultCycles.cast("long").alias("default_cycles"),
        s.ChemoRadiation.cast("boolean").alias("chemo_radiation"),
        s.OPCSProcurementCode.alias("procurement_opcs_code"),
        s.OPCSDeliveryCode.alias("delivery_opcs_code"),
        s.drug_similarity.cast("decimal(38,6)").alias("drug_similarity"),
        s.iqemo_chemotherapy_course_id.cast("string").alias("iqemo_course_id"),
        F.concat_ws(":", s.aria_pt_id, s.aria_rx_id.cast("string"),
                    s.aria_item_no.cast("string")).alias("aria_rx_key"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        event_time.alias("record_status_effective_from"),
        F.when(inactive, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.SRC_ADC_UPDT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("sact").alias("_source_system"),
        F.lit(SRC_CANCER_TREATMENT).alias("_source_table"),
        s.TREATMENT_KEY.cast("string").alias("_source_row_id"),
        s.drug_concept_id.cast("string").alias("_drug_omop_code"),
        s.drug_concept_name.alias("_drug_omop_display"),
        s.procurement_snomed_concept_id.cast("string").alias("_procurement_omop_code"),
        s.procurement_snomed_concept_id.cast("string").alias("_procurement_omop_display"),
        s.OPCSProcurementCode.alias("_procurement_opcs4_code"),
        s.OPCSProcurementCode.alias("_procurement_opcs4_display"),
        s.delivery_snomed_concept_id.cast("string").alias("_delivery_omop_code"),
        s.delivery_snomed_concept_id.cast("string").alias("_delivery_omop_display"),
        s.OPCSDeliveryCode.alias("_delivery_opcs4_code"),
        s.OPCSDeliveryCode.alias("_delivery_opcs4_display"),
    )

def _cancer_treatment_canonical():
    # Keep the normalized cancer treatment rows that pass the existing source-code admission
    # rule.
    return _cancer_treatment_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_CANCER_TREATMENT = "4_prod.bronze.map_cancer_treatment"




def _condition_stage_canonical_pregate():
    # Normalize condition stage source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_ARIA_STAGING)
    event_id = stable_id("condition_stage:aria", s.ARIA_PT_ID, s.ARIA_DX_ID)
    source_display = F.coalesce(s.DX_NAME, s.DX_DESC)
    source_code = _code_or_display(s.ICD_CODE, source_display)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID), ("urn:aria:mrn", s.ARIA_MRN)],
        SRC_ARIA_STAGING,
        F.concat_ws(":", s.ARIA_PT_ID, s.ARIA_DX_ID.cast("string")),
    )
    inactive = (
        (F.coalesce(s.CUR_ENTRY_IND, F.lit("Y")) != F.lit("Y"))
        | ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    )
    # contract v2: publish the ARIA patient-diagnosis composite and native identifier types while retaining the SHA event key
    return s.select(
        event_id.alias("patient_event_key"),
        s.ARIA_PT_ID.cast("string").alias("aria_pt_id"),
        s.ARIA_DX_ID.cast("bigint").alias("aria_dx_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
        .when(_present(s.ARIA_MRN), F.lit("provisional"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.ONSET_DATE_CLEAN.cast("timestamp").alias("event_datetime"),
        s.RESOLUTION_DATE_CLEAN.cast("timestamp").alias("event_end_datetime"),
        F.lit("http://hl7.org/fhir/sid/icd-10").alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("http://hl7.org/fhir/sid/icd-10"), source_code,
                       source_display, True),
            coding_obj(F.lit("urn:omop:concept_id"), s.DIAGNOSIS_CONCEPT_ID,
                       s.DIAGNOSIS_CONCEPT_NAME, False,
                       "bronze.map_aria_diagnosis_staging", None),
        ).alias("stage_code"),
        s.STAGE_OF_DISEASE.alias("stage_of_disease"), s.STG_CRIT_DESC.alias("stage_criteria"),
        s.DX_TYP.alias("dx_type"), s.CONFIRM_DX.alias("dx_confirmed"),
        s.MTHD_OF_DX.alias("dx_method"),
        (F.upper(F.coalesce(s.HX_OF_IND, F.lit("N"))) == "Y").alias("history_ind"),
        (F.upper(F.coalesce(s.CUR_ENTRY_IND, F.lit("N"))) == "Y")
        .alias("current_entry_ind"),
        (F.upper(F.coalesce(s.CS_OF_DTH_IND, F.lit("N"))) == "Y")
        .alias("cause_of_death_ind"),
        s.ONSET_DATE_CLEAN.cast("timestamp").alias("onset_datetime"),
        s.RESOLUTION_DATE_CLEAN.cast("timestamp").alias("resolution_datetime"),
        s.CLINICAL_DESC.alias("clinical_description"), s.DX_CMT.alias("dx_comment"),
        s.PERSON_LINK_STATUS.alias("person_link_status"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        s.EVLV_TSTAMP_CLEAN.alias("record_status_effective_from"),
        F.when(inactive, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.PIPELINE_UPDT_DT_TM.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("aria").alias("_source_system"), F.lit(SRC_ARIA_STAGING).alias("_source_table"),
        F.concat_ws(":", s.ARIA_PT_ID, s.ARIA_DX_ID.cast("string")).alias("_source_row_id"),
        s.DIAGNOSIS_CONCEPT_ID.cast("string").alias("_omop_code"),
        s.DIAGNOSIS_CONCEPT_NAME.alias("_omop_display"),
    )

def _condition_stage_canonical():
    # Keep the normalized condition stage rows that pass the existing source-code admission
    # rule.
    return _condition_stage_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_ARIA_STAGING = "4_prod.bronze.map_aria_diagnosis_staging"




SRC_ENDOBASE_EXAM = "4_prod.bronze.map_endobase_exam"

SRC_ENDOBASE_EXAM_TERM = "4_prod.bronze.map_endobase_exam_term"

def _endoscopy_finding_canonical_pregate():
    # Normalize endoscopy finding source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s, t, event_time, event_time_source = _endobase_term_base()
    s = s.where(~F.coalesce(t.FREE_TEXT_IND, F.lit(False)))
    event_id = stable_id("endoscopy_finding:endobase", t.ENDOBASE_EXAM_TERM_ID)
    source_code = t._finding_source_code
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", t.PERSON_ID)],
        SRC_ENDOBASE_EXAM_TERM,
        t.ENDOBASE_EXAM_TERM_ID,
    )
    inactive = ~F.coalesce(t.SOURCE_PRESENT_IND, F.lit(True))
    loaded_at = F.greatest(t.ADC_UPDT, F.coalesce(F.col("x._x_loaded_at"), t.ADC_UPDT))
    # contract v2: publish Endobase native identifiers and native person/encounter types while retaining SHA relationship keys
    return s.select(
        event_id.alias("patient_event_key"),
        t.ENDOBASE_EXAM_TERM_ID.cast("bigint").alias("endobase_exam_term_id"),
        t.ENDOBASE_EXAM_ID.cast("bigint").alias("endobase_exam_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        t.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(t.PERSON_ID.isNotNull(), F.lit("resolved"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.col("x._x_encntr_id").cast("bigint").alias("encounter_id"),
        event_time.alias("event_datetime"), F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:endobase:dgvs-term").alias("source_coding_system"),
        source_code.alias("source_code"), t.TERM_TEXT.alias("source_display"),
        t._finding_code_json.alias("_finding_code_json"),
        F.when(t.ENDOBASE_EXAM_ID.isNotNull(),
               stable_id("procedure:endobase_exam", t.ENDOBASE_EXAM_ID))
        .alias("endoscopy_exam_event_key"),
        t.SECTION_TAB_ID.cast("string").alias("section_id"),
        t.SUBSECTION_TAB_ID.cast("string").alias("subsection_id"),
        t.PARENT_EXAM_TERM_ID.cast("string").alias("parent_term_id"),
        t.DISPLAY_ORDER.cast("long").alias("display_order"),
        t.CONFIRMED_IND.alias("confirmed_ind"), t.TEXT_CHANGED_IND.alias("text_changed_ind"),
        t.FREE_TEXT_IND.alias("free_text_ind"),
        t.TERM_MAPPING_STATUS.alias("term_mapping_status"),
        t.PERSON_LINK_STATUS.alias("person_link_status"), t.CREATED_TS.alias("authored_datetime"),
        event_time_source.alias("event_time_source"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        t.CREATED_TS.alias("record_status_effective_from"),
        F.when(inactive, t.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.date_format(loaded_at, "yyyyMMddHHmmss").alias("load_batch_id"),
        t.ADC_UPDT.alias("source_update_timestamp"), loaded_at.alias("loaded_at"),
        F.lit("endobase").alias("_source_system"),
        F.lit(SRC_ENDOBASE_EXAM_TERM).alias("_source_table"),
        t.ENDOBASE_EXAM_TERM_ID.cast("string").alias("_source_row_id"),
        t.SNOMED_CODE.cast("string").alias("_snomed_code"),
        t.TERM_TEXT.alias("_snomed_display"),
        t.OMOP_CONCEPT_ID.cast("string").alias("_omop_code"),
        t.TERM_TEXT.alias("_omop_display"),
    )

def _endoscopy_finding_canonical():
    # Keep the normalized endoscopy finding rows that pass the existing source-code admission
    # rule.
    return _endoscopy_finding_canonical_pregate().where(_usable_code(F.col("source_code")))




def _endobase_term_base():
    # Read Endobase examination terms and retain a source code or meaningful display-only
    # coding.
    t0 = read_source(SRC_ENDOBASE_EXAM_TERM)
    term_source_code = _code_or_display(t0.DGVS_TERM_ID.cast("string"), t0.TERM_TEXT)
    source_coding = coding_obj(F.lit("urn:endobase:dgvs-term"), term_source_code,
                               t0.TERM_TEXT, True)
    snomed_coding = coding_obj(F.lit("http://snomed.info/sct"), t0.SNOMED_CODE,
                               t0.TERM_TEXT, False,
                               "bronze.map_endobase_exam_term:c4", None)
    omop_coding = coding_obj(F.lit("urn:omop:concept_id"), t0.OMOP_CONCEPT_ID,
                             t0.TERM_TEXT, False,
                             "bronze.map_endobase_exam_term:c4", None)
    finding_code_json = codeable_concept_json(source_coding, snomed_coding, omop_coding)
    t = (
        t0.withColumn("_finding_source_code", term_source_code)
        .withColumn("_finding_code_json", finding_code_json)
        .alias("t")
    )
    x = read_source(SRC_ENDOBASE_EXAM).select(
        F.col("ENDOBASE_EXAM_ID").alias("_x_exam_id"),
        F.coalesce(F.col("PERFORMED_TS_CLEAN"), F.col("EXAM_TS_CLEAN"),
                   F.col("TRUE_START_TS_CLEAN"), F.col("START_TS_CLEAN"))
        .alias("_x_exam_ts"),
        F.col("MILL_ENCNTR_ID").alias("_x_encntr_id"),
        F.col("ADC_UPDT").alias("_x_loaded_at"),
    ).alias("x")
    s = t.join(x, t.ENDOBASE_EXAM_ID == F.col("x._x_exam_id"), "left")
    event_time = F.coalesce(F.col("x._x_exam_ts"), t.CREATED_TS)
    event_time_source = F.when(F.col("x._x_exam_ts").isNotNull(), F.lit("exam")) \
        .otherwise(F.lit("authored"))
    return s, t, event_time, event_time_source




def _device_canonical_pregate():
    # Normalize device source rows before the downstream admission filter so excluded evidence
    # remains countable.
    d = read_source(SRC_MEDICONNECT_DEVICE)
    m = read_source(SRC_MEDICONNECT_TYPE_MAP).select(
        F.col("DEVICE_TYPE").cast("string").alias("_m_device_type"),
        F.col("DEVICE_ROLE").alias("_m_device_role"),
        F.col("SNOMED_CONCEPT_ID").cast("string").alias("_m_snomed_code"),
        F.col("SNOMED_CONCEPT_NAME").alias("_m_snomed_display"),
        F.col("MAPPING_STATUS").alias("_m_mapping_status"),
    )
    s = d.join(m, d.DEVICE_TYPE.cast("string") == m._m_device_type, "left")
    event_id = stable_id("device:mediconnect", s.MC_DEVICE_ID)
    raw_type = F.when(s.DEVICE_TYPE.cast("long") != 0, s.DEVICE_TYPE.cast("string"))
    source_display = F.coalesce(s.TYPE, s.MODEL_NAME)
    source_code = _code_or_display(raw_type, source_display)
    snomed_code = F.coalesce(s.DEVICE_SNOMED_CONCEPT_ID.cast("string"), s._m_snomed_code)
    snomed_display = F.coalesce(s.DEVICE_SNOMED_CONCEPT_NAME, s._m_snomed_display)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID), ("urn:mediconnect:patient-id", s.PATIENTID)],
        SRC_MEDICONNECT_DEVICE,
        s.MC_DEVICE_ID,
    )
    inactive = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    implant_time = s.IMPLANTED_DATE_CLEAN.cast("timestamp")
    # contract v2: publish the MediConnect native device id and native person/encounter types while retaining the SHA event key
    return s.select(
        event_id.alias("patient_event_key"),
        s.MC_DEVICE_ID.cast("string").alias("mc_device_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
        .when(_present(s.PATIENTID), F.lit("provisional"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        implant_time.alias("event_datetime"), F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:barts:mediconnect:device-type").alias("source_coding_system"),
        source_code.alias("source_code"), source_display.alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:barts:mediconnect:device-type"), source_code,
                       source_display, True),
            coding_obj(F.lit("http://snomed.info/sct"), snomed_code,
                       snomed_display, False,
                       "lookup.mediconnect_device_type_map:applied", None),
        ).alias("_device_code_json"),
        F.coalesce(s.DEVICE_ROLE, s._m_device_role).alias("device_role"),
        s.MODEL_NAME.alias("model_name"),
        s.MODEL_CODE.alias("model_code"),
        F.coalesce(s.MANUFACTURER_CLEAN, s.MANUFACTURER).alias("manufacturer"),
        s.MANUFACTURER_PARENT.alias("manufacturer_parent"), s.SERIAL_NO.alias("serial_number"),
        implant_time.alias("implanted_datetime"),
        s.IMPLANTED_DATE_QUALITY.alias("implant_date_quality"),
        s.EXPLANTED_IND.alias("explanted_ind"), s.LEAD_CHAMBER.alias("lead_chamber"),
        s.LEAD_LOCATION.alias("lead_location"), s.POCKET_SITE.alias("pocket_site"),
        s.STATUS.alias("status_display"),
        F.coalesce(s.DEVICE_MAPPING_STATUS, s._m_mapping_status).alias("device_mapping_status"),
        s.COMMENT.alias("comment_text"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        implant_time.alias("record_status_effective_from"),
        F.when(inactive, F.coalesce(s.SOURCE_ABSENT_DETECTED_TS, s.ADC_UPDT))
        .alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.lit(None).cast("timestamp").alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("mediconnect").alias("_source_system"),
        F.lit(SRC_MEDICONNECT_DEVICE).alias("_source_table"),
        s.MC_DEVICE_ID.cast("string").alias("_source_row_id"),
        snomed_code.alias("_snomed_code"), snomed_display.alias("_snomed_display"),
    )

def _device_canonical():
    # Keep the normalized device rows that pass the existing source-code admission rule.
    return _device_canonical_pregate().where(_usable_code(F.col("source_code")))

def _registry_entry_canonical():
    # Assemble normalized registry entry rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    lanes = [_registry_entry_lane(*cfg) for cfg in IWEB_REGISTRY_CONFIG]
    out = lanes[0]
    for lane_df in lanes[1:]:
        out = out.unionByName(lane_df)
    return out




SRC_MEDICONNECT_DEVICE = "4_prod.bronze.mediconnect_device"

IWEB_REGISTRY_SLOTS = {
    "acs_transfer": "4_prod.bronze.iweb_acs_transfer",
    "cardiac_mdt": "4_prod.bronze.iweb_cardiac_mdt",
    "mortality_review": "4_prod.bronze.iweb_cardiac_mortality_review",
    "surgery_episode": "4_prod.bronze.iweb_cardiac_surgery_episode",
    "surgery_followup": "4_prod.bronze.iweb_cardiac_surgery_followup",
    "surgery_procedure": "4_prod.bronze.iweb_cardiac_surgery_procedure",
    "coronary_procedure": "4_prod.bronze.iweb_coronary_procedure",
    "coronary_lesion": "4_prod.bronze.iweb_coronary_lesion",
    "eracs_episode": "4_prod.bronze.iweb_eracs_episode",
    "noncoronary_procedure": "4_prod.bronze.iweb_noncoronary_procedure",
}

def _iweb_field(s, field, module):
    # iWeb bronze nests module fields into typed structs (2026-08-21 restructure of
    # iweb_cardiac_surgery_episode); dev fixtures predate it and still carry them at the
    # top level. Resolve either shape and fail closed if the field vanishes entirely.
    # Read an iWeb field from either the older flat schema or the specified nested module; fail
    # if neither exists.
    if field in s.columns:
        return s[field]
    if module in s.columns and field in s.schema[module].dataType.fieldNames():
        return s[module][field]
    raise KeyError(f"iweb field {field} absent at top level and inside {module}")

IWEB_REGISTRY_CONFIG = [
    ("acs_transfer", "acs_transfer",
     lambda s: F.coalesce(s.ARRIVE_HERE, s.ADMISSIONDATE), lambda s: s.DATE_OF_DISCHARGE,
     None, None, lambda s: s.DATE_OF_DEATH, None, lambda s: s.SOURCE_ABSENT_DETECTED_TS),
    ("cardiac_mdt", "cardiac_mdt",
     lambda s: s.MD_TDATE, lambda s: F.lit(None).cast("timestamp"),
     None, lambda s: s.REGISTRY_TYPE, lambda s: s.DATE_OF_DEATH, None,
     lambda s: s.SOURCE_ABSENT_DETECTED_TS),
    ("mortality_review", "mortality_review",
     lambda s: F.coalesce(s.DATE_AND_TIME_OF_DEATH, s.DATE_OF_DEATH_CLEAN.cast("timestamp")),
     lambda s: F.lit(None).cast("timestamp"), None, None,
     lambda s: s.DATE_OF_DEATH_DEMOG, None, lambda s: s.SOURCE_ABSENT_DETECTED_TS),
    ("surgery_episode", "surgery_episode",
     lambda s: F.coalesce(s.DATE_AND_TIME_OF_OPERATION, s.DATE_OF_OPERATION),
     lambda s: _iweb_field(s, "DATE_OF_DISCHARGE_OR_DEATH", "POST1_MODULE"),
     None, None, lambda s: s.DATE_OF_DEATH,
     None, lambda s: s.SOURCE_ABSENT_DETECTED_TS),
    ("surgery_followup", "surgery_followup",
     lambda s: s.DISCHARGE_DT, lambda s: F.lit(None).cast("timestamp"),
     None, None, lambda s: s.DATE_OF_DEATH, None, lambda s: s.SOURCE_ABSENT_DETECTED_TS),
    ("surgery_procedure", "surgery_procedure",
     None, lambda s: F.lit(None).cast("timestamp"),
     ("surgery_episode", "PARENT_ENTRY_ID", "DATE_AND_TIME_OF_OPERATION"),
     None, lambda s: s.DATE_OF_DEATH, None, lambda s: s.SOURCE_ABSENT_DETECTED_TS),
    ("coronary_procedure", "coronary_procedure",
     lambda s: s.PRE_DATEANDTIMEOFOPERATION_CLEAN,
     lambda s: s.POST_DATE_OF_DISCHARGE_TS_CLEAN,
     None, None, lambda s: s.PRE_DATE_OF_DEATH_CLEAN, lambda s: s.NHS_NUMBER,
     lambda s: F.lit(None).cast("timestamp")),
    ("coronary_lesion", "coronary_lesion",
     None, lambda s: F.lit(None).cast("timestamp"),
     ("coronary_procedure", "PARENT_ENTRY_ID", "PRE_DATEANDTIMEOFOPERATION_CLEAN"),
     None, lambda s: s.DATE_OF_DEATH, None, lambda s: s.SOURCE_ABSENT_DETECTED_TS),
    ("eracs_episode", "eracs_episode",
     lambda s: F.coalesce(s.ADMISSION_DT_TM, s.DATE_OF_ENTRY),
     lambda s: s.ACTUAL_DISCHARGE_DT_TM, None, None, lambda s: s.DATE_OF_DEATH,
     None, lambda s: s.SOURCE_ABSENT_DETECTED_TS),
    ("noncoronary_procedure", "noncoronary_procedure",
     lambda s: s.DATEOFPROCEDURE, lambda s: s.DATE_OF_DISCHARGE_DEATH,
     None, None, lambda s: s.DATE_OF_DEATH, None, lambda s: s.SOURCE_ABSENT_DETECTED_TS),
]

def _registry_entry_lane(family, slot_key, event_fn, end_fn, parent, rtype_fn, dod_fn,
                         nhs_fn, absent_ts_fn):
    # Project one configured iWeb registry source while preserving all original fields as a JSON
    # payload.
    base = read_source(IWEB_REGISTRY_SLOTS[slot_key])
    source_columns = list(base.columns)
    base = base.withColumn(
        "_registry_payload_json",
        F.to_json(F.struct(*[base[c] for c in source_columns])),
    )
    s = base
    registry_type = rtype_fn(s) if rtype_fn else F.lit(None).cast("string")
    row_key = F.concat_ws(":", F.coalesce(registry_type, F.lit("~")),
                          s.ENTRY_ID.cast("string"))
    event_id = stable_id("registry_entry:iweb", F.lit(family),
                         F.coalesce(registry_type, F.lit("~")), s.ENTRY_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID), ("urn:barts:mrn", s.MRN)],
        IWEB_REGISTRY_SLOTS[slot_key], row_key,
    )
    if parent:
        parent_family, parent_fk, parent_event_col = parent
        p = read_source(IWEB_REGISTRY_SLOTS[parent_family]).select(
            F.col("ENTRY_ID").alias("_p_entry_id"),
            F.col(parent_event_col).alias("_p_event_ts"),
        )
        s = s.join(p, s[parent_fk] == F.col("_p_entry_id"), "left")
        parent_id = stable_id("registry_entry:iweb", F.lit(parent_family),
                              F.lit("~"), s[parent_fk])
        event_expr = F.col("_p_event_ts")
    else:
        parent_id = F.lit(None).cast("string")
        event_expr = event_fn(s)
    nhs_expr = nhs_fn(s) if nhs_fn else F.lit(None).cast("string")
    inactive = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    # contract v2: publish source_object plus native iWeb entry ids and native person/encounter types while retaining SHA event and parent keys
    return s.select(
        event_id.alias("patient_event_key"),
        F.lit(family).alias("source_object"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
        .when(_present(s.MRN) | _present(nhs_expr), F.lit("provisional"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        event_expr.cast("timestamp").alias("event_datetime"),
        end_fn(s).cast("timestamp").alias("event_end_datetime"),
        F.lit(None).cast("string").alias("source_coding_system"),
        F.lit(None).cast("string").alias("source_code"),
        F.lit(None).cast("string").alias("source_display"),
        F.lit(family).alias("registry_family"), registry_type.alias("registry_type"),
        s.ENTRY_ID.cast("bigint").alias("entry_id"),
        parent_id.alias("parent_registry_entry_key"), s.LINKAGE_STATUS.alias("linkage_status"),
        s.MRN.alias("mrn"), nhs_expr.alias("nhs_number"),
        dod_fn(s).cast("timestamp").alias("date_of_death"),
        s._registry_payload_json.alias("_registry_payload_json"),
        s.DATE_LAST_CHANGED.alias("source_edit_datetime"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        F.lit(None).cast("timestamp").alias("record_status_effective_from"),
        F.when(inactive, absent_ts_fn(s)).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.DATE_LAST_CHANGED.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("iweb").alias("_source_system"),
        F.lit(IWEB_REGISTRY_SLOTS[slot_key]).alias("_source_table"), row_key.alias("_source_row_id"),
    )




# ==== Pathway and administrative feeds ====

def _clamped_ts(col):
    """Return NULL for source timestamps outside the governed [1950, 2100) range."""
    return F.when(
        (col >= F.lit("1950-01-01").cast("timestamp"))
        & (col < F.lit("2100-01-01").cast("timestamp")),
        col,
    )

def _community_care_contact_canonical():
    # Assemble normalized community care contact rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    s = read_source(SRC_COMMUNITY_CONTACT)
    event_id = stable_id("community_care_contact:csds", s.community_care_contact_key)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.person_id),
         ("urn:barts:community_patient_key", s.community_patient_key)],
        SRC_COMMUNITY_CONTACT,
        s.community_care_contact_key,
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    raw_event = F.coalesce(
        s.care_contact_datetime_local.cast("timestamp"), s.care_contact_date.cast("timestamp")
    )
    # contract v2: publish the source-preserved community contact key and native identifier types while retaining the SHA event key
    return s.select(
        event_id.alias("patient_event_key"),
        s.community_care_contact_key.cast("string").alias("community_care_contact_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.person_id.cast("bigint").alias("person_id"),
        F.when(s.person_id.isNotNull(), F.lit("resolved"))
         .when(s.community_patient_key.isNotNull(), F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        _clamped_ts(raw_event).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.when(s.consultation_type_code.isNotNull(),
               F.lit("urn:barts:community:consultation-type")).alias("source_coding_system"),
        s.consultation_type_code.cast("string").alias("source_code"),
        s.consultation_type_description.alias("source_display"),
        s.care_contact_date.cast("timestamp").alias("care_contact_date"),
        s.source_database_id.cast("string").alias("community_database_id"),
        s.care_contact_id.alias("care_contact_id"),
        s.community_patient_key.alias("community_patient_key"),
        s.service_request_id.alias("service_request_id"),
        s.source_service_id.cast("string").alias("service_id"),
        s.source_service_name.alias("service_name"),
        s.source_team_id.alias("team_id"),
        s.care_contact_id_variant_count.alias("care_contact_id_variant_count"),
        s.person_match_status.alias("person_match_status"),
        s.clinical_contact_duration_minutes.alias("duration_minutes"),
        s.clinical_contact_duration_quality_status.alias("duration_quality_status"),
        s.earliest_reasonable_offer_date.cast("timestamp").alias("earliest_reasonable_offer_date"),
        s.earliest_clinically_appropriate_date.cast("timestamp")
         .alias("earliest_clinically_appropriate_date"),
        s.commissioner_ods_code.alias("commissioner_ods_code"),
        s.commissioner_organization_id.cast("string").alias("commissioner_organization_id"),
        s.commissioner_organization_name.alias("commissioner_organization_name"),
        s.normalized_consultation_mechanism_code.alias("consultation_mechanism_code"),
        s.normalized_consultation_mechanism_description.alias("consultation_mechanism_display"),
        s.activity_location_type_code.alias("location_type_code"),
        s.activity_location_type_description.alias("location_type_display"),
        s.service_team_type_code.alias("service_team_type_code"),
        s.service_team_type_description.alias("service_team_type_display"),
        s.service_team_type_mapping_status.alias("service_team_type_mapping_status"),
        s.source_consultation_term.alias("source_consultation_term"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        raw_event.alias("record_status_effective_from"),
        F.when(retracted, s.SOURCE_ABSENT_DETECTED_TS).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("community_care_contact").alias("source_feed"),
        F.lit(None).cast("string").alias("load_batch_id"),
        F.lit(None).cast("timestamp").alias("source_update_timestamp"),
        F.lit(None).cast("timestamp").alias("loaded_at"),
        F.lit("bh-community").alias("_source_system"),
        F.lit(SRC_COMMUNITY_CONTACT).alias("_source_table"),
        s.community_care_contact_key.alias("_source_row_id"),
    )


SRC_COMMUNITY_CONTACT = "4_prod.bronze.map_community_care_contact"




def _community_care_activity_canonical_pregate():
    # Normalize community care activity source rows before the downstream admission filter so
    # excluded evidence remains countable.
    s = read_source(SRC_COMMUNITY_ACTIVITY)
    event_id = stable_id("community_care_activity:csds", s.community_care_activity_key)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.person_id),
         ("urn:barts:community_patient_key", s.community_patient_key)],
        SRC_COMMUNITY_ACTIVITY,
        s.community_care_activity_key,
    )
    source_code = _code_or_display(
        s.community_care_activity_type_code, s.community_care_activity_type_description
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    raw_event = s.care_activity_date.cast("timestamp")
    # contract v2: publish the source-preserved activity key, native identifiers, and the hashed parent contact as a key
    return s.select(
        event_id.alias("patient_event_key"),
        s.community_care_activity_key.cast("string").alias("community_care_activity_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.person_id.cast("bigint").alias("person_id"),
        F.when(s.person_id.isNotNull(), F.lit("resolved"))
         .when(s.community_patient_key.isNotNull(), F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        _clamped_ts(raw_event).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.when(source_code.isNotNull(), F.lit("urn:barts:community:activity-type"))
         .alias("source_coding_system"),
        source_code.alias("source_code"),
        s.community_care_activity_type_description.alias("source_display"),
        raw_event.alias("care_activity_date"),
        s.care_activity_date_quality_status.alias("care_activity_date_quality_status"),
        F.when(s.community_care_contact_key.isNotNull(),
               stable_id("community_care_contact:csds", s.community_care_contact_key))
         .alias("community_contact_key"),
        s.contact_match_status.alias("contact_match_status"),
        s.contact_candidate_count.alias("contact_candidate_count"),
        s.same_date_contact_candidate_count.alias("same_date_contact_candidate_count"),
        s.source_database_id.cast("string").alias("community_database_id"),
        s.care_activity_id.alias("care_activity_id"),
        s.community_patient_key.alias("community_patient_key"),
        s.source_service_id.cast("string").alias("service_id"),
        s.source_service_name.alias("service_name"),
        s.source_care_professional_local_id.alias("care_professional_local_id"),
        s.clinical_contact_duration_minutes.alias("duration_minutes"),
        s.clinical_contact_duration_quality_status.alias("duration_quality_status"),
        s.source_clinical_term.alias("source_clinical_term"),
        s.source_clinical_term_key.alias("source_clinical_term_key"),
        s.source_observation_type_id.cast("string").alias("observation_type_id"),
        s.source_code_category_id.cast("string").alias("code_category_id"),
        s.observation_value_raw.alias("observation_value_raw"),
        s.observation_value_numeric.alias("observation_value_numeric"),
        s.unit_source_value.alias("unit_source_value"),
        s.normalized_ucum_code.alias("normalized_ucum_code"),
        s.unit_omop_concept_id.cast("string").alias("unit_concept_id"),
        s.unit_omop_concept_name.alias("unit_concept_name"),
        s.unit_mapping_status.alias("unit_mapping_status"),
        s.unit_mapping_method.alias("unit_mapping_method"),
        s.snomed_candidate_count.alias("snomed_candidate_count"),
        s.snomed_candidate_concept_id.cast("string").alias("snomed_candidate_concept_id"),
        s.snomed_candidate_code.alias("snomed_candidate_code"),
        s.snomed_candidate_name.alias("snomed_candidate_name"),
        s.snomed_candidate_domain.alias("snomed_candidate_domain"),
        s.snomed_candidate_class.alias("snomed_candidate_class"),
        s.snomed_candidate_method.alias("snomed_candidate_method"),
        s.snomed_candidate_status.alias("snomed_candidate_status"),
        s.person_match_status.alias("person_match_status"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        raw_event.alias("record_status_effective_from"),
        F.when(retracted, s.SOURCE_ABSENT_DETECTED_TS).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("clinical").alias("fact_category"),
        F.lit("community_care_activity").alias("source_feed"),
        F.lit(None).cast("string").alias("load_batch_id"),
        F.lit(None).cast("timestamp").alias("source_update_timestamp"),
        F.lit(None).cast("timestamp").alias("loaded_at"),
        F.lit("bh-community").alias("_source_system"),
        F.lit(SRC_COMMUNITY_ACTIVITY).alias("_source_table"),
        s.community_care_activity_key.alias("_source_row_id"),
        s.snomed_candidate_code.alias("_snomed_candidate_code"),
        s.snomed_candidate_name.alias("_snomed_candidate_name"),
        s.snomed_candidate_concept_id.cast("string").alias("_snomed_candidate_omop_code"),
        s.snomed_candidate_name.alias("_snomed_candidate_omop_display"),
    )

def _community_care_activity_canonical():
    # Keep the normalized community care activity rows that pass the existing source-code
    # admission rule.
    return _community_care_activity_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_COMMUNITY_ACTIVITY = "4_prod.bronze.map_community_care_activity"




def _hrg_grouping_canonical():
    # Assemble normalized hrg grouping rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    return _hrg_arm(read_source(SRC_SLAM_APC_HRG), "slam_apc_hrg").unionByName(
        _hrg_arm(read_source(SRC_SLAM_OP_HRG), "slam_op_hrg")
    )

SRC_SLAM_APC_HRG = "4_prod.bronze.map_slam_apc_hrg"

SRC_SLAM_OP_HRG = "4_prod.bronze.map_slam_op_hrg"

def _hrg_arm(s, arm):
    # Project an inpatient or outpatient HRG feed using that arm's native episode or attendance
    # identifiers.
    is_apc = arm == "slam_apc_hrg"
    cds_id = s.CDS_APC_ID if is_apc else s.CDS_OPA_ID
    raw_event = s.EPISODE_START_DT_TM if is_apc else s.ATTENDANCE_DT_TM
    raw_end = s.EPISODE_END_DT_TM if is_apc else F.lit(None).cast("timestamp")
    source_code = s.FCE_HRG_CD if is_apc else s.HRG_CD
    source_display = s.FCE_HRG_DESC if is_apc else s.HRG_DESC
    event_id = stable_id("hrg_grouping:slam", F.lit(arm), cds_id)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)],
        SRC_SLAM_APC_HRG if is_apc else SRC_SLAM_OP_HRG,
        cds_id,
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    ns = lambda: F.lit(None).cast("string")
    ni = lambda: F.lit(None).cast("int")
    nt = lambda: F.lit(None).cast("timestamp")
    # contract v2: publish source_object and the native CDS id for each SLAM arm while retaining the SHA event key
    return s.select(
        event_id.alias("patient_event_key"),
        F.lit(arm).alias("source_object"),
        cds_id.cast("string").alias("cds_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        _clamped_ts(raw_event.cast("timestamp")).alias("event_datetime"),
        _clamped_ts(raw_end.cast("timestamp")).alias("event_end_datetime"),
        F.when(source_code.isNotNull(), F.lit("urn:barts:slam:hrg"))
         .alias("source_coding_system"),
        source_code.cast("string").alias("source_code"),
        source_display.alias("source_display"),
        cds_id.cast("string").alias("cds_record_id"),
        s.PERSON_LINK_METHOD.alias("person_link_method"),
        (s.ADMISSION_DT_TM if is_apc else nt()).alias("admission_datetime"),
        (s.DISCHARGE_DT_TM if is_apc else nt()).alias("discharge_datetime"),
        (s.EPISODE_START_DT_TM if is_apc else nt()).alias("episode_start_datetime"),
        (s.EPISODE_END_DT_TM if is_apc else nt()).alias("episode_end_datetime"),
        (s.HOSP_PROV_SPELL_NUM if is_apc else ns()).alias("hosp_prov_spell_num"),
        (s.PROVIDER_ORG_CD if is_apc else ns()).alias("provider_org_cd"),
        (s.EPISODE_ORDER if is_apc else ni()).alias("episode_order"),
        (s.EPISODE_DURATION_DAYS if is_apc else ni()).alias("episode_duration_days"),
        s.MAIN_SPECIALTY_CD.alias("main_specialty_cd"),
        s.TREATMENT_FUNCTION_CD.alias("treatment_function_cd"),
        (s.ADMISSION_METHOD_CD if is_apc else ns()).alias("admission_method_cd"),
        (s.ADMISSION_SOURCE_CD if is_apc else ns()).alias("admission_source_cd"),
        (s.ADMISSION_SOURCE_DESC if is_apc else ns()).alias("admission_source_desc"),
        (s.DISCHARGE_METHOD_CD if is_apc else ns()).alias("discharge_method_cd"),
        (s.DISCHARGE_DEST_CD if is_apc else ns()).alias("discharge_dest_cd"),
        (s.DISCHARGE_DEST_DESC if is_apc else ns()).alias("discharge_dest_desc"),
        (s.PATIENT_CLASS_CD if is_apc else ns()).alias("patient_class_cd"),
        (s.PATIENT_CLASS_DESC if is_apc else ns()).alias("patient_class_desc"),
        s.SOURCE_AGE.alias("source_age"), s.SOURCE_SEX_CD.alias("source_sex_cd"),
        (s.NEONATAL_CARE_LEVEL_CD if is_apc else ni()).alias("neonatal_care_level_cd"),
        (s.CRITICAL_CARE_DAYS if is_apc else ni()).alias("critical_care_days"),
        (s.REHAB_DAYS if is_apc else ni()).alias("rehab_days"),
        (F.to_json(s.ICD_DIAG_CODES) if is_apc else ns()).alias("icd_diagnosis_codes_json"),
        F.to_json(s.OPCS_PROC_CODES).alias("opcs_procedure_codes_json"),
        (s.FCE_HRG_CD if is_apc else ns()).alias("fce_hrg_cd"),
        (s.FCE_HRG_DESC if is_apc else ns()).alias("fce_hrg_desc"),
        (s.FCE_GROUPING_METHOD_FLAG if is_apc else ns()).alias("fce_grouping_method_flag"),
        (s.FCE_DOMINANT_PROC_CD if is_apc else ns()).alias("fce_dominant_proc_cd"),
        (s.FCE_DOMINANT_PROC_DESC if is_apc else ns()).alias("fce_dominant_proc_desc"),
        (s.FCE_PBC_CD if is_apc else ns()).alias("fce_pbc_cd"),
        (s.FCE_CALC_EPISODE_DUR if is_apc else ni()).alias("fce_calc_episode_duration"),
        (s.FCE_REPORTING_EPISODE_DUR if is_apc else ni())
         .alias("fce_reporting_episode_duration"),
        (s.DOMINANT_EPISODE_FLAG if is_apc else ns()).alias("dominant_episode_flag"),
        (s.SPELL_HRG_CD if is_apc else ns()).alias("spell_hrg_cd"),
        (s.SPELL_HRG_DESC if is_apc else ns()).alias("spell_hrg_desc"),
        (s.SPELL_GROUPING_METHOD_FLAG if is_apc else ns()).alias("spell_grouping_method_flag"),
        (s.SPELL_DOMINANT_PROC_CD if is_apc else ns()).alias("spell_dominant_proc_cd"),
        (s.SPELL_DOMINANT_PROC_DESC if is_apc else ns()).alias("spell_dominant_proc_desc"),
        (s.SPELL_PRIMARY_DIAG_CD if is_apc else ns()).alias("spell_primary_diag_cd"),
        (s.SPELL_PRIMARY_DIAG_DESC if is_apc else ns()).alias("spell_primary_diag_desc"),
        (s.SPELL_SECONDARY_DIAG_CD if is_apc else ns()).alias("spell_secondary_diag_cd"),
        (s.SPELL_SECONDARY_DIAG_DESC if is_apc else ns()).alias("spell_secondary_diag_desc"),
        (s.SPELL_EPISODE_COUNT if is_apc else ni()).alias("spell_episode_count"),
        (s.SPELL_LOS if is_apc else ni()).alias("spell_los"),
        (s.SPELL_REPORTING_LOS if is_apc else ni()).alias("spell_reporting_los"),
        (s.SPELL_CRITICAL_CARE_DAYS if is_apc else ni()).alias("spell_critical_care_days"),
        (s.SPELL_SSC_CD if is_apc else ns()).alias("spell_ssc_cd"),
        (s.SPELL_BEST_PRACTICE_CD if is_apc else ns()).alias("spell_best_practice_cd"),
        (ns() if is_apc else s.FIRST_ATTEND_CD).alias("first_attend_cd"),
        (ns() if is_apc else s.FIRST_ATTEND_DESC).alias("first_attend_desc"),
        (ns() if is_apc else s.GROUPING_METHOD_FLAG).alias("grouping_method_flag"),
        (ns() if is_apc else s.DOMINANT_PROC_CD).alias("dominant_proc_cd"),
        (ns() if is_apc else s.DOMINANT_PROC_DESC).alias("dominant_proc_desc"),
        s.GROUPER_ERRORS.alias("grouper_errors"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        raw_event.cast("timestamp").alias("record_status_effective_from"),
        F.when(retracted, s.SOURCE_ABSENT_DETECTED_TS).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"), F.lit(arm).alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.SOURCE_RECORD_UPDATED_DT.alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("slam").alias("_source_system"),
        F.lit(SRC_SLAM_APC_HRG if is_apc else SRC_SLAM_OP_HRG).alias("_source_table"),
        cds_id.cast("string").alias("_source_row_id"),
    )




def _costed_activity_canonical():
    # Assemble normalized costed activity rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_SLAM_COSTED_ACTIVITY)
    event_id = stable_id("costed_activity:slam", s.EXTRACT_CD, s.ACTIVITY_RECORD_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)],
        SRC_SLAM_COSTED_ACTIVITY,
        F.concat_ws(":", s.EXTRACT_CD, s.ACTIVITY_RECORD_ID.cast("string")),
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    # contract v2: publish the SLAM native composite with BIGINT activity_record_id and native person/encounter types
    return s.select(
        event_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.ACTIVITY_START_DT_TM.alias("event_datetime"),
        s.ACTIVITY_END_DT_TM.alias("event_end_datetime"),
        F.when(s.POD_CD.isNotNull(), F.lit("urn:barts:slam:pod"))
         .alias("source_coding_system"),
        s.POD_CD.alias("source_code"), F.lit(None).cast("string").alias("source_display"),
        s.EXTRACT_CD.alias("extract_cd"),
        s.ACTIVITY_RECORD_ID.cast("bigint").alias("activity_record_id"),
        s.FEED_TYPE.alias("feed_type"), s.PLEMI.alias("plemi"),
        s.NHS_NUMBER_STATUS_CD.alias("nhs_number_status_cd"), s.CDS_ID.alias("cds_id"),
        s.ATTENDANCE_ID.alias("attendance_id"), s.ARRIVAL_DT.alias("arrival_date"),
        s.ARRIVAL_TM.alias("arrival_time"), s.DEPARTURE_DT.alias("departure_date"),
        s.DEPARTURE_TM.alias("departure_time"), s.DEPARTURE_TYPE_CD.alias("departure_type_cd"),
        s.PROVIDER_ORG_CD.alias("provider_org_cd"), s.PATIENT_ORG_CD.alias("patient_org_cd"),
        s.PATHWAY_ID.alias("pathway_id"), s.POD_CD.alias("pod_cd"),
        s.TREATMENT_FUNCTION_CD.alias("treatment_function_cd"), s.SOURCE_LOS.alias("source_los"),
        s.CF_BAND_CD.alias("cf_band_cd"), s.EPISODE_NUMBER.alias("episode_number"),
        s.EPISODE_START_DT_TM.alias("episode_start_datetime"),
        s.EPISODE_END_DT_TM.alias("episode_end_datetime"),
        s.EPISODE_TYPE_CD.alias("episode_type_cd"), s.HOSP_SPELL_ID.alias("hosp_spell_id"),
        s.HRG_CD.alias("hrg_cd"), s.HRG_DESC.alias("hrg_desc"),
        s.FCE_HRG_CD.alias("fce_hrg_cd"), s.FCE_HRG_DESC.alias("fce_hrg_desc"),
        s.SPELL_HRG_CD.alias("spell_hrg_cd"), s.SPELL_HRG_DESC.alias("spell_hrg_desc"),
        s.APPOINTMENT_DT.alias("appointment_date"), s.APPOINTMENT_TM.alias("appointment_time"),
        s.CRITICAL_CARE_UNIT_FUNCTION_CD.alias("critical_care_unit_function_cd"),
        s.ORGANS_SUPPORTED.alias("organs_supported"),
        s.CRITICAL_CARE_PERIOD_TYPE_CD.alias("critical_care_period_type_cd"),
        s.CRITICAL_CARE_LEVEL_IND.alias("critical_care_level_ind"),
        s.UNBUNDLED_ACTIVITY_DT_TM.alias("unbundled_activity_datetime"),
        s.UNBUNDLED_ACTIVITY_CD.alias("unbundled_activity_cd"),
        s.UNBUNDLED_HRG_CD.alias("unbundled_hrg_cd"),
        s.UNBUNDLED_HRG_DESC.alias("unbundled_hrg_desc"),
        s.PARTIAL_COSTING_IND.alias("partial_costing_ind"), s.CARE_DT_TM.alias("care_datetime"),
        s.CARE_ID.alias("care_id"), s.CLINICAL_CONTACT_DURATION.alias("clinical_contact_duration"),
        s.CHS_CURRENCY_CD.alias("chs_currency_cd"), s.TEAM_TYPE_CD.cast("string").alias("team_type_cd"),
        s.CONTACT_SUBJECT_CD.alias("contact_subject_cd"), s.CONSULT_TYPE_CD.alias("consult_type_cd"),
        s.CONSULT_MEDIUM_CD.alias("consult_medium_cd"), s.LOCATION_CD.alias("location_cd"),
        s.GP_THERAPY_IND.alias("gp_therapy_ind"), s.SERVICE_REQUEST_ID.alias("service_request_id"),
        s.COST_LINE_COUNT.alias("cost_line_count"),
        s.TOTAL_COST_SUM.cast("decimal(38,6)").alias("total_cost_sum"),
        s.TOTAL_O_COST_SUM.cast("decimal(38,6)").alias("total_o_cost_sum"),
        s.PERSON_LINK_METHOD.alias("person_link_method"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        s.ACTIVITY_START_DT_TM.alias("record_status_effective_from"),
        F.when(retracted, s.SOURCE_ABSENT_DETECTED_TS).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("slam_costed_activity").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("slam-plics").alias("_source_system"),
        F.lit(SRC_SLAM_COSTED_ACTIVITY).alias("_source_table"),
        F.concat_ws(":", s.EXTRACT_CD, s.ACTIVITY_RECORD_ID.cast("string"))
         .alias("_source_row_id"),
    )


SRC_SLAM_COSTED_ACTIVITY = "4_prod.bronze.map_slam_costed_activity"




def _drug_expenditure_canonical():
    # Assemble normalized drug expenditure rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_FINANCE_HCD)
    event_id = stable_id("drug_expenditure:hcd", s.ROW_HASH)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_FINANCE_HCD, s.ROW_HASH
    )
    coded = _usable_code(s.DMD_CODE)
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    # contract v2: publish the HCD source row hash as evidence and native identifier types while retaining the SHA event key
    return s.select(
        event_id.alias("patient_event_key"),
        s.ROW_HASH.cast("string").alias("source_row_hash"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"), s.EFFECTIVE_DT_TM.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.when(coded, F.lit("http://snomed.info/sct"))
         .otherwise(F.lit("urn:barts:hcd:chargeable-item")).alias("source_coding_system"),
        F.when(coded, s.DMD_CODE.cast("string"))
         .otherwise(s.CHARGEABLE_ITEM).alias("source_code"),
        F.coalesce(s.DMD_CONCEPT_NAME, s.CHARGEABLE_ITEM).alias("source_display"),
        s.TRANSACTION_ID.alias("transaction_id"), s.FINANCIAL_YEAR.alias("financial_year"),
        s.FINANCIAL_MONTH.alias("financial_month"), s.REPORTING_YEAR.alias("reporting_year"),
        s.REPORTING_MONTH.alias("reporting_month"), s.PROVIDER_ORG_CD.alias("provider_org_cd"),
        s.SITE_CD.alias("site_cd"), s.SITE_NAME.alias("site_name"),
        s.SPECIALTY_CD.alias("specialty_cd"), s.CONSULTANT_CD.alias("consultant_cd"),
        s.PATIENT_TYPE.alias("patient_type"), s.POD_CD.alias("pod_cd"),
        s.CHARGEABLE_ITEM.alias("chargeable_item"), s.ADDITIONAL_INFO.alias("additional_info"),
        s.DMD_RAW.alias("dmd_raw"), s.DMD_CODE.alias("dmd_code"),
        s.DMD_CONCEPT_ID.cast("string").alias("dmd_concept_id"),
        s.DMD_CONCEPT_NAME.alias("dmd_concept_name"),
        s.DRUG_STANDARD_CONCEPT_ID.cast("string").alias("drug_standard_concept_id"),
        s.DRUG_STANDARD_CONCEPT_NAME.alias("drug_standard_concept_name"),
        s.DMD_MAPPING_STATUS.alias("dmd_mapping_status"), s.DMD_TAXONOMY_CD.alias("dmd_taxonomy_cd"),
        s.ROUTE_OF_ADMINISTRATION.alias("route_of_administration"), s.STRENGTH.alias("strength"),
        s.VOLUME.alias("volume"), s.PACK_SIZE.alias("pack_size"), s.QUANTITY.alias("quantity"),
        s.UNIT_OF_MEASURE.alias("unit_of_measure"), s.DISPENSING_ROUTE.alias("dispensing_route"),
        s.DISPENSING_LOCATION.alias("dispensing_location"), s.INDICATION.alias("indication"),
        s.FUNDING_REFERENCE.alias("funding_reference"), s.HCDR_CATEGORY_CD.alias("hcdr_category_cd"),
        s.HCDR_CATEGORY_DESC.alias("hcdr_category_desc"), s.CCG_RESIDENCE_CD.alias("ccg_residence_cd"),
        s.CCG_GP_CD.alias("ccg_gp_cd"), s.COMMISSIONER_CD.alias("commissioner_cd"),
        s.COMMISSIONER_TYPE.alias("commissioner_type"), s.SERVICE_LINE.alias("service_line"),
        s.SERVICE_CATEGORY_CD.alias("service_category_cd"),
        s.UNIT_PRICE_SUPPLIER.alias("unit_price_supplier"),
        s.UNIT_PRICE_COMMISSIONER.alias("unit_price_commissioner"), s.VAT.alias("vat"),
        s.VAT_CD.alias("vat_cd"), s.INCOME.alias("income"), s.COST.alias("cost"),
        s.MARGIN.alias("margin"), s.LLOYDS_DISPENSING_FEE.alias("lloyds_dispensing_fee"),
        s.PRODUCTION_FEE.alias("production_fee"),
        s.FIXED_PATIENT_INCOME.alias("fixed_patient_income"),
        s.COST_CENTRE_DESC.alias("cost_centre_desc"), s.DRUG_FEED.alias("drug_feed"),
        s.DATA_SET.alias("data_set"), s.DRUG_CATEGORY.alias("drug_category"),
        s.LEDGER_CD.alias("ledger_cd"), s.EXCLUSION_FLAG.alias("exclusion_flag"),
        s.EXCLUSION_REASON.alias("exclusion_reason"), s.LEDGER_LV3_CD.alias("ledger_lv3_cd"),
        s.LEDGER_LV3_DESC.alias("ledger_lv3_desc"), s.LEDGER_LV6_CD.alias("ledger_lv6_cd"),
        s.LEDGER_LV6_DESC.alias("ledger_lv6_desc"), s.LEDGER_LV7_CD.alias("ledger_lv7_cd"),
        s.LEDGER_LV7_DESC.alias("ledger_lv7_desc"), s.LEDGER_LV9_CD.alias("ledger_lv9_cd"),
        s.LEDGER_LV9_DESC.alias("ledger_lv9_desc"), s.SLR_CD.alias("slr_cd"),
        s.DIABETIC_FLAG.alias("diabetic_flag"), s.IMCOE_FLAG.alias("imcoe_flag"),
        s.SOURCE_DUPLICATE_COUNT.alias("source_duplicate_count"),
        s.PERSON_LINK_METHOD.alias("person_link_method"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        s.EFFECTIVE_DT_TM.alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("finance_hcd_expenditure").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("slr-finance").alias("_source_system"),
        F.lit(SRC_FINANCE_HCD).alias("_source_table"), s.ROW_HASH.alias("_source_row_id"),
        s.DMD_CODE.cast("string").alias("_dmd_code"), s.DMD_CONCEPT_NAME.alias("_dmd_display"),
        s.DRUG_STANDARD_CONCEPT_ID.cast("string").alias("_omop_code"),
        s.DRUG_STANDARD_CONCEPT_NAME.alias("_omop_display"),
    )


SRC_FINANCE_HCD = "4_prod.tmp.journey_finance_hcd_expenditure_s42"




def _medication_supply_canonical_pregate():
    # Normalize medication supply source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_HOMECARE_REQUEST)
    person_id = s.PERSON_ID.cast("bigint")
    event_id = stable_id("medication_supply:jac", s.HOMECARE_REQUEST_ITEM_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", person_id), ("urn:jac:lnkpid", s.LNKPID)],
        SRC_HOMECARE_REQUEST,
        s.HOMECARE_REQUEST_ITEM_ID,
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    coded = _usable_code(s.DMD_VTM_CODE)
    # contract v2: publish the JAC homecare request item id and native identifier types while retaining the SHA event key
    return s.select(
        event_id.alias("patient_event_key"),
        s.HOMECARE_REQUEST_ITEM_ID.cast("string").alias("homecare_request_item_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        person_id.alias("person_id"),
        F.when(person_id.isNotNull(), F.lit("resolved"))
         .when(s.LNKPID.isNotNull(), F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.REQUEST_CREATED.cast("timestamp").alias("event_datetime"),
        s.REQUEST_COMPLETED.cast("timestamp").alias("event_end_datetime"),
        F.when(coded, F.lit("http://snomed.info/sct"))
         .otherwise(F.lit("urn:jac:homecare:item")).alias("source_coding_system"),
        _code_or_display(s.DMD_VTM_CODE, s.ITEM_DESCRIPTION).alias("source_code"),
        F.coalesce(s.DMD_VTM_NAME, s.ITEM_DESCRIPTION).alias("source_display"),
        s.REQUEST_KEY.alias("request_key"), s.ITEM_SEQ.alias("item_seq"),
        s.REQUEST_STATUS.alias("request_status_code"),
        s.REQUEST_STATUS_DESC.alias("request_status_display"),
        s.ITEM_STATUS.alias("item_status_code"), s.ITEM_STATUS_DESC.alias("item_status_display"),
        s.ITEM_TYPE.alias("item_type"), s.ITEM_DESCRIPTION.alias("item_description"),
        s.PACK_DESCRIPTION.alias("pack_description"), s.REQUEST_QUANTITY.alias("quantity_requested"),
        s.ORIGINAL_QUANTITY.alias("quantity_original"),
        s.QUANTITY_DELIVERED.alias("quantity_delivered"), s.ORDER_UNIT.alias("order_unit"),
        s.LABEL_DIRECTIONS.alias("label_directions"), s.NFD_REASON.alias("nfd_reason"),
        s.SUPPLY_START_DATE.cast("timestamp").alias("supply_start_date"),
        s.SUPPLY_INTERVAL.alias("supply_interval"), s.SUPPLY_PERIOD.alias("supply_period"),
        s.TOTAL_ITEM_COUNT.alias("request_item_count"),
        s.COMPLETE_ITEM_COUNT.alias("request_complete_item_count"),
        s.REQUEST_RELEASED.cast("timestamp").alias("request_released_date"),
        s.LOCATION_NAME.alias("location_name"), s.COSTCENTRE_NAME.alias("cost_centre_name"),
        s.INDICATION.alias("indication"), s.CLINIC.alias("clinic"),
        s.DMD_VTM_CONCEPT_ID.cast("string").alias("dmd_vtm_concept_id"),
        s.DMD_VTM_CODE.cast("string").alias("dmd_vtm_code"),
        s.DMD_VTM_NAME.alias("dmd_vtm_name"), s.DRUG_MAPPING_METHOD.alias("drug_mapping_method"),
        s.CARE_SITE_CD.cast("string").alias("care_site_cd"),
        s.CARE_SITE_MATCH_METHOD.alias("care_site_match_method"),
        s.LNKPID.alias("lnkpid"), s.NAME_KEY.alias("name_key"),
        s.MRN_CANDIDATES.alias("mrn_candidates"), s.NHS_CANDIDATES.alias("nhs_candidates"),
        s.PERSON_MATCH_METHOD.alias("person_match_method"),
        s.PERSON_MATCH_STATUS.alias("person_match_status"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        s.REQUEST_CREATED.cast("timestamp").alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("clinical").alias("fact_category"), F.lit("homecare_request").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.coalesce(s.ITEM_SOURCE_RECORD_UPDATED_DT, s.SOURCE_RECORD_UPDATED_DT)
         .alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"), F.lit("jac-homecare").alias("_source_system"),
        F.lit(SRC_HOMECARE_REQUEST).alias("_source_table"),
        s.HOMECARE_REQUEST_ITEM_ID.alias("_source_row_id"),
        s.DMD_VTM_CODE.cast("string").alias("_dmd_code"), s.DMD_VTM_NAME.alias("_dmd_display"),
    )

def _medication_supply_canonical():
    # Keep the normalized medication supply rows that pass the existing source-code admission
    # rule.
    return _medication_supply_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_HOMECARE_REQUEST = "4_prod.bronze.map_homecare_request"




SRC_EAL_PROCEDURE = "4_prod.bronze.map_elective_access_list_procedure"

def _elective_access_entry_canonical():
    # Assemble normalized elective access entry rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    s = read_source(SRC_EAL)
    prim = read_source(SRC_EAL_PROCEDURE).where(
        F.coalesce(F.col("SOURCE_PRESENT_IND"), F.lit(True))
        & (F.col("PROCEDURE_TYPE_SEQ") == 1)
        & (F.col("PROCEDURE_SEQ") == 1)
    ).select("WAITING_LIST_OID", "PROCEDURE_CODE", "PROCEDURE_DESC", "PROCEDURE_CATALOG")
    prio = read_source(SRC_EAL_ATTRIBUTE).where(
        F.coalesce(F.col("SOURCE_PRESENT_IND"), F.lit(True))
    ).select(
        "WAITING_LIST_OID",
        F.col("FIELD_VALUE_VAR").alias("_p_code"),
        F.col("FIELD_VALUE_DATE_CLEAN").alias("_p_date"),
    )
    j = s.join(prim, "WAITING_LIST_OID", "left").join(prio, "WAITING_LIST_OID", "left")
    event_id = stable_id("elective_access:luna", s.SOURCE_SYSTEM_OID, s.WAITING_LIST_OID)
    luna_pid = F.when(
        s.SOURCE_SYSTEM_OID.isNotNull() & s.PATIENT_OID.isNotNull(),
        F.concat_ws(":", s.SOURCE_SYSTEM_OID.cast("string"), s.PATIENT_OID.cast("string")),
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID), ("urn:barts:luna:patient-oid", luna_pid)],
        SRC_EAL,
        s.WAITING_LIST_OID.cast("string"),
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    superseded = ~F.coalesce(s.ACTIVE_IND, F.lit(True))
    coded = _usable_code(j.PROCEDURE_CODE)
    # contract v2: publish the LUNA native composite as BIGINT columns and native person/encounter types while retaining the SHA event key
    return j.select(
        event_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .when(luna_pid.isNotNull(), F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.CREATED_DT_TM.alias("event_datetime"), s.ADMIT_DT_TM_CLEAN.alias("event_end_datetime"),
        F.when(coded, F.lit("http://fhir.hl7.org.uk/CodeSystem/OPCS-4"))
         .alias("source_coding_system"),
        F.when(coded, j.PROCEDURE_CODE.cast("string")).alias("source_code"),
        j.PROCEDURE_DESC.alias("source_display"),
        s.WAITING_LIST_OID.cast("bigint").alias("waiting_list_oid"),
        s.SOURCE_SYSTEM_OID.cast("bigint").alias("source_system_oid"),
        s.PATHWAY_OID.cast("string").alias("pathway_oid"),
        s.REFERRAL_OID.cast("string").alias("referral_oid"),
        s.PATIENT_OID.cast("string").alias("patient_oid"),
        s.WAITING_LIST_ID.alias("waiting_list_id"),
        s.LEGACY_WAITING_LIST_ID.alias("legacy_waiting_list_id"),
        s.WAITING_LIST_NAME.alias("waiting_list_name"), s.WAITING_LIST_CODE.alias("waiting_list_code"),
        s.DEPARTMENT.alias("department"), s.DIVISION.alias("division"),
        s.BUSINESS_UNIT.alias("business_unit"), F.col("_p_code").alias("clinical_priority_code"),
        F.col("_p_date").alias("clinical_priority_recorded_datetime"),
        s.SITE_RVID.alias("site_rvid"), s.TREATMENT_FUNCTION_RVID.alias("treatment_function_rvid"),
        s.ADMIN_CATEGORY_RVID.alias("admin_category_rvid"),
        s.INTENDED_MANAGEMENT_RVID.alias("intended_management_rvid"),
        s.ADMIT_METHOD_RVID.alias("admit_method_rvid"),
        s.WAITING_LIST_PRIORITY_RVID.alias("priority_rvid"),
        s.WAITING_LIST_STATUS_RVID.alias("status_rvid"),
        s.ELECTIVE_ADMISSION_TYPE_RVID.alias("elective_admission_type_rvid"),
        s.ENCOUNTER_TYPE_RVID.alias("encounter_type_rvid"),
        s.REMOVAL_REASON_RVID.alias("removal_reason_rvid"), s.DIVISION_RVID.alias("division_rvid"),
        s.TCI_LOCATION_RVID.alias("tci_location_rvid"),
        s.ADMIT_OFFER_OUTCOME_RVID.alias("admit_offer_outcome_rvid"),
        s.LEAD_CLINICIAN_PRID.alias("lead_clinician_prid"),
        s.WAITING_LIST_STATUS_REASON.alias("status_reason"),
        s.WAITING_LIST_STATUS_CHANGE_DT_TM_CLEAN.alias("status_change_datetime"),
        s.DECIDED_TO_ADMIT_DT_TM_CLEAN.alias("decided_to_admit_datetime"),
        s.TCI_DT_TM_CLEAN.alias("tci_datetime"), s.TCI_DT_TM_FUTURE_IND.alias("tci_future_ind"),
        s.TCI_CREATED_DT_TM.alias("tci_created_datetime"),
        s.GUARANTEED_ACTIVITY_DT_TM_CLEAN.alias("guaranteed_activity_datetime"),
        s.ACTUAL_GUARANTEED_ACTIVITY_DT_TM_CLEAN.alias("actual_guaranteed_activity_datetime"),
        s.PLANNED_DT_TM_CLEAN.alias("planned_datetime"),
        s.EARLIEST_REASONABLE_OFFER_DT_TM_CLEAN.alias("earliest_reasonable_offer_datetime"),
        s.ADMIT_DT_TM_CLEAN.alias("admit_datetime"), s.COMMENTS.alias("comments"),
        s.ACTIVE_IND.alias("active_ind"), s.CREATED_DT_TM.alias("created_datetime"),
        s.CREATED_BY_PRID.alias("created_by_prid"), s.MODIFIED_DT_TM.alias("modified_datetime"),
        s.MODIFIED_BY_PRID.alias("modified_by_prid"), s.PERSON_LINK_STATUS.alias("person_link_status"),
        s.PERSON_LINK_METHOD.alias("person_link_method"),
        s.IDENTIFIER_LINK_STATUS.alias("identifier_link_status"),
        s.LINKAGE_HISTORICAL_FALLBACK_IND.alias("linkage_historical_fallback_ind"),
        s.LINKAGE_FALLBACK_CONFLICT_IND.alias("linkage_fallback_conflict_ind"),
        s.NHS_NUMBER_VALID_IND.alias("nhs_number_valid_ind"),
        F.when(retracted, F.lit("retracted"))
         .when(superseded, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        s.CREATED_DT_TM.alias("record_status_effective_from"),
        F.when(retracted, s.SOURCE_ABSENT_DETECTED_TS)
         .when(superseded, s.MODIFIED_DT_TM).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("elective_access_list").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.MODIFIED_DT_TM.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("luna").alias("_source_system"), F.lit(SRC_EAL).alias("_source_table"),
        s.WAITING_LIST_OID.cast("string").alias("_source_row_id"),
        F.when(coded, j.PROCEDURE_CODE.cast("string")).alias("_opcs4_code"),
        j.PROCEDURE_DESC.alias("_opcs4_display"),
    )


SRC_EAL = "4_prod.bronze.map_elective_access_list"

SRC_EAL_ATTRIBUTE = "4_prod.bronze.map_elective_access_list_attribute"




def _pathway_tracking_canonical():
    # Assemble normalized pathway tracking rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_CANCER_PTL)
    event_id = stable_id("pathway_tracking:pathfinder", s.PTL_UNIQUE_ID)
    luna_pid = F.when(
        s.SOURCE_SYSTEM_OID.isNotNull() & s.PATIENT_OID.isNotNull(),
        F.concat_ws(":", s.SOURCE_SYSTEM_OID.cast("string"), s.PATIENT_OID.cast("string")),
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID), ("urn:barts:luna:patient-oid", luna_pid)],
        SRC_CANCER_PTL,
        s.PTL_UNIQUE_ID.cast("string"),
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    # contract v2: publish the Pathfinder native PTL identifier and native person/encounter types while retaining the SHA event key
    return s.select(
        event_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .when(luna_pid.isNotNull(), F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.LATEST_ACTIVITY_DATE_CLEAN.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit(None).cast("string").alias("source_coding_system"),
        F.lit(None).cast("string").alias("source_code"),
        F.lit(None).cast("string").alias("source_display"),
        s.PTL_UNIQUE_ID.cast("bigint").alias("ptl_unique_id"),
        s.PTL_GROUP_ID.alias("ptl_group_id"), s.ARCHIVED_PATHWAY.alias("archived_pathway"),
        s.PATHWAY_OID.cast("string").alias("pathway_oid"),
        s.REFERRAL_OID.cast("string").alias("referral_oid"),
        s.PATIENT_OID.cast("string").alias("patient_oid"),
        s.PTL_ACTIVITY_OID.cast("string").alias("ptl_activity_oid"),
        s.PARENT_PTL_UNIQUE_ID.cast("string").alias("parent_ptl_unique_id"),
        s.PARENT_PTL_ACTIVITY_OID.cast("string").alias("parent_ptl_activity_oid"),
        s.PARENT_PRESENT_IND.alias("parent_present_ind"),
        s.LATEST_ACTIVITY_TYPE.alias("latest_activity_type"),
        s.LATEST_ACTIVITY_OID.cast("string").alias("latest_activity_oid"),
        s.LATEST_ACTIVITY_DATE_FUTURE_IND.alias("latest_activity_date_future_ind"),
        s.DAYS_WAITED.alias("days_waited"), s.SPECIALTY.alias("specialty"),
        s.TREATMENT_FUNCTION.alias("treatment_function"),
        s.TREATMENT_FUNCTION_CODE.alias("treatment_function_code"), s.SITE.alias("site"),
        s.SITE_GROUP.alias("site_group"), s.DIVISION.alias("division"),
        s.LEAD_CLINICIAN.alias("lead_clinician"),
        s.LEAD_CLINICIAN_PRID.alias("lead_clinician_prid"), s.SITE_RVID.alias("site_rvid"),
        s.SOURCE_KEY_STATUS.alias("source_key_status"), s.PATIENT_SPINE_IND.alias("patient_spine_ind"),
        s.PATHWAY_SPINE_IND.alias("pathway_spine_ind"),
        s.REFERRAL_SPINE_IND.alias("referral_spine_ind"),
        s.PATIENT_SPINE_LINK_STATUS.alias("patient_spine_link_status"),
        s.PATHWAY_SPINE_LINK_STATUS.alias("pathway_spine_link_status"),
        s.REFERRAL_SPINE_LINK_STATUS.alias("referral_spine_link_status"),
        s.NHS_NUMBER_VALID_IND.alias("nhs_number_valid_ind"),
        s.LINKAGE_HISTORICAL_FALLBACK_IND.alias("linkage_historical_fallback_ind"),
        s.LINKAGE_FALLBACK_CONFLICT_IND.alias("linkage_fallback_conflict_ind"),
        s.PERSON_LINK_STATUS.alias("person_link_status"),
        s.PERSON_LINK_METHOD.alias("person_link_method"),
        s.IDENTIFIER_LINK_STATUS.alias("identifier_link_status"),
        s.SOURCE_SYSTEM_OID.alias("source_system_oid"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        s.LATEST_ACTIVITY_DATE_CLEAN.alias("record_status_effective_from"),
        F.when(retracted, s.SOURCE_ABSENT_DETECTED_TS).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"), F.lit("cancer_ptl").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.PIPELINE_UPDT_DT_TM.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("luna-pathfinder").alias("_source_system"),
        F.lit(SRC_CANCER_PTL).alias("_source_table"),
        s.PTL_UNIQUE_ID.cast("string").alias("_source_row_id"),
    )


SRC_CANCER_PTL = "4_prod.bronze.map_cancer_ptl"




def _pregnancy_reconciliation_source():
    # Publish unmatched maternity records with their original identifiers and reconciliation
    # reasons.
    s = read_source(SRC_MAT_MSDS_UNMATCHED)
    # contract v2: rename the deterministic pregnancy reconciliation identity as a key
    return s.select(
        stable_id("pregnancy_reconciliation:msds", s.UNMATCHED_KEY)
         .alias("pregnancy_reconciliation_key"),
        s.UNMATCHED_REASON.alias("unmatched_reason"),
        s.PREGNANCYID_RAW.alias("pregnancy_id_raw"),
        s.Pregnancy_ID.cast("string").alias("pregnancy_id"),
        s.LPIDMother.alias("lpid_mother"),
        s.AntenatalAppointmentDate.alias("antenatal_appointment_date"),
        s.PregnancyFirstContactDate.alias("pregnancy_first_contact_date"),
        s.ExpectedDeliveryDate.alias("expected_delivery_date"),
        s.LastMensPeriodDate.alias("last_mens_period_date"),
        s.FolicAcidSupplement_CD.alias("folic_acid_supplement_cd"),
        s.PreviousLiveBirths.alias("previous_live_births"),
        s.PreviousStillBirths.alias("previous_still_births"),
        s.PreviousLossesUnder24Weeks.alias("previous_losses_under_24_weeks"),
        s.PreviousCaesareanSections.alias("previous_caesarean_sections"),
        s.SOURCE_SYSTEM.alias("source_system_code"), s.IS_VALID.alias("is_valid"),
        s.MSDS_SOURCE_VERSION.alias("msds_source_version"), F.lit("active").alias("record_status"),
        s.RECORD_UPDATED_DT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
    )


SRC_MAT_MSDS_UNMATCHED = "4_prod.bronze.map_mat_pregnancy_msds_unmatched"




def _critical_care_period_canonical():
    # Assemble normalized critical care period rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    s = read_source(SRC_CC_PERIOD)
    ranked = s.select(
        "*",
        F.struct(
            F.coalesce(F.col("CURRENT_IND").cast("int"), F.lit(0)).alias("_cur"),
            F.coalesce(
                F.col("Record_Updated_Dt").cast("timestamp"),
                F.lit("1900-01-01").cast("timestamp"),
            ).alias("_upd"),
            F.coalesce(F.col("Crit_Care_Period_Id"), F.lit(-1)).alias("_sid"),
        ).alias("_pick"),
    )
    best = ranked.groupBy("PERIOD_BUSINESS_KEY").agg(F.max("_pick").alias("_best"))
    d = ranked.join(best, ["PERIOD_BUSINESS_KEY"]).where(F.col("_pick") == F.col("_best"))
    event_id = stable_id("critical_care_period:cds", d.PERIOD_BUSINESS_KEY)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", d.PERSON_ID)], SRC_CC_PERIOD, d.PERIOD_BUSINESS_KEY
    )
    retracted = ~F.coalesce(d.SOURCE_PRESENT_IND, F.lit(True))
    # contract v2: publish native critical-care and Millennium identifiers while retaining SHA event and cross-encounter keys
    return d.select(
        event_id.alias("patient_event_key"),
        d.Crit_Care_Period_Id.cast("bigint").alias("crit_care_period_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        d.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(d.PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        d.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        d.CC_Period_Start_Dt_Tm_CLEAN.alias("event_datetime"),
        d.CC_Period_Disch_Dt_Tm_CLEAN.alias("event_end_datetime"),
        F.lit(None).cast("string").alias("source_coding_system"),
        F.lit(None).cast("string").alias("source_code"),
        F.lit(None).cast("string").alias("source_display"),
        d.CC_Period_Disch_Dt_Tm_CLEAN.alias("period_end_datetime"),
        d.PERIOD_BUSINESS_KEY.cast("bigint").alias("period_business_key"),
        (~F.coalesce(d.CURRENT_IND, F.lit(False))).alias("no_current_version_ind"),
        d.BUSINESS_KEY_STATUS.alias("business_key_status"),
        d.Is_Valid.alias("source_valid_ind"), d.CC_TYPE_DESC.alias("care_type"),
        d.CC_UNIT_FUNCTION_DESC.alias("unit_function"), d.Unit_Id.alias("unit_id"),
        d.Source_System.alias("cds_source_system"), d.CC_Level2_Days.alias("level2_days"),
        d.CC_Level3_Days.alias("level3_days"),
        d.CC_No_Organ_Systems.alias("organ_systems_supported"),
        d.Gestation_Length.alias("gestation_length"),
        d.CC_DISCH_STATUS_DESC.alias("discharge_status"),
        d.CC_DISCH_DEST_DESC.alias("discharge_destination"),
        d.ENCNTR_ID.cast("string").alias("source_encounter_id"),
        d.CC_ENCNTR_ID.cast("bigint").alias("cc_encounter_id"),
        F.when(d.CC_ENCNTR_ID.isNotNull(), stable_id("encounter:mill", d.CC_ENCNTR_ID))
         .alias("cc_encounter_key"),
        d.CDS_APC_ID.alias("cds_apc_id"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
         .alias("record_status"),
        d.CC_Period_Start_Dt_Tm_CLEAN.alias("record_status_effective_from"),
        F.when(retracted, d.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("critical_care_period").alias("source_feed"),
        F.date_format(d.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        d.PIPELINE_UPDT_DT_TM.alias("source_update_timestamp"),
        d.ADC_UPDT.alias("loaded_at"), F.lit("cds-ccmds").alias("_source_system"),
        F.lit(SRC_CC_PERIOD).alias("_source_table"),
        d.PERIOD_BUSINESS_KEY.cast("string").alias("_source_row_id"),
    )




# ==== Remaining planes and supporting products ====

SRC_CC_PERIOD = "4_prod.bronze.map_critical_care_period"




def _critical_care_activity_canonical_pregate():
    # Normalize critical care activity source rows before the downstream admission filter so
    # excluded evidence remains countable.
    s = read_source(SRC_CC_ACTIVITY)
    event_id = stable_id("critical_care_activity:cds", s.ROW_HASH)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_CC_ACTIVITY, s.ROW_HASH
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    # contract v2: publish the CCMDS source row hash as evidence and native identifier types while retaining the SHA event key
    return s.select(
        event_id.alias("patient_event_key"),
        s.ROW_HASH.cast("string").alias("source_row_hash"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.Activity_Date_CLEAN.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:nhs:ccmds:activity").alias("source_coding_system"),
        s.Activity_Code.cast("string").alias("source_code"),
        s.ACTIVITY_DESC.alias("source_display"),
        s.PERIOD_LINK_STATUS.alias("period_link_status"),
        s.PERIOD_BUSINESS_KEY.cast("string").alias("period_business_key"),
        s.PARENT_PERIOD_SURROGATE_ID.cast("string").alias("parent_period_id"),
        s.CDS_APC_ID.alias("cds_apc_id"),
        F.when(s.CC_Type == 1, F.lit("adult"))
         .when(s.CC_Type == 2, F.lit("neonatal"))
         .otherwise(s.CC_Type.cast("string")).alias("cc_type"),
        s.SOURCE_DUPLICATE_COUNT.alias("source_duplicate_count"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
         .alias("record_status"),
        s.Activity_Date_CLEAN.alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("clinical").alias("fact_category"),
        F.lit("critical_care_activity").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.PIPELINE_UPDT_DT_TM.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("cds-ccmds").alias("_source_system"),
        F.lit(SRC_CC_ACTIVITY).alias("_source_table"),
        s.ROW_HASH.cast("string").alias("_source_row_id"),
    )

def _critical_care_activity_canonical():
    # Keep the normalized critical care activity rows that pass the existing source-code
    # admission rule.
    return _critical_care_activity_canonical_pregate().where(
        _usable_code(F.col("source_code"))
    )


SRC_CC_ACTIVITY = "4_prod.bronze.map_critical_care_activity"




def _critical_care_admission_canonical():
    # Assemble normalized critical care admission rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    s = read_source(SRC_CC_ADMISSION)
    event_id = stable_id("critical_care_admission:medicus", s.ADMISSION_KEY)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_CC_ADMISSION, s.ADMISSION_KEY
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    # contract v2: publish native person/encounter types and retain the Medicus admission key with the SHA event key
    return s.select(
        event_id.alias("patient_event_key"),
        s.SOURCE_PAT_ID.cast("bigint").alias("source_pat_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.date_unit_adm_CLEAN.alias("event_datetime"),
        s.date_unit_discharge_CLEAN.alias("event_end_datetime"),
        F.lit(None).cast("string").alias("source_coding_system"),
        F.lit(None).cast("string").alias("source_code"),
        F.lit(None).cast("string").alias("source_display"),
        s.ADMISSION_KEY.alias("admission_key"), s.SOURCE_UNIT.alias("source_unit"),
        s.UNIT_RAW.alias("unit_raw"), s.SOURCE_SITE.alias("source_site"),
        s.date_hospital_adm_CLEAN.alias("hospital_admission_datetime"),
        s.date_unit_adm_CLEAN.alias("unit_admission_datetime"),
        s.date_unit_discharge_CLEAN.alias("unit_discharge_datetime"),
        s.date_hospital_discharge_CLEAN.alias("hospital_discharge_datetime"),
        s.dgn_adm1_code.alias("admission_diagnosis_code"),
        s.maxorgansupp.alias("max_organ_support"), s.cause_of_death.alias("cause_of_death"),
        s.date_of_death_CLEAN.alias("date_of_death"),
        s.updated_at.alias("source_updated_at"), s.PERSON_LINK_STATUS.alias("person_link_status"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
         .alias("record_status"),
        s.date_unit_adm_CLEAN.alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("critical_care_admission").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.updated_at.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("medicus").alias("_source_system"),
        F.lit(SRC_CC_ADMISSION).alias("_source_table"),
        s.ADMISSION_KEY.alias("_source_row_id"),
    )


SRC_CC_ADMISSION = "4_prod.bronze.map_critical_care_admission"




def _cc_daily_score_canonical_pregate():
    # Normalize cc daily score source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_CC_DAILY_SCORE)
    event_id = stable_id(
        "critical_care_daily_score:medicus",
        s.SOURCE_UNIT, s.SOURCE_SITE, s.SOURCE_DAILY_ID, s.SCORE_TYPE,
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)],
        SRC_CC_DAILY_SCORE,
        F.concat_ws("|", s.SOURCE_UNIT, s.SOURCE_SITE, s.SOURCE_DAILY_ID, s.SCORE_TYPE),
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    event_time = F.coalesce(s.DATE_DAILY_CLEAN, s.DATE_SCORE_CALC_CLEAN)
    # contract v2: publish the four-part Medicus native score key and native identifier types while retaining SHA relationship keys
    return s.select(
        event_id.alias("patient_event_key"),
        s.SOURCE_UNIT.cast("string").alias("source_unit"),
        s.SOURCE_SITE.cast("string").alias("source_site"),
        s.SOURCE_DAILY_ID.cast("bigint").alias("source_daily_id"),
        s.SCORE_TYPE.cast("string").alias("score_type"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"), event_time.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:medicus:critical-care-score").alias("source_coding_system"),
        s.SCORE_TYPE.alias("source_code"), s.SCORE_TYPE.alias("source_display"),
        s.SCORE_VALUE.alias("score_value"), s.SCORE_VALUE_RAW.alias("score_value_raw"),
        s.DATE_DAILY_CLEAN.alias("score_date"),
        s.DATE_SCORE_CALC_CLEAN.alias("score_calc_date"),
        s.ADMISSION_KEY.alias("admission_key"),
        stable_id("critical_care_admission:medicus", s.ADMISSION_KEY)
         .alias("critical_care_admission_key"),
        s.DAY_LATEST_IND.alias("day_latest_ind"), s.ROW_CLASS.alias("row_class"),
        s.PERSON_LINK_STATUS.alias("person_link_status"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
         .alias("record_status"),
        event_time.alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("clinical").alias("fact_category"),
        F.lit("critical_care_daily_score").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.UPDATED_AT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("medicus").alias("_source_system"),
        F.lit(SRC_CC_DAILY_SCORE).alias("_source_table"),
        F.concat_ws("|", s.SOURCE_UNIT, s.SOURCE_SITE, s.SOURCE_DAILY_ID, s.SCORE_TYPE)
         .alias("_source_row_id"),
    )

def _cc_daily_score_canonical():
    # Keep the normalized cc daily score rows that pass the existing source-code admission rule.
    return _cc_daily_score_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_CC_DAILY_SCORE = "4_prod.bronze.map_critical_care_daily_score"




SRC_NEO_EPISODE = "4_prod.bronze.map_neonatal_episode"

def _neonatal_episode_canonical():
    # Assemble normalized neonatal episode rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_NEO_EPISODE)
    event_id = stable_id("neonatal_episode:badgernet", s.EntityID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.BABY_PERSON_ID)], SRC_NEO_EPISODE, s.EntityID
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    event_time = F.coalesce(s.AdmitTime_CLEAN, s.BirthTimeBaby_CLEAN)
    # contract v2: publish native person/encounter types while retaining the BadgerNet entity id and SHA event key
    return s.select(
        event_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.BABY_PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.BABY_PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"), event_time.alias("event_datetime"),
        s.DischTime_CLEAN.alias("event_end_datetime"),
        F.lit(None).cast("string").alias("source_coding_system"),
        F.lit(None).cast("string").alias("source_code"),
        F.lit(None).cast("string").alias("source_display"),
        s.EntityID.alias("entity_id"), s.BadgerUniqueID.alias("badger_unique_id"),
        s.MOTHER_PERSON_ID.cast("string").alias("mother_person_id"),
        s.CareLocationID.alias("care_location_id"),
        s.CareLocationName.alias("care_location_name"),
        s.BirthTimeBaby_CLEAN.alias("birth_datetime"),
        s.BirthTimeBaby.alias("birth_datetime_raw"), s.AdmitTime_CLEAN.alias("admit_datetime"),
        s.DischTime_CLEAN.alias("discharge_datetime"),
        s.GestationWeeks.alias("gestation_weeks"), s.GestationDays.alias("gestation_days"),
        s.Birthweight.alias("birthweight"), s.Sex.alias("sex"),
        s.FinalNNUOutcome.alias("final_nnu_outcome"), s.UnitLevel.alias("unit_level"),
        s.PERSON_LINK_STATUS.alias("person_link_status"),
        s.RecordTimestamp.alias("record_timestamp"), s.LastUpdate.alias("last_update"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
         .alias("record_status"),
        event_time.alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("neonatal_episode").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.LastUpdate.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("badgernet").alias("_source_system"),
        F.lit(SRC_NEO_EPISODE).alias("_source_table"), s.EntityID.alias("_source_row_id"),
    )




def _neonatal_care_day_canonical_pregate():
    # Normalize neonatal care day source rows before the downstream admission filter so excluded
    # evidence remains countable.
    s = read_source(SRC_NEO_CRITICAL_CARE)
    event_id = stable_id("neonatal_care_day:badgernet", s.EntityID, s.ActivityDate)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.BABY_PERSON_ID)],
        SRC_NEO_CRITICAL_CARE,
        F.concat_ws("|", s.EntityID, s.ActivityDate),
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    activity_array = ",".join(f"CCAC{i}" for i in range(1, 21))
    drug_array = ",".join(f"HCDRUG{i}" for i in range(1, 21))
    # contract v2: publish native person/encounter types and rename the hashed neonatal parent reference as a key
    return s.select(
        event_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.BABY_PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.BABY_PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.ActivityDate.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:nhs:nccmds:activity").alias("source_coding_system"),
        s.CCAC1.alias("source_code"), s.CCAC1.alias("source_display"),
        s.EntityID.alias("entity_id"), s.ActivityDate.alias("activity_date"),
        s.WardLocation.alias("ward_location"),
        s.CriticalCareUnitFunction.alias("unit_function"),
        s.CriticalCareStartDate_CLEAN.alias("critical_care_start"),
        s.CriticalCareDischargeDate_CLEAN.alias("critical_care_discharge"),
        F.expr(f"to_json(filter(array({activity_array}), x -> x is not null))")
         .alias("activity_codes_json"),
        F.expr(f"to_json(filter(array({drug_array}), x -> x is not null))")
         .alias("high_cost_drugs_json"),
        s.EPISODE_LINK_STATUS.alias("episode_link_status"),
        stable_id("neonatal_episode:badgernet", s.EntityID).alias("neonatal_episode_key"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
         .alias("record_status"),
        s.ActivityDate.alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("clinical").alias("fact_category"),
        F.lit("neonatal_critical_care").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("badgernet").alias("_source_system"),
        F.lit(SRC_NEO_CRITICAL_CARE).alias("_source_table"),
        F.concat_ws("|", s.EntityID, s.ActivityDate).alias("_source_row_id"),
    )

def _neonatal_care_day_canonical():
    # Keep the normalized neonatal care day rows that pass the existing source-code admission
    # rule.
    return _neonatal_care_day_canonical_pregate().where(_usable_code(F.col("source_code")))


SRC_NEO_CRITICAL_CARE = "4_prod.bronze.map_neonatal_critical_care"




def _neonatal_examination_canonical_pregate():
    # Normalize neonatal examination source rows before the downstream admission filter so
    # excluded evidence remains countable.
    s = read_source(SRC_NEO_EXAMINATION)
    event_id = stable_id("neonatal_examination:badgernet", s.EntityID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.BABY_PERSON_ID)], SRC_NEO_EXAMINATION, s.EntityID
    )
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    source_code = F.when(F.coalesce(s.EXAM_POPULATED_IND, F.lit(False)), F.lit("NIPE"))
    # contract v2: publish native person/encounter types and rename the hashed neonatal parent reference as a key
    return s.select(
        event_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.BABY_PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.BABY_PERSON_ID.isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        s.DateOfExamination_CLEAN.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:badgernet:examination").alias("source_coding_system"),
        source_code.alias("source_code"), F.lit("Newborn and Infant Physical Examination")
         .alias("source_display"),
        s.EntityID.alias("entity_id"),
        s.DateOfExamination_CLEAN.alias("examination_datetime"),
        s.EXAM_DATE_DERIVED.alias("exam_date_derived"),
        s.IncludeInDischargeLetter.alias("include_in_discharge_letter"),
        s.HeadCircumference.alias("head_circumference"), s.Spine.alias("spine_finding"),
        s.Heart.alias("heart_finding"), s.Genitalia.alias("genitalia_finding"),
        s.Hips.alias("hips_finding"), s.HipsRight.alias("right_hip_finding"),
        s.Eyes.alias("eyes_finding"), s.SpineComments.alias("spine_comments"),
        s.HeartComments.alias("heart_comments"),
        s.GenitaliaComments.alias("genitalia_comments"),
        F.coalesce(s.HipsOverallComments, s.HipsComments).alias("hips_comments"),
        s.EyesComments.alias("eyes_comments"),
        stable_id("neonatal_episode:badgernet", s.EntityID).alias("neonatal_episode_key"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active"))
         .alias("record_status"),
        s.DateOfExamination_CLEAN.alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("clinical").alias("fact_category"),
        F.lit("neonatal_examination").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("badgernet").alias("_source_system"),
        F.lit(SRC_NEO_EXAMINATION).alias("_source_table"), s.EntityID.alias("_source_row_id"),
    )

def _neonatal_examination_canonical():
    # Keep the normalized neonatal examination rows that pass the existing source-code admission
    # rule.
    return _neonatal_examination_canonical_pregate().where(
        _usable_code(F.col("source_code"))
    )


SRC_NEO_EXAMINATION = "4_prod.bronze.map_neonatal_examination"




def _mat_pregnancy_dedup():
    """One deterministic full row per Pregnancy_ID; no flow window function."""
    p = read_source(SRC_MAT_PREGNANCY)
    ranked = p.select(
        "*",
        F.struct(
            (~F.coalesce(p.SOURCE_DELETED_IND, F.lit(False))).cast("int").alias("_live"),
            F.coalesce(p.ADC_UPDT, F.lit("1900-01-01").cast("timestamp")).alias("_upd"),
            F.coalesce(p.Person_ID, F.lit(-1)).alias("_pid"),
            F.coalesce(p.ROW_HASH, F.lit(-1)).alias("_rh"),
        ).alias("_pick"),
    )
    best = ranked.groupBy("Pregnancy_ID").agg(F.max("_pick").alias("_best"))
    return (
        ranked.join(best, ["Pregnancy_ID"])
        .where(F.col("_pick") == F.col("_best"))
        .drop("_pick", "_best")
    )

def _maternity_join(slot):
    # Left-join a deduplicated maternity feed to the pregnancy spine without discarding
    # unmatched source rows.
    s = _maternity_source_dedup(slot).alias("s")
    p = _mat_pregnancy_spine().alias("p")
    return s.join(p, s.PREGNANCY_ID_PARSED == p.Pregnancy_ID, "left")

def _maternity_identity_status(embedded, spine):
    # Label person identity as resolved from the source, recovered from the spine, or
    # unresolved.
    return (
        F.when(embedded.isNotNull(), F.lit("resolved"))
        .when(spine.isNotNull(), F.lit("recovered"))
        .otherwise(F.lit("unresolved"))
    )

def _baby_delivery_canonical():
    # Assemble normalized baby delivery rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    d = _maternity_join(SRC_MATERNITY_BABY)
    person = F.coalesce(F.col("_spine_person_id"), F.col("s.PERSON_ID"))
    event_id = stable_id("baby_delivery:msds", F.col("s.ROW_HASH"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", person)], SRC_MATERNITY_BABY, F.col("s.ROW_HASH")
    )
    mismatch = (
        F.col("s._source_person_conflict_ind")
        | (
            F.col("s.PERSON_ID").isNotNull()
            & F.col("_spine_person_id").isNotNull()
            & (F.col("s.PERSON_ID") != F.col("_spine_person_id"))
        )
    )
    # contract v2: publish the MSDS row-hash exception and native delivery id while retaining SHA relationship keys
    return d.select(
        event_id.alias("patient_event_key"),
        F.col("s.ROW_HASH").cast("string").alias("source_row_hash"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        person.cast("bigint").alias("person_id"),
        _maternity_identity_status(F.col("s.PERSON_ID"), F.col("_spine_person_id"))
         .alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        F.col("s.PERSONBIRTHDATETIMEBABY_CLEAN").alias("event_datetime"),
        F.col("s.DISCHARGEDATETIMEBABYHSP_CLEAN").alias("event_end_datetime"),
        F.lit("urn:nhs:msds:delivery-method").alias("source_coding_system"),
        F.col("s.DELIVERYMETHODCODE").alias("source_code"),
        F.col("s.DELIVERYMETHODCODE").alias("source_display"),
        F.col("s.BABY_PERSON_ID").cast("string").alias("baby_person_id"),
        F.col("s.BIRTHORDERMATERNITYSUS").alias("birth_order"),
        F.col("s.PREGOUTCOME").alias("pregnancy_outcome"),
        F.col("s.DELIVERYMETHODCODE").alias("delivery_method_code"),
        F.col("s.PERSONPHENSEX").alias("phenotypic_sex"),
        F.col("s.GESTATIONLENGTHBIRTH").alias("gestation_length_birth"),
        F.col("s.BIRTHWEIGHT").alias("birthweight"), F.col("s.APGARSCORE5").alias("apgar_5"),
        F.col("s.PERSONDEATHDATETIMEBABY_CLEAN").alias("baby_death_datetime"),
        F.col("s.BABYFIRSTFEEDDATETIME_CLEAN").alias("first_feed_datetime"),
        F.col("s.BABYFIRSTFEEDINDCODE").alias("first_feed_code"),
        F.col("s.BABYFIRSTFEEDBREASTMILKSTATUS").alias("first_feed_breast_milk_status"),
        F.col("s.BABYBREASTMILKSTATUSDISCHARGE").alias("breast_milk_status_discharge"),
        F.col("s.SKINTOSKINCONTACT1HOURIND").alias("skin_to_skin_ind"),
        F.col("s.DISCHARGEDATETIMEBABYHSP_CLEAN").alias("baby_discharge_datetime"),
        F.col("s.ORGSITEIDACTUALDELIVERY").alias("delivery_org_site"),
        F.col("s.SETTINGPLACEBIRTH").alias("birth_setting"),
        F.col("s.PLACETYPEACTUALDELIVERY").alias("birth_place_type"),
        F.col("s.PLACETYPEACTUALMIDWIFERY").alias("midwifery_place_type"),
        F.col("s.LABOURDELIVERYID").alias("labour_delivery_id"),
        stable_id("labour_delivery:msds", F.col("s.LABOURDELIVERYID"))
         .alias("labour_delivery_key"),
        F.col("s.PREGNANCY_ID_PARSED").cast("string").alias("pregnancy_id"),
        stable_id("journey:pregnancy", F.col("s.PREGNANCY_ID_PARSED"))
         .alias("journey_pregnancy_key"),
        F.col("s.PERSON_LINK_STATUS").alias("source_link_status"),
        F.col("p.Pregnancy_ID").isNull().alias("pregnancy_orphan_ind"),
        mismatch.alias("spine_person_mismatch_ind"), F.lit("active").alias("record_status"),
        F.col("s.PERSONBIRTHDATETIMEBABY_CLEAN").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("clinical").alias("fact_category"),
        F.lit("maternity_baby_delivery").alias("source_feed"),
        F.date_format(F.col("s.ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("s.RECORD_UPDATED_DT").alias("source_update_timestamp"),
        F.col("s.ADC_UPDT").alias("loaded_at"), F.lit("msds").alias("_source_system"),
        F.lit(SRC_MATERNITY_BABY).alias("_source_table"),
        F.col("s.ROW_HASH").cast("string").alias("_source_row_id"),
    )




SRC_MATERNITY_BABY = "4_prod.bronze.map_maternity_baby_delivery"

def _mat_pregnancy_spine():
    # Select pregnancy-to-person links and deletion evidence from the deduplicated pregnancy
    # source.
    d = _mat_pregnancy_dedup()
    return d.select(
        "Pregnancy_ID",
        F.col("Person_ID").alias("_spine_person_id"),
        F.col("SOURCE_DELETED_IND").alias("_spine_deleted_ind"),
    )

def _maternity_source_dedup(slot):
    """Collapse source-row identity fan-out without choosing an arbitrary person.

    The MSDS adapters can publish the same ROW_HASH more than once when one clinical row
    is projected onto conflicting person matches. All non-PERSON_ID columns are identical
    for that source identity. Keep one deterministic clinical row, retain PERSON_ID only
    when the evidence is single-valued, and expose the conflict to the maternity facts.
    """
    s = read_source(slot)
    non_person_cols = [c for c in s.columns if c != "PERSON_ID"]
    grouped = s.groupBy(*non_person_cols).agg(
        F.countDistinct("PERSON_ID").alias("_person_id_count"),
        F.max("PERSON_ID").alias("_single_person_id"),
    )
    conflict = F.col("_person_id_count") > 1
    return (
        grouped
        .withColumn(
            "PERSON_LINK_STATUS",
            F.when(conflict, F.lit("CONFLICTING")).otherwise(F.col("PERSON_LINK_STATUS")),
        )
        .withColumn(
            "PERSON_ID",
            F.when(F.col("_person_id_count") == 1, F.col("_single_person_id")),
        )
        .withColumn("_source_person_conflict_ind", conflict)
        .drop("_person_id_count", "_single_person_id")
    )




def _labour_delivery_canonical():
    # Assemble normalized labour delivery rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    d = _maternity_join(SRC_MATERNITY_LABOUR)
    person = F.coalesce(F.col("_spine_person_id"), F.col("s.PERSON_ID"))
    event_id = stable_id("labour_delivery:msds", F.col("s.LABOURDELIVERYID"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", person)], SRC_MATERNITY_LABOUR, F.col("s.LABOURDELIVERYID")
    )
    event_time = _clamped_ts(F.coalesce(
        F.col("s.LABOURONSETDATETIME_CLEAN"), F.col("s.CAESAREANDATETIME_CLEAN"),
        F.col("s.STARTDATETIMEMOTHERDELIVERYHPS_CLEAN"),
    ))
    mismatch = (
        F.col("s._source_person_conflict_ind")
        | (
            F.col("s.PERSON_ID").isNotNull() & F.col("_spine_person_id").isNotNull()
            & (F.col("s.PERSON_ID") != F.col("_spine_person_id"))
        )
    )
    # contract v2: publish native person/encounter types and retain the native labour id while renaming the pregnancy hash as a key
    return d.select(
        event_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        person.cast("bigint").alias("person_id"),
        _maternity_identity_status(F.col("s.PERSON_ID"), F.col("_spine_person_id"))
         .alias("identity_status"),
        F.lit(None).cast("bigint").alias("encounter_id"), event_time.alias("event_datetime"),
        F.col("s.DISCHARGEDATETIMEMOTHERHSP_CLEAN").alias("event_end_datetime"),
        F.lit("urn:nhs:msds:labour-onset-method").alias("source_coding_system"),
        F.col("s.LABOURONSETMETHOD").alias("source_code"),
        F.col("s.LABOURONSETMETHOD").alias("source_display"),
        F.col("s.LABOURDELIVERYID").alias("labour_delivery_id"),
        F.col("s.LABOURONSETMETHOD").alias("labour_onset_method"),
        F.col("s.LABOURONSETPRESENTATION").alias("labour_onset_presentation"),
        F.col("s.CAESAREANDATETIME_CLEAN").alias("caesarean_datetime"),
        F.col("s.DECISIONTODELIVERDATETIME_CLEAN").alias("decision_to_deliver_datetime"),
        F.col("s.ROMDATETIME_CLEAN").alias("rom_datetime"),
        F.col("s.ROMMETHOD").alias("rom_method"), F.col("s.ROMREASON").alias("rom_reason"),
        F.col("s.LABOURONSETSECONDSTAGEDATETIME_CLEAN").alias("second_stage_datetime"),
        F.col("s.LABOURTHIRDSTAGEENDDATETIME_CLEAN").alias("third_stage_end_datetime"),
        F.col("s.EPISIOTOMYREASON").alias("episiotomy_reason"),
        F.col("s.PLACENTADELIVERYMETHOD").alias("placenta_delivery_method"),
        F.col("s.ADMMETHCODEMOTHDELHSP").alias("mother_admission_method"),
        F.col("s.DISCHARGEDATETIMEMOTHERHSP_CLEAN").alias("mother_discharge_datetime"),
        F.col("s.DISCHMETHCODEMOTHPOSTDELHSP").alias("mother_discharge_method"),
        F.col("s.DISCHDESTCODEMOTHPOSTDELHSP").alias("mother_discharge_destination"),
        F.col("s.ORGSITEIDINTRA").alias("intrapartum_org_site"),
        F.col("s.SETTINGINTRACARE").alias("intrapartum_setting"),
        F.col("s.ORGIDPOSTNATALPATHLEADPROVIDER").alias("postnatal_lead_provider"),
        F.col("s.PREGNANCY_ID_PARSED").cast("string").alias("pregnancy_id"),
        stable_id("journey:pregnancy", F.col("s.PREGNANCY_ID_PARSED"))
         .alias("journey_pregnancy_key"),
        F.col("s.PERSON_LINK_STATUS").alias("source_link_status"),
        F.col("p.Pregnancy_ID").isNull().alias("pregnancy_orphan_ind"),
        mismatch.alias("spine_person_mismatch_ind"), F.lit("active").alias("record_status"),
        event_time.alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("clinical").alias("fact_category"),
        F.lit("maternity_labour_delivery").alias("source_feed"),
        F.date_format(F.col("s.ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("s.RECORD_UPDATED_DT").alias("source_update_timestamp"),
        F.col("s.ADC_UPDT").alias("loaded_at"), F.lit("msds").alias("_source_system"),
        F.lit(SRC_MATERNITY_LABOUR).alias("_source_table"),
        F.col("s.LABOURDELIVERYID").alias("_source_row_id"),
    )


SRC_MATERNITY_LABOUR = "4_prod.bronze.map_maternity_labour_delivery"




def _maternity_care_contact_canonical():
    # Assemble normalized maternity care contact rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    d = _maternity_join(SRC_MATERNITY_CONTACT)
    person = F.coalesce(F.col("_spine_person_id"), F.col("s.PERSON_ID"))
    event_id = stable_id(
        "maternity_care_contact:msds", F.col("s.CARECONID"), F.col("s.PREGNANCYID")
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", person)], SRC_MATERNITY_CONTACT,
        F.concat_ws("|", F.col("s.CARECONID"), F.col("s.PREGNANCYID")),
    )
    mismatch = (
        F.col("s._source_person_conflict_ind")
        | (
            F.col("s.PERSON_ID").isNotNull() & F.col("_spine_person_id").isNotNull()
            & (F.col("s.PERSON_ID") != F.col("_spine_person_id"))
        )
    )
    # contract v2: publish native person, encounter, and organization ids while renaming the pregnancy hash as a key
    return d.select(
        event_id.alias("patient_event_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"), person.cast("bigint").alias("person_id"),
        _maternity_identity_status(F.col("s.PERSON_ID"), F.col("_spine_person_id"))
         .alias("identity_status"), F.lit(None).cast("bigint").alias("encounter_id"),
        F.col("s.CCONTACTDATETIME_CLEAN").alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit(None).cast("string").alias("source_coding_system"),
        F.lit(None).cast("string").alias("source_code"),
        F.lit(None).cast("string").alias("source_display"),
        F.col("s.CARECONID").alias("care_contact_id"), F.col("s.ATTENDCODE").alias("attend_code"),
        F.col("s.CONSULTTYPE").alias("consult_type"),
        F.col("s.CCSUBJECT").alias("contact_subject"), F.col("s.MEDIUM").alias("medium"),
        F.col("s.CONTACTDURATION").alias("duration"),
        F.col("s.ADMINCATCODE").alias("admin_category"),
        F.col("s.GPTHERAPYIND").alias("gp_therapy_ind"),
        F.col("s.CANCELDATE_CLEAN").alias("cancel_datetime"),
        F.col("s.CANCELREASON").alias("cancel_reason"),
        F.col("s.REPLAPPTOFFDATE_CLEAN").alias("replacement_offer_datetime"),
        F.col("s.REPLAPPTDATE_CLEAN").alias("replacement_appointment_datetime"),
        F.col("s.ORGIDCOMM").cast("bigint").alias("organization_id"),
        F.col("s.ORGSITEIDOFTREAT").alias("site_id"), F.col("s.LOCCODE").alias("location_code"),
        F.col("s.PREGNANCY_ID_PARSED").cast("string").alias("pregnancy_id"),
        stable_id("journey:pregnancy", F.col("s.PREGNANCY_ID_PARSED")).alias("journey_pregnancy_key"),
        F.col("s.PERSON_LINK_STATUS").alias("source_link_status"),
        F.col("p.Pregnancy_ID").isNull().alias("pregnancy_orphan_ind"),
        mismatch.alias("spine_person_mismatch_ind"), F.lit("active").alias("record_status"),
        F.col("s.CCONTACTDATETIME_CLEAN").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("maternity_care_contact").alias("source_feed"),
        F.date_format(F.col("s.ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("s.RECORD_UPDATED_DT").alias("source_update_timestamp"),
        F.col("s.ADC_UPDT").alias("loaded_at"), F.lit("msds").alias("_source_system"),
        F.lit(SRC_MATERNITY_CONTACT).alias("_source_table"),
        F.concat_ws("|", F.col("s.CARECONID"), F.col("s.PREGNANCYID"))
         .alias("_source_row_id"),
    )


SRC_MATERNITY_CONTACT = "4_prod.bronze.map_maternity_care_contact"




# S3 contract finalisation.  Lists are mutated in place because the decorated
# build functions resolve these globals when Lakeflow evaluates the graph.
def _s3_replace_public_variant(columns, old_name, axes):
    # Replace a legacy coding payload in the public column list with scalar terminology-axis
    # columns, without duplicates.
    position = columns.index(old_name)
    existing = set(columns[:position] + columns[position + 1:])
    replacement = []
    for axis, source_only, _subject in axes:
        replacement.extend(column for column in axis_columns(axis, source_only) if column not in existing)
    columns[position:position + 1] = replacement

# PACS_INTEGRATION_SILVER_SHARED_V1


# ==== PACS integration (2026-09-24): accession membership and imaging report links ====
# Contract: pacs_integration_20260924. Bronze owner stages the sources (map_pacs_examination
# v3.2, map_radiology_event v2, map_pacs_exam_request_report_link, bridge v4); Journey owns
# these projections. The existing imaging pregates are unchanged unless integration=True, so
# the patient-event index flows keep byte-identical plans and are not recomputed.
PACS_INTEGRATION_CONTRACT = "2026-09-24.pacs_integration.v1"
SRC_PACS_EXAM_REPORT_LINK = "4_prod.bronze.map_pacs_exam_request_report_link"
MILL_RADIOLOGY_EXAM_CLASS_CD = 234
MILL_RADIOLOGY_DOCUMENT_CLASS_CD = 224
# Millennium RESULT_STATUS_CD values that are not performed evidence: Canceled, the four
# In Error meanings and Not Done (the estate's clinical_event validity rule).
MILL_NOT_PERFORMED_STATUS_CDS = (26, 28, 29, 30, 31, 36)
# Accession formats that are not a validated Cerner parse (Cerner-only candidates only).
UNVALIDATED_ACCESSION_FORMATS = ("UNRECOGNISED", "OTHER")
# Request-text anonymisation feed (staged by the anonymisation owner, see
# deploy/anon_registration_spec.md). Order matters: the lane hashes these columns as one struct.
PACS_REQUEST_ANON_OUTPUTS = {
    "CLINICAL_QUESTION": "anon_clinical_question",
    "CLINICAL_ANAMNESIS": "anon_clinical_anamnesis",
    "CLINICAL_QUESTION_EXAM_SEGMENT": "anon_clinical_question_exam_segment",
    "CLINICAL_ANAMNESIS_EXAM_SEGMENT": "anon_clinical_anamnesis_exam_segment",
}

IMAGING_INTEGRATION_COLUMNS = [
    "sectra_accession_number",
    "accession_format",
    "accession_parse_method",
    "accession_rule_version",
    "accession_evidence_level",
    "accession_identity_status",
    "accession_candidate_count",
    "linked_pacs_examination_id",
    "pacs_link_method",
    "source_event_class_code",
    "source_status_code",
    "performed_datetime",
    "performed_datetime_source",
    "performed_evidence",
    "performed_evidence_verified_ind",
    "report_available_ind",
    "report_link_status",
    "report_count",
    "report_reference_count",
    "linked_report_event_id",
    "native_image_count",
    "native_series_count",
    "measured_series_count",
    "request_clinical_question",
    "request_clinical_history",
    "request_clinical_question_exam_segment",
    "request_clinical_question_segment_status",
    "request_clinical_history_exam_segment",
    "request_clinical_history_segment_status",
    "request_clinical_question_anonymised",
    "request_clinical_history_anonymised",
    "request_clinical_question_exam_segment_anonymised",
    "request_clinical_history_exam_segment_anonymised",
    "request_text_anonymisation_status",
]

IMAGING_INTEGRATION_COLUMN_COMMENTS = {
    "sectra_accession_number": "Barts/Sectra extraction accession. PACS: trimmed REQUEST_ID_STRING (map_pacs_examination.SECTRA_ACCESSION_NBR). Millennium: REFERENCE_NBR parsed by the shared bronze accession rules. Scopes an extraction request: one accession can hold several examinations and study UIDs. Leading zeros preserved. Distinct from accession_identifier, which keeps its existing meaning. Identifier.",
    "accession_format": "Bronze accession-rule format class (SECTRA_16, SITE_16, RNH_14, OTHER_SITE_14, NUMERIC_16, LEGACY_7_8, LEGACY_6, NUMERIC_OTHER, OTHER or UNRECOGNISED).",
    "accession_parse_method": "PACS: ACCESSION_PARSE_METHOD. Millennium: REF_PARSE_METHOD. Provenance of sectra_accession_number.",
    "accession_rule_version": "Version of the shared bronze accession rules that produced sectra_accession_number.",
    "accession_evidence_level": "pacs_request (PACS request accession), cerner_reference_pacs_linked (Cerner reference resolved to one PACS examination), cerner_reference_parsed (Cerner-only parsed reference), or NULL when no accession.",
    "accession_identity_status": "Row-level accession identity evidence. PACS: ACCESSION_IDENTITY_STATUS over the accession's PACS examinations. Millennium: CONFLICTING_PERSONS when the PACS link was refused for identity conflict, otherwise NULL. The cross-arm accession status is on clinical_imaging_accession_member.",
    "accession_candidate_count": "PACS: published examinations sharing the accession. Millennium: present PACS examinations sharing the parsed accession before code and identity filtering.",
    "linked_pacs_examination_id": "PACS examination this row resolves to. PACS rows: their own PACS_EXAMINATION_ID. Millennium rows: map_radiology_event.PACS_EXAMINATION_ID, set only for a unique accession+code or unique accession-only match.",
    "pacs_link_method": "NATIVE_PACS_EXAMINATION for PACS rows; the bronze PACS_LINK_METHOD for Millennium rows. Ambiguous and conflicting candidates are never resolved by choice.",
    "source_event_class_code": "Millennium EVENT_CLASS_CD (234 examination, 224 report document, 231 section); NULL for PACS rows.",
    "source_status_code": "Native status. PACS: EXAMINATION_STATUS_CD. Millennium: RESULT_STATUS_CD.",
    "performed_datetime": "Valid performed time. PACS: PERFORMED_DT_TM_CLEAN (examination time, else the documented arrival fallback; future and 2099 sentinel values are NULL). Millennium: PERFORMED_DT_TM_CLEAN. Never a scheduled, request, report or load time. event_datetime keeps its existing meaning.",
    "performed_datetime_source": "Which clean clock performed_datetime came from (EXAMINATION_DT_TM, ARRIVAL_DT_TM or PERFORMED_DT_TM).",
    "performed_evidence": "PACS: bronze PERFORMED_EVIDENCE set (STATUS, REPORT, IMAGES, SERIES). Millennium: RESULT_STATUS for an examination event whose status is not cancelled, in error or not done.",
    "performed_evidence_verified_ind": "True only for verified performed evidence. PACS status 40 (booking) and dangling-only report links are not evidence. Millennium: examination event with a performed result status.",
    "report_available_ind": "PACS: REPORT_AVAILABLE_IND (at least one resolved report; a dangling report id never sets it). Millennium: a report document event is linked through REF_EXAM_KEY.",
    "report_link_status": "PACS: RESOLVED, DANGLING_ONLY or NONE. Millennium: REPORT_LINK_STATUS (SELF, SIBLING, SIBLING_LATEST_OF_MULTIPLE, AMBIGUOUS or NONE). All links are in clinical_imaging_report_link.",
    "report_count": "PACS: REPORT_COUNT_RESOLVED. Millennium: identity-compatible report document events sharing REF_EXAM_KEY.",
    "report_reference_count": "PACS: REPORT_REF_COUNT, including dangling report ids. NULL for Millennium rows.",
    "linked_report_event_id": "Millennium: RADIOLOGY_REPORT_EVENT_ID, the latest identity-compatible report document event. A convenience pointer; other reports stay relational. NULL for PACS rows.",
    "native_image_count": "Native Sectra IMAGE_COUNT (sparsely populated; PACS feed clock). Not evidence of present retrievability.",
    "native_series_count": "Native Sectra SERIES_COUNT (sparsely populated).",
    "measured_series_count": "SERIES_COUNT_MEASURED from the raw series table. The series feed has a separate clock and ends 2024-11-02; NULL after that.",
    "request_clinical_question": "PACS request clinical question, verbatim and request-wide. Identifiable free text (ig_risk 4); never copied into accession summaries or anonymised serving projections.",
    "request_clinical_history": "PACS request clinical information (CLINICAL_ANAMNESIS), verbatim and request-wide. Identifiable free text (ig_risk 4).",
    "request_clinical_question_exam_segment": "The clinical-question section headed by this examination's code; set only when its segment status is EXAM_SEGMENT. Identifiable free text (ig_risk 4).",
    "request_clinical_question_segment_status": "EXAM_SEGMENT, NOT_SEGMENTED (text is request-wide), CODE_NOT_IN_SEGMENTS, DUPLICATE_SEGMENT_CODE, DUPLICATE_EXAM_CODE_ON_REQUEST, NO_EXAM_CODE or NO_TEXT.",
    "request_clinical_history_exam_segment": "The clinical-history section headed by this examination's code; set only when its segment status is EXAM_SEGMENT. Identifiable free text (ig_risk 4).",
    "request_clinical_history_segment_status": "Same vocabulary as request_clinical_question_segment_status.",
    "request_clinical_question_anonymised": "Approved-lane anonymised clinical question. NULL unless the registered request-text feed reports anonymized for the current source text hash. Never falls back to raw text or a report's anonymised text.",
    "request_clinical_history_anonymised": "Approved-lane anonymised clinical history; same rule as request_clinical_question_anonymised.",
    "request_clinical_question_exam_segment_anonymised": "Approved-lane anonymised examination-specific question segment; same rule.",
    "request_clinical_history_exam_segment_anonymised": "Approved-lane anonymised examination-specific history segment; same rule.",
    "request_text_anonymisation_status": "not_registered (feed not yet deployed), current (anonymized for the current text), stale (text changed since redaction), pending, failed, unresolved_person, or no_text. Millennium rows are NULL.",
}

# IG tags for the added imaging columns (applied by deploy/metadata_replay after each build).
IMAGING_INTEGRATION_IG = {c: ("0", "0") for c in IMAGING_INTEGRATION_COLUMNS}
IMAGING_INTEGRATION_IG.update({
    "sectra_accession_number": ("4", "2"),
    "performed_datetime": ("1", "1"),
    "linked_report_event_id": ("0", "1"),
    "request_clinical_question": ("4", "2"),
    "request_clinical_history": ("4", "2"),
    "request_clinical_question_exam_segment": ("4", "2"),
    "request_clinical_history_exam_segment": ("4", "2"),
    "request_clinical_question_anonymised": ("3", "2"),
    "request_clinical_history_anonymised": ("3", "2"),
    "request_clinical_question_exam_segment_anonymised": ("3", "2"),
    "request_clinical_history_exam_segment_anonymised": ("3", "2"),
})


def _anon_state_hash(df, text_columns):
    # Reproduce the approved lane's source-text hash: sha2 over to_json of the registered text
    # columns, each coalesced to an empty string, in registration order.
    return F.sha2(F.to_json(F.struct(*[
        F.coalesce(df[c].cast("string"), F.lit("")).alias(c) for c in text_columns
    ])), 256)


def _anon_output_or_null(df, output_column, text_columns, key_present=None):
    # Approved anonymised output only when the lane reports anonymized for the current text.
    # A missing registration or stale/failed state yields NULL, never raw text.
    needed = {output_column, "anon_status", "anon_source_text_sha"}
    if not needed.issubset(set(df.columns)):
        return F.lit(None).cast("string")
    current = (df["anon_status"] == F.lit("anonymized")) & (
        df["anon_source_text_sha"] == _anon_state_hash(df, text_columns))
    if key_present is not None:
        current = current & key_present
    return F.when(current, df[output_column])


def _pacs_request_anon_status(s):
    text_columns = list(PACS_REQUEST_ANON_OUTPUTS)
    has_text = None
    for c in text_columns:
        term = s[c].isNotNull() & (F.trim(s[c]) != "")
        has_text = term if has_text is None else has_text | term
    needed = set(PACS_REQUEST_ANON_OUTPUTS.values()) | {"anon_status", "anon_source_text_sha"}
    if not needed.issubset(set(s.columns)):
        return F.when(has_text, F.lit("not_registered")).otherwise(F.lit("no_text"))
    current_hash = _anon_state_hash(s, text_columns)
    return (
        F.when(~has_text, F.lit("no_text"))
        .when(s["anon_status"].isNull(), F.lit("pending"))
        .when(~s["anon_source_text_sha"].eqNullSafe(current_hash), F.lit("stale"))
        .when(s["anon_status"] == F.lit("anonymized"), F.lit("current"))
        .otherwise(s["anon_status"])
    )


def _pacs_exam_integration_columns(s):
    # PACS-arm integration fields, read from map_pacs_examination v3.2.
    text_columns = list(PACS_REQUEST_ANON_OUTPUTS)
    anon = {src: _anon_output_or_null(s, out, text_columns)
            for src, out in PACS_REQUEST_ANON_OUTPUTS.items()}
    return [
        s.SECTRA_ACCESSION_NBR.alias("sectra_accession_number"),
        s.ACCESSION_FORMAT.alias("accession_format"),
        s.ACCESSION_PARSE_METHOD.alias("accession_parse_method"),
        s.ACCESSION_RULE_VERSION.alias("accession_rule_version"),
        F.when(s.SECTRA_ACCESSION_NBR.isNotNull(), F.lit("pacs_request"))
        .alias("accession_evidence_level"),
        s.ACCESSION_IDENTITY_STATUS.alias("accession_identity_status"),
        s.ACCESSION_EXAM_COUNT.cast("int").alias("accession_candidate_count"),
        s.PACS_EXAMINATION_ID.cast("bigint").alias("linked_pacs_examination_id"),
        F.lit("NATIVE_PACS_EXAMINATION").alias("pacs_link_method"),
        F.lit(None).cast("string").alias("source_event_class_code"),
        s.EXAMINATION_STATUS_CD.cast("string").alias("source_status_code"),
        s.PERFORMED_DT_TM_CLEAN.alias("performed_datetime"),
        s.PERFORMED_DT_TM_SOURCE.alias("performed_datetime_source"),
        s.PERFORMED_EVIDENCE.alias("performed_evidence"),
        F.coalesce(s.PERFORMED_EVIDENCE_VERIFIED_IND, F.lit(False))
        .alias("performed_evidence_verified_ind"),
        F.coalesce(s.REPORT_AVAILABLE_IND, F.lit(False)).alias("report_available_ind"),
        F.when(F.coalesce(s.REPORT_AVAILABLE_IND, F.lit(False)), F.lit("RESOLVED"))
        .when(F.coalesce(s.REPORT_REF_COUNT, F.lit(0)) > 0, F.lit("DANGLING_ONLY"))
        .otherwise(F.lit("NONE")).alias("report_link_status"),
        s.REPORT_COUNT_RESOLVED.cast("int").alias("report_count"),
        s.REPORT_REF_COUNT.cast("int").alias("report_reference_count"),
        F.lit(None).cast("bigint").alias("linked_report_event_id"),
        s.IMAGE_COUNT.cast("bigint").alias("native_image_count"),
        s.SERIES_COUNT.cast("bigint").alias("native_series_count"),
        s.SERIES_COUNT_MEASURED.cast("bigint").alias("measured_series_count"),
        s.CLINICAL_QUESTION.alias("request_clinical_question"),
        s.CLINICAL_ANAMNESIS.alias("request_clinical_history"),
        s.CLINICAL_QUESTION_EXAM_SEGMENT.alias("request_clinical_question_exam_segment"),
        s.CLINICAL_QUESTION_SEGMENT_STATUS.alias("request_clinical_question_segment_status"),
        s.CLINICAL_ANAMNESIS_EXAM_SEGMENT.alias("request_clinical_history_exam_segment"),
        s.CLINICAL_ANAMNESIS_SEGMENT_STATUS.alias("request_clinical_history_segment_status"),
        anon["CLINICAL_QUESTION"].alias("request_clinical_question_anonymised"),
        anon["CLINICAL_ANAMNESIS"].alias("request_clinical_history_anonymised"),
        anon["CLINICAL_QUESTION_EXAM_SEGMENT"]
        .alias("request_clinical_question_exam_segment_anonymised"),
        anon["CLINICAL_ANAMNESIS_EXAM_SEGMENT"]
        .alias("request_clinical_history_exam_segment_anonymised"),
        _pacs_request_anon_status(s).alias("request_text_anonymisation_status"),
    ]


def _mill_radiology_integration_columns(s):
    # Millennium-arm integration fields, read from map_radiology_event v2.
    is_exam = s.EVENT_CLASS_CD.cast("bigint") == F.lit(MILL_RADIOLOGY_EXAM_CLASS_CD)
    performed_status = (
        s.RESULT_STATUS_CD.isNotNull()
        & ~s.RESULT_STATUS_CD.cast("bigint").isin(*MILL_NOT_PERFORMED_STATUS_CDS)
        & ~F.coalesce(s.IN_ERROR_IND, F.lit(False))
    )
    return [
        s.SECTRA_ACCESSION_NBR.alias("sectra_accession_number"),
        s.ACCESSION_FORMAT.alias("accession_format"),
        s.REF_PARSE_METHOD.alias("accession_parse_method"),
        s.ACCESSION_RULE_VERSION.alias("accession_rule_version"),
        F.when(s.SECTRA_ACCESSION_NBR.isNull(), F.lit(None).cast("string"))
        .when(s.PACS_EXAMINATION_ID.isNotNull(), F.lit("cerner_reference_pacs_linked"))
        .otherwise(F.lit("cerner_reference_parsed")).alias("accession_evidence_level"),
        F.when(s.PACS_LINK_METHOD == F.lit("IDENTITY_CONFLICT"), F.lit("CONFLICTING_PERSONS"))
        .alias("accession_identity_status"),
        s.PACS_LINK_CANDIDATE_COUNT.cast("int").alias("accession_candidate_count"),
        s.PACS_EXAMINATION_ID.cast("bigint").alias("linked_pacs_examination_id"),
        s.PACS_LINK_METHOD.alias("pacs_link_method"),
        s.EVENT_CLASS_CD.cast("bigint").cast("string").alias("source_event_class_code"),
        s.RESULT_STATUS_CD.cast("bigint").cast("string").alias("source_status_code"),
        s.PERFORMED_DT_TM_CLEAN.alias("performed_datetime"),
        F.when(s.PERFORMED_DT_TM_CLEAN.isNotNull(), F.lit("PERFORMED_DT_TM"))
        .alias("performed_datetime_source"),
        F.when(is_exam & performed_status, F.lit("RESULT_STATUS")).alias("performed_evidence"),
        F.coalesce(is_exam & performed_status, F.lit(False))
        .alias("performed_evidence_verified_ind"),
        s.RADIOLOGY_REPORT_EVENT_ID.isNotNull().alias("report_available_ind"),
        s.REPORT_LINK_STATUS.alias("report_link_status"),
        s.RADIOLOGY_REPORT_CANDIDATE_COUNT.cast("int").alias("report_count"),
        F.lit(None).cast("int").alias("report_reference_count"),
        s.RADIOLOGY_REPORT_EVENT_ID.cast("bigint").alias("linked_report_event_id"),
        F.lit(None).cast("bigint").alias("native_image_count"),
        F.lit(None).cast("bigint").alias("native_series_count"),
        F.lit(None).cast("bigint").alias("measured_series_count"),
        *[F.lit(None).cast("string").alias(c) for c in (
            "request_clinical_question", "request_clinical_history",
            "request_clinical_question_exam_segment", "request_clinical_question_segment_status",
            "request_clinical_history_exam_segment", "request_clinical_history_segment_status",
            "request_clinical_question_anonymised", "request_clinical_history_anonymised",
            "request_clinical_question_exam_segment_anonymised",
            "request_clinical_history_exam_segment_anonymised",
            "request_text_anonymisation_status",
        )],
    ]


# ---- clinical_imaging_accession_member ----
IMAGING_ACCESSION_MEMBER_COLUMNS = [
    "member_key",
    "accession_key",
    "sectra_accession_number",
    "patient_event_key",
    "source_object",
    "event_id",
    "pacs_examination_id",
    "linked_pacs_examination_id",
    "examination_key",
    "study_instance_uid",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "accession_format",
    "accession_parse_method",
    "accession_rule_version",
    "accession_evidence_level",
    "pacs_link_method",
    "accession_person_count",
    "accession_person_status",
    "performed_datetime",
    "performed_datetime_source",
    "performed_evidence",
    "performed_evidence_verified_ind",
    "eligibility_as_of",
    "default_selection_eligible_ind",
    "eligibility_blocking_reasons",
    "source_coding_system",
    "source_code",
    "source_display",
    "modality_code",
    "body_site_code",
    "status_code",
    "source_status_code",
    "record_status",
    "report_available_ind",
    "report_link_status",
    "report_patient_event_key",
    "linked_report_event_id",
    "native_image_count",
    "native_series_count",
    "measured_series_count",
    "source_feed",
    "source_update_timestamp",
    "loaded_at",
]

IMAGING_ACCESSION_MEMBER_COLUMN_COMMENTS = {
    "member_key": "Deterministic SHA-256 of the member (imaging_accession_member namespace + patient_event_key). One row per PACS examination or Millennium examination event that carries an accession.",
    "accession_key": "upper(trim(sectra_accession_number)): the grouping key shared by PACS and Millennium members of one accession. Identifier.",
    "sectra_accession_number": "Barts/Sectra extraction accession as published by the member's source. Identifier.",
    "patient_event_key": "patient_event_key of this member, minted exactly as clinical_imaging_exam mints it. Joins clinical_imaging_exam when the examination is admitted there; examinations without a usable code are retained here but not admitted there, so their exam_* concept columns are NULL.",
    "source_object": "pacs (Sectra examination) or millennium (Cerner examination event, EVENT_CLASS_CD 234).",
    "event_id": "Millennium EVENT_ID for millennium members.",
    "pacs_examination_id": "Native PACS_EXAMINATION_ID for pacs members.",
    "linked_pacs_examination_id": "PACS examination the member resolves to (own id for pacs; unique bronze link for millennium).",
    "examination_key": "Physical-examination identity used for distinct counts: pacs:<PACS examination id> when resolved, otherwise mill:<EVENT_ID>. A Millennium event linked to a PACS examination counts once with it.",
    "study_instance_uid": "DICOM StudyInstanceUID (image retrieval key; pacs members only). One accession can hold several.",
    "subject_key": "Deterministic subject key inherited from clinical_imaging_exam.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved.",
    "identity_status": "Member identity resolution: resolved, provisional or unresolved.",
    "encounter_id": "Millennium ENCNTR_ID when supplied (millennium members).",
    "accession_format": "Bronze accession-rule format class.",
    "accession_parse_method": "Provenance of sectra_accession_number.",
    "accession_rule_version": "Bronze accession-rule version.",
    "accession_evidence_level": "pacs_request, cerner_reference_pacs_linked or cerner_reference_parsed.",
    "pacs_link_method": "NATIVE_PACS_EXAMINATION or the bronze Millennium PACS link method.",
    "accession_person_count": "Distinct non-null person_id across all members of the accession (both arms).",
    "accession_person_status": "single_person, single_person_partial (some members unresolved), conflicting_persons (more than one person_id, or a refused identity-conflict link) or no_person. Conflicts are never resolved by choice.",
    "performed_datetime": "Valid performed time (future and sentinel values already NULL in bronze).",
    "performed_datetime_source": "Clean clock that supplied performed_datetime.",
    "performed_evidence": "Performed evidence set (see clinical_imaging_exam).",
    "performed_evidence_verified_ind": "Verified performed evidence; PACS status 40 and dangling report ids are not evidence.",
    "eligibility_as_of": "Selection as-of time: the member's own bronze load clock. A performed time after it is ineligible.",
    "default_selection_eligible_ind": "True when eligibility_blocking_reasons is NULL. Ineligible members stay queryable here.",
    "eligibility_blocking_reasons": "Comma list, NULL when eligible: source_not_current, no_valid_performed_time, performed_after_as_of, performed_evidence_unverified, accession_identity_conflict, unvalidated_cerner_accession. Missing codes never block.",
    "source_coding_system": "Source examination coding system.",
    "source_code": "Source examination code or display fallback; may be NULL (uncoded examinations are retained).",
    "source_display": "Source examination description.",
    "modality_code": "Observed modality: PACS MODALITY or Millennium NHSI_MODALITY_CATEGORY. Normalised concepts come from clinical_imaging_exam's mapped exam axis.",
    "body_site_code": "Observed body part (PACS BODY_PART).",
    "status_code": "Silver imaging status inherited from clinical_imaging_exam.",
    "source_status_code": "Native status code (PACS EXAMINATION_STATUS_CD or Millennium RESULT_STATUS_CD).",
    "record_status": "Silver record status inherited from clinical_imaging_exam (active, retracted or superseded).",
    "report_available_ind": "At least one resolved report linked to this member (see clinical_imaging_report_link for every link).",
    "report_link_status": "Source report-link status.",
    "report_patient_event_key": "Latest resolved PACS report document key (convenience pointer only).",
    "linked_report_event_id": "Latest Millennium report document EVENT_ID (convenience pointer only).",
    "native_image_count": "Native Sectra IMAGE_COUNT; PACS feed clock.",
    "native_series_count": "Native Sectra SERIES_COUNT.",
    "measured_series_count": "Measured series count; raw series feed clock ends 2024-11-02.",
    "source_feed": "pacs_examination or radiology_event.",
    "source_update_timestamp": "Source update clock inherited from clinical_imaging_exam.",
    "loaded_at": "Bronze load clock inherited from clinical_imaging_exam.",
}

IMAGING_ACCESSION_MEMBER_IG = {c: ("0", "0") for c in IMAGING_ACCESSION_MEMBER_COLUMNS}
IMAGING_ACCESSION_MEMBER_IG.update({
    "accession_key": ("4", "2"), "sectra_accession_number": ("4", "2"),
    "study_instance_uid": ("3", "2"), "subject_key": ("2", "1"), "person_id": ("2", "1"),
    "encounter_id": ("2", "1"), "event_id": ("0", "1"), "performed_datetime": ("1", "1"),
    "linked_report_event_id": ("0", "1"),
})


def _imaging_member_source(pregate_df):
    # Project one arm of the integrated imaging pregate onto the membership shape.
    keep = [
        "patient_event_key", "source_object", "event_id", "pacs_examination_id",
        "study_instance_uid", "subject_key", "subject_id_system", "person_id",
        "identity_status", "encounter_id", "source_coding_system", "source_code",
        "source_display", "modality_code", "body_site_code", "status_code", "record_status",
        "report_patient_event_key", "source_feed", "source_update_timestamp", "loaded_at",
        *IMAGING_INTEGRATION_COLUMNS,
    ]
    return pregate_df.select(*keep)


def _imaging_accession_member_canonical():
    """One row per accession-bearing imaging examination member, both arms.

    Reads the integrated pregates, so examinations without a usable code are retained.
    Millennium members are examination events only (EVENT_CLASS_CD 234); report documents
    and sections are relational report links, not members.
    """
    pacs = _imaging_member_source(_imaging_exam_canonical_pregate(integration=True))
    mill = _imaging_member_source(
        _mill_radiology_exam_canonical_pregate(integration=True)
        .where(F.col("source_event_class_code") == F.lit(str(MILL_RADIOLOGY_EXAM_CLASS_CD)))
    )
    members = (
        pacs.unionByName(mill)
        .where(_present(F.col("sectra_accession_number")))
        .withColumn("accession_key", F.upper(F.trim(F.col("sectra_accession_number"))))
    )
    # Cross-arm accession identity: an incrementalizable aggregate joined back, not a window.
    accession_identity = members.groupBy("accession_key").agg(
        F.countDistinct("person_id").alias("accession_person_count"),
        F.max(F.col("person_id").isNull().cast("int")).alias("_any_unresolved"),
        F.max((F.col("accession_identity_status") == F.lit("CONFLICTING_PERSONS"))
              .cast("int")).alias("_any_refused_conflict"),
    )
    m = members.join(accession_identity, "accession_key", "left")
    person_status = (
        F.when((F.col("accession_person_count") > 1) | (F.col("_any_refused_conflict") == 1),
               F.lit("conflicting_persons"))
        .when(F.col("accession_person_count") == 0, F.lit("no_person"))
        .when(F.col("_any_unresolved") == 1, F.lit("single_person_partial"))
        .otherwise(F.lit("single_person"))
    )
    reasons = F.concat_ws(
        ",",
        F.when(F.col("record_status") != F.lit("active"), F.lit("source_not_current")),
        F.when(F.col("performed_datetime").isNull(), F.lit("no_valid_performed_time")),
        F.when(F.col("performed_datetime") > F.col("loaded_at"), F.lit("performed_after_as_of")),
        F.when(~F.col("performed_evidence_verified_ind"), F.lit("performed_evidence_unverified")),
        F.when(person_status == F.lit("conflicting_persons"),
               F.lit("accession_identity_conflict")),
        F.when((F.col("accession_evidence_level") == F.lit("cerner_reference_parsed"))
               & F.col("accession_format").isin(*UNVALIDATED_ACCESSION_FORMATS),
               F.lit("unvalidated_cerner_accession")),
    )
    blocking = F.when(reasons != F.lit(""), reasons)
    examination_key = F.coalesce(
        F.concat(F.lit("pacs:"), F.col("linked_pacs_examination_id").cast("string")),
        F.concat(F.lit("mill:"), F.col("event_id").cast("string")),
    )
    return m.select(
        stable_id("imaging_accession_member", F.col("patient_event_key")).alias("member_key"),
        "accession_key", "sectra_accession_number", "patient_event_key", "source_object",
        "event_id", "pacs_examination_id", "linked_pacs_examination_id",
        examination_key.alias("examination_key"), "study_instance_uid", "subject_key",
        "subject_id_system", "person_id", "identity_status", "encounter_id",
        "accession_format", "accession_parse_method", "accession_rule_version",
        "accession_evidence_level", "pacs_link_method",
        F.col("accession_person_count").cast("int").alias("accession_person_count"),
        person_status.alias("accession_person_status"),
        "performed_datetime", "performed_datetime_source", "performed_evidence",
        "performed_evidence_verified_ind",
        F.col("loaded_at").alias("eligibility_as_of"),
        blocking.isNull().alias("default_selection_eligible_ind"),
        blocking.alias("eligibility_blocking_reasons"),
        "source_coding_system", "source_code", "source_display", "modality_code",
        "body_site_code", "status_code", "source_status_code", "record_status",
        "report_available_ind", "report_link_status", "report_patient_event_key",
        "linked_report_event_id", "native_image_count", "native_series_count",
        "measured_series_count", "source_feed", "source_update_timestamp", "loaded_at",
    )


# ---- clinical_imaging_report_link ----
IMAGING_REPORT_LINK_COLUMNS = [
    "link_key",
    "link_scope",
    "link_method",
    "accession_key",
    "sectra_accession_number",
    "member_patient_event_key",
    "document_patient_event_key",
    "document_source_feed",
    "pacs_report_id",
    "cerner_document_event_id",
    "document_version_id",
    "document_version_count",
    "document_version_decision",
    "report_resolved_ind",
    "report_datetime",
    "report_status_code",
    "text_source",
    "text_sha256",
    "text_integrity_status",
    "report_text_available_ind",
    "approved_anonymised_text_available_ind",
    "person_id",
    "record_status",
    "loaded_at",
]

IMAGING_REPORT_LINK_COLUMN_COMMENTS = {
    "link_key": "Deterministic SHA-256 over scope, member/accession and document identity.",
    "link_scope": "examination (Sectra report attached to one examination), direct_cerner_event (Cerner report document sharing the examination's REF_EXAM_KEY), or accession (Sectra report attached to the request only; member_patient_event_key is NULL).",
    "link_method": "PACS_EXAMINATION_REPORT, PACS_REQUEST_REPORT or CERNER_REF_EXAM_KEY.",
    "accession_key": "upper(trim(sectra_accession_number)); joins clinical_imaging_accession_member. Identifier.",
    "sectra_accession_number": "Accession of the linked examination or request. Identifier.",
    "member_patient_event_key": "clinical_imaging_exam / accession member patient_event_key; NULL for accession-scope links.",
    "document_patient_event_key": "text_document patient_event_key of the linked document version; NULL for a dangling PACS report id.",
    "document_source_feed": "pacs_report or mill_blob_text.",
    "pacs_report_id": "Sectra ReportId (PACS links). May be dangling (report_resolved_ind false).",
    "cerner_document_event_id": "Cerner report document EVENT_ID: the REF_EXAM_KEY sibling for Cerner links, or the bridge EVENT_ID supplying a PACS report's text.",
    "document_version_id": "Silver document version_id of the linked version.",
    "document_version_count": "Cerner links: current (active) silver document versions for the event. 1 for PACS links.",
    "document_version_decision": "single_current, identical_text_collapsed (several current versions, one text), text_version_ambiguous (conflicting current texts: no text is certified) or no_current_version. PACS native reports: native_report.",
    "report_resolved_ind": "True when the report resolves to a real report/document row. A dangling id never makes a report available.",
    "report_datetime": "Report time (Sectra REPORT_DT_TM; Cerner document event_datetime).",
    "report_status_code": "Source report status code.",
    "text_source": "pacs_native, mill_blob_text_bridge (bridge v4, version-safe), mill_blob_text, or NULL when no text.",
    "text_sha256": "SHA-256 of the linked document text (silver text_document.text_sha256).",
    "text_integrity_status": "Bridge v4 TEXT_INTEGRITY_STATUS for bridged PACS text; NULL otherwise.",
    "report_text_available_ind": "Non-empty report text is available for this link and not version-ambiguous.",
    "approved_anonymised_text_available_ind": "The approved anonymisation lane holds output for the current text (anon_status anonymized and matching source hash). False when missing or stale; never inferred from raw text.",
    "person_id": "Report/document person when resolved.",
    "record_status": "Link record status: active, or the linked document/report record status when that is not active.",
    "loaded_at": "Bronze load clock of the link source row.",
}

IMAGING_REPORT_LINK_IG = {c: ("0", "0") for c in IMAGING_REPORT_LINK_COLUMNS}
IMAGING_REPORT_LINK_IG.update({
    "accession_key": ("4", "2"), "sectra_accession_number": ("4", "2"),
    "person_id": ("2", "1"), "cerner_document_event_id": ("0", "1"),
    "report_datetime": ("1", "1"), "text_sha256": ("1", "1"),
})


def _document_versions():
    # Silver document versions, read once for both arms; the link never re-parses text.
    d = spark.read.table(_n("journey_text.document"))
    return d.select(
        F.col("patient_event_key").alias("_doc_key"),
        F.col("event_id").alias("_doc_event_id"),
        F.col("update_count").alias("_doc_update_count"),
        F.col("valid_from_datetime").alias("_doc_valid_from"),
        F.col("pacs_report_id").alias("_doc_pacs_report_id"),
        F.col("version_id").alias("_doc_version_id"),
        F.col("text_sha256").alias("_doc_text_sha256"),
        (F.col("document_text").isNotNull() & (F.trim(F.col("document_text")) != ""))
        .alias("_doc_has_text"),
        F.col("record_status").alias("_doc_record_status"),
        F.col("event_datetime").alias("_doc_event_datetime"),
        F.col("status_code").alias("_doc_status_code"),
        F.col("person_id").alias("_doc_person_id"),
        F.col("source_feed").alias("_doc_source_feed"),
    )


def _pacs_report_links(docs):
    lk = read_source(SRC_PACS_EXAM_REPORT_LINK).where(
        F.coalesce(F.col("SOURCE_PRESENT_IND"), F.lit(True))
        & F.col("LINK_SCOPE").isin("EXAMINATION", "REQUEST")
    )
    report = read_source(SRC_PACS_REPORT)
    native_text = report.REPORT_TEXT.isNotNull() & (F.trim(report.REPORT_TEXT) != "")
    report = report.select(
        F.col("PACS_REPORT_ID").alias("_r_id"),
        native_text.alias("_r_native_text"),
        _anon_output_or_null(report, "anon_report_text", ["REPORT_TEXT"]).isNotNull()
        .alias("_r_native_anon"),
        F.coalesce(F.col("SOURCE_PRESENT_IND"), F.lit(True)).alias("_r_present"),
    )
    bridge_src = read_source(SRC_PACS_TEXT_BRIDGE)
    bridge_present = F.coalesce(bridge_src.SOURCE_PRESENT_IND, F.lit(True))
    bridge = bridge_src.where(bridge_present).select(
        F.col("REPORT_ID").alias("_b_report_id"),
        F.col("EVENT_ID").cast("bigint").alias("_b_event_id"),
        (F.col("BRIDGED_TEXT").isNotNull() & (F.trim(F.col("BRIDGED_TEXT")) != ""))
        .alias("_b_has_text"),
        (F.col("TEXT_INTEGRITY_STATUS") if "TEXT_INTEGRITY_STATUS" in bridge_src.columns
         else F.lit(None).cast("string")).alias("_b_integrity"),
        _anon_output_or_null(bridge_src, "anon_bridged_text", ["BRIDGED_TEXT"]).isNotNull()
        .alias("_b_anon"),
    )
    pacs_docs = docs.where(F.col("_doc_source_feed") == F.lit("pacs_report"))
    j = (
        lk.join(report, lk.PACS_REPORT_ID == F.col("_r_id"), "left")
        .join(bridge, lk.PACS_REPORT_ID == F.col("_b_report_id"), "left")
        .join(pacs_docs, lk.PACS_REPORT_ID == F.col("_doc_pacs_report_id"), "left")
    )
    resolved = F.coalesce(F.col("REPORT_RESOLVED_IND"), F.lit(False)) & F.col("_r_id").isNotNull()
    native = F.coalesce(F.col("_r_native_text"), F.lit(False))
    bridged = ~native & F.coalesce(F.col("_b_has_text"), F.lit(False))
    scope = F.when(F.col("LINK_SCOPE") == F.lit("EXAMINATION"), F.lit("examination")) \
        .otherwise(F.lit("accession"))
    member = F.when(F.col("LINK_SCOPE") == F.lit("EXAMINATION"),
                    stable_id("imaging_exam:pacs", F.col("PACS_EXAMINATION_ID")))
    accession_key = F.upper(F.trim(F.col("SECTRA_ACCESSION_NBR")))
    retracted = ~F.coalesce(F.col("_r_present"), F.lit(True))
    return j.select(
        stable_id("imaging_report_link", scope, F.coalesce(member, accession_key),
                  F.lit("pacs_report"), F.col("PACS_REPORT_ID")).alias("link_key"),
        scope.alias("link_scope"),
        F.when(F.col("LINK_SCOPE") == F.lit("EXAMINATION"), F.lit("PACS_EXAMINATION_REPORT"))
        .otherwise(F.lit("PACS_REQUEST_REPORT")).alias("link_method"),
        accession_key.alias("accession_key"),
        F.col("SECTRA_ACCESSION_NBR").alias("sectra_accession_number"),
        member.alias("member_patient_event_key"),
        F.when(resolved, F.col("_doc_key")).alias("document_patient_event_key"),
        F.lit("pacs_report").alias("document_source_feed"),
        F.col("PACS_REPORT_ID").cast("bigint").alias("pacs_report_id"),
        F.when(bridged, F.col("_b_event_id")).alias("cerner_document_event_id"),
        F.col("_doc_version_id").alias("document_version_id"),
        F.when(resolved, F.lit(1)).cast("int").alias("document_version_count"),
        F.when(~resolved, F.lit(None).cast("string"))
        .when(native, F.lit("native_report"))
        .when(bridged, F.lit("single_current"))
        .otherwise(F.lit("no_current_version")).alias("document_version_decision"),
        resolved.alias("report_resolved_ind"),
        F.col("REPORT_DT_TM").alias("report_datetime"),
        F.col("REPORT_STATUS_CD").cast("string").alias("report_status_code"),
        F.when(resolved & native, F.lit("pacs_native"))
        .when(resolved & bridged, F.lit("mill_blob_text_bridge")).alias("text_source"),
        F.when(resolved & (native | bridged), F.col("_doc_text_sha256")).alias("text_sha256"),
        F.when(bridged, F.col("_b_integrity")).alias("text_integrity_status"),
        (resolved & (native | bridged) & ~retracted).alias("report_text_available_ind"),
        (resolved & ~retracted & (
            (native & F.coalesce(F.col("_r_native_anon"), F.lit(False)))
            | (bridged & F.coalesce(F.col("_b_anon"), F.lit(False)))
        )).alias("approved_anonymised_text_available_ind"),
        F.coalesce(F.col("_doc_person_id"), F.col("EXAMINATION_PERSON_ID")).cast("bigint")
        .alias("person_id"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        F.col("ADC_UPDT").alias("loaded_at"),
    )


def _cerner_report_links(docs):
    r = read_source(SRC_RADIOLOGY_EVENT)
    exams = r.where(
        (F.col("EVENT_CLASS_CD").cast("bigint") == F.lit(MILL_RADIOLOGY_EXAM_CLASS_CD))
        & _present(F.col("REF_EXAM_KEY"))
    ).select(
        F.col("EVENT_ID").alias("_exam_event_id"), F.col("REF_EXAM_KEY").alias("_k"),
        F.col("PERSON_ID").alias("_exam_person"), F.col("SECTRA_ACCESSION_NBR").alias("_acc"),
    )
    doc_events = r.where(
        (F.col("EVENT_CLASS_CD").cast("bigint") == F.lit(MILL_RADIOLOGY_DOCUMENT_CLASS_CD))
        & ~F.coalesce(F.col("IN_ERROR_IND"), F.lit(False))
        & _present(F.col("REF_EXAM_KEY"))
    ).select(
        F.col("EVENT_ID").alias("_doc_evt"), F.col("REF_EXAM_KEY").alias("_k"),
        F.col("PERSON_ID").alias("_doc_evt_person"),
        F.col("PIPELINE_UPDT_DT_TM").alias("_doc_loaded_at"),
    )
    # Identity-compatible siblings only, the same rule as the bronze pointer; conflicting
    # pairs are counted in bronze (REPORT_IDENTITY_CONFLICT_COUNT) and never linked.
    pairs = exams.join(doc_events, "_k").where(
        F.col("_exam_person").isNull() | F.col("_doc_evt_person").isNull()
        | (F.col("_exam_person") == F.col("_doc_evt_person"))
    )
    current = docs.where(
        (F.col("_doc_source_feed") == F.lit("mill_blob_text"))
        & (F.col("_doc_record_status") == F.lit("active"))
    )
    # Multi-current blob versions stay visible: count them and their distinct texts. No
    # timestamp-only winner is picked (the silver is_latest_version flag is not used here).
    version_stats = current.groupBy("_doc_event_id").agg(
        F.count(F.lit(1)).alias("_versions"),
        F.countDistinct(F.when(F.col("_doc_has_text"), F.col("_doc_text_sha256")))
        .alias("_distinct_texts"),
    )
    blob = read_source(SRC_MILL_BLOB_TEXT)
    blob_anon = blob.select(
        F.col("EVENT_ID").cast("bigint").alias("_a_event_id"),
        F.col("UPDT_CNT").cast("bigint").alias("_a_update_count"),
        F.col("VALID_FROM_DT_TM").alias("_a_valid_from"),
        _anon_output_or_null(blob, "anon_text", ["BLOB_TEXT"]).isNotNull().alias("_a_anon"),
    )
    j = (
        pairs.join(current, pairs._doc_evt == current._doc_event_id, "left")
        .join(version_stats.withColumnRenamed("_doc_event_id", "_vs_event_id"),
              F.col("_doc_evt") == F.col("_vs_event_id"), "left")
        .join(blob_anon,
              (F.col("_doc_event_id") == F.col("_a_event_id"))
              & (F.col("_doc_update_count") == F.col("_a_update_count"))
              & (F.col("_doc_valid_from") == F.col("_a_valid_from")), "left")
    )
    versions = F.coalesce(F.col("_versions"), F.lit(0))
    decision = (
        F.when(versions == 0, F.lit("no_current_version"))
        .when(F.col("_distinct_texts") > 1, F.lit("text_version_ambiguous"))
        .when(versions == 1, F.lit("single_current"))
        .otherwise(F.lit("identical_text_collapsed"))
    )
    certified = decision.isin("single_current", "identical_text_collapsed") \
        & F.coalesce(F.col("_doc_has_text"), F.lit(False))
    member = stable_id("imaging_exam:mill", F.col("_exam_event_id"))
    return j.select(
        stable_id("imaging_report_link", F.lit("direct_cerner_event"), member,
                  F.lit("mill_blob_text"), F.col("_doc_evt"),
                  F.coalesce(F.col("_doc_key"), F.lit("NONE"))).alias("link_key"),
        F.lit("direct_cerner_event").alias("link_scope"),
        F.lit("CERNER_REF_EXAM_KEY").alias("link_method"),
        F.upper(F.trim(F.col("_acc"))).alias("accession_key"),
        F.col("_acc").alias("sectra_accession_number"),
        member.alias("member_patient_event_key"),
        F.col("_doc_key").alias("document_patient_event_key"),
        F.lit("mill_blob_text").alias("document_source_feed"),
        F.lit(None).cast("bigint").alias("pacs_report_id"),
        F.col("_doc_evt").cast("bigint").alias("cerner_document_event_id"),
        F.col("_doc_version_id").alias("document_version_id"),
        versions.cast("int").alias("document_version_count"),
        decision.alias("document_version_decision"),
        F.col("_doc_key").isNotNull().alias("report_resolved_ind"),
        F.col("_doc_event_datetime").alias("report_datetime"),
        F.col("_doc_status_code").alias("report_status_code"),
        F.when(certified, F.lit("mill_blob_text")).alias("text_source"),
        F.when(certified, F.col("_doc_text_sha256")).alias("text_sha256"),
        F.lit(None).cast("string").alias("text_integrity_status"),
        certified.alias("report_text_available_ind"),
        (certified & F.coalesce(F.col("_a_anon"), F.lit(False)))
        .alias("approved_anonymised_text_available_ind"),
        F.coalesce(F.col("_doc_person_id"), F.col("_doc_evt_person")).cast("bigint")
        .alias("person_id"),
        F.lit("active").alias("record_status"),
        F.col("_doc_loaded_at").alias("loaded_at"),
    )


def _imaging_report_link_canonical():
    """Relational imaging report links: every examination/accession-to-document relationship.

    Multiple reports and addenda stay separate rows. Dangling PACS report ids are kept with
    report_resolved_ind false and never make a report available.
    """
    docs = _document_versions()
    return (
        _pacs_report_links(docs).select(*IMAGING_REPORT_LINK_COLUMNS)
        .unionByName(_cerner_report_links(docs).select(*IMAGING_REPORT_LINK_COLUMNS))
    )
