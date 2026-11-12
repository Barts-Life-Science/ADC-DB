"""Silver core: journey_core.

- cut from silver_journey_shared.py (pre-R3 text, 1.0.0)
- names no dataset, model or gold table (gate 1)

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


CORE_VERSION = "1.0.0"


from pyspark.sql import SparkSession


import pyspark.sql.functions as F


from pyspark.sql import Column


from pyspark.sql.window import Window


import sys as _r1_sys






import json as _json


from silver_core.active_rows import active_rows  # ACTIVE_ROWS_V1


from pyspark.sql import functions as F, Window


spark = SparkSession.builder.getOrCreate()


_r1_sys.path.append(spark.conf.get("journey.r1_shared_path", "/Workspace/Shared/ADC-DB/Prod/Pipelines/_shared"))
import mapping_contract as R1C  # noqa: E402
import mapping_lanes as R1L  # noqa: E402


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


SOURCE_OVERRIDES = _json.loads(spark.conf.get("journey.source_overrides", "{}"))


def read_source(name):
    # Read a source table, honoring the configured development source override when present.
    # ACTIVE_ROWS_V1: bronze rows that are not active never enter silver; the rule lives in silver_active_rows.py.
    return active_rows(spark.read.table(SOURCE_OVERRIDES.get(name, name)), name)


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


SNOMED_URI = "http://snomed.info/sct"


OMOP_URI = "urn:omop:concept_id"


# FINDING_AXIS_EVENTCD_V1: Cerner EVENT_CD -> finding (question/observable) concept, built by
# CC/FindingAxis_20261025 from the curated Barts catalogues, OMOP exact names and Opus-verified S5 proposals.
# INSR_W2 W2.03: OMOP Maps-to per (vocabulary, code), built from 3_lookup.omop.concept / concept_relationship.
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


# INSR_W2 W2.04: Millennium code values decoded through 3_lookup.mill.mill_code_value (unique per CODE_VALUE,
# row tracking + CDF + DV). Each decoder is one code set, small enough to broadcast.
SRC_MILL_CODE_VALUE = "3_lookup.mill.mill_code_value"


def _code_value_decoder(code_set, prefix):
    """CODE_VALUE -> DISPLAY for one Millennium code set, as columns <prefix>_code / <prefix>_display.

    Read directly, not through read_source: its active-row rule drops ACTIVE_IND 0 code values, but a retired code
    still names what historical facts recorded (W9.05 smoke: allergy severity 53323300 'Medium' is inactive)."""
    return (spark.read.table(SOURCE_OVERRIDES.get(SRC_MILL_CODE_VALUE, SRC_MILL_CODE_VALUE))
            .where(F.col("CODE_SET") == F.lit(code_set))
            .select(F.col("CODE_VALUE").cast("bigint").alias(f"{prefix}_code"),
                    F.col("DISPLAY").alias(f"{prefix}_display")))


def _decode_display(df, code_column, display_column, code_set):
    """Fill display_column from the code set when the source carried only the code (existing text wins)."""
    prefix = f"_cv{code_set}"
    cv = F.broadcast(_code_value_decoder(code_set, prefix))
    # The code columns carry bronze DOUBLE codes rendered as strings ('3294.0', '5.33233E7'): a direct bigint cast
    # misses the exponent form, so read the text as DOUBLE first (Java's double text round-trips exactly).
    joined = df.join(cv, F.col(code_column).cast("double").cast("bigint") == F.col(f"{prefix}_code"), "left")
    return (joined.withColumn(display_column, F.coalesce(F.col(display_column), F.col(f"{prefix}_display")))
                  .drop(f"{prefix}_code", f"{prefix}_display"))


# ==== Pathway and administrative feeds ====
def _clamped_ts(col):
    """Return NULL for source timestamps outside the governed [1950, 2100) range."""
    return F.when(
        (col >= F.lit("1950-01-01").cast("timestamp"))
        & (col < F.lit("2100-01-01").cast("timestamp")),
        col,
    )




def declare(spec):
    """Register one dataset (R2's domain files call core.declare). Defined in table_spec; imported on use, which avoids an import cycle."""
    from silver_core.table_spec import declare as _declare
    return _declare(spec)
