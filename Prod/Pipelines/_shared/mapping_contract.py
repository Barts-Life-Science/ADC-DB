"""R1 mapping contract: the axis fields, the bronze STRUCT, the map key and the resolution hash.

Imported by the bronze attach, the resolver, silver and the parity harness, so a key or a hash can only be computed
one way. It reads nothing; it only builds column expressions and SQL text.

  * map key: sha2 over to_json(struct(lane_id, <the lane's key inputs>)) with nulls kept, the recipe of silver's
    _s3_domain_key (silver_journey_shared.py:1664), so equal inputs share one resolution.
  * resolution_hash: sha2 over the ordered components' clinical outputs (HASH_FIELDS). map_version, the R1
    provenance fields (resolver_version, lookup_versions_json, resolved_at) and the descriptive/diagnostic fields
    (NON_CLINICAL_FIELDS) are outside it, so a vocabulary or lookup reload that changes no clinical output changes
    no hash and triggers no bronze backfill (design rev 3, R1; plan D12).
"""
import pyspark.sql.functions as F

RESOLVER_VERSION = "R1_RESOLVER_V1"

# The 24-field axis contract in silver's order (silver_journey_shared.axis_columns) with gold's types
# (gold_journey_shared._S3_GOLD_AXIS_SCHEMA).
AXIS_FIELDS = [
    ("source_system", "string"), ("source_code", "string"), ("source_display", "string"),
    ("snomed_code", "string"), ("snomed_display", "string"), ("omop_concept_id", "bigint"),
    ("map_method", "string"), ("map_version", "string"), ("map_confidence", "double"),
    ("map_rule_id", "string"), ("map_candidate_count", "int"),
    ("target_system", "string"), ("target_code", "string"), ("target_display", "string"),
    ("omop_source_concept_id", "bigint"), ("map_cosine", "double"), ("map_scoring_model", "string"),
    ("map_status", "string"), ("map_status_reason", "string"), ("map_competing_count", "int"),
    ("map_candidate_set_id", "string"), ("map_component_index", "int"),
    ("map_component_count", "int"), ("map_rollup_levels", "int"),
]
# R1 fields carried on the bronze STRUCT after the axis contract (deviation D5).
R1_FIELDS = [("extras_json", "string"), ("component_identity", "string"),
             ("resolution_hash", "string"), ("resolver_version", "string")]
# Provenance: kept on rows, never hashed. lookup_versions_json and resolved_at live only in mapping_resolution.
PROVENANCE_FIELDS = ("map_version", "resolver_version", "resolution_hash", "lookup_versions_json", "resolved_at")
# Kept on rows, never hashed: descriptions and scoring diagnostics. A 3_lookup.omop.concept refresh that renames
# descriptions, or a rescore that moves confidence but no outcome, changes no hash and triggers no backfill (D12).
NON_CLINICAL_FIELDS = ("source_display", "snomed_display", "target_display", "map_confidence", "map_candidate_count",
                       "map_status_reason", "map_competing_count", "map_candidate_set_id")
# Clinical outputs (design rev 3: codes and ids, method, status, rule, cosine, components) plus the gate inputs
# map_scoring_model and map_rollup_levels, in AXIS_FIELDS order, then the R1 extras and component identity.
HASH_FIELDS = tuple([n for n, _ in AXIS_FIELDS if n != "map_version" and n not in NON_CLINICAL_FIELDS]
                    + ["extras_json", "component_identity"])

PROD_LOOKUP, PROD_CONTROL, DEV_SCHEMA = "`3_lookup`.omop", "`6_mgmt`.bronze", "`8_dev`.insr_r1"
# R4's release marker (design R4 "Release boundary"). The backfill holds pending lane changes while a release is open
# (Ben's 2026-11-01 ruling: gold's per-key _mapping_admission MV and the silver rows it gates come from one
# resolution). Dev: a table of the same shape in the R1 dev schema; absent = no release open.
RELEASE_STATE_PROD, RELEASE_STATE_DEV = "`6_mgmt`.data_quality.release_state", "`8_dev`.insr_r1.release_state"


# SRC_LINEAGE_V1: source-field rules that bronze (mapping_chain) and silver (cross_qc prepare steps) both apply.
# One definition here, two callers, so the two layers cannot drift and silver never reads a source field back out of
# the bronze mapping STRUCT.
ICD10_SYSTEM = "http://hl7.org/fhir/sid/icd-10"
ARIA_ADVERSE_EVENT_SYSTEM = "urn:barts:aria:adverse-event-term"


def aria_stage_source_system(system_col, code_col):
    """condition_stage.stage_diagnosis source system with the ARIA relabel (P9_L2538b).

    About 98k ARIA staging rows are labelled ICD-10 but carry adverse-event terms ("Leukocytes", "Neutrophils" ...)
    rather than codes. A row labelled ICD-10 whose code does not start with a letter and two digits is relabelled
    'urn:barts:aria:adverse-event-term', so no ICD-10 consumer reads it as a code (no mapping is attempted: J33).
    Every other row keeps its system. Pure: builds a Column from the two input Columns, reads nothing."""
    labelled_icd10 = system_col == ICD10_SYSTEM
    icd_shaped = F.upper(F.trim(code_col)).rlike(r"^[A-Z][0-9]{2}")
    return F.when(labelled_icd10 & ~icd_shaped, F.lit(ARIA_ADVERSE_EVENT_SYSTEM)).otherwise(system_col)


def schemas_for(target_fqn):
    """Lookup and control schemas for a bronze target: prod for 4_prod targets, the R1 dev schema for 8_dev ones."""
    if target_fqn.replace("`", "").startswith("8_dev."):
        return {"lookup": DEV_SCHEMA, "control": DEV_SCHEMA}
    return {"lookup": PROD_LOOKUP, "control": PROD_CONTROL}


def table_names(schemas):
    """Fully qualified names of the R1 objects in one schema pair."""
    lk, ct = schemas["lookup"], schemas["control"]
    return {"lane": f"{lk}.mapping_lane", "resolution": f"{lk}.mapping_resolution",
            "change": f"{lk}.mapping_resolution_change", "usage": f"{lk}.mapping_resolution_usage",
            "policy": f"{lk}.mapping_admission_policy", "attach_state": f"{ct}.mapping_attach_state",
            "audit": f"{ct}.mapping_resolver_audit", "crosswalk": f"{ct}.mapping_snomed_crosswalk",
            "release_state": RELEASE_STATE_DEV if lk == DEV_SCHEMA else RELEASE_STATE_PROD}


def axis_struct_ddl(with_r1_fields=True):
    """struct<...> DDL of one axis (24 fields, plus the R1 fields on bronze)."""
    fields = AXIS_FIELDS + (R1_FIELDS if with_r1_fields else [])
    return "struct<" + ",".join(f"{n}:{t}" for n, t in fields) + ">"


def lane_columns(lane_id):
    """(<LANE>_MAPPING, <LANE>_MAP_KEY, <LANE>_MAP_COMPONENTS_JSON): the three bronze columns of one lane."""
    up = lane_id.upper()
    return f"{up}_MAPPING", f"{up}_MAP_KEY", f"{up}_MAP_COMPONENTS_JSON"


def is_r1_column(name):
    """True for <LANE>_MAPPING, <LANE>_MAP_KEY and <LANE>_MAP_COMPONENTS_JSON of any lane: exact names from
    lane_columns over the lane registry, never a suffix guess (fold-in R1-b: no builder row hash may cover them;
    the backfill owns them and the STRUCT carries refresh clocks such as resolver_version)."""
    import mapping_lanes                                   # imported here: mapping_lanes imports this module
    if not hasattr(is_r1_column, "_names"):
        is_r1_column._names = frozenset(c for lane_id in mapping_lanes.LANES for c in lane_columns(lane_id))
    return name in is_r1_column._names


def assert_key_inputs(columns, lane_id, key_inputs):
    """Build-time guard (design: 'a build-time assertion checks that every key input is present')."""
    missing = [c for c in key_inputs if c not in columns]
    if missing:
        raise RuntimeError(f"R1 lane {lane_id}: key inputs missing from the rows: {missing}")


def map_key_expr(lane_id, key_inputs):
    """sha2 of the lane id plus its key inputs, nulls kept (the _s3_domain_key recipe)."""
    return F.sha2(F.to_json(F.struct(F.lit(lane_id).alias("lane_id"), *[F.col(c) for c in key_inputs]),
                            {"ignoreNullFields": "false"}), 256)


def resolution_hash_sql(alias):
    """SQL for one component's hash payload: to_json of the HASH_FIELDS of `alias` (a struct or table alias)."""
    return "to_json(named_struct(" + ", ".join(f"'{n}', {alias}.{n}" for n in HASH_FIELDS) + "))"


def resolution_hash_of_components(components_col):
    """Hash of a key: sha2 over the components' hash payloads, ordered by component index (array input)."""
    payload = F.transform(F.array_sort(components_col, lambda a, b: F.when(
        a["map_component_index"] < b["map_component_index"], -1).when(
        a["map_component_index"] > b["map_component_index"], 1).otherwise(0)),
        lambda c: F.to_json(F.struct(*[c[n].alias(n) for n in HASH_FIELDS])))
    return F.sha2(F.concat_ws("\u0001", payload), 256)


# Parity runs (dev) read every source "as of" a model's last silver refresh T. Delta refuses a timestamp later than a
# table's latest commit (DELTA_TIMESTAMP_GREATER_THAN_COMMIT: every table that did not change after T), so readers
# resolve T to a version first. Prod runs never pass an as-of timestamp. This is the one helper here that reads.
_VERSION_AS_OF = {}


def version_as_of(spark, name, as_of):
    """Latest Delta version of `name` committed at or before `as_of` (memoised per run), or None when the table had
    no commit by then (created after T: read at its current version, like a dev override)."""
    key = (name.replace("`", ""), as_of)
    if key not in _VERSION_AS_OF:
        row = (spark.sql(f"DESCRIBE HISTORY {name}").where(F.col("timestamp") <= F.lit(as_of).cast("timestamp"))
               .agg(F.max("version").alias("v")).first())
        _VERSION_AS_OF[key] = None if row["v"] is None else int(row["v"])
    return _VERSION_AS_OF[key]


def reader_as_of(spark, name, as_of):
    """spark.read pinned to `name`'s version at `as_of` (plain spark.read when as_of is empty or the table is newer)."""
    version = version_as_of(spark, name, as_of) if as_of else None
    return spark.read if version is None else spark.read.option("versionAsOf", version)


def sql_as_of(spark, name, as_of):
    """The ' VERSION AS OF n' suffix for `name` at `as_of` in SQL text ('' when not pinned)."""
    version = version_as_of(spark, name, as_of) if as_of else None
    return "" if version is None else f" VERSION AS OF {version}"
