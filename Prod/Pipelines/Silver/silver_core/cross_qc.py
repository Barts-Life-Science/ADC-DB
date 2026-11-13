"""Silver core: cross_qc.

- cut from silver_journey_shared.py (pre-R3 text, 1.0.0)
- names no dataset, model or gold table (gate 1)

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import (
    LIFECYCLE_FIELD_NAMES,
    OMOP_URI,
    R1C,
    R1L,
    SNOMED_URI,
    _n,
    canonical_coding_system,
    read_source,
    spark,
    stable_id,
)


from pyspark.sql import SparkSession


import pyspark.sql.functions as F


from pyspark.sql import Column


from pyspark.sql.window import Window


import sys as _r1_sys


import mapping_contract as R1C  # noqa: E402


import mapping_lanes as R1L  # noqa: E402


import json as _json


from dataclasses import dataclass


from silver_core.active_rows import active_rows  # ACTIVE_ROWS_V1


from pyspark.sql import functions as F, Window


_S3_CODING_SCHEMA = "array<struct<coding_system:string,coding_code:string,coding_display:string,is_source:boolean,map_source:string,map_version:string>>"


# ==== SRC_LINEAGE_V1: source fields come from the row's own scalar columns ====
# Every {axis}_source_system/_code/_display in silver is set by _s3_source_axis_from_columns from plain columns of the
# row, never from the JSON bridge or the bronze mapping STRUCT. Unity Catalog column lineage then charges each source
# field with only the columns it really comes from (Phase 0a, 2026-11-12: identical values on every coded table).


@dataclass(frozen=True)
class SourceInputs:
    """The row's own scalar columns a coding axis takes its three source fields from.

    system, code, display - column names on the row before the public projection (internal columns allowed)
    system_default        - literal used when the system column is NULL (the bridge's own coalesce), or None
    A model overrides DEFAULT_SOURCE_INPUTS for one coded column in CrossQc.source_inputs only when its bridge built
    the source coding from other columns (Phase 0a found one: specimen's bronze namespace).
    """
    system: str
    code: str
    display: str
    system_default: str = None


# The bridge's source coding of every coded model is built from these three columns (Phase 0a).
DEFAULT_SOURCE_INPUTS = SourceInputs("source_coding_system", "source_code", "source_display")


def _s3_source_axis_from_columns(df, axis, source_system, source_code, source_display):
    """The one place silver sets an axis's three source fields: from scalar Column expressions (None = NULL), with
    canonical_coding_system on the system, exactly as bronze's _s3_direct_axis and source-coding flatten do."""
    as_string = lambda value: value.cast("string") if value is not None else F.lit(None).cast("string")  # noqa: E731
    system = canonical_coding_system(source_system) if source_system is not None else F.lit(None).cast("string")
    return (df.withColumn(f"{axis}_source_system", system)
              .withColumn(f"{axis}_source_code", as_string(source_code))
              .withColumn(f"{axis}_source_display", as_string(source_display)))


def _s3_source_axis_from_bridge(df, json_name, axis):
    """Pre-SRC_LINEAGE_V1 source fields: the bridge's is_source coding (system canonicalised). Used only by direct
    callers of _s3_flatten_codeable_json that pass no source_inputs (endoscopy_finding, asset), whose output this
    release leaves unchanged; the cross-QC engine always passes source_inputs."""
    source = F.element_at(F.filter(F.from_json(F.col(json_name), _S3_CODING_SCHEMA), lambda x: x["is_source"]), 1)
    return (df.withColumn(f"{axis}_source_system", canonical_coding_system(source["coding_system"]))
              .withColumn(f"{axis}_source_code", source["coding_code"])
              .withColumn(f"{axis}_source_display", source["coding_display"]))


def _s3_source_inputs(df, qc, public_name):
    """(system, code, display) Columns of one coded column's source inputs: the model's override, else the default.

    A named input column missing from the row is a hard error at graph build (a silent NULL would publish an
    axis without its source)."""
    inputs = (qc.source_inputs.get(public_name) if qc is not None else None) or DEFAULT_SOURCE_INPUTS
    missing = [c for c in (inputs.system, inputs.code, inputs.display) if c not in df.columns]
    if missing:
        model = qc.model if qc is not None else "?"
        raise RuntimeError(f"{model}.{public_name}: source input columns missing from the row: {missing}")
    system = F.col(inputs.system)
    if inputs.system_default is not None:
        system = F.coalesce(system, F.lit(inputs.system_default))
    return system, F.col(inputs.code), F.col(inputs.display)


def _s3_flatten_codeable_json(df, json_name, axis, source_only=False, qc=None, source_inputs=None):
    """Flatten one CodeableConcept JSON bridge into the flat axis contract without crossing a VARIANT join boundary.

    SRC_LINEAGE_V1: the three source fields come from source_inputs (the row's scalar columns, see
    _s3_source_inputs), not from the bridge's is_source coding. A source-only or lane axis needs nothing else and
    never reads the JSON; otherwise the mapped fields are still derived from the bridge's SNOMED/OMOP codings (their
    lineage to the mapping is genuine).

    A direct caller outside the cross-QC engine that passes no source_inputs (endoscopy_finding, asset: not in this
    release's scope) keeps the pre-release behaviour, source fields from the bridge's is_source coding, so its output
    is unchanged (_s3_source_axis_from_bridge)."""
    if source_inputs is None:
        df = _s3_source_axis_from_bridge(df, json_name, axis)
    else:
        df = _s3_source_axis_from_columns(df, axis, *source_inputs)
    if source_only:
        return df
    arr = F.from_json(F.col(json_name), _S3_CODING_SCHEMA)
    source = F.element_at(F.filter(arr, lambda x: x["is_source"]), 1)
    snomed = F.element_at(F.filter(arr, lambda x: canonical_coding_system(x["coding_system"]) == F.lit(SNOMED_URI)), 1)
    omop = F.element_at(F.filter(arr, lambda x: canonical_coding_system(x["coding_system"]) == F.lit(OMOP_URI)), 1)
    source_system = canonical_coding_system(source["coding_system"])
    source_native = source_system == F.lit(SNOMED_URI)
    # SCT_TRUD_V1: SNOMED columns hold concept IDs only. A native SNOMED source code is Cerner's description ID, so
    # the concept comes from the bridge's own SNOMED coding (bronze SNOMED_CODE = the CKI concept) when present;
    # the source code is only a fallback, and the resolver's concept guard checks it (rf2 TUI -> CUI or NULL).
    bridge_snomed = F.element_at(F.filter(arr, lambda x: ~F.coalesce(x["is_source"], F.lit(False)) &
                                          (canonical_coding_system(x["coding_system"]) == F.lit(SNOMED_URI))), 1)
    has_cki = bridge_snomed["coding_code"].isNotNull()
    native_code = F.when(has_cki, bridge_snomed["coding_code"]).otherwise(source["coding_code"])
    native_display = F.when(has_cki, F.coalesce(bridge_snomed["coding_display"], source["coding_display"])) \
                      .otherwise(source["coding_display"])
    native_rule = F.when(has_cki, F.lit("source:canonical_snomed:cki")) \
                   .otherwise(F.lit("source:canonical_snomed:source_code"))
    mapped = source_native | snomed["coding_code"].isNotNull() | omop["coding_code"].isNotNull()
    has_source = source["coding_code"].isNotNull() | source["coding_display"].isNotNull()
    method_source = F.coalesce(snomed["map_source"], omop["map_source"])
    method_lower = F.lower(F.coalesce(method_source, F.lit("")))
    # Bronze embedding tiers keep stable lane names so Gold can consult the
    # measured calibration table. Other governed mappings are deterministic.
    # INSR R3: the per-model lanes of the legacy CASE are data (CrossQc.embedding_lanes), applied in CASE order.
    embedding_lane = None
    for lane_axis, lane_pattern, lane_value in (qc.embedding_lanes if qc is not None else ()):
        lane_condition = F.lit(axis) == lane_axis
        if lane_pattern is not None:
            lane_condition = lane_condition & method_lower.rlike(lane_pattern)
        embedding_lane = (F.when(lane_condition, F.lit(lane_value)) if embedding_lane is None
                          else embedding_lane.when(lane_condition, F.lit(lane_value)))
    if embedding_lane is None:
        embedding_lane = F.lit(None).cast("string")
    embedding = embedding_lane.isNotNull()
    deterministic_method = (F.when(method_lower.rlike("exact|synonym"), F.lit("exact_synonym"))
                              .otherwise(F.lit("rule")))
    return (df.withColumn(f"{axis}_snomed_code", F.when(source_native, native_code).otherwise(snomed["coding_code"]))
              .withColumn(f"{axis}_snomed_display", F.when(source_native, native_display).otherwise(snomed["coding_display"]))
              .withColumn(f"{axis}_omop_concept_id", F.expr(f"try_cast(element_at(filter(from_json(`{json_name}`, '{_S3_CODING_SCHEMA}'), x -> lower(x.coding_system) = '{OMOP_URI}'), 1).coding_code as bigint)"))
              .withColumn(f"{axis}_map_method", F.when(source_native, F.lit("source_native"))
                          .when(mapped & embedding, F.lit("embedding_proposed"))
                          .when(mapped, deterministic_method))
              .withColumn(f"{axis}_map_version", F.coalesce(snomed["map_version"], omop["map_version"]))
              .withColumn(f"{axis}_map_confidence", F.when(source_native, F.lit(1.0)).cast("double"))
              .withColumn(f"{axis}_map_rule_id", F.when(source_native, native_rule)
                          .when(mapped & embedding, F.concat(F.lit("embedding:"), embedding_lane, F.lit(":tier")))
                          .when(mapped, F.concat(F.lit("source:"), F.coalesce(method_source, F.lit("governed_mapping")))))
              .withColumn(f"{axis}_map_candidate_count", F.when(mapped, F.lit(1)).otherwise(F.lit(0)).cast("int"))
              .withColumn(f"{axis}_target_system", F.when(source_native | snomed["coding_code"].isNotNull(), F.lit(SNOMED_URI))
                          .when(omop["coding_code"].isNotNull(), F.lit(OMOP_URI)))
              .withColumn(f"{axis}_target_code", F.when(source_native, native_code)
                          .otherwise(F.coalesce(snomed["coding_code"], omop["coding_code"])))
              .withColumn(f"{axis}_target_display", F.when(source_native, native_display)
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
    df = _s3_source_axis_from_columns(df, axis, source_system, source_code, source_display)  # SRC_LINEAGE_V1
    return (df.withColumn(f"{axis}_snomed_code", col(snomed_code, "string"))
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


def _cross_qc_primitive(df, qc, variant_json_columns):
    # Replace configured VARIANT fields with JSON strings and select the internal lifecycle-
    # stage columns.
    for public_name, json_name in variant_json_columns.items():
        if public_name in df.columns:
            df = df.withColumn(json_name, F.to_json(F.col(public_name))).drop(public_name)
        elif json_name not in df.columns:
            raise RuntimeError(
                f"{qc.model}: missing VARIANT column {public_name!r} and JSON bridge {json_name!r}"
            )

    spec = {"fk": qc.fk, "dates": qc.dates}
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


# The three source fields of the axis contract; silver sets them only from scalar columns (SRC_LINEAGE_V1).
_SOURCE_FIELDS = ("source_system", "source_code", "source_display")


def _r1_expand_lane(df, qc, lane_id):
    """INSR R1: one lane's bronze STRUCT (_r1_<lane>_mapping, carried from the pregate) -> the flat 24-column axis
    contract, plus the lane's extra columns (W2.03 omop provenance, imaging body site) parsed once from extras_json.

    SRC_LINEAGE_V1: every row keeps its own three source fields, set before this step from its scalar columns by the
    flatten or the model's prepare steps (the same rules bronze applies); the STRUCT supplies only mapped fields. A
    lane axis whose source fields were not set is a hard error at graph build.

    A row whose key is not resolved yet (NULL STRUCT: a silver-derived key new in this release, design D4) carries
    map_status 'pending_resolution' and has every mapped field NULL; the next bronze run resolves the key."""
    lane = R1L.LANES[lane_id]
    mapping = f"_r1_{lane_id}_mapping"
    resolved = F.col(mapping).isNotNull()
    source_columns = [f"{lane.axis}_{field}" for field in _SOURCE_FIELDS]
    missing = [column for column in source_columns if column not in df.columns]
    if missing:
        raise RuntimeError(f"{qc.model}: lane {lane_id} has no source fields set before expansion: {missing}")
    for field_name, typ in R1C.AXIS_FIELDS:
        column = f"{lane.axis}_{field_name}"
        if field_name in _SOURCE_FIELDS:
            continue                                   # SRC_LINEAGE_V1: the row's own value stands
        elif field_name == "map_status":
            has_source = F.lit(False)
            for source_column in source_columns:
                has_source = has_source | F.col(source_column).isNotNull()
            fallback = F.when(has_source, F.lit("pending_resolution"))
        else:
            fallback = F.lit(None).cast(typ)
        df = df.withColumn(column, F.when(resolved, F.col(f"{mapping}.{field_name}")).otherwise(fallback))
    # Ben's 2026-11-01 ruling (R4 D4 reverted): the key this row was resolved by, so gold's materialised tables can
    # join the per-key _mapping_admission MV. Set for unresolved rows too (their key resolves on the next bronze run).
    df = df.withColumn(f"{lane.axis}_map_key", F.col(f"_r1_{lane_id}_map_key"))
    if lane.extras:
        schema = "struct<" + ",".join(f"{c}:string" for c in lane.extras) + ">"
        df = df.withColumn("_r1_extras", F.from_json(F.col(f"{mapping}.extras_json"), schema))
        for column in lane.extras:
            df = df.withColumn(column, F.col(f"_r1_extras.{column}"))
        df = df.drop("_r1_extras")
    return df


def _r1_explode_components(df, qc, lane_id):
    """INSR R1: a source code the resolver split into several concepts becomes one row per component, exactly as
    the S3 lookup did: component 1 keeps the event key; components 2+ get stable_id('s3b_component', source key,
    axis, component identity) and their shared quantities cleared. All components keep the row's own source fields
    (SRC_LINEAGE_V1)."""
    lane = R1L.LANES[lane_id]
    if not lane.allow_split:
        return df
    comps = f"_r1_{lane_id}_components_json"
    if "source_patient_event_key" not in df.columns:
        df = df.withColumn("source_patient_event_key", F.col("patient_event_key"))
    schema = "array<" + R1C.axis_struct_ddl() + ">"
    df = df.withColumn("_r1_comp", F.explode_outer(F.from_json(F.col(comps), schema)))
    split = F.coalesce(F.col("_r1_comp.map_component_index"), F.lit(1)) > 1
    for name, _ in R1C.AXIS_FIELDS:
        if name in _SOURCE_FIELDS:
            continue                                   # SRC_LINEAGE_V1: every component keeps the row's source
        df = df.withColumn(f"{lane.axis}_{name}", F.when(F.col("_r1_comp").isNotNull(), F.col(f"_r1_comp.{name}"))
                           .otherwise(F.col(f"{lane.axis}_{name}")))
    df = df.withColumn("patient_event_key", F.when(split, stable_id(
        "s3b_component", F.col("source_patient_event_key"), F.lit(lane.axis), F.col("_r1_comp.component_identity")))
        .otherwise(F.col("patient_event_key")))
    for quantity in qc.shared_quantities:
        if quantity in df.columns:
            df = df.withColumn(quantity, F.when(split, F.lit(None).cast(df.schema[quantity].dataType))
                               .otherwise(F.col(quantity)))
    return df.drop("_r1_comp")


def _r1_attach_silver_arm(df, source, model):
    """INSR R1 (deviation D4): a silver-derived arm takes its lane columns from mapping_resolution by the shared key."""
    import mapping_attach as R1A
    return R1A.attach_silver_arm(df, source, model, read_source)   # dev source_overrides apply


def _cross_qc_prepare(df, qc, variant_json_columns, public_columns, lifecycle_columns):
    """Rows of a public clinical table before its S3 mapping chain: validated, flattened, direct axes, bronze
    provenance and the medication bronze joins. Everything here is per row."""
    # contract v2: lifecycle columns are validated here but never appended to the public parent.
    # Read the internal stage and reconstruct the public schema, rejecting lifecycle fields in
    # the public column list.
    leaked = [column for column in public_columns if column in LIFECYCLE_FIELD_NAMES
              and column not in qc.promote_lifecycle]  # INSR_R4_SILVER_V1 (decision 1): promoted columns are public
    if leaked:
        raise RuntimeError(f"{qc.model}: lifecycle columns remain in the public list: {leaked}")
    missing = [column for column in lifecycle_columns if column not in df.columns]
    if missing:
        raise RuntimeError(f"{qc.model}: lifecycle source columns are missing: {missing}")
    if "source_coding_system" in df.columns:
        df = df.withColumn("source_coding_system", canonical_coding_system(F.col("source_coding_system")))
    retired = qc.coded_axes
    # INSR R1 (mapping into bronze): a lane axis is flattened source-only here; its mapped fields come from the lane
    # STRUCT in _r1_expand_lane. SRC_LINEAGE_V1: its source fields are the row's scalar columns for every row.
    lane_axes = {R1L.LANES[lane_id].axis for lane_id in R1L.lanes_of_model(qc.model)}
    for public_name, json_name in variant_json_columns.items():
        if public_name in retired:
            axis, source_only = retired[public_name]
            df = _s3_flatten_codeable_json(df, json_name, axis, source_only or axis in lane_axes, qc,
                                           _s3_source_inputs(df, qc, public_name))
        else:
            df = df.withColumn(public_name, F.expr(f"parse_json(`{json_name}`)"))
    for step in qc.prepare_steps:
        df = step(df)
    # INSR R1: the device model's OMOP-slot correction runs in the resolver (device lane, mapping_inputs
    # prepare__device); its mapped slots arrive in the lane STRUCT.
    # INSR R1 (mapping into bronze): no bronze provenance re-join and no medication re-joins (the resolver read
    # those fields from the bronze row); mapped axes arrive as one STRUCT per lane from bronze (or, for silver-derived
    # arms, from mapping_resolution by the same key) and are expanded here. The direct-axis block above is kept: it
    # gives direct lanes their source fields, which _r1_expand_lane keeps for every row (SRC_LINEAGE_V1).
    for lane_id in R1L.lanes_of_model(qc.model):
        df = _r1_expand_lane(df, qc, lane_id)
    return df


def _cross_qc_finish(df, qc, public_columns):
    """The per-row steps after the mapping chain, and the public column list."""
    for step in qc.finish_steps:
        df = step(df)
    if qc.parent_inputs:  # INSR_R4_SILVER_V1 (decision 2): R3 D9 hook
        from silver_core.parent_inputs import join_parent_inputs
        df = join_parent_inputs(df, qc.parent_inputs)
    # INSR_W2: canonical columns added by later releases are published through silver_value_policy.
    from silver_core import value_policy as insr_value_policy
    return df.select(*insr_value_policy.public_columns_for(tuple(qc.extra_public_columns) + tuple(qc.promote_lifecycle)
                                                    + tuple(qc.parent_inputs.get("columns", ())),
                                                    public_columns, df.columns))


# S3_DOMAIN_STAGES_V1
# Public clinical tables whose mapping chain is too deep for Lakeflow to plan incrementally in one MV (planning cost
# grows ~9x per chained lookup axis; three axes exhausted a 45 GiB driver on 100 rows, 2026-10-09). For these the
# chain on `axes` runs once per distinct mapping input, in private stages of at most S3_STAGE_JOIN_BUDGET joins, and
# the public MV joins its rows to the last stage. `inputs` are the non-axis columns the chain reads; an omission fails
# at graph build, because a stage holds only the declared columns. pathology_result maps only its test axis this way:
# its result axis keys on free-text values, so it stays per row.

# variant_json_columns each domain was declared with; the public MV must pass the same.
def _cross_qc_public(df, qc, variant_json_columns, public_columns, lifecycle_columns):
    # contract v2: lifecycle columns are validated here but never appended to the public parent.
    # INSR R1 (mapping into bronze): no mapping chain in silver. Per-row preparation (lane STRUCT expansion), the
    # component split the resolver recorded, then the per-row finish.
    df = _cross_qc_prepare(df, qc, variant_json_columns, public_columns, lifecycle_columns)
    for lane_id in R1L.lanes_of_model(qc.model):
        df = _r1_explode_components(df, qc, lane_id)
    # Each lane axis also publishes the key it was resolved by (Ben's 2026-11-01 ruling: gold's materialised tables
    # join the per-key _mapping_admission MV on it). Additive; OMOP ignores it.
    _r1_map_keys = [f"{R1L.LANES[l].axis}_map_key" for l in R1L.lanes_of_model(qc.model)]
    return _cross_qc_finish(df, qc, list(public_columns) + [k for k in _r1_map_keys if k not in public_columns])
