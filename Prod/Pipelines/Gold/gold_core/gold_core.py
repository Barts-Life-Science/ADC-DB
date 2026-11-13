"""Gold core: the shared gold transformations, table-free.

- cut from Gold/gold_journey_shared.py (pre-R3 text)
- functions that took table_name/target_name take a GoldRules (gold_spec.py)
- names no gold table (gate 1); the datasets every _qc reads are the CORE_DATASETS roles

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import json


import re


from pyspark.sql import SparkSession


from pyspark.sql import functions as F


from pyspark import pipelines as dp


import sys as _r1_sys




# D12: the infrastructure datasets every _qc reads, by role (the only dataset names in gold core).
CORE_DATASETS = {'encounter_keys': 'gold_qc._encounter_keys',  # GOLD_MV_SPLIT_V1: internal MVs in spine
 'episode_keys': 'gold_qc._episode_keys',
 'person_dates': 'gold_qc._person_dates',
 'person_scope': 'gold_qc._person_scope',
 'person_spine': 'spine_person'}


spark = SparkSession.builder.getOrCreate()


SILVER_CATALOG_SCHEMA = spark.conf.get("journey.silver_catalog_schema")


PUBLIC_SCHEMA = spark.conf.get("journey.public_schema")


# Underscore-prefixed datasets are implementation objects, not research products, and live
# in the internal schema exactly as they do in the silver pipeline (dev gold_v2_internal;
# 4_prod.tmp at the Session 4 cutover). The 94 quality-controlled twins moved here on
# 2026-09-09 when they became materialized views: a twin held as a temporary view carries
# no row tracking, so Enzyme could only rebuild every published Gold MV from scratch on
# each update (decision_register: gold_qc_twin_incremental_refresh; proof in
# 8_dev.s2_evidence.gold_incremental_probe).
INTERNAL_SCHEMA = spark.conf.get("journey.internal_schema")


def _n(name):
    """Flatten a logical Gold plane into a configured two-part dataset name."""
    logical_schema, table = name.split(".", 1)
    if not logical_schema.startswith("gold_"):
        raise ValueError(f"unexpected flow schema in {name!r}")
    plane = logical_schema[len("gold_") :]
    if table.startswith("_"):
        return INTERNAL_SCHEMA + "._" + plane + table
    return PUBLIC_SCHEMA + "." + plane + "_" + table


def _src(flat):
    """The configured Silver table a Gold product reads."""
    return SILVER_CATALOG_SCHEMA + "." + flat


GOLD_SEX_PLAUSIBILITY_TABLE = "3_lookup.omop.concept_sex_plausibility"


_r1_sys.path.append(spark.conf.get("journey.r1_shared_path", "/Workspace/Shared/ADC-DB/Prod/Pipelines/_shared"))
import mapping_admission_gate as R1G  # noqa: E402


R1_POLICY = R1G.load_policy(spark, spark.conf.get("journey.r1_policy_table",
                                                  "`3_lookup`.omop.mapping_admission_policy"))


GOLD_V2_POLICIES = R1_POLICY.banded


GOVERNED_TRANSLATION_RULES = R1_POLICY.translation_prefixes


GOVERNED_ROLLUP_RULES = R1_POLICY.rollup_prefixes


# GOLD_COMPACT_GATE_PATCH_APPLIED v1
def _s3_axis_gate_and_policy(item):
    """Decide whether a mapped target meets the configured policy and return its provenance: the shared gate
    (mapping_admission_gate.gate_and_policy, AST-equal to the previous body) over the table-held policy."""
    return R1G.gate_and_policy(item, R1_POLICY)


def _s3_axis_gate(item):
    """Return only the admission decision for one terminology mapping."""
    return _s3_axis_gate_and_policy(item)[0]


def _s3_axis_source_columns(axis):
    """SRC_LINEAGE_V1: the three source columns of a coding axis. Gold carries them as plain columns from silver
    (never through the JSON transport), so their lineage is silver's own source columns."""
    return [f"{axis}_source_system", f"{axis}_source_code", f"{axis}_source_display"]


def _s3_axis_mapped_columns(axis, source_only=False):
    """SRC_LINEAGE_V1: the mapped-target and provenance columns of a coding axis (none for a source-only axis);
    these are the only fields the JSON transport carries across the QC joins."""
    if source_only:
        return []
    return [f"{axis}_snomed_code",f"{axis}_snomed_display",f"{axis}_omop_concept_id",
            f"{axis}_map_method",f"{axis}_map_version",f"{axis}_map_confidence",
            f"{axis}_map_rule_id",f"{axis}_map_candidate_count",
            f"{axis}_target_system",f"{axis}_target_code",f"{axis}_target_display",
            f"{axis}_omop_source_concept_id",f"{axis}_map_cosine",f"{axis}_map_scoring_model",
            f"{axis}_map_status",f"{axis}_map_status_reason",f"{axis}_map_competing_count",
            f"{axis}_map_candidate_set_id",f"{axis}_map_component_index",
            f"{axis}_map_component_count",f"{axis}_map_rollup_levels"]


def _s3_axis_columns(axis, source_only=False):
    """List the source, mapped-target and provenance columns belonging to a coding axis (public order)."""
    return _s3_axis_source_columns(axis) + _s3_axis_mapped_columns(axis, source_only)


# SRC_LINEAGE_V1: the mapped fields of one axis inside the JSON transport (source fields travel as plain columns).
_S3_GOLD_AXIS_MAPPED_SCHEMA = "struct<snomed_code:string,snomed_display:string,omop_concept_id:bigint,map_method:string,map_version:string,map_confidence:double,map_rule_id:string,map_candidate_count:int,target_system:string,target_code:string,target_display:string,omop_source_concept_id:bigint,map_cosine:double,map_scoring_model:string,map_status:string,map_status_reason:string,map_competing_count:int,map_candidate_set_id:string,map_component_index:int,map_component_count:int,map_rollup_levels:int>"


def _s3_mapped_axes(axes):
    """The axes of one spec whose mapped fields ride the JSON transport (source-only axes have none)."""
    return [(axis, source_only) for axis, source_only in axes if not source_only]


def _s3_has_transport(axes):
    """True when a spec carries a JSON transport column; a spec whose axes are all source-only has none."""
    return bool(_s3_mapped_axes(axes))


def _s3_transport_schema(axes):
    """from_json schema of one spec's transport: struct<axis: mapped-only struct> over its mapped axes."""
    return "struct<" + ",".join(f"{axis}:{_S3_GOLD_AXIS_MAPPED_SCHEMA}" for axis, _ in _s3_mapped_axes(axes)) + ">"


def _s3_axis_payload(source, axes):
    """Carry the mapped fields of the coding axes across QC joins as one JSON string, avoiding join-shaped VARIANT
    output. SRC_LINEAGE_V1: source fields are not packed (plain columns keep their own lineage); a spec with no
    mapped axis has no payload (callers check _s3_has_transport first)."""
    fields=[]
    for axis, source_only in _s3_mapped_axes(axes):
        names=_s3_axis_mapped_columns(axis,source_only)
        fields.append(F.struct(*[F.col(name).alias(name[len(axis)+1:]) for name in names]).alias(axis))
    return F.to_json(F.struct(*fields))


def _s3_flatten_gold_public(df, rules):
    """Restore flat coding columns and null targets that fail the mapping policy.

    SRC_LINEAGE_V1: each axis's three source columns arrive as plain columns from the projection and are never
    assigned from the payload (a missing one is a hard error). A spec with a transport restores its mapped fields
    from it and moves its columns, source fields included, into the transport's slot, so the public column order is
    unchanged. A spec whose axes are all source-only has no transport: its projection already lists the three
    source columns in that slot, so there is nothing to parse or gate."""
    specs=rules.s3_axis_specs
    for old_name, axes in specs.items():
        sources=[name for axis, _ in axes for name in _s3_axis_source_columns(axis)]
        missing=[name for name in sources if name not in df.columns]
        if missing:
            raise RuntimeError(f"{rules.table}.{old_name}: source columns missing from the public projection: {missing}")
        if not _s3_has_transport(axes):
            continue
        # Parse once and gate once (see GOLD_PARSE_ONCE in _qc): inline from_json and a per-field gate
        # copy the parse into every reference.
        parsed="__gold_parsed_"+old_name
        df=df.withColumn(parsed,F.from_json(F.col(old_name), _s3_transport_schema(axes)))
        payload=F.col(parsed)
        scratch=[parsed]
        replacement=[]
        for axis, source_only in axes:
            if not source_only:
                a=payload[axis]
                from gold_core import gold_single_copy  # INSR_R4_GOLD_V1 (lazy)
                gate, _ = gold_single_copy.verdict(axis)  # INSR_R4_GOLD_V1
                df=df.withColumn("__gold_gate_"+axis,gate)
                scratch.append("__gold_gate_"+axis)
                gate=F.col("__gold_gate_"+axis)
                for field in ("snomed_code","snomed_display","omop_concept_id",
                              "target_system","target_code","target_display"):
                    df=df.withColumn(f"{axis}_{field}",F.when(gate,a[field]))
                for field in ("map_method","map_version","map_confidence","map_rule_id","map_candidate_count",
                              "omop_source_concept_id","map_cosine","map_scoring_model","map_status",
                              "map_status_reason","map_competing_count","map_candidate_set_id",
                              "map_component_index","map_component_count","map_rollup_levels"):
                    df=df.withColumn(f"{axis}_{field}",a[field])
                # BBR_FOLDIN_V1 (BBR-10, review fix 11-03): "mapped" means a code landed. A mapping the
                # admission gate withholds (below its band cutoff, or no matching policy) publishes no code, so
                # it reads no_candidate with the reason; method, rule and score stay as provenance.
                withheld=(~gate)&(F.coalesce(a["map_status"],F.lit(""))=="mapped")
                df=df.withColumn(f"{axis}_map_status_reason",F.when(withheld,F.lit("withheld_by_admission_policy"))
                                 .otherwise(a["map_status_reason"]))
                df=df.withColumn(f"{axis}_map_status",F.when(withheld,F.lit("no_candidate")).otherwise(a["map_status"]))
            replacement.extend(_s3_axis_columns(axis,source_only))
        order=[]
        for name in df.columns:
            if name==old_name: order.extend(replacement)
            elif name not in replacement: order.append(name)
        df=df.select(*[name for name in order if name not in scratch])
    # SCT_TRUD_V1: the W4 value rules on the restored axis columns (deferred by insr_w4_apply_value_rules, e.g.
    # sct_concept_only: SNOMED columns hold concept IDs only) apply here, where those columns first exist.
    restored = {name for axes in specs.values() for axis, source_only in axes
                for name in _s3_axis_columns(axis, source_only)}
    for rule_id, rule in sorted(rules.value_rules.items()):
        targets = [rule["column"], *rule["companions"]]
        if rule["table"] != rules.table or not set(targets) <= restored:
            continue
        df = df.withColumn("__insr_w4_hit", F.coalesce(F.expr(rule["match"]), F.lit(False)))
        for column in targets:
            df = df.withColumn(column, F.when(~F.col("__insr_w4_hit"), F.col(column)))
        df = df.drop("__insr_w4_hit")
    return df


def _s3_gate_direct_public(df, rules):
    """Null governed target columns on flat axes while preserving provenance."""
    for axis, source_only in rules.s3b_direct_axes:
        if source_only:
            continue
        gate = F.coalesce(F.col(f"_{axis}_gate_pass"), F.lit(False))
        for suffix in ("snomed_code", "snomed_display", "omop_concept_id",
                       "target_system", "target_code", "target_display"):
            name = f"{axis}_{suffix}"
            if name in df.columns:
                df = df.withColumn(name, F.when(gate, F.col(name)))
    return df


def _select_alias(expression):
    """Split an explicitly aliased SQL projection into its output name and expression."""
    match = re.match(r"(?is)^\s*([\s\S]*?)\s+AS\s+\x60?([A-Za-z_][A-Za-z0-9_]*)\x60?\s*$", expression)
    if not match:
        raise ValueError(f"Gold SELECT expression has no explicit alias: {expression}")
    return match.group(2), match.group(1)


def _lifecycle_one_row_source(rules):
    """One row per join key of a table's internal silver metadata (body of its _qc_lifecycle_ MV).

    Per key it keeps the greatest struct of the support columns, so a whole row survives
    intact and the same input always gives the same survivor (dropDuplicates would keep an
    arbitrary row). The struct leads with the load clocks, so the most recently loaded copy
    wins; copies from the same load fall back to comparing the remaining columns in order,
    which for identity_status keeps 'unresolved' over 'resolved' (the conservative choice:
    the row leaves the research surface rather than being published on a disputed identity;
    9 such keys in prod on 2026-10-25). Identical duplicates collapse to themselves and clean
    keys are unchanged. A single-source group-by is Enzyme-eligible (EXPLAIN CREATE
    MATERIALIZED VIEW on 8_dev.incr_probe; dev updates planned GROUP_AGGREGATE).
    """
    lifecycle = rules.lifecycle
    keys = lifecycle["join_keys"]
    internal_source = spark.conf.get("journey.silver_internal_catalog_schema")
    metadata = spark.read.table(internal_source + "._" + rules.table + "_metadata")
    support = [column for column in lifecycle["support_columns"] if column in metadata.columns]
    clocks = [column for column in ("load_batch_id", "loaded_at", "source_update_timestamp") if column in support]
    ordered = clocks + [column for column in support if column not in clocks]
    chosen = F.max(F.struct(*[F.col(column) for column in ordered])).alias("__gold_lifecycle_row")
    return metadata.groupBy(*keys).agg(chosen).select(
        *keys, *[F.col("__gold_lifecycle_row." + column).alias(column) for column in support]
    )


# INSR_W3 W3.02 (C0 ruling 1, 2026-10-29) -----------------------------------------------------------------------
# Gold person scope. Silver reference_person_scope holds one row per spine person with
# in_scope = has_encounter OR has_clinical_data; gold_qc._person_scope projects it. Gold drops the rows of a spine
# person whose in_scope is false. A row with no person_id is not person-attributable and is kept; a row whose
# person_id is not a spine person is an orphan, which the fk_columns containment in _qc nulls exactly as before.
# _qc (flag + mandatory rule) and _tdx_product / the direct twins (filter) share _person_scope_join, so the
# predicate cannot diverge between the paths.
def _person_scope_join(df, person_col="person_id"):
    """Left-join the gold person scope on person_col.

    Adds _qc_scope_person_id (NULL when the person is not a spine person) and _qc_person_in_scope (FALSE only for
    a spine person whose in_scope is false; TRUE for in-scope persons, NULL person_id and orphans). The scope MV
    holds only these two columns, so a change to a person descriptor never touches the person twins.
    """
    scope = spark.read.table(_n(CORE_DATASETS["person_scope"])).select("_qc_scope_person_id", "_qc_scope_in_scope")
    person = F.expr(f"try_cast(`{person_col}` AS BIGINT)")
    return (
        df.join(scope, person == F.col("_qc_scope_person_id"), "left")
        .withColumn("_qc_person_in_scope", F.coalesce(F.col("_qc_scope_in_scope"), F.lit(True)))
        .drop("_qc_scope_in_scope")
    )


def _apply_person_scope(df, person_col="person_id"):
    """Filter form of the gold person scope, for products without a QC twin (TDX, direct twins).

    Keeps in-scope persons, rows with no person and orphan persons; a table without person_col passes unchanged.
    """
    if person_col not in df.columns:
        return df
    return (
        _person_scope_join(df, person_col)
        .where(F.col("_qc_person_in_scope"))
        .drop("_qc_scope_person_id", "_qc_person_in_scope")
    )


def _flag_person_scope_any(df, person_cols):
    """Two-person rows (spine_person_relationship): flag _qc_person_in_scope TRUE when any named person is in scope.

    Each column is joined to the same scope MV in turn. A NULL or orphan person counts as in scope (as in
    _person_scope_join), so the row leaves gold only when every named person is a spine person whose in_scope is
    false. The twin's generated mandatory rule reads the flag, so Lakeflow counts the drops.
    """
    flags = []
    for position, column in enumerate(person_cols):
        flag = f"_qc_person_in_scope_{position}"
        df = (_person_scope_join(df, column)
              .withColumnRenamed("_qc_person_in_scope", flag)
              .drop("_qc_scope_person_id"))
        flags.append(flag)
    any_in_scope = F.col(flags[0])
    for flag in flags[1:]:
        any_in_scope = any_in_scope | F.col(flag)
    return df.withColumn("_qc_person_in_scope", any_in_scope).drop(*flags)


def _assert_axis_sources_selected(rules, select_exprs):
    """SRC_LINEAGE_V1, graph-build check: the twin's select names every {axis}_source_* of every coded spec, so the
    source fields reach the public projection as plain columns (gold_tables/*: one line each, mirroring the QC CASE
    the table applies to its plain source_code), and an all-source-only spec no longer selects its old column."""
    aliases = {_select_alias(expression)[0] for expression in select_exprs}
    for old_name, axes in rules.s3_axis_specs.items():
        missing = [name for axis, _ in axes for name in _s3_axis_source_columns(axis) if name not in aliases]
        if missing:
            raise RuntimeError(f"{rules.table}.{old_name}: the QC select lacks the axis source columns {missing}")
        if not _s3_has_transport(axes) and old_name in aliases:
            raise RuntimeError(f"{rules.table}.{old_name}: an all-source-only spec has no transport; drop its select")


def _qc(rules, select_exprs, fk_columns=(), date_flags=()):
    """Apply Gold repairs while keeping lifecycle and QC fields internal.

    Public silver-v2 supplies research fields and source history. Separate
    internal metadata supplies the inputs used by unchanged Gold expectations. Date flags are
    recomputed from Gold's repaired event time. VARIANT values cross joins as
    JSON strings and are parsed only by the single-source public wrapper.
    """
    select_exprs = list(select_exprs) + [f"`{c}` AS `{c}`" for c in getattr(rules, "r4_append", ())]  # INSR_R4_GOLD_V1: silver columns gold did not publish yet
    from gold_core import gold_single_copy  # INSR_R4_GOLD_V1 (lazy import, no cycle)
    select_exprs = list(select_exprs) + [f"`{c}` AS `{c}`" for c in gold_single_copy.admission_columns(rules)]  # INSR_R4_GOLD_V1: admission verdicts (D4)
    source = spark.read.table(_src(rules.table))
    source = gold_single_copy.with_admission(source, rules)  # INSR_R4_GOLD_V1: per-key admission (D4)
    variants = set(rules.variant_columns)
    _assert_axis_sources_selected(rules, select_exprs)  # SRC_LINEAGE_V1
    if variants:
        for old_name, axes in rules.s3_axis_specs.items():
            if old_name not in source.columns and _s3_has_transport(axes):  # SRC_LINEAGE_V1: no payload, no transport
                source = source.withColumn("__gold_source_json_" + old_name, _s3_axis_payload(source, axes))
        source = source.select(
            *[
                F.to_json(F.col(column)).alias("__gold_source_json_" + column)
                if column in variants
                else F.col(column)
                for column in source.columns
            ]
        )
    lifecycle = rules.lifecycle
    if lifecycle:
        keys = lifecycle["join_keys"]
        support = [c for c in lifecycle["support_columns"] if c != "load_batch_id" or rules.one_row_per_key]  # INSR_R4_GOLD_V1: no rule reads load_batch_id; a one_row_per_key table (pathology_order) keeps its lifecycle join through its dedup MV (D5 reverted, Ben 2026-11-01)
        # Source history now comes directly from the research table. Only QC
        # and batch metadata requires the separate internal projection.
        internal_support = [column for column in support if column not in source.columns]
        if internal_support:
            internal_source = spark.conf.get("journey.silver_internal_catalog_schema")
            life_table = internal_source + "._" + rules.table + "_metadata"
            if bool(rules.one_row_per_key):  # LIFECYCLE_ONE_ROW_V1
                life_table = _n("gold_qc._lifecycle_" + rules.table)
            life = spark.read.table(life_table).select(*keys, *internal_support)
            source = source.join(life, keys, "left")
    else:
        support = []

    source_support = rules.source_support
    prepared = []
    selected_names = set()
    for expression in select_exprs:
        alias, body = _select_alias(expression)
        selected_names.add(alias)
        if alias in variants:
            prepared.append(
                f"`__gold_source_json_{alias}` "
                f"AS `__gold_json_{alias}`"
            )
        else:
            prepared.append(expression)
    for column in [*support, *source_support]:
        if column not in selected_names:
            prepared.append(f"`{column}` AS `{column}`")

    sex_column = rules.sex_plausibility
    if sex_column:
        prepared.append(f"`{sex_column}` AS `_qc_sex_concept_id`")
    df = source.selectExpr(*prepared)
    df = insr_w4_apply_value_rules(rules, df)  # INSR_W4
    if sex_column:
        # DQ4 G2: internal flag event_sex_implausible; the public wrappers select explicitly.
        sex_rules = spark.read.table(GOLD_SEX_PLAUSIBILITY_TABLE).select(
            F.col("concept_id").alias("_qc_sex_rule_concept_id"),
            F.col("plausible_gender").alias("_qc_sex_plausible"),
        )
        sexes = (
            spark.read.table(_src(CORE_DATASETS["person_spine"]))
            .select(F.col("person_id").alias("_qc_sex_person_id"), F.col("gender_display").alias("_qc_person_sex"))
            .where(F.col("_qc_sex_person_id").isNotNull() & F.col("_qc_person_sex").isin("Male", "Female"))
            .dropDuplicates(["_qc_sex_person_id"])
        )
        df = (
            df.join(F.broadcast(sex_rules), df._qc_sex_concept_id == sex_rules._qc_sex_rule_concept_id, "left")
            .join(sexes, df.person_id == sexes._qc_sex_person_id, "left")
            .withColumn(
                "event_sex_implausible",
                F.coalesce(F.col("_qc_sex_plausible") != F.col("_qc_person_sex"), F.lit(False)),
            )
            .drop("_qc_sex_concept_id", "_qc_sex_rule_concept_id", "_qc_sex_plausible", "_qc_sex_person_id", "_qc_person_sex")
        )

    if "person_id" in df.columns:
        # INSR_W3 W3.02: one join gives the scope flag (mandatory rule <table>.person_id.person_in_scope) and the
        # spine containment below; the scope MV holds exactly the silver spine persons (one row each).
        df = _person_scope_join(df)
        if "person_id" in fk_columns:
            # INSR_W3 W3.08 (L5240, F6): pre-null orphan flag, counted by <table>.person_id.orphan_nulled.
            df = df.withColumn("_qc_person_id_orphan",
                               F.col("person_id").isNotNull() & F.col("_qc_scope_person_id").isNull())
            df = df.withColumn(
                "person_id",
                F.when(F.col("_qc_scope_person_id").isNotNull(), F.col("person_id")),
            )
        df = df.drop("_qc_scope_person_id")
    if "encounter_id" in fk_columns:
        parent = spark.read.table(_n(CORE_DATASETS["encounter_keys"]))  # GOLD_MV_SPLIT_V1: MV in the internal schema
        df = (
            df.join(parent, df.encounter_id == parent._qc_encounter_id, "left")
            .withColumn("_qc_encounter_id_orphan", F.col("encounter_id").isNotNull() & F.col("_qc_encounter_id").isNull())  # INSR_W3 W3.08
            .withColumn(
                "encounter_id",
                F.when(F.col("_qc_encounter_id").isNotNull(), F.col("encounter_id")),
            )
            .drop("_qc_encounter_id")
        )
    if "episode_id" in fk_columns:
        parent = spark.read.table(_n(CORE_DATASETS["episode_keys"]))  # GOLD_MV_SPLIT_V1: MV in the internal schema
        df = (
            df.join(parent, df.episode_id == parent._qc_episode_id, "left")
            .withColumn("_qc_episode_id_orphan", F.col("episode_id").isNotNull() & F.col("_qc_episode_id").isNull())  # INSR_W3 W3.08
            .withColumn(
                "episode_id",
                F.when(F.col("_qc_episode_id").isNotNull(), F.col("episode_id")),
            )
            .drop("_qc_episode_id")
        )

    requested_date_flags = tuple(
        dict.fromkeys((*date_flags, *rules.date_flags))
    )
    requested_date_flags = insr_w4_date_flags(rules, requested_date_flags)  # INSR_W4
    if requested_date_flags:
        dates = spark.read.table(_n(CORE_DATASETS["person_dates"]))  # INSR_R4_GOLD_V1: an MV in the internal schema
        df = df.join(dates, df.person_id == dates._qc_date_person_id, "left")
        if "event_before_birth" in requested_date_flags:
            df = df.withColumn(
                "event_before_birth",
                F.coalesce(
                    F.col("event_datetime") < F.col("_qc_birth_datetime"),
                    F.lit(False),
                ),
            )
        if "event_after_death_30d" in requested_date_flags:
            df = df.withColumn(
                "event_after_death_30d",
                F.coalesce(
                    F.col("event_datetime")
                    > F.col("_qc_deceased_datetime") + F.expr("INTERVAL 30 DAYS"),
                    F.lit(False),
                ),
            )
        df = insr_w4_birth_flags(rules, df)  # INSR_W4
        df = df.drop(
            "_qc_date_person_id",
            "_qc_birth_datetime",
            "_qc_deceased_datetime",
        )

    # Keep an internal per-axis gate flag on the QC twin. The public wrapper
    # applies the same predicate to code columns and leaves provenance visible.
    for old_name, axes in rules.s3_axis_specs.items():
        transport = "__gold_json_" + old_name
        if transport not in df.columns:
            continue
        # GOLD_PARSE_ONCE_PATCH_APPLIED v1
        # Parse once into a column. An inline from_json is copied into every field reference of the
        # policy gate (495 copies per axis at 22 policies), and Enzyme then plans the join-shaped twin
        # for tens of minutes until the driver runs out of memory (2026-09-25/26 gold failures).
        parsed = "__gold_parsed_" + old_name
        df = df.withColumn(parsed, F.from_json(F.col(transport), _s3_transport_schema(axes)))  # SRC_LINEAGE_V1
        payload = F.col(parsed)
        for axis, source_only in axes:
            if not source_only:
                item = payload[axis]
                gate, policy_id = gold_single_copy.verdict(axis)  # INSR_R4_GOLD_V1
                has_target = item["snomed_code"].isNotNull() | item["omop_concept_id"].isNotNull()
                df = df.withColumn(f"_{axis}_gate_pass", F.when(has_target, gate))
                df = df.withColumn(f"_{axis}_policy_id", F.when(has_target, policy_id))
        df = df.drop(parsed)
    for axis, source_only in rules.s3b_direct_axes:
        if not source_only:
            required = _s3_axis_columns(axis, False)
            if any(name not in df.columns for name in required):
                continue
            item = {name[len(axis)+1:]: F.col(name) for name in required}
            gate, policy_id = gold_single_copy.verdict(axis)  # INSR_R4_GOLD_V1
            has_target = item["target_code"].isNotNull() | item["snomed_code"].isNotNull() | item["omop_concept_id"].isNotNull()
            df = df.withColumn(f"_{axis}_gate_pass", F.when(has_target, gate))
            df = df.withColumn(f"_{axis}_policy_id", F.when(has_target, policy_id))
    return df


def _with_parent_status(child, parent, keys):
    """Keep rejected children until the native expectation counts them; parents must be unique on keys."""
    # Each caller projects and deduplicates only its parent keys before this join.
    # A marker, rather than a nullable key, also handles composite keys without ambiguity.
    return child.join(parent.withColumn("__gold_parent_present", F.lit(True)), keys, "left")


def _with_row_key(df, rules):
    """Add the public row_key declared for table_name (W7.04 reads it as the table key)."""
    return df.withColumn("row_key", F.expr(rules.row_key_sql))


# S3D_GOLD_ADMISSION_PATCH_APPLIED v1
def _released(frame):
    """Release text when anonymisation succeeded and its input digest and context still match.

    Any redactor version is released: the version travels with the text as provenance.
    """
    columns = set(frame.columns)
    required = {
        "anon_status", "anon_source_text_sha",
        "anon_input_digest", "anon_context_fingerprint", "context_fingerprint_current",
    }
    if not required.issubset(columns):
        return F.lit(False)
    return (
        (F.col("anon_status") == "anonymized")
        & F.col("anon_source_text_sha").eqNullSafe(F.col("anon_input_digest"))
        & F.col("anon_context_fingerprint").eqNullSafe(F.col("context_fingerprint_current"))
    )


def _tdx_product(rules):
    """Prepare TDX values and fail-closed text; native decorators handle row admission."""
    df = spark.read.table(f"{SILVER_CATALOG_SCHEMA}.{rules.table}")
    df = _apply_person_scope(df)  # INSR_W3 W3.02: same predicate as _qc (C0 ruling 1)
    for column, condition in rules.tdx_value_repairs.items():
        # INSR_W3 W3.06: Gold: NULL where the TDX repair condition holds (placeholder dates, open-end markers).
        df = df.withColumn(column, F.when(~F.coalesce(F.expr(condition), F.lit(False)), F.col(column)))
    for step in rules.tdx_finish:
        df = step(df)
    if bool(rules.tdx_substitutions):
        released = _released(df)
        for original, anonymised in rules.tdx_substitutions:
            if original in df.columns and anonymised in df.columns:
                # GOLD_COLUMN_PARITY_V1: the anon_* column itself is published as well.
                df = df.withColumn(original, F.when(released, F.col(anonymised)))
    df = insr_w4_apply_value_rules(rules, df)  # INSR_W4
    return df


def _tdx_qc(rules, key_name):
    """Retain the old admitted-key evidence table without treating advisory warnings as failures."""
    frame = spark.read.table(f"{PUBLIC_SCHEMA}.{rules.table}")
    return frame.select(F.col(key_name), F.lit("passed").alias("qc_status"), F.col("loaded_at").alias("qc_checked_at"))


def insr_w4_apply_value_rules(rules, df):
    """NULL each W4 value-rule column (and its *_local companion) where the rule's match condition holds.

    The condition is materialised into a helper column before any column changes, so a condition that reads the
    column it NULLs is evaluated on the original value. A rule whose columns are absent fails the update loudly.
    """
    # SCT_TRUD_V1: the coding-axis columns _s3_flatten_gold_public restores from a JSON column do not exist yet
    # when _qc runs this; a rule on those columns only (none present) is applied by the flatten instead.
    restored = {name for axes in rules.s3_axis_specs.values()
                for axis, source_only in axes for name in _s3_axis_columns(axis, source_only)}
    for rule_id, rule in sorted(rules.value_rules.items()):
        if rule["table"] != rules.table:
            continue
        targets = [rule["column"], *rule["companions"]]
        missing = [column for column in targets if column not in df.columns]
        if missing and set(targets) <= restored:
            # SCT_TRUD_V1: every target is rebuilt by _s3_flatten_gold_public from the axis JSON, which applies this
            # rule there. Defer even when some targets already exist as flat columns (prod P4 validate 2026-11-06:
            # clinical_specimen carries specimen_type_snomed_code flat, its companions only after the flatten); the
            # flatten overwrites those columns, so applying the rule here would be undone anyway.
            continue
        if missing:
            raise ValueError(f"{rule_id}: {missing} not in {rules.table}")
        df = df.withColumn("__insr_w4_hit", F.coalesce(F.expr(rule["match"]), F.lit(False)))
        for column in targets:
            df = df.withColumn(column, F.when(~F.col("__insr_w4_hit"), F.col(column)))
        df = df.drop("__insr_w4_hit")
    return df


def insr_w4_date_flags(rules, requested):
    """Add the token that makes _qc join _gold_person_dates for tables with W4 before-birth rules."""
    if bool(rules.birth_flag_columns) and "insr_w4_birth" not in requested:
        return (*requested, "insr_w4_birth")
    return requested


def insr_w4_birth_flags(rules, df):
    """insr_w4_before_birth_<column>: the column precedes the person's birth (FALSE when either side is NULL).

    Internal twin columns read by the F5(g) warn rules; the public wrappers select explicitly, so they stay internal.
    """
    for column in rules.birth_flag_columns:
        df = df.withColumn(f"insr_w4_before_birth_{column}",
                           F.coalesce(F.col(column) < F.col("_qc_birth_datetime"), F.lit(False)))
    return df
