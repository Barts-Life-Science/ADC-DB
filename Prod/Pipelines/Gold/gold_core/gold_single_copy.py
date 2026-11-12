"""INSR R4 single-copy gold (design R4; Deviation D3; D4 and D5 reverted by Ben, 2026-11-01). Generic core: no
table names (R3 gate 1a).

R3's gold modules collect GoldDatasets (GOLD_SPECS) and R3's gold_spec.declare(dataset) registers today's shape: a
QC twin plus a public MV that reads the twin by name. R4's pipeline source (Gold/gold_pipeline_r4.py) calls
declare_all instead:
  * a twin (twin_of) is never declared: its builder and drop expectations feed its public table;
  * every public table is an MV (GOLD_MV_SPLIT_V1); the view tier and its deploy notebook are retired;
  * an mv-tier public table with a twin is one MV under the public dataset's options: the twin's rows, the twin's
    drop rules as a WHERE, then the public projection (GoldRules.public_select / public_steps, else the generated
    PUBLIC_SELECT fallback);
  * an mv-tier public table without a twin keeps its own builder, with its drop rules as a WHERE;
  * every other dataset goes through R3's gold_spec.declare unchanged.
A Lakeflow expectation cannot read the internal QC columns once the twin is gone, so no expectation is declared on
a single-copy MV; release_rule_count carries the counts (Deviation D3).
Admission (D4 reverted): declare_admission declares gold_qc._mapping_admission from R1's admission_sql; _qc calls
with_admission on the silver rows and verdict(axis) at each gate, so the policy reaches a materialised table only as
data (a policy change re-plans nothing). Datasets this module does not touch (the pathology_order lifecycle dedup
among them, D5 reverted) go through R3's gold_spec.declare unchanged.
"""
import pyspark.sql.functions as F
from pyspark import pipelines as dp

from gold_core import gold_spec

DROP = "expect_all_or_drop"


def _drop_predicates(dataset):
    """The drop predicates recorded on a dataset (warn and fail rules are counted by the release report, D3)."""
    return [p for name, rules in dataset.expectations if name == DROP for p in rules.values()]


def _passes(df, predicates):
    """Keep the rows every drop rule passes (TRUE only: NULL drops, as expect_all_or_drop did)."""
    for predicate in predicates:
        df = df.where(F.coalesce(F.expr(predicate), F.lit(False)))
    return df


def _project(df, rules, fallback, append):
    """The public projection of a twin frame: R3's public_select (else the generated fallback), then R3's
    public_steps, the rest of the public body in order (each df -> df on the projected frame, as the legacy public
    builder ran them after its select). Appended silver columns ride through the projection (they are in the twin
    via rules.r4_append) and the steps keep columns they do not name."""
    select = getattr(rules, "public_select", None)
    select = fallback if select is None else select
    if isinstance(select, dict):
        df = df.drop(*select["all_but"])
    elif isinstance(select, list):
        # R3's public_steps (flatten / gate) read the per-key verdict columns, so they ride the projection when
        # steps follow it; _drop_admission removes them after the steps.
        carried = admission_columns(rules) if getattr(rules, "public_steps", ()) else []
        df = df.selectExpr(*select, *[f"`{c}` AS `{c}`" for c in list(append) + carried])
    for step in getattr(rules, "public_steps", ()) or ():
        df = step(df)
    return df


def _declare_single_copy(public, twin, rules, fallback, append):
    """One MV under the public dataset's options: twin rows filtered by the twin's drop rules, then projected; or,
    without a twin, the public builder's rows filtered by its own drop rules."""
    def single_copy():
        """Single-copy rows (D3: drop rules as a WHERE, no expectation, no twin)."""
        if twin is None:
            return _drop_admission(_passes(public.build(), _drop_predicates(public)))
        return _drop_admission(_project(_passes(twin.build(), _drop_predicates(twin)), rules, fallback, append))
    single_copy.__name__ = public.build.__name__
    return dp.materialized_view(**public.options)(single_copy)


ADMISSION_DATASET = "gold_qc._mapping_admission"


def declare_admission(spark):
    """gold_qc._mapping_admission (Deviation D4 reverted, Ben 2026-11-01): one verdict per (lane_id, map_key,
    component_index) of mapping_resolution, the policy joined as data (R1 mapping_admission_gate.admission_sql). A
    policy change is a data change here and for the MVs that join it."""
    import mapping_admission_gate as R1G             # on sys.path through journey.r1_shared_path (R1 Task 18)
    from gold_core.gold_core import _n              # R3's internal-name resolver (R4.11 Step 1: it lives in gold_core.py)
    resolution = spark.conf.get("journey.r1_resolution_table", "`3_lookup`.omop.mapping_resolution")
    policy = spark.conf.get("journey.r1_policy_table", "`3_lookup`.omop.mapping_admission_policy")

    def mapping_admission():
        """Per-key admission verdicts (R1 admission_sql)."""
        return spark.sql(R1G.admission_sql(resolution, policy))
    return dp.materialized_view(name=_n(ADMISSION_DATASET), cluster_by=["lane_id", "map_key"])(mapping_admission)


def admission_columns(rules):
    """The verdict columns with_admission adds for a table; they ride _qc's select and leave at the projection."""
    routes = getattr(rules, "r4_admission", None) or {}
    return [c for axis in sorted(routes) for c in (f"__adm_{axis}_gate", f"__adm_{axis}_policy")]


def with_admission(df, rules):
    """Silver rows of a materialised table plus each gated axis's verdict (D4 reverted), before _qc parses anything:
      * an axis with an R1 lane: LEFT JOIN the lane's slice of gold_qc._mapping_admission on (<axis>_map_key,
        coalesce(<axis>_map_component_index, 1)); one row per key, so no row multiplies;
      * a gated axis with no lane (None): LEFT JOIN the current policy rows on <axis>_map_method (R1's view form).
    Both joins read data, never literals, so a policy change re-plans nothing."""
    import mapping_admission_gate as R1G
    from gold_core.gold_core import _n, spark       # the pipeline's session, as every R3 gold module reads it
    for axis, lane_id in sorted((getattr(rules, "r4_admission", None) or {}).items()):
        gate_col, policy_col = f"__adm_{axis}_gate", f"__adm_{axis}_policy"
        if lane_id:
            verdicts = (spark.read.table(_n(ADMISSION_DATASET)).where(F.col("lane_id") == lane_id)
                        .select(F.col("map_key").alias("__adm_key"), F.col("component_index").alias("__adm_component"),
                                F.col("gate_pass").alias(gate_col), F.col("policy_id").alias(policy_col)))
            on = ((F.col(f"{axis}_map_key") == F.col("__adm_key"))
                  & (F.coalesce(F.col(f"{axis}_map_component_index"), F.lit(1)) == F.col("__adm_component")))
            df = df.join(verdicts, on, "left").drop("__adm_key", "__adm_component")
        else:
            table = spark.conf.get("journey.r1_policy_table", "`3_lookup`.omop.mapping_admission_policy")
            sql = R1G.view_gate_sql("s", axis, table)
            policy = spark.read.table(table).where("is_current").alias(f"_pol_{axis}")
            df = (df.alias("s").join(policy, F.col(f"s.{axis}_map_method") == F.col(f"_pol_{axis}.map_method"), "left")
                  .select("s.*", F.expr(sql["gate"]).alias(gate_col), F.expr(sql["policy_id"]).alias(policy_col)))
    return df


def verdict(axis):
    """(gate, policy_id) of one axis from with_admission's columns, in place of the literal gate. A row with no
    verdict row is not admitted ('withheld:no_matching_policy'), as the literal gate treats a row with no method;
    every key has a verdict row (decision 19), so only unkeyed rows (pending_resolution) get there."""
    return (F.coalesce(F.col(f"__adm_{axis}_gate"), F.lit(False)),
            F.coalesce(F.col(f"__adm_{axis}_policy"), F.lit("withheld:no_matching_policy")))


def _drop_admission(df):
    """Drop the verdict columns before publication (gold publishes no internal column)."""
    return df.drop(*[c for c in df.columns if c.startswith("__adm_")])


def _append_to_direct_selects(module, append):
    """Appended silver columns for a twin that selects directly (`<TABLE>_SELECT` passed to selectExpr, no _qc call
    and so no RULES entry, e.g. registry_mortality_review): extend that module list in place, before any builder runs,
    with the columns it does not already name. A table with a RULES entry gets them through _qc (r4_append)."""
    rules = getattr(module, "RULES", {})
    for table, columns in append.items():
        select = getattr(module, f"{table.upper()}_SELECT", None)
        if table in rules or not isinstance(select, list):
            continue
        named = {str(expr).rsplit(" AS ", 1)[-1].strip().strip("`") for expr in select}
        select.extend(f"`{c}` AS `{c}`" for c in columns if c not in named)


def declare_all(modules, tiers, twin_of, append, public_fallback, admission=None):
    """Declare the reduced gold pipeline from R3's modules (see module docstring).

    tiers {public table: "view" | "mv"}, twin_of {twin leaf: public table}, append {table: [silver columns]},
    public_fallback {table: projection}, admission {MV-tier table: {gated axis: lane_id | None}} - all generated
    into Gold/views/gold_tiers.py."""
    datasets = {d.name: (d, m) for m in modules for d in getattr(m, "GOLD_SPECS", ())}
    twins = {public: datasets[twin][0] for twin, public in twin_of.items() if twin in datasets}
    for m in modules:                       # columns gold did not publish yet ride in the twin's _qc select
        for table, rules in getattr(m, "RULES", {}).items():   # helper modules (person dates) have no RULES
            rules.r4_append = tuple(append.get(table, ()))
            rules.r4_admission = dict((admission or {}).get(table, {}))      # D4 reverted: with_admission's routes
        _append_to_direct_selects(m, append)
    for name, (dataset, module) in datasets.items():
        if name in twin_of:                  # GOLD_MV_SPLIT_V1: no view tier
            continue
        if name not in tiers:
            gold_spec.declare(dataset)
            continue
        _declare_single_copy(dataset, twins.get(name), getattr(module, "RULES", {}).get(name), public_fallback.get(name),
                             tuple(append.get(name, ())))
