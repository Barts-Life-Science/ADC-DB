"""INSR R4 silver helpers (design R4, decisions 1, 2 and 14). Generic: no table names (R3 gate 1a).

  parent_inputs_spec(pi)                one reduction MV per parent: one row per parent key with the inputs of the
                                        parent's gold drop rules (from the parent row that best passes them)
  with_parent_inputs(specs, child, pi)  a child's builder gains parent_<input> + parent_row_count (left join)
  join_parent_inputs(df, hook)          the same join for a cross-QC model whose CrossQc.parent_inputs is set
  with_cluster_by(specs, leaf, cols)    liquid clustering for one table (options["cluster_by"])
  with_columns_last(specs, leaf, cols)  a table's builder output with the named columns moved to the end (decision 1:
                                        contract columns a module stops dropping are published after the others)

The ParentInputs values are generated per table from the parent's GoldSpec (r4/release/gen_silver_components.py)
and appended to the parent and child modules; the logic here never names a table.

Configured names: R3's journey_core._n only takes the logical "journey_<plane>.<table>" form, so the generated
ParentInputs carry the already-configured two-part names (parent_name, name), evaluated by the module's own _n(...).
"""
import dataclasses

import pyspark.sql.functions as F

from silver_core.journey_core import spark   # R3 core: the session
from silver_core.table_spec import TableSpec

# R3 TableSpec fields (r4/release/r3_contract.py; test_contract_literals_agree keeps them equal).
TABLESPEC_FIELDS = ("name", "options", "build", "kind", "value_policy", "wrap_policy")
PARENT_ROW_COUNT = "parent_row_count"


@dataclasses.dataclass(frozen=True)
class ParentInputs:
    """What one parent's children carry: the reduction dataset, its keys, the inputs and the parent's drop rules."""
    parent: str          # parent leaf, e.g. clinical_medication_order
    dataset: str         # reduction leaf, e.g. _clinical_medication_order_parent_inputs
    keys: tuple          # parent key columns the children join on
    inputs: tuple        # parent columns the gold drop rules and the person scope read
    admit_sql: str       # the parent's own gold drop rules ANDed (row-local SQL over the parent row)
    name: str = ""       # the reduction's configured two-part name (the parent module's _n(...) form)
    parent_name: str = ""  # the parent's configured two-part name (the module's _n(...) form)


def _replace(spec, **changes):
    """dataclasses.replace that also keeps attributes set outside the dataclass fields (none today; kept so a later
    finalise() attribute is not lost when a spec is wrapped)."""
    out = dataclasses.replace(spec, **changes)
    fields = {f.name for f in dataclasses.fields(spec)}
    out.__dict__.update({k: v for k, v in spec.__dict__.items() if k not in fields})
    return out


# ---- the reduction MV (one per parent) ----

def _parent_inputs_frame(pi):
    """One row per parent key. The parent row kept is the greatest of (passes the drop rules, has a person, inputs):
    a row gold would publish wins over one it would drop, a row with a person over one without, and the struct
    order makes the choice deterministic (no window, so Enzyme plans GROUP_AGGREGATE)."""
    parent = spark.read.table(pi.parent_name)
    has_person = F.col("person_id").isNotNull() if "person_id" in pi.inputs else F.lit(True)
    best = F.max(F.struct(F.coalesce(F.expr(pi.admit_sql), F.lit(False)).alias("_r4_admit"),
                          has_person.alias("_r4_has_person"),
                          *[F.col(c) for c in pi.inputs])).alias("_r4_best")
    keyed = parent.where(" AND ".join(f"`{k}` IS NOT NULL" for k in pi.keys))
    return (keyed.groupBy(*pi.keys).agg(best, F.count(F.lit(1)).alias(PARENT_ROW_COUNT))
            .select(*pi.keys, *[F.col(f"_r4_best.{c}").alias(f"parent_{c}") for c in pi.inputs], PARENT_ROW_COUNT))


def parent_inputs_spec(pi):
    """The reduction MV as one R3 TableSpec (internal schema through pi.name; no comment option: R3 gate 5)."""
    def build():
        """Reduction rows of one parent (see _parent_inputs_frame)."""
        return _parent_inputs_frame(pi)
    build.__name__ = pi.dataset
    return TableSpec(name=pi.dataset, options={"name": pi.name, "refresh_policy": "auto"}, build=build)


# ---- the child join (one per child) ----

def _join(df, reduction_name, inputs, keys):
    """Left-join a reduction on the parent keys; a child with no parent row gets parent_row_count 0."""
    reduction = spark.read.table(reduction_name)
    added = [f"parent_{c}" for c in inputs]
    joined = df.join(reduction, list(keys), "left")
    return joined.select(*df.columns, *added, F.coalesce(F.col(PARENT_ROW_COUNT), F.lit(0)).alias(PARENT_ROW_COUNT))


def with_parent_inputs(specs, child, pi):
    """The child's spec with its builder wrapped: the builder's rows plus the parent columns, at the end."""
    out = []
    for spec in specs:
        if spec.name != child:
            out.append(spec)
            continue

        def build(_inner=spec.build):
            """The child's own rows joined to its parent's reduction (decision 2)."""
            return _join(_inner(), pi.name, pi.inputs, pi.keys)
        build.__name__ = spec.build.__name__
        out.append(_replace(spec, build=build))
    return out


def join_parent_inputs(df, hook):
    """CrossQc.parent_inputs hook (R3 D9) for a cross-QC model: the same join, from the hook dict
    ({"name": configured reduction name, "inputs": [...], "keys": [...]})."""
    return _join(df, hook["name"], hook["inputs"], hook["keys"])


# ---- contract columns a module stops dropping ----

def with_columns_last(specs, leaf, columns):
    """The leaf's spec with its builder wrapped so the named columns come last, in the given order."""
    out = []
    for spec in specs:
        if spec.name != leaf:
            out.append(spec)
            continue

        def build(_inner=spec.build):
            """The table's own rows, contract columns last."""
            df = _inner()
            return df.select(*[c for c in df.columns if c not in columns], *columns)
        build.__name__ = spec.build.__name__
        out.append(_replace(spec, build=build))
    return out


# ---- clustering ----

def with_cluster_by(specs, leaf, columns):
    """Liquid clustering for one spec, merged into its declaration options."""
    return [_replace(s, options={**s.options, "cluster_by": list(columns)}) if s.name == leaf else s for s in specs]
