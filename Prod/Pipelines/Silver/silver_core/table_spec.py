"""Silver core: what a table module declares, and the bindings that let moved builder code keep its legacy calls.

A table module (silver_tables/<domain>/<table>.py) reads, top to bottom:

    SPECS = []                                    # filled by the collector while the module is imported
    COLLECT = TableCollector(SPECS)
    activate(COLLECT)                             # factories in _shared/_common modules declare into this module
    materialized_view = COLLECT.materialized_view # the legacy decorator call shape; comment options are refused
    dp = COLLECT                                  # code that called dp.materialized_view directly keeps working
    ... the moved helpers and builders, verbatim ...
    CROSS_QC = {model: CrossQc(...)}              # modules whose builders call the cross-QC engine
    _cross_qc_public = CrossQcBinding(CROSS_QC).public   (and primitive / prepare / finish)
    VALUE_POLICIES = {leaf: ValuePolicy(...)}     # modules with value-policy tables, then their registrations
    finish_public = ValuePolicyBinding(VALUE_POLICIES).finish_public
    WRAP_POLICY = {leaf, ...}                     # datasets the legacy wrapper ran finish_public around
    finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)
    COMMENTS = {...}                              # data for the P8 metadata step; nothing in the pipeline reads it

R2's domain file declares each spec with silver_core.journey_core.declare(spec).
This file names no table and no model (gate 1); it is part of the core lock (gate 2).
"""
from dataclasses import dataclass, field
from typing import Callable

from silver_core import cross_qc, value_policy

COMMENT_OPTIONS = ("comment", "column_comments")


@dataclass
class TableSpec:
    """One Lakeflow dataset: its leaf name, its declaration options (no comments), its builder and its policy.

    wrap_policy is True only for datasets the legacy shared materialized_view wrapper ran finish_public around.
    A builder that already calls finish_public itself (notebook 7's canonical) has wrap_policy False, so the policy
    is never applied twice; its table's ValuePolicy still serves that call through ValuePolicyBinding.
    """
    name: str
    options: dict
    build: Callable
    kind: str = "materialized_view"
    value_policy: object = None
    wrap_policy: bool = False


class TableCollector:
    """Stands in for the legacy materialized_view / dp: records a TableSpec instead of registering."""

    def __init__(self, specs):
        self.specs = specs

    def _collect(self, kind, options):
        """Decorator that appends one spec and returns the builder unchanged."""
        refused = [k for k in COMMENT_OPTIONS if k in options]
        if refused:
            raise TypeError(f"comments are data (the module's COMMENTS), not declaration options: {refused}")

        def collect(build):
            """Record the builder."""
            self.specs.append(TableSpec(name=str(options["name"]).split(".", 1)[-1], options=dict(options),
                                        build=build, kind=kind))
            return build
        return collect

    def materialized_view(self, **options):
        """The legacy decorator call shape."""
        return self._collect("materialized_view", options)

    def temporary_view(self, **options):
        """Kept for symmetry with gold; silver declares none today."""
        return self._collect("temporary_view", options)


def declare(spec):
    """Register one dataset with Lakeflow: the builder, wrapped by its table's value policy exactly where the legacy
    shared materialized_view wrapped it (spec.wrap_policy; that wrapper ran finish_public, then attached comments)."""
    from silver_core.journey_core import dp
    build = spec.build
    policy = spec.value_policy
    if spec.wrap_policy:
        def build_with_policy():
            """The builder's rows with the table's value policy applied."""
            return value_policy.finish_public(spec.build(), policy)
        build_with_policy.__name__ = spec.build.__name__
        build = build_with_policy
    return getattr(dp, spec.kind)(**spec.options)(build)


# SRC_LINEAGE_V1: table modules declare source-input overrides with this type (defined in cross_qc, which reads it).
SourceInputs = cross_qc.SourceInputs


@dataclass
class CrossQc:
    """Per-model data the cross-QC engine used to look up by model name.

    fk, dates          - SILVER_CROSS_QC[model]["fk" / "dates"]
    coded_axes         - S3_PUBLIC_CODED_AXES.get(model, {})
    shared_quantities  - S3B_SHARED_QUANTITIES.get(model, ())
    extra_public_columns - silver_value_policy.EXTRA_PUBLIC_COLUMNS.get(model, ())
    finish_steps       - the model's former per-model `if` body in _cross_qc_finish, as functions df -> df
    prepare_steps      - the model's former branch of _cross_qc_prepare's per-model if/elif direct-axis
                         chain, as functions df -> df (run where the chain ran, before the lane expansion)
    embedding_lanes    - the model's branches of _s3_flatten_codeable_json's embedding_lane CASE, in CASE order:
                         ((axis, method rlike pattern or None, lane name), ...)
    promote_lifecycle, parent_inputs - R4 hooks (deviation D9); R3's engine does not read them
    source_inputs      - SRC_LINEAGE_V1: {coded public column: SourceInputs} overrides of DEFAULT_SOURCE_INPUTS
    """
    model: str
    fk: tuple = ()
    dates: tuple = ()
    coded_axes: dict = field(default_factory=dict)
    shared_quantities: tuple = ()
    extra_public_columns: tuple = ()
    finish_steps: tuple = ()
    prepare_steps: tuple = ()
    embedding_lanes: tuple = ()
    promote_lifecycle: tuple = ()
    parent_inputs: dict = field(default_factory=dict)
    source_inputs: dict = field(default_factory=dict)


class CrossQcBinding:
    """The cross-QC entry points bound to one module's {model: CrossQc}, so builders keep passing the model name."""

    def __init__(self, registry):
        self.registry = registry

    def _qc(self, model_name):
        """The module's CrossQc for a model; a model the module does not own is a hard error."""
        try:
            return self.registry[model_name]
        except KeyError:
            raise KeyError(f"model {model_name!r} has no CrossQc in this module") from None

    def primitive(self, df, model_name, variant_json_columns):
        """Legacy _cross_qc_primitive(df, model_name, variant_json_columns)."""
        return cross_qc._cross_qc_primitive(df, self._qc(model_name), variant_json_columns)

    def prepare(self, df, model_name, *args):
        """Legacy _cross_qc_prepare(df, model_name, ...)."""
        return cross_qc._cross_qc_prepare(df, self._qc(model_name), *args)

    def finish(self, df, model_name, *args):
        """Legacy _cross_qc_finish(df, model_name, ...)."""
        return cross_qc._cross_qc_finish(df, self._qc(model_name), *args)

    def public(self, df, model_name, *args):
        """Legacy _cross_qc_public(df, model_name, ...)."""
        return cross_qc._cross_qc_public(df, self._qc(model_name), *args)


@dataclass
class ValuePolicy:
    """One public table's value policy (INSR_W2_VALUE_POLICY_V1 registrations, split per table).

    enabled is the legacy applies_to(table) result, recorded explicitly: a table listed only for comments ran a
    no-op finish_public; a REBUILD_ONLY table never ran it. Set enabled=True when you add a rule to a new table.
    """
    table: str
    enabled: bool = False
    zero_as_null: tuple = ()
    zero_value_as_null: tuple = ()
    system_user_columns: dict = field(default_factory=dict)
    hub_zero_key: str = None
    rebuild_only: bool = False
    derived: list = field(default_factory=list)
    value_rules: list = field(default_factory=list)

    def register(self, name, inputs, build, comment=None):
        """A derived column (legacy register(table, name, inputs, build, comment)); comment is kept as data only."""
        self.derived.append(value_policy.Derived(name, tuple(inputs), build, comment))

    def value_rule(self, column, inputs, build):
        """A listed change to an existing column (legacy value_rule(table, column, inputs, build))."""
        self.value_rules.append((column, tuple(inputs), build))

    def applies(self):
        """Whether finish_public runs for this table (legacy applies_to)."""
        return self.enabled and not self.rebuild_only


class ValuePolicyBinding:
    """Policy entry points bound to one module's {leaf: ValuePolicy}, for builders that pass the table name."""

    def __init__(self, registry):
        self.registry = registry

    def finish_public(self, df, table):
        """Legacy finish_public(df, table); an unlisted table is returned unchanged."""
        return value_policy.finish_public(df, self.registry.get(table))

    def apply_mill_zero_policy(self, df, table):
        """Legacy apply_mill_zero_policy(df, table)."""
        return value_policy.apply_mill_zero_policy(df, self.registry[table])

    def apply_derived(self, df, table):
        """Legacy apply_derived(df, table)."""
        return value_policy.apply_derived(df, self.registry[table])

    def apply_value_rules(self, df, table):
        """Legacy apply_value_rules(df, table)."""
        return value_policy.apply_value_rules(df, self.registry[table])


class _ActiveCollector:
    """The collector of the table module being imported. Factories that live in a _shared/_common module call
    ACTIVE.materialized_view, so their datasets land in the SPECS of the module that ran the factory."""
    current = None

    def materialized_view(self, **options):
        """Delegate to the active table module's collector."""
        if self.current is None:
            raise RuntimeError("a dataset was declared outside a table module import")
        return self.current.materialized_view(**options)


ACTIVE = _ActiveCollector()


def activate(collector):
    """Make `collector` the target of ACTIVE (first statement after a table module's imports)."""
    ACTIVE.current = collector


def finalise(specs, value_policies, wrap_policy=frozenset()):
    """Attach each table's policy to its spec, mark the specs the legacy wrapper wrapped (the emitter writes
    WRAP_POLICY from the capture), and close the module's collection (last code statement)."""
    for spec in specs:
        spec.value_policy = value_policies.get(spec.name)
        spec.wrap_policy = spec.name in wrap_policy
        if spec.wrap_policy and not (spec.value_policy is not None and spec.value_policy.applies()):
            raise ValueError(f"{spec.name}: wrapped by the value policy but has no applying ValuePolicy")
    ACTIVE.current = None
