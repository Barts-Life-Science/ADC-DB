"""Gold core: per-table rules (GoldRules), one Lakeflow dataset (GoldDataset), the collector and the bindings.

A gold table module (gold_tables/<folder>/<table>.py) reads, top to bottom:

    GOLD_SPECS = []
    dp = DatasetCollector(GOLD_SPECS)   # @dp.materialized_view / @dp.expect_all... are collected, not registered
    activate(dp)
    ... the moved constants (<TABLE>_SELECT, _MANDATORY_RULES, ...) and builders, verbatim ...
    RULES = {table: GoldRules(...)}     # the per-table data the shared gold code used to look up by name
    _qc = QcBinding(RULES)._qc          # and the other legacy entry points (GOLD_BOUND)
    finalise()
    COMMENTS = {...}                    # data for the P8 metadata step and R4's view DDL; no builder reads it

gold_core.declare(dataset) registers today's shape (twin + public MV, each with its expectations); R4 replaces the
declaration for the tables that become views or single-copy MVs and renders from RULES. This file names no table.
"""
from dataclasses import dataclass, field
from typing import Callable

from gold_core import gold_core as core

COMMENT_OPTIONS = ("comment", "column_comments")


@dataclass
class GoldRules:
    """Everything gold applies to one published table (legacy notebook constants + shared-dict slices)."""
    table: str
    select: list = None
    mandatory: dict = None
    advisory: dict = None
    person_scope: dict = None
    fail: dict = None
    lifecycle: dict = None
    date_flags: tuple = ()
    source_support: tuple = ()
    sex_plausibility: str = None
    variant_columns: tuple = ()
    s3_axis_specs: dict = field(default_factory=dict)
    s3b_direct_axes: tuple = ()
    one_row_per_key: bool = False
    row_key_sql: str = None
    row_key_columns: str = None
    tdx_value_repairs: dict = field(default_factory=dict)
    tdx_substitutions: list = field(default_factory=list)
    tdx_finish: tuple = ()
    value_rules: dict = field(default_factory=dict)
    birth_flag_columns: tuple = ()
    parent: str = None
    # R4 amendment (asked at R4.11 Step 1): the public builder of a table whose public body is more than a projection
    # of its twin, as data + functions. public_select is the projection applied to the twin frame first (a list of
    # selectExpr strings, or {"all_but": [columns to drop]}); public_steps are the rest of the public body, each
    # df -> df, applied after it in order. None / () when the public body is a pure projection (R4 then uses its own
    # PUBLIC_SELECT fallback).
    public_select: object = None
    public_steps: tuple = ()


@dataclass
class GoldDataset:
    """One gold Lakeflow dataset: leaf, kind, options (no comments), builder, expectations in application order."""
    name: str
    kind: str
    options: dict
    build: Callable
    expectations: list = field(default_factory=list)


class DatasetCollector:
    """Stands in for `dp` inside a gold module: records datasets and their expectations."""

    def __init__(self, specs):
        self.specs = specs

    def _declare(self, kind, options):
        """Decorator that records one dataset (expectations already sit on the function, bottom-up)."""
        refused = [k for k in COMMENT_OPTIONS if k in options]
        if refused:
            raise TypeError(f"comments are data (the module's COMMENTS), not declaration options: {refused}")

        def collect(build):
            """Record the builder with the expectations stacked below the declaration."""
            self.specs.append(GoldDataset(name=str(options["name"]).split(".", 1)[-1], kind=kind,
                                          options=dict(options), build=build,
                                          expectations=list(build.__dict__.pop("_gold_expect", []))))
            return build
        return collect

    def materialized_view(self, **options):
        """Legacy @dp.materialized_view(...)."""
        return self._declare("materialized_view", options)

    def temporary_view(self, **options):
        """Legacy @dp.temporary_view(...)."""
        return self._declare("temporary_view", options)

    def _expect(self, name, rules):
        """Legacy @dp.<expectation>(rules): recorded on the function in application order."""
        def mark(fn):
            """Append (name, rules)."""
            fn.__dict__.setdefault("_gold_expect", []).append((name, rules))
            return fn
        return mark

    def expect_all(self, rules):
        """Warn expectations."""
        return self._expect("expect_all", rules)

    def expect_all_or_drop(self, rules):
        """Drop expectations."""
        return self._expect("expect_all_or_drop", rules)

    def expect_or_fail(self, name, condition):
        """Fail expectation (single rule)."""
        return self._expect("expect_or_fail", (name, condition))


def declare(dataset):
    """Register one gold dataset with its expectations re-applied in their original order."""
    from pyspark import pipelines as dp
    build = dataset.build
    for decorator, rules in dataset.expectations:
        # expect_or_fail takes (name, condition); expect_all / expect_all_or_drop take one {name: condition} dict.
        if decorator == "expect_or_fail":
            build = getattr(dp, decorator)(*rules)(build)
        else:
            build = getattr(dp, decorator)(rules)(build)
    return getattr(dp, dataset.kind)(**dataset.options)(build)


class QcBinding:
    """The legacy name-taking gold entry points, bound to one module's {table: GoldRules}."""

    def __init__(self, rules):
        self.rules = rules

    def _r(self, table):
        """The module's GoldRules for a table; another module's table is a hard error."""
        try:
            return self.rules[table]
        except KeyError:
            raise KeyError(f"table {table!r} has no GoldRules in this module") from None

    def _qc(self, table_name, select_exprs, fk_columns=(), date_flags=()):
        """Legacy _qc(table_name, select_exprs, fk_columns, date_flags)."""
        return core._qc(self._r(table_name), select_exprs, fk_columns, date_flags)

    def _tdx_product(self, target_name):
        """Legacy _tdx_product(target_name)."""
        return core._tdx_product(self._r(target_name))

    def _tdx_qc(self, target_name, key_name):
        """Legacy _tdx_qc(target_name, key_name)."""
        return core._tdx_qc(self._r(target_name), key_name)

    def _with_row_key(self, df, table_name):
        """Legacy _with_row_key(df, table_name)."""
        return core._with_row_key(df, self._r(table_name))

    def _s3_flatten_gold_public(self, df, table_name):
        """Legacy _s3_flatten_gold_public(df, table_name)."""
        return core._s3_flatten_gold_public(df, self._r(table_name))

    def _s3_gate_direct_public(self, df, table_name):
        """Legacy _s3_gate_direct_public(df, table_name)."""
        return core._s3_gate_direct_public(df, self._r(table_name))

    def _lifecycle_one_row_source(self, table_name):
        """Legacy _lifecycle_one_row_source(table_name)."""
        return core._lifecycle_one_row_source(self._r(table_name))

    def insr_w4_apply_value_rules(self, table_name, df):
        """Legacy insr_w4_apply_value_rules(table_name, df)."""
        return core.insr_w4_apply_value_rules(self._r(table_name), df)

    def insr_w4_date_flags(self, table_name, requested):
        """Legacy insr_w4_date_flags(table_name, requested)."""
        return core.insr_w4_date_flags(self._r(table_name), requested)

    def insr_w4_birth_flags(self, table_name, df):
        """Legacy insr_w4_birth_flags(table_name, df)."""
        return core.insr_w4_birth_flags(self._r(table_name), df)


class _Active:
    """The collector of the gold module being imported (for builders defined in gold_tables/_common)."""
    current = None

    def __getattr__(self, name):
        """Delegate decorator calls to the active module's collector."""
        if self.current is None:
            raise RuntimeError("a gold dataset was declared outside a gold table module import")
        return getattr(self.current, name)


ACTIVE = _Active()


def activate(collector):
    """Make a module's collector the target of ACTIVE."""
    ACTIVE.current = collector


def finalise():
    """Close the module's collection."""
    ACTIVE.current = None
