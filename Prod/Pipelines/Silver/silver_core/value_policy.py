"""Silver core: value_policy.

- cut from silver_value_policy.py (pre-R3 text, 1.0.0)
- names no dataset, model or gold table (gate 1)

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""




from collections import namedtuple


POLICY_VERSION = "INSR_W2_VALUE_POLICY_V1"


def _F():
    """pyspark.sql.functions, imported on first use (keeps the registries importable without Spark)."""
    from pyspark.sql import functions as F
    return F


# ---------------------------------------------------------------------------------------------------------------
# §1 Millennium zero references and the system user (W2.01; ledger P4a/P4d/P4e/P4f)
# ---------------------------------------------------------------------------------------------------------------

# Zero carries a meaning in these columns (P4d, DOCUMENT): never NULLed.
MILL_ZERO_KEEP = frozenset({"orig_action_id", "sch_reason_cd", "recurrence_parent_sch_event_id", "period_oid"})


def _is_zero(column):
    """True when a column holds a numeric zero in any type (0, 0.0, '0', '0.0'); NULL and text give False.

    The text test decides, not a cast: a DOUBLE id 0.0 renders as '0.0' (P_W112 lesson: an equality test with '0'
    misses it), and a non-numeric code is never a zero.
    """
    F = _F()
    text = F.trim(column.cast("string"))
    return F.coalesce(text.rlike(r"^[+-]?0+(\.0+)?$"), F.lit(False))


def zero_to_null(column):
    """Millennium uses 0 as 'no reference': return NULL for a numeric zero, the value (same type) otherwise."""
    F = _F()
    return F.when(~_is_zero(column), column)


def try_bigint(name):
    """Column `name` as BIGINT, NULL when its text is not a number.

    try_cast through F.expr (portable below Spark 3.5): under ANSI a plain .cast("bigint") raises on text such
    as 'UNKNOWN' and would fail the whole MV refresh.
    """
    return _F().expr(f"try_cast(`{name}` AS BIGINT)")


def mill_fk(name):
    """Millennium id column `name` as BIGINT with 0 -> NULL (patient-flow ids, text_document encounter_id)."""
    F = _F()
    as_long = try_bigint(name)
    return F.when(as_long != 0, as_long)


def apply_mill_zero_policy(df, policy):
    """Flag the system user, NULL zero references/values, drop the hub id-0 row. Missing columns fail loudly."""
    F = _F()
    listed = list(policy.zero_as_null) + list(policy.zero_value_as_null)
    missing = [c for c in listed if c not in df.columns]
    if missing:
        raise AssertionError(f"INSR_W2 value policy: {policy.table} lacks listed columns {missing}")
    for column, role in policy.system_user_columns.items():
        source = F.col(column)
        df = df.withColumn(f"{role}_system_ind",
                           F.when(source.isNotNull(), F.coalesce(try_bigint(column) == 1, F.lit(False))))
        df = df.withColumn(column, F.when(~F.coalesce(try_bigint(column).isin(0, 1), F.lit(False)), source))
    for column in listed:
        df = df.withColumn(column, zero_to_null(F.col(column)))
    key = policy.hub_zero_key
    if key:
        df = df.where(F.col(key).isNull() | ~_is_zero(F.col(key)))
    return df


# ---------------------------------------------------------------------------------------------------------------
# §2 Derived columns (additive; inputs must already be published by the table)
# ---------------------------------------------------------------------------------------------------------------

# name: output column; inputs: columns it reads; build: callable(F) -> Column; comment: UC column comment.
Derived = namedtuple("Derived", "name inputs build comment")


def apply_derived(df, policy):
    """Add the table's derived columns in registry order. A missing input fails the graph build."""
    F = _F()
    for item in policy.derived:
        missing = [c for c in item.inputs if c not in df.columns]
        if missing:
            raise AssertionError(f"INSR_W2 derived {policy.table}.{item.name} lacks inputs {missing}")
        df = df.withColumn(item.name, item.build(F))
    return df


def finish_public(df, policy):
    """§2 derived columns (read source values), then §4 value rules, then the §1 zero/system-user policy."""
    if not (policy is not None and policy.applies()):
        return df
    return apply_mill_zero_policy(apply_value_rules(apply_derived(df, policy), policy), policy)


def public_columns_for(extra_public_columns, public_columns, available):
    """The model's public column list plus its extra columns that the rows actually carry."""
    extra = [c for c in extra_public_columns if c in available and c not in public_columns]
    return list(public_columns) + extra


def _zero_measure_rule(value, unit):
    """J24/F8: a measure of 0 with no unit (NULL, blank or the zero code) records nothing: NULL it."""
    def build(F):
        unit_blank = F.col(unit).isNull() | F.trim(F.col(unit).cast("string")).isin("", "0", "0.0")
        return F.when(~((F.col(value) == 0) & unit_blank), F.col(value))
    return build


# ---------------------------------------------------------------------------------------------------------------
# §3 Time-only values (W2.07, J4): Millennium stores a time of day with the placeholder date 2000-01-01.
# ---------------------------------------------------------------------------------------------------------------
# Placed after §4's value_rule because its rules use both register (§2) and value_rule (§4). The helpers are also
# imported by builders (silver_journey_shared date_event arm, 6_silver_forms response split).
TIME_ONLY_DATE = "2000-01-01"


def time_only(column):
    """True when a timestamp sits on the 2000-01-01 placeholder date (P3d: 99.9% non-midnight, x38,000 neighbours)."""
    F = _F()
    return F.coalesce(F.to_date(column) == F.lit(TIME_ONLY_DATE).cast("date"), F.lit(False))


def drop_time_only(column):
    """The timestamp, or NULL when it is a time-only placeholder."""
    F = _F()
    return F.when(~time_only(column), column)


def time_part(column):
    """'HH:mm:ss' for a time-only placeholder, else NULL."""
    F = _F()
    return F.when(time_only(column), F.date_format(column, "HH:mm:ss"))


def apply_value_rules(df, policy):
    """Apply the table's listed value rules in order. A missing input fails the graph build."""
    F = _F()
    for column, inputs, build in policy.value_rules:
        missing = [c for c in inputs if c not in df.columns]
        if missing:
            raise AssertionError(f"INSR_W2 value rule {policy.table}.{column} lacks inputs {missing}")
        df = df.withColumn(column, build(F))
    return df
