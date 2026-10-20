"""Shared schema and small Spark helpers for the RBI resource-use dataset.

Every stream builder ends with `to_stream`, so all streams union cleanly into the published
fact. The helpers are tiny and side-effect free; nb_test_rbi_schema tests each one.
Timestamps in gold are UTC instants; `london` gives Europe/London wall-clock time, and every
calendar-day split (bed-days, person-month, service-day) uses London dates. The pipeline and
tests set spark.sql.session.timeZone=UTC, so a London wall-clock timestamp and a date cast
to timestamp compare on the same footing.
"""
from pyspark.sql import functions as F

import rbi_rules as R

LONDON = "Europe/London"

# Published column order and types for every stream row. Costing adds the cost columns.
STREAM_COLUMNS = [
    ("resource_use_event_key", "string"),
    ("domain", "string"),
    ("stream", "string"),
    ("unit_type", "string"),
    ("quantity", "double"),
    ("quantity_requested", "double"),
    ("quantity_performed_result", "double"),
    ("quantity_performed_reported", "double"),
    ("quantity_unknown", "double"),
    ("quantity_scheduled", "double"),
    ("consumption_status", "string"),
    ("consumption_evidence", "string"),
    ("event_start", "timestamp"),
    ("event_end", "timestamp"),
    ("event_date_local", "date"),
    ("financial_year", "string"),
    ("person_id", "bigint"),
    ("encounter_id", "bigint"),
    ("attribution_method", "string"),
    ("attribution_score", "double"),
    ("candidate_count", "int"),
    ("nurse_unit_code", "string"),
    ("site_code", "string"),
    ("service_code", "string"),
    ("clinic_code", "string"),
    ("theatre_code", "string"),
    ("tracking_class", "string"),
    ("stratum_l1", "string"),
    ("stratum_l2", "string"),
    ("priced_by_other_stream", "boolean"),
    ("drug_concept_id", "bigint"),
    ("source_amount", "double"),
    ("source_table", "string"),
    ("source_key", "string"),
    ("rule_version", "string"),
]


def to_stream(df):
    """Project a stream DataFrame onto STREAM_COLUMNS; absent columns become typed NULLs."""
    present = set(df.columns)
    return df.select([
        (F.col(name) if name in present else F.lit(None)).cast(dtype).alias(name)
        for name, dtype in STREAM_COLUMNS
    ])


def london(col):
    """UTC instant → Europe/London wall-clock timestamp."""
    return F.from_utc_timestamp(col, LONDON)


def financial_year(local_date):
    """NHS financial-year label ('2023/24') for a London-local date column; April starts the year."""
    start = F.when(F.month(local_date) >= 4, F.year(local_date)).otherwise(F.year(local_date) - 1)
    return F.concat(start.cast("string"), F.lit("/"), F.lpad(((start + 1) % 100).cast("string"), 2, "0"))


def event_key(*cols):
    """Deterministic row key: SHA-256 over each column's string form, NULL-safe and order-sensitive."""
    return F.sha2(F.concat_ws("|", *[F.coalesce(c.cast("string"), F.lit("<null>")) for c in cols]), 256)


def status_from_map(col, mapping):
    """Map a source status to consumed / not_consumed via a rule dict; anything else is unknown.

    Built as a CASE chain (not a map lookup) so ANSI mode never raises on unlisted keys.
    """
    expr = None
    for value, status in mapping.items():
        cond = col == F.lit(value)
        expr = F.when(cond, F.lit(status)) if expr is None else expr.when(cond, F.lit(status))
    return F.lit("unknown") if expr is None else expr.otherwise(F.lit("unknown"))


def first_match_class(col, rules, default="unclassified"):
    """Ordered regex classification: the first matching (rule_id, label, pattern) wins.

    Returns (label column, rule_id column). NULL input → (default, NULL).
    """
    label, rule_id = F.lit(default), F.lit(None).cast("string")
    for rid, lab, pattern in reversed(rules):
        hit = F.coalesce(col.rlike(pattern), F.lit(False))
        label = F.when(hit, F.lit(lab)).otherwise(label)
        rule_id = F.when(hit, F.lit(rid)).otherwise(rule_id)
    return label, rule_id


def in_window(ts_col, window_start):
    """True when a timestamp is on or after the window start (an ISO date string)."""
    return ts_col >= F.to_timestamp(F.lit(window_start))


def try_bigint(name):
    """Cast a column to BIGINT, giving NULL (not an ANSI error) for non-numeric text."""
    return F.expr(f"try_cast(`{name}` AS BIGINT)")


# INCR_FIX_20261018: a SUM or AVG over DOUBLE depends on row order (floating point), which Lakeflow
# treats as non-deterministic ("Sum: FloatPrecision"), so the table never refreshes incrementally.
# Summing as DECIMAL is exact and order-independent; results go back to DOUBLE so schemas do not change.
SUM_DECIMAL = "decimal(38,18)"  # 20 integer digits (totals ~1e10), 18 places: finer than DOUBLE


def exact_sum(column):
    """SUM of a DOUBLE column name or expression, computed as DECIMAL(38,18), returned as DOUBLE."""
    value = F.col(column) if isinstance(column, str) else column
    return F.sum(value.cast(SUM_DECIMAL)).cast("double")


def exact_avg(column):
    """AVG of a DOUBLE column name or expression, computed as DECIMAL(38,18), returned as DOUBLE."""
    value = F.col(column) if isinstance(column, str) else column
    return F.avg(value.cast(SUM_DECIMAL)).cast("double")


def sample_persons(persons, mod):
    """The dev person cohort: persons with pmod(xxhash64(person_id), mod) = 0 (hash, not modulo of the id)."""
    p = persons.select(F.col("person_id").cast("bigint").alias("person_id")).where(F.col("person_id").isNotNull())
    return p.where(F.pmod(F.xxhash64(F.col("person_id")), F.lit(mod)) == 0).distinct()


def restrict_to_sample(df, sample):
    """Keep only rows whose person_id is in the sample cohort; sample=None means no restriction."""
    if sample is None:
        return df
    return df.join(sample, "person_id", "left_semi")


from rbi_schema_names import STREAM_COLUMN_NAMES as _NAMES  # noqa: E402

assert [n for n, _ in STREAM_COLUMNS] == _NAMES, "rbi_schema_names.STREAM_COLUMN_NAMES drifted from STREAM_COLUMNS"
