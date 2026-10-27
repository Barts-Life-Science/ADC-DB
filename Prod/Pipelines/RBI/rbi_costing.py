"""Costing for RBI: PLICS lines, unit-cost calibration, modelled pricing, actual allocation and
source precedence (spec §4, §10.4–10.6).

Money rules every function follows:
- Every PLICS sum is duplicate-weighted: total_cost × source_duplicate_count.
- PLICS activity_count is a diagnostic only. Unit costs divide £ by the quantity RBI itself
  observes for the same linked entity.
- Calibration works at the grain of the linked entity (encounter, or appointment for
  outpatients). Several PLICS activities (one per episode) can share one encounter, so their
  £ is summed before dividing, and each observed row is counted once.
- High-cost-drug administrations are classified the same way in calibration, allocation and
  pricing.
- Every division has a guarded denominator; nothing relies on ANSI-off behaviour.
- One authoritative £ per row (cost_total), plus overhead. Nothing is added twice.
"""
from pyspark.sql import functions as F

import rbi_rules as R
from rbi_schema import event_key, exact_avg, exact_sum, financial_year, london  # RBI_REL_20261025: event_key

COST_METHOD_VERSION = "rbi-cost-2"


def _family(code_col):
    """(stream, component) columns for an activity cost code, mirroring rbi_rules.plics_family."""
    prefix = F.substring(code_col, 1, 3)
    stream, component = F.lit(None).cast("string"), F.lit("unallocated")
    for p, (st, comp) in R.PLICS_FAMILY.items():
        stream = F.when(prefix == p, F.lit(st).cast("string")).otherwise(stream)
        component = F.when(prefix == p, F.lit(comp)).otherwise(component)
    hcd = F.coalesce(code_col.isin(*sorted(R.HCD_PHA_CODES)), F.lit(False))
    return (F.when(hcd, F.lit("high_cost_drug")).otherwise(stream),
            F.when(hcd, F.lit("pharmacy_hcd")).otherwise(component))


def _case(col, mapping):
    """CASE expression mapping exact values to labels; NULL when unmapped."""
    expr = None
    for value, label in mapping.items():
        cond = col == F.lit(value)
        expr = F.when(cond, F.lit(label)) if expr is None else expr.when(cond, F.lit(label))
    return expr


def setting_of_stream(stream_col):
    """Overhead setting for an RBI stream (spec §10.4); NULL for unpriced streams."""
    return _case(stream_col, R.STREAM_SETTING)


def plics_weighted_lines(lines, activities):
    """Duplicate-weighted PLICS cost lines with family, setting, FY, month and person from their activity."""
    stream, component = _family(F.col("activity_cost_item_cd"))
    act = activities.select("extract_cd", "activity_record_id", "feed_type",
                            F.col("person_id").cast("bigint").alias("person_id"),
                            F.to_date(london(F.col("event_datetime"))).alias("_activity_date"))
    return (lines.join(act, ["extract_cd", "activity_record_id"], "inner")
            .select("extract_cd", "activity_record_id", "activity_cost_item_cd", "feed_type", "person_id",
                    stream.alias("stream"), component.alias("component"),
                    (F.col("total_cost").cast("double") * F.col("source_duplicate_count")).alias("cost_weighted"),
                    (F.expr("try_cast(activity_count AS DOUBLE)") * F.col("source_duplicate_count")).alias("units_reported"),
                    _case(F.col("feed_type"), R.FEED_TYPE_SETTING).alias("setting"),
                    financial_year(F.col("_activity_date")).alias("financial_year"),
                    F.trunc(F.col("_activity_date"), "month").alias("activity_month")))


# ---- HCD classification (shared by calibration, allocation and pricing; spec §4.1) -------------

def hcd_ledger_cover(events):
    """What the HCD ledger covers, as two kinds of row (INCR_FIX_20261018).

    _kind = 'month': one row per (person_id, month) with ledger lines (drug_concept_id NULL).
    _kind = 'concept': one row per HCD drug concept seen in the ledger (person_id, month NULL).
    Rows rather than one array on every row, so readers equi-join instead of cross-joining:
    a cross join stops every downstream costing table from refreshing incrementally.
    """
    hcd = events.where(F.col("stream") == "high_cost_drug")
    months = hcd.select(
        F.lit("month").alias("_kind"),
        "person_id",
        F.trunc(F.col("event_date_local"), "month").alias("month"),
        F.lit(None).cast(events.schema["drug_concept_id"].dataType).alias("drug_concept_id"),
    )
    concepts = hcd.where(F.col("drug_concept_id").isNotNull()).select(
        F.lit("concept").alias("_kind"),
        F.lit(None).cast(events.schema["person_id"].dataType).alias("person_id"),
        F.lit(None).cast("date").alias("month"),
        "drug_concept_id",
    )
    return months.unionByName(concepts).distinct()


def hcd_cover_months(cover):
    """The person-months the HCD ledger covers (the 'month' rows of hcd_ledger_cover)."""
    return cover.where(F.col("_kind") == "month").select("person_id", "month")


def _hcd_flags(events, cover):
    """Add _is_hcd_admin (medication admin of an HCD drug) and _ledger_covers (ledger has the person-month)."""
    concepts = (cover.where(F.col("_kind") == "concept")
                .select("drug_concept_id", F.lit(True).alias("_is_hcd_concept")))
    months = hcd_cover_months(cover).withColumn("_ledger_covers", F.lit(True))
    return (events.join(concepts, "drug_concept_id", "left")
            .withColumn("_is_hcd_admin", (F.col("stream") == "medication_admin")
                        & F.coalesce(F.col("_is_hcd_concept"), F.lit(False)))
            .withColumn("month", F.trunc(F.col("event_date_local"), "month"))
            .join(months, ["person_id", "month"], "left")
            .withColumn("_ledger_covers", F.coalesce(F.col("_ledger_covers"), F.lit(False)))
            .drop("_is_hcd_concept", "month"))


def _targets(events, cover):
    """Consumed, measurable rows that may anchor a calibration or receive allocated £.

    HCD administrations are relabelled stream 'high_cost_drug', so they never sit in the
    ordinary-pharmacy denominator and can receive HCD PLICS lines. The ledger rows themselves
    (no encounter) are never targets.
    """
    e = _hcd_flags(events, cover)
    e = (e.withColumn("_target_stream", F.when(F.col("_is_hcd_admin"), F.lit("high_cost_drug")).otherwise(F.col("stream")))
         .where(F.col("stream") != "high_cost_drug"))
    priced_tracking = (F.col("stream") != "tracking_attendance") | F.col("tracking_class").isin(*R.PRICED_TRACKING_CLASSES)
    return (e.where((F.col("consumption_status") == "consumed")
                    & ~F.coalesce(F.col("priced_by_other_stream"), F.lit(False))
                    & (F.col("quantity") > 0) & priced_tracking)
            .select(F.col("_target_stream").alias("_ev_stream"), F.col("encounter_id").alias("_ev_encounter_id"),
                    F.col("financial_year").alias("_ev_fy"), "resource_use_event_key", "quantity", "stratum_l1", "stratum_l2"))


def _match_events(rows, targets):
    """Join rows carrying (_enc, _appt, stream) to target rows of the same stream.

    Outpatient £ goes to the specific linked appointment; everything else goes to the linked
    encounter's rows of that stream.
    """
    op = rows.where(F.col("stream") == "outpatient_appointment").join(
        targets, (F.col("_appt") == F.col("resource_use_event_key")) & (F.col("_ev_stream") == F.col("stream")))
    other = rows.where(F.col("stream") != "outpatient_appointment").join(
        targets, (F.col("_enc") == F.col("_ev_encounter_id")) & (F.col("_ev_stream") == F.col("stream")))
    return op.unionByName(other)


def _linked(links):
    """Score-1 activity links renamed for joining."""
    return links.where(F.col("link_score") == 1.0).select(
        "extract_cd", "activity_record_id", F.col("encounter_id").alias("_enc"), F.col("appointment_key").alias("_appt"))


# ---- Calibration (spec §4.2) -------------------------------------------------------------------

def _entity_pounds(weighted_lines, links, fys):
    """Σ family £ per linked entity × stream × FY, with the entity's activity count.

    Entity = the appointment for outpatient £, otherwise the encounter. Summing all activities
    that share an entity before matching rows keeps numerator and denominator on the same grain
    (review fix 1).
    """
    lines = weighted_lines.where(F.col("stream").isin(*R.PRICED_STREAMS + ("high_cost_drug",)))
    if fys is not None:
        lines = lines.where(F.col("financial_year").isin(*fys))
    act = (lines.groupBy("extract_cd", "activity_record_id", "stream", "financial_year")
           .agg(exact_sum("cost_weighted").alias("_pounds"), exact_sum("units_reported").alias("_units"))
           .join(_linked(links), ["extract_cd", "activity_record_id"]))
    is_op = F.col("stream") == "outpatient_appointment"
    act = act.withColumns({
        "_enc": F.when(is_op, F.lit(None).cast("bigint")).otherwise(F.col("_enc")),
        "_appt": F.when(is_op, F.col("_appt")),
    }).where(F.col("_enc").isNotNull() | F.col("_appt").isNotNull())
    act = act.withColumn("_entity", F.coalesce(F.concat(F.lit("appt:"), F.col("_appt")),
                                               F.concat(F.lit("enc:"), F.col("_enc").cast("string"))))
    return (act.groupBy("_entity", "_enc", "_appt", "stream", "financial_year")
            .agg(exact_sum("_pounds").alias("_pounds"), exact_sum("_units").alias("_units_reported"),
                 F.countDistinct("extract_cd", "activity_record_id").alias("_acts")))


def calibrate_unit_costs(weighted_lines, links, events, cover, base_fys=None):
    """unit_cost = Σ family £ ÷ Σ observed consumed quantity, per stream × stratum level × base FY (spec §4.2).

    Grain: linked entity. An entity's £ is split across its rows of the same FY pro rata to
    quantity, so a spell over two wards funds both wards, and two episodes on one spell share
    one set of bed-days. base_fys=None calibrates every FY with PLICS lines.
    """
    ent = _entity_pounds(weighted_lines, links, base_fys).where(F.col("stream").isin(*R.PRICED_STREAMS))
    matched = _match_events(ent, _targets(events, cover)).where(F.col("_ev_fy") == F.col("financial_year"))
    q_ent = matched.groupBy("_entity", "stream", "financial_year").agg(exact_sum("quantity").alias("_q_ent"))
    m = (matched.join(q_ent, ["_entity", "stream", "financial_year"])
         .withColumn("_alloc", F.col("_pounds") * F.col("quantity") / F.col("_q_ent")))  # _q_ent > 0: targets have quantity > 0

    def level(name, value_col):
        return (m.groupBy("stream", F.col("financial_year").alias("base_fy"), value_col.alias("stratum_value"))
                .agg(exact_sum("_alloc").alias("pounds"), exact_sum("quantity").alias("quantity"),
                     F.countDistinct("_entity").alias("n_entities"))
                .withColumn("stratum_level", F.lit(name)))

    out = (level("l1", F.coalesce(F.col("stratum_l1"), F.lit("unknown")))
           .unionByName(level("l2", F.coalesce(F.col("stratum_l2"), F.lit("unknown"))))
           .unionByName(level("all", F.lit("all"))))
    # Diagnostic only: what PLICS's own activity_count would imply, per stream × base FY.
    hit = m.select("_entity", "stream", "financial_year").distinct()
    reported = (ent.join(hit, ["_entity", "stream", "financial_year"])
                .groupBy("stream", F.col("financial_year").alias("base_fy"))
                .agg(exact_sum("_pounds").alias("_p"), exact_sum("_units_reported").alias("_u"))
                .select("stream", "base_fy", F.when(F.col("_u") > 0, F.col("_p") / F.col("_u")).alias("activity_count_implied_unit_cost")))
    return (out.withColumn("unit_cost", F.when(F.col("quantity") > 0, F.col("pounds") / F.col("quantity")))
            .join(reported, ["stream", "base_fy"], "left")
            .withColumn("cost_method_version", F.lit(COST_METHOD_VERSION)))


def overhead_ratios(weighted_lines, links, base_fys=None):
    """Support-cost ratio per setting × base FY: Σ SUP £ ÷ Σ direct £ over score-1 linked activities (spec §10.4).

    Direct = every component except overhead and unallocated. Settings with no positive direct £ are omitted.
    """
    lw = weighted_lines if base_fys is None else weighted_lines.where(F.col("financial_year").isin(*base_fys))
    lw = lw.join(_linked(links).select("extract_cd", "activity_record_id"), ["extract_cd", "activity_record_id"])
    direct = ~F.col("component").isin("overhead", "unallocated")
    return (lw.groupBy("setting", F.col("financial_year").alias("base_fy"))
            .agg(exact_sum(F.when(F.col("component") == "overhead", F.col("cost_weighted")).otherwise(0.0)).alias("_sup"),
                 exact_sum(F.when(direct, F.col("cost_weighted")).otherwise(0.0)).alias("_direct"))
            .where(F.col("_direct") > 0)
            .select("setting", "base_fy", (F.col("_sup") / F.col("_direct")).alias("overhead_ratio")))


# ---- Modelled pricing (spec §4.2) ------------------------------------------------------------

def _priceable(events, cover):
    """Consumed, priceable rows (HCD administrations are never modelled; the ledger prices them, or nothing does)."""
    e = _hcd_flags(events, cover)
    priced_tracking = (F.col("stream") != "tracking_attendance") | F.col("tracking_class").isin(*R.PRICED_TRACKING_CLASSES)
    return e.where((F.col("consumption_status") == "consumed") & F.col("stream").isin(*R.PRICED_STREAMS)
                   & ~F.coalesce(F.col("priced_by_other_stream"), F.lit(False)) & (F.col("quantity") > 0)
                   & priced_tracking & ~F.col("_is_hcd_admin"))


def price_key_space(events, cover):
    """Distinct (stream, stratum_l1, stratum_l2, financial_year) of priceable rows: the small table pricing resolves."""
    return _priceable(events, cover).select("stream", "stratum_l1", "stratum_l2", "financial_year").distinct()


def price_keys(unit_costs, uplift, key_space, min_n=R.MIN_CALIBRATION_ACTIVITIES):
    """Resolve unit cost, level, base FY and uplift once per price key (spec §4.2).

    Unit cost from the latest base FY per stream, at the most specific level with n_entities ≥ min_n
    (l1 → l2 → all); uplift = index(row FY) ÷ index(base FY). Doing this on the small key table keeps
    the per-row pricing a single lookup join, which Lakeflow can refresh incrementally.
    """
    latest = unit_costs.groupBy("stream").agg(F.max("base_fy").alias("cost_base_fy"))
    usable = (unit_costs.where((F.col("n_entities") >= min_n) & F.col("unit_cost").isNotNull())
              .join(latest, "stream").where(F.col("base_fy") == F.col("cost_base_fy")))

    def lvl(name, key_col):
        return usable.where(F.col("stratum_level") == name).select(
            "stream", F.col("stratum_value").alias(key_col), F.col("unit_cost").alias(f"_uc_{name}"))

    k = (key_space.join(latest, "stream", "left")
         .join(lvl("l1", "stratum_l1"), ["stream", "stratum_l1"], "left")
         .join(lvl("l2", "stratum_l2"), ["stream", "stratum_l2"], "left")
         .join(lvl("all", "_all").drop("_all"), "stream", "left"))
    idx = uplift.select("financial_year", "index_value")
    k = (k.join(idx.withColumnRenamed("index_value", "_idx_row"), "financial_year", "left")
         .join(idx.select(F.col("financial_year").alias("cost_base_fy"), F.col("index_value").alias("_idx_base")), "cost_base_fy", "left"))
    unit = F.coalesce("_uc_l1", "_uc_l2", "_uc_all")
    factor = F.when(F.col("_idx_base") > 0, F.col("_idx_row") / F.col("_idx_base"))
    return k.select("stream", "stratum_l1", "stratum_l2", "financial_year",
                    unit.alias("unit_cost"),
                    (F.when(F.col("_uc_l1").isNotNull(), F.lit("l1")).when(F.col("_uc_l2").isNotNull(), F.lit("l2"))
                     .when(F.col("_uc_all").isNotNull(), F.lit("all"))).alias("cost_stratum_level"),
                    F.when(unit.isNotNull(), F.col("cost_base_fy")).alias("cost_base_fy"),
                    F.when(unit.isNotNull(), factor).alias("uplift_factor"))


def price_from_keys(events, keys, cover):
    """Modelled £ = quantity × unit cost × uplift, by one NULL-safe lookup join on the price key."""
    k = keys.select(*[F.col(c).alias(f"_k_{c}") for c in keys.columns])
    e = _priceable(events, cover)
    cond = ((e["stream"] == k["_k_stream"]) & e["stratum_l1"].eqNullSafe(k["_k_stratum_l1"])
            & e["stratum_l2"].eqNullSafe(k["_k_stratum_l2"]) & (e["financial_year"] == k["_k_financial_year"]))
    j = e.join(k, cond, "left")
    return j.select("resource_use_event_key",
                    (F.col("quantity") * F.col("_k_unit_cost") * F.col("_k_uplift_factor")).alias("cost_modelled"),
                    F.col("_k_cost_stratum_level").alias("cost_stratum_level"),
                    F.col("_k_cost_base_fy").alias("cost_base_fy"),
                    F.col("_k_uplift_factor").alias("uplift_factor"))


def price_modelled(events, unit_costs, uplift, cover, min_n=R.MIN_CALIBRATION_ACTIVITIES):
    """Modelled £ for consumed, priceable rows (spec §4.2): price_keys resolved once, then one lookup.

    Returns resource_use_event_key, cost_modelled, cost_stratum_level, cost_base_fy, uplift_factor.
    """
    keys = price_keys(unit_costs, uplift, price_key_space(events, cover), min_n)
    return price_from_keys(events, keys, cover)


# ---- Allocation ledger (spec §4.4) -------------------------------------------------------------

LEDGER_COLUMNS = ["cost_allocation_ledger_key", "extract_cd", "activity_record_id", "component", "resource_use_event_key",
                  "amount_kind", "amount", "cost_basis"]


def ledger_row_key(extract_cd, activity_record_id, component, resource_use_event_key, amount_kind, cost_basis):
    """cost_allocation_ledger_key (RBI_REL_20261025): SHA-256 over the ledger's unique grain.

    Grain = extract_cd × activity_record_id × component × receiving fact row × amount_kind × cost_basis.
    Unattached rows (unallocated / superseded) have no fact row, so their NULL resource_use_event_key
    enters the hash as '~'. One row per grain is guaranteed by construction (see _by_activity_component)
    and asserted in rbi_validate.
    """
    return event_key(extract_cd, activity_record_id, component, F.coalesce(resource_use_event_key, F.lit("~")),
                     amount_kind, cost_basis)


def _ledger(df, kind, amount_col, basis, key_col=None):
    """Shape allocation slices into ledger rows (key NULL = not attached to a fact row), with the row key first."""
    row = df.select("extract_cd", "activity_record_id", "component",
                    (F.col(key_col) if key_col else F.lit(None).cast("string")).alias("resource_use_event_key"),
                    F.lit(kind).alias("amount_kind"), F.col(amount_col).cast("double").alias("amount"),
                    F.lit(basis).alias("cost_basis"))
    key = ledger_row_key(F.col("extract_cd"), F.col("activity_record_id"), F.col("component"),
                         F.col("resource_use_event_key"), F.col("amount_kind"), F.col("cost_basis"))
    return row.withColumn("cost_allocation_ledger_key", key).select(*LEDGER_COLUMNS)


def _by_activity_component(df, amount_col):
    """Sum an unattached ledger branch to one row per extract_cd × activity_record_id × component (RBI_REL_20261025).

    The branches arrive grouped by person_id, activity_month and stream too. Those are functions of the
    activity today, but nothing enforces it, so collapsing here guarantees the ledger grain rather than
    assuming it. DECIMAL sum (exact_sum): the £ total is unchanged.
    """
    return (df.groupBy("extract_cd", "activity_record_id", "component")
            .agg(exact_sum(amount_col).alias(amount_col)))


def allocate_plics_actuals(weighted_lines, links, events, cover):
    """Line-level PLICS £ as an allocation ledger (spec §4.4).

    - direct: each linked activity's line £ goes to its consumed rows of the line's stream,
      pro rata to quantity (outpatient: the linked appointment).
    - HCD lines (pharmacy_hcd): 'superseded' when the HCD ledger covers that person-month.
      Otherwise they go to the encounter's HCD administrations.
    - overhead: SUP £ is spread over the activity's direct slices pro rata to their £, only when
      the activity's direct £ is positive. Otherwise the SUP £ stays unallocated (review fix 3).
    - unallocated: whatever finds no target (unlinked activity, no consumed row, unmapped family).
    Σ ledger amount = Σ weighted lines, per activity × component.
    """
    keys = ["extract_cd", "activity_record_id"]
    lines = (weighted_lines.groupBy(*keys, "person_id", "activity_month", "stream", "component")
             .agg(exact_sum("cost_weighted").alias("_pounds")))
    months = hcd_cover_months(cover).select("person_id", F.col("month").alias("activity_month"), F.lit(True).alias("_covered"))
    lines = (lines.join(months, ["person_id", "activity_month"], "left")
             .withColumn("_covered", F.coalesce(F.col("_covered"), F.lit(False))))
    is_superseded = (F.col("component") == "pharmacy_hcd") & F.col("_covered")
    superseded, live = lines.where(is_superseded), lines.where(~is_superseded)

    direct = live.where(F.col("stream").isNotNull()).join(_linked(links), keys, "left")
    matched = _match_events(direct.where(F.col("_enc").isNotNull() | F.col("_appt").isNotNull()), _targets(events, cover))
    q = matched.groupBy(*keys, "stream").agg(exact_sum("quantity").alias("_q"))
    direct_alloc = (matched.join(q, [*keys, "stream"])
                    .select(*keys, "component", "resource_use_event_key",
                            (F.col("_pounds") * F.col("quantity") / F.col("_q")).alias("_amt")))  # _q > 0: targets have quantity > 0
    hit = matched.select(*keys, "stream").distinct().withColumn("_hit", F.lit(True))
    direct_missed = direct.join(hit, [*keys, "stream"], "left").where(F.col("_hit").isNull())

    sup = live.where(F.col("component") == "overhead").groupBy(*keys).agg(exact_sum("_pounds").alias("_sup"))
    positive = direct_alloc.groupBy(*keys).agg(exact_sum("_amt").alias("_direct")).where(F.col("_direct") > 0)
    sup_alloc = (direct_alloc.join(sup, keys).join(positive, keys)
                 .select(*keys, F.lit("overhead").alias("component"), "resource_use_event_key",
                         (F.col("_sup") * F.col("_amt") / F.col("_direct")).alias("_amt")))
    sup_missed = sup.join(positive, keys, "left_anti").withColumn("component", F.lit("overhead"))
    other = live.where(F.col("stream").isNull() & (F.col("component") != "overhead"))

    # Unattached branches are collapsed to the ledger grain first (RBI_REL_20261025).
    return (_ledger(direct_alloc, "direct", "_amt", "actual_plics", "resource_use_event_key")
            .unionByName(_ledger(sup_alloc, "overhead", "_amt", "actual_plics", "resource_use_event_key"))
            .unionByName(_ledger(_by_activity_component(direct_missed, "_pounds"), "unallocated", "_pounds", "actual_plics"))
            .unionByName(_ledger(_by_activity_component(other, "_pounds"), "unallocated", "_pounds", "actual_plics"))
            .unionByName(_ledger(sup_missed, "unallocated", "_sup", "actual_plics"))
            .unionByName(_ledger(_by_activity_component(superseded, "_pounds"), "superseded", "_pounds", "actual_plics")))


def allocate_episode_totals(activities, links, events, modelled):
    """Episode totals without cost lines (NCC2324 and similar) as ledger rows (spec §4.4).

    The total T of an activity with cost_line_count = 0 is shared over its linked encounter's
    rows in proportion to their modelled £. With no modelled row, T stays unallocated.
    """
    keys = ["extract_cd", "activity_record_id"]
    ep = (activities.where((F.col("cost_line_count") == 0) & F.col("total_cost_sum").isNotNull())
          .select(*keys, F.col("total_cost_sum").cast("double").alias("_total"))
          .join(_linked(links), keys, "left")
          .withColumn("component", F.lit("episode_total")))
    rows = (events.select("resource_use_event_key", F.col("encounter_id").alias("_enc"))
            .join(modelled.where(F.col("cost_modelled") > 0).select("resource_use_event_key", "cost_modelled"), "resource_use_event_key"))
    m = ep.join(rows, "_enc")
    tot = m.groupBy(*keys).agg(exact_sum("cost_modelled").alias("_m")).where(F.col("_m") > 0)
    alloc = (m.join(tot, keys)
             .select(*keys, "component", "resource_use_event_key", (F.col("_total") * F.col("cost_modelled") / F.col("_m")).alias("_amt")))
    missed = ep.join(tot, keys, "left_anti")
    return (_ledger(alloc, "direct", "_amt", "actual_ncc_allocated", "resource_use_event_key")
            .unionByName(_ledger(missed, "unallocated", "_total", "actual_ncc_allocated")))


def ledger_event_allocations(ledger):
    """Per fact row: actual £ by basis from the ledger (plics_direct, plics_overhead, ncc_direct)."""
    e = ledger.where(F.col("resource_use_event_key").isNotNull())
    plics, ncc = F.col("cost_basis") == "actual_plics", F.col("cost_basis") == "actual_ncc_allocated"
    return e.groupBy("resource_use_event_key").agg(
        exact_sum(F.when(plics & (F.col("amount_kind") == "direct"), F.col("amount"))).alias("plics_direct"),
        exact_sum(F.when(plics & (F.col("amount_kind") == "overhead"), F.col("amount"))).alias("plics_overhead"),
        exact_sum(F.when(ncc & (F.col("amount_kind") == "direct"), F.col("amount"))).alias("ncc_direct"),
    )


def ledger_unallocated(ledger):
    """Per activity × component × basis: unallocated £ (cost_total) and superseded £ (cost_superseded)."""
    u = ledger.where(F.col("resource_use_event_key").isNull())
    return u.groupBy("extract_cd", "activity_record_id", "component", "cost_basis").agg(
        F.coalesce(exact_sum(F.when(F.col("amount_kind") == "unallocated", F.col("amount"))), F.lit(0.0)).alias("cost_total"),
        F.coalesce(exact_sum(F.when(F.col("amount_kind") == "superseded", F.col("amount"))), F.lit(0.0)).alias("cost_superseded"),
    )


# ---- Precedence: one authoritative £ per row (spec §4.1, §10.5) ---------------------------

def apply_costs(events, modelled, allocations, overheads, cover):
    """Attach cost columns to every stream row by source precedence.

    Precedence: HCD ledger → line-level PLICS → NCC episode total → covered by HCD ledger
    (HCD administrations, £0) → modelled bottom-up → none (with a reason).
    Overhead: actual SUP share for PLICS rows; modelled £ × setting ratio for modelled rows;
    0 for NCC rows (NCC totals are fully absorbed) and ledger rows.
    `allocations` is ledger_event_allocations(...) over all ledgers.
    """
    e = (_hcd_flags(events, cover)
         .join(modelled, "resource_use_event_key", "left")
         .join(allocations, "resource_use_event_key", "left")
         .withColumn("_setting", setting_of_stream(F.col("stream"))))
    e = e.join(overheads.select(F.col("setting").alias("_setting"), F.col("base_fy").alias("cost_base_fy"), "overhead_ratio"),
               ["_setting", "cost_base_fy"], "left")

    basis = (F.when(F.col("stream") == "high_cost_drug", F.lit("drug_expenditure_actual"))
             .when(F.col("plics_direct").isNotNull(), F.lit("actual_plics"))
             .when(F.col("ncc_direct").isNotNull(), F.lit("actual_ncc_allocated"))
             .when(F.col("_is_hcd_admin") & F.col("_ledger_covers"), F.lit("covered_by_hcd_ledger"))
             .when(F.col("cost_modelled").isNotNull(), F.lit("modelled_bottom_up"))
             .otherwise(F.lit("none")))
    e = e.withColumn("cost_basis", basis)
    reason = (F.when(F.col("cost_basis") != "none", F.lit(None).cast("string"))
              .when(F.col("consumption_status") == "not_consumed", F.lit("not_consumed"))
              .when(F.col("consumption_status") == R.SCHEDULED_STATUS, F.lit("scheduled"))  # RBI_REL_20261025
              .when(F.col("consumption_status") == "unknown", F.lit("consumption_unknown"))
              .when(F.coalesce(F.col("priced_by_other_stream"), F.lit(False)), F.lit("priced_by_other_stream"))
              .when((F.col("stream") == "tracking_attendance") & ~F.col("tracking_class").isin(*R.PRICED_TRACKING_CLASSES),
                    F.lit("unpriced_tracking_class"))
              .when(F.coalesce(F.col("quantity"), F.lit(0.0)) <= 0, F.lit("zero_quantity"))
              .when(F.col("_is_hcd_admin"), F.lit("hcd_drug_without_ledger"))
              .otherwise(F.lit("no_calibration")))
    total = (F.when(F.col("cost_basis") == "drug_expenditure_actual", F.col("source_amount"))
             .when(F.col("cost_basis") == "actual_plics", F.col("plics_direct"))
             .when(F.col("cost_basis") == "actual_ncc_allocated", F.col("ncc_direct"))
             .when(F.col("cost_basis") == "modelled_bottom_up", F.col("cost_modelled"))
             .otherwise(F.lit(0.0)))
    overhead = (F.when(F.col("cost_basis") == "actual_plics", F.coalesce(F.col("plics_overhead"), F.lit(0.0)))
                .when(F.col("cost_basis") == "modelled_bottom_up", F.col("cost_modelled") * F.coalesce(F.col("overhead_ratio"), F.lit(0.0)))
                .otherwise(F.lit(0.0)))
    return e.withColumns({
        "cost_reason": reason,
        "cost_total": F.coalesce(total, F.lit(0.0)),
        "cost_overhead": overhead,
        "cost_modelled_shadow": F.when(F.col("cost_basis").isin("actual_plics", "actual_ncc_allocated"), F.col("cost_modelled")),
        "cost_superseded": F.lit(0.0),
        "cost_method_version": F.lit(COST_METHOD_VERSION),
    }).drop("plics_direct", "plics_overhead", "ncc_direct", "_setting", "overhead_ratio", "_is_hcd_admin",
            "_ledger_covers", "cost_modelled")


# ---- Backtest (spec §4.2, §10.11) --------------------------------------------------------------

def backtest(weighted_lines, links, events, uplift, cover, base_fy, test_fy, min_n=R.MIN_CALIBRATION_ACTIVITIES):
    """Price test-FY linked entities with base-FY unit costs and compare to their PLICS £.

    Per stream: n_entities compared, mean absolute percentage error (MAPE) over entities with
    actual £ > 0 and a modelled price, total actual and total modelled. Report-only.
    Same entity grain and HCD classification as calibration.
    """
    uc = calibrate_unit_costs(weighted_lines, links, events, cover, [base_fy])
    ent = _entity_pounds(weighted_lines, links, [test_fy]).where(F.col("stream").isin(*R.PRICED_STREAMS))
    matched = _match_events(ent, _targets(events, cover)).where(F.col("_ev_fy") == F.col("financial_year"))
    keys = matched.select("resource_use_event_key").distinct()
    # Price the matched rows as if they were in the test year, using base-year unit costs.
    test_events = events.join(keys, "resource_use_event_key").withColumn("financial_year", F.lit(test_fy))
    priced = price_modelled(test_events, uc, uplift, cover, min_n=min_n).select("resource_use_event_key", "cost_modelled")
    per_entity = (matched.join(priced, "resource_use_event_key", "left")
                  .groupBy("_entity", "stream")
                  .agg(F.first("_pounds").alias("_actual"), exact_sum("cost_modelled").alias("_modelled")))
    ok = (F.col("_actual") > 0) & F.col("_modelled").isNotNull()
    return (per_entity.groupBy("stream")
            .agg(F.count(F.when(ok, 1)).alias("n_entities"),
                 exact_avg(F.when(ok, F.abs(F.col("_modelled") - F.col("_actual")) / F.col("_actual"))).alias("mape"),
                 exact_sum("_actual").alias("total_actual"), exact_sum("_modelled").alias("total_modelled"))
            .withColumns({"base_fy": F.lit(base_fy), "test_fy": F.lit(test_fy),
                          "cost_method_version": F.lit(COST_METHOD_VERSION)}))
