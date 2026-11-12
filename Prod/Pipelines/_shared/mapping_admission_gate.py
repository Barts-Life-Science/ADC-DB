"""R1 method-admission gate, shared by gold journey and omop_gold (design rev 3 R1, decision 7; deviation D6).

Policy lives in 3_lookup.omop.mapping_admission_policy (one row per policy_version x map_method, is_current marks the
live version). Three renderings of the same rule:
  * gate_and_policy(item, policy): the column expression gold uses today (gold_journey_shared.py:701-748), with
    the prefix lists and banded policies read from the table at graph build (load_policy); AST-equal to the original;
  * view_gate_sql(alias, axis, table): the inline SQL for R4's gold views (LEFT JOIN on map_method + exists());
  * admission_sql(resolution, table): the per-(lane, key, component) verdict over mapping_resolution, the body of
    R4's gold_qc._mapping_admission MV that the materialised gold tables join (Ben's 2026-11-01 ruling). The policy
    is joined as data there too, so a policy change is a data change for the MV and its readers, never a re-plan.
The view and per-key forms share one SQL rule (_gate_sql); Task 13 proves both equal the literal form.
"""
from dataclasses import dataclass

import pyspark.sql.functions as F

REJECTED_STATUSES = ("judge_rejected", "vetoed_polarity")
TRANSLATION_METHODS = ("rule", "exact_synonym")


@dataclass(frozen=True)
class Policy:
    """The current policy in the shapes gold's expression uses."""
    translation_prefixes: tuple
    rollup_prefixes: tuple
    rollup_max_levels: int
    banded: dict          # {policy id: {method, lane, scoring_model, hierarchy_version, cutoff, rollup_config?}}
    version: str

    @staticmethod
    def from_rows(rows):
        """Rebuild the literals from table rows (dicts); refuses mixed versions or a missing translation row."""
        versions = {r["policy_version"] for r in rows}
        assert len(versions) == 1, f"policy rows from several versions: {sorted(versions)}"
        by_method = {r["map_method"]: r for r in rows}
        translation = tuple(by_method["rule"]["translation_prefixes"])
        assert tuple(by_method["exact_synonym"]["translation_prefixes"]) == translation, "rule/exact_synonym differ"
        rollup = by_method["ancestor_rollup"]
        banded = {}
        for r in rows:
            for p in r.get("banded") or []:
                entry = {"method": r["map_method"], "lane": p["lane"], "scoring_model": p["scoring_model"],
                         "hierarchy_version": p["hierarchy_version"], "cutoff": p["cutoff"]}
                if p.get("max_levels") is not None:
                    entry["rollup_config"] = {"max_levels": p["max_levels"]}
                banded[p["id"]] = entry
        return Policy(translation, tuple(rollup["rollup_prefixes"]), int(rollup["rollup_max_levels"]), banded,
                      versions.pop())


def seed_rows(translation_prefixes, rollup_prefixes, banded, policy_version, rollup_max_levels=4):
    """Policy rows from today's literals and the journey.s3b_policies dict (P1 seed; also the test fixture)."""
    methods = set(TRANSLATION_METHODS) | {"ancestor_rollup"} | {p["method"] for p in banded.values()}
    rows = []
    for method in sorted(methods):
        mine = [{"id": pid, "lane": p["lane"], "scoring_model": p["scoring_model"],
                 "hierarchy_version": p["hierarchy_version"], "cutoff": float(p["cutoff"]),
                 "max_levels": (p.get("rollup_config") or {}).get("max_levels")}
                for pid, p in sorted(banded.items()) if p["method"] == method]
        rows.append({"policy_version": policy_version, "map_method": method,
                     "translation_prefixes": list(translation_prefixes) if method in TRANSLATION_METHODS else [],
                     "rollup_prefixes": list(rollup_prefixes) if method == "ancestor_rollup" else [],
                     "rollup_max_levels": rollup_max_levels if method == "ancestor_rollup" else None,
                     "banded": mine, "is_current": True})
    return rows


def load_policy(spark, table="`3_lookup`.omop.mapping_admission_policy"):
    """The current Policy, read at graph build (one small collect). Refuses an empty or multi-version table."""
    rows = [r.asDict(recursive=True) for r in spark.read.table(table).where("is_current").collect()]
    assert rows, f"{table} holds no current policy rows"
    return Policy.from_rows(rows)


def gate_and_policy(item, policy):
    """Decide whether a mapped target meets the configured policy and return its provenance (gold's rule, verbatim)."""
    method = item["map_method"]
    rule = F.coalesce(item["map_rule_id"], F.lit(""))
    native = method == "source_native"
    levels = F.coalesce(item["map_rollup_levels"], F.lit(99))
    translation = (
        (method.isin("rule", "exact_synonym")
         & F.exists(F.array(*[F.lit(p) for p in policy.translation_prefixes]), lambda p: rule.startswith(p)))
        | ((method == "ancestor_rollup")
           & F.exists(F.array(*[F.lit(p) for p in policy.rollup_prefixes]), lambda p: rule.startswith(p))
           & (levels <= F.lit(4)))
    ) & ~rule.contains("EXACT_string_similarity")
    policy_id = F.when(native, F.lit("source_native")).when(translation, F.lit("governed_translation"))
    banded = F.lit(False)
    if policy.banded:
        policies = F.array(*[
            F.struct(
                F.lit(current_id).alias("id"),
                F.lit(policy["method"]).alias("method"),
                F.lit(f":{policy['lane']}:").alias("lane"),
                F.lit(policy["scoring_model"]).alias("scoring_model"),
                F.lit(policy["hierarchy_version"]).alias("hierarchy_version"),
                F.lit(float(policy["cutoff"])).alias("cutoff"),
                F.lit(int(policy.get("rollup_config", {}).get("max_levels", 3))).alias("max_levels"),
            )
            for current_id, policy in sorted(policy.banded.items())
        ])
        matched = F.filter(policies, lambda p: (
            (method == p["method"]) &
            rule.contains(p["lane"]) &
            (F.coalesce(item["map_scoring_model"], F.lit("")) == p["scoring_model"]) &
            F.coalesce(item["map_version"], F.lit("")).contains(p["hierarchy_version"]) &
            (F.coalesce(item["map_cosine"], F.lit(0.0)) >= p["cutoff"]) &
            ((method != "ancestor_rollup") | (levels <= p["max_levels"]))
        ))
        # The last matching policy in id order wins, as the unrolled CASE chain did.
        banded = F.size(matched) > 0
        policy_id = F.when(banded, F.element_at(matched, -1)["id"]).otherwise(policy_id)
    rejected = F.coalesce(item["map_status"], F.lit("")).isin("judge_rejected", "vetoed_polarity")
    gate = F.coalesce((native | translation | banded) & ~rejected, F.lit(False))
    policy_id = F.when(rejected, F.concat(F.lit("withheld:"), item["map_status"])).otherwise(policy_id)
    return gate, F.coalesce(policy_id, F.lit("withheld:no_matching_policy"))


def _gate_sql(c, p):
    """(gate SQL, policy_id SQL) over one set of gate inputs: c(field) names an input column, p is the alias of the
    joined current-policy row. One rule text for the view form and the per-key form, so the two cannot drift."""
    rule = f"coalesce({c('map_rule_id')}, '')"
    levels = f"coalesce({c('map_rollup_levels')}, 99)"
    translation = (f"(({c('map_method')} IN ('rule', 'exact_synonym') AND exists({p}.translation_prefixes, x -> "
                   f"startswith({rule}, x))) OR ({c('map_method')} = 'ancestor_rollup' AND "
                   f"exists({p}.rollup_prefixes, x -> startswith({rule}, x)) AND {levels} <= 4)) "
                   f"AND NOT contains({rule}, 'EXACT_string_similarity')")
    matched = (f"filter({p}.banded, b -> contains({rule}, concat(':', b.lane, ':')) AND "
               f"coalesce({c('map_scoring_model')}, '') = b.scoring_model AND "
               f"contains(coalesce({c('map_version')}, ''), b.hierarchy_version) AND "
               f"coalesce({c('map_cosine')}, 0.0) >= b.cutoff AND "
               f"({c('map_method')} <> 'ancestor_rollup' OR {levels} <= coalesce(b.max_levels, 3)))")
    rejected = f"coalesce({c('map_status')}, '') IN ('judge_rejected', 'vetoed_polarity')"
    native = f"{c('map_method')} = 'source_native'"
    gate = f"coalesce(({native} OR ({translation}) OR size({matched}) > 0) AND NOT {rejected}, false)"
    policy_id = (f"coalesce(CASE WHEN {rejected} THEN concat('withheld:', {c('map_status')}) "
                 f"WHEN size({matched}) > 0 THEN element_at({matched}, -1).id "
                 f"WHEN {native} THEN 'source_native' WHEN {translation} THEN 'governed_translation' END, "
                 f"'withheld:no_matching_policy')")
    return gate, policy_id


def view_gate_sql(alias, axis, table):
    """{"join": LEFT JOIN clause, "gate": boolean SQL, "policy_id": SQL} for one axis of a gold view (R4).

    One row per map_method in the current version, so the join never multiplies rows; a method with no row gets NULL
    arrays and only the native rule can admit it, exactly as the literal form."""
    p = f"_pol_{axis}"
    c = lambda field: f"{alias}.{axis}_{field}"   # noqa: E731  (column of the axis)
    join = (f"LEFT JOIN (SELECT * FROM {table} WHERE is_current) {p} ON {p}.map_method = {c('map_method')}")
    gate, policy_id = _gate_sql(c, p)
    return {"join": join, "gate": gate, "policy_id": policy_id}


def admission_sql(resolution, table):
    """Body of R4's gold_qc._mapping_admission MV (Ben's 2026-11-01 ruling): one verdict per (lane_id, map_key,
    component_index) of mapping_resolution. Split keys (about 1.5% of rows) can carry a different method per
    component, and silver's component explode gives each component its own row, so gold joins on all three columns
    (component 1 for unsplit rows). The policy is LEFT JOINed as data (one row per map_method, so no row multiplies),
    never collected into literals: a policy change is a data change for this MV and the gold MVs that join it. No
    policy_version column, so a version bump that moves no verdict changes no row. Every key has a row (decision 19)."""
    c = lambda field: f"r.{field}"   # noqa: E731  (resolution column)
    gate, policy_id = _gate_sql(c, "p")
    return (f"SELECT r.lane_id, r.map_key, r.component_index, {gate} AS gate_pass, {policy_id} AS policy_id "
            f"FROM {resolution} r "
            f"LEFT JOIN (SELECT * FROM {table} WHERE is_current) p ON p.map_method = r.map_method")
