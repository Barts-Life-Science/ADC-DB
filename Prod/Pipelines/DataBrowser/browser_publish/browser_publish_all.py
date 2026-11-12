# Databricks notebook source
# MAGIC %md
# MAGIC # Browser publish — every dataset, prod, in one run
# MAGIC
# MAGIC Publishes the data browser's datasets to `d_databrowser` and verifies each one, using the
# MAGIC same `browser_publish_registry` and `browser_publish_verify` notebooks in this folder that a
# MAGIC by-hand publish uses. For each dataset, in order:
# MAGIC
# MAGIC 1. **Publish** (`registry_action=publish`, `env=prod`): staged views, column policy from the
# MAGIC    IG tags, profiles, key and join checks, then the live views, grants and registry row.
# MAGIC 2. **Verify**: every gate must pass, or the dataset is reported failed.
# MAGIC 3. **Report** the verified release's summary. Nothing is exported: the VM-side generators read the
# MAGIC    registry's latest verified release (`d_databrowser.meta.dataset_registry`) directly.
# MAGIC
# MAGIC **Safe to re-run.** A dataset whose newest prod release is from `since` onwards and already
# MAGIC verified is skipped (its summary is reported); one published but not verified is only
# MAGIC verified. One dataset failing does not stop the others; the run fails at the end, listing it.
# MAGIC
# MAGIC **Start with `mode=plan`** (the default): it prints what each dataset would get and touches
# MAGIC nothing. Then run with `mode=run`.
# MAGIC
# MAGIC **Time and cost.** Research (`gold`) is a full publish of ~150 tables, about 70 minutes; the
# MAGIC others take 10–25 minutes each (OMOP's dev run took 22). All of it is SQL on this notebook's
# MAGIC compute; nothing is recomputed in the source pipelines.
# MAGIC
# MAGIC **Tables waiting for IG tags** are left out of a release and listed in the summary
# MAGIC (`waiting_for_tags`). Tag them through Table Metadata Compliance, then a targeted republish
# MAGIC (`browser_publish_registry`, `registry_action=republish`) adds them.

# COMMAND ----------
# MAGIC %run ./browser_publish_config

# COMMAND ----------
import json
import traceback
from datetime import datetime, timezone

dbutils.widgets.text("datasets", "gold,rbi,pms_pharma,pms_device,omop",
                     "datasets, in publish order")
dbutils.widgets.dropdown("mode", "plan", ["plan", "run"], "plan = report only")
dbutils.widgets.text("since", "2026-10-29",
                     "a verified release from this date on counts as done")
# INSR_W7 W7.10: {dataset: [tables]} to republish on the current verified
# release instead of a full publish (merge_release); {} = full publishes
dbutils.widgets.text("republish_tables", "{}", "JSON {dataset: [tables]}")
# INSR_W7 W7.10: a source column without IG tags makes its table leave the
# release; the run stops on any unless this is yes
dbutils.widgets.dropdown("allow_untagged", "no", ["no", "yes"])

DATASETS = [d.strip() for d in dbutils.widgets.get("datasets").split(",") if d.strip()]
MODE = dbutils.widgets.get("mode")
SINCE = dbutils.widgets.get("since").strip()
REPUBLISH = json.loads(dbutils.widgets.get("republish_tables") or "{}")
ALLOW_UNTAGGED = dbutils.widgets.get("allow_untagged") == "yes"
assert set(REPUBLISH) <= set(DATASETS), f"republish_tables names {sorted(set(REPUBLISH) - set(DATASETS))}"

unknown = sorted(set(DATASETS) - set(PROFILES))
assert not unknown, f"unknown datasets {unknown}; known: {sorted(PROFILES)}"
assert len(set(DATASETS)) == len(DATASETS), "a dataset is listed twice"

# Every dataset shares the prod ledgers (registry and receipts).
_PROD = targets_for(PROFILES[DATASETS[0]], "prod")
REGISTRY = f"{_PROD['catalog']}.{_PROD['meta']}.dataset_registry"
RECEIPTS = f"{_PROD['catalog']}.{_PROD['app']}.run_receipt"
TAG = "/* dbrowse ctx=system u=publish-all feature=publish-all */ "

# Generous ceilings: a stuck child fails its dataset rather than the whole run.
TIMEOUT_SECONDS = {"gold": 4 * 3600}
DEFAULT_TIMEOUT_SECONDS = 2 * 3600
STARTED_AT = datetime.now(timezone.utc).isoformat()
print(f"mode={MODE} since={SINCE} datasets={DATASETS}")

# INSR_R4_RELEASE_GATE_V1 (design R4 "Release boundary"): gold tables are views over silver, so a publish reads
# only a closed release. The browser keeps its last published state; re-run after the release closes.
if MODE == "run" and spark.catalog.tableExists("6_mgmt.data_quality.release_state"):
    _r4_open = [r["release_id"] for r in spark.sql("SELECT release_id FROM `6_mgmt`.data_quality.release_state WHERE status = 'open'").collect()]
    if _r4_open:
        raise RuntimeError(f"RELEASE_OPEN: {_r4_open} is open; publish after it closes (INSR R4 release marker)")

# COMMAND ----------
# Reading the prod ledgers


def newest_release(dataset):
    """The dataset's newest prod registry row with the status of its latest
    verify receipt (None when never verified), or None when the dataset has
    no prod release at all. This is what release_step decides from."""
    row = spark.sql(TAG + f"""
        SELECT run_id, published_at FROM {REGISTRY}
        WHERE dataset = '{dataset}'
        ORDER BY published_at DESC LIMIT 1
    """).first()
    if row is None:
        return None
    receipt = spark.sql(TAG + f"""
        SELECT status FROM {RECEIPTS}
        WHERE dataset = '{dataset}' AND run_id = '{row['run_id']}'
          AND kind = 'verify'
        ORDER BY run_at DESC LIMIT 1
    """).first()
    return {"run_id": row["run_id"], "published_at": row["published_at"],
            "verify_status": receipt["status"] if receipt else None}


def schema_doc_json(dataset, run_id):
    """The release's schema doc, as stored on its registry row."""
    return spark.sql(TAG + f"""
        SELECT schema_doc FROM {REGISTRY}
        WHERE dataset = '{dataset}' AND run_id = '{run_id}'
    """).first()["schema_doc"]


def latest_failure(dataset):
    """The error text of this run's newest failed receipt for the dataset,
    so a failure is explained here rather than only in the child's output."""
    row = spark.sql(TAG + f"""
        SELECT kind, receipt FROM {RECEIPTS}
        WHERE dataset = '{dataset}' AND status = 'failed'
          AND run_at >= '{STARTED_AT}'
        ORDER BY run_at DESC LIMIT 1
    """).first()
    if row is None:
        return None
    receipt = json.loads(row["receipt"])
    failed_gates = [g["gate"] for g in receipt.get("gates", []) if not g["passed"]]
    return {"kind": row["kind"], "failed_gates": failed_gates,
            "error": (receipt.get("error") or "")[-3000:]}

# COMMAND ----------
# Running the publish and verify notebooks


def run_child(notebook, dataset, args):
    """Run a sibling notebook for one dataset and return its exit value."""
    timeout = TIMEOUT_SECONDS.get(dataset, DEFAULT_TIMEOUT_SECONDS)
    print(f"  {notebook} {args}")
    result = dbutils.notebook.run(f"./{notebook}", timeout, args)
    return json.loads(result) if result else {}


def publish(dataset):
    """A full prod publish; returns the new release's run id."""
    result = run_child("browser_publish_registry", dataset,
                       {"env": "prod", "dataset": dataset,
                        "registry_action": "publish"})
    return result["run_id"]


def republish(dataset, tables, parent_run_id):
    """INSR_W7 W7.10: a targeted republish of `tables` on the dataset's
    current verified release (every other table is carried); returns the new
    run id."""
    result = run_child("browser_publish_registry", dataset,
                       {"env": "prod", "dataset": dataset,
                        "registry_action": "republish",
                        "tables": ",".join(tables),
                        "parent_run_id": parent_run_id})
    return result["run_id"]


def untagged_columns(dataset):
    """INSR_W7 W7.10: 'table.column' for each public source column with no
    ig_risk tag (its table would wait out of the release). One
    information_schema read."""
    p = PROFILES[dataset]
    internal = ", ".join(f"'{c}'" for c in INTERNAL_COLUMNS)
    rows = spark.sql(TAG + f"""
        SELECT c.table_name, c.column_name
        FROM {p.source_catalog}.information_schema.columns c
        LEFT JOIN {p.source_catalog}.information_schema.column_tags t
          ON t.schema_name = c.table_schema AND t.table_name = c.table_name
         AND t.column_name = c.column_name AND t.tag_name = 'ig_risk'
        WHERE c.table_schema = '{p.source_schema}' AND t.tag_value IS NULL
          AND left(c.table_name, 1) <> '_'
          AND c.column_name NOT IN ({internal})
    """).collect()
    skipped = tuple(p.skip_prefixes)
    return sorted(f"{r['table_name']}.{r['column_name']}" for r in rows
                  if not r["table_name"].startswith(skipped)
                  and r["table_name"] not in p.skip_tables)


def verify(dataset, run_id):
    """Run every verify gate on the release (raises if any fails)."""
    run_child("browser_publish_verify", dataset,
              {"env": "prod", "dataset": dataset, "run_id": run_id,
               "verify_action": "verify"})


def release_summary(dataset, run_id):
    """Summary line for a verified release, read from its registry row. Nothing is written: the VM-side generators
    read the registry's latest verified release directly (no exported files, 2026-11-11)."""
    envelope = verified_release_envelope(dataset, run_id,
                                         schema_doc_json(dataset, run_id))
    doc = envelope["schema_doc"]
    return {"run_id": run_id, "tables": len(doc.get("tables", {})),
            "waiting_for_tags": doc.get("waiting_for_tags") or {},
            "excluded_unreadable": doc.get("excluded_unreadable") or [],
            "excluded_empty": doc.get("excluded_empty") or []}

# COMMAND ----------
# One dataset at a time, in the order given


def process(dataset):
    """Bring one dataset to a verified prod release (or, in plan
    mode, say what that would take)."""
    newest = newest_release(dataset)
    step = release_step(newest, since=SINCE)
    current = (f"newest {newest['run_id']} ({newest['published_at']}, "
               f"verify={newest['verify_status']})" if newest else "never published")
    print(f"\n== {dataset}: {step} — {current}")
    gaps = untagged_columns(dataset)  # INSR_W7 W7.10
    if gaps:
        print(f"  {len(gaps)} untagged source columns, e.g. {gaps[:10]}")
    tables = REPUBLISH.get(dataset)
    if MODE == "plan":
        return {"status": "planned", "step": step, "current": current,
                "untagged_columns": len(gaps), "republish_tables": tables}
    if step == "publish" and gaps and not ALLOW_UNTAGGED:
        raise RuntimeError(f"{dataset}: {len(gaps)} source columns lack IG tags; "
                           f"their tables would leave the release: {gaps[:20]}")

    run_id = newest["run_id"] if newest else None
    if step == "publish":
        verified_parent = newest is not None and newest["verify_status"] == "ok"
        run_id = (republish(dataset, tables, newest["run_id"])
                  if tables and verified_parent else publish(dataset))
    if step in ("publish", "verify"):
        verify(dataset, run_id)
    return {"status": "ok", "step": step, **release_summary(dataset, run_id)}


results = {}
for dataset in DATASETS:
    try:
        results[dataset] = process(dataset)
    except Exception as exc:
        results[dataset] = {"status": "failed",
                            "exception": str(exc)[-1500:],
                            "receipt": latest_failure(dataset)}
        traceback.print_exc()
    print(json.dumps({dataset: results[dataset]}, indent=2, default=str))

# COMMAND ----------
# Summary

print(json.dumps(results, indent=2, default=str))
failed = [d for d, r in results.items() if r["status"] == "failed"]
waiting = {d: sorted(r.get("waiting_for_tags", {})) for d, r in results.items()
           if r.get("waiting_for_tags")}
if waiting:
    print(f"Tables waiting for IG tags (left out of their release): {waiting}")
if failed:
    raise RuntimeError(f"datasets failed: {failed} — see the summary above")
# INSR_W7 W7.10: in plan mode an untagged source column is a failure INSR_Apply can read (its notebook step fails
# on a non-empty "fails"), so P10's plan step stops the release before any dataset is published.
summary = {d: {k: r.get(k) for k in ("status", "step", "run_id", "tables", "untagged_columns")}
           for d, r in results.items()}
fails = {d: r["untagged_columns"] for d, r in results.items()
         if MODE == "plan" and r.get("untagged_columns") and not ALLOW_UNTAGGED}
dbutils.notebook.exit(json.dumps({**summary, "fails": fails}, default=str))

