# Databricks notebook source
# BrowserPublish / promote — human-gated 8_dev -> d_databrowser preflight.
#
# This notebook never performs the production publish itself. It verifies that
# a named dev release has an independent verify/ok receipt, then prints the
# explicit production commands an operator may choose to run.

# COMMAND ----------
# MAGIC %run ./browser_publish_config

# COMMAND ----------
import re

dbutils.widgets.dropdown("dataset", "gold", sorted(PROFILES))
DATASET = dbutils.widgets.get("dataset")
PROFILE = PROFILES[DATASET]
dbutils.widgets.text("dev_run_id", "", "verified dev run_id")
dbutils.widgets.text("prod_run_id", "", "production run_id to deactivate")
dbutils.widgets.dropdown("action", "none", ["none", "promote", "deactivate"])

DEV_RUN_ID = dbutils.widgets.get("dev_run_id")
PROD_RUN_ID = dbutils.widgets.get("prod_run_id")
ACTION = dbutils.widgets.get("action")
_DEV = targets_for(PROFILE, "dev")
_PROD = targets_for(PROFILE, "prod")
TAG = (f"/* dbrowse ctx=system u={DATASET}-promote "
       f"feature={DATASET}-promote */ ")


def preflight():
    """Refuse to promote a dev run that lacks independent verification."""
    assert re.fullmatch(r"[0-9a-fA-F]{32}", DEV_RUN_ID), \
        f"invalid dev_run_id {DEV_RUN_ID!r}"
    receipt = spark.sql(TAG + f"""
        SELECT status FROM {_DEV['catalog']}.{_DEV['app']}.run_receipt
        WHERE dataset = '{DATASET}' AND run_id = '{DEV_RUN_ID}'
          AND kind = 'verify'
        ORDER BY run_at DESC LIMIT 1
    """).first()
    assert receipt is not None and receipt["status"] == "ok", (
        f"dev run {DEV_RUN_ID} has no kind='verify' status='ok' receipt — "
        "run browser_publish_verify against the dev release first")
    print(f"dev run {DEV_RUN_ID} verified; ready for the human-gated publish")


def deactivate():
    """Make the latest production release invisible to registry consumers.

    This is deliberately a receipt append rather than a view drop: the
    owner's-rights views remain available for diagnosis, while model/index
    generation sees the latest verification state as non-ok and refuses it.
    """
    assert re.fullmatch(r"[0-9a-fA-F]{32}", PROD_RUN_ID), \
        f"invalid prod_run_id {PROD_RUN_ID!r}"
    spark.sql(TAG + f"""
        INSERT INTO {_PROD['catalog']}.{_PROD['app']}.run_receipt
            (dataset, run_id, run_at, kind, receipt, status)
        VALUES ('{DATASET}', '{PROD_RUN_ID}', CAST(current_timestamp() AS STRING),
                'verify', '{{"reason":"operator deactivation"}}',
                'disabled')
    """)
    print(f"deactivated {DATASET} production run {PROD_RUN_ID}; views retained")


if ACTION == "promote":
    preflight()
    print(f"Now run browser_publish_registry with env=prod, "
          f"dataset={DATASET}, registry_action=publish.")
    print(f"Then run browser_publish_verify with env=prod, "
          f"dataset={DATASET} and the NEW run_id.")
elif ACTION == "deactivate":
    deactivate()

