"""Gold forms._shared: 0 dataset(s).

- shared gold helpers (no datasets)
- moved verbatim from: 6_gold_forms:240

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, _s3_axis_columns, _s3_axis_gate_and_policy, _src, _with_parent_status, spark
from gold_core import gold_single_copy  # INSR_R4_GOLD_V1


# ==== S3b form response and smoking-status products ====
def _s3b_form_qc(table_name, axes, rules=None):  # INSR_R4_GOLD_V1: rules = the MV-tier table's GoldRules
    """Retain parent-admission evidence and prepare each form axis's mapping decision."""
    source = spark.read.table(_src(table_name))
    if rules is not None:  # INSR_R4_GOLD_V1: per-key admission (D4 reverted)
        source = gold_single_copy.with_admission(source, rules)
    parent = spark.read.table(_n("gold_clinical.form")).select("patient_event_key").distinct()
    df = _with_parent_status(source, parent, ["patient_event_key"])
    for axis in axes:
        required = _s3_axis_columns(axis)
        if any(name not in df.columns for name in required):
            continue
        item = {name[len(axis)+1:]: F.col(name) for name in required}
        gate, policy_id = (gold_single_copy.verdict(axis) if rules is not None  # INSR_R4_GOLD_V1
                           else _s3_axis_gate_and_policy(item))
        has_target = item["target_code"].isNotNull() | item["snomed_code"].isNotNull() | item["omop_concept_id"].isNotNull()
        df = (df.withColumn(f"_{axis}_gate_pass", F.when(has_target, gate))
              .withColumn(f"_{axis}_policy_id", F.when(has_target, policy_id)))
    return df
