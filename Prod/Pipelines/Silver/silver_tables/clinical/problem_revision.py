"""Silver clinical.problem_revision: 1 dataset(s).

- datasets: _clinical_problem_revision_grouped
- moved verbatim from: 2_silver_clinical_events:349, 2_silver_clinical_events:431, silver_journey_shared.py:2683, silver_journey_shared.py:2738

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables._common.journey import (
    SRC_PROBLEM,
    _condition_diagnosis_canonical,
    _condition_problem_canonical,
    _maternity_diagnosis_canonical,
)
from silver_tables.clinical._shared import SRC_PROBLEM_HISTORY


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# contract v2: retain the full canonical shape for internal reuse.
CONDITION_SOURCE_COLUMNS = [
    "patient_event_key",
    "source_object",
    "diagnosis_id",
    "problem_id",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "encounter_slice_id",  # ENCOUNTER_SLICE_V1
    "event_datetime",
    "event_end_datetime",
    "event_datetime_local",
    "event_end_datetime_local",
    "abatement_datetime_local",
    "source_coding_system",
    "source_code",
    "source_display",
    "condition_code", '_r1_condition_mapping', '_r1_condition_map_key', '_r1_condition_components_json',
    "category_code",
    "category_display",
    # DIAG_PRIORITY_V1: diagnosis ranking.
    "diag_priority",
    "clinical_diag_priority",
    "ranking_code",
    "ranking_display",
    "clinical_status_code",
    "clinical_status_display",
    "verification_status_code",
    "verification_status_display",
    "onset_datetime",
    "abatement_datetime",
    "body_site_code",
    "body_site_display",
    "severity_code",
    "severity_display",
    "laterality_code",
    "laterality_display",
    "asserted_datetime",
    "asserter_practitioner_id",
    "recorder_practitioner_id",
    "revision_history_count",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]


CONDITION_PRIMITIVE_COLUMNS = [
    {
        "condition_code": "_condition_code_json",
        "revision_history": "_revision_history_json",
        "revision_history_count": "_revision_history_count",
    }.get(name, name)
    for name in CONDITION_SOURCE_COLUMNS
]


def _problem_revision_grouped_query():
    # Group revision history for problem identifiers that are present in the current problem
    # source.
    s = read_source(SRC_PROBLEM_HISTORY)
    current = read_source(SRC_PROBLEM).select("PROBLEM_ID").distinct()
    s = s.join(current, "PROBLEM_ID", "inner")
    revision = F.struct(
        F.coalesce(s.PROBLEM_REVISION_RANK, F.lit(0)).cast("long").alias("sequence"),
        s.PROBLEM_REVISION_RANK.cast("long").alias("revision_rank"),
        s.IS_CURRENT_PROBLEM_REVISION.alias("is_current_revision"),
        s.LIFE_CYCLE_STATUS_CD.cast("string").alias("clinical_status_code"),
        s.life_cycle_status_desc.alias("clinical_status_display"),
        s.LIFE_CYCLE_DT_TM.alias("clinical_status_datetime"),
        s.CONFIRMATION_STATUS_CD.cast("string").alias("verification_status_code"),
        s.confirmation_status_desc.alias("verification_status_display"),
        F.coalesce(s.ANNOTATED_DISPLAY, s.PROBLEM_DISPLAY, s.SOURCE_STRING).alias("display"),
        s.SEVERITY_CD.cast("string").alias("severity_code"),
        s.severity_desc.alias("severity_display"),
        s.ONSET_DT_TM.alias("onset_datetime"),
        s.ASSERTED_DT_TM.alias("asserted_datetime"),
        s.BEG_EFFECTIVE_DT_TM.alias("effective_start"),
        s.END_EFFECTIVE_DT_TM.alias("effective_end"),
        F.coalesce(s.SOURCE_DELETED_IND, F.lit(False)).alias("source_tombstone_ind"),
        s.UPDT_DT_TM.alias("source_update_timestamp"),
    )
    history = (
        s.groupBy("PROBLEM_ID")
        .agg(
            F.to_json(F.sort_array(F.collect_list(revision))).alias("_revision_history_json"),
            F.count(F.lit(1)).cast("long").alias("_revision_history_count"),
            F.max(s.ADC_UPDT).alias("_evidence_loaded_at"),
        )
        .select(
            stable_id("condition:mill:problem", F.col("PROBLEM_ID"))
            .alias("_revision_condition_event_id"),
            F.col("_revision_history_json"),
            F.col("_revision_history_count"),
            F.col("_evidence_loaded_at"),
        )
    )
    # contract v2: join problem revision history by patient_event_key after fact_row_id removal
    problem = (
        _condition_problem_canonical()
        .join(
            history,
            F.col("patient_event_key") == history["_revision_condition_event_id"],
            "left",
        )
        .withColumn(
            "_revision_history_count",
            F.coalesce(F.col("_revision_history_count"), F.lit(0).cast("long")),
        )
        .withColumn(
            "loaded_at", F.greatest(F.col("loaded_at"), F.col("_evidence_loaded_at"))
        )
    )
    diagnosis = (
        _condition_diagnosis_canonical()
        .withColumn("_revision_history_json", F.lit(None).cast("string"))
        .withColumn("_revision_history_count", F.lit(0).cast("long"))
    )
    maternity = (
        _maternity_diagnosis_canonical()
        .withColumn("_revision_history_json", F.lit(None).cast("string"))
        .withColumn("_revision_history_count", F.lit(0).cast("long"))
    )
    # INSR_W2 W2.19: problem-arm columns, NULL on the diagnosis and maternity arms.
    carry = ['clinical_status_effective_datetime', 'onset_precision_flag', 'onset_precision_code']
    cols = list(CONDITION_PRIMITIVE_COLUMNS) + carry

    def with_carry(df):
        """Add the carried columns as typed NULLs where an arm has none."""
        for c in carry:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("timestamp" if c.endswith("datetime") else "string"))
        return df
    return (
        with_carry(diagnosis).select(*cols)
        .unionByName(with_carry(problem).select(*cols))
        .unionByName(with_carry(maternity).select(*cols))
    )


@materialized_view(
    name=_n("journey_clinical._problem_revision_grouped"),
    private=True,
    refresh_policy="auto",
)
def _problem_revision_grouped():
    # Build the declared dataset: Internal condition primitive with ordered problem-revision
    # history and deterministic JSON boundaries.
    return _problem_revision_grouped_query()


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_problem_revision_grouped': {'columns': {},
                                        'table': 'Internal condition primitive with ordered problem-revision '
                                                 'history and deterministic JSON boundaries.'}}
