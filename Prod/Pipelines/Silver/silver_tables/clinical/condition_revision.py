"""Silver clinical.condition_revision: 1 dataset(s).

- datasets: clinical_condition_revision
- moved verbatim from: 2_silver_clinical_events:1211, 2_silver_clinical_events:1235, 2_silver_clinical_events:1282, 2_silver_clinical_events:1320

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables._common.journey import _condition_problem_canonical
from silver_tables.clinical._shared import SRC_PROBLEM_HISTORY


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / condition revision

# ==== journey_clinical.condition_revision ====

# contract v2: retain the full canonical shape for internal reuse.
CONDITION_REVISION_SOURCE_COLUMNS = [
    "patient_event_key",
    "problem_id",
    "sequence",
    "revision_rank",
    "is_current_revision",
    "clinical_status_code",
    "clinical_status_display",
    "clinical_status_datetime",
    "verification_status_code",
    "verification_status_display",
    "display",
    "severity_code",
    "severity_display",
    "onset_datetime",
    "asserted_datetime",
    "effective_start",
    "effective_end",
    "source_tombstone_ind",
    "source_update_timestamp",
    "loaded_at",
]


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
CONDITION_REVISION_PUBLIC_COLUMNS = [
    'patient_event_key',
    'problem_id',
    'sequence',
    'revision_rank',
    'is_current_revision',
    'clinical_status_code',
    'clinical_status_display',
    'clinical_status_datetime',
    'verification_status_code',
    'verification_status_display',
    'display',
    'severity_code',
    'severity_display',
    'onset_datetime',
    'asserted_datetime',
    'effective_start',
    'effective_end',
    'source_tombstone_ind',
    'loaded_at',
    'source_update_timestamp',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_condition_revision():
    # Assemble condition revision rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    s = read_source(SRC_PROBLEM_HISTORY)
    # Contract v2 parent/child conservation: clinical_condition publishes only the
    # problems whose code-or-display passes Gate B, so the revision child applies
    # the same gate. Taking the eligible ids from the parent's own canonical
    # expression keeps the child from drifting if that rule ever changes. Without
    # it the child carried 22 rows that no reader could reach through the parent.
    current = (
        _condition_problem_canonical()
        .select(F.col("problem_id").alias("PROBLEM_ID"))
        .distinct()
    )
    s = s.join(current, "PROBLEM_ID", "inner")
    return s.select(
        stable_id("condition:mill:problem", s.PROBLEM_ID).alias("patient_event_key"),
        s.PROBLEM_ID.cast("bigint").alias("problem_id"),
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
        s.ADC_UPDT.alias("loaded_at"),
    ).select(*CONDITION_REVISION_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_clinical.condition_revision"),
    refresh_policy="auto",
)
def condition_revision():
    # Build the declared dataset: One Millennium problem-history revision per parent
    # clinical_condition problem row.
    return _lifecycle_source_condition_revision().select(*CONDITION_REVISION_PUBLIC_COLUMNS)


VALUE_POLICIES = {
    'clinical_condition_revision': ValuePolicy(table='clinical_condition_revision', enabled=True, zero_as_null=('severity_code', 'verification_status_code')),
}


WRAP_POLICY = {'clinical_condition_revision'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_condition_revision': {'columns': {'asserted_datetime': 'Source assertion timestamp carried by '
                                                                  'this revision.',
                                             'clinical_status_code': 'Source lifecycle-status code for this '
                                                                     'revision.',
                                             'clinical_status_datetime': 'Source lifecycle-status timestamp '
                                                                         'for this revision.',
                                             'clinical_status_display': 'Source lifecycle-status display for '
                                                                        'this revision.',
                                             'display': 'Best available source problem display for this '
                                                        'revision.',
                                             'effective_end': 'Source revision effective-end timestamp.',
                                             'effective_start': 'Source revision effective-start timestamp.',
                                             'is_current_revision': 'Whether the source marks this as the '
                                                                    'current problem revision.',
                                             'loaded_at': 'Bronze load timestamp of the problem-history row.',
                                             'onset_datetime': 'Source onset timestamp carried by this '
                                                               'revision.',
                                             'patient_event_key': 'Deterministic key of the parent '
                                                                  'clinical_condition row.',
                                             'problem_id': 'Millennium PROBLEM_ID of the parent problem '
                                                           'condition.',
                                             'revision_rank': 'Source problem revision rank.',
                                             'sequence': 'Problem revision sequence, defaulted to zero when '
                                                         'the source rank is null.',
                                             'severity_code': 'Source severity code for this revision.',
                                             'severity_display': 'Source severity display for this revision.',
                                             'source_tombstone_ind': 'Whether the source revision is marked '
                                                                     'deleted.',
                                             'source_update_timestamp': 'Source application update datetime.',
                                             'verification_status_code': 'Source confirmation-status code '
                                                                         'for this revision.',
                                             'verification_status_display': 'Source confirmation-status '
                                                                            'display for this revision.'},
                                 'table': 'One Millennium problem-history revision per parent '
                                          'clinical_condition problem row.'}}
