"""Silver activity.theatre_case_milestone: 1 dataset(s).

- datasets: reference_theatre_case_milestone
- moved verbatim from: 4_silver_care_pathways:4734, 4_silver_care_pathways:4754, 4_silver_care_pathways:4757, 4_silver_care_pathways:4762, 4_silver_care_pathways:4784

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables.activity._shared import _theatre_parent_keys


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / theatre case milestone
SRC_THEATRE_TIMES = "4_prod.bronze.map_theatre_case_times"


THEATRE_CASE_MILESTONE_LIFECYCLE_FIELDS = [
]


THEATRE_CASE_MILESTONE_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_theatre_case_milestone():
    # Assemble theatre case milestone rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    s = read_source(SRC_THEATRE_TIMES).alias("s")
    d = s.join(_theatre_parent_keys(), s.SURG_CASE_ID == F.col("_parent_surg_case_id"), "left")
    # contract v2: rename deterministic theatre identities as keys and publish native person and encounter ids
    return d.select(
        stable_id("theatre_milestone:surginet", s.CASE_TIMES_ID).alias("theatre_case_milestone_key"),
        s.SURG_CASE_ID.cast("string").alias("theatre_case_key"),
        F.when(F.col("_parent_surg_case_id").isNotNull(), F.lit("linked"))
         .otherwise(F.lit("orphan")).alias("case_link_status"),
        s.TASK_ASSAY_CD.cast("string").alias("task_assay_code"),
        s.TASK_ASSAY_DESCRIPTION.alias("task_assay_description"),
        s.STAGE_DESCRIPTION.alias("stage_description"), s.CASE_TIME_DT_TM.alias("case_time_datetime"),
        s.CASE_TIME_DT_TM_QUALITY.alias("case_time_quality"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.ACTIVE_IND.alias("active_ind"), s.ADC_UPDT.alias("loaded_at"), F.lit("surginet").alias("_source_system"),
        F.lit(SRC_THEATRE_TIMES).alias("_source_table"),
        s.CASE_TIMES_ID.cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.theatre_case_milestone"),
    cluster_by=["person_id"], refresh_policy="auto",
)
def theatre_case_milestone():
    # Build the declared dataset: SurgiNet case-time milestones; source-case-time meaning is
    # dead upstream.
    return _lifecycle_source_theatre_case_milestone().drop(*THEATRE_CASE_MILESTONE_LIFECYCLE_FIELDS, *THEATRE_CASE_MILESTONE_RETIRED_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_theatre_case_milestone': {'columns': {'_source_row_id': 'S14 theatre activity; retained as a '
                                                                    'source-faithful bronze input or Journey '
                                                                    'standard-block field.',
                                                  '_source_system': 'Value describing source system for the '
                                                                    'theatre case milestone record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Whitespace and source sentinel '
                                                                    'text are retained unless the pipeline '
                                                                    'explicitly normalizes them; null means '
                                                                    'no value was supplied.',
                                                  '_source_table': 'Value describing source table for the '
                                                                   'theatre case milestone record. It is '
                                                                   'produced by the silver transformation '
                                                                   'and has no direct bronze-column lineage '
                                                                   'entry. Whitespace and source sentinel '
                                                                   'text are retained unless the pipeline '
                                                                   'explicitly normalizes them; null means '
                                                                   'no value was supplied.',
                                                  'active_ind': 'S14 theatre activity; retained as a '
                                                                'source-faithful bronze input or Journey '
                                                                'standard-block field.',
                                                  'case_link_status': 'S14 theatre feeder and Journey '
                                                                      'standard block; retained as a '
                                                                      'source-faithful bronze input or '
                                                                      'Journey standard-block field.',
                                                  'case_time_datetime': 'S14 theatre performed-milestone '
                                                                        'time after deterministic quality '
                                                                        'bounds.',
                                                  'case_time_quality': 'S14 theatre activity; retained as a '
                                                                       'source-faithful bronze input or '
                                                                       'Journey standard-block field.',
                                                  'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT.',
                                                  'loaded_at': 'map_theatre_case_times.ADC_UPDT carried '
                                                               'unchanged for the milestone row. The '
                                                               'parent-case existence join adds no '
                                                               'timestamp; this is bronze ingestion '
                                                               'provenance, not CASE_TIME_DT_TM or Silver '
                                                               'refresh time.',
                                                  'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                                  'stage_description': 'S14 theatre activity; retained as a '
                                                                       'source-faithful bronze input or '
                                                                       'Journey standard-block field.',
                                                  'task_assay_code': 'S14 theatre activity; retained as a '
                                                                     'source-faithful bronze input or '
                                                                     'Journey standard-block field.',
                                                  'task_assay_description': 'S14 theatre activity; retained '
                                                                            'as a source-faithful bronze '
                                                                            'input or Journey standard-block '
                                                                            'field.',
                                                  'theatre_case_key': 'Source-preserved theatre case key.',
                                                  'theatre_case_milestone_key': 'Deterministic SHA-256 key '
                                                                                'for the theatre milestone '
                                                                                'row.'},
                                      'table': 'SurgiNet case-time milestones; source-case-time meaning is '
                                               'dead upstream.'}}
