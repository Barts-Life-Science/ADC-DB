"""Silver spine.practitioner_location_evidence: 1 dataset(s).

- datasets: reference_practitioner_location_evidence
- moved verbatim from: 1_silver_spine_and_reference:6767, 1_silver_spine_and_reference:6787, 1_silver_spine_and_reference:6790, 1_silver_spine_and_reference:6795, 1_silver_spine_and_reference:6816

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _clamped_ts, _n, read_source, stable_id


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / practitioner location evidence
SRC_PRSNL_LOC_EVIDENCE = "4_prod.bronze.map_medical_personnel_location_evidence"


PRACTITIONER_LOCATION_EVIDENCE_LIFECYCLE_FIELDS = [
]


PRACTITIONER_LOCATION_EVIDENCE_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_practitioner_location_evidence():
    # Assemble practitioner location evidence rows with lifecycle and source evidence for the
    # public product and its internal metadata.
    s = read_source(SRC_PRSNL_LOC_EVIDENCE)
    # contract v2: rename the evidence hash as a key and publish native practitioner_id
    return s.select(
        stable_id("practitioner_location_evidence:mill", s.PERSON_ID, s.LOC_NURSE_UNIT_CD)
         .alias("practitioner_location_evidence_key"),
        s.PERSON_ID.cast("bigint").alias("practitioner_id"),
        s.PERSON_ID.cast("string").alias("practitioner_person_id"),
        s.LOC_NURSE_UNIT_CD.cast("string").alias("location_code"),
        s.EVENT_COUNT.alias("event_count"), s.FIRST_EVENT_DT_TM.alias("first_event_datetime_raw"),
        _clamped_ts(s.FIRST_EVENT_DT_TM).alias("first_event_datetime"),
        s.LAST_EVENT_DT_TM.alias("last_event_datetime_raw"),
        _clamped_ts(s.LAST_EVENT_DT_TM).alias("last_event_datetime"),
        s.LOCATION_RANK.alias("location_rank"), s.TOP_COUNT_TIE_COUNT.alias("top_count_tie_count"),
        s.EVIDENCE_ADC_UPDT.alias("loaded_at"), F.lit("millennium").alias("_source_system"),
        F.lit(SRC_PRSNL_LOC_EVIDENCE).alias("_source_table"),
        F.concat_ws("|", s.PERSON_ID, s.LOC_NURSE_UNIT_CD).alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.practitioner_location_evidence"),
    cluster_by=["practitioner_id"], refresh_policy="auto",
)
def practitioner_location_evidence():
    # Build the declared dataset: Staff location evidence with raw and sentinel-clamped event
    # bounds.
    return _lifecycle_source_practitioner_location_evidence().drop(*PRACTITIONER_LOCATION_EVIDENCE_LIFECYCLE_FIELDS, *PRACTITIONER_LOCATION_EVIDENCE_RETIRED_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_practitioner_location_evidence': {'columns': {'_source_row_id': 'Observed nurse-unit code.',
                                                          '_source_system': 'Value describing source system '
                                                                            'for the practitioner location '
                                                                            'evidence record. It is produced '
                                                                            'by the silver transformation '
                                                                            'and has no direct bronze-column '
                                                                            'lineage entry. Whitespace and '
                                                                            'source sentinel text are '
                                                                            'retained unless the pipeline '
                                                                            'explicitly normalizes them; '
                                                                            'null means no value was '
                                                                            'supplied.',
                                                          '_source_table': 'Value describing source table '
                                                                           'for the practitioner location '
                                                                           'evidence record. It is produced '
                                                                           'by the silver transformation and '
                                                                           'has no direct bronze-column '
                                                                           'lineage entry. Whitespace and '
                                                                           'source sentinel text are '
                                                                           'retained unless the pipeline '
                                                                           'explicitly normalizes them; null '
                                                                           'means no value was supplied.',
                                                          'event_count': 'Current canonical logical-event '
                                                                         'count at this location.',
                                                          'first_event_datetime': 'Earliest supporting event '
                                                                                  'timestamp.',
                                                          'first_event_datetime_raw': 'Earliest supporting '
                                                                                      'event timestamp.',
                                                          'last_event_datetime': 'Latest supporting event '
                                                                                 'timestamp.',
                                                          'last_event_datetime_raw': 'Latest supporting '
                                                                                     'event timestamp.',
                                                          'loaded_at': 'map_medical_personnel_location_evidence.EVIDENCE_ADC_UPDT '
                                                                       'carried unchanged. It is the '
                                                                       'supplied supporting-evidence clock, '
                                                                       'not FIRST_EVENT_DT_TM, '
                                                                       'LAST_EVENT_DT_TM or Silver refresh '
                                                                       'time; Silver performs no further '
                                                                       'timestamp aggregation or clamping on '
                                                                       'this field.',
                                                          'location_code': 'Observed nurse-unit code.',
                                                          'location_rank': 'Deterministic personnel location '
                                                                           'rank.',
                                                          'practitioner_id': 'Native Millennium personnel '
                                                                             'PERSON_ID as BIGINT; joins to '
                                                                             'reference_practitioner.practitioner_id.',
                                                          'practitioner_location_evidence_key': 'Deterministic '
                                                                                                'SHA-256 key '
                                                                                                'for '
                                                                                                'practitioner-location '
                                                                                                'evidence.',
                                                          'practitioner_person_id': 'Source-system '
                                                                                    'identifier for the '
                                                                                    'practitioner person '
                                                                                    'associated with each '
                                                                                    'practitioner location '
                                                                                    'evidence record. It is '
                                                                                    'derived from bronze '
                                                                                    'field `PERSON_ID` in '
                                                                                    '`4_prod.bronze.map_medical_personnel_location_evidence`. '
                                                                                    'Its uniqueness is '
                                                                                    'limited to the '
                                                                                    'originating source '
                                                                                    'namespace; null means '
                                                                                    'no identifier was '
                                                                                    'supplied.',
                                                          'top_count_tie_count': 'Number of locations tied '
                                                                                 'on the maximum event '
                                                                                 'count.'},
                                              'table': 'Staff location evidence with raw and '
                                                       'sentinel-clamped event bounds.'}}
