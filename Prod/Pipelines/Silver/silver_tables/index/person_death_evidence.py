"""Silver index.person_death_evidence: 1 dataset(s).

- datasets: reference_person_death_evidence
- moved verbatim from: 1_silver_spine_and_reference:6272, 1_silver_spine_and_reference:6301, 1_silver_spine_and_reference:6304, 1_silver_spine_and_reference:6309, 1_silver_spine_and_reference:6356

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _clamped_ts, _n, read_source, spark, stable_id
from silver_tables.spine._shared import SRC_PERSON


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / person death evidence
SRC_DEATH = "4_prod.bronze.map_death"


PERSON_DEATH_EVIDENCE_LIFECYCLE_FIELDS = [
]


PERSON_DEATH_EVIDENCE_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_person_death_evidence():
    # Assemble person death evidence rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    s = read_source(SRC_DEATH)
    # INSR_W2 W2.06 (L5145): the source AGE_AT_DEATH is constant 0; age is derived from birth and death dates.
    birth = read_source(SRC_PERSON).select(F.col("person_id").cast("bigint").alias("_d_pid"),
                                           F.col("birth_date").alias("_d_birth"))
    s = s.join(birth, s.PERSON_ID.cast("bigint") == F.col("_d_pid"), "left")
    # INSR_W2 W2.09 (L5145): bronze LAST_CLINICAL_EVENT_DT_TM counts administrative and bulk events (38.5% fall
    # after death outside the bulk windows); the derived value takes clinical-category feed events only.
    scope = spark.read.table(_n("journey_reference.person_scope")).select(
        F.col("person_id").alias("_s_pid"), F.col("last_clinical_event_datetime").alias("_s_last_clin"),
        F.col("last_activity_date").alias("_s_last_act"))  # BBR_FOLDIN_V1 BBR-15
    s = s.join(scope, s.PERSON_ID.cast("bigint") == F.col("_s_pid"), "left")
    age = F.floor(F.months_between(F.coalesce(_clamped_ts(s.DECEASED_DT_TM), s.CALC_DEATH_DATE.cast("timestamp")),
                                   F.col("_d_birth").cast("timestamp")) / 12).cast("int")
    # contract v2: rename the deterministic death-evidence identity as a key and publish native person_id
    return s.select(
        stable_id("person_death_evidence:mill", s.PERSON_ID).alias("person_death_evidence_key"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        s.DECEASED_DT_TM.alias("deceased_datetime_raw"),
        _clamped_ts(s.DECEASED_DT_TM).alias("deceased_datetime"),
        s.CALC_DEATH_DATE.alias("calculated_death_date"),
        s.DECEASED_DT_TM_PREC_FLAG.alias("precision_flag"),
        s.DECEASED_DT_TM_PREC_DESC.alias("precision_desc"),
        s.DECEASED_SOURCE_DESC.alias("source_desc"),
        s.DECEASED_METHOD_DESC.alias("method_desc"),
        s.DEATH_DATE_ESTIMATE_SOURCE.alias("death_date_estimate_source"),
        s.CAUSE_OF_DEATH.alias("cause_of_death"), s.AUTOPSY_DESC.alias("autopsy_desc"),
        F.when(age.between(0, 120), age).alias("age_at_death"),  # INSR_W2 W2.06: derived, NULL if invalid
        s.AGE_AT_DEATH.alias("age_at_death_raw"), s.LAST_ENCNTR_DT_TM.alias("last_encounter_datetime"),
        s.LAST_CLINICAL_EVENT_DT_TM.alias("last_clinical_event_datetime"),
        F.col("_s_last_clin").alias("last_clinical_event_datetime_derived"),  # INSR_W2 W2.09
        F.coalesce(F.col("_s_last_clin") > F.date_add(F.coalesce(_clamped_ts(s.DECEASED_DT_TM),
                   s.CALC_DEATH_DATE.cast("timestamp")), 1), F.lit(False)).alias("post_death_clinical_activity_ind"),
        # BBR_FOLDIN_V1 (BBR-15): bronze has no last activity for most persons; the person-scope last event date
        # fills it (the censor date for living persons), and the basis says which one it is.
        F.coalesce(s.LAST_KNOWN_ACTIVITY_DT_TM, F.col("_s_last_act").cast("timestamp"))
         .alias("last_known_activity_datetime"),
        F.when(s.LAST_KNOWN_ACTIVITY_DT_TM.isNotNull(), F.lit("bronze_last_known_activity"))
         .when(F.col("_s_last_act").isNotNull(), F.lit("person_scope_last_activity_date"))
         .alias("last_known_activity_basis"),
        s.CLINICAL_EVENT_COUNT.alias("clinical_event_count"),
        F.lit("active").alias("record_status"), s.ADC_UPDT.alias("loaded_at"), F.lit("millennium").alias("_source_system"),
        F.lit(SRC_DEATH).alias("_source_table"), s.PERSON_ID.cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.person_death_evidence"),
    cluster_by=["person_id"], refresh_policy="auto",
)
def person_death_evidence():
    # Build the declared dataset: Corroborating death-register evidence; person-spine deceased
    # fields remain authoritative.
    return _lifecycle_source_person_death_evidence().drop(*PERSON_DEATH_EVIDENCE_LIFECYCLE_FIELDS, *PERSON_DEATH_EVIDENCE_RETIRED_COLUMNS)


VALUE_POLICIES = {
    'reference_person_death_evidence': ValuePolicy(table='reference_person_death_evidence', enabled=True),
}


WRAP_POLICY = {'reference_person_death_evidence'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_person_death_evidence': {'columns': {'_source_row_id': 'This is the value of the unique primary '
                                                                   'identifier of the person table. It is an '
                                                                   'internal system assigned number.',
                                                 '_source_system': 'Value describing source system for the '
                                                                   'person death evidence record. It is '
                                                                   'produced by the silver transformation '
                                                                   'and has no direct bronze-column lineage '
                                                                   'entry. Whitespace and source sentinel '
                                                                   'text are retained unless the pipeline '
                                                                   'explicitly normalizes them; null means '
                                                                   'no value was supplied.',
                                                 '_source_table': 'Value describing source table for the '
                                                                  'person death evidence record. It is '
                                                                  'produced by the silver transformation and '
                                                                  'has no direct bronze-column lineage '
                                                                  'entry. Whitespace and source sentinel '
                                                                  'text are retained unless the pipeline '
                                                                  'explicitly normalizes them; null means no '
                                                                  'value was supplied.',
                                                 'age_at_death': 'Whole years from birth_date to the death '
                                                                 'date (deceased_datetime, else '
                                                                 'calculated_death_date); NULL outside 0-120 '
                                                                 'or when either date is missing. INSR_W2 '
                                                                 'W2.06.',
                                                 'age_at_death_raw': 'Source AGE_AT_DEATH exactly as '
                                                                     'supplied (constant 0 at source, '
                                                                     'P7_L5145).',
                                                 'autopsy_desc': 'Resolved autopsy code description/display.',
                                                 'calculated_death_date': 'Calendar date associated with '
                                                                          'calculated death for the person '
                                                                          'death evidence record. It is '
                                                                          'derived from bronze field '
                                                                          '`CALC_DEATH_DATE` in '
                                                                          '`4_prod.bronze.map_death`. The '
                                                                          'value has no time-of-day '
                                                                          'component; null means the source '
                                                                          'date was unavailable.',
                                                 'cause_of_death': 'Raw free-text cause of death.',
                                                 'clinical_event_count': 'Count of selected latest logical '
                                                                         'clinical-event source states for '
                                                                         'the person.',
                                                 'death_date_estimate_source': 'RECORDED_DEATH, '
                                                                               'LAST_ENCOUNTER, '
                                                                               'LAST_CLINICAL_EVENT or NONE.',
                                                 'deceased_datetime': 'Date and time of death.',
                                                 'deceased_datetime_raw': 'Date and time of death.',
                                                 'last_clinical_event_datetime': 'Latest EVENT_END_DT_TM '
                                                                                 'across selected latest '
                                                                                 'source versions, without '
                                                                                 'result-status filtering.',
                                                 'last_clinical_event_datetime_derived': 'Latest '
                                                                                         'clinical-category '
                                                                                         'event across the '
                                                                                         'silver event feeds '
                                                                                         '(person_scope); '
                                                                                         'excludes the '
                                                                                         'administrative and '
                                                                                         'bulk events that '
                                                                                         'bronze '
                                                                                         'LAST_CLINICAL_EVENT_DT_TM '
                                                                                         'counts (L5145). '
                                                                                         'INSR_W2.',
                                                 'last_encounter_datetime': 'It represents the timestamp of '
                                                                            'the last encounter associated '
                                                                            'with a deceased individual.',
                                                 'last_known_activity_basis': 'Which value '
                                                                              'last_known_activity_datetime '
                                                                              'holds: '
                                                                              'bronze_last_known_activity '
                                                                              '(map_death '
                                                                              'LAST_KNOWN_ACTIVITY_DT_TM) or '
                                                                              'person_scope_last_activity_date '
                                                                              '(the latest event date across '
                                                                              'the event feeds, '
                                                                              'reference_person_scope, when '
                                                                              'bronze has none). '
                                                                              'BBR_FOLDIN_V1 BBR-15.',
                                                 'last_known_activity_datetime': 'Greatest of '
                                                                                 'LAST_ENCNTR_DT_TM and '
                                                                                 'LAST_NON_ERROR_CLINICAL_EVENT_DT_TM; '
                                                                                 'when bronze has none, the '
                                                                                 'latest event date across '
                                                                                 'the event feeds '
                                                                                 '(reference_person_scope.last_activity_date, '
                                                                                 'at midnight); see '
                                                                                 'last_known_activity_basis. '
                                                                                 'This is activity, not a '
                                                                                 'confirmed death timestamp. '
                                                                                 'BBR_FOLDIN_V1 BBR-15.',
                                                 'loaded_at': 'map_death.ADC_UPDT carried unchanged for the '
                                                              'contributing death-evidence row. This is '
                                                              'bronze load provenance, not '
                                                              'recorded/calculated death time, last-known '
                                                              'activity or Silver refresh time.',
                                                 'method_desc': 'Description of the code.',
                                                 'person_death_evidence_key': 'Deterministic SHA-256 key for '
                                                                              'the person death-evidence '
                                                                              'row.',
                                                 'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                                 'post_death_clinical_activity_ind': 'True when a '
                                                                                     'clinical-category '
                                                                                     'event falls more than '
                                                                                     'one day after the '
                                                                                     'death date. INSR_W2.',
                                                 'precision_desc': 'Decoded death timestamp precision: '
                                                                   'unknown, full date/time, month, year or '
                                                                   'day.',
                                                 'precision_flag': 'Source precision flag for '
                                                                   'DECEASED_DT_TM.',
                                                 'record_status': 'Always active in this projection. The '
                                                                  'label is a literal, not derived from '
                                                                  'death evidence, source presence, clinical '
                                                                  'activity or effective dates; it does not '
                                                                  'assert that the person is alive.',
                                                 'source_desc': 'Description of the code.'},
                                     'table': 'Corroborating death-register evidence; person-spine deceased '
                                              'fields remain authoritative.'}}
