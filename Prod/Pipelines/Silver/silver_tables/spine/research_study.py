"""Silver spine.research_study: 1 dataset(s).

- datasets: reference_research_study
- moved verbatim from: 1_silver_spine_and_reference:5451, 1_silver_spine_and_reference:5480, 1_silver_spine_and_reference:5483, 1_silver_spine_and_reference:5488, 1_silver_spine_and_reference:5517

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / research study
SRC_RESEARCH_STUDY = "4_prod.bronze.map_research_study"


RESEARCH_STUDY_LIFECYCLE_FIELDS = [
]


RESEARCH_STUDY_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_research_study():
    # Assemble research study rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    s = read_source(SRC_RESEARCH_STUDY)
    # contract v2: rename the deterministic research-study identity as a key
    return s.select(
        stable_id("research_study:cerner", s.PROT_MASTER_ID).alias("research_study_key"),
        s.PROT_MASTER_ID.cast("string").alias("source_protocol_id"),
        s.STUDY_MNEMONIC.alias("study_mnemonic"), s.STUDY_MNEMONIC_KEY.alias("study_mnemonic_key"),
        s.PROT_TYPE_CD.cast("long").alias("protocol_type_code"),
        s.PROT_TYPE_DESC.alias("protocol_type_desc"),
        s.PROT_PHASE_CD.cast("long").alias("protocol_phase_code"),
        s.PROT_PHASE_DESC.alias("protocol_phase_desc"),
        s.PROT_STATUS_CD.cast("long").alias("protocol_status_code"),
        s.PROT_STATUS_DESC.alias("protocol_status_desc"),
        s.PROT_PURPOSE_CD.cast("long").alias("protocol_purpose_code"),
        s.PROT_PURPOSE_DESC.alias("protocol_purpose_desc"),
        s.PARENT_PROT_MASTER_ID.cast("string").alias("parent_protocol_id"),
        s.PREV_PROT_MASTER_ID.cast("string").alias("previous_protocol_id"),
        (s.PARENT_PROT_MASTER_ID == s.PROT_MASTER_ID).alias("root_protocol_ind"),
        s.BEG_EFFECTIVE_DT_TM.alias("beg_effective"), s.END_EFFECTIVE_DT_TM.alias("end_effective"),
        (s.END_EFFECTIVE_DT_TM >= F.lit("2100-01-01").cast("timestamp")).alias("open_ended_ind"),
        s.DISPLAY_IND.cast("long").alias("display_ind"), F.lit("active").alias("record_status"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("cerner-research").alias("_source_system"),
        F.lit(SRC_RESEARCH_STUDY).alias("_source_table"),
        s.PROT_MASTER_ID.cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.research_study"),
    refresh_policy="auto",
)
def research_study():
    # Build the declared dataset: Cerner research protocol amendments with parent and previous
    # protocol lineage.
    return _lifecycle_source_research_study().drop(*RESEARCH_STUDY_LIFECYCLE_FIELDS, *RESEARCH_STUDY_RETIRED_COLUMNS)


VALUE_POLICIES = {
    'reference_research_study': ValuePolicy(table='reference_research_study', enabled=True, hub_zero_key='source_protocol_id'),
}


WRAP_POLICY = {'reference_research_study'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_research_study': {'columns': {'_source_row_id': 'Source-system identifier for the source row '
                                                            'associated with each research study record. It '
                                                            'is derived from bronze field `PROT_MASTER_ID` '
                                                            'in `4_prod.bronze.map_research_study`. Its '
                                                            'uniqueness is limited to the originating source '
                                                            'namespace; null means no identifier was '
                                                            'supplied.',
                                          '_source_system': 'Value describing source system for the research '
                                                            'study record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Whitespace and source sentinel '
                                                            'text are retained unless the pipeline '
                                                            'explicitly normalizes them; null means no value '
                                                            'was supplied.',
                                          '_source_table': 'Value describing source table for the research '
                                                           'study record. It is produced by the silver '
                                                           'transformation and has no direct bronze-column '
                                                           'lineage entry. Whitespace and source sentinel '
                                                           'text are retained unless the pipeline explicitly '
                                                           'normalizes them; null means no value was '
                                                           'supplied.',
                                          'beg_effective': 'The date and time for which this table row '
                                                           'becomes effective.  Normally, this will be the '
                                                           'date and time the row is added, but could be a '
                                                           'past or future date and time.',
                                          'display_ind': 'This field is an indicator that tells whether the '
                                                         'protocol enrollment information for this protocol '
                                                         'will be displayed in the electronic medical '
                                                         'record.',
                                          'end_effective': 'The date/time after which the row is no longer '
                                                           'valid as active current data.  This may be '
                                                           'valued with the date that the row became '
                                                           'inactive.',
                                          'loaded_at': 'map_research_study.ADC_UPDT carried unchanged for '
                                                       'the protocol/amendment row. No parent or '
                                                       'previous-protocol timestamp is joined; this is '
                                                       'bronze ingestion provenance, not protocol validity '
                                                       'time or Silver refresh time.',
                                          'open_ended_ind': 'The date/time after which the row is no longer '
                                                            'valid as active current data.  This may be '
                                                            'valued with the date that the row became '
                                                            'inactive.',
                                          'parent_protocol_id': 'Source-system identifier for the parent '
                                                                'protocol associated with each research '
                                                                'study record. It is derived from bronze '
                                                                'field `PARENT_PROT_MASTER_ID` in '
                                                                '`4_prod.bronze.map_research_study`. Its '
                                                                'uniqueness is limited to the originating '
                                                                'source namespace; null means no identifier '
                                                                'was supplied.',
                                          'previous_protocol_id': 'Source-system identifier for the previous '
                                                                  'protocol associated with each research '
                                                                  'study record. It is derived from bronze '
                                                                  'field `PREV_PROT_MASTER_ID` in '
                                                                  '`4_prod.bronze.map_research_study`. Its '
                                                                  'uniqueness is limited to the originating '
                                                                  'source namespace; null means no '
                                                                  'identifier was supplied.',
                                          'protocol_phase_code': 'This field contains a code for the phase '
                                                                 'of the protocol.  Examples of phases would '
                                                                 'include, but not be limited to, phase 1, '
                                                                 'phase 1A, phase 1B, phase 2, phase 3, etc.',
                                          'protocol_phase_desc': 'The description for the code value',
                                          'protocol_purpose_code': 'This field contains a code identifying '
                                                                   'the purpose for the protocol/study.  '
                                                                   'Examples of purposes would include, but '
                                                                   'not be limited to, cancer control, '
                                                                   'epidemiology, etc.',
                                          'protocol_purpose_desc': 'The description for the code value',
                                          'protocol_status_code': 'This field contains a code identifying '
                                                                  'the status of the protocol/study: '
                                                                  'approved, open, suspended (temporarily '
                                                                  'closed to accrual), closed to accrual, '
                                                                  'closed and terminated.',
                                          'protocol_status_desc': 'The description for the code value',
                                          'protocol_type_code': 'This field contains a code identifying the '
                                                                'type of protocol; therapeutic, '
                                                                'non-therapeutic, BCM guidelines etc.',
                                          'protocol_type_desc': 'The description for the code value',
                                          'record_status': 'Always active in this projection. Protocol '
                                                           'status, display flag, begin/end-effective dates '
                                                           'and amendment parent/previous links do not '
                                                           'determine this literal label; it does not assert '
                                                           'that a study is open or recruiting.',
                                          'research_study_key': 'Deterministic SHA-256 key for the Cerner '
                                                                'research protocol.',
                                          'root_protocol_ind': 'Indicator of whether root protocol applies '
                                                               'to the research study record. It is derived '
                                                               'from bronze field `PARENT_PROT_MASTER_ID` in '
                                                               '`4_prod.bronze.map_research_study`, selected '
                                                               'as the provenance anchor from 2 recorded '
                                                               'bronze source fields. Null means the source '
                                                               'did not state the indicator and must not be '
                                                               'interpreted as false.',
                                          'source_protocol_id': 'Source-system identifier for the source '
                                                                'protocol associated with each research '
                                                                'study record. It is derived from bronze '
                                                                'field `PROT_MASTER_ID` in '
                                                                '`4_prod.bronze.map_research_study`. Its '
                                                                'uniqueness is limited to the originating '
                                                                'source namespace; null means no identifier '
                                                                'was supplied.',
                                          'study_mnemonic': 'Mnemonic assigned to the protocol',
                                          'study_mnemonic_key': 'Mnemonic of the protocol stripped of '
                                                                'special characters'},
                              'table': 'Cerner research protocol amendments with parent and previous '
                                       'protocol lineage.'}}
