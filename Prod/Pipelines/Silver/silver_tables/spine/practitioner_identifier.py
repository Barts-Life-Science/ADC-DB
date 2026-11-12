"""Silver spine.practitioner_identifier: 1 dataset(s).

- datasets: reference_practitioner_identifier
- moved verbatim from: 1_silver_spine_and_reference:6613, 1_silver_spine_and_reference:6635, 1_silver_spine_and_reference:6638, 1_silver_spine_and_reference:6643, 1_silver_spine_and_reference:6666

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


# Reference / practitioner identifier
SRC_PRSNL_ALIAS = "4_prod.bronze.map_medical_personnel_alias"


PRACTITIONER_IDENTIFIER_LIFECYCLE_FIELDS = [
]


PRACTITIONER_IDENTIFIER_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_practitioner_identifier():
    # Assemble practitioner identifier rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    s = read_source(SRC_PRSNL_ALIAS)
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    # contract v2: rename the row hash as a key and publish native practitioner_id
    return s.select(
        stable_id("practitioner_identifier:mill", s.PRSNL_ALIAS_ID)
         .alias("practitioner_identifier_key"),
        s.PERSON_ID.cast("bigint").alias("practitioner_id"),
        s.PERSON_ID.cast("string").alias("practitioner_person_id"),
        s.PRSNL_ALIAS_ID.cast("string").alias("source_alias_id"),
        s.ALIAS_TYPE_CDF_MEANING.alias("alias_type_meaning"),
        s.ALIAS_TYPE_DISPLAY.alias("alias_type_display"), s.ALIAS.alias("alias"),
        s.ACTIVE_IND.alias("active_ind"), s.EFFECTIVE_NOW_IND.alias("effective_now_ind"),
        s.BEG_EFFECTIVE_DT_TM.alias("beg_effective"), s.END_EFFECTIVE_DT_TM.alias("end_effective"),
        s.CONTRIBUTOR_SYSTEM_CD.cast("string").alias("contributor_system_code"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_PRSNL_ALIAS).alias("_source_table"),
        s.PRSNL_ALIAS_ID.cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.practitioner_identifier"),
    cluster_by=["practitioner_id"], refresh_policy="auto",
)
def practitioner_identifier():
    # Build the declared dataset: Staff aliases including postcode-type identifiers; alias
    # values are IG-sensitive.
    return _lifecycle_source_practitioner_identifier().drop(*PRACTITIONER_IDENTIFIER_LIFECYCLE_FIELDS, *PRACTITIONER_IDENTIFIER_RETIRED_COLUMNS)


VALUE_POLICIES = {
    'reference_practitioner_identifier': ValuePolicy(table='reference_practitioner_identifier', enabled=True, hub_zero_key='practitioner_id'),
}


WRAP_POLICY = {'reference_practitioner_identifier'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_practitioner_identifier': {'columns': {'_source_row_id': 'Native personnel-alias record '
                                                                     'identifier from `PRSNL_ALIAS_ID`, '
                                                                     'retained as a string.',
                                                   '_source_system': 'Value describing source system for the '
                                                                     'practitioner identifier record. It is '
                                                                     'produced by the silver transformation '
                                                                     'and has no direct bronze-column '
                                                                     'lineage entry. Whitespace and source '
                                                                     'sentinel text are retained unless the '
                                                                     'pipeline explicitly normalizes them; '
                                                                     'null means no value was supplied.',
                                                   '_source_table': 'Value describing source table for the '
                                                                    'practitioner identifier record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Whitespace and source sentinel '
                                                                    'text are retained unless the pipeline '
                                                                    'explicitly normalizes them; null means '
                                                                    'no value was supplied.',
                                                   'active_ind': 'Source numeric active indicator for the '
                                                                 'personnel alias, carried from `ACTIVE_IND` '
                                                                 'without boolean conversion.',
                                                   'alias': 'Raw personnel alias value.',
                                                   'alias_type_display': 'Display for the alias type.',
                                                   'alias_type_meaning': 'CDF meaning for the alias type.',
                                                   'beg_effective': 'Alias begin-effective timestamp.',
                                                   'contributor_system_code': 'Alias contributor-system '
                                                                              'code.',
                                                   'effective_now_ind': 'Whether the alias is effective at '
                                                                        'pipeline time.',
                                                   'end_effective': 'Alias end-effective timestamp.',
                                                   'loaded_at': 'map_medical_personnel_alias.ADC_UPDT '
                                                                'carried unchanged for the alias row. This '
                                                                'is bronze load provenance, not the alias '
                                                                'begin/end-effective timestamp or Silver '
                                                                'refresh time.',
                                                   'practitioner_id': 'Native Millennium personnel PERSON_ID '
                                                                      'as BIGINT; joins to '
                                                                      'reference_practitioner.practitioner_id.',
                                                   'practitioner_identifier_key': 'Deterministic SHA-256 key '
                                                                                  'for the practitioner '
                                                                                  'alias row.',
                                                   'practitioner_person_id': 'Source-system identifier for '
                                                                             'the practitioner person '
                                                                             'associated with each '
                                                                             'practitioner identifier '
                                                                             'record. It is derived from '
                                                                             'bronze field `PERSON_ID` in '
                                                                             '`4_prod.bronze.map_medical_personnel_alias`. '
                                                                             'Its uniqueness is limited to '
                                                                             'the originating source '
                                                                             'namespace; null means no '
                                                                             'identifier was supplied.',
                                                   'record_status': 'Retracted when '
                                                                    'map_medical_personnel_alias.SOURCE_PRESENT_IND '
                                                                    'is false, otherwise active; a missing '
                                                                    'presence flag defaults true. '
                                                                    'ACTIVE_IND, EFFECTIVE_NOW_IND and alias '
                                                                    'validity dates do not determine this '
                                                                    'label, and no superseded status is '
                                                                    'emitted.',
                                                   'source_alias_id': 'Native personnel-alias record '
                                                                      'identifier from `PRSNL_ALIAS_ID`, '
                                                                      'retained as a string.'},
                                       'table': 'Staff aliases including postcode-type identifiers; alias '
                                                'values are IG-sensitive.'}}
