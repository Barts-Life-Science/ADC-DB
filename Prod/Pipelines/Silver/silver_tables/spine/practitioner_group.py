"""Silver spine.practitioner_group: 1 dataset(s).

- datasets: reference_practitioner_group
- moved verbatim from: 1_silver_spine_and_reference:6684, 1_silver_spine_and_reference:6710, 1_silver_spine_and_reference:6713, 1_silver_spine_and_reference:6718, 1_silver_spine_and_reference:6749

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables.spine._shared import SRC_PRACTITIONER


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / practitioner group
SRC_PRSNL_GROUP = "4_prod.bronze.map_medical_personnel_group"


PRACTITIONER_GROUP_LIFECYCLE_FIELDS = [
]


PRACTITIONER_GROUP_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_practitioner_group():
    # Assemble practitioner group rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    s = read_source(SRC_PRSNL_GROUP).alias("s")
    p = read_source(SRC_PRACTITIONER).select(
        F.col("PERSON_ID").alias("_practitioner_person_id")
    ).distinct().alias("p")
    d = s.join(p, s.PERSON_ID == p._practitioner_person_id, "left")
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    # contract v2: rename the membership hash as a key and publish native practitioner_id
    return d.select(
        stable_id("practitioner_group:mill", s.PRSNL_GROUP_RELTN_ID)
         .alias("practitioner_group_key"),
        s.PERSON_ID.cast("bigint").alias("practitioner_id"),
        s.PERSON_ID.cast("string").alias("practitioner_person_id"),
        F.col("_practitioner_person_id").isNotNull().alias("in_practitioner_dimension_ind"),
        s.PRSNL_GROUP_ID.cast("string").alias("source_group_id"),
        s.PRSNL_GROUP_NAME.alias("group_name"), s.GROUP_LABEL.alias("group_label"),
        s.CDF_MEANING.alias("group_type_meaning"), s.GROUP_TYPE_DISPLAY.alias("group_type_display"),
        s.PRIMARY_IND.alias("primary_ind"), s.RELATION_ACTIVE_IND.alias("relation_active_ind"),
        s.RELATION_BEG_EFFECTIVE_DT_TM.alias("relation_beg_effective"),
        s.RELATION_END_EFFECTIVE_DT_TM.alias("relation_end_effective"),
        s.GROUP_ACTIVE_IND.alias("group_active_ind"),
        s.GROUP_BEG_EFFECTIVE_DT_TM.alias("group_beg_effective"),
        s.GROUP_END_EFFECTIVE_DT_TM.alias("group_end_effective"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_PRSNL_GROUP).alias("_source_table"),
        s.PRSNL_GROUP_RELTN_ID.cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.practitioner_group"),
    cluster_by=["practitioner_id"], refresh_policy="auto",
)
def practitioner_group():
    # Build the declared dataset: Staff-group memberships; personnel-dimension orphans are
    # retained and flagged.
    return _lifecycle_source_practitioner_group().drop(*PRACTITIONER_GROUP_LIFECYCLE_FIELDS, *PRACTITIONER_GROUP_RETIRED_COLUMNS)


VALUE_POLICIES = {
    'reference_practitioner_group': ValuePolicy(table='reference_practitioner_group', enabled=True, hub_zero_key='practitioner_id'),
}


WRAP_POLICY = {'reference_practitioner_group'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_practitioner_group': {'columns': {'_source_row_id': 'Personnel-group relationship identifier.',
                                              '_source_system': 'Value describing source system for the '
                                                                'practitioner group record. It is produced '
                                                                'by the silver transformation and has no '
                                                                'direct bronze-column lineage entry. '
                                                                'Whitespace and source sentinel text are '
                                                                'retained unless the pipeline explicitly '
                                                                'normalizes them; null means no value was '
                                                                'supplied.',
                                              '_source_table': 'Value describing source table for the '
                                                               'practitioner group record. It is produced by '
                                                               'the silver transformation and has no direct '
                                                               'bronze-column lineage entry. Whitespace and '
                                                               'source sentinel text are retained unless the '
                                                               'pipeline explicitly normalizes them; null '
                                                               'means no value was supplied.',
                                              'group_active_ind': 'Source numeric active indicator for the '
                                                                  'personnel group, carried from '
                                                                  '`GROUP_ACTIVE_IND` without boolean '
                                                                  'conversion.',
                                              'group_beg_effective': 'Group begin-effective timestamp.',
                                              'group_end_effective': 'Group end-effective timestamp.',
                                              'group_label': 'Best label from group name, description and '
                                                             'code metadata.',
                                              'group_name': 'Raw personnel-group name.',
                                              'group_type_display': 'Code-value display for the group type.',
                                              'group_type_meaning': 'CDF meaning for the group type code.',
                                              'in_practitioner_dimension_ind': 'Exact Millennium personnel '
                                                                               'identifier.',
                                              'loaded_at': 'map_medical_personnel_group.ADC_UPDT carried '
                                                           'unchanged for the membership row. The '
                                                           'personnel-directory existence join supplies only '
                                                           'a person identifier and contributes no '
                                                           'timestamp; this is bronze load provenance, not '
                                                           'relationship validity time or Silver refresh '
                                                           'time.',
                                              'practitioner_group_key': 'Deterministic SHA-256 key for the '
                                                                        'practitioner-group membership.',
                                              'practitioner_id': 'Native Millennium personnel PERSON_ID as '
                                                                 'BIGINT; joins to '
                                                                 'reference_practitioner.practitioner_id.',
                                              'practitioner_person_id': 'Source-system identifier for the '
                                                                        'practitioner person associated with '
                                                                        'each practitioner group record. It '
                                                                        'is derived from bronze field '
                                                                        '`PERSON_ID` in '
                                                                        '`4_prod.bronze.map_medical_personnel_group`. '
                                                                        'Its uniqueness is limited to the '
                                                                        'originating source namespace; null '
                                                                        'means no identifier was supplied.',
                                              'primary_ind': 'Source relationship primary indicator.',
                                              'record_status': 'Retracted when '
                                                               'map_medical_personnel_group.SOURCE_PRESENT_IND '
                                                               'is false, otherwise active; a missing '
                                                               'presence flag defaults true. Relation/group '
                                                               'activity, validity dates and '
                                                               'personnel-directory membership do not '
                                                               'determine this label, and no superseded '
                                                               'status is emitted.',
                                              'relation_active_ind': 'Source numeric active indicator for '
                                                                     'the personnel-to-group relationship, '
                                                                     'carried from `RELATION_ACTIVE_IND` '
                                                                     'without boolean conversion.',
                                              'relation_beg_effective': 'Relationship begin-effective '
                                                                        'timestamp.',
                                              'relation_end_effective': 'Relationship end-effective '
                                                                        'timestamp.',
                                              'source_group_id': 'Native personnel-group identifier from '
                                                                 '`PRSNL_GROUP_ID`, retained as a string.'},
                                  'table': 'Staff-group memberships; personnel-dimension orphans are '
                                           'retained and flagged.'}}
