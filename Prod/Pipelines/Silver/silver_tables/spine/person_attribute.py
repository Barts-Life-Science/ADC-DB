"""Silver spine.person_attribute: 1 dataset(s).

- datasets: reference_person_attribute
- moved verbatim from: 1_silver_spine_and_reference:6924, 1_silver_spine_and_reference:6949, 1_silver_spine_and_reference:6952, 1_silver_spine_and_reference:6957, 1_silver_spine_and_reference:6968

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n
from silver_tables.spine._shared import _attribute_reference


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / person attribute
SRC_PERSON_ATTRIBUTE = "4_prod.bronze.map_person_attribute"


PERSON_ATTRIBUTE_LIFECYCLE_FIELDS = [
]


PERSON_ATTRIBUTE_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_person_attribute():
    # contract v2: rename the row identity as a key and enforce native BIGINT person_id at publication
    # Assemble person attribute rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return (
        _attribute_reference(
            SRC_PERSON_ATTRIBUTE, "person_attribute:mill", "person_attribute_key", "person"
        )
        .withColumn("person_id", F.col("person_id").cast("bigint"))
    )


@materialized_view(
    name=_n("journey_reference.person_attribute"),
    cluster_by=["person_id"], refresh_policy="auto",
)
def person_attribute():
    # Build the declared dataset: Allowlisted person attributes; NO_FIXED_ABODE and
    # CHILD_IN_PUBLIC_CARE are IG-sensitive.
    return _lifecycle_source_person_attribute().drop(*PERSON_ATTRIBUTE_LIFECYCLE_FIELDS, *PERSON_ATTRIBUTE_RETIRED_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_person_attribute': {'columns': {'_source_row_id': 'Unique source primary key of the originating '
                                                              'mill_person_info row, carried through as the '
                                                              'row grain of this table.',
                                            '_source_system': 'Value describing source system for the person '
                                                              'attribute record. It is produced by the '
                                                              'silver transformation and has no direct '
                                                              'bronze-column lineage entry. Whitespace and '
                                                              'source sentinel text are retained unless the '
                                                              'pipeline explicitly normalizes them; null '
                                                              'means no value was supplied.',
                                            '_source_table': 'Value describing source table for the person '
                                                             'attribute record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Whitespace and source sentinel '
                                                             'text are retained unless the pipeline '
                                                             'explicitly normalizes them; null means no '
                                                             'value was supplied.',
                                            'active_ind': 'Indicator of whether the source row is in an '
                                                          'active state or has been made inactive, such as '
                                                          'logically deleted or combined away.',
                                            'answered_ind': 'Flag indicating whether a coded value was '
                                                            'actually recorded for the attribute rather than '
                                                            'left unanswered.',
                                            'attribute_name': 'Standardised label for the person attribute '
                                                              'represented by the row, derived from the '
                                                              'source information sub-type code.',
                                            'beg_effective': 'Row begin effective date/time after removal of '
                                                             'sentinel and implausible values, leaving null '
                                                             'where no usable value remains.',
                                            'current_ind': 'Flag marking the latest active row for each '
                                                           'person and attribute sub-type within the '
                                                           'retained history.',
                                            'end_effective': 'Row end effective date/time after removal of '
                                                             'sentinel and open-ended placeholder values, '
                                                             'leaving null where no usable value remains.',
                                            'link_status': "Indicator of whether the attribute row's person "
                                                           'identifier matched a person in the curated '
                                                           'person foundation table.',
                                            'loaded_at': 'map_person_attribute.PIPELINE_UPDT_DT_TM carried '
                                                         'unchanged through the shared attribute projection. '
                                                         'This is the contributing bronze write clock, not '
                                                         "the attribute's effective dates, clinical value "
                                                         'time or Silver refresh time.',
                                            'person_attribute_key': 'Deterministic SHA-256 key for the '
                                                                    'person attribute row.',
                                            'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                            'source_person_id': 'Internal system-assigned person identifier '
                                                                'from the source person table, linking the '
                                                                'attribute row to a person.',
                                            'value_code': 'Coded value recorded for the attribute, where '
                                                          'zero indicates the attribute was not answered and '
                                                          'is never decoded.',
                                            'value_code_set': 'Code set of the decoded coded value as held '
                                                              'in the reference code value lookup.',
                                            'value_datetime': 'Attribute date/time after removal of sentinel '
                                                              'and implausible values, leaving null where no '
                                                              'usable value remains.',
                                            'value_display': 'The display string for the code_value',
                                            'value_kind': 'Indicator of the value representation expected '
                                                          'for the attribute, such as a coded value, date or '
                                                          'numeric value.',
                                            'value_numeric': 'If the comment is of a numeric type, it is '
                                                             'stored in this attribute'},
                                'table': 'Allowlisted person attributes; NO_FIXED_ABODE and '
                                         'CHILD_IN_PUBLIC_CARE are IG-sensitive.'}}
