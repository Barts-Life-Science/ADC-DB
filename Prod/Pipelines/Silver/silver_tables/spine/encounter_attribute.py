"""Silver spine.encounter_attribute: 1 dataset(s).

- datasets: reference_encounter_attribute
- moved verbatim from: 1_silver_spine_and_reference:6865, 1_silver_spine_and_reference:6890, 1_silver_spine_and_reference:6893, 1_silver_spine_and_reference:6898, 1_silver_spine_and_reference:6906

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables.spine._shared import _attribute_reference


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / encounter attribute
SRC_ENCOUNTER_ATTRIBUTE = "4_prod.bronze.map_encounter_attribute"


ENCOUNTER_ATTRIBUTE_LIFECYCLE_FIELDS = [
]


ENCOUNTER_ATTRIBUTE_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_encounter_attribute():
    # contract v2: rename the deterministic encounter-attribute identity as a key
    # Assemble encounter attribute rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _attribute_reference(
        SRC_ENCOUNTER_ATTRIBUTE, "encounter_attribute:mill", "encounter_attribute_key", "encounter"
    )


@materialized_view(
    name=_n("journey_reference.encounter_attribute"),
    cluster_by=["encounter_id"], refresh_policy="auto",
)
def encounter_attribute():
    # Build the declared dataset: Allowlisted encounter attributes with source-record
    # identifiers and current or effective-period evidence, linked to the native encounter.
    return _lifecycle_source_encounter_attribute().drop(*ENCOUNTER_ATTRIBUTE_LIFECYCLE_FIELDS, *ENCOUNTER_ATTRIBUTE_RETIRED_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_encounter_attribute': {'columns': {'_source_row_id': 'Primary key of the source mill_encntr_info '
                                                                 'row (ENCNTR_INFO_ID), giving one row per '
                                                                 'allowlisted encounter information record.',
                                               '_source_system': 'Value describing source system for the '
                                                                 'encounter attribute record. It is produced '
                                                                 'by the silver transformation and has no '
                                                                 'direct bronze-column lineage entry. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                               '_source_table': 'Value describing source table for the '
                                                                'encounter attribute record. It is produced '
                                                                'by the silver transformation and has no '
                                                                'direct bronze-column lineage entry. '
                                                                'Whitespace and source sentinel text are '
                                                                'retained unless the pipeline explicitly '
                                                                'normalizes them; null means no value was '
                                                                'supplied.',
                                               'active_ind': 'Source indicator of whether the row is active '
                                                             'or in an inactive state such as logically '
                                                             'deleted or combined away.',
                                               'answered_ind': 'Flag indicating whether the row carries an '
                                                               'answered coded value rather than the '
                                                               'unanswered zero code.',
                                               'attribute_name': 'Standardised curated label for the '
                                                                 'encounter attribute, derived from the '
                                                                 'source information sub-type code.',
                                               'beg_effective': 'BEG_EFFECTIVE_DT_TM with future-dated and '
                                                                'sentinel values excluded, leaving only '
                                                                'usable effective-from date and time values.',
                                               'current_ind': 'Flag marking the latest active row for each '
                                                              'encounter and information subtype within the '
                                                              'retained history.',
                                               'encounter_attribute_key': 'Deterministic SHA-256 key for the '
                                                                          'encounter attribute row.',
                                               'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT; joins '
                                                               'to spine_encounter.encounter_id.',
                                               'end_effective': 'END_EFFECTIVE_DT_TM with future-dated and '
                                                                'sentinel values excluded, leaving only '
                                                                'usable effective-to date and time values.',
                                               'link_status': "Indicator of whether the row's encounter "
                                                              'identifier resolves to an encounter in the '
                                                              'curated encounter foundation.',
                                               'loaded_at': 'map_encounter_attribute.PIPELINE_UPDT_DT_TM '
                                                            'carried unchanged through the shared attribute '
                                                            'projection. This is the contributing bronze '
                                                            "write clock, not the attribute's effective "
                                                            'dates, clinical value time or Silver refresh '
                                                            'time.',
                                               'source_encounter_id': 'Internal encounter identifier from '
                                                                      'the source encounter information row '
                                                                      '(ENCNTR_ID), linking the attribute to '
                                                                      'its encounter.',
                                               'value_code': 'Coded value of the encounter information row, '
                                                             'drawn from the code set applicable to the '
                                                             'attribute, where zero denotes an unanswered '
                                                             'value.',
                                               'value_code_set': 'Code set to which VALUE_CD actually '
                                                                 'belongs, resolved from the code value '
                                                                 'lookup.',
                                               'value_datetime': 'VALUE_DT_TM with future-dated and sentinel '
                                                                 'values excluded, leaving only usable date '
                                                                 'and time values.',
                                               'value_display': 'The display string for the code_value',
                                               'value_kind': 'Indicator of which value representation '
                                                             'applies to the row, such as a coded, date/time '
                                                             'or numeric value.',
                                               'value_numeric': 'The numeric (non-codified) value of the '
                                                                'encounter information.  If the value of the '
                                                                'row necessitates storing a numeric '
                                                                '(non-codified) value, it is placed in this '
                                                                'column.'},
                                   'table': 'Allowlisted encounter attributes with source-record identifiers '
                                            'and current or effective-period evidence, linked to the native '
                                            'encounter.'}}
