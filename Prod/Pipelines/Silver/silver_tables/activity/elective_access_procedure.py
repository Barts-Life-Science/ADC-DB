"""Silver activity.elective_access_procedure: 1 dataset(s).

- datasets: reference_elective_access_procedure
- moved verbatim from: 4_silver_care_pathways:4313, 4_silver_care_pathways:4316, 4_silver_care_pathways:4321, 4_silver_care_pathways:4347

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables._common.journey import SRC_EAL_PROCEDURE


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


ELECTIVE_ACCESS_PROCEDURE_LIFECYCLE_FIELDS = [
]


ELECTIVE_ACCESS_PROCEDURE_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_elective_access_procedure():
    # Assemble elective access procedure rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    s = read_source(SRC_EAL_PROCEDURE)
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    superseded = ~F.coalesce(s.ACTIVE_IND, F.lit(True))
    # contract v2: rename deterministic row and parent identities as keys and publish parent native identifiers with consistent types
    return s.select(
        stable_id("elective_access_procedure:luna", s.WAITING_LIST_OID,
                  s.PROCEDURE_TYPE_SEQ, s.PROCEDURE_SEQ)
         .alias("elective_access_procedure_key"),
        stable_id("elective_access:luna", s.SOURCE_SYSTEM_OID, s.WAITING_LIST_OID)
         .alias("elective_access_entry_key"),
        s.WAITING_LIST_OID.cast("bigint").alias("waiting_list_oid"),
        s.PROCEDURE_CODE.alias("procedure_code"), s.PROCEDURE_DESC.alias("procedure_desc"),
        s.PROCEDURE_CATALOG.alias("procedure_catalog"), s.PROCEDURE_RVID.alias("procedure_rvid"),
        s.PROCEDURE_TYPE_SEQ.alias("procedure_type_seq"), s.PROCEDURE_SEQ.alias("procedure_seq"),
        s.ACTIVE_IND.alias("active_ind"), s.PARENT_PRESENT_IND.alias("parent_present_ind"),
        s.SOURCE_SYSTEM_OID.cast("bigint").alias("source_system_oid"),
        s.SOURCE_SYSTEM_OID_INHERITED_IND.alias("source_system_oid_inherited_ind"),
        F.when(retracted, F.lit("retracted"))
         .when(superseded, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        s.ADC_UPDT.alias("loaded_at"),
    )


@materialized_view(
    name=_n("journey_reference.elective_access_procedure"),
    refresh_policy="auto",
)
def elective_access_procedure():
    # Build the declared dataset: All LUNA elective-access planned-procedure slots, including
    # bare and tombstoned rows.
    return _lifecycle_source_elective_access_procedure().drop(*ELECTIVE_ACCESS_PROCEDURE_LIFECYCLE_FIELDS, *ELECTIVE_ACCESS_PROCEDURE_RETIRED_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_elective_access_procedure': {'columns': {'active_ind': 'B3 bronze field ACTIVE_IND; source value '
                                                                   'retained unless documented as derived.',
                                                     'elective_access_entry_key': 'Deterministic SHA-256 key '
                                                                                  'for the parent elective '
                                                                                  'access entry.',
                                                     'elective_access_procedure_key': 'Deterministic SHA-256 '
                                                                                      'key for this LUNA '
                                                                                      'procedure row.',
                                                     'loaded_at': 'map_elective_access_list_procedure.ADC_UPDT '
                                                                  'carried unchanged for the '
                                                                  'planned-procedure slot. No parent '
                                                                  'waiting-list load clock is joined or '
                                                                  'added; this is bronze load provenance, '
                                                                  'not procedure time or Silver refresh '
                                                                  'time.',
                                                     'parent_present_ind': 'B3 bronze field '
                                                                           'PARENT_PRESENT_IND; source value '
                                                                           'retained unless documented as '
                                                                           'derived.',
                                                     'procedure_catalog': 'B3 bronze field '
                                                                          'PROCEDURE_CATALOG; source value '
                                                                          'retained unless documented as '
                                                                          'derived.',
                                                     'procedure_code': 'B3 bronze field PROCEDURE_CODE; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                     'procedure_desc': 'B3 bronze field PROCEDURE_DESC; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                     'procedure_rvid': 'B3 bronze field PROCEDURE_RVID; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                     'procedure_seq': 'B3 bronze field PROCEDURE_SEQ; source '
                                                                      'value retained unless documented as '
                                                                      'derived.',
                                                     'procedure_type_seq': 'B3 bronze field '
                                                                           'PROCEDURE_TYPE_SEQ; source value '
                                                                           'retained unless documented as '
                                                                           'derived.',
                                                     'record_status': 'Retracted when SOURCE_PRESENT_IND is '
                                                                      'false; otherwise superseded when '
                                                                      'ACTIVE_IND is false; otherwise '
                                                                      'active. Both missing flags default '
                                                                      'true, and retraction has priority. '
                                                                      'PARENT_PRESENT_IND is published '
                                                                      'separately and does not determine '
                                                                      'this row status.',
                                                     'source_system_oid': 'LUNA SOURCE_SYSTEM_OID as BIGINT.',
                                                     'source_system_oid_inherited_ind': 'Always true on '
                                                                                        'present rows; '
                                                                                        'proves namespace '
                                                                                        'provenance is the '
                                                                                        'parent.',
                                                     'waiting_list_oid': 'LUNA WAITING_LIST_OID as BIGINT.'},
                                         'table': 'All LUNA elective-access planned-procedure slots, '
                                                  'including bare and tombstoned rows.'}}
