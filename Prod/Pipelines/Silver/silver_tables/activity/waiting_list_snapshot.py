"""Silver activity.waiting_list_snapshot: 2 dataset(s).

- datasets: clinical_waiting_list_snapshot, _clinical_waiting_list_snapshot_metadata
- moved verbatim from: 4_silver_care_pathways:2022, 4_silver_care_pathways:2024, 4_silver_care_pathways:2147, 4_silver_care_pathways:2151, 4_silver_care_pathways:2155, 4_silver_care_pathways:2163, 4_silver_care_pathways:2168, 4_silver_care_pathways:2179

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id, subject_key_with_system


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / waiting list snapshot
SRC_WAITING_LIST_SNAPSHOT = "4_prod.bronze.map_waiting_list_snapshot"


def _waiting_list_snapshot_canonical():
    # Assemble normalized waiting list snapshot rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    s = read_source(SRC_WAITING_LIST_SNAPSHOT).alias("s")
    snap_id = stable_id("wl_census:mill_pm", s.SNAPSHOT_DATE, s.PM_WAIT_LIST_ID)
    row_ref = F.concat_ws(":", s.SNAPSHOT_DATE.cast("string"), s.PM_WAIT_LIST_ID.cast("string"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_WAITING_LIST_SNAPSHOT, row_ref
    )
    # contract v2: rename the census SHA key and publish native person, encounter, and location identifiers
    return s.select(
        snap_id.alias("waiting_list_snapshot_key"),
        s.SNAPSHOT_DATE.alias("snapshot_date"),
        s.SNAPSHOT_CUTOFF_TS.alias("snapshot_cutoff_ts"),
        s.PM_WAIT_LIST_ID.cast("long").alias("pm_wait_list_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
        .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.SCH_EVENT_ID.cast("long").alias("sch_event_id"),
        s.STATUS_CD.cast("string").alias("status_code"), s.STATUS_DESC.alias("status_display"),
        s.SUB_STATUS_DESC.alias("sub_status_display"),
        s.ACTIVE_STATUS_DESC.alias("active_status_display"),
        s.URGENCY_DESC.alias("urgency_display"), s.STAND_BY_DESC.alias("stand_by_display"),
        s.ADMIT_CATEGORY_DESC.alias("admit_category_display"),
        s.ADMIT_BOOKING_DESC.alias("admit_booking_display"),
        s.ADMIT_TYPE_DESC.alias("admit_type_display"),
        s.ADMIT_OFFER_OUTCOME_DESC.alias("admit_offer_outcome_display"),
        s.MANAGEMENT_DESC.alias("management_display"),
        s.ATTENDANCE_DESC.alias("attendance_display"),
        s.REASON_FOR_CHANGE_DESC.alias("reason_for_change_display"),
        s.REASON_FOR_REMOVAL_DESC.alias("reason_for_removal_display"),
        s.ANESTHETIC_DESC.alias("anesthetic_display"),
        s.PLANNED_PROCEDURE_CD.cast("string").alias("planned_procedure_code"),
        s.PLANNED_PROCEDURE_DESC.alias("planned_procedure_display"),
        s.REFERRAL_SOURCE_DESC.alias("referral_source_display"),
        s.REFERRAL_TYPE_DESC.alias("referral_type_display"),
        s.SERVICE_TYPE_REQUESTED_DESC.alias("service_type_requested_display"),
        s.FROM_ED_IND.cast("long").alias("from_ed_ind"),
        s.SUSPENDED_DAYS.alias("suspended_days"),
        s.RECOMMEND_DT_TM.alias("recommend_datetime"),
        s.REFERRAL_DT_TM.alias("referral_datetime"),
        s.ORIG_REQUEST_RECEIVED_DT_TM.alias("original_request_received_datetime"),
        s.ADMIT_DECISION_DT_TM.alias("admit_decision_datetime"),
        s.ADMIT_GUARANTEED_DT_TM.alias("admit_guaranteed_datetime"),
        s.PROVISIONAL_ADMIT_DT_TM.alias("provisional_admit_datetime"),
        s.PREV_PROV_ADMIT_DT_TM.alias("previous_provisional_admit_datetime"),
        s.WAITING_START_DT_TM.alias("waiting_start_datetime"),
        s.WAITING_END_DT_TM.alias("waiting_end_datetime"),
        s.ADJ_WAITING_START_DT_TM.alias("adjusted_waiting_start_datetime"),
        s.SCHEDULE_DT_TM.alias("scheduled_datetime"),
        s.REQUESTED_DT_TM.alias("requested_datetime"),
        s.REMOVAL_DT_TM.alias("removal_datetime"),
        s.LAST_DNA_DT_TM.alias("last_dna_datetime"),
        s.STATUS_DT_TM.alias("status_datetime"),
        s.STATUS_END_DT_TM.alias("status_end_datetime"),
        s.PLAUSIBLY_OPEN_IND.alias("plausibly_open_ind"),  # INSR_W2 W2.20
        s.LOC_NURSE_UNIT_CD.cast("string").alias("location_code"),
        s.LOC_NURSE_UNIT_DESC.alias("location_display"),
        s.LOC_FACILITY_DESC.alias("facility_display"),
        F.lit("administrative").alias("fact_category"),
        F.lit("waiting_list_census").alias("source_feed"),
        s.SNAPSHOT_CUTOFF_TS.alias("loaded_at"),
    )


WAITING_LIST_SNAPSHOT_LIFECYCLE_FIELDS = [
    'identity_status',
]


WAITING_LIST_SNAPSHOT_RETIRED_COLUMNS = [

]


WAITING_LIST_SNAPSHOT_LIFECYCLE_COLUMNS = [
    'waiting_list_snapshot_key',
    'snapshot_date',
    'pm_wait_list_id',
    'identity_status',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_waiting_list_snapshot():
    # Assemble waiting list snapshot rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _waiting_list_snapshot_canonical()


@materialized_view(
    name=_n("journey_clinical.waiting_list_snapshot"),
    refresh_policy="auto",
)
def waiting_list_snapshot():
    # Build the declared dataset: Immutable weekly waiting-list census (one row per
    # SNAPSHOT_DATE x PM_WAIT_LIST_ID). Snapshots never enter patient_event.
    return _lifecycle_source_waiting_list_snapshot().drop(*WAITING_LIST_SNAPSHOT_LIFECYCLE_FIELDS, *WAITING_LIST_SNAPSHOT_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._waiting_list_snapshot_metadata'),
    refresh_policy="auto",
)
def waiting_list_snapshot_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_waiting_list_snapshot; same row grain as the research table. Join keys:
    # waiting_list_snapshot_key, snapshot_date, pm_wait_list_id.
    return (_lifecycle_source_waiting_list_snapshot()).select(*WAITING_LIST_SNAPSHOT_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'clinical_waiting_list_snapshot': ValuePolicy(table='clinical_waiting_list_snapshot', enabled=True, zero_as_null=('sch_event_id', 'location_code')),
}


WRAP_POLICY = {'clinical_waiting_list_snapshot'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_waiting_list_snapshot_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                           'unchanged from the public '
                                                                           'parent; combine it with the '
                                                                           "companion's other key columns "
                                                                           'when joining.',
                                                          'activity_record_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'admission_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                          'allergy_instance_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                          'aria_dx_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                          'aria_pt_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                          'artifact_link_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                          'birth_row_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                          'bloodtrack_transaction_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                          'cancer_treatment_cycle_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                          'care_contact_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                          'care_participation_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                          'cds_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                          'chemotherapy_course_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
                                                                                    'one lifecycle row to '
                                                                                    'that research row.',
                                                          'community_care_activity_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                          'community_care_contact_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                          'concept_map_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                          'cost_line_item_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'dcp_forms_activity_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                          'device_mapping_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'diagnosis_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                          'dicom_path': 'Primary-key component carried '
                                                                        'unchanged from the public parent; '
                                                                        "combine it with the companion's "
                                                                        'other key columns when joining.',
                                                          'doc_response_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                          'elective_access_procedure_key': 'Deterministic v2 '
                                                                                           'key carried '
                                                                                           'unchanged from '
                                                                                           'the public '
                                                                                           'parent; joins '
                                                                                           'one lifecycle '
                                                                                           'row to that '
                                                                                           'research row.',
                                                          'encounter_attribute_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                          'encounter_bounds_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                          'encounter_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                          'encounter_id_resolved': 'True when encounter_id '
                                                                                   'is null or resolves to '
                                                                                   'the governed encounter '
                                                                                   'spine; false records '
                                                                                   'unresolved linkage '
                                                                                   'without dropping the '
                                                                                   'row.',
                                                          'encounter_identifier_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                          'encounter_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                          'endobase_exam_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                          'endobase_exam_term_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                          'entity_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                          'entry_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                          'episode_encounter_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                          'episode_encounter_reltn_id': 'Native source '
                                                                                        'identifier carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                          'episode_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                          'episode_id_resolved': 'True when episode_id is '
                                                                                 'null or resolves to the '
                                                                                 'governed episode spine; '
                                                                                 'false records unresolved '
                                                                                 'linkage without dropping '
                                                                                 'the row.',
                                                          'episode_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                          'event_after_death_30d': 'True when the event '
                                                                                   'timestamp is more than '
                                                                                   '30 days after the '
                                                                                   "resolved person's death "
                                                                                   'timestamp; silver '
                                                                                   'records the doubt and '
                                                                                   'does not filter the row.',
                                                          'event_before_birth': 'True when the event '
                                                                                'timestamp precedes the '
                                                                                "resolved person's birth "
                                                                                'timestamp; silver records '
                                                                                'the doubt and does not '
                                                                                'filter the row.',
                                                          'event_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                          'extract_cd': 'Primary-key component carried '
                                                                        'unchanged from the public parent; '
                                                                        "combine it with the companion's "
                                                                        'other key columns when joining.',
                                                          'fhx_activity_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                          'gene_tested_row_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                          'genetic_result_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                          'genetic_test_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                          'homecare_request_item_id': 'Native source '
                                                                                      'identifier carried '
                                                                                      'unchanged from the '
                                                                                      'public parent; joins '
                                                                                      'one lifecycle row to '
                                                                                      'that research row.',
                                                          'identity_status': 'Subject identity resolution '
                                                                             'state recorded by the silver '
                                                                             'canonical query.',
                                                          'implant_event_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                          'implant_sequence': 'Primary-key component carried '
                                                                              'unchanged from the public '
                                                                              'parent; combine it with the '
                                                                              "companion's other key columns "
                                                                              'when joining.',
                                                          'indication_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                          'is_illogical': 'Source RTT chronology flag '
                                                                          'indicating an illogical activity '
                                                                          'sequence; retained for the gold '
                                                                          'drop rule.',
                                                          'journey_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                          'journey_link_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                          'journey_participant_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                          'labour_delivery_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'load_batch_id': 'Deterministic batch token '
                                                                           "derived from the source row's "
                                                                           'bronze load timestamp.',
                                                          'loaded_at': 'Latest bronze load timestamp '
                                                                       'contributing to the public row; '
                                                                       "never the lifecycle pipeline's "
                                                                       'wall-clock time.',
                                                          'location_code': 'Primary-key component carried '
                                                                           'unchanged from the public '
                                                                           'parent; combine it with the '
                                                                           "companion's other key columns "
                                                                           'when joining.',
                                                          'location_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                          'location_stay_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                          'mc_device_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                          'microbiology_isolate_id': 'Native source '
                                                                                     'identifier carried '
                                                                                     'unchanged from the '
                                                                                     'public parent; joins '
                                                                                     'one lifecycle row to '
                                                                                     'that research row.',
                                                          'order_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                          'organization_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                          'organization_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                          'pacs_examination_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                          'pacs_report_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                          'pathway_oid': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                          'patient_event_key': 'Deterministic SHA-256 event '
                                                                               'key carried unchanged from '
                                                                               'the public parent; joins one '
                                                                               'lifecycle row to that event '
                                                                               'row.',
                                                          'patient_event_row_key': 'Deterministic row-level '
                                                                                   'key carried unchanged '
                                                                                   'from '
                                                                                   'events_patient_event; '
                                                                                   'joins one lifecycle row '
                                                                                   'to one event-index row.',
                                                          'period_business_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                          'period_oid': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                          'person_address_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'person_attribute_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                          'person_death_evidence_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                          'person_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                          'person_id_resolved': 'True when person_id is null '
                                                                                'or resolves to the governed '
                                                                                'person spine; false records '
                                                                                'unresolved linkage without '
                                                                                'dropping the row.',
                                                          'person_identifier_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                          'person_relationship_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                          'pharmacy_issue_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                          'pm_wait_list_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                          'practitioner_group_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                          'practitioner_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                          'practitioner_identifier_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                          'practitioner_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                          'practitioner_location_evidence_key': 'Deterministic '
                                                                                                'v2 key '
                                                                                                'carried '
                                                                                                'unchanged '
                                                                                                'from the '
                                                                                                'public '
                                                                                                'parent; '
                                                                                                'joins one '
                                                                                                'lifecycle '
                                                                                                'row to that '
                                                                                                'research '
                                                                                                'row.',
                                                          'pregnancy_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                          'pregnancy_reconciliation_key': 'Deterministic v2 '
                                                                                          'key carried '
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
                                                          'problem_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                          'procedure_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                          'pt_prot_reg_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                          'ptl_unique_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                          'record_status': 'active, superseded or retracted, '
                                                                           "normalised from the source's own "
                                                                           'status fields; superseded rows '
                                                                           'are earlier versions the source '
                                                                           'kept.',
                                                          'record_status_effective_from': 'Timestamp from '
                                                                                          'which the '
                                                                                          'normalised record '
                                                                                          'status applies, '
                                                                                          'when the source '
                                                                                          'supplies one.',
                                                          'record_status_effective_to': 'Timestamp until '
                                                                                        'which the '
                                                                                        'normalised record '
                                                                                        'status applies; '
                                                                                        'null means no '
                                                                                        'source end is '
                                                                                        'known.',
                                                          'referral_oid': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                          'report_version_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                          'request_thread_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'research_study_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'row_source': 'Physical bronze row source, CURRENT '
                                                                        'or HIST, carried unchanged from the '
                                                                        'public parent; a key component '
                                                                        'because the research table is at '
                                                                        'physical version grain and '
                                                                        'patient_event_key is shared across '
                                                                        "an entry's versions.",
                                                          'rtt_activity_oid': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                          'sch_event_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                          'score_type': 'Primary-key component carried '
                                                                        'unchanged from the public parent; '
                                                                        "combine it with the companion's "
                                                                        'other key columns when joining.',
                                                          'sequence_nbr': 'Primary-key component carried '
                                                                          'unchanged from the public parent; '
                                                                          "combine it with the companion's "
                                                                          'other key columns when joining.',
                                                          'service_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                          'service_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                          'snapshot_date': 'Primary-key component carried '
                                                                           'unchanged from the public '
                                                                           'parent; combine it with the '
                                                                           "companion's other key columns "
                                                                           'when joining.',
                                                          'source_daily_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                          'source_identifier_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                          'source_object': 'Source-arm discriminator carried '
                                                                           'unchanged from the public '
                                                                           'parent; use it with arm-specific '
                                                                           'native identifiers when joining.',
                                                          'source_site': 'Primary-key component carried '
                                                                         'unchanged from the public parent; '
                                                                         "combine it with the companion's "
                                                                         'other key columns when joining.',
                                                          'source_system_oid': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                          'source_unit': 'Primary-key component carried '
                                                                         'unchanged from the public parent; '
                                                                         "combine it with the companion's "
                                                                         'other key columns when joining.',
                                                          'source_update_timestamp': 'Native source-system '
                                                                                     'update timestamp '
                                                                                     'carried through the '
                                                                                     'canonical silver '
                                                                                     'query.',
                                                          'source_version_id': 'Native source version '
                                                                               'identifier carried unchanged '
                                                                               'from the public parent; '
                                                                               'CURRENT rows carry -1. A key '
                                                                               'component alongside '
                                                                               'row_source.',
                                                          'surg_case_proc_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                          'susceptibility_result_id': 'Native source '
                                                                                      'identifier carried '
                                                                                      'unchanged from the '
                                                                                      'public parent; joins '
                                                                                      'one lifecycle row to '
                                                                                      'that research row.',
                                                          'theatre_attendance_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                          'theatre_case_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                          'theatre_case_milestone_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                          'theatre_implant_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                          'transfusion_event_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                          'transfusion_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                          'treatment_cycle_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'treatment_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                          'update_count': 'Primary-key component carried '
                                                                          'unchanged from the public parent; '
                                                                          "combine it with the companion's "
                                                                          'other key columns when joining.',
                                                          'valid_from_datetime': 'Primary-key component '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; combine it '
                                                                                 "with the companion's other "
                                                                                 'key columns when joining.',
                                                          'value_set_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                          'waiting_list_oid': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                          'waiting_list_snapshot_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.'},
                                              'table': 'Internal quality and batch metadata for '
                                                       'clinical_waiting_list_snapshot; same row grain as '
                                                       'the research table. Join keys: '
                                                       'waiting_list_snapshot_key, snapshot_date, '
                                                       'pm_wait_list_id.'},
 'clinical_waiting_list_snapshot': {'columns': {'active_status_display': 'Source active-status display.',
                                                'adjusted_waiting_start_datetime': 'Adjusted waiting-start '
                                                                                   'timestamp.',
                                                'admit_booking_display': 'Source admission-booking display.',
                                                'admit_category_display': 'Source admission-category '
                                                                          'display.',
                                                'admit_decision_datetime': 'Source decision-to-admit '
                                                                           'timestamp.',
                                                'admit_guaranteed_datetime': 'Source guaranteed-admission '
                                                                             'timestamp.',
                                                'admit_offer_outcome_display': 'Source admission-offer '
                                                                               'outcome display.',
                                                'admit_type_display': 'Source admission-type display.',
                                                'anesthetic_display': 'Source anesthetic display.',
                                                'attendance_display': 'Source attendance display.',
                                                'encounter_id': 'Millennium ENCNTR_ID when supplied; native '
                                                                'encounter foreign key.',
                                                'facility_display': 'Source facility display.',
                                                'fact_category': 'Whether this fact is clinical or '
                                                                 'administrative in the v2 plane merge.',
                                                'from_ed_ind': 'Raw source indicator for origin in the '
                                                               'emergency department.',
                                                'last_dna_datetime': 'Source last did-not-attend timestamp.',
                                                'loaded_at': 'Single immutable UTC cutoff timestamp shared '
                                                             'by every row for SNAPSHOT_DATE.',
                                                'location_code': 'Millennium nurse-unit code.',
                                                'location_display': 'Source nurse-unit display.',
                                                'management_display': 'Source management display.',
                                                'original_request_received_datetime': 'Original '
                                                                                      'request-received '
                                                                                      'timestamp.',
                                                'person_id': 'Millennium PERSON_ID; native person foreign '
                                                             'key.',
                                                'planned_procedure_code': 'Source planned-procedure code.',
                                                'planned_procedure_display': 'Source planned-procedure '
                                                                             'display.',
                                                'plausibly_open_ind': 'Census row plausibly still open at '
                                                                      'the cut-off (W1.09). (bronze '
                                                                      'PLAUSIBLY_OPEN_IND, INSR W1). INSR_W2 '
                                                                      'W2.20.',
                                                'pm_wait_list_id': 'Native waiting-list entry identifier.',
                                                'previous_provisional_admit_datetime': 'Previous '
                                                                                       'provisional-admission '
                                                                                       'timestamp.',
                                                'provisional_admit_datetime': 'Source provisional-admission '
                                                                              'timestamp.',
                                                'reason_for_change_display': 'Source reason-for-change '
                                                                             'display.',
                                                'reason_for_removal_display': 'Source reason-for-removal '
                                                                              'display.',
                                                'recommend_datetime': 'Source recommendation timestamp.',
                                                'referral_datetime': 'Source referral timestamp.',
                                                'referral_source_display': 'Source referral-source display.',
                                                'referral_type_display': 'Source referral-type display.',
                                                'removal_datetime': 'Source removal timestamp.',
                                                'requested_datetime': 'Source requested timestamp.',
                                                'sch_event_id': 'Raw scheduling event linkage evidence.',
                                                'scheduled_datetime': 'Source scheduled timestamp.',
                                                'service_type_requested_display': 'Source '
                                                                                  'requested-service-type '
                                                                                  'display.',
                                                'snapshot_cutoff_ts': 'Immutable census cutoff timestamp.',
                                                'snapshot_date': 'Census snapshot date.',
                                                'source_feed': 'Registered feed owning the census row.',
                                                'stand_by_display': 'Source stand-by display.',
                                                'status_code': 'Source waiting-list status code.',
                                                'status_datetime': 'Source status timestamp.',
                                                'status_display': 'Source waiting-list status display.',
                                                'status_end_datetime': 'Source status-end timestamp.',
                                                'sub_status_display': 'Source waiting-list sub-status '
                                                                      'display.',
                                                'subject_id_system': 'Identifier system used for '
                                                                     'subject_key.',
                                                'subject_key': 'Deterministic SHA-256 over the strongest '
                                                               'source identifier. Not salted, not secret; a '
                                                               'stable join key across feeds where person_id '
                                                               'is unresolved.',
                                                'suspended_days': 'Source count of suspended days.',
                                                'urgency_display': 'Source urgency display.',
                                                'waiting_end_datetime': 'Source waiting-end timestamp.',
                                                'waiting_list_snapshot_key': 'Deterministic SHA-256 '
                                                                             'census-row key.',
                                                'waiting_start_datetime': 'Source waiting-start timestamp.'},
                                    'table': 'Immutable weekly waiting-list census (one row per '
                                             'SNAPSHOT_DATE x PM_WAIT_LIST_ID). Snapshots never enter '
                                             'patient_event.'}}

# INSR_R4_SILVER_V1 (module_drop): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import with_columns_last  # noqa: E402
WAITING_LIST_SNAPSHOT_LIFECYCLE_FIELDS = [c for c in WAITING_LIST_SNAPSHOT_LIFECYCLE_FIELDS if c not in ['identity_status']]  # decision 1: published
SPECS = with_columns_last(SPECS, 'clinical_waiting_list_snapshot', ('identity_status',))  # additions last
