"""Silver activity.appointment: 4 dataset(s).

- datasets: _clinical_appointment_primitive, _clinical_qc_appointment, clinical_appointment, _clinical_appointment_metadata
- moved verbatim from: 4_silver_care_pathways:1084, 4_silver_care_pathways:1132, 4_silver_care_pathways:1150, 4_silver_care_pathways:1168, 4_silver_care_pathways:1236, 4_silver_care_pathways:1255, silver_journey_shared.py:6775, silver_journey_shared.py:6920

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n, spark
from silver_tables._common.journey import _appointment_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# ==== Scheduling activity and SurgiNet case procedures ====

# contract v2: retain the full canonical shape for internal reuse.
APPOINTMENT_SOURCE_COLUMNS = [
    "patient_event_key",
    "sch_event_id",
    "recurrence_parent_sch_event_id",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "event_datetime_local",
    "event_end_datetime_local",
    "source_coding_system",
    "source_code",
    "source_display",
    "appointment_type_code",
    "appointment_type_display",
    "status_code",
    "status_display",
    "status_meaning",
    "referral_identifier",
    "requested_datetime",
    "original_requested_start",
    "original_requested_end",
    "first_booked_datetime",
    "requested_practitioner_id",
    "allocated_practitioner_id",
    "location_code",
    "location_display",  # SDI_APPT_LOCATION_V1
    "clinic_resource_code",  # SDI_APPT_LOCATION_V1
    "clinic_resource_display",  # SDI_APPT_LOCATION_V1
    "service_resource_code",  # SDI_APPT_LOCATION_V1
    "service_resource_display",  # SDI_APPT_LOCATION_V1
    "organization_id",
    "recurrence_parent_key",
    "recurrence_type_flag",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "fact_category",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]


APPOINTMENT_PRIMITIVE_COLUMNS = [
    {
        "booking_iterations": "_booking_iterations_json",
        "resource_history": "_resource_history_json",
    }.get(name, name)
    for name in APPOINTMENT_SOURCE_COLUMNS
]


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
APPOINTMENT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'sch_event_id',
    'recurrence_parent_sch_event_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'event_datetime_local',
    'event_end_datetime_local',
    'source_coding_system',
    'source_code',
    'source_display',
    'appointment_type_code',
    'appointment_type_display',
    'status_code',
    'status_display',
    'status_meaning',
    'referral_identifier',
    'requested_datetime',
    'original_requested_start',
    'original_requested_end',
    'first_booked_datetime',
    'requested_practitioner_id',
    'allocated_practitioner_id',
    'location_code',
    'location_display',  # SDI_APPT_LOCATION_V1
    'clinic_resource_code',  # SDI_APPT_LOCATION_V1
    'clinic_resource_display',  # SDI_APPT_LOCATION_V1
    'service_resource_code',  # SDI_APPT_LOCATION_V1
    'service_resource_display',  # SDI_APPT_LOCATION_V1
    'organization_id',
    'recurrence_parent_key',
    'recurrence_type_flag',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


APPOINTMENT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'sch_event_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


# Clinical /  appointment primitive
@materialized_view(
    name=_n("journey_clinical._appointment_primitive"),
    private=True,
    refresh_policy="auto",
)
def _appointment_primitive():
    # Build the declared dataset: Internal orderable appointment join; booking/resource VARIANT
    # arrays cross as JSON.
    return _appointment_canonical().select(*APPOINTMENT_PRIMITIVE_COLUMNS)


# Clinical /  qc appointment
@materialized_view(
    name=_n("journey_clinical._qc_appointment"),
    refresh_policy="auto",
)
def _qc_appointment():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for appointment.
    return _cross_qc_primitive(
        spark.read.table(_n("journey_clinical._appointment_primitive")),
        "appointment",
        {},
    )


@materialized_view(
    name=_n("journey_clinical.appointment"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def appointment():
    # Build the declared dataset: Scheduling appointments with ordered booking and resource
    # history; SCH_EVENT_ID is never treated as a clinical EVENT_ID.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_appointment")),
        "appointment",
        {
        },
        APPOINTMENT_PUBLIC_COLUMNS,
        APPOINTMENT_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._appointment_metadata'),
    refresh_policy="auto",
)
def appointment_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_appointment;
    # same row grain as the research table. Join keys: patient_event_key, sch_event_id.
    return (spark.read.table(_n("journey_clinical._qc_appointment"))).select(*APPOINTMENT_LIFECYCLE_COLUMNS)


CROSS_QC = {
    'appointment': CrossQc(model='appointment', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved']),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_appointment': ValuePolicy(table='clinical_appointment', enabled=True, zero_as_null=('allocated_practitioner_id', 'organization_id', 'requested_practitioner_id')),
}


WRAP_POLICY = {'clinical_appointment'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_appointment_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                'activity_record_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                'admission_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'allergy_instance_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                'aria_dx_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                'aria_pt_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                'artifact_link_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                'birth_row_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'bloodtrack_transaction_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                'cancer_treatment_cycle_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                'care_contact_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                'care_participation_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                'cds_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                                'chemotherapy_course_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                'community_care_activity_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                'community_care_contact_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                'concept_map_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                                'cost_line_item_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                'dcp_forms_activity_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                'device_mapping_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                'diagnosis_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'dicom_path': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                                'doc_response_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                'elective_access_procedure_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                'encounter_attribute_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                'encounter_bounds_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                'encounter_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'encounter_id_resolved': 'True when encounter_id is null or '
                                                                         'resolves to the governed encounter '
                                                                         'spine; false records unresolved '
                                                                         'linkage without dropping the row.',
                                                'encounter_identifier_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                'encounter_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'endobase_exam_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                'endobase_exam_term_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                'entity_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                                'entry_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                                'episode_encounter_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                'episode_encounter_reltn_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                'episode_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                'episode_id_resolved': 'True when episode_id is null or '
                                                                       'resolves to the governed episode '
                                                                       'spine; false records unresolved '
                                                                       'linkage without dropping the row.',
                                                'episode_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                                'event_after_death_30d': 'True when the event timestamp is '
                                                                         'more than 30 days after the '
                                                                         "resolved person's death timestamp; "
                                                                         'silver records the doubt and does '
                                                                         'not filter the row.',
                                                'event_before_birth': 'True when the event timestamp '
                                                                      "precedes the resolved person's birth "
                                                                      'timestamp; silver records the doubt '
                                                                      'and does not filter the row.',
                                                'event_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                                'extract_cd': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                                'fhx_activity_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                'gene_tested_row_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                'genetic_result_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                'genetic_test_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                'homecare_request_item_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                'identity_status': 'Subject identity resolution state '
                                                                   'recorded by the silver canonical query.',
                                                'implant_event_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                'implant_sequence': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                'indication_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'is_illogical': 'Source RTT chronology flag indicating an '
                                                                'illogical activity sequence; retained for '
                                                                'the gold drop rule.',
                                                'journey_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                                'journey_link_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                'journey_participant_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                'labour_delivery_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                'load_batch_id': 'Deterministic batch token derived from the '
                                                                 "source row's bronze load timestamp.",
                                                'loaded_at': 'Latest bronze load timestamp contributing to '
                                                             "the public row; never the lifecycle pipeline's "
                                                             'wall-clock time.',
                                                'location_code': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                'location_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                                'location_stay_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                'mc_device_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'microbiology_isolate_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                'order_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                                'organization_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                'organization_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                'pacs_examination_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                'pacs_report_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                'pathway_oid': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                'patient_event_key': 'Deterministic SHA-256 event key '
                                                                     'carried unchanged from the public '
                                                                     'parent; joins one lifecycle row to '
                                                                     'that event row.',
                                                'patient_event_row_key': 'Deterministic row-level key '
                                                                         'carried unchanged from '
                                                                         'events_patient_event; joins one '
                                                                         'lifecycle row to one event-index '
                                                                         'row.',
                                                'period_business_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                'period_oid': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                'person_address_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                'person_attribute_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                'person_death_evidence_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                'person_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                                'person_id_resolved': 'True when person_id is null or '
                                                                      'resolves to the governed person '
                                                                      'spine; false records unresolved '
                                                                      'linkage without dropping the row.',
                                                'person_identifier_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                'person_relationship_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                'pharmacy_issue_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                'pm_wait_list_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                'practitioner_group_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                'practitioner_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                'practitioner_identifier_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                'practitioner_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                'practitioner_location_evidence_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                'pregnancy_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                'problem_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                'procedure_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'pt_prot_reg_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                'ptl_unique_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'record_status': 'active, superseded or retracted, '
                                                                 "normalised from the source's own status "
                                                                 'fields; superseded rows are earlier '
                                                                 'versions the source kept.',
                                                'record_status_effective_from': 'Timestamp from which the '
                                                                                'normalised record status '
                                                                                'applies, when the source '
                                                                                'supplies one.',
                                                'record_status_effective_to': 'Timestamp until which the '
                                                                              'normalised record status '
                                                                              'applies; null means no source '
                                                                              'end is known.',
                                                'referral_oid': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'report_version_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                'request_thread_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                'research_study_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                'row_source': 'Physical bronze row source, CURRENT or HIST, '
                                                              'carried unchanged from the public parent; a '
                                                              'key component because the research table is '
                                                              'at physical version grain and '
                                                              "patient_event_key is shared across an entry's "
                                                              'versions.',
                                                'rtt_activity_oid': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                'sch_event_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'score_type': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                                'sequence_nbr': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                                'service_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                'service_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                                'snapshot_date': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                'source_daily_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                'source_identifier_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                'source_object': 'Source-arm discriminator carried unchanged '
                                                                 'from the public parent; use it with '
                                                                 'arm-specific native identifiers when '
                                                                 'joining.',
                                                'source_site': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                                'source_system_oid': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                'source_unit': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                                'source_update_timestamp': 'Native source-system update '
                                                                           'timestamp carried through the '
                                                                           'canonical silver query.',
                                                'source_version_id': 'Native source version identifier '
                                                                     'carried unchanged from the public '
                                                                     'parent; CURRENT rows carry -1. A key '
                                                                     'component alongside row_source.',
                                                'surg_case_proc_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                'susceptibility_result_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                'theatre_attendance_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                'theatre_case_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                'theatre_case_milestone_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                'theatre_implant_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                'transfusion_event_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                'transfusion_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                                'treatment_cycle_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                'treatment_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'update_count': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                                'valid_from_datetime': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                'value_set_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'waiting_list_oid': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.'},
                                    'table': 'Internal quality and batch metadata for clinical_appointment; '
                                             'same row grain as the research table. Join keys: '
                                             'patient_event_key, sch_event_id.'},
 '_clinical_appointment_primitive': {'columns': {},
                                     'table': 'Internal orderable appointment join; booking/resource VARIANT '
                                              'arrays cross as JSON.'},
 '_clinical_qc_appointment': {'columns': {},
                              'table': 'Private JSON bridge and Gold cross-rule flags for appointment.'},
 'clinical_appointment': {'columns': {'allocated_practitioner_id': 'Millennium personnel PERSON_ID for the '
                                                                   'representative allocated practitioner.',
                                      'appointment_type_code': 'Source appointment-type code.',
                                      'appointment_type_display': 'Source appointment-type display.',
                                      'clinic_resource_code': 'Millennium RESOURCE_CD of the booked primary '
                                                              'non-patient resource (PRIMARY_ROLE_IND 1, '
                                                              'ROLE_MEANING not PATIENT): the clinic/session '
                                                              'resource an outpatient appointment is booked '
                                                              'into, or the theatre for a surgical booking. '
                                                              'Taken from the latest SCHEDULE_SEQ, lowest '
                                                              'code on ties. SCH_CLINIC_ID is 0 at source, '
                                                              'so no clinic id is published. Specialty is '
                                                              "not carried: join the linked encounter's "
                                                              'responsible_service_display.',
                                      'clinic_resource_display': 'Source RESOURCE_DESCRIPTION for '
                                                                 'clinic_resource_code.',
                                      'confidentiality_code': 'Source confidentiality code when supplied.',
                                      'encounter_id': 'Millennium ENCNTR_ID when supplied; native encounter '
                                                      'foreign key.',
                                      'event_datetime': 'Booked slot start where available',
                                      'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                              'time (GMT/BST). Millennium rows take the '
                                                              'bronze *_LOCAL companions of the same source '
                                                              'columns and fallback order; rows from '
                                                              'local-clock sources (LUNA, PACS/DICOM, TFC '
                                                              'LIMS, CCMDS, MSDS, BadgerNet, EndoBase, SLAM, '
                                                              'iWeb, Datix and similar) carry event_datetime '
                                                              'unchanged. event_datetime is a UTC instant '
                                                              'for Millennium rows, so take local calendar '
                                                              'dates and clock times from this column.',
                                      'event_end_datetime': 'Booked slot end where available.',
                                      'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                  'wall-clock time (GMT/BST), by the same '
                                                                  'rule as event_datetime_local; null where '
                                                                  'event_end_datetime is null.',
                                      'fact_category': 'Whether this fact is clinical or administrative in '
                                                       'the v2 plane merge.',
                                      'first_booked_datetime': 'First booking timestamp supplied by '
                                                               'scheduling.',
                                      'loaded_at': 'Greatest of map_appointment.ADC_UPDT and the '
                                                   'per-SCH_EVENT_ID maxima of ADC_UPDT from '
                                                   'map_appointment_schedule and map_appointment_resource. '
                                                   'This combines all contributing '
                                                   'appointment/booking/resource load clocks, independently '
                                                   'of chosen slot times, location or personnel; it is '
                                                   'bronze ingestion provenance, not Silver refresh time.',
                                      'location_code': 'Millennium scheduling location code: the lowest '
                                                       "APPT_LOCATION_CD across the appointment's resource "
                                                       'rows, else the lowest '
                                                       'map_appointment_schedule.LOCATION_CD. Decodes '
                                                       'through reference_location (location_code, any '
                                                       'level).',
                                      'location_display': 'Display of location_code from the same side that '
                                                          'supplied it: '
                                                          'map_appointment_resource.APPT_LOCATION_DESCRIPTION, '
                                                          'else coalesce(LOCATION_DESCRIPTION, '
                                                          'LOCATION_FREETEXT) from the schedule row with '
                                                          'that code. NULL when location_code is NULL.',
                                      'organization_id': 'Millennium ORGANIZATION_ID for the scheduling '
                                                         'organization.',
                                      'original_requested_end': 'Original requested slot end.',
                                      'original_requested_start': 'Original requested slot start.',
                                      'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                           'cross-feed join key.',
                                      'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                      'record_status': 'Superseded when map_appointment.SOURCE_PRESENT_IND '
                                                       'cast to Boolean is false, ACTIVE_IND cast to LONG is '
                                                       'zero, or a non-null END_EFFECTIVE_DT_TM is before '
                                                       '2100-01-01; otherwise active. Missing presence '
                                                       'defaults true and missing activity defaults 1. '
                                                       'Source absence does not emit retracted; scheduling '
                                                       'status and booking/resource flags are separate.',
                                      'record_status_effective_from': 'map_appointment.BEG_EFFECTIVE_DT_TM '
                                                                      'carried unchanged. No scheduled-slot, '
                                                                      'requested-start, first-booked or '
                                                                      'ingestion-time fallback is applied; a '
                                                                      'missing value remains null.',
                                      'record_status_effective_to': 'For superseded appointment rows, first '
                                                                    'non-null SOURCE_ABSENT_DETECTED_TS, '
                                                                    'END_EFFECTIVE_DT_TM, then ADC_UPDT; '
                                                                    'otherwise null. The selected end is not '
                                                                    'range-clamped, so an inactive/absent '
                                                                    'row can retain a far-future end. This '
                                                                    'is row-status history, not the '
                                                                    'scheduled slot end.',
                                      'recurrence_parent_key': 'Deterministic SHA-256 key of the parent '
                                                               'appointment.',
                                      'recurrence_parent_sch_event_id': 'Millennium parent SCH_EVENT_ID for '
                                                                        'recurring appointments when '
                                                                        'supplied.',
                                      'recurrence_type_flag': 'Raw source recurrence flag.',
                                      'referral_identifier': 'UBRN or other referral alias retained as '
                                                             'linkage evidence only.',
                                      'requested_datetime': 'Source referral/request timestamp.',
                                      'requested_practitioner_id': 'Millennium personnel PERSON_ID for the '
                                                                   'requested practitioner.',
                                      'sch_event_id': 'Millennium SCH_EVENT_ID; primary key of this table.',
                                      'service_resource_code': 'Millennium SERVICE_RESOURCE_CD from the '
                                                               'latest SCHEDULE_SEQ (lowest code on ties); '
                                                               'populated for theatre/endoscopy/cath-lab '
                                                               'bookings only, NULL for outpatient clinics.',
                                      'service_resource_display': 'Source SERVICE_RESOURCE_DESCRIPTION for '
                                                                  'service_resource_code.',
                                      'source_code': 'Verbatim appointment-type code.',
                                      'source_coding_system': 'Verbatim appointment-type coding system.',
                                      'source_display': 'Verbatim appointment-type display.',
                                      'source_feed': 'Registered feed owning the fact.',
                                      'source_update_timestamp': 'Greatest of '
                                                                 'map_appointment.SOURCE_ADC_UPDT and the '
                                                                 'per-SCH_EVENT_ID maxima of SOURCE_ADC_UPDT '
                                                                 'from map_appointment_schedule and '
                                                                 'map_appointment_resource. All contributing '
                                                                 'booking/resource rows participate, not '
                                                                 'only current rows or those supplying a '
                                                                 'chosen slot/location. No ADC_UPDT '
                                                                 'ingestion fallback or Silver refresh '
                                                                 'timestamp is added.',
                                      'status_code': 'Source scheduling-state code.',
                                      'status_display': 'Source scheduling-state display.',
                                      'status_meaning': 'Source scheduling-state meaning.',
                                      'subject_id_system': 'Identifier system used for subject_key.',
                                      'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                     'identifier. Not salted, not secret; a stable join key '
                                                     'across feeds where person_id is unresolved.',
                                      'vip_ind': 'Source VIP indicator when supplied.',
                                      'withheld_identity_ind': 'Withheld-identity indicator when supplied.'},
                          'table': 'Scheduling appointments with ordered booking and resource history; '
                                   'SCH_EVENT_ID is never treated as a clinical EVENT_ID.'}}
