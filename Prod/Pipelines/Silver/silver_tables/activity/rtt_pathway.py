"""Silver activity.rtt_pathway: 4 dataset(s).

- datasets: _clinical_rtt_pathway_primitive, _clinical_qc_rtt_pathway, clinical_rtt_pathway, _clinical_rtt_pathway_metadata
- moved verbatim from: 4_silver_care_pathways:1532, 4_silver_care_pathways:1549, 4_silver_care_pathways:1570, 4_silver_care_pathways:1623, 4_silver_care_pathways:1688, 4_silver_care_pathways:1707

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark
from silver_tables._common.journey import _rtt_pathway_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical /  rtt pathway primitive
@materialized_view(
    name=_n("journey_clinical._rtt_pathway_primitive"),
    private=True,
    refresh_policy="auto",
)
def _rtt_pathway_primitive():
    # Build the declared dataset: Internal RTT period rows; encounter-type tags cross as JSON.
    return _rtt_pathway_canonical().drop("_source_system", "_source_table", "_source_row_id")


# Clinical /  qc rtt pathway
@materialized_view(
    name=_n("journey_clinical._qc_rtt_pathway"),
    refresh_policy="auto",
)
def _qc_rtt_pathway():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for rtt_pathway.
    return _cross_qc_primitive(
        spark.read.table(_n("journey_clinical._rtt_pathway_primitive")),
        "rtt_pathway",
        {"encounter_types": "_encounter_types_json"},
    )


# Clinical / rtt pathway

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
RTT_PATHWAY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'source_system_oid',
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
    'pathway_oid',
    'period_oid',
    'is_latest_period',
    'start_status_code',
    'start_status_display',
    'stop_status_code',
    'stop_status_display',
    'current_status_code',
    'current_status_display',
    'sequence_asc',
    'sequence_desc',
    'clock_discrepant',
    'core_clock_start',
    'core_clock_stop',
    'pathway_start_date',
    'pathway_type_code',
    'pathway_type_display',
    'breach_date',
    'days_waited',
    'days_waited_active',
    'op_appt_dna_count',
    'treatment_function_code',
    'treatment_function_display',
    'site_code',
    'site_display',
    'referring_facility_code',
    'referring_facility_display',
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


RTT_PATHWAY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_system_oid',
    'pathway_oid',
    'period_oid',
    'identity_status',
    'person_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_clinical.rtt_pathway"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def rtt_pathway():
    # Build the declared dataset: LUNA RTT pathway/clock periods (hybrid grain: one row per
    # period, plus clockless-pathway sentinel rows). Clock discrepancies are carried, never
    # filtered.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_rtt_pathway")),
        "rtt_pathway",
        {},
        RTT_PATHWAY_PUBLIC_COLUMNS,
        RTT_PATHWAY_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._rtt_pathway_metadata'),
    refresh_policy="auto",
)
def rtt_pathway_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_rtt_pathway;
    # same row grain as the research table. Join keys: patient_event_key, source_system_oid,
    # pathway_oid, period_oid.
    return (spark.read.table(_n("journey_clinical._qc_rtt_pathway"))).select(*RTT_PATHWAY_LIFECYCLE_COLUMNS)


CROSS_QC = {
    'rtt_pathway': CrossQc(model='rtt_pathway', fk=('person_id',), dates=('event_after_death_30d', 'event_before_birth'), promote_lifecycle=['identity_status', 'person_id_resolved']),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_rtt_pathway': ValuePolicy(table='clinical_rtt_pathway', enabled=True),
}


# W2.10 (L5193) spine_episode period_start/end_derived: NOT built. Gate W2_10_episode_drift (ledger) found the
# earliest member-encounter start more than 1 day from the source period_start on 83.3% of 4.33M episodes
# (median 27 days later), above the 0.20 ship line, so neither the episode edit nor its comments ship.

# W2.11 (L3301, J18): period_oid 0 is LUNA's clockless-pathway sentinel (MILL_ZERO_KEEP); flag it.
VALUE_POLICIES["clinical_rtt_pathway"].register("is_clockless", ["period_oid"],
         lambda F: F.coalesce(F.col("period_oid").cast("bigint") == 0, F.lit(False)),
         "True for a clockless pathway (period_oid 0, LUNA's no-clock sentinel; kept, not NULLed). INSR_W2 W2.11.")


WRAP_POLICY = {'clinical_rtt_pathway'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_qc_rtt_pathway': {'columns': {},
                              'table': 'Private JSON bridge and Gold cross-rule flags for rtt_pathway.'},
 '_clinical_rtt_pathway_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                    'table': 'Internal quality and batch metadata for clinical_rtt_pathway; '
                                             'same row grain as the research table. Join keys: '
                                             'patient_event_key, source_system_oid, pathway_oid, '
                                             'period_oid.'},
 '_clinical_rtt_pathway_primitive': {'columns': {},
                                     'table': 'Internal RTT period rows; encounter-type tags cross as JSON.'},
 'clinical_rtt_pathway': {'columns': {'breach_date': 'Source breach timestamp.',
                                      'clock_discrepant': 'Bronze flag indicating disagreement with the core '
                                                          'clock timestamps.',
                                      'confidentiality_code': 'Source confidentiality code when supplied.',
                                      'core_clock_start': 'Core-system clock start retained as discrepant '
                                                          'sidecar data.',
                                      'core_clock_stop': 'Core-system clock stop retained as discrepant '
                                                         'sidecar data.',
                                      'current_status_code': 'Source current RTT-status code.',
                                      'current_status_display': 'Source current RTT-status display.',
                                      'days_waited': 'Source total days waited.',
                                      'days_waited_active': 'Source active days waited.',
                                      'encounter_id': 'Nullable Millennium ENCNTR_ID; the LUNA pathway feed '
                                                      'supplies no encounter.',
                                      'event_datetime': 'RTT clock-period start timestamp.',
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
                                      'event_end_datetime': 'RTT clock-period stop timestamp.',
                                      'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                  'wall-clock time (GMT/BST), by the same '
                                                                  'rule as event_datetime_local; null where '
                                                                  'event_end_datetime is null.',
                                      'fact_category': 'Whether this fact is clinical or administrative in '
                                                       'the v2 plane merge.',
                                      'is_clockless': "True for a clockless pathway (period_oid 0, LUNA's "
                                                      'no-clock sentinel; kept, not NULLed). INSR_W2 W2.11.',
                                      'is_latest_period': 'Whether this is the latest period for the '
                                                          'pathway.',
                                      'loaded_at': 'map_rtt_pathway.ADC_UPDT carried unchanged through the '
                                                   'RTT primitive and quality wrappers. This is bronze '
                                                   'ingestion provenance, not a clinical RTT clock, period '
                                                   'creation/modification time or Silver refresh time; a '
                                                   'missing source timestamp remains null.',
                                      'op_appt_dna_count': 'Source outpatient did-not-attend count.',
                                      'pathway_oid': 'Raw LUNA pathway object identifier.',
                                      'pathway_start_date': 'Source pathway start date.',
                                      'pathway_type_code': 'Source pathway-type code.',
                                      'pathway_type_display': 'Source pathway-type display.',
                                      'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                           'cross-feed join key.',
                                      'period_oid': 'Raw LUNA period object identifier; 0 marks the '
                                                    'clockless-pathway sentinel and the column is never '
                                                    'null.',
                                      'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                   'foreign key.',
                                      'record_status': 'Derived LUNA RTT row status: superseded when '
                                                       'SOURCE_PRESENT_IND cast to Boolean, '
                                                       'PERIOD_ACTIVE_IND or CORE_ACTIVE_IND is false; '
                                                       'otherwise active. Each missing flag defaults true. '
                                                       'Source absence maps to superseded, not retracted, '
                                                       'and this label is separate from the clinical RTT '
                                                       'status or clock-stop state.',
                                      'record_status_effective_from': 'map_rtt_pathway.PERIOD_CREATED_DATETIME '
                                                                      'carried unchanged as the history '
                                                                      'start. No RTT clock-start, '
                                                                      'pathway-start or ingestion-time '
                                                                      'fallback is applied; a missing source '
                                                                      'value remains null.',
                                      'record_status_effective_to': 'For a superseded RTT row, first '
                                                                    'non-null '
                                                                    'map_rtt_pathway.SOURCE_ABSENT_DETECTED_TS, '
                                                                    'PERIOD_MODIFIED_DATETIME, then '
                                                                    'ADC_UPDT. Null for active rows or when '
                                                                    'every fallback is missing. This is a '
                                                                    'derived row-status end that may use '
                                                                    'ingestion time; clinical STOP_DATETIME '
                                                                    'is not used as the fallback.',
                                      'referring_facility_code': 'Source referring-facility code.',
                                      'referring_facility_display': 'Source referring-facility display.',
                                      'sequence_asc': 'Ascending pathway-period sequence.',
                                      'sequence_desc': 'Descending pathway-period sequence.',
                                      'site_code': 'Source site code.',
                                      'site_display': 'Source site display.',
                                      'source_code': 'Verbatim current RTT-status code.',
                                      'source_coding_system': 'Verbatim LUNA RTT-status coding system.',
                                      'source_display': 'Verbatim current RTT-status display.',
                                      'source_feed': 'Registered feed owning the fact.',
                                      'source_system_oid': 'LUNA SOURCE_SYSTEM_OID; component of the native '
                                                           'pathway-period primary key.',
                                      'source_update_timestamp': 'map_rtt_pathway.SOURCE_ADC_UPDT carried '
                                                                 'unchanged for the contributing '
                                                                 'pathway/period row. It is distinct from '
                                                                 'ADC_UPDT ingestion provenance and is not '
                                                                 'PERIOD_MODIFIED_DATETIME, an RTT clock '
                                                                 'start/stop or Silver refresh time; a '
                                                                 'missing source timestamp remains null.',
                                      'start_status_code': 'Source clock-start RTT-status code.',
                                      'start_status_display': 'Source clock-start RTT-status display.',
                                      'stop_status_code': 'Source clock-stop RTT-status code.',
                                      'stop_status_display': 'Source clock-stop RTT-status display.',
                                      'subject_id_system': 'Identifier system used for subject_key.',
                                      'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                     'identifier. Not salted, not secret; a stable join key '
                                                     'across feeds where person_id is unresolved.',
                                      'treatment_function_code': 'Source treatment-function code.',
                                      'treatment_function_display': 'Source treatment-function display.',
                                      'vip_ind': 'Source VIP indicator when supplied.',
                                      'withheld_identity_ind': 'Withheld-identity indicator when supplied.'},
                          'table': 'LUNA RTT pathway/clock periods (hybrid grain: one row per period, plus '
                                   'clockless-pathway sentinel rows). Clock discrepancies are carried, never '
                                   'filtered.'}}
