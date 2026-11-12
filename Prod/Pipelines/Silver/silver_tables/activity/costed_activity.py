"""Silver activity.costed_activity: 2 dataset(s).

- datasets: clinical_costed_activity, _clinical_costed_activity_metadata
- moved verbatim from: 4_silver_care_pathways:3064, 4_silver_care_pathways:3148, 4_silver_care_pathways:3229, 4_silver_care_pathways:3319, 4_silver_care_pathways:3324, 4_silver_care_pathways:3336, silver_value_policy.py:235, silver_value_policy.py:236

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n
from silver_tables._common.journey import _costed_activity_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# W2.06 J23 (L2306): PLICS FY-end defaults. The supplied end is kept as episode_end_raw and the default is flagged;
# the value rule in §4 NULLs the defaulted end on admitted PODs only.
_J23_DEFAULT = "2021-03-31 23:59:00"


_J23_ADMITTED_PODS = ("NEL", "EL", "DC")


def _j23_defaulted(F):
    """True when episode_end_datetime is the PLICS FY-end default (2021-03-31 23:59, no departure date)."""
    return F.coalesce((F.col("episode_end_datetime") == F.lit(_J23_DEFAULT).cast("timestamp"))
                      & F.col("departure_date").isNull(), F.lit(False))


# Clinical / costed activity

# contract v2: retain the full canonical shape for internal reuse.
COSTED_ACTIVITY_SOURCE_COLUMNS = [
    "patient_event_key",
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
    "extract_cd",
    "activity_record_id",
    "feed_type",
    "plemi",
    "nhs_number_status_cd",
    "cds_id",
    "attendance_id",
    "arrival_date",
    "arrival_time",
    "departure_date",
    "departure_time",
    "departure_type_cd",
    "provider_org_cd",
    "patient_org_cd",
    "pathway_id",
    "pod_cd",
    "treatment_function_cd",
    "source_los",
    "cf_band_cd",
    "episode_number",
    "episode_start_datetime",
    "episode_end_datetime",
    "episode_type_cd",
    "hosp_spell_id",
    "hrg_cd",
    "hrg_desc",
    "fce_hrg_cd",
    "fce_hrg_desc",
    "spell_hrg_cd",
    "spell_hrg_desc",
    "appointment_date",
    "appointment_time",
    "critical_care_unit_function_cd",
    "organs_supported",
    "critical_care_period_type_cd",
    "critical_care_level_ind",
    "unbundled_activity_datetime",
    "unbundled_activity_cd",
    "unbundled_hrg_cd",
    "unbundled_hrg_desc",
    "partial_costing_ind",
    "care_datetime",
    "care_id",
    "clinical_contact_duration",
    "chs_currency_cd",
    "team_type_cd",
    "contact_subject_cd",
    "consult_type_cd",
    "consult_medium_cd",
    "location_cd",
    "gp_therapy_ind",
    "service_request_id",
    "cost_line_count",
    "total_cost_sum",
    "total_o_cost_sum",
    "person_link_method",
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


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
COSTED_ACTIVITY_PUBLIC_COLUMNS = [
    'patient_event_key',
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
    'extract_cd',
    'activity_record_id',
    'feed_type',
    'plemi',
    'nhs_number_status_cd',
    'cds_id',
    'attendance_id',
    'arrival_date',
    'arrival_time',
    'departure_date',
    'departure_time',
    'departure_type_cd',
    'provider_org_cd',
    'patient_org_cd',
    'pathway_id',
    'pod_cd',
    'treatment_function_cd',
    'source_los',
    'cf_band_cd',
    'episode_number',
    'episode_start_datetime',
    'episode_end_datetime',
    'episode_type_cd',
    'hosp_spell_id',
    'hrg_cd',
    'hrg_desc',
    'fce_hrg_cd',
    'fce_hrg_desc',
    'spell_hrg_cd',
    'spell_hrg_desc',
    'appointment_date',
    'appointment_time',
    'critical_care_unit_function_cd',
    'organs_supported',
    'critical_care_period_type_cd',
    'critical_care_level_ind',
    'unbundled_activity_datetime',
    'unbundled_activity_cd',
    'unbundled_hrg_cd',
    'unbundled_hrg_desc',
    'partial_costing_ind',
    'care_datetime',
    'care_id',
    'clinical_contact_duration',
    'chs_currency_cd',
    'team_type_cd',
    'contact_subject_cd',
    'consult_type_cd',
    'consult_medium_cd',
    'location_cd',
    'gp_therapy_ind',
    'service_request_id',
    'cost_line_count',
    'total_cost_sum',
    'total_o_cost_sum',
    'person_link_method',
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


COSTED_ACTIVITY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'extract_cd',
    'activity_record_id',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_costed_activity():
    # Assemble costed activity rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return _costed_activity_canonical().select(*COSTED_ACTIVITY_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_clinical.costed_activity"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def costed_activity():
    # Build the declared dataset: One frozen PLICS costed activity from FY19/20 through FY21/22
    # extracts.
    return _lifecycle_source_costed_activity().select(*COSTED_ACTIVITY_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._costed_activity_metadata'),
    refresh_policy="auto",
)
def costed_activity_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_costed_activity; same row grain as the research table. Join keys:
    # patient_event_key, extract_cd, activity_record_id.
    return (_lifecycle_source_costed_activity()).select(*COSTED_ACTIVITY_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'clinical_costed_activity': ValuePolicy(table='clinical_costed_activity', enabled=True),
}


VALUE_POLICIES["clinical_costed_activity"].register("episode_end_raw", ["episode_end_datetime"],
         lambda F: F.col("episode_end_datetime"),
         "episode_end_datetime exactly as supplied by the PLICS extract, before the J23 FY-end default rule. INSR_W2.")


VALUE_POLICIES["clinical_costed_activity"].register("episode_end_defaulted_ind", ["episode_end_datetime", "departure_date"],
         _j23_defaulted,
         "True when the episode end is the PLICS financial-year-end default (2021-03-31 23:59 with no departure "
         "date): an extract boundary, not a discharge (J23). INSR_W2.")


# W2.06 J23 (L2306): admitted PODs (NEL/EL/DC; 216 rows) lose the defaulted end, which is not a discharge.
# Non-admitted PODs (1,342,681 rows) keep it, flagged by episode_end_defaulted_ind. A NULL pod_cd is not an admitted
# POD (isin on NULL is NULL, so it is coalesced; the lean fixture found the NULL path NULLing the end).
VALUE_POLICIES["clinical_costed_activity"].value_rule("episode_end_datetime", ["episode_end_datetime", "departure_date", "pod_cd"],
           lambda F: F.when(~(_j23_defaulted(F)
                              & F.coalesce(F.upper(F.trim(F.col("pod_cd"))).isin(*_J23_ADMITTED_PODS), F.lit(False))),
                            F.col("episode_end_datetime")))


WRAP_POLICY = {'clinical_costed_activity'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_costed_activity_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                    'activity_record_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                    'admission_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                    'allergy_instance_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                    'aria_dx_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                    'aria_pt_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                    'artifact_link_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                    'birth_row_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                    'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                    'care_contact_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'care_participation_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                    'cds_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                    'chemotherapy_course_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                    'community_care_activity_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                    'community_care_contact_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                    'concept_map_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'cost_line_item_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                    'dcp_forms_activity_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                    'device_mapping_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                    'diagnosis_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'dicom_path': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                    'doc_response_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'elective_access_procedure_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                    'encounter_attribute_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                    'encounter_bounds_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                    'encounter_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'encounter_id_resolved': 'True when encounter_id is null '
                                                                             'or resolves to the governed '
                                                                             'encounter spine; false records '
                                                                             'unresolved linkage without '
                                                                             'dropping the row.',
                                                    'encounter_identifier_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                    'encounter_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                    'endobase_exam_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'endobase_exam_term_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                    'entity_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                    'entry_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                    'episode_encounter_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                    'episode_encounter_reltn_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                    'episode_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                    'episode_id_resolved': 'True when episode_id is null or '
                                                                           'resolves to the governed episode '
                                                                           'spine; false records unresolved '
                                                                           'linkage without dropping the '
                                                                           'row.',
                                                    'episode_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                                    'event_after_death_30d': 'True when the event timestamp '
                                                                             'is more than 30 days after the '
                                                                             "resolved person's death "
                                                                             'timestamp; silver records the '
                                                                             'doubt and does not filter the '
                                                                             'row.',
                                                    'event_before_birth': 'True when the event timestamp '
                                                                          "precedes the resolved person's "
                                                                          'birth timestamp; silver records '
                                                                          'the doubt and does not filter the '
                                                                          'row.',
                                                    'event_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                    'extract_cd': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                    'fhx_activity_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'gene_tested_row_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                    'genetic_result_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                    'genetic_test_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'homecare_request_item_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                    'identity_status': 'Subject identity resolution state '
                                                                       'recorded by the silver canonical '
                                                                       'query.',
                                                    'implant_event_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'implant_sequence': 'Primary-key component carried '
                                                                        'unchanged from the public parent; '
                                                                        "combine it with the companion's "
                                                                        'other key columns when joining.',
                                                    'indication_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                    'is_illogical': 'Source RTT chronology flag indicating '
                                                                    'an illogical activity sequence; '
                                                                    'retained for the gold drop rule.',
                                                    'journey_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                                    'journey_link_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'journey_participant_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                    'labour_delivery_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                    'load_batch_id': 'Deterministic batch token derived from '
                                                                     "the source row's bronze load "
                                                                     'timestamp.',
                                                    'loaded_at': 'Latest bronze load timestamp contributing '
                                                                 'to the public row; never the lifecycle '
                                                                 "pipeline's wall-clock time.",
                                                    'location_code': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                    'location_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                    'location_stay_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                    'mc_device_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'microbiology_isolate_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                    'order_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                    'organization_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'organization_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'pacs_examination_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                    'pacs_report_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                    'pathway_oid': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                    'patient_event_key': 'Deterministic SHA-256 event key '
                                                                         'carried unchanged from the public '
                                                                         'parent; joins one lifecycle row to '
                                                                         'that event row.',
                                                    'patient_event_row_key': 'Deterministic row-level key '
                                                                             'carried unchanged from '
                                                                             'events_patient_event; joins '
                                                                             'one lifecycle row to one '
                                                                             'event-index row.',
                                                    'period_business_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                    'period_oid': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                    'person_address_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                    'person_attribute_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                    'person_death_evidence_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                    'person_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                    'person_id_resolved': 'True when person_id is null or '
                                                                          'resolves to the governed person '
                                                                          'spine; false records unresolved '
                                                                          'linkage without dropping the row.',
                                                    'person_identifier_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                    'person_relationship_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                    'pharmacy_issue_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                    'pm_wait_list_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'practitioner_group_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                    'practitioner_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'practitioner_identifier_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                    'practitioner_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'practitioner_location_evidence_key': 'Deterministic v2 '
                                                                                          'key carried '
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
                                                    'pregnancy_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                    'problem_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                    'procedure_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'pt_prot_reg_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                    'ptl_unique_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                    'record_status': 'active, superseded or retracted, '
                                                                     "normalised from the source's own "
                                                                     'status fields; superseded rows are '
                                                                     'earlier versions the source kept.',
                                                    'record_status_effective_from': 'Timestamp from which '
                                                                                    'the normalised record '
                                                                                    'status applies, when '
                                                                                    'the source supplies '
                                                                                    'one.',
                                                    'record_status_effective_to': 'Timestamp until which the '
                                                                                  'normalised record status '
                                                                                  'applies; null means no '
                                                                                  'source end is known.',
                                                    'referral_oid': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
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
                                                    'row_source': 'Physical bronze row source, CURRENT or '
                                                                  'HIST, carried unchanged from the public '
                                                                  'parent; a key component because the '
                                                                  'research table is at physical version '
                                                                  'grain and patient_event_key is shared '
                                                                  "across an entry's versions.",
                                                    'rtt_activity_oid': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'sch_event_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'score_type': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                    'sequence_nbr': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                    'service_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                    'service_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                                    'snapshot_date': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                    'source_daily_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'source_identifier_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                    'source_object': 'Source-arm discriminator carried '
                                                                     'unchanged from the public parent; use '
                                                                     'it with arm-specific native '
                                                                     'identifiers when joining.',
                                                    'source_site': 'Primary-key component carried unchanged '
                                                                   'from the public parent; combine it with '
                                                                   "the companion's other key columns when "
                                                                   'joining.',
                                                    'source_system_oid': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                    'source_unit': 'Primary-key component carried unchanged '
                                                                   'from the public parent; combine it with '
                                                                   "the companion's other key columns when "
                                                                   'joining.',
                                                    'source_update_timestamp': 'Native source-system update '
                                                                               'timestamp carried through '
                                                                               'the canonical silver query.',
                                                    'source_version_id': 'Native source version identifier '
                                                                         'carried unchanged from the public '
                                                                         'parent; CURRENT rows carry -1. A '
                                                                         'key component alongside '
                                                                         'row_source.',
                                                    'surg_case_proc_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                    'susceptibility_result_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                    'theatre_attendance_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                    'theatre_case_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                    'theatre_implant_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                    'transfusion_event_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                    'transfusion_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'treatment_cycle_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                    'treatment_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                    'update_count': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                    'valid_from_datetime': 'Primary-key component carried '
                                                                           'unchanged from the public '
                                                                           'parent; combine it with the '
                                                                           "companion's other key columns "
                                                                           'when joining.',
                                                    'value_set_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                    'waiting_list_oid': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'waiting_list_snapshot_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.'},
                                        'table': 'Internal quality and batch metadata for '
                                                 'clinical_costed_activity; same row grain as the research '
                                                 'table. Join keys: patient_event_key, extract_cd, '
                                                 'activity_record_id.'},
 'clinical_costed_activity': {'columns': {'activity_record_id': 'SLAM ACTIVITY_RECORD_ID as BIGINT; second '
                                                                'component of the primary key.',
                                          'appointment_date': 'Appointment date for the costed outpatient or '
                                                              'non-admitted activity, as supplied by the '
                                                              'source extract.',
                                          'appointment_time': 'Appointment time for the costed activity as '
                                                              'supplied by the source extract, stored in a '
                                                              'timestamp column.',
                                          'arrival_date': 'Arrival date for the attendance, carried from the '
                                                          'source costing extract.',
                                          'arrival_time': 'Arrival time for the attendance, carried from the '
                                                          'source costing extract and held as a timestamp.',
                                          'attendance_id': 'Source attendance identifier for the activity '
                                                           'record as supplied by the costing extract.',
                                          'care_datetime': 'Community care contact date and time supplied by '
                                                           'the PE2122 NCC extract.',
                                          'care_id': 'Source identifier for the community care contact '
                                                     'record, supplied by the PE2122 NCC extract.',
                                          'cds_id': 'Commissioning Data Set record identifier carried from '
                                                    'the source costing extract and used in the CDS-to-MRN '
                                                    'person linkage fallback.',
                                          'cf_band_cd': 'Cystic Fibrosis year-of-care currency band supplied '
                                                        'by the source.',
                                          'chs_currency_cd': 'Community health services currency code '
                                                             'supplied by the PE2122 NCC costing extract.',
                                          'clinical_contact_duration': 'Duration of the clinical contact '
                                                                       'recorded in the PE2122 NCC extract, '
                                                                       'in the units supplied by the source.',
                                          'confidentiality_code': 'Source confidentiality classification '
                                                                  'attached to the record for each costed '
                                                                  'activity record. It is produced by the '
                                                                  'silver transformation and has no direct '
                                                                  'bronze-column lineage entry. Source code '
                                                                  'meanings and sentinel values are retained '
                                                                  'unless the pipeline explicitly maps them; '
                                                                  'null means no code was supplied.',
                                          'consult_medium_cd': 'Consultation medium code recording how the '
                                                               'community contact was conducted, as supplied '
                                                               'by the PE2122 NCC costing extract (source '
                                                               'CMedium).',
                                          'consult_type_cd': 'Consultation type code for the community '
                                                             'contact as supplied by the PE2122 NCC costing '
                                                             'extract (source ConsultType).',
                                          'contact_subject_cd': 'Community contact subject code supplied by '
                                                                'the PE2122 NCC costing extract (source '
                                                                'CCSubject), preserved without an invented '
                                                                'mapping.',
                                          'cost_line_count': 'Duplicate-weighted count of '
                                                             'map_slam_cost_line_item rows aggregated for '
                                                             'this activity record, using the collapsed '
                                                             'source duplicate counts.',
                                          'critical_care_level_ind': 'Critical care level indicator supplied '
                                                                     'by the source costing extract.',
                                          'critical_care_period_type_cd': 'Critical care period type code '
                                                                          'supplied by the source costing '
                                                                          'extract, preserved without an '
                                                                          'invented mapping.',
                                          'critical_care_unit_function_cd': 'Critical care unit function '
                                                                            'code for the activity record, '
                                                                            'supplied by the source costing '
                                                                            'extract without an invented '
                                                                            'mapping.',
                                          'departure_date': 'Departure date for the attendance, carried from '
                                                            'the source costing extract (DepDate).',
                                          'departure_time': 'Departure time for the attendance, carried from '
                                                            'the source costing extract and held as a '
                                                            'timestamp.',
                                          'departure_type_cd': 'Source code describing the type of departure '
                                                               'recorded for the attendance; preserved '
                                                               'without an invented mapping.',
                                          'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                          'available.',
                                          'episode_end_datetime': 'Episode end date and time as supplied by '
                                                                  'the source PLICS costing extract '
                                                                  '(EpEnDte).',
                                          'episode_end_defaulted_ind': 'True when the episode end is the '
                                                                       'PLICS financial-year-end default '
                                                                       '(2021-03-31 23:59 with no departure '
                                                                       'date): an extract boundary, not a '
                                                                       'discharge (J23). INSR_W2.',
                                          'episode_end_raw': 'episode_end_datetime exactly as supplied by '
                                                             'the PLICS extract, before the J23 FY-end '
                                                             'default rule. INSR_W2.',
                                          'episode_number': 'Sequence number of the episode within the '
                                                            'hospital spell, as supplied by the source '
                                                            'costing extract (EpiNo).',
                                          'episode_start_datetime': 'Episode start date and time as supplied '
                                                                    'by the source PLICS costing extract '
                                                                    '(EpStDte).',
                                          'episode_type_cd': 'Source-supplied episode type classification '
                                                             'code from the PLICS costing extract, preserved '
                                                             'without an invented mapping.',
                                          'event_datetime': 'Start date and time of the costed activity as '
                                                            'supplied by the source PLICS extract '
                                                            '(ActivityStartDate).',
                                          'event_datetime_local': 'event_datetime in Europe/London '
                                                                  'wall-clock time (GMT/BST). Millennium '
                                                                  'rows take the bronze *_LOCAL companions '
                                                                  'of the same source columns and fallback '
                                                                  'order; rows from local-clock sources '
                                                                  '(LUNA, PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                  'BadgerNet, EndoBase, SLAM, iWeb, Datix '
                                                                  'and similar) carry event_datetime '
                                                                  'unchanged. event_datetime is a UTC '
                                                                  'instant for Millennium rows, so take '
                                                                  'local calendar dates and clock times from '
                                                                  'this column.',
                                          'event_end_datetime': 'End date and time of the costed activity as '
                                                                'supplied by the source PLICS extract '
                                                                '(ActivityEndDate).',
                                          'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                      'wall-clock time (GMT/BST), by the '
                                                                      'same rule as event_datetime_local; '
                                                                      'null where event_end_datetime is '
                                                                      'null.',
                                          'extract_cd': 'SLAM EXTRACT_CD; first component of the primary '
                                                        'key.',
                                          'fact_category': 'Contract category used to group the record with '
                                                           'comparable clinical facts for each costed '
                                                           'activity record. It is produced by the silver '
                                                           'transformation and has no direct bronze-column '
                                                           'lineage entry. Whitespace and source sentinel '
                                                           'text are retained unless the pipeline explicitly '
                                                           'normalizes them; null means no value was '
                                                           'supplied.',
                                          'fce_hrg_cd': 'Healthcare Resource Group code derived at finished '
                                                        'consultant episode level in the source costing '
                                                        'extract.',
                                          'fce_hrg_desc': 'Description of FCE_HRG_CD sourced from the HRG v4 '
                                                          'reference lookup.',
                                          'feed_type': 'Source-supplied code identifying the activity feed '
                                                       'the costed record came from, preserved without an '
                                                       'invented mapping.',
                                          'gp_therapy_ind': 'Source-supplied therapy indicator flag for the '
                                                            'community contact taken from the PE2122 NCC '
                                                            'extract (source GPTherapyInd), preserved '
                                                            'without an invented mapping.',
                                          'hosp_spell_id': 'Source hospital spell identifier for the '
                                                           'activity record, as supplied by the PLICS '
                                                           'costing extract.',
                                          'hrg_cd': 'Healthcare Resource Group (HRG4+) code assigned to the '
                                                    'costed activity record in the source extract.',
                                          'hrg_desc': 'Description of HRG_CD sourced from the HRG v4 '
                                                      'reference lookup.',
                                          'loaded_at': 'map_slam_costed_activity.ADC_UPDT carried unchanged '
                                                       'for the contributing activity row, identical to '
                                                       'source_update_timestamp. No cost-line child clock is '
                                                       "joined or aggregated here. This is the input row's "
                                                       'load provenance, not the clinical activity date or '
                                                       'Silver refresh time.',
                                          'location_cd': 'Source location code for the site or place where '
                                                         'the costed activity or contact took place, as '
                                                         'supplied by the PE2122 NCC extract (source '
                                                         'LocCode).',
                                          'nhs_number_status_cd': 'Non-identifying source quality/status '
                                                                  'code used during linkage.',
                                          'organs_supported': 'Source-supplied critical care organs '
                                                              'supported value for the activity record.',
                                          'partial_costing_ind': 'Source indicator flagging partially costed '
                                                                 'activity, available only from the PE2122 '
                                                                 'NCC extract.',
                                          'pathway_id': 'Patient pathway identifier for the activity record '
                                                        'as supplied by the source costing extract (PathID); '
                                                        'use with EXTRACT_CD as extract-scoped identifiers '
                                                        'are not unique across extracts.',
                                          'patient_event_key': 'Deterministic event key derived from source '
                                                               'identifiers; retained as a stable join key.',
                                          'patient_org_cd': "Organisation code associated with the patient's "
                                                            'commissioner or responsible organisation, as '
                                                            'supplied by the source extract.',
                                          'person_id': 'Native Millennium PERSON_ID as BIGINT when resolved.',
                                          'person_link_method': 'Code recording the linkage path used to '
                                                                'derive PERSON_ID, distinguishing the '
                                                                'NHS-number alias match from the CDS-to-MRN '
                                                                'fallback.',
                                          'plemi': 'PLICS matching identifier reused across extracts; never '
                                                   'join without EXTRACT_CD.',
                                          'pod_cd': 'Source point-of-delivery classification; preserved '
                                                    'without an invented mapping.',
                                          'provider_org_cd': 'Organisation code of the provider submitting '
                                                             'the costed activity, as supplied by the source '
                                                             'extract (OrgId).',
                                          'record_status': 'Retracted when '
                                                           'map_slam_costed_activity.SOURCE_PRESENT_IND is '
                                                           'false, otherwise active; missing presence '
                                                           'defaults true. No superseded status is emitted. '
                                                           'This is source-row presence, not the clinical '
                                                           "activity's completion state or the "
                                                           'partial-costing indicator.',
                                          'record_status_effective_from': 'Start date and time of the costed '
                                                                          'activity as supplied by the '
                                                                          'source PLICS extract '
                                                                          '(ActivityStartDate).',
                                          'record_status_effective_to': 'map_slam_costed_activity.SOURCE_ABSENT_DETECTED_TS '
                                                                        'only when SOURCE_PRESENT_IND is '
                                                                        'false; otherwise null. Missing '
                                                                        'absence time remains null; '
                                                                        'ACTIVITY_END_DT_TM and ADC_UPDT are '
                                                                        'not fallbacks. This is the '
                                                                        'row-absence end proxy, not the '
                                                                        'clinical activity end.',
                                          'service_request_id': 'Source service request identifier for the '
                                                                'community contact carried from the PE2122 '
                                                                'NCC costing extract (source SerReqID); use '
                                                                'with EXTRACT_CD as extract-scoped '
                                                                'identifiers are not unique across extracts.',
                                          'source_code': 'Source point-of-delivery classification; preserved '
                                                         'without an invented mapping.',
                                          'source_coding_system': 'Source point-of-delivery classification; '
                                                                  'preserved without an invented mapping.',
                                          'source_display': 'Human-readable label supplied by the source '
                                                            'system for the source code for each costed '
                                                            'activity record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Whitespace and source sentinel '
                                                            'text are retained unless the pipeline '
                                                            'explicitly normalizes them; null means no value '
                                                            'was supplied.',
                                          'source_feed': 'Originating data feed responsible for the record '
                                                         'for each costed activity record. It is produced by '
                                                         'the silver transformation and has no direct '
                                                         'bronze-column lineage entry. Whitespace and source '
                                                         'sentinel text are retained unless the pipeline '
                                                         'explicitly normalizes them; null means no value '
                                                         'was supplied.',
                                          'source_los': 'Length-of-stay value carried from the source '
                                                        'costing extract without recalculation.',
                                          'source_update_timestamp': 'map_slam_costed_activity.ADC_UPDT '
                                                                     'carried unchanged, identical to '
                                                                     'loaded_at. The Silver projection '
                                                                     'supplies no independent native '
                                                                     'application-update clock and does not '
                                                                     'substitute Silver refresh time.',
                                          'spell_hrg_cd': 'Healthcare Resource Group code derived at '
                                                          'hospital spell level in the source costing '
                                                          'extract.',
                                          'spell_hrg_desc': 'Description of SPELL_HRG_CD sourced from the '
                                                            'HRG v4 reference lookup.',
                                          'subject_id_system': 'Internal Millennium person identifier '
                                                               'assigned by the person-alias linkage, used '
                                                               'to group activity for the same patient '
                                                               'without publishing direct identifiers.',
                                          'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                         'identifier. Not salted, not secret; a stable join '
                                                         'key across feeds where person_id is unresolved.',
                                          'team_type_cd': 'Coded team type for the community contact, '
                                                          'supplied as an integer by the PE2122 NCC extract.',
                                          'total_cost_sum': 'Duplicate-weighted sum of TOTAL_COST across the '
                                                            'cost line items for this activity record, '
                                                            'retained at exact decimal precision.',
                                          'total_o_cost_sum': 'Duplicate-weighted sum of TOTAL_O_COST, a '
                                                              'component of total cost, across the cost line '
                                                              'items for this activity record, retained at '
                                                              'exact decimal precision.',
                                          'treatment_function_cd': 'Treatment function code for the costed '
                                                                   'activity as supplied by the source '
                                                                   'costing extract (Tfc).',
                                          'unbundled_activity_cd': 'Code identifying the unbundled activity '
                                                                   'component of the costed record, as '
                                                                   'supplied by the source extract.',
                                          'unbundled_activity_datetime': 'Date and time of the unbundled '
                                                                         'activity component associated with '
                                                                         'the costed record.',
                                          'unbundled_hrg_cd': 'Healthcare Resource Group code for the '
                                                              'unbundled activity component of the costed '
                                                              'record.',
                                          'unbundled_hrg_desc': 'Description of UNBUNDLED_HRG_CD sourced '
                                                                'from the HRG v4 reference lookup.',
                                          'vip_ind': 'Indicator that the source record carries VIP handling '
                                                     'status for each costed activity record. It is produced '
                                                     'by the silver transformation and has no direct '
                                                     'bronze-column lineage entry. Null means the source did '
                                                     'not state the indicator and must not be interpreted as '
                                                     'false.',
                                          'withheld_identity_ind': 'Indicator that the source identity was '
                                                                   'deliberately withheld for each costed '
                                                                   'activity record. It is produced by the '
                                                                   'silver transformation and has no direct '
                                                                   'bronze-column lineage entry. Null means '
                                                                   'the source did not state the indicator '
                                                                   'and must not be interpreted as false.'},
                              'table': 'One frozen PLICS costed activity from FY19/20 through FY21/22 '
                                       'extracts.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
COSTED_ACTIVITY_PUBLIC_COLUMNS = list(COSTED_ACTIVITY_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
