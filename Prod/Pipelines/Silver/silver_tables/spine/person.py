"""Silver spine.person: 2 dataset(s).

- datasets: spine_person, _spine_person_metadata
- moved verbatim from: 1_silver_spine_and_reference:352, 1_silver_spine_and_reference:435, 1_silver_spine_and_reference:456

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.cross_qc import _s3_direct_axis
from silver_core.journey_core import _n
from silver_tables._common.journey import _s3_lookup_axis
from silver_tables.spine._shared import (
    PERSON_LIFECYCLE_FIELDS,
    PERSON_RETIRED_COLUMNS,
    SRC_S3B_ETHNICITY_MAP,
    _lifecycle_source_person,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


PERSON_LIFECYCLE_COLUMNS = [
    'person_id',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_spine.person"),
    refresh_policy="auto",
)
def person():
    # Build the declared dataset: One resolved Millennium person. Silver retains inactive,
    # implausible-looking, and deceased rows. Current MRN / NHS number selected
    # deterministically from map_patient_identifier; multi-active pools publish as ambiguous,
    # never tiebroken.
    df = _lifecycle_source_person().drop(*PERSON_LIFECYCLE_FIELDS, *PERSON_RETIRED_COLUMNS)
    df = _s3_direct_axis(
        df, "ethnicity", F.lit("urn:cerner:ethnicity"),
        F.col("ethnicity_code"), F.col("ethnicity_display"),
    )
    return _s3_lookup_axis(
        df, "ethnicity", SRC_S3B_ETHNICITY_MAP, "urn:cerner:ethnicity",
        F.col("ethnicity_code"), allow_split=False, model_name="person", row_key="person_id",
    )


@materialized_view(
    name=_n('journey_spine._person_metadata'),
    refresh_policy="auto",
)
def person_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_person; same row
    # grain as the research table. Join keys: person_id.
    return (_lifecycle_source_person()).select(*PERSON_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'spine_person': ValuePolicy(table='spine_person', enabled=True, zero_as_null=('ethnicity_source_code',)),
}


WRAP_POLICY = {'spine_person'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_spine_person_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from the '
                                                         "public parent; combine it with the companion's "
                                                         'other key columns when joining.',
                                        'activity_record_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                        'admission_key': 'Deterministic v2 key carried unchanged from the '
                                                         'public parent; joins one lifecycle row to that '
                                                         'research row.',
                                        'allergy_instance_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                        'aria_dx_id': 'Native source identifier carried unchanged from the '
                                                      'public parent; joins one lifecycle row to that '
                                                      'research row.',
                                        'aria_pt_id': 'Native source identifier carried unchanged from the '
                                                      'public parent; joins one lifecycle row to that '
                                                      'research row.',
                                        'artifact_link_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                        'birth_row_id': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                        'bloodtrack_transaction_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                        'cancer_treatment_cycle_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                        'care_contact_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                        'care_participation_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                        'cds_id': 'Native source identifier carried unchanged from the '
                                                  'public parent; joins one lifecycle row to that research '
                                                  'row.',
                                        'chemotherapy_course_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                        'community_care_activity_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                        'community_care_contact_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                        'concept_map_key': 'Deterministic v2 key carried unchanged from the '
                                                           'public parent; joins one lifecycle row to that '
                                                           'research row.',
                                        'cost_line_item_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                        'dcp_forms_activity_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                        'device_mapping_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                        'diagnosis_id': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                        'dicom_path': 'Primary-key component carried unchanged from the '
                                                      "public parent; combine it with the companion's other "
                                                      'key columns when joining.',
                                        'doc_response_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                        'elective_access_procedure_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                        'encounter_attribute_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                        'encounter_bounds_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                        'encounter_id': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                        'encounter_id_resolved': 'True when encounter_id is null or resolves '
                                                                 'to the governed encounter spine; false '
                                                                 'records unresolved linkage without '
                                                                 'dropping the row.',
                                        'encounter_identifier_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                        'encounter_key': 'Deterministic v2 key carried unchanged from the '
                                                         'public parent; joins one lifecycle row to that '
                                                         'research row.',
                                        'endobase_exam_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                        'endobase_exam_term_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                        'entity_id': 'Native source identifier carried unchanged from the '
                                                     'public parent; joins one lifecycle row to that '
                                                     'research row.',
                                        'entry_id': 'Native source identifier carried unchanged from the '
                                                    'public parent; joins one lifecycle row to that research '
                                                    'row.',
                                        'episode_encounter_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                        'episode_encounter_reltn_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                        'episode_id': 'Native source identifier carried unchanged from the '
                                                      'public parent; joins one lifecycle row to that '
                                                      'research row.',
                                        'episode_id_resolved': 'True when episode_id is null or resolves to '
                                                               'the governed episode spine; false records '
                                                               'unresolved linkage without dropping the row.',
                                        'episode_key': 'Deterministic v2 key carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                        'event_after_death_30d': 'True when the event timestamp is more than '
                                                                 "30 days after the resolved person's death "
                                                                 'timestamp; silver records the doubt and '
                                                                 'does not filter the row.',
                                        'event_before_birth': 'True when the event timestamp precedes the '
                                                              "resolved person's birth timestamp; silver "
                                                              'records the doubt and does not filter the '
                                                              'row.',
                                        'event_id': 'Native source identifier carried unchanged from the '
                                                    'public parent; joins one lifecycle row to that research '
                                                    'row.',
                                        'extract_cd': 'Primary-key component carried unchanged from the '
                                                      "public parent; combine it with the companion's other "
                                                      'key columns when joining.',
                                        'fhx_activity_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                        'gene_tested_row_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                        'genetic_result_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                        'genetic_test_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                        'homecare_request_item_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                        'identity_status': 'Subject identity resolution state recorded by '
                                                           'the silver canonical query.',
                                        'implant_event_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                        'implant_sequence': 'Primary-key component carried unchanged from '
                                                            'the public parent; combine it with the '
                                                            "companion's other key columns when joining.",
                                        'indication_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                        'is_illogical': 'Source RTT chronology flag indicating an illogical '
                                                        'activity sequence; retained for the gold drop rule.',
                                        'journey_key': 'Deterministic v2 key carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                        'journey_link_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                        'journey_participant_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                        'labour_delivery_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                        'load_batch_id': 'Deterministic batch token derived from the source '
                                                         "row's bronze load timestamp.",
                                        'loaded_at': 'Latest bronze load timestamp contributing to the '
                                                     "public row; never the lifecycle pipeline's wall-clock "
                                                     'time.',
                                        'location_code': 'Primary-key component carried unchanged from the '
                                                         "public parent; combine it with the companion's "
                                                         'other key columns when joining.',
                                        'location_key': 'Deterministic v2 key carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                        'location_stay_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                        'mc_device_id': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                        'microbiology_isolate_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                        'order_id': 'Native source identifier carried unchanged from the '
                                                    'public parent; joins one lifecycle row to that research '
                                                    'row.',
                                        'organization_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                        'organization_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                        'pacs_examination_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                        'pacs_report_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                        'pathway_oid': 'Native source identifier carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                        'patient_event_key': 'Deterministic SHA-256 event key carried '
                                                             'unchanged from the public parent; joins one '
                                                             'lifecycle row to that event row.',
                                        'patient_event_row_key': 'Deterministic row-level key carried '
                                                                 'unchanged from events_patient_event; joins '
                                                                 'one lifecycle row to one event-index row.',
                                        'period_business_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                        'period_oid': 'Native source identifier carried unchanged from the '
                                                      'public parent; joins one lifecycle row to that '
                                                      'research row.',
                                        'person_address_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                        'person_attribute_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                        'person_death_evidence_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                        'person_id': 'Native source identifier carried unchanged from the '
                                                     'public parent; joins one lifecycle row to that '
                                                     'research row.',
                                        'person_id_resolved': 'True when person_id is null or resolves to '
                                                              'the governed person spine; false records '
                                                              'unresolved linkage without dropping the row.',
                                        'person_identifier_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                        'person_relationship_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                        'pharmacy_issue_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                        'pm_wait_list_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                        'practitioner_group_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                        'practitioner_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                        'practitioner_identifier_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                        'practitioner_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                        'practitioner_location_evidence_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                        'pregnancy_id': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                        'pregnancy_reconciliation_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                        'problem_id': 'Native source identifier carried unchanged from the '
                                                      'public parent; joins one lifecycle row to that '
                                                      'research row.',
                                        'procedure_id': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                        'pt_prot_reg_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                        'ptl_unique_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                        'record_status': 'active, superseded or retracted, normalised from '
                                                         "the source's own status fields; superseded rows "
                                                         'are earlier versions the source kept.',
                                        'record_status_effective_from': 'Timestamp from which the normalised '
                                                                        'record status applies, when the '
                                                                        'source supplies one.',
                                        'record_status_effective_to': 'Timestamp until which the normalised '
                                                                      'record status applies; null means no '
                                                                      'source end is known.',
                                        'referral_oid': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                        'report_version_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                        'request_thread_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                        'research_study_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                        'row_source': 'Physical bronze row source, CURRENT or HIST, carried '
                                                      'unchanged from the public parent; a key component '
                                                      'because the research table is at physical version '
                                                      'grain and patient_event_key is shared across an '
                                                      "entry's versions.",
                                        'rtt_activity_oid': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                        'sch_event_id': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                        'score_type': 'Primary-key component carried unchanged from the '
                                                      "public parent; combine it with the companion's other "
                                                      'key columns when joining.',
                                        'sequence_nbr': 'Primary-key component carried unchanged from the '
                                                        "public parent; combine it with the companion's "
                                                        'other key columns when joining.',
                                        'service_id': 'Native source identifier carried unchanged from the '
                                                      'public parent; joins one lifecycle row to that '
                                                      'research row.',
                                        'service_key': 'Deterministic v2 key carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                        'snapshot_date': 'Primary-key component carried unchanged from the '
                                                         "public parent; combine it with the companion's "
                                                         'other key columns when joining.',
                                        'source_daily_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                        'source_identifier_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                        'source_object': 'Source-arm discriminator carried unchanged from '
                                                         'the public parent; use it with arm-specific native '
                                                         'identifiers when joining.',
                                        'source_site': 'Primary-key component carried unchanged from the '
                                                       "public parent; combine it with the companion's other "
                                                       'key columns when joining.',
                                        'source_system_oid': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                        'source_unit': 'Primary-key component carried unchanged from the '
                                                       "public parent; combine it with the companion's other "
                                                       'key columns when joining.',
                                        'source_update_timestamp': 'Native source-system update timestamp '
                                                                   'carried through the canonical silver '
                                                                   'query.',
                                        'source_version_id': 'Native source version identifier carried '
                                                             'unchanged from the public parent; CURRENT rows '
                                                             'carry -1. A key component alongside '
                                                             'row_source.',
                                        'surg_case_proc_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                        'susceptibility_result_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                        'theatre_attendance_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                        'theatre_case_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                        'theatre_case_milestone_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                        'theatre_implant_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                        'transfusion_event_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                        'transfusion_key': 'Deterministic v2 key carried unchanged from the '
                                                           'public parent; joins one lifecycle row to that '
                                                           'research row.',
                                        'treatment_cycle_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                        'treatment_key': 'Deterministic v2 key carried unchanged from the '
                                                         'public parent; joins one lifecycle row to that '
                                                         'research row.',
                                        'update_count': 'Primary-key component carried unchanged from the '
                                                        "public parent; combine it with the companion's "
                                                        'other key columns when joining.',
                                        'valid_from_datetime': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                        'value_set_key': 'Deterministic v2 key carried unchanged from the '
                                                         'public parent; joins one lifecycle row to that '
                                                         'research row.',
                                        'waiting_list_oid': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                        'waiting_list_snapshot_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.'},
                            'table': 'Internal quality and batch metadata for spine_person; same row grain '
                                     'as the research table. Join keys: person_id.'},
 'spine_person': {'columns': {'active': 'Source active indicator.',
                              'address_selection_status': 'Provenance for current versus latest-known '
                                                          'address selection.',
                              'birth_date': 'Calendar birth date.',
                              'birth_datetime': 'Source-compatible birth timestamp.',
                              'birth_precision_code': 'Source code describing birth-date precision.',
                              'birth_precision_display': 'Display for the source birth precision.',
                              'birth_precision_flag': 'Raw source birth precision flag.',
                              'confidentiality_code': 'Source confidentiality level carried for downstream '
                                                      'access control.',
                              'current_address_id': 'Current source address reference when available.',
                              'current_mrn': 'Current Barts hospital MRN (RNJ 5C4 MRN pool, 6200990) '
                                             'selected from map_patient_identifier: active alias first, then '
                                             'latest valid end-effective, NULLS LAST, deterministic '
                                             'SOURCE_PK tiebreak. MRNs from other pools, including BHRUT RF4 '
                                             'MRNs, are never selected. NULL when more than one distinct '
                                             'active Barts MRN exists (ambiguous) or the person has no Barts '
                                             'MRN.',
                              'current_mrn_pool': 'Alias pool of the selected MRN: always barts (RNJ 5C4 '
                                                  'MRN) when the person has a Barts MRN, including ambiguous '
                                                  'selections; NULL when the person has no Barts MRN.',
                              'current_mrn_status': 'Lifecycle status of the selected hospital MRN alias; '
                                                    'NULL when selection is ambiguous or no alias exists.',
                              'deceased_datetime': 'Source death timestamp.',
                              'deceased_datetime_precision': 'Raw source death-time precision.',
                              'deceased_ind': 'True when a death date is recorded or '
                                              "map_person.deceased_display is 'Deceased' (DECEASED_CD "
                                              '3768549, CDF YES); false otherwise, including DECEASED_CD '
                                              "'No' (684730) and 0. Never NULL.",
                              'deceased_ind_source': 'Evidence behind deceased_ind: death_date (a death '
                                                     'timestamp is recorded) or deceased_code (DECEASED_CD '
                                                     "displays 'Deceased' with no date); NULL when "
                                                     'deceased_ind is false.',
                              'ethnicity_code': 'Source ethnicity code.',
                              'ethnicity_display': 'Source ethnicity display.',
                              'ethnicity_map_candidate_count': 'Number of candidate targets observed; a '
                                                               'mapped parent is unambiguous when this is 1.',
                              'ethnicity_map_candidate_set_id': 'Stable identifier of the retained candidate '
                                                                'set.',
                              'ethnicity_map_competing_count': 'Candidates within 0.05 cosine of the '
                                                               'selected candidate.',
                              'ethnicity_map_component_count': 'Number of source-derived components.',
                              'ethnicity_map_component_index': 'One-based source-derived component index.',
                              'ethnicity_map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                          'measured one.',
                              'ethnicity_map_cosine': 'Measured cosine between the normalised source and '
                                                      'selected target; null when scoring did not run.',
                              'ethnicity_map_method': 'Mapping method: source_native, rule, exact_synonym or '
                                                      'embedding_proposed.',
                              'ethnicity_map_rollup_levels': 'Maximum hierarchy distance to a selected '
                                                             'common ancestor.',
                              'ethnicity_map_rule_id': 'Stable rule or lane identifier that produced the '
                                                       'mapping.',
                              'ethnicity_map_scoring_model': 'Embedding model that produced map_cosine.',
                              'ethnicity_map_status': 'Explicit mapping outcome from the S3b status '
                                                      'vocabulary.',
                              'ethnicity_map_status_reason': 'Reason for the mapping outcome or abstention.',
                              'ethnicity_map_version': 'Version of the source, terminology release or model '
                                                       'used by the mapping.',
                              'ethnicity_omop_concept_id': 'OMOP concept identifier selected with the ethnic '
                                                           'category mapping.',
                              'ethnicity_omop_source_concept_id': 'Non-standard OMOP source concept '
                                                                  'identifier, kept separate from a standard '
                                                                  'target.',
                              'ethnicity_snomed_code': 'SNOMED CT code selected for the ethnic category; '
                                                       'null when no unambiguous mapping is available.',
                              'ethnicity_snomed_display': 'SNOMED CT preferred display selected for the '
                                                          'ethnic category.',
                              'ethnicity_source_code': 'Source code for the ethnic category, retained '
                                                       'without de-identification.',
                              'ethnicity_source_display': 'Source display for the ethnic category.',
                              'ethnicity_source_system': 'Canonical coding-system URI for the source ethnic '
                                                         'category.',
                              'ethnicity_target_code': 'Code of the selected target in its own vocabulary.',
                              'ethnicity_target_display': 'Display of the selected target in its own '
                                                          'vocabulary.',
                              'ethnicity_target_system': 'Coding-system URI of the selected target.',
                              'gender_code': 'Source administrative gender code.',
                              'gender_display': 'Source administrative gender display.',
                              'language_code': 'Preferred language source code.',
                              'language_display': 'Preferred language display.',
                              'latest_known_address_id': 'Latest-known source address reference.',
                              'loaded_at': 'Greatest of map_person.ADC_UPDT and the maximum '
                                           'map_patient_identifier.PIPELINE_UPDT_DT_TM in each matching MRN '
                                           'and NHS alias pool. These are pool-wide clocks, not just the '
                                           'displayed winning alias; missing alias clocks fall back to the '
                                           'person clock. This is contributing ingestion provenance, not the '
                                           'Silver refresh time; null only when all contributing clocks are '
                                           'null.',
                              'marital_status_code': 'Source marital-status code.',
                              'marital_status_display': 'Source marital-status display.',
                              'mrn_selection_status': 'Selection outcome for the Barts MRN: active_alias, '
                                                      'latest_valid_alias, ambiguous (more than one distinct '
                                                      'active Barts MRN) or no_alias (no Barts MRN, '
                                                      'including persons known only by a BHRUT MRN).',
                              'nhs_number': 'Current NHS number selected from map_patient_identifier: active '
                                            'alias first, then latest valid end-effective, NULLS LAST, '
                                            'deterministic SOURCE_PK tiebreak; NULL when selection is '
                                            'ambiguous or no alias exists.',
                              'nhs_number_selection_status': 'Selection outcome for the NHS-number alias: '
                                                             'active_alias, latest_valid_alias, ambiguous '
                                                             '(more than one distinct active NHS number) or '
                                                             'no_alias.',
                              'nhs_number_status': 'Lifecycle status of the selected NHS-number alias; NULL '
                                                   'when selection is ambiguous or no alias exists.',
                              'person_id': 'Millennium person identifier and table primary key.',
                              'record_status': 'Derived person-row status: active only when '
                                               'map_person.active_ind is 1 and end_effective_dt_tm is null '
                                               'or not earlier than 2100-01-01; otherwise superseded, '
                                               'including a missing active_ind. This is a normalized '
                                               'row-status label, not the source numeric ACTIVE_IND.',
                              'record_status_effective_from': 'Source person active-status timestamp carried '
                                                              'from `active_status_dt_tm` without a fallback '
                                                              'timestamp.',
                              'record_status_effective_to': 'Source `end_effective_dt_tm` when earlier than '
                                                            '2100-01-01; otherwise null, including missing '
                                                            'or open-ended sentinel timestamps.',
                              'religion_code': 'Source religion code.',
                              'religion_display': 'Source religion display.',
                              'religion_group': 'Top-level religion group: Christian, Muslim, Hindu, Sikh, '
                                                'Jewish, Buddhist, Other, or Not stated/unknown (ancestor '
                                                "roll-up of religion_snomed_code; the source's 'Other' stays "
                                                'Other); NULL when a named religion matched no SNOMED '
                                                'concept (never imputed). BBR_FOLDIN_V1 BBR-16.',
                              'religion_map_rule_id': 'Rule of the religion mapping (bbr:religion:v1, method '
                                                      'exact_synonym) from '
                                                      '3_lookup.omop.religion_snomed_map; NULL when the code '
                                                      'has no map row. BBR_FOLDIN_V1 BBR-16.',
                              'religion_snomed_code': 'SNOMED concept of the recorded religion (descendant '
                                                      'of 108334009), matched exactly or by synonym on the '
                                                      'Cerner display against RF2 descriptions (TRUD first) '
                                                      'then OMOP synonyms; NULL when no concept matched '
                                                      '(never imputed). BBR_FOLDIN_V1 BBR-16.',
                              'religion_snomed_display': 'SNOMED preferred term of religion_snomed_code. '
                                                         'BBR_FOLDIN_V1 BBR-16.',
                              'source_update_timestamp': 'Greatest of map_person.source_updt_dt_tm and the '
                                                         'maximum map_patient_identifier.SOURCE_ADC_UPDT in '
                                                         'each matching MRN and NHS alias pool. The pool '
                                                         'maxima include all aliases, even when the '
                                                         'displayed identifier is ambiguous; missing alias '
                                                         'clocks fall back to the person clock. This is not '
                                                         'the Silver refresh time; null only when all '
                                                         'contributing clocks are null.',
                              'vip_ind': 'Source VIP indicator carried as data.'},
                  'table': 'One resolved Millennium person. Silver retains inactive, implausible-looking, '
                           'and deceased rows. Current MRN / NHS number selected deterministically from '
                           'map_patient_identifier; multi-active pools publish as ambiguous, never '
                           'tiebroken.'}}
