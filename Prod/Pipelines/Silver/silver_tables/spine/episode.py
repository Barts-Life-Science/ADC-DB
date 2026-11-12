"""Silver spine.episode: 2 dataset(s).

- datasets: spine_episode, _spine_episode_metadata
- moved verbatim from: 1_silver_spine_and_reference:764, 1_silver_spine_and_reference:798, 1_silver_spine_and_reference:802, 1_silver_spine_and_reference:806, 1_silver_spine_and_reference:813, 1_silver_spine_and_reference:866, 1_silver_spine_and_reference:879

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


# Spine / episode

# ==== Episode spine containers — source-backed only, never event-indexed ====
SRC_EPISODE = "4_prod.bronze.map_episode"


EPISODE_LIFECYCLE_FIELDS = [
    'load_batch_id',
]


EPISODE_RETIRED_COLUMNS = [

]


EPISODE_LIFECYCLE_COLUMNS = [
    'episode_key',
    'episode_id',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_episode():
    # Assemble episode rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    s = read_source(SRC_EPISODE)
    skey, _ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_EPISODE, s.EPISODE_ID
    )
    superseded = (~F.coalesce(s.SOURCE_PRESENT_IND.cast("boolean"), F.lit(True))) | (
        F.coalesce(s.ACTIVE_IND, F.lit(1)) != 1
    )
    # contract v2: retain the episode and direct-encounter SHA values as keys and publish their Millennium ids
    return s.select(
        stable_id("episode:mill", s.EPISODE_ID).alias("episode_key"),
        s.EPISODE_ID.cast("bigint").alias("episode_id"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        skey.alias("subject_key"),
        s.DISPLAY.alias("episode_display"),
        s.EPISODE_TYPE_CD.cast("string").alias("episode_type_code"),
        s.EPISODE_TYPE_DESC.alias("episode_type_display"),
        s.EPISODE_STATUS_CD.cast("string").alias("status_code"),
        s.EPISODE_STATUS_DESC.alias("status_display"),
        s.EPISODE_START_DT_TM_CLEAN.alias("period_start"),
        s.EPISODE_STOP_DT_TM_CLEAN.alias("period_end"),
        s.EPISODE_BREACH_DT_TM_CLEAN.alias("breach_datetime"),
        s.EPISODE_PAUSE_DAYS_CNT.cast("long").alias("pause_days"),
        s.EPISODE_CLOSE_REASON_CD.cast("string").alias("close_reason_code"),
        s.EPISODE_CLOSE_REASON_DESC.alias("close_reason_display"),
        s.SERVICE_CATEGORY_CD.cast("string").alias("service_category_code"),
        s.SERVICE_CATEGORY_DESC.alias("service_category_display"),
        s.REFER_FACILITY_CD.cast("string").alias("referring_facility_code"),
        s.REFER_FACILITY_DESC.alias("referring_facility_display"),
        s.CONTRIBUTOR_SYSTEM_CD.cast("string").alias("contributor_system_code"),
        s.CONTRIBUTOR_SYSTEM_DESC.alias("contributor_system_display"),
        F.when(s.ENCNTR_ID.isNotNull(),
               stable_id("encounter:mill", s.ENCNTR_ID)).alias("direct_encounter_key"),
        s.ENCNTR_ID.cast("bigint").alias("direct_encounter_id"),
        F.when(superseded, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        s.ACTIVE_STATUS_DT_TM.alias("record_status_effective_from"),
        F.when(
            superseded,
            F.coalesce(
                F.when(s.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"),
                       s.END_EFFECTIVE_DT_TM),
                s.ACTIVE_STATUS_DT_TM,
                s.ADC_UPDT,
            ),
        ).alias("record_status_effective_to"),
        F.lit(SRC_EPISODE).alias("source_table"),
        s.EPISODE_ID.cast("string").alias("source_row_id"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("loaded_at"),
    )


@materialized_view(
    name=_n("journey_spine.episode"),
    cluster_by=["person_id", "period_start"],
    refresh_policy="auto",
)
def episode():
    # Build the declared dataset: One Millennium episode container (map_episode). Source-backed
    # only; the direct encounter reference is agreement-gated evidence, never containment.
    return _lifecycle_source_episode().drop(*EPISODE_LIFECYCLE_FIELDS, *EPISODE_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_spine._episode_metadata'),
    refresh_policy="auto",
)
def episode_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_episode; same
    # row grain as the research table. Join keys: episode_key, episode_id.
    return (_lifecycle_source_episode()).select(*EPISODE_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_spine_episode_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from the '
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
                                         'birth_row_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
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
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                         'dcp_forms_activity_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                         'device_mapping_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                         'diagnosis_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'dicom_path': 'Primary-key component carried unchanged from the '
                                                       "public parent; combine it with the companion's other "
                                                       'key columns when joining.',
                                         'doc_response_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                         'elective_access_procedure_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                         'encounter_attribute_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                         'encounter_bounds_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                         'encounter_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'encounter_id_resolved': 'True when encounter_id is null or '
                                                                  'resolves to the governed encounter spine; '
                                                                  'false records unresolved linkage without '
                                                                  'dropping the row.',
                                         'encounter_identifier_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                         'encounter_key': 'Deterministic v2 key carried unchanged from the '
                                                          'public parent; joins one lifecycle row to that '
                                                          'research row.',
                                         'endobase_exam_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                         'endobase_exam_term_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                         'entity_id': 'Native source identifier carried unchanged from the '
                                                      'public parent; joins one lifecycle row to that '
                                                      'research row.',
                                         'entry_id': 'Native source identifier carried unchanged from the '
                                                     'public parent; joins one lifecycle row to that '
                                                     'research row.',
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
                                                                'unresolved linkage without dropping the '
                                                                'row.',
                                         'episode_key': 'Deterministic v2 key carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                         'event_after_death_30d': 'True when the event timestamp is more '
                                                                  "than 30 days after the resolved person's "
                                                                  'death timestamp; silver records the doubt '
                                                                  'and does not filter the row.',
                                         'event_before_birth': 'True when the event timestamp precedes the '
                                                               "resolved person's birth timestamp; silver "
                                                               'records the doubt and does not filter the '
                                                               'row.',
                                         'event_id': 'Native source identifier carried unchanged from the '
                                                     'public parent; joins one lifecycle row to that '
                                                     'research row.',
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
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                         'identity_status': 'Subject identity resolution state recorded by '
                                                            'the silver canonical query.',
                                         'implant_event_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                         'implant_sequence': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                         'indication_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                         'is_illogical': 'Source RTT chronology flag indicating an illogical '
                                                         'activity sequence; retained for the gold drop '
                                                         'rule.',
                                         'journey_key': 'Deterministic v2 key carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                         'journey_link_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
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
                                         'mc_device_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'microbiology_isolate_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                         'order_id': 'Native source identifier carried unchanged from the '
                                                     'public parent; joins one lifecycle row to that '
                                                     'research row.',
                                         'organization_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                         'organization_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
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
                                                                  'unchanged from events_patient_event; '
                                                                  'joins one lifecycle row to one '
                                                                  'event-index row.',
                                         'period_business_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                         'period_oid': 'Native source identifier carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                         'person_address_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                         'person_attribute_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                         'person_death_evidence_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
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
                                         'practitioner_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                         'practitioner_location_evidence_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                         'pregnancy_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'pregnancy_reconciliation_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                         'problem_id': 'Native source identifier carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                         'procedure_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'pt_prot_reg_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                         'ptl_unique_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                         'record_status': 'active, superseded or retracted, normalised from '
                                                          "the source's own status fields; superseded rows "
                                                          'are earlier versions the source kept.',
                                         'record_status_effective_from': 'Timestamp from which the '
                                                                         'normalised record status applies, '
                                                                         'when the source supplies one.',
                                         'record_status_effective_to': 'Timestamp until which the normalised '
                                                                       'record status applies; null means no '
                                                                       'source end is known.',
                                         'referral_oid': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'report_version_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                         'request_thread_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                         'research_study_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                         'row_source': 'Physical bronze row source, CURRENT or HIST, carried '
                                                       'unchanged from the public parent; a key component '
                                                       'because the research table is at physical version '
                                                       'grain and patient_event_key is shared across an '
                                                       "entry's versions.",
                                         'rtt_activity_oid': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                         'sch_event_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
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
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                         'source_object': 'Source-arm discriminator carried unchanged from '
                                                          'the public parent; use it with arm-specific '
                                                          'native identifiers when joining.',
                                         'source_site': 'Primary-key component carried unchanged from the '
                                                        "public parent; combine it with the companion's "
                                                        'other key columns when joining.',
                                         'source_system_oid': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                         'source_unit': 'Primary-key component carried unchanged from the '
                                                        "public parent; combine it with the companion's "
                                                        'other key columns when joining.',
                                         'source_update_timestamp': 'Native source-system update timestamp '
                                                                    'carried through the canonical silver '
                                                                    'query.',
                                         'source_version_id': 'Native source version identifier carried '
                                                              'unchanged from the public parent; CURRENT '
                                                              'rows carry -1. A key component alongside '
                                                              'row_source.',
                                         'surg_case_proc_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                         'susceptibility_result_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                         'theatre_attendance_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                         'theatre_case_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
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
                                         'valid_from_datetime': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                         'value_set_key': 'Deterministic v2 key carried unchanged from the '
                                                          'public parent; joins one lifecycle row to that '
                                                          'research row.',
                                         'waiting_list_oid': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                         'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.'},
                             'table': 'Internal quality and batch metadata for spine_episode; same row grain '
                                      'as the research table. Join keys: episode_key, episode_id.'},
 'spine_episode': {'columns': {'breach_datetime': 'Sentinel-cleaned episode breach timestamp when supplied.',
                               'close_reason_code': 'Source close-reason code.',
                               'close_reason_display': 'Source close-reason display.',
                               'contributor_system_code': 'Source contributor-system code.',
                               'contributor_system_display': 'Source contributor-system display.',
                               'direct_encounter_id': 'Millennium ENCNTR_ID for the agreement-gated direct '
                                                      'reference; evidence only.',
                               'direct_encounter_key': 'Deterministic encounter key for the agreement-gated '
                                                       'direct reference; evidence only.',
                               'episode_display': 'Source episode display name.',
                               'episode_id': 'Millennium EPISODE_ID; primary key of this table.',
                               'episode_key': 'Deterministic SHA-256 key minted from Millennium EPISODE_ID.',
                               'episode_type_code': 'Source episode type code.',
                               'episode_type_display': 'Source episode type display.',
                               'loaded_at': 'map_episode.ADC_UPDT carried unchanged from the contributing '
                                            'bronze episode row. This is ingestion provenance, not the '
                                            'Silver refresh time or the clinical episode start/end; a '
                                            'missing source timestamp remains null.',
                               'pause_days': 'Source pause-day count.',
                               'period_end': 'Sentinel-cleaned episode end timestamp when supplied.',
                               'period_start': 'Sentinel-cleaned episode begin timestamp (bronze DQ '
                                               'triplet).',
                               'person_id': 'Millennium PERSON_ID as BIGINT.',
                               'record_status': 'Derived map_episode row status: superseded when '
                                                'SOURCE_PRESENT_IND is false or ACTIVE_IND is not 1; '
                                                'otherwise active. A null SOURCE_PRESENT_IND defaults to '
                                                'present and a null ACTIVE_IND defaults to 1. Source absence '
                                                'maps to superseded here, not retracted; this is not the '
                                                'clinical episode status code.',
                               'record_status_effective_from': 'map_episode.ACTIVE_STATUS_DT_TM carried '
                                                               'unchanged as the source active-status '
                                                               'timestamp. No episode-start or '
                                                               'ingestion-clock fallback is applied; a '
                                                               'missing source value remains null.',
                               'record_status_effective_to': 'For a superseded episode, the first available '
                                                             'of map_episode.END_EFFECTIVE_DT_TM earlier '
                                                             'than 2100-01-01, ACTIVE_STATUS_DT_TM, and '
                                                             'ADC_UPDT. Null for an active episode or when '
                                                             'every fallback is unavailable. This is a '
                                                             'derived end boundary and may use ingestion '
                                                             'time; it is not a Boolean active indicator.',
                               'referring_facility_code': 'Source referring-facility code.',
                               'referring_facility_display': 'Source referring-facility display.',
                               'service_category_code': 'Source service-category code.',
                               'service_category_display': 'Source service-category display.',
                               'source_row_id': 'Native source row identifier.',
                               'source_table': 'Registered bronze source table.',
                               'status_code': 'Source episode status code.',
                               'status_display': 'Source episode status display.',
                               'subject_key': 'Deterministic SHA-256 over the strongest source identifier. '
                                              'Not salted, not secret; a stable join key across feeds where '
                                              'person_id is unresolved.'},
                   'table': 'One Millennium episode container (map_episode). Source-backed only; the direct '
                            'encounter reference is agreement-gated evidence, never containment.'}}
