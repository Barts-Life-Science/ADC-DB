"""Silver clinical.clinical_score: 3 dataset(s).

- datasets: _clinical_qc_clinical_score, clinical_clinical_score, _clinical_clinical_score_metadata
- moved verbatim from: 2_silver_clinical_events:4723, 2_silver_clinical_events:4788, 2_silver_clinical_events:4808, silver_value_policy.py:385, silver_value_policy.py:386

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark
from silver_tables._common.journey import _clinical_score_canonical
from silver_tables.clinical._shared import SCORE_LIFECYCLE_COLUMNS, SCORE_PUBLIC_COLUMNS


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# W2.11 (J25): NEWS2 and MUST bands from the total score.
_NEWS2 = r"(?i)\bnews[\s\S]?2\b|national early warning"


_MUST = r"(?i)\bmust\b|malnutrition universal"


# Clinical /  qc clinical score
@materialized_view(
    name=_n("journey_clinical._qc_clinical_score"),
    refresh_policy="auto",
)
def _qc_clinical_score():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # clinical_score.
    return _cross_qc_primitive(
        _clinical_score_canonical(),
        "clinical_score",
        {
            "score_code": "_qc_score_code_json",
        },
    )


@materialized_view(
    name=_n("journey_clinical.clinical_score"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def clinical_score():
    # Build the declared dataset: Native numeric-event and governed form-promoted clinical score
    # results.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_clinical_score")),
        "clinical_score",
        {
            "score_code": "_qc_score_code_json",
        },
        SCORE_PUBLIC_COLUMNS,
        SCORE_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._clinical_score_metadata'),
    refresh_policy="auto",
)
def clinical_score_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_clinical_score; same row grain as the research table. Join keys:
    # patient_event_key, source_object, doc_response_key, event_id.
    return (spark.read.table(_n("journey_clinical._qc_clinical_score"))).select(*SCORE_LIFECYCLE_COLUMNS)


CROSS_QC = {
    'clinical_score': CrossQc(model='clinical_score', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'score_code': ('score', False)}, promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved']),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_clinical_score': ValuePolicy(table='clinical_clinical_score', enabled=True, zero_as_null=('performer_practitioner_id',), system_user_columns={'performer_practitioner_id': 'performer'}),
}


VALUE_POLICIES["clinical_clinical_score"].register("score_arm", ["source_display"],
         lambda F: F.when(F.coalesce(F.col("source_display"), F.lit("")).rlike(_NEWS2), F.lit("news2"))
                    .when(F.coalesce(F.col("source_display"), F.lit("")).rlike(_MUST), F.lit("must")),
         "news2 or must when the score is one of those instruments' totals; NULL otherwise. INSR_W2 W2.11.")


VALUE_POLICIES["clinical_clinical_score"].register("score_band", ["source_display", "value_number"],
         lambda F: F.when(F.coalesce(F.col("source_display"), F.lit("")).rlike(_NEWS2),
                          # A total outside the instrument's range (NEWS2 0-20) is not a total: no band.
                          F.when(F.col("value_number") > 20, F.lit(None).cast("string"))
                           .when(F.col("value_number") >= 7, "high").when(F.col("value_number") >= 5, "medium")
                           .when(F.col("value_number") >= 0, "low"))
                    .when(F.coalesce(F.col("source_display"), F.lit("")).rlike(_MUST),
                          # MUST totals run 0-6; prod carries 'MUST Score' rows of 23-235235 (W2 review 2026-10-30).
                          F.when(F.col("value_number") > 6, F.lit(None).cast("string"))
                           .when(F.col("value_number") >= 2, "high").when(F.col("value_number") == 1, "medium")
                           .when(F.col("value_number") == 0, "low")),
         "Risk band: NEWS2 total 0-4 low, 5-6 medium, 7-20 high (RCP 2017); MUST 0 low, 1 medium, 2-6 high (BAPEN); "
         "NULL for a value outside the instrument's range. The NEWS2 single-parameter-3 rule needs components and is "
         "not applied. INSR_W2 W2.11.")


VALUE_POLICIES["clinical_clinical_score"].register("band_rule_id", ["source_display"],
         lambda F: F.when(F.coalesce(F.col("source_display"), F.lit("")).rlike(_NEWS2), F.lit("rcp_news2_2017_aggregate"))
                    .when(F.coalesce(F.col("source_display"), F.lit("")).rlike(_MUST), F.lit("bapen_must_2003")),
         "Rule that set score_band. INSR_W2 W2.11.")


WRAP_POLICY = {'clinical_clinical_score'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_clinical_score_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'aria_pt_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'artifact_link_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                   'birth_row_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                   'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
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
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                   'community_care_activity_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                   'community_care_contact_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
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
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
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
                                                   'encounter_identifier_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
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
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                   'entry_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                   'episode_encounter_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                   'episode_encounter_reltn_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                   'episode_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'episode_id_resolved': 'True when episode_id is null or '
                                                                          'resolves to the governed episode '
                                                                          'spine; false records unresolved '
                                                                          'linkage without dropping the row.',
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
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
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
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                   'is_illogical': 'Source RTT chronology flag indicating an '
                                                                   'illogical activity sequence; retained '
                                                                   'for the gold drop rule.',
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
                                                                    "the source row's bronze load timestamp.",
                                                   'loaded_at': 'Latest bronze load timestamp contributing '
                                                                'to the public row; never the lifecycle '
                                                                "pipeline's wall-clock time.",
                                                   'location_code': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
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
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
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
                                                                            'events_patient_event; joins one '
                                                                            'lifecycle row to one '
                                                                            'event-index row.',
                                                   'period_business_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                   'period_oid': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                   'person_id_resolved': 'True when person_id is null or '
                                                                         'resolves to the governed person '
                                                                         'spine; false records unresolved '
                                                                         'linkage without dropping the row.',
                                                   'person_identifier_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
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
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                   'practitioner_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'practitioner_location_evidence_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                   'pregnancy_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                   'problem_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'procedure_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'pt_prot_reg_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                   'ptl_unique_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
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
                                                   'service_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'service_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                                   'snapshot_date': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                   'source_daily_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'source_identifier_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                   'source_object': 'Source-arm discriminator carried '
                                                                    'unchanged from the public parent; use '
                                                                    'it with arm-specific native identifiers '
                                                                    'when joining.',
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
                                                                              'timestamp carried through the '
                                                                              'canonical silver query.',
                                                   'source_version_id': 'Native source version identifier '
                                                                        'carried unchanged from the public '
                                                                        'parent; CURRENT rows carry -1. A '
                                                                        'key component alongside row_source.',
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
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                   'theatre_implant_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                   'transfusion_event_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
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
                                                                          'unchanged from the public parent; '
                                                                          "combine it with the companion's "
                                                                          'other key columns when joining.',
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
                                                'clinical_clinical_score; same row grain as the research '
                                                'table. Join keys: patient_event_key, source_object, '
                                                'doc_response_key, event_id.'},
 '_clinical_qc_clinical_score': {'columns': {},
                                 'table': 'Private JSON bridge and Gold cross-rule flags for '
                                          'clinical_score.'},
 'clinical_clinical_score': {'columns': {'band_rule_id': 'Rule that set score_band. INSR_W2 W2.11.',
                                         'component_count': 'Number of ordered component objects.',
                                         'confidentiality_code': 'Security classification when supplied.',
                                         'doc_response_key': 'PowerForm DOC_RESPONSE_KEY; native key for the '
                                                             'form_promotion arm.',
                                         'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                         'spine_encounter.encounter_id.',
                                         'event_datetime': 'Clinical start date and time of the event as '
                                                           'recorded on the Millennium CLINICAL_EVENT row.',
                                         'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                                 'time (GMT/BST). Millennium rows take the '
                                                                 'bronze *_LOCAL companions of the same '
                                                                 'source columns and fallback order; rows '
                                                                 'from local-clock sources (LUNA, '
                                                                 'PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                 'BadgerNet, EndoBase, SLAM, iWeb, Datix and '
                                                                 'similar) carry event_datetime unchanged. '
                                                                 'event_datetime is a UTC instant for '
                                                                 'Millennium rows, so take local calendar '
                                                                 'dates and clock times from this column.',
                                         'event_end_datetime': 'Score end timestamp.',
                                         'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                     'wall-clock time (GMT/BST), by the same '
                                                                     'rule as event_datetime_local; null '
                                                                     'where event_end_datetime is null.',
                                         'event_id': 'Millennium numeric EVENT_ID; native key for the '
                                                     'numeric_event arm.',
                                         'interpretation_code': 'Source interpretation code.',
                                         'interpretation_display': 'Source interpretation display.',
                                         'loaded_at': 'map_numeric_events.ADC_UPDT for numeric-event rows, '
                                                      'or map_powerform_assessment_item.ADC_UPDT for '
                                                      "form-promoted rows. Each arm's bronze load clock is "
                                                      'carried unchanged through its staging, union and '
                                                      'quality wrappers. No form-parent or '
                                                      'promotion-configuration clock is added; missing '
                                                      'values remain null and this is not Silver refresh '
                                                      'time.',
                                         'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                              'cross-feed join key.',
                                         'performer_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                      'the performer.',
                                         'performer_system_ind': 'True when the source '
                                                                 'performer_practitioner_id was 1, the '
                                                                 'Cerner system user (J5); the id is then '
                                                                 'published as NULL. NULL when the source id '
                                                                 'was NULL. INSR_W2.',
                                         'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                         'promotion_rule_id': 'Governed promotion-rule identifier.',
                                         'record_status': 'Arm-specific row status. Numeric events are '
                                                          'retracted when '
                                                          'map_numeric_events.SOURCE_DELETED_IND is true '
                                                          '(null defaults false), otherwise superseded when '
                                                          'CLINICAL_EVENT_VALID_UNTIL_DT_TM is before '
                                                          '2100-01-01, otherwise active. Form-promoted rows '
                                                          'are retracted when CANONICAL_SOURCE_DELETED_IND '
                                                          'is true or SOURCE_PRESENT_IND is false or '
                                                          'missing; otherwise superseded when ACTIVE_IND is '
                                                          'zero or missing, otherwise active. Retraction has '
                                                          'priority over supersession; this is not the '
                                                          'clinical result-status code.',
                                         'record_status_effective_from': 'map_numeric_events.CLINICAL_EVENT_VALID_FROM_DT_TM '
                                                                         'on the numeric-event arm; '
                                                                         'map_powerform_assessment_item.FIRST_DOCUMENTED_DT_TM '
                                                                         'on the form-promotion arm. Each is '
                                                                         'carried unchanged, with no '
                                                                         'performed/response/ingestion-time '
                                                                         'fallback; a missing value remains '
                                                                         'null.',
                                         'record_status_effective_to': 'Numeric events return '
                                                                       'CLINICAL_EVENT_VALID_UNTIL_DT_TM '
                                                                       'when deleted or ended, otherwise '
                                                                       'null; deletion can therefore retain '
                                                                       'a far-future end value. '
                                                                       'Form-promoted rows return '
                                                                       'LAST_DOCUMENTED_DT_TM when deleted, '
                                                                       'absent or inactive, otherwise null. '
                                                                       'No missing-end fallback is applied, '
                                                                       'and the form documentation end is '
                                                                       'not an independently observed '
                                                                       'status-transition time.',
                                         'result_status_code': 'Source result status code.',
                                         'result_status_display': 'Source result status display.',
                                         'score_arm': 'news2 or must when the score is one of those '
                                                      "instruments' totals; NULL otherwise. INSR_W2 W2.11.",
                                         'score_band': 'Risk band: NEWS2 total 0-4 low, 5-6 medium, 7-20 '
                                                       'high (RCP 2017); MUST 0 low, 1 medium, 2-6 high '
                                                       "(BAPEN); NULL for a value outside the instrument's "
                                                       'range. The NEWS2 single-parameter-3 rule needs '
                                                       'components and is not applied. INSR_W2 W2.11.',
                                         'score_map_candidate_count': 'Number of candidate targets observed; '
                                                                      'a mapped parent is unambiguous when '
                                                                      'this is 1.',
                                         'score_map_candidate_set_id': 'Stable identifier of the retained '
                                                                       'candidate set.',
                                         'score_map_competing_count': 'Candidates within 0.05 cosine of the '
                                                                      'selected candidate.',
                                         'score_map_component_count': 'Number of source-derived components.',
                                         'score_map_component_index': 'One-based source-derived component '
                                                                      'index.',
                                         'score_map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                                 'measured one.',
                                         'score_map_cosine': 'Measured cosine between the normalised source '
                                                             'and selected target; null when scoring did not '
                                                             'run.',
                                         'score_map_method': 'Mapping method: source_native, rule, '
                                                             'exact_synonym or embedding_proposed.',
                                         'score_map_rollup_levels': 'Maximum hierarchy distance to a '
                                                                    'selected common ancestor.',
                                         'score_map_rule_id': 'Stable rule or lane identifier that produced '
                                                              'the mapping.',
                                         'score_map_scoring_model': 'Embedding model that produced '
                                                                    'map_cosine.',
                                         'score_map_status': 'Explicit mapping outcome from the S3b status '
                                                             'vocabulary.',
                                         'score_map_status_reason': 'Reason for the mapping outcome or '
                                                                    'abstention.',
                                         'score_map_version': 'Version of the source, terminology release or '
                                                              'model used by the mapping.',
                                         'score_name': 'Human-readable score or scale name.',
                                         'score_omop_concept_id': 'OMOP concept identifier selected with the '
                                                                  'clinical score mapping.',
                                         'score_omop_source_concept_id': 'Non-standard OMOP source concept '
                                                                         'identifier, kept separate from a '
                                                                         'standard target.',
                                         'score_snomed_code': 'SNOMED CT code selected for the clinical '
                                                              'score; null when no unambiguous mapping is '
                                                              'available.',
                                         'score_snomed_display': 'SNOMED CT preferred display selected for '
                                                                 'the clinical score.',
                                         'score_source_code': 'Source code for the clinical score, retained '
                                                              'without de-identification.',
                                         'score_source_display': 'Source display for the clinical score.',
                                         'score_source_system': 'Canonical coding-system URI for the source '
                                                                'clinical score.',
                                         'score_target_code': 'Code of the selected target in its own '
                                                              'vocabulary.',
                                         'score_target_display': 'Display of the selected target in its own '
                                                                 'vocabulary.',
                                         'score_target_system': 'Coding-system URI of the selected target.',
                                         'source_code': 'Source score code.',
                                         'source_coding_system': 'Coding system or source namespace in which '
                                                                 'the source code is defined for each '
                                                                 'clinical score record. It is produced by '
                                                                 'the silver transformation and has no '
                                                                 'direct bronze-column lineage entry. Source '
                                                                 'code meanings and sentinel values are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'maps them; null means no code was '
                                                                 'supplied.',
                                         'source_dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID '
                                                                         'of the source form for promoted '
                                                                         'rows.',
                                         'source_display': 'Source score display.',
                                         'source_feed': 'Registered source route.',
                                         'source_form_key': 'Deterministic SHA-256 key of the source '
                                                            'PowerForm instance for promoted rows.',
                                         'source_object': 'Native source arm: numeric_event or '
                                                          'form_promotion.',
                                         'source_update_timestamp': 'For numeric events, greatest of '
                                                                    'map_numeric_events.STRING_RESULT_UPDT_DT_TM, '
                                                                    'CLINICAL_EVENT_UPDT_DT_TM and ADC_UPDT. '
                                                                    'For form-promoted rows, '
                                                                    'map_powerform_assessment_item.ADC_UPDT '
                                                                    'directly. This mixes source and '
                                                                    'ingestion provenance rather than '
                                                                    'uniformly representing a native '
                                                                    'modification clock; the promotion '
                                                                    'lookup adds no clock and Silver refresh '
                                                                    'time is not substituted.',
                                         'subject_id_system': 'Identifier system used for subject_key.',
                                         'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                        'identifier. Not salted, not secret; a stable join '
                                                        'key across feeds where person_id is unresolved.',
                                         'unit_concept_id': 'Mapped unit concept identifier.',
                                         'unit_source_value': 'Source unit when supplied.',
                                         'value_number': 'Numeric score value.',
                                         'value_text': 'Verbatim score text.',
                                         'vip_ind': 'VIP indicator when supplied.',
                                         'withheld_identity_ind': 'Withheld identity indicator.'},
                             'table': 'Native numeric-event and governed form-promoted clinical score '
                                      'results.'}}

# INSR_R4_SILVER_V1 (parent): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import ParentInputs, parent_inputs_spec  # noqa: E402
R4_PARENT_INPUTS = ParentInputs(parent='clinical_clinical_score', dataset='_clinical_clinical_score_parent_inputs', keys=('patient_event_key',), inputs=('identity_status', 'person_id'), admit_sql="coalesce(((identity_status = 'resolved')), false)", name=_n('journey_clinical._clinical_score_parent_inputs'), parent_name=_n('journey_clinical.clinical_score'))
SPECS = list(SPECS) + [parent_inputs_spec(R4_PARENT_INPUTS)]
