"""Silver spine.encounter_identifier: 2 dataset(s).

- datasets: spine_encounter_identifier, _spine_encounter_identifier_metadata
- moved verbatim from: 1_silver_spine_and_reference:1027, 1_silver_spine_and_reference:1041, 1_silver_spine_and_reference:959, 1_silver_spine_and_reference:978, 1_silver_spine_and_reference:982, 1_silver_spine_and_reference:986, 1_silver_spine_and_reference:993

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Spine / encounter identifier
SRC_ENCOUNTER_IDENTIFIER = "4_prod.bronze.map_encounter_identifier"


ENCOUNTER_IDENTIFIER_LIFECYCLE_FIELDS = [
    'load_batch_id',
]


ENCOUNTER_IDENTIFIER_RETIRED_COLUMNS = [

]


ENCOUNTER_IDENTIFIER_LIFECYCLE_COLUMNS = [
    'encounter_identifier_key',
    'source_identifier_id',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_encounter_identifier():
    # 22 live rows carry a NULL ALIAS_VALUE (8 of them current). An identifier row with
    # no value says nothing, so those rows are excluded and the row-count check compares
    # against the non-null source count.
    # Assemble encounter identifier rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    a = read_source(SRC_ENCOUNTER_IDENTIFIER).where(F.col("ALIAS_VALUE").isNotNull())
    # contract v2: rename the assignment hash and publish native SOURCE_PK and ENCNTR_ID values
    return a.select(
        stable_id("encounter_identifier:mill_alias", a.SOURCE_PK).alias("encounter_identifier_key"),
        a.SOURCE_PK.cast("string").alias("source_identifier_id"),
        a.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        # Type-scoped system: 23.17M ALIAS_VALUEs live under MULTIPLE alias types — a single
        # system string would collide (system, value) across types.
        F.concat(
            F.lit("urn:cerner:encntr_alias:"),
            F.lower(F.regexp_replace(F.coalesce(a.ALIAS_TYPE, F.lit("unknown")),
                                     "[^A-Za-z0-9]+", "_")),
        ).alias("identifier_system"),
        a.ALIAS_TYPE.alias("identifier_type_code"),
        a.ALIAS_VALUE.alias("identifier_value"),
        F.when(F.coalesce(a.CURRENT_IND, F.lit(False)), F.lit("active"))
         .otherwise(F.lit("inactive")).alias("status"),
        a.CURRENT_IND.alias("current_ind"),
        a.MULTI_ACTIVE_IND.alias("multi_active_ind"),
        a.BEG_EFFECTIVE_DT_TM.alias("valid_from"),
        F.when(a.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"),
               a.END_EFFECTIVE_DT_TM).alias("valid_to"),
        F.lit(SRC_ENCOUNTER_IDENTIFIER).alias("source_table"),
        a.SOURCE_PK.cast("string").alias("source_row_id"),
        F.date_format(a.PIPELINE_UPDT_DT_TM, "yyyyMMddHHmmss").alias("load_batch_id"),
        a.PIPELINE_UPDT_DT_TM.alias("loaded_at"),
    )


@materialized_view(
    name=_n("journey_spine.encounter_identifier"),
    refresh_policy="auto",
)
def encounter_identifier():
    # Build the declared dataset: One source encounter identifier assignment
    # (map_encounter_identifier); reference surface parallel to person_identifier. Feed family
    # has no ADC_UPDT: loaded_at rides the bronze pipeline write timestamp.
    return _lifecycle_source_encounter_identifier().drop(*ENCOUNTER_IDENTIFIER_LIFECYCLE_FIELDS, *ENCOUNTER_IDENTIFIER_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_spine._encounter_identifier_metadata'),
    refresh_policy="auto",
)
def encounter_identifier_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # spine_encounter_identifier; same row grain as the research table. Join keys:
    # encounter_identifier_key, source_identifier_id.
    return (_lifecycle_source_encounter_identifier()).select(*ENCOUNTER_IDENTIFIER_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_spine_encounter_identifier_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                      'activity_record_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                      'admission_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                      'allergy_instance_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                      'aria_dx_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'aria_pt_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'artifact_link_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'birth_row_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                      'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                      'care_contact_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'care_participation_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
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
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                      'community_care_contact_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                      'concept_map_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'cost_line_item_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                      'dcp_forms_activity_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                      'device_mapping_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                      'diagnosis_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'dicom_path': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                      'doc_response_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'elective_access_procedure_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                      'encounter_attribute_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                      'encounter_bounds_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                      'encounter_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'encounter_id_resolved': 'True when encounter_id is '
                                                                               'null or resolves to the '
                                                                               'governed encounter spine; '
                                                                               'false records unresolved '
                                                                               'linkage without dropping the '
                                                                               'row.',
                                                      'encounter_identifier_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                      'encounter_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                      'endobase_exam_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'endobase_exam_term_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                      'entity_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                      'entry_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                      'episode_encounter_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                      'episode_encounter_reltn_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
                                                                                    'one lifecycle row to '
                                                                                    'that research row.',
                                                      'episode_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'episode_id_resolved': 'True when episode_id is null '
                                                                             'or resolves to the governed '
                                                                             'episode spine; false records '
                                                                             'unresolved linkage without '
                                                                             'dropping the row.',
                                                      'episode_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                      'event_after_death_30d': 'True when the event '
                                                                               'timestamp is more than 30 '
                                                                               'days after the resolved '
                                                                               "person's death timestamp; "
                                                                               'silver records the doubt and '
                                                                               'does not filter the row.',
                                                      'event_before_birth': 'True when the event timestamp '
                                                                            "precedes the resolved person's "
                                                                            'birth timestamp; silver records '
                                                                            'the doubt and does not filter '
                                                                            'the row.',
                                                      'event_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
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
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                      'genetic_result_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'genetic_test_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'homecare_request_item_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                      'journey_participant_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                      'labour_delivery_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                      'load_batch_id': 'Deterministic batch token derived '
                                                                       "from the source row's bronze load "
                                                                       'timestamp.',
                                                      'loaded_at': 'Latest bronze load timestamp '
                                                                   'contributing to the public row; never '
                                                                   "the lifecycle pipeline's wall-clock "
                                                                   'time.',
                                                      'location_code': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                      'location_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'location_stay_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'mc_device_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'microbiology_isolate_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                      'order_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                      'organization_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'organization_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'pacs_examination_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                      'pacs_report_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                      'pathway_oid': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                      'patient_event_key': 'Deterministic SHA-256 event key '
                                                                           'carried unchanged from the '
                                                                           'public parent; joins one '
                                                                           'lifecycle row to that event row.',
                                                      'patient_event_row_key': 'Deterministic row-level key '
                                                                               'carried unchanged from '
                                                                               'events_patient_event; joins '
                                                                               'one lifecycle row to one '
                                                                               'event-index row.',
                                                      'period_business_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                      'period_oid': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'person_address_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                      'person_attribute_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                      'person_death_evidence_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                      'person_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                      'person_id_resolved': 'True when person_id is null or '
                                                                            'resolves to the governed person '
                                                                            'spine; false records unresolved '
                                                                            'linkage without dropping the '
                                                                            'row.',
                                                      'person_identifier_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                      'person_relationship_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                      'pharmacy_issue_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'pm_wait_list_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'practitioner_group_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                      'practitioner_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'practitioner_identifier_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                      'practitioner_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'practitioner_location_evidence_key': 'Deterministic '
                                                                                            'v2 key carried '
                                                                                            'unchanged from '
                                                                                            'the public '
                                                                                            'parent; joins '
                                                                                            'one lifecycle '
                                                                                            'row to that '
                                                                                            'research row.',
                                                      'pregnancy_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                      'problem_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'procedure_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
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
                                                      'record_status_effective_to': 'Timestamp until which '
                                                                                    'the normalised record '
                                                                                    'status applies; null '
                                                                                    'means no source end is '
                                                                                    'known.',
                                                      'referral_oid': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'report_version_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'request_thread_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                      'research_study_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
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
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'score_type': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                      'sequence_nbr': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                      'service_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'service_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                      'snapshot_date': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                      'source_daily_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'source_identifier_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                      'source_object': 'Source-arm discriminator carried '
                                                                       'unchanged from the public parent; '
                                                                       'use it with arm-specific native '
                                                                       'identifiers when joining.',
                                                      'source_site': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                      'source_system_oid': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'source_unit': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                      'source_update_timestamp': 'Native source-system '
                                                                                 'update timestamp carried '
                                                                                 'through the canonical '
                                                                                 'silver query.',
                                                      'source_version_id': 'Native source version identifier '
                                                                           'carried unchanged from the '
                                                                           'public parent; CURRENT rows '
                                                                           'carry -1. A key component '
                                                                           'alongside row_source.',
                                                      'surg_case_proc_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'susceptibility_result_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                      'theatre_attendance_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                      'theatre_case_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                      'theatre_implant_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                      'transfusion_event_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                      'transfusion_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'treatment_cycle_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                      'treatment_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                      'update_count': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                      'valid_from_datetime': 'Primary-key component carried '
                                                                             'unchanged from the public '
                                                                             'parent; combine it with the '
                                                                             "companion's other key columns "
                                                                             'when joining.',
                                                      'value_set_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                      'waiting_list_oid': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'waiting_list_snapshot_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.'},
                                          'table': 'Internal quality and batch metadata for '
                                                   'spine_encounter_identifier; same row grain as the '
                                                   'research table. Join keys: encounter_identifier_key, '
                                                   'source_identifier_id.'},
 'spine_encounter_identifier': {'columns': {'current_ind': 'Bronze current-alias indicator.',
                                            'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                            'spine_encounter.encounter_id.',
                                            'encounter_identifier_key': 'Deterministic SHA-256 key minted '
                                                                        'from the source alias row.',
                                            'identifier_system': 'Type-scoped identifier system URI '
                                                                 '(urn:cerner:encntr_alias:<normalized alias '
                                                                 'type>) — 23.17M values live under multiple '
                                                                 'alias types',
                                            'identifier_type_code': 'Source alias type verbatim.',
                                            'identifier_value': 'Identifier value; published and IG-governed '
                                                                'at serve time',
                                            'loaded_at': 'map_encounter_identifier.PIPELINE_UPDT_DT_TM '
                                                         'carried unchanged from the contributing bronze '
                                                         'alias row. This records bronze processing '
                                                         'provenance, not identifier validity or the Silver '
                                                         'refresh time; a missing source timestamp remains '
                                                         'null.',
                                            'multi_active_ind': 'Bronze multiple-active-aliases indicator '
                                                                'for this encounter and type.',
                                            'source_identifier_id': 'Native map_encounter_identifier '
                                                                    'SOURCE_PK rendered as text.',
                                            'source_row_id': 'Native source row identifier.',
                                            'source_table': 'Registered bronze source table.',
                                            'status': 'Alias lifecycle status.',
                                            'valid_from': 'Alias effective-from timestamp.',
                                            'valid_to': 'Alias effective-to timestamp when supplied.'},
                                'table': 'One source encounter identifier assignment '
                                         '(map_encounter_identifier); reference surface parallel to '
                                         'person_identifier. Feed family has no ADC_UPDT: loaded_at rides '
                                         'the bronze pipeline write timestamp.'}}
