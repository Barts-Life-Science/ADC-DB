"""Silver spine.person_relationship: 2 dataset(s).

- datasets: spine_person_relationship, _spine_person_relationship_metadata
- moved verbatim from: 4_silver_care_pathways:145, 4_silver_care_pathways:149, 4_silver_care_pathways:153, 4_silver_care_pathways:160, 4_silver_care_pathways:165, 4_silver_care_pathways:176, 4_silver_care_pathways:61, 4_silver_care_pathways:83

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id, subject_key_with_system
from silver_tables._common.care_pathways import SRC_MAT_BIRTH


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


PERSON_RELATIONSHIP_PUBLIC_COLUMNS = [
    "person_relationship_key",
    "birth_row_id",
    "source_subject_key",
    "source_person_id",
    "target_subject_key",
    "target_person_id",
    "relationship_type_code",
    "inverse_relationship_type_code",
    "valid_from",
    "valid_to",
    "record_status",
    "construction_rule",
    "construction_version",
    "source_system",
    "source_table",
    "source_row_id",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]


def _person_relationship_canonical():
    # Assemble normalized person relationship rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    b = read_source(SRC_MAT_BIRTH)
    source_row_id = b.BirthRow_ID.cast("string")
    mother_key, _ = subject_key_with_system(
        [("urn:cerner:person_id", b.MotherPerson_ID)], SRC_MAT_BIRTH, source_row_id
    )
    baby_key, _ = subject_key_with_system(
        [
            ("urn:cerner:person_id", b.BabyPerson_ID),
            ("https://fhir.nhs.uk/Id/nhs-number", b.Baby_NHS),
            ("urn:barts:mrn", b.Baby_MRN),
        ],
        SRC_MAT_BIRTH,
        source_row_id,
    )
    deleted = F.coalesce(b.PregnancySource_DELETE_IND.cast("long"), F.lit(0)) != 0
    # contract v2: retain the relationship SHA as person_relationship_key and publish native BirthRow_ID
    return b.select(
        stable_id("person_relationship:mother_to_child", b.BirthRow_ID)
        .alias("person_relationship_key"),
        b.BirthRow_ID.cast("string").alias("birth_row_id"),
        mother_key.alias("source_subject_key"),
        b.MotherPerson_ID.cast("string").alias("source_person_id"),
        baby_key.alias("target_subject_key"),
        b.BabyPerson_ID.cast("string").alias("target_person_id"),
        F.lit("mother_to_child").alias("relationship_type_code"),
        F.lit("child_of_mother").alias("inverse_relationship_type_code"),
        b.BirthDateTime.alias("valid_from"), F.lit(None).cast("timestamp").alias("valid_to"),
        F.when(deleted, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        F.lit("mat-birth-mother-baby").alias("construction_rule"),
        F.lit("pregnancy-v1").alias("construction_version"),
        F.lit("millennium-maternity").alias("source_system"),
        F.lit(SRC_MAT_BIRTH).alias("source_table"), source_row_id.alias("source_row_id"),
        F.date_format(b.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.greatest(b.BirthSourceRecordUpdatedDateTime, b.PregnancySourceRecordUpdatedDateTime,
                   b.NNUSourceLastUpdate).alias("source_update_timestamp"),
        b.ADC_UPDT.alias("loaded_at"),
    ).select(*PERSON_RELATIONSHIP_PUBLIC_COLUMNS)


PERSON_RELATIONSHIP_LIFECYCLE_FIELDS = [
    'load_batch_id',
]


PERSON_RELATIONSHIP_RETIRED_COLUMNS = [

]


PERSON_RELATIONSHIP_LIFECYCLE_COLUMNS = [
    'person_relationship_key',
    'birth_row_id',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_person_relationship():
    # Assemble person relationship rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _person_relationship_canonical()


@materialized_view(
    name=_n("journey_spine.person_relationship"),
    refresh_policy="auto",
)
def person_relationship():
    # Build the declared dataset: Source-backed relationships between people, including mother-
    # to-baby links supplied by maternity records.
    return _lifecycle_source_person_relationship().drop(*PERSON_RELATIONSHIP_LIFECYCLE_FIELDS, *PERSON_RELATIONSHIP_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_spine._person_relationship_metadata'),
    refresh_policy="auto",
)
def person_relationship_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # spine_person_relationship; same row grain as the research table. Join keys:
    # person_relationship_key, birth_row_id.
    return (_lifecycle_source_person_relationship()).select(*PERSON_RELATIONSHIP_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_spine_person_relationship_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                     'activity_record_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                     'birth_row_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                     'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                     'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
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
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                     'community_care_contact_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
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
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
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
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
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
                                                     'entry_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                     'episode_encounter_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                     'episode_encounter_reltn_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                     'episode_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                     'episode_id_resolved': 'True when episode_id is null or '
                                                                            'resolves to the governed '
                                                                            'episode spine; false records '
                                                                            'unresolved linkage without '
                                                                            'dropping the row.',
                                                     'episode_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                     'event_after_death_30d': 'True when the event timestamp '
                                                                              'is more than 30 days after '
                                                                              "the resolved person's death "
                                                                              'timestamp; silver records the '
                                                                              'doubt and does not filter the '
                                                                              'row.',
                                                     'event_before_birth': 'True when the event timestamp '
                                                                           "precedes the resolved person's "
                                                                           'birth timestamp; silver records '
                                                                           'the doubt and does not filter '
                                                                           'the row.',
                                                     'event_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                     'journey_participant_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                     'labour_delivery_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                     'load_batch_id': 'Deterministic batch token derived '
                                                                      "from the source row's bronze load "
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
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                     'microbiology_isolate_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                     'order_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                     'patient_event_key': 'Deterministic SHA-256 event key '
                                                                          'carried unchanged from the public '
                                                                          'parent; joins one lifecycle row '
                                                                          'to that event row.',
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
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
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
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                     'practitioner_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                     'practitioner_location_evidence_key': 'Deterministic v2 '
                                                                                           'key carried '
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
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
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
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
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
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                                  'one lifecycle row to that '
                                                                                  'research row.'},
                                         'table': 'Internal quality and batch metadata for '
                                                  'spine_person_relationship; same row grain as the research '
                                                  'table. Join keys: person_relationship_key, birth_row_id.'},
 'spine_person_relationship': {'columns': {'birth_row_id': 'MSDS BirthRow_ID; native source primary key.',
                                           'construction_rule': 'Governed relationship-construction rule.',
                                           'construction_version': 'Governed relationship-construction '
                                                                   'version.',
                                           'inverse_relationship_type_code': 'Controlled inverse '
                                                                             'relationship type.',
                                           'loaded_at': 'map_mat_birth.ADC_UPDT carried unchanged for the '
                                                        'mother-to-child relationship. The three '
                                                        'contributing source-update clocks are published '
                                                        'separately as source_update_timestamp and do not '
                                                        'replace this value; no parent-person or separate '
                                                        'pregnancy load clock is joined.',
                                           'person_relationship_key': 'Deterministic SHA-256 relationship '
                                                                      'key.',
                                           'record_status': 'Superseded when '
                                                            'map_mat_birth.PregnancySource_DELETE_IND cast '
                                                            'to LONG is nonzero, otherwise active; null '
                                                            'defaults to zero. No retracted status is '
                                                            'emitted. This is the pregnancy-enrichment '
                                                            'deletion flag carried on the birth row, not a '
                                                            'Boolean output or an independent test of '
                                                            'birth-source absence.',
                                           'relationship_type_code': 'Controlled source-to-target '
                                                                     'relationship type.',
                                           'source_person_id': 'Resolved source-side person identifier when '
                                                               'available.',
                                           'source_row_id': 'Stable source birth-row identifier.',
                                           'source_subject_key': 'Always-populated subject key for the '
                                                                 'relationship source person.',
                                           'source_system': 'Source-system label for these maternity-derived '
                                                            'records: millennium-maternity.',
                                           'source_table': 'Fully qualified configured bronze source table.',
                                           'source_update_timestamp': 'Greatest of '
                                                                      'map_mat_birth.BirthSourceRecordUpdatedDateTime, '
                                                                      'PregnancySourceRecordUpdatedDateTime '
                                                                      'and NNUSourceLastUpdate for the '
                                                                      'contributing birth row. Null inputs '
                                                                      'are ignored; all null gives null. No '
                                                                      'separate pregnancy or neonatal row is '
                                                                      'joined to add a clock, and the '
                                                                      'current Silver refresh time is not '
                                                                      'substituted.',
                                           'target_person_id': 'Resolved target-side person identifier when '
                                                               'available.',
                                           'target_subject_key': 'Always-populated subject key for the '
                                                                 'relationship target person.',
                                           'valid_from': 'Relationship validity start from source evidence.',
                                           'valid_to': 'Relationship validity end when source evidence '
                                                       'supplies one.'},
                               'table': 'Source-backed relationships between people, including '
                                        'mother-to-baby links supplied by maternity records.'}}
