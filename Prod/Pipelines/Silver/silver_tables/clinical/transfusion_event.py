"""Silver clinical.transfusion_event: 2 dataset(s).

- datasets: clinical_transfusion_event, _clinical_transfusion_event_metadata
- moved verbatim from: 2_silver_clinical_events:7340, 2_silver_clinical_events:7372, 2_silver_clinical_events:7377, 2_silver_clinical_events:7381, 2_silver_clinical_events:7389, 2_silver_clinical_events:7452, 2_silver_clinical_events:7463

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, _present, read_source, stable_id, subject_key_with_system


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / transfusion event
SRC_BLOODTRACK_TRANSACTION = "4_prod.bronze.map_bloodtrack_transaction"


TRANSFUSION_EVENT_LIFECYCLE_FIELDS = [
    'identity_status',
    'load_batch_id',
]


TRANSFUSION_EVENT_RETIRED_COLUMNS = [

]


TRANSFUSION_EVENT_LIFECYCLE_COLUMNS = [
    'transfusion_event_key',
    'bloodtrack_transaction_key',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_transfusion_event():
    # Assemble transfusion event rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    s = read_source(SRC_BLOODTRACK_TRANSACTION)
    event_id = stable_id("transfusion_event:bloodtrack", s.BLOODTRACK_TRANSACTION_KEY)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID),
         ("urn:bloodtrack:source-patient-number", s.SOURCE_PATIENT_NUMBER)],
        SRC_BLOODTRACK_TRANSACTION,
        s.BLOODTRACK_TRANSACTION_KEY,
    )
    inactive = ~F.coalesce(s.IS_CURRENT_IN_SOURCE, F.lit(True))
    # contract v2: publish the native BloodTrack transaction key and rename SHA/source-row identities explicitly
    return s.select(
        event_id.alias("transfusion_event_key"),
        s.BLOODTRACK_TRANSACTION_KEY.cast("string").alias("bloodtrack_transaction_key"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
        .when(_present(s.SOURCE_PATIENT_NUMBER), F.lit("provisional"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.coalesce(s.EVENT_TS_EFFECTIVE, s.SERVER_TRANSACTION_TS).alias("event_datetime"),
        F.coalesce(s.EVENT_TS_EFFECTIVE, s.SERVER_TRANSACTION_TS).alias("event_datetime_local"),
        s.WORKFLOW_STEP.alias("workflow_step"),
        s.TRANSACTION_SUCCESS_IND.alias("transaction_success_ind"),
        s.UNIT_NUMBER.alias("unit_number"), s.PRODUCT_CODE.alias("product_code"),
        s.PRODUCT_DESCRIPTION.alias("product_description"),
        s.BLOODTRACK_UNIT_ID.cast("string").alias("bloodtrack_unit_id"),
        s.DEVICE_NAME.alias("device_name"), s.SOURCE_LOCATION_NAME.alias("source_location"),
        s.LINKAGE_STATUS.alias("linkage_status"), s.RESPONSE_CODE.alias("response_code"),
        s.RESPONSE_TEXT.alias("response_text"), s.BLOOD_UNIT_STATE.alias("blood_unit_state"),
        s.BLOOD_UNIT_FATE.alias("blood_unit_fate"),
        s.ALERT_PRESENT_IND.alias("alert_present_ind"),
        s.COMMENT_PRESENT_IND.alias("comment_present_ind"),
        s.PRODUCT_MAPPING_METHOD.alias("product_mapping_method"),  # INSR_W2 W2.20
        s.PRODUCT_MAPPING_RULE_ID.alias("product_mapping_rule_id"),  # INSR_W2 W2.20
        s.UNIT_GROUP_MAPPING_METHOD.alias("unit_group_mapping_method"),  # INSR_W2 W2.20
        s.UNIT_GROUP_MAPPING_RULE_ID.alias("unit_group_mapping_rule_id"),  # INSR_W2 W2.20
        s.PATIENT_GROUP_MAPPING_METHOD.alias("patient_group_mapping_method"),  # INSR_W2 W2.20
        s.PATIENT_GROUP_MAPPING_RULE_ID.alias("patient_group_mapping_rule_id"),  # INSR_W2 W2.20
        s.PRODUCT_MAPPING_SOURCE.alias("product_mapping_source"),  # INSR_W2 W2.20
        s.FIRST_NAME.alias("first_name"),  # INSR_W2 W2.20
        s.HEALTH_NUMBER.alias("health_number"),  # INSR_W2 W2.20
        s.LAST_NAME.alias("last_name"),  # INSR_W2 W2.20
        s.PATIENT_BIRTH_DATE.alias("patient_birth_date"),  # INSR_W2 W2.20
        s.PATIENT_GENDER.alias("patient_gender"),  # INSR_W2 W2.20
        s.PATIENT_ID.alias("patient_id"),  # INSR_W2 W2.20
        s.PATIENT_NAME.alias("patient_name"),  # INSR_W2 W2.20
        s.PATIENT_NUMBER.alias("patient_number"),  # INSR_W2 W2.20
        s.PORTER_USER_NAME.alias("porter_user_name"),  # INSR_W2 W2.20
        s.SECONDARY_FIRST_NAME.alias("secondary_first_name"),  # INSR_W2 W2.20
        s.SECONDARY_LAST_NAME.alias("secondary_last_name"),  # INSR_W2 W2.20
        s.SECONDARY_USER_NAME.alias("secondary_user_name"),  # INSR_W2 W2.20
        s.USER_NAME.alias("user_name"),  # INSR_W2 W2.20
        s.SOURCE_MRN_NORM.alias("source_mrn_norm"),  # INSR_W2 W2.20
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        F.lit(SRC_BLOODTRACK_TRANSACTION).alias("source_table"),
        s.BLOODTRACK_TRANSACTION_KEY.cast("string").alias("source_row_key"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("loaded_at"),
    )


@materialized_view(
    name=_n("journey_clinical.transfusion_event"),
    refresh_policy="auto",
)
def transfusion_event():
    # Build the declared dataset: BloodTrack scan-grain workflow evidence including failed and
    # safety-check attempts.
    return _lifecycle_source_transfusion_event().drop(*TRANSFUSION_EVENT_LIFECYCLE_FIELDS, *TRANSFUSION_EVENT_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._transfusion_event_metadata'),
    refresh_policy="auto",
)
def transfusion_event_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_transfusion_event; same row grain as the research table. Join keys:
    # transfusion_event_key, bloodtrack_transaction_key.
    return (_lifecycle_source_transfusion_event()).select(*TRANSFUSION_EVENT_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'clinical_transfusion_event': ValuePolicy(table='clinical_transfusion_event', enabled=True),
}


WRAP_POLICY = {'clinical_transfusion_event'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_transfusion_event_metadata': {'columns': {'activity_date': 'Primary-key component carried '
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
                                                   'clinical_transfusion_event; same row grain as the '
                                                   'research table. Join keys: transfusion_event_key, '
                                                   'bloodtrack_transaction_key.'},
 'clinical_transfusion_event': {'columns': {'alert_present_ind': 'Alert presence indicator.',
                                            'blood_unit_fate': 'Source BloodTrack final fate recorded for '
                                                               'the blood unit, such as confirmed use or '
                                                               'non-use.',
                                            'blood_unit_state': 'Source BloodTrack status of the blood unit '
                                                                'within the storage and issue workflow at '
                                                                'the time of the transaction.',
                                            'bloodtrack_transaction_key': 'BloodTrack transaction key; '
                                                                          'native primary key of this table.',
                                            'bloodtrack_unit_id': 'BloodTrack unit identifier.',
                                            'comment_present_ind': 'Comment presence indicator.',
                                            'device_name': 'Scanning device name.',
                                            'event_datetime': 'Effective transaction timestamp.',
                                            'event_datetime_local': 'event_datetime in Europe/London '
                                                                    'wall-clock time (GMT/BST). Millennium '
                                                                    'rows take the bronze *_LOCAL companions '
                                                                    'of the same source columns and fallback '
                                                                    'order; rows from local-clock sources '
                                                                    '(LUNA, PACS/DICOM, TFC LIMS, CCMDS, '
                                                                    'MSDS, BadgerNet, EndoBase, SLAM, iWeb, '
                                                                    'Datix and similar) carry event_datetime '
                                                                    'unchanged. event_datetime is a UTC '
                                                                    'instant for Millennium rows, so take '
                                                                    'local calendar dates and clock times '
                                                                    'from this column.',
                                            'first_name': 'BloodTrack FIRST_NAME as sent by the source; '
                                                          'identifier, published and tagged '
                                                          '(INSR_W1_BLOODTRACK_V1). (bronze FIRST_NAME, INSR '
                                                          'W1). INSR_W2 W2.20.',
                                            'health_number': 'BloodTrack HEALTH_NUMBER as sent by the '
                                                             'source; identifier, published and tagged '
                                                             '(INSR_W1_BLOODTRACK_V1). (bronze '
                                                             'HEALTH_NUMBER, INSR W1). INSR_W2 W2.20.',
                                            'last_name': 'BloodTrack LAST_NAME as sent by the source; '
                                                         'identifier, published and tagged '
                                                         '(INSR_W1_BLOODTRACK_V1). (bronze LAST_NAME, INSR '
                                                         'W1). INSR_W2 W2.20.',
                                            'linkage_status': 'Patient linkage status.',
                                            'loaded_at': 'map_bloodtrack_transaction.ADC_UPDT carried '
                                                         'unchanged for the transaction row. No join to the '
                                                         'transfusion aggregate contributes its '
                                                         'PIPELINE_LOADED_AT; device/event times and the '
                                                         'current Silver refresh time do not replace this '
                                                         'value.',
                                            'patient_birth_date': 'BloodTrack PATIENT_BIRTH_DATE as sent by '
                                                                  'the source; identifier, published and '
                                                                  'tagged (INSR_W1_BLOODTRACK_V1). (bronze '
                                                                  'PATIENT_BIRTH_DATE, INSR W1). INSR_W2 '
                                                                  'W2.20.',
                                            'patient_gender': 'BloodTrack PATIENT_GENDER as sent by the '
                                                              'source; identifier, published and tagged '
                                                              '(INSR_W1_BLOODTRACK_V1). (bronze '
                                                              'PATIENT_GENDER, INSR W1). INSR_W2 W2.20.',
                                            'patient_group_mapping_method': 'PATIENT_GROUP map provenance '
                                                                            '(method). (bronze '
                                                                            'PATIENT_GROUP_MAPPING_METHOD, '
                                                                            'INSR W1). INSR_W2 W2.20.',
                                            'patient_group_mapping_rule_id': 'PATIENT_GROUP map provenance '
                                                                             '(rule_id). (bronze '
                                                                             'PATIENT_GROUP_MAPPING_RULE_ID, '
                                                                             'INSR W1). INSR_W2 W2.20.',
                                            'patient_id': 'BloodTrack PATIENT_ID as sent by the source; '
                                                          'identifier, published and tagged '
                                                          '(INSR_W1_BLOODTRACK_V1). (bronze PATIENT_ID, INSR '
                                                          'W1). INSR_W2 W2.20.',
                                            'patient_name': 'BloodTrack PATIENT_NAME as sent by the source; '
                                                            'identifier, published and tagged '
                                                            '(INSR_W1_BLOODTRACK_V1). (bronze PATIENT_NAME, '
                                                            'INSR W1). INSR_W2 W2.20.',
                                            'patient_number': 'BloodTrack PATIENT_NUMBER as sent by the '
                                                              'source; identifier, published and tagged '
                                                              '(INSR_W1_BLOODTRACK_V1). (bronze '
                                                              'PATIENT_NUMBER, INSR W1). INSR_W2 W2.20.',
                                            'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                         'foreign key.',
                                            'porter_user_name': 'BloodTrack PORTER_USER_NAME as sent by the '
                                                                'source; identifier, published and tagged '
                                                                '(INSR_W1_BLOODTRACK_V1). (bronze '
                                                                'PORTER_USER_NAME, INSR W1). INSR_W2 W2.20.',
                                            'product_code': 'Product code scanned.',
                                            'product_description': 'Source BloodTrack textual description of '
                                                                   'the blood component product.',
                                            'product_mapping_method': 'PRODUCT map provenance (method). '
                                                                      '(bronze PRODUCT_MAPPING_METHOD, INSR '
                                                                      'W1). INSR_W2 W2.20.',
                                            'product_mapping_rule_id': 'PRODUCT map provenance (rule_id). '
                                                                       '(bronze PRODUCT_MAPPING_RULE_ID, '
                                                                       'INSR W1). INSR_W2 W2.20.',
                                            'product_mapping_source': 'Map table the product concept came '
                                                                      'from. (bronze PRODUCT_MAPPING_SOURCE, '
                                                                      'INSR W1). INSR_W2 W2.20.',
                                            'record_status': 'Superseded when '
                                                             'map_bloodtrack_transaction.IS_CURRENT_IN_SOURCE '
                                                             'is false, otherwise active; a missing flag '
                                                             'defaults true. No retracted status is emitted. '
                                                             'Workflow step, transaction success, response '
                                                             'and blood-unit state/fate do not set this '
                                                             'label.',
                                            'response_code': 'Device or workflow response code.',
                                            'response_text': 'Device or workflow response text.',
                                            'secondary_first_name': 'BloodTrack SECONDARY_FIRST_NAME as sent '
                                                                    'by the source; identifier, published '
                                                                    'and tagged (INSR_W1_BLOODTRACK_V1). '
                                                                    '(bronze SECONDARY_FIRST_NAME, INSR W1). '
                                                                    'INSR_W2 W2.20.',
                                            'secondary_last_name': 'BloodTrack SECONDARY_LAST_NAME as sent '
                                                                   'by the source; identifier, published and '
                                                                   'tagged (INSR_W1_BLOODTRACK_V1). (bronze '
                                                                   'SECONDARY_LAST_NAME, INSR W1). INSR_W2 '
                                                                   'W2.20.',
                                            'secondary_user_name': 'BloodTrack SECONDARY_USER_NAME as sent '
                                                                   'by the source; identifier, published and '
                                                                   'tagged (INSR_W1_BLOODTRACK_V1). (bronze '
                                                                   'SECONDARY_USER_NAME, INSR W1). INSR_W2 '
                                                                   'W2.20.',
                                            'source_location': 'Source workflow location name.',
                                            'source_mrn_norm': 'Digits-only normalisation of '
                                                               'SOURCE_PATIENT_NUMBER (the BloodTrack MRN) '
                                                               'used for MRN linkage and TRANSFUSION_KEY. '
                                                               'Identifier, published and tagged. '
                                                               'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                               'SOURCE_MRN_NORM, INSR W1). INSR_W2 W2.20.',
                                            'source_row_key': 'Verbatim BloodTrack transaction key retained '
                                                              'as source-row provenance.',
                                            'source_table': 'Registered source table.',
                                            'subject_id_system': 'Identifier system used for subject_key.',
                                            'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                           'identifier. Not salted, not secret; a stable '
                                                           'join key across feeds where person_id is '
                                                           'unresolved.',
                                            'transaction_success_ind': 'Whether the transaction succeeded.',
                                            'transfusion_event_key': 'Deterministic SHA-256 BloodTrack '
                                                                     'scan-event key.',
                                            'unit_group_mapping_method': 'UNIT_GROUP map provenance '
                                                                         '(method). (bronze '
                                                                         'UNIT_GROUP_MAPPING_METHOD, INSR '
                                                                         'W1). INSR_W2 W2.20.',
                                            'unit_group_mapping_rule_id': 'UNIT_GROUP map provenance '
                                                                          '(rule_id). (bronze '
                                                                          'UNIT_GROUP_MAPPING_RULE_ID, INSR '
                                                                          'W1). INSR_W2 W2.20.',
                                            'unit_number': 'Blood unit number.',
                                            'user_name': 'BloodTrack USER_NAME as sent by the source; '
                                                         'identifier, published and tagged '
                                                         '(INSR_W1_BLOODTRACK_V1). (bronze USER_NAME, INSR '
                                                         'W1). INSR_W2 W2.20.',
                                            'workflow_step': 'BloodTrack workflow step.'},
                                'table': 'BloodTrack scan-grain workflow evidence including failed and '
                                         'safety-check attempts.'}}

# INSR_R4_SILVER_V1 (module_drop): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import with_columns_last  # noqa: E402
TRANSFUSION_EVENT_LIFECYCLE_FIELDS = [c for c in TRANSFUSION_EVENT_LIFECYCLE_FIELDS if c not in ['identity_status']]  # decision 1: published
SPECS = with_columns_last(SPECS, 'clinical_transfusion_event', ('identity_status',))  # additions last
