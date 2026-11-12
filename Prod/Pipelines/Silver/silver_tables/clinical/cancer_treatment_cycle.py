"""Silver clinical.cancer_treatment_cycle: 2 dataset(s).

- datasets: clinical_cancer_treatment_cycle, _clinical_cancer_treatment_cycle_metadata
- moved verbatim from: 2_silver_clinical_events:7640, 2_silver_clinical_events:7667, 2_silver_clinical_events:7671, 2_silver_clinical_events:7675, 2_silver_clinical_events:7683, 2_silver_clinical_events:7724, 2_silver_clinical_events:7735

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables._common.journey import SRC_CANCER_TREATMENT


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / cancer treatment cycle
SRC_CANCER_TREATMENT_CYCLE = "4_prod.bronze.map_cancer_treatment_cycle"


CANCER_TREATMENT_CYCLE_LIFECYCLE_FIELDS = [
    'load_batch_id',
]


CANCER_TREATMENT_CYCLE_RETIRED_COLUMNS = [

]


CANCER_TREATMENT_CYCLE_LIFECYCLE_COLUMNS = [
    'cancer_treatment_cycle_key',
    'chemotherapy_course_id',
    'treatment_cycle_id',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_cancer_treatment_cycle():
    # Assemble cancer treatment cycle rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    s = read_source(SRC_CANCER_TREATMENT_CYCLE)
    # INSR_W2 W2.08 (L2204): carry person_id natively from the bronze parent course (one person per course; a
    # course with conflicting persons gets NULL), so a cycle whose parent fails the drug admission is still a
    # person's data.
    parent = read_source(SRC_CANCER_TREATMENT).groupBy(
        F.col("iqemo_chemotherapy_course_id").cast("bigint").alias("_cy_course")).agg(
        F.min(F.col("PERSON_ID").cast("bigint")).alias("_cy_lo"), F.max(F.col("PERSON_ID").cast("bigint")).alias("_cy_hi"))
    s = s.join(parent, s.CHEMOTHERAPY_COURSE_ID.cast("bigint") == F.col("_cy_course"), "left")
    row_key = F.concat_ws(":", s.CHEMOTHERAPY_COURSE_ID.cast("string"),
                          s.TREATMENT_CYCLE_ID.cast("string"))
    # contract v2: retain the composite SHA as cancer_treatment_cycle_key and publish the two native iQemo key components
    return s.select(
        stable_id("cancer_cycle:iqemo", s.CHEMOTHERAPY_COURSE_ID,
                  s.TREATMENT_CYCLE_ID).alias("cancer_treatment_cycle_key"),
        s.CHEMOTHERAPY_COURSE_ID.cast("bigint").alias("chemotherapy_course_id"),
        s.TREATMENT_CYCLE_ID.cast("bigint").alias("treatment_cycle_id"),
        F.when(F.col("_cy_lo") == F.col("_cy_hi"), F.col("_cy_lo")).alias("person_id"),  # INSR_W2 W2.08
        s.CHEMOTHERAPY_COURSE_ID.cast("string").alias("iqemo_course_id"),
        s.TREATMENT_CYCLE_ID.cast("string").alias("cycle_sequence_id"),
        s.RegimenCycleID.cast("string").alias("regimen_cycle_id"),
        s.TreatmentCycleCode.alias("cycle_code"),
        s.PRESCRIBED_DATE_CLEAN.alias("prescribed_datetime"),
        s.PHARMACY_CONFIRMED_DATE_CLEAN.alias("pharmacy_confirmed_datetime"),
        s.START_DATE_CLEAN.alias("cycle_start_datetime"),
        s.CANCELLATION_DATE_CLEAN.alias("cancellation_datetime"),
        s.CycleStatus.cast("string").alias("cycle_status_code"),
        s.TreatmentResponseID.cast("string").alias("treatment_response_id"),
        s.LINE_OF_TREATMENT_ID.cast("string").alias("line_of_treatment"),
        s.REGIMEN_NUMBER.cast("string").alias("regimen_number"),
        s.MAP_CANCER_TREATMENT_LINK_STATUS.alias("course_link_status"),
        s.OutcomeComments.alias("outcome_comments"),
        F.lit("active").alias("record_status"),
        F.lit(SRC_CANCER_TREATMENT_CYCLE).alias("source_table"),
        row_key.alias("source_row_id"),
        F.date_format(s.PIPELINE_UPDT_DT_TM, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.PIPELINE_UPDT_DT_TM.alias("loaded_at"),
    )


@materialized_view(
    name=_n("journey_clinical.cancer_treatment_cycle"),
    refresh_policy="auto",
)
def cancer_treatment_cycle():
    # Build the declared dataset: One iQemo chemotherapy cycle record linked to its parent
    # treatment course, with recorded cycle timing.
    return _lifecycle_source_cancer_treatment_cycle().drop(*CANCER_TREATMENT_CYCLE_LIFECYCLE_FIELDS, *CANCER_TREATMENT_CYCLE_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._cancer_treatment_cycle_metadata'),
    refresh_policy="auto",
)
def cancer_treatment_cycle_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_cancer_treatment_cycle; same row grain as the research table. Join keys:
    # cancer_treatment_cycle_key, chemotherapy_course_id, treatment_cycle_id.
    return (_lifecycle_source_cancer_treatment_cycle()).select(*CANCER_TREATMENT_CYCLE_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_cancer_treatment_cycle_metadata': {'columns': {'activity_date': 'Primary-key component carried '
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
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                           'aria_dx_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                           'aria_pt_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                           'artifact_link_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                           'birth_row_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                           'bloodtrack_transaction_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                           'cancer_treatment_cycle_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                           'care_contact_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                           'care_participation_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                           'cds_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                           'chemotherapy_course_id': 'Native source '
                                                                                     'identifier carried '
                                                                                     'unchanged from the '
                                                                                     'public parent; joins '
                                                                                     'one lifecycle row to '
                                                                                     'that research row.',
                                                           'community_care_activity_key': 'Deterministic v2 '
                                                                                          'key carried '
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
                                                           'community_care_contact_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                           'concept_map_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                           'cost_line_item_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                           'dcp_forms_activity_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
                                                                                    'one lifecycle row to '
                                                                                    'that research row.',
                                                           'device_mapping_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                           'diagnosis_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                           'dicom_path': 'Primary-key component carried '
                                                                         'unchanged from the public parent; '
                                                                         "combine it with the companion's "
                                                                         'other key columns when joining.',
                                                           'doc_response_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                           'elective_access_procedure_key': 'Deterministic '
                                                                                            'v2 key carried '
                                                                                            'unchanged from '
                                                                                            'the public '
                                                                                            'parent; joins '
                                                                                            'one lifecycle '
                                                                                            'row to that '
                                                                                            'research row.',
                                                           'encounter_attribute_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                           'encounter_bounds_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                           'encounter_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                           'encounter_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                           'endobase_exam_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                           'endobase_exam_term_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
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
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                           'episode_encounter_reltn_id': 'Native source '
                                                                                         'identifier carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
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
                                                                                    'does not filter the '
                                                                                    'row.',
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
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                           'gene_tested_row_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                           'genetic_result_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                           'genetic_test_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
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
                                                           'implant_sequence': 'Primary-key component '
                                                                               'carried unchanged from the '
                                                                               'public parent; combine it '
                                                                               "with the companion's other "
                                                                               'key columns when joining.',
                                                           'indication_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                           'location_stay_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                           'mc_device_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                           'organization_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                           'pacs_examination_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                                'the public parent; joins '
                                                                                'one lifecycle row to that '
                                                                                'event row.',
                                                           'patient_event_row_key': 'Deterministic row-level '
                                                                                    'key carried unchanged '
                                                                                    'from '
                                                                                    'events_patient_event; '
                                                                                    'joins one lifecycle row '
                                                                                    'to one event-index row.',
                                                           'period_business_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                           'person_death_evidence_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                           'person_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                           'person_id_resolved': 'True when person_id is '
                                                                                 'null or resolves to the '
                                                                                 'governed person spine; '
                                                                                 'false records unresolved '
                                                                                 'linkage without dropping '
                                                                                 'the row.',
                                                           'person_identifier_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                           'person_relationship_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                           'pharmacy_issue_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                           'pm_wait_list_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                           'practitioner_group_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                           'practitioner_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                           'practitioner_identifier_key': 'Deterministic v2 '
                                                                                          'key carried '
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
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
                                                                                                 'row to '
                                                                                                 'that '
                                                                                                 'research '
                                                                                                 'row.',
                                                           'pregnancy_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                           'pregnancy_reconciliation_key': 'Deterministic v2 '
                                                                                           'key carried '
                                                                                           'unchanged from '
                                                                                           'the public '
                                                                                           'parent; joins '
                                                                                           'one lifecycle '
                                                                                           'row to that '
                                                                                           'research row.',
                                                           'problem_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                           'procedure_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                           'pt_prot_reg_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                           'ptl_unique_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                           'record_status': 'active, superseded or '
                                                                            'retracted, normalised from the '
                                                                            "source's own status fields; "
                                                                            'superseded rows are earlier '
                                                                            'versions the source kept.',
                                                           'record_status_effective_from': 'Timestamp from '
                                                                                           'which the '
                                                                                           'normalised '
                                                                                           'record status '
                                                                                           'applies, when '
                                                                                           'the source '
                                                                                           'supplies one.',
                                                           'record_status_effective_to': 'Timestamp until '
                                                                                         'which the '
                                                                                         'normalised record '
                                                                                         'status applies; '
                                                                                         'null means no '
                                                                                         'source end is '
                                                                                         'known.',
                                                           'referral_oid': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                           'row_source': 'Physical bronze row source, '
                                                                         'CURRENT or HIST, carried unchanged '
                                                                         'from the public parent; a key '
                                                                         'component because the research '
                                                                         'table is at physical version grain '
                                                                         'and patient_event_key is shared '
                                                                         "across an entry's versions.",
                                                           'rtt_activity_oid': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                           'sch_event_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                           'score_type': 'Primary-key component carried '
                                                                         'unchanged from the public parent; '
                                                                         "combine it with the companion's "
                                                                         'other key columns when joining.',
                                                           'sequence_nbr': 'Primary-key component carried '
                                                                           'unchanged from the public '
                                                                           'parent; combine it with the '
                                                                           "companion's other key columns "
                                                                           'when joining.',
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
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                           'source_identifier_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                           'source_object': 'Source-arm discriminator '
                                                                            'carried unchanged from the '
                                                                            'public parent; use it with '
                                                                            'arm-specific native identifiers '
                                                                            'when joining.',
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
                                                                                'identifier carried '
                                                                                'unchanged from the public '
                                                                                'parent; CURRENT rows carry '
                                                                                '-1. A key component '
                                                                                'alongside row_source.',
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
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                           'theatre_case_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                           'theatre_case_milestone_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                           'theatre_implant_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                           'transfusion_event_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; combine it with the '
                                                                           "companion's other key columns "
                                                                           'when joining.',
                                                           'valid_from_datetime': 'Primary-key component '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; '
                                                                                  'combine it with the '
                                                                                  "companion's other key "
                                                                                  'columns when joining.',
                                                           'value_set_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                           'waiting_list_oid': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                           'waiting_list_snapshot_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.'},
                                               'table': 'Internal quality and batch metadata for '
                                                        'clinical_cancer_treatment_cycle; same row grain as '
                                                        'the research table. Join keys: '
                                                        'cancer_treatment_cycle_key, chemotherapy_course_id, '
                                                        'treatment_cycle_id.'},
 'clinical_cancer_treatment_cycle': {'columns': {'cancellation_datetime': 'Sentinel-cleaned cancellation '
                                                                          'timestamp.',
                                                 'cancer_treatment_cycle_key': 'Deterministic SHA-256 key '
                                                                               'for the iQemo course-cycle '
                                                                               'pair.',
                                                 'chemotherapy_course_id': 'iQemo CHEMOTHERAPY_COURSE_ID as '
                                                                           'BIGINT; first component of the '
                                                                           'primary key.',
                                                 'course_link_status': 'Parent course-link status.',
                                                 'cycle_code': 'Source cycle code.',
                                                 'cycle_sequence_id': 'Verbatim treatment-cycle sequence '
                                                                      'token.',
                                                 'cycle_start_datetime': 'Sentinel-cleaned cycle start '
                                                                         'timestamp.',
                                                 'cycle_status_code': 'Source cycle status code.',
                                                 'iqemo_course_id': 'iQemo chemotherapy course identifier.',
                                                 'line_of_treatment': 'Coded lookup identifier for the line '
                                                                      'of treatment represented by the '
                                                                      'parent chemotherapy course.',
                                                 'loaded_at': 'map_cancer_treatment_cycle.PIPELINE_UPDT_DT_TM '
                                                              'carried unchanged. No treatment-parent clock '
                                                              'is joined, and ADC_UPDT, prescription dates, '
                                                              'cancellation time and the current Silver '
                                                              'refresh time are not substituted.',
                                                 'outcome_comments': 'Source outcome comments.',
                                                 'person_id': 'Native Millennium PERSON_ID of the parent '
                                                              'iQemo course (bronze map_cancer_treatment); '
                                                              'NULL when the course has no person or more '
                                                              'than one. INSR_W2 W2.08.',
                                                 'pharmacy_confirmed_datetime': 'Sentinel-cleaned '
                                                                                'pharmacy-confirmed '
                                                                                'timestamp.',
                                                 'prescribed_datetime': 'Sentinel-cleaned prescribed '
                                                                        'timestamp.',
                                                 'record_status': 'Always active in the current '
                                                                  'map_cancer_treatment_cycle projection. '
                                                                  'CycleStatus and CANCELLATION_DATE_CLEAN '
                                                                  'remain separate clinical fields and do '
                                                                  'not change this literal label; no '
                                                                  'superseded or retracted status is '
                                                                  'emitted.',
                                                 'regimen_cycle_id': 'Regimen cycle identifier.',
                                                 'regimen_number': 'Sequential number identifying the '
                                                                   'regimen within the parent chemotherapy '
                                                                   'course.',
                                                 'source_row_id': 'Composite source row identifier.',
                                                 'source_table': 'Registered source table.',
                                                 'treatment_cycle_id': 'iQemo TREATMENT_CYCLE_ID as BIGINT; '
                                                                       'second component of the primary key.',
                                                 'treatment_response_id': 'Source treatment-response '
                                                                          'identifier from '
                                                                          '`TreatmentResponseID`, retained '
                                                                          'as a string on the treatment '
                                                                          'cycle.'},
                                     'table': 'One iQemo chemotherapy cycle record linked to its parent '
                                              'treatment course, with recorded cycle timing.'}}
