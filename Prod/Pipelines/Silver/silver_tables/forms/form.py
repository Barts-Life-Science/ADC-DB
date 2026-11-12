"""Silver forms.form: 4 dataset(s).

- datasets: _clinical_form_grouped, _clinical_qc_form, clinical_form, _clinical_form_metadata
- moved verbatim from: 2_silver_clinical_events:3929, 2_silver_clinical_events:3931, 2_silver_clinical_events:3933, 2_silver_clinical_events:4021, 2_silver_clinical_events:4039, 2_silver_clinical_events:4060, 2_silver_clinical_events:4102, 2_silver_clinical_events:4152

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, spark
from silver_tables._common.clinical_events import SRC_FORM_ITEM
from silver_tables._common.journey import SRC_FORM_ACTIVITY, _form_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


SRC_FORM_ASSESSMENT = "4_prod.bronze.map_powerform_assessment"


SRC_FORM_VTE = "4_prod.bronze.map_mat_vte_assessment"


def _form_grouped_query():
    # Join form activity, item and assessment records and group their responses into the
    # internal form payload.
    a = read_source(SRC_FORM_ACTIVITY).alias("a")
    i = read_source(SRC_FORM_ITEM).alias("i")
    g = read_source(SRC_FORM_ASSESSMENT).alias("g")
    v = read_source(SRC_FORM_VTE).select(
        "DOC_RESPONSE_KEY", "Pregnancy_ID", "PregnancyMatchMethod", "Source_ADC_UPDT"
    ).alias("v")
    joined = (
        a.join(i, a.DOC_RESPONSE_KEY == i.DOC_RESPONSE_KEY, "left")
         .join(g, a.DCP_FORMS_ACTIVITY_ID == g.DCP_FORMS_ACTIVITY_ID, "left")
         .join(v, a.DOC_RESPONSE_KEY == v.DOC_RESPONSE_KEY, "left")
    )
    response = F.struct(
        F.coalesce(a.RESPONSE_SEQUENCE_NBR, F.lit(0)).cast("long").alias("sequence"),
        a.DOC_RESPONSE_KEY.alias("response_id"),
        a.SECTION_DESC_TXT.alias("section"),
        a.SECTION_REF_ID.cast("string").alias("section_id"),
        a.DCP_INPUT_REF_ID.cast("string").alias("element_id"),
        a.ELEMENT_LABEL_TXT.alias("element_label"),
        a.GRID_NAME_TXT.alias("grid_name"),
        a.GRID_ROW_DESC_TXT.alias("grid_row"),
        i.RESPONSE_KIND.alias("response_kind"),
        F.coalesce(i.CANONICAL_TEXT_RESULT, i.RESPONSE_TEXT_TXT,
                   a.RESPONSE_VALUE_TXT, a.STRING_RESPONSE_TXT).alias("response_value_text"),
        F.coalesce(i.CANONICAL_NUMERIC_RESULT, a.NUMERIC_RESPONSE_NBR)
         .cast("decimal(38,10)").alias("response_value_number"),
        F.coalesce(i.CANONICAL_DATE_RESULT, i.RESPONSE_DT_TM).alias("response_value_datetime"),
        F.when(i.RESPONSE_NOMENCLATURE_ID.isNotNull(), F.lit("urn:cerner:nomenclature"))
         .when(i.RESPONSE_CODE_VALUE_ID.isNotNull(), F.lit("urn:cerner:code_value"))
         .alias("response_coding_system"),
        F.coalesce(i.RESPONSE_NOMENCLATURE_ID, i.RESPONSE_CODE_VALUE_ID)
         .cast("string").alias("response_coding_code"),
        i.RESPONSE_TEXT_TXT.alias("response_coding_display"),
        i.QUESTION_CONCEPT_ID.cast("string").alias("question_concept_id"),
        i.VALUE_CONCEPT_ID.cast("string").alias("value_concept_id"),
        i.UNIT_CONCEPT_ID.cast("string").alias("unit_concept_id"),
        v.Pregnancy_ID.cast("string").alias("pregnancy_id"),
        v.PregnancyMatchMethod.alias("pregnancy_match_method"),
        (a.ACTIVE_IND == 1).alias("active_ind"),
        i.SOURCE_PRESENT_IND.alias("source_present_ind"),
        i.CANONICAL_SOURCE_DELETED_IND.alias("source_deleted_ind"),
        i.CANONICAL_MATCH_STATUS.alias("canonical_match_status"),
    )
    empty_response = (
        a.RESPONSE_VALUE_TXT.isNull() & a.STRING_RESPONSE_TXT.isNull()
        & a.NUMERIC_RESPONSE_NBR.isNull() & a.RESPONSE_NOMENCLATURE_ID.isNull()
        & a.RESPONSE_CODE_VALUE_ID.isNull()
    )
    grouped = joined.groupBy(a.DCP_FORMS_ACTIVITY_ID).agg(
        F.max(a.PERSON_ID_LONG).alias("PERSON_ID"),
        F.max(a.ENCNTR_ID_LONG).alias("ENCNTR_ID"),
        F.max(a.ORGANIZATION_ID).alias("ORGANIZATION_ID"),
        F.max(a.FORM_REF_ID).alias("FORM_REF_ID"),
        F.max(a.FORM_STATUS_CD).alias("FORM_STATUS_CD"),
        F.max(a.STATUS).alias("STATUS"),
        F.max(a.FORM_DESC_TXT).alias("FORM_DESC_TXT"),
        F.min(a.DOCUMENTATION_DT_TM).alias("authored_datetime"),
        F.max(F.coalesce(a.LAST_DOCUMENTED_DT_TM, a.PERFORMED_DT_TM))
         .alias("completed_datetime"),
        F.min(a.DOCUMENTATION_DT_TM_LOCAL).alias("authored_datetime_local"),
        F.max(F.coalesce(a.LAST_DOCUMENTED_DT_TM_LOCAL, a.PERFORMED_DT_TM_LOCAL))
         .alias("completed_datetime_local"),
        F.max(a.PERFORMED_PRSNL_ID_LONG).alias("PERFORMED_PRSNL_ID"),
        F.parse_json(F.to_json(F.sort_array(F.collect_list(response)))).alias("responses"),
        F.count(F.lit(1)).cast("long").alias("response_row_count"),
        F.sum(F.when(a.ACTIVE_IND == 1, F.lit(1)).otherwise(F.lit(0)))
         .cast("long").alias("active_response_row_count"),
        F.sum(F.when(empty_response, F.lit(1)).otherwise(F.lit(0)))
         .cast("long").alias("empty_response_row_count"),
        F.max(g.INVALID_ROW_COUNT).cast("long").alias("invalid_response_row_count"),
        F.max(g.CANONICAL_MATCHED_ROW_COUNT).cast("long").alias("matched_response_row_count"),
        F.max(g.CANONICAL_UNMATCHED_ROW_COUNT).cast("long").alias("unmatched_response_row_count"),
        F.max(g.CONTEXT_CONFLICT_IND.cast("int")).alias("context_conflict_int"),
        F.max(g.CONTEXT_QUARANTINED_IND.cast("int")).alias("context_quarantined_int"),
        F.max(g.ASSESSMENT_ACTIVE_IND.cast("int")).alias("assessment_active_int"),
        F.max(g.SOURCE_PRESENT_IND.cast("int")).alias("source_present_int"),
        F.max(F.greatest(a.ADC_UPDT, a.SOURCE_ACTIVITY_ADC_UPDT,
                         a.SOURCE_EVENT_ADC_UPDT, a.SOURCE_LABEL_ADC_UPDT))
         .alias("source_update_timestamp"),
        F.max(a.ADC_UPDT).alias("activity_loaded_at"),
        F.max(i.ADC_UPDT).alias("item_loaded_at"),
        F.max(g.ADC_UPDT).alias("assessment_loaded_at"),
        F.max(v.Source_ADC_UPDT).alias("vte_loaded_at"),
    )
    return grouped


@materialized_view(
    name=_n("journey_clinical._form_grouped"),
    refresh_policy="auto",
)
def _form_grouped():
    # Build the declared dataset: Internal top-level aggregate for incrementally maintained
    # ordered PowerForm responses.
    return _form_grouped_query()


# Clinical /  qc form
@materialized_view(
    name=_n("journey_clinical._qc_form"),
    refresh_policy="auto",
)
def _qc_form():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for form.
    return _cross_qc_primitive(
        _form_canonical(),
        "form",
        {"responses": "_qc_responses_json"},
    )


# Clinical / form

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
FORM_PUBLIC_COLUMNS = [
    'patient_event_key',
    'dcp_forms_activity_id',
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
    'form_type_code',
    'form_type_display',
    'form_status_code',
    'form_status_display',
    'authored_datetime',
    'completed_datetime',
    'performed_practitioner_id',
    'organization_id',
    # contract v2: the ordered payload is decomposed into clinical_form_response,
    # one row per response, so the parent no longer publishes it. The reconciliation
    # counters below stay: they are how a reader checks the child is complete.
    'response_row_count',
    'active_response_row_count',
    'empty_response_row_count',
    'invalid_response_row_count',
    'matched_response_row_count',
    'unmatched_response_row_count',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


FORM_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'dcp_forms_activity_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_clinical.form"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def form():
    # Build the declared dataset: One PowerForm instance with ordered lossless responses and
    # reconciliation evidence.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_form")),
        "form",
        {},
        FORM_PUBLIC_COLUMNS,
        FORM_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._form_metadata'),
    refresh_policy="auto",
)
def form_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_form; same
    # row grain as the research table. Join keys: patient_event_key, dcp_forms_activity_id.
    return (spark.read.table(_n("journey_clinical._qc_form"))).select(*FORM_LIFECYCLE_COLUMNS)


CROSS_QC = {
    'form': CrossQc(model='form', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved']),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_form_grouped': {'columns': {},
                            'table': 'Internal top-level aggregate for incrementally maintained ordered '
                                     'PowerForm responses.'},
 '_clinical_form_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from the '
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
                             'table': 'Internal quality and batch metadata for clinical_form; same row grain '
                                      'as the research table. Join keys: patient_event_key, '
                                      'dcp_forms_activity_id.'},
 '_clinical_qc_form': {'columns': {}, 'table': 'Private JSON bridge and Gold cross-rule flags for form.'},
 'clinical_form': {'columns': {'active_response_row_count': 'Number of active response rows.',
                               'authored_datetime': 'Earliest form documentation timestamp.',
                               'completed_datetime': 'Latest documentation or performed timestamp.',
                               'confidentiality_code': 'Security classification when supplied.',
                               'dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID; primary key of '
                                                        'this table.',
                               'empty_response_row_count': 'Number of retained empty response rows.',
                               'encounter_id': 'Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.',
                               'event_datetime': 'First documented timestamp.',
                               'event_datetime_local': 'event_datetime in Europe/London wall-clock time '
                                                       '(GMT/BST). Millennium rows take the bronze *_LOCAL '
                                                       'companions of the same source columns and fallback '
                                                       'order; rows from local-clock sources (LUNA, '
                                                       'PACS/DICOM, TFC LIMS, CCMDS, MSDS, BadgerNet, '
                                                       'EndoBase, SLAM, iWeb, Datix and similar) carry '
                                                       'event_datetime unchanged. event_datetime is a UTC '
                                                       'instant for Millennium rows, so take local calendar '
                                                       'dates and clock times from this column.',
                               'event_end_datetime': 'Last documented or performed timestamp.',
                               'event_end_datetime_local': 'event_end_datetime in Europe/London wall-clock '
                                                           'time (GMT/BST), by the same rule as '
                                                           'event_datetime_local; null where '
                                                           'event_end_datetime is null.',
                               'form_status_code': 'Source form status code.',
                               'form_status_display': 'Source form status display.',
                               'form_type_code': 'Source form type code.',
                               'form_type_display': 'Source form type display.',
                               'invalid_response_row_count': 'Number of source-classified invalid response '
                                                             'rows.',
                               'loaded_at': 'Greatest of the form-level maximum ADC_UPDT values from '
                                            'mill_form_activity, map_powerform_assessment_item and '
                                            'map_powerform_assessment, and maximum Source_ADC_UPDT from '
                                            'map_mat_vte_assessment. This is the contributing-source '
                                            'ingestion/provenance clock, not the Silver refresh time or form '
                                            'event time; null only when every contributing clock is null.',
                               'matched_response_row_count': 'Number of canonical matched response rows.',
                               'organization_id': 'Millennium ORGANIZATION_ID for the source organization.',
                               'patient_event_key': 'Deterministic SHA-256 event identity; stable cross-feed '
                                                    'join key.',
                               'performed_practitioner_id': 'Millennium personnel PERSON_ID for the '
                                                            'performer.',
                               'person_id': 'Millennium PERSON_ID; native person foreign key.',
                               'record_status': 'Derived PowerForm row status: retracted when the grouped '
                                                'maximum of map_powerform_assessment.SOURCE_PRESENT_IND is '
                                                '0; otherwise superseded when the grouped maximum of '
                                                'ASSESSMENT_ACTIVE_IND is 0 or no mill_form_activity '
                                                'response has ACTIVE_IND = 1; otherwise active. Missing '
                                                'assessment flags alone do not trigger retraction or '
                                                'supersession. This is a status label, not the raw Boolean '
                                                'assessment flag.',
                               'record_status_effective_from': 'Earliest '
                                                               'mill_form_activity.DOCUMENTATION_DT_TM for '
                                                               'the form instance, carried from '
                                                               'authored_datetime. This is the form '
                                                               'documentation start used by the research '
                                                               'status projection, not an independently '
                                                               'recorded status-change timestamp; null when '
                                                               'all contributing documentation timestamps '
                                                               'are null.',
                               'record_status_effective_to': 'For a retracted or superseded form, '
                                                             'completed_datetime: the maximum of '
                                                             'coalesce(mill_form_activity.LAST_DOCUMENTED_DT_TM, '
                                                             'mill_form_activity.PERFORMED_DT_TM) across its '
                                                             'responses. Null for active forms or when no '
                                                             'contributing completion timestamp is '
                                                             'available. This is a documentation/completion '
                                                             'boundary, not a Boolean assessment flag or an '
                                                             'independently recorded status-change time.',
                               'response_row_count': 'Number of response rows grouped into the form.',
                               'source_code': 'Source form reference identifier.',
                               'source_coding_system': 'Source form coding system.',
                               'source_display': 'Source form description.',
                               'source_feed': 'Registered owning feed.',
                               'source_update_timestamp': "Maximum, across the form's mill_form_activity "
                                                          'rows, of the greatest ADC_UPDT, '
                                                          'SOURCE_ACTIVITY_ADC_UPDT, SOURCE_EVENT_ADC_UPDT '
                                                          'and SOURCE_LABEL_ADC_UPDT. This combines the '
                                                          'contributing activity, event, label and '
                                                          'bronze-ingestion clocks; it is not uniformly a '
                                                          'native clinical-system update time. Null only '
                                                          'when every contributing clock is null.',
                               'subject_id_system': 'Identifier system used for subject_key.',
                               'subject_key': 'Deterministic SHA-256 over the strongest source identifier. '
                                              'Not salted, not secret; a stable join key across feeds where '
                                              'person_id is unresolved.',
                               'unmatched_response_row_count': 'Number of canonical unmatched response rows.',
                               'vip_ind': 'VIP indicator when supplied.',
                               'withheld_identity_ind': 'Withheld identity indicator.'},
                   'table': 'One PowerForm instance with ordered lossless responses and reconciliation '
                            'evidence.'}}

# INSR_R4_SILVER_V1 (parent): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import ParentInputs, parent_inputs_spec  # noqa: E402
R4_PARENT_INPUTS = ParentInputs(parent='clinical_form', dataset='_clinical_form_parent_inputs', keys=('patient_event_key',), inputs=('identity_status', 'person_id'), admit_sql="coalesce(((identity_status = 'resolved')), false)", name=_n('journey_clinical._form_parent_inputs'), parent_name=_n('journey_clinical.form'))
SPECS = list(SPECS) + [parent_inputs_spec(R4_PARENT_INPUTS)]
