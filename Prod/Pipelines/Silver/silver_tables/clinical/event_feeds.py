"""Silver clinical.event_feeds: 16 dataset(s).

- datasets: _events_feed_family_history, _events_feed_presenting_complaint, _events_feed_allergy_intolerance, _events_feed_transfusion, _events_feed_cancer_treatment, _events_feed_condition_stage, _events_feed_endoscopy_finding, _events_feed_condition, _events_feed_procedure, _events_feed_vital_sign, _events_feed_clinical_score, _events_feed_medication_admin ...
- moved verbatim from: 1_silver_spine_and_reference:3588

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_tables._common.journey import (
    _allergy_canonical,
    _cancer_treatment_canonical,
    _cc_procedure_canonical,
    _clinical_score_canonical,
    _coded_finding_typed,
    _condition_diagnosis_canonical,
    _condition_problem_canonical,
    _condition_stage_canonical,
    _date_finding_typed,
    _endobase_procedure_canonical,
    _endoscopy_finding_canonical,
    _family_history_canonical,
    _imaging_exam_canonical,
    _implant_procedure_canonical,
    _maternity_diagnosis_canonical,
    _medication_admin_canonical,
    _medication_dispense_canonical,
    _medication_order_canonical,
    _mill_radiology_exam_canonical,
    _nomen_finding_typed,
    _presenting_complaint_canonical,
    _procedure_canonical,
    _text_finding_typed,
    _theatre_procedure_canonical,
    _transfusion_canonical,
    _vital_sign_canonical,
)
from silver_tables._common.spine_and_reference import (
    _coded_bridge_event_map,
    _declare_event_feed,
    _dynamic_map_projection,
    _event_index_projection,
    _map_projection,
    _transfusion_blood_group_event_map,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


EVENT_FEED_SPECS = {
    'family_history': {
        "fact_table": 'journey_clinical.family_history',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_family_history_canonical(), "family_history",
                "journey_clinical.family_history", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_family_history_canonical(), "_snomed_code", "_snomed_display",
                "http://snomed.info/sct", "family_history", "bronze.map_family_history"),
            _map_projection(_family_history_canonical(), "_icd10_code", "_icd10_display",
                "http://hl7.org/fhir/sid/icd-10", "family_history", "bronze.map_family_history"),
            _map_projection(_family_history_canonical(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "family_history", "bronze.map_family_history"),
        ],
        "s3_axes": [('journey_clinical.family_history', 'condition')],
        "supplement": [('journey_clinical.family_history', 'family_history', 'journey_clinical.family_history', 'condition', 'family_history')],
        "registry_findings": False,
    },
    'presenting_complaint': {
        "fact_table": 'journey_clinical.presenting_complaint',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_presenting_complaint_canonical(), "presenting_complaint",
                "journey_clinical.presenting_complaint", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [('journey_clinical.presenting_complaint', 'complaint')],
        "supplement": [('journey_clinical.presenting_complaint', 'presenting_complaint', 'journey_clinical.presenting_complaint', 'complaint', 'presenting_complaint')],
        "registry_findings": False,
    },
    'allergy_intolerance': {
        "fact_table": 'journey_clinical.allergy_intolerance',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_allergy_canonical(), "allergy_intolerance",
                "journey_clinical.allergy_intolerance", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_allergy_canonical(), "_snomed_code", "_snomed_display",
                "http://snomed.info/sct", "condition", "bronze.map_allergy"),
        ],
        "s3_axes": [('journey_clinical.allergy_intolerance', 'substance')],
        "supplement": [],
        "registry_findings": False,
    },
    'transfusion': {
        "fact_table": 'journey_clinical.transfusion',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_transfusion_canonical(), "transfusion",
                "journey_clinical.transfusion", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_transfusion_canonical(), "product_concept_id", "product_concept_name",
                "http://snomed.info/sct", "device",
                "lookup.bloodtrack_product_group_map:applied"),
            _map_projection(_transfusion_canonical(), "_product_proc_concept_id",
                "_product_proc_concept_name", "http://snomed.info/sct", "procedure",
                "lookup.bloodtrack_product_group_map:applied"),
            _transfusion_blood_group_event_map(),
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'cancer_treatment': {
        "fact_table": 'journey_clinical.cancer_treatment',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_cancer_treatment_canonical(), "cancer_treatment",
                "journey_clinical.cancer_treatment", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_cancer_treatment_canonical(), "_drug_omop_code", "_drug_omop_display",
                "urn:omop:concept_id", "drug",
                "lookup.cancer_treatment_term_map:applied"),
            _map_projection(_cancer_treatment_canonical(), "_procurement_omop_code",
                "_procurement_omop_display", "urn:omop:concept_id", "procedure",
                "bronze.map_cancer_treatment:procurement"),
            _map_projection(_cancer_treatment_canonical(), "_procurement_opcs4_code",
                "_procurement_opcs4_display",
                "http://fhir.hl7.org.uk/CodeSystem/OPCS-4", "procedure",
                "bronze.map_cancer_treatment:procurement"),
            _map_projection(_cancer_treatment_canonical(), "_delivery_omop_code",
                "_delivery_omop_display", "urn:omop:concept_id", "procedure",
                "bronze.map_cancer_treatment:delivery"),
            _map_projection(_cancer_treatment_canonical(), "_delivery_opcs4_code",
                "_delivery_opcs4_display",
                "http://fhir.hl7.org.uk/CodeSystem/OPCS-4", "procedure",
                "bronze.map_cancer_treatment:delivery"),
        ],
        "s3_axes": [('journey_clinical.cancer_treatment', 'drug')],
        "supplement": [],
        "registry_findings": False,
    },
    'condition_stage': {
        "fact_table": 'journey_clinical.condition_stage',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_condition_stage_canonical(), "condition_stage",
                "journey_clinical.condition_stage", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_condition_stage_canonical(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "condition",
                "bronze.map_aria_diagnosis_staging"),
        ],
        "s3_axes": [('journey_clinical.condition_stage', 'stage_diagnosis'), ('journey_clinical.condition_stage', 'stage_group')],
        "supplement": [],
        "registry_findings": False,
    },
    'endoscopy_finding': {
        "fact_table": 'journey_clinical.endoscopy_finding',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_endoscopy_finding_canonical(), "endoscopy_finding",
                "journey_clinical.endoscopy_finding", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_endoscopy_finding_canonical(), "_snomed_code", "_snomed_display",
                "http://snomed.info/sct", "observation",
                "bronze.map_endobase_exam_term:c4"),
            _map_projection(_endoscopy_finding_canonical(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "observation",
                "bronze.map_endobase_exam_term:c4"),
        ],
        "s3_axes": [('journey_clinical.endoscopy_finding', 'finding')],
        "supplement": [],
        "registry_findings": False,
    },
    'condition': {
        "fact_table": 'journey_clinical.condition',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_condition_diagnosis_canonical(), "condition",
                "journey_clinical.condition", "clinical"),
            _event_index_projection(_condition_problem_canonical(), "condition",
                "journey_clinical.condition", "clinical"),
            _event_index_projection(_maternity_diagnosis_canonical(), "condition",
                "journey_clinical.condition", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_condition_diagnosis_canonical(), "_snomed_code", "_snomed_display",
                "http://snomed.info/sct", "condition", "bronze.map_diagnosis"),
            _map_projection(_condition_diagnosis_canonical(), "_icd10_code", "_icd10_display",
                "http://hl7.org/fhir/sid/icd-10", "condition", "bronze.map_diagnosis"),
            _map_projection(_condition_diagnosis_canonical(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "condition", "bronze.map_diagnosis"),
            _map_projection(_condition_problem_canonical(), "_snomed_code", "_snomed_display",
                "http://snomed.info/sct", "condition", "bronze.map_problem"),
            _map_projection(_condition_problem_canonical(), "_icd10_code", "_icd10_display",
                "http://hl7.org/fhir/sid/icd-10", "condition", "bronze.map_problem"),
            _map_projection(_condition_problem_canonical(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "condition", "bronze.map_problem"),
            _map_projection(_maternity_diagnosis_canonical(), "_snomed_code", "_snomed_display",
                "http://snomed.info/sct", "condition",
                "bronze.map_maternity_diagnosis"),
        ],
        "s3_axes": [('journey_clinical.condition', 'condition')],
        "supplement": [('journey_clinical.condition', 'condition', 'journey_clinical.condition', 'condition', 'condition')],
        "registry_findings": False,
    },
    'procedure': {
        "fact_table": 'journey_clinical.procedure',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_procedure_canonical(), "procedure",
                "journey_clinical.procedure", "clinical"),
            _event_index_projection(_implant_procedure_canonical(), "procedure",
                "journey_clinical.procedure", "clinical"),
            _event_index_projection(_theatre_procedure_canonical(), "procedure",
                "journey_clinical.procedure", "clinical"),
            _event_index_projection(_endobase_procedure_canonical(), "procedure",
                "journey_clinical.procedure", "clinical"),
            _event_index_projection(_cc_procedure_canonical(), "procedure",
                "journey_clinical.procedure", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_procedure_canonical(), "_snomed_code", "_snomed_display",
                "http://snomed.info/sct", "procedure", "bronze.map_procedure"),
            _map_projection(_procedure_canonical(), "_opcs4_code", "_opcs4_display",
                "http://fhir.hl7.org.uk/CodeSystem/OPCS-4", "procedure", "bronze.map_procedure"),
            _map_projection(_cc_procedure_canonical(), "_opcs4_code", "_opcs4_display",
                "http://fhir.hl7.org.uk/CodeSystem/OPCS-4", "procedure",
                "bronze.map_critical_care_procedure"),
            _map_projection(_procedure_canonical(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "procedure", "bronze.map_procedure"),
            _map_projection(_implant_procedure_canonical(), "_device_snomed_code", "_device_snomed_display",
                "http://snomed.info/sct", "device", "bronze.map_implant_details"),
        ],
        "s3_axes": [('journey_clinical.procedure', 'procedure'), ('journey_clinical.procedure', 'device')],
        "supplement": [('journey_clinical.procedure', 'procedure', 'journey_clinical.procedure', 'procedure', 'procedure')],
        "registry_findings": False,
    },
    'vital_sign': {
        "fact_table": 'journey_clinical.vital_sign',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_vital_sign_canonical(), "vital_sign",
                "journey_clinical.vital_sign", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_vital_sign_canonical(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "measurement", "bronze.map_numeric_events_or_form"),
        ],
        "s3_axes": [('journey_clinical.vital_sign', 'vital'), ('journey_clinical.vital_sign', 'unit'), ('journey_clinical.vital_sign', 'method'), ('journey_clinical.vital_sign', 'body_site'), ('journey_clinical.vital_sign', 'interpretation')],
        "supplement": [],
        "registry_findings": False,
    },
    'clinical_score': {
        "fact_table": 'journey_clinical.clinical_score',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_clinical_score_canonical(), "clinical_score",
                "journey_clinical.clinical_score", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_clinical_score_canonical(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "observation", "bronze.map_numeric_events_or_form"),
        ],
        "s3_axes": [('journey_clinical.clinical_score', 'score'), ('journey_clinical.clinical_score_component', 'component')],
        "supplement": [],
        "registry_findings": False,
    },
    'medication_admin': {
        "fact_table": 'journey_clinical.medication_admin',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_medication_admin_canonical(), "medication_admin",
                "journey_clinical.medication_admin", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_medication_admin_canonical(), "_snomed_code", "_snomed_display",
                "http://snomed.info/sct", "drug", "bronze.map_med_admin"),
            _map_projection(_medication_admin_canonical(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "drug", "bronze.map_med_admin"),
            _map_projection(_medication_admin_canonical(), "_rxnorm_code", "_rxnorm_display",
                "http://www.nlm.nih.gov/research/umls/rxnorm", "drug",
                "bronze.map_med_admin"),
        ],
        "s3_axes": [('journey_clinical.medication_admin', 'medication')],
        "supplement": [('journey_clinical.medication_admin', 'medication_admin', 'journey_clinical.medication_admin', 'medication', 'medication_admin')],
        "registry_findings": False,
    },
    'medication_order': {
        "fact_table": 'journey_clinical.medication_order',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_medication_order_canonical(), "medication_order",
                "journey_clinical.medication_order", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [('journey_clinical.medication_order', 'medication')],
        "supplement": [('journey_clinical.medication_order', 'medication_order', 'journey_clinical.medication_order', 'medication', 'medication_order')],
        "registry_findings": False,
    },
    'medication_dispense': {
        "fact_table": 'journey_clinical.medication_dispense',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_medication_dispense_canonical(), "medication_dispense",
                "journey_clinical.medication_dispense", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_medication_dispense_canonical(), "_dmd_code", "_dmd_display",
                "http://snomed.info/sct", "drug", "bronze.map_pharmacy_issue"),
        ],
        "s3_axes": [('journey_clinical.medication_dispense', 'medication')],
        "supplement": [('journey_clinical.medication_dispense', 'medication_dispense', 'journey_clinical.medication_dispense', 'medication', 'medication_dispense')],
        "registry_findings": False,
    },
    'clinical_finding': {
        "fact_table": 'journey_clinical.clinical_finding',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_coded_finding_typed(), "clinical_finding",
                "journey_clinical.clinical_finding", "clinical"),
            _event_index_projection(_nomen_finding_typed(), "clinical_finding",
                "journey_clinical.clinical_finding", "clinical"),
            _event_index_projection(_date_finding_typed(), "clinical_finding",
                "journey_clinical.clinical_finding", "clinical"),
            _event_index_projection(_text_finding_typed(), "clinical_finding",
                "journey_clinical.clinical_finding", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _coded_bridge_event_map(),
            _dynamic_map_projection(
                _coded_finding_typed(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "_omop_domain", "bronze.map_coded_events:manual",
                ),
            _dynamic_map_projection(
                _nomen_finding_typed(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "_omop_domain", "bronze.map_nomen_events",
                ),
            _map_projection(
                _nomen_finding_typed(), "_snomed_code", "_snomed_display",
                "http://snomed.info/sct", "observation", "bronze.map_nomen_events",
                ),
            _dynamic_map_projection(
                _text_finding_typed(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "_omop_domain", "bronze.map_text_events:concept",
                ),
            _dynamic_map_projection(
                _text_finding_typed(), "_omop_value_code", "_omop_value_display",
                "urn:omop:concept_id", "_omop_value_domain", "bronze.map_text_events:value",
                ),
        ],
        "s3_axes": [('journey_clinical.clinical_finding', 'finding'), ('journey_clinical.clinical_finding', 'value')],
        "supplement": [],
        "registry_findings": True,
    },
    'imaging_exam': {
        "fact_table": 'journey_clinical.imaging_exam',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_imaging_exam_canonical(), "imaging_exam",
                "journey_clinical.imaging_exam", "clinical"),
            _event_index_projection(_mill_radiology_exam_canonical(), "imaging_exam",
                "journey_clinical.imaging_exam", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [('journey_clinical.imaging_exam', 'exam')],
        "supplement": [],
        "registry_findings": False,
    },
}


for _feed, _spec in EVENT_FEED_SPECS.items():
    _declare_event_feed(_feed, _spec)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_events_feed_allergy_intolerance': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                                  'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                                  'available.',
                                                  'event_datetime': 'Clinical or administrative event time.',
                                                  'event_datetime_local': 'event_datetime in Europe/London '
                                                                          'wall-clock time (GMT/BST). '
                                                                          'Millennium rows take the bronze '
                                                                          '*_LOCAL companions of the same '
                                                                          'source columns and fallback '
                                                                          'order; rows from local-clock '
                                                                          'sources (LUNA, PACS/DICOM, TFC '
                                                                          'LIMS, CCMDS, MSDS, BadgerNet, '
                                                                          'EndoBase, SLAM, iWeb, Datix and '
                                                                          'similar) carry event_datetime '
                                                                          'unchanged. event_datetime is a '
                                                                          'UTC instant for Millennium rows, '
                                                                          'so take local calendar dates and '
                                                                          'clock times from this column.',
                                                  'event_end_datetime': 'Event end when the source supplies '
                                                                        'one.',
                                                  'event_end_datetime_local': 'event_end_datetime in '
                                                                              'Europe/London wall-clock time '
                                                                              '(GMT/BST), by the same rule '
                                                                              'as event_datetime_local; null '
                                                                              'where event_end_datetime is '
                                                                              'null.',
                                                  'event_type': 'Fact kind (condition',
                                                  'fact_category': 'Clinical versus administrative fact.',
                                                  'fact_row_key': 'Deterministic row key inside fact_table.',
                                                  'fact_table': "Typed fact table holding the event's "
                                                                'values.',
                                                  'loaded_at': 'Per-event loaded_at carried unchanged from '
                                                               'the contributing canonical arm through '
                                                               'internal_event_index, then selected from the '
                                                               'event side of the public mapping join. '
                                                               'Source arms have different clock '
                                                               'expressions, including typed nulls; this is '
                                                               "not uniformly one bronze row's write time. "
                                                               'Mapping clocks do not contribute, and '
                                                               'mapping fan-out does not calculate a new '
                                                               'maximum. Later typed-product enrichment, '
                                                               'such as implant attribute clocks or '
                                                               'additional document-history rows, is not '
                                                               'folded back into this event-index clock. No '
                                                               'current Silver refresh timestamp is '
                                                               'substituted.',
                                                  'map_candidate_count': 'Number of candidates for the '
                                                                         'event, coding system and mapping '
                                                                         'source.',
                                                  'map_confidence': 'Mapping confidence in [0,1] where the '
                                                                    'lane measured one.',
                                                  'map_loaded_at': 'loaded_at of the mapping row (null when '
                                                                   'the event has no mapping); the clock '
                                                                   'concept_registry uses for mapped codes.',
                                                  'map_method': 'How the mapping was reached: source_native '
                                                                'or rule for legacy lanes; S3 lookup lanes '
                                                                'retain their explicit method.',
                                                  'map_rule_id': 'Rule or lane identifier that produced this '
                                                                 'candidate.',
                                                  'map_source': 'Governed mapping product that produced the '
                                                                'row.',
                                                  'map_version': 'Mapping content version.',
                                                  'mapped_code': 'Unique valid standard OMOP concept reached '
                                                                 'from exact ICD code semantics; NULL when '
                                                                 'absent or ambiguous.',
                                                  'mapped_coding_system': 'Standard coding system for this '
                                                                          'mapping row; null on the unmapped '
                                                                          'row.',
                                                  'mapped_display': 'Standard display or definition.',
                                                  'patient_event_key': 'Immutable deterministic fact-grain '
                                                                       'event key shared across a mapped '
                                                                       "event's N rows.",
                                                  'patient_event_row_key': 'Deterministic row key (the '
                                                                           'mapping id for mapped rows; a '
                                                                           'namespaced unmapped id '
                                                                           'otherwise).',
                                                  'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                               'available.',
                                                  'record_status': 'Status label carried unchanged from the '
                                                                   'contributing canonical arm through '
                                                                   'internal_event_index and the public '
                                                                   'event-to-mapping left join. It is not a '
                                                                   'Boolean mirror of IS_CURRENT_IN_SOURCE '
                                                                   'or a verbatim closure flag: each arm '
                                                                   'supplies its own active, superseded or '
                                                                   'retracted recipe. Every mapping row for '
                                                                   'an event inherits the event-side status; '
                                                                   'mapping fields do not replace it. The '
                                                                   'input is the canonical event-index row, '
                                                                   'not a promise of equality with a later '
                                                                   "typed public product's history or "
                                                                   'latest-version selection.',
                                                  'source_code': 'Source code as recorded.',
                                                  'source_coding_system': 'The description for the code '
                                                                          'value',
                                                  'source_display': 'Source display as recorded.',
                                                  'source_object': 'Native source entity in plain language',
                                                  'source_row_key': 'Native source record key (BIGINT ids '
                                                                    'rendered as text); the typed native id '
                                                                    'lives on the fact table.',
                                                  'source_system': 'Plain-language source system (Millennium',
                                                  'subject_id_system': 'Identifier system behind '
                                                                       'subject_key.',
                                                  'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                 'source identifier. Not salted, not secret; '
                                                                 'a stable join key across feeds where '
                                                                 'person_id is unresolved.',
                                                  'target_domain': 'Downstream routing domain for this '
                                                                   'mapping.',
                                                  'vip_ind': 'Source VIP indicator carried as data.',
                                                  'withheld_identity_ind': 'Source withheld-identity '
                                                                           'indicator.'},
                                      'table': 'INTERNAL event feed for '
                                               'journey_clinical.allergy_intolerance: one row per admitted '
                                               'event x standard mapping (unmapped events carry one row with '
                                               'a null mapping block), identical to the rows the retired '
                                               'events_patient_event held for this fact table. Read by the '
                                               'OMOP model and the OMOP id registry as a union over all '
                                               'feeds.'},
 '_events_feed_cancer_treatment': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                               'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                               'available.',
                                               'event_datetime': 'Clinical or administrative event time.',
                                               'event_datetime_local': 'event_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST). '
                                                                       'Millennium rows take the bronze '
                                                                       '*_LOCAL companions of the same '
                                                                       'source columns and fallback order; '
                                                                       'rows from local-clock sources (LUNA, '
                                                                       'PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                       'BadgerNet, EndoBase, SLAM, iWeb, '
                                                                       'Datix and similar) carry '
                                                                       'event_datetime unchanged. '
                                                                       'event_datetime is a UTC instant for '
                                                                       'Millennium rows, so take local '
                                                                       'calendar dates and clock times from '
                                                                       'this column.',
                                               'event_end_datetime': 'Event end when the source supplies '
                                                                     'one.',
                                               'event_end_datetime_local': 'event_end_datetime in '
                                                                           'Europe/London wall-clock time '
                                                                           '(GMT/BST), by the same rule as '
                                                                           'event_datetime_local; null where '
                                                                           'event_end_datetime is null.',
                                               'event_type': 'Fact kind (condition',
                                               'fact_category': 'Clinical versus administrative fact.',
                                               'fact_row_key': 'Deterministic row key inside fact_table.',
                                               'fact_table': "Typed fact table holding the event's values.",
                                               'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                            'contributing canonical arm through '
                                                            'internal_event_index, then selected from the '
                                                            'event side of the public mapping join. Source '
                                                            'arms have different clock expressions, '
                                                            'including typed nulls; this is not uniformly '
                                                            "one bronze row's write time. Mapping clocks do "
                                                            'not contribute, and mapping fan-out does not '
                                                            'calculate a new maximum. Later typed-product '
                                                            'enrichment, such as implant attribute clocks or '
                                                            'additional document-history rows, is not folded '
                                                            'back into this event-index clock. No current '
                                                            'Silver refresh timestamp is substituted.',
                                               'map_candidate_count': 'Number of candidates for the event, '
                                                                      'coding system and mapping source.',
                                               'map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                                 'measured one.',
                                               'map_loaded_at': 'loaded_at of the mapping row (null when the '
                                                                'event has no mapping); the clock '
                                                                'concept_registry uses for mapped codes.',
                                               'map_method': 'How the mapping was reached: source_native or '
                                                             'rule for legacy lanes; S3 lookup lanes retain '
                                                             'their explicit method.',
                                               'map_rule_id': 'Rule or lane identifier that produced this '
                                                              'candidate.',
                                               'map_source': 'Governed mapping product that produced the '
                                                             'row.',
                                               'map_version': 'Mapping content version.',
                                               'mapped_code': 'Unique valid standard OMOP concept reached '
                                                              'from exact ICD code semantics; NULL when '
                                                              'absent or ambiguous.',
                                               'mapped_coding_system': 'Standard coding system for this '
                                                                       'mapping row; null on the unmapped '
                                                                       'row.',
                                               'mapped_display': 'Standard display or definition.',
                                               'patient_event_key': 'Immutable deterministic fact-grain '
                                                                    'event key shared across a mapped '
                                                                    "event's N rows.",
                                               'patient_event_row_key': 'Deterministic row key (the mapping '
                                                                        'id for mapped rows; a namespaced '
                                                                        'unmapped id otherwise).',
                                               'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                            'available.',
                                               'record_status': 'Status label carried unchanged from the '
                                                                'contributing canonical arm through '
                                                                'internal_event_index and the public '
                                                                'event-to-mapping left join. It is not a '
                                                                'Boolean mirror of IS_CURRENT_IN_SOURCE or a '
                                                                'verbatim closure flag: each arm supplies '
                                                                'its own active, superseded or retracted '
                                                                'recipe. Every mapping row for an event '
                                                                'inherits the event-side status; mapping '
                                                                'fields do not replace it. The input is the '
                                                                'canonical event-index row, not a promise of '
                                                                'equality with a later typed public '
                                                                "product's history or latest-version "
                                                                'selection.',
                                               'source_code': 'Source code as recorded.',
                                               'source_coding_system': 'The description for the code value',
                                               'source_display': 'Source display as recorded.',
                                               'source_object': 'Native source entity in plain language',
                                               'source_row_key': 'Native source record key (BIGINT ids '
                                                                 'rendered as text); the typed native id '
                                                                 'lives on the fact table.',
                                               'source_system': 'Plain-language source system (Millennium',
                                               'subject_id_system': 'Identifier system behind subject_key.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'target_domain': 'Downstream routing domain for this mapping.',
                                               'vip_ind': 'Source VIP indicator carried as data.',
                                               'withheld_identity_ind': 'Source withheld-identity '
                                                                        'indicator.'},
                                   'table': 'INTERNAL event feed for journey_clinical.cancer_treatment: one '
                                            'row per admitted event x standard mapping (unmapped events '
                                            'carry one row with a null mapping block), identical to the rows '
                                            'the retired events_patient_event held for this fact table. Read '
                                            'by the OMOP model and the OMOP id registry as a union over all '
                                            'feeds.'},
 '_events_feed_clinical_finding': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                               'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                               'available.',
                                               'event_datetime': 'Clinical or administrative event time.',
                                               'event_datetime_local': 'event_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST). '
                                                                       'Millennium rows take the bronze '
                                                                       '*_LOCAL companions of the same '
                                                                       'source columns and fallback order; '
                                                                       'rows from local-clock sources (LUNA, '
                                                                       'PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                       'BadgerNet, EndoBase, SLAM, iWeb, '
                                                                       'Datix and similar) carry '
                                                                       'event_datetime unchanged. '
                                                                       'event_datetime is a UTC instant for '
                                                                       'Millennium rows, so take local '
                                                                       'calendar dates and clock times from '
                                                                       'this column.',
                                               'event_end_datetime': 'Event end when the source supplies '
                                                                     'one.',
                                               'event_end_datetime_local': 'event_end_datetime in '
                                                                           'Europe/London wall-clock time '
                                                                           '(GMT/BST), by the same rule as '
                                                                           'event_datetime_local; null where '
                                                                           'event_end_datetime is null.',
                                               'event_type': 'Fact kind (condition',
                                               'fact_category': 'Clinical versus administrative fact.',
                                               'fact_row_key': 'Deterministic row key inside fact_table.',
                                               'fact_table': "Typed fact table holding the event's values.",
                                               'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                            'contributing canonical arm through '
                                                            'internal_event_index, then selected from the '
                                                            'event side of the public mapping join. Source '
                                                            'arms have different clock expressions, '
                                                            'including typed nulls; this is not uniformly '
                                                            "one bronze row's write time. Mapping clocks do "
                                                            'not contribute, and mapping fan-out does not '
                                                            'calculate a new maximum. Later typed-product '
                                                            'enrichment, such as implant attribute clocks or '
                                                            'additional document-history rows, is not folded '
                                                            'back into this event-index clock. No current '
                                                            'Silver refresh timestamp is substituted.',
                                               'map_candidate_count': 'Number of candidates for the event, '
                                                                      'coding system and mapping source.',
                                               'map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                                 'measured one.',
                                               'map_loaded_at': 'loaded_at of the mapping row (null when the '
                                                                'event has no mapping); the clock '
                                                                'concept_registry uses for mapped codes.',
                                               'map_method': 'How the mapping was reached: source_native or '
                                                             'rule for legacy lanes; S3 lookup lanes retain '
                                                             'their explicit method.',
                                               'map_rule_id': 'Rule or lane identifier that produced this '
                                                              'candidate.',
                                               'map_source': 'Governed mapping product that produced the '
                                                             'row.',
                                               'map_version': 'Mapping content version.',
                                               'mapped_code': 'Unique valid standard OMOP concept reached '
                                                              'from exact ICD code semantics; NULL when '
                                                              'absent or ambiguous.',
                                               'mapped_coding_system': 'Standard coding system for this '
                                                                       'mapping row; null on the unmapped '
                                                                       'row.',
                                               'mapped_display': 'Standard display or definition.',
                                               'patient_event_key': 'Immutable deterministic fact-grain '
                                                                    'event key shared across a mapped '
                                                                    "event's N rows.",
                                               'patient_event_row_key': 'Deterministic row key (the mapping '
                                                                        'id for mapped rows; a namespaced '
                                                                        'unmapped id otherwise).',
                                               'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                            'available.',
                                               'record_status': 'Status label carried unchanged from the '
                                                                'contributing canonical arm through '
                                                                'internal_event_index and the public '
                                                                'event-to-mapping left join. It is not a '
                                                                'Boolean mirror of IS_CURRENT_IN_SOURCE or a '
                                                                'verbatim closure flag: each arm supplies '
                                                                'its own active, superseded or retracted '
                                                                'recipe. Every mapping row for an event '
                                                                'inherits the event-side status; mapping '
                                                                'fields do not replace it. The input is the '
                                                                'canonical event-index row, not a promise of '
                                                                'equality with a later typed public '
                                                                "product's history or latest-version "
                                                                'selection.',
                                               'source_code': 'Source code as recorded.',
                                               'source_coding_system': 'The description for the code value',
                                               'source_display': 'Source display as recorded.',
                                               'source_object': 'Native source entity in plain language',
                                               'source_row_key': 'Native source record key (BIGINT ids '
                                                                 'rendered as text); the typed native id '
                                                                 'lives on the fact table.',
                                               'source_system': 'Plain-language source system (Millennium',
                                               'subject_id_system': 'Identifier system behind subject_key.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'target_domain': 'Downstream routing domain for this mapping.',
                                               'vip_ind': 'Source VIP indicator carried as data.',
                                               'withheld_identity_ind': 'Source withheld-identity '
                                                                        'indicator.'},
                                   'table': 'INTERNAL event feed for journey_clinical.clinical_finding: one '
                                            'row per admitted event x standard mapping (unmapped events '
                                            'carry one row with a null mapping block), identical to the rows '
                                            'the retired events_patient_event held for this fact table. Read '
                                            'by the OMOP model and the OMOP id registry as a union over all '
                                            'feeds.'},
 '_events_feed_clinical_score': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                             'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                             'event_datetime': 'Clinical or administrative event time.',
                                             'event_datetime_local': 'event_datetime in Europe/London '
                                                                     'wall-clock time (GMT/BST). Millennium '
                                                                     'rows take the bronze *_LOCAL '
                                                                     'companions of the same source columns '
                                                                     'and fallback order; rows from '
                                                                     'local-clock sources (LUNA, PACS/DICOM, '
                                                                     'TFC LIMS, CCMDS, MSDS, BadgerNet, '
                                                                     'EndoBase, SLAM, iWeb, Datix and '
                                                                     'similar) carry event_datetime '
                                                                     'unchanged. event_datetime is a UTC '
                                                                     'instant for Millennium rows, so take '
                                                                     'local calendar dates and clock times '
                                                                     'from this column.',
                                             'event_end_datetime': 'Event end when the source supplies one.',
                                             'event_end_datetime_local': 'event_end_datetime in '
                                                                         'Europe/London wall-clock time '
                                                                         '(GMT/BST), by the same rule as '
                                                                         'event_datetime_local; null where '
                                                                         'event_end_datetime is null.',
                                             'event_type': 'Fact kind (condition',
                                             'fact_category': 'Clinical versus administrative fact.',
                                             'fact_row_key': 'Deterministic row key inside fact_table.',
                                             'fact_table': "Typed fact table holding the event's values.",
                                             'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                          'contributing canonical arm through '
                                                          'internal_event_index, then selected from the '
                                                          'event side of the public mapping join. Source '
                                                          'arms have different clock expressions, including '
                                                          'typed nulls; this is not uniformly one bronze '
                                                          "row's write time. Mapping clocks do not "
                                                          'contribute, and mapping fan-out does not '
                                                          'calculate a new maximum. Later typed-product '
                                                          'enrichment, such as implant attribute clocks or '
                                                          'additional document-history rows, is not folded '
                                                          'back into this event-index clock. No current '
                                                          'Silver refresh timestamp is substituted.',
                                             'map_candidate_count': 'Number of candidates for the event, '
                                                                    'coding system and mapping source.',
                                             'map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                               'measured one.',
                                             'map_loaded_at': 'loaded_at of the mapping row (null when the '
                                                              'event has no mapping); the clock '
                                                              'concept_registry uses for mapped codes.',
                                             'map_method': 'How the mapping was reached: source_native or '
                                                           'rule for legacy lanes; S3 lookup lanes retain '
                                                           'their explicit method.',
                                             'map_rule_id': 'Rule or lane identifier that produced this '
                                                            'candidate.',
                                             'map_source': 'Governed mapping product that produced the row.',
                                             'map_version': 'Mapping content version.',
                                             'mapped_code': 'Unique valid standard OMOP concept reached from '
                                                            'exact ICD code semantics; NULL when absent or '
                                                            'ambiguous.',
                                             'mapped_coding_system': 'Standard coding system for this '
                                                                     'mapping row; null on the unmapped row.',
                                             'mapped_display': 'Standard display or definition.',
                                             'patient_event_key': 'Immutable deterministic fact-grain event '
                                                                  "key shared across a mapped event's N "
                                                                  'rows.',
                                             'patient_event_row_key': 'Deterministic row key (the mapping id '
                                                                      'for mapped rows; a namespaced '
                                                                      'unmapped id otherwise).',
                                             'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                          'available.',
                                             'record_status': 'Status label carried unchanged from the '
                                                              'contributing canonical arm through '
                                                              'internal_event_index and the public '
                                                              'event-to-mapping left join. It is not a '
                                                              'Boolean mirror of IS_CURRENT_IN_SOURCE or a '
                                                              'verbatim closure flag: each arm supplies its '
                                                              'own active, superseded or retracted recipe. '
                                                              'Every mapping row for an event inherits the '
                                                              'event-side status; mapping fields do not '
                                                              'replace it. The input is the canonical '
                                                              'event-index row, not a promise of equality '
                                                              "with a later typed public product's history "
                                                              'or latest-version selection.',
                                             'source_code': 'Source code as recorded.',
                                             'source_coding_system': 'The description for the code value',
                                             'source_display': 'Source display as recorded.',
                                             'source_object': 'Native source entity in plain language',
                                             'source_row_key': 'Native source record key (BIGINT ids '
                                                               'rendered as text); the typed native id lives '
                                                               'on the fact table.',
                                             'source_system': 'Plain-language source system (Millennium',
                                             'subject_id_system': 'Identifier system behind subject_key.',
                                             'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                            'identifier. Not salted, not secret; a stable '
                                                            'join key across feeds where person_id is '
                                                            'unresolved.',
                                             'target_domain': 'Downstream routing domain for this mapping.',
                                             'vip_ind': 'Source VIP indicator carried as data.',
                                             'withheld_identity_ind': 'Source withheld-identity indicator.'},
                                 'table': 'INTERNAL event feed for journey_clinical.clinical_score: one row '
                                          'per admitted event x standard mapping (unmapped events carry one '
                                          'row with a null mapping block), identical to the rows the retired '
                                          'events_patient_event held for this fact table. Read by the OMOP '
                                          'model and the OMOP id registry as a union over all feeds.'},
 '_events_feed_condition': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                        'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                        'event_datetime': 'Clinical or administrative event time.',
                                        'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                                'time (GMT/BST). Millennium rows take the '
                                                                'bronze *_LOCAL companions of the same '
                                                                'source columns and fallback order; rows '
                                                                'from local-clock sources (LUNA, PACS/DICOM, '
                                                                'TFC LIMS, CCMDS, MSDS, BadgerNet, EndoBase, '
                                                                'SLAM, iWeb, Datix and similar) carry '
                                                                'event_datetime unchanged. event_datetime is '
                                                                'a UTC instant for Millennium rows, so take '
                                                                'local calendar dates and clock times from '
                                                                'this column.',
                                        'event_end_datetime': 'Event end when the source supplies one.',
                                        'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                    'wall-clock time (GMT/BST), by the same '
                                                                    'rule as event_datetime_local; null '
                                                                    'where event_end_datetime is null.',
                                        'event_type': 'Fact kind (condition',
                                        'fact_category': 'Clinical versus administrative fact.',
                                        'fact_row_key': 'Deterministic row key inside fact_table.',
                                        'fact_table': "Typed fact table holding the event's values.",
                                        'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                     'contributing canonical arm through '
                                                     'internal_event_index, then selected from the event '
                                                     'side of the public mapping join. Source arms have '
                                                     'different clock expressions, including typed nulls; '
                                                     "this is not uniformly one bronze row's write time. "
                                                     'Mapping clocks do not contribute, and mapping fan-out '
                                                     'does not calculate a new maximum. Later typed-product '
                                                     'enrichment, such as implant attribute clocks or '
                                                     'additional document-history rows, is not folded back '
                                                     'into this event-index clock. No current Silver refresh '
                                                     'timestamp is substituted.',
                                        'map_candidate_count': 'Number of candidates for the event, coding '
                                                               'system and mapping source.',
                                        'map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                          'measured one.',
                                        'map_loaded_at': 'loaded_at of the mapping row (null when the event '
                                                         'has no mapping); the clock concept_registry uses '
                                                         'for mapped codes.',
                                        'map_method': 'How the mapping was reached: source_native or rule '
                                                      'for legacy lanes; S3 lookup lanes retain their '
                                                      'explicit method.',
                                        'map_rule_id': 'Rule or lane identifier that produced this '
                                                       'candidate.',
                                        'map_source': 'Governed mapping product that produced the row.',
                                        'map_version': 'Mapping content version.',
                                        'mapped_code': 'Unique valid standard OMOP concept reached from '
                                                       'exact ICD code semantics; NULL when absent or '
                                                       'ambiguous.',
                                        'mapped_coding_system': 'Standard coding system for this mapping '
                                                                'row; null on the unmapped row.',
                                        'mapped_display': 'Standard display or definition.',
                                        'patient_event_key': 'Immutable deterministic fact-grain event key '
                                                             "shared across a mapped event's N rows.",
                                        'patient_event_row_key': 'Deterministic row key (the mapping id for '
                                                                 'mapped rows; a namespaced unmapped id '
                                                                 'otherwise).',
                                        'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                     'available.',
                                        'record_status': 'Status label carried unchanged from the '
                                                         'contributing canonical arm through '
                                                         'internal_event_index and the public '
                                                         'event-to-mapping left join. It is not a Boolean '
                                                         'mirror of IS_CURRENT_IN_SOURCE or a verbatim '
                                                         'closure flag: each arm supplies its own active, '
                                                         'superseded or retracted recipe. Every mapping row '
                                                         'for an event inherits the event-side status; '
                                                         'mapping fields do not replace it. The input is the '
                                                         'canonical event-index row, not a promise of '
                                                         "equality with a later typed public product's "
                                                         'history or latest-version selection.',
                                        'source_code': 'Source code as recorded.',
                                        'source_coding_system': 'The description for the code value',
                                        'source_display': 'Source display as recorded.',
                                        'source_object': 'Native source entity in plain language',
                                        'source_row_key': 'Native source record key (BIGINT ids rendered as '
                                                          'text); the typed native id lives on the fact '
                                                          'table.',
                                        'source_system': 'Plain-language source system (Millennium',
                                        'subject_id_system': 'Identifier system behind subject_key.',
                                        'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                       'identifier. Not salted, not secret; a stable join '
                                                       'key across feeds where person_id is unresolved.',
                                        'target_domain': 'Downstream routing domain for this mapping.',
                                        'vip_ind': 'Source VIP indicator carried as data.',
                                        'withheld_identity_ind': 'Source withheld-identity indicator.'},
                            'table': 'INTERNAL event feed for journey_clinical.condition: one row per '
                                     'admitted event x standard mapping (unmapped events carry one row with '
                                     'a null mapping block), identical to the rows the retired '
                                     'events_patient_event held for this fact table. Read by the OMOP model '
                                     'and the OMOP id registry as a union over all feeds.'},
 '_events_feed_condition_stage': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                              'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                              'available.',
                                              'event_datetime': 'Clinical or administrative event time.',
                                              'event_datetime_local': 'event_datetime in Europe/London '
                                                                      'wall-clock time (GMT/BST). Millennium '
                                                                      'rows take the bronze *_LOCAL '
                                                                      'companions of the same source columns '
                                                                      'and fallback order; rows from '
                                                                      'local-clock sources (LUNA, '
                                                                      'PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                      'BadgerNet, EndoBase, SLAM, iWeb, '
                                                                      'Datix and similar) carry '
                                                                      'event_datetime unchanged. '
                                                                      'event_datetime is a UTC instant for '
                                                                      'Millennium rows, so take local '
                                                                      'calendar dates and clock times from '
                                                                      'this column.',
                                              'event_end_datetime': 'Event end when the source supplies one.',
                                              'event_end_datetime_local': 'event_end_datetime in '
                                                                          'Europe/London wall-clock time '
                                                                          '(GMT/BST), by the same rule as '
                                                                          'event_datetime_local; null where '
                                                                          'event_end_datetime is null.',
                                              'event_type': 'Fact kind (condition',
                                              'fact_category': 'Clinical versus administrative fact.',
                                              'fact_row_key': 'Deterministic row key inside fact_table.',
                                              'fact_table': "Typed fact table holding the event's values.",
                                              'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                           'contributing canonical arm through '
                                                           'internal_event_index, then selected from the '
                                                           'event side of the public mapping join. Source '
                                                           'arms have different clock expressions, including '
                                                           'typed nulls; this is not uniformly one bronze '
                                                           "row's write time. Mapping clocks do not "
                                                           'contribute, and mapping fan-out does not '
                                                           'calculate a new maximum. Later typed-product '
                                                           'enrichment, such as implant attribute clocks or '
                                                           'additional document-history rows, is not folded '
                                                           'back into this event-index clock. No current '
                                                           'Silver refresh timestamp is substituted.',
                                              'map_candidate_count': 'Number of candidates for the event, '
                                                                     'coding system and mapping source.',
                                              'map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                                'measured one.',
                                              'map_loaded_at': 'loaded_at of the mapping row (null when the '
                                                               'event has no mapping); the clock '
                                                               'concept_registry uses for mapped codes.',
                                              'map_method': 'How the mapping was reached: source_native or '
                                                            'rule for legacy lanes; S3 lookup lanes retain '
                                                            'their explicit method.',
                                              'map_rule_id': 'Rule or lane identifier that produced this '
                                                             'candidate.',
                                              'map_source': 'Governed mapping product that produced the row.',
                                              'map_version': 'Mapping content version.',
                                              'mapped_code': 'Unique valid standard OMOP concept reached '
                                                             'from exact ICD code semantics; NULL when '
                                                             'absent or ambiguous.',
                                              'mapped_coding_system': 'Standard coding system for this '
                                                                      'mapping row; null on the unmapped '
                                                                      'row.',
                                              'mapped_display': 'Standard display or definition.',
                                              'patient_event_key': 'Immutable deterministic fact-grain event '
                                                                   "key shared across a mapped event's N "
                                                                   'rows.',
                                              'patient_event_row_key': 'Deterministic row key (the mapping '
                                                                       'id for mapped rows; a namespaced '
                                                                       'unmapped id otherwise).',
                                              'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                           'available.',
                                              'record_status': 'Status label carried unchanged from the '
                                                               'contributing canonical arm through '
                                                               'internal_event_index and the public '
                                                               'event-to-mapping left join. It is not a '
                                                               'Boolean mirror of IS_CURRENT_IN_SOURCE or a '
                                                               'verbatim closure flag: each arm supplies its '
                                                               'own active, superseded or retracted recipe. '
                                                               'Every mapping row for an event inherits the '
                                                               'event-side status; mapping fields do not '
                                                               'replace it. The input is the canonical '
                                                               'event-index row, not a promise of equality '
                                                               "with a later typed public product's history "
                                                               'or latest-version selection.',
                                              'source_code': 'Source code as recorded.',
                                              'source_coding_system': 'The description for the code value',
                                              'source_display': 'Source display as recorded.',
                                              'source_object': 'Native source entity in plain language',
                                              'source_row_key': 'Native source record key (BIGINT ids '
                                                                'rendered as text); the typed native id '
                                                                'lives on the fact table.',
                                              'source_system': 'Plain-language source system (Millennium',
                                              'subject_id_system': 'Identifier system behind subject_key.',
                                              'subject_key': 'Deterministic SHA-256 over the strongest '
                                                             'source identifier. Not salted, not secret; a '
                                                             'stable join key across feeds where person_id '
                                                             'is unresolved.',
                                              'target_domain': 'Downstream routing domain for this mapping.',
                                              'vip_ind': 'Source VIP indicator carried as data.',
                                              'withheld_identity_ind': 'Source withheld-identity indicator.'},
                                  'table': 'INTERNAL event feed for journey_clinical.condition_stage: one '
                                           'row per admitted event x standard mapping (unmapped events carry '
                                           'one row with a null mapping block), identical to the rows the '
                                           'retired events_patient_event held for this fact table. Read by '
                                           'the OMOP model and the OMOP id registry as a union over all '
                                           'feeds.'},
 '_events_feed_endoscopy_finding': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                                'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                                'available.',
                                                'event_datetime': 'Clinical or administrative event time.',
                                                'event_datetime_local': 'event_datetime in Europe/London '
                                                                        'wall-clock time (GMT/BST). '
                                                                        'Millennium rows take the bronze '
                                                                        '*_LOCAL companions of the same '
                                                                        'source columns and fallback order; '
                                                                        'rows from local-clock sources '
                                                                        '(LUNA, PACS/DICOM, TFC LIMS, CCMDS, '
                                                                        'MSDS, BadgerNet, EndoBase, SLAM, '
                                                                        'iWeb, Datix and similar) carry '
                                                                        'event_datetime unchanged. '
                                                                        'event_datetime is a UTC instant for '
                                                                        'Millennium rows, so take local '
                                                                        'calendar dates and clock times from '
                                                                        'this column.',
                                                'event_end_datetime': 'Event end when the source supplies '
                                                                      'one.',
                                                'event_end_datetime_local': 'event_end_datetime in '
                                                                            'Europe/London wall-clock time '
                                                                            '(GMT/BST), by the same rule as '
                                                                            'event_datetime_local; null '
                                                                            'where event_end_datetime is '
                                                                            'null.',
                                                'event_type': 'Fact kind (condition',
                                                'fact_category': 'Clinical versus administrative fact.',
                                                'fact_row_key': 'Deterministic row key inside fact_table.',
                                                'fact_table': "Typed fact table holding the event's values.",
                                                'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                             'contributing canonical arm through '
                                                             'internal_event_index, then selected from the '
                                                             'event side of the public mapping join. Source '
                                                             'arms have different clock expressions, '
                                                             'including typed nulls; this is not uniformly '
                                                             "one bronze row's write time. Mapping clocks do "
                                                             'not contribute, and mapping fan-out does not '
                                                             'calculate a new maximum. Later typed-product '
                                                             'enrichment, such as implant attribute clocks '
                                                             'or additional document-history rows, is not '
                                                             'folded back into this event-index clock. No '
                                                             'current Silver refresh timestamp is '
                                                             'substituted.',
                                                'map_candidate_count': 'Number of candidates for the event, '
                                                                       'coding system and mapping source.',
                                                'map_confidence': 'Mapping confidence in [0,1] where the '
                                                                  'lane measured one.',
                                                'map_loaded_at': 'loaded_at of the mapping row (null when '
                                                                 'the event has no mapping); the clock '
                                                                 'concept_registry uses for mapped codes.',
                                                'map_method': 'How the mapping was reached: source_native or '
                                                              'rule for legacy lanes; S3 lookup lanes retain '
                                                              'their explicit method.',
                                                'map_rule_id': 'Rule or lane identifier that produced this '
                                                               'candidate.',
                                                'map_source': 'Governed mapping product that produced the '
                                                              'row.',
                                                'map_version': 'Mapping content version.',
                                                'mapped_code': 'Unique valid standard OMOP concept reached '
                                                               'from exact ICD code semantics; NULL when '
                                                               'absent or ambiguous.',
                                                'mapped_coding_system': 'Standard coding system for this '
                                                                        'mapping row; null on the unmapped '
                                                                        'row.',
                                                'mapped_display': 'Standard display or definition.',
                                                'patient_event_key': 'Immutable deterministic fact-grain '
                                                                     'event key shared across a mapped '
                                                                     "event's N rows.",
                                                'patient_event_row_key': 'Deterministic row key (the mapping '
                                                                         'id for mapped rows; a namespaced '
                                                                         'unmapped id otherwise).',
                                                'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                             'available.',
                                                'record_status': 'Status label carried unchanged from the '
                                                                 'contributing canonical arm through '
                                                                 'internal_event_index and the public '
                                                                 'event-to-mapping left join. It is not a '
                                                                 'Boolean mirror of IS_CURRENT_IN_SOURCE or '
                                                                 'a verbatim closure flag: each arm supplies '
                                                                 'its own active, superseded or retracted '
                                                                 'recipe. Every mapping row for an event '
                                                                 'inherits the event-side status; mapping '
                                                                 'fields do not replace it. The input is the '
                                                                 'canonical event-index row, not a promise '
                                                                 'of equality with a later typed public '
                                                                 "product's history or latest-version "
                                                                 'selection.',
                                                'source_code': 'Source code as recorded.',
                                                'source_coding_system': 'The description for the code value',
                                                'source_display': 'Source display as recorded.',
                                                'source_object': 'Native source entity in plain language',
                                                'source_row_key': 'Native source record key (BIGINT ids '
                                                                  'rendered as text); the typed native id '
                                                                  'lives on the fact table.',
                                                'source_system': 'Plain-language source system (Millennium',
                                                'subject_id_system': 'Identifier system behind subject_key.',
                                                'subject_key': 'Deterministic SHA-256 over the strongest '
                                                               'source identifier. Not salted, not secret; a '
                                                               'stable join key across feeds where person_id '
                                                               'is unresolved.',
                                                'target_domain': 'Downstream routing domain for this '
                                                                 'mapping.',
                                                'vip_ind': 'Source VIP indicator carried as data.',
                                                'withheld_identity_ind': 'Source withheld-identity '
                                                                         'indicator.'},
                                    'table': 'INTERNAL event feed for journey_clinical.endoscopy_finding: '
                                             'one row per admitted event x standard mapping (unmapped events '
                                             'carry one row with a null mapping block), identical to the '
                                             'rows the retired events_patient_event held for this fact '
                                             'table. Read by the OMOP model and the OMOP id registry as a '
                                             'union over all feeds.'},
 '_events_feed_family_history': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                             'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                             'event_datetime': 'Clinical or administrative event time.',
                                             'event_datetime_local': 'event_datetime in Europe/London '
                                                                     'wall-clock time (GMT/BST). Millennium '
                                                                     'rows take the bronze *_LOCAL '
                                                                     'companions of the same source columns '
                                                                     'and fallback order; rows from '
                                                                     'local-clock sources (LUNA, PACS/DICOM, '
                                                                     'TFC LIMS, CCMDS, MSDS, BadgerNet, '
                                                                     'EndoBase, SLAM, iWeb, Datix and '
                                                                     'similar) carry event_datetime '
                                                                     'unchanged. event_datetime is a UTC '
                                                                     'instant for Millennium rows, so take '
                                                                     'local calendar dates and clock times '
                                                                     'from this column.',
                                             'event_end_datetime': 'Event end when the source supplies one.',
                                             'event_end_datetime_local': 'event_end_datetime in '
                                                                         'Europe/London wall-clock time '
                                                                         '(GMT/BST), by the same rule as '
                                                                         'event_datetime_local; null where '
                                                                         'event_end_datetime is null.',
                                             'event_type': 'Fact kind (condition',
                                             'fact_category': 'Clinical versus administrative fact.',
                                             'fact_row_key': 'Deterministic row key inside fact_table.',
                                             'fact_table': "Typed fact table holding the event's values.",
                                             'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                          'contributing canonical arm through '
                                                          'internal_event_index, then selected from the '
                                                          'event side of the public mapping join. Source '
                                                          'arms have different clock expressions, including '
                                                          'typed nulls; this is not uniformly one bronze '
                                                          "row's write time. Mapping clocks do not "
                                                          'contribute, and mapping fan-out does not '
                                                          'calculate a new maximum. Later typed-product '
                                                          'enrichment, such as implant attribute clocks or '
                                                          'additional document-history rows, is not folded '
                                                          'back into this event-index clock. No current '
                                                          'Silver refresh timestamp is substituted.',
                                             'map_candidate_count': 'Number of candidates for the event, '
                                                                    'coding system and mapping source.',
                                             'map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                               'measured one.',
                                             'map_loaded_at': 'loaded_at of the mapping row (null when the '
                                                              'event has no mapping); the clock '
                                                              'concept_registry uses for mapped codes.',
                                             'map_method': 'How the mapping was reached: source_native or '
                                                           'rule for legacy lanes; S3 lookup lanes retain '
                                                           'their explicit method.',
                                             'map_rule_id': 'Rule or lane identifier that produced this '
                                                            'candidate.',
                                             'map_source': 'Governed mapping product that produced the row.',
                                             'map_version': 'Mapping content version.',
                                             'mapped_code': 'Unique valid standard OMOP concept reached from '
                                                            'exact ICD code semantics; NULL when absent or '
                                                            'ambiguous.',
                                             'mapped_coding_system': 'Standard coding system for this '
                                                                     'mapping row; null on the unmapped row.',
                                             'mapped_display': 'Standard display or definition.',
                                             'patient_event_key': 'Immutable deterministic fact-grain event '
                                                                  "key shared across a mapped event's N "
                                                                  'rows.',
                                             'patient_event_row_key': 'Deterministic row key (the mapping id '
                                                                      'for mapped rows; a namespaced '
                                                                      'unmapped id otherwise).',
                                             'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                          'available.',
                                             'record_status': 'Status label carried unchanged from the '
                                                              'contributing canonical arm through '
                                                              'internal_event_index and the public '
                                                              'event-to-mapping left join. It is not a '
                                                              'Boolean mirror of IS_CURRENT_IN_SOURCE or a '
                                                              'verbatim closure flag: each arm supplies its '
                                                              'own active, superseded or retracted recipe. '
                                                              'Every mapping row for an event inherits the '
                                                              'event-side status; mapping fields do not '
                                                              'replace it. The input is the canonical '
                                                              'event-index row, not a promise of equality '
                                                              "with a later typed public product's history "
                                                              'or latest-version selection.',
                                             'source_code': 'Source code as recorded.',
                                             'source_coding_system': 'The description for the code value',
                                             'source_display': 'Source display as recorded.',
                                             'source_object': 'Native source entity in plain language',
                                             'source_row_key': 'Native source record key (BIGINT ids '
                                                               'rendered as text); the typed native id lives '
                                                               'on the fact table.',
                                             'source_system': 'Plain-language source system (Millennium',
                                             'subject_id_system': 'Identifier system behind subject_key.',
                                             'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                            'identifier. Not salted, not secret; a stable '
                                                            'join key across feeds where person_id is '
                                                            'unresolved.',
                                             'target_domain': 'Downstream routing domain for this mapping.',
                                             'vip_ind': 'Source VIP indicator carried as data.',
                                             'withheld_identity_ind': 'Source withheld-identity indicator.'},
                                 'table': 'INTERNAL event feed for journey_clinical.family_history: one row '
                                          'per admitted event x standard mapping (unmapped events carry one '
                                          'row with a null mapping block), identical to the rows the retired '
                                          'events_patient_event held for this fact table. Read by the OMOP '
                                          'model and the OMOP id registry as a union over all feeds.'},
 '_events_feed_imaging_exam': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                           'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                           'event_datetime': 'Clinical or administrative event time.',
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
                                           'event_end_datetime': 'Event end when the source supplies one.',
                                           'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST), by the '
                                                                       'same rule as event_datetime_local; '
                                                                       'null where event_end_datetime is '
                                                                       'null.',
                                           'event_type': 'Fact kind (condition',
                                           'fact_category': 'Clinical versus administrative fact.',
                                           'fact_row_key': 'Deterministic row key inside fact_table.',
                                           'fact_table': "Typed fact table holding the event's values.",
                                           'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                        'contributing canonical arm through '
                                                        'internal_event_index, then selected from the event '
                                                        'side of the public mapping join. Source arms have '
                                                        'different clock expressions, including typed nulls; '
                                                        "this is not uniformly one bronze row's write time. "
                                                        'Mapping clocks do not contribute, and mapping '
                                                        'fan-out does not calculate a new maximum. Later '
                                                        'typed-product enrichment, such as implant attribute '
                                                        'clocks or additional document-history rows, is not '
                                                        'folded back into this event-index clock. No current '
                                                        'Silver refresh timestamp is substituted.',
                                           'map_candidate_count': 'Number of candidates for the event, '
                                                                  'coding system and mapping source.',
                                           'map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                             'measured one.',
                                           'map_loaded_at': 'loaded_at of the mapping row (null when the '
                                                            'event has no mapping); the clock '
                                                            'concept_registry uses for mapped codes.',
                                           'map_method': 'How the mapping was reached: source_native or rule '
                                                         'for legacy lanes; S3 lookup lanes retain their '
                                                         'explicit method.',
                                           'map_rule_id': 'Rule or lane identifier that produced this '
                                                          'candidate.',
                                           'map_source': 'Governed mapping product that produced the row.',
                                           'map_version': 'Mapping content version.',
                                           'mapped_code': 'Unique valid standard OMOP concept reached from '
                                                          'exact ICD code semantics; NULL when absent or '
                                                          'ambiguous.',
                                           'mapped_coding_system': 'Standard coding system for this mapping '
                                                                   'row; null on the unmapped row.',
                                           'mapped_display': 'Standard display or definition.',
                                           'patient_event_key': 'Immutable deterministic fact-grain event '
                                                                "key shared across a mapped event's N rows.",
                                           'patient_event_row_key': 'Deterministic row key (the mapping id '
                                                                    'for mapped rows; a namespaced unmapped '
                                                                    'id otherwise).',
                                           'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                        'available.',
                                           'record_status': 'Status label carried unchanged from the '
                                                            'contributing canonical arm through '
                                                            'internal_event_index and the public '
                                                            'event-to-mapping left join. It is not a Boolean '
                                                            'mirror of IS_CURRENT_IN_SOURCE or a verbatim '
                                                            'closure flag: each arm supplies its own active, '
                                                            'superseded or retracted recipe. Every mapping '
                                                            'row for an event inherits the event-side '
                                                            'status; mapping fields do not replace it. The '
                                                            'input is the canonical event-index row, not a '
                                                            'promise of equality with a later typed public '
                                                            "product's history or latest-version selection.",
                                           'source_code': 'Source code as recorded.',
                                           'source_coding_system': 'The description for the code value',
                                           'source_display': 'Source display as recorded.',
                                           'source_object': 'Native source entity in plain language',
                                           'source_row_key': 'Native source record key (BIGINT ids rendered '
                                                             'as text); the typed native id lives on the '
                                                             'fact table.',
                                           'source_system': 'Plain-language source system (Millennium',
                                           'subject_id_system': 'Identifier system behind subject_key.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'target_domain': 'Downstream routing domain for this mapping.',
                                           'vip_ind': 'Source VIP indicator carried as data.',
                                           'withheld_identity_ind': 'Source withheld-identity indicator.'},
                               'table': 'INTERNAL event feed for journey_clinical.imaging_exam: one row per '
                                        'admitted event x standard mapping (unmapped events carry one row '
                                        'with a null mapping block), identical to the rows the retired '
                                        'events_patient_event held for this fact table. Read by the OMOP '
                                        'model and the OMOP id registry as a union over all feeds.'},
 '_events_feed_medication_admin': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                               'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                               'available.',
                                               'event_datetime': 'Clinical or administrative event time.',
                                               'event_datetime_local': 'event_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST). '
                                                                       'Millennium rows take the bronze '
                                                                       '*_LOCAL companions of the same '
                                                                       'source columns and fallback order; '
                                                                       'rows from local-clock sources (LUNA, '
                                                                       'PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                       'BadgerNet, EndoBase, SLAM, iWeb, '
                                                                       'Datix and similar) carry '
                                                                       'event_datetime unchanged. '
                                                                       'event_datetime is a UTC instant for '
                                                                       'Millennium rows, so take local '
                                                                       'calendar dates and clock times from '
                                                                       'this column.',
                                               'event_end_datetime': 'Event end when the source supplies '
                                                                     'one.',
                                               'event_end_datetime_local': 'event_end_datetime in '
                                                                           'Europe/London wall-clock time '
                                                                           '(GMT/BST), by the same rule as '
                                                                           'event_datetime_local; null where '
                                                                           'event_end_datetime is null.',
                                               'event_type': 'Fact kind (condition',
                                               'fact_category': 'Clinical versus administrative fact.',
                                               'fact_row_key': 'Deterministic row key inside fact_table.',
                                               'fact_table': "Typed fact table holding the event's values.",
                                               'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                            'contributing canonical arm through '
                                                            'internal_event_index, then selected from the '
                                                            'event side of the public mapping join. Source '
                                                            'arms have different clock expressions, '
                                                            'including typed nulls; this is not uniformly '
                                                            "one bronze row's write time. Mapping clocks do "
                                                            'not contribute, and mapping fan-out does not '
                                                            'calculate a new maximum. Later typed-product '
                                                            'enrichment, such as implant attribute clocks or '
                                                            'additional document-history rows, is not folded '
                                                            'back into this event-index clock. No current '
                                                            'Silver refresh timestamp is substituted.',
                                               'map_candidate_count': 'Number of candidates for the event, '
                                                                      'coding system and mapping source.',
                                               'map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                                 'measured one.',
                                               'map_loaded_at': 'loaded_at of the mapping row (null when the '
                                                                'event has no mapping); the clock '
                                                                'concept_registry uses for mapped codes.',
                                               'map_method': 'How the mapping was reached: source_native or '
                                                             'rule for legacy lanes; S3 lookup lanes retain '
                                                             'their explicit method.',
                                               'map_rule_id': 'Rule or lane identifier that produced this '
                                                              'candidate.',
                                               'map_source': 'Governed mapping product that produced the '
                                                             'row.',
                                               'map_version': 'Mapping content version.',
                                               'mapped_code': 'Unique valid standard OMOP concept reached '
                                                              'from exact ICD code semantics; NULL when '
                                                              'absent or ambiguous.',
                                               'mapped_coding_system': 'Standard coding system for this '
                                                                       'mapping row; null on the unmapped '
                                                                       'row.',
                                               'mapped_display': 'Standard display or definition.',
                                               'patient_event_key': 'Immutable deterministic fact-grain '
                                                                    'event key shared across a mapped '
                                                                    "event's N rows.",
                                               'patient_event_row_key': 'Deterministic row key (the mapping '
                                                                        'id for mapped rows; a namespaced '
                                                                        'unmapped id otherwise).',
                                               'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                            'available.',
                                               'record_status': 'Status label carried unchanged from the '
                                                                'contributing canonical arm through '
                                                                'internal_event_index and the public '
                                                                'event-to-mapping left join. It is not a '
                                                                'Boolean mirror of IS_CURRENT_IN_SOURCE or a '
                                                                'verbatim closure flag: each arm supplies '
                                                                'its own active, superseded or retracted '
                                                                'recipe. Every mapping row for an event '
                                                                'inherits the event-side status; mapping '
                                                                'fields do not replace it. The input is the '
                                                                'canonical event-index row, not a promise of '
                                                                'equality with a later typed public '
                                                                "product's history or latest-version "
                                                                'selection.',
                                               'source_code': 'Source code as recorded.',
                                               'source_coding_system': 'The description for the code value',
                                               'source_display': 'Source display as recorded.',
                                               'source_object': 'Native source entity in plain language',
                                               'source_row_key': 'Native source record key (BIGINT ids '
                                                                 'rendered as text); the typed native id '
                                                                 'lives on the fact table.',
                                               'source_system': 'Plain-language source system (Millennium',
                                               'subject_id_system': 'Identifier system behind subject_key.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'target_domain': 'Downstream routing domain for this mapping.',
                                               'vip_ind': 'Source VIP indicator carried as data.',
                                               'withheld_identity_ind': 'Source withheld-identity '
                                                                        'indicator.'},
                                   'table': 'INTERNAL event feed for journey_clinical.medication_admin: one '
                                            'row per admitted event x standard mapping (unmapped events '
                                            'carry one row with a null mapping block), identical to the rows '
                                            'the retired events_patient_event held for this fact table. Read '
                                            'by the OMOP model and the OMOP id registry as a union over all '
                                            'feeds.'},
 '_events_feed_medication_dispense': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                                  'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                                  'available.',
                                                  'event_datetime': 'Clinical or administrative event time.',
                                                  'event_datetime_local': 'event_datetime in Europe/London '
                                                                          'wall-clock time (GMT/BST). '
                                                                          'Millennium rows take the bronze '
                                                                          '*_LOCAL companions of the same '
                                                                          'source columns and fallback '
                                                                          'order; rows from local-clock '
                                                                          'sources (LUNA, PACS/DICOM, TFC '
                                                                          'LIMS, CCMDS, MSDS, BadgerNet, '
                                                                          'EndoBase, SLAM, iWeb, Datix and '
                                                                          'similar) carry event_datetime '
                                                                          'unchanged. event_datetime is a '
                                                                          'UTC instant for Millennium rows, '
                                                                          'so take local calendar dates and '
                                                                          'clock times from this column.',
                                                  'event_end_datetime': 'Event end when the source supplies '
                                                                        'one.',
                                                  'event_end_datetime_local': 'event_end_datetime in '
                                                                              'Europe/London wall-clock time '
                                                                              '(GMT/BST), by the same rule '
                                                                              'as event_datetime_local; null '
                                                                              'where event_end_datetime is '
                                                                              'null.',
                                                  'event_type': 'Fact kind (condition',
                                                  'fact_category': 'Clinical versus administrative fact.',
                                                  'fact_row_key': 'Deterministic row key inside fact_table.',
                                                  'fact_table': "Typed fact table holding the event's "
                                                                'values.',
                                                  'loaded_at': 'Per-event loaded_at carried unchanged from '
                                                               'the contributing canonical arm through '
                                                               'internal_event_index, then selected from the '
                                                               'event side of the public mapping join. '
                                                               'Source arms have different clock '
                                                               'expressions, including typed nulls; this is '
                                                               "not uniformly one bronze row's write time. "
                                                               'Mapping clocks do not contribute, and '
                                                               'mapping fan-out does not calculate a new '
                                                               'maximum. Later typed-product enrichment, '
                                                               'such as implant attribute clocks or '
                                                               'additional document-history rows, is not '
                                                               'folded back into this event-index clock. No '
                                                               'current Silver refresh timestamp is '
                                                               'substituted.',
                                                  'map_candidate_count': 'Number of candidates for the '
                                                                         'event, coding system and mapping '
                                                                         'source.',
                                                  'map_confidence': 'Mapping confidence in [0,1] where the '
                                                                    'lane measured one.',
                                                  'map_loaded_at': 'loaded_at of the mapping row (null when '
                                                                   'the event has no mapping); the clock '
                                                                   'concept_registry uses for mapped codes.',
                                                  'map_method': 'How the mapping was reached: source_native '
                                                                'or rule for legacy lanes; S3 lookup lanes '
                                                                'retain their explicit method.',
                                                  'map_rule_id': 'Rule or lane identifier that produced this '
                                                                 'candidate.',
                                                  'map_source': 'Governed mapping product that produced the '
                                                                'row.',
                                                  'map_version': 'Mapping content version.',
                                                  'mapped_code': 'Unique valid standard OMOP concept reached '
                                                                 'from exact ICD code semantics; NULL when '
                                                                 'absent or ambiguous.',
                                                  'mapped_coding_system': 'Standard coding system for this '
                                                                          'mapping row; null on the unmapped '
                                                                          'row.',
                                                  'mapped_display': 'Standard display or definition.',
                                                  'patient_event_key': 'Immutable deterministic fact-grain '
                                                                       'event key shared across a mapped '
                                                                       "event's N rows.",
                                                  'patient_event_row_key': 'Deterministic row key (the '
                                                                           'mapping id for mapped rows; a '
                                                                           'namespaced unmapped id '
                                                                           'otherwise).',
                                                  'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                               'available.',
                                                  'record_status': 'Status label carried unchanged from the '
                                                                   'contributing canonical arm through '
                                                                   'internal_event_index and the public '
                                                                   'event-to-mapping left join. It is not a '
                                                                   'Boolean mirror of IS_CURRENT_IN_SOURCE '
                                                                   'or a verbatim closure flag: each arm '
                                                                   'supplies its own active, superseded or '
                                                                   'retracted recipe. Every mapping row for '
                                                                   'an event inherits the event-side status; '
                                                                   'mapping fields do not replace it. The '
                                                                   'input is the canonical event-index row, '
                                                                   'not a promise of equality with a later '
                                                                   "typed public product's history or "
                                                                   'latest-version selection.',
                                                  'source_code': 'Source code as recorded.',
                                                  'source_coding_system': 'The description for the code '
                                                                          'value',
                                                  'source_display': 'Source display as recorded.',
                                                  'source_object': 'Native source entity in plain language',
                                                  'source_row_key': 'Native source record key (BIGINT ids '
                                                                    'rendered as text); the typed native id '
                                                                    'lives on the fact table.',
                                                  'source_system': 'Plain-language source system (Millennium',
                                                  'subject_id_system': 'Identifier system behind '
                                                                       'subject_key.',
                                                  'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                 'source identifier. Not salted, not secret; '
                                                                 'a stable join key across feeds where '
                                                                 'person_id is unresolved.',
                                                  'target_domain': 'Downstream routing domain for this '
                                                                   'mapping.',
                                                  'vip_ind': 'Source VIP indicator carried as data.',
                                                  'withheld_identity_ind': 'Source withheld-identity '
                                                                           'indicator.'},
                                      'table': 'INTERNAL event feed for '
                                               'journey_clinical.medication_dispense: one row per admitted '
                                               'event x standard mapping (unmapped events carry one row with '
                                               'a null mapping block), identical to the rows the retired '
                                               'events_patient_event held for this fact table. Read by the '
                                               'OMOP model and the OMOP id registry as a union over all '
                                               'feeds.'},
 '_events_feed_medication_order': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                               'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                               'available.',
                                               'event_datetime': 'Clinical or administrative event time.',
                                               'event_datetime_local': 'event_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST). '
                                                                       'Millennium rows take the bronze '
                                                                       '*_LOCAL companions of the same '
                                                                       'source columns and fallback order; '
                                                                       'rows from local-clock sources (LUNA, '
                                                                       'PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                       'BadgerNet, EndoBase, SLAM, iWeb, '
                                                                       'Datix and similar) carry '
                                                                       'event_datetime unchanged. '
                                                                       'event_datetime is a UTC instant for '
                                                                       'Millennium rows, so take local '
                                                                       'calendar dates and clock times from '
                                                                       'this column.',
                                               'event_end_datetime': 'Event end when the source supplies '
                                                                     'one.',
                                               'event_end_datetime_local': 'event_end_datetime in '
                                                                           'Europe/London wall-clock time '
                                                                           '(GMT/BST), by the same rule as '
                                                                           'event_datetime_local; null where '
                                                                           'event_end_datetime is null.',
                                               'event_type': 'Fact kind (condition',
                                               'fact_category': 'Clinical versus administrative fact.',
                                               'fact_row_key': 'Deterministic row key inside fact_table.',
                                               'fact_table': "Typed fact table holding the event's values.",
                                               'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                            'contributing canonical arm through '
                                                            'internal_event_index, then selected from the '
                                                            'event side of the public mapping join. Source '
                                                            'arms have different clock expressions, '
                                                            'including typed nulls; this is not uniformly '
                                                            "one bronze row's write time. Mapping clocks do "
                                                            'not contribute, and mapping fan-out does not '
                                                            'calculate a new maximum. Later typed-product '
                                                            'enrichment, such as implant attribute clocks or '
                                                            'additional document-history rows, is not folded '
                                                            'back into this event-index clock. No current '
                                                            'Silver refresh timestamp is substituted.',
                                               'map_candidate_count': 'Number of candidates for the event, '
                                                                      'coding system and mapping source.',
                                               'map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                                 'measured one.',
                                               'map_loaded_at': 'loaded_at of the mapping row (null when the '
                                                                'event has no mapping); the clock '
                                                                'concept_registry uses for mapped codes.',
                                               'map_method': 'How the mapping was reached: source_native or '
                                                             'rule for legacy lanes; S3 lookup lanes retain '
                                                             'their explicit method.',
                                               'map_rule_id': 'Rule or lane identifier that produced this '
                                                              'candidate.',
                                               'map_source': 'Governed mapping product that produced the '
                                                             'row.',
                                               'map_version': 'Mapping content version.',
                                               'mapped_code': 'Unique valid standard OMOP concept reached '
                                                              'from exact ICD code semantics; NULL when '
                                                              'absent or ambiguous.',
                                               'mapped_coding_system': 'Standard coding system for this '
                                                                       'mapping row; null on the unmapped '
                                                                       'row.',
                                               'mapped_display': 'Standard display or definition.',
                                               'patient_event_key': 'Immutable deterministic fact-grain '
                                                                    'event key shared across a mapped '
                                                                    "event's N rows.",
                                               'patient_event_row_key': 'Deterministic row key (the mapping '
                                                                        'id for mapped rows; a namespaced '
                                                                        'unmapped id otherwise).',
                                               'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                            'available.',
                                               'record_status': 'Status label carried unchanged from the '
                                                                'contributing canonical arm through '
                                                                'internal_event_index and the public '
                                                                'event-to-mapping left join. It is not a '
                                                                'Boolean mirror of IS_CURRENT_IN_SOURCE or a '
                                                                'verbatim closure flag: each arm supplies '
                                                                'its own active, superseded or retracted '
                                                                'recipe. Every mapping row for an event '
                                                                'inherits the event-side status; mapping '
                                                                'fields do not replace it. The input is the '
                                                                'canonical event-index row, not a promise of '
                                                                'equality with a later typed public '
                                                                "product's history or latest-version "
                                                                'selection.',
                                               'source_code': 'Source code as recorded.',
                                               'source_coding_system': 'The description for the code value',
                                               'source_display': 'Source display as recorded.',
                                               'source_object': 'Native source entity in plain language',
                                               'source_row_key': 'Native source record key (BIGINT ids '
                                                                 'rendered as text); the typed native id '
                                                                 'lives on the fact table.',
                                               'source_system': 'Plain-language source system (Millennium',
                                               'subject_id_system': 'Identifier system behind subject_key.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'target_domain': 'Downstream routing domain for this mapping.',
                                               'vip_ind': 'Source VIP indicator carried as data.',
                                               'withheld_identity_ind': 'Source withheld-identity '
                                                                        'indicator.'},
                                   'table': 'INTERNAL event feed for journey_clinical.medication_order: one '
                                            'row per admitted event x standard mapping (unmapped events '
                                            'carry one row with a null mapping block), identical to the rows '
                                            'the retired events_patient_event held for this fact table. Read '
                                            'by the OMOP model and the OMOP id registry as a union over all '
                                            'feeds.'},
 '_events_feed_presenting_complaint': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                                   'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                                   'available.',
                                                   'event_datetime': 'Clinical or administrative event time.',
                                                   'event_datetime_local': 'event_datetime in Europe/London '
                                                                           'wall-clock time (GMT/BST). '
                                                                           'Millennium rows take the bronze '
                                                                           '*_LOCAL companions of the same '
                                                                           'source columns and fallback '
                                                                           'order; rows from local-clock '
                                                                           'sources (LUNA, PACS/DICOM, TFC '
                                                                           'LIMS, CCMDS, MSDS, BadgerNet, '
                                                                           'EndoBase, SLAM, iWeb, Datix and '
                                                                           'similar) carry event_datetime '
                                                                           'unchanged. event_datetime is a '
                                                                           'UTC instant for Millennium rows, '
                                                                           'so take local calendar dates and '
                                                                           'clock times from this column.',
                                                   'event_end_datetime': 'Event end when the source supplies '
                                                                         'one.',
                                                   'event_end_datetime_local': 'event_end_datetime in '
                                                                               'Europe/London wall-clock '
                                                                               'time (GMT/BST), by the same '
                                                                               'rule as '
                                                                               'event_datetime_local; null '
                                                                               'where event_end_datetime is '
                                                                               'null.',
                                                   'event_type': 'Fact kind (condition',
                                                   'fact_category': 'Clinical versus administrative fact.',
                                                   'fact_row_key': 'Deterministic row key inside fact_table.',
                                                   'fact_table': "Typed fact table holding the event's "
                                                                 'values.',
                                                   'loaded_at': 'Per-event loaded_at carried unchanged from '
                                                                'the contributing canonical arm through '
                                                                'internal_event_index, then selected from '
                                                                'the event side of the public mapping join. '
                                                                'Source arms have different clock '
                                                                'expressions, including typed nulls; this is '
                                                                "not uniformly one bronze row's write time. "
                                                                'Mapping clocks do not contribute, and '
                                                                'mapping fan-out does not calculate a new '
                                                                'maximum. Later typed-product enrichment, '
                                                                'such as implant attribute clocks or '
                                                                'additional document-history rows, is not '
                                                                'folded back into this event-index clock. No '
                                                                'current Silver refresh timestamp is '
                                                                'substituted.',
                                                   'map_candidate_count': 'Number of candidates for the '
                                                                          'event, coding system and mapping '
                                                                          'source.',
                                                   'map_confidence': 'Mapping confidence in [0,1] where the '
                                                                     'lane measured one.',
                                                   'map_loaded_at': 'loaded_at of the mapping row (null when '
                                                                    'the event has no mapping); the clock '
                                                                    'concept_registry uses for mapped codes.',
                                                   'map_method': 'How the mapping was reached: source_native '
                                                                 'or rule for legacy lanes; S3 lookup lanes '
                                                                 'retain their explicit method.',
                                                   'map_rule_id': 'Rule or lane identifier that produced '
                                                                  'this candidate.',
                                                   'map_source': 'Governed mapping product that produced the '
                                                                 'row.',
                                                   'map_version': 'Mapping content version.',
                                                   'mapped_code': 'Unique valid standard OMOP concept '
                                                                  'reached from exact ICD code semantics; '
                                                                  'NULL when absent or ambiguous.',
                                                   'mapped_coding_system': 'Standard coding system for this '
                                                                           'mapping row; null on the '
                                                                           'unmapped row.',
                                                   'mapped_display': 'Standard display or definition.',
                                                   'patient_event_key': 'Immutable deterministic fact-grain '
                                                                        'event key shared across a mapped '
                                                                        "event's N rows.",
                                                   'patient_event_row_key': 'Deterministic row key (the '
                                                                            'mapping id for mapped rows; a '
                                                                            'namespaced unmapped id '
                                                                            'otherwise).',
                                                   'person_id': 'Resolved Millennium PERSON_ID as BIGINT '
                                                                'when available.',
                                                   'record_status': 'Status label carried unchanged from the '
                                                                    'contributing canonical arm through '
                                                                    'internal_event_index and the public '
                                                                    'event-to-mapping left join. It is not a '
                                                                    'Boolean mirror of IS_CURRENT_IN_SOURCE '
                                                                    'or a verbatim closure flag: each arm '
                                                                    'supplies its own active, superseded or '
                                                                    'retracted recipe. Every mapping row for '
                                                                    'an event inherits the event-side '
                                                                    'status; mapping fields do not replace '
                                                                    'it. The input is the canonical '
                                                                    'event-index row, not a promise of '
                                                                    'equality with a later typed public '
                                                                    "product's history or latest-version "
                                                                    'selection.',
                                                   'source_code': 'Source code as recorded.',
                                                   'source_coding_system': 'The description for the code '
                                                                           'value',
                                                   'source_display': 'Source display as recorded.',
                                                   'source_object': 'Native source entity in plain language',
                                                   'source_row_key': 'Native source record key (BIGINT ids '
                                                                     'rendered as text); the typed native id '
                                                                     'lives on the fact table.',
                                                   'source_system': 'Plain-language source system '
                                                                    '(Millennium',
                                                   'subject_id_system': 'Identifier system behind '
                                                                        'subject_key.',
                                                   'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                  'source identifier. Not salted, not '
                                                                  'secret; a stable join key across feeds '
                                                                  'where person_id is unresolved.',
                                                   'target_domain': 'Downstream routing domain for this '
                                                                    'mapping.',
                                                   'vip_ind': 'Source VIP indicator carried as data.',
                                                   'withheld_identity_ind': 'Source withheld-identity '
                                                                            'indicator.'},
                                       'table': 'INTERNAL event feed for '
                                                'journey_clinical.presenting_complaint: one row per admitted '
                                                'event x standard mapping (unmapped events carry one row '
                                                'with a null mapping block), identical to the rows the '
                                                'retired events_patient_event held for this fact table. Read '
                                                'by the OMOP model and the OMOP id registry as a union over '
                                                'all feeds.'},
 '_events_feed_procedure': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                        'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                        'event_datetime': 'Clinical or administrative event time.',
                                        'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                                'time (GMT/BST). Millennium rows take the '
                                                                'bronze *_LOCAL companions of the same '
                                                                'source columns and fallback order; rows '
                                                                'from local-clock sources (LUNA, PACS/DICOM, '
                                                                'TFC LIMS, CCMDS, MSDS, BadgerNet, EndoBase, '
                                                                'SLAM, iWeb, Datix and similar) carry '
                                                                'event_datetime unchanged. event_datetime is '
                                                                'a UTC instant for Millennium rows, so take '
                                                                'local calendar dates and clock times from '
                                                                'this column.',
                                        'event_end_datetime': 'Event end when the source supplies one.',
                                        'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                    'wall-clock time (GMT/BST), by the same '
                                                                    'rule as event_datetime_local; null '
                                                                    'where event_end_datetime is null.',
                                        'event_type': 'Fact kind (condition',
                                        'fact_category': 'Clinical versus administrative fact.',
                                        'fact_row_key': 'Deterministic row key inside fact_table.',
                                        'fact_table': "Typed fact table holding the event's values.",
                                        'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                     'contributing canonical arm through '
                                                     'internal_event_index, then selected from the event '
                                                     'side of the public mapping join. Source arms have '
                                                     'different clock expressions, including typed nulls; '
                                                     "this is not uniformly one bronze row's write time. "
                                                     'Mapping clocks do not contribute, and mapping fan-out '
                                                     'does not calculate a new maximum. Later typed-product '
                                                     'enrichment, such as implant attribute clocks or '
                                                     'additional document-history rows, is not folded back '
                                                     'into this event-index clock. No current Silver refresh '
                                                     'timestamp is substituted.',
                                        'map_candidate_count': 'Number of candidates for the event, coding '
                                                               'system and mapping source.',
                                        'map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                          'measured one.',
                                        'map_loaded_at': 'loaded_at of the mapping row (null when the event '
                                                         'has no mapping); the clock concept_registry uses '
                                                         'for mapped codes.',
                                        'map_method': 'How the mapping was reached: source_native or rule '
                                                      'for legacy lanes; S3 lookup lanes retain their '
                                                      'explicit method.',
                                        'map_rule_id': 'Rule or lane identifier that produced this '
                                                       'candidate.',
                                        'map_source': 'Governed mapping product that produced the row.',
                                        'map_version': 'Mapping content version.',
                                        'mapped_code': 'Unique valid standard OMOP concept reached from '
                                                       'exact ICD code semantics; NULL when absent or '
                                                       'ambiguous.',
                                        'mapped_coding_system': 'Standard coding system for this mapping '
                                                                'row; null on the unmapped row.',
                                        'mapped_display': 'Standard display or definition.',
                                        'patient_event_key': 'Immutable deterministic fact-grain event key '
                                                             "shared across a mapped event's N rows.",
                                        'patient_event_row_key': 'Deterministic row key (the mapping id for '
                                                                 'mapped rows; a namespaced unmapped id '
                                                                 'otherwise).',
                                        'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                     'available.',
                                        'record_status': 'Status label carried unchanged from the '
                                                         'contributing canonical arm through '
                                                         'internal_event_index and the public '
                                                         'event-to-mapping left join. It is not a Boolean '
                                                         'mirror of IS_CURRENT_IN_SOURCE or a verbatim '
                                                         'closure flag: each arm supplies its own active, '
                                                         'superseded or retracted recipe. Every mapping row '
                                                         'for an event inherits the event-side status; '
                                                         'mapping fields do not replace it. The input is the '
                                                         'canonical event-index row, not a promise of '
                                                         "equality with a later typed public product's "
                                                         'history or latest-version selection.',
                                        'source_code': 'Source code as recorded.',
                                        'source_coding_system': 'The description for the code value',
                                        'source_display': 'Source display as recorded.',
                                        'source_object': 'Native source entity in plain language',
                                        'source_row_key': 'Native source record key (BIGINT ids rendered as '
                                                          'text); the typed native id lives on the fact '
                                                          'table.',
                                        'source_system': 'Plain-language source system (Millennium',
                                        'subject_id_system': 'Identifier system behind subject_key.',
                                        'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                       'identifier. Not salted, not secret; a stable join '
                                                       'key across feeds where person_id is unresolved.',
                                        'target_domain': 'Downstream routing domain for this mapping.',
                                        'vip_ind': 'Source VIP indicator carried as data.',
                                        'withheld_identity_ind': 'Source withheld-identity indicator.'},
                            'table': 'INTERNAL event feed for journey_clinical.procedure: one row per '
                                     'admitted event x standard mapping (unmapped events carry one row with '
                                     'a null mapping block), identical to the rows the retired '
                                     'events_patient_event held for this fact table. Read by the OMOP model '
                                     'and the OMOP id registry as a union over all feeds.'},
 '_events_feed_transfusion': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                          'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                          'event_datetime': 'Clinical or administrative event time.',
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
                                          'event_end_datetime': 'Event end when the source supplies one.',
                                          'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                      'wall-clock time (GMT/BST), by the '
                                                                      'same rule as event_datetime_local; '
                                                                      'null where event_end_datetime is '
                                                                      'null.',
                                          'event_type': 'Fact kind (condition',
                                          'fact_category': 'Clinical versus administrative fact.',
                                          'fact_row_key': 'Deterministic row key inside fact_table.',
                                          'fact_table': "Typed fact table holding the event's values.",
                                          'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                       'contributing canonical arm through '
                                                       'internal_event_index, then selected from the event '
                                                       'side of the public mapping join. Source arms have '
                                                       'different clock expressions, including typed nulls; '
                                                       "this is not uniformly one bronze row's write time. "
                                                       'Mapping clocks do not contribute, and mapping '
                                                       'fan-out does not calculate a new maximum. Later '
                                                       'typed-product enrichment, such as implant attribute '
                                                       'clocks or additional document-history rows, is not '
                                                       'folded back into this event-index clock. No current '
                                                       'Silver refresh timestamp is substituted.',
                                          'map_candidate_count': 'Number of candidates for the event, coding '
                                                                 'system and mapping source.',
                                          'map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                            'measured one.',
                                          'map_loaded_at': 'loaded_at of the mapping row (null when the '
                                                           'event has no mapping); the clock '
                                                           'concept_registry uses for mapped codes.',
                                          'map_method': 'How the mapping was reached: source_native or rule '
                                                        'for legacy lanes; S3 lookup lanes retain their '
                                                        'explicit method.',
                                          'map_rule_id': 'Rule or lane identifier that produced this '
                                                         'candidate.',
                                          'map_source': 'Governed mapping product that produced the row.',
                                          'map_version': 'Mapping content version.',
                                          'mapped_code': 'Unique valid standard OMOP concept reached from '
                                                         'exact ICD code semantics; NULL when absent or '
                                                         'ambiguous.',
                                          'mapped_coding_system': 'Standard coding system for this mapping '
                                                                  'row; null on the unmapped row.',
                                          'mapped_display': 'Standard display or definition.',
                                          'patient_event_key': 'Immutable deterministic fact-grain event key '
                                                               "shared across a mapped event's N rows.",
                                          'patient_event_row_key': 'Deterministic row key (the mapping id '
                                                                   'for mapped rows; a namespaced unmapped '
                                                                   'id otherwise).',
                                          'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                       'available.',
                                          'record_status': 'Status label carried unchanged from the '
                                                           'contributing canonical arm through '
                                                           'internal_event_index and the public '
                                                           'event-to-mapping left join. It is not a Boolean '
                                                           'mirror of IS_CURRENT_IN_SOURCE or a verbatim '
                                                           'closure flag: each arm supplies its own active, '
                                                           'superseded or retracted recipe. Every mapping '
                                                           'row for an event inherits the event-side status; '
                                                           'mapping fields do not replace it. The input is '
                                                           'the canonical event-index row, not a promise of '
                                                           "equality with a later typed public product's "
                                                           'history or latest-version selection.',
                                          'source_code': 'Source code as recorded.',
                                          'source_coding_system': 'The description for the code value',
                                          'source_display': 'Source display as recorded.',
                                          'source_object': 'Native source entity in plain language',
                                          'source_row_key': 'Native source record key (BIGINT ids rendered '
                                                            'as text); the typed native id lives on the fact '
                                                            'table.',
                                          'source_system': 'Plain-language source system (Millennium',
                                          'subject_id_system': 'Identifier system behind subject_key.',
                                          'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                         'identifier. Not salted, not secret; a stable join '
                                                         'key across feeds where person_id is unresolved.',
                                          'target_domain': 'Downstream routing domain for this mapping.',
                                          'vip_ind': 'Source VIP indicator carried as data.',
                                          'withheld_identity_ind': 'Source withheld-identity indicator.'},
                              'table': 'INTERNAL event feed for journey_clinical.transfusion: one row per '
                                       'admitted event x standard mapping (unmapped events carry one row '
                                       'with a null mapping block), identical to the rows the retired '
                                       'events_patient_event held for this fact table. Read by the OMOP '
                                       'model and the OMOP id registry as a union over all feeds.'},
 '_events_feed_vital_sign': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                         'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                         'event_datetime': 'Clinical or administrative event time.',
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
                                         'event_end_datetime': 'Event end when the source supplies one.',
                                         'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                     'wall-clock time (GMT/BST), by the same '
                                                                     'rule as event_datetime_local; null '
                                                                     'where event_end_datetime is null.',
                                         'event_type': 'Fact kind (condition',
                                         'fact_category': 'Clinical versus administrative fact.',
                                         'fact_row_key': 'Deterministic row key inside fact_table.',
                                         'fact_table': "Typed fact table holding the event's values.",
                                         'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                      'contributing canonical arm through '
                                                      'internal_event_index, then selected from the event '
                                                      'side of the public mapping join. Source arms have '
                                                      'different clock expressions, including typed nulls; '
                                                      "this is not uniformly one bronze row's write time. "
                                                      'Mapping clocks do not contribute, and mapping fan-out '
                                                      'does not calculate a new maximum. Later typed-product '
                                                      'enrichment, such as implant attribute clocks or '
                                                      'additional document-history rows, is not folded back '
                                                      'into this event-index clock. No current Silver '
                                                      'refresh timestamp is substituted.',
                                         'map_candidate_count': 'Number of candidates for the event, coding '
                                                                'system and mapping source.',
                                         'map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                           'measured one.',
                                         'map_loaded_at': 'loaded_at of the mapping row (null when the event '
                                                          'has no mapping); the clock concept_registry uses '
                                                          'for mapped codes.',
                                         'map_method': 'How the mapping was reached: source_native or rule '
                                                       'for legacy lanes; S3 lookup lanes retain their '
                                                       'explicit method.',
                                         'map_rule_id': 'Rule or lane identifier that produced this '
                                                        'candidate.',
                                         'map_source': 'Governed mapping product that produced the row.',
                                         'map_version': 'Mapping content version.',
                                         'mapped_code': 'Unique valid standard OMOP concept reached from '
                                                        'exact ICD code semantics; NULL when absent or '
                                                        'ambiguous.',
                                         'mapped_coding_system': 'Standard coding system for this mapping '
                                                                 'row; null on the unmapped row.',
                                         'mapped_display': 'Standard display or definition.',
                                         'patient_event_key': 'Immutable deterministic fact-grain event key '
                                                              "shared across a mapped event's N rows.",
                                         'patient_event_row_key': 'Deterministic row key (the mapping id for '
                                                                  'mapped rows; a namespaced unmapped id '
                                                                  'otherwise).',
                                         'person_id': 'Resolved Millennium PERSON_ID as BIGINT when '
                                                      'available.',
                                         'record_status': 'Status label carried unchanged from the '
                                                          'contributing canonical arm through '
                                                          'internal_event_index and the public '
                                                          'event-to-mapping left join. It is not a Boolean '
                                                          'mirror of IS_CURRENT_IN_SOURCE or a verbatim '
                                                          'closure flag: each arm supplies its own active, '
                                                          'superseded or retracted recipe. Every mapping row '
                                                          'for an event inherits the event-side status; '
                                                          'mapping fields do not replace it. The input is '
                                                          'the canonical event-index row, not a promise of '
                                                          "equality with a later typed public product's "
                                                          'history or latest-version selection.',
                                         'source_code': 'Source code as recorded.',
                                         'source_coding_system': 'The description for the code value',
                                         'source_display': 'Source display as recorded.',
                                         'source_object': 'Native source entity in plain language',
                                         'source_row_key': 'Native source record key (BIGINT ids rendered as '
                                                           'text); the typed native id lives on the fact '
                                                           'table.',
                                         'source_system': 'Plain-language source system (Millennium',
                                         'subject_id_system': 'Identifier system behind subject_key.',
                                         'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                        'identifier. Not salted, not secret; a stable join '
                                                        'key across feeds where person_id is unresolved.',
                                         'target_domain': 'Downstream routing domain for this mapping.',
                                         'vip_ind': 'Source VIP indicator carried as data.',
                                         'withheld_identity_ind': 'Source withheld-identity indicator.'},
                             'table': 'INTERNAL event feed for journey_clinical.vital_sign: one row per '
                                      'admitted event x standard mapping (unmapped events carry one row with '
                                      'a null mapping block), identical to the rows the retired '
                                      'events_patient_event held for this fact table. Read by the OMOP model '
                                      'and the OMOP id registry as a union over all feeds.'}}
