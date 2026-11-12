"""Silver activity.event_feeds: 13 dataset(s).

- datasets: _events_feed_appointment, _events_feed_referral, _events_feed_rtt_pathway, _events_feed_rtt_activity, _events_feed_waiting_list_entry, _events_feed_community_care_activity, _events_feed_community_care_contact, _events_feed_hrg_grouping, _events_feed_costed_activity, _events_feed_drug_expenditure, _events_feed_medication_supply, _events_feed_elective_access_entry ...
- moved verbatim from: 1_silver_spine_and_reference:3588

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_tables._common.journey import (
    _appointment_canonical,
    _community_care_activity_canonical,
    _community_care_contact_canonical,
    _costed_activity_canonical,
    _drug_expenditure_canonical,
    _elective_access_entry_canonical,
    _hrg_grouping_canonical,
    _medication_supply_canonical,
    _pathway_tracking_canonical,
    _referral_canonical,
    _rtt_activity_canonical,
    _rtt_pathway_canonical,
    _waiting_list_index_representative,
)
from silver_tables._common.spine_and_reference import (
    _declare_event_feed,
    _event_index_projection,
    _luna_code_event_map,
    _map_projection,
    _seed_code_event_map,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


EVENT_FEED_SPECS = {
    'appointment': {
        "fact_table": 'journey_clinical.appointment',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_appointment_canonical(), "appointment",
                "journey_clinical.appointment", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'referral': {
        "fact_table": 'journey_clinical.referral',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_referral_canonical(), "referral",
                "journey_clinical.referral", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _luna_code_event_map(
                _referral_canonical(), "treatment_function_code",
                "urn:barts:luna:treatment-function"),
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'rtt_pathway': {
        "fact_table": 'journey_clinical.rtt_pathway',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_rtt_pathway_canonical(), "rtt_pathway",
                "journey_clinical.rtt_pathway", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _luna_code_event_map(
                _rtt_pathway_canonical(), "treatment_function_code",
                "urn:barts:luna:treatment-function"),
            _luna_code_event_map(
                _rtt_pathway_canonical(), "current_status_code",
                "urn:barts:luna:rtt-status"),
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'rtt_activity': {
        "fact_table": 'journey_clinical.rtt_activity',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_rtt_activity_canonical(), "rtt_activity",
                "journey_clinical.rtt_activity", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _luna_code_event_map(
                _rtt_activity_canonical(), "treatment_function_code",
                "urn:barts:luna:treatment-function"),
            _luna_code_event_map(
                _rtt_activity_canonical(), "status_code",
                "urn:barts:luna:rtt-status"),
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'waiting_list_entry': {
        "fact_table": 'journey_clinical.waiting_list_entry',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_waiting_list_index_representative(), "waiting_list_entry",
                "journey_clinical.waiting_list_entry", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _seed_code_event_map(
                _waiting_list_index_representative(), "status_code",
                "urn:cerner:code_value", "wl_status_cdf_meaning"),
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'community_care_activity': {
        "fact_table": 'journey_clinical.community_care_activity',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_community_care_activity_canonical(), "community_care_activity",
                "journey_clinical.community_care_activity", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_community_care_activity_canonical(),
                "_snomed_candidate_code", "_snomed_candidate_name",
                "http://snomed.info/sct", "community_activity",
                "bronze.map_community_care_activity"),
            _map_projection(_community_care_activity_canonical(),
                "_snomed_candidate_omop_code", "_snomed_candidate_omop_display",
                "urn:omop:concept_id", "community_activity",
                "bronze.map_community_care_activity"),
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'community_care_contact': {
        "fact_table": 'journey_clinical.community_care_contact',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_community_care_contact_canonical(), "community_care_contact",
                "journey_clinical.community_care_contact", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'hrg_grouping': {
        "fact_table": 'journey_clinical.hrg_grouping',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_hrg_grouping_canonical(), "hrg_grouping",
                "journey_clinical.hrg_grouping", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'costed_activity': {
        "fact_table": 'journey_clinical.costed_activity',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_costed_activity_canonical(), "costed_activity",
                "journey_clinical.costed_activity", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'drug_expenditure': {
        "fact_table": 'journey_clinical.drug_expenditure',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_drug_expenditure_canonical(), "drug_expenditure",
                "journey_clinical.drug_expenditure", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_drug_expenditure_canonical(), "_dmd_code", "_dmd_display",
                "http://snomed.info/sct", "drug",
                "bronze.map_finance_hcd_expenditure"),
            _map_projection(_drug_expenditure_canonical(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "drug",
                "bronze.map_finance_hcd_expenditure"),
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'medication_supply': {
        "fact_table": 'journey_clinical.medication_supply',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_medication_supply_canonical(), "medication_supply",
                "journey_clinical.medication_supply", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_medication_supply_canonical(), "_dmd_code", "_dmd_display",
                "http://snomed.info/sct", "drug", "bronze.map_homecare_request"),
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'elective_access_entry': {
        "fact_table": 'journey_clinical.elective_access_entry',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_elective_access_entry_canonical(), "elective_access_entry",
                "journey_clinical.elective_access_entry", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_elective_access_entry_canonical(), "_opcs4_code", "_opcs4_display",
                "http://fhir.hl7.org.uk/CodeSystem/OPCS-4", "procedure",
                "bronze.map_elective_access_list_procedure"),
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'pathway_tracking': {
        "fact_table": 'journey_clinical.pathway_tracking',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_pathway_tracking_canonical(), "pathway_tracking",
                "journey_clinical.pathway_tracking", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
}


for _feed, _spec in EVENT_FEED_SPECS.items():
    _declare_event_feed(_feed, _spec)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_events_feed_appointment': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                              'table': 'INTERNAL event feed for journey_clinical.appointment: one row per '
                                       'admitted event x standard mapping (unmapped events carry one row '
                                       'with a null mapping block), identical to the rows the retired '
                                       'events_patient_event held for this fact table. Read by the OMOP '
                                       'model and the OMOP id registry as a union over all feeds.'},
 '_events_feed_community_care_activity': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                                      'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                                      'available.',
                                                      'event_datetime': 'Clinical or administrative event '
                                                                        'time.',
                                                      'event_datetime_local': 'event_datetime in '
                                                                              'Europe/London wall-clock time '
                                                                              '(GMT/BST). Millennium rows '
                                                                              'take the bronze *_LOCAL '
                                                                              'companions of the same source '
                                                                              'columns and fallback order; '
                                                                              'rows from local-clock sources '
                                                                              '(LUNA, PACS/DICOM, TFC LIMS, '
                                                                              'CCMDS, MSDS, BadgerNet, '
                                                                              'EndoBase, SLAM, iWeb, Datix '
                                                                              'and similar) carry '
                                                                              'event_datetime unchanged. '
                                                                              'event_datetime is a UTC '
                                                                              'instant for Millennium rows, '
                                                                              'so take local calendar dates '
                                                                              'and clock times from this '
                                                                              'column.',
                                                      'event_end_datetime': 'Event end when the source '
                                                                            'supplies one.',
                                                      'event_end_datetime_local': 'event_end_datetime in '
                                                                                  'Europe/London wall-clock '
                                                                                  'time (GMT/BST), by the '
                                                                                  'same rule as '
                                                                                  'event_datetime_local; '
                                                                                  'null where '
                                                                                  'event_end_datetime is '
                                                                                  'null.',
                                                      'event_type': 'Fact kind (condition',
                                                      'fact_category': 'Clinical versus administrative fact.',
                                                      'fact_row_key': 'Deterministic row key inside '
                                                                      'fact_table.',
                                                      'fact_table': "Typed fact table holding the event's "
                                                                    'values.',
                                                      'loaded_at': 'Per-event loaded_at carried unchanged '
                                                                   'from the contributing canonical arm '
                                                                   'through internal_event_index, then '
                                                                   'selected from the event side of the '
                                                                   'public mapping join. Source arms have '
                                                                   'different clock expressions, including '
                                                                   'typed nulls; this is not uniformly one '
                                                                   "bronze row's write time. Mapping clocks "
                                                                   'do not contribute, and mapping fan-out '
                                                                   'does not calculate a new maximum. Later '
                                                                   'typed-product enrichment, such as '
                                                                   'implant attribute clocks or additional '
                                                                   'document-history rows, is not folded '
                                                                   'back into this event-index clock. No '
                                                                   'current Silver refresh timestamp is '
                                                                   'substituted.',
                                                      'map_candidate_count': 'Number of candidates for the '
                                                                             'event, coding system and '
                                                                             'mapping source.',
                                                      'map_confidence': 'Mapping confidence in [0,1] where '
                                                                        'the lane measured one.',
                                                      'map_loaded_at': 'loaded_at of the mapping row (null '
                                                                       'when the event has no mapping); the '
                                                                       'clock concept_registry uses for '
                                                                       'mapped codes.',
                                                      'map_method': 'How the mapping was reached: '
                                                                    'source_native or rule for legacy lanes; '
                                                                    'S3 lookup lanes retain their explicit '
                                                                    'method.',
                                                      'map_rule_id': 'Rule or lane identifier that produced '
                                                                     'this candidate.',
                                                      'map_source': 'Governed mapping product that produced '
                                                                    'the row.',
                                                      'map_version': 'Mapping content version.',
                                                      'mapped_code': 'Unique valid standard OMOP concept '
                                                                     'reached from exact ICD code semantics; '
                                                                     'NULL when absent or ambiguous.',
                                                      'mapped_coding_system': 'Standard coding system for '
                                                                              'this mapping row; null on the '
                                                                              'unmapped row.',
                                                      'mapped_display': 'Standard display or definition.',
                                                      'patient_event_key': 'Immutable deterministic '
                                                                           'fact-grain event key shared '
                                                                           "across a mapped event's N rows.",
                                                      'patient_event_row_key': 'Deterministic row key (the '
                                                                               'mapping id for mapped rows; '
                                                                               'a namespaced unmapped id '
                                                                               'otherwise).',
                                                      'person_id': 'Resolved Millennium PERSON_ID as BIGINT '
                                                                   'when available.',
                                                      'record_status': 'Status label carried unchanged from '
                                                                       'the contributing canonical arm '
                                                                       'through internal_event_index and the '
                                                                       'public event-to-mapping left join. '
                                                                       'It is not a Boolean mirror of '
                                                                       'IS_CURRENT_IN_SOURCE or a verbatim '
                                                                       'closure flag: each arm supplies its '
                                                                       'own active, superseded or retracted '
                                                                       'recipe. Every mapping row for an '
                                                                       'event inherits the event-side '
                                                                       'status; mapping fields do not '
                                                                       'replace it. The input is the '
                                                                       'canonical event-index row, not a '
                                                                       'promise of equality with a later '
                                                                       "typed public product's history or "
                                                                       'latest-version selection.',
                                                      'source_code': 'Source code as recorded.',
                                                      'source_coding_system': 'The description for the code '
                                                                              'value',
                                                      'source_display': 'Source display as recorded.',
                                                      'source_object': 'Native source entity in plain '
                                                                       'language',
                                                      'source_row_key': 'Native source record key (BIGINT '
                                                                        'ids rendered as text); the typed '
                                                                        'native id lives on the fact table.',
                                                      'source_system': 'Plain-language source system '
                                                                       '(Millennium',
                                                      'subject_id_system': 'Identifier system behind '
                                                                           'subject_key.',
                                                      'subject_key': 'Deterministic SHA-256 over the '
                                                                     'strongest source identifier. Not '
                                                                     'salted, not secret; a stable join key '
                                                                     'across feeds where person_id is '
                                                                     'unresolved.',
                                                      'target_domain': 'Downstream routing domain for this '
                                                                       'mapping.',
                                                      'vip_ind': 'Source VIP indicator carried as data.',
                                                      'withheld_identity_ind': 'Source withheld-identity '
                                                                               'indicator.'},
                                          'table': 'INTERNAL event feed for '
                                                   'journey_clinical.community_care_activity: one row per '
                                                   'admitted event x standard mapping (unmapped events carry '
                                                   'one row with a null mapping block), identical to the '
                                                   'rows the retired events_patient_event held for this fact '
                                                   'table. Read by the OMOP model and the OMOP id registry '
                                                   'as a union over all feeds.'},
 '_events_feed_community_care_contact': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                                     'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                                     'available.',
                                                     'event_datetime': 'Clinical or administrative event '
                                                                       'time.',
                                                     'event_datetime_local': 'event_datetime in '
                                                                             'Europe/London wall-clock time '
                                                                             '(GMT/BST). Millennium rows '
                                                                             'take the bronze *_LOCAL '
                                                                             'companions of the same source '
                                                                             'columns and fallback order; '
                                                                             'rows from local-clock sources '
                                                                             '(LUNA, PACS/DICOM, TFC LIMS, '
                                                                             'CCMDS, MSDS, BadgerNet, '
                                                                             'EndoBase, SLAM, iWeb, Datix '
                                                                             'and similar) carry '
                                                                             'event_datetime unchanged. '
                                                                             'event_datetime is a UTC '
                                                                             'instant for Millennium rows, '
                                                                             'so take local calendar dates '
                                                                             'and clock times from this '
                                                                             'column.',
                                                     'event_end_datetime': 'Event end when the source '
                                                                           'supplies one.',
                                                     'event_end_datetime_local': 'event_end_datetime in '
                                                                                 'Europe/London wall-clock '
                                                                                 'time (GMT/BST), by the '
                                                                                 'same rule as '
                                                                                 'event_datetime_local; null '
                                                                                 'where event_end_datetime '
                                                                                 'is null.',
                                                     'event_type': 'Fact kind (condition',
                                                     'fact_category': 'Clinical versus administrative fact.',
                                                     'fact_row_key': 'Deterministic row key inside '
                                                                     'fact_table.',
                                                     'fact_table': "Typed fact table holding the event's "
                                                                   'values.',
                                                     'loaded_at': 'Per-event loaded_at carried unchanged '
                                                                  'from the contributing canonical arm '
                                                                  'through internal_event_index, then '
                                                                  'selected from the event side of the '
                                                                  'public mapping join. Source arms have '
                                                                  'different clock expressions, including '
                                                                  'typed nulls; this is not uniformly one '
                                                                  "bronze row's write time. Mapping clocks "
                                                                  'do not contribute, and mapping fan-out '
                                                                  'does not calculate a new maximum. Later '
                                                                  'typed-product enrichment, such as implant '
                                                                  'attribute clocks or additional '
                                                                  'document-history rows, is not folded back '
                                                                  'into this event-index clock. No current '
                                                                  'Silver refresh timestamp is substituted.',
                                                     'map_candidate_count': 'Number of candidates for the '
                                                                            'event, coding system and '
                                                                            'mapping source.',
                                                     'map_confidence': 'Mapping confidence in [0,1] where '
                                                                       'the lane measured one.',
                                                     'map_loaded_at': 'loaded_at of the mapping row (null '
                                                                      'when the event has no mapping); the '
                                                                      'clock concept_registry uses for '
                                                                      'mapped codes.',
                                                     'map_method': 'How the mapping was reached: '
                                                                   'source_native or rule for legacy lanes; '
                                                                   'S3 lookup lanes retain their explicit '
                                                                   'method.',
                                                     'map_rule_id': 'Rule or lane identifier that produced '
                                                                    'this candidate.',
                                                     'map_source': 'Governed mapping product that produced '
                                                                   'the row.',
                                                     'map_version': 'Mapping content version.',
                                                     'mapped_code': 'Unique valid standard OMOP concept '
                                                                    'reached from exact ICD code semantics; '
                                                                    'NULL when absent or ambiguous.',
                                                     'mapped_coding_system': 'Standard coding system for '
                                                                             'this mapping row; null on the '
                                                                             'unmapped row.',
                                                     'mapped_display': 'Standard display or definition.',
                                                     'patient_event_key': 'Immutable deterministic '
                                                                          'fact-grain event key shared '
                                                                          "across a mapped event's N rows.",
                                                     'patient_event_row_key': 'Deterministic row key (the '
                                                                              'mapping id for mapped rows; a '
                                                                              'namespaced unmapped id '
                                                                              'otherwise).',
                                                     'person_id': 'Resolved Millennium PERSON_ID as BIGINT '
                                                                  'when available.',
                                                     'record_status': 'Status label carried unchanged from '
                                                                      'the contributing canonical arm '
                                                                      'through internal_event_index and the '
                                                                      'public event-to-mapping left join. It '
                                                                      'is not a Boolean mirror of '
                                                                      'IS_CURRENT_IN_SOURCE or a verbatim '
                                                                      'closure flag: each arm supplies its '
                                                                      'own active, superseded or retracted '
                                                                      'recipe. Every mapping row for an '
                                                                      'event inherits the event-side status; '
                                                                      'mapping fields do not replace it. The '
                                                                      'input is the canonical event-index '
                                                                      'row, not a promise of equality with a '
                                                                      "later typed public product's history "
                                                                      'or latest-version selection.',
                                                     'source_code': 'Source code as recorded.',
                                                     'source_coding_system': 'The description for the code '
                                                                             'value',
                                                     'source_display': 'Source display as recorded.',
                                                     'source_object': 'Native source entity in plain '
                                                                      'language',
                                                     'source_row_key': 'Native source record key (BIGINT ids '
                                                                       'rendered as text); the typed native '
                                                                       'id lives on the fact table.',
                                                     'source_system': 'Plain-language source system '
                                                                      '(Millennium',
                                                     'subject_id_system': 'Identifier system behind '
                                                                          'subject_key.',
                                                     'subject_key': 'Deterministic SHA-256 over the '
                                                                    'strongest source identifier. Not '
                                                                    'salted, not secret; a stable join key '
                                                                    'across feeds where person_id is '
                                                                    'unresolved.',
                                                     'target_domain': 'Downstream routing domain for this '
                                                                      'mapping.',
                                                     'vip_ind': 'Source VIP indicator carried as data.',
                                                     'withheld_identity_ind': 'Source withheld-identity '
                                                                              'indicator.'},
                                         'table': 'INTERNAL event feed for '
                                                  'journey_clinical.community_care_contact: one row per '
                                                  'admitted event x standard mapping (unmapped events carry '
                                                  'one row with a null mapping block), identical to the rows '
                                                  'the retired events_patient_event held for this fact '
                                                  'table. Read by the OMOP model and the OMOP id registry as '
                                                  'a union over all feeds.'},
 '_events_feed_costed_activity': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                  'table': 'INTERNAL event feed for journey_clinical.costed_activity: one '
                                           'row per admitted event x standard mapping (unmapped events carry '
                                           'one row with a null mapping block), identical to the rows the '
                                           'retired events_patient_event held for this fact table. Read by '
                                           'the OMOP model and the OMOP id registry as a union over all '
                                           'feeds.'},
 '_events_feed_drug_expenditure': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                   'table': 'INTERNAL event feed for journey_clinical.drug_expenditure: one '
                                            'row per admitted event x standard mapping (unmapped events '
                                            'carry one row with a null mapping block), identical to the rows '
                                            'the retired events_patient_event held for this fact table. Read '
                                            'by the OMOP model and the OMOP id registry as a union over all '
                                            'feeds.'},
 '_events_feed_elective_access_entry': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                                    'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                                    'available.',
                                                    'event_datetime': 'Clinical or administrative event '
                                                                      'time.',
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
                                                                            'UTC instant for Millennium '
                                                                            'rows, so take local calendar '
                                                                            'dates and clock times from this '
                                                                            'column.',
                                                    'event_end_datetime': 'Event end when the source '
                                                                          'supplies one.',
                                                    'event_end_datetime_local': 'event_end_datetime in '
                                                                                'Europe/London wall-clock '
                                                                                'time (GMT/BST), by the same '
                                                                                'rule as '
                                                                                'event_datetime_local; null '
                                                                                'where event_end_datetime is '
                                                                                'null.',
                                                    'event_type': 'Fact kind (condition',
                                                    'fact_category': 'Clinical versus administrative fact.',
                                                    'fact_row_key': 'Deterministic row key inside '
                                                                    'fact_table.',
                                                    'fact_table': "Typed fact table holding the event's "
                                                                  'values.',
                                                    'loaded_at': 'Per-event loaded_at carried unchanged from '
                                                                 'the contributing canonical arm through '
                                                                 'internal_event_index, then selected from '
                                                                 'the event side of the public mapping join. '
                                                                 'Source arms have different clock '
                                                                 'expressions, including typed nulls; this '
                                                                 "is not uniformly one bronze row's write "
                                                                 'time. Mapping clocks do not contribute, '
                                                                 'and mapping fan-out does not calculate a '
                                                                 'new maximum. Later typed-product '
                                                                 'enrichment, such as implant attribute '
                                                                 'clocks or additional document-history '
                                                                 'rows, is not folded back into this '
                                                                 'event-index clock. No current Silver '
                                                                 'refresh timestamp is substituted.',
                                                    'map_candidate_count': 'Number of candidates for the '
                                                                           'event, coding system and mapping '
                                                                           'source.',
                                                    'map_confidence': 'Mapping confidence in [0,1] where the '
                                                                      'lane measured one.',
                                                    'map_loaded_at': 'loaded_at of the mapping row (null '
                                                                     'when the event has no mapping); the '
                                                                     'clock concept_registry uses for mapped '
                                                                     'codes.',
                                                    'map_method': 'How the mapping was reached: '
                                                                  'source_native or rule for legacy lanes; '
                                                                  'S3 lookup lanes retain their explicit '
                                                                  'method.',
                                                    'map_rule_id': 'Rule or lane identifier that produced '
                                                                   'this candidate.',
                                                    'map_source': 'Governed mapping product that produced '
                                                                  'the row.',
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
                                                    'record_status': 'Status label carried unchanged from '
                                                                     'the contributing canonical arm through '
                                                                     'internal_event_index and the public '
                                                                     'event-to-mapping left join. It is not '
                                                                     'a Boolean mirror of '
                                                                     'IS_CURRENT_IN_SOURCE or a verbatim '
                                                                     'closure flag: each arm supplies its '
                                                                     'own active, superseded or retracted '
                                                                     'recipe. Every mapping row for an event '
                                                                     'inherits the event-side status; '
                                                                     'mapping fields do not replace it. The '
                                                                     'input is the canonical event-index '
                                                                     'row, not a promise of equality with a '
                                                                     "later typed public product's history "
                                                                     'or latest-version selection.',
                                                    'source_code': 'Source code as recorded.',
                                                    'source_coding_system': 'The description for the code '
                                                                            'value',
                                                    'source_display': 'Source display as recorded.',
                                                    'source_object': 'Native source entity in plain language',
                                                    'source_row_key': 'Native source record key (BIGINT ids '
                                                                      'rendered as text); the typed native '
                                                                      'id lives on the fact table.',
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
                                                 'journey_clinical.elective_access_entry: one row per '
                                                 'admitted event x standard mapping (unmapped events carry '
                                                 'one row with a null mapping block), identical to the rows '
                                                 'the retired events_patient_event held for this fact table. '
                                                 'Read by the OMOP model and the OMOP id registry as a union '
                                                 'over all feeds.'},
 '_events_feed_hrg_grouping': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                               'table': 'INTERNAL event feed for journey_clinical.hrg_grouping: one row per '
                                        'admitted event x standard mapping (unmapped events carry one row '
                                        'with a null mapping block), identical to the rows the retired '
                                        'events_patient_event held for this fact table. Read by the OMOP '
                                        'model and the OMOP id registry as a union over all feeds.'},
 '_events_feed_medication_supply': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                    'table': 'INTERNAL event feed for journey_clinical.medication_supply: '
                                             'one row per admitted event x standard mapping (unmapped events '
                                             'carry one row with a null mapping block), identical to the '
                                             'rows the retired events_patient_event held for this fact '
                                             'table. Read by the OMOP model and the OMOP id registry as a '
                                             'union over all feeds.'},
 '_events_feed_pathway_tracking': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                   'table': 'INTERNAL event feed for journey_clinical.pathway_tracking: one '
                                            'row per admitted event x standard mapping (unmapped events '
                                            'carry one row with a null mapping block), identical to the rows '
                                            'the retired events_patient_event held for this fact table. Read '
                                            'by the OMOP model and the OMOP id registry as a union over all '
                                            'feeds.'},
 '_events_feed_referral': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                       'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                       'event_datetime': 'Clinical or administrative event time.',
                                       'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                               'time (GMT/BST). Millennium rows take the '
                                                               'bronze *_LOCAL companions of the same source '
                                                               'columns and fallback order; rows from '
                                                               'local-clock sources (LUNA, PACS/DICOM, TFC '
                                                               'LIMS, CCMDS, MSDS, BadgerNet, EndoBase, '
                                                               'SLAM, iWeb, Datix and similar) carry '
                                                               'event_datetime unchanged. event_datetime is '
                                                               'a UTC instant for Millennium rows, so take '
                                                               'local calendar dates and clock times from '
                                                               'this column.',
                                       'event_end_datetime': 'Event end when the source supplies one.',
                                       'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                   'wall-clock time (GMT/BST), by the same '
                                                                   'rule as event_datetime_local; null where '
                                                                   'event_end_datetime is null.',
                                       'event_type': 'Fact kind (condition',
                                       'fact_category': 'Clinical versus administrative fact.',
                                       'fact_row_key': 'Deterministic row key inside fact_table.',
                                       'fact_table': "Typed fact table holding the event's values.",
                                       'loaded_at': 'Per-event loaded_at carried unchanged from the '
                                                    'contributing canonical arm through '
                                                    'internal_event_index, then selected from the event side '
                                                    'of the public mapping join. Source arms have different '
                                                    'clock expressions, including typed nulls; this is not '
                                                    "uniformly one bronze row's write time. Mapping clocks "
                                                    'do not contribute, and mapping fan-out does not '
                                                    'calculate a new maximum. Later typed-product '
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
                                       'map_method': 'How the mapping was reached: source_native or rule for '
                                                     'legacy lanes; S3 lookup lanes retain their explicit '
                                                     'method.',
                                       'map_rule_id': 'Rule or lane identifier that produced this candidate.',
                                       'map_source': 'Governed mapping product that produced the row.',
                                       'map_version': 'Mapping content version.',
                                       'mapped_code': 'Unique valid standard OMOP concept reached from exact '
                                                      'ICD code semantics; NULL when absent or ambiguous.',
                                       'mapped_coding_system': 'Standard coding system for this mapping row; '
                                                               'null on the unmapped row.',
                                       'mapped_display': 'Standard display or definition.',
                                       'patient_event_key': 'Immutable deterministic fact-grain event key '
                                                            "shared across a mapped event's N rows.",
                                       'patient_event_row_key': 'Deterministic row key (the mapping id for '
                                                                'mapped rows; a namespaced unmapped id '
                                                                'otherwise).',
                                       'person_id': 'Resolved Millennium PERSON_ID as BIGINT when available.',
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
                                                      'identifier. Not salted, not secret; a stable join key '
                                                      'across feeds where person_id is unresolved.',
                                       'target_domain': 'Downstream routing domain for this mapping.',
                                       'vip_ind': 'Source VIP indicator carried as data.',
                                       'withheld_identity_ind': 'Source withheld-identity indicator.'},
                           'table': 'INTERNAL event feed for journey_clinical.referral: one row per admitted '
                                    'event x standard mapping (unmapped events carry one row with a null '
                                    'mapping block), identical to the rows the retired events_patient_event '
                                    'held for this fact table. Read by the OMOP model and the OMOP id '
                                    'registry as a union over all feeds.'},
 '_events_feed_rtt_activity': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                               'table': 'INTERNAL event feed for journey_clinical.rtt_activity: one row per '
                                        'admitted event x standard mapping (unmapped events carry one row '
                                        'with a null mapping block), identical to the rows the retired '
                                        'events_patient_event held for this fact table. Read by the OMOP '
                                        'model and the OMOP id registry as a union over all feeds.'},
 '_events_feed_rtt_pathway': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                              'table': 'INTERNAL event feed for journey_clinical.rtt_pathway: one row per '
                                       'admitted event x standard mapping (unmapped events carry one row '
                                       'with a null mapping block), identical to the rows the retired '
                                       'events_patient_event held for this fact table. Read by the OMOP '
                                       'model and the OMOP id registry as a union over all feeds.'},
 '_events_feed_waiting_list_entry': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                                 'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                                 'available.',
                                                 'event_datetime': 'Clinical or administrative event time.',
                                                 'event_datetime_local': 'event_datetime in Europe/London '
                                                                         'wall-clock time (GMT/BST). '
                                                                         'Millennium rows take the bronze '
                                                                         '*_LOCAL companions of the same '
                                                                         'source columns and fallback order; '
                                                                         'rows from local-clock sources '
                                                                         '(LUNA, PACS/DICOM, TFC LIMS, '
                                                                         'CCMDS, MSDS, BadgerNet, EndoBase, '
                                                                         'SLAM, iWeb, Datix and similar) '
                                                                         'carry event_datetime unchanged. '
                                                                         'event_datetime is a UTC instant '
                                                                         'for Millennium rows, so take local '
                                                                         'calendar dates and clock times '
                                                                         'from this column.',
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
                                                 'loaded_at': 'Per-event loaded_at carried unchanged from '
                                                              'the contributing canonical arm through '
                                                              'internal_event_index, then selected from the '
                                                              'event side of the public mapping join. Source '
                                                              'arms have different clock expressions, '
                                                              'including typed nulls; this is not uniformly '
                                                              "one bronze row's write time. Mapping clocks "
                                                              'do not contribute, and mapping fan-out does '
                                                              'not calculate a new maximum. Later '
                                                              'typed-product enrichment, such as implant '
                                                              'attribute clocks or additional '
                                                              'document-history rows, is not folded back '
                                                              'into this event-index clock. No current '
                                                              'Silver refresh timestamp is substituted.',
                                                 'map_candidate_count': 'Number of candidates for the event, '
                                                                        'coding system and mapping source.',
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
                                                                  'Boolean mirror of IS_CURRENT_IN_SOURCE or '
                                                                  'a verbatim closure flag: each arm '
                                                                  'supplies its own active, superseded or '
                                                                  'retracted recipe. Every mapping row for '
                                                                  'an event inherits the event-side status; '
                                                                  'mapping fields do not replace it. The '
                                                                  'input is the canonical event-index row, '
                                                                  'not a promise of equality with a later '
                                                                  "typed public product's history or "
                                                                  'latest-version selection.',
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
                                                                'source identifier. Not salted, not secret; '
                                                                'a stable join key across feeds where '
                                                                'person_id is unresolved.',
                                                 'target_domain': 'Downstream routing domain for this '
                                                                  'mapping.',
                                                 'vip_ind': 'Source VIP indicator carried as data.',
                                                 'withheld_identity_ind': 'Source withheld-identity '
                                                                          'indicator.'},
                                     'table': 'INTERNAL event feed for journey_clinical.waiting_list_entry: '
                                              'one row per admitted event x standard mapping (unmapped '
                                              'events carry one row with a null mapping block), identical to '
                                              'the rows the retired events_patient_event held for this fact '
                                              'table. Read by the OMOP model and the OMOP id registry as a '
                                              'union over all feeds.'}}
