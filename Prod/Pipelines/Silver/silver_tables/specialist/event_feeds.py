"""Silver specialist.event_feeds: 13 dataset(s).

- datasets: _events_feed_registry_entry, _events_feed_device, _events_feed_critical_care_period, _events_feed_critical_care_activity, _events_feed_critical_care_admission, _events_feed_critical_care_daily_score, _events_feed_neonatal_episode, _events_feed_neonatal_care_day, _events_feed_neonatal_examination, _events_feed_baby_delivery, _events_feed_labour_delivery, _events_feed_maternity_care_contact ...
- moved verbatim from: 1_silver_spine_and_reference:3588

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import INTERNAL_SCHEMA, spark
from silver_tables._common.journey import (
    _baby_delivery_canonical,
    _cc_daily_score_canonical,
    _critical_care_activity_canonical,
    _critical_care_admission_canonical,
    _critical_care_period_canonical,
    _device_canonical,
    _labour_delivery_canonical,
    _maternity_care_contact_canonical,
    _neonatal_care_day_canonical,
    _neonatal_episode_canonical,
    _neonatal_examination_canonical,
    _registry_entry_canonical,
)
from silver_tables._common.spine_and_reference import (
    _declare_event_feed,
    _event_index_projection,
    _iweb_multiselect_event_map,
    _map_projection,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


EVENT_FEED_SPECS = {
    'registry_entry': {
        "fact_table": 'journey_clinical.registry_entry',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_registry_entry_canonical(), "registry_entry",
                "journey_clinical.registry_entry", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _iweb_multiselect_event_map(),
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'device': {
        "fact_table": 'journey_clinical.device',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_device_canonical(), "device",
                "journey_clinical.device", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
            _map_projection(_device_canonical(), "_snomed_code", "_snomed_display",
                "http://snomed.info/sct", "device",
                "lookup.mediconnect_device_type_map:applied"),
        ],
        "s3_axes": [('journey_clinical.device', 'device')],
        "supplement": [],
        "registry_findings": False,
    },
    'critical_care_period': {
        "fact_table": 'journey_clinical.critical_care_period',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_critical_care_period_canonical(), "critical_care_period",
                "journey_clinical.critical_care_period", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'critical_care_activity': {
        "fact_table": 'journey_clinical.critical_care_activity',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_critical_care_activity_canonical(), "critical_care_activity",
                "journey_clinical.critical_care_activity", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'critical_care_admission': {
        "fact_table": 'journey_clinical.critical_care_admission',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_critical_care_admission_canonical(), "critical_care_admission",
                "journey_clinical.critical_care_admission", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'critical_care_daily_score': {
        "fact_table": 'journey_clinical.critical_care_daily_score',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_cc_daily_score_canonical(), "critical_care_daily_score",
                "journey_clinical.critical_care_daily_score", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'neonatal_episode': {
        "fact_table": 'journey_clinical.neonatal_episode',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_neonatal_episode_canonical(), "neonatal_episode",
                "journey_clinical.neonatal_episode", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'neonatal_care_day': {
        "fact_table": 'journey_clinical.neonatal_care_day',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_neonatal_care_day_canonical(), "neonatal_care_day",
                "journey_clinical.neonatal_care_day", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'neonatal_examination': {
        "fact_table": 'journey_clinical.neonatal_examination',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_neonatal_examination_canonical(), "neonatal_examination",
                "journey_clinical.neonatal_examination", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'baby_delivery': {
        "fact_table": 'journey_clinical.baby_delivery',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_baby_delivery_canonical(), "baby_delivery",
                "journey_clinical.baby_delivery", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'labour_delivery': {
        "fact_table": 'journey_clinical.labour_delivery',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_labour_delivery_canonical(), "labour_delivery",
                "journey_clinical.labour_delivery", "clinical"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'maternity_care_contact': {
        "fact_table": 'journey_clinical.maternity_care_contact',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(_maternity_care_contact_canonical(), "maternity_care_contact",
                "journey_clinical.maternity_care_contact", "administrative"),
        ],
        # Mapping rows for those events (legacy source/vocabulary arms).
        "maps": lambda: [
        ],
        "s3_axes": [],
        "supplement": [],
        "registry_findings": False,
    },
    'safety_incident': {
        "fact_table": 'journey_clinical.safety_incident',
        # Event rows: the canonical lanes that publish this fact table.
        "lanes": lambda: [
            _event_index_projection(spark.read.table(INTERNAL_SCHEMA + "._clinical_safety_incident_index_lane"), "safety_incident", "journey_clinical.safety_incident", "clinical"),
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


COMMENTS = {'_events_feed_baby_delivery': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                                         'different clock expressions, including typed '
                                                         "nulls; this is not uniformly one bronze row's "
                                                         'write time. Mapping clocks do not contribute, and '
                                                         'mapping fan-out does not calculate a new maximum. '
                                                         'Later typed-product enrichment, such as implant '
                                                         'attribute clocks or additional document-history '
                                                         'rows, is not folded back into this event-index '
                                                         'clock. No current Silver refresh timestamp is '
                                                         'substituted.',
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
                                                             'event-to-mapping left join. It is not a '
                                                             'Boolean mirror of IS_CURRENT_IN_SOURCE or a '
                                                             'verbatim closure flag: each arm supplies its '
                                                             'own active, superseded or retracted recipe. '
                                                             'Every mapping row for an event inherits the '
                                                             'event-side status; mapping fields do not '
                                                             'replace it. The input is the canonical '
                                                             'event-index row, not a promise of equality '
                                                             "with a later typed public product's history or "
                                                             'latest-version selection.',
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
                                                           'identifier. Not salted, not secret; a stable '
                                                           'join key across feeds where person_id is '
                                                           'unresolved.',
                                            'target_domain': 'Downstream routing domain for this mapping.',
                                            'vip_ind': 'Source VIP indicator carried as data.',
                                            'withheld_identity_ind': 'Source withheld-identity indicator.'},
                                'table': 'INTERNAL event feed for journey_clinical.baby_delivery: one row '
                                         'per admitted event x standard mapping (unmapped events carry one '
                                         'row with a null mapping block), identical to the rows the retired '
                                         'events_patient_event held for this fact table. Read by the OMOP '
                                         'model and the OMOP id registry as a union over all feeds.'},
 '_events_feed_critical_care_activity': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                                  'journey_clinical.critical_care_activity: one row per '
                                                  'admitted event x standard mapping (unmapped events carry '
                                                  'one row with a null mapping block), identical to the rows '
                                                  'the retired events_patient_event held for this fact '
                                                  'table. Read by the OMOP model and the OMOP id registry as '
                                                  'a union over all feeds.'},
 '_events_feed_critical_care_admission': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                                   'journey_clinical.critical_care_admission: one row per '
                                                   'admitted event x standard mapping (unmapped events carry '
                                                   'one row with a null mapping block), identical to the '
                                                   'rows the retired events_patient_event held for this fact '
                                                   'table. Read by the OMOP model and the OMOP id registry '
                                                   'as a union over all feeds.'},
 '_events_feed_critical_care_daily_score': {'columns': {'confidentiality_code': 'Source confidentiality '
                                                                                'label.',
                                                        'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                                        'available.',
                                                        'event_datetime': 'Clinical or administrative event '
                                                                          'time.',
                                                        'event_datetime_local': 'event_datetime in '
                                                                                'Europe/London wall-clock '
                                                                                'time (GMT/BST). Millennium '
                                                                                'rows take the bronze '
                                                                                '*_LOCAL companions of the '
                                                                                'same source columns and '
                                                                                'fallback order; rows from '
                                                                                'local-clock sources (LUNA, '
                                                                                'PACS/DICOM, TFC LIMS, '
                                                                                'CCMDS, MSDS, BadgerNet, '
                                                                                'EndoBase, SLAM, iWeb, Datix '
                                                                                'and similar) carry '
                                                                                'event_datetime unchanged. '
                                                                                'event_datetime is a UTC '
                                                                                'instant for Millennium '
                                                                                'rows, so take local '
                                                                                'calendar dates and clock '
                                                                                'times from this column.',
                                                        'event_end_datetime': 'Event end when the source '
                                                                              'supplies one.',
                                                        'event_end_datetime_local': 'event_end_datetime in '
                                                                                    'Europe/London '
                                                                                    'wall-clock time '
                                                                                    '(GMT/BST), by the same '
                                                                                    'rule as '
                                                                                    'event_datetime_local; '
                                                                                    'null where '
                                                                                    'event_end_datetime is '
                                                                                    'null.',
                                                        'event_type': 'Fact kind (condition',
                                                        'fact_category': 'Clinical versus administrative '
                                                                         'fact.',
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
                                                                     "bronze row's write time. Mapping "
                                                                     'clocks do not contribute, and mapping '
                                                                     'fan-out does not calculate a new '
                                                                     'maximum. Later typed-product '
                                                                     'enrichment, such as implant attribute '
                                                                     'clocks or additional document-history '
                                                                     'rows, is not folded back into this '
                                                                     'event-index clock. No current Silver '
                                                                     'refresh timestamp is substituted.',
                                                        'map_candidate_count': 'Number of candidates for the '
                                                                               'event, coding system and '
                                                                               'mapping source.',
                                                        'map_confidence': 'Mapping confidence in [0,1] where '
                                                                          'the lane measured one.',
                                                        'map_loaded_at': 'loaded_at of the mapping row (null '
                                                                         'when the event has no mapping); '
                                                                         'the clock concept_registry uses '
                                                                         'for mapped codes.',
                                                        'map_method': 'How the mapping was reached: '
                                                                      'source_native or rule for legacy '
                                                                      'lanes; S3 lookup lanes retain their '
                                                                      'explicit method.',
                                                        'map_rule_id': 'Rule or lane identifier that '
                                                                       'produced this candidate.',
                                                        'map_source': 'Governed mapping product that '
                                                                      'produced the row.',
                                                        'map_version': 'Mapping content version.',
                                                        'mapped_code': 'Unique valid standard OMOP concept '
                                                                       'reached from exact ICD code '
                                                                       'semantics; NULL when absent or '
                                                                       'ambiguous.',
                                                        'mapped_coding_system': 'Standard coding system for '
                                                                                'this mapping row; null on '
                                                                                'the unmapped row.',
                                                        'mapped_display': 'Standard display or definition.',
                                                        'patient_event_key': 'Immutable deterministic '
                                                                             'fact-grain event key shared '
                                                                             "across a mapped event's N "
                                                                             'rows.',
                                                        'patient_event_row_key': 'Deterministic row key (the '
                                                                                 'mapping id for mapped '
                                                                                 'rows; a namespaced '
                                                                                 'unmapped id otherwise).',
                                                        'person_id': 'Resolved Millennium PERSON_ID as '
                                                                     'BIGINT when available.',
                                                        'record_status': 'Status label carried unchanged '
                                                                         'from the contributing canonical '
                                                                         'arm through internal_event_index '
                                                                         'and the public event-to-mapping '
                                                                         'left join. It is not a Boolean '
                                                                         'mirror of IS_CURRENT_IN_SOURCE or '
                                                                         'a verbatim closure flag: each arm '
                                                                         'supplies its own active, '
                                                                         'superseded or retracted recipe. '
                                                                         'Every mapping row for an event '
                                                                         'inherits the event-side status; '
                                                                         'mapping fields do not replace it. '
                                                                         'The input is the canonical '
                                                                         'event-index row, not a promise of '
                                                                         'equality with a later typed public '
                                                                         "product's history or "
                                                                         'latest-version selection.',
                                                        'source_code': 'Source code as recorded.',
                                                        'source_coding_system': 'The description for the '
                                                                                'code value',
                                                        'source_display': 'Source display as recorded.',
                                                        'source_object': 'Native source entity in plain '
                                                                         'language',
                                                        'source_row_key': 'Native source record key (BIGINT '
                                                                          'ids rendered as text); the typed '
                                                                          'native id lives on the fact '
                                                                          'table.',
                                                        'source_system': 'Plain-language source system '
                                                                         '(Millennium',
                                                        'subject_id_system': 'Identifier system behind '
                                                                             'subject_key.',
                                                        'subject_key': 'Deterministic SHA-256 over the '
                                                                       'strongest source identifier. Not '
                                                                       'salted, not secret; a stable join '
                                                                       'key across feeds where person_id is '
                                                                       'unresolved.',
                                                        'target_domain': 'Downstream routing domain for this '
                                                                         'mapping.',
                                                        'vip_ind': 'Source VIP indicator carried as data.',
                                                        'withheld_identity_ind': 'Source withheld-identity '
                                                                                 'indicator.'},
                                            'table': 'INTERNAL event feed for '
                                                     'journey_clinical.critical_care_daily_score: one row '
                                                     'per admitted event x standard mapping (unmapped events '
                                                     'carry one row with a null mapping block), identical to '
                                                     'the rows the retired events_patient_event held for '
                                                     'this fact table. Read by the OMOP model and the OMOP '
                                                     'id registry as a union over all feeds.'},
 '_events_feed_critical_care_period': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                                'journey_clinical.critical_care_period: one row per admitted '
                                                'event x standard mapping (unmapped events carry one row '
                                                'with a null mapping block), identical to the rows the '
                                                'retired events_patient_event held for this fact table. Read '
                                                'by the OMOP model and the OMOP id registry as a union over '
                                                'all feeds.'},
 '_events_feed_device': {'columns': {'confidentiality_code': 'Source confidentiality label.',
                                     'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                     'event_datetime': 'Clinical or administrative event time.',
                                     'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                             'time (GMT/BST). Millennium rows take the '
                                                             'bronze *_LOCAL companions of the same source '
                                                             'columns and fallback order; rows from '
                                                             'local-clock sources (LUNA, PACS/DICOM, TFC '
                                                             'LIMS, CCMDS, MSDS, BadgerNet, EndoBase, SLAM, '
                                                             'iWeb, Datix and similar) carry event_datetime '
                                                             'unchanged. event_datetime is a UTC instant for '
                                                             'Millennium rows, so take local calendar dates '
                                                             'and clock times from this column.',
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
                                                  'contributing canonical arm through internal_event_index, '
                                                  'then selected from the event side of the public mapping '
                                                  'join. Source arms have different clock expressions, '
                                                  'including typed nulls; this is not uniformly one bronze '
                                                  "row's write time. Mapping clocks do not contribute, and "
                                                  'mapping fan-out does not calculate a new maximum. Later '
                                                  'typed-product enrichment, such as implant attribute '
                                                  'clocks or additional document-history rows, is not folded '
                                                  'back into this event-index clock. No current Silver '
                                                  'refresh timestamp is substituted.',
                                     'map_candidate_count': 'Number of candidates for the event, coding '
                                                            'system and mapping source.',
                                     'map_confidence': 'Mapping confidence in [0,1] where the lane measured '
                                                       'one.',
                                     'map_loaded_at': 'loaded_at of the mapping row (null when the event has '
                                                      'no mapping); the clock concept_registry uses for '
                                                      'mapped codes.',
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
                                     'record_status': 'Status label carried unchanged from the contributing '
                                                      'canonical arm through internal_event_index and the '
                                                      'public event-to-mapping left join. It is not a '
                                                      'Boolean mirror of IS_CURRENT_IN_SOURCE or a verbatim '
                                                      'closure flag: each arm supplies its own active, '
                                                      'superseded or retracted recipe. Every mapping row for '
                                                      'an event inherits the event-side status; mapping '
                                                      'fields do not replace it. The input is the canonical '
                                                      'event-index row, not a promise of equality with a '
                                                      "later typed public product's history or "
                                                      'latest-version selection.',
                                     'source_code': 'Source code as recorded.',
                                     'source_coding_system': 'The description for the code value',
                                     'source_display': 'Source display as recorded.',
                                     'source_object': 'Native source entity in plain language',
                                     'source_row_key': 'Native source record key (BIGINT ids rendered as '
                                                       'text); the typed native id lives on the fact table.',
                                     'source_system': 'Plain-language source system (Millennium',
                                     'subject_id_system': 'Identifier system behind subject_key.',
                                     'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                    'identifier. Not salted, not secret; a stable join key '
                                                    'across feeds where person_id is unresolved.',
                                     'target_domain': 'Downstream routing domain for this mapping.',
                                     'vip_ind': 'Source VIP indicator carried as data.',
                                     'withheld_identity_ind': 'Source withheld-identity indicator.'},
                         'table': 'INTERNAL event feed for journey_clinical.device: one row per admitted '
                                  'event x standard mapping (unmapped events carry one row with a null '
                                  'mapping block), identical to the rows the retired events_patient_event '
                                  'held for this fact table. Read by the OMOP model and the OMOP id registry '
                                  'as a union over all feeds.'},
 '_events_feed_labour_delivery': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                  'table': 'INTERNAL event feed for journey_clinical.labour_delivery: one '
                                           'row per admitted event x standard mapping (unmapped events carry '
                                           'one row with a null mapping block), identical to the rows the '
                                           'retired events_patient_event held for this fact table. Read by '
                                           'the OMOP model and the OMOP id registry as a union over all '
                                           'feeds.'},
 '_events_feed_maternity_care_contact': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                                  'journey_clinical.maternity_care_contact: one row per '
                                                  'admitted event x standard mapping (unmapped events carry '
                                                  'one row with a null mapping block), identical to the rows '
                                                  'the retired events_patient_event held for this fact '
                                                  'table. Read by the OMOP model and the OMOP id registry as '
                                                  'a union over all feeds.'},
 '_events_feed_neonatal_care_day': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                    'table': 'INTERNAL event feed for journey_clinical.neonatal_care_day: '
                                             'one row per admitted event x standard mapping (unmapped events '
                                             'carry one row with a null mapping block), identical to the '
                                             'rows the retired events_patient_event held for this fact '
                                             'table. Read by the OMOP model and the OMOP id registry as a '
                                             'union over all feeds.'},
 '_events_feed_neonatal_episode': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                   'table': 'INTERNAL event feed for journey_clinical.neonatal_episode: one '
                                            'row per admitted event x standard mapping (unmapped events '
                                            'carry one row with a null mapping block), identical to the rows '
                                            'the retired events_patient_event held for this fact table. Read '
                                            'by the OMOP model and the OMOP id registry as a union over all '
                                            'feeds.'},
 '_events_feed_neonatal_examination': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                                'journey_clinical.neonatal_examination: one row per admitted '
                                                'event x standard mapping (unmapped events carry one row '
                                                'with a null mapping block), identical to the rows the '
                                                'retired events_patient_event held for this fact table. Read '
                                                'by the OMOP model and the OMOP id registry as a union over '
                                                'all feeds.'},
 '_events_feed_registry_entry': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                 'table': 'INTERNAL event feed for journey_clinical.registry_entry: one row '
                                          'per admitted event x standard mapping (unmapped events carry one '
                                          'row with a null mapping block), identical to the rows the retired '
                                          'events_patient_event held for this fact table. Read by the OMOP '
                                          'model and the OMOP id registry as a union over all feeds.'},
 '_events_feed_safety_incident': {'columns': {'confidentiality_code': 'Source confidentiality label.',
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
                                  'table': 'INTERNAL event feed for journey_clinical.safety_incident: one '
                                           'row per admitted event x standard mapping (unmapped events carry '
                                           'one row with a null mapping block), identical to the rows the '
                                           'retired events_patient_event held for this fact table. Read by '
                                           'the OMOP model and the OMOP id registry as a union over all '
                                           'feeds.'}}
