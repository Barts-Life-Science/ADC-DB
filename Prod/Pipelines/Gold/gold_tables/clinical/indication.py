"""Gold clinical.indication: 2 dataset(s).

- datasets: _qc_clinical_indication, clinical_indication
- moved verbatim from: 2_gold_clinical_events:3067, 2_gold_clinical_events:3115, 2_gold_clinical_events:3124, 2_gold_clinical_events:3208, 2_gold_clinical_events:3213, 2_gold_clinical_events:3218, 2_gold_clinical_events:3240

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.indication ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_INDICATION_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`indication_id` AS `indication_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`encounter_link_method` AS `encounter_link_method`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`relation_type` AS `relation_type`',
    '`source_field` AS `source_field`',
    '`source_text` AS `source_text`',
    '`evidence_text` AS `evidence_text`',
    '`evidence_start` AS `evidence_start`',
    '`evidence_end` AS `evidence_end`',
    '`snomed_code` AS `snomed_code`',
    '`snomed_term` AS `snomed_term`',
    '`omop_concept_id` AS `omop_concept_id`',
    '`assertion` AS `assertion`',
    '`temporality` AS `temporality`',
    '`experiencer` AS `experiencer`',
    '`rule_id` AS `rule_id`',
    '`rule_version` AS `rule_version`',
    '`confidence` AS `confidence`',
    '`mapping_status` AS `mapping_status`',
    '`ig_release_status` AS `ig_release_status`',
    '`is_current` AS `is_current`',
    '`research_qi_only` AS `research_qi_only`',
    '`specimen_key` AS `specimen_key`',
    '`accession_identifier` AS `accession_identifier`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_indication_metadata; source history stays on the main research table.
CLINICAL_INDICATION_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 3,282,008,054 rows of
    # 3,282,008,054 that are current and attributable; identity_status = 'resolved' keeps
    # the 3,282,007,974 rows of 3,282,008,054 that are current and attributable. Superseded
    # versions and rows whose identity was never resolved are not research data, and a
    # consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_INDICATION_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.indication.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 80 of 3,282,008,054 at the profile.
    "gold.clinical.indication.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 3,282,008,054 at the profile.
    "gold.clinical.indication.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 4,323,749 of 3,282,008,054 rows (0.132%) when profiled on
    # 2026-08-24.
    "gold.clinical.indication.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 268,150 of 3,282,008,054 rows (0.00817%) when profiled on
    # 2026-08-24.
    "gold.clinical.indication.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_INDICATION_PERSON_SCOPE_RULES = {
    "gold.clinical.indication.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_INDICATION_ORPHAN_RULES = {
    "gold.clinical.indication.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
    "gold.clinical.indication.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_indication"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_INDICATION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_INDICATION_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_INDICATION_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_INDICATION_ORPHAN_RULES)
def _gold_qc_clinical_indication():
    """Quality-controlled twin of journey_clinical.indication."""
    # 84 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_indication",
        CLINICAL_INDICATION_SELECT,
        fk_columns=["person_id", "encounter_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.indication"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_indication():
    """Contract-v2 public twin of clinical_indication; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_indication")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`indication_id` AS `indication_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`encounter_link_method` AS `encounter_link_method`',  # GOLD_COLUMN_PARITY_V1
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`relation_type` AS `relation_type`',
        '`source_field` AS `source_field`',
        '`source_text` AS `source_text`',
        '`evidence_text` AS `evidence_text`',
        '`evidence_start` AS `evidence_start`',
        '`evidence_end` AS `evidence_end`',
        '`snomed_code` AS `snomed_code`',
        '`snomed_term` AS `snomed_term`',
        '`omop_concept_id` AS `omop_concept_id`',
        '`assertion` AS `assertion`',
        '`temporality` AS `temporality`',
        '`experiencer` AS `experiencer`',
        '`rule_id` AS `rule_id`',
        '`rule_version` AS `rule_version`',
        '`confidence` AS `confidence`',
        '`mapping_status` AS `mapping_status`',
        '`ig_release_status` AS `ig_release_status`',
        '`is_current` AS `is_current`',
        '`research_qi_only` AS `research_qi_only`',
        '`specimen_key` AS `specimen_key`',
        '`accession_identifier` AS `accession_identifier`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'clinical_indication': GoldRules(
        table='clinical_indication',
        select=CLINICAL_INDICATION_SELECT,
        mandatory=CLINICAL_INDICATION_MANDATORY_RULES,
        advisory=CLINICAL_INDICATION_ADVISORY_RULES,
        person_scope=CLINICAL_INDICATION_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth')),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_indication': {'columns': {'accession_identifier': 'Alias-resolved accession identifier.',
                                         'assertion': 'present, absent, possible, family_history, or '
                                                      'unknown.',
                                         'confidence': 'Confidence value supplied by the pathology '
                                                       'indication rule, carried unchanged from bronze '
                                                       '`confidence`; Silver does not recalibrate this '
                                                       'value.',
                                         'confidentiality_code': 'Source confidentiality classification '
                                                                 'attached to the record for each indication '
                                                                 'record. It is produced by the silver '
                                                                 'transformation and has no direct '
                                                                 'bronze-column lineage entry. Source code '
                                                                 'meanings and sentinel values are retained '
                                                                 'unless the pipeline explicitly maps them; '
                                                                 'null means no code was supplied.',
                                         'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                         'encounter_link_method': 'How encounter_id was set '
                                                                  '(PATH_ENCOUNTER_V1). source_accession: '
                                                                  "the row's own Millennium source names the "
                                                                  'encounter. accession_single: every '
                                                                  'current Millennium source of the '
                                                                  'alias-resolved accession names this one '
                                                                  'encounter (how TFC LIMS rows inherit one '
                                                                  'once their accession is merged with the '
                                                                  'Millennium order). accession_multiple: '
                                                                  'those sources name different encounters, '
                                                                  'so encounter_id is NULL. none: the '
                                                                  'accession has no Millennium source, so '
                                                                  'encounter_id is NULL. Gold: encounter_id '
                                                                  'can still be NULL here when the encounter '
                                                                  'is not in the Gold encounter spine.',
                                         'event_datetime': 'Accession report/sample/request fallback time.',
                                         'event_datetime_local': 'Local-time event timestamp, taken from the '
                                                                 'accession report, sample or request local '
                                                                 'time as a fallback. Gold: NULL wherever '
                                                                 'Gold nulls event_datetime.',
                                         'event_end_datetime': 'Event end time.',
                                         'event_end_datetime_local': 'Local-time equivalent of the event end '
                                                                     'timestamp.',
                                         'evidence_end': 'Evidence end offset.',
                                         'evidence_start': 'Evidence start offset.',
                                         'evidence_text': 'Exact evidence span.',
                                         'experiencer': 'patient, family, or unknown.',
                                         'ig_release_status': 'IG release status published verbatim.',
                                         'indication_id': 'Pathology indication_id; primary key of this '
                                                          'table.',
                                         'is_current': 'Whether evidence remains current.',
                                         'loaded_at': 'map_pathology_indication.ADC_UPDT carried unchanged '
                                                      'through its joins, usable-code filtering and '
                                                      'source/public projections. Alias resolution and '
                                                      'accession identity add no load clocks; neither joined '
                                                      'report/parent timestamps nor the separately '
                                                      'aggregated accession evidence clock replace this '
                                                      'value. It is identical to source_update_timestamp, '
                                                      'not the clinical event time or Silver refresh time.',
                                         'mapping_status': 'Source mapping status.',
                                         'omop_concept_id': 'Standard OMOP condition concept.',
                                         'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                              'cross-feed join key.',
                                         'person_id': 'Resolved Millennium PERSON_ID as BIGINT through the '
                                                      'shared pathology identity projection.',
                                         'record_status': 'Always active in the current '
                                                          'map_pathology_indication projection. is_current, '
                                                          'assertion, temporality, experiencer and '
                                                          'mapping/release status do not change this literal '
                                                          'label; usable-code filtering separately controls '
                                                          'publication eligibility.',
                                         'record_status_effective_from': 'Always null as a TIMESTAMP in the '
                                                                         'current indication projection. No '
                                                                         'source-status start is supplied '
                                                                         "here; the clinical event's "
                                                                         'report/sample/request date '
                                                                         'fallbacks and ADC_UPDT are not '
                                                                         'used for this field.',
                                         'record_status_effective_to': 'Always null as a TIMESTAMP in the '
                                                                       'current indication projection, '
                                                                       'including when its status or '
                                                                       'source-current flag changes. No '
                                                                       'report, clinical-event or ingestion '
                                                                       'timestamp is substituted as a status '
                                                                       'end.',
                                         'relation_type': 'Evidence relation type.',
                                         'research_qi_only': 'Research/QI release flag.',
                                         'rule_id': 'Deterministic rule identifier.',
                                         'rule_version': 'Deterministic rule version.',
                                         'snomed_code': 'SNOMED condition code.',
                                         'snomed_term': 'SNOMED display term.',
                                         'source_code': 'Coded diagnosis display string.',
                                         'source_coding_system': 'Indication text namespace.',
                                         'source_display': 'Coded diagnosis display string.',
                                         'source_feed': 'Originating data feed responsible for the record '
                                                        'for each indication record. It is produced by the '
                                                        'silver transformation and has no direct '
                                                        'bronze-column lineage entry. Whitespace and source '
                                                        'sentinel text are retained unless the pipeline '
                                                        'explicitly normalizes them; null means no value was '
                                                        'supplied.',
                                         'source_field': 'Source evidence field.',
                                         'source_text': 'Unmodified coded diagnosis display string.',
                                         'source_update_timestamp': 'map_pathology_indication.ADC_UPDT '
                                                                    'carried unchanged from the contributing '
                                                                    'row, identical to loaded_at. No '
                                                                    'independent native application-update '
                                                                    'clock is projected here, and Silver '
                                                                    'refresh time is not substituted.',
                                         'specimen_key': 'Deterministic specimen key.',
                                         'subject_id_system': 'Identifier namespace selected for '
                                                              '`subject_key`: Millennium person identifier '
                                                              'first, then MRN, then NHS number; nosubject '
                                                              'denotes the per-source-record fallback when '
                                                              'none is present.',
                                         'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                        'identifier. Not salted, not secret; a stable join '
                                                        'key across feeds where person_id is unresolved.',
                                         'temporality': 'current, historical, future, or unknown.',
                                         'vip_ind': 'Indicator that the source record carries VIP handling '
                                                    'status for each indication record. It is produced by '
                                                    'the silver transformation and has no direct '
                                                    'bronze-column lineage entry. Null means the source did '
                                                    'not state the indicator and must not be interpreted as '
                                                    'false.',
                                         'withheld_identity_ind': 'Withheld identity indicator.'},
                             'quantity_override': True,
                             'table': 'Internal quality-controlled twin of clinical_indication: the Gold '
                                      'repairs, nulls and expectations are applied here and the published '
                                      'Gold MV reads this object. Materialized rather than temporary so the '
                                      'published MV can refresh incrementally.'},
 'clinical_indication': {'columns': {'accession_identifier': 'Alias-resolved accession identifier.',
                                     'assertion': 'present, absent, possible, family_history, or unknown.',
                                     'confidence': 'Confidence value supplied by the pathology indication '
                                                   'rule, carried unchanged from bronze `confidence`; Silver '
                                                   'does not recalibrate this value.',
                                     'confidentiality_code': 'Source confidentiality classification attached '
                                                             'to the record for each indication record. It '
                                                             'is produced by the silver transformation and '
                                                             'has no direct bronze-column lineage entry. '
                                                             'Source code meanings and sentinel values are '
                                                             'retained unless the pipeline explicitly maps '
                                                             'them; null means no code was supplied.',
                                     'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                     'encounter_link_method': 'How encounter_id was set (PATH_ENCOUNTER_V1). '
                                                              "source_accession: the row's own Millennium "
                                                              'source names the encounter. accession_single: '
                                                              'every current Millennium source of the '
                                                              'alias-resolved accession names this one '
                                                              'encounter (how TFC LIMS rows inherit one once '
                                                              'their accession is merged with the Millennium '
                                                              'order). accession_multiple: those sources '
                                                              'name different encounters, so encounter_id is '
                                                              'NULL. none: the accession has no Millennium '
                                                              'source, so encounter_id is NULL. Gold: '
                                                              'encounter_id can still be NULL here when the '
                                                              'encounter is not in the Gold encounter spine.',
                                     'event_datetime': 'Accession report/sample/request fallback time.',
                                     'event_datetime_local': 'Local-time event timestamp, taken from the '
                                                             'accession report, sample or request local time '
                                                             'as a fallback. Gold: NULL wherever Gold nulls '
                                                             'event_datetime.',
                                     'event_end_datetime': 'Event end time.',
                                     'event_end_datetime_local': 'Local-time equivalent of the event end '
                                                                 'timestamp.',
                                     'evidence_end': 'Evidence end offset.',
                                     'evidence_start': 'Evidence start offset.',
                                     'evidence_text': 'Exact evidence span.',
                                     'experiencer': 'patient, family, or unknown.',
                                     'ig_release_status': 'IG release status published verbatim.',
                                     'indication_id': 'Pathology indication_id; primary key of this table.',
                                     'is_current': 'Whether evidence remains current.',
                                     'loaded_at': 'map_pathology_indication.ADC_UPDT carried unchanged '
                                                  'through its joins, usable-code filtering and '
                                                  'source/public projections. Alias resolution and accession '
                                                  'identity add no load clocks; neither joined report/parent '
                                                  'timestamps nor the separately aggregated accession '
                                                  'evidence clock replace this value. It is identical to '
                                                  'source_update_timestamp, not the clinical event time or '
                                                  'Silver refresh time.',
                                     'mapping_status': 'Source mapping status.',
                                     'omop_concept_id': 'Standard OMOP condition concept.',
                                     'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                          'cross-feed join key.',
                                     'person_id': 'Resolved Millennium PERSON_ID as BIGINT through the '
                                                  'shared pathology identity projection.',
                                     'record_status': 'Always active in the current map_pathology_indication '
                                                      'projection. is_current, assertion, temporality, '
                                                      'experiencer and mapping/release status do not change '
                                                      'this literal label; usable-code filtering separately '
                                                      'controls publication eligibility.',
                                     'record_status_effective_from': 'Always null as a TIMESTAMP in the '
                                                                     'current indication projection. No '
                                                                     'source-status start is supplied here; '
                                                                     "the clinical event's "
                                                                     'report/sample/request date fallbacks '
                                                                     'and ADC_UPDT are not used for this '
                                                                     'field.',
                                     'record_status_effective_to': 'Always null as a TIMESTAMP in the '
                                                                   'current indication projection, including '
                                                                   'when its status or source-current flag '
                                                                   'changes. No report, clinical-event or '
                                                                   'ingestion timestamp is substituted as a '
                                                                   'status end.',
                                     'relation_type': 'Evidence relation type.',
                                     'research_qi_only': 'Research/QI release flag.',
                                     'rule_id': 'Deterministic rule identifier.',
                                     'rule_version': 'Deterministic rule version.',
                                     'snomed_code': 'SNOMED condition code.',
                                     'snomed_term': 'SNOMED display term.',
                                     'source_code': 'Coded diagnosis display string.',
                                     'source_coding_system': 'Indication text namespace.',
                                     'source_display': 'Coded diagnosis display string.',
                                     'source_feed': 'Originating data feed responsible for the record for '
                                                    'each indication record. It is produced by the silver '
                                                    'transformation and has no direct bronze-column lineage '
                                                    'entry. Whitespace and source sentinel text are retained '
                                                    'unless the pipeline explicitly normalizes them; null '
                                                    'means no value was supplied.',
                                     'source_field': 'Source evidence field.',
                                     'source_text': 'Unmodified coded diagnosis display string.',
                                     'source_update_timestamp': 'map_pathology_indication.ADC_UPDT carried '
                                                                'unchanged from the contributing row, '
                                                                'identical to loaded_at. No independent '
                                                                'native application-update clock is '
                                                                'projected here, and Silver refresh time is '
                                                                'not substituted.',
                                     'specimen_key': 'Deterministic specimen key.',
                                     'subject_id_system': 'Identifier namespace selected for `subject_key`: '
                                                          'Millennium person identifier first, then MRN, '
                                                          'then NHS number; nosubject denotes the '
                                                          'per-source-record fallback when none is present.',
                                     'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                    'identifier. Not salted, not secret; a stable join key '
                                                    'across feeds where person_id is unresolved.',
                                     'temporality': 'current, historical, future, or unknown.',
                                     'vip_ind': 'Indicator that the source record carries VIP handling '
                                                'status for each indication record. It is produced by the '
                                                'silver transformation and has no direct bronze-column '
                                                'lineage entry. Null means the source did not state the '
                                                'indicator and must not be interpreted as false.',
                                     'withheld_identity_ind': 'Withheld identity indicator.'},
                         'quantity_override': False,
                         'table': 'Accession-scoped diagnosis-context evidence from '
                                  'diagnosis_context_window_v1. Evidence spans are the complete coded '
                                  'diagnosis display string; event time uses accession report_dt, sample_dt, '
                                  'then request_dt. Text is ig_risk 4, ig_severity 2; ig_release_status is '
                                  'describe-only and prod activation is gated. Gold QC twin of the silver '
                                  'product: 3 columns are repaired or nulled, 1 rule(s) drop rows, 4 '
                                  'check(s) are advisory. Each rule states its reason in the pipeline '
                                  'notebook, and Lakeflow expectation metrics report what every rule matched '
                                  'on each update.'}}
