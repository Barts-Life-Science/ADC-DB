"""Gold clinical.research_enrollment: 2 dataset(s).

- datasets: _qc_clinical_research_enrollment, clinical_research_enrollment
- moved verbatim from: 2_gold_clinical_events:5570, 2_gold_clinical_events:5612, 2_gold_clinical_events:5620, 2_gold_clinical_events:5696, 2_gold_clinical_events:5700, 2_gold_clinical_events:5717

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.research_enrollment ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_RESEARCH_ENROLLMENT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`pt_prot_reg_id` AS `pt_prot_reg_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`research_study_key` AS `research_study_key`',
    '`registration_id` AS `registration_id`',
    '`protocol_accession_number` AS `protocol_accession_number`',
    '`protocol_arm_id` AS `protocol_arm_id`',
    '`status_code` AS `status_code`',
    '`status_desc` AS `status_desc`',
    '`off_study_datetime` AS `off_study_datetime`',
    '`treatment_start_datetime` AS `treatment_start_datetime`',
    '`treatment_completion_datetime` AS `treatment_completion_datetime`',
    '`removal_reason_desc` AS `removal_reason_desc`',
    "CASE WHEN TRIM(CAST(`removal_reason_text` AS STRING)) = '' THEN NULL ELSE `removal_reason_text` END AS `removal_reason_text`",
    '`off_treatment_reason_desc` AS `off_treatment_reason_desc`',
    "CASE WHEN TRIM(CAST(`off_treatment_reason_text` AS STRING)) = '' THEN NULL ELSE `off_treatment_reason_text` END AS `off_treatment_reason_text`",
    '`source_episode_id` AS `source_episode_id`',
    '`enrolling_organization_id` AS `enrolling_organization_id`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_research_enrollment_metadata; source history stays on the main research table.
CLINICAL_RESEARCH_ENROLLMENT_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 34,354 rows of 34,354 that
    # are current and attributable; identity_status = 'resolved' keeps the 34,354 rows of
    # 34,354 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_RESEARCH_ENROLLMENT_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.research_enrollment.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 34,354 at the profile.
    "gold.clinical.research_enrollment.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 34,354 at the profile.
    "gold.clinical.research_enrollment.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 24 of 34,354 rows (0.0699%) when profiled on 2026-08-24.
    "gold.clinical.research_enrollment.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 2 of 34,354 rows (0.00582%) when profiled on 2026-08-24.
    "gold.clinical.research_enrollment.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_RESEARCH_ENROLLMENT_PERSON_SCOPE_RULES = {
    "gold.clinical.research_enrollment.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_research_enrollment"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_RESEARCH_ENROLLMENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_RESEARCH_ENROLLMENT_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_RESEARCH_ENROLLMENT_PERSON_SCOPE_RULES)
def _gold_qc_clinical_research_enrollment():
    """Quality-controlled twin of journey_clinical.research_enrollment."""
    df = _qc(
        "clinical_research_enrollment",
        CLINICAL_RESEARCH_ENROLLMENT_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.research_enrollment"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_research_enrollment():
    """Contract-v2 public twin of clinical_research_enrollment; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_research_enrollment")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`pt_prot_reg_id` AS `pt_prot_reg_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`research_study_key` AS `research_study_key`',
        '`registration_id` AS `registration_id`',
        '`protocol_accession_number` AS `protocol_accession_number`',
        '`protocol_arm_id` AS `protocol_arm_id`',
        '`status_code` AS `status_code`',
        '`status_desc` AS `status_desc`',
        '`off_study_datetime` AS `off_study_datetime`',
        '`treatment_start_datetime` AS `treatment_start_datetime`',
        '`treatment_completion_datetime` AS `treatment_completion_datetime`',
        '`removal_reason_desc` AS `removal_reason_desc`',
        '`removal_reason_text` AS `removal_reason_text`',
        '`off_treatment_reason_desc` AS `off_treatment_reason_desc`',
        '`off_treatment_reason_text` AS `off_treatment_reason_text`',
        '`source_episode_id` AS `source_episode_id`',
        '`enrolling_organization_id` AS `enrolling_organization_id`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'clinical_research_enrollment': GoldRules(
        table='clinical_research_enrollment',
        select=CLINICAL_RESEARCH_ENROLLMENT_SELECT,
        mandatory=CLINICAL_RESEARCH_ENROLLMENT_MANDATORY_RULES,
        advisory=CLINICAL_RESEARCH_ENROLLMENT_ADVISORY_RULES,
        person_scope=CLINICAL_RESEARCH_ENROLLMENT_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth'),
        value_rules={'gold.clinical.research_enrollment.encounter_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                            'column': 'encounter_id',
                                                                            'companions': [],
                                                                            'match': '`encounter_id` = 0',
                                                                            'table': 'clinical_research_enrollment'},
         'gold.clinical.research_enrollment.protocol_arm_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                               'column': 'protocol_arm_id',
                                                                               'companions': [],
                                                                               'match': 'TRIM(`protocol_arm_id`) '
                                                                                        "= '0'",
                                                                               'table': 'clinical_research_enrollment'},
         'gold.clinical.research_enrollment.source_episode_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                 'column': 'source_episode_id',
                                                                                 'companions': [],
                                                                                 'match': 'TRIM(`source_episode_id`) '
                                                                                          "= '0'",
                                                                                 'table': 'clinical_research_enrollment'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_research_enrollment': {'columns': {'confidentiality_code': 'Source confidentiality '
                                                                          'classification attached to the '
                                                                          'record for each research '
                                                                          'enrollment record. It is produced '
                                                                          'by the silver transformation and '
                                                                          'has no direct bronze-column '
                                                                          'lineage entry. Source code '
                                                                          'meanings and sentinel values are '
                                                                          'retained unless the pipeline '
                                                                          'explicitly maps them; null means '
                                                                          'no code was supplied.',
                                                  'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT '
                                                                  'when available.',
                                                  'enrolling_organization_id': 'Source-system identifier for '
                                                                               'the enrolling organization '
                                                                               'associated with each '
                                                                               'research enrollment record. '
                                                                               'It is carried from bronze '
                                                                               'field '
                                                                               '`ENROLLING_ORGANIZATION_ID` '
                                                                               'in '
                                                                               '`4_prod.bronze.map_research_subject`. '
                                                                               'Its uniqueness is limited to '
                                                                               'the originating source '
                                                                               'namespace; null means no '
                                                                               'identifier was supplied.',
                                                  'event_datetime': 'Date and time when the represented '
                                                                    'clinical or administrative event '
                                                                    'occurred for each research enrollment '
                                                                    'record. It is derived from bronze field '
                                                                    '`ON_STUDY_DT_TM_CLEAN` in '
                                                                    '`4_prod.bronze.map_research_subject`. '
                                                                    'Source precision and timezone handling '
                                                                    'follow the pipeline expression; null '
                                                                    'means the time was unavailable.',
                                                  'event_datetime_local': 'Local-time representation of the '
                                                                          'research-subject registration '
                                                                          'event start date and time.',
                                                  'event_end_datetime': 'Date and time when the represented '
                                                                        'clinical or administrative event '
                                                                        'ended for each research enrollment '
                                                                        'record. It is derived from bronze '
                                                                        'field `OFF_STUDY_DT_TM_CLEAN` in '
                                                                        '`4_prod.bronze.map_research_subject`. '
                                                                        'Source precision and timezone '
                                                                        'handling follow the pipeline '
                                                                        'expression; null means the time was '
                                                                        'unavailable.',
                                                  'event_end_datetime_local': 'Local-time representation of '
                                                                              'the research-subject '
                                                                              'registration event end date '
                                                                              'and time.',
                                                  'fact_category': 'Contract category used to group the '
                                                                   'record with comparable clinical facts '
                                                                   'for each research enrollment record. It '
                                                                   'is produced by the silver transformation '
                                                                   'and has no direct bronze-column lineage '
                                                                   'entry. Whitespace and source sentinel '
                                                                   'text are retained unless the pipeline '
                                                                   'explicitly normalizes them; null means '
                                                                   'no value was supplied.',
                                                  'loaded_at': 'map_research_subject.ADC_UPDT carried '
                                                               'unchanged for the contributing '
                                                               'research-subject row. The study key is '
                                                               'derived without joining a study load clock. '
                                                               'This input load provenance is distinct from '
                                                               'PIPELINE_UPDT_DT_TM published as '
                                                               'source_update_timestamp and from Silver '
                                                               'refresh time.',
                                                  'off_study_datetime': 'Date and time associated with off '
                                                                        'study for the research enrollment '
                                                                        'record. It is derived from bronze '
                                                                        'field `OFF_STUDY_DT_TM_CLEAN` in '
                                                                        '`4_prod.bronze.map_research_subject`. '
                                                                        'Source precision and timezone '
                                                                        'handling follow the pipeline '
                                                                        'expression; null means the time was '
                                                                        'unavailable.',
                                                  'off_treatment_reason_desc': 'The description for the code '
                                                                               'value',
                                                  'off_treatment_reason_text': 'Textual reason patient is '
                                                                               'taken off treatment.',
                                                  'patient_event_key': 'Deterministic event key derived from '
                                                                       'source identifiers; retained as a '
                                                                       'stable join key.',
                                                  'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                               'resolved.',
                                                  'protocol_accession_number': 'This field contains the '
                                                                               "patient's accession number "
                                                                               '(order of enrollment) for '
                                                                               'this protocol.',
                                                  'protocol_arm_id': 'Source-system identifier for the '
                                                                     'protocol arm associated with each '
                                                                     'research enrollment record. It is '
                                                                     'derived from bronze field '
                                                                     '`PROT_ARM_ID` in '
                                                                     '`4_prod.bronze.map_research_subject`. '
                                                                     'Its uniqueness is limited to the '
                                                                     'originating source namespace; null '
                                                                     'means no identifier was supplied.',
                                                  'pt_prot_reg_id': 'Cerner PT_PROT_REG_ID; primary key of '
                                                                    'this table.',
                                                  'record_status': 'Always active in the current '
                                                                   'map_research_subject projection. No '
                                                                   'superseded or retracted status is '
                                                                   'emitted; STATUS_ENUM, '
                                                                   'off-study/treatment dates and removal '
                                                                   'reasons remain separate clinical fields '
                                                                   'and do not change this literal label.',
                                                  'record_status_effective_from': 'map_research_subject.ON_STUDY_DT_TM_CLEAN '
                                                                                  'carried unchanged. This '
                                                                                  'study-entry-time proxy is '
                                                                                  'not an independently '
                                                                                  'observed source-status '
                                                                                  'transition; there is no '
                                                                                  'registration, '
                                                                                  'treatment-start or '
                                                                                  'ingestion-time fallback.',
                                                  'record_status_effective_to': 'Always null as a TIMESTAMP '
                                                                                'in the current '
                                                                                'research-enrollment '
                                                                                'projection. '
                                                                                'OFF_STUDY_DT_TM_CLEAN and '
                                                                                'treatment/removal fields '
                                                                                'are not used as history '
                                                                                'ends; a null here must not '
                                                                                'be interpreted as evidence '
                                                                                'that study participation is '
                                                                                'ongoing.',
                                                  'registration_id': 'Source-system identifier for the '
                                                                     'registration associated with each '
                                                                     'research enrollment record. It is '
                                                                     'derived from bronze field `REG_ID` in '
                                                                     '`4_prod.bronze.map_research_subject`. '
                                                                     'Its uniqueness is limited to the '
                                                                     'originating source namespace; null '
                                                                     'means no identifier was supplied.',
                                                  'removal_reason_desc': 'The description for the code value',
                                                  'removal_reason_text': 'Textual reason patient is taken '
                                                                         'off study.',
                                                  'research_study_key': 'Deterministic SHA-256 key for the '
                                                                        'related research study.',
                                                  'source_code': 'Number to indicate the current status of '
                                                                 'the enrolled patient(1: On '
                                                                 'Study,2:OnTreatment, 3:Off '
                                                                 'Treatment,4-OnFollowup,5-OffStudy)',
                                                  'source_coding_system': 'Coding system or source namespace '
                                                                          'in which the source code is '
                                                                          'defined for each research '
                                                                          'enrollment record. It is produced '
                                                                          'by the silver transformation and '
                                                                          'has no direct bronze-column '
                                                                          'lineage entry. Source code '
                                                                          'meanings and sentinel values are '
                                                                          'retained unless the pipeline '
                                                                          'explicitly maps them; null means '
                                                                          'no code was supplied.',
                                                  'source_display': 'Human-readable label supplied by the '
                                                                    'source system for the source code for '
                                                                    'each research enrollment record. It is '
                                                                    'derived from bronze field `STATUS_DESC` '
                                                                    'in '
                                                                    '`4_prod.bronze.map_research_subject`. '
                                                                    'Whitespace and source sentinel text are '
                                                                    'retained unless the pipeline explicitly '
                                                                    'normalizes them; null means no value '
                                                                    'was supplied.',
                                                  'source_episode_id': 'Source-system identifier for the '
                                                                       'source episode associated with each '
                                                                       'research enrollment record. It is '
                                                                       'derived from bronze field '
                                                                       '`EPISODE_ID` in '
                                                                       '`4_prod.bronze.map_research_subject`. '
                                                                       'Its uniqueness is limited to the '
                                                                       'originating source namespace; null '
                                                                       'means no identifier was supplied.',
                                                  'source_feed': 'Originating data feed responsible for the '
                                                                 'record for each research enrollment '
                                                                 'record. It is produced by the silver '
                                                                 'transformation and has no direct '
                                                                 'bronze-column lineage entry. Whitespace '
                                                                 'and source sentinel text are retained '
                                                                 'unless the pipeline explicitly normalizes '
                                                                 'them; null means no value was supplied.',
                                                  'source_update_timestamp': 'Bronze pipeline processing '
                                                                             'timestamp carried from '
                                                                             '`PIPELINE_UPDT_DT_TM` in '
                                                                             '`4_prod.bronze.map_research_subject`. '
                                                                             'It records when bronze '
                                                                             'processed the source row, not '
                                                                             'a native clinical-system '
                                                                             'update timestamp, clinical '
                                                                             'event time, or the current '
                                                                             'Silver refresh time.',
                                                  'status_code': 'Number to indicate the current status of '
                                                                 'the enrolled patient(1: On '
                                                                 'Study,2:OnTreatment, 3:Off '
                                                                 'Treatment,4-OnFollowup,5-OffStudy)',
                                                  'status_desc': 'Value describing status desc for the '
                                                                 'research enrollment record. It is carried '
                                                                 'from bronze field `STATUS_DESC` in '
                                                                 '`4_prod.bronze.map_research_subject`. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                                  'subject_id_system': 'Identifier namespace used to '
                                                                       'interpret the subject identifier on '
                                                                       'this record for each research '
                                                                       'enrollment record. It is derived '
                                                                       'from bronze field `PERSON_ID` in '
                                                                       '`4_prod.bronze.map_research_subject`. '
                                                                       'Its uniqueness is limited to the '
                                                                       'originating source namespace; null '
                                                                       'means no identifier was supplied.',
                                                  'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                 'source identifier. Not salted, not secret; '
                                                                 'a stable join key across feeds where '
                                                                 'person_id is unresolved.',
                                                  'treatment_completion_datetime': 'Date and time associated '
                                                                                   'with treatment '
                                                                                   'completion for the '
                                                                                   'research enrollment '
                                                                                   'record. It is derived '
                                                                                   'from bronze field '
                                                                                   '`TX_COMPLETION_DT_TM_CLEAN` '
                                                                                   'in '
                                                                                   '`4_prod.bronze.map_research_subject`. '
                                                                                   'Source precision and '
                                                                                   'timezone handling follow '
                                                                                   'the pipeline expression; '
                                                                                   'null means the time was '
                                                                                   'unavailable.',
                                                  'treatment_start_datetime': 'Date and time associated with '
                                                                              'treatment start for the '
                                                                              'research enrollment record. '
                                                                              'It is derived from bronze '
                                                                              'field `TX_START_DT_TM_CLEAN` '
                                                                              'in '
                                                                              '`4_prod.bronze.map_research_subject`. '
                                                                              'Source precision and timezone '
                                                                              'handling follow the pipeline '
                                                                              'expression; null means the '
                                                                              'time was unavailable.',
                                                  'vip_ind': 'Indicator that the source record carries VIP '
                                                             'handling status for each research enrollment '
                                                             'record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Null means the source did not '
                                                             'state the indicator and must not be '
                                                             'interpreted as false.',
                                                  'withheld_identity_ind': 'Indicator that the source '
                                                                           'identity was deliberately '
                                                                           'withheld for each research '
                                                                           'enrollment record. It is '
                                                                           'produced by the silver '
                                                                           'transformation and has no direct '
                                                                           'bronze-column lineage entry. '
                                                                           'Null means the source did not '
                                                                           'state the indicator and must not '
                                                                           'be interpreted as false.'},
                                      'quantity_override': True,
                                      'table': 'Internal quality-controlled twin of '
                                               'clinical_research_enrollment: the Gold repairs, nulls and '
                                               'expectations are applied here and the published Gold MV '
                                               'reads this object. Materialized rather than temporary so the '
                                               'published MV can refresh incrementally.'},
 'clinical_research_enrollment': {'columns': {'confidentiality_code': 'Source confidentiality classification '
                                                                      'attached to the record for each '
                                                                      'research enrollment record. It is '
                                                                      'produced by the silver transformation '
                                                                      'and has no direct bronze-column '
                                                                      'lineage entry. Source code meanings '
                                                                      'and sentinel values are retained '
                                                                      'unless the pipeline explicitly maps '
                                                                      'them; null means no code was '
                                                                      'supplied.',
                                              'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                              'available.',
                                              'enrolling_organization_id': 'Source-system identifier for the '
                                                                           'enrolling organization '
                                                                           'associated with each research '
                                                                           'enrollment record. It is carried '
                                                                           'from bronze field '
                                                                           '`ENROLLING_ORGANIZATION_ID` in '
                                                                           '`4_prod.bronze.map_research_subject`. '
                                                                           'Its uniqueness is limited to the '
                                                                           'originating source namespace; '
                                                                           'null means no identifier was '
                                                                           'supplied.',
                                              'event_datetime': 'Date and time when the represented clinical '
                                                                'or administrative event occurred for each '
                                                                'research enrollment record. It is derived '
                                                                'from bronze field `ON_STUDY_DT_TM_CLEAN` in '
                                                                '`4_prod.bronze.map_research_subject`. '
                                                                'Source precision and timezone handling '
                                                                'follow the pipeline expression; null means '
                                                                'the time was unavailable.',
                                              'event_datetime_local': 'Local-time representation of the '
                                                                      'research-subject registration event '
                                                                      'start date and time.',
                                              'event_end_datetime': 'Date and time when the represented '
                                                                    'clinical or administrative event ended '
                                                                    'for each research enrollment record. It '
                                                                    'is derived from bronze field '
                                                                    '`OFF_STUDY_DT_TM_CLEAN` in '
                                                                    '`4_prod.bronze.map_research_subject`. '
                                                                    'Source precision and timezone handling '
                                                                    'follow the pipeline expression; null '
                                                                    'means the time was unavailable.',
                                              'event_end_datetime_local': 'Local-time representation of the '
                                                                          'research-subject registration '
                                                                          'event end date and time.',
                                              'fact_category': 'Contract category used to group the record '
                                                               'with comparable clinical facts for each '
                                                               'research enrollment record. It is produced '
                                                               'by the silver transformation and has no '
                                                               'direct bronze-column lineage entry. '
                                                               'Whitespace and source sentinel text are '
                                                               'retained unless the pipeline explicitly '
                                                               'normalizes them; null means no value was '
                                                               'supplied.',
                                              'loaded_at': 'map_research_subject.ADC_UPDT carried unchanged '
                                                           'for the contributing research-subject row. The '
                                                           'study key is derived without joining a study '
                                                           'load clock. This input load provenance is '
                                                           'distinct from PIPELINE_UPDT_DT_TM published as '
                                                           'source_update_timestamp and from Silver refresh '
                                                           'time.',
                                              'off_study_datetime': 'Date and time associated with off study '
                                                                    'for the research enrollment record. It '
                                                                    'is derived from bronze field '
                                                                    '`OFF_STUDY_DT_TM_CLEAN` in '
                                                                    '`4_prod.bronze.map_research_subject`. '
                                                                    'Source precision and timezone handling '
                                                                    'follow the pipeline expression; null '
                                                                    'means the time was unavailable.',
                                              'off_treatment_reason_desc': 'The description for the code '
                                                                           'value',
                                              'off_treatment_reason_text': 'Textual reason patient is taken '
                                                                           'off treatment.',
                                              'patient_event_key': 'Deterministic event key derived from '
                                                                   'source identifiers; retained as a stable '
                                                                   'join key.',
                                              'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                           'resolved.',
                                              'protocol_accession_number': 'This field contains the '
                                                                           "patient's accession number "
                                                                           '(order of enrollment) for this '
                                                                           'protocol.',
                                              'protocol_arm_id': 'Source-system identifier for the protocol '
                                                                 'arm associated with each research '
                                                                 'enrollment record. It is derived from '
                                                                 'bronze field `PROT_ARM_ID` in '
                                                                 '`4_prod.bronze.map_research_subject`. Its '
                                                                 'uniqueness is limited to the originating '
                                                                 'source namespace; null means no identifier '
                                                                 'was supplied.',
                                              'pt_prot_reg_id': 'Cerner PT_PROT_REG_ID; primary key of this '
                                                                'table.',
                                              'record_status': 'Always active in the current '
                                                               'map_research_subject projection. No '
                                                               'superseded or retracted status is emitted; '
                                                               'STATUS_ENUM, off-study/treatment dates and '
                                                               'removal reasons remain separate clinical '
                                                               'fields and do not change this literal label.',
                                              'record_status_effective_from': 'map_research_subject.ON_STUDY_DT_TM_CLEAN '
                                                                              'carried unchanged. This '
                                                                              'study-entry-time proxy is not '
                                                                              'an independently observed '
                                                                              'source-status transition; '
                                                                              'there is no registration, '
                                                                              'treatment-start or '
                                                                              'ingestion-time fallback.',
                                              'record_status_effective_to': 'Always null as a TIMESTAMP in '
                                                                            'the current research-enrollment '
                                                                            'projection. '
                                                                            'OFF_STUDY_DT_TM_CLEAN and '
                                                                            'treatment/removal fields are '
                                                                            'not used as history ends; a '
                                                                            'null here must not be '
                                                                            'interpreted as evidence that '
                                                                            'study participation is ongoing.',
                                              'registration_id': 'Source-system identifier for the '
                                                                 'registration associated with each research '
                                                                 'enrollment record. It is derived from '
                                                                 'bronze field `REG_ID` in '
                                                                 '`4_prod.bronze.map_research_subject`. Its '
                                                                 'uniqueness is limited to the originating '
                                                                 'source namespace; null means no identifier '
                                                                 'was supplied.',
                                              'removal_reason_desc': 'The description for the code value',
                                              'removal_reason_text': 'Textual reason patient is taken off '
                                                                     'study.',
                                              'research_study_key': 'Deterministic SHA-256 key for the '
                                                                    'related research study.',
                                              'source_code': 'Number to indicate the current status of the '
                                                             'enrolled patient(1: On Study,2:OnTreatment, '
                                                             '3:Off Treatment,4-OnFollowup,5-OffStudy)',
                                              'source_coding_system': 'Coding system or source namespace in '
                                                                      'which the source code is defined for '
                                                                      'each research enrollment record. It '
                                                                      'is produced by the silver '
                                                                      'transformation and has no direct '
                                                                      'bronze-column lineage entry. Source '
                                                                      'code meanings and sentinel values are '
                                                                      'retained unless the pipeline '
                                                                      'explicitly maps them; null means no '
                                                                      'code was supplied.',
                                              'source_display': 'Human-readable label supplied by the source '
                                                                'system for the source code for each '
                                                                'research enrollment record. It is derived '
                                                                'from bronze field `STATUS_DESC` in '
                                                                '`4_prod.bronze.map_research_subject`. '
                                                                'Whitespace and source sentinel text are '
                                                                'retained unless the pipeline explicitly '
                                                                'normalizes them; null means no value was '
                                                                'supplied.',
                                              'source_episode_id': 'Source-system identifier for the source '
                                                                   'episode associated with each research '
                                                                   'enrollment record. It is derived from '
                                                                   'bronze field `EPISODE_ID` in '
                                                                   '`4_prod.bronze.map_research_subject`. '
                                                                   'Its uniqueness is limited to the '
                                                                   'originating source namespace; null means '
                                                                   'no identifier was supplied.',
                                              'source_feed': 'Originating data feed responsible for the '
                                                             'record for each research enrollment record. It '
                                                             'is produced by the silver transformation and '
                                                             'has no direct bronze-column lineage entry. '
                                                             'Whitespace and source sentinel text are '
                                                             'retained unless the pipeline explicitly '
                                                             'normalizes them; null means no value was '
                                                             'supplied.',
                                              'source_update_timestamp': 'Bronze pipeline processing '
                                                                         'timestamp carried from '
                                                                         '`PIPELINE_UPDT_DT_TM` in '
                                                                         '`4_prod.bronze.map_research_subject`. '
                                                                         'It records when bronze processed '
                                                                         'the source row, not a native '
                                                                         'clinical-system update timestamp, '
                                                                         'clinical event time, or the '
                                                                         'current Silver refresh time.',
                                              'status_code': 'Number to indicate the current status of the '
                                                             'enrolled patient(1: On Study,2:OnTreatment, '
                                                             '3:Off Treatment,4-OnFollowup,5-OffStudy)',
                                              'status_desc': 'Value describing status desc for the research '
                                                             'enrollment record. It is carried from bronze '
                                                             'field `STATUS_DESC` in '
                                                             '`4_prod.bronze.map_research_subject`. '
                                                             'Whitespace and source sentinel text are '
                                                             'retained unless the pipeline explicitly '
                                                             'normalizes them; null means no value was '
                                                             'supplied.',
                                              'subject_id_system': 'Identifier namespace used to interpret '
                                                                   'the subject identifier on this record '
                                                                   'for each research enrollment record. It '
                                                                   'is derived from bronze field `PERSON_ID` '
                                                                   'in `4_prod.bronze.map_research_subject`. '
                                                                   'Its uniqueness is limited to the '
                                                                   'originating source namespace; null means '
                                                                   'no identifier was supplied.',
                                              'subject_key': 'Deterministic SHA-256 over the strongest '
                                                             'source identifier. Not salted, not secret; a '
                                                             'stable join key across feeds where person_id '
                                                             'is unresolved.',
                                              'treatment_completion_datetime': 'Date and time associated '
                                                                               'with treatment completion '
                                                                               'for the research enrollment '
                                                                               'record. It is derived from '
                                                                               'bronze field '
                                                                               '`TX_COMPLETION_DT_TM_CLEAN` '
                                                                               'in '
                                                                               '`4_prod.bronze.map_research_subject`. '
                                                                               'Source precision and '
                                                                               'timezone handling follow the '
                                                                               'pipeline expression; null '
                                                                               'means the time was '
                                                                               'unavailable.',
                                              'treatment_start_datetime': 'Date and time associated with '
                                                                          'treatment start for the research '
                                                                          'enrollment record. It is derived '
                                                                          'from bronze field '
                                                                          '`TX_START_DT_TM_CLEAN` in '
                                                                          '`4_prod.bronze.map_research_subject`. '
                                                                          'Source precision and timezone '
                                                                          'handling follow the pipeline '
                                                                          'expression; null means the time '
                                                                          'was unavailable.',
                                              'vip_ind': 'Indicator that the source record carries VIP '
                                                         'handling status for each research enrollment '
                                                         'record. It is produced by the silver '
                                                         'transformation and has no direct bronze-column '
                                                         'lineage entry. Null means the source did not state '
                                                         'the indicator and must not be interpreted as '
                                                         'false.',
                                              'withheld_identity_ind': 'Indicator that the source identity '
                                                                       'was deliberately withheld for each '
                                                                       'research enrollment record. It is '
                                                                       'produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Null '
                                                                       'means the source did not state the '
                                                                       'indicator and must not be '
                                                                       'interpreted as false.'},
                                  'quantity_override': False,
                                  'table': 'One Cerner research-subject registration (PT_PROT_REG) with '
                                           'clean dates. Cerner is not the research system of record: '
                                           'registrations from 2023 are about 5% of the 2015-2019 rate (see '
                                           'the coverage register). Coverage: Cerner research registrations '
                                           'from 2023 are about 5% of the 2015-2019 rate at source; Cerner '
                                           'is not the research system of record; complete-through month per '
                                           'release: 6_mgmt.data_quality.release_finding (finding_code '
                                           'F2_complete_through).'}}
