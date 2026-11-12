"""Gold clinical.endoscopy_finding: 2 dataset(s).

- datasets: _qc_clinical_endoscopy_finding, clinical_endoscopy_finding
- moved verbatim from: 2_gold_clinical_events:1436, 2_gold_clinical_events:1475, 2_gold_clinical_events:1482, 2_gold_clinical_events:1538, 2_gold_clinical_events:1542, 2_gold_clinical_events:1555

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.endoscopy_finding ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_ENDOSCOPY_FINDING_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`endobase_exam_term_id` AS `endobase_exam_term_id`',
    '`endobase_exam_id` AS `endobase_exam_id`',
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
    '`finding_code` AS `finding_code`',
    '`endoscopy_exam_event_key` AS `endoscopy_exam_event_key`',
    '`section_id` AS `section_id`',
    '`subsection_id` AS `subsection_id`',
    '`parent_term_id` AS `parent_term_id`',
    '`display_order` AS `display_order`',
    '`confirmed_ind` AS `confirmed_ind`',
    '`text_changed_ind` AS `text_changed_ind`',
    '`free_text_ind` AS `free_text_ind`',
    '`term_mapping_status` AS `term_mapping_status`',
    '`person_link_status` AS `person_link_status`',
    '`authored_datetime` AS `authored_datetime`',
    '`event_time_source` AS `event_time_source`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_endoscopy_finding_metadata; source history stays on the main research table.
CLINICAL_ENDOSCOPY_FINDING_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 9,220,706 rows of 9,220,706
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}


CLINICAL_ENDOSCOPY_FINDING_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.endoscopy_finding.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Warned rather than filtered on identity_status = 'resolved': Endobase has no sound
    # person linkage; the mandatory predicate would empty the 9,220,706-row product.
    "gold.clinical.endoscopy_finding.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 9,220,706 at the profile.
    "gold.clinical.endoscopy_finding.record_status.default_view_active":
        "record_status = 'active'",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_ENDOSCOPY_FINDING_PERSON_SCOPE_RULES = {
    "gold.clinical.endoscopy_finding.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_endoscopy_finding"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_ENDOSCOPY_FINDING_MANDATORY_RULES)
@dp.expect_all(CLINICAL_ENDOSCOPY_FINDING_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_ENDOSCOPY_FINDING_PERSON_SCOPE_RULES)
def _gold_qc_clinical_endoscopy_finding():
    """Quality-controlled twin of journey_clinical.endoscopy_finding."""
    df = _qc("clinical_endoscopy_finding", CLINICAL_ENDOSCOPY_FINDING_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.endoscopy_finding"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_endoscopy_finding():
    """Contract-v2 public twin of clinical_endoscopy_finding; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_endoscopy_finding")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`endobase_exam_term_id` AS `endobase_exam_term_id`',
        '`endobase_exam_id` AS `endobase_exam_id`',
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
        '`__gold_json_finding_code` AS `finding_code`',
        '`endoscopy_exam_event_key` AS `endoscopy_exam_event_key`',
        '`section_id` AS `section_id`',
        '`subsection_id` AS `subsection_id`',
        '`parent_term_id` AS `parent_term_id`',
        '`display_order` AS `display_order`',
        '`confirmed_ind` AS `confirmed_ind`',
        '`text_changed_ind` AS `text_changed_ind`',
        '`free_text_ind` AS `free_text_ind`',
        '`term_mapping_status` AS `term_mapping_status`',
        '`person_link_status` AS `person_link_status`',
        '`authored_datetime` AS `authored_datetime`',
        '`event_time_source` AS `event_time_source`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _s3_flatten_gold_public(df, "clinical_endoscopy_finding")


def _public_steps_clinical_endoscopy_finding(df):
    """clinical_endoscopy_finding: the public body after its projection (GoldRules.public_select), moved from gold_clinical_endoscopy_finding; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_endoscopy_finding')


RULES = {
    'clinical_endoscopy_finding': GoldRules(
        table='clinical_endoscopy_finding',
        select=CLINICAL_ENDOSCOPY_FINDING_SELECT,
        mandatory=CLINICAL_ENDOSCOPY_FINDING_MANDATORY_RULES,
        advisory=CLINICAL_ENDOSCOPY_FINDING_ADVISORY_RULES,
        person_scope=CLINICAL_ENDOSCOPY_FINDING_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        variant_columns=('finding_code',),
        s3_axis_specs={'finding_code': [('finding', False)]},
        value_rules={'gold.clinical.endoscopy_finding.encounter_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                          'column': 'encounter_id',
                                                                          'companions': [],
                                                                          'match': '`encounter_id` = 0',
                                                                          'table': 'clinical_endoscopy_finding'},
         'gold.clinical.endoscopy_finding.finding_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                       'column': 'finding_snomed_code',
                                                                                       'companions': ['finding_snomed_display',
                                                                                                      'finding_target_code',
                                                                                                      'finding_target_display'],
                                                                                       'match': '`finding_snomed_code` '
                                                                                                'IS NOT '
                                                                                                'NULL AND '
                                                                                                '(NOT '
                                                                                                '`finding_snomed_code` '
                                                                                                'RLIKE '
                                                                                                "'^[1-9][0-9]{5,17}$' "
                                                                                                'OR '
                                                                                                'substring(`finding_snomed_code`, '
                                                                                                'length(`finding_snomed_code`) '
                                                                                                '- 2, 2) '
                                                                                                'NOT IN '
                                                                                                "('00', "
                                                                                                "'10') OR "
                                                                                                '(substring(`finding_snomed_code`, '
                                                                                                'length(`finding_snomed_code`) '
                                                                                                '- 2, 2) = '
                                                                                                "'10' AND "
                                                                                                'length(`finding_snomed_code`) '
                                                                                                '< 11))',
                                                                                       'table': 'clinical_endoscopy_finding'},
         'gold.clinical.endoscopy_finding.finding_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                       'column': 'finding_target_code',
                                                                                       'companions': ['finding_target_display',
                                                                                                      'finding_snomed_code',
                                                                                                      'finding_snomed_display'],
                                                                                       'match': '`finding_target_system` '
                                                                                                '= '
                                                                                                "'http://snomed.info/sct' "
                                                                                                'AND '
                                                                                                '`finding_target_code` '
                                                                                                'IS NOT '
                                                                                                'NULL AND '
                                                                                                '(NOT '
                                                                                                '`finding_target_code` '
                                                                                                'RLIKE '
                                                                                                "'^[1-9][0-9]{5,17}$' "
                                                                                                'OR '
                                                                                                'substring(`finding_target_code`, '
                                                                                                'length(`finding_target_code`) '
                                                                                                '- 2, 2) '
                                                                                                'NOT IN '
                                                                                                "('00', "
                                                                                                "'10') OR "
                                                                                                '(substring(`finding_target_code`, '
                                                                                                'length(`finding_target_code`) '
                                                                                                '- 2, 2) = '
                                                                                                "'10' AND "
                                                                                                'length(`finding_target_code`) '
                                                                                                '< 11))',
                                                                                       'table': 'clinical_endoscopy_finding'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`endobase_exam_term_id` AS `endobase_exam_term_id`',
            '`endobase_exam_id` AS `endobase_exam_id`',
            '`subject_key` AS `subject_key`',
            '`subject_id_system` AS `subject_id_system`',
            '`person_id` AS `person_id`',
            '`encounter_id` AS `encounter_id`',
            '`event_datetime` AS `event_datetime`',
            '`event_end_datetime` AS `event_end_datetime`',
            '`event_datetime_local` AS `event_datetime_local`',
            '`event_end_datetime_local` AS `event_end_datetime_local`',
            '`source_coding_system` AS `source_coding_system`',
            '`source_code` AS `source_code`',
            '`source_display` AS `source_display`',
            '`__gold_json_finding_code` AS `finding_code`',
            '`endoscopy_exam_event_key` AS `endoscopy_exam_event_key`',
            '`section_id` AS `section_id`',
            '`subsection_id` AS `subsection_id`',
            '`parent_term_id` AS `parent_term_id`',
            '`display_order` AS `display_order`',
            '`confirmed_ind` AS `confirmed_ind`',
            '`text_changed_ind` AS `text_changed_ind`',
            '`free_text_ind` AS `free_text_ind`',
            '`term_mapping_status` AS `term_mapping_status`',
            '`person_link_status` AS `person_link_status`',
            '`authored_datetime` AS `authored_datetime`',
            '`event_time_source` AS `event_time_source`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
        ],
        public_steps=(_public_steps_clinical_endoscopy_finding,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_endoscopy_finding': {'columns': {'authored_datetime': 'Creation timestamp of the EndoBase '
                                                                     'examination-term record, carried from '
                                                                     '`CREATED_TS`; not a report-level '
                                                                     'authoring timestamp.',
                                                'confidentiality_code': 'Security classification when '
                                                                        'supplied.',
                                                'confirmed_ind': 'Finding confirmation indicator.',
                                                'display_order': 'Source display order.',
                                                'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                                'available.',
                                                'endobase_exam_id': 'Endobase ENDOBASE_EXAM_ID; native '
                                                                    'parent examination identifier.',
                                                'endobase_exam_term_id': 'Endobase ENDOBASE_EXAM_TERM_ID; '
                                                                         'primary key of this table.',
                                                'endoscopy_exam_event_key': 'Deterministic SHA-256 key for '
                                                                            'the parent Endobase '
                                                                            'examination.',
                                                'event_datetime': 'Parent exam time',
                                                'event_datetime_local': 'Parent exam event timestamp '
                                                                        'expressed in local time.',
                                                'event_end_datetime': 'Finding end timestamp when supplied.',
                                                'event_end_datetime_local': 'Finding end timestamp expressed '
                                                                            'in local time when supplied.',
                                                'event_time_source': 'Whether event time came from the exam '
                                                                     'or authored fallback.',
                                                'finding_code': 'Source DGVS and mapped SNOMED/OMOP codings.',
                                                'free_text_ind': 'Free-text route indicator.',
                                                'loaded_at': 'Greatest of map_endobase_exam_term.ADC_UPDT '
                                                             'and the left-joined '
                                                             'map_endobase_exam.ADC_UPDT, with missing '
                                                             'parent time first replaced by term time. '
                                                             'Parent lookup uses ENDOBASE_EXAM_ID. A '
                                                             'non-null parent time survives a null term '
                                                             'time; both null gives null. This can differ '
                                                             'from source_update_timestamp and the status '
                                                             'end, which use only the term clock; it is not '
                                                             'clinical event time or the current Silver '
                                                             'refresh time.',
                                                'parent_term_id': 'Parent term identifier.',
                                                'patient_event_key': 'Deterministic event key derived from '
                                                                     'source identifiers; retained as a '
                                                                     'stable join key.',
                                                'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                             'resolved.',
                                                'person_link_status': 'Bronze person-link status.',
                                                'record_status': 'Superseded when '
                                                                 'map_endobase_exam_term.SOURCE_PRESENT_IND '
                                                                 'is false, otherwise active; missing '
                                                                 'presence defaults true. No retracted '
                                                                 'status is emitted. The non-free-text and '
                                                                 'usable-code filters control eligibility '
                                                                 'separately, while parent-exam fields do '
                                                                 'not determine this label.',
                                                'record_status_effective_from': 'map_endobase_exam_term.CREATED_TS '
                                                                                'carried directly as the '
                                                                                'record-status start. This '
                                                                                'differs from clinical event '
                                                                                'time, which can use the '
                                                                                "joined parent exam's "
                                                                                'performed/exam/start '
                                                                                'timestamps before falling '
                                                                                'back to term creation time.',
                                                'record_status_effective_to': 'map_endobase_exam_term.ADC_UPDT '
                                                                              'when its source-presence flag '
                                                                              'is false; otherwise null. The '
                                                                              'parent-exam load clock and '
                                                                              'clinical event time are not '
                                                                              'used as status ends, even '
                                                                              'when they contribute to other '
                                                                              'fields.',
                                                'section_id': 'Report section identifier.',
                                                'source_code': 'DGVS term identifier with term-text '
                                                               'fallback.',
                                                'source_coding_system': 'DGVS term coding system.',
                                                'source_display': 'Endobase term text.',
                                                'source_update_timestamp': 'map_endobase_exam_term.ADC_UPDT '
                                                                           'carried unchanged. The '
                                                                           'parent-exam ADC_UPDT contributes '
                                                                           'to loaded_at only, not this '
                                                                           'field; no independent native '
                                                                           'application-update clock or '
                                                                           'current Silver refresh timestamp '
                                                                           'is substituted.',
                                                'subject_id_system': 'Identifier system used for '
                                                                     'subject_key.',
                                                'subject_key': 'Deterministic SHA-256 over the strongest '
                                                               'source identifier. Not salted, not secret; a '
                                                               'stable join key across feeds where person_id '
                                                               'is unresolved.',
                                                'subsection_id': 'Report subsection identifier.',
                                                'term_mapping_status': 'C4 term-mapping status.',
                                                'text_changed_ind': 'Source text-changed indicator.',
                                                'vip_ind': 'VIP indicator when supplied.',
                                                'withheld_identity_ind': 'Identity-withholding status is not '
                                                                         'populated by the current source '
                                                                         'projections; null does not mean '
                                                                         'identity was not withheld.'},
                                    'quantity_override': True,
                                    'table': 'Internal quality-controlled twin of '
                                             'clinical_endoscopy_finding: the Gold repairs, nulls and '
                                             'expectations are applied here and the published Gold MV reads '
                                             'this object. Materialized rather than temporary so the '
                                             'published MV can refresh incrementally.'},
 'clinical_endoscopy_finding': {'columns': {'authored_datetime': 'Creation timestamp of the EndoBase '
                                                                 'examination-term record, carried from '
                                                                 '`CREATED_TS`; not a report-level authoring '
                                                                 'timestamp.',
                                            'confidentiality_code': 'Security classification when supplied.',
                                            'confirmed_ind': 'Finding confirmation indicator.',
                                            'display_order': 'Source display order.',
                                            'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                            'available.',
                                            'endobase_exam_id': 'Endobase ENDOBASE_EXAM_ID; native parent '
                                                                'examination identifier.',
                                            'endobase_exam_term_id': 'Endobase ENDOBASE_EXAM_TERM_ID; '
                                                                     'primary key of this table.',
                                            'endoscopy_exam_event_key': 'Deterministic SHA-256 key for the '
                                                                        'parent Endobase examination.',
                                            'event_datetime': 'Parent exam time. Endoscopy finding time; '
                                                              'most rows carry a date only (midnight), so '
                                                              'order findings within a day by '
                                                              'endoscopy_exam_event_key, not by time.',
                                            'event_datetime_local': 'Parent exam event timestamp expressed '
                                                                    'in local time.',
                                            'event_end_datetime': 'Finding end timestamp when supplied.',
                                            'event_end_datetime_local': 'Finding end timestamp expressed in '
                                                                        'local time when supplied.',
                                            'event_time_source': 'Whether event time came from the exam or '
                                                                 'authored fallback.',
                                            'finding_code': 'Source DGVS and mapped SNOMED/OMOP codings.',
                                            'free_text_ind': 'Free-text route indicator.',
                                            'loaded_at': 'Greatest of map_endobase_exam_term.ADC_UPDT and '
                                                         'the left-joined map_endobase_exam.ADC_UPDT, with '
                                                         'missing parent time first replaced by term time. '
                                                         'Parent lookup uses ENDOBASE_EXAM_ID. A non-null '
                                                         'parent time survives a null term time; both null '
                                                         'gives null. This can differ from '
                                                         'source_update_timestamp and the status end, which '
                                                         'use only the term clock; it is not clinical event '
                                                         'time or the current Silver refresh time.',
                                            'parent_term_id': 'Parent term identifier.',
                                            'patient_event_key': 'Deterministic event key derived from '
                                                                 'source identifiers; retained as a stable '
                                                                 'join key.',
                                            'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                         'resolved.',
                                            'person_link_status': 'Bronze person-link status.',
                                            'record_status': 'Superseded when '
                                                             'map_endobase_exam_term.SOURCE_PRESENT_IND is '
                                                             'false, otherwise active; missing presence '
                                                             'defaults true. No retracted status is emitted. '
                                                             'The non-free-text and usable-code filters '
                                                             'control eligibility separately, while '
                                                             'parent-exam fields do not determine this '
                                                             'label.',
                                            'record_status_effective_from': 'map_endobase_exam_term.CREATED_TS '
                                                                            'carried directly as the '
                                                                            'record-status start. This '
                                                                            'differs from clinical event '
                                                                            'time, which can use the joined '
                                                                            "parent exam's "
                                                                            'performed/exam/start timestamps '
                                                                            'before falling back to term '
                                                                            'creation time.',
                                            'record_status_effective_to': 'map_endobase_exam_term.ADC_UPDT '
                                                                          'when its source-presence flag is '
                                                                          'false; otherwise null. The '
                                                                          'parent-exam load clock and '
                                                                          'clinical event time are not used '
                                                                          'as status ends, even when they '
                                                                          'contribute to other fields.',
                                            'section_id': 'Report section identifier.',
                                            'source_code': 'DGVS term identifier with term-text fallback.',
                                            'source_coding_system': 'DGVS term coding system.',
                                            'source_display': 'Endobase term text.',
                                            'source_update_timestamp': 'map_endobase_exam_term.ADC_UPDT '
                                                                       'carried unchanged. The parent-exam '
                                                                       'ADC_UPDT contributes to loaded_at '
                                                                       'only, not this field; no independent '
                                                                       'native application-update clock or '
                                                                       'current Silver refresh timestamp is '
                                                                       'substituted.',
                                            'subject_id_system': 'Identifier system used for subject_key.',
                                            'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                           'identifier. Not salted, not secret; a stable '
                                                           'join key across feeds where person_id is '
                                                           'unresolved.',
                                            'subsection_id': 'Report subsection identifier.',
                                            'term_mapping_status': 'C4 term-mapping status.',
                                            'text_changed_ind': 'Source text-changed indicator.',
                                            'vip_ind': 'VIP indicator when supplied.',
                                            'withheld_identity_ind': 'Identity-withholding status is not '
                                                                     'populated by the current source '
                                                                     'projections; null does not mean '
                                                                     'identity was not withheld.'},
                                'quantity_override': False,
                                'table': 'One coded Endobase exam term with parent-exam clinical time and C4 '
                                         'mapping columns wired for later application. Gold QC twin of the '
                                         'silver product: 0 columns are repaired or nulled, 1 rule(s) drop '
                                         'rows, 2 check(s) are advisory. Each rule states its reason in the '
                                         'pipeline notebook, and Lakeflow expectation metrics report what '
                                         'every rule matched on each update. Endoscopy finding volume steps '
                                         'in July 2017 when structured Endobase findings capture changed.'}}
