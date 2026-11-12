"""Gold clinical.allergy_intolerance: 2 dataset(s).

- datasets: _qc_clinical_allergy_intolerance, clinical_allergy_intolerance
- moved verbatim from: 2_gold_clinical_events:161, 2_gold_clinical_events:165, 2_gold_clinical_events:178, 2_gold_clinical_events:37, 2_gold_clinical_events:75, 2_gold_clinical_events:81, 2_gold_clinical_events:88

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.allergy_intolerance ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_ALLERGY_INTOLERANCE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`allergy_instance_id` AS `allergy_instance_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`substance_code` AS `substance_code`',
    '`substance_type_code` AS `substance_type_code`',
    '`substance_type_display` AS `substance_type_display`',
    '`reaction_class_code` AS `reaction_class_code`',
    '`reaction_class_display` AS `reaction_class_display`',
    '`reaction_status_code` AS `reaction_status_code`',
    '`reaction_status_display` AS `reaction_status_display`',
    '`severity_code` AS `severity_code`',
    '`severity_display` AS `severity_display`',
    '`absence_assertion_ind` AS `absence_assertion_ind`',
    '`onset_precision_display` AS `onset_precision_display`',
    '`source_of_info_display` AS `source_of_info_display`',
    '`verified_status_flag` AS `verified_status_flag`',
    '`reviewed_datetime` AS `reviewed_datetime`',
    '`cancel_reason_display` AS `cancel_reason_display`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_ALLERGY_INTOLERANCE_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`substance_omop_map_rule_id` AS `substance_omop_map_rule_id`',
    '`substance_omop_map_version` AS `substance_omop_map_version`',
]


# contract v2: QC/batch inputs come from internal _clinical_allergy_intolerance_metadata; source history stays on the main research table.
CLINICAL_ALLERGY_INTOLERANCE_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 518,748 rows of 518,748
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_ALLERGY_INTOLERANCE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.allergy_intolerance.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 518,748 at the profile.
    "gold.clinical.allergy_intolerance.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 762 of 518,748 rows (0.147%) when profiled on 2026-08-24.
    "gold.clinical.allergy_intolerance.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 109 of 518,748 rows (0.021%) when profiled on 2026-08-24.
    "gold.clinical.allergy_intolerance.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_ALLERGY_INTOLERANCE_PERSON_SCOPE_RULES = {
    "gold.clinical.allergy_intolerance.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_allergy_intolerance"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_ALLERGY_INTOLERANCE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_ALLERGY_INTOLERANCE_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_ALLERGY_INTOLERANCE_PERSON_SCOPE_RULES)
def _gold_qc_clinical_allergy_intolerance():
    """Quality-controlled twin of journey_clinical.allergy_intolerance."""
    df = _qc("clinical_allergy_intolerance", CLINICAL_ALLERGY_INTOLERANCE_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.allergy_intolerance"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_allergy_intolerance():
    """Contract-v2 public twin of clinical_allergy_intolerance; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_allergy_intolerance")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`allergy_instance_id` AS `allergy_instance_id`',
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
        '`__gold_json_substance_code` AS `substance_code`',
        '`substance_type_code` AS `substance_type_code`',
        '`substance_type_display` AS `substance_type_display`',
        '`reaction_class_code` AS `reaction_class_code`',
        '`reaction_class_display` AS `reaction_class_display`',
        '`reaction_status_code` AS `reaction_status_code`',
        '`reaction_status_display` AS `reaction_status_display`',
        '`severity_code` AS `severity_code`',
        '`severity_display` AS `severity_display`',
        '`absence_assertion_ind` AS `absence_assertion_ind`',
        '`onset_precision_display` AS `onset_precision_display`',
        '`source_of_info_display` AS `source_of_info_display`',
        '`verified_status_flag` AS `verified_status_flag`',
        '`reviewed_datetime` AS `reviewed_datetime`',
        '`cancel_reason_display` AS `cancel_reason_display`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        '`substance_omop_map_rule_id` AS `substance_omop_map_rule_id`',  # INSR_W3 W3.12
        '`substance_omop_map_version` AS `substance_omop_map_version`',  # INSR_W3 W3.12
    )
    return _s3_flatten_gold_public(df, "clinical_allergy_intolerance")


def _public_steps_clinical_allergy_intolerance(df):
    """clinical_allergy_intolerance: the public body after its projection (GoldRules.public_select), moved from gold_clinical_allergy_intolerance; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_allergy_intolerance')


RULES = {
    'clinical_allergy_intolerance': GoldRules(
        table='clinical_allergy_intolerance',
        select=CLINICAL_ALLERGY_INTOLERANCE_SELECT,
        mandatory=CLINICAL_ALLERGY_INTOLERANCE_MANDATORY_RULES,
        advisory=CLINICAL_ALLERGY_INTOLERANCE_ADVISORY_RULES,
        person_scope=CLINICAL_ALLERGY_INTOLERANCE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'person_id_resolved',
                             'encounter_id_resolved',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth'),
        variant_columns=('substance_code',),
        s3_axis_specs={'substance_code': [('substance', False)]},
        value_rules={'gold.clinical.allergy_intolerance.substance_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                           'column': 'substance_snomed_code',
                                                                                           'companions': ['substance_snomed_display',
                                                                                                          'substance_target_code',
                                                                                                          'substance_target_display'],
                                                                                           'match': '`substance_snomed_code` '
                                                                                                    'IS '
                                                                                                    'NOT '
                                                                                                    'NULL '
                                                                                                    'AND '
                                                                                                    '(NOT '
                                                                                                    '`substance_snomed_code` '
                                                                                                    'RLIKE '
                                                                                                    "'^[1-9][0-9]{5,17}$' "
                                                                                                    'OR '
                                                                                                    'substring(`substance_snomed_code`, '
                                                                                                    'length(`substance_snomed_code`) '
                                                                                                    '- 2, '
                                                                                                    '2) '
                                                                                                    'NOT '
                                                                                                    'IN '
                                                                                                    "('00', "
                                                                                                    "'10') "
                                                                                                    'OR '
                                                                                                    '(substring(`substance_snomed_code`, '
                                                                                                    'length(`substance_snomed_code`) '
                                                                                                    '- 2, '
                                                                                                    '2) = '
                                                                                                    "'10' "
                                                                                                    'AND '
                                                                                                    'length(`substance_snomed_code`) '
                                                                                                    '< '
                                                                                                    '11))',
                                                                                           'table': 'clinical_allergy_intolerance'},
         'gold.clinical.allergy_intolerance.substance_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                           'column': 'substance_target_code',
                                                                                           'companions': ['substance_target_display',
                                                                                                          'substance_snomed_code',
                                                                                                          'substance_snomed_display'],
                                                                                           'match': '`substance_target_system` '
                                                                                                    '= '
                                                                                                    "'http://snomed.info/sct' "
                                                                                                    'AND '
                                                                                                    '`substance_target_code` '
                                                                                                    'IS '
                                                                                                    'NOT '
                                                                                                    'NULL '
                                                                                                    'AND '
                                                                                                    '(NOT '
                                                                                                    '`substance_target_code` '
                                                                                                    'RLIKE '
                                                                                                    "'^[1-9][0-9]{5,17}$' "
                                                                                                    'OR '
                                                                                                    'substring(`substance_target_code`, '
                                                                                                    'length(`substance_target_code`) '
                                                                                                    '- 2, '
                                                                                                    '2) '
                                                                                                    'NOT '
                                                                                                    'IN '
                                                                                                    "('00', "
                                                                                                    "'10') "
                                                                                                    'OR '
                                                                                                    '(substring(`substance_target_code`, '
                                                                                                    'length(`substance_target_code`) '
                                                                                                    '- 2, '
                                                                                                    '2) = '
                                                                                                    "'10' "
                                                                                                    'AND '
                                                                                                    'length(`substance_target_code`) '
                                                                                                    '< '
                                                                                                    '11))',
                                                                                           'table': 'clinical_allergy_intolerance'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`allergy_instance_id` AS `allergy_instance_id`',
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
            '`__gold_json_substance_code` AS `substance_code`',
            '`substance_type_code` AS `substance_type_code`',
            '`substance_type_display` AS `substance_type_display`',
            '`reaction_class_code` AS `reaction_class_code`',
            '`reaction_class_display` AS `reaction_class_display`',
            '`reaction_status_code` AS `reaction_status_code`',
            '`reaction_status_display` AS `reaction_status_display`',
            '`severity_code` AS `severity_code`',
            '`severity_display` AS `severity_display`',
            '`absence_assertion_ind` AS `absence_assertion_ind`',
            '`onset_precision_display` AS `onset_precision_display`',
            '`source_of_info_display` AS `source_of_info_display`',
            '`verified_status_flag` AS `verified_status_flag`',
            '`reviewed_datetime` AS `reviewed_datetime`',
            '`cancel_reason_display` AS `cancel_reason_display`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
            '`substance_omop_map_rule_id` AS `substance_omop_map_rule_id`',
            '`substance_omop_map_version` AS `substance_omop_map_version`',
        ],
        public_steps=(_public_steps_clinical_allergy_intolerance,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_allergy_intolerance': {'columns': {'absence_assertion_ind': 'True when the row asserts '
                                                                           'ABSENCE of allergy (e.g. no '
                                                                           'known allergies) rather than a '
                                                                           'positive assertion.',
                                                  'allergy_instance_id': 'Source-system identifier for the '
                                                                         'allergy instance associated with '
                                                                         'each allergy intolerance record. '
                                                                         'It is carried from bronze field '
                                                                         '`ALLERGY_INSTANCE_ID` in '
                                                                         '`4_prod.bronze.map_allergy`. Its '
                                                                         'uniqueness is limited to the '
                                                                         'originating source namespace; null '
                                                                         'means no identifier was supplied.',
                                                  'cancel_reason_display': 'Cancel reason when cancelled.',
                                                  'confidentiality_code': 'Security classification when '
                                                                          'supplied.',
                                                  'encounter_id': 'Recording encounter reference when '
                                                                  'supplied.',
                                                  'event_datetime': 'Allergy onset timestamp from '
                                                                    '`ONSET_DT_TM_CLEAN`, falling back to '
                                                                    '`CREATED_DT_TM_CLEAN` when onset is '
                                                                    'absent; null when both cleaned '
                                                                    'timestamps are absent.',
                                                  'event_datetime_local': 'Allergy or intolerance onset date '
                                                                          'and time expressed in local time, '
                                                                          'corresponding to the '
                                                                          'sentinel-cleaned onset in '
                                                                          'event_datetime.',
                                                  'event_end_datetime': 'Reaction-status end when resolved '
                                                                        'or cancelled.',
                                                  'event_end_datetime_local': 'Local-time end date and time '
                                                                              'of the allergy or intolerance '
                                                                              'when its reaction status is '
                                                                              'resolved or cancelled, '
                                                                              'corresponding to '
                                                                              'event_end_datetime. Gold: '
                                                                              'NULL wherever Gold nulls '
                                                                              'event_end_datetime.',
                                                  'loaded_at': 'map_allergy.ADC_UPDT carried unchanged '
                                                               'through usable-code filtering and '
                                                               'quality/public projections. This is the '
                                                               'contributing bronze load clock, distinct '
                                                               'from PIPELINE_UPDT_DT_TM published as '
                                                               'source_update_timestamp; it is not allergy '
                                                               'onset, status-change time or Silver refresh '
                                                               'time.',
                                                  'onset_precision_display': 'Source onset precision.',
                                                  'patient_event_key': 'Stable silver-v2 key identifying the '
                                                                       'patient event associated with each '
                                                                       'allergy intolerance record. It is '
                                                                       'derived from bronze field '
                                                                       '`ALLERGY_INSTANCE_ID` in '
                                                                       '`4_prod.bronze.map_allergy`. This is '
                                                                       'a contract identifier rather than a '
                                                                       'display value; null means the source '
                                                                       'identifiers were insufficient to '
                                                                       'derive the key.',
                                                  'person_id': 'Resolved Millennium person identifier when '
                                                               'available.',
                                                  'reaction_class_code': 'Source reaction class code.',
                                                  'reaction_class_display': 'Reaction class '
                                                                            '(Allergy/Intolerance/Side '
                                                                            'Effect/...).',
                                                  'reaction_status_code': 'Source reaction status code.',
                                                  'reaction_status_display': 'Reaction status '
                                                                             '(Active/Cancelled/Resolved/Proposed).',
                                                  'record_status': 'Retracted when '
                                                                   "upper(coalesce(REACTION_STATUS_DESC,'')) "
                                                                   'equals CANCELLED, without trimming; '
                                                                   'otherwise superseded when '
                                                                   'END_EFFECTIVE_DT_TM is before 2100-01-01 '
                                                                   'or ACTIVE_IND cast to LONG equals zero; '
                                                                   'otherwise active. A null end defaults to '
                                                                   'not-ended and a null activity flag '
                                                                   'defaults to 1. Cancellation takes '
                                                                   'priority, and other nonzero activity '
                                                                   'values do not trigger supersession.',
                                                  'record_status_effective_from': 'First non-null '
                                                                                  'map_allergy.REACTION_STATUS_DT_TM_CLEAN, '
                                                                                  'ACTIVE_STATUS_DT_TM, then '
                                                                                  'BEG_EFFECTIVE_DT_TM. This '
                                                                                  'source-status/validity '
                                                                                  'timestamp is distinct '
                                                                                  'from the onset/created '
                                                                                  'fallbacks used for '
                                                                                  'event_datetime; null when '
                                                                                  'all three are missing.',
                                                  'record_status_effective_to': 'CANCEL_DT_TM_CLEAN for '
                                                                                'cancelled allergy rows, '
                                                                                'even if null; otherwise '
                                                                                'END_EFFECTIVE_DT_TM for '
                                                                                'ended/inactive rows; '
                                                                                'otherwise null. There is no '
                                                                                'fallback between those '
                                                                                'branches. Supersession '
                                                                                'caused by ACTIVE_IND=0 can '
                                                                                'retain a far-future end '
                                                                                'value because the selected '
                                                                                'end is not range-clamped.',
                                                  'reviewed_datetime': 'Sentinel-cleaned last review '
                                                                       'timestamp.',
                                                  'severity_code': 'Source severity code.',
                                                  'severity_display': 'Source allergy severity description '
                                                                      'carried from `SEVERITY_DESC`, for '
                                                                      'example mild, moderate or severe.',
                                                  'source_code': 'Substance code (SNOMED then source '
                                                                 'identifier then nomenclature id)',
                                                  'source_coding_system': 'Verbatim substance code system.',
                                                  'source_display': 'Verbatim substance display text.',
                                                  'source_of_info_display': 'Allergy information-source '
                                                                            'description from '
                                                                            '`SOURCE_OF_INFO_DESC`, falling '
                                                                            'back to the recorded free text '
                                                                            'in `SOURCE_OF_INFO_FT` when the '
                                                                            'description is null.',
                                                  'source_update_timestamp': 'Bronze pipeline processing '
                                                                             'timestamp carried from '
                                                                             '`PIPELINE_UPDT_DT_TM` in '
                                                                             '`4_prod.bronze.map_allergy`. '
                                                                             'It records when bronze '
                                                                             'processed the source row, not '
                                                                             'a native clinical-system '
                                                                             'update timestamp, clinical '
                                                                             'event time, or the current '
                                                                             'Silver refresh time.',
                                                  'subject_id_system': 'Identifier system used for '
                                                                       'subject_key.',
                                                  'subject_key': 'Always-populated deterministic subject '
                                                                 'key.',
                                                  'substance_code': 'Source and mapped substance codings as '
                                                                    'a one-level CodeableConcept VARIANT.',
                                                  'substance_omop_map_rule_id': 'Rule that filled '
                                                                                'substance_omop_concept_id / '
                                                                                'substance_omop_source_concept_id '
                                                                                "from the OMOP 'Maps to' "
                                                                                'lookup '
                                                                                '(omop:maps_to:<vocabulary>); '
                                                                                'NULL when those ids came '
                                                                                "with the row's own mapping. "
                                                                                'INSR_W2 W2.03.',
                                                  'substance_omop_map_version': 'Vocabulary release '
                                                                                '(<vocabulary>|<version>) of '
                                                                                'the lookup that filled the '
                                                                                'substance OMOP ids; NULL '
                                                                                'when the lane filled '
                                                                                'nothing. INSR_W2 W2.03.',
                                                  'substance_type_code': 'Source substance type code.',
                                                  'substance_type_display': 'Substance type '
                                                                            '(Drug/Food/Environment/...).',
                                                  'verified_status_flag': 'Pharmacy-verified flag verbatim.',
                                                  'vip_ind': 'VIP indicator when supplied.',
                                                  'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                           'supplied.'},
                                      'quantity_override': True,
                                      'table': 'Internal quality-controlled twin of '
                                               'clinical_allergy_intolerance: the Gold repairs, nulls and '
                                               'expectations are applied here and the published Gold MV '
                                               'reads this object. Materialized rather than temporary so the '
                                               'published MV can refresh incrementally.'},
 'clinical_allergy_intolerance': {'columns': {'absence_assertion_ind': 'True when the row asserts ABSENCE of '
                                                                       'allergy (e.g. no known allergies) '
                                                                       'rather than a positive assertion.',
                                              'allergy_instance_id': 'Source-system identifier for the '
                                                                     'allergy instance associated with each '
                                                                     'allergy intolerance record. It is '
                                                                     'carried from bronze field '
                                                                     '`ALLERGY_INSTANCE_ID` in '
                                                                     '`4_prod.bronze.map_allergy`. Its '
                                                                     'uniqueness is limited to the '
                                                                     'originating source namespace; null '
                                                                     'means no identifier was supplied.',
                                              'cancel_reason_display': 'Cancel reason when cancelled.',
                                              'confidentiality_code': 'Security classification when '
                                                                      'supplied.',
                                              'encounter_id': 'Recording encounter reference when supplied.',
                                              'event_datetime': 'Allergy onset timestamp from '
                                                                '`ONSET_DT_TM_CLEAN`, falling back to '
                                                                '`CREATED_DT_TM_CLEAN` when onset is absent; '
                                                                'null when both cleaned timestamps are '
                                                                'absent.',
                                              'event_datetime_local': 'Allergy or intolerance onset date and '
                                                                      'time expressed in local time, '
                                                                      'corresponding to the sentinel-cleaned '
                                                                      'onset in event_datetime.',
                                              'event_end_datetime': 'Reaction-status end when resolved or '
                                                                    'cancelled.',
                                              'event_end_datetime_local': 'Local-time end date and time of '
                                                                          'the allergy or intolerance when '
                                                                          'its reaction status is resolved '
                                                                          'or cancelled, corresponding to '
                                                                          'event_end_datetime. Gold: NULL '
                                                                          'wherever Gold nulls '
                                                                          'event_end_datetime.',
                                              'loaded_at': 'map_allergy.ADC_UPDT carried unchanged through '
                                                           'usable-code filtering and quality/public '
                                                           'projections. This is the contributing bronze '
                                                           'load clock, distinct from PIPELINE_UPDT_DT_TM '
                                                           'published as source_update_timestamp; it is not '
                                                           'allergy onset, status-change time or Silver '
                                                           'refresh time.',
                                              'onset_precision_display': 'Source onset precision.',
                                              'patient_event_key': 'Stable silver-v2 key identifying the '
                                                                   'patient event associated with each '
                                                                   'allergy intolerance record. It is '
                                                                   'derived from bronze field '
                                                                   '`ALLERGY_INSTANCE_ID` in '
                                                                   '`4_prod.bronze.map_allergy`. This is a '
                                                                   'contract identifier rather than a '
                                                                   'display value; null means the source '
                                                                   'identifiers were insufficient to derive '
                                                                   'the key.',
                                              'person_id': 'Resolved Millennium person identifier when '
                                                           'available.',
                                              'reaction_class_code': 'Source reaction class code.',
                                              'reaction_class_display': 'Reaction class '
                                                                        '(Allergy/Intolerance/Side '
                                                                        'Effect/...).',
                                              'reaction_status_code': 'Source reaction status code.',
                                              'reaction_status_display': 'Reaction status '
                                                                         '(Active/Cancelled/Resolved/Proposed).',
                                              'record_status': 'Retracted when '
                                                               "upper(coalesce(REACTION_STATUS_DESC,'')) "
                                                               'equals CANCELLED, without trimming; '
                                                               'otherwise superseded when '
                                                               'END_EFFECTIVE_DT_TM is before 2100-01-01 or '
                                                               'ACTIVE_IND cast to LONG equals zero; '
                                                               'otherwise active. A null end defaults to '
                                                               'not-ended and a null activity flag defaults '
                                                               'to 1. Cancellation takes priority, and other '
                                                               'nonzero activity values do not trigger '
                                                               'supersession.',
                                              'record_status_effective_from': 'First non-null '
                                                                              'map_allergy.REACTION_STATUS_DT_TM_CLEAN, '
                                                                              'ACTIVE_STATUS_DT_TM, then '
                                                                              'BEG_EFFECTIVE_DT_TM. This '
                                                                              'source-status/validity '
                                                                              'timestamp is distinct from '
                                                                              'the onset/created fallbacks '
                                                                              'used for event_datetime; null '
                                                                              'when all three are missing.',
                                              'record_status_effective_to': 'CANCEL_DT_TM_CLEAN for '
                                                                            'cancelled allergy rows, even if '
                                                                            'null; otherwise '
                                                                            'END_EFFECTIVE_DT_TM for '
                                                                            'ended/inactive rows; otherwise '
                                                                            'null. There is no fallback '
                                                                            'between those branches. '
                                                                            'Supersession caused by '
                                                                            'ACTIVE_IND=0 can retain a '
                                                                            'far-future end value because '
                                                                            'the selected end is not '
                                                                            'range-clamped.',
                                              'reviewed_datetime': 'Sentinel-cleaned last review timestamp.',
                                              'severity_code': 'Source severity code.',
                                              'severity_display': 'Source allergy severity description '
                                                                  'carried from `SEVERITY_DESC`, for example '
                                                                  'mild, moderate or severe.',
                                              'source_code': 'Substance code (SNOMED then source identifier '
                                                             'then nomenclature id)',
                                              'source_coding_system': 'Verbatim substance code system.',
                                              'source_display': 'Verbatim substance display text.',
                                              'source_of_info_display': 'Allergy information-source '
                                                                        'description from '
                                                                        '`SOURCE_OF_INFO_DESC`, falling back '
                                                                        'to the recorded free text in '
                                                                        '`SOURCE_OF_INFO_FT` when the '
                                                                        'description is null.',
                                              'source_update_timestamp': 'Bronze pipeline processing '
                                                                         'timestamp carried from '
                                                                         '`PIPELINE_UPDT_DT_TM` in '
                                                                         '`4_prod.bronze.map_allergy`. It '
                                                                         'records when bronze processed the '
                                                                         'source row, not a native '
                                                                         'clinical-system update timestamp, '
                                                                         'clinical event time, or the '
                                                                         'current Silver refresh time.',
                                              'subject_id_system': 'Identifier system used for subject_key.',
                                              'subject_key': 'Always-populated deterministic subject key.',
                                              'substance_code': 'Source and mapped substance codings as a '
                                                                'one-level CodeableConcept VARIANT.',
                                              'substance_omop_map_rule_id': 'Rule that filled '
                                                                            'substance_omop_concept_id / '
                                                                            'substance_omop_source_concept_id '
                                                                            "from the OMOP 'Maps to' lookup "
                                                                            '(omop:maps_to:<vocabulary>); '
                                                                            'NULL when those ids came with '
                                                                            "the row's own mapping. INSR_W2 "
                                                                            'W2.03.',
                                              'substance_omop_map_version': 'Vocabulary release '
                                                                            '(<vocabulary>|<version>) of the '
                                                                            'lookup that filled the '
                                                                            'substance OMOP ids; NULL when '
                                                                            'the lane filled nothing. '
                                                                            'INSR_W2 W2.03.',
                                              'substance_type_code': 'Source substance type code.',
                                              'substance_type_display': 'Substance type '
                                                                        '(Drug/Food/Environment/...).',
                                              'verified_status_flag': 'Pharmacy-verified flag verbatim.',
                                              'vip_ind': 'VIP indicator when supplied.',
                                              'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                       'supplied.'},
                                  'quantity_override': False,
                                  'table': 'One Millennium allergy/intolerance assertion (map_allergy) with '
                                           'reaction class, severity, absence assertions, and cancel/review '
                                           'lifecycle. Gold QC twin of the silver product: 5 columns are '
                                           'repaired or nulled, 1 rule(s) drop rows, 3 check(s) are '
                                           'advisory. Each rule states its reason in the pipeline notebook, '
                                           'and Lakeflow expectation metrics report what every rule matched '
                                           'on each update.'}}
