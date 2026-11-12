"""Gold clinical.transfusion_event: 2 dataset(s).

- datasets: _qc_clinical_transfusion_event, clinical_transfusion_event
- moved verbatim from: 2_gold_clinical_events:6499, 2_gold_clinical_events:6527, 2_gold_clinical_events:6552, 2_gold_clinical_events:6560, 2_gold_clinical_events:6637, 2_gold_clinical_events:6641, 2_gold_clinical_events:6658

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.transfusion_event ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_TRANSFUSION_EVENT_SELECT = [
    '`transfusion_event_key` AS `transfusion_event_key`',
    '`bloodtrack_transaction_key` AS `bloodtrack_transaction_key`',
    '`person_id` AS `person_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`event_datetime` AS `event_datetime`',
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`workflow_step` AS `workflow_step`',
    '`transaction_success_ind` AS `transaction_success_ind`',
    '`unit_number` AS `unit_number`',
    '`product_code` AS `product_code`',
    '`product_description` AS `product_description`',
    '`bloodtrack_unit_id` AS `bloodtrack_unit_id`',
    '`device_name` AS `device_name`',
    '`source_location` AS `source_location`',
    '`linkage_status` AS `linkage_status`',
    "CASE WHEN UPPER(TRIM(CAST(`response_code` AS STRING))) = '0' THEN NULL ELSE `response_code` END AS `response_code`",
    '`response_text` AS `response_text`',
    '`blood_unit_state` AS `blood_unit_state`',
    '`blood_unit_fate` AS `blood_unit_fate`',
    '`alert_present_ind` AS `alert_present_ind`',
    '`comment_present_ind` AS `comment_present_ind`',
    '`source_table` AS `source_table`',
    '`source_row_key` AS `source_row_key`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_TRANSFUSION_EVENT_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`product_mapping_method` AS `product_mapping_method`',
    '`product_mapping_rule_id` AS `product_mapping_rule_id`',
    '`unit_group_mapping_method` AS `unit_group_mapping_method`',
    '`unit_group_mapping_rule_id` AS `unit_group_mapping_rule_id`',
    '`patient_group_mapping_method` AS `patient_group_mapping_method`',
    '`patient_group_mapping_rule_id` AS `patient_group_mapping_rule_id`',
    '`product_mapping_source` AS `product_mapping_source`',
    '`first_name` AS `first_name`',
    '`health_number` AS `health_number`',
    '`last_name` AS `last_name`',
    '`patient_birth_date` AS `patient_birth_date`',
    '`patient_gender` AS `patient_gender`',
    '`patient_id` AS `patient_id`',
    '`patient_name` AS `patient_name`',
    '`patient_number` AS `patient_number`',
    '`porter_user_name` AS `porter_user_name`',
    '`secondary_first_name` AS `secondary_first_name`',
    '`secondary_last_name` AS `secondary_last_name`',
    '`secondary_user_name` AS `secondary_user_name`',
    '`user_name` AS `user_name`',
    '`source_mrn_norm` AS `source_mrn_norm`',
]


# contract v2: QC/batch inputs come from internal _clinical_transfusion_event_metadata; source history stays on the main research table.
CLINICAL_TRANSFUSION_EVENT_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 228,205 rows of 228,205 that
    # are current and attributable; identity_status = 'resolved' keeps the 227,703 rows of
    # 228,205 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_TRANSFUSION_EVENT_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 502 of 228,205 at the profile.
    "gold.clinical.transfusion_event.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 228,205 at the profile.
    "gold.clinical.transfusion_event.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 2,924 of 228,205 rows (1.28%) when profiled on 2026-08-24.
    "gold.clinical.transfusion_event.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_TRANSFUSION_EVENT_PERSON_SCOPE_RULES = {
    "gold.clinical.transfusion_event.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_transfusion_event"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_TRANSFUSION_EVENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_TRANSFUSION_EVENT_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_TRANSFUSION_EVENT_PERSON_SCOPE_RULES)
def _gold_qc_clinical_transfusion_event():
    """Quality-controlled twin of journey_clinical.transfusion_event."""
    df = _qc(
        "clinical_transfusion_event",
        CLINICAL_TRANSFUSION_EVENT_SELECT,
        date_flags=["event_after_death_30d"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.transfusion_event"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_transfusion_event():
    """Contract-v2 public twin of clinical_transfusion_event; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_transfusion_event")).selectExpr(
        '`transfusion_event_key` AS `transfusion_event_key`',
        '`bloodtrack_transaction_key` AS `bloodtrack_transaction_key`',
        '`person_id` AS `person_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`event_datetime` AS `event_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`workflow_step` AS `workflow_step`',
        '`transaction_success_ind` AS `transaction_success_ind`',
        '`unit_number` AS `unit_number`',
        '`product_code` AS `product_code`',
        '`product_description` AS `product_description`',
        '`bloodtrack_unit_id` AS `bloodtrack_unit_id`',
        '`device_name` AS `device_name`',
        '`source_location` AS `source_location`',
        '`linkage_status` AS `linkage_status`',
        '`response_code` AS `response_code`',
        '`response_text` AS `response_text`',
        '`blood_unit_state` AS `blood_unit_state`',
        '`blood_unit_fate` AS `blood_unit_fate`',
        '`alert_present_ind` AS `alert_present_ind`',
        '`comment_present_ind` AS `comment_present_ind`',
        '`source_table` AS `source_table`',
        '`source_row_key` AS `source_row_key`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
        '`product_mapping_method` AS `product_mapping_method`',  # INSR_W3 W3.12
        '`product_mapping_rule_id` AS `product_mapping_rule_id`',  # INSR_W3 W3.12
        '`unit_group_mapping_method` AS `unit_group_mapping_method`',  # INSR_W3 W3.12
        '`unit_group_mapping_rule_id` AS `unit_group_mapping_rule_id`',  # INSR_W3 W3.12
        '`patient_group_mapping_method` AS `patient_group_mapping_method`',  # INSR_W3 W3.12
        '`patient_group_mapping_rule_id` AS `patient_group_mapping_rule_id`',  # INSR_W3 W3.12
        '`product_mapping_source` AS `product_mapping_source`',  # INSR_W3 W3.12
        '`first_name` AS `first_name`',  # INSR_W3 W3.12
        '`health_number` AS `health_number`',  # INSR_W3 W3.12
        '`last_name` AS `last_name`',  # INSR_W3 W3.12
        '`patient_birth_date` AS `patient_birth_date`',  # INSR_W3 W3.12
        '`patient_gender` AS `patient_gender`',  # INSR_W3 W3.12
        '`patient_id` AS `patient_id`',  # INSR_W3 W3.12
        '`patient_name` AS `patient_name`',  # INSR_W3 W3.12
        '`patient_number` AS `patient_number`',  # INSR_W3 W3.12
        '`porter_user_name` AS `porter_user_name`',  # INSR_W3 W3.12
        '`secondary_first_name` AS `secondary_first_name`',  # INSR_W3 W3.12
        '`secondary_last_name` AS `secondary_last_name`',  # INSR_W3 W3.12
        '`secondary_user_name` AS `secondary_user_name`',  # INSR_W3 W3.12
        '`user_name` AS `user_name`',  # INSR_W3 W3.12
        '`source_mrn_norm` AS `source_mrn_norm`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'clinical_transfusion_event': GoldRules(
        table='clinical_transfusion_event',
        select=CLINICAL_TRANSFUSION_EVENT_SELECT,
        mandatory=CLINICAL_TRANSFUSION_EVENT_MANDATORY_RULES,
        advisory=CLINICAL_TRANSFUSION_EVENT_ADVISORY_RULES,
        person_scope=CLINICAL_TRANSFUSION_EVENT_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['transfusion_event_key'],
         'support_columns': ['identity_status', 'record_status', 'load_batch_id', 'loaded_at']},
        date_flags=('event_after_death_30d',)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_transfusion_event': {'columns': {'alert_present_ind': 'Alert presence indicator.',
                                                'blood_unit_fate': 'Source BloodTrack final fate recorded '
                                                                   'for the blood unit, such as confirmed '
                                                                   'use or non-use.',
                                                'blood_unit_state': 'Source BloodTrack status of the blood '
                                                                    'unit within the storage and issue '
                                                                    'workflow at the time of the '
                                                                    'transaction.',
                                                'bloodtrack_transaction_key': 'BloodTrack transaction key; '
                                                                              'native primary key of this '
                                                                              'table.',
                                                'bloodtrack_unit_id': 'BloodTrack unit identifier.',
                                                'comment_present_ind': 'Comment presence indicator.',
                                                'device_name': 'Scanning device name.',
                                                'event_datetime': 'Effective transaction timestamp.',
                                                'event_datetime_local': 'Effective BloodTrack transaction '
                                                                        'timestamp expressed in local time.',
                                                'first_name': 'BloodTrack FIRST_NAME as sent by the source; '
                                                              'identifier, published and tagged '
                                                              '(INSR_W1_BLOODTRACK_V1). (bronze FIRST_NAME, '
                                                              'INSR W1). INSR_W2 W2.20.',
                                                'health_number': 'BloodTrack HEALTH_NUMBER as sent by the '
                                                                 'source; identifier, published and tagged '
                                                                 '(INSR_W1_BLOODTRACK_V1). (bronze '
                                                                 'HEALTH_NUMBER, INSR W1). INSR_W2 W2.20.',
                                                'last_name': 'BloodTrack LAST_NAME as sent by the source; '
                                                             'identifier, published and tagged '
                                                             '(INSR_W1_BLOODTRACK_V1). (bronze LAST_NAME, '
                                                             'INSR W1). INSR_W2 W2.20.',
                                                'linkage_status': 'Patient linkage status.',
                                                'loaded_at': 'map_bloodtrack_transaction.ADC_UPDT carried '
                                                             'unchanged for the transaction row. No join to '
                                                             'the transfusion aggregate contributes its '
                                                             'PIPELINE_LOADED_AT; device/event times and the '
                                                             'current Silver refresh time do not replace '
                                                             'this value.',
                                                'patient_birth_date': 'BloodTrack PATIENT_BIRTH_DATE as sent '
                                                                      'by the source; identifier, published '
                                                                      'and tagged (INSR_W1_BLOODTRACK_V1). '
                                                                      '(bronze PATIENT_BIRTH_DATE, INSR W1). '
                                                                      'INSR_W2 W2.20.',
                                                'patient_gender': 'BloodTrack PATIENT_GENDER as sent by the '
                                                                  'source; identifier, published and tagged '
                                                                  '(INSR_W1_BLOODTRACK_V1). (bronze '
                                                                  'PATIENT_GENDER, INSR W1). INSR_W2 W2.20.',
                                                'patient_group_mapping_method': 'PATIENT_GROUP map '
                                                                                'provenance (method). '
                                                                                '(bronze '
                                                                                'PATIENT_GROUP_MAPPING_METHOD, '
                                                                                'INSR W1). INSR_W2 W2.20.',
                                                'patient_group_mapping_rule_id': 'PATIENT_GROUP map '
                                                                                 'provenance (rule_id). '
                                                                                 '(bronze '
                                                                                 'PATIENT_GROUP_MAPPING_RULE_ID, '
                                                                                 'INSR W1). INSR_W2 W2.20.',
                                                'patient_id': 'BloodTrack PATIENT_ID as sent by the source; '
                                                              'identifier, published and tagged '
                                                              '(INSR_W1_BLOODTRACK_V1). (bronze PATIENT_ID, '
                                                              'INSR W1). INSR_W2 W2.20.',
                                                'patient_name': 'BloodTrack PATIENT_NAME as sent by the '
                                                                'source; identifier, published and tagged '
                                                                '(INSR_W1_BLOODTRACK_V1). (bronze '
                                                                'PATIENT_NAME, INSR W1). INSR_W2 W2.20.',
                                                'patient_number': 'BloodTrack PATIENT_NUMBER as sent by the '
                                                                  'source; identifier, published and tagged '
                                                                  '(INSR_W1_BLOODTRACK_V1). (bronze '
                                                                  'PATIENT_NUMBER, INSR W1). INSR_W2 W2.20.',
                                                'person_id': 'Millennium PERSON_ID when resolved; native '
                                                             'person foreign key.',
                                                'porter_user_name': 'BloodTrack PORTER_USER_NAME as sent by '
                                                                    'the source; identifier, published and '
                                                                    'tagged (INSR_W1_BLOODTRACK_V1). (bronze '
                                                                    'PORTER_USER_NAME, INSR W1). INSR_W2 '
                                                                    'W2.20.',
                                                'product_code': 'Product code scanned.',
                                                'product_description': 'Source BloodTrack textual '
                                                                       'description of the blood component '
                                                                       'product.',
                                                'product_mapping_method': 'PRODUCT map provenance (method). '
                                                                          '(bronze PRODUCT_MAPPING_METHOD, '
                                                                          'INSR W1). INSR_W2 W2.20.',
                                                'product_mapping_rule_id': 'PRODUCT map provenance '
                                                                           '(rule_id). (bronze '
                                                                           'PRODUCT_MAPPING_RULE_ID, INSR '
                                                                           'W1). INSR_W2 W2.20.',
                                                'product_mapping_source': 'Map table the product concept '
                                                                          'came from. (bronze '
                                                                          'PRODUCT_MAPPING_SOURCE, INSR W1). '
                                                                          'INSR_W2 W2.20.',
                                                'record_status': 'Superseded when '
                                                                 'map_bloodtrack_transaction.IS_CURRENT_IN_SOURCE '
                                                                 'is false, otherwise active; a missing flag '
                                                                 'defaults true. No retracted status is '
                                                                 'emitted. Workflow step, transaction '
                                                                 'success, response and blood-unit '
                                                                 'state/fate do not set this label.',
                                                'response_code': 'Device or workflow response code.',
                                                'response_text': 'Device or workflow response text.',
                                                'secondary_first_name': 'BloodTrack SECONDARY_FIRST_NAME as '
                                                                        'sent by the source; identifier, '
                                                                        'published and tagged '
                                                                        '(INSR_W1_BLOODTRACK_V1). (bronze '
                                                                        'SECONDARY_FIRST_NAME, INSR W1). '
                                                                        'INSR_W2 W2.20.',
                                                'secondary_last_name': 'BloodTrack SECONDARY_LAST_NAME as '
                                                                       'sent by the source; identifier, '
                                                                       'published and tagged '
                                                                       '(INSR_W1_BLOODTRACK_V1). (bronze '
                                                                       'SECONDARY_LAST_NAME, INSR W1). '
                                                                       'INSR_W2 W2.20.',
                                                'secondary_user_name': 'BloodTrack SECONDARY_USER_NAME as '
                                                                       'sent by the source; identifier, '
                                                                       'published and tagged '
                                                                       '(INSR_W1_BLOODTRACK_V1). (bronze '
                                                                       'SECONDARY_USER_NAME, INSR W1). '
                                                                       'INSR_W2 W2.20.',
                                                'source_location': 'Source workflow location name.',
                                                'source_mrn_norm': 'Digits-only normalisation of '
                                                                   'SOURCE_PATIENT_NUMBER (the BloodTrack '
                                                                   'MRN) used for MRN linkage and '
                                                                   'TRANSFUSION_KEY. Identifier, published '
                                                                   'and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                   '(bronze SOURCE_MRN_NORM, INSR W1). '
                                                                   'INSR_W2 W2.20.',
                                                'source_row_key': 'Verbatim BloodTrack transaction key '
                                                                  'retained as source-row provenance.',
                                                'source_table': 'Registered source table.',
                                                'subject_id_system': 'Identifier system used for '
                                                                     'subject_key.',
                                                'subject_key': 'Deterministic SHA-256 over the strongest '
                                                               'source identifier. Not salted, not secret; a '
                                                               'stable join key across feeds where person_id '
                                                               'is unresolved.',
                                                'transaction_success_ind': 'Whether the transaction '
                                                                           'succeeded.',
                                                'transfusion_event_key': 'Deterministic SHA-256 BloodTrack '
                                                                         'scan-event key.',
                                                'unit_group_mapping_method': 'UNIT_GROUP map provenance '
                                                                             '(method). (bronze '
                                                                             'UNIT_GROUP_MAPPING_METHOD, '
                                                                             'INSR W1). INSR_W2 W2.20.',
                                                'unit_group_mapping_rule_id': 'UNIT_GROUP map provenance '
                                                                              '(rule_id). (bronze '
                                                                              'UNIT_GROUP_MAPPING_RULE_ID, '
                                                                              'INSR W1). INSR_W2 W2.20.',
                                                'unit_number': 'Blood unit number.',
                                                'user_name': 'BloodTrack USER_NAME as sent by the source; '
                                                             'identifier, published and tagged '
                                                             '(INSR_W1_BLOODTRACK_V1). (bronze USER_NAME, '
                                                             'INSR W1). INSR_W2 W2.20.',
                                                'workflow_step': 'BloodTrack workflow step.'},
                                    'quantity_override': True,
                                    'table': 'Internal quality-controlled twin of '
                                             'clinical_transfusion_event: the Gold repairs, nulls and '
                                             'expectations are applied here and the published Gold MV reads '
                                             'this object. Materialized rather than temporary so the '
                                             'published MV can refresh incrementally.'},
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
                                            'event_datetime_local': 'Effective BloodTrack transaction '
                                                                    'timestamp expressed in local time.',
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
                                'quantity_override': False,
                                'table': 'One BloodTrack scan-grain workflow event, including failed '
                                         'attempts and safety checks that do not count as transfusions. Gold '
                                         'QC twin of the silver product: 1 columns are repaired or nulled, 1 '
                                         'rule(s) drop rows, 3 check(s) are advisory. Each rule states its '
                                         'reason in the pipeline notebook, and Lakeflow expectation metrics '
                                         'report what every rule matched on each update. Coverage: '
                                         'BloodTrack transfusion records start on 2023-05-02 (go-live); '
                                         'there are no earlier rows at source; complete-through month per '
                                         'release: 6_mgmt.data_quality.release_finding (finding_code '
                                         'F2_complete_through).'}}
