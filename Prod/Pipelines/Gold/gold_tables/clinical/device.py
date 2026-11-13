"""Gold clinical.device: 2 dataset(s).

- datasets: _qc_clinical_device, clinical_device
- moved verbatim from: 2_gold_clinical_events:1223, 2_gold_clinical_events:1262, 2_gold_clinical_events:1268, 2_gold_clinical_events:1276, 2_gold_clinical_events:1355, 2_gold_clinical_events:1359, 2_gold_clinical_events:1372

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.device ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_DEVICE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`mc_device_id` AS `mc_device_id`',
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
    '`device_code` AS `device_code`',
    '`device_source_system` AS `device_source_system`',  # SRC_LINEAGE_V1
    '`device_source_code` AS `device_source_code`',  # SRC_LINEAGE_V1
    '`device_source_display` AS `device_source_display`',  # SRC_LINEAGE_V1
    '`device_role` AS `device_role`',
    '`model_name` AS `model_name`',
    "CASE WHEN UPPER(TRIM(CAST(`model_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `model_code` END AS `model_code`",
    '`manufacturer` AS `manufacturer`',
    '`manufacturer_parent` AS `manufacturer_parent`',
    '`serial_number` AS `serial_number`',
    '`implanted_datetime` AS `implanted_datetime`',
    '`implant_date_quality` AS `implant_date_quality`',
    '`explanted_ind` AS `explanted_ind`',
    '`lead_chamber` AS `lead_chamber`',
    '`lead_location` AS `lead_location`',
    '`pocket_site` AS `pocket_site`',
    '`status_display` AS `status_display`',
    '`device_mapping_status` AS `device_mapping_status`',
    "CASE WHEN TRIM(CAST(`comment_text` AS STRING)) = '' THEN NULL ELSE `comment_text` END AS `comment_text`",
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_DEVICE_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`device_mapping_rule_id` AS `device_mapping_rule_id`',
    '`device_mapping_llm_decision` AS `device_mapping_llm_decision`',
]


# contract v2: QC/batch inputs come from internal _clinical_device_metadata; source history stays on the main research table.
CLINICAL_DEVICE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 37,089 rows of 37,089 that
    # are current and attributable; identity_status = 'resolved' keeps the 37,063 rows of
    # 37,089 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_DEVICE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.device.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 26 of 37,089 at the profile.
    "gold.clinical.device.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 37,089 at the profile.
    "gold.clinical.device.record_status.default_view_active": "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 364 of 37,089 rows (0.981%) when profiled on 2026-08-24.
    "gold.clinical.device.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 79 of 37,089 rows (0.213%) when profiled on 2026-08-24.
    "gold.clinical.device.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_DEVICE_PERSON_SCOPE_RULES = {
    "gold.clinical.device.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_device"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_DEVICE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_DEVICE_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_DEVICE_PERSON_SCOPE_RULES)
def _gold_qc_clinical_device():
    """Quality-controlled twin of journey_clinical.device."""
    df = _qc("clinical_device", CLINICAL_DEVICE_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.device"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_device():
    """Contract-v2 public twin of clinical_device; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_device")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`mc_device_id` AS `mc_device_id`',
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
        '`__gold_json_device_code` AS `device_code`',
        '`device_source_system` AS `device_source_system`',  # SRC_LINEAGE_V1
        '`device_source_code` AS `device_source_code`',  # SRC_LINEAGE_V1
        '`device_source_display` AS `device_source_display`',  # SRC_LINEAGE_V1
        '`device_role` AS `device_role`',
        '`model_name` AS `model_name`',
        '`model_code` AS `model_code`',
        '`manufacturer` AS `manufacturer`',
        '`manufacturer_parent` AS `manufacturer_parent`',
        '`serial_number` AS `serial_number`',
        '`implanted_datetime` AS `implanted_datetime`',
        '`implant_date_quality` AS `implant_date_quality`',
        '`explanted_ind` AS `explanted_ind`',
        '`lead_chamber` AS `lead_chamber`',
        '`lead_location` AS `lead_location`',
        '`pocket_site` AS `pocket_site`',
        '`status_display` AS `status_display`',
        '`device_mapping_status` AS `device_mapping_status`',
        '`comment_text` AS `comment_text`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        '`device_mapping_rule_id` AS `device_mapping_rule_id`',  # INSR_W3 W3.12
        '`device_mapping_llm_decision` AS `device_mapping_llm_decision`',  # INSR_W3 W3.12
    )
    return _s3_flatten_gold_public(df, "clinical_device")


def _public_steps_clinical_device(df):
    """clinical_device: the public body after its projection (GoldRules.public_select), moved from gold_clinical_device; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_device')


RULES = {
    'clinical_device': GoldRules(
        table='clinical_device',
        select=CLINICAL_DEVICE_SELECT,
        mandatory=CLINICAL_DEVICE_MANDATORY_RULES,
        advisory=CLINICAL_DEVICE_ADVISORY_RULES,
        person_scope=CLINICAL_DEVICE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth'),
        variant_columns=('device_code',),
        s3_axis_specs={'device_code': [('device', False)]},
        value_rules={'gold.clinical.device.device_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                           'column': 'device_snomed_code',
                                                                           'companions': ['device_snomed_display',
                                                                                          'device_target_code',
                                                                                          'device_target_display'],
                                                                           'match': '`device_snomed_code` '
                                                                                    'IS NOT NULL AND (NOT '
                                                                                    '`device_snomed_code` '
                                                                                    'RLIKE '
                                                                                    "'^[1-9][0-9]{5,17}$' "
                                                                                    'OR '
                                                                                    'substring(`device_snomed_code`, '
                                                                                    'length(`device_snomed_code`) '
                                                                                    "- 2, 2) NOT IN ('00', "
                                                                                    "'10') OR "
                                                                                    '(substring(`device_snomed_code`, '
                                                                                    'length(`device_snomed_code`) '
                                                                                    "- 2, 2) = '10' AND "
                                                                                    'length(`device_snomed_code`) '
                                                                                    '< 11))',
                                                                           'table': 'clinical_device'},
         'gold.clinical.device.device_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                           'column': 'device_target_code',
                                                                           'companions': ['device_target_display',
                                                                                          'device_snomed_code',
                                                                                          'device_snomed_display'],
                                                                           'match': '`device_target_system` '
                                                                                    '= '
                                                                                    "'http://snomed.info/sct' "
                                                                                    'AND '
                                                                                    '`device_target_code` '
                                                                                    'IS NOT NULL AND (NOT '
                                                                                    '`device_target_code` '
                                                                                    'RLIKE '
                                                                                    "'^[1-9][0-9]{5,17}$' "
                                                                                    'OR '
                                                                                    'substring(`device_target_code`, '
                                                                                    'length(`device_target_code`) '
                                                                                    "- 2, 2) NOT IN ('00', "
                                                                                    "'10') OR "
                                                                                    '(substring(`device_target_code`, '
                                                                                    'length(`device_target_code`) '
                                                                                    "- 2, 2) = '10' AND "
                                                                                    'length(`device_target_code`) '
                                                                                    '< 11))',
                                                                           'table': 'clinical_device'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`mc_device_id` AS `mc_device_id`',
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
            '`__gold_json_device_code` AS `device_code`',
            '`device_source_system` AS `device_source_system`',  # SRC_LINEAGE_V1
            '`device_source_code` AS `device_source_code`',  # SRC_LINEAGE_V1
            '`device_source_display` AS `device_source_display`',  # SRC_LINEAGE_V1
            '`device_role` AS `device_role`',
            '`model_name` AS `model_name`',
            '`model_code` AS `model_code`',
            '`manufacturer` AS `manufacturer`',
            '`manufacturer_parent` AS `manufacturer_parent`',
            '`serial_number` AS `serial_number`',
            '`implanted_datetime` AS `implanted_datetime`',
            '`implant_date_quality` AS `implant_date_quality`',
            '`explanted_ind` AS `explanted_ind`',
            '`lead_chamber` AS `lead_chamber`',
            '`lead_location` AS `lead_location`',
            '`pocket_site` AS `pocket_site`',
            '`status_display` AS `status_display`',
            '`device_mapping_status` AS `device_mapping_status`',
            '`comment_text` AS `comment_text`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
            '`device_mapping_rule_id` AS `device_mapping_rule_id`',
            '`device_mapping_llm_decision` AS `device_mapping_llm_decision`',
        ],
        public_steps=(_public_steps_clinical_device,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_device': {'columns': {'comment_text': 'Source device comment.',
                                     'confidentiality_code': 'Security classification when supplied.',
                                     'device_code': 'Source and mapped device codings.',
                                     'device_mapping_llm_decision': 'Verifier verdict of the device-type '
                                                                    'mapping row. (bronze '
                                                                    'DEVICE_MAPPING_LLM_DECISION, INSR W1). '
                                                                    'INSR_W2 W2.20.',
                                     'device_mapping_rule_id': 'Rule id of the device-type mapping row '
                                                               '(INSR_W1_MEDICONNECT_V1). (bronze '
                                                               'DEVICE_MAPPING_RULE_ID, INSR W1). INSR_W2 '
                                                               'W2.20.',
                                     'device_mapping_status': 'Device terminology mapping status.',
                                     'device_role': 'GENERATOR | LEAD | MONITOR | OTHER',
                                     'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when available.',
                                     'event_datetime': 'Sentinel-cleaned implantation timestamp.',
                                     'event_datetime_local': 'Implantation event timestamp expressed in '
                                                             'local time, derived from the device QC bridge.',
                                     'event_end_datetime': 'Device lifecycle end timestamp when available.',
                                     'event_end_datetime_local': 'Device lifecycle end timestamp expressed '
                                                                 'in local time, when available.',
                                     'explanted_ind': 'Inferred explant indicator; not independently '
                                                      'confirmed.',
                                     'implant_date_quality': 'Implant-date quality class.',
                                     'implanted_datetime': 'Source implantation timestamp.',
                                     'lead_chamber': 'Cardiac chamber for a lead, parsed from the structured '
                                                     'source LOCATION1 text and null for non-lead records.',
                                     'lead_location': 'Anatomical implant location of a lead, parsed from '
                                                      'the structured source LOCATION1 text and null for '
                                                      'non-lead records.',
                                     'loaded_at': 'mediconnect_device.ADC_UPDT carried unchanged. The '
                                                  'device-type lookup contributes mapping fields but no '
                                                  'clocks, and registry rows in the shared internal stage '
                                                  'are excluded before device QC/public projections. This is '
                                                  'not a maximum with registry or lookup clocks, implant '
                                                  'time or the current Silver refresh time.',
                                     'manufacturer': 'Raw manufacturer name for the device or lead exactly '
                                                     'as recorded in the source registry.',
                                     'manufacturer_parent': 'Parent or successor corporate group for the '
                                                            'recorded manufacturer, derived from the raw '
                                                            'MANUFACTURER value and null where no parent '
                                                            'applies.',
                                     'mc_device_id': 'MediConnect MC_DEVICE_ID; primary key of this table.',
                                     'model_code': 'Device model code.',
                                     'model_name': 'Device model name.',
                                     'patient_event_key': 'Deterministic event key derived from source '
                                                          'identifiers; retained as a stable join key.',
                                     'person_id': 'Native Millennium PERSON_ID as BIGINT when resolved.',
                                     'pocket_site': 'Device pocket site.',
                                     'record_status': 'Superseded when mediconnect_device.SOURCE_PRESENT_IND '
                                                      'is false, otherwise active; missing presence defaults '
                                                      'true. No retracted status is emitted. STATUS, '
                                                      'EXPLANTED_IND and mapping status do not set this '
                                                      'derived label, and registry rows in the shared '
                                                      'internal stage are filtered out before device '
                                                      'publication.',
                                     'record_status_effective_from': 'Validated implant date derived from '
                                                                     'IMPLANTED_DATE, excluding sentinel, '
                                                                     'future and missing values flagged by '
                                                                     'IMPLANTED_DATE_QUALITY.',
                                     'record_status_effective_to': 'For a source-absent mediconnect_device '
                                                                   'row, first non-null '
                                                                   'SOURCE_ABSENT_DETECTED_TS then ADC_UPDT; '
                                                                   'otherwise null. This is not an explant '
                                                                   'timestamp. Registry-family absence '
                                                                   'clocks and device-type mapping clocks '
                                                                   'are not used.',
                                     'serial_number': 'Device serial number.',
                                     'source_code': 'Device type with type/model display fallback for '
                                                    'unknown type zero.',
                                     'source_coding_system': 'MediConnect device-type coding system.',
                                     'source_display': 'Device type or model display.',
                                     'source_update_timestamp': 'Always null as a TIMESTAMP in the current '
                                                                'MediConnect device projection. Neither '
                                                                'ADC_UPDT nor registry DATE_LAST_CHANGED is '
                                                                'substituted. Registry rows are filtered out '
                                                                'of the shared internal stage before device '
                                                                'publication; device-type mappings select no '
                                                                'clock.',
                                     'status_display': 'Source device status display.',
                                     'subject_id_system': 'Identifier system used for subject_key.',
                                     'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                    'identifier. Not salted, not secret; a stable join key '
                                                    'across feeds where person_id is unresolved.',
                                     'vip_ind': 'VIP indicator when supplied.',
                                     'withheld_identity_ind': 'Identity-withholding status is not populated '
                                                              'by the current source projections; null does '
                                                              'not mean identity was not withheld.'},
                         'quantity_override': True,
                         'table': 'Internal quality-controlled twin of clinical_device: the Gold repairs, '
                                  'nulls and expectations are applied here and the published Gold MV reads '
                                  'this object. Materialized rather than temporary so the published MV can '
                                  'refresh incrementally.'},
 'clinical_device': {'columns': {'comment_text': 'Source device comment.',
                                 'confidentiality_code': 'Security classification when supplied.',
                                 'device_code': 'Source and mapped device codings.',
                                 'device_mapping_llm_decision': 'Verifier verdict of the device-type mapping '
                                                                'row. (bronze DEVICE_MAPPING_LLM_DECISION, '
                                                                'INSR W1). INSR_W2 W2.20.',
                                 'device_mapping_rule_id': 'Rule id of the device-type mapping row '
                                                           '(INSR_W1_MEDICONNECT_V1). (bronze '
                                                           'DEVICE_MAPPING_RULE_ID, INSR W1). INSR_W2 W2.20.',
                                 'device_mapping_status': 'Device terminology mapping status.',
                                 'device_role': 'GENERATOR | LEAD | MONITOR | OTHER',
                                 'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when available.',
                                 'event_datetime': 'Sentinel-cleaned implantation timestamp.',
                                 'event_datetime_local': 'Implantation event timestamp expressed in local '
                                                         'time, derived from the device QC bridge.',
                                 'event_end_datetime': 'Device lifecycle end timestamp when available.',
                                 'event_end_datetime_local': 'Device lifecycle end timestamp expressed in '
                                                             'local time, when available.',
                                 'explanted_ind': 'Inferred explant indicator; not independently confirmed.',
                                 'implant_date_quality': 'Implant-date quality class.',
                                 'implanted_datetime': 'Source implantation timestamp.',
                                 'lead_chamber': 'Cardiac chamber for a lead, parsed from the structured '
                                                 'source LOCATION1 text and null for non-lead records.',
                                 'lead_location': 'Anatomical implant location of a lead, parsed from the '
                                                  'structured source LOCATION1 text and null for non-lead '
                                                  'records.',
                                 'loaded_at': 'mediconnect_device.ADC_UPDT carried unchanged. The '
                                              'device-type lookup contributes mapping fields but no clocks, '
                                              'and registry rows in the shared internal stage are excluded '
                                              'before device QC/public projections. This is not a maximum '
                                              'with registry or lookup clocks, implant time or the current '
                                              'Silver refresh time.',
                                 'manufacturer': 'Raw manufacturer name for the device or lead exactly as '
                                                 'recorded in the source registry.',
                                 'manufacturer_parent': 'Parent or successor corporate group for the '
                                                        'recorded manufacturer, derived from the raw '
                                                        'MANUFACTURER value and null where no parent '
                                                        'applies.',
                                 'mc_device_id': 'MediConnect MC_DEVICE_ID; primary key of this table.',
                                 'model_code': 'Device model code.',
                                 'model_name': 'Device model name.',
                                 'patient_event_key': 'Deterministic event key derived from source '
                                                      'identifiers; retained as a stable join key.',
                                 'person_id': 'Native Millennium PERSON_ID as BIGINT when resolved.',
                                 'pocket_site': 'Device pocket site.',
                                 'record_status': 'Superseded when mediconnect_device.SOURCE_PRESENT_IND is '
                                                  'false, otherwise active; missing presence defaults true. '
                                                  'No retracted status is emitted. STATUS, EXPLANTED_IND and '
                                                  'mapping status do not set this derived label, and '
                                                  'registry rows in the shared internal stage are filtered '
                                                  'out before device publication.',
                                 'record_status_effective_from': 'Validated implant date derived from '
                                                                 'IMPLANTED_DATE, excluding sentinel, future '
                                                                 'and missing values flagged by '
                                                                 'IMPLANTED_DATE_QUALITY.',
                                 'record_status_effective_to': 'For a source-absent mediconnect_device row, '
                                                               'first non-null SOURCE_ABSENT_DETECTED_TS '
                                                               'then ADC_UPDT; otherwise null. This is not '
                                                               'an explant timestamp. Registry-family '
                                                               'absence clocks and device-type mapping '
                                                               'clocks are not used.',
                                 'serial_number': 'Device serial number.',
                                 'source_code': 'Device type with type/model display fallback for unknown '
                                                'type zero.',
                                 'source_coding_system': 'MediConnect device-type coding system.',
                                 'source_display': 'Device type or model display.',
                                 'source_update_timestamp': 'Always null as a TIMESTAMP in the current '
                                                            'MediConnect device projection. Neither ADC_UPDT '
                                                            'nor registry DATE_LAST_CHANGED is substituted. '
                                                            'Registry rows are filtered out of the shared '
                                                            'internal stage before device publication; '
                                                            'device-type mappings select no clock.',
                                 'status_display': 'Source device status display.',
                                 'subject_id_system': 'Identifier system used for subject_key.',
                                 'subject_key': 'Deterministic SHA-256 over the strongest source identifier. '
                                                'Not salted, not secret; a stable join key across feeds '
                                                'where person_id is unresolved.',
                                 'vip_ind': 'VIP indicator when supplied.',
                                 'withheld_identity_ind': 'Identity-withholding status is not populated by '
                                                          'the current source projections; null does not '
                                                          'mean identity was not withheld.'},
                     'quantity_override': False,
                     'table': 'One frozen MediConnect implanted-device registry row with source-type and '
                              'mapped SNOMED device coding. Gold QC twin of the silver product: 2 columns '
                              'are repaired or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each '
                              'rule states its reason in the pipeline notebook, and Lakeflow expectation '
                              'metrics report what every rule matched on each update. Coverage: Frozen feeds '
                              'with no rows in the last 12 months: condition stage to 2023-10, costed '
                              'activity and cost line items to 2022-03, device to 2020-01; complete-through '
                              'month per release: 6_mgmt.data_quality.release_finding (finding_code '
                              'F2_complete_through).'}}
