"""Gold clinical.medication_dispense: 2 dataset(s).

- datasets: _qc_clinical_medication_dispense, clinical_medication_dispense
- moved verbatim from: 2_gold_clinical_events:3584, 2_gold_clinical_events:3624, 2_gold_clinical_events:3633, 2_gold_clinical_events:3699, 2_gold_clinical_events:3703, 2_gold_clinical_events:3716

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.medication_dispense ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_MEDICATION_DISPENSE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`pharmacy_issue_id` AS `pharmacy_issue_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    "CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1970-01-01' THEN NULL ELSE `event_datetime` END AS `event_datetime`",
    '`event_end_datetime` AS `event_end_datetime`',
    "CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1970-01-01' THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`",  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`medication_code` AS `medication_code`',
    '`medication_source_system` AS `medication_source_system`',  # SRC_LINEAGE_V1
    '`medication_source_code` AS `medication_source_code`',  # SRC_LINEAGE_V1
    '`medication_source_display` AS `medication_source_display`',  # SRC_LINEAGE_V1
    '`status_code` AS `status_code`',
    '`issue_type` AS `issue_type`',
    '`issue_category` AS `issue_category`',
    '`quantity` AS `quantity`',
    '`quantity_unit` AS `quantity_unit`',
    '`issued_containers` AS `issued_containers`',
    '`units_per_container` AS `units_per_container`',
    '`drug_form` AS `drug_form`',
    '`drug_strength` AS `drug_strength`',
    '`location_code` AS `location_code`',
    '`issue_value_gbp` AS `issue_value_gbp`',
    '`source_transaction_identifier` AS `source_transaction_identifier`',
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


# contract v2: QC/batch inputs come from internal _clinical_medication_dispense_metadata; source history stays on the main research table.
CLINICAL_MEDICATION_DISPENSE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 7,616,956 rows of 7,673,150
    # that are current and attributable; identity_status = 'resolved' keeps the 7,146,842
    # rows of 7,673,150 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_MEDICATION_DISPENSE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.medication_dispense.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 526,308 of 7,673,150 at the profile.
    "gold.clinical.medication_dispense.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 56,194 of 7,673,150 at the profile.
    "gold.clinical.medication_dispense.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 3,063,192 of 7,673,150 rows (39.9%) when profiled on 2026-08-24.
    "gold.clinical.medication_dispense.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_MEDICATION_DISPENSE_PERSON_SCOPE_RULES = {
    "gold.clinical.medication_dispense.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_dispense"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_DISPENSE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MEDICATION_DISPENSE_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_DISPENSE_PERSON_SCOPE_RULES)
def _gold_qc_clinical_medication_dispense():
    """Quality-controlled twin of journey_clinical.medication_dispense."""
    df = _qc("clinical_medication_dispense", CLINICAL_MEDICATION_DISPENSE_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.medication_dispense"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_medication_dispense():
    """Contract-v2 public twin of clinical_medication_dispense; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_medication_dispense")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`pharmacy_issue_id` AS `pharmacy_issue_id`',
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
        '`__gold_json_medication_code` AS `medication_code`',
        '`medication_source_system` AS `medication_source_system`',  # SRC_LINEAGE_V1
        '`medication_source_code` AS `medication_source_code`',  # SRC_LINEAGE_V1
        '`medication_source_display` AS `medication_source_display`',  # SRC_LINEAGE_V1
        '`status_code` AS `status_code`',
        '`issue_type` AS `issue_type`',
        '`issue_category` AS `issue_category`',
        '`quantity` AS `quantity`',
        '`quantity_unit` AS `quantity_unit`',
        '`issued_containers` AS `issued_containers`',
        '`units_per_container` AS `units_per_container`',
        '`drug_form` AS `drug_form`',
        '`drug_strength` AS `drug_strength`',
        '`location_code` AS `location_code`',
        '`issue_value_gbp` AS `issue_value_gbp`',
        '`source_transaction_identifier` AS `source_transaction_identifier`',
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
    return _s3_flatten_gold_public(df, "clinical_medication_dispense")


def _public_steps_clinical_medication_dispense(df):
    """clinical_medication_dispense: the public body after its projection (GoldRules.public_select), moved from gold_clinical_medication_dispense; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_medication_dispense')


RULES = {
    'clinical_medication_dispense': GoldRules(
        table='clinical_medication_dispense',
        select=CLINICAL_MEDICATION_DISPENSE_SELECT,
        mandatory=CLINICAL_MEDICATION_DISPENSE_MANDATORY_RULES,
        advisory=CLINICAL_MEDICATION_DISPENSE_ADVISORY_RULES,
        person_scope=CLINICAL_MEDICATION_DISPENSE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_before_birth',),
        variant_columns=('medication_code',),
        s3_axis_specs={'medication_code': [('medication', False)]},
        value_rules={'gold.clinical.medication_dispense.medication_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                            'column': 'medication_snomed_code',
                                                                                            'companions': ['medication_snomed_display',
                                                                                                           'medication_target_code',
                                                                                                           'medication_target_display'],
                                                                                            'match': '`medication_snomed_code` '
                                                                                                     'IS '
                                                                                                     'NOT '
                                                                                                     'NULL '
                                                                                                     'AND '
                                                                                                     '(NOT '
                                                                                                     '`medication_snomed_code` '
                                                                                                     'RLIKE '
                                                                                                     "'^[1-9][0-9]{5,17}$' "
                                                                                                     'OR '
                                                                                                     'substring(`medication_snomed_code`, '
                                                                                                     'length(`medication_snomed_code`) '
                                                                                                     '- 2, '
                                                                                                     '2) '
                                                                                                     'NOT '
                                                                                                     'IN '
                                                                                                     "('00', "
                                                                                                     "'10') "
                                                                                                     'OR '
                                                                                                     '(substring(`medication_snomed_code`, '
                                                                                                     'length(`medication_snomed_code`) '
                                                                                                     '- 2, '
                                                                                                     '2) = '
                                                                                                     "'10' "
                                                                                                     'AND '
                                                                                                     'length(`medication_snomed_code`) '
                                                                                                     '< '
                                                                                                     '11))',
                                                                                            'table': 'clinical_medication_dispense'},
         'gold.clinical.medication_dispense.medication_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                            'column': 'medication_target_code',
                                                                                            'companions': ['medication_target_display',
                                                                                                           'medication_snomed_code',
                                                                                                           'medication_snomed_display'],
                                                                                            'match': '`medication_target_system` '
                                                                                                     '= '
                                                                                                     "'http://snomed.info/sct' "
                                                                                                     'AND '
                                                                                                     '`medication_target_code` '
                                                                                                     'IS '
                                                                                                     'NOT '
                                                                                                     'NULL '
                                                                                                     'AND '
                                                                                                     '(NOT '
                                                                                                     '`medication_target_code` '
                                                                                                     'RLIKE '
                                                                                                     "'^[1-9][0-9]{5,17}$' "
                                                                                                     'OR '
                                                                                                     'substring(`medication_target_code`, '
                                                                                                     'length(`medication_target_code`) '
                                                                                                     '- 2, '
                                                                                                     '2) '
                                                                                                     'NOT '
                                                                                                     'IN '
                                                                                                     "('00', "
                                                                                                     "'10') "
                                                                                                     'OR '
                                                                                                     '(substring(`medication_target_code`, '
                                                                                                     'length(`medication_target_code`) '
                                                                                                     '- 2, '
                                                                                                     '2) = '
                                                                                                     "'10' "
                                                                                                     'AND '
                                                                                                     'length(`medication_target_code`) '
                                                                                                     '< '
                                                                                                     '11))',
                                                                                            'table': 'clinical_medication_dispense'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`source_patient_event_key` AS `source_patient_event_key`',
            '`pharmacy_issue_id` AS `pharmacy_issue_id`',
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
            '`__gold_json_medication_code` AS `medication_code`',
            '`medication_source_system` AS `medication_source_system`',  # SRC_LINEAGE_V1
            '`medication_source_code` AS `medication_source_code`',  # SRC_LINEAGE_V1
            '`medication_source_display` AS `medication_source_display`',  # SRC_LINEAGE_V1
            '`status_code` AS `status_code`',
            '`issue_type` AS `issue_type`',
            '`issue_category` AS `issue_category`',
            '`quantity` AS `quantity`',
            '`quantity_unit` AS `quantity_unit`',
            '`issued_containers` AS `issued_containers`',
            '`units_per_container` AS `units_per_container`',
            '`drug_form` AS `drug_form`',
            '`drug_strength` AS `drug_strength`',
            '`location_code` AS `location_code`',
            '`issue_value_gbp` AS `issue_value_gbp`',
            '`source_transaction_identifier` AS `source_transaction_identifier`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`source_feed` AS `source_feed`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
        ],
        public_steps=(_public_steps_clinical_medication_dispense,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_medication_dispense': {'columns': {'confidentiality_code': 'Source confidentiality code when '
                                                                          'supplied.',
                                                  'drug_form': 'Source drug form.',
                                                  'drug_strength': 'Source drug strength.',
                                                  'encounter_id': 'Nullable Millennium ENCNTR_ID; JAC does '
                                                                  'not supply encounter context.',
                                                  'event_datetime': 'Source issue timestamp.',
                                                  'event_datetime_local': 'Source issue timestamp for the '
                                                                          'dispense event expressed in local '
                                                                          'time. Gold: NULL wherever Gold '
                                                                          'nulls event_datetime.',
                                                  'event_end_datetime': 'Dispense end timestamp when '
                                                                        'supplied.',
                                                  'event_end_datetime_local': 'Dispense end timestamp '
                                                                              'expressed in local time when '
                                                                              'supplied.',
                                                  'issue_category': 'Governed JAC transaction category; only '
                                                                    'ISSUE is typed here.',
                                                  'issue_type': 'Raw JAC issue type.',
                                                  'issue_value_gbp': 'Source-recorded issue value including '
                                                                     'legitimate negative values outside '
                                                                     'this route.',
                                                  'issued_containers': 'Parsed source container count.',
                                                  'loaded_at': 'map_pharmacy_issue.ADC_UPDT carried '
                                                               'unchanged through ISSUE-category and '
                                                               'usable-code filtering and the QC/public '
                                                               'projection. It is distinct from '
                                                               'SOURCE_RECORD_UPDATED_DT used for the '
                                                               'source-update/history-start fields and from '
                                                               'ISSUE_DTTM. QC adds no parent clocks; Silver '
                                                               'refresh time is not substituted.',
                                                  'location_code': 'Millennium care-site code supplied by '
                                                                   'the JAC crosswalk.',
                                                  'medication_code': 'Source JAC and mapped dm+d medication '
                                                                     'codings.',
                                                  'patient_event_key': 'Deterministic SHA-256 event '
                                                                       'identity; stable cross-feed join '
                                                                       'key.',
                                                  'person_id': 'Millennium PERSON_ID when resolved; native '
                                                               'person foreign key.',
                                                  'pharmacy_issue_id': 'JAC PHARMACY_ISSUE_ID; primary key '
                                                                       'of this table.',
                                                  'quantity': 'Best-effort parsed total units.',
                                                  'quantity_unit': 'Source dose-unit description.',
                                                  'record_status': 'Retracted when '
                                                                   'map_pharmacy_issue.SOURCE_PRESENT_IND is '
                                                                   'false, otherwise active; a missing '
                                                                   'presence flag does not retract the row. '
                                                                   'No superseded status is emitted. This is '
                                                                   'distinct from clinical status_code '
                                                                   'stopped/completed. Only normalized '
                                                                   'ISSUE-category rows with usable '
                                                                   'medication codes are eligible.',
                                                  'record_status_effective_from': 'Upstream mirror '
                                                                                  'timestamp; '
                                                                                  'snapshot-level, not a '
                                                                                  'reliable row watermark.',
                                                  'record_status_effective_to': 'map_pharmacy_issue.ADC_UPDT '
                                                                                'only when '
                                                                                'SOURCE_PRESENT_IND is '
                                                                                'false; otherwise null. This '
                                                                                'is the input-load-time end '
                                                                                'proxy, not ISSUE_DTTM or a '
                                                                                'clinical dispensing '
                                                                                'completion time; a missing '
                                                                                'ADC_UPDT remains null.',
                                                  'source_code': 'Verbatim JAC drug identifier.',
                                                  'source_coding_system': 'Verbatim JAC drug coding system.',
                                                  'source_display': 'Verbatim JAC drug description.',
                                                  'source_feed': 'Registered source feed owning the typed '
                                                                 'fact.',
                                                  'source_patient_event_key': 'Stable patient-event identity '
                                                                              'retained from the originating '
                                                                              'Silver row before source '
                                                                              'splitting.',
                                                  'source_transaction_identifier': 'Raw JAC dailyissues '
                                                                                   'traceability key.',
                                                  'source_update_timestamp': 'Upstream mirror timestamp; '
                                                                             'snapshot-level, not a reliable '
                                                                             'row watermark.',
                                                  'status_code': 'Source-presence-derived dispense status.',
                                                  'subject_id_system': 'Identifier system used for '
                                                                       'subject_key.',
                                                  'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                 'source identifier. Not salted, not secret; '
                                                                 'a stable join key across feeds where '
                                                                 'person_id is unresolved.',
                                                  'units_per_container': 'Parsed source units per container.',
                                                  'vip_ind': 'VIP indicator when supplied.',
                                                  'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                           'supplied.'},
                                      'quantity_override': True,
                                      'table': 'Internal quality-controlled twin of '
                                               'clinical_medication_dispense: the Gold repairs, nulls and '
                                               'expectations are applied here and the published Gold MV '
                                               'reads this object. Materialized rather than temporary so the '
                                               'published MV can refresh incrementally.'},
 'clinical_medication_dispense': {'columns': {'confidentiality_code': 'Source confidentiality code when '
                                                                      'supplied.',
                                              'drug_form': 'Source drug form.',
                                              'drug_strength': 'Source drug strength.',
                                              'encounter_id': 'Nullable Millennium ENCNTR_ID; JAC does not '
                                                              'supply encounter context.',
                                              'event_datetime': 'Source issue timestamp.',
                                              'event_datetime_local': 'Source issue timestamp for the '
                                                                      'dispense event expressed in local '
                                                                      'time. Gold: NULL wherever Gold nulls '
                                                                      'event_datetime.',
                                              'event_end_datetime': 'Dispense end timestamp when supplied.',
                                              'event_end_datetime_local': 'Dispense end timestamp expressed '
                                                                          'in local time when supplied.',
                                              'issue_category': 'Governed JAC transaction category; only '
                                                                'ISSUE is typed here.',
                                              'issue_type': 'Raw JAC issue type.',
                                              'issue_value_gbp': 'Source-recorded issue value including '
                                                                 'legitimate negative values outside this '
                                                                 'route.',
                                              'issued_containers': 'Parsed source container count.',
                                              'loaded_at': 'map_pharmacy_issue.ADC_UPDT carried unchanged '
                                                           'through ISSUE-category and usable-code filtering '
                                                           'and the QC/public projection. It is distinct '
                                                           'from SOURCE_RECORD_UPDATED_DT used for the '
                                                           'source-update/history-start fields and from '
                                                           'ISSUE_DTTM. QC adds no parent clocks; Silver '
                                                           'refresh time is not substituted.',
                                              'location_code': 'Millennium care-site code supplied by the '
                                                               'JAC crosswalk.',
                                              'medication_code': 'Source JAC and mapped dm+d medication '
                                                                 'codings.',
                                              'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                   'stable cross-feed join key.',
                                              'person_id': 'Millennium PERSON_ID when resolved; native '
                                                           'person foreign key.',
                                              'pharmacy_issue_id': 'JAC PHARMACY_ISSUE_ID; primary key of '
                                                                   'this table.',
                                              'quantity': 'Best-effort parsed total units.',
                                              'quantity_unit': 'Carried only on component 1 of a split '
                                                               'compound medication statement; aggregate '
                                                               'over medication_map_component_index = 1 or '
                                                               'count distinct source_patient_event_key.',
                                              'record_status': 'Retracted when '
                                                               'map_pharmacy_issue.SOURCE_PRESENT_IND is '
                                                               'false, otherwise active; a missing presence '
                                                               'flag does not retract the row. No superseded '
                                                               'status is emitted. This is distinct from '
                                                               'clinical status_code stopped/completed. Only '
                                                               'normalized ISSUE-category rows with usable '
                                                               'medication codes are eligible.',
                                              'record_status_effective_from': 'Upstream mirror timestamp; '
                                                                              'snapshot-level, not a '
                                                                              'reliable row watermark.',
                                              'record_status_effective_to': 'map_pharmacy_issue.ADC_UPDT '
                                                                            'only when SOURCE_PRESENT_IND is '
                                                                            'false; otherwise null. This is '
                                                                            'the input-load-time end proxy, '
                                                                            'not ISSUE_DTTM or a clinical '
                                                                            'dispensing completion time; a '
                                                                            'missing ADC_UPDT remains null.',
                                              'source_code': 'Verbatim JAC drug identifier.',
                                              'source_coding_system': 'Verbatim JAC drug coding system.',
                                              'source_display': 'Verbatim JAC drug description.',
                                              'source_feed': 'Registered source feed owning the typed fact.',
                                              'source_patient_event_key': 'Stable patient-event identity '
                                                                          'retained from the originating '
                                                                          'Silver row before source '
                                                                          'splitting.',
                                              'source_transaction_identifier': 'Raw JAC dailyissues '
                                                                               'traceability key.',
                                              'source_update_timestamp': 'Upstream mirror timestamp; '
                                                                         'snapshot-level, not a reliable row '
                                                                         'watermark.',
                                              'status_code': 'Source-presence-derived dispense status.',
                                              'subject_id_system': 'Identifier system used for subject_key.',
                                              'subject_key': 'Deterministic SHA-256 over the strongest '
                                                             'source identifier. Not salted, not secret; a '
                                                             'stable join key across feeds where person_id '
                                                             'is unresolved.',
                                              'units_per_container': 'Parsed source units per container.',
                                              'vip_ind': 'VIP indicator when supplied.',
                                              'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                       'supplied.'},
                                  'quantity_override': False,
                                  'table': 'One JAC issue/dispense transaction admitted directly by the '
                                           'pharmacy ISSUE route. Gold QC twin of the silver product: 2 '
                                           'columns are repaired or nulled, 1 rule(s) drop rows, 3 check(s) '
                                           'are advisory. Each rule states its reason in the pipeline '
                                           'notebook, and Lakeflow expectation metrics report what every '
                                           'rule matched on each update. Coverage: JAC pharmacy issues are '
                                           'sparse before 2019 (about 22,000 in 2018 against 252,000 in 2019 '
                                           'and 761,000 in 2020); outpatient dispensing starts in 2019; '
                                           'complete-through month per release: '
                                           '6_mgmt.data_quality.release_finding (finding_code '
                                           'F2_complete_through).'}}
