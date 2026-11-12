"""Gold clinical.medication_order: 2 dataset(s).

- datasets: _qc_clinical_medication_order, clinical_medication_order
- moved verbatim from: 2_gold_clinical_events:3777, 2_gold_clinical_events:3830, 2_gold_clinical_events:3835, 2_gold_clinical_events:3844, 2_gold_clinical_events:3964, 2_gold_clinical_events:3968, 2_gold_clinical_events:3981

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.medication_order ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_MEDICATION_ORDER_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`order_id` AS `order_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    "CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE CASE WHEN YEAR(CAST(`event_end_datetime` AS DATE)) >= 9999 OR CAST(`event_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_end_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_end_datetime` END END AS `event_end_datetime`",
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    "CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE CASE WHEN YEAR(CAST(`event_end_datetime` AS DATE)) >= 9999 OR CAST(`event_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_end_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_end_datetime` END END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`",  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`medication_code` AS `medication_code`',
    '`order_status_code` AS `order_status_code`',
    '`order_status_display` AS `order_status_display`',
    '`department_status_code` AS `department_status_code`',
    '`department_status_display` AS `department_status_display`',
    '`active_status_code` AS `active_status_code`',
    '`active_status_display` AS `active_status_display`',
    '`intent_code` AS `intent_code`',
    '`medication_order_type_code` AS `medication_order_type_code`',
    '`medication_order_type_display` AS `medication_order_type_display`',
    '`authored_datetime` AS `authored_datetime`',
    '`effective_start_datetime` AS `effective_start_datetime`',
    "CASE WHEN YEAR(CAST(`projected_stop_datetime` AS DATE)) >= 9999 OR CAST(`projected_stop_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`projected_stop_datetime` AS DATE)) < 9999 THEN NULL ELSE `projected_stop_datetime` END AS `projected_stop_datetime`",
    '`discontinued_datetime` AS `discontinued_datetime`',
    '`frequency_id` AS `frequency_id`',
    '`prn_ind` AS `prn_ind`',
    '`iv_ind` AS `iv_ind`',
    '`suspend_ind` AS `suspend_ind`',
    '`resume_ind` AS `resume_ind`',
    '`discontinue_ind` AS `discontinue_ind`',
    # INSR_W3 W3.06: Gold: NULL where the Cerner system user id 1 (J5; silver publishes the flag).
    "CASE WHEN CAST(`requester_practitioner_id` AS BIGINT) = 1 THEN NULL ELSE `requester_practitioner_id` END AS `requester_practitioner_id`",
    '`organization_id` AS `organization_id`',
    '`clinical_display_line` AS `clinical_display_line`',
    '`order_detail_display_line` AS `order_detail_display_line`',
    '`status_history_count` AS `status_history_count`',
    '`ingredient_count` AS `ingredient_count`',
    '`order_detail_count` AS `order_detail_count`',
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


CLINICAL_MEDICATION_ORDER_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`requester_system_ind` AS `requester_system_ind`',
]


# contract v2: QC/batch inputs come from internal _clinical_medication_order_metadata; source history stays on the main research table.
CLINICAL_MEDICATION_ORDER_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 365,848,704 rows of
    # 365,848,704 that are current and attributable; identity_status = 'resolved' keeps the
    # 365,848,704 rows of 365,848,704 that are current and attributable. Superseded versions
    # and rows whose identity was never resolved are not research data, and a consumer who
    # wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_MEDICATION_ORDER_ADVISORY_RULES = {
    # INSR_W4 BBR date_ordering (design BBR-13): A prescription (not a continuous-order instance, whose start is its scheduled dose time) should start within 30 days of being authored (BBR-13).
    'gold.clinical.medication_order.effective_start_datetime.insr_bbr_start_after_authored':
        "NOT COALESCE((coalesce(`order_role`, '') <> 'instance' AND `effective_start_datetime` > `authored_datetime` + INTERVAL 30 DAYS), FALSE)",
    # INSR_W4 W4.05 date_future (estate 2026-10-29): Projected stops more than 5 years after load are almost always keying errors (L1012).
    'gold.clinical.medication_order.projected_stop_datetime.insr_f5f_projected_stop_far':
        'NOT COALESCE((`projected_stop_datetime` > `loaded_at` + INTERVAL 1825 DAYS), FALSE)',
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.medication_order.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 464 of 365,848,704
    # rows (0.000127%) when profiled on 2026-08-24.
    "gold.clinical.medication_order.effective_start_datetime.future_owner":
        "NOT COALESCE((CAST(`effective_start_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counted rather than nulled because an order that is still active has a planned stop
    # date, which is in the future by construction. Seen on 7,281 of 365,848,704 rows
    # (0.00199%) when profiled on 2026-08-24.
    "gold.clinical.medication_order.event_end_datetime.future_owner":
        "NOT COALESCE((CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 365,848,704 at the profile.
    "gold.clinical.medication_order.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 7,281 of
    # 365,848,704 rows (0.00199%) when profiled on 2026-08-24.
    "gold.clinical.medication_order.projected_stop_datetime.future_owner":
        "NOT COALESCE((CAST(`projected_stop_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 365,848,704 at the profile.
    "gold.clinical.medication_order.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 180,555 of 365,848,704 rows (0.0494%) when profiled on
    # 2026-08-24.
    "gold.clinical.medication_order.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 27,575 of 365,848,704 rows (0.00754%) when profiled on
    # 2026-08-24.
    "gold.clinical.medication_order.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_MEDICATION_ORDER_PERSON_SCOPE_RULES = {
    "gold.clinical.medication_order.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_order"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ORDER_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MEDICATION_ORDER_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ORDER_PERSON_SCOPE_RULES)
def _gold_qc_clinical_medication_order():
    """Quality-controlled twin of journey_clinical.medication_order."""
    df = _qc("clinical_medication_order", CLINICAL_MEDICATION_ORDER_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.medication_order"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_medication_order():
    """Contract-v2 public twin of clinical_medication_order; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_medication_order")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`order_id` AS `order_id`',
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
        '`order_status_code` AS `order_status_code`',
        '`order_status_display` AS `order_status_display`',
        '`department_status_code` AS `department_status_code`',
        '`department_status_display` AS `department_status_display`',
        '`active_status_code` AS `active_status_code`',
        '`active_status_display` AS `active_status_display`',
        '`intent_code` AS `intent_code`',
        '`medication_order_type_code` AS `medication_order_type_code`',
        '`medication_order_type_display` AS `medication_order_type_display`',
        '`authored_datetime` AS `authored_datetime`',
        '`effective_start_datetime` AS `effective_start_datetime`',
        '`projected_stop_datetime` AS `projected_stop_datetime`',
        '`discontinued_datetime` AS `discontinued_datetime`',
        '`frequency_id` AS `frequency_id`',
        '`prn_ind` AS `prn_ind`',
        '`iv_ind` AS `iv_ind`',
        '`suspend_ind` AS `suspend_ind`',
        '`resume_ind` AS `resume_ind`',
        '`discontinue_ind` AS `discontinue_ind`',
        '`requester_practitioner_id` AS `requester_practitioner_id`',
        '`organization_id` AS `organization_id`',
        '`clinical_display_line` AS `clinical_display_line`',
        '`order_detail_display_line` AS `order_detail_display_line`',
        '`status_history_count` AS `status_history_count`',
        '`ingredient_count` AS `ingredient_count`',
        '`order_detail_count` AS `order_detail_count`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        '`requester_system_ind` AS `requester_system_ind`',  # INSR_W3 W3.12
    )
    return _s3_flatten_gold_public(df, "clinical_medication_order")


def _public_steps_clinical_medication_order(df):
    """clinical_medication_order: the public body after its projection (GoldRules.public_select), moved from gold_clinical_medication_order; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_medication_order')


RULES = {
    'clinical_medication_order': GoldRules(
        table='clinical_medication_order',
        select=CLINICAL_MEDICATION_ORDER_SELECT,
        mandatory=CLINICAL_MEDICATION_ORDER_MANDATORY_RULES,
        advisory=CLINICAL_MEDICATION_ORDER_ADVISORY_RULES,
        person_scope=CLINICAL_MEDICATION_ORDER_PERSON_SCOPE_RULES,
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
        variant_columns=('medication_code',),
        s3_axis_specs={'medication_code': [('medication', False)]},
        value_rules={'gold.clinical.medication_order.frequency_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                         'column': 'frequency_id',
                                                                         'companions': [],
                                                                         'match': 'TRIM(`frequency_id`) = '
                                                                                  "'0'",
                                                                         'table': 'clinical_medication_order'},
         'gold.clinical.medication_order.medication_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                         'column': 'medication_snomed_code',
                                                                                         'companions': ['medication_snomed_display',
                                                                                                        'medication_target_code',
                                                                                                        'medication_target_display'],
                                                                                         'match': '`medication_snomed_code` '
                                                                                                  'IS NOT '
                                                                                                  'NULL '
                                                                                                  'AND '
                                                                                                  '(NOT '
                                                                                                  '`medication_snomed_code` '
                                                                                                  'RLIKE '
                                                                                                  "'^[1-9][0-9]{5,17}$' "
                                                                                                  'OR '
                                                                                                  'substring(`medication_snomed_code`, '
                                                                                                  'length(`medication_snomed_code`) '
                                                                                                  '- 2, 2) '
                                                                                                  'NOT IN '
                                                                                                  "('00', "
                                                                                                  "'10') "
                                                                                                  'OR '
                                                                                                  '(substring(`medication_snomed_code`, '
                                                                                                  'length(`medication_snomed_code`) '
                                                                                                  '- 2, 2) '
                                                                                                  "= '10' "
                                                                                                  'AND '
                                                                                                  'length(`medication_snomed_code`) '
                                                                                                  '< 11))',
                                                                                         'table': 'clinical_medication_order'},
         'gold.clinical.medication_order.medication_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                         'column': 'medication_target_code',
                                                                                         'companions': ['medication_target_display',
                                                                                                        'medication_snomed_code',
                                                                                                        'medication_snomed_display'],
                                                                                         'match': '`medication_target_system` '
                                                                                                  '= '
                                                                                                  "'http://snomed.info/sct' "
                                                                                                  'AND '
                                                                                                  '`medication_target_code` '
                                                                                                  'IS NOT '
                                                                                                  'NULL '
                                                                                                  'AND '
                                                                                                  '(NOT '
                                                                                                  '`medication_target_code` '
                                                                                                  'RLIKE '
                                                                                                  "'^[1-9][0-9]{5,17}$' "
                                                                                                  'OR '
                                                                                                  'substring(`medication_target_code`, '
                                                                                                  'length(`medication_target_code`) '
                                                                                                  '- 2, 2) '
                                                                                                  'NOT IN '
                                                                                                  "('00', "
                                                                                                  "'10') "
                                                                                                  'OR '
                                                                                                  '(substring(`medication_target_code`, '
                                                                                                  'length(`medication_target_code`) '
                                                                                                  '- 2, 2) '
                                                                                                  "= '10' "
                                                                                                  'AND '
                                                                                                  'length(`medication_target_code`) '
                                                                                                  '< 11))',
                                                                                         'table': 'clinical_medication_order'},
         'gold.clinical.medication_order.requester_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                      'column': 'requester_practitioner_id',
                                                                                      'companions': [],
                                                                                      'match': '`requester_practitioner_id` '
                                                                                               '= 0',
                                                                                      'table': 'clinical_medication_order'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`source_patient_event_key` AS `source_patient_event_key`',
            '`order_id` AS `order_id`',
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
            '`order_status_code` AS `order_status_code`',
            '`order_status_display` AS `order_status_display`',
            '`department_status_code` AS `department_status_code`',
            '`department_status_display` AS `department_status_display`',
            '`active_status_code` AS `active_status_code`',
            '`active_status_display` AS `active_status_display`',
            '`intent_code` AS `intent_code`',
            '`medication_order_type_code` AS `medication_order_type_code`',
            '`medication_order_type_display` AS `medication_order_type_display`',
            '`authored_datetime` AS `authored_datetime`',
            '`effective_start_datetime` AS `effective_start_datetime`',
            '`projected_stop_datetime` AS `projected_stop_datetime`',
            '`discontinued_datetime` AS `discontinued_datetime`',
            '`frequency_id` AS `frequency_id`',
            '`prn_ind` AS `prn_ind`',
            '`iv_ind` AS `iv_ind`',
            '`suspend_ind` AS `suspend_ind`',
            '`resume_ind` AS `resume_ind`',
            '`discontinue_ind` AS `discontinue_ind`',
            '`requester_practitioner_id` AS `requester_practitioner_id`',
            '`organization_id` AS `organization_id`',
            '`clinical_display_line` AS `clinical_display_line`',
            '`order_detail_display_line` AS `order_detail_display_line`',
            '`status_history_count` AS `status_history_count`',
            '`ingredient_count` AS `ingredient_count`',
            '`order_detail_count` AS `order_detail_count`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`source_feed` AS `source_feed`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
            '`requester_system_ind` AS `requester_system_ind`',
        ],
        public_steps=(_public_steps_clinical_medication_order,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_medication_order': {'columns': {'active_status_code': 'Source active-status code.',
                                               'active_status_display': 'Source active-status display.',
                                               'authored_datetime': 'Original order time.',
                                               'clinical_display_line': 'Source clinical display line.',
                                               'confidentiality_code': 'Source confidentiality '
                                                                       'classification attached to the '
                                                                       'record for each medication order '
                                                                       'record. It is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'department_status_code': 'Department workflow status code.',
                                               'department_status_display': 'Department workflow status '
                                                                            'display.',
                                               'discontinue_ind': 'Source discontinued indicator.',
                                               'discontinued_datetime': 'Discontinue effective time.',
                                               'effective_start_datetime': 'Current effective start.',
                                               'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                               'spine_encounter.encounter_id.',
                                               'event_datetime': 'Order authored or start time.',
                                               'event_datetime_local': 'Order authored or start time of the '
                                                                       'medication order expressed in local '
                                                                       'time.',
                                               'event_end_datetime': 'Projected stop or discontinue time.',
                                               'event_end_datetime_local': 'Projected stop or discontinue '
                                                                           'time of the medication order '
                                                                           'expressed in local time. Gold: '
                                                                           'NULL wherever Gold nulls '
                                                                           'event_end_datetime.',
                                               'frequency_id': 'Source frequency identifier.',
                                               'ingredient_count': 'Number of retained ingredient rows.',
                                               'intent_code': 'Medication request intent.',
                                               'iv_ind': 'S10 medication_order; retained as a '
                                                         'source-faithful bronze input or Journey '
                                                         'standard-block field.',
                                               'loaded_at': 'Greatest of map_medication_order.ADC_UPDT and '
                                                            'the per-ORDER_ID maximum ADC_UPDT from '
                                                            'map_medication_order_action, '
                                                            'map_medication_order_ingredient and '
                                                            'map_medication_order_detail. All contributing '
                                                            'child rows participate, not only the latest '
                                                            'action; JSON decoding and quality/public '
                                                            'projections preserve this combined ingestion '
                                                            'clock.',
                                               'medication_code': 'Source medication CodeableConcept.',
                                               'medication_order_type_code': 'Source medication-order type '
                                                                             'code.',
                                               'medication_order_type_display': 'Source medication-order '
                                                                                'type display.',
                                               'order_detail_count': 'Number of retained detail rows.',
                                               'order_detail_display_line': 'Source order-detail display '
                                                                            'line.',
                                               'order_id': 'Millennium ORDER_ID; primary key of this table.',
                                               'order_status_code': 'Current source order status code.',
                                               'order_status_display': 'Current source order status display.',
                                               'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                                  'organization.',
                                               'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                    'stable cross-feed join key.',
                                               'person_id': 'Millennium PERSON_ID; native person foreign '
                                                            'key.',
                                               'prn_ind': 'True when source `PRN_IND` equals 1, indicating '
                                                          'as-needed medication; other non-null values '
                                                          'become false and null remains null.',
                                               'projected_stop_datetime': 'Projected stop time.',
                                               'record_status': 'Retracted when SOURCE_PRESENT_IND is false '
                                                                '(null defaults true); otherwise superseded '
                                                                'when INACTIVE_ORDER_FLAG equals 1 (null '
                                                                'defaults 0) or ACTIVE_IND equals 0 (null '
                                                                'defaults 1); otherwise active. Retraction '
                                                                'takes priority. Clinical order status, '
                                                                'discontinue flags and child-source presence '
                                                                'do not directly determine this label.',
                                               'record_status_effective_from': 'map_medication_order.ORIG_ORDER_DT_TM '
                                                                               'carried unchanged as the '
                                                                               'history start. Unlike '
                                                                               'event_datetime, it has no '
                                                                               'CURRENT_START_DT_TM '
                                                                               'fallback; this '
                                                                               'authored-order time is not '
                                                                               'an independently observed '
                                                                               'status-transition timestamp.',
                                               'record_status_effective_to': 'SOURCE_ABSENT_DETECTED_TS for '
                                                                             'retracted orders, even if '
                                                                             'null; otherwise first non-null '
                                                                             'DISCONTINUE_EFFECTIVE_DT_TM '
                                                                             'then STATUS_DT_TM for '
                                                                             'superseded orders; otherwise '
                                                                             'null. There is no fallback '
                                                                             'between branches, no '
                                                                             'projected/soft-stop or '
                                                                             'ingestion fallback, and no '
                                                                             'sentinel range clamp.',
                                               'requester_practitioner_id': 'Millennium personnel PERSON_ID '
                                                                            'for the requesting '
                                                                            'practitioner.',
                                               'requester_system_ind': 'True when the source '
                                                                       'requester_practitioner_id was 1, the '
                                                                       'Cerner system user (J5); the id is '
                                                                       'then published as NULL. NULL when '
                                                                       'the source id was NULL. INSR_W2.',
                                               'resume_ind': 'Source resumed indicator.',
                                               'source_code': 'Source medication synonym identifier.',
                                               'source_coding_system': 'Source medication coding system.',
                                               'source_display': 'Source medication display.',
                                               'source_feed': 'Registered source feed.',
                                               'source_patient_event_key': 'Stable patient-event identity '
                                                                           'retained from the originating '
                                                                           'Silver row before source '
                                                                           'splitting.',
                                               'source_update_timestamp': "Greatest of the order's "
                                                                          'SOURCE_ADC_UPDT and ADC_UPDT plus '
                                                                          'each action, ingredient and '
                                                                          "detail group's maximum of "
                                                                          'greatest(SOURCE_ADC_UPDT, '
                                                                          'ORDER_SOURCE_ADC_UPDT, ADC_UPDT), '
                                                                          'grouped by ORDER_ID across all '
                                                                          'contributing rows. This mixes '
                                                                          'source and ingestion clocks; it '
                                                                          'is not solely a native/raw-source '
                                                                          'update timestamp or Silver '
                                                                          'refresh time.',
                                               'status_history_count': 'Number of retained action rows.',
                                               'subject_id_system': 'Subject-key identifier system.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'suspend_ind': 'Source suspended indicator.',
                                               'vip_ind': 'Indicator that the source record carries VIP '
                                                          'handling status for each medication order record. '
                                                          'It is produced by the silver transformation and '
                                                          'has no direct bronze-column lineage entry. Null '
                                                          'means the source did not state the indicator and '
                                                          'must not be interpreted as false.',
                                               'withheld_identity_ind': 'Withheld identity indicator.'},
                                   'quantity_override': True,
                                   'table': 'Internal quality-controlled twin of clinical_medication_order: '
                                            'the Gold repairs, nulls and expectations are applied here and '
                                            'the published Gold MV reads this object. Materialized rather '
                                            'than temporary so the published MV can refresh incrementally.'},
 'clinical_medication_order': {'columns': {'active_status_code': 'Source active-status code.',
                                           'active_status_display': 'Source active-status display.',
                                           'authored_datetime': 'Original order time.',
                                           'clinical_display_line': 'Source clinical display line.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each '
                                                                   'medication order record. It is produced '
                                                                   'by the silver transformation and has no '
                                                                   'direct bronze-column lineage entry. '
                                                                   'Source code meanings and sentinel values '
                                                                   'are retained unless the pipeline '
                                                                   'explicitly maps them; null means no code '
                                                                   'was supplied.',
                                           'department_status_code': 'Department workflow status code.',
                                           'department_status_display': 'Department workflow status display.',
                                           'discontinue_ind': 'Source discontinued indicator.',
                                           'discontinued_datetime': 'Discontinue effective time.',
                                           'effective_start_datetime': 'Cerner CURRENT_START_DT_TM. On '
                                                                       'instance rows (order_role = '
                                                                       "'instance') this is the scheduled "
                                                                       "dose time and can follow the order's "
                                                                       'last status; on prescriptions it is '
                                                                       'the requested start.',
                                           'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                           'spine_encounter.encounter_id.',
                                           'event_datetime': 'Order authored or start time.',
                                           'event_datetime_local': 'Order authored or start time of the '
                                                                   'medication order expressed in local '
                                                                   'time.',
                                           'event_end_datetime': 'Projected stop or discontinue time.',
                                           'event_end_datetime_local': 'Projected stop or discontinue time '
                                                                       'of the medication order expressed in '
                                                                       'local time. Gold: NULL wherever Gold '
                                                                       'nulls event_end_datetime.',
                                           'frequency_id': 'Source frequency identifier.',
                                           'ingredient_count': 'Number of retained ingredient rows.',
                                           'intent_code': 'FHIR MedicationRequest.intent: plan for a '
                                                          'recorded/home medication (ORIG_ORD_AS_FLAG 2, see '
                                                          'reported_ind and category_code), order for every '
                                                          'other order; NULL when the flag is absent. Was '
                                                          "the constant 'order' before this release.",
                                           'iv_ind': 'S10 medication_order; retained as a source-faithful '
                                                     'bronze input or Journey standard-block field.',
                                           'loaded_at': 'Greatest of map_medication_order.ADC_UPDT and the '
                                                        'per-ORDER_ID maximum ADC_UPDT from '
                                                        'map_medication_order_action, '
                                                        'map_medication_order_ingredient and '
                                                        'map_medication_order_detail. All contributing child '
                                                        'rows participate, not only the latest action; JSON '
                                                        'decoding and quality/public projections preserve '
                                                        'this combined ingestion clock.',
                                           'medication_code': 'Source medication CodeableConcept.',
                                           'medication_order_type_code': 'Source medication-order type code.',
                                           'medication_order_type_display': 'Source medication-order type '
                                                                            'display.',
                                           'order_detail_count': 'Number of retained detail rows.',
                                           'order_detail_display_line': 'Source order-detail display line.',
                                           'order_id': 'Millennium ORDER_ID; primary key of this table.',
                                           'order_status_code': 'Current source order status code.',
                                           'order_status_display': 'Current source order status display.',
                                           'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                              'organization.',
                                           'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                'stable cross-feed join key.',
                                           'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                           'prn_ind': 'True when source `PRN_IND` equals 1, indicating '
                                                      'as-needed medication; other non-null values become '
                                                      'false and null remains null.',
                                           'projected_stop_datetime': 'Projected stop time.',
                                           'record_status': 'Retracted when SOURCE_PRESENT_IND is false '
                                                            '(null defaults true); otherwise superseded when '
                                                            'INACTIVE_ORDER_FLAG equals 1 (null defaults 0) '
                                                            'or ACTIVE_IND equals 0 (null defaults 1); '
                                                            'otherwise active. Retraction takes priority. '
                                                            'Clinical order status, discontinue flags and '
                                                            'child-source presence do not directly determine '
                                                            'this label.',
                                           'record_status_effective_from': 'map_medication_order.ORIG_ORDER_DT_TM '
                                                                           'carried unchanged as the history '
                                                                           'start. Unlike event_datetime, it '
                                                                           'has no CURRENT_START_DT_TM '
                                                                           'fallback; this authored-order '
                                                                           'time is not an independently '
                                                                           'observed status-transition '
                                                                           'timestamp.',
                                           'record_status_effective_to': 'SOURCE_ABSENT_DETECTED_TS for '
                                                                         'retracted orders, even if null; '
                                                                         'otherwise first non-null '
                                                                         'DISCONTINUE_EFFECTIVE_DT_TM then '
                                                                         'STATUS_DT_TM for superseded '
                                                                         'orders; otherwise null. There is '
                                                                         'no fallback between branches, no '
                                                                         'projected/soft-stop or ingestion '
                                                                         'fallback, and no sentinel range '
                                                                         'clamp.',
                                           'requester_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                        'the requesting practitioner.',
                                           'requester_system_ind': 'True when the source '
                                                                   'requester_practitioner_id was 1, the '
                                                                   'Cerner system user (J5); the id is then '
                                                                   'published as NULL. NULL when the source '
                                                                   'id was NULL. INSR_W2.',
                                           'resume_ind': 'Source resumed indicator.',
                                           'source_code': 'Source medication synonym identifier.',
                                           'source_coding_system': 'Source medication coding system.',
                                           'source_display': 'Source medication display.',
                                           'source_feed': 'Registered source feed.',
                                           'source_patient_event_key': 'Stable patient-event identity '
                                                                       'retained from the originating Silver '
                                                                       'row before source splitting.',
                                           'source_update_timestamp': "Greatest of the order's "
                                                                      'SOURCE_ADC_UPDT and ADC_UPDT plus '
                                                                      'each action, ingredient and detail '
                                                                      "group's maximum of "
                                                                      'greatest(SOURCE_ADC_UPDT, '
                                                                      'ORDER_SOURCE_ADC_UPDT, ADC_UPDT), '
                                                                      'grouped by ORDER_ID across all '
                                                                      'contributing rows. This mixes source '
                                                                      'and ingestion clocks; it is not '
                                                                      'solely a native/raw-source update '
                                                                      'timestamp or Silver refresh time.',
                                           'status_history_count': 'Number of retained action rows.',
                                           'subject_id_system': 'Subject-key identifier system.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'suspend_ind': 'Source suspended indicator.',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each medication order record. It is '
                                                      'produced by the silver transformation and has no '
                                                      'direct bronze-column lineage entry. Null means the '
                                                      'source did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Withheld identity indicator.'},
                               'quantity_override': False,
                               'table': 'One Cerner medication order. Continuous orders appear as a parent '
                                        'prescription (order_role continuous_parent) and one instance row '
                                        'per scheduled dose (order_role instance, template_order_id = the '
                                        "parent's order_id); count prescriptions with order_role <> "
                                        "'instance'. Attribute, detail and ingredient rows of an instance "
                                        'belong to its parent: join them on content_order_id (Ben decision '
                                        'BBR-D2, by reference). Administered doses are in '
                                        'clinical_medication_admin. Coverage: No medication orders have an '
                                        'original order date in July 2019 to June 2021 at source (raw ratio '
                                        '0.00); orders active in that window carry earlier original dates; '
                                        'Medication order volume in May-June 2026 is lower at source (raw '
                                        '0.88 by original date; 0.72 by requested start for PMS orders) than '
                                        'in the neighbouring months; Orders loaded in November 2019 run at '
                                        'about 2.5 times the previous months at source (a bulk order load), '
                                        'so first-order dates of encounters pile up in that month; '
                                        'complete-through month per release: '
                                        '6_mgmt.data_quality.release_finding (finding_code '
                                        'F2_complete_through).'}}
