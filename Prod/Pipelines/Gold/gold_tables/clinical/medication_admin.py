"""Gold clinical.medication_admin: 2 dataset(s).

- datasets: _qc_clinical_medication_admin, clinical_medication_admin
- moved verbatim from: 2_gold_clinical_events:3309, 2_gold_clinical_events:3374, 2_gold_clinical_events:3379, 2_gold_clinical_events:3387, 2_gold_clinical_events:3482, 2_gold_clinical_events:3486, 2_gold_clinical_events:3499

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.medication_admin ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_MEDICATION_ADMIN_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`event_id` AS `event_id`',
    '`order_id` AS `order_id`',
    '`location_key` AS `location_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`medication_code` AS `medication_code`',
    '`medication_source_system` AS `medication_source_system`',  # SRC_LINEAGE_V1
    '`medication_source_code` AS `medication_source_code`',  # SRC_LINEAGE_V1
    '`medication_source_display` AS `medication_source_display`',  # SRC_LINEAGE_V1
    '`medication_order_key` AS `medication_order_key`',
    '`administration_status_code` AS `administration_status_code`',
    '`administration_status_display` AS `administration_status_display`',
    "CASE WHEN UPPER(TRIM(CAST(`source_event_type_code` AS STRING))) = '0' THEN NULL ELSE `source_event_type_code` END AS `source_event_type_code`",
    '`source_event_type_display` AS `source_event_type_display`',
    '`status_history_count` AS `status_history_count`',
    'CASE WHEN ABS(CAST(`dose_value` AS DOUBLE)) > 1e12 OR `dose_value` < 0 THEN NULL ELSE `dose_value` END AS `dose_value`',
    '`dose_unit` AS `dose_unit`',
    # INSR_W3 W3.06: Gold: NULL where the constant-zero initial dose (L2879).
    "CASE WHEN `initial_dose_value` = 0 THEN NULL ELSE `initial_dose_value` END AS `initial_dose_value`",
    '`initial_dose_unit` AS `initial_dose_unit`',
    '`dose_in_mg` AS `dose_in_mg`',
    '`dose_in_ml` AS `dose_in_ml`',
    '`dose_standardization_status` AS `dose_standardization_status`',
    "CASE WHEN UPPER(TRIM(CAST(`route_code` AS STRING))) = '0' THEN NULL ELSE `route_code` END AS `route_code`",
    '`route_display` AS `route_display`',
    "CASE WHEN UPPER(TRIM(CAST(`site_code` AS STRING))) = '0' THEN NULL ELSE `site_code` END AS `site_code`",
    '`site_display` AS `site_display`',
    # INSR_W3 W3.06: Gold: NULL where a zero volume with no unit (L2879).
    "CASE WHEN (`infused_volume` = 0 AND (`infused_volume_unit` IS NULL OR TRIM(CAST(`infused_volume_unit` AS STRING)) IN ('', '0'))) THEN NULL ELSE `infused_volume` END AS `infused_volume`",
    '`infused_volume_unit` AS `infused_volume_unit`',
    # INSR_W3 W3.06: Gold: NULL where a zero rate with no unit (L2879).
    "CASE WHEN (`infusion_rate` = 0 AND (`infusion_rate_unit` IS NULL OR TRIM(CAST(`infusion_rate_unit` AS STRING)) IN ('', '0'))) THEN NULL ELSE `infusion_rate` END AS `infusion_rate`",
    '`infusion_rate_unit` AS `infusion_rate_unit`',
    '`ingredient_count` AS `ingredient_count`',
    '`performer_practitioner_id` AS `performer_practitioner_id`',
    '`verifier_practitioner_id` AS `verifier_practitioner_id`',
    '`location_code` AS `location_code`',
    '`organization_id` AS `organization_id`',
    '`scheduled_datetime` AS `scheduled_datetime`',
    'CASE WHEN CAST(`performed_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`performed_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `performed_datetime` END AS `performed_datetime`',
    '`verified_datetime` AS `verified_datetime`',
    '`order_status_code` AS `order_status_code`',
    '`order_status_display` AS `order_status_display`',
    '`prn_ind` AS `prn_ind`',
    '`iv_ind` AS `iv_ind`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    # INSR_W3 W3.05: Gold: NULL where an open-end marker in [2099-12-31, 9999).
    "CASE WHEN (CAST(`record_status_effective_to` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`record_status_effective_to` AS DATE)) < 9999) THEN NULL ELSE `record_status_effective_to` END AS `record_status_effective_to`",
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_MEDICATION_ADMIN_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`all_ingredients_zero_ind` AS `all_ingredients_zero_ind`',
]


# contract v2: QC/batch inputs come from internal _clinical_medication_admin_metadata; source history stays on the main research table.
CLINICAL_MEDICATION_ADMIN_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 60,094,539 rows of
    # 60,094,539 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_MEDICATION_ADMIN_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.medication_admin.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 60,094,539 at the profile.
    "gold.clinical.medication_admin.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 9,527 of 60,094,539 rows (0.0159%) when profiled on 2026-08-24.
    "gold.clinical.medication_admin.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,772 of 60,094,539 rows (0.00295%) when profiled on 2026-08-24.
    "gold.clinical.medication_admin.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_MEDICATION_ADMIN_PERSON_SCOPE_RULES = {
    "gold.clinical.medication_admin.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_admin"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ADMIN_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MEDICATION_ADMIN_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ADMIN_PERSON_SCOPE_RULES)
def _gold_qc_clinical_medication_admin():
    """Quality-controlled twin of journey_clinical.medication_admin."""
    df = _qc("clinical_medication_admin", CLINICAL_MEDICATION_ADMIN_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.medication_admin"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_medication_admin():
    """Contract-v2 public twin of clinical_medication_admin; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_medication_admin")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`event_id` AS `event_id`',
        '`order_id` AS `order_id`',
        '`location_key` AS `location_key`',
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
        '`medication_order_key` AS `medication_order_key`',
        '`administration_status_code` AS `administration_status_code`',
        '`administration_status_display` AS `administration_status_display`',
        '`source_event_type_code` AS `source_event_type_code`',
        '`source_event_type_display` AS `source_event_type_display`',
        '`status_history_count` AS `status_history_count`',
        '`dose_value` AS `dose_value`',
        '`dose_unit` AS `dose_unit`',
        '`initial_dose_value` AS `initial_dose_value`',
        '`initial_dose_unit` AS `initial_dose_unit`',
        '`dose_in_mg` AS `dose_in_mg`',
        '`dose_in_ml` AS `dose_in_ml`',
        '`dose_standardization_status` AS `dose_standardization_status`',
        '`route_code` AS `route_code`',
        '`route_display` AS `route_display`',
        '`site_code` AS `site_code`',
        '`site_display` AS `site_display`',
        '`infused_volume` AS `infused_volume`',
        '`infused_volume_unit` AS `infused_volume_unit`',
        '`infusion_rate` AS `infusion_rate`',
        '`infusion_rate_unit` AS `infusion_rate_unit`',
        '`ingredient_count` AS `ingredient_count`',
        '`performer_practitioner_id` AS `performer_practitioner_id`',
        '`verifier_practitioner_id` AS `verifier_practitioner_id`',
        '`location_code` AS `location_code`',
        '`organization_id` AS `organization_id`',
        '`scheduled_datetime` AS `scheduled_datetime`',
        '`performed_datetime` AS `performed_datetime`',
        '`verified_datetime` AS `verified_datetime`',
        '`order_status_code` AS `order_status_code`',
        '`order_status_display` AS `order_status_display`',
        '`prn_ind` AS `prn_ind`',
        '`iv_ind` AS `iv_ind`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        '`all_ingredients_zero_ind` AS `all_ingredients_zero_ind`',  # INSR_W3 W3.12
    )
    return _s3_flatten_gold_public(df, "clinical_medication_admin")


def _public_steps_clinical_medication_admin(df):
    """clinical_medication_admin: the public body after its projection (GoldRules.public_select), moved from gold_clinical_medication_admin; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_medication_admin')


RULES = {
    'clinical_medication_admin': GoldRules(
        table='clinical_medication_admin',
        select=CLINICAL_MEDICATION_ADMIN_SELECT,
        mandatory=CLINICAL_MEDICATION_ADMIN_MANDATORY_RULES,
        advisory=CLINICAL_MEDICATION_ADMIN_ADVISORY_RULES,
        person_scope=CLINICAL_MEDICATION_ADMIN_PERSON_SCOPE_RULES,
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
        value_rules={'gold.clinical.medication_admin.medication_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
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
                                                                                         'table': 'clinical_medication_admin'},
         'gold.clinical.medication_admin.medication_target_code.insr_sct_concept_only': {'action': 'invalidate',
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
                                                                                         'table': 'clinical_medication_admin'},
         'gold.clinical.medication_admin.verifier_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                     'column': 'verifier_practitioner_id',
                                                                                     'companions': [],
                                                                                     'match': '`verifier_practitioner_id` '
                                                                                              '= 0',
                                                                                     'table': 'clinical_medication_admin'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`source_patient_event_key` AS `source_patient_event_key`',
            '`event_id` AS `event_id`',
            '`order_id` AS `order_id`',
            '`location_key` AS `location_key`',
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
            '`medication_order_key` AS `medication_order_key`',
            '`administration_status_code` AS `administration_status_code`',
            '`administration_status_display` AS `administration_status_display`',
            '`source_event_type_code` AS `source_event_type_code`',
            '`source_event_type_display` AS `source_event_type_display`',
            '`status_history_count` AS `status_history_count`',
            '`dose_value` AS `dose_value`',
            '`dose_unit` AS `dose_unit`',
            '`initial_dose_value` AS `initial_dose_value`',
            '`initial_dose_unit` AS `initial_dose_unit`',
            '`dose_in_mg` AS `dose_in_mg`',
            '`dose_in_ml` AS `dose_in_ml`',
            '`dose_standardization_status` AS `dose_standardization_status`',
            '`route_code` AS `route_code`',
            '`route_display` AS `route_display`',
            '`site_code` AS `site_code`',
            '`site_display` AS `site_display`',
            '`infused_volume` AS `infused_volume`',
            '`infused_volume_unit` AS `infused_volume_unit`',
            '`infusion_rate` AS `infusion_rate`',
            '`infusion_rate_unit` AS `infusion_rate_unit`',
            '`ingredient_count` AS `ingredient_count`',
            '`performer_practitioner_id` AS `performer_practitioner_id`',
            '`verifier_practitioner_id` AS `verifier_practitioner_id`',
            '`location_code` AS `location_code`',
            '`organization_id` AS `organization_id`',
            '`scheduled_datetime` AS `scheduled_datetime`',
            '`performed_datetime` AS `performed_datetime`',
            '`verified_datetime` AS `verified_datetime`',
            '`order_status_code` AS `order_status_code`',
            '`order_status_display` AS `order_status_display`',
            '`prn_ind` AS `prn_ind`',
            '`iv_ind` AS `iv_ind`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`source_feed` AS `source_feed`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
            '`all_ingredients_zero_ind` AS `all_ingredients_zero_ind`',
        ],
        public_steps=(_public_steps_clinical_medication_admin,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_medication_admin': {'columns': {'administration_status_code': 'Current source result status '
                                                                             'code.',
                                               'administration_status_display': 'Current source result '
                                                                                'status display.',
                                               'all_ingredients_zero_ind': 'True when every ingredient of '
                                                                           'this administration recorded a '
                                                                           'dose of 0 (J24): the ingredient '
                                                                           'doses are then published as NULL '
                                                                           'in ingredients and '
                                                                           'clinical_medication_admin_ingredient. '
                                                                           'INSR_W2 W2.05.',
                                               'confidentiality_code': 'Source confidentiality '
                                                                       'classification attached to the '
                                                                       'record for each medication admin '
                                                                       'record. It is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'dose_in_mg': 'Bronze-standardized milligram dose.',
                                               'dose_in_ml': 'Bronze-standardized millilitre dose.',
                                               'dose_standardization_status': 'Bronze dose-standardization '
                                                                              'status.',
                                               'dose_unit': 'Administered dose unit.',
                                               'dose_value': 'Effective administered dose.',
                                               'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                               'spine_encounter.encounter_id.',
                                               'event_datetime': 'Administration start or performed time.',
                                               'event_datetime_local': 'Medication administration start or '
                                                                       'performed time expressed in local '
                                                                       'time.',
                                               'event_end_datetime': 'Administration end time.',
                                               'event_end_datetime_local': 'Medication administration end '
                                                                           'time expressed in local time.',
                                               'event_id': 'Millennium medication-administration EVENT_ID; '
                                                           'primary key of this table.',
                                               'infused_volume': 'The volume at any one point in time that '
                                                                 'remains in the IV Bag.',
                                               'infused_volume_unit': 'Text description of the unit of '
                                                                      'measure for infused volume.',
                                               'infusion_rate': 'For continuously administered medications, '
                                                                'IV or IVP, the infusion rate and unit is '
                                                                'used to capture the flow rate of the '
                                                                'medication into the patient.',
                                               'infusion_rate_unit': 'Text description of the unit of '
                                                                     'measure for volume or quantity of the '
                                                                     'medication.',
                                               'ingredient_count': 'Number of retained ingredient rows.',
                                               'initial_dose_unit': 'Initially documented dose unit.',
                                               'initial_dose_value': 'Initially documented dose.',
                                               'iv_ind': 'Indicator showing whether the order is an '
                                                         'intravenous (IV) medication order.',
                                               'loaded_at': 'Greatest of map_med_admin.ADC_UPDT and the '
                                                            'maximum map_med_admin_ingredient.ADC_UPDT for '
                                                            'the same EVENT_ID. The order-ID existence join '
                                                            'contributes no separate load timestamp. This '
                                                            'combines administration and ingredient '
                                                            'ingestion provenance, not administration event '
                                                            'time or Silver refresh time; null only when all '
                                                            'contributing load clocks are null.',
                                               'location_code': 'Millennium NURSE_UNIT_CD for the '
                                                                'administration location.',
                                               'location_key': 'Deterministic SHA-256 nurse-unit key '
                                                               'retained beside location_code.',
                                               'medication_code': 'Source and mapped medication codings.',
                                               'medication_order_key': 'Deterministic SHA-256 key of the '
                                                                       'linked medication order.',
                                               'order_id': 'Millennium medication ORDER_ID when the linked '
                                                           'order exists.',
                                               'order_status_code': 'Linked source order status code.',
                                               'order_status_display': 'Linked source order status display.',
                                               'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                                  'organization.',
                                               'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                    'stable cross-feed join key.',
                                               'performed_datetime': 'Performed administration time.',
                                               'performer_practitioner_id': 'Millennium personnel PERSON_ID '
                                                                            'for the administration '
                                                                            'performer.',
                                               'person_id': 'Millennium PERSON_ID; native person foreign '
                                                            'key.',
                                               'prn_ind': 'True when source `PRN_IND` equals 1, indicating '
                                                          'as-needed medication; other non-null values '
                                                          'become false and null remains null.',
                                               'record_status': 'Derived administration-row status: '
                                                                'superseded when '
                                                                'map_med_admin.CE_VALID_UNTIL_DT_TM is '
                                                                'before 2100-01-01 or MR_IS_CURRENT_IND is '
                                                                'false; otherwise active. Null '
                                                                'MR_IS_CURRENT_IND defaults true and a null '
                                                                'clinical-event end alone does not supersede '
                                                                'the row. This projection does not emit '
                                                                'retracted or derive status from the '
                                                                'administration result/workflow code.',
                                               'record_status_effective_from': 'First non-null '
                                                                               'map_med_admin.CE_VALID_FROM_DT_TM, '
                                                                               'MR_VALID_FROM_DT_TM, then '
                                                                               'ADMIN_START_DT_TM. This is a '
                                                                               'source validity start with '
                                                                               'an administration-time '
                                                                               'fallback, not a uniform '
                                                                               'independent status-change '
                                                                               'timestamp; null when all '
                                                                               'three are missing.',
                                               'record_status_effective_to': 'For a superseded '
                                                                             'administration, first non-null '
                                                                             'map_med_admin.CE_VALID_UNTIL_DT_TM '
                                                                             'then MR_VALID_UNTIL_DT_TM; '
                                                                             'null for active rows or when '
                                                                             'both are missing. The end '
                                                                             'selection does not remove '
                                                                             'far-future sentinels, so '
                                                                             'supersession caused by '
                                                                             'MR_IS_CURRENT_IND=false can '
                                                                             'retain an open-ended '
                                                                             'clinical-event timestamp.',
                                               'route_code': 'Administration route code.',
                                               'route_display': 'Administration route display.',
                                               'scheduled_datetime': 'Scheduled administration time.',
                                               'site_code': 'Administration site code.',
                                               'site_display': 'Administration site display.',
                                               'source_code': 'Source order-synonym identifier.',
                                               'source_coding_system': 'Source medication coding system.',
                                               'source_display': 'Source medication display.',
                                               'source_event_type_code': 'Source medication event type code.',
                                               'source_event_type_display': 'Source medication event type '
                                                                            'display.',
                                               'source_feed': 'Registered source feed.',
                                               'source_patient_event_key': 'Stable patient-event identity '
                                                                           'retained from the originating '
                                                                           'Silver row before source '
                                                                           'splitting.',
                                               'source_update_timestamp': 'Greatest of '
                                                                          'map_med_admin.MAE_ADC_UPDT, '
                                                                          'MR_ADC_UPDT, ORDERS_ADC_UPDT, '
                                                                          'OI_ADC_UPDT, SYNONYM_ADC_UPDT, '
                                                                          'LOOKUP_ADC_UPDT and ADC_UPDT, '
                                                                          'plus the per-EVENT_ID maximum of '
                                                                          'greatest(UPDT_DT_TM, ADC_UPDT) '
                                                                          'from map_med_admin_ingredient. '
                                                                          'This combines contributing '
                                                                          'source/lookup/ingestion clocks, '
                                                                          'not a single application update '
                                                                          'time or Silver refresh time; null '
                                                                          'only when all contributing clocks '
                                                                          'are null.',
                                               'status_history_count': 'Number of retained status '
                                                                       'milestones.',
                                               'subject_id_system': 'Subject-key identifier system.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'verified_datetime': 'Date and time at which the medication '
                                                                    'administration event was verified.',
                                               'verifier_practitioner_id': 'Millennium personnel PERSON_ID '
                                                                           'for the administration verifier.',
                                               'vip_ind': 'Indicator that the source record carries VIP '
                                                          'handling status for each medication admin record. '
                                                          'It is produced by the silver transformation and '
                                                          'has no direct bronze-column lineage entry. Null '
                                                          'means the source did not state the indicator and '
                                                          'must not be interpreted as false.',
                                               'withheld_identity_ind': 'Withheld identity indicator.'},
                                   'quantity_override': True,
                                   'table': 'Internal quality-controlled twin of clinical_medication_admin: '
                                            'the Gold repairs, nulls and expectations are applied here and '
                                            'the published Gold MV reads this object. Materialized rather '
                                            'than temporary so the published MV can refresh incrementally.'},
 'clinical_medication_admin': {'columns': {'administration_status_code': 'Current source result status code.',
                                           'administration_status_display': 'Current source result status '
                                                                            'display.',
                                           'all_ingredients_zero_ind': 'True when every ingredient of this '
                                                                       'administration recorded a dose of 0 '
                                                                       '(J24): the ingredient doses are then '
                                                                       'published as NULL in ingredients and '
                                                                       'clinical_medication_admin_ingredient. '
                                                                       'INSR_W2 W2.05.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each '
                                                                   'medication admin record. It is produced '
                                                                   'by the silver transformation and has no '
                                                                   'direct bronze-column lineage entry. '
                                                                   'Source code meanings and sentinel values '
                                                                   'are retained unless the pipeline '
                                                                   'explicitly maps them; null means no code '
                                                                   'was supplied.',
                                           'dose_in_mg': 'Bronze-standardized milligram dose.',
                                           'dose_in_ml': 'Bronze-standardized millilitre dose.',
                                           'dose_standardization_status': 'Bronze dose-standardization '
                                                                          'status.',
                                           'dose_unit': 'Carried only on component 1 of a split compound '
                                                        'medication statement; aggregate over '
                                                        'medication_map_component_index = 1 or count '
                                                        'distinct source_patient_event_key.',
                                           'dose_value': 'Carried only on component 1 of a split compound '
                                                         'medication statement; aggregate over '
                                                         'medication_map_component_index = 1 or count '
                                                         'distinct source_patient_event_key.',
                                           'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                           'spine_encounter.encounter_id.',
                                           'event_datetime': 'Administration start or performed time.',
                                           'event_datetime_local': 'Medication administration start or '
                                                                   'performed time expressed in local time.',
                                           'event_end_datetime': 'Administration end time.',
                                           'event_end_datetime_local': 'Medication administration end time '
                                                                       'expressed in local time.',
                                           'event_id': 'Millennium medication-administration EVENT_ID; '
                                                       'primary key of this table.',
                                           'infused_volume': 'The volume at any one point in time that '
                                                             'remains in the IV Bag.',
                                           'infused_volume_unit': 'Text description of the unit of measure '
                                                                  'for infused volume.',
                                           'infusion_rate': 'For continuously administered medications, IV '
                                                            'or IVP, the infusion rate and unit is used to '
                                                            'capture the flow rate of the medication into '
                                                            'the patient.',
                                           'infusion_rate_unit': 'Text description of the unit of measure '
                                                                 'for volume or quantity of the medication.',
                                           'ingredient_count': 'Number of retained ingredient rows.',
                                           'initial_dose_unit': 'Initially documented dose unit.',
                                           'initial_dose_value': 'Initially documented dose.',
                                           'iv_ind': 'Indicator showing whether the order is an intravenous '
                                                     '(IV) medication order.',
                                           'loaded_at': 'Greatest of map_med_admin.ADC_UPDT and the maximum '
                                                        'map_med_admin_ingredient.ADC_UPDT for the same '
                                                        'EVENT_ID. The order-ID existence join contributes '
                                                        'no separate load timestamp. This combines '
                                                        'administration and ingredient ingestion provenance, '
                                                        'not administration event time or Silver refresh '
                                                        'time; null only when all contributing load clocks '
                                                        'are null.',
                                           'location_code': 'Millennium NURSE_UNIT_CD for the administration '
                                                            'location.',
                                           'location_key': 'Deterministic SHA-256 nurse-unit key retained '
                                                           'beside location_code.',
                                           'medication_code': 'Source and mapped medication codings.',
                                           'medication_order_key': 'Deterministic SHA-256 key of the linked '
                                                                   'medication order.',
                                           'order_id': 'Millennium medication ORDER_ID when the linked order '
                                                       'exists.',
                                           'order_status_code': 'Linked source order status code.',
                                           'order_status_display': 'Linked source order status display.',
                                           'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                              'organization.',
                                           'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                'stable cross-feed join key.',
                                           'performed_datetime': 'Performed administration time.',
                                           'performer_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                        'the administration performer.',
                                           'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                           'prn_ind': 'True when source `PRN_IND` equals 1, indicating '
                                                      'as-needed medication; other non-null values become '
                                                      'false and null remains null.',
                                           'record_status': 'Derived administration-row status: superseded '
                                                            'when map_med_admin.CE_VALID_UNTIL_DT_TM is '
                                                            'before 2100-01-01 or MR_IS_CURRENT_IND is '
                                                            'false; otherwise active. Null MR_IS_CURRENT_IND '
                                                            'defaults true and a null clinical-event end '
                                                            'alone does not supersede the row. This '
                                                            'projection does not emit retracted or derive '
                                                            'status from the administration result/workflow '
                                                            'code.',
                                           'record_status_effective_from': 'First non-null '
                                                                           'map_med_admin.CE_VALID_FROM_DT_TM, '
                                                                           'MR_VALID_FROM_DT_TM, then '
                                                                           'ADMIN_START_DT_TM. This is a '
                                                                           'source validity start with an '
                                                                           'administration-time fallback, '
                                                                           'not a uniform independent '
                                                                           'status-change timestamp; null '
                                                                           'when all three are missing.',
                                           'record_status_effective_to': 'For a superseded administration, '
                                                                         'first non-null '
                                                                         'map_med_admin.CE_VALID_UNTIL_DT_TM '
                                                                         'then MR_VALID_UNTIL_DT_TM; null '
                                                                         'for active rows or when both are '
                                                                         'missing. The end selection does '
                                                                         'not remove far-future sentinels, '
                                                                         'so supersession caused by '
                                                                         'MR_IS_CURRENT_IND=false can retain '
                                                                         'an open-ended clinical-event '
                                                                         'timestamp.',
                                           'route_code': 'Administration route code.',
                                           'route_display': 'Administration route display.',
                                           'scheduled_datetime': 'Scheduled administration time.',
                                           'site_code': 'Administration site code.',
                                           'site_display': 'Administration site display.',
                                           'source_code': 'Source order-synonym identifier.',
                                           'source_coding_system': 'Source medication coding system.',
                                           'source_display': 'Source medication display.',
                                           'source_event_type_code': 'Source medication event type code.',
                                           'source_event_type_display': 'Source medication event type '
                                                                        'display.',
                                           'source_feed': 'Registered source feed.',
                                           'source_patient_event_key': 'Stable patient-event identity '
                                                                       'retained from the originating Silver '
                                                                       'row before source splitting.',
                                           'source_update_timestamp': 'Greatest of '
                                                                      'map_med_admin.MAE_ADC_UPDT, '
                                                                      'MR_ADC_UPDT, ORDERS_ADC_UPDT, '
                                                                      'OI_ADC_UPDT, SYNONYM_ADC_UPDT, '
                                                                      'LOOKUP_ADC_UPDT and ADC_UPDT, plus '
                                                                      'the per-EVENT_ID maximum of '
                                                                      'greatest(UPDT_DT_TM, ADC_UPDT) from '
                                                                      'map_med_admin_ingredient. This '
                                                                      'combines contributing '
                                                                      'source/lookup/ingestion clocks, not a '
                                                                      'single application update time or '
                                                                      'Silver refresh time; null only when '
                                                                      'all contributing clocks are null.',
                                           'status_history_count': 'Number of retained status milestones.',
                                           'subject_id_system': 'Subject-key identifier system.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'verified_datetime': 'Date and time at which the medication '
                                                                'administration event was verified.',
                                           'verifier_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                       'the administration verifier.',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each medication admin record. It is '
                                                      'produced by the silver transformation and has no '
                                                      'direct bronze-column lineage entry. Null means the '
                                                      'source did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Withheld identity indicator.'},
                               'quantity_override': False,
                               'table': 'One Millennium medication-administration event with current state '
                                        'and ordered lifecycle evidence. Gold QC twin of the silver product: '
                                        '8 columns are repaired or nulled, 1 rule(s) drop rows, 3 check(s) '
                                        'are advisory. Each rule states its reason in the pipeline notebook, '
                                        'and Lakeflow expectation metrics report what every rule matched on '
                                        'each update. Coverage: Electronic administration recording starts '
                                        'in August 2021 (raw holds 20 rows from 2016 to July 2021) apart '
                                        'from a 2015 pilot island of about 19,000 rows; complete-through '
                                        'month per release: 6_mgmt.data_quality.release_finding '
                                        '(finding_code F2_complete_through).'}}
